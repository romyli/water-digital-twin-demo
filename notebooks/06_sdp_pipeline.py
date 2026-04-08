# Databricks notebook source

# MAGIC %md
# MAGIC # 06 — Spark Declarative Pipeline (SDP/DLT): Bronze → Silver
# MAGIC
# MAGIC This notebook defines the production-grade SDP (DLT) pipeline for the Water Digital Twin demo.
# MAGIC It transforms raw Bronze-layer data into cleansed Silver-layer tables with data quality expectations
# MAGIC and geospatial enrichment.
# MAGIC
# MAGIC **Catalog:** `water_digital_twin`
# MAGIC
# MAGIC ## Note on Data Generation Notebooks
# MAGIC The data generation notebooks (01-05) write directly to both Bronze and Silver tables for
# MAGIC convenience during demo setup. This SDP pipeline serves as the **production-grade alternative**
# MAGIC that enforces schema validation, data quality expectations, and referential integrity.
# MAGIC When running the full demo, you can either:
# MAGIC 1. Use the data gen notebooks for a quick one-time load, OR
# MAGIC 2. Load only Bronze tables and let this SDP pipeline handle all Bronze → Silver transformations.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "water_digital_twin"

# H3 resolution constants
H3_RESOLUTION_ENTITY = 8   # For sensors, properties (fine-grained)
H3_RESOLUTION_DMA = 7      # For DMA centroids (coarser)

# Default pressure thresholds (metres head)
DEFAULT_RED_THRESHOLD = 15.0
DEFAULT_AMBER_THRESHOLD = 25.0

# Valid enums
VALID_STATUS_ENUM = ["operational", "tripped", "failed", "maintenance", "decommissioned"]
VALID_PROPERTY_TYPES = [
    "domestic", "school", "hospital", "commercial",
    "nursery", "key_account", "dialysis_home"
]
VALID_COMPLAINT_TYPES = ["no_water", "low_pressure", "discoloured_water", "other"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: Streaming Telemetry
# MAGIC
# MAGIC Stream `bronze.raw_telemetry` → `silver.fact_telemetry`
# MAGIC - Join with `dim_sensor` to add `sensor_type`
# MAGIC - Split `value` into pressure fields (`value`, `total_head_pressure`) and flow field (`flow_rate`)

# COMMAND ----------

@dlt.table(
    name="fact_telemetry",
    schema=f"{CATALOG}.silver",
    comment="Cleansed sensor telemetry — 15-minute intervals with sensor type enrichment",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "sensor_id,timestamp"
    }
)
@dlt.expect("sensor_value_range_pressure",
            "sensor_type != 'pressure' OR (value BETWEEN 0 AND 120)")
@dlt.expect("sensor_value_range_flow",
            "sensor_type != 'flow' OR (flow_rate BETWEEN 0 AND 500)")
def fact_telemetry():
    """
    Stream raw telemetry from Bronze, enrich with sensor_type from dim_sensor,
    and split value into pressure/flow fields.
    """
    raw_telemetry = dlt.read_stream(f"{CATALOG}.bronze.raw_telemetry")

    # Read dim_sensor as a static table for the broadcast join
    dim_sensor = spark.read.table(f"{CATALOG}.silver.dim_sensor").select(
        "sensor_id", "sensor_type"
    )

    enriched = (
        raw_telemetry
        .join(F.broadcast(dim_sensor), on="sensor_id", how="left")
        .withColumn(
            "sensor_type",
            F.coalesce(F.col("sensor_type"), F.lit("unknown"))
        )
        # For pressure sensors: keep value, set total_head_pressure = value
        .withColumn(
            "total_head_pressure",
            F.when(F.col("sensor_type") == "pressure", F.col("value"))
             .otherwise(F.lit(None).cast(DoubleType()))
        )
        # For flow sensors: map raw value to flow_rate
        .withColumn(
            "flow_rate",
            F.when(F.col("sensor_type") == "flow", F.col("value"))
             .otherwise(F.lit(None).cast(DoubleType()))
        )
        # Null out the generic value column for flow sensors
        .withColumn(
            "value",
            F.when(F.col("sensor_type") == "pressure", F.col("value"))
             .otherwise(F.lit(None).cast(DoubleType()))
        )
        .select(
            "sensor_id",
            "timestamp",
            "sensor_type",
            "value",
            "total_head_pressure",
            "flow_rate",
            "quality_flag"
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: DMA Boundaries
# MAGIC
# MAGIC Batch `bronze.raw_dma_boundaries` → `silver.dim_dma`
# MAGIC - Compute centroid lat/lon from WKT polygon geometry
# MAGIC - Add average elevation (placeholder; in production from DEM dataset)
# MAGIC - Compute H3 index at resolution 7 for centroid
# MAGIC - Set default pressure thresholds (RED < 15.0m, AMBER < 25.0m)

# COMMAND ----------

@dlt.table(
    name="dim_dma",
    schema=f"{CATALOG}.silver",
    comment="Cleansed DMA dimension — 500 DMAs with centroids, H3 index, and default thresholds",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("null_geometry", "geometry_wkt IS NOT NULL")
def dim_dma():
    """
    Batch load DMA boundaries from Bronze and enrich with centroid, H3 index,
    and default pressure thresholds.
    """
    raw_dma = dlt.read(f"{CATALOG}.bronze.raw_dma_boundaries")

    enriched = (
        raw_dma
        # Parse WKT polygon and compute centroid
        .withColumn("geom", F.expr("ST_GeomFromWKT(geometry_wkt)"))
        .withColumn("centroid", F.expr("ST_Centroid(geom)"))
        .withColumn("centroid_latitude", F.expr("ST_Y(centroid)"))
        .withColumn("centroid_longitude", F.expr("ST_X(centroid)"))
        # Average elevation — placeholder; in production derived from a DEM dataset
        .withColumn("avg_elevation", F.lit(50.0).cast(DoubleType()))
        # H3 index at resolution 7 for DMA centroid
        .withColumn(
            "h3_index",
            F.expr(f"h3_pointash3(centroid_latitude, centroid_longitude, {H3_RESOLUTION_DMA})")
        )
        # Default pressure thresholds (metres head)
        .withColumn("pressure_red_threshold", F.lit(DEFAULT_RED_THRESHOLD))
        .withColumn("pressure_amber_threshold", F.lit(DEFAULT_AMBER_THRESHOLD))
        .select(
            "dma_code",
            "dma_name",
            "dma_area_code",
            "geometry_wkt",
            "centroid_latitude",
            "centroid_longitude",
            "avg_elevation",
            "h3_index",
            "pressure_red_threshold",
            "pressure_amber_threshold"
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: PMA Boundaries
# MAGIC
# MAGIC Batch `bronze.raw_pma_boundaries` → `silver.dim_pma`
# MAGIC - Compute centroid lat/lon from WKT polygon geometry

# COMMAND ----------

@dlt.table(
    name="dim_pma",
    schema=f"{CATALOG}.silver",
    comment="Cleansed PMA dimension — ~100 PMAs with centroid coordinates",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("null_geometry", "geometry_wkt IS NOT NULL")
def dim_pma():
    """
    Batch load PMA boundaries from Bronze and compute centroid coordinates.
    """
    raw_pma = dlt.read(f"{CATALOG}.bronze.raw_pma_boundaries")

    enriched = (
        raw_pma
        .withColumn("geom", F.expr("ST_GeomFromWKT(geometry_wkt)"))
        .withColumn("centroid", F.expr("ST_Centroid(geom)"))
        .withColumn("centroid_latitude", F.expr("ST_Y(centroid)"))
        .withColumn("centroid_longitude", F.expr("ST_X(centroid)"))
        .select(
            "pma_code",
            "pma_name",
            "dma_code",
            "geometry_wkt",
            "centroid_latitude",
            "centroid_longitude"
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: Assets
# MAGIC
# MAGIC Batch `bronze.raw_assets` → `silver.dim_assets`
# MAGIC - Normalise status to standard enum: `operational`, `tripped`, `failed`, `maintenance`, `decommissioned`
# MAGIC - Parse additional fields from `metadata_json`

# COMMAND ----------

@dlt.table(
    name="dim_assets",
    schema=f"{CATALOG}.silver",
    comment="Cleansed asset dimension — pump stations, trunk mains, valves, PRVs",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("null_geometry", "geometry_wkt IS NOT NULL")
def dim_assets():
    """
    Batch load assets from Bronze, normalise status values, and parse geometry.
    """
    raw_assets = dlt.read(f"{CATALOG}.bronze.raw_assets")

    # Normalise raw status strings to the standard enum
    status_normalised = (
        F.when(F.lower(F.col("status")).isin("operational", "active", "running", "online"), "operational")
         .when(F.lower(F.col("status")).isin("tripped", "trip"), "tripped")
         .when(F.lower(F.col("status")).isin("failed", "failure", "broken"), "failed")
         .when(F.lower(F.col("status")).isin("maintenance", "maint", "under_maintenance"), "maintenance")
         .when(F.lower(F.col("status")).isin("decommissioned", "decom", "retired"), "decommissioned")
         .otherwise("operational")
    )

    enriched = (
        raw_assets
        .withColumn("status", status_normalised)
        # Extract structured fields from metadata_json
        .withColumn(
            "diameter_inches",
            F.get_json_object(F.col("metadata_json"), "$.diameter_inches").cast("int")
        )
        .withColumn(
            "length_km",
            F.get_json_object(F.col("metadata_json"), "$.length_km").cast("double")
        )
        .withColumn(
            "trip_timestamp",
            F.get_json_object(F.col("metadata_json"), "$.trip_timestamp").cast("timestamp")
        )
        .withColumn(
            "installed_date",
            F.get_json_object(F.col("metadata_json"), "$.installed_date").cast("date")
        )
        .select(
            "asset_id",
            "asset_type",
            "name",
            "status",
            "latitude",
            "longitude",
            "geometry_wkt",
            "diameter_inches",
            "length_km",
            "trip_timestamp",
            "installed_date"
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: Customer Contacts → Properties
# MAGIC
# MAGIC Batch `bronze.raw_customer_contacts` → `silver.dim_properties`
# MAGIC - Normalise `property_type` to standard enum
# MAGIC - Generate WKT POINT geometry from lat/lon
# MAGIC - Compute H3 index at resolution 8

# COMMAND ----------

@dlt.table(
    name="dim_properties",
    schema=f"{CATALOG}.silver",
    comment="Cleansed property dimension — 50,000 properties with normalised types and H3 index",
    table_properties={"quality": "silver"}
)
@dlt.expect("referential_integrity_dma", "dma_code IS NOT NULL")
def dim_properties():
    """
    Batch load customer contacts from Bronze, normalise property types,
    generate WKT geometry, and compute H3 index.
    """
    raw_contacts = dlt.read(f"{CATALOG}.bronze.raw_customer_contacts")

    # Normalise property_type to standard enum
    property_type_normalised = (
        F.when(F.lower(F.col("property_type")).isin(
            "domestic", "residential", "house", "flat"), "domestic")
         .when(F.lower(F.col("property_type")).isin(
            "school", "primary_school", "secondary_school", "academy"), "school")
         .when(F.lower(F.col("property_type")).isin(
            "hospital", "clinic", "medical_centre", "health_centre"), "hospital")
         .when(F.lower(F.col("property_type")).isin(
            "commercial", "business", "office", "retail"), "commercial")
         .when(F.lower(F.col("property_type")).isin(
            "nursery", "childcare", "creche"), "nursery")
         .when(F.lower(F.col("property_type")).isin(
            "key_account", "key-account", "critical_infrastructure"), "key_account")
         .when(F.lower(F.col("property_type")).isin(
            "dialysis_home", "dialysis", "home_dialysis"), "dialysis_home")
         .otherwise("domestic")
    )

    enriched = (
        raw_contacts
        .withColumn("property_type", property_type_normalised)
        # Generate WKT POINT geometry from coordinates
        .withColumn(
            "geometry_wkt",
            F.concat(
                F.lit("POINT("),
                F.col("longitude").cast(StringType()),
                F.lit(" "),
                F.col("latitude").cast(StringType()),
                F.lit(")")
            )
        )
        # H3 index at resolution 8 for fine-grained spatial grouping
        .withColumn(
            "h3_index",
            F.expr(f"h3_pointash3(latitude, longitude, {H3_RESOLUTION_ENTITY})")
        )
        .select(
            "uprn",
            "address",
            "postcode",
            "property_type",
            "dma_code",
            F.lit(None).cast(StringType()).alias("pma_code"),
            "customer_height",
            "latitude",
            "longitude",
            "geometry_wkt",
            "h3_index"
        )
    )

    return enriched

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: Customer Complaints
# MAGIC
# MAGIC Batch `bronze.raw_complaints` → `silver.customer_complaints`
# MAGIC - Validate and normalise `complaint_type` to allowed enum values

# COMMAND ----------

@dlt.table(
    name="customer_complaints",
    schema=f"{CATALOG}.silver",
    comment="Cleansed customer complaints with validated complaint types",
    table_properties={"quality": "silver"}
)
@dlt.expect("complaint_type_valid",
            "complaint_type IN ('no_water', 'low_pressure', 'discoloured_water', 'other')")
@dlt.expect("referential_integrity_dma", "dma_code IS NOT NULL")
def customer_complaints():
    """
    Batch load complaints from Bronze, normalise complaint_type to the standard enum.
    """
    raw_complaints = dlt.read(f"{CATALOG}.bronze.raw_complaints")

    # Normalise complaint_type to standard enum
    complaint_type_normalised = (
        F.when(F.lower(F.col("complaint_type")).isin(
            "no_water", "no water", "supply_loss"), "no_water")
         .when(F.lower(F.col("complaint_type")).isin(
            "low_pressure", "low pressure", "weak_flow"), "low_pressure")
         .when(F.lower(F.col("complaint_type")).isin(
            "discoloured_water", "discoloured", "brown_water", "dirty_water"), "discoloured_water")
         .otherwise("other")
    )

    cleansed = (
        raw_complaints
        .withColumn("complaint_type", complaint_type_normalised)
        .select(
            "complaint_id",
            "uprn",
            "dma_code",
            "complaint_timestamp",
            "complaint_type"
        )
    )

    return cleansed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations Summary
# MAGIC
# MAGIC | Expectation | Rule | Action |
# MAGIC |---|---|---|
# MAGIC | `sensor_value_range_pressure` | Pressure BETWEEN 0 AND 120 | EXPECT (warn, keep row) |
# MAGIC | `sensor_value_range_flow` | Flow rate BETWEEN 0 AND 500 | EXPECT (warn, keep row) |
# MAGIC | `null_geometry` | `geometry_wkt IS NOT NULL` | EXPECT OR DROP (remove row) |
# MAGIC | `referential_integrity_dma` | `dma_code IS NOT NULL` | EXPECT (warn, keep row) |
# MAGIC | `complaint_type_valid` | complaint_type IN valid enum | EXPECT (warn, keep row) |
