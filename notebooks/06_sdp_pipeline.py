# Databricks notebook source

# MAGIC %md
# MAGIC # 06 — Spark Declarative Pipeline: Files → Bronze → Silver → Gold
# MAGIC
# MAGIC Full medallion SDP pipeline. Auto Loader ingests JSON files from the landing zone
# MAGIC volume into Bronze streaming tables, then transforms to Silver materialized views,
# MAGIC then aggregates into Gold materialized views.
# MAGIC
# MAGIC **Pipeline config:** catalog = `water_digital_twin`, no fixed target schema.
# MAGIC Tables are placed in `bronze`, `silver`, or `gold` via qualified names.
# MAGIC
# MAGIC **API:** `pyspark.pipelines` (Spark Declarative Pipelines).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "water_digital_twin"
VOLUME = f"/Volumes/{CATALOG}/bronze/landing_zone"

H3_RESOLUTION_ENTITY = 8
H3_RESOLUTION_DMA = 7

DEFAULT_RED_THRESHOLD = 15.0
DEFAULT_AMBER_THRESHOLD = 25.0

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Bronze Layer — Auto Loader Ingestion
# MAGIC
# MAGIC Streaming tables that ingest JSON files from the landing zone volume.

# COMMAND ----------

@dp.table(name="bronze.raw_telemetry")
def raw_telemetry():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_telemetry")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_dma_boundaries")
def raw_dma_boundaries():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_dma_boundaries")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_pma_boundaries")
def raw_pma_boundaries():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_pma_boundaries")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_assets")
def raw_assets():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_assets")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_customer_contacts")
def raw_customer_contacts():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_customer_contacts")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_complaints")
def raw_complaints():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_complaints")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_sensors")
def raw_sensors():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_sensors")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_reservoirs")
def raw_reservoirs():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_reservoirs")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_asset_dma_feed")
def raw_asset_dma_feed():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_asset_dma_feed")
    )

# COMMAND ----------

@dp.table(name="bronze.raw_reservoir_dma_feed")
def raw_reservoir_dma_feed():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"{VOLUME}/raw_reservoir_dma_feed")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Silver Layer — Cleansed Dimensions & Facts

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_telemetry
# MAGIC
# MAGIC Splits raw `value` into pressure and flow fields.

# COMMAND ----------

@dp.table(name="silver.fact_telemetry")
@dp.expect("pressure_in_range", "sensor_type != 'pressure' OR (value BETWEEN 0 AND 120)")
@dp.expect("flow_in_range", "sensor_type != 'flow' OR (flow_rate BETWEEN 0 AND 500)")
def fact_telemetry():
    raw = spark.readStream.table("bronze.raw_telemetry")

    return (
        raw
        .withColumn(
            "total_head_pressure",
            F.when(F.col("sensor_type") == "pressure", F.col("value"))
        )
        .withColumn(
            "flow_rate",
            F.when(F.col("sensor_type") == "flow", F.col("value"))
        )
        .withColumn(
            "value",
            F.when(F.col("sensor_type") == "pressure", F.col("value"))
        )
        .select(
            "sensor_id", "dma_code", "timestamp", "sensor_type",
            "value", "total_head_pressure", "flow_rate", "quality_flag"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_dma
# MAGIC
# MAGIC DMA boundaries with centroid, H3 index, and pressure thresholds.

# COMMAND ----------

@dp.materialized_view(name="silver.dim_dma")
@dp.expect_or_drop("has_geometry", "geometry_wkt IS NOT NULL")
def dim_dma():
    raw = spark.read.table("bronze.raw_dma_boundaries")

    return (
        raw
        .withColumn("geom", F.expr("ST_GeomFromWKT(geometry_wkt)"))
        .withColumn("centroid", F.expr("ST_Centroid(geom)"))
        .withColumn("centroid_latitude", F.expr("ST_Y(centroid)"))
        .withColumn("centroid_longitude", F.expr("ST_X(centroid)"))
        .withColumn(
            "h3_index",
            F.expr(f"h3_pointash3(concat('POINT(', centroid_longitude, ' ', centroid_latitude, ')'), {H3_RESOLUTION_DMA})")
        )
        .withColumn("pressure_red_threshold", F.lit(DEFAULT_RED_THRESHOLD))
        .withColumn("pressure_amber_threshold", F.lit(DEFAULT_AMBER_THRESHOLD))
        .select(
            "dma_code", "dma_name", "dma_area_code", "geometry_wkt",
            "centroid_latitude", "centroid_longitude", "avg_elevation",
            "h3_index", "pressure_red_threshold", "pressure_amber_threshold"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_pma

# COMMAND ----------

@dp.materialized_view(name="silver.dim_pma")
@dp.expect_or_drop("has_geometry", "geometry_wkt IS NOT NULL")
def dim_pma():
    raw = spark.read.table("bronze.raw_pma_boundaries")

    return (
        raw
        .withColumn("geom", F.expr("ST_GeomFromWKT(geometry_wkt)"))
        .withColumn("centroid", F.expr("ST_Centroid(geom)"))
        .withColumn("centroid_latitude", F.expr("ST_Y(centroid)"))
        .withColumn("centroid_longitude", F.expr("ST_X(centroid)"))
        .select(
            "pma_code", "pma_name", "dma_code", "geometry_wkt",
            "centroid_latitude", "centroid_longitude"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_assets
# MAGIC
# MAGIC Normalises status enum.

# COMMAND ----------

@dp.materialized_view(name="silver.dim_assets")
@dp.expect_or_drop("has_geometry", "geometry_wkt IS NOT NULL")
def dim_assets():
    raw = spark.read.table("bronze.raw_assets")

    status_norm = (
        F.when(F.lower(F.col("status")).isin("operational", "active", "running", "online"), "operational")
         .when(F.lower(F.col("status")).isin("tripped", "trip"), "tripped")
         .when(F.lower(F.col("status")).isin("failed", "failure", "broken"), "failed")
         .when(F.lower(F.col("status")).isin("maintenance", "maint", "under_maintenance"), "maintenance")
         .when(F.lower(F.col("status")).isin("decommissioned", "decom", "retired"), "decommissioned")
         .otherwise("operational")
    )

    return (
        raw
        .withColumn("status", status_norm)
        .select(
            "asset_id", "asset_type", "asset_name", "dma_code", "status",
            "latitude", "longitude", "elevation_m", "geometry_wkt",
            "diameter_inches", "length_km", "trip_timestamp", "install_date",
            "manufacturer", "model", "capacity_kw", "last_maintenance", "notes"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_properties
# MAGIC
# MAGIC Normalises property types, generates WKT geometry, computes H3 index.

# COMMAND ----------

@dp.materialized_view(name="silver.dim_properties")
@dp.expect("has_dma", "dma_code IS NOT NULL")
def dim_properties():
    raw = spark.read.table("bronze.raw_customer_contacts")

    prop_type_norm = (
        F.when(F.lower(F.col("property_type")).isin("domestic", "residential", "house", "flat"), "domestic")
         .when(F.lower(F.col("property_type")).isin("school", "primary_school", "secondary_school", "academy"), "school")
         .when(F.lower(F.col("property_type")).isin("hospital", "clinic", "medical_centre", "health_centre"), "hospital")
         .when(F.lower(F.col("property_type")).isin("commercial", "business", "office", "retail"), "commercial")
         .when(F.lower(F.col("property_type")).isin("nursery", "childcare", "creche"), "nursery")
         .when(F.lower(F.col("property_type")).isin("key_account", "key-account", "critical_infrastructure"), "key_account")
         .when(F.lower(F.col("property_type")).isin("dialysis_home", "dialysis", "home_dialysis"), "dialysis_home")
         .otherwise("domestic")
    )

    return (
        raw
        .withColumn("property_type", prop_type_norm)
        .withColumn("geometry_wkt", F.concat(F.lit("POINT("), F.col("longitude"), F.lit(" "), F.col("latitude"), F.lit(")")))
        .withColumn(
            "h3_index",
            F.expr(f"h3_pointash3(concat('POINT(', longitude, ' ', latitude, ')'), {H3_RESOLUTION_ENTITY})")
        )
        .select(
            "property_id", "address", "postcode", "property_type", "dma_code",
            "customer_height_m", "elevation_m", "latitude", "longitude",
            "occupants", "is_sensitive_premise", "geometry_wkt", "h3_index"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## customer_complaints

# COMMAND ----------

@dp.materialized_view(name="silver.customer_complaints")
@dp.expect("valid_complaint_type", "complaint_type IN ('no_water', 'low_pressure', 'discoloured_water', 'other')")
@dp.expect("has_dma", "dma_code IS NOT NULL")
def customer_complaints():
    raw = spark.read.table("bronze.raw_complaints")

    complaint_norm = (
        F.when(F.lower(F.col("complaint_type")).isin("no_water", "no water", "supply_loss"), "no_water")
         .when(F.lower(F.col("complaint_type")).isin("low_pressure", "low pressure", "weak_flow"), "low_pressure")
         .when(F.lower(F.col("complaint_type")).isin("discoloured_water", "discoloured", "brown_water", "dirty_water"), "discoloured_water")
         .otherwise("other")
    )

    return (
        raw
        .withColumn("complaint_type", complaint_norm)
        .select(
            "complaint_id", "property_id", "dma_code", "complaint_timestamp",
            "complaint_type", "description", "contact_channel",
            "customer_height_m", "property_type", "status"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_sensor

# COMMAND ----------

@dp.materialized_view(name="silver.dim_sensor")
@dp.expect("has_dma", "dma_code IS NOT NULL")
def dim_sensor():
    return spark.read.table("bronze.raw_sensors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_asset_dma_feed

# COMMAND ----------

@dp.materialized_view(name="silver.dim_asset_dma_feed")
def dim_asset_dma_feed():
    return spark.read.table("bronze.raw_asset_dma_feed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_reservoirs

# COMMAND ----------

@dp.materialized_view(name="silver.dim_reservoirs")
def dim_reservoirs():
    return spark.read.table("bronze.raw_reservoirs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_reservoir_dma_feed

# COMMAND ----------

@dp.materialized_view(name="silver.dim_reservoir_dma_feed")
def dim_reservoir_dma_feed():
    return spark.read.table("bronze.raw_reservoir_dma_feed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Gold Layer — Aggregated Views

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_dma_pressure
# MAGIC
# MAGIC Aggregated pressure metrics per DMA per timestamp.

# COMMAND ----------

@dp.materialized_view(name="gold.vw_dma_pressure")
def vw_dma_pressure():
    telemetry = spark.read.table("silver.fact_telemetry")
    dma = spark.read.table("silver.dim_dma")

    return (
        telemetry
        .filter(F.col("sensor_type") == "pressure")
        .groupBy("dma_code", "timestamp")
        .agg(
            F.avg("value").alias("avg_pressure"),
            F.max("value").alias("max_pressure"),
            F.min("value").alias("min_pressure"),
            F.avg("total_head_pressure").alias("avg_total_head_pressure"),
            F.count("*").alias("reading_count"),
        )
        .join(F.broadcast(dma.select("dma_code", "dma_name")), on="dma_code", how="left")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_reservoir_status
# MAGIC
# MAGIC Reservoir capacity and hours remaining, joined with fed DMAs.

# COMMAND ----------

@dp.materialized_view(name="gold.vw_reservoir_status")
def vw_reservoir_status():
    reservoirs = spark.read.table("silver.dim_reservoirs")
    feeds = spark.read.table("silver.dim_reservoir_dma_feed")
    dma = spark.read.table("silver.dim_dma")

    return (
        reservoirs
        .join(feeds, on="reservoir_id", how="inner")
        .join(F.broadcast(dma.select("dma_code", "dma_name")), on="dma_code", how="left")
        .select(
            "reservoir_id", "reservoir_name", "dma_code",
            F.col("dma_name").alias("fed_dma_name"),
            "feed_type", "current_level_pct", "capacity_ml",
            "hourly_demand_rate_ml", "hours_remaining",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_property_pressure
# MAGIC
# MAGIC Per-property effective pressure accounting for customer elevation.

# COMMAND ----------

@dp.materialized_view(name="gold.vw_property_pressure")
def vw_property_pressure():
    props = spark.read.table("silver.dim_properties")
    telemetry = spark.read.table("silver.fact_telemetry")

    return (
        props.select("property_id", "dma_code", "property_type", "customer_height_m")
        .join(
            telemetry.filter(F.col("sensor_type") == "pressure")
                     .select("dma_code", "timestamp", "total_head_pressure"),
            on="dma_code", how="inner"
        )
        .withColumn(
            "effective_pressure",
            F.col("total_head_pressure") - F.col("customer_height_m")
        )
    )
