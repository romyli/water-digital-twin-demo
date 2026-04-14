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

@dp.table(
    name="silver.fact_telemetry",
    comment="Sensor telemetry readings at 15-minute intervals. Pressure sensors report in the value column (metres head); flow sensors report in the flow_rate column (litres per second). Join to dim_sensor on sensor_id for sensor metadata and DMA assignment.",
    schema="""
        sensor_id STRING NOT NULL COMMENT 'Sensor that produced the reading. Foreign key to dim_sensor.'
            CONSTRAINT fk_telemetry_sensor FOREIGN KEY REFERENCES silver.dim_sensor(sensor_id),
        dma_code STRING NOT NULL COMMENT 'DMA code where the reading was taken. Foreign key to dim_dma.'
            CONSTRAINT fk_telemetry_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        timestamp STRING COMMENT 'Timestamp of the telemetry reading (UTC).',
        sensor_type STRING COMMENT 'Sensor type: pressure or flow.',
        value DOUBLE COMMENT 'Pressure reading in metres head. NULL for flow sensors.',
        total_head_pressure DOUBLE COMMENT 'Total head pressure in metres head. NULL for flow sensors.',
        flow_rate DOUBLE COMMENT 'Flow rate in litres per second. NULL for pressure sensors.',
        quality_flag STRING COMMENT 'Data quality flag for the reading.'
    """,
)
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

@dp.materialized_view(
    name="silver.dim_dma",
    comment="District Metered Area (DMA) reference data. Each DMA is a discrete, boundary-metered zone of the water distribution network with a unique code (e.g. DEMO_DMA_01). Contains centroid coordinates, H3 index, average elevation, and configurable pressure RED/AMBER thresholds.",
    schema="""
        dma_code STRING NOT NULL COMMENT 'Unique District Metered Area identifier (e.g. DEMO_DMA_01). Primary key.'
            CONSTRAINT pk_dim_dma PRIMARY KEY,
        dma_name STRING COMMENT 'Human-readable DMA name (e.g. Crystal Palace South, Sydenham Hill).',
        dma_area_code STRING COMMENT 'Parent area grouping code for the DMA.',
        geometry_wkt STRING COMMENT 'DMA boundary polygon in WKT format (WGS84 / SRID 4326).',
        centroid_latitude DOUBLE COMMENT 'Latitude of the DMA centroid (WGS84).',
        centroid_longitude DOUBLE COMMENT 'Longitude of the DMA centroid (WGS84).',
        avg_elevation DOUBLE COMMENT 'Average ground elevation in metres above ordnance datum (m AOD).',
        h3_index BIGINT COMMENT 'Uber H3 spatial index at resolution 7 for the DMA centroid.',
        pressure_red_threshold DOUBLE COMMENT 'Pressure threshold (metres head) below which the DMA is classified RED. Default: 15.0 m.',
        pressure_amber_threshold DOUBLE COMMENT 'Pressure threshold (metres head) below which the DMA is classified AMBER. Default: 25.0 m.'
    """,
)
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

@dp.materialized_view(
    name="silver.dim_pma",
    comment="Pressure Management Area (PMA) reference data. Sub-zones within DMAs where pressure is actively managed via PRVs.",
    schema="""
        pma_code STRING NOT NULL COMMENT 'Unique Pressure Management Area identifier (e.g. PMA_01). Primary key.'
            CONSTRAINT pk_dim_pma PRIMARY KEY,
        pma_name STRING COMMENT 'Human-readable PMA name.',
        dma_code STRING NOT NULL COMMENT 'Parent DMA code. Foreign key to dim_dma.'
            CONSTRAINT fk_pma_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        geometry_wkt STRING COMMENT 'PMA boundary polygon in WKT format (WGS84 / SRID 4326).',
        centroid_latitude DOUBLE COMMENT 'Latitude of the PMA centroid (WGS84).',
        centroid_longitude DOUBLE COMMENT 'Longitude of the PMA centroid (WGS84).'
    """,
)
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

@dp.materialized_view(
    name="silver.dim_assets",
    comment="Infrastructure asset register covering pump stations, trunk mains, isolation valves, and pressure reducing valves (PRVs). Contains asset status, geographic location, and maintenance history. Use dim_asset_dma_feed to find which DMAs an asset serves.",
    schema="""
        asset_id STRING NOT NULL COMMENT 'Unique asset identifier (e.g. DEMO_PUMP_01, TM_003). Primary key.'
            CONSTRAINT pk_dim_assets PRIMARY KEY,
        asset_type STRING COMMENT 'Asset type: pump_station, trunk_main, isolation_valve, or prv.',
        asset_name STRING COMMENT 'Human-readable asset name.',
        dma_code STRING COMMENT 'Primary DMA. Use dim_asset_dma_feed for full mapping.'
            CONSTRAINT fk_assets_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        status STRING COMMENT 'Status: operational, tripped, failed, maintenance, or decommissioned.',
        latitude DOUBLE COMMENT 'Asset latitude (WGS84).',
        longitude DOUBLE COMMENT 'Asset longitude (WGS84).',
        elevation_m DOUBLE COMMENT 'Elevation in metres above ordnance datum (m AOD).',
        geometry_wkt STRING COMMENT 'WKT geometry (WGS84). POINT for pumps/valves; LINESTRING for mains.',
        diameter_inches DOUBLE COMMENT 'Pipe diameter in inches. NULL for pump stations.',
        length_km DOUBLE COMMENT 'Pipeline length in km. Trunk mains only.',
        trip_timestamp STRING COMMENT 'Timestamp when the asset tripped (UTC). NULL if not tripped.',
        install_date STRING COMMENT 'Installation date (YYYY-MM-DD).',
        manufacturer STRING COMMENT 'Asset manufacturer.',
        model STRING COMMENT 'Asset model or specification.',
        capacity_kw DOUBLE COMMENT 'Pump motor capacity in kW. Pump stations only.',
        last_maintenance STRING COMMENT 'Date of most recent maintenance (YYYY-MM-DD).',
        notes STRING COMMENT 'Free-text notes about the asset.'
    """,
)
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

@dp.materialized_view(
    name="silver.dim_properties",
    comment="Property register for all residential and non-residential premises in the network. Includes property type classification (domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account), sensitive premise flags, elevation, and expected base pressure.",
    schema="""
        property_id STRING NOT NULL COMMENT 'Unique property identifier (format: PROP_NNNNNN). Primary key.'
            CONSTRAINT pk_dim_properties PRIMARY KEY,
        address STRING COMMENT 'Street address of the property.',
        postcode STRING COMMENT 'UK postcode of the property.',
        property_type STRING COMMENT 'Property classification: domestic, commercial, school, hospital, care_home, dialysis_home, nursery, or key_account.',
        dma_code STRING NOT NULL COMMENT 'DMA code the property is located in. Foreign key to dim_dma.'
            CONSTRAINT fk_property_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        customer_height_m DOUBLE COMMENT 'Customer tap point height above ordnance datum in metres.',
        elevation_m DOUBLE COMMENT 'Ground elevation in metres above ordnance datum (m AOD).',
        base_pressure DOUBLE COMMENT 'Expected normal pressure at the tap in metres head (m).',
        latitude DOUBLE COMMENT 'Property latitude (WGS84).',
        longitude DOUBLE COMMENT 'Property longitude (WGS84).',
        occupants BIGINT COMMENT 'Number of occupants at the property.',
        is_sensitive_premise BOOLEAN COMMENT 'TRUE if school, hospital, care home, or dialysis-dependent home. Priority response required.',
        sensitive_premise_type STRING COMMENT 'Type of sensitive premise: school, hospital, care_home, or dialysis_home. NULL for non-sensitive.',
        geometry_wkt STRING COMMENT 'Property location as WKT POINT (WGS84 / SRID 4326).',
        h3_index BIGINT COMMENT 'Uber H3 spatial index at resolution 8.'
    """,
)
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
            "customer_height_m", "elevation_m", "base_pressure", "latitude", "longitude",
            "occupants", "is_sensitive_premise", "sensitive_premise_type",
            "geometry_wkt", "h3_index"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## customer_complaints

# COMMAND ----------

@dp.materialized_view(
    name="silver.customer_complaints",
    comment="Customer complaints received via contact centre. Each row is a single complaint with type (no_water, low_pressure, discoloured_water, other), DMA assignment, contact channel, and resolution status.",
    schema="""
        complaint_id STRING NOT NULL COMMENT 'Unique complaint identifier. Primary key.'
            CONSTRAINT pk_customer_complaints PRIMARY KEY,
        property_id STRING COMMENT 'Property that lodged the complaint. Foreign key to dim_properties.'
            CONSTRAINT fk_complaint_property FOREIGN KEY REFERENCES silver.dim_properties(property_id),
        dma_code STRING NOT NULL COMMENT 'DMA code of the complaining property. Foreign key to dim_dma.'
            CONSTRAINT fk_complaint_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        complaint_timestamp STRING COMMENT 'Timestamp when the complaint was received (UTC).',
        complaint_type STRING COMMENT 'Category: no_water, low_pressure, discoloured_water, or other.',
        description STRING COMMENT 'Free-text complaint description from the customer.',
        contact_channel STRING COMMENT 'Channel: phone, web, or email.',
        customer_height_m DOUBLE COMMENT 'Customer tap height in metres above ordnance datum.',
        property_type STRING COMMENT 'Property type of the complainant.',
        status STRING COMMENT 'Complaint resolution status: open, resolved, or closed.'
    """,
)
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

@dp.materialized_view(
    name="silver.dim_sensor",
    comment="Sensor registry for all pressure and flow sensors deployed across the network. Contains sensor type, DMA assignment, geographic coordinates, installation date, calibration history, and operational status (active or maintenance).",
    schema="""
        sensor_id STRING NOT NULL COMMENT 'Unique sensor identifier. Primary key.'
            CONSTRAINT pk_dim_sensor PRIMARY KEY,
        sensor_type STRING COMMENT 'Sensor measurement type: pressure (metres head) or flow (litres per second).',
        dma_code STRING NOT NULL COMMENT 'DMA code where the sensor is located. Foreign key to dim_dma.'
            CONSTRAINT fk_sensor_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        latitude DOUBLE COMMENT 'Sensor latitude (WGS84).',
        longitude DOUBLE COMMENT 'Sensor longitude (WGS84).',
        elevation_m DOUBLE COMMENT 'Sensor elevation in metres above ordnance datum (m AOD).',
        install_date STRING COMMENT 'Date the sensor was installed (YYYY-MM-DD).',
        manufacturer STRING COMMENT 'Sensor hardware manufacturer.',
        model STRING COMMENT 'Sensor model identifier.',
        status STRING COMMENT 'Operational status: active or maintenance.',
        last_calibration STRING COMMENT 'Date of most recent calibration (YYYY-MM-DD).'
    """,
)
@dp.expect("has_dma", "dma_code IS NOT NULL")
def dim_sensor():
    return (
        spark.read.table("bronze.raw_sensors")
        .select(
            "sensor_id", "sensor_type", "dma_code",
            "latitude", "longitude", "elevation_m",
            "install_date", "manufacturer", "model",
            "status", "last_calibration"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_asset_dma_feed

# COMMAND ----------

@dp.materialized_view(
    name="silver.dim_asset_dma_feed",
    comment="Bridge table mapping infrastructure assets (pumps, mains, valves) to the DMAs they serve. Use this table to find which pump stations feed a given DMA or which DMAs are affected when an asset trips.",
    schema="""
        asset_id STRING NOT NULL COMMENT 'Asset identifier. Foreign key to dim_assets.'
            CONSTRAINT fk_asset_feed_asset FOREIGN KEY REFERENCES silver.dim_assets(asset_id),
        dma_code STRING NOT NULL COMMENT 'DMA code served by this asset. Foreign key to dim_dma.'
            CONSTRAINT fk_asset_feed_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        feed_type STRING COMMENT 'Feed relationship: primary (main supply) or secondary (supplementary).',
        notes STRING COMMENT 'Description of the feed relationship.'
    """,
)
def dim_asset_dma_feed():
    return (
        spark.read.table("bronze.raw_asset_dma_feed")
        .select("asset_id", "dma_code", "feed_type", "notes")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_reservoirs

# COMMAND ----------

@dp.materialized_view(
    name="silver.dim_reservoirs",
    comment="Service reservoir metadata including total capacity, current fill level, hourly demand rate, and estimated hours of supply remaining. Use dim_reservoir_dma_feed to find which DMAs a reservoir feeds.",
    schema="""
        reservoir_id STRING NOT NULL COMMENT 'Unique service reservoir identifier (e.g. DEMO_SR_01). Primary key.'
            CONSTRAINT pk_dim_reservoirs PRIMARY KEY,
        reservoir_name STRING COMMENT 'Human-readable reservoir name.',
        latitude DOUBLE COMMENT 'Reservoir latitude (WGS84).',
        longitude DOUBLE COMMENT 'Reservoir longitude (WGS84).',
        elevation_m DOUBLE COMMENT 'Elevation in metres above ordnance datum (m AOD).',
        capacity_ml DOUBLE COMMENT 'Total storage capacity in megalitres (ML).',
        current_level_pct DOUBLE COMMENT 'Current fill level as percentage (0-100%).',
        current_volume_ml DOUBLE COMMENT 'Current stored volume in megalitres (ML).',
        hourly_demand_rate_ml DOUBLE COMMENT 'Hourly draw-down rate in ML/hr.',
        hours_remaining DOUBLE COMMENT 'Estimated hours of supply remaining at current demand.',
        status STRING COMMENT 'Operational status: active or offline.',
        last_inspection STRING COMMENT 'Date of most recent inspection (YYYY-MM-DD).',
        notes STRING COMMENT 'Free-text notes about the reservoir.'
    """,
)
def dim_reservoirs():
    return (
        spark.read.table("bronze.raw_reservoirs")
        .select(
            "reservoir_id", "reservoir_name",
            "latitude", "longitude", "elevation_m",
            "capacity_ml", "current_level_pct", "current_volume_ml",
            "hourly_demand_rate_ml", "hours_remaining",
            "status", "last_inspection", "notes"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_reservoir_dma_feed

# COMMAND ----------

@dp.materialized_view(
    name="silver.dim_reservoir_dma_feed",
    comment="Bridge table mapping service reservoirs to the DMAs they supply. Feed type indicates primary (direct) or secondary (supplementary) supply relationship.",
    schema="""
        reservoir_id STRING NOT NULL COMMENT 'Reservoir identifier. Foreign key to dim_reservoirs.'
            CONSTRAINT fk_reservoir_feed_reservoir FOREIGN KEY REFERENCES silver.dim_reservoirs(reservoir_id),
        dma_code STRING NOT NULL COMMENT 'DMA code fed by this reservoir. Foreign key to dim_dma.'
            CONSTRAINT fk_reservoir_feed_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        feed_type STRING COMMENT 'Feed type: primary (direct supply) or secondary (supplementary).'
    """,
)
def dim_reservoir_dma_feed():
    return (
        spark.read.table("bronze.raw_reservoir_dma_feed")
        .select("reservoir_id", "dma_code", "feed_type")
    )

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

@dp.materialized_view(
    name="gold.vw_dma_pressure",
    comment="Pre-aggregated pressure metrics per DMA per 15-minute interval. One row per DMA per timestamp. Use for: pressure trend analysis, DMA comparison, anomaly correlation. Grain: dma_code + timestamp.",
    schema="""
        dma_code STRING COMMENT 'District Metered Area identifier. Foreign key to dim_dma.'
            CONSTRAINT fk_dma_pressure_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        timestamp STRING COMMENT 'Timestamp of the 15-minute telemetry aggregation window.',
        avg_pressure DOUBLE COMMENT 'Average pressure in metres head across all pressure sensors in the DMA.',
        max_pressure DOUBLE COMMENT 'Maximum pressure reading in the DMA (metres head).',
        min_pressure DOUBLE COMMENT 'Minimum pressure reading in the DMA (metres head). Low values indicate supply issues.',
        avg_total_head_pressure DOUBLE COMMENT 'Average total head pressure in metres head.',
        reading_count LONG COMMENT 'Number of individual sensor readings in this aggregation window.',
        dma_name STRING COMMENT 'Human-readable DMA display name.'
    """,
)
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

@dp.materialized_view(
    name="gold.vw_reservoir_status",
    comment="Service reservoir status joined with DMA feed topology. One row per reservoir-DMA feed relationship. Use for: reservoir monitoring, supply risk assessment, DMA resilience analysis. Grain: reservoir_id + dma_code.",
    schema="""
        reservoir_id STRING COMMENT 'Unique service reservoir identifier (e.g. DEMO_SR_01). Foreign key to dim_reservoirs.'
            CONSTRAINT fk_res_status_reservoir FOREIGN KEY REFERENCES silver.dim_reservoirs(reservoir_id),
        reservoir_name STRING COMMENT 'Human-readable reservoir name.',
        dma_code STRING COMMENT 'DMA code of the area fed by this reservoir. Foreign key to dim_dma.'
            CONSTRAINT fk_res_status_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        fed_dma_name STRING COMMENT 'Display name of the fed DMA.',
        feed_type STRING COMMENT 'Feed relationship: primary (main supply) or secondary (supplementary).',
        current_level_pct DOUBLE COMMENT 'Current reservoir fill level as percentage (0-100%).',
        capacity_ml DOUBLE COMMENT 'Total reservoir storage capacity in megalitres (ML).',
        hourly_demand_rate_ml DOUBLE COMMENT 'Hourly draw-down rate in megalitres per hour (ML/hr).',
        hours_remaining DOUBLE COMMENT 'Estimated hours of supply remaining at current demand rate.'
    """,
)
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

@dp.materialized_view(
    name="gold.vw_property_pressure",
    comment="Per-property effective pressure at each 15-minute interval, accounting for customer tap height above ordnance datum. Effective pressure = total_head_pressure - customer_height_m. Use for: property-level impact assessment, sensitive premises analysis. Grain: property_id + timestamp.",
    schema="""
        property_id STRING COMMENT 'Unique property identifier. Foreign key to dim_properties.'
            CONSTRAINT fk_prop_pressure_property FOREIGN KEY REFERENCES silver.dim_properties(property_id),
        dma_code STRING COMMENT 'DMA code the property belongs to. Foreign key to dim_dma.'
            CONSTRAINT fk_prop_pressure_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        property_type STRING COMMENT 'Property classification: domestic, commercial, school, hospital, care_home, dialysis_home, nursery, or key_account.',
        customer_height_m DOUBLE COMMENT 'Customer tap point height above ordnance datum in metres.',
        base_pressure DOUBLE COMMENT 'Expected normal pressure at the tap (metres head).',
        timestamp STRING COMMENT 'Timestamp of the 15-minute pressure reading.',
        total_head_pressure DOUBLE COMMENT 'Measured total head pressure from the DMA sensors (metres head).',
        effective_pressure DOUBLE COMMENT 'Pressure at the customer tap = total_head_pressure - customer_height_m (metres head). Negative values indicate no supply.'
    """,
)
def vw_property_pressure():
    props = spark.read.table("silver.dim_properties")
    telemetry = spark.read.table("silver.fact_telemetry")

    return (
        props.select("property_id", "dma_code", "property_type", "customer_height_m", "base_pressure")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## mv_sensor_latest
# MAGIC
# MAGIC Pre-computed latest reading per sensor. Eliminates slow LATERAL JOINs
# MAGIC against `fact_telemetry` at query time.

# COMMAND ----------

@dp.materialized_view(
    name="gold.mv_sensor_latest",
    comment="Latest telemetry reading per sensor, pre-joined with sensor metadata. Eliminates slow LATERAL JOINs at query time. One row per sensor. Use for: real-time sensor dashboards, current DMA status, map overlays. Grain: sensor_id.",
    schema="""
        sensor_id STRING COMMENT 'Unique sensor identifier. Foreign key to dim_sensor.'
            CONSTRAINT fk_sensor_latest_sensor FOREIGN KEY REFERENCES silver.dim_sensor(sensor_id),
        latitude DOUBLE COMMENT 'Sensor latitude (WGS84).',
        longitude DOUBLE COMMENT 'Sensor longitude (WGS84).',
        elevation_m DOUBLE COMMENT 'Sensor elevation in metres above ordnance datum (m AOD).',
        status STRING COMMENT 'Sensor operational status: active or maintenance.',
        dma_code STRING COMMENT 'DMA code where the sensor is located. Foreign key to dim_dma.'
            CONSTRAINT fk_sensor_latest_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        timestamp STRING COMMENT 'Timestamp of the most recent reading.',
        sensor_type STRING COMMENT 'Sensor measurement type: pressure (metres head) or flow (litres per second).',
        value DOUBLE COMMENT 'Latest pressure reading in metres head. NULL for flow sensors.',
        total_head_pressure DOUBLE COMMENT 'Latest total head pressure in metres head. NULL for flow sensors.',
        flow_rate DOUBLE COMMENT 'Latest flow rate in litres per second. NULL for pressure sensors.',
        quality_flag STRING COMMENT 'Data quality flag for the reading.'
    """,
)
def mv_sensor_latest():
    telemetry = spark.read.table("silver.fact_telemetry")
    sensors = spark.read.table("silver.dim_sensor")

    # Window to find the latest reading per sensor
    from pyspark.sql.window import Window
    w = Window.partitionBy("sensor_id").orderBy(F.col("timestamp").desc())

    latest = (
        telemetry
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "sensor_id", "dma_code", "timestamp", "sensor_type",
            "value", "total_head_pressure", "flow_rate", "quality_flag"
        )
    )

    return (
        sensors.select("sensor_id", "latitude", "longitude", "elevation_m", "status")
        .join(latest, on="sensor_id", how="inner")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## vw_dma_summary
# MAGIC
# MAGIC Pre-aggregated DMA-level metrics: total properties, sensitive premises count,
# MAGIC and downstream DMA count (from asset-DMA feed topology).

# COMMAND ----------

@dp.materialized_view(
    name="gold.vw_dma_summary",
    comment="Pre-aggregated DMA-level summary: total properties, sensitive premises count, and downstream DMA count from asset-DMA feed topology. One row per DMA. Use for: DMA overview panels, map tooltips, quick reference. Grain: dma_code.",
    schema="""
        dma_code STRING COMMENT 'District Metered Area identifier. Foreign key to dim_dma.'
            CONSTRAINT fk_dma_summary_dma FOREIGN KEY REFERENCES silver.dim_dma(dma_code),
        dma_name STRING COMMENT 'Human-readable DMA display name.',
        dma_area_code STRING COMMENT 'Parent area grouping code.',
        avg_elevation DOUBLE COMMENT 'Average ground elevation in metres above ordnance datum (m AOD).',
        centroid_latitude DOUBLE COMMENT 'Latitude of the DMA centroid (WGS84).',
        centroid_longitude DOUBLE COMMENT 'Longitude of the DMA centroid (WGS84).',
        total_properties LONG COMMENT 'Total number of properties (residential and non-residential) in the DMA.',
        sensitive_premises_count LONG COMMENT 'Number of sensitive premises (hospitals, schools, care homes, dialysis homes) in the DMA.',
        downstream_dma_count LONG COMMENT 'Number of DMAs with secondary feed dependency on assets that also serve this DMA.'
    """,
)
def vw_dma_summary():
    props = spark.read.table("silver.dim_properties")
    dma = spark.read.table("silver.dim_dma")
    feeds = spark.read.table("silver.dim_asset_dma_feed")

    # Count properties and sensitive premises per DMA
    prop_stats = (
        props
        .groupBy("dma_code")
        .agg(
            F.count("*").alias("total_properties"),
            F.sum(F.when(F.col("is_sensitive_premise") == True, 1).otherwise(0)).alias("sensitive_premises_count"),
        )
    )

    # Count downstream DMAs: for each DMA, count how many OTHER DMAs share
    # a common feeding asset (secondary feeds = downstream dependency)
    downstream = (
        feeds
        .filter(F.col("feed_type") == "secondary")
        .groupBy("dma_code")
        .agg(F.count("*").alias("downstream_dma_count"))
    )

    return (
        dma.select("dma_code", "dma_name", "dma_area_code", "avg_elevation",
                    "centroid_latitude", "centroid_longitude")
        .join(prop_stats, on="dma_code", how="left")
        .join(downstream, on="dma_code", how="left")
        .fillna(0, subset=["total_properties", "sensitive_premises_count", "downstream_dma_count"])
    )
