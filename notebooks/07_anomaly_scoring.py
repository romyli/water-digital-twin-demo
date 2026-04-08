# Databricks notebook source

# MAGIC %md
# MAGIC # 07 — Anomaly Scoring & Status Computation
# MAGIC
# MAGIC Computes Gold-layer tables from Silver telemetry and dimension data:
# MAGIC 1. **gold.anomaly_scores** — Per-sensor statistical anomaly detection (rolling 7-day baseline)
# MAGIC 2. **gold.dma_rag_history** — RAG status per DMA per 15-minute interval
# MAGIC 3. **gold.dma_status** — Current RAG status per DMA (latest window)
# MAGIC 4. **gold.dma_summary** — Pre-materialised DMA summary for sub-second dashboard rendering
# MAGIC
# MAGIC **Catalog:** `water_digital_twin`
# MAGIC **Demo timestamp:** `2026-04-07 05:30:00`
# MAGIC
# MAGIC All writes use overwrite mode to ensure idempotency.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, BooleanType, StringType, IntegerType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "water_digital_twin"
DEMO_TIMESTAMP = "2026-04-07 05:30:00"

# Anomaly detection parameters
ANOMALY_SIGMA_THRESHOLD = 2.5   # Readings above this are flagged as anomalies
BASELINE_WINDOW_DAYS = 7        # Rolling baseline window
BASELINE_TIME_TOLERANCE_MIN = 15  # +/- minutes for same-time-of-day matching

# RAG status thresholds (metres head) — defaults; per-DMA overrides from dim_dma
DEFAULT_RED_THRESHOLD = 15.0
DEFAULT_AMBER_THRESHOLD = 25.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anomaly Scores (`gold.anomaly_scores`)
# MAGIC
# MAGIC For each sensor reading in `silver.fact_telemetry`:
# MAGIC - Compute rolling 7-day baseline at the same time of day (+/- 15 min)
# MAGIC - `anomaly_sigma` = |actual - baseline| / std_dev
# MAGIC - `is_anomaly` = TRUE if sigma > 2.5
# MAGIC - `scoring_method` = 'statistical'
# MAGIC
# MAGIC **Critical demo assertions:**
# MAGIC - DEMO_SENSOR_01 after 02:03 on 2026-04-07 must score > 3.0 sigma
# MAGIC - DEMO_DMA_02/03 sensors must remain < 2.0 sigma

# COMMAND ----------

def compute_anomaly_scores():
    """
    Compute statistical anomaly scores using a rolling 7-day same-time-of-day baseline.
    
    For each sensor reading at time T:
    1. Collect all readings from the same sensor at time T +/- 15 min over the prior 7 days
    2. Compute mean (baseline) and std_dev of those historical readings
    3. anomaly_sigma = |actual - baseline| / std_dev
    """
    fact_telemetry = spark.read.table(f"{CATALOG}.silver.fact_telemetry")
    
    # Extract time-of-day features for matching
    telemetry = (
        fact_telemetry
        .withColumn("reading_date", F.to_date("timestamp"))
        .withColumn("minutes_of_day",
                     F.hour("timestamp") * 60 + F.minute("timestamp"))
    )
    
    # Self-join: for each reading, find historical readings at the same time of day
    # within the 7-day window
    current = telemetry.alias("curr")
    historical = telemetry.alias("hist")
    
    baseline_df = (
        current.join(
            historical,
            (F.col("curr.sensor_id") == F.col("hist.sensor_id"))
            & (F.col("hist.reading_date") >= F.date_sub(F.col("curr.reading_date"), BASELINE_WINDOW_DAYS))
            & (F.col("hist.reading_date") < F.col("curr.reading_date"))
            & (F.abs(F.col("hist.minutes_of_day") - F.col("curr.minutes_of_day")) <= BASELINE_TIME_TOLERANCE_MIN),
            how="left"
        )
        .groupBy(
            F.col("curr.sensor_id").alias("sensor_id"),
            F.col("curr.timestamp").alias("timestamp"),
            F.col("curr.sensor_type").alias("sensor_type"),
            F.col("curr.value").alias("pressure_value"),
            F.col("curr.flow_rate").alias("flow_rate")
        )
        .agg(
            F.avg(
                F.when(F.col("curr.sensor_type") == "pressure", F.col("hist.value"))
                 .otherwise(F.col("hist.flow_rate"))
            ).alias("baseline_value"),
            F.stddev(
                F.when(F.col("curr.sensor_type") == "pressure", F.col("hist.value"))
                 .otherwise(F.col("hist.flow_rate"))
            ).alias("baseline_stddev")
        )
    )
    
    # Compute anomaly sigma
    anomaly_scores = (
        baseline_df
        .withColumn(
            "actual_value",
            F.when(F.col("sensor_type") == "pressure", F.col("pressure_value"))
             .otherwise(F.col("flow_rate"))
        )
        # Guard against null/zero std_dev
        .withColumn(
            "baseline_stddev_safe",
            F.when(
                (F.col("baseline_stddev").isNull()) | (F.col("baseline_stddev") < 0.001),
                F.lit(0.001)
            ).otherwise(F.col("baseline_stddev"))
        )
        .withColumn(
            "anomaly_sigma",
            F.round(
                F.abs(F.col("actual_value") - F.col("baseline_value")) / F.col("baseline_stddev_safe"),
                2
            )
        )
        # Handle cases where baseline is null (no historical data yet)
        .withColumn(
            "anomaly_sigma",
            F.coalesce(F.col("anomaly_sigma"), F.lit(0.0))
        )
        .withColumn(
            "baseline_value",
            F.round(F.coalesce(F.col("baseline_value"), F.col("actual_value")), 2)
        )
        .withColumn(
            "is_anomaly",
            F.col("anomaly_sigma") > ANOMALY_SIGMA_THRESHOLD
        )
        .withColumn("scoring_method", F.lit("statistical"))
        .select(
            "sensor_id",
            "timestamp",
            F.round("anomaly_sigma", 2).alias("anomaly_sigma"),
            "baseline_value",
            F.round("actual_value", 2).alias("actual_value"),
            "is_anomaly",
            "scoring_method"
        )
    )
    
    return anomaly_scores

# COMMAND ----------

anomaly_scores_df = compute_anomaly_scores()

# Write to gold.anomaly_scores — full overwrite for idempotency
(
    anomaly_scores_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.anomaly_scores")
)

print(f"Wrote {spark.read.table(f'{CATALOG}.gold.anomaly_scores').count()} anomaly score rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Anomaly Score Demo Assertions

# COMMAND ----------

# Validate: DEMO_SENSOR_01 after 02:03 should have sigma > 3.0
demo_sensor_01_post_trip = spark.sql(f"""
    SELECT sensor_id, timestamp, anomaly_sigma, actual_value, baseline_value, is_anomaly
    FROM {CATALOG}.gold.anomaly_scores
    WHERE sensor_id = 'DEMO_SENSOR_01'
      AND timestamp > '2026-04-07 02:03:00'
    ORDER BY timestamp
    LIMIT 10
""")
display(demo_sensor_01_post_trip)

# COMMAND ----------

# Validate: DEMO_DMA_02/03 sensors should have sigma < 2.0
amber_dma_scores = spark.sql(f"""
    SELECT a.sensor_id, s.dma_code, a.timestamp, a.anomaly_sigma, a.is_anomaly
    FROM {CATALOG}.gold.anomaly_scores a
    JOIN {CATALOG}.silver.dim_sensor s ON a.sensor_id = s.sensor_id
    WHERE s.dma_code IN ('DEMO_DMA_02', 'DEMO_DMA_03')
      AND a.timestamp >= '2026-04-07 02:00:00'
    ORDER BY a.anomaly_sigma DESC
    LIMIT 10
""")
display(amber_dma_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DMA RAG History (`gold.dma_rag_history`)
# MAGIC
# MAGIC Per DMA per 15-minute interval:
# MAGIC - Compute avg/min pressure from `fact_telemetry` (pressure sensors only)
# MAGIC - RED if `min_pressure < red_threshold` (15.0m)
# MAGIC - AMBER if `min_pressure < amber_threshold` (25.0m)
# MAGIC - GREEN otherwise
# MAGIC
# MAGIC **Demo timeline:**
# MAGIC - DEMO_DMA_01: GREEN -> AMBER (~02:15) -> RED (~02:30 onward)
# MAGIC - DEMO_DMA_02/03: GREEN -> AMBER (~02:30 onward)

# COMMAND ----------

def compute_dma_rag_history():
    """
    Compute RAG status for each DMA at each 15-minute telemetry interval.
    Uses pressure sensor readings joined with DMA-level thresholds.
    """
    fact_telemetry = spark.read.table(f"{CATALOG}.silver.fact_telemetry")
    dim_sensor = spark.read.table(f"{CATALOG}.silver.dim_sensor")
    dim_dma = spark.read.table(f"{CATALOG}.silver.dim_dma")
    
    # Aggregate pressure readings per DMA per 15-min interval
    dma_pressure = (
        fact_telemetry
        .filter(F.col("sensor_type") == "pressure")
        .join(dim_sensor.select("sensor_id", "dma_code"), on="sensor_id", how="inner")
        .groupBy("dma_code", "timestamp")
        .agg(
            F.round(F.avg("value"), 2).alias("avg_pressure"),
            F.round(F.min("value"), 2).alias("min_pressure")
        )
    )
    
    # Join with DMA thresholds and compute RAG status
    rag_history = (
        dma_pressure
        .join(
            dim_dma.select("dma_code", "pressure_red_threshold", "pressure_amber_threshold"),
            on="dma_code",
            how="left"
        )
        .withColumn(
            "red_threshold",
            F.coalesce(F.col("pressure_red_threshold"), F.lit(DEFAULT_RED_THRESHOLD))
        )
        .withColumn(
            "amber_threshold",
            F.coalesce(F.col("pressure_amber_threshold"), F.lit(DEFAULT_AMBER_THRESHOLD))
        )
        .withColumn(
            "rag_status",
            F.when(F.col("min_pressure") < F.col("red_threshold"), "RED")
             .when(F.col("min_pressure") < F.col("amber_threshold"), "AMBER")
             .otherwise("GREEN")
        )
        .select(
            "dma_code",
            "timestamp",
            "rag_status",
            "avg_pressure",
            "min_pressure"
        )
    )
    
    return rag_history

# COMMAND ----------

rag_history_df = compute_dma_rag_history()

(
    rag_history_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dma_rag_history")
)

print(f"Wrote {spark.read.table(f'{CATALOG}.gold.dma_rag_history').count()} RAG history rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate RAG History Demo Assertions

# COMMAND ----------

# DEMO_DMA_01 timeline: should go GREEN -> AMBER (~02:15) -> RED (~02:30+)
demo_dma_01_timeline = spark.sql(f"""
    SELECT dma_code, timestamp, rag_status, avg_pressure, min_pressure
    FROM {CATALOG}.gold.dma_rag_history
    WHERE dma_code = 'DEMO_DMA_01'
      AND timestamp BETWEEN '2026-04-07 01:00:00' AND '2026-04-07 05:30:00'
    ORDER BY timestamp
""")
display(demo_dma_01_timeline)

# COMMAND ----------

# DEMO_DMA_02/03 timeline: should go GREEN -> AMBER (~02:30+), never RED
amber_dma_timeline = spark.sql(f"""
    SELECT dma_code, timestamp, rag_status, avg_pressure, min_pressure
    FROM {CATALOG}.gold.dma_rag_history
    WHERE dma_code IN ('DEMO_DMA_02', 'DEMO_DMA_03')
      AND timestamp BETWEEN '2026-04-07 01:00:00' AND '2026-04-07 05:30:00'
    ORDER BY dma_code, timestamp
""")
display(amber_dma_timeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DMA Status (`gold.dma_status`)
# MAGIC
# MAGIC Current RAG status per DMA — one row per DMA from the latest 15-minute window.
# MAGIC Joins with `dim_incidents` for `has_active_incident`.
# MAGIC
# MAGIC **At demo time (2026-04-07 05:30:00):** 1 RED, 2 AMBER, 497 GREEN

# COMMAND ----------

def compute_dma_status():
    """
    Materialise the current DMA status from the latest RAG history entry per DMA.
    Enriches with sensor/property counts and active incident flag.
    """
    rag_history = spark.read.table(f"{CATALOG}.gold.dma_rag_history")
    dim_sensor = spark.read.table(f"{CATALOG}.silver.dim_sensor")
    dim_properties = spark.read.table(f"{CATALOG}.silver.dim_properties")
    dim_incidents = spark.read.table(f"{CATALOG}.gold.dim_incidents")
    dim_dma = spark.read.table(f"{CATALOG}.silver.dim_dma")
    
    # Get the latest RAG entry per DMA
    latest_window = Window.partitionBy("dma_code").orderBy(F.desc("timestamp"))
    
    latest_rag = (
        rag_history
        .withColumn("rn", F.row_number().over(latest_window))
        .filter(F.col("rn") == 1)
        .select("dma_code", "rag_status", "avg_pressure", "min_pressure")
    )
    
    # Sensor counts per DMA (active sensors only)
    sensor_counts = (
        dim_sensor
        .filter(F.col("is_active") == True)
        .groupBy("dma_code")
        .agg(F.count("sensor_id").alias("sensor_count"))
    )
    
    # Property counts per DMA
    property_counts = (
        dim_properties
        .groupBy("dma_code")
        .agg(
            F.count("uprn").alias("property_count"),
            F.sum(
                F.when(
                    F.col("property_type").isin("hospital", "school", "dialysis_home"),
                    F.lit(1)
                ).otherwise(F.lit(0))
            ).cast(IntegerType()).alias("sensitive_premises_count")
        )
    )
    
    # Active incidents per DMA
    active_incidents = (
        dim_incidents
        .filter(F.col("status") == "active")
        .select("dma_code")
        .distinct()
        .withColumn("has_active_incident", F.lit(True))
    )
    
    # All DMAs as the base (ensures we have 500 rows even if some have no telemetry)
    all_dmas = dim_dma.select("dma_code")
    
    # Assemble the status table
    dma_status = (
        all_dmas
        .join(latest_rag, on="dma_code", how="left")
        .join(sensor_counts, on="dma_code", how="left")
        .join(property_counts, on="dma_code", how="left")
        .join(active_incidents, on="dma_code", how="left")
        .withColumn("rag_status", F.coalesce(F.col("rag_status"), F.lit("GREEN")))
        .withColumn("avg_pressure", F.coalesce(F.col("avg_pressure"), F.lit(0.0)))
        .withColumn("min_pressure", F.coalesce(F.col("min_pressure"), F.lit(0.0)))
        .withColumn("sensor_count", F.coalesce(F.col("sensor_count"), F.lit(0)).cast(IntegerType()))
        .withColumn("property_count", F.coalesce(F.col("property_count"), F.lit(0)).cast(IntegerType()))
        .withColumn("sensitive_premises_count",
                     F.coalesce(F.col("sensitive_premises_count"), F.lit(0)).cast(IntegerType()))
        .withColumn("has_active_incident",
                     F.coalesce(F.col("has_active_incident"), F.lit(False)))
        .withColumn("last_updated", F.lit(DEMO_TIMESTAMP).cast(TimestampType()))
        .select(
            "dma_code",
            "rag_status",
            "avg_pressure",
            "min_pressure",
            "sensor_count",
            "property_count",
            "sensitive_premises_count",
            "has_active_incident",
            "last_updated"
        )
    )
    
    return dma_status

# COMMAND ----------

dma_status_df = compute_dma_status()

(
    dma_status_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dma_status")
)

print(f"Wrote {spark.read.table(f'{CATALOG}.gold.dma_status').count()} DMA status rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate DMA Status Demo Assertions

# COMMAND ----------

# Should show: 1 RED, 2 AMBER, 497 GREEN
rag_distribution = spark.sql(f"""
    SELECT rag_status, COUNT(*) AS dma_count
    FROM {CATALOG}.gold.dma_status
    GROUP BY rag_status
    ORDER BY dma_count DESC
""")
display(rag_distribution)

# COMMAND ----------

# DEMO_DMA_01 detail — should be RED with has_active_incident = true, sensitive_premises >= 6
demo_dma_01_status = spark.sql(f"""
    SELECT *
    FROM {CATALOG}.gold.dma_status
    WHERE dma_code = 'DEMO_DMA_01'
""")
display(demo_dma_01_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DMA Summary (`gold.dma_summary`)
# MAGIC
# MAGIC Pre-materialised join of `dma_status` + reservoir levels + incident links + counts.
# MAGIC Powers the DMA detail panel for sub-second rendering.
# MAGIC
# MAGIC **DEMO_DMA_01 assertions:**
# MAGIC - `reservoir_level_pct` = 43
# MAGIC - `reservoir_hours_remaining` ~ 3.1
# MAGIC - `active_incident_id` = 'INC-2026-0407-001'

# COMMAND ----------

def compute_dma_summary():
    """
    Build the pre-materialised DMA summary by joining status, reservoir,
    incident, and complaint data.
    """
    dma_status = spark.read.table(f"{CATALOG}.gold.dma_status")
    dim_dma = spark.read.table(f"{CATALOG}.silver.dim_dma")
    dim_reservoirs = spark.read.table(f"{CATALOG}.silver.dim_reservoirs")
    dim_reservoir_feed = spark.read.table(f"{CATALOG}.silver.dim_reservoir_dma_feed")
    dim_incidents = spark.read.table(f"{CATALOG}.gold.dim_incidents")
    fact_telemetry = spark.read.table(f"{CATALOG}.silver.fact_telemetry")
    dim_sensor = spark.read.table(f"{CATALOG}.silver.dim_sensor")
    customer_complaints = spark.read.table(f"{CATALOG}.silver.customer_complaints")
    
    # Reservoir data per DMA (primary feed preferred)
    reservoir_feed = (
        dim_reservoir_feed
        .join(dim_reservoirs, on="reservoir_id", how="inner")
        # Prefer primary feed; if multiple, take the one with highest level
        .withColumn(
            "feed_priority",
            F.when(F.col("feed_type") == "primary", 1).otherwise(2)
        )
    )
    
    reservoir_window = Window.partitionBy("dma_code").orderBy("feed_priority", F.desc("current_level_pct"))
    
    reservoir_per_dma = (
        reservoir_feed
        .withColumn("rn", F.row_number().over(reservoir_window))
        .filter(F.col("rn") == 1)
        .withColumn(
            "reservoir_hours_remaining",
            F.round(
                (F.col("current_level_pct") / 100.0 * F.col("capacity_ml"))
                / F.col("hourly_demand_rate_ml"),
                1
            )
        )
        .select(
            "dma_code",
            F.col("reservoir_id").alias("feeding_reservoir_id"),
            F.col("current_level_pct").alias("reservoir_level_pct"),
            "reservoir_hours_remaining"
        )
    )
    
    # Active incident per DMA
    active_incidents = (
        dim_incidents
        .filter(F.col("status") == "active")
        .select(
            "dma_code",
            F.col("incident_id").alias("active_incident_id")
        )
    )
    
    # Average flow per DMA (latest telemetry window)
    latest_flow_window = Window.partitionBy("dma_code").orderBy(F.desc("timestamp"))
    
    flow_sensors = dim_sensor.filter(F.col("sensor_type") == "flow").select("sensor_id", "dma_code")
    
    avg_flow_per_dma = (
        fact_telemetry
        .filter(F.col("sensor_type") == "flow")
        .join(flow_sensors, on="sensor_id", how="inner")
        .groupBy("dma_code")
        .agg(F.round(F.avg("flow_rate"), 2).alias("avg_flow"))
    )
    
    # Complaints in last 24 hours from demo timestamp
    complaints_24h = (
        customer_complaints
        .filter(
            F.col("complaint_timestamp") >= F.lit(DEMO_TIMESTAMP).cast(TimestampType()) - F.expr("INTERVAL 24 HOURS")
        )
        .groupBy("dma_code")
        .agg(F.count("complaint_id").alias("active_complaints_count"))
    )
    
    # Assemble the summary
    dma_summary = (
        dma_status
        .join(dim_dma.select("dma_code", "dma_name"), on="dma_code", how="left")
        .join(reservoir_per_dma, on="dma_code", how="left")
        .join(active_incidents, on="dma_code", how="left")
        .join(avg_flow_per_dma, on="dma_code", how="left")
        .join(complaints_24h, on="dma_code", how="left")
        .withColumn("avg_flow", F.coalesce(F.col("avg_flow"), F.lit(0.0)))
        .withColumn("active_complaints_count",
                     F.coalesce(F.col("active_complaints_count"), F.lit(0)).cast(IntegerType()))
        .withColumn("last_updated", F.lit(DEMO_TIMESTAMP).cast(TimestampType()))
        .select(
            "dma_code",
            "dma_name",
            "rag_status",
            "avg_pressure",
            "avg_flow",
            "property_count",
            "sensor_count",
            "sensitive_premises_count",
            "feeding_reservoir_id",
            "reservoir_level_pct",
            "reservoir_hours_remaining",
            "active_incident_id",
            "active_complaints_count",
            "last_updated"
        )
    )
    
    return dma_summary

# COMMAND ----------

dma_summary_df = compute_dma_summary()

(
    dma_summary_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dma_summary")
)

print(f"Wrote {spark.read.table(f'{CATALOG}.gold.dma_summary').count()} DMA summary rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate DMA Summary Demo Assertions

# COMMAND ----------

# DEMO_DMA_01: reservoir_level_pct=43, hours_remaining~3.1, active_incident_id='INC-2026-0407-001'
demo_dma_01_summary = spark.sql(f"""
    SELECT
        dma_code, dma_name, rag_status,
        avg_pressure, avg_flow,
        property_count, sensor_count, sensitive_premises_count,
        feeding_reservoir_id, reservoir_level_pct, reservoir_hours_remaining,
        active_incident_id, active_complaints_count
    FROM {CATALOG}.gold.dma_summary
    WHERE dma_code = 'DEMO_DMA_01'
""")
display(demo_dma_01_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Complete
# MAGIC
# MAGIC All four Gold tables have been written:
# MAGIC - `gold.anomaly_scores` — per-sensor anomaly sigma scores
# MAGIC - `gold.dma_rag_history` — historical RAG timeline per DMA
# MAGIC - `gold.dma_status` — current snapshot (1 RED, 2 AMBER, 497 GREEN)
# MAGIC - `gold.dma_summary` — pre-joined summary for dashboard rendering
# MAGIC
# MAGIC Re-run this notebook at any time to refresh; all writes are idempotent (overwrite mode).
