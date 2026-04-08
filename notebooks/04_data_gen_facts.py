# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Fact Data Generation
# MAGIC
# MAGIC Generates telemetry readings (pressure & flow) and customer complaints.
# MAGIC Depends on geography (notebook 02) and dimensions (notebook 03).
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Layer | Table | Approx Rows |
# MAGIC |-------|-------|-------------|
# MAGIC | Target | Path | Approx Rows |
# MAGIC |--------|------|-------------|
# MAGIC | Volume | `landing_zone/raw_telemetry/` | ~600K |
# MAGIC | Volume | `landing_zone/raw_complaints/` | 30+ |
# MAGIC
# MAGIC Bronze/Silver/Gold tables are created by the SDP pipeline (`06_sdp_pipeline`).

# COMMAND ----------

# DBTITLE 1,Configuration & Load Dependencies
import math
import random
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F

CATALOG = "water_digital_twin"
VOLUME = f"/Volumes/{CATALOG}/bronze/landing_zone"
SEED = 42

# Time window: 7 days of 15-min intervals
# 2026-03-31 00:00:00 to 2026-04-07 06:00:00
START_TIME = datetime(2026, 3, 31, 0, 0, 0)
END_TIME = datetime(2026, 4, 7, 6, 0, 0)
INTERVAL_MINUTES = 15

# Incident timestamp
INCIDENT_TIME = datetime(2026, 4, 7, 2, 3, 0)

# Thresholds
PRESSURE_RED = 15.0
PRESSURE_AMBER = 25.0

# Load sensor data from Volume (written by NB03)
df_sensors = spark.read.json(f"{VOLUME}/raw_sensors")
sensor_rows = df_sensors.collect()
sensor_lookup = {row["sensor_id"]: row for row in sensor_rows}

# Load properties for complaints from Volume (written by NB03)
df_props = spark.read.json(f"{VOLUME}/raw_customer_contacts")

print(f"Loaded {len(sensor_rows)} sensors")
print(f"Pressure sensors: {sum(1 for s in sensor_rows if s['sensor_type'] == 'pressure')}")
print(f"Flow sensors: {sum(1 for s in sensor_rows if s['sensor_type'] == 'flow')}")

# COMMAND ----------

# DBTITLE 1,Build Timestamp Series

timestamps = []
t = START_TIME
while t <= END_TIME:
    timestamps.append(t)
    t += timedelta(minutes=INTERVAL_MINUTES)

print(f"Timestamps: {len(timestamps)} intervals from {timestamps[0]} to {timestamps[-1]}")

# COMMAND ----------

# DBTITLE 1,Diurnal Pattern Helpers

def diurnal_pressure(hour_frac):
    """
    Normal diurnal pressure pattern (metres head).
    Night: ~55m +/- 3m (low demand, high pressure)
    Morning peak (06:00-09:00): ~40m +/- 3m (high demand, low pressure)
    Afternoon: ~48m +/- 3m (moderate)
    Evening peak (17:00-20:00): ~42m +/- 3m
    """
    if 0 <= hour_frac < 5:
        base = 55.0
    elif 5 <= hour_frac < 6:
        base = 55.0 - (hour_frac - 5) * 15.0  # ramp down
    elif 6 <= hour_frac < 9:
        base = 40.0
    elif 9 <= hour_frac < 12:
        base = 40.0 + (hour_frac - 9) * 2.67  # gradual recovery
    elif 12 <= hour_frac < 14:
        base = 48.0
    elif 14 <= hour_frac < 17:
        base = 48.0 - (hour_frac - 14) * 2.0  # ramp down to evening
    elif 17 <= hour_frac < 20:
        base = 42.0
    elif 20 <= hour_frac < 22:
        base = 42.0 + (hour_frac - 20) * 6.5  # recovery
    else:
        base = 55.0
    return base


def diurnal_flow(hour_frac):
    """
    Normal diurnal flow pattern (litres/sec).
    Night: ~25 l/s (low demand)
    Morning peak: ~60 l/s
    Afternoon: ~45 l/s
    Evening peak: ~55 l/s
    """
    if 0 <= hour_frac < 5:
        base = 25.0
    elif 5 <= hour_frac < 6:
        base = 25.0 + (hour_frac - 5) * 35.0
    elif 6 <= hour_frac < 9:
        base = 60.0
    elif 9 <= hour_frac < 12:
        base = 60.0 - (hour_frac - 9) * 5.0
    elif 12 <= hour_frac < 14:
        base = 45.0
    elif 14 <= hour_frac < 17:
        base = 45.0 + (hour_frac - 14) * 3.33
    elif 17 <= hour_frac < 20:
        base = 55.0
    elif 20 <= hour_frac < 22:
        base = 55.0 - (hour_frac - 20) * 15.0
    else:
        base = 25.0
    return base

# COMMAND ----------

# DBTITLE 1,Generate Telemetry Data

random.seed(SEED + 5000)

# Categorise sensors by behaviour
demo_dma_01_pressure_ids = [s["sensor_id"] for s in sensor_rows if s["dma_code"] == "DEMO_DMA_01" and s["sensor_type"] == "pressure"]
demo_dma_01_flow_ids = [s["sensor_id"] for s in sensor_rows if s["dma_code"] == "DEMO_DMA_01" and s["sensor_type"] == "flow"]
demo_dma_02_sensor_ids = [s["sensor_id"] for s in sensor_rows if s["dma_code"] == "DEMO_DMA_02"]
demo_dma_03_sensor_ids = [s["sensor_id"] for s in sensor_rows if s["dma_code"] == "DEMO_DMA_03"]

print(f"DEMO_DMA_01 pressure sensors: {len(demo_dma_01_pressure_ids)}")
print(f"DEMO_DMA_01 flow sensors: {len(demo_dma_01_flow_ids)}")
print(f"DEMO_DMA_02 sensors: {len(demo_dma_02_sensor_ids)}")
print(f"DEMO_DMA_03 sensors: {len(demo_dma_03_sensor_ids)}")

# We build telemetry in batches to manage memory
telemetry_batch = []
BATCH_SIZE = 100000
batch_num = 0
total_rows = 0

# Pre-compute per-sensor random offsets for consistency
sensor_offsets = {}
for s in sensor_rows:
    sensor_offsets[s["sensor_id"]] = {
        "pressure_bias": random.uniform(-2, 2),
        "flow_bias": random.uniform(-3, 3),
    }

def get_reading(sensor, ts):
    """Generate a single telemetry reading for a sensor at a timestamp."""
    sid = sensor["sensor_id"]
    stype = sensor["sensor_type"]
    dma = sensor["dma_code"]
    hour_frac = ts.hour + ts.minute / 60.0
    is_after_incident = ts >= INCIDENT_TIME
    bias = sensor_offsets[sid]

    quality_flag = "good"
    anomaly_flag = False

    if stype == "pressure":
        base = diurnal_pressure(hour_frac)

        if dma == "DEMO_DMA_01" and is_after_incident:
            # Sharp drop: pressure collapses to ~5-10m for ALL pressure sensors
            # DEMO_SENSOR_01 drops to ~5-8m (low elevation sensor)
            if sid == "DEMO_SENSOR_01":
                value = random.uniform(5.0, 8.0)
            else:
                # Other DEMO_DMA_01 pressure sensors drop to ~5-12m
                value = random.uniform(5.0, 12.0)
            anomaly_flag = True
            quality_flag = "suspect"
        elif dma == "DEMO_DMA_02" and is_after_incident:
            # Mild dip to 25-35m (AMBER, not RED)
            dip = random.uniform(25.0, 35.0)
            value = dip + random.uniform(-2, 2)
            anomaly_flag = True
        elif dma == "DEMO_DMA_03" and is_after_incident:
            # Mild dip to 25-35m (AMBER, not RED)
            dip = random.uniform(25.0, 35.0)
            value = dip + random.uniform(-2, 2)
            anomaly_flag = True
        else:
            # Normal operation
            value = base + bias["pressure_bias"] + random.gauss(0, 3.0)

        value = round(max(0.0, value), 2)
        unit = "m"

    else:  # flow
        base = diurnal_flow(hour_frac)

        if dma == "DEMO_DMA_01" and is_after_incident:
            # Flow drops to ~12 l/s
            value = random.uniform(10.0, 14.0)
            anomaly_flag = True
            quality_flag = "suspect"
        elif dma in ("DEMO_DMA_02", "DEMO_DMA_03") and is_after_incident:
            # Mild flow reduction
            value = base * random.uniform(0.6, 0.8) + random.gauss(0, 2)
            anomaly_flag = True
        else:
            value = base + bias["flow_bias"] + random.gauss(0, 5.0)

        value = round(max(0.0, value), 2)
        unit = "l/s"

    return {
        "sensor_id": sid,
        "sensor_type": stype,
        "dma_code": dma,
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "value": value,
        "unit": unit,
        "quality_flag": quality_flag,
        "anomaly_flag": anomaly_flag,
    }

# COMMAND ----------

# DBTITLE 1,Build & Write Telemetry in Batches

# Generate telemetry for all sensors
# To manage memory, process in sensor batches and append
first_batch = True
sensor_batch_size = 500  # process 500 sensors at a time

for s_start in range(0, len(sensor_rows), sensor_batch_size):
    s_batch = sensor_rows[s_start:s_start + sensor_batch_size]
    batch_data = []

    for sensor in s_batch:
        for ts in timestamps:
            batch_data.append(get_reading(sensor, ts))

    df_batch = spark.createDataFrame(pd.DataFrame(batch_data))

    write_mode = "overwrite" if first_batch else "append"
    df_batch.write.format("json").mode(write_mode).save(f"{VOLUME}/raw_telemetry")
    first_batch = False

    total_rows += len(batch_data)
    print(f"  Wrote batch {s_start // sensor_batch_size + 1}: {len(batch_data)} rows (cumulative: {total_rows})")

print(f"\nTotal telemetry rows written: {total_rows}")

# COMMAND ----------

# DBTITLE 1,Validate Telemetry — DEMO_SENSOR_01

df_tel = spark.read.json(f"{VOLUME}/raw_telemetry")
display(df_tel.filter("sensor_id = 'DEMO_SENSOR_01' AND timestamp >= '2026-04-07 01:00:00'").select("sensor_id", "timestamp", "value", "unit", "anomaly_flag").orderBy("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complaints (30+ for DEMO_DMA_01)

# COMMAND ----------

# DBTITLE 1,Generate Complaint Records

random.seed(SEED + 6000)

# Get high-elevation DEMO_DMA_01 properties (customer_height > 35m)
demo_props = df_props.filter(
    (df_props.dma_code == "DEMO_DMA_01") & (df_props.customer_height_m > 35.0)
).collect()

print(f"High-elevation DEMO_DMA_01 properties available: {len(demo_props)}")

# Generate 35 complaints between 03:00-05:30 (1h lag after pressure drop at 02:03)
NUM_COMPLAINTS = 35
complaint_records = []

# Shuffle properties and pick the first NUM_COMPLAINTS
random.shuffle(demo_props)
selected_props = demo_props[:NUM_COMPLAINTS]

COMPLAINT_TYPES = ["no_water", "low_pressure"]
COMPLAINT_DESCRIPTIONS = {
    "no_water": [
        "No water coming from taps since early morning",
        "Complete loss of water supply",
        "Taps running dry, no water at all",
        "Woke up to no water in the house",
        "No water supply since approximately 3am",
    ],
    "low_pressure": [
        "Very low water pressure, barely a trickle",
        "Water pressure extremely low, cannot shower",
        "Pressure has dropped significantly overnight",
        "Low pressure affecting all taps in property",
        "Water dribbling from taps, unusable pressure",
    ],
}

CONTACT_CHANNELS = ["phone", "web", "app", "email"]

for i, prop in enumerate(selected_props):
    # Complaint time: random between 03:00 and 05:30 on 2026-04-07
    complaint_hour = random.uniform(3.0, 5.5)
    complaint_minutes = int((complaint_hour % 1) * 60)
    complaint_time = datetime(2026, 4, 7, int(complaint_hour), complaint_minutes, random.randint(0, 59))

    ctype = random.choice(COMPLAINT_TYPES)
    description = random.choice(COMPLAINT_DESCRIPTIONS[ctype])

    complaint_records.append({
        "complaint_id": f"CMP-2026-0407-{i+1:03d}",
        "property_id": prop["property_id"],
        "dma_code": "DEMO_DMA_01",
        "complaint_type": ctype,
        "description": description,
        "complaint_timestamp": complaint_time.strftime("%Y-%m-%d %H:%M:%S"),
        "contact_channel": random.choice(CONTACT_CHANNELS),
        "customer_height_m": float(prop["customer_height_m"]),
        "property_type": prop["property_type"],
        "status": "open",
        "resolution_timestamp": None,
        "resolution_notes": None,
    })

print(f"Total complaints: {len(complaint_records)}")
print(f"  no_water: {sum(1 for c in complaint_records if c['complaint_type'] == 'no_water')}")
print(f"  low_pressure: {sum(1 for c in complaint_records if c['complaint_type'] == 'low_pressure')}")
print(f"  Min time: {min(c['complaint_timestamp'] for c in complaint_records)}")
print(f"  Max time: {max(c['complaint_timestamp'] for c in complaint_records)}")

# COMMAND ----------

# DBTITLE 1,Write bronze.raw_complaints

path = f"{VOLUME}/raw_complaints"
spark.createDataFrame(pd.DataFrame(complaint_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(complaint_records)} complaint records to {path}")

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("=== Fact Data Generation Complete ===")

telemetry_count = spark.read.json(f"{VOLUME}/raw_telemetry").count()
complaint_count = spark.read.json(f"{VOLUME}/raw_complaints").count()
print(f"  Total telemetry rows: {telemetry_count}")
print(f"  Total complaints: {complaint_count}")

# Verify pressure pattern for DEMO_SENSOR_01
df_tel = spark.read.json(f"{VOLUME}/raw_telemetry")
display(
    df_tel.filter("sensor_id = 'DEMO_SENSOR_01'")
    .withColumn("period", F.when(F.col("timestamp") < "2026-04-07 02:03:00", "before_incident").otherwise("after_incident"))
    .groupBy("period").agg(F.avg("value").alias("avg_pressure"), F.min("value").alias("min_pressure"), F.count("*").alias("readings"))
)
