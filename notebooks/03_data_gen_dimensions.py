# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Dimension Data Generation
# MAGIC
# MAGIC Depends on geography data from notebook `02_data_gen_geography`.
# MAGIC Generates sensors, properties, assets, asset-DMA feed mappings, and reservoirs.
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Target | Path | Row Count |
# MAGIC |--------|------|-----------|
# MAGIC | Volume | `landing_zone/raw_sensors/` | 10,000 |
# MAGIC | Volume | `landing_zone/raw_customer_contacts/` | 50,000 |
# MAGIC | Volume | `landing_zone/raw_assets/` | 30+ |
# MAGIC | Volume | `landing_zone/raw_asset_dma_feed/` | mappings |
# MAGIC | Volume | `landing_zone/raw_reservoirs/` | 20 |
# MAGIC | Volume | `landing_zone/raw_reservoir_dma_feed/` | mappings |
# MAGIC
# MAGIC Bronze/Silver/Gold tables are created by the SDP pipeline (`06_sdp_pipeline`).

# COMMAND ----------

# DBTITLE 1,Configuration & Read Geography
import math
import random
import json
from datetime import datetime
import pandas as pd

CATALOG = "water_digital_twin"
VOLUME = f"/Volumes/{CATALOG}/bronze/landing_zone"
SEED = 42

# Read DMA data from Volume (written by NB02)
df_dma = spark.read.json(f"{VOLUME}/raw_dma_boundaries")
dma_rows = df_dma.collect()
dma_lookup = {row["dma_code"]: row for row in dma_rows}

print(f"Loaded {len(dma_rows)} DMAs")
demo_dma_01 = dma_lookup["DEMO_DMA_01"]
print(f"DEMO_DMA_01: {demo_dma_01['dma_name']} at ({demo_dma_01['centroid_latitude']}, {demo_dma_01['centroid_longitude']})")

# Constants
KM_PER_DEG_LAT = 111.32
KM_PER_DEG_LON = 111.32 * math.cos(math.radians(51.5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sensors (10,000)

# COMMAND ----------

# DBTITLE 1,Generate Sensor Records

random.seed(SEED + 1000)

# Distribute 10,000 sensors: 8,000 pressure + 2,000 flow across 500 DMAs
# DEMO_DMA_01 gets 20 sensors (18 pressure + 2 flow)
TOTAL_PRESSURE = 8000
TOTAL_FLOW = 2000
DEMO_DMA_01_PRESSURE = 18
DEMO_DMA_01_FLOW = 2

sensor_records = []
sensor_counter = {"pressure": 0, "flow": 0}

# --- DEMO_DMA_01 sensors first ---
demo_dma = dma_lookup["DEMO_DMA_01"]
demo_lat = demo_dma["centroid_latitude"]
demo_lon = demo_dma["centroid_longitude"]
demo_elev = demo_dma["avg_elevation"]

# DEMO_SENSOR_01 — the key pressure sensor at low elevation (15 m)
sensor_records.append({
    "sensor_id": "DEMO_SENSOR_01",
    "sensor_type": "pressure",
    "dma_code": "DEMO_DMA_01",
    "latitude": round(demo_lat + random.uniform(-0.005, 0.005), 6),
    "longitude": round(demo_lon + random.uniform(-0.005, 0.005), 6),
    "elevation_m": 15.0,
    "install_date": "2022-06-15",
    "manufacturer": "Siemens",
    "model": "SITRANS P320",
    "status": "active",
    "last_calibration": "2025-11-01",
})
sensor_counter["pressure"] += 1

# Remaining 17 pressure sensors in DEMO_DMA_01
for i in range(2, DEMO_DMA_01_PRESSURE + 1):
    elev = round(demo_elev + random.uniform(-15, 15), 1)
    sensor_records.append({
        "sensor_id": f"DEMO_PRESSURE_{i:02d}",
        "sensor_type": "pressure",
        "dma_code": "DEMO_DMA_01",
        "latitude": round(demo_lat + random.uniform(-0.008, 0.008), 6),
        "longitude": round(demo_lon + random.uniform(-0.008, 0.008), 6),
        "elevation_m": max(5.0, elev),
        "install_date": f"202{random.randint(1,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "manufacturer": random.choice(["Siemens", "ABB", "Endress+Hauser"]),
        "model": random.choice(["SITRANS P320", "266GST", "Cerabar PMC71"]),
        "status": "active",
        "last_calibration": f"2025-{random.randint(6,12):02d}-01",
    })
    sensor_counter["pressure"] += 1

# 2 flow sensors in DEMO_DMA_01
for i, sid in enumerate(["DEMO_FLOW_01", "DEMO_FLOW_02"]):
    sensor_records.append({
        "sensor_id": sid,
        "sensor_type": "flow",
        "dma_code": "DEMO_DMA_01",
        "latitude": round(demo_lat + random.uniform(-0.008, 0.008), 6),
        "longitude": round(demo_lon + random.uniform(-0.008, 0.008), 6),
        "elevation_m": round(demo_elev + random.uniform(-10, 10), 1),
        "install_date": f"2023-0{i+3}-15",
        "manufacturer": "ABB",
        "model": "AquaMaster4",
        "status": "active",
        "last_calibration": "2025-09-01",
    })
    sensor_counter["flow"] += 1

# --- Distribute remaining sensors across all other DMAs ---
remaining_pressure = TOTAL_PRESSURE - DEMO_DMA_01_PRESSURE
remaining_flow = TOTAL_FLOW - DEMO_DMA_01_FLOW
other_dma_codes = [code for code in dma_lookup if code != "DEMO_DMA_01"]

# Pressure sensors
pressure_per_dma = remaining_pressure // len(other_dma_codes)
extra_pressure = remaining_pressure % len(other_dma_codes)

for i, dma_code in enumerate(other_dma_codes):
    dma = dma_lookup[dma_code]
    count = pressure_per_dma + (1 if i < extra_pressure else 0)
    for j in range(count):
        sensor_counter["pressure"] += 1
        sensor_records.append({
            "sensor_id": f"PS_{sensor_counter['pressure']:06d}",
            "sensor_type": "pressure",
            "dma_code": dma_code,
            "latitude": round(dma["centroid_latitude"] + random.uniform(-0.008, 0.008), 6),
            "longitude": round(dma["centroid_longitude"] + random.uniform(-0.008, 0.008), 6),
            "elevation_m": round(max(2.0, dma["avg_elevation"] + random.uniform(-15, 15)), 1),
            "install_date": f"20{random.randint(18,24)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "manufacturer": random.choice(["Siemens", "ABB", "Endress+Hauser", "Honeywell"]),
            "model": random.choice(["SITRANS P320", "266GST", "Cerabar PMC71", "SmartLine ST800"]),
            "status": random.choices(["active", "maintenance"], weights=[0.95, 0.05])[0],
            "last_calibration": f"2025-{random.randint(1,12):02d}-01",
        })

# Flow sensors
flow_per_dma = remaining_flow // len(other_dma_codes)
extra_flow = remaining_flow % len(other_dma_codes)

for i, dma_code in enumerate(other_dma_codes):
    dma = dma_lookup[dma_code]
    count = flow_per_dma + (1 if i < extra_flow else 0)
    for j in range(count):
        sensor_counter["flow"] += 1
        sensor_records.append({
            "sensor_id": f"FS_{sensor_counter['flow']:06d}",
            "sensor_type": "flow",
            "dma_code": dma_code,
            "latitude": round(dma["centroid_latitude"] + random.uniform(-0.008, 0.008), 6),
            "longitude": round(dma["centroid_longitude"] + random.uniform(-0.008, 0.008), 6),
            "elevation_m": round(max(2.0, dma["avg_elevation"] + random.uniform(-10, 10)), 1),
            "install_date": f"20{random.randint(18,24)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            "manufacturer": random.choice(["ABB", "Siemens", "Itron"]),
            "model": random.choice(["AquaMaster4", "SITRANS FM MAG 8000", "Flostar M"]),
            "status": random.choices(["active", "maintenance"], weights=[0.95, 0.05])[0],
            "last_calibration": f"2025-{random.randint(1,12):02d}-01",
        })

print(f"Total sensors: {len(sensor_records)}")
print(f"  Pressure: {sum(1 for s in sensor_records if s['sensor_type'] == 'pressure')}")
print(f"  Flow: {sum(1 for s in sensor_records if s['sensor_type'] == 'flow')}")
print(f"  DEMO_DMA_01: {sum(1 for s in sensor_records if s['dma_code'] == 'DEMO_DMA_01')}")

# COMMAND ----------

# DBTITLE 1,Write raw_sensors to Volume

path = f"{VOLUME}/raw_sensors"
spark.createDataFrame(pd.DataFrame(sensor_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(sensor_records)} sensor records to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Properties (50,000)

# COMMAND ----------

# DBTITLE 1,Generate Property Records

random.seed(SEED + 2000)

PROPERTY_TYPES_NORMAL = {
    "domestic": 0.90,
    "commercial": 0.06,
    "school": 0.015,
    "hospital": 0.005,
    "dialysis_home": 0.005,
    "care_home": 0.015,
}

TOTAL_PROPERTIES = 50000
DEMO_DMA_01_PROPERTIES = 810  # 800+ as specified

property_records = []
property_counter = 0

# --- DEMO_DMA_01 properties ---
demo_dma = dma_lookup["DEMO_DMA_01"]
d_lat = demo_dma["centroid_latitude"]
d_lon = demo_dma["centroid_longitude"]
d_elev = demo_dma["avg_elevation"]

# Ensure required counts: 750 domestic, 2+ schools, 1+ hospital, 3+ dialysis_home, 15+ commercial
demo_type_counts = {
    "domestic": 750,
    "commercial": 18,
    "school": 5,
    "hospital": 2,
    "dialysis_home": 4,
    "care_home": 31,  # fill to reach 810
}

STREET_NAMES = [
    "Church Road", "High Street", "Station Road", "Park Lane", "Victoria Road",
    "Manor Road", "Queens Road", "Kings Avenue", "London Road", "Hill Road",
    "Westow Street", "Anerley Road", "Crystal Palace Parade", "Sydenham Road",
    "Auckland Road", "Belvedere Road", "Cintra Park", "Dulwich Wood Avenue",
    "Fox Hill", "Gipsy Hill", "Harold Road", "Jasper Road", "Kirkdale",
    "Lancaster Road", "Maple Road", "Norwood Road", "Oakfield Road",
    "Palace Road", "Rockmount Road", "Selhurst Road", "Thicket Road",
    "Upper Norwood", "Virgo Fidelis", "Woodland Road",
]

for ptype, count in demo_type_counts.items():
    for i in range(count):
        property_counter += 1
        plat = round(d_lat + random.uniform(-0.008, 0.008), 6)
        plon = round(d_lon + random.uniform(-0.008, 0.008), 6)

        # 50%+ of DEMO_DMA_01 properties should have customer_height > 35m
        # Schools and hospitals at 40-80m elevation
        if ptype in ("school", "hospital"):
            customer_height = round(random.uniform(40, 80), 1)
        elif ptype == "dialysis_home":
            customer_height = round(random.uniform(35, 70), 1)
        elif random.random() < 0.55:  # 55% chance of high elevation
            customer_height = round(random.uniform(36, 110), 1)
        else:
            customer_height = round(random.uniform(5, 34), 1)

        street = random.choice(STREET_NAMES)
        house_num = random.randint(1, 200)

        is_sensitive = ptype in ("school", "hospital", "dialysis_home", "care_home")

        property_records.append({
            "property_id": f"PROP_{property_counter:06d}",
            "dma_code": "DEMO_DMA_01",
            "property_type": ptype,
            "address": f"{house_num} {street}",
            "postcode": f"SE{random.randint(19,27)} {random.randint(1,9)}{random.choice('ABCDEFGHJKLMNPRSTUVWXY')}{random.choice('ABCDEFGHJKLMNPRSTUVWXY')}",
            "latitude": plat,
            "longitude": plon,
            "customer_height_m": customer_height,
            "elevation_m": round(max(5.0, d_elev + random.uniform(-15, 15)), 1),
            "occupants": random.randint(1, 6) if ptype == "domestic" else random.randint(10, 500),
            "is_sensitive_premise": is_sensitive,
            "contact_phone": f"07{random.randint(100,999)}{random.randint(100000,999999)}",
            "contact_email": f"resident{property_counter}@example.com",
            "created_at": datetime(2024, 6, 1).isoformat(),
        })

# --- Distribute remaining properties across other DMAs ---
remaining = TOTAL_PROPERTIES - DEMO_DMA_01_PROPERTIES
other_dma_codes = [code for code in dma_lookup if code != "DEMO_DMA_01"]
props_per_dma = remaining // len(other_dma_codes)
extra_props = remaining % len(other_dma_codes)

for i, dma_code in enumerate(other_dma_codes):
    dma = dma_lookup[dma_code]
    count = props_per_dma + (1 if i < extra_props else 0)
    for j in range(count):
        property_counter += 1
        # Assign type based on normal distribution
        r = random.random()
        cum = 0
        ptype = "domestic"
        for pt, prob in PROPERTY_TYPES_NORMAL.items():
            cum += prob
            if r <= cum:
                ptype = pt
                break

        plat = round(dma["centroid_latitude"] + random.uniform(-0.008, 0.008), 6)
        plon = round(dma["centroid_longitude"] + random.uniform(-0.008, 0.008), 6)
        customer_height = round(random.uniform(3, max(10, dma["avg_elevation"])), 1)
        is_sensitive = ptype in ("school", "hospital", "dialysis_home", "care_home")

        street = random.choice(STREET_NAMES)
        house_num = random.randint(1, 200)

        property_records.append({
            "property_id": f"PROP_{property_counter:06d}",
            "dma_code": dma_code,
            "property_type": ptype,
            "address": f"{house_num} {street}",
            "postcode": f"{'SE' if dma['centroid_latitude'] < 51.5 else 'N'}{random.randint(1,28)} {random.randint(1,9)}{random.choice('ABCDEFGHJKLMNPRSTUVWXY')}{random.choice('ABCDEFGHJKLMNPRSTUVWXY')}",
            "latitude": plat,
            "longitude": plon,
            "customer_height_m": customer_height,
            "elevation_m": round(max(2.0, dma["avg_elevation"] + random.uniform(-15, 15)), 1),
            "occupants": random.randint(1, 6) if ptype == "domestic" else random.randint(10, 500),
            "is_sensitive_premise": is_sensitive,
            "contact_phone": f"07{random.randint(100,999)}{random.randint(100000,999999)}",
            "contact_email": f"resident{property_counter}@example.com",
            "created_at": datetime(2024, 6, 1).isoformat(),
        })

print(f"Total properties: {len(property_records)}")
demo_props = [p for p in property_records if p["dma_code"] == "DEMO_DMA_01"]
print(f"  DEMO_DMA_01 properties: {len(demo_props)}")
print(f"    Domestic: {sum(1 for p in demo_props if p['property_type'] == 'domestic')}")
print(f"    Schools: {sum(1 for p in demo_props if p['property_type'] == 'school')}")
print(f"    Hospitals: {sum(1 for p in demo_props if p['property_type'] == 'hospital')}")
print(f"    Dialysis homes: {sum(1 for p in demo_props if p['property_type'] == 'dialysis_home')}")
print(f"    Commercial: {sum(1 for p in demo_props if p['property_type'] == 'commercial')}")
print(f"    Height > 35m: {sum(1 for p in demo_props if p['customer_height_m'] > 35)}/{len(demo_props)} ({100*sum(1 for p in demo_props if p['customer_height_m'] > 35)/len(demo_props):.0f}%)")

# COMMAND ----------

# DBTITLE 1,Write raw_customer_contacts to Volume

path = f"{VOLUME}/raw_customer_contacts"
spark.createDataFrame(pd.DataFrame(property_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(property_records)} property records to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assets (30+)

# COMMAND ----------

# DBTITLE 1,Generate Asset Records

random.seed(SEED + 3000)

asset_records = []

# --- DEMO_PUMP_01: tripped pump station ---
asset_records.append({
    "asset_id": "DEMO_PUMP_01",
    "asset_type": "pump_station",
    "asset_name": "Crystal Palace Booster Station",
    "dma_code": "DEMO_DMA_01",
    "latitude": 51.44,
    "longitude": -0.08,
    "elevation_m": 85.0,
    "status": "tripped",
    "trip_timestamp": "2026-04-07 02:03:00",
    "install_date": "2015-03-20",
    "manufacturer": "Grundfos",
    "model": "CRE 64-2-1",
    "capacity_kw": 45.0,
    "diameter_inches": None,
    "length_km": None,
    "geometry_wkt": "POINT(-0.08 51.44)",
    "last_maintenance": "2025-12-01",
    "notes": "Primary booster for Crystal Palace high-elevation zone",
})

# --- DEMO_TM_001: trunk main ---
asset_records.append({
    "asset_id": "DEMO_TM_001",
    "asset_type": "trunk_main",
    "asset_name": "Crystal Palace Trunk Main",
    "dma_code": "DEMO_DMA_01",
    "latitude": 51.43,
    "longitude": -0.075,
    "elevation_m": 70.0,
    "status": "active",
    "trip_timestamp": None,
    "install_date": "2005-08-15",
    "manufacturer": "Saint-Gobain",
    "model": "Ductile Iron DN300",
    "capacity_kw": None,
    "diameter_inches": 12.0,
    "length_km": 3.2,
    "geometry_wkt": "LINESTRING(-0.09 51.44, -0.085 51.435, -0.075 51.43, -0.065 51.425, -0.06 51.42)",
    "last_maintenance": "2025-09-15",
    "notes": "12-inch trunk main feeding Crystal Palace area from south",
})

# --- DEMO_VALVE_01 and DEMO_VALVE_02: isolation valves ---
for i, (vlat, vlon) in enumerate([(51.435, -0.075), (51.425, -0.065)], 1):
    asset_records.append({
        "asset_id": f"DEMO_VALVE_{i:02d}",
        "asset_type": "isolation_valve",
        "asset_name": f"Crystal Palace Isolation Valve {i}",
        "dma_code": "DEMO_DMA_01",
        "latitude": vlat,
        "longitude": vlon,
        "elevation_m": round(70 + random.uniform(-5, 5), 1),
        "status": "open",
        "trip_timestamp": None,
        "install_date": f"201{random.randint(0,9)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "manufacturer": "AVK",
        "model": "Series 36/80",
        "capacity_kw": None,
        "diameter_inches": 12.0,
        "length_km": None,
        "geometry_wkt": f"POINT({vlon} {vlat})",
        "last_maintenance": f"2025-{random.randint(6,12):02d}-01",
        "notes": f"Isolation valve {i} on Crystal Palace trunk main",
    })

# --- 10 more pump stations across London ---
pump_locations = [
    ("Highgate Booster Station", "DMA_00010", 51.57, -0.15, 80),
    ("Muswell Hill Pump Station", "DMA_00020", 51.58, -0.10, 75),
    ("Wimbledon Booster Station", "DMA_00030", 51.40, -0.20, 55),
    ("Greenwich Pump Station", "DMA_00040", 51.48, 0.00, 25),
    ("Hampstead Booster Station", "DMA_00050", 51.56, -0.18, 70),
    ("Dulwich Pump Station", "DMA_00060", 51.44, -0.09, 65),
    ("Streatham Booster Station", "DMA_00070", 51.43, -0.13, 50),
    ("Brixton Pump Station", "DMA_00080", 51.46, -0.11, 35),
    ("Lewisham Pump Station", "DMA_00090", 51.46, -0.01, 30),
    ("Woolwich Pump Station", "DMA_00100", 51.49, 0.07, 20),
]

for idx, (name, dma_code, plat, plon, elev) in enumerate(pump_locations, 2):
    asset_records.append({
        "asset_id": f"PUMP_{idx:03d}",
        "asset_type": "pump_station",
        "asset_name": name,
        "dma_code": dma_code,
        "latitude": plat,
        "longitude": plon,
        "elevation_m": float(elev),
        "status": "active",
        "trip_timestamp": None,
        "install_date": f"20{random.randint(10,22)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "manufacturer": random.choice(["Grundfos", "Xylem", "Sulzer"]),
        "model": random.choice(["CRE 64-2-1", "e-SV 46", "MPC-E 4"]),
        "capacity_kw": round(random.uniform(20, 80), 1),
        "diameter_inches": None,
        "length_km": None,
        "geometry_wkt": f"POINT({plon} {plat})",
        "last_maintenance": f"2025-{random.randint(1,12):02d}-01",
        "notes": f"Booster station serving {name.split(' ')[0]} area",
    })

# --- 10 trunk mains ---
trunk_main_specs = [
    ("Highgate Trunk Main", "DMA_00010", 51.57, -0.15, 8, 2.5),
    ("Muswell Hill Trunk Main", "DMA_00020", 51.58, -0.10, 10, 1.8),
    ("Wimbledon Trunk Main", "DMA_00030", 51.40, -0.20, 14, 4.1),
    ("Greenwich Trunk Main", "DMA_00040", 51.48, 0.00, 16, 3.5),
    ("Hampstead Trunk Main", "DMA_00050", 51.56, -0.18, 10, 2.2),
    ("Dulwich Trunk Main", "DMA_00060", 51.44, -0.09, 12, 2.8),
    ("Streatham Trunk Main", "DMA_00070", 51.43, -0.13, 8, 1.9),
    ("Brixton Trunk Main", "DMA_00080", 51.46, -0.11, 10, 2.3),
    ("Lewisham Trunk Main", "DMA_00090", 51.46, -0.01, 12, 3.0),
    ("Woolwich Trunk Main", "DMA_00100", 51.49, 0.07, 14, 2.7),
]

for idx, (name, dma_code, tlat, tlon, diam, length) in enumerate(trunk_main_specs, 2):
    end_lat = tlat + random.uniform(-0.02, 0.02)
    end_lon = tlon + random.uniform(-0.02, 0.02)
    asset_records.append({
        "asset_id": f"TM_{idx:03d}",
        "asset_type": "trunk_main",
        "asset_name": name,
        "dma_code": dma_code,
        "latitude": tlat,
        "longitude": tlon,
        "elevation_m": round(random.uniform(10, 60), 1),
        "status": "active",
        "trip_timestamp": None,
        "install_date": f"20{random.randint(0,15):02d}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "manufacturer": random.choice(["Saint-Gobain", "Wavin", "Kubota"]),
        "model": f"Ductile Iron DN{diam*25}",
        "capacity_kw": None,
        "diameter_inches": float(diam),
        "length_km": length,
        "geometry_wkt": f"LINESTRING({tlon} {tlat}, {end_lon} {end_lat})",
        "last_maintenance": f"2025-{random.randint(1,12):02d}-01",
        "notes": f"{diam}-inch trunk main in {name.split(' ')[0]} area",
    })

# --- 10 PRVs (Pressure Reducing Valves) ---
prv_locations = [
    ("Highgate PRV", "DMA_00010", 51.565, -0.148),
    ("Muswell Hill PRV", "DMA_00020", 51.575, -0.098),
    ("Wimbledon PRV", "DMA_00030", 51.405, -0.195),
    ("Greenwich PRV", "DMA_00040", 51.478, 0.005),
    ("Hampstead PRV", "DMA_00050", 51.555, -0.178),
    ("Dulwich PRV", "DMA_00060", 51.438, -0.088),
    ("Streatham PRV", "DMA_00070", 51.428, -0.128),
    ("Brixton PRV", "DMA_00080", 51.458, -0.108),
    ("Lewisham PRV", "DMA_00090", 51.458, -0.008),
    ("Woolwich PRV", "DMA_00100", 51.488, 0.072),
]

for idx, (name, dma_code, plat, plon) in enumerate(prv_locations, 1):
    asset_records.append({
        "asset_id": f"PRV_{idx:03d}",
        "asset_type": "prv",
        "asset_name": name,
        "dma_code": dma_code,
        "latitude": plat,
        "longitude": plon,
        "elevation_m": round(random.uniform(15, 70), 1),
        "status": "active",
        "trip_timestamp": None,
        "install_date": f"20{random.randint(10,22)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "manufacturer": random.choice(["Bermad", "Singer", "Cla-Val"]),
        "model": random.choice(["Model 720", "106-PR", "90-01"]),
        "capacity_kw": None,
        "diameter_inches": float(random.choice([6, 8, 10, 12])),
        "length_km": None,
        "geometry_wkt": f"POINT({plon} {plat})",
        "last_maintenance": f"2025-{random.randint(1,12):02d}-01",
        "notes": f"Pressure reducing valve in {name.split(' ')[0]} area",
    })

print(f"Total assets: {len(asset_records)}")
for atype in ["pump_station", "trunk_main", "isolation_valve", "prv"]:
    print(f"  {atype}: {sum(1 for a in asset_records if a['asset_type'] == atype)}")

# COMMAND ----------

# DBTITLE 1,Write raw_assets to Volume

path = f"{VOLUME}/raw_assets"
spark.createDataFrame(pd.DataFrame(asset_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(asset_records)} asset records to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Asset-DMA Feed Mapping

# COMMAND ----------

# DBTITLE 1,Generate Asset-DMA Feed Mappings

feed_records = []

# DEMO_PUMP_01 feeds DEMO_DMA_01 (primary), DEMO_DMA_02 (secondary), DEMO_DMA_03 (secondary)
feed_records.append({"asset_id": "DEMO_PUMP_01", "dma_code": "DEMO_DMA_01", "feed_type": "primary", "notes": "Primary booster feed"})
feed_records.append({"asset_id": "DEMO_PUMP_01", "dma_code": "DEMO_DMA_02", "feed_type": "secondary", "notes": "Secondary feed via interconnection"})
feed_records.append({"asset_id": "DEMO_PUMP_01", "dma_code": "DEMO_DMA_03", "feed_type": "secondary", "notes": "Secondary feed via trunk main"})

# DEMO trunk main and valves feed DEMO_DMA_01
feed_records.append({"asset_id": "DEMO_TM_001", "dma_code": "DEMO_DMA_01", "feed_type": "primary", "notes": "Primary trunk main"})
feed_records.append({"asset_id": "DEMO_VALVE_01", "dma_code": "DEMO_DMA_01", "feed_type": "primary", "notes": "Isolation valve on trunk main"})
feed_records.append({"asset_id": "DEMO_VALVE_02", "dma_code": "DEMO_DMA_01", "feed_type": "primary", "notes": "Isolation valve on trunk main"})

# Other pumps feed their local DMAs
for a in asset_records:
    if a["asset_id"].startswith("PUMP_") or a["asset_id"].startswith("TM_") or a["asset_id"].startswith("PRV_"):
        feed_records.append({
            "asset_id": a["asset_id"],
            "dma_code": a["dma_code"],
            "feed_type": "primary",
            "notes": f"{a['asset_type']} serving local DMA",
        })

print(f"Total asset-DMA feed mappings: {len(feed_records)}")

# COMMAND ----------

# DBTITLE 1,Write raw_asset_dma_feed to Volume

path = f"{VOLUME}/raw_asset_dma_feed"
spark.createDataFrame(pd.DataFrame(feed_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(feed_records)} feed records to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reservoirs (20)

# COMMAND ----------

# DBTITLE 1,Generate Reservoir Records

random.seed(SEED + 4000)

reservoir_records = []

# DEMO_SR_01 — key service reservoir for DEMO_DMA_01
reservoir_records.append({
    "reservoir_id": "DEMO_SR_01",
    "reservoir_name": "Crystal Palace Service Reservoir",
    "latitude": 51.425,
    "longitude": -0.065,
    "elevation_m": 95.0,
    "capacity_ml": 5.0,
    "current_level_pct": 43.0,
    "current_volume_ml": round(5.0 * 0.43, 2),
    "hourly_demand_rate_ml": 0.694,
    "hours_remaining": round((5.0 * 0.43) / 0.694, 1),
    "status": "active",
    "last_inspection": "2025-10-15",
    "notes": "Primary reservoir for Crystal Palace zone. ~3.1h supply remaining at current demand.",
})

# 19 more reservoirs across London
reservoir_specs = [
    ("Highgate Reservoir", 51.57, -0.14, 90, 8.0),
    ("Hampstead Reservoir", 51.56, -0.17, 85, 6.5),
    ("Muswell Hill Reservoir", 51.59, -0.11, 80, 4.0),
    ("Wimbledon Reservoir", 51.41, -0.21, 60, 7.5),
    ("Greenwich Reservoir", 51.47, 0.01, 35, 5.5),
    ("Dulwich Reservoir", 51.45, -0.08, 70, 3.5),
    ("Streatham Reservoir", 51.43, -0.14, 55, 4.5),
    ("Brixton Reservoir", 51.46, -0.12, 40, 6.0),
    ("Lewisham Reservoir", 51.46, -0.02, 30, 5.0),
    ("Woolwich Reservoir", 51.49, 0.06, 20, 8.0),
    ("Enfield Reservoir", 51.65, -0.09, 45, 10.0),
    ("Tottenham Reservoir", 51.60, -0.07, 35, 7.0),
    ("Walthamstow Reservoir", 51.59, -0.04, 25, 12.0),
    ("Barking Reservoir", 51.54, 0.08, 15, 9.0),
    ("Richmond Reservoir", 51.46, -0.30, 30, 6.0),
    ("Ealing Reservoir", 51.51, -0.31, 25, 7.5),
    ("Harrow Reservoir", 51.58, -0.34, 50, 5.5),
    ("Croydon Reservoir", 51.38, -0.10, 60, 8.5),
    ("Sutton Reservoir", 51.36, -0.19, 45, 6.0),
]

for idx, (name, rlat, rlon, elev, capacity) in enumerate(reservoir_specs, 2):
    current_pct = round(random.uniform(55, 92), 1)
    hourly_rate = round(random.uniform(0.3, 1.5), 3)
    current_vol = round(capacity * current_pct / 100, 2)
    reservoir_records.append({
        "reservoir_id": f"SR_{idx:03d}",
        "reservoir_name": name,
        "latitude": rlat,
        "longitude": rlon,
        "elevation_m": float(elev),
        "capacity_ml": capacity,
        "current_level_pct": current_pct,
        "current_volume_ml": current_vol,
        "hourly_demand_rate_ml": hourly_rate,
        "hours_remaining": round(current_vol / hourly_rate, 1),
        "status": "active",
        "last_inspection": f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
        "notes": f"Service reservoir for {name.split(' ')[0]} zone",
    })

print(f"Total reservoirs: {len(reservoir_records)}")
demo_sr = reservoir_records[0]
print(f"DEMO_SR_01: capacity={demo_sr['capacity_ml']}ML, level={demo_sr['current_level_pct']}%, hours_remaining={demo_sr['hours_remaining']}h")

# COMMAND ----------

# DBTITLE 1,Write raw_reservoirs to Volume

path = f"{VOLUME}/raw_reservoirs"
spark.createDataFrame(pd.DataFrame(reservoir_records)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(reservoir_records)} reservoir records to {path}")

# COMMAND ----------

# DBTITLE 1,Generate & Write Reservoir-DMA Feed Mapping

random.seed(SEED + 4500)

res_dma_feeds = []

# DEMO_SR_01 feeds DEMO_DMA_01 (primary) plus a few nearby DMAs
res_dma_feeds.append({"reservoir_id": "DEMO_SR_01", "dma_code": "DEMO_DMA_01", "feed_type": "primary"})
res_dma_feeds.append({"reservoir_id": "DEMO_SR_01", "dma_code": "DEMO_DMA_02", "feed_type": "secondary"})
res_dma_feeds.append({"reservoir_id": "DEMO_SR_01", "dma_code": "DEMO_DMA_03", "feed_type": "secondary"})

# Each other reservoir feeds 3-8 DMAs
all_dma_codes = [row["dma_code"] for row in dma_rows]
for res in reservoir_records[1:]:
    num_fed = random.randint(3, 8)
    # Pick DMAs geographically nearest to the reservoir
    dma_dists = [
        (code, (dma_lookup[code]["centroid_latitude"] - res["latitude"])**2 + (dma_lookup[code]["centroid_longitude"] - res["longitude"])**2)
        for code in all_dma_codes
    ]
    dma_dists.sort(key=lambda x: x[1])
    for i, (dma_code, _) in enumerate(dma_dists[:num_fed]):
        res_dma_feeds.append({
            "reservoir_id": res["reservoir_id"],
            "dma_code": dma_code,
            "feed_type": "primary" if i == 0 else "secondary",
        })

print(f"Total reservoir-DMA feed mappings: {len(res_dma_feeds)}")

path = f"{VOLUME}/raw_reservoir_dma_feed"
spark.createDataFrame(pd.DataFrame(res_dma_feeds)).write.format("json").mode("overwrite").save(path)
print(f"Wrote {len(res_dma_feeds)} reservoir-DMA feed records to {path}")

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("=== Dimension Data Generation Complete ===")
print(f"  Sensors: {len(sensor_records)} (pressure: {sum(1 for s in sensor_records if s['sensor_type']=='pressure')}, flow: {sum(1 for s in sensor_records if s['sensor_type']=='flow')})")
print(f"  Properties: {len(property_records)}")
print(f"  Assets: {len(asset_records)}")
print(f"  Asset-DMA feeds: {len(feed_records)}")
print(f"  Reservoirs: {len(reservoir_records)}")
print(f"  Reservoir-DMA feeds: {len(res_dma_feeds)}")

display(spark.read.json(f"{VOLUME}/raw_sensors").filter("sensor_id LIKE 'DEMO%'").select("sensor_id", "sensor_type", "dma_code", "elevation_m"))
display(spark.read.json(f"{VOLUME}/raw_assets").filter("asset_id LIKE 'DEMO%'").select("asset_id", "asset_type", "status", "trip_timestamp"))
