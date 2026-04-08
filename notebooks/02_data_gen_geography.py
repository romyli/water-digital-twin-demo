# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Geography Data Generation
# MAGIC
# MAGIC Generates **500 DMA hexagonal polygons** tessellating Greater London, plus **~100 PMAs** as
# MAGIC subdivisions of selected DMAs.  All data is idempotent (fixed random seeds) and written as
# MAGIC Delta tables into the `water_digital_twin` catalog.
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Layer | Table |
# MAGIC |-------|-------|
# MAGIC | Bronze | `bronze.raw_dma_boundaries` |
# MAGIC | Bronze | `bronze.raw_pma_boundaries` |
# MAGIC | Silver | `silver.dim_dma` |
# MAGIC | Silver | `silver.dim_pma` |

# COMMAND ----------

# DBTITLE 1,Configuration & Constants
import math
import random
import json
from datetime import datetime

CATALOG = "water_digital_twin"
SEED = 42
random.seed(SEED)

# Greater London bounding box
LAT_MIN, LAT_MAX = 51.28, 51.70
LON_MIN, LON_MAX = -0.51, 0.33

# Demo DMA centre (Crystal Palace area)
DEMO_CENTER_LAT = 51.42
DEMO_CENTER_LON = -0.07

# Hex grid sizing — target ~500 hexagons across the bounding box
# Each hex is ~3 km across (flat-to-flat), so the radius is ~1.5 km
HEX_RADIUS_KM = 1.5

# Approximate km per degree at London's latitude
KM_PER_DEG_LAT = 111.32
KM_PER_DEG_LON = 111.32 * math.cos(math.radians(51.5))

HEX_RADIUS_LAT = HEX_RADIUS_KM / KM_PER_DEG_LAT
HEX_RADIUS_LON = HEX_RADIUS_KM / KM_PER_DEG_LON

# Pressure thresholds
PRESSURE_RED_THRESHOLD = 15.0
PRESSURE_AMBER_THRESHOLD = 25.0

# COMMAND ----------

# DBTITLE 1,Ensure Catalog & Schemas Exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

# COMMAND ----------

# DBTITLE 1,Helper — Hexagonal Polygon Generator

def hex_polygon_coords(center_lat, center_lon, radius_lat, radius_lon):
    """Return a list of 7 (lon, lat) pairs forming a flat-topped hexagon (closed ring)."""
    coords = []
    for i in range(7):
        angle_deg = 60 * (i % 6)
        angle_rad = math.radians(angle_deg)
        lon = center_lon + radius_lon * math.cos(angle_rad)
        lat = center_lat + radius_lat * math.sin(angle_rad)
        coords.append((round(lon, 8), round(lat, 8)))
    return coords


def polygon_wkt(coords):
    """Convert a list of (lon, lat) tuples to a WKT POLYGON string."""
    ring = ", ".join(f"{lon} {lat}" for lon, lat in coords)
    return f"POLYGON(({ring}))"

# COMMAND ----------

# DBTITLE 1,Build Hex Grid Centres

# Flat-topped hex tiling offsets
dx = HEX_RADIUS_LON * 3           # horizontal spacing between column centres
dy = HEX_RADIUS_LAT * math.sqrt(3)  # vertical spacing between row centres

# Build grid
hex_centers = []
col_idx = 0
lon = LON_MIN
while lon <= LON_MAX + dx:
    lat_offset = (dy / 2) if (col_idx % 2 == 1) else 0.0
    row_idx = 0
    lat = LAT_MIN + lat_offset
    while lat <= LAT_MAX + dy:
        hex_centers.append((lat, lon, col_idx, row_idx))
        lat += dy
        row_idx += 1
    lon += dx
    col_idx += 1

print(f"Total hex centres generated: {len(hex_centers)}")

# COMMAND ----------

# DBTITLE 1,Trim to 500 DMAs & Identify Demo DMAs

# Sort by distance to geographic centre of London to keep the most central 500
london_center_lat = (LAT_MIN + LAT_MAX) / 2
london_center_lon = (LON_MIN + LON_MAX) / 2

hex_centers.sort(key=lambda c: (c[0] - london_center_lat) ** 2 + (c[1] - london_center_lon) ** 2)
hex_centers = hex_centers[:500]

# Find the hex centre closest to Crystal Palace for DEMO_DMA_01
hex_centers.sort(
    key=lambda c: (c[0] - DEMO_CENTER_LAT) ** 2 + (c[1] - DEMO_CENTER_LON) ** 2
)
demo_01_idx = 0  # closest to Crystal Palace

# Tag the first three as demo DMAs — they are adjacent due to grid proximity
demo_indices = {0, 1, 2}

print(f"DEMO_DMA_01 centre: ({hex_centers[0][0]:.4f}, {hex_centers[0][1]:.4f})")
print(f"DEMO_DMA_02 centre: ({hex_centers[1][0]:.4f}, {hex_centers[1][1]:.4f})")
print(f"DEMO_DMA_03 centre: ({hex_centers[2][0]:.4f}, {hex_centers[2][1]:.4f})")

# COMMAND ----------

# DBTITLE 1,London Area Name Generator

# Realistic London-area names for non-demo DMAs
LONDON_AREA_PREFIXES = [
    "Acton", "Balham", "Bermondsey", "Bethnal Green", "Bow", "Brixton", "Bromley",
    "Camden", "Camberwell", "Canary Wharf", "Catford", "Chelsea", "Chiswick",
    "Clapham", "Clapton", "Cricklewood", "Croydon", "Dagenham", "Deptford",
    "Dulwich", "Ealing", "East Ham", "Edmonton", "Eltham", "Enfield",
    "Finchley", "Finsbury Park", "Forest Hill", "Fulham", "Greenwich",
    "Hackney", "Hammersmith", "Hampstead", "Hanwell", "Harlesden", "Harrow",
    "Hendon", "Herne Hill", "Highbury", "Highgate", "Holloway", "Hornsey",
    "Hounslow", "Ilford", "Islington", "Kennington", "Kentish Town",
    "Kensington", "Kilburn", "Kingston", "Lambeth", "Lee", "Lewisham",
    "Leyton", "Limehouse", "Maida Vale", "Manor Park", "Marylebone",
    "Mill Hill", "Mitcham", "Morden", "Muswell Hill", "New Cross",
    "Newham", "Norbury", "North Finchley", "Notting Hill", "Nunhead",
    "Old Kent Road", "Oval", "Paddington", "Palmers Green", "Peckham",
    "Penge", "Plaistow", "Plumstead", "Poplar", "Putney",
    "Richmond", "Romford", "Rotherhithe", "Seven Sisters", "Shepherd's Bush",
    "Shoreditch", "Sidcup", "Southall", "Southgate", "Southwark",
    "Stamford Hill", "Stepney", "Stockwell", "Stoke Newington", "Stratford",
    "Streatham", "Surbiton", "Sutton", "Tooting", "Tottenham",
    "Twickenham", "Uxbridge", "Vauxhall", "Walthamstow", "Wandsworth",
    "Wapping", "Wembley", "West Ham", "Westminster", "Whitechapel",
    "Willesden", "Wimbledon", "Wood Green", "Woodford", "Woolwich",
]

LONDON_AREA_SUFFIXES = [
    "North", "South", "East", "West", "Central", "Hill", "Park",
    "Green", "Gate", "Common", "Vale", "Rise", "Grove", "Fields",
]

DEMO_DMA_NAMES = {
    0: "Crystal Palace South",
    1: "Sydenham Hill",
    2: "Norwood Junction",
}

random.seed(SEED + 100)
used_names = set(DEMO_DMA_NAMES.values())

def generate_area_name(idx):
    """Generate a unique London-area name."""
    if idx in DEMO_DMA_NAMES:
        return DEMO_DMA_NAMES[idx]
    for _ in range(200):
        prefix = random.choice(LONDON_AREA_PREFIXES)
        suffix = random.choice(LONDON_AREA_SUFFIXES)
        name = f"{prefix} {suffix}"
        if name not in used_names:
            used_names.add(name)
            return name
    # fallback
    name = f"Area {idx}"
    used_names.add(name)
    return name

# COMMAND ----------

# DBTITLE 1,Elevation Model

# Simplified elevation model for Greater London
# Thames corridor is low (~5-15 m), hills are higher
ELEVATION_PEAKS = [
    # (lat, lon, peak_elevation, radius_km)
    (51.42, -0.07, 110, 3.0),   # Crystal Palace
    (51.57, -0.15, 100, 3.0),   # Highgate / Hampstead Heath
    (51.45, -0.08, 90, 2.5),    # Upper Norwood / Sydenham Hill
    (51.47, 0.00, 60, 2.0),     # Shooter's Hill area
    (51.58, -0.10, 80, 2.5),    # Muswell Hill
    (51.40, -0.20, 50, 2.0),    # Wimbledon Common
]

THAMES_LAT = 51.50  # approximate Thames latitude
THAMES_WIDTH_DEG = 0.02

def compute_elevation(lat, lon):
    """Compute a plausible elevation for a given lat/lon in Greater London."""
    # Base elevation: lower near Thames, gentle rise away from it
    dist_from_thames = abs(lat - THAMES_LAT)
    base_elev = 5.0 + dist_from_thames * 150  # gentle slope

    # Cap base at ~30 m
    base_elev = min(base_elev, 30.0)

    # Near Thames corridor — very low
    if dist_from_thames < THAMES_WIDTH_DEG:
        base_elev = random.uniform(3.0, 10.0)

    # Add hill peaks via Gaussian influence
    peak_contribution = 0.0
    for plat, plon, pelev, pradius in ELEVATION_PEAKS:
        dist_km = math.sqrt(
            ((lat - plat) * KM_PER_DEG_LAT) ** 2
            + ((lon - plon) * KM_PER_DEG_LON) ** 2
        )
        influence = pelev * math.exp(-0.5 * (dist_km / pradius) ** 2)
        peak_contribution = max(peak_contribution, influence)

    elevation = base_elev + peak_contribution * 0.7
    # Add small random noise
    elevation += random.uniform(-2, 2)
    return round(max(elevation, 2.0), 1)

# COMMAND ----------

# DBTITLE 1,H3 Index Helper (Resolution 7)

def lat_lon_to_h3_res7(lat, lon):
    """
    Generate a deterministic H3-like index string for a given lat/lon at resolution 7.
    Uses a simple geohash-style encoding since H3 library may not be available.
    """
    lat_norm = int(((lat - LAT_MIN) / (LAT_MAX - LAT_MIN)) * 0xFFFFF) & 0xFFFFF
    lon_norm = int(((lon - LON_MIN) / (LON_MAX - LON_MIN)) * 0xFFFFF) & 0xFFFFF
    index_val = (0x87 << 56) | (lat_norm << 28) | (lon_norm << 4) | 7
    return f"{index_val:016x}"

# COMMAND ----------

# DBTITLE 1,Build DMA Records

random.seed(SEED + 200)

dma_records = []
for idx, (lat, lon, col, row) in enumerate(hex_centers):
    if idx in demo_indices:
        dma_code = f"DEMO_DMA_{idx + 1:02d}"
    else:
        dma_code = f"DMA_{idx + 1:05d}"

    name = generate_area_name(idx)
    coords = hex_polygon_coords(lat, lon, HEX_RADIUS_LAT, HEX_RADIUS_LON)
    wkt = polygon_wkt(coords)
    elevation = compute_elevation(lat, lon)
    h3_index = lat_lon_to_h3_res7(lat, lon)

    dma_records.append({
        "dma_code": dma_code,
        "dma_name": name,
        "centroid_lat": round(lat, 6),
        "centroid_lon": round(lon, 6),
        "polygon_wkt": wkt,
        "area_km2": round(math.pi * HEX_RADIUS_KM ** 2 * 0.83, 2),  # hex area approx 0.83 * pi*r^2
        "elevation_m": elevation,
        "h3_index_res7": h3_index,
        "pressure_red_threshold": PRESSURE_RED_THRESHOLD,
        "pressure_amber_threshold": PRESSURE_AMBER_THRESHOLD,
        "region": "Greater London",
        "water_company": "Water Utilities",
        "created_at": datetime(2025, 1, 1).isoformat(),
    })

print(f"DMA records built: {len(dma_records)}")
print(f"Demo DMAs: {[r['dma_code'] for r in dma_records if r['dma_code'].startswith('DEMO')]}")

# Verify Crystal Palace elevation
demo_01 = next(r for r in dma_records if r["dma_code"] == "DEMO_DMA_01")
print(f"DEMO_DMA_01 elevation: {demo_01['elevation_m']} m (target: 90-110 m)")

# COMMAND ----------

# DBTITLE 1,Write bronze.raw_dma_boundaries

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, FloatType, TimestampType
)

raw_dma_schema = StructType([
    StructField("dma_code", StringType(), False),
    StructField("dma_name", StringType(), False),
    StructField("centroid_lat", DoubleType(), False),
    StructField("centroid_lon", DoubleType(), False),
    StructField("polygon_wkt", StringType(), False),
    StructField("area_km2", DoubleType(), False),
    StructField("elevation_m", FloatType(), False),
    StructField("h3_index_res7", StringType(), False),
    StructField("region", StringType(), False),
    StructField("water_company", StringType(), False),
    StructField("created_at", StringType(), False),
])

raw_dma_rows = [
    (
        r["dma_code"], r["dma_name"], r["centroid_lat"], r["centroid_lon"],
        r["polygon_wkt"], r["area_km2"], float(r["elevation_m"]),
        r["h3_index_res7"], r["region"], r["water_company"], r["created_at"],
    )
    for r in dma_records
]

df_raw_dma = spark.createDataFrame(raw_dma_rows, schema=raw_dma_schema)
(
    df_raw_dma.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_dma_boundaries")
)
print(f"Wrote {df_raw_dma.count()} rows to {CATALOG}.bronze.raw_dma_boundaries")

# COMMAND ----------

# DBTITLE 1,Write silver.dim_dma

dim_dma_schema = StructType([
    StructField("dma_code", StringType(), False),
    StructField("dma_name", StringType(), False),
    StructField("centroid_lat", DoubleType(), False),
    StructField("centroid_lon", DoubleType(), False),
    StructField("polygon_wkt", StringType(), False),
    StructField("area_km2", DoubleType(), False),
    StructField("elevation_m", FloatType(), False),
    StructField("h3_index_res7", StringType(), False),
    StructField("pressure_red_threshold", FloatType(), False),
    StructField("pressure_amber_threshold", FloatType(), False),
    StructField("region", StringType(), False),
    StructField("water_company", StringType(), False),
    StructField("created_at", StringType(), False),
])

dim_dma_rows = [
    (
        r["dma_code"], r["dma_name"], r["centroid_lat"], r["centroid_lon"],
        r["polygon_wkt"], r["area_km2"], float(r["elevation_m"]),
        r["h3_index_res7"], float(r["pressure_red_threshold"]),
        float(r["pressure_amber_threshold"]),
        r["region"], r["water_company"], r["created_at"],
    )
    for r in dma_records
]

df_dim_dma = spark.createDataFrame(dim_dma_rows, schema=dim_dma_schema)
(
    df_dim_dma.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.dim_dma")
)
print(f"Wrote {df_dim_dma.count()} rows to {CATALOG}.silver.dim_dma")

# COMMAND ----------

# DBTITLE 1,Generate PMA Records (~100 PMAs)

random.seed(SEED + 300)

# Select DMAs that will have PMAs — all demo DMAs plus ~30 random others
pma_dma_indices = list(demo_indices)
other_indices = [i for i in range(len(dma_records)) if i not in demo_indices]
random.shuffle(other_indices)
pma_dma_indices.extend(other_indices[:32])  # ~35 DMAs with PMAs, averaging ~3 PMAs each

PMA_SUFFIXES = ["Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot"]

pma_records = []
pma_counter = 1
for dma_idx in pma_dma_indices:
    dma = dma_records[dma_idx]
    num_pmas = random.randint(2, 4)
    for p in range(num_pmas):
        # Offset PMA centroid slightly from DMA centroid
        angle = (2 * math.pi * p) / num_pmas
        offset_lat = HEX_RADIUS_LAT * 0.3 * math.sin(angle)
        offset_lon = HEX_RADIUS_LON * 0.3 * math.cos(angle)

        pma_lat = round(dma["centroid_lat"] + offset_lat, 6)
        pma_lon = round(dma["centroid_lon"] + offset_lon, 6)

        if dma["dma_code"].startswith("DEMO"):
            pma_code = f"{dma['dma_code']}_PMA_{PMA_SUFFIXES[p].upper()}"
        else:
            pma_code = f"PMA_{pma_counter:05d}"

        pma_records.append({
            "pma_code": pma_code,
            "pma_name": f"{dma['dma_name']} {PMA_SUFFIXES[p]}",
            "parent_dma_code": dma["dma_code"],
            "centroid_lat": pma_lat,
            "centroid_lon": pma_lon,
            "elevation_m": compute_elevation(pma_lat, pma_lon),
            "h3_index_res7": lat_lon_to_h3_res7(pma_lat, pma_lon),
            "created_at": datetime(2025, 1, 1).isoformat(),
        })
        pma_counter += 1

print(f"PMA records built: {len(pma_records)}")

# COMMAND ----------

# DBTITLE 1,Write bronze.raw_pma_boundaries

raw_pma_schema = StructType([
    StructField("pma_code", StringType(), False),
    StructField("pma_name", StringType(), False),
    StructField("parent_dma_code", StringType(), False),
    StructField("centroid_lat", DoubleType(), False),
    StructField("centroid_lon", DoubleType(), False),
    StructField("elevation_m", FloatType(), False),
    StructField("h3_index_res7", StringType(), False),
    StructField("created_at", StringType(), False),
])

raw_pma_rows = [
    (
        r["pma_code"], r["pma_name"], r["parent_dma_code"],
        r["centroid_lat"], r["centroid_lon"], float(r["elevation_m"]),
        r["h3_index_res7"], r["created_at"],
    )
    for r in pma_records
]

df_raw_pma = spark.createDataFrame(raw_pma_rows, schema=raw_pma_schema)
(
    df_raw_pma.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.raw_pma_boundaries")
)
print(f"Wrote {df_raw_pma.count()} rows to {CATALOG}.bronze.raw_pma_boundaries")

# COMMAND ----------

# DBTITLE 1,Write silver.dim_pma

dim_pma_schema = StructType([
    StructField("pma_code", StringType(), False),
    StructField("pma_name", StringType(), False),
    StructField("parent_dma_code", StringType(), False),
    StructField("centroid_lat", DoubleType(), False),
    StructField("centroid_lon", DoubleType(), False),
    StructField("elevation_m", FloatType(), False),
    StructField("h3_index_res7", StringType(), False),
    StructField("created_at", StringType(), False),
])

dim_pma_rows = raw_pma_rows  # same structure for dim layer

df_dim_pma = spark.createDataFrame(dim_pma_rows, schema=dim_pma_schema)
(
    df_dim_pma.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.dim_pma")
)
print(f"Wrote {df_dim_pma.count()} rows to {CATALOG}.silver.dim_pma")

# COMMAND ----------

# DBTITLE 1,Validation
print("=== Geography Data Generation Complete ===")
print(f"  DMAs: {len(dma_records)}")
print(f"  PMAs: {len(pma_records)}")
print(f"  Demo DMA codes: {[r['dma_code'] for r in dma_records if r['dma_code'].startswith('DEMO')]}")
print(f"  Pressure red threshold: {PRESSURE_RED_THRESHOLD} m")
print(f"  Pressure amber threshold: {PRESSURE_AMBER_THRESHOLD} m")

# Quick verification queries
display(spark.sql(f"SELECT dma_code, dma_name, elevation_m, centroid_lat, centroid_lon FROM {CATALOG}.silver.dim_dma WHERE dma_code LIKE 'DEMO%'"))
display(spark.sql(f"SELECT COUNT(*) as total_dmas FROM {CATALOG}.silver.dim_dma"))
display(spark.sql(f"SELECT COUNT(*) as total_pmas FROM {CATALOG}.silver.dim_pma"))
