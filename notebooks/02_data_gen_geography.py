# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Geography Data Generation (Real Ward Boundaries)
# MAGIC
# MAGIC Downloads **ONS Electoral Ward boundaries** (Dec 2025, Generalised Clipped) from the
# MAGIC ONS Open Geography Portal and uses the **500 most central Greater London wards** as
# MAGIC realistic DMA polygons.  Ward shapes follow real streets and administrative boundaries,
# MAGIC producing a far more authentic map than synthetic hex grids.
# MAGIC
# MAGIC **Data source:** ONS Open Geography Portal — Wards (December 2025) Boundaries UK BGC
# MAGIC **Licence:** Open Government Licence v3.0 — free for commercial and non-commercial use
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Layer | Table |
# MAGIC |-------|-------|
# MAGIC | Volume | `landing_zone/raw_dma_boundaries/` |
# MAGIC | Volume | `landing_zone/raw_pma_boundaries/` |

# COMMAND ----------

# DBTITLE 1,Configuration & Constants
import math
import random
import json
import requests
import pandas as pd
from datetime import datetime

CATALOG = "water_digital_twin"
VOLUME = f"/Volumes/{CATALOG}/bronze/landing_zone"
SEED = 42
random.seed(SEED)

# Crystal Palace target centre for DEMO_DMA_01
DEMO_CENTER_LAT = 51.42
DEMO_CENTER_LON = -0.07

# ONS ArcGIS Feature Server — Wards Dec 2025, Generalised Clipped Boundaries (UK)
ONS_WARD_URL = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
    "WD_DEC_2025_UK_BGC/FeatureServer/0/query"
)

# Pressure thresholds (metres head)
PRESSURE_RED_THRESHOLD = 15.0
PRESSURE_AMBER_THRESHOLD = 25.0

# Approximate km per degree at London's latitude
KM_PER_DEG_LAT = 111.32
KM_PER_DEG_LON = 111.32 * math.cos(math.radians(51.5))

# COMMAND ----------

# DBTITLE 1,Download London Ward Boundaries from ONS
params = {
    "where": "LAD25CD LIKE 'E09%'",  # E09 = London boroughs
    "outFields": "WD25CD,WD25NM,LAD25NM,LONG,LAT,Shape__Area",
    "outSR": "4326",
    "f": "geojson",
    "resultRecordCount": 2000,
}

print("Downloading ward boundaries from ONS Open Geography Portal...")
resp = requests.get(ONS_WARD_URL, params=params, timeout=60)
resp.raise_for_status()
geojson = resp.json()

all_wards = geojson["features"]
print(f"Downloaded {len(all_wards)} London ward boundaries")

# COMMAND ----------

# DBTITLE 1,Select 500 Most Central Wards & Identify Demo DMAs

# Sort by distance to Crystal Palace to select the 500 most central
for ward in all_wards:
    lat = ward["properties"]["LAT"]
    lon = ward["properties"]["LONG"]
    ward["_dist"] = math.sqrt((lat - DEMO_CENTER_LAT) ** 2 + (lon - DEMO_CENTER_LON) ** 2)

all_wards.sort(key=lambda w: w["_dist"])
wards = all_wards[:500]

# Find the ward closest to Crystal Palace — this becomes DEMO_DMA_01
# Find the next two closest that border different Local Authorities for variety
demo_01 = wards[0]
demo_02 = wards[1]
demo_03 = wards[2]

print(f"DEMO_DMA_01: {demo_01['properties']['WD25NM']} ({demo_01['properties']['LAD25NM']})")
print(f"  Centre: ({demo_01['properties']['LAT']:.5f}, {demo_01['properties']['LONG']:.5f})")
print(f"DEMO_DMA_02: {demo_02['properties']['WD25NM']} ({demo_02['properties']['LAD25NM']})")
print(f"DEMO_DMA_03: {demo_03['properties']['WD25NM']} ({demo_03['properties']['LAD25NM']})")

# COMMAND ----------

# DBTITLE 1,Real Elevation Data (SRTM via Open-Meteo API)

# Uses the Open-Meteo Elevation API which returns SRTM (Shuttle Radar Topography
# Mission) data at ~30m resolution. This gives real terrain elevation for any
# lat/lon — accurate enough for 3D map visualisation.
#
# API docs: https://open-meteo.com/en/docs/elevation-api
# No API key required. Free for non-commercial and commercial use.

ELEVATION_API_URL = "https://api.open-meteo.com/v1/elevation"
ELEVATION_BATCH_SIZE = 100  # API accepts up to ~100 coords per request

_elevation_cache = {}  # (lat_round, lon_round) -> elevation


def fetch_elevations_batch(coords):
    """
    Fetch real SRTM elevations for a list of (lat, lon) tuples.
    Returns a list of elevation values in metres.
    """
    results = []
    for i in range(0, len(coords), ELEVATION_BATCH_SIZE):
        batch = coords[i : i + ELEVATION_BATCH_SIZE]
        lat_str = ",".join(f"{lat:.5f}" for lat, lon in batch)
        lon_str = ",".join(f"{lon:.5f}" for lat, lon in batch)

        resp = requests.get(
            ELEVATION_API_URL,
            params={"latitude": lat_str, "longitude": lon_str},
            timeout=30,
        )
        resp.raise_for_status()
        elevations = resp.json()["elevation"]
        results.extend(elevations)

        # Cache for later reuse (e.g., by property/sensor generation notebooks)
        for (lat, lon), elev in zip(batch, elevations):
            _elevation_cache[(round(lat, 4), round(lon, 4))] = elev

    return results


def get_elevation(lat, lon):
    """
    Get real SRTM elevation for a single point. Uses cache if available,
    otherwise fetches from API.
    """
    key = (round(lat, 4), round(lon, 4))
    if key in _elevation_cache:
        return _elevation_cache[key]

    resp = requests.get(
        ELEVATION_API_URL,
        params={"latitude": f"{lat:.5f}", "longitude": f"{lon:.5f}"},
        timeout=10,
    )
    resp.raise_for_status()
    elev = resp.json()["elevation"][0]
    _elevation_cache[key] = elev
    return elev


# Pre-fetch elevations for all 500 DMA centroids in batch
print("Fetching real SRTM elevations for 500 DMA centroids...")
dma_coords = [(w["properties"]["LAT"], w["properties"]["LONG"]) for w in wards]
dma_elevations = fetch_elevations_batch(dma_coords)
print(f"  Range: {min(dma_elevations):.0f}m to {max(dma_elevations):.0f}m")
print(f"  DEMO_DMA_01 ({wards[0]['properties']['WD25NM']}): {dma_elevations[0]:.0f}m")
print(f"  Median: {sorted(dma_elevations)[250]:.0f}m")

# COMMAND ----------

# DBTITLE 1,Convert GeoJSON to WKT

def geojson_to_wkt(geometry):
    """Convert a GeoJSON geometry to WKT string."""
    geom_type = geometry["type"]
    coords = geometry["coordinates"]

    if geom_type == "Polygon":
        rings = []
        for ring in coords:
            ring_str = ", ".join(f"{c[0]} {c[1]}" for c in ring)
            rings.append(f"({ring_str})")
        return f"POLYGON({', '.join(rings)})"

    elif geom_type == "MultiPolygon":
        # Take the largest polygon (by vertex count) for simplicity
        largest = max(coords, key=lambda poly: len(poly[0]))
        rings = []
        for ring in largest:
            ring_str = ", ".join(f"{c[0]} {c[1]}" for c in ring)
            rings.append(f"({ring_str})")
        return f"POLYGON({', '.join(rings)})"

    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")

# COMMAND ----------

# DBTITLE 1,Build DMA Records from Ward Boundaries

random.seed(SEED + 200)

DEMO_CODES = {0: "DEMO_DMA_01", 1: "DEMO_DMA_02", 2: "DEMO_DMA_03"}

dma_records = []
for idx, ward in enumerate(wards):
    props = ward["properties"]

    # Assign DMA code
    if idx in DEMO_CODES:
        dma_code = DEMO_CODES[idx]
        # Use the real ward name as the DMA name
        dma_name = props["WD25NM"]
    else:
        dma_code = f"DMA_{idx + 1:05d}"
        dma_name = props["WD25NM"]

    centroid_lat = props["LAT"]
    centroid_lon = props["LONG"]

    # Convert geometry to WKT
    geometry_wkt = geojson_to_wkt(ward["geometry"])

    # Real SRTM elevation from pre-fetched batch
    elevation = round(dma_elevations[idx], 1)

    # Area code from borough
    dma_area_code = props["LAD25NM"]

    dma_records.append({
        "dma_code": dma_code,
        "dma_name": dma_name,
        "dma_area_code": dma_area_code,
        "geometry_wkt": geometry_wkt,
        "centroid_latitude": round(centroid_lat, 6),
        "centroid_longitude": round(centroid_lon, 6),
        "avg_elevation": elevation,
        "pressure_red_threshold": PRESSURE_RED_THRESHOLD,
        "pressure_amber_threshold": PRESSURE_AMBER_THRESHOLD,
        "source_ward_code": props["WD25CD"],
        "source_system": "ons_open_geography",
        "ingested_at": datetime(2026, 4, 1).isoformat(),
    })

print(f"DMA records built: {len(dma_records)}")
demo_recs = [r for r in dma_records if r["dma_code"].startswith("DEMO")]
for r in demo_recs:
    print(f"  {r['dma_code']}: {r['dma_name']} ({r['dma_area_code']}) — elev {r['avg_elevation']}m")

# COMMAND ----------

# DBTITLE 1,Write raw_dma_boundaries to Volume

# Write full records (including enriched columns) so downstream notebooks can read them
dma_path = f"{VOLUME}/raw_dma_boundaries"
spark.createDataFrame(pd.DataFrame(dma_records)).write.format("json").mode("overwrite").save(dma_path)
print(f"Wrote {len(dma_records)} DMA records to {dma_path}")

# COMMAND ----------

# DBTITLE 1,Generate PMA Records (~100 PMAs)

# PMAs are subdivisions of selected DMAs — we create them by subdividing the
# parent DMA polygon conceptually (offset centroids within the DMA).
# In reality PMAs follow PRV zones; here we approximate with offsets.

random.seed(SEED + 300)

HEX_RADIUS_LAT = 1.5 / KM_PER_DEG_LAT  # ~1.5 km offset radius
HEX_RADIUS_LON = 1.5 / KM_PER_DEG_LON

# Select DMAs that will have PMAs — demo DMAs + ~30 random others
pma_dma_indices = [0, 1, 2]  # demo DMAs
other_indices = list(range(3, len(dma_records)))
random.shuffle(other_indices)
pma_dma_indices.extend(other_indices[:32])

PMA_SUFFIXES = ["Alpha", "Bravo", "Charlie", "Delta"]

pma_records = []
pma_counter = 1
for dma_idx in pma_dma_indices:
    dma = dma_records[dma_idx]
    num_pmas = random.randint(2, 4)
    for p in range(num_pmas):
        angle = (2 * math.pi * p) / num_pmas
        offset_lat = HEX_RADIUS_LAT * 0.3 * math.sin(angle)
        offset_lon = HEX_RADIUS_LON * 0.3 * math.cos(angle)

        pma_lat = round(dma["centroid_latitude"] + offset_lat, 6)
        pma_lon = round(dma["centroid_longitude"] + offset_lon, 6)

        if dma["dma_code"].startswith("DEMO"):
            pma_code = f"{dma['dma_code']}_PMA_{PMA_SUFFIXES[p].upper()}"
        else:
            pma_code = f"PMA_{pma_counter:05d}"

        # Create a small polygon around the PMA centroid (approximate)
        pma_radius_lat = HEX_RADIUS_LAT * 0.15
        pma_radius_lon = HEX_RADIUS_LON * 0.15
        pma_coords = []
        for i in range(7):
            a = math.radians(60 * (i % 6))
            pma_coords.append(
                f"{round(pma_lon + pma_radius_lon * math.cos(a), 8)} "
                f"{round(pma_lat + pma_radius_lat * math.sin(a), 8)}"
            )
        pma_wkt = f"POLYGON(({', '.join(pma_coords)}))"

        pma_records.append({
            "pma_code": pma_code,
            "pma_name": f"{dma['dma_name']} {PMA_SUFFIXES[p]}",
            "dma_code": dma["dma_code"],
            "geometry_wkt": pma_wkt,
            "source_system": "derived",
            "ingested_at": datetime(2026, 4, 1).isoformat(),
        })
        pma_counter += 1

print(f"PMA records built: {len(pma_records)}")

# COMMAND ----------

# DBTITLE 1,Write raw_pma_boundaries to Volume

pma_path = f"{VOLUME}/raw_pma_boundaries"
spark.createDataFrame(pd.DataFrame(pma_records)).write.format("json").mode("overwrite").save(pma_path)
print(f"Wrote {len(pma_records)} PMA records to {pma_path}")

# COMMAND ----------

# DBTITLE 1,Validation

print("=== Geography Data Generation Complete ===")
print(f"  DMAs: {len(dma_records)}")
print(f"  PMAs: {len(pma_records)}")
print()

demo_recs = [r for r in dma_records if r["dma_code"].startswith("DEMO")]
for r in demo_recs:
    print(f"  {r['dma_code']}: {r['dma_name']} ({r['dma_area_code']})")
    print(f"    Centre: ({r['centroid_latitude']}, {r['centroid_longitude']})")
    print(f"    Elevation: {r['avg_elevation']} m")
    print()

# Verify files in volume
display(spark.read.json(f"{VOLUME}/raw_dma_boundaries").filter("dma_code LIKE 'DEMO%'").select("dma_code", "dma_name", "dma_area_code"))
print(f"  DMA files: {spark.read.json(f'{VOLUME}/raw_dma_boundaries').count()} rows")
print(f"  PMA files: {spark.read.json(f'{VOLUME}/raw_pma_boundaries').count()} rows")
