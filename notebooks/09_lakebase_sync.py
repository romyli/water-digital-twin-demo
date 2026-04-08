# Databricks notebook source

# MAGIC %md
# MAGIC # 09 — Delta Gold → Lakebase Sync
# MAGIC
# MAGIC **Purpose:** Sync 15 Delta tables (Silver + Gold) to Lakebase (Databricks PostgreSQL) for sub-second
# MAGIC app serving. The Databricks App's FastAPI backend reads from Lakebase via the Data API for
# MAGIC low-latency responses.
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Connect to the Lakebase project `water-digital-twin-lakebase` via the Databricks SDK
# MAGIC 2. Create the PostGIS extension and all 15 tables with DDL
# MAGIC 3. TRUNCATE + batch INSERT each table (full refresh for demo simplicity)
# MAGIC 4. Verify row counts
# MAGIC
# MAGIC **Prerequisite:** Lakebase project `water-digital-twin-lakebase` must be provisioned and running
# MAGIC (see `guides/01_workspace_setup_guide.md`).
# MAGIC
# MAGIC **Catalog:** `water_digital_twin` &nbsp;|&nbsp; **Demo timestamp:** `2026-04-07 05:30:00`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Lakebase Connection

# COMMAND ----------

# -- Lakebase connection config ------------------------------------------------

LAKEBASE_PROJECT_NAME = "water-digital-twin-lakebase"
CATALOG = "water_digital_twin"
BATCH_SIZE = 1000

# Delta source tables → Lakebase target table mapping
TABLE_SYNC_CONFIG = [
    # (lakebase_table, delta_source, has_geometry, geometry_type, filter)
    # Silver spatial tables
    ("dim_dma",         f"{CATALOG}.silver.dim_dma",         True,  "Polygon",  None),
    ("dim_sensor",      f"{CATALOG}.silver.dim_sensor",      True,  "Point",    None),
    ("dim_properties",  f"{CATALOG}.silver.dim_properties",  True,  "Point",    None),
    ("dim_assets",      f"{CATALOG}.silver.dim_assets",      True,  None,       None),   # generic GEOMETRY (POINT or LINESTRING)
    ("dim_reservoirs",  f"{CATALOG}.silver.dim_reservoirs",  True,  "Point",    None),
    # Gold tables (no geometry)
    ("dim_incidents",          f"{CATALOG}.gold.dim_incidents",          False, None, None),
    ("incident_events",        f"{CATALOG}.gold.incident_events",        False, None, None),
    ("communications_log",     f"{CATALOG}.gold.communications_log",     False, None, None),
    ("shift_handovers",        f"{CATALOG}.gold.shift_handovers",        False, None, None),
    ("dim_response_playbooks", f"{CATALOG}.gold.dim_response_playbooks", False, None, None),
    ("comms_requests",         f"{CATALOG}.gold.comms_requests",         False, None, None),
    ("dma_status",             f"{CATALOG}.gold.dma_status",             False, None, None),
    ("dma_summary",            f"{CATALOG}.gold.dma_summary",            False, None, None),
    ("anomaly_scores",         f"{CATALOG}.gold.anomaly_scores",         False, None, None),
    # Silver fact table (last 7 days only)
    ("fact_telemetry", f"{CATALOG}.silver.fact_telemetry", False, None, "last_7_days"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialise Databricks SDK client and resolve Lakebase endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import json, time

w = WorkspaceClient()

# Lakebase Data API endpoint
# Docs: POST /api/2.0/lakebase/data/{project_name}/sql
LAKEBASE_SQL_ENDPOINT = f"/api/2.0/lakebase/data/{LAKEBASE_PROJECT_NAME}/sql"

def execute_lakebase_sql(sql: str, params: list = None):
    """Execute a SQL statement against Lakebase via the Data API."""
    payload = {"statement": sql}
    if params:
        payload["parameters"] = params
    response = w.api_client.do(
        method="POST",
        path=LAKEBASE_SQL_ENDPOINT,
        body=payload,
    )
    return response

def execute_lakebase_sql_with_retry(sql: str, params: list = None, max_retries: int = 3):
    """Execute SQL with retry logic for transient errors."""
    for attempt in range(max_retries):
        try:
            return execute_lakebase_sql(sql, params)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  Retry {attempt + 1}/{max_retries} after error: {e}")
                time.sleep(2 ** attempt)
            else:
                raise

# Quick connectivity test
result = execute_lakebase_sql("SELECT 1 AS health_check")
print(f"Lakebase connection OK: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create PostGIS Extension and Lakebase Tables (DDL)
# MAGIC
# MAGIC Each spatial table gets a native PostGIS `geom` column with a GiST index for fast spatial queries.
# MAGIC The app's map layer reads these geometries directly.

# COMMAND ----------

# -- PostGIS extension ---------------------------------------------------------

execute_lakebase_sql("CREATE EXTENSION IF NOT EXISTS postgis")
print("PostGIS extension enabled.")

# COMMAND ----------

# -- DDL: All 15 Lakebase tables -----------------------------------------------

DDL_STATEMENTS = {
    # -------------------------------------------------------------------------
    # Silver spatial tables
    # -------------------------------------------------------------------------
    "dim_dma": [
        "DROP TABLE IF EXISTS dim_dma CASCADE",
        """CREATE TABLE dim_dma (
            dma_code            TEXT PRIMARY KEY,
            dma_name            TEXT,
            dma_area_code       TEXT,
            geometry_wkt        TEXT,
            geom                GEOMETRY(Polygon, 4326),
            centroid_latitude   DOUBLE PRECISION,
            centroid_longitude  DOUBLE PRECISION,
            avg_elevation       DOUBLE PRECISION,
            pressure_red_threshold   DOUBLE PRECISION DEFAULT 15.0,
            pressure_amber_threshold DOUBLE PRECISION DEFAULT 25.0
        )""",
    ],
    "dim_sensor": [
        "DROP TABLE IF EXISTS dim_sensor CASCADE",
        """CREATE TABLE dim_sensor (
            sensor_id       TEXT PRIMARY KEY,
            name            TEXT,
            sensor_type     TEXT,
            dma_code        TEXT,
            pma_code        TEXT,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            elevation       DOUBLE PRECISION,
            geometry_wkt    TEXT,
            geom            GEOMETRY(Point, 4326),
            h3_index        TEXT,
            is_active       BOOLEAN,
            installed_date  DATE
        )""",
    ],
    "dim_properties": [
        "DROP TABLE IF EXISTS dim_properties CASCADE",
        """CREATE TABLE dim_properties (
            uprn            TEXT PRIMARY KEY,
            address         TEXT,
            postcode        TEXT,
            property_type   TEXT,
            dma_code        TEXT,
            pma_code        TEXT,
            customer_height DOUBLE PRECISION,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            geometry_wkt    TEXT,
            geom            GEOMETRY(Point, 4326),
            h3_index        TEXT
        )""",
    ],
    "dim_assets": [
        "DROP TABLE IF EXISTS dim_assets CASCADE",
        """CREATE TABLE dim_assets (
            asset_id        TEXT PRIMARY KEY,
            asset_type      TEXT,
            name            TEXT,
            status          TEXT,
            latitude        DOUBLE PRECISION,
            longitude       DOUBLE PRECISION,
            geometry_wkt    TEXT,
            geom            GEOMETRY,
            diameter_inches INTEGER,
            length_km       DOUBLE PRECISION,
            trip_timestamp  TIMESTAMP,
            installed_date  DATE
        )""",
    ],
    "dim_reservoirs": [
        "DROP TABLE IF EXISTS dim_reservoirs CASCADE",
        """CREATE TABLE dim_reservoirs (
            reservoir_id        TEXT PRIMARY KEY,
            name                TEXT,
            capacity_ml         DOUBLE PRECISION,
            current_level_pct   DOUBLE PRECISION,
            hourly_demand_rate_ml DOUBLE PRECISION,
            latitude            DOUBLE PRECISION,
            longitude           DOUBLE PRECISION,
            geometry_wkt        TEXT,
            geom                GEOMETRY(Point, 4326)
        )""",
    ],
    # -------------------------------------------------------------------------
    # Gold tables (no geometry)
    # -------------------------------------------------------------------------
    "dim_incidents": [
        "DROP TABLE IF EXISTS dim_incidents CASCADE",
        """CREATE TABLE dim_incidents (
            incident_id                 TEXT PRIMARY KEY,
            dma_code                    TEXT,
            root_cause_asset_id         TEXT,
            start_timestamp             TIMESTAMP,
            end_timestamp               TIMESTAMP,
            status                      TEXT,
            severity                    TEXT,
            total_properties_affected   INTEGER,
            sensitive_premises_affected BOOLEAN
        )""",
    ],
    "incident_events": [
        "DROP TABLE IF EXISTS incident_events CASCADE",
        """CREATE TABLE incident_events (
            event_id        TEXT PRIMARY KEY,
            incident_id     TEXT,
            timestamp       TIMESTAMP,
            event_type      TEXT,
            source          TEXT,
            description     TEXT,
            operator_id     TEXT
        )""",
    ],
    "communications_log": [
        "DROP TABLE IF EXISTS communications_log CASCADE",
        """CREATE TABLE communications_log (
            log_id          TEXT PRIMARY KEY,
            incident_id     TEXT,
            timestamp       TIMESTAMP,
            contact_role    TEXT,
            method          TEXT,
            summary         TEXT,
            action_agreed   TEXT,
            operator_id     TEXT
        )""",
    ],
    "shift_handovers": [
        "DROP TABLE IF EXISTS shift_handovers CASCADE",
        """CREATE TABLE shift_handovers (
            handover_id           TEXT PRIMARY KEY,
            incident_id           TEXT,
            outgoing_operator     TEXT,
            incoming_operator     TEXT,
            generated_summary     TEXT,
            risk_of_escalation    TEXT,
            current_trajectory    TEXT,
            operator_edits        TEXT,
            signed_off_at         TIMESTAMP,
            acknowledged_at       TIMESTAMP
        )""",
    ],
    "dim_response_playbooks": [
        "DROP TABLE IF EXISTS dim_response_playbooks CASCADE",
        """CREATE TABLE dim_response_playbooks (
            playbook_id         TEXT PRIMARY KEY,
            incident_type       TEXT,
            sop_reference       TEXT,
            action_steps        TEXT,
            last_updated_by     TEXT,
            last_updated_at     TIMESTAMP
        )""",
    ],
    "comms_requests": [
        "DROP TABLE IF EXISTS comms_requests CASCADE",
        """CREATE TABLE comms_requests (
            request_id                  TEXT PRIMARY KEY,
            incident_id                 TEXT,
            dma_code                    TEXT,
            requested_by                TEXT,
            requested_at                TIMESTAMP,
            message_template            TEXT,
            affected_postcodes          TEXT,
            estimated_restoration_time  TEXT,
            customer_count              INTEGER,
            status                      TEXT
        )""",
    ],
    "dma_status": [
        "DROP TABLE IF EXISTS dma_status CASCADE",
        """CREATE TABLE dma_status (
            dma_code                    TEXT PRIMARY KEY,
            rag_status                  TEXT,
            avg_pressure                DOUBLE PRECISION,
            min_pressure                DOUBLE PRECISION,
            sensor_count                INTEGER,
            property_count              INTEGER,
            sensitive_premises_count    INTEGER,
            has_active_incident         BOOLEAN,
            last_updated                TIMESTAMP
        )""",
    ],
    "dma_summary": [
        "DROP TABLE IF EXISTS dma_summary CASCADE",
        """CREATE TABLE dma_summary (
            dma_code                    TEXT PRIMARY KEY,
            dma_name                    TEXT,
            rag_status                  TEXT,
            avg_pressure                DOUBLE PRECISION,
            avg_flow                    DOUBLE PRECISION,
            property_count              INTEGER,
            sensor_count                INTEGER,
            sensitive_premises_count    INTEGER,
            feeding_reservoir_id        TEXT,
            reservoir_level_pct         DOUBLE PRECISION,
            reservoir_hours_remaining   DOUBLE PRECISION,
            active_incident_id          TEXT,
            active_complaints_count     INTEGER,
            last_updated                TIMESTAMP
        )""",
    ],
    "anomaly_scores": [
        "DROP TABLE IF EXISTS anomaly_scores CASCADE",
        """CREATE TABLE anomaly_scores (
            sensor_id       TEXT,
            timestamp       TIMESTAMP,
            anomaly_sigma   DOUBLE PRECISION,
            baseline_value  DOUBLE PRECISION,
            actual_value    DOUBLE PRECISION,
            is_anomaly      BOOLEAN,
            scoring_method  TEXT,
            PRIMARY KEY (sensor_id, timestamp, scoring_method)
        )""",
    ],
    "fact_telemetry": [
        "DROP TABLE IF EXISTS fact_telemetry CASCADE",
        """CREATE TABLE fact_telemetry (
            sensor_id           TEXT,
            timestamp           TIMESTAMP,
            sensor_type         TEXT,
            value               DOUBLE PRECISION,
            total_head_pressure DOUBLE PRECISION,
            flow_rate           DOUBLE PRECISION,
            quality_flag        TEXT,
            PRIMARY KEY (sensor_id, timestamp)
        )""",
    ],
}

# Execute DDL
for i, (table_name, stmts) in enumerate(DDL_STATEMENTS.items(), 1):
    print(f"Creating table {i}/15: {table_name} ...")
    for stmt in stmts:
        execute_lakebase_sql(stmt)
    print(f"  OK")

print("\nAll 15 Lakebase tables created.")

# COMMAND ----------

# -- GiST spatial indexes for geometry columns ----------------------------------

SPATIAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dim_dma_geom ON dim_dma USING GIST(geom)",
    "CREATE INDEX IF NOT EXISTS idx_dim_sensor_geom ON dim_sensor USING GIST(geom)",
    "CREATE INDEX IF NOT EXISTS idx_dim_properties_geom ON dim_properties USING GIST(geom)",
    "CREATE INDEX IF NOT EXISTS idx_dim_assets_geom ON dim_assets USING GIST(geom)",
    "CREATE INDEX IF NOT EXISTS idx_dim_reservoirs_geom ON dim_reservoirs USING GIST(geom)",
    # Performance indexes for common query patterns
    "CREATE INDEX IF NOT EXISTS idx_fact_telemetry_ts ON fact_telemetry(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_scores_ts ON anomaly_scores(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_incident_events_incident ON incident_events(incident_id)",
    "CREATE INDEX IF NOT EXISTS idx_communications_log_incident ON communications_log(incident_id)",
]

for idx_sql in SPATIAL_INDEXES:
    idx_name = idx_sql.split("IF NOT EXISTS ")[-1].split(" ON")[0]
    print(f"Creating index: {idx_name} ...")
    execute_lakebase_sql(idx_sql)
    print(f"  OK")

print("\nAll spatial and performance indexes created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sync Delta Tables → Lakebase
# MAGIC
# MAGIC For each table:
# MAGIC 1. Read the Delta source table with Spark
# MAGIC 2. TRUNCATE the Lakebase target (full refresh)
# MAGIC 3. INSERT rows in batches of ~1000
# MAGIC 4. For geometry columns: use `ST_GeomFromText(geometry_wkt, 4326)` in the INSERT

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timedelta

DEMO_TIMESTAMP = datetime(2026, 4, 7, 5, 30, 0)


def escape_value(val):
    """Escape a Python value for PostgreSQL insertion."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, datetime):
        return f"'{val.isoformat()}'"
    # String — escape single quotes
    s = str(val).replace("'", "''")
    return f"'{s}'"


def build_insert_sql(table_name: str, columns: list, rows: list, geom_col_index: int = None):
    """
    Build a batch INSERT statement.
    If geom_col_index is set, that column's value is wrapped in ST_GeomFromText(..., 4326).
    """
    if not rows:
        return None

    col_names = ", ".join(columns)
    value_rows = []
    for row in rows:
        vals = []
        for i, val in enumerate(row):
            if i == geom_col_index and val is not None:
                wkt = str(val).replace("'", "''")
                vals.append(f"ST_GeomFromText('{wkt}', 4326)")
            else:
                vals.append(escape_value(val))
        value_rows.append(f"({', '.join(vals)})")

    return f"INSERT INTO {table_name} ({col_names}) VALUES {', '.join(value_rows)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync helper function

# COMMAND ----------

def sync_table(
    lakebase_table: str,
    delta_source: str,
    has_geometry: bool,
    geometry_type: str,
    row_filter: str,
):
    """
    Read a Delta table, truncate the Lakebase target, and batch-insert all rows.

    For spatial tables the geometry_wkt column is converted to a PostGIS geom via
    ST_GeomFromText. The original geometry_wkt TEXT column is also preserved so the
    app can access raw WKT if needed.
    """
    print(f"\n{'='*70}")
    print(f"Syncing: {delta_source} -> Lakebase.{lakebase_table}")
    print(f"{'='*70}")

    # 1. Read Delta source
    df = spark.table(delta_source)

    # Apply filters
    if row_filter == "last_7_days":
        cutoff = DEMO_TIMESTAMP - timedelta(days=7)
        df = df.filter(F.col("timestamp") >= F.lit(cutoff))
        print(f"  Filter: last 7 days (since {cutoff})")

    row_count = df.count()
    print(f"  Source rows: {row_count:,}")

    if row_count == 0:
        print(f"  SKIP -- no rows to sync.")
        return 0

    # 2. Determine columns for Lakebase INSERT.
    # For spatial tables: source columns + `geom` populated from geometry_wkt.
    source_cols = df.columns

    if has_geometry:
        lakebase_cols = source_cols + ["geom"]
        geom_col_index = len(lakebase_cols) - 1
    else:
        lakebase_cols = source_cols
        geom_col_index = None

    # 3. TRUNCATE Lakebase table
    execute_lakebase_sql_with_retry(f"TRUNCATE TABLE {lakebase_table}")
    print(f"  Truncated Lakebase table.")

    # 4. Collect data and batch insert
    rows = df.collect()
    inserted = 0

    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start : batch_start + BATCH_SIZE]
        batch_data = []
        for row in batch:
            row_vals = [row[c] for c in source_cols]
            if has_geometry:
                # Append geometry_wkt value for ST_GeomFromText conversion
                row_vals.append(row["geometry_wkt"])
            batch_data.append(row_vals)

        insert_sql = build_insert_sql(
            lakebase_table, lakebase_cols, batch_data, geom_col_index
        )
        if insert_sql:
            execute_lakebase_sql_with_retry(insert_sql)
            inserted += len(batch)
            if inserted % 5000 == 0 or inserted == len(rows):
                print(f"  Inserted: {inserted:,} / {row_count:,}")

    print(f"  Sync complete: {inserted:,} rows inserted.")
    return inserted

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run sync for all 15 tables

# COMMAND ----------

sync_results = {}
total_start = time.time()

for lakebase_table, delta_source, has_geom, geom_type, row_filter in TABLE_SYNC_CONFIG:
    try:
        count = sync_table(lakebase_table, delta_source, has_geom, geom_type, row_filter)
        sync_results[lakebase_table] = {"status": "OK", "rows": count}
    except Exception as e:
        print(f"\n  ERROR syncing {lakebase_table}: {e}")
        sync_results[lakebase_table] = {"status": "FAILED", "error": str(e)}

total_elapsed = time.time() - total_start
print(f"\n{'='*70}")
print(f"All syncs completed in {total_elapsed:.1f}s")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verification — Row Counts
# MAGIC
# MAGIC Confirm every Lakebase table has the expected data by running `SELECT COUNT(*)` against each.

# COMMAND ----------

print(f"{'Table':<30} {'Lakebase Rows':>15}  {'Status':>10}")
print("-" * 60)

all_ok = True
for lakebase_table, _, _, _, _ in TABLE_SYNC_CONFIG:
    try:
        result = execute_lakebase_sql(f"SELECT COUNT(*) AS cnt FROM {lakebase_table}")
        # Parse count from response (format depends on SDK version)
        if isinstance(result, dict) and "data" in result:
            count = result["data"][0][0]
        elif isinstance(result, dict) and "rows" in result:
            count = result["rows"][0][0]
        else:
            count = result
        status = "OK" if int(str(count)) > 0 else "EMPTY"
        if status == "EMPTY":
            all_ok = False
        print(f"{lakebase_table:<30} {str(count):>15}  {status:>10}")
    except Exception as e:
        all_ok = False
        print(f"{lakebase_table:<30} {'ERROR':>15}  {str(e)[:30]:>10}")

print("-" * 60)
if all_ok:
    print("All Lakebase tables populated successfully.")
else:
    print("WARNING: Some tables are empty or had errors. Check sync output above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify PostGIS geometries

# COMMAND ----------

SPATIAL_TABLES = ["dim_dma", "dim_sensor", "dim_properties", "dim_assets", "dim_reservoirs"]

print("PostGIS geometry validation:")
print(f"{'Table':<20} {'Valid Geom (sample 5)':>25}")
print("-" * 50)

for table in SPATIAL_TABLES:
    try:
        result = execute_lakebase_sql(
            f"""SELECT COUNT(*) FROM (
                SELECT ST_IsValid(geom) AS v
                FROM {table}
                WHERE geom IS NOT NULL
                LIMIT 5
            ) sub WHERE v = true"""
        )
        if isinstance(result, dict) and "data" in result:
            valid_count = result["data"][0][0]
        elif isinstance(result, dict) and "rows" in result:
            valid_count = result["rows"][0][0]
        else:
            valid_count = result
        print(f"{table:<20} {str(valid_count):>25}")
    except Exception as e:
        print(f"{table:<20} {'ERROR: ' + str(e)[:30]:>25}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify spatial query performance (sample)

# COMMAND ----------

# Sample query: find DMAs containing a point near Crystal Palace (~DEMO_DMA_01)
test_query = """
SELECT dma_code, dma_name
FROM dim_dma
WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(-0.07, 51.42), 4326))
LIMIT 5
"""
result = execute_lakebase_sql(test_query)
print("Spatial query test -- DMAs containing point (-0.07, 51.42):")
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Step | Description | Status |
# MAGIC |------|-------------|--------|
# MAGIC | 1 | PostGIS extension created | Done |
# MAGIC | 2 | 15 Lakebase tables created with DDL | Done |
# MAGIC | 3 | GiST spatial indexes on 5 geometry tables | Done |
# MAGIC | 4 | Full sync from Delta Gold/Silver to Lakebase | Done |
# MAGIC | 5 | Row count verification | Done |
# MAGIC | 6 | PostGIS geometry validation | Done |
# MAGIC
# MAGIC **Next step:** Deploy the Databricks App (`app/`) which reads from Lakebase via the Data API
# MAGIC for sub-second rendering of maps, charts, and incident data.
