# Databricks notebook source

# MAGIC %md
# MAGIC # 09 — Delta → Lakebase Sync
# MAGIC
# MAGIC Syncs Silver + Gold Delta tables to Lakebase for sub-second app serving.
# MAGIC
# MAGIC **Approach:**
# MAGIC - Schema-driven DDL — reads actual Delta schema, no hardcoded columns
# MAGIC - Bulk loads via PostgreSQL COPY (100x+ faster than INSERT)
# MAGIC - Spatial tables get PostGIS `geom` column via `ST_GeomFromEWKB` with GiST index
# MAGIC
# MAGIC **Production alternative:** Use Databricks **Synced Tables** for managed replication.
# MAGIC For spatial tables, create intermediate views that serialize WKT → EWKB first.
# MAGIC See: [Geospatial Sync Guide](https://docs.google.com/document/d/1lwzR7cGLpLsK9pMh1qkIL1r2EB69Xh1P7bmM1c8dV4g)
# MAGIC
# MAGIC **Catalog:** `water_digital_twin`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import psycopg2
import io
import time
from pyspark.sql import functions as F
from datetime import datetime, timedelta

CATALOG = "water_digital_twin"
DEMO_TIMESTAMP = datetime(2026, 4, 7, 5, 30, 0)

# Lakebase Autoscaling project
LAKEBASE_ENDPOINT_PATH = "projects/water-digital-twin-demo/branches/production/endpoints/primary"

TABLES = [
    # (target_name, delta_source, has_geometry, filter)
    # Silver spatial
    ("dim_dma",                f"{CATALOG}.silver.dim_dma",                True,  None),
    ("dim_pma",                f"{CATALOG}.silver.dim_pma",                True,  None),
    ("dim_properties",         f"{CATALOG}.silver.dim_properties",         True,  None),
    ("dim_assets",             f"{CATALOG}.silver.dim_assets",             True,  None),
    # Silver non-spatial
    ("dim_sensor",             f"{CATALOG}.silver.dim_sensor",             False, None),
    ("dim_reservoirs",         f"{CATALOG}.silver.dim_reservoirs",         False, None),
    ("dim_asset_dma_feed",     f"{CATALOG}.silver.dim_asset_dma_feed",     False, None),
    ("dim_reservoir_dma_feed", f"{CATALOG}.silver.dim_reservoir_dma_feed", False, None),
    ("customer_complaints",    f"{CATALOG}.silver.customer_complaints",    False, None),
    # Gold
    ("dim_incidents",          f"{CATALOG}.gold.dim_incidents",            False, None),
    ("incident_events",        f"{CATALOG}.gold.incident_events",          False, None),
    ("communications_log",     f"{CATALOG}.gold.communications_log",       False, None),
    ("shift_handovers",        f"{CATALOG}.gold.shift_handovers",          False, None),
    ("dim_response_playbooks", f"{CATALOG}.gold.dim_response_playbooks",   False, None),
    ("comms_requests",         f"{CATALOG}.gold.comms_requests",           False, None),
    ("dma_status",             f"{CATALOG}.gold.dma_status",               False, None),
    ("dma_summary",            f"{CATALOG}.gold.dma_summary",              False, None),
    ("dma_rag_history",        f"{CATALOG}.gold.dma_rag_history",          False, None),
    ("anomaly_scores",         f"{CATALOG}.gold.anomaly_scores",           False, None),
    # Silver fact (last 7 days)
    ("fact_telemetry",         f"{CATALOG}.silver.fact_telemetry",         False, "last_7_days"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to Lakebase

# COMMAND ----------

# SDK handles auth automatically — generate a proper database JWT credential
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Resolve endpoint host
branch_path = LAKEBASE_ENDPOINT_PATH.rsplit("/endpoints/", 1)[0]
resp = w.api_client.do("GET", f"/api/2.0/postgres/{branch_path}/endpoints")
endpoint = [e for e in resp["endpoints"] if e["name"] == LAKEBASE_ENDPOINT_PATH][0]
host = endpoint["status"]["hosts"]["host"]

# Generate database credential (JWT — valid 60 min)
cred = w.api_client.do("POST", "/api/2.0/postgres/credentials",
                        body={"endpoint": LAKEBASE_ENDPOINT_PATH})
token = cred["token"]

# Get current user email
user = w.current_user.me()
email = user.user_name

print(f"Host: {host}")
print(f"User: {email}")

conn = psycopg2.connect(
    host=host, port=5432, dbname="databricks_postgres",
    user=email, password=token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT 1")
print(f"Lakebase connection OK: {cur.fetchone()}")

cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
print("PostGIS extension enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sync All Tables

# COMMAND ----------

# Spark type → Postgres type
SPARK_TO_PG = {
    "StringType": "TEXT", "DoubleType": "DOUBLE PRECISION", "FloatType": "DOUBLE PRECISION",
    "IntegerType": "INTEGER", "LongType": "BIGINT", "ShortType": "SMALLINT",
    "BooleanType": "BOOLEAN", "TimestampType": "TIMESTAMP", "TimestampNTZType": "TIMESTAMP",
    "DateType": "DATE", "DecimalType": "NUMERIC", "BinaryType": "BYTEA",
}


def copy_val(val):
    """Format a value for COPY TSV."""
    if val is None:
        return "\\N"
    if isinstance(val, bool):
        return "t" if val else "f"
    if isinstance(val, (int, float)):
        return "\\N" if val != val else str(val)  # NaN → NULL
    return str(val).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def sync_table(table_name, delta_source, has_geometry, row_filter):
    """Read Delta, auto-create Lakebase table, bulk COPY, add PostGIS geom if needed."""
    print(f"\n  {delta_source} → {table_name}")

    df = spark.table(delta_source)
    if row_filter == "last_7_days":
        df = df.filter(F.col("timestamp") >= F.lit(DEMO_TIMESTAMP - timedelta(days=7)))

    row_count = df.count()
    if row_count == 0:
        print("    SKIP — empty")
        return 0

    # DDL from actual schema
    cols = []
    for f in df.schema.fields:
        pg_type = SPARK_TO_PG.get(type(f.dataType).__name__, "TEXT")
        cols.append(f"    {f.name} {pg_type}")

    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
    cur.execute(f"CREATE TABLE {table_name} (\n{','.join(chr(10) + c for c in cols)}\n)")

    # Bulk COPY
    source_cols = df.columns
    rows = df.collect()
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(copy_val(row[c]) for c in source_cols) + "\n")
    buf.seek(0)
    cur.copy_from(buf, table_name, columns=source_cols, null="\\N")
    print(f"    COPY: {row_count:,} rows")

    # PostGIS geometry from WKT → EWKB + GiST index
    if has_geometry and "geometry_wkt" in source_cols:
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN geom GEOMETRY")
        cur.execute(f"UPDATE {table_name} SET geom = ST_GeomFromText(geometry_wkt, 4326) WHERE geometry_wkt IS NOT NULL")
        cur.execute(f"CREATE INDEX idx_{table_name}_geom ON {table_name} USING GIST(geom)")
        cur.execute(f"ANALYZE {table_name}")
        print(f"    PostGIS geom + GiST index added")

    return row_count

# COMMAND ----------

total_start = time.time()
results = {}

for table_name, delta_source, has_geom, row_filter in TABLES:
    try:
        count = sync_table(table_name, delta_source, has_geom, row_filter)
        results[table_name] = count
    except Exception as e:
        print(f"    ERROR: {e}")
        results[table_name] = -1

elapsed = time.time() - total_start
ok = sum(1 for v in results.values() if v >= 0)
print(f"\nDone: {ok}/{len(results)} tables synced in {elapsed:.0f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. App Write-Back Tables
# MAGIC
# MAGIC These tables are **Lakebase-only** — the app writes to them directly.
# MAGIC They are never synced from Delta, so re-running the sync won't wipe app data.
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `app_communications_log` | Operator comms entries added via the app |
# MAGIC | `app_playbook_action_log` | Playbook step decisions (Accept / Defer / N/A) |
# MAGIC | `app_comms_requests` | Proactive comms requests triggered from Regulatory view |

# COMMAND ----------

APP_TABLES_DDL = [
    """
    CREATE TABLE IF NOT EXISTS app_communications_log (
        incident_id   TEXT,
        channel       TEXT,
        recipient     TEXT,
        message       TEXT,
        sent_by       TEXT,
        sent_at       TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app_playbook_action_log (
        incident_id   TEXT,
        action_id     TEXT,
        decision      TEXT,
        note          TEXT,
        decided_at    TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app_comms_requests (
        incident_id      TEXT,
        channel          TEXT,
        message_template TEXT,
        target_audience  TEXT,
        created_at       TIMESTAMP
    )
    """,
]

for ddl in APP_TABLES_DDL:
    table_name = ddl.split("IF NOT EXISTS")[1].split("(")[0].strip()
    cur.execute(ddl)
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    print(f"  {table_name}: {cur.fetchone()[0]:,} rows (preserved)")

print("App write-back tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grant App Service Principal Access
# MAGIC
# MAGIC The Databricks App runs as a service principal that needs read/write access
# MAGIC to all synced and app write-back tables. The SP gets `CAN_CONNECT_AND_CREATE`
# MAGIC from the app resource config, but needs explicit table-level grants.

# COMMAND ----------

# Grant the app SP access to all tables
# The SP subject is its application (client) ID from the app config
APP_SP_CLIENT_ID = "a6e153ab-211a-47e2-be5b-09754cdd220e"

try:
    cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")

    # Create role for the SP if not exists
    cur.execute(f"SELECT databricks_create_role('{APP_SP_CLIENT_ID}', 'SERVICE_PRINCIPAL')")
    print(f"Created/ensured role for SP {APP_SP_CLIENT_ID}")

    # Grant connect
    cur.execute(f'GRANT CONNECT ON DATABASE "databricks_postgres" TO "{APP_SP_CLIENT_ID}"')

    # Grant on public schema (where all tables live)
    cur.execute(f'GRANT USAGE ON SCHEMA public TO "{APP_SP_CLIENT_ID}"')
    cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "{APP_SP_CLIENT_ID}"')
    cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{APP_SP_CLIENT_ID}"')

    print(f"Granted full table access to app SP on public schema")
except Exception as e:
    print(f"Permission grant error (may be OK if already granted): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification

# COMMAND ----------

print(f"{'Table':<35} {'Rows':>10}")
print("-" * 50)
all_tables = [t[0] for t in TABLES] + ["app_communications_log", "app_playbook_action_log", "app_comms_requests"]
for table_name in all_tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        print(f"{table_name:<35} {cur.fetchone()[0]:>10,}")
    except:
        print(f"{table_name:<35} {'ERROR':>10}")

# COMMAND ----------

# Spatial query test
try:
    cur.execute("""
        SELECT dma_code, dma_name FROM dim_dma
        WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(-0.07, 51.42), 4326))
        LIMIT 5
    """)
    print("Spatial query — DMAs containing (-0.07, 51.42):")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
except Exception as e:
    print(f"Spatial query: {e}")

# COMMAND ----------

cur.close()
conn.close()
print("Connection closed.")
