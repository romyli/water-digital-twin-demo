# Databricks notebook source

# MAGIC %md
# MAGIC # Water Digital Twin -- Reset & Re-Seed Demo
# MAGIC
# MAGIC Drops all tables and re-runs the full data generation pipeline to restore the demo
# MAGIC to a known-good state.
# MAGIC
# MAGIC **Target reset time:** < 15 minutes on a standard cluster.
# MAGIC
# MAGIC | Widget | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `dry_run` | `true` | When `true`, shows what *would* be deleted without executing any destructive operations |

# COMMAND ----------

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run (preview only)")

DRY_RUN = dbutils.widgets.get("dry_run") == "true"
CATALOG = "water_digital_twin"
SCHEMAS = ["bronze", "silver", "gold"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 -- Confirmation

# COMMAND ----------

if DRY_RUN:
    displayHTML(
        "<div style='padding:16px; background:#fff3cd; border:2px solid #ffc107; "
        "border-radius:8px; font-size:16px;'>"
        "<strong>&#x26A0;&#xFE0F; DRY RUN MODE</strong><br/>"
        "No data will be modified. Review the output below, then set <code>dry_run = false</code> "
        "and re-run to execute the reset."
        "</div>"
    )
else:
    displayHTML(
        "<div style='padding:16px; background:#ffcdd2; border:2px solid #c62828; "
        "border-radius:8px; font-size:16px;'>"
        "<strong>&#x1F6A8; LIVE MODE -- DATA WILL BE DELETED</strong><br/>"
        "All tables in <code>water_digital_twin.{bronze, silver, gold}</code> will be dropped "
        "and re-created from scratch."
        "</div>"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Drop All Tables

# COMMAND ----------

import time

start_time = time.time()

spark.sql(f"USE CATALOG {CATALOG}")

dropped_tables = []
for schema in SCHEMAS:
    tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}")
    table_names = [row.tableName for row in tables_df.collect()]

    for table_name in table_names:
        fqn = f"{CATALOG}.{schema}.{table_name}"
        if DRY_RUN:
            print(f"  [DRY RUN] Would drop: {fqn}")
        else:
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            print(f"  Dropped: {fqn}")
        dropped_tables.append(fqn)

    # Also drop views
    # Views show up in SHOW TABLES but need DROP VIEW
    # We already dropped them as tables; ignore errors for views
    if not DRY_RUN:
        try:
            views_df = spark.sql(f"SHOW VIEWS IN {CATALOG}.{schema}")
            for row in views_df.collect():
                vfqn = f"{CATALOG}.{schema}.{row.viewName}"
                spark.sql(f"DROP VIEW IF EXISTS {vfqn}")
                print(f"  Dropped view: {vfqn}")
        except Exception:
            pass  # SHOW VIEWS may not be supported on all runtimes

print(f"\nTotal objects identified for drop: {len(dropped_tables)}")

if DRY_RUN:
    print("\n--- DRY RUN COMPLETE. Set dry_run=false to execute. ---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Re-Run Data Generation Notebooks
# MAGIC
# MAGIC Notebooks are executed in dependency order via `dbutils.notebook.run()`.

# COMMAND ----------

NOTEBOOKS = [
    ("notebooks/01_schema_ddl",          "Create schemas & tables"),
    ("notebooks/02_data_gen_geography",   "Generate DMA polygons"),
    ("notebooks/03_data_gen_dimensions",  "Generate sensors, properties, assets"),
    ("notebooks/04_data_gen_facts",       "Generate telemetry, complaints"),
    ("notebooks/05_data_gen_incidents",   "Generate incidents, events, comms"),
]

TIMEOUT_SECONDS = 600  # 10 min per notebook

if DRY_RUN:
    print("Notebooks that would be executed:")
    for nb_path, desc in NOTEBOOKS:
        print(f"  [DRY RUN] {nb_path} -- {desc}")
else:
    for nb_path, desc in NOTEBOOKS:
        print(f"Running {nb_path} -- {desc} ...")
        nb_start = time.time()
        try:
            result = dbutils.notebook.run(f"../{nb_path}", TIMEOUT_SECONDS)
            elapsed = time.time() - nb_start
            print(f"  Completed in {elapsed:.0f}s (result: {result})")
        except Exception as e:
            elapsed = time.time() - nb_start
            print(f"  FAILED after {elapsed:.0f}s: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Run Pipeline, Anomaly Scoring & Metric Views

# COMMAND ----------

PIPELINE_NOTEBOOKS = [
    ("notebooks/06_sdp_pipeline",     "SDP Bronze->Silver->Gold pipeline"),
    ("notebooks/07_anomaly_scoring",  "Anomaly scoring & RAG status"),
    ("notebooks/08_metric_views",     "Metric views for Genie & dashboards"),
]

if DRY_RUN:
    print("Pipeline notebooks that would be executed:")
    for nb_path, desc in PIPELINE_NOTEBOOKS:
        print(f"  [DRY RUN] {nb_path} -- {desc}")
else:
    for nb_path, desc in PIPELINE_NOTEBOOKS:
        print(f"Running {nb_path} -- {desc} ...")
        nb_start = time.time()
        try:
            result = dbutils.notebook.run(f"../{nb_path}", TIMEOUT_SECONDS)
            elapsed = time.time() - nb_start
            print(f"  Completed in {elapsed:.0f}s (result: {result})")
        except Exception as e:
            elapsed = time.time() - nb_start
            print(f"  FAILED after {elapsed:.0f}s: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Sync to Lakebase

# COMMAND ----------

LAKEBASE_NOTEBOOK = "notebooks/09_lakebase_sync"

if DRY_RUN:
    print(f"  [DRY RUN] Would run: {LAKEBASE_NOTEBOOK}")
else:
    print(f"Running {LAKEBASE_NOTEBOOK} ...")
    nb_start = time.time()
    try:
        result = dbutils.notebook.run(f"../{LAKEBASE_NOTEBOOK}", TIMEOUT_SECONDS)
        elapsed = time.time() - nb_start
        print(f"  Completed in {elapsed:.0f}s (result: {result})")
    except Exception as e:
        elapsed = time.time() - nb_start
        print(f"  FAILED after {elapsed:.0f}s: {e}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Health Check Verification

# COMMAND ----------

if DRY_RUN:
    print("[DRY RUN] Would run health check queries here.")
    print("\n--- DRY RUN COMPLETE. No data was modified. ---")
    print("Set dry_run = false and re-run all cells to execute the full reset.")
else:
    print("Running inline health check queries ...")
    # Import the same check definitions used by demo_health_check.py
    # We inline a lightweight version here for self-contained execution.

    quick_checks = [
        ("DEMO_DMA_01 is RED",
         "SELECT rag_status FROM gold.dma_status WHERE dma_code='DEMO_DMA_01'",
         lambda r: r["rag_status"] == "RED"),
        ("500 DMAs (1R/2A/497G)",
         """SELECT COUNT(*) AS total,
                   SUM(CASE WHEN rag_status='RED' THEN 1 ELSE 0 END) AS red,
                   SUM(CASE WHEN rag_status='AMBER' THEN 1 ELSE 0 END) AS amber,
                   SUM(CASE WHEN rag_status='GREEN' THEN 1 ELSE 0 END) AS green
            FROM gold.dma_status""",
         lambda r: r["total"] == 500 and r["red"] == 1 and r["amber"] == 2 and r["green"] == 497),
        ("Active incident INC-2026-0407-001",
         "SELECT status, severity FROM gold.fact_incident WHERE incident_id='INC-2026-0407-001'",
         lambda r: r["status"] == "active" and r["severity"] == "high"),
        ("DEMO_SENSOR_01 anomaly > 3.0",
         """SELECT MAX(anomaly_sigma) AS max_sigma FROM gold.fact_anomaly_score
            WHERE sensor_id='DEMO_SENSOR_01' AND scored_at > '2026-04-07 02:03:00'""",
         lambda r: r["max_sigma"] is not None and r["max_sigma"] > 3.0),
        ("Playbook SOP-WN-042",
         "SELECT COUNT(*) AS cnt FROM gold.dim_playbook WHERE sop_code='SOP-WN-042'",
         lambda r: r["cnt"] >= 1),
    ]

    qc_pass = 0
    qc_total = len(quick_checks)
    for name, query, validator in quick_checks:
        try:
            row = spark.sql(query).collect()[0].asDict()
            ok = validator(row)
        except Exception as e:
            ok = False
            row = str(e)
        icon = "PASS" if ok else "FAIL"
        if ok:
            qc_pass += 1
        print(f"  [{icon}] {name}")

    total_elapsed = time.time() - start_time
    print(f"\nQuick check: {qc_pass}/{qc_total} passed")
    print(f"Total reset time: {total_elapsed / 60:.1f} minutes")

    if qc_pass == qc_total:
        displayHTML(
            "<div style='padding:16px; background:#c8e6c9; border:2px solid #2e7d32; "
            "border-radius:8px; font-size:16px;'>"
            f"<strong>&#x2705; Reset complete!</strong> {qc_pass}/{qc_total} quick checks passed. "
            f"Total time: {total_elapsed / 60:.1f} min.<br/>"
            "Run <code>scripts/demo_health_check</code> for the full 22-check verification."
            "</div>"
        )
    else:
        displayHTML(
            "<div style='padding:16px; background:#ffcdd2; border:2px solid #c62828; "
            "border-radius:8px; font-size:16px;'>"
            f"<strong>&#x274C; Reset completed with issues.</strong> {qc_pass}/{qc_total} quick checks passed.<br/>"
            "Run <code>scripts/demo_health_check</code> to identify specific failures."
            "</div>"
        )
