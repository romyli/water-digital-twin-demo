# Water Digital Twin Demo -- Runbook

## Overview

This demo showcases a **water utility digital twin** built on the Databricks platform. It simulates a realistic operational scenario for **Water Utilities**, covering real-time telemetry ingestion, anomaly detection, incident management, regulatory reporting, and a full-stack operational app -- all powered by Unity Catalog, Mosaic AI, Lakebase, and Databricks Apps.

The demo is designed for a live 45-60 minute walkthrough with audiences ranging from field operators to C-suite executives.

---

## Prerequisites

| Requirement | Detail |
|---|---|
| **Workspace URL** | `https://adb-984752964297111.11.azuredatabricks.net/` |
| **CLI Profile** | `adb-98` |
| **Databricks CLI** | v0.230+ installed and configured |
| **Unity Catalog** | Catalog `water_digital_twin` with schemas: `bronze`, `silver`, `gold` |
| **Preview Features** | See `guides/01_workspace_setup_guide.md` for required preview enrollments |
| **Lakebase** | Postgres instance provisioned and accessible from the workspace |
| **Compute** | A running SQL warehouse (for SQL notebooks) and a cluster (for Python notebooks) |

---

## Setup Steps

Run these steps in order. Each step depends on the prior one completing successfully.

| # | Action | Path / Command |
|---|---|---|
| 1 | **Workspace & preview setup** | Follow `guides/01_workspace_setup_guide.md` -- enable preview features and provision Lakebase |
| 2 | **Create schemas & tables** | Run `notebooks/01_schema_ddl.sql` -- creates all Unity Catalog tables in `water_digital_twin.{bronze,silver,gold}` |
| 3 | **Generate geography data** | Run `notebooks/02_data_gen_geography.py` -- creates DMA polygons and geographic boundaries |
| 4 | **Generate dimension data** | Run `notebooks/03_data_gen_dimensions.py` -- creates sensors, properties, assets (pumps, valves, reservoirs, trunk mains) |
| 5 | **Generate fact data** | Run `notebooks/04_data_gen_facts.py` -- creates telemetry readings, customer complaints |
| 6 | **Generate incident data** | Run `notebooks/05_data_gen_incidents.py` -- creates incidents, event logs, comms logs, playbooks |
| 7 | **Run SDP pipeline** | Run `notebooks/06_sdp_pipeline.py` -- transforms Bronze to Silver to Gold (optional if data gen writes directly to Gold) |
| 8 | **Run anomaly scoring** | Run `notebooks/07_anomaly_scoring.py` -- computes anomaly sigma scores, RAG status, DMA status rollup |
| 9 | **Create metric views** | Run `notebooks/08_metric_views.sql` -- creates metric views for Genie and dashboards |
| 10 | **Sync to Lakebase** | Run `notebooks/09_lakebase_sync.py` -- syncs Gold tables to Lakebase Postgres for app serving |
| 11 | **Deploy the app** | `databricks apps deploy water-digital-twin --source-code-path ./app --profile adb-98` |
| 12 | **Configure Genie spaces** | Follow `guides/03_genie_operator_guide.md` and `guides/04_genie_executive_guide.md` |
| 13 | **Set up dashboards** | Follow `guides/05_dashboard_guide.md` |

---

## Demo Preparation

**10 minutes before the demo:**

1. Run the health check script: `scripts/demo_health_check.py`
   - Confirm **22/22 checks pass**
   - If any check fails, run `scripts/reset_demo.py` (with `--dry-run` first) to re-seed data
2. Open the app in a browser and verify it loads
3. Open the Genie spaces and confirm they respond to a test query
4. Pre-load any browser tabs you will need during the walkthrough
5. Set the demo timestamp context: **2026-04-07 05:30:00** (shift handover time)

---

## Asset Inventory

### Notebooks

| File | Description |
|---|---|
| `notebooks/01_schema_ddl.sql` | DDL for all Unity Catalog tables across bronze, silver, gold schemas |
| `notebooks/02_data_gen_geography.py` | Generates DMA polygons and geographic reference data |
| `notebooks/03_data_gen_dimensions.py` | Generates dimension tables: sensors, properties, assets |
| `notebooks/04_data_gen_facts.py` | Generates fact tables: telemetry readings, customer complaints |
| `notebooks/05_data_gen_incidents.py` | Generates incident records, event logs, comms logs, playbooks |
| `notebooks/06_sdp_pipeline.py` | SDP pipeline: Bronze to Silver to Gold transformations |
| `notebooks/07_anomaly_scoring.py` | Anomaly detection, sigma scoring, RAG status computation |
| `notebooks/08_metric_views.sql` | Metric views for Genie and dashboards |
| `notebooks/09_lakebase_sync.py` | Syncs Gold tables to Lakebase Postgres |

### Scripts

| File | Description |
|---|---|
| `scripts/demo_health_check.py` | Runs 22 verification queries with pass/fail reporting |
| `scripts/reset_demo.py` | Drops and re-seeds all demo data (supports `--dry-run`) |

### Guides

| File | Description |
|---|---|
| `guides/01_workspace_setup_guide.md` | Workspace configuration, preview features, Lakebase provisioning |
| `guides/03_genie_operator_guide.md` | Setting up the Network Operations Genie Space |
| `guides/04_genie_executive_guide.md` | Setting up the Executive Genie Space |
| `guides/05_dashboard_guide.md` | Dashboard creation and configuration |

### App

| File | Description |
|---|---|
| `app/` | Full-stack Databricks App source code (deployed via `databricks apps deploy`) |

---

## Architecture

```
Data Flow:

  OSI PI (SCADA)          External Sources
       |                       |
       v                       v
  +-----------+         +-----------+
  |  BRONZE   |         |  BRONZE   |
  | raw_telem |         | complaints|
  +-----------+         +-----------+
       |                       |
       v                       v
  +-----------+         +-----------+
  |  SILVER   |         |  SILVER   |
  | cleansed  |         | validated |
  +-----------+         +-----------+
       |                       |
       +----------+------------+
                  |
                  v
            +-----------+
            |   GOLD    |
            | dma_status|
            | anomaly   |
            | incidents |
            +-----------+
                  |
         +--------+--------+
         |                 |
         v                 v
    +---------+      +----------+
    | LAKEBASE|      |  METRIC  |
    | Postgres|      |  VIEWS   |
    +---------+      +----------+
         |                 |
         v                 v
    +---------+      +----------+
    |   APP   |      |  GENIE   |
    | (React/ |      |  SPACES  |
    | FastAPI)|      +----------+
    +---------+            |
                           v
                    +----------+
                    |DASHBOARDS|
                    +----------+
```

**Key components:**

- **Unity Catalog** (`water_digital_twin`): All tables governed under a single catalog with bronze/silver/gold schemas
- **Mosaic AI**: Powers anomaly detection, shift handover narrative generation, and forecast models
- **Lakebase**: Low-latency Postgres serving layer for the operational app
- **Databricks Apps**: Full-stack app for operators and managers
- **Genie Spaces**: Natural language query interface for operators and executives

---

## Troubleshooting

### Common Issues

| Issue | Cause | Fix |
|---|---|---|
| Health check shows RED DMA as GREEN | Anomaly scoring not run or data gen timestamps wrong | Re-run `notebooks/07_anomaly_scoring.py` |
| App returns 500 error | Lakebase sync incomplete or Lakebase not provisioned | Check Lakebase status; re-run `notebooks/09_lakebase_sync.py` |
| Genie returns "no data" | Metric views not created or warehouse not running | Run `notebooks/08_metric_views.sql`; start warehouse |
| `databricks apps deploy` fails | CLI profile misconfigured or app already exists | Verify `databricks auth profiles`; check `databricks apps list --profile adb-98` |
| Complaints count too low | Data gen facts notebook did not complete | Re-run `notebooks/04_data_gen_facts.py` then `notebooks/05_data_gen_incidents.py` |
| Schema not found | DDL notebook not run or wrong catalog | Run `notebooks/01_schema_ddl.sql`; verify `USE CATALOG water_digital_twin` |
| Anomaly sigma is 0 | Telemetry data missing or anomaly notebook error | Check `silver` telemetry tables; re-run `notebooks/07_anomaly_scoring.py` |
| Reset script times out | Large data volumes or slow cluster | Use a larger cluster; check for concurrent jobs |
| DMA polygons missing on map | Geography notebook not run | Run `notebooks/02_data_gen_geography.py` |
| Health check shows < 500 DMAs | Incomplete geography generation | Re-run `notebooks/02_data_gen_geography.py` then downstream notebooks |

### Reset Procedure

If the demo is in a broken state, run the full reset:

```bash
# 1. Verify what will be deleted (dry run)
# Run scripts/reset_demo.py with dry_run widget set to "true"

# 2. Execute the reset
# Set dry_run widget to "false" and run scripts/reset_demo.py

# 3. Verify
# Run scripts/demo_health_check.py -- expect 22/22 passed
```

Target reset time: **< 15 minutes** on a standard cluster.
