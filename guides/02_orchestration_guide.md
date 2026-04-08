# Orchestration Guide

**Water Utilities -- Digital Twin Demo**

Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
CLI profile: `adb-98`

---

## Overview

This guide creates a Databricks Workflow that runs the full data pipeline end-to-end. The DAG has 9 tasks with parallelism where dependencies allow.

### Task Dependency DAG

```
01_schema_ddl
    |
    v
02_data_gen_geography
    |
    v
03_data_gen_dimensions
    |
    +-----------+-----------+
    |                       |
    v                       v
04_data_gen_facts     05_data_gen_incidents
    |                       |
    +-----------+-----------+
                |
                v
          06_sdp_pipeline
                |
                v
        07_anomaly_scoring
                |
        +-------+-------+
        |               |
        v               v
  08_metric_views  09_lakebase_sync
```

---

## Step 1 -- Create the Workflow

1. In the workspace sidebar, navigate to **Workflows > Create Job**.
2. Set the job name: **Water Digital Twin -- Full Pipeline**.

---

## Step 2 -- Configure Shared Cluster

All tasks share a single job cluster to reduce spin-up time and cost.

1. Under **Job clusters**, click **Add a job cluster**.
2. Configure:
   - **Name:** `pipeline-cluster`
   - **Node type:** `Standard_DS3_v2`
   - **Workers:** Min `2`, Max `4` (autoscaling)
   - **Databricks Runtime:** `15.4 LTS`
   - **Spark config (optional):**
     ```
     spark.sql.session.timeZone UTC
     ```
3. Click **Confirm**.

---

## Step 3 -- Add Tasks

Add each task below. For every task, set **Cluster** to `pipeline-cluster` (the shared job cluster).

### Task 1: `01_schema_ddl`

| Field | Value |
|---|---|
| Task name | `01_schema_ddl` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/01_schema_ddl` |
| Depends on | _(none -- root task)_ |

---

### Task 2: `02_data_gen_geography`

| Field | Value |
|---|---|
| Task name | `02_data_gen_geography` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/02_data_gen_geography` |
| Depends on | `01_schema_ddl` |

---

### Task 3: `03_data_gen_dimensions`

| Field | Value |
|---|---|
| Task name | `03_data_gen_dimensions` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/03_data_gen_dimensions` |
| Depends on | `02_data_gen_geography` |

---

### Task 4: `04_data_gen_facts`

| Field | Value |
|---|---|
| Task name | `04_data_gen_facts` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/04_data_gen_facts` |
| Depends on | `03_data_gen_dimensions` |

---

### Task 5: `05_data_gen_incidents`

| Field | Value |
|---|---|
| Task name | `05_data_gen_incidents` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/05_data_gen_incidents` |
| Depends on | `03_data_gen_dimensions` |

> **Note:** Tasks 04 and 05 run **in parallel** -- both depend only on Task 03.

---

### Task 6: `06_sdp_pipeline`

| Field | Value |
|---|---|
| Task name | `06_sdp_pipeline` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/06_sdp_pipeline` |
| Depends on | `04_data_gen_facts`, `05_data_gen_incidents` |

> **Note:** This task waits for **both** parallel branches to complete.

---

### Task 7: `07_anomaly_scoring`

| Field | Value |
|---|---|
| Task name | `07_anomaly_scoring` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/07_anomaly_scoring` |
| Depends on | `06_sdp_pipeline` |

---

### Task 8: `08_metric_views`

| Field | Value |
|---|---|
| Task name | `08_metric_views` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/08_metric_views` |
| Depends on | `07_anomaly_scoring` |

---

### Task 9: `09_lakebase_sync`

| Field | Value |
|---|---|
| Task name | `09_lakebase_sync` |
| Type | Notebook |
| Path | `/Workspace/water-digital-twin/notebooks/09_lakebase_sync` |
| Depends on | `07_anomaly_scoring` |

> **Note:** Tasks 08 and 09 run **in parallel** -- both depend only on Task 07.

---

## Step 4 -- Run the Workflow

1. Click **Run now** to trigger the full pipeline.
2. Expected total runtime: **8-15 minutes** depending on cluster start time.
3. Monitor progress in the Workflow run view -- verify all 9 tasks complete with status **Succeeded**.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `01_schema_ddl` fails with permission error | Catalog not created | Run Part C of [01 -- Workspace Setup Guide](01_workspace_setup_guide.md) |
| Spatial SQL errors in `02_data_gen_geography` | Preview not enabled | Enable Spatial SQL in Admin Console (Part A1) |
| `08_metric_views` fails | Metric Views preview not enabled | Enable in Admin Console (Part A2) |
| `09_lakebase_sync` fails with connection error | Lakebase not provisioned | Complete Part B of [01 -- Workspace Setup Guide](01_workspace_setup_guide.md) |

---

## Next Step

Proceed to [03 -- Genie Operator Guide](03_genie_operator_guide.md) to create the operator Genie Space.
