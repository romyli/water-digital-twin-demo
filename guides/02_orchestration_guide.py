# Databricks notebook source

# MAGIC %md
# MAGIC # Orchestration Guide
# MAGIC 
# MAGIC **Water Utilities -- Digital Twin Demo**
# MAGIC 
# MAGIC Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
# MAGIC CLI profile: `adb-98`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC This guide creates a Databricks Workflow that runs the full data pipeline end-to-end. The DAG has 9 tasks with parallelism where dependencies allow.
# MAGIC 
# MAGIC ### Task Dependency DAG
# MAGIC 
# MAGIC ```
# MAGIC 01_schema_ddl
# MAGIC     |
# MAGIC     v
# MAGIC 02_data_gen_geography
# MAGIC     |
# MAGIC     v
# MAGIC 03_data_gen_dimensions
# MAGIC     |
# MAGIC     +-----------+-----------+
# MAGIC     |                       |
# MAGIC     v                       v
# MAGIC 04_data_gen_facts     05_data_gen_incidents
# MAGIC     |                       |
# MAGIC     +-----------+-----------+
# MAGIC                 |
# MAGIC                 v
# MAGIC           06_sdp_pipeline
# MAGIC                 |
# MAGIC                 v
# MAGIC         07_anomaly_scoring
# MAGIC                 |
# MAGIC         +-------+-------+
# MAGIC         |               |
# MAGIC         v               v
# MAGIC   08_metric_views  09_lakebase_sync
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Create the Workflow
# MAGIC 
# MAGIC 1. In the workspace sidebar, navigate to **Workflows > Create Job**.
# MAGIC 2. Set the job name: **Water Digital Twin -- Full Pipeline**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Configure Shared Cluster
# MAGIC 
# MAGIC All tasks share a single job cluster to reduce spin-up time and cost.
# MAGIC 
# MAGIC 1. Under **Job clusters**, click **Add a job cluster**.
# MAGIC 2. Configure:
# MAGIC    - **Name:** `pipeline-cluster`
# MAGIC    - **Node type:** `Standard_DS3_v2`
# MAGIC    - **Workers:** Min `2`, Max `4` (autoscaling)
# MAGIC    - **Databricks Runtime:** `15.4 LTS`
# MAGIC    - **Spark config (optional):**
# MAGIC      ```
# MAGIC      spark.sql.session.timeZone UTC
# MAGIC      ```
# MAGIC 3. Click **Confirm**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Add Tasks
# MAGIC 
# MAGIC Add each task below. For every task, set **Cluster** to `pipeline-cluster` (the shared job cluster).
# MAGIC 
# MAGIC ### Task 1: `01_schema_ddl`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `01_schema_ddl` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/01_schema_ddl` |
# MAGIC | Depends on | _(none -- root task)_ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: `02_data_gen_geography`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `02_data_gen_geography` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/02_data_gen_geography` |
# MAGIC | Depends on | `01_schema_ddl` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: `03_data_gen_dimensions`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `03_data_gen_dimensions` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/03_data_gen_dimensions` |
# MAGIC | Depends on | `02_data_gen_geography` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: `04_data_gen_facts`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `04_data_gen_facts` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/04_data_gen_facts` |
# MAGIC | Depends on | `03_data_gen_dimensions` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: `05_data_gen_incidents`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `05_data_gen_incidents` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/05_data_gen_incidents` |
# MAGIC | Depends on | `03_data_gen_dimensions` |
# MAGIC 
# MAGIC > **Note:** Tasks 04 and 05 run **in parallel** -- both depend only on Task 03.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: `06_sdp_pipeline`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `06_sdp_pipeline` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/06_sdp_pipeline` |
# MAGIC | Depends on | `04_data_gen_facts`, `05_data_gen_incidents` |
# MAGIC 
# MAGIC > **Note:** This task waits for **both** parallel branches to complete.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: `07_anomaly_scoring`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `07_anomaly_scoring` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/07_anomaly_scoring` |
# MAGIC | Depends on | `06_sdp_pipeline` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: `08_metric_views`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `08_metric_views` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/08_metric_views` |
# MAGIC | Depends on | `07_anomaly_scoring` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9: `09_lakebase_sync`
# MAGIC 
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `09_lakebase_sync` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `/Workspace/water-digital-twin/notebooks/09_lakebase_sync` |
# MAGIC | Depends on | `07_anomaly_scoring` |
# MAGIC 
# MAGIC > **Note:** Tasks 08 and 09 run **in parallel** -- both depend only on Task 07.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Run the Workflow
# MAGIC 
# MAGIC 1. Click **Run now** to trigger the full pipeline.
# MAGIC 2. Expected total runtime: **8-15 minutes** depending on cluster start time.
# MAGIC 3. Monitor progress in the Workflow run view -- verify all 9 tasks complete with status **Succeeded**.
# MAGIC 
# MAGIC ### Troubleshooting
# MAGIC 
# MAGIC | Symptom | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `01_schema_ddl` fails with permission error | Catalog not created | Run Part C of [01 -- Workspace Setup Guide](01_workspace_setup_guide.md) |
# MAGIC | Spatial SQL errors in `02_data_gen_geography` | Preview not enabled | Enable Spatial SQL in Admin Console (Part A1) |
# MAGIC | `08_metric_views` fails | Metric Views preview not enabled | Enable in Admin Console (Part A2) |
# MAGIC | `09_lakebase_sync` fails with connection error | Lakebase not provisioned | Complete Part B of [01 -- Workspace Setup Guide](01_workspace_setup_guide.md) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC 
# MAGIC Proceed to [03 -- Genie Operator Guide](03_genie_operator_guide.md) to create the operator Genie Space.
