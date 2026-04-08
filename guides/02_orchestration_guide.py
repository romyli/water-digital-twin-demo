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
# MAGIC This guide creates a Databricks Workflow that runs the full data pipeline end-to-end.
# MAGIC All notebook tasks use **serverless** compute. The SDP pipeline task uses its own serverless pipeline compute.
# MAGIC
# MAGIC ### Architecture
# MAGIC
# MAGIC ```
# MAGIC Data Gen Notebooks (01-05)          SDP Pipeline (06)                    Post-Pipeline (07-09)
# MAGIC ─────────────────────────          ──────────────────                   ─────────────────────
# MAGIC Write JSON files to Volume   →     Auto Loader → Bronze (streaming)    Anomaly scoring,
# MAGIC (landing_zone)                     Bronze → Silver (materialized)       metric views,
# MAGIC                                    Silver → Gold (materialized)         Lakebase sync
# MAGIC ```
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
# MAGIC ## Step 2 -- Compute Configuration
# MAGIC
# MAGIC All notebook tasks use **serverless compute** (no cluster configuration needed).
# MAGIC Simply leave the compute setting as the default — serverless is automatic.
# MAGIC
# MAGIC The SDP pipeline task (Task 6) uses its own serverless pipeline compute, configured in the pipeline settings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Add Tasks
# MAGIC
# MAGIC Add each task below. All notebook tasks run on **serverless** (default).
# MAGIC
# MAGIC ### Task 1: `01_schema_ddl`
# MAGIC
# MAGIC Creates catalog, schemas, and the landing zone volume.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `01_schema_ddl` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/01_schema_ddl` |
# MAGIC | Depends on | _(none -- root task)_ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2: `02_data_gen_geography`
# MAGIC
# MAGIC Downloads ONS ward boundaries and writes DMA/PMA JSON files to the landing zone volume.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `02_data_gen_geography` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/02_data_gen_geography` |
# MAGIC | Depends on | `01_schema_ddl` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3: `03_data_gen_dimensions`
# MAGIC
# MAGIC Generates sensors, properties, assets, reservoirs — writes JSON to landing zone.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `03_data_gen_dimensions` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/03_data_gen_dimensions` |
# MAGIC | Depends on | `02_data_gen_geography` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4: `04_data_gen_facts`
# MAGIC
# MAGIC Generates ~600K telemetry readings and complaints — writes JSON to landing zone.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `04_data_gen_facts` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/04_data_gen_facts` |
# MAGIC | Depends on | `03_data_gen_dimensions` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5: `05_data_gen_incidents`
# MAGIC
# MAGIC Generates incidents, event logs, communications — writes directly to gold tables.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `05_data_gen_incidents` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/05_data_gen_incidents` |
# MAGIC | Depends on | `03_data_gen_dimensions` |
# MAGIC
# MAGIC > **Note:** Tasks 04 and 05 run **in parallel** -- both depend only on Task 03.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6: `06_sdp_pipeline`
# MAGIC
# MAGIC SDP pipeline that runs the full medallion flow: Auto Loader ingests JSON files from
# MAGIC the landing zone volume into Bronze streaming tables, transforms to Silver materialized
# MAGIC views, then aggregates into Gold materialized views.
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `06_sdp_pipeline` |
# MAGIC | Type | **Pipeline** (not Notebook) |
# MAGIC | Pipeline | `Water Digital Twin — SDP Pipeline` |
# MAGIC | Depends on | `04_data_gen_facts`, `05_data_gen_incidents` |
# MAGIC
# MAGIC > **Important:** This is a **Pipeline task**, not a notebook task:
# MAGIC > 1. Set **Type** to **Pipeline**
# MAGIC > 2. Select the pipeline from the dropdown
# MAGIC > 3. The pipeline uses its own serverless compute (configured in pipeline settings with `"serverless": true`)
# MAGIC > 4. This task waits for **both** parallel branches (04 + 05) to complete before triggering

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 7: `07_anomaly_scoring`
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `07_anomaly_scoring` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/07_anomaly_scoring` |
# MAGIC | Depends on | `06_sdp_pipeline` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 8: `08_metric_views`
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `08_metric_views` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/08_metric_views` |
# MAGIC | Depends on | `07_anomaly_scoring` |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 9: `09_lakebase_sync`
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | Task name | `09_lakebase_sync` |
# MAGIC | Type | Notebook |
# MAGIC | Path | `notebooks/09_lakebase_sync` |
# MAGIC | Depends on | `07_anomaly_scoring` |
# MAGIC
# MAGIC > **Note:** Tasks 08 and 09 run **in parallel** -- both depend only on Task 07.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Run the Workflow
# MAGIC
# MAGIC 1. Click **Run now** to trigger the full pipeline.
# MAGIC 2. Expected total runtime: **10-20 minutes** (serverless spin-up is fast).
# MAGIC 3. Monitor progress in the Workflow run view -- verify all 9 tasks complete with status **Succeeded**.
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC | Symptom | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `01_schema_ddl` fails with permission error | Catalog not created | Run Part C of **01 -- Workspace Setup Guide** |
# MAGIC | `02_data_gen_geography` fails with HTTP error | ONS API timeout | Re-run the task (transient network issue) |
# MAGIC | Spatial SQL errors in `02` or `06` | Preview not enabled | Enable Spatial SQL in Admin Console (Part A1) |
# MAGIC | `06_sdp_pipeline` fails with `cloudFiles` error | Volume not created | Verify `landing_zone` volume exists in `bronze` schema |
# MAGIC | `08_metric_views` fails | Metric Views preview not enabled | Enable in Admin Console (Part A2) |
# MAGIC | `09_lakebase_sync` fails with connection error | Lakebase not provisioned | Complete Part B of **01 -- Workspace Setup Guide** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Proceed to **03 -- Genie Operator Guide** to create the operator Genie Space.
