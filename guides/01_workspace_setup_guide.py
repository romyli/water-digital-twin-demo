# Databricks notebook source

# MAGIC %md
# MAGIC # Workspace Setup Guide
# MAGIC 
# MAGIC **Water Utilities -- Digital Twin Demo**
# MAGIC 
# MAGIC Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
# MAGIC CLI profile: `adb-98`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A -- Enable Preview Features
# MAGIC 
# MAGIC Three preview features must be enabled before running the pipeline.
# MAGIC 
# MAGIC ### A1. Spatial SQL (Public Preview)
# MAGIC 
# MAGIC Spatial SQL adds geometry types and spatial functions (ST_Point, ST_Distance, ST_Buffer, etc.) to Databricks SQL.
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC 1. Open the workspace Admin Console: **Settings > Admin Console**.
# MAGIC 2. Navigate to **Previews**.
# MAGIC 3. Find **Spatial SQL** and toggle it **On**.
# MAGIC 4. Click **Save**.
# MAGIC 
# MAGIC **Verification:**
# MAGIC 
# MAGIC Run the following in a SQL warehouse to confirm spatial functions are available:
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT ST_Point(-0.05, 51.45) AS test_point;
# MAGIC ```
# MAGIC 
# MAGIC Expected: Returns a point geometry object without errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Metric Views (Public Preview)
# MAGIC 
# MAGIC Metric Views provide reusable, governed metric definitions in Unity Catalog that Genie and dashboards can consume.
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC 1. Open the workspace Admin Console: **Settings > Admin Console**.
# MAGIC 2. Navigate to **Previews**.
# MAGIC 3. Find **Metric Views** and toggle it **On**.
# MAGIC 4. Click **Save**.
# MAGIC 
# MAGIC **Verification:**
# MAGIC 
# MAGIC Run the following to confirm metric view DDL is supported:
# MAGIC 
# MAGIC ```sql
# MAGIC CREATE METRIC VIEW water_digital_twin.gold.__test_mv
# MAGIC AS SELECT 1 AS dummy_metric, CURRENT_DATE() AS date_col
# MAGIC FROM VALUES (1);
# MAGIC 
# MAGIC -- If successful, clean up:
# MAGIC DROP METRIC VIEW water_digital_twin.gold.__test_mv;
# MAGIC ```
# MAGIC 
# MAGIC Expected: Both statements succeed without errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. ai_forecast() (Public Preview)
# MAGIC 
# MAGIC `ai_forecast()` is a built-in table-valued function that produces time-series forecasts directly in SQL.
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC 1. Open the workspace Admin Console: **Settings > Admin Console**.
# MAGIC 2. Navigate to **Previews**.
# MAGIC 3. Find **AI Functions** (or **ai_forecast**) and toggle it **On**.
# MAGIC 4. Click **Save**.
# MAGIC 
# MAGIC **Verification:**
# MAGIC 
# MAGIC Run the following to confirm the function is available:
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT * FROM ai_forecast(
# MAGIC   (SELECT DATE '2026-01-01' + INTERVAL x DAY AS ds,
# MAGIC           x * 1.0 AS y
# MAGIC    FROM VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) AS t(x)),
# MAGIC   horizon => 3
# MAGIC );
# MAGIC ```
# MAGIC 
# MAGIC Expected: Returns 3 rows of forecasted values without errors.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B -- Create Lakebase Instance
# MAGIC 
# MAGIC Lakebase provides a managed Postgres-compatible database for serving low-latency operational queries from the digital twin.
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC 1. In the workspace sidebar, navigate to **Catalog > Lakebase**.
# MAGIC 2. Click **Create project**.
# MAGIC 3. Set the project name: `water-digital-twin-lakebase`.
# MAGIC 4. Select the default compute options (or adjust as needed for demo sizing).
# MAGIC 5. Click **Create**.
# MAGIC 6. Wait for provisioning to complete (typically 3-5 minutes).
# MAGIC 7. Once provisioned, note the **Endpoint URL** -- this will be used in notebook `09_lakebase_sync`.
# MAGIC 
# MAGIC **Verification:**
# MAGIC 
# MAGIC Confirm the project appears in the Lakebase section with status **Running**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C -- Unity Catalog Setup
# MAGIC 
# MAGIC The demo uses a three-layer medallion architecture within a single catalog.
# MAGIC 
# MAGIC **Steps:**
# MAGIC 
# MAGIC ### C1. Create the Catalog
# MAGIC 
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS water_digital_twin
# MAGIC COMMENT 'Water Utilities Digital Twin Demo -- anonymized synthetic data';
# MAGIC ```
# MAGIC 
# MAGIC ### C2. Create Schemas
# MAGIC 
# MAGIC ```sql
# MAGIC USE CATALOG water_digital_twin;
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze
# MAGIC COMMENT 'Raw ingested telemetry and reference data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS silver
# MAGIC COMMENT 'Cleansed dimensions and fact tables';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS gold
# MAGIC COMMENT 'Aggregated views, metric views, anomaly scores, and incident data';
# MAGIC ```
# MAGIC 
# MAGIC **Verification:**
# MAGIC 
# MAGIC ```sql
# MAGIC SHOW SCHEMAS IN water_digital_twin;
# MAGIC ```
# MAGIC 
# MAGIC Expected: `bronze`, `silver`, `gold` (plus `default` and `information_schema`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC 
# MAGIC Proceed to [02 -- Orchestration Guide](02_orchestration_guide.md) to create the pipeline workflow.
