-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Water Digital Twin — Unity Catalog DDL
-- MAGIC
-- MAGIC This notebook creates the **catalog, schemas, and landing volume** for the Water Digital Twin demo.
-- MAGIC All statements are idempotent (`CREATE ... IF NOT EXISTS`).
-- MAGIC
-- MAGIC ### What this notebook creates
-- MAGIC - Catalog `water_digital_twin` with `bronze`, `silver`, `gold` schemas
-- MAGIC - **Landing volume** (`bronze.landing_zone`) — file drop zone for data generation
-- MAGIC
-- MAGIC ### What this notebook does NOT create
-- MAGIC - **Bronze tables** — created by the SDP pipeline via Auto Loader from landing zone files
-- MAGIC - **Silver tables** — created by the SDP pipeline via `@dp.materialized_view`
-- MAGIC - **Gold computed tables** — created by `07_anomaly_scoring` and `05_data_gen_incidents`

-- COMMAND ----------

-- ============================================================
-- CATALOG & SCHEMAS
-- ============================================================

CREATE CATALOG IF NOT EXISTS water_digital_twin
COMMENT 'Water utility digital twin demo — bronze/silver/gold medallion architecture';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.bronze
COMMENT 'Raw ingestion layer — data as-is from source systems';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.silver
COMMENT 'Curated layer — cleansed dimensions and facts';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.gold
COMMENT 'Consumption layer — aggregated views, incidents, and operational tables';

-- COMMAND ----------

-- ============================================================
-- LANDING VOLUME
-- ============================================================

CREATE VOLUME IF NOT EXISTS water_digital_twin.bronze.landing_zone
COMMENT 'File landing zone for data generation notebooks — JSON files ingested by SDP Auto Loader';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Next Steps
-- MAGIC
-- MAGIC With the catalog, schemas, and landing volume created, the pipeline execution order is:
-- MAGIC 1. **Data generation** (`02`–`05`) — writes JSON files to `bronze.landing_zone` volume
-- MAGIC 2. **SDP pipeline** (`06_sdp_pipeline`) — Auto Loader ingests files into Bronze, transforms to Silver and Gold
-- MAGIC 3. **Anomaly scoring** (`07_anomaly_scoring`) — creates and populates Gold computed tables
-- MAGIC 4. **Gold views & metric views** (`08_metric_views`) — creates Gold views (requires Silver tables to exist)
