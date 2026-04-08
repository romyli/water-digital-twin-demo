# Workspace Setup Guide

**Water Utilities -- Digital Twin Demo**

Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
CLI profile: `adb-98`

---

## Part A -- Enable Preview Features

Three preview features must be enabled before running the pipeline.

### A1. Spatial SQL (Public Preview)

Spatial SQL adds geometry types and spatial functions (ST_Point, ST_Distance, ST_Buffer, etc.) to Databricks SQL.

**Steps:**

1. Open the workspace Admin Console: **Settings > Admin Console**.
2. Navigate to **Previews**.
3. Find **Spatial SQL** and toggle it **On**.
4. Click **Save**.

**Verification:**

Run the following in a SQL warehouse to confirm spatial functions are available:

```sql
SELECT ST_Point(-0.05, 51.45) AS test_point;
```

Expected: Returns a point geometry object without errors.

---

### A2. Metric Views (Public Preview)

Metric Views provide reusable, governed metric definitions in Unity Catalog that Genie and dashboards can consume.

**Steps:**

1. Open the workspace Admin Console: **Settings > Admin Console**.
2. Navigate to **Previews**.
3. Find **Metric Views** and toggle it **On**.
4. Click **Save**.

**Verification:**

Run the following to confirm metric view DDL is supported:

```sql
CREATE METRIC VIEW water_digital_twin.gold.__test_mv
AS SELECT 1 AS dummy_metric, CURRENT_DATE() AS date_col
FROM VALUES (1);

-- If successful, clean up:
DROP METRIC VIEW water_digital_twin.gold.__test_mv;
```

Expected: Both statements succeed without errors.

---

### A3. ai_forecast() (Public Preview)

`ai_forecast()` is a built-in table-valued function that produces time-series forecasts directly in SQL.

**Steps:**

1. Open the workspace Admin Console: **Settings > Admin Console**.
2. Navigate to **Previews**.
3. Find **AI Functions** (or **ai_forecast**) and toggle it **On**.
4. Click **Save**.

**Verification:**

Run the following to confirm the function is available:

```sql
SELECT * FROM ai_forecast(
  (SELECT DATE '2026-01-01' + INTERVAL x DAY AS ds,
          x * 1.0 AS y
   FROM VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) AS t(x)),
  horizon => 3
);
```

Expected: Returns 3 rows of forecasted values without errors.

---

## Part B -- Create Lakebase Instance

Lakebase provides a managed Postgres-compatible database for serving low-latency operational queries from the digital twin.

**Steps:**

1. In the workspace sidebar, navigate to **Catalog > Lakebase**.
2. Click **Create project**.
3. Set the project name: `water-digital-twin-lakebase`.
4. Select the default compute options (or adjust as needed for demo sizing).
5. Click **Create**.
6. Wait for provisioning to complete (typically 3-5 minutes).
7. Once provisioned, note the **Endpoint URL** -- this will be used in notebook `09_lakebase_sync`.

**Verification:**

Confirm the project appears in the Lakebase section with status **Running**.

---

## Part C -- Unity Catalog Setup

The demo uses a three-layer medallion architecture within a single catalog.

**Steps:**

### C1. Create the Catalog

```sql
CREATE CATALOG IF NOT EXISTS water_digital_twin
COMMENT 'Water Utilities Digital Twin Demo -- anonymized synthetic data';
```

### C2. Create Schemas

```sql
USE CATALOG water_digital_twin;

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingested telemetry and reference data';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleansed dimensions and fact tables';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Aggregated views, metric views, anomaly scores, and incident data';
```

**Verification:**

```sql
SHOW SCHEMAS IN water_digital_twin;
```

Expected: `bronze`, `silver`, `gold` (plus `default` and `information_schema`).

---

## Next Step

Proceed to [02 -- Orchestration Guide](02_orchestration_guide.md) to create the pipeline workflow.
