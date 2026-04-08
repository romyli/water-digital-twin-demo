# Databricks notebook source

# MAGIC %md
# MAGIC # Dashboard Guide
# MAGIC 
# MAGIC **Water Utilities -- Digital Twin Demo**
# MAGIC 
# MAGIC Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
# MAGIC CLI profile: `adb-98`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC This guide creates a **Lakeview dashboard** called **"Water Operations -- Executive View"** with two pages and three global filters. The dashboard provides executive-level visibility into active incidents, regulatory exposure, and AMP8 investment trends.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Create the Dashboard
# MAGIC 
# MAGIC 1. In the workspace sidebar, navigate to **Dashboards**.
# MAGIC 2. Click **Create Dashboard** (Lakeview).
# MAGIC 3. Set the name: **Water Operations -- Executive View**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Configure Global Filters
# MAGIC 
# MAGIC Add three dashboard-level filters that apply across all pages:
# MAGIC 
# MAGIC ### Filter 1: Time Range
# MAGIC 
# MAGIC - **Field:** `detected_ts` (from `dim_incidents`)
# MAGIC - **Type:** Date range picker
# MAGIC - **Default:** Last 30 days
# MAGIC 
# MAGIC ### Filter 2: DMA Code
# MAGIC 
# MAGIC - **Field:** `dma_code`
# MAGIC - **Type:** Multi-select dropdown
# MAGIC - **Source:** `SELECT DISTINCT dma_code FROM water_digital_twin.silver.dim_dma ORDER BY dma_code`
# MAGIC - **Default:** All
# MAGIC 
# MAGIC ### Filter 3: Incident Severity
# MAGIC 
# MAGIC - **Field:** `severity`
# MAGIC - **Type:** Multi-select dropdown
# MAGIC - **Values:** `critical`, `major`, `minor`
# MAGIC - **Default:** All

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 1: Incident Overview
# MAGIC 
# MAGIC This page has 8 tiles providing a real-time view of active incidents and regulatory exposure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.1: Active Incidents (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Count of currently active incidents.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS active_incidents
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE status = 'active';
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Large number, red highlight if > 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.2: Total Properties Affected (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Total properties currently affected by active incidents.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   SUM(properties_affected) AS total_properties_affected
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE status = 'active';
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Large number.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.3: Properties Exceeding 3-Hour Threshold (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Properties where supply interruption has exceeded the Ofwat 3-hour threshold.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   SUM(properties_affected) AS properties_over_3h
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE status = 'active'
# MAGIC   AND TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) > 180;
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Large number with amber/red conditional formatting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.4: Estimated Penalty Exposure (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Total estimated Ofwat penalty based on current active incidents. Formula: properties x max(0, hours - 3) x GBP 580.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   CONCAT(
# MAGIC     '£',
# MAGIC     FORMAT_NUMBER(
# MAGIC       SUM(
# MAGIC         properties_affected
# MAGIC         * GREATEST(
# MAGIC             TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) / 60.0 - 3,
# MAGIC             0
# MAGIC           )
# MAGIC         * 580
# MAGIC       ),
# MAGIC       0
# MAGIC     )
# MAGIC   ) AS estimated_penalty
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE status = 'active';
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Large number, red text. Include subtitle "Ofwat OPA estimate".

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.5: DMA Health Map (Map)
# MAGIC 
# MAGIC **Type:** Map visualization
# MAGIC **Description:** Geographic view of all DMAs coloured by RAG status.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.dma_code,
# MAGIC   d.dma_name,
# MAGIC   d.latitude,
# MAGIC   d.longitude,
# MAGIC   s.rag_status,
# MAGIC   s.current_pressure_m,
# MAGIC   COALESCE(i.properties_affected, 0) AS properties_affected
# MAGIC FROM water_digital_twin.silver.dim_dma        d
# MAGIC JOIN water_digital_twin.gold.dma_status       s ON d.dma_code = s.dma_code
# MAGIC LEFT JOIN water_digital_twin.gold.dim_incidents i
# MAGIC   ON d.dma_code = i.dma_code AND i.status = 'active';
# MAGIC ```
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Latitude field: `latitude`
# MAGIC - Longitude field: `longitude`
# MAGIC - Colour by: `rag_status` (RED = red, AMBER = amber, GREEN = green)
# MAGIC - Tooltip: DMA code, name, pressure, properties affected

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.6: Regulatory Deadlines (Table)
# MAGIC 
# MAGIC **Type:** Table
# MAGIC **Description:** Active incidents with time remaining until key regulatory thresholds.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   i.incident_id,
# MAGIC   i.dma_code,
# MAGIC   i.properties_affected,
# MAGIC   i.detected_ts,
# MAGIC   ROUND(TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) / 60.0, 1) AS hours_elapsed,
# MAGIC   CASE
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) < 180
# MAGIC     THEN CONCAT(ROUND((180 - TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP())) / 60.0, 1), 'h to 3h threshold')
# MAGIC     ELSE 'EXCEEDED'
# MAGIC   END AS ofwat_3h_status,
# MAGIC   CASE
# MAGIC     WHEN TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) < 720
# MAGIC     THEN CONCAT(ROUND((720 - TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP())) / 60.0, 1), 'h to 12h escalation')
# MAGIC     ELSE 'ESCALATED'
# MAGIC   END AS dwi_12h_status,
# MAGIC   r.dwi_notified_ts
# MAGIC FROM water_digital_twin.gold.dim_incidents            i
# MAGIC LEFT JOIN water_digital_twin.gold.regulatory_notifications r
# MAGIC   ON i.incident_id = r.incident_id
# MAGIC WHERE i.status = 'active'
# MAGIC ORDER BY hours_elapsed DESC;
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Conditional formatting -- red for EXCEEDED/ESCALATED cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.7: C-MeX Proactive Rate (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Percentage of customers who received proactive notification before contacting the company.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   ROUND(
# MAGIC     SUM(n.proactive_notifications) * 100.0
# MAGIC     / NULLIF(SUM(n.proactive_notifications + n.reactive_complaints), 0),
# MAGIC     1
# MAGIC   ) AS proactive_rate_pct
# MAGIC FROM water_digital_twin.gold.dim_incidents          i
# MAGIC JOIN water_digital_twin.gold.incident_notifications n ON i.incident_id = n.incident_id
# MAGIC WHERE i.status = 'active';
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Percentage with green highlight if > 80%.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 1.8: Properties Exceeding 12 Hours (Counter)
# MAGIC 
# MAGIC **Type:** Counter / Single value
# MAGIC **Description:** Properties where supply interruption exceeds 12 hours (Category 1 DWI escalation).
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   COALESCE(SUM(properties_affected), 0) AS properties_over_12h
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE status = 'active'
# MAGIC   AND TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) > 720;
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Large number, red if > 0. Subtitle: "Cat 1 DWI Escalation".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page 2: AMP8 Investment Insights
# MAGIC 
# MAGIC This page has 4 tiles focused on trend analysis and long-term performance patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 2.1: Top 10 DMAs by Incident Count (Bar Chart)
# MAGIC 
# MAGIC **Type:** Bar chart (horizontal)
# MAGIC **Description:** DMAs ranked by total incident count in the current year.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   dma_code,
# MAGIC   COUNT(*) AS incident_count,
# MAGIC   SUM(properties_affected) AS total_properties_affected
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE detected_ts >= DATE_TRUNC('year', CURRENT_DATE())
# MAGIC GROUP BY dma_code
# MAGIC ORDER BY incident_count DESC
# MAGIC LIMIT 10;
# MAGIC ```
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - X axis: `incident_count`
# MAGIC - Y axis: `dma_code`
# MAGIC - Colour: Gradient by `total_properties_affected`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 2.2: Incident Trend (Line Chart)
# MAGIC 
# MAGIC **Type:** Line chart
# MAGIC **Description:** Monthly incident count trend for the current AMP period.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', detected_ts) AS month,
# MAGIC   COUNT(*)                         AS incident_count,
# MAGIC   SUM(properties_affected)         AS total_properties_affected
# MAGIC FROM water_digital_twin.gold.dim_incidents
# MAGIC WHERE detected_ts >= '2025-04-01'
# MAGIC GROUP BY DATE_TRUNC('month', detected_ts)
# MAGIC ORDER BY month;
# MAGIC ```
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - X axis: `month`
# MAGIC - Y axis: `incident_count`
# MAGIC - Secondary Y axis (optional): `total_properties_affected`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 2.3: Anomaly Trends (Line Chart)
# MAGIC 
# MAGIC **Type:** Line chart
# MAGIC **Description:** Daily count of high-sigma anomaly detections across the network.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   DATE_TRUNC('day', scored_ts) AS day,
# MAGIC   COUNT(*)                     AS anomaly_events,
# MAGIC   ROUND(AVG(anomaly_score), 2) AS avg_sigma
# MAGIC FROM water_digital_twin.gold.anomaly_scores
# MAGIC WHERE anomaly_score > 3
# MAGIC   AND scored_ts >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC GROUP BY DATE_TRUNC('day', scored_ts)
# MAGIC ORDER BY day;
# MAGIC ```
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - X axis: `day`
# MAGIC - Y axis: `anomaly_events`
# MAGIC - Secondary Y axis: `avg_sigma`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tile 2.4: Sensor Coverage (Table)
# MAGIC 
# MAGIC **Type:** Table
# MAGIC **Description:** Sensor deployment coverage by DMA with anomaly detection rates.
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   d.dma_code,
# MAGIC   d.dma_name,
# MAGIC   COUNT(DISTINCT s.sensor_id)   AS total_sensors,
# MAGIC   COUNT(DISTINCT CASE WHEN a.anomaly_score > 3 THEN s.sensor_id END) AS sensors_with_anomalies,
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN a.anomaly_score > 3 THEN s.sensor_id END) * 100.0
# MAGIC     / NULLIF(COUNT(DISTINCT s.sensor_id), 0),
# MAGIC     1
# MAGIC   ) AS anomaly_rate_pct
# MAGIC FROM water_digital_twin.silver.dim_dma       d
# MAGIC JOIN water_digital_twin.silver.dim_sensor    s ON d.dma_code = s.dma_code
# MAGIC LEFT JOIN water_digital_twin.gold.anomaly_scores a
# MAGIC   ON s.sensor_id = a.sensor_id
# MAGIC   AND a.scored_ts >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC GROUP BY d.dma_code, d.dma_name
# MAGIC ORDER BY total_sensors DESC;
# MAGIC ```
# MAGIC 
# MAGIC **Display:** Sort by total sensors descending. Highlight rows where `anomaly_rate_pct` > 50%.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Publish
# MAGIC 
# MAGIC 1. Review all tiles render correctly with the demo data.
# MAGIC 2. Click **Publish** to make the dashboard available to workspace users.
# MAGIC 3. Optionally schedule a refresh cadence (e.g., every 15 minutes) for live demo scenarios.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification Checklist
# MAGIC 
# MAGIC | Item | Expected |
# MAGIC |---|---|
# MAGIC | Page 1 - Active Incidents counter | >= 1 |
# MAGIC | Page 1 - Properties affected | 441+ |
# MAGIC | Page 1 - Properties >3h | 441 (current active incident) |
# MAGIC | Page 1 - Penalty estimate | ~£180K |
# MAGIC | Page 1 - DMA map | DEMO_DMA_01 shown in red |
# MAGIC | Page 1 - Regulatory table | Shows EXCEEDED for 3h threshold |
# MAGIC | Page 1 - C-MeX rate | High percentage (>80%) |
# MAGIC | Page 1 - Properties >12h | 0 or low count |
# MAGIC | Page 2 - Top 10 DMAs | DEMO_DMA_01 present |
# MAGIC | Page 2 - Incident trend | Shows monthly data from Apr 2025 |
# MAGIC | Page 2 - Anomaly trends | Shows daily anomaly counts |
# MAGIC | Page 2 - Sensor coverage | All DMAs listed with sensor counts |
# MAGIC | Filters | Time range, DMA, and severity filters functional |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Previous Steps
# MAGIC 
# MAGIC - [01 -- Workspace Setup Guide](01_workspace_setup_guide.md)
# MAGIC - [02 -- Orchestration Guide](02_orchestration_guide.md)
# MAGIC - [03 -- Genie Operator Guide](03_genie_operator_guide.md)
# MAGIC - [04 -- Genie Executive Guide](04_genie_executive_guide.md)
