-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 08 — Metric Views
-- MAGIC
-- MAGIC Creates **8 metric views** (`mv_*`) with semantic metadata (dimensions + measures)
-- MAGIC for Genie Spaces and dashboards.
-- MAGIC
-- MAGIC **Syntax:** `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$`
-- MAGIC
-- MAGIC **Prerequisites:** SDP pipeline must have completed (Bronze → Silver → Gold).
-- MAGIC Gold `vw_*` views are created by the SDP pipeline — this notebook only creates `mv_*` metric views.
-- MAGIC
-- MAGIC **Catalog:** `water_digital_twin`
-- MAGIC
-- MAGIC | # | Metric View | Source | Genie Space |
-- MAGIC |---|---|---|---|
-- MAGIC | 1 | `mv_dma_pressure` | `gold.vw_dma_pressure` | Operator |
-- MAGIC | 2 | `mv_flow_anomaly` | `silver.fact_telemetry` | Operator |
-- MAGIC | 3 | `mv_reservoir_status` | `gold.vw_reservoir_status` | Operator |
-- MAGIC | 4 | `mv_regulatory_compliance` | `gold.dma_status` | Both |
-- MAGIC | 5 | `mv_incident_summary` | `gold.dim_incidents` | Both |
-- MAGIC | 6 | `mv_penalty_exposure` | `gold.dim_incidents` | Executive |
-- MAGIC | 7 | `mv_anomaly_scores` | `gold.anomaly_scores` | Operator |
-- MAGIC | 8 | `mv_sensor_status` | `silver.dim_sensor` | Operator |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. mv_dma_pressure
-- MAGIC
-- MAGIC Average, min, and max pressure by DMA over time.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_dma_pressure
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.vw_dma_pressure
comment: "Average, min, and max pressure by DMA over time. Primary metric view for operator Genie Space."
dimensions:
  - name: timestamp
    expr: timestamp
    comment: "Timestamp of the 15-minute telemetry reading window"
  - name: dma_code
    expr: dma_code
    comment: "District Metered Area identifier"
  - name: dma_name
    expr: dma_name
    comment: "District Metered Area display name"
measures:
  - name: avg_pressure
    expr: AVG(avg_pressure)
    comment: "Average pressure in metres head across all sensors in the DMA"
  - name: max_pressure
    expr: MAX(max_pressure)
    comment: "Maximum pressure reading in the DMA (metres head)"
  - name: min_pressure
    expr: MIN(min_pressure)
    comment: "Minimum pressure reading in the DMA — low values indicate supply issues"
  - name: avg_total_head
    expr: AVG(avg_total_head_pressure)
    comment: "Average total head pressure in metres"
  - name: reading_count
    expr: SUM(reading_count)
    comment: "Total number of sensor readings in the window"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. mv_flow_anomaly
-- MAGIC
-- MAGIC Flow rate deviation by DMA entry point.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_flow_anomaly
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.silver.fact_telemetry
filter: sensor_type = 'flow'
comment: "Flow rate deviation by DMA entry point. Detects supply interruptions (low flow) and bursts (high flow)."
dimensions:
  - name: timestamp
    expr: timestamp
    comment: "Timestamp of the 15-minute telemetry reading window"
  - name: dma_code
    expr: dma_code
    comment: "District Metered Area identifier"
  - name: sensor_id
    expr: sensor_id
    comment: "Flow sensor identifier"
measures:
  - name: avg_flow_rate
    expr: AVG(flow_rate)
    comment: "Average flow rate in litres per second"
  - name: min_flow_rate
    expr: MIN(flow_rate)
    comment: "Minimum flow rate — low values indicate supply interruption"
  - name: max_flow_rate
    expr: MAX(flow_rate)
    comment: "Maximum flow rate — high values may indicate a burst"
  - name: flow_reading_count
    expr: COUNT(flow_rate)
    comment: "Number of flow sensor readings"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. mv_reservoir_status
-- MAGIC
-- MAGIC Service reservoir status, estimated hours remaining.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_reservoir_status
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.vw_reservoir_status
comment: "Service reservoir status, estimated hours remaining, and fed DMA mapping."
dimensions:
  - name: reservoir_id
    expr: reservoir_id
    comment: "Unique reservoir identifier"
  - name: reservoir_name
    expr: reservoir_name
    comment: "Reservoir display name"
  - name: dma_code
    expr: dma_code
    comment: "DMA code of the area fed by this reservoir"
  - name: fed_dma_name
    expr: fed_dma_name
    comment: "DMA display name of the fed area"
  - name: feed_type
    expr: feed_type
    comment: "Feed relationship type: primary or secondary"
measures:
  - name: current_level_pct
    expr: MAX(current_level_pct)
    comment: "Current reservoir fill level as percentage (0-100)"
  - name: capacity_ml
    expr: MAX(capacity_ml)
    comment: "Total reservoir capacity in megalitres"
  - name: hours_remaining
    expr: MAX(hours_remaining)
    comment: "Estimated hours of supply remaining at current demand rate"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. mv_regulatory_compliance
-- MAGIC
-- MAGIC RAG status distribution, property counts, and sensitive premises across all DMAs.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_regulatory_compliance
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.dma_status
comment: "RAG status distribution, property counts, and sensitive premises across all DMAs. Used by both operator and executive Genie Spaces."
dimensions:
  - name: dma_code
    expr: dma_code
    comment: "District Metered Area identifier"
  - name: rag_status
    expr: rag_status
    comment: "Current RAG status of the DMA: GREEN, AMBER, or RED"
  - name: has_active_incident
    expr: has_active_incident
    comment: "Whether the DMA has an active incident"
measures:
  - name: total_properties
    expr: SUM(property_count)
    comment: "Total number of properties across selected DMAs"
  - name: sensitive_premises_total
    expr: SUM(sensitive_premises_count)
    comment: "Total sensitive premises (hospitals, schools, dialysis homes)"
  - name: red_dma_count
    expr: "SUM(CASE WHEN rag_status = 'RED' THEN 1 ELSE 0 END)"
    comment: "Number of DMAs in RED status — immediate attention required"
  - name: amber_dma_count
    expr: "SUM(CASE WHEN rag_status = 'AMBER' THEN 1 ELSE 0 END)"
    comment: "Number of DMAs in AMBER status — monitoring required"
  - name: green_dma_count
    expr: "SUM(CASE WHEN rag_status = 'GREEN' THEN 1 ELSE 0 END)"
    comment: "Number of DMAs in GREEN status — operating normally"
  - name: dma_count
    expr: COUNT(dma_code)
    comment: "Total number of DMAs"
  - name: incident_dma_count
    expr: "SUM(CASE WHEN has_active_incident = true THEN 1 ELSE 0 END)"
    comment: "Number of DMAs with active incidents"
  - name: total_sensor_count
    expr: SUM(sensor_count)
    comment: "Total sensors across selected DMAs"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. mv_incident_summary
-- MAGIC
-- MAGIC Active and historical incident summary with duration and threshold breach metrics.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_incident_summary
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.dim_incidents
comment: "Incident summary with duration, threshold breaches, and sensitive premises impact. Used by both Genie Spaces."
dimensions:
  - name: incident_id
    expr: incident_id
    comment: "Unique incident identifier (format: INC-YYYY-MMDD-NNN)"
  - name: dma_code
    expr: dma_code
    comment: "Primary affected District Metered Area"
  - name: status
    expr: status
    comment: "Incident status: active or resolved"
  - name: severity
    expr: severity
    comment: "Incident severity: low, medium, high, or critical"
  - name: start_timestamp
    expr: start_timestamp
    comment: "Timestamp when the incident started"
measures:
  - name: total_properties_affected
    expr: SUM(total_properties_affected)
    comment: "Total properties affected across all incidents"
  - name: active_incident_count
    expr: "SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)"
    comment: "Number of currently active incidents"
  - name: incidents_with_sensitive_premises
    expr: "SUM(CASE WHEN sensitive_premises_affected = true THEN 1 ELSE 0 END)"
    comment: "Number of incidents affecting sensitive premises"
  - name: avg_duration_hours
    expr: "AVG(TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())))"
    comment: "Average incident duration in hours (active incidents measured to current time)"
  - name: max_duration_hours
    expr: "MAX(TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())))"
    comment: "Maximum incident duration in hours"
  - name: incidents_over_3h
    expr: "SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) > 3 THEN 1 ELSE 0 END)"
    comment: "Incidents exceeding Ofwat 3-hour supply interruption threshold"
  - name: incidents_over_12h
    expr: "SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) > 12 THEN 1 ELSE 0 END)"
    comment: "Incidents exceeding 12-hour Category 1 escalation threshold"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. mv_penalty_exposure
-- MAGIC
-- MAGIC Ofwat OPA penalty exposure for supply interruption incidents.
-- MAGIC Formula: `properties × MAX(0, hours_interrupted − 3) × £580/property/hour`

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_penalty_exposure
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.dim_incidents
comment: "Ofwat OPA penalty exposure. Formula: properties x MAX(0, hours - 3) x 580 GBP per property per hour beyond the 3-hour threshold."
dimensions:
  - name: incident_id
    expr: incident_id
    comment: "Unique incident identifier"
  - name: dma_code
    expr: dma_code
    comment: "Primary affected District Metered Area"
  - name: severity
    expr: severity
    comment: "Incident severity: low, medium, high, or critical"
  - name: status
    expr: status
    comment: "Incident status: active or resolved"
measures:
  - name: total_penalty_gbp
    expr: "SUM(total_properties_affected * GREATEST(TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) - 3, 0) * 580)"
    comment: "Total estimated Ofwat penalty in GBP across all matching incidents"
  - name: total_properties_at_risk
    expr: SUM(total_properties_affected)
    comment: "Total properties affected across matching incidents"
  - name: max_hours_interrupted
    expr: "MAX(TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())))"
    comment: "Longest supply interruption duration in hours"
  - name: incidents_incurring_penalty
    expr: "SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) > 3 THEN 1 ELSE 0 END)"
    comment: "Number of incidents that have exceeded the 3-hour penalty threshold"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. mv_anomaly_scores
-- MAGIC
-- MAGIC Sensor-level anomaly scores and detection metrics.
-- MAGIC Join with `silver.dim_sensor` for DMA-level aggregation.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_anomaly_scores
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.gold.anomaly_scores
comment: "Sensor-level anomaly scores. Join with silver.dim_sensor on sensor_id for DMA-level aggregation."
dimensions:
  - name: sensor_id
    expr: sensor_id
    comment: "Sensor identifier — join to dim_sensor for DMA code and sensor type"
  - name: timestamp
    expr: timestamp
    comment: "Timestamp of the scored reading"
  - name: is_anomaly
    expr: is_anomaly
    comment: "TRUE if anomaly_sigma exceeds 2.5 threshold"
measures:
  - name: avg_anomaly_sigma
    expr: AVG(anomaly_sigma)
    comment: "Average anomaly score in standard deviations"
  - name: max_anomaly_sigma
    expr: MAX(anomaly_sigma)
    comment: "Maximum anomaly score in standard deviations"
  - name: anomaly_count
    expr: "SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END)"
    comment: "Number of readings flagged as anomalous (> 2.5 sigma)"
  - name: total_readings
    expr: "COUNT(*)"
    comment: "Total number of scored readings"
  - name: anomaly_rate_pct
    expr: "ROUND(SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)"
    comment: "Percentage of readings flagged as anomalous"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. mv_sensor_status
-- MAGIC
-- MAGIC Sensor availability and operational status by DMA.

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.mv_sensor_status
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: water_digital_twin.silver.dim_sensor
comment: "Sensor availability and operational status by DMA and type."
dimensions:
  - name: dma_code
    expr: dma_code
    comment: "District Metered Area the sensor is assigned to"
  - name: sensor_type
    expr: sensor_type
    comment: "Sensor type: pressure or flow"
  - name: status
    expr: status
    comment: "Sensor operational status: active or maintenance"
measures:
  - name: sensor_count
    expr: "COUNT(sensor_id)"
    comment: "Total number of sensors"
  - name: active_sensor_count
    expr: "SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)"
    comment: "Number of sensors currently active"
  - name: maintenance_sensor_count
    expr: "SUM(CASE WHEN status = 'maintenance' THEN 1 ELSE 0 END)"
    comment: "Number of sensors currently in maintenance"
  - name: uptime_pct
    expr: "ROUND(SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0 / COUNT(sensor_id), 1)"
    comment: "Percentage of sensors that are active (operational uptime)"
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation Queries

-- COMMAND ----------

-- mv_dma_pressure: pressure trend for DEMO_DMA_01
SELECT timestamp, dma_code, dma_name,
       MEASURE(avg_pressure) AS avg_pressure,
       MEASURE(min_pressure) AS min_pressure,
       MEASURE(reading_count) AS reading_count
FROM water_digital_twin.gold.mv_dma_pressure
WHERE dma_code = 'DEMO_DMA_01'
  AND timestamp BETWEEN '2026-04-07 01:00:00' AND '2026-04-07 05:30:00'
GROUP BY timestamp, dma_code, dma_name
ORDER BY timestamp
LIMIT 10;

-- COMMAND ----------

-- mv_reservoir_status: DEMO_SR_01 status
SELECT reservoir_id, reservoir_name, dma_code, feed_type,
       MEASURE(current_level_pct) AS current_level_pct,
       MEASURE(capacity_ml) AS capacity_ml,
       MEASURE(hours_remaining) AS hours_remaining
FROM water_digital_twin.gold.mv_reservoir_status
WHERE reservoir_id = 'DEMO_SR_01'
GROUP BY reservoir_id, reservoir_name, dma_code, feed_type;

-- COMMAND ----------

-- mv_incident_summary: all incidents with duration
SELECT incident_id, dma_code, status, severity, start_timestamp,
       MEASURE(total_properties_affected) AS total_properties_affected,
       MEASURE(avg_duration_hours) AS avg_duration_hours,
       MEASURE(incidents_over_3h) AS incidents_over_3h
FROM water_digital_twin.gold.mv_incident_summary
GROUP BY incident_id, dma_code, status, severity, start_timestamp
ORDER BY start_timestamp DESC;

-- COMMAND ----------

-- mv_penalty_exposure: active incident penalties
SELECT incident_id, dma_code, severity, status,
       MEASURE(total_properties_at_risk) AS total_properties_at_risk,
       MEASURE(max_hours_interrupted) AS max_hours_interrupted,
       MEASURE(total_penalty_gbp) AS total_penalty_gbp
FROM water_digital_twin.gold.mv_penalty_exposure
WHERE status = 'active'
GROUP BY incident_id, dma_code, severity, status;

-- COMMAND ----------

-- mv_anomaly_scores: anomalies for DEMO_SENSOR_01
SELECT sensor_id,
       MEASURE(max_anomaly_sigma) AS max_anomaly_sigma,
       MEASURE(anomaly_count) AS anomaly_count,
       MEASURE(total_readings) AS total_readings,
       MEASURE(anomaly_rate_pct) AS anomaly_rate_pct
FROM water_digital_twin.gold.mv_anomaly_scores
WHERE sensor_id = 'DEMO_SENSOR_01'
GROUP BY sensor_id;

-- COMMAND ----------

-- mv_sensor_status: sensor availability by DMA
SELECT dma_code, sensor_type,
       MEASURE(sensor_count) AS sensor_count,
       MEASURE(active_sensor_count) AS active_sensor_count,
       MEASURE(uptime_pct) AS uptime_pct
FROM water_digital_twin.gold.mv_sensor_status
WHERE dma_code = 'DEMO_DMA_01'
GROUP BY dma_code, sensor_type;

-- COMMAND ----------

-- mv_regulatory_compliance: RAG distribution
SELECT rag_status,
       MEASURE(dma_count) AS dma_count,
       MEASURE(total_properties) AS total_properties,
       MEASURE(sensitive_premises_total) AS sensitive_premises_total
FROM water_digital_twin.gold.mv_regulatory_compliance
GROUP BY rag_status
ORDER BY rag_status;
