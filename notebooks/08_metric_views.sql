-- Databricks notebook source

-- MAGIC %md
-- MAGIC # 08 — Metric Views (mv_ prefix)
-- MAGIC
-- MAGIC Creates 5 metric views in `water_digital_twin.gold` for consumption by Genie Spaces and dashboards.
-- MAGIC
-- MAGIC **Metric Views** are a Databricks preview feature that attach semantic metadata (dimensions, measures)
-- MAGIC directly to SQL views, enabling AI-powered natural language querying via Genie.
-- MAGIC
-- MAGIC **Catalog:** `water_digital_twin`
-- MAGIC
-- MAGIC ## Approach
-- MAGIC This notebook uses `CREATE OR REPLACE METRIC VIEW` syntax (Databricks preview).
-- MAGIC Each metric view includes a fallback `CREATE OR REPLACE VIEW` that can be used if the
-- MAGIC `METRIC VIEW` syntax is not yet available on the target workspace.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. mv_dma_pressure
-- MAGIC
-- MAGIC Average, min, and max pressure by DMA over time.
-- MAGIC Primary metric view for the operator Genie Space.
-- MAGIC
-- MAGIC Source: `vw_dma_pressure` joined with `dim_dma` for DMA name.

-- COMMAND ----------

-- Metric View: mv_dma_pressure
-- Primary metric view for operator Genie Space — pressure by DMA over time
CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_dma_pressure
COMMENT 'Average, min, and max pressure by DMA over time. Primary metric view for operator Genie Space.'
AS SELECT
  vp.timestamp     AS telemetry_time,
  vp.dma_code,
  d.dma_name,
  vp.avg_pressure,
  vp.max_pressure,
  vp.min_pressure,
  vp.avg_total_head_pressure,
  vp.reading_count,
  vp.avg_pressure              AS pressure_value,
  vp.avg_total_head_pressure   AS total_head_value
FROM water_digital_twin.gold.vw_dma_pressure vp
LEFT JOIN water_digital_twin.silver.dim_dma d
  ON vp.dma_code = d.dma_code
WITH MEASURES (
  avg_pressure      = AVG(pressure_value)            COMMENT 'Average pressure in metres head across all sensors in the DMA',
  max_pressure      = MAX(max_pressure)              COMMENT 'Maximum pressure reading in the DMA (metres head)',
  min_pressure      = MIN(min_pressure)              COMMENT 'Minimum pressure reading in the DMA — low values indicate supply issues',
  avg_total_head    = AVG(total_head_value)           COMMENT 'Average total head pressure in metres',
  reading_count     = SUM(reading_count)             COMMENT 'Total number of sensor readings in the window'
)
WITH DIMENSIONS (
  telemetry_time    COMMENT 'Timestamp of the 15-minute telemetry reading window',
  dma_code          COMMENT 'District Metered Area identifier',
  dma_name          COMMENT 'District Metered Area display name'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fallback: mv_dma_pressure as regular view
-- MAGIC Uncomment and run below if METRIC VIEW syntax is not available on your workspace.

-- COMMAND ----------

-- FALLBACK: Regular view with semantic metadata in comments
-- Dimensions: telemetry_time (timestamp of 15-min window), dma_code (DMA identifier), dma_name (DMA display name)
-- Measures: avg_pressure=AVG(pressure), max_pressure=MAX, min_pressure=MIN, avg_total_head=AVG(total_head), reading_count=COUNT

-- CREATE OR REPLACE VIEW water_digital_twin.gold.mv_dma_pressure
-- COMMENT 'Average, min, and max pressure by DMA over time. Primary metric view for operator Genie Space.'
-- AS SELECT
--   vp.timestamp     AS telemetry_time,
--   vp.dma_code,
--   d.dma_name,
--   vp.avg_pressure,
--   vp.max_pressure,
--   vp.min_pressure,
--   vp.avg_total_head_pressure,
--   vp.reading_count
-- FROM water_digital_twin.gold.vw_dma_pressure vp
-- LEFT JOIN water_digital_twin.silver.dim_dma d
--   ON vp.dma_code = d.dma_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. mv_flow_anomaly
-- MAGIC
-- MAGIC Flow rate deviation by DMA entry point. Detects supply interruptions (low flow) and bursts (high flow).
-- MAGIC
-- MAGIC Source: `fact_telemetry` (flow sensors only) joined with `dim_sensor` and `dim_dma`.

-- COMMAND ----------

-- Metric View: mv_flow_anomaly
-- Flow rate metrics per sensor and DMA — detects supply interruptions and bursts
CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_flow_anomaly
COMMENT 'Flow rate deviation by DMA entry point. Detects supply interruptions (low flow) and bursts (high flow).'
AS SELECT
  ft.timestamp       AS telemetry_time,
  s.dma_code,
  d.dma_name,
  s.sensor_id,
  ft.flow_rate
FROM water_digital_twin.silver.fact_telemetry ft
INNER JOIN water_digital_twin.silver.dim_sensor s
  ON ft.sensor_id = s.sensor_id
LEFT JOIN water_digital_twin.silver.dim_dma d
  ON s.dma_code = d.dma_code
WHERE ft.sensor_type = 'flow'
WITH MEASURES (
  avg_flow_rate        = AVG(flow_rate)    COMMENT 'Average flow rate in litres per second',
  min_flow_rate        = MIN(flow_rate)    COMMENT 'Minimum flow rate — low values indicate supply interruption',
  max_flow_rate        = MAX(flow_rate)    COMMENT 'Maximum flow rate — high values may indicate a burst',
  flow_reading_count   = COUNT(flow_rate)  COMMENT 'Number of flow sensor readings'
)
WITH DIMENSIONS (
  telemetry_time       COMMENT 'Timestamp of the 15-minute telemetry reading window',
  dma_code             COMMENT 'District Metered Area identifier',
  dma_name             COMMENT 'District Metered Area display name',
  sensor_id            COMMENT 'Flow sensor identifier'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fallback: mv_flow_anomaly as regular view

-- COMMAND ----------

-- FALLBACK: Regular view with semantic metadata in comments
-- Dimensions: telemetry_time, dma_code, dma_name, sensor_id
-- Measures: avg_flow_rate=AVG(flow_rate), min_flow_rate=MIN, max_flow_rate=MAX, flow_reading_count=COUNT

-- CREATE OR REPLACE VIEW water_digital_twin.gold.mv_flow_anomaly
-- COMMENT 'Flow rate deviation by DMA entry point. Detects supply interruptions and bursts.'
-- AS SELECT
--   ft.timestamp       AS telemetry_time,
--   s.dma_code,
--   d.dma_name,
--   s.sensor_id,
--   ft.flow_rate
-- FROM water_digital_twin.silver.fact_telemetry ft
-- INNER JOIN water_digital_twin.silver.dim_sensor s
--   ON ft.sensor_id = s.sensor_id
-- LEFT JOIN water_digital_twin.silver.dim_dma d
--   ON s.dma_code = d.dma_code
-- WHERE ft.sensor_type = 'flow';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. mv_reservoir_status
-- MAGIC
-- MAGIC Service reservoir status, estimated hours remaining, and fed DMA mapping.
-- MAGIC
-- MAGIC Source: `dim_reservoirs` joined with `dim_reservoir_dma_feed` and `dim_dma`.

-- COMMAND ----------

-- Metric View: mv_reservoir_status
-- Reservoir capacity, current level, and estimated hours remaining per fed DMA
CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_reservoir_status
COMMENT 'Service reservoir status, estimated hours remaining, and fed DMA mapping.'
AS SELECT
  r.reservoir_id,
  r.name              AS reservoir_name,
  rf.dma_code         AS fed_dma_code,
  d.dma_name          AS fed_dma_name,
  rf.feed_type,
  r.current_level_pct,
  r.capacity_ml,
  r.hourly_demand_rate_ml,
  -- Pre-compute hours remaining for measure aggregation
  CASE
    WHEN r.hourly_demand_rate_ml > 0
    THEN ROUND((r.current_level_pct / 100.0 * r.capacity_ml) / r.hourly_demand_rate_ml, 1)
    ELSE NULL
  END AS hours_remaining_value
FROM water_digital_twin.silver.dim_reservoirs r
JOIN water_digital_twin.silver.dim_reservoir_dma_feed rf
  ON r.reservoir_id = rf.reservoir_id
LEFT JOIN water_digital_twin.silver.dim_dma d
  ON rf.dma_code = d.dma_code
WITH MEASURES (
  current_level_pct    = MAX(current_level_pct)          COMMENT 'Current reservoir fill level as percentage (0-100)',
  capacity_ml          = MAX(capacity_ml)                COMMENT 'Total reservoir capacity in megalitres',
  hours_remaining      = MAX(hours_remaining_value)      COMMENT 'Estimated hours of supply remaining at current demand rate'
)
WITH DIMENSIONS (
  reservoir_id         COMMENT 'Unique reservoir identifier',
  reservoir_name       COMMENT 'Reservoir display name',
  fed_dma_code         COMMENT 'DMA code of the area fed by this reservoir',
  fed_dma_name         COMMENT 'DMA display name of the fed area',
  feed_type            COMMENT 'Feed relationship type: primary or secondary'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fallback: mv_reservoir_status as regular view

-- COMMAND ----------

-- FALLBACK: Regular view with semantic metadata in comments
-- Dimensions: reservoir_id, reservoir_name, fed_dma_code, fed_dma_name, feed_type
-- Measures: current_level_pct=MAX, capacity_ml=MAX, hours_remaining=MAX((level/100*capacity)/demand_rate)

-- CREATE OR REPLACE VIEW water_digital_twin.gold.mv_reservoir_status
-- COMMENT 'Service reservoir status, estimated hours remaining, and fed DMA mapping.'
-- AS SELECT
--   r.reservoir_id,
--   r.name              AS reservoir_name,
--   rf.dma_code         AS fed_dma_code,
--   d.dma_name          AS fed_dma_name,
--   rf.feed_type,
--   r.current_level_pct,
--   r.capacity_ml,
--   CASE
--     WHEN r.hourly_demand_rate_ml > 0
--     THEN ROUND((r.current_level_pct / 100.0 * r.capacity_ml) / r.hourly_demand_rate_ml, 1)
--     ELSE NULL
--   END AS hours_remaining
-- FROM water_digital_twin.silver.dim_reservoirs r
-- JOIN water_digital_twin.silver.dim_reservoir_dma_feed rf
--   ON r.reservoir_id = rf.reservoir_id
-- LEFT JOIN water_digital_twin.silver.dim_dma d
--   ON rf.dma_code = d.dma_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. mv_regulatory_compliance
-- MAGIC
-- MAGIC Regulatory compliance metrics: Ofwat thresholds, sensitive premises, OPA penalty exposure.
-- MAGIC
-- MAGIC Source: `dma_status` LEFT JOIN `dim_incidents` (active only).
-- MAGIC
-- MAGIC OPA penalty rate: GBP 580 per property per hour beyond the 3-hour threshold.

-- COMMAND ----------

-- Metric View: mv_regulatory_compliance
-- Ofwat regulatory exposure: properties exceeding thresholds, sensitive premises, OPA penalties
CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_regulatory_compliance
COMMENT 'Regulatory compliance metrics: Ofwat thresholds, sensitive premises, OPA penalty exposure. Penalty rate: GBP 580/property/hour.'
AS SELECT
  ds.dma_code,
  di.incident_id,
  ds.rag_status,
  ds.property_count,
  ds.sensitive_premises_count,
  di.total_properties_affected,
  di.sensitive_premises_affected,
  -- Hours since incident start
  CASE
    WHEN di.start_timestamp IS NOT NULL
    THEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP))
    ELSE 0
  END AS hours_since_start,
  -- Properties exceeding 3-hour Ofwat threshold
  CASE
    WHEN di.start_timestamp IS NOT NULL
     AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 3
    THEN di.total_properties_affected
    ELSE 0
  END AS properties_exceeding_3h_value,
  -- Properties exceeding 12-hour Ofwat threshold
  CASE
    WHEN di.start_timestamp IS NOT NULL
     AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 12
    THEN di.total_properties_affected
    ELSE 0
  END AS properties_exceeding_12h_value,
  -- Penalty hours (hours beyond the 3h grace period)
  CASE
    WHEN di.start_timestamp IS NOT NULL
     AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 3
    THEN (TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) - 3) * di.total_properties_affected * 580
    ELSE 0
  END AS opa_penalty_value
FROM water_digital_twin.gold.dma_status ds
LEFT JOIN water_digital_twin.gold.dim_incidents di
  ON ds.dma_code = di.dma_code AND di.status = 'active'
WITH MEASURES (
  properties_exceeding_3h   = SUM(properties_exceeding_3h_value)    COMMENT 'Total properties affected beyond 3-hour Ofwat threshold',
  properties_exceeding_12h  = SUM(properties_exceeding_12h_value)   COMMENT 'Total properties affected beyond 12-hour Ofwat threshold',
  sensitive_premises_total  = SUM(sensitive_premises_count)         COMMENT 'Total sensitive premises (hospitals, schools, dialysis homes) across affected DMAs',
  estimated_opa_penalty     = SUM(opa_penalty_value)                COMMENT 'Estimated OPA penalty exposure in GBP (GBP 580 per property per hour beyond 3h)',
  total_properties_affected = SUM(total_properties_affected)        COMMENT 'Total number of properties affected by active incidents'
)
WITH DIMENSIONS (
  dma_code                  COMMENT 'District Metered Area identifier',
  incident_id               COMMENT 'Active incident identifier (NULL if no incident)',
  rag_status                COMMENT 'Current RAG status of the DMA: GREEN, AMBER, or RED'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fallback: mv_regulatory_compliance as regular view

-- COMMAND ----------

-- FALLBACK: Regular view with semantic metadata in comments
-- Dimensions: dma_code, incident_id, rag_status
-- Measures: properties_exceeding_3h=SUM, properties_exceeding_12h=SUM,
--           sensitive_premises_total=SUM, estimated_opa_penalty=SUM(props*(hrs-3)*580),
--           total_properties_affected=SUM

-- CREATE OR REPLACE VIEW water_digital_twin.gold.mv_regulatory_compliance
-- COMMENT 'Regulatory compliance metrics: Ofwat thresholds, sensitive premises, OPA penalty exposure.'
-- AS SELECT
--   ds.dma_code,
--   di.incident_id,
--   ds.rag_status,
--   ds.property_count,
--   ds.sensitive_premises_count,
--   di.total_properties_affected,
--   CASE
--     WHEN di.start_timestamp IS NOT NULL
--     THEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP))
--     ELSE 0
--   END AS hours_since_start,
--   CASE
--     WHEN di.start_timestamp IS NOT NULL
--      AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 3
--     THEN di.total_properties_affected
--     ELSE 0
--   END AS properties_exceeding_3h,
--   CASE
--     WHEN di.start_timestamp IS NOT NULL
--      AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 12
--     THEN di.total_properties_affected
--     ELSE 0
--   END AS properties_exceeding_12h,
--   CASE
--     WHEN di.start_timestamp IS NOT NULL
--      AND TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) > 3
--     THEN (TIMESTAMPDIFF(HOUR, di.start_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP)) - 3)
--          * di.total_properties_affected * 580
--     ELSE 0
--   END AS estimated_opa_penalty
-- FROM water_digital_twin.gold.dma_status ds
-- LEFT JOIN water_digital_twin.gold.dim_incidents di
--   ON ds.dma_code = di.dma_code AND di.status = 'active';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. mv_incident_summary
-- MAGIC
-- MAGIC Active and historical incident summary for the executive Genie Space.
-- MAGIC
-- MAGIC Source: `dim_incidents` joined with `dma_summary`.

-- COMMAND ----------

-- Metric View: mv_incident_summary
-- Executive-level incident summary with property impact and duration metrics
CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_incident_summary
COMMENT 'Active and historical incident summary for executive Genie Space.'
AS SELECT
  i.incident_id,
  i.dma_code,
  i.status,
  i.severity,
  CAST(i.start_timestamp AS DATE)   AS start_date,
  i.start_timestamp,
  i.end_timestamp,
  i.total_properties_affected       AS incident_properties_affected,
  i.sensitive_premises_affected,
  s.property_count                  AS dma_property_count,
  s.sensitive_premises_count        AS dma_sensitive_premises_count,
  -- Duration in hours for measure computation
  TIMESTAMPDIFF(
    HOUR,
    i.start_timestamp,
    COALESCE(i.end_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP))
  ) AS duration_hours_value,
  -- Boolean flags as integers for aggregation
  CASE WHEN i.status = 'active' THEN 1 ELSE 0 END AS is_active_flag,
  CASE WHEN i.sensitive_premises_affected = TRUE THEN 1 ELSE 0 END AS has_sensitive_flag
FROM water_digital_twin.gold.dim_incidents i
LEFT JOIN water_digital_twin.gold.dma_summary s
  ON i.dma_code = s.dma_code
WITH MEASURES (
  total_properties_affected         = SUM(incident_properties_affected)   COMMENT 'Total properties affected across all incidents',
  active_incident_count             = SUM(is_active_flag)                 COMMENT 'Number of currently active incidents',
  incidents_with_sensitive_premises = SUM(has_sensitive_flag)             COMMENT 'Number of incidents affecting sensitive premises (hospitals, schools, dialysis homes)',
  avg_duration_hours                = AVG(duration_hours_value)           COMMENT 'Average incident duration in hours (ongoing incidents use current time)'
)
WITH DIMENSIONS (
  incident_id   COMMENT 'Unique incident identifier (format: INC-YYYY-MMDD-NNN)',
  dma_code      COMMENT 'Primary affected District Metered Area',
  status        COMMENT 'Incident status: active, resolved, or closed',
  severity      COMMENT 'Incident severity: low, medium, high, or critical',
  start_date    COMMENT 'Date the incident started'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Fallback: mv_incident_summary as regular view

-- COMMAND ----------

-- FALLBACK: Regular view with semantic metadata in comments
-- Dimensions: incident_id, dma_code, status, severity, start_date
-- Measures: total_properties_affected=SUM, active_incident_count=COUNT(WHERE active),
--           incidents_with_sensitive_premises=COUNT(WHERE sensitive=true),
--           avg_duration_hours=AVG(TIMESTAMPDIFF)

-- CREATE OR REPLACE VIEW water_digital_twin.gold.mv_incident_summary
-- COMMENT 'Active and historical incident summary for executive Genie Space.'
-- AS SELECT
--   i.incident_id,
--   i.dma_code,
--   i.status,
--   i.severity,
--   CAST(i.start_timestamp AS DATE)   AS start_date,
--   i.start_timestamp,
--   i.end_timestamp,
--   i.total_properties_affected,
--   i.sensitive_premises_affected,
--   s.property_count                  AS dma_property_count,
--   s.sensitive_premises_count        AS dma_sensitive_premises_count,
--   TIMESTAMPDIFF(
--     HOUR,
--     i.start_timestamp,
--     COALESCE(i.end_timestamp, CAST('2026-04-07 05:30:00' AS TIMESTAMP))
--   ) AS duration_hours
-- FROM water_digital_twin.gold.dim_incidents i
-- LEFT JOIN water_digital_twin.gold.dma_summary s
--   ON i.dma_code = s.dma_code;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation Queries
-- MAGIC
-- MAGIC Run the cells below to verify metric views are working correctly with demo data.

-- COMMAND ----------

-- Validate mv_dma_pressure: DEMO_DMA_01 pressure readings
SELECT telemetry_time, dma_code, dma_name, avg_pressure, min_pressure, reading_count
FROM water_digital_twin.gold.mv_dma_pressure
WHERE dma_code = 'DEMO_DMA_01'
  AND telemetry_time BETWEEN '2026-04-07 01:00:00' AND '2026-04-07 05:30:00'
ORDER BY telemetry_time;

-- COMMAND ----------

-- Validate mv_flow_anomaly: DEMO_FLOW_01/02 flow readings
SELECT telemetry_time, dma_code, sensor_id, flow_rate
FROM water_digital_twin.gold.mv_flow_anomaly
WHERE sensor_id IN ('DEMO_FLOW_01', 'DEMO_FLOW_02')
  AND telemetry_time BETWEEN '2026-04-07 01:00:00' AND '2026-04-07 05:30:00'
ORDER BY sensor_id, telemetry_time;

-- COMMAND ----------

-- Validate mv_reservoir_status: DEMO_SR_01 at 43% with ~3.1h remaining
SELECT reservoir_id, reservoir_name, fed_dma_code, feed_type,
       current_level_pct, capacity_ml, hours_remaining_value AS hours_remaining
FROM water_digital_twin.gold.mv_reservoir_status
WHERE reservoir_id = 'DEMO_SR_01';

-- COMMAND ----------

-- Validate mv_regulatory_compliance: penalty exposure for active incident
SELECT dma_code, incident_id, rag_status, hours_since_start,
       properties_exceeding_3h_value AS properties_exceeding_3h,
       sensitive_premises_count,
       opa_penalty_value AS estimated_opa_penalty
FROM water_digital_twin.gold.mv_regulatory_compliance
WHERE incident_id IS NOT NULL;

-- COMMAND ----------

-- Validate mv_incident_summary: 1 active + historical incidents
SELECT incident_id, dma_code, status, severity, start_date,
       incident_properties_affected, duration_hours_value AS duration_hours,
       has_sensitive_flag AS sensitive_premises
FROM water_digital_twin.gold.mv_incident_summary
ORDER BY start_date DESC;
