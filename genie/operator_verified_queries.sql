-- =============================================================================
-- Operator Genie Space — "Network Operations"
-- Verified SQL Queries for Water Utilities Digital Twin Demo
-- Catalog: water_digital_twin
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1: "Which DMAs had the biggest pressure drop in the last 6 hours?"
-- Expected: DEMO_DMA_01 shows the largest drop (~45 m → ~8 m)
-- Usage Guidance: Use when the user asks about recent pressure changes,
--   pressure drops, or which DMAs are losing pressure. Not for single-sensor
--   trends (use Q3) or pinpointing when a drop started (use Q12).
-- ---------------------------------------------------------------------------
WITH latest AS (
  SELECT dma_code, avg_pressure, rag_status
  FROM water_digital_twin.gold.dma_rag_history
  WHERE timestamp = (SELECT MAX(timestamp) FROM water_digital_twin.gold.dma_rag_history)
),
earlier AS (
  SELECT dma_code, avg_pressure
  FROM water_digital_twin.gold.dma_rag_history
  WHERE timestamp = (
    SELECT MAX(timestamp) - INTERVAL 6 HOURS FROM water_digital_twin.gold.dma_rag_history
  )
)
SELECT
  l.dma_code,
  d.dma_name,
  ROUND(e.avg_pressure, 1)                   AS pressure_6h_ago_m,
  ROUND(l.avg_pressure, 1)                   AS pressure_now_m,
  ROUND(e.avg_pressure - l.avg_pressure, 1)  AS pressure_drop_m,
  l.rag_status
FROM latest l
JOIN earlier e ON l.dma_code = e.dma_code
JOIN water_digital_twin.silver.dim_dma d ON l.dma_code = d.dma_code
WHERE e.avg_pressure - l.avg_pressure > 0
ORDER BY pressure_drop_m DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q2: "How many hospitals and schools are in :dma_code?"
-- Parameterized — earns "Trusted" label. Default: DEMO_DMA_01
-- Expected: 2+ schools, 1+ hospital (for DEMO_DMA_01)
-- Usage Guidance: Use when the user asks about sensitive or critical
--   properties (hospitals, schools) in a specific DMA. For listing affected
--   schools across all red/amber DMAs, use Q8 instead.
-- ---------------------------------------------------------------------------
SELECT
  property_type,
  COUNT(*) AS property_count
FROM water_digital_twin.silver.dim_properties
WHERE dma_code = :dma_code
  AND property_type IN ('school', 'hospital')
GROUP BY property_type
ORDER BY property_type;

-- ---------------------------------------------------------------------------
-- Q3: "Show pressure trend for :sensor_id over last 24 hours"
-- Parameterized — earns "Trusted" label. Default: DEMO_SENSOR_01
-- Expected: ~45-55 m until ~02:00, then drops to ~5-10 m (for DEMO_SENSOR_01)
-- Usage Guidance: Use when the user asks for a time-series or trend for a
--   specific sensor. Returns raw telemetry rows suitable for charting.
--   For DMA-level pressure summaries use Q1 or Q12.
-- ---------------------------------------------------------------------------
SELECT
  timestamp,
  sensor_id,
  ROUND(value, 2) AS pressure_m
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id = :sensor_id
  AND sensor_type = 'pressure'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;

-- ---------------------------------------------------------------------------
-- Q4: "Which pump stations feed DMAs that are currently red?"
-- Expected: DEMO_PUMP_01
-- Usage Guidance: Use when the user asks about infrastructure upstream of
--   affected DMAs — specifically pump stations. For reservoir levels use Q5.
--   For all assets sharing a DMA's supply chain use Q6.
-- ---------------------------------------------------------------------------
SELECT
  a.asset_id,
  a.asset_name,
  a.asset_type,
  f.dma_code,
  s.rag_status
FROM water_digital_twin.silver.dim_assets          a
JOIN water_digital_twin.silver.dim_asset_dma_feed   f ON a.asset_id = f.asset_id
JOIN water_digital_twin.gold.dma_status             s ON f.dma_code = s.dma_code
WHERE a.asset_type = 'pump_station'
  AND s.rag_status = 'RED';

-- ---------------------------------------------------------------------------
-- Q5: "Current reservoir level for red/amber DMAs?"
-- Expected: DEMO_SR_01 at ~43%
-- Usage Guidance: Use when the user asks about reservoir levels, water
--   storage, or supply capacity for DMAs that are currently in trouble.
--   For pump stations feeding affected DMAs use Q4.
-- ---------------------------------------------------------------------------
SELECT
  r.reservoir_id,
  r.reservoir_name,
  ROUND(r.current_level_pct, 1) AS level_pct,
  rf.dma_code,
  s.rag_status
FROM water_digital_twin.silver.dim_reservoirs          r
JOIN water_digital_twin.silver.dim_reservoir_dma_feed   rf ON r.reservoir_id = rf.reservoir_id
JOIN water_digital_twin.gold.dma_status                 s  ON rf.dma_code    = s.dma_code
WHERE s.rag_status IN ('RED', 'AMBER')
ORDER BY r.current_level_pct ASC;

-- ---------------------------------------------------------------------------
-- Q6: "Which DMAs share a reservoir or pump station with :dma_code?"
-- Parameterized — earns "Trusted" label. Default: DEMO_DMA_01
-- Expected: DMAs fed by the same reservoirs or pump stations
-- Usage Guidance: Use when the user asks about downstream risk, cascade
--   impact, or which other DMAs could be affected by the same supply issue.
--   Also useful for "what else is connected to this DMA" questions.
-- ---------------------------------------------------------------------------
SELECT DISTINCT
  other_feed.dma_code   AS shared_dma,
  d.dma_name,
  'reservoir'           AS shared_via,
  r.reservoir_name      AS shared_asset
FROM water_digital_twin.silver.dim_reservoir_dma_feed  my_feed
JOIN water_digital_twin.silver.dim_reservoir_dma_feed  other_feed ON my_feed.reservoir_id = other_feed.reservoir_id
JOIN water_digital_twin.silver.dim_reservoirs          r          ON my_feed.reservoir_id = r.reservoir_id
JOIN water_digital_twin.silver.dim_dma                 d          ON other_feed.dma_code  = d.dma_code
WHERE my_feed.dma_code = :dma_code
  AND other_feed.dma_code != :dma_code

UNION

SELECT DISTINCT
  other_feed.dma_code   AS shared_dma,
  d.dma_name,
  'pump_station'        AS shared_via,
  a.asset_name          AS shared_asset
FROM water_digital_twin.silver.dim_asset_dma_feed  my_feed
JOIN water_digital_twin.silver.dim_asset_dma_feed  other_feed ON my_feed.asset_id = other_feed.asset_id
JOIN water_digital_twin.silver.dim_assets          a          ON my_feed.asset_id = a.asset_id
JOIN water_digital_twin.silver.dim_dma             d          ON other_feed.dma_code = d.dma_code
WHERE my_feed.dma_code = :dma_code
  AND other_feed.dma_code != :dma_code
  AND a.asset_type = 'pump_station'

ORDER BY shared_via, shared_dma;

-- ---------------------------------------------------------------------------
-- Q7: "Properties without supply for more than 3 hours?"
-- Expected: 312+ properties
-- Usage Guidance: Use when the user asks about supply interruption duration,
--   properties without water, or the 3-hour regulatory threshold. The 3-hour
--   mark matters because Ofwat penalties begin after that point.
-- ---------------------------------------------------------------------------
SELECT
  p.dma_code,
  COUNT(*)                                               AS properties_affected,
  ROUND(
    TIMESTAMPDIFF(MINUTE, i.start_timestamp, CURRENT_TIMESTAMP()) / 60.0, 1
  )                                                      AS hours_without_supply
FROM water_digital_twin.silver.dim_properties  p
JOIN water_digital_twin.gold.dma_status        s ON p.dma_code = s.dma_code
JOIN water_digital_twin.gold.dim_incidents     i ON s.dma_code = i.dma_code
WHERE s.rag_status = 'RED'
  AND i.status     = 'active'
  AND TIMESTAMPDIFF(MINUTE, i.start_timestamp, CURRENT_TIMESTAMP()) > 180
GROUP BY p.dma_code,
         ROUND(TIMESTAMPDIFF(MINUTE, i.start_timestamp, CURRENT_TIMESTAMP()) / 60.0, 1)
ORDER BY properties_affected DESC;

-- ---------------------------------------------------------------------------
-- Q8: "Schools in affected DMAs"
-- Expected: 2+ schools in DEMO_DMA_01
-- Usage Guidance: Use when the user asks about schools specifically across
--   all currently affected (red/amber) DMAs. For a count of hospitals AND
--   schools in one specific DMA, use Q2 instead.
-- ---------------------------------------------------------------------------
SELECT
  p.property_id,
  p.address,
  p.dma_code,
  s.rag_status
FROM water_digital_twin.silver.dim_properties  p
JOIN water_digital_twin.gold.dma_status        s ON p.dma_code = s.dma_code
WHERE p.property_type = 'school'
  AND s.rag_status IN ('RED', 'AMBER')
ORDER BY p.dma_code, p.address;

-- ---------------------------------------------------------------------------
-- Q9: "Flow into DEMO_DMA_01 at 2 am vs now?"
-- Expected: ~45 l/s at 02:00 → ~12 l/s now
-- Usage Guidance: Use when the user asks about flow rate changes or wants
--   to compare flow before and after the incident. Hardcoded to DEMO_DMA_01
--   flow sensors and the 02:00 incident time.
-- ---------------------------------------------------------------------------
SELECT
  sensor_id,
  timestamp,
  ROUND(flow_rate, 2) AS flow_rate_ls
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id IN ('DEMO_FLOW_01', 'DEMO_FLOW_02')
  AND sensor_type = 'flow'
  AND (
        timestamp BETWEEN TIMESTAMP '2026-04-07 02:00:00' AND TIMESTAMP '2026-04-07 02:05:00'
     OR timestamp >= CURRENT_TIMESTAMP() - INTERVAL 5 MINUTES
  )
ORDER BY sensor_id, timestamp;

-- ---------------------------------------------------------------------------
-- Q10: "Any unusual sensor readings in :dma_code?"
-- Parameterized — earns "Trusted" label. Default: DEMO_DMA_01
-- Expected: DEMO_SENSOR_01 + others with anomaly_sigma > 3 (for DEMO_DMA_01)
-- Usage Guidance: Use when the user asks about anomalies, unusual readings,
--   or outlier sensors in a DMA. Returns sensors exceeding 3-sigma threshold.
--   For raw sensor time-series use Q3.
-- ---------------------------------------------------------------------------
SELECT
  a.sensor_id,
  s.sensor_type,
  ROUND(a.anomaly_sigma, 2) AS anomaly_score_sigma,
  a.timestamp
FROM water_digital_twin.gold.anomaly_scores    a
JOIN water_digital_twin.silver.dim_sensor      s ON a.sensor_id = s.sensor_id
WHERE s.dma_code = :dma_code
  AND a.anomaly_sigma > 3
ORDER BY a.anomaly_sigma DESC;

-- ---------------------------------------------------------------------------
-- Q11: "How many DMAs are red, amber, and green right now?"
-- Expected: Mostly GREEN, 1+ RED (DEMO_DMA_01), possibly some AMBER
-- Usage Guidance: Use when the user asks for a network-wide summary or
--   overall status. Good starting point for "how are things looking?" or
--   "any DMAs in trouble?" questions.
-- ---------------------------------------------------------------------------
SELECT
  rag_status,
  COUNT(*) AS dma_count
FROM water_digital_twin.gold.dma_status
GROUP BY rag_status
ORDER BY
  CASE rag_status WHEN 'RED' THEN 1 WHEN 'AMBER' THEN 2 WHEN 'GREEN' THEN 3 END;

-- ---------------------------------------------------------------------------
-- Q12: "When did the pressure drop start in :dma_code?"
-- Parameterized — earns "Trusted" label. Default: DEMO_DMA_01
-- Expected: Pressure drop visible around 02:00 on 2026-04-07
-- Usage Guidance: Use when the user asks about the timeline or onset of an
--   incident in a specific DMA. Shows RAG status transitions over 24 hours.
--   For network-wide pressure comparison use Q1.
-- ---------------------------------------------------------------------------
SELECT
  timestamp,
  ROUND(avg_pressure, 1) AS avg_pressure_m,
  rag_status,
  LAG(rag_status) OVER (ORDER BY timestamp) AS previous_status
FROM water_digital_twin.gold.dma_rag_history
WHERE dma_code = :dma_code
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;
