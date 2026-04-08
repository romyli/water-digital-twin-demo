-- =============================================================================
-- Operator Genie Space — "Network Operations"
-- Verified SQL Queries for Water Utilities Digital Twin Demo
-- Catalog: water_digital_twin
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1: "Which DMAs had the biggest pressure drop in the last 6 hours?"
-- Expected: DEMO_DMA_01 shows the largest drop (~45 m → ~8 m)
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
-- Q2: "How many hospitals and schools are in DEMO_DMA_01?"
-- Expected: 2+ schools, 1+ hospital
-- ---------------------------------------------------------------------------
SELECT
  property_type,
  COUNT(*) AS property_count
FROM water_digital_twin.silver.dim_properties
WHERE dma_code = 'DEMO_DMA_01'
  AND property_type IN ('school', 'hospital')
GROUP BY property_type
ORDER BY property_type;

-- ---------------------------------------------------------------------------
-- Q3: "Show pressure trend for DEMO_SENSOR_01 over last 24 hours"
-- Expected: ~45-55 m until ~02:00, then drops to ~5-10 m
-- ---------------------------------------------------------------------------
SELECT
  timestamp,
  sensor_id,
  ROUND(value, 2) AS pressure_m
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id = 'DEMO_SENSOR_01'
  AND sensor_type = 'pressure'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;

-- ---------------------------------------------------------------------------
-- Q4: "Which pump stations feed DMAs that are currently red?"
-- Expected: DEMO_PUMP_01
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
-- Q6: "All DMAs within 5 km of DEMO_DMA_01"
-- Expected: Neighbouring DMAs based on centroid distance
-- ---------------------------------------------------------------------------
SELECT
  b.dma_code                                          AS nearby_dma,
  b.dma_name,
  ROUND(ST_Distance(a.centroid, b.centroid) / 1000, 2) AS distance_km
FROM water_digital_twin.silver.dim_dma a
CROSS JOIN water_digital_twin.silver.dim_dma b
WHERE a.dma_code = 'DEMO_DMA_01'
  AND b.dma_code != 'DEMO_DMA_01'
  AND ST_Distance(a.centroid, b.centroid) <= 5000
ORDER BY distance_km;

-- ---------------------------------------------------------------------------
-- Q7: "Properties without supply for more than 3 hours?"
-- Expected: 312+ properties
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
-- Q9: "Flow rate at DEMO_DMA_01 entry at 2 am vs now?"
-- Expected: ~45 l/s at 02:00 → ~12 l/s now
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
-- Q10: "Sensors in DEMO_DMA_01 with anomaly scores > 3σ?"
-- Expected: DEMO_SENSOR_01 + others
-- ---------------------------------------------------------------------------
SELECT
  a.sensor_id,
  s.sensor_type,
  ROUND(a.anomaly_sigma, 2) AS anomaly_score_sigma,
  a.timestamp
FROM water_digital_twin.gold.anomaly_scores    a
JOIN water_digital_twin.silver.dim_sensor      s ON a.sensor_id = s.sensor_id
WHERE s.dma_code = 'DEMO_DMA_01'
  AND a.anomaly_sigma > 3
ORDER BY a.anomaly_sigma DESC;
