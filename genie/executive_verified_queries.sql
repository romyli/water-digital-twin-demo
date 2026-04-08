-- =============================================================================
-- Executive Genie Space — "Water Operations Intelligence"
-- Verified SQL Queries for Water Utilities Digital Twin Demo
-- Catalog: water_digital_twin
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1: "Total Ofwat penalty exposure?"
-- Expected: ~£180K
-- Formula: affected_properties × MAX(0, hours_interrupted - 3) × £580
-- ---------------------------------------------------------------------------
SELECT
  i.incident_id,
  i.dma_code,
  i.properties_affected,
  ROUND(
    TIMESTAMPDIFF(MINUTE, i.detected_ts, COALESCE(i.resolved_ts, CURRENT_TIMESTAMP())) / 60.0, 1
  ) AS total_hours,
  ROUND(
    i.properties_affected
    * GREATEST(
        TIMESTAMPDIFF(MINUTE, i.detected_ts, COALESCE(i.resolved_ts, CURRENT_TIMESTAMP())) / 60.0 - 3,
        0
      )
    * 580,
    0
  ) AS estimated_penalty_gbp
FROM water_digital_twin.gold.dim_incidents i
WHERE i.status = 'active'
ORDER BY estimated_penalty_gbp DESC;

-- ---------------------------------------------------------------------------
-- Q2: "Properties exceeding 3-hour threshold this month?"
-- Expected: 441
-- ---------------------------------------------------------------------------
SELECT
  COUNT(DISTINCT p.property_id) AS properties_over_3h
FROM water_digital_twin.silver.dim_properties  p
JOIN water_digital_twin.gold.dim_incidents     i ON p.dma_code = i.dma_code
WHERE i.status = 'active'
  AND i.detected_ts >= DATE_TRUNC('month', CURRENT_DATE())
  AND TIMESTAMPDIFF(MINUTE, i.detected_ts, COALESCE(i.resolved_ts, CURRENT_TIMESTAMP())) > 180;

-- ---------------------------------------------------------------------------
-- Q3: "Top 10 DMAs by incident count this year?"
-- Expected: DEMO_DMA_01 in top results
-- ---------------------------------------------------------------------------
SELECT
  dma_code,
  COUNT(*)                                         AS incident_count,
  SUM(properties_affected)                         AS total_properties_affected,
  ROUND(AVG(
    TIMESTAMPDIFF(MINUTE, detected_ts, COALESCE(resolved_ts, CURRENT_TIMESTAMP())) / 60.0
  ), 1)                                            AS avg_duration_hours
FROM water_digital_twin.gold.dim_incidents
WHERE detected_ts >= DATE_TRUNC('year', CURRENT_DATE())
GROUP BY dma_code
ORDER BY incident_count DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q4: "Incidents in last 30 days with >100 properties affected?"
-- Expected: Active incident with 441 properties
-- ---------------------------------------------------------------------------
SELECT
  incident_id,
  dma_code,
  detected_ts,
  resolved_ts,
  status,
  properties_affected,
  ROUND(
    TIMESTAMPDIFF(MINUTE, detected_ts, COALESCE(resolved_ts, CURRENT_TIMESTAMP())) / 60.0, 1
  ) AS duration_hours
FROM water_digital_twin.gold.dim_incidents
WHERE detected_ts >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND properties_affected > 100
ORDER BY properties_affected DESC;

-- ---------------------------------------------------------------------------
-- Q5: "% of customers with proactive notification before complaining?"
-- Expected: High % — ~423 proactive vs ~47 complaints
-- ---------------------------------------------------------------------------
SELECT
  i.incident_id,
  i.dma_code,
  n.proactive_notifications,
  n.reactive_complaints,
  ROUND(
    n.proactive_notifications * 100.0
    / NULLIF(n.proactive_notifications + n.reactive_complaints, 0),
    1
  ) AS proactive_pct
FROM water_digital_twin.gold.dim_incidents          i
JOIN water_digital_twin.gold.incident_notifications n ON i.incident_id = n.incident_id
WHERE i.detected_ts >= DATE_TRUNC('quarter', CURRENT_DATE())
ORDER BY proactive_pct DESC;

-- ---------------------------------------------------------------------------
-- Q6: "Hospitals/schools affected by supply interruptions this quarter?"
-- Expected: DEMO_DMA_01 with 1 hospital, 2 schools
-- ---------------------------------------------------------------------------
SELECT
  i.dma_code,
  p.property_type,
  COUNT(*) AS affected_count,
  COLLECT_LIST(p.property_name) AS property_names
FROM water_digital_twin.gold.dim_incidents     i
JOIN water_digital_twin.silver.dim_properties  p ON i.dma_code = p.dma_code
WHERE i.detected_ts >= DATE_TRUNC('quarter', CURRENT_DATE())
  AND i.status IN ('active', 'resolved')
  AND p.property_type IN ('hospital', 'school')
GROUP BY i.dma_code, p.property_type
ORDER BY i.dma_code, p.property_type;

-- ---------------------------------------------------------------------------
-- Q7: "Average time from detection to DWI notification?"
-- Expected: A few hours
-- ---------------------------------------------------------------------------
SELECT
  ROUND(
    AVG(
      TIMESTAMPDIFF(MINUTE, i.detected_ts, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS avg_detection_to_dwi_hours,
  ROUND(
    MIN(
      TIMESTAMPDIFF(MINUTE, i.detected_ts, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS min_hours,
  ROUND(
    MAX(
      TIMESTAMPDIFF(MINUTE, i.detected_ts, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS max_hours
FROM water_digital_twin.gold.dim_incidents            i
JOIN water_digital_twin.gold.regulatory_notifications  r ON i.incident_id = r.incident_id
WHERE r.dwi_notified_ts IS NOT NULL
  AND i.detected_ts >= DATE_TRUNC('year', CURRENT_DATE());

-- ---------------------------------------------------------------------------
-- Q8: "Incident frequency AMP8 vs AMP7 for top 10 DMAs?"
-- Expected: Comparison across AMP periods (AMP7: 2020-2025, AMP8: 2025-2030)
-- ---------------------------------------------------------------------------
SELECT
  dma_code,
  SUM(CASE WHEN detected_ts BETWEEN '2020-04-01' AND '2025-03-31' THEN 1 ELSE 0 END) AS amp7_incidents,
  SUM(CASE WHEN detected_ts >= '2025-04-01' THEN 1 ELSE 0 END)                        AS amp8_incidents,
  ROUND(
    SUM(CASE WHEN detected_ts >= '2025-04-01' THEN 1 ELSE 0 END) * 100.0
    / NULLIF(SUM(CASE WHEN detected_ts BETWEEN '2020-04-01' AND '2025-03-31' THEN 1 ELSE 0 END), 0),
    1
  ) AS amp8_vs_amp7_pct
FROM water_digital_twin.gold.dim_incidents
GROUP BY dma_code
ORDER BY amp7_incidents DESC
LIMIT 10;
