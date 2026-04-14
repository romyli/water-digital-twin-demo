-- =============================================================================
-- Executive Genie Space — "Water Operations Intelligence"
-- Verified SQL Queries for Water Utilities Digital Twin Demo
-- Catalog: water_digital_twin
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1: "Total Ofwat penalty exposure?"
-- Expected: ~£180K
-- Formula: affected_properties × MAX(0, hours_interrupted - 3) × £580
-- Usage Guidance: Use when the user asks about penalties, fines, financial
--   risk, or Ofwat exposure. Calculates per-incident penalty using the
--   £580/property/hour-over-3h formula. For property counts only use Q2.
-- ---------------------------------------------------------------------------
SELECT
  i.incident_id,
  i.dma_code,
  i.total_properties_affected,
  ROUND(
    TIMESTAMPDIFF(MINUTE, i.start_timestamp, COALESCE(i.end_timestamp, CURRENT_TIMESTAMP())) / 60.0, 1
  ) AS total_hours,
  ROUND(
    i.total_properties_affected
    * GREATEST(
        TIMESTAMPDIFF(MINUTE, i.start_timestamp, COALESCE(i.end_timestamp, CURRENT_TIMESTAMP())) / 60.0 - 3,
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
-- Usage Guidance: Use when the user asks how many properties have breached
--   the regulatory threshold this month. Returns a single count. For the
--   financial impact of those breaches use Q1.
-- ---------------------------------------------------------------------------
SELECT
  COUNT(DISTINCT p.property_id) AS properties_over_3h
FROM water_digital_twin.silver.dim_properties  p
JOIN water_digital_twin.gold.dim_incidents     i ON p.dma_code = i.dma_code
WHERE i.status = 'active'
  AND i.start_timestamp >= DATE_TRUNC('month', CURRENT_DATE())
  AND TIMESTAMPDIFF(MINUTE, i.start_timestamp, COALESCE(i.end_timestamp, CURRENT_TIMESTAMP())) > 180;

-- ---------------------------------------------------------------------------
-- Q3: "Which areas are giving us the most trouble this year?"
-- Expected: DEMO_DMA_01 in top results
-- Usage Guidance: Use when the user asks about repeat offenders, worst-
--   performing DMAs, or incident frequency. Shows year-to-date rankings
--   by incident count with average duration.
-- ---------------------------------------------------------------------------
SELECT
  dma_code,
  COUNT(*)                                         AS incident_count,
  SUM(total_properties_affected)                   AS total_properties_affected,
  ROUND(AVG(
    TIMESTAMPDIFF(MINUTE, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) / 60.0
  ), 1)                                            AS avg_duration_hours
FROM water_digital_twin.gold.dim_incidents
WHERE start_timestamp >= DATE_TRUNC('year', CURRENT_DATE())
GROUP BY dma_code
ORDER BY incident_count DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Q4: "Incidents since :start_date affecting more than :min_properties properties?"
-- Parameterized — earns "Trusted" label. Defaults: 2026-03-14, 100
-- Expected: Active incident with 441 properties
-- Usage Guidance: Use when the user asks about major incidents in a date
--   range or wants to filter by severity (property count). Flexible query
--   for ad-hoc incident lookups.
-- ---------------------------------------------------------------------------
SELECT
  incident_id,
  dma_code,
  start_timestamp,
  end_timestamp,
  status,
  total_properties_affected,
  ROUND(
    TIMESTAMPDIFF(MINUTE, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())) / 60.0, 1
  ) AS duration_hours
FROM water_digital_twin.gold.dim_incidents
WHERE start_timestamp >= :start_date
  AND total_properties_affected > :min_properties
ORDER BY total_properties_affected DESC;

-- ---------------------------------------------------------------------------
-- Q5: "% of customers with proactive notification before complaining?"
-- Expected: High % — ~423 proactive vs ~47 complaints
-- Usage Guidance: Use when the user asks about customer communication
--   effectiveness, proactive vs reactive notifications, or complaint ratios.
--   Shows how well the team is getting ahead of customer calls.
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
WHERE i.start_timestamp >= DATE_TRUNC('quarter', CURRENT_DATE())
ORDER BY proactive_pct DESC;

-- ---------------------------------------------------------------------------
-- Q6: "Hospitals/schools affected in :dma_code this quarter?"
-- Parameterized — earns "Trusted" label. Default: DEMO_DMA_01
-- Expected: 1 hospital, 2 schools (for DEMO_DMA_01)
-- Usage Guidance: Use when the user asks about vulnerable or sensitive
--   properties (hospitals, schools) affected by incidents in a specific DMA.
--   Scoped to current quarter. For all properties use Q2.
-- ---------------------------------------------------------------------------
SELECT
  i.dma_code,
  p.property_type,
  COUNT(*) AS affected_count
FROM water_digital_twin.gold.dim_incidents     i
JOIN water_digital_twin.silver.dim_properties  p ON i.dma_code = p.dma_code
WHERE i.dma_code = :dma_code
  AND i.start_timestamp >= DATE_TRUNC('quarter', CURRENT_DATE())
  AND i.status IN ('active', 'resolved')
  AND p.property_type IN ('hospital', 'school')
GROUP BY i.dma_code, p.property_type
ORDER BY i.dma_code, p.property_type;

-- ---------------------------------------------------------------------------
-- Q7: "Average time from detection to DWI notification?"
-- Expected: A few hours
-- Usage Guidance: Use when the user asks about regulatory response times,
--   DWI notification speed, or compliance reporting timeliness. Shows
--   avg/min/max hours from incident detection to DWI notification.
-- ---------------------------------------------------------------------------
SELECT
  ROUND(
    AVG(
      TIMESTAMPDIFF(MINUTE, i.start_timestamp, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS avg_detection_to_dwi_hours,
  ROUND(
    MIN(
      TIMESTAMPDIFF(MINUTE, i.start_timestamp, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS min_hours,
  ROUND(
    MAX(
      TIMESTAMPDIFF(MINUTE, i.start_timestamp, r.dwi_notified_ts)
    ) / 60.0,
    1
  ) AS max_hours
FROM water_digital_twin.gold.dim_incidents            i
JOIN water_digital_twin.gold.regulatory_notifications  r ON i.incident_id = r.incident_id
WHERE r.dwi_notified_ts IS NOT NULL
  AND i.start_timestamp >= DATE_TRUNC('year', CURRENT_DATE());

-- ---------------------------------------------------------------------------
-- Q8: "Are we seeing more or fewer incidents than in AMP7?"
-- Expected: Annualised comparison across AMP periods (AMP7: 2020-2025, AMP8: 2025-2030)
-- Note: AMP7 ran 5 full years; AMP8 is partial — annualise both for fair comparison
-- Usage Guidance: Use when the user asks about trends over time, AMP period
--   comparisons, or whether performance is improving. Annualises AMP8 data
--   for a fair like-for-like comparison with AMP7.
-- ---------------------------------------------------------------------------
SELECT
  dma_code,
  SUM(CASE WHEN start_timestamp BETWEEN '2020-04-01' AND '2025-03-31' THEN 1 ELSE 0 END) AS amp7_total,
  ROUND(
    SUM(CASE WHEN start_timestamp BETWEEN '2020-04-01' AND '2025-03-31' THEN 1 ELSE 0 END) / 5.0, 1
  ) AS amp7_per_year,
  SUM(CASE WHEN start_timestamp >= '2025-04-01' THEN 1 ELSE 0 END) AS amp8_to_date,
  ROUND(
    SUM(CASE WHEN start_timestamp >= '2025-04-01' THEN 1 ELSE 0 END)
    * 365.25 / GREATEST(DATEDIFF(CURRENT_DATE(), DATE '2025-04-01'), 1),
    1
  ) AS amp8_annualised
FROM water_digital_twin.gold.dim_incidents
GROUP BY dma_code
ORDER BY amp7_total DESC
LIMIT 10;
