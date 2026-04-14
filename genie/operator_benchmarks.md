# Operator Genie Space — Benchmarks

## Q1: Pressure drops across DMAs

**Question:** Which DMAs had the biggest pressure drop in the last 6 hours?

```sql
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
```

**Question:** Where has pressure fallen the most recently?

*Same SQL as above.*

---

## Q2: Hospitals and schools in a DMA

**Question:** How many hospitals and schools are in DEMO_DMA_01?

```sql
SELECT
  property_type,
  COUNT(*) AS property_count
FROM water_digital_twin.silver.dim_properties
WHERE dma_code = 'DEMO_DMA_01'
  AND property_type IN ('school', 'hospital')
GROUP BY property_type
ORDER BY property_type;
```

**Question:** Critical facilities in DEMO_DMA_03?

```sql
SELECT
  property_type,
  COUNT(*) AS property_count
FROM water_digital_twin.silver.dim_properties
WHERE dma_code = 'DEMO_DMA_03'
  AND property_type IN ('school', 'hospital')
GROUP BY property_type
ORDER BY property_type;
```

---

## Q3: Sensor pressure trend

**Question:** Show pressure trend for DEMO_SENSOR_01 over last 24 hours

```sql
SELECT
  timestamp,
  sensor_id,
  ROUND(value, 2) AS pressure_m
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id = 'DEMO_SENSOR_01'
  AND sensor_type = 'pressure'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;
```

**Question:** What has DEMO_SENSOR_03 been doing in the last day?

```sql
SELECT
  timestamp,
  sensor_id,
  ROUND(value, 2) AS pressure_m
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id = 'DEMO_SENSOR_03'
  AND sensor_type = 'pressure'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;
```

---

## Q4: Pump stations feeding red DMAs

**Question:** Which pump stations feed DMAs that are currently red?

```sql
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
```

**Question:** What pumps are supplying affected areas?

*Same SQL as above.*

---

## Q5: Reservoir levels for affected DMAs

**Question:** Current reservoir level for red and amber DMAs?

```sql
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
```

**Question:** How full are the reservoirs for affected areas?

*Same SQL as above.*

---

## Q6: Shared infrastructure

**Question:** Which DMAs share a reservoir or pump station with DEMO_DMA_01?

```sql
SELECT DISTINCT
  other_feed.dma_code   AS shared_dma,
  d.dma_name,
  'reservoir'           AS shared_via,
  r.reservoir_name      AS shared_asset
FROM water_digital_twin.silver.dim_reservoir_dma_feed  my_feed
JOIN water_digital_twin.silver.dim_reservoir_dma_feed  other_feed ON my_feed.reservoir_id = other_feed.reservoir_id
JOIN water_digital_twin.silver.dim_reservoirs          r          ON my_feed.reservoir_id = r.reservoir_id
JOIN water_digital_twin.silver.dim_dma                 d          ON other_feed.dma_code  = d.dma_code
WHERE my_feed.dma_code = 'DEMO_DMA_01'
  AND other_feed.dma_code != 'DEMO_DMA_01'

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
WHERE my_feed.dma_code = 'DEMO_DMA_01'
  AND other_feed.dma_code != 'DEMO_DMA_01'
  AND a.asset_type = 'pump_station'

ORDER BY shared_via, shared_dma;
```

**Question:** What other areas could be affected if DEMO_DMA_01's supply fails?

*Same SQL as above.*

---

## Q7: Properties exceeding 3-hour threshold

**Question:** Properties without supply for more than 3 hours?

```sql
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
```

**Question:** How many properties have breached the 3-hour threshold?

*Same SQL as above.*

---

## Q8: Schools in affected DMAs

**Question:** Schools in affected DMAs?

```sql
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
```

**Question:** Which schools are in red or amber areas?

*Same SQL as above.*

---

## Q9: Flow comparison

**Question:** Flow into DEMO_DMA_01 at 2 am vs now?

```sql
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
```

**Question:** How has flow changed since 2am?

*Same SQL as above.*

---

## Q10: Anomalous sensors

**Question:** Any unusual sensor readings in DEMO_DMA_01?

```sql
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
```

**Question:** Which sensors are showing outlier readings in DEMO_DMA_01?

*Same SQL as above.*

---

## Q11: Network-wide RAG summary

**Question:** How many DMAs are red, amber, and green right now?

```sql
SELECT
  rag_status,
  COUNT(*) AS dma_count
FROM water_digital_twin.gold.dma_status
GROUP BY rag_status
ORDER BY
  CASE rag_status WHEN 'RED' THEN 1 WHEN 'AMBER' THEN 2 WHEN 'GREEN' THEN 3 END;
```

**Question:** What's the overall network status?

*Same SQL as above.*

**Question:** Any DMAs in trouble?

*Same SQL as above.*

---

## Q12: Pressure drop timeline

**Question:** When did the pressure drop start in DEMO_DMA_01?

```sql
SELECT
  timestamp,
  ROUND(avg_pressure, 1) AS avg_pressure_m,
  rag_status,
  LAG(rag_status) OVER (ORDER BY timestamp) AS previous_status
FROM water_digital_twin.gold.dma_rag_history
WHERE dma_code = 'DEMO_DMA_01'
  AND timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY timestamp;
```

**Question:** Timeline of the incident in DMA 01?

*Same SQL as above.*

**Question:** When did DEMO_DMA_01 go red?

*Same SQL as above.*
