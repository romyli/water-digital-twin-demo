# Dashboard Guide

**Water Utilities -- Digital Twin Demo**

Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
CLI profile: `adb-98`

---

## Overview

This guide creates a **Lakeview dashboard** called **"Water Operations -- Executive View"** with two pages and three global filters. The dashboard provides executive-level visibility into active incidents, regulatory exposure, and AMP8 investment trends.

---

## Step 1 -- Create the Dashboard

1. In the workspace sidebar, navigate to **Dashboards**.
2. Click **Create Dashboard** (Lakeview).
3. Set the name: **Water Operations -- Executive View**.

---

## Step 2 -- Configure Global Filters

Add three dashboard-level filters that apply across all pages:

### Filter 1: Time Range

- **Field:** `detected_ts` (from `dim_incidents`)
- **Type:** Date range picker
- **Default:** Last 30 days

### Filter 2: DMA Code

- **Field:** `dma_code`
- **Type:** Multi-select dropdown
- **Source:** `SELECT DISTINCT dma_code FROM water_digital_twin.silver.dim_dma ORDER BY dma_code`
- **Default:** All

### Filter 3: Incident Severity

- **Field:** `severity`
- **Type:** Multi-select dropdown
- **Values:** `critical`, `major`, `minor`
- **Default:** All

---

## Page 1: Incident Overview

This page has 8 tiles providing a real-time view of active incidents and regulatory exposure.

---

### Tile 1.1: Active Incidents (Counter)

**Type:** Counter / Single value
**Description:** Count of currently active incidents.

```sql
SELECT
  COUNT(*) AS active_incidents
FROM water_digital_twin.gold.dim_incidents
WHERE status = 'active';
```

**Display:** Large number, red highlight if > 0.

---

### Tile 1.2: Total Properties Affected (Counter)

**Type:** Counter / Single value
**Description:** Total properties currently affected by active incidents.

```sql
SELECT
  SUM(properties_affected) AS total_properties_affected
FROM water_digital_twin.gold.dim_incidents
WHERE status = 'active';
```

**Display:** Large number.

---

### Tile 1.3: Properties Exceeding 3-Hour Threshold (Counter)

**Type:** Counter / Single value
**Description:** Properties where supply interruption has exceeded the Ofwat 3-hour threshold.

```sql
SELECT
  SUM(properties_affected) AS properties_over_3h
FROM water_digital_twin.gold.dim_incidents
WHERE status = 'active'
  AND TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) > 180;
```

**Display:** Large number with amber/red conditional formatting.

---

### Tile 1.4: Estimated Penalty Exposure (Counter)

**Type:** Counter / Single value
**Description:** Total estimated Ofwat penalty based on current active incidents. Formula: properties x max(0, hours - 3) x GBP 580.

```sql
SELECT
  CONCAT(
    '£',
    FORMAT_NUMBER(
      SUM(
        properties_affected
        * GREATEST(
            TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) / 60.0 - 3,
            0
          )
        * 580
      ),
      0
    )
  ) AS estimated_penalty
FROM water_digital_twin.gold.dim_incidents
WHERE status = 'active';
```

**Display:** Large number, red text. Include subtitle "Ofwat OPA estimate".

---

### Tile 1.5: DMA Health Map (Map)

**Type:** Map visualization
**Description:** Geographic view of all DMAs coloured by RAG status.

```sql
SELECT
  d.dma_code,
  d.dma_name,
  d.latitude,
  d.longitude,
  s.rag_status,
  s.current_pressure_m,
  COALESCE(i.properties_affected, 0) AS properties_affected
FROM water_digital_twin.silver.dim_dma        d
JOIN water_digital_twin.gold.dma_status       s ON d.dma_code = s.dma_code
LEFT JOIN water_digital_twin.gold.dim_incidents i
  ON d.dma_code = i.dma_code AND i.status = 'active';
```

**Configuration:**
- Latitude field: `latitude`
- Longitude field: `longitude`
- Colour by: `rag_status` (RED = red, AMBER = amber, GREEN = green)
- Tooltip: DMA code, name, pressure, properties affected

---

### Tile 1.6: Regulatory Deadlines (Table)

**Type:** Table
**Description:** Active incidents with time remaining until key regulatory thresholds.

```sql
SELECT
  i.incident_id,
  i.dma_code,
  i.properties_affected,
  i.detected_ts,
  ROUND(TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) / 60.0, 1) AS hours_elapsed,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) < 180
    THEN CONCAT(ROUND((180 - TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP())) / 60.0, 1), 'h to 3h threshold')
    ELSE 'EXCEEDED'
  END AS ofwat_3h_status,
  CASE
    WHEN TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP()) < 720
    THEN CONCAT(ROUND((720 - TIMESTAMPDIFF(MINUTE, i.detected_ts, CURRENT_TIMESTAMP())) / 60.0, 1), 'h to 12h escalation')
    ELSE 'ESCALATED'
  END AS dwi_12h_status,
  r.dwi_notified_ts
FROM water_digital_twin.gold.dim_incidents            i
LEFT JOIN water_digital_twin.gold.regulatory_notifications r
  ON i.incident_id = r.incident_id
WHERE i.status = 'active'
ORDER BY hours_elapsed DESC;
```

**Display:** Conditional formatting -- red for EXCEEDED/ESCALATED cells.

---

### Tile 1.7: C-MeX Proactive Rate (Counter)

**Type:** Counter / Single value
**Description:** Percentage of customers who received proactive notification before contacting the company.

```sql
SELECT
  ROUND(
    SUM(n.proactive_notifications) * 100.0
    / NULLIF(SUM(n.proactive_notifications + n.reactive_complaints), 0),
    1
  ) AS proactive_rate_pct
FROM water_digital_twin.gold.dim_incidents          i
JOIN water_digital_twin.gold.incident_notifications n ON i.incident_id = n.incident_id
WHERE i.status = 'active';
```

**Display:** Percentage with green highlight if > 80%.

---

### Tile 1.8: Properties Exceeding 12 Hours (Counter)

**Type:** Counter / Single value
**Description:** Properties where supply interruption exceeds 12 hours (Category 1 DWI escalation).

```sql
SELECT
  COALESCE(SUM(properties_affected), 0) AS properties_over_12h
FROM water_digital_twin.gold.dim_incidents
WHERE status = 'active'
  AND TIMESTAMPDIFF(MINUTE, detected_ts, CURRENT_TIMESTAMP()) > 720;
```

**Display:** Large number, red if > 0. Subtitle: "Cat 1 DWI Escalation".

---

## Page 2: AMP8 Investment Insights

This page has 4 tiles focused on trend analysis and long-term performance patterns.

---

### Tile 2.1: Top 10 DMAs by Incident Count (Bar Chart)

**Type:** Bar chart (horizontal)
**Description:** DMAs ranked by total incident count in the current year.

```sql
SELECT
  dma_code,
  COUNT(*) AS incident_count,
  SUM(properties_affected) AS total_properties_affected
FROM water_digital_twin.gold.dim_incidents
WHERE detected_ts >= DATE_TRUNC('year', CURRENT_DATE())
GROUP BY dma_code
ORDER BY incident_count DESC
LIMIT 10;
```

**Configuration:**
- X axis: `incident_count`
- Y axis: `dma_code`
- Colour: Gradient by `total_properties_affected`

---

### Tile 2.2: Incident Trend (Line Chart)

**Type:** Line chart
**Description:** Monthly incident count trend for the current AMP period.

```sql
SELECT
  DATE_TRUNC('month', detected_ts) AS month,
  COUNT(*)                         AS incident_count,
  SUM(properties_affected)         AS total_properties_affected
FROM water_digital_twin.gold.dim_incidents
WHERE detected_ts >= '2025-04-01'
GROUP BY DATE_TRUNC('month', detected_ts)
ORDER BY month;
```

**Configuration:**
- X axis: `month`
- Y axis: `incident_count`
- Secondary Y axis (optional): `total_properties_affected`

---

### Tile 2.3: Anomaly Trends (Line Chart)

**Type:** Line chart
**Description:** Daily count of high-sigma anomaly detections across the network.

```sql
SELECT
  DATE_TRUNC('day', scored_ts) AS day,
  COUNT(*)                     AS anomaly_events,
  ROUND(AVG(anomaly_score), 2) AS avg_sigma
FROM water_digital_twin.gold.anomaly_scores
WHERE anomaly_score > 3
  AND scored_ts >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY DATE_TRUNC('day', scored_ts)
ORDER BY day;
```

**Configuration:**
- X axis: `day`
- Y axis: `anomaly_events`
- Secondary Y axis: `avg_sigma`

---

### Tile 2.4: Sensor Coverage (Table)

**Type:** Table
**Description:** Sensor deployment coverage by DMA with anomaly detection rates.

```sql
SELECT
  d.dma_code,
  d.dma_name,
  COUNT(DISTINCT s.sensor_id)   AS total_sensors,
  COUNT(DISTINCT CASE WHEN a.anomaly_score > 3 THEN s.sensor_id END) AS sensors_with_anomalies,
  ROUND(
    COUNT(DISTINCT CASE WHEN a.anomaly_score > 3 THEN s.sensor_id END) * 100.0
    / NULLIF(COUNT(DISTINCT s.sensor_id), 0),
    1
  ) AS anomaly_rate_pct
FROM water_digital_twin.silver.dim_dma       d
JOIN water_digital_twin.silver.dim_sensor    s ON d.dma_code = s.dma_code
LEFT JOIN water_digital_twin.gold.anomaly_scores a
  ON s.sensor_id = a.sensor_id
  AND a.scored_ts >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY d.dma_code, d.dma_name
ORDER BY total_sensors DESC;
```

**Display:** Sort by total sensors descending. Highlight rows where `anomaly_rate_pct` > 50%.

---

## Step 3 -- Publish

1. Review all tiles render correctly with the demo data.
2. Click **Publish** to make the dashboard available to workspace users.
3. Optionally schedule a refresh cadence (e.g., every 15 minutes) for live demo scenarios.

---

## Verification Checklist

| Item | Expected |
|---|---|
| Page 1 - Active Incidents counter | >= 1 |
| Page 1 - Properties affected | 441+ |
| Page 1 - Properties >3h | 441 (current active incident) |
| Page 1 - Penalty estimate | ~£180K |
| Page 1 - DMA map | DEMO_DMA_01 shown in red |
| Page 1 - Regulatory table | Shows EXCEEDED for 3h threshold |
| Page 1 - C-MeX rate | High percentage (>80%) |
| Page 1 - Properties >12h | 0 or low count |
| Page 2 - Top 10 DMAs | DEMO_DMA_01 present |
| Page 2 - Incident trend | Shows monthly data from Apr 2025 |
| Page 2 - Anomaly trends | Shows daily anomaly counts |
| Page 2 - Sensor coverage | All DMAs listed with sensor counts |
| Filters | Time range, DMA, and severity filters functional |

---

## Previous Steps

- [01 -- Workspace Setup Guide](01_workspace_setup_guide.md)
- [02 -- Orchestration Guide](02_orchestration_guide.md)
- [03 -- Genie Operator Guide](03_genie_operator_guide.md)
- [04 -- Genie Executive Guide](04_genie_executive_guide.md)
