# Genie Operator Space Guide

**Water Utilities -- Digital Twin Demo**

Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
CLI profile: `adb-98`

---

## Overview

This guide creates the **"Network Operations"** Genie Space used by water network operators and engineers to query real-time telemetry, DMA health, and anomaly data using natural language.

---

## Step 1 -- Create the Genie Space

1. In the workspace sidebar, navigate to **Genie**.
2. Click **New** (or **Create Genie Space**).
3. Set the name: **Network Operations**.
4. Set the description:
   > Real-time operational intelligence for Water Utilities network operators. Query DMA health, sensor telemetry, pressure trends, anomaly scores, and asset status across the distribution network.

---

## Step 2 -- Add Trusted Assets

Add the following 16 tables and metric views as trusted assets. These are the only objects Genie will use to answer questions.

### Metric Views (gold schema)

| # | Asset | Description |
|---|---|---|
| 1 | `water_digital_twin.gold.mv_pressure_avg_by_dma` | Average pressure by DMA over time |
| 2 | `water_digital_twin.gold.mv_flow_rate_by_dma` | Flow rates aggregated by DMA |
| 3 | `water_digital_twin.gold.mv_anomaly_count_by_dma` | Count of anomaly events per DMA |
| 4 | `water_digital_twin.gold.mv_sensor_uptime` | Sensor availability and uptime metrics |
| 5 | `water_digital_twin.gold.mv_dma_rag_summary` | RAG status summary across all DMAs |
| 6 | `water_digital_twin.gold.mv_incident_duration` | Duration metrics for active and resolved incidents |
| 7 | `water_digital_twin.gold.mv_properties_affected` | Properties affected by active incidents |
| 8 | `water_digital_twin.gold.mv_reservoir_levels` | Current reservoir levels and trends |

### Silver Dimension Tables

| # | Asset | Description |
|---|---|---|
| 9 | `water_digital_twin.silver.dim_dma` | DMA reference data (codes, names, centroids, boundaries) |
| 10 | `water_digital_twin.silver.dim_sensor` | Sensor registry (IDs, types, locations, DMA assignments) |
| 11 | `water_digital_twin.silver.dim_properties` | Property register (residential, schools, hospitals, etc.) |
| 12 | `water_digital_twin.silver.dim_assets` | Infrastructure assets (pumps, valves, treatment works) |

### Gold Operational Tables

| # | Asset | Description |
|---|---|---|
| 13 | `water_digital_twin.gold.dma_status` | Current RAG status for every DMA |
| 14 | `water_digital_twin.gold.dma_rag_history` | Historical RAG snapshots with pressure comparisons |
| 15 | `water_digital_twin.gold.anomaly_scores` | Sensor-level anomaly scores (sigma-based) |
| 16 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records |

---

## Step 3 -- Set the System Prompt

Paste the following system prompt into the Genie Space configuration:

```
You are a water network operations assistant for Water Utilities. You help operators and engineers understand the real-time state of the distribution network.

Key domain concepts:
- DMA (District Metered Area): A discrete zone of the water distribution network with metered inflows and outflows. Each DMA has a unique code (e.g., DEMO_DMA_01).
- PMA (Pressure Management Area): A sub-zone within a DMA where pressure is actively managed via PRVs (Pressure Reducing Valves).
- RAG Status: Red/Amber/Green health classification for each DMA.
  - RED: Significant pressure drop or supply interruption detected. Immediate attention required.
  - AMBER: Pressure trending below normal thresholds. Monitoring required.
  - GREEN: Operating within normal parameters.
- Pressure: Measured in metres head (m). Normal range is typically 15-60 m. Below 10 m indicates likely supply loss.
- Flow rate: Measured in litres per second (l/s).
- Anomaly score: Expressed in standard deviations (sigma). Scores above 3 sigma indicate statistically significant deviations warranting investigation.
- Sensitive properties: Schools, hospitals, and care homes require priority attention during supply interruptions.

Data model notes:
- Telemetry data is in fact_telemetry (silver schema) with sensor_id, metric, value, unit, event_ts.
- DMA-to-asset relationships use bridge tables: dim_asset_dma_feed, dim_reservoir_dma_feed.
- Spatial queries use ST_Distance on centroid geometry columns in dim_dma.
- The demo snapshot time is 2026-04-07 05:30:00 UTC. CURRENT_TIMESTAMP() reflects this.

When answering:
- Always include units (m, l/s, sigma, %, etc.).
- When referencing DMAs, include both the code and name.
- For pressure questions, clarify whether the value is current or historical.
- Flag any RED status DMAs prominently.
- For sensitive property queries, always break down by property type (school, hospital, care home).
```

---

## Step 4 -- Add Sample Questions

Add these 10 sample questions to help users discover the Space's capabilities:

1. Which DMAs had the biggest pressure drop in the last 6 hours?
2. How many hospitals and schools are in DEMO_DMA_01?
3. Show pressure trend for DEMO_SENSOR_01 over last 24 hours
4. Which pump stations feed DMAs that are currently red?
5. Current reservoir level for red/amber DMAs?
6. All DMAs within 5 km of DEMO_DMA_01
7. Properties without supply for more than 3 hours?
8. Schools in affected DMAs
9. Flow rate at DEMO_DMA_01 entry at 2 am vs now?
10. Sensors in DEMO_DMA_01 with anomaly scores above 3 sigma?

---

## Step 5 -- Add Verified Queries

Verified queries ensure Genie returns exact, tested SQL for critical questions. These are stored in `genie/operator_verified_queries.sql` in the project repo.

For each verified query:

1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
2. Click **Add verified query**.
3. Enter the **natural language question** as the trigger phrase.
4. Paste the corresponding SQL from `genie/operator_verified_queries.sql`.
5. Add a brief description of the expected result.
6. Click **Save**.

Repeat for all 10 queries (Q1-Q10).

> **Tip:** Verified queries take priority over Genie-generated SQL. If a user's question closely matches a verified query trigger, Genie will use the verified SQL verbatim.

---

## Verification

Test the Space by asking each of the 10 sample questions and confirming results match expected values:

| Question | Key expected result |
|---|---|
| Q1 - Biggest pressure drop | DEMO_DMA_01: ~45 m to ~8 m |
| Q2 - Hospitals/schools in DMA_01 | 2+ schools, 1+ hospital |
| Q3 - Pressure trend SENSOR_01 | ~45-55 m until 02:00, then ~5-10 m |
| Q4 - Pump stations feeding red DMAs | DEMO_PUMP_01 |
| Q5 - Reservoir levels | DEMO_SR_01 at ~43% |
| Q6 - Nearby DMAs | Neighbours within 5 km |
| Q7 - Properties >3h without supply | 312+ properties |
| Q8 - Schools in affected DMAs | 2+ in DEMO_DMA_01 |
| Q9 - Flow rate 2am vs now | ~45 l/s to ~12 l/s |
| Q10 - Anomaly scores >3 sigma | DEMO_SENSOR_01 + others |

---

## Next Step

Proceed to [04 -- Genie Executive Guide](04_genie_executive_guide.md) to create the executive Genie Space.
