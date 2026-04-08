# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Operator Space Guide
# MAGIC 
# MAGIC **Water Utilities -- Digital Twin Demo**
# MAGIC 
# MAGIC Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
# MAGIC CLI profile: `adb-98`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC 
# MAGIC This guide creates the **"Network Operations"** Genie Space used by water network operators and engineers to query real-time telemetry, DMA health, and anomaly data using natural language.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Create the Genie Space
# MAGIC 
# MAGIC 1. In the workspace sidebar, navigate to **Genie**.
# MAGIC 2. Click **New** (or **Create Genie Space**).
# MAGIC 3. Set the name: **Network Operations**.
# MAGIC 4. Set the description:
# MAGIC    > Real-time operational intelligence for Water Utilities network operators. Query DMA health, sensor telemetry, pressure trends, anomaly scores, and asset status across the distribution network.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Add Trusted Assets
# MAGIC 
# MAGIC Add the following 16 tables and metric views as trusted assets. These are the only objects Genie will use to answer questions.
# MAGIC 
# MAGIC ### Metric Views (gold schema)
# MAGIC 
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 1 | `water_digital_twin.gold.mv_pressure_avg_by_dma` | Average pressure by DMA over time |
# MAGIC | 2 | `water_digital_twin.gold.mv_flow_rate_by_dma` | Flow rates aggregated by DMA |
# MAGIC | 3 | `water_digital_twin.gold.mv_anomaly_count_by_dma` | Count of anomaly events per DMA |
# MAGIC | 4 | `water_digital_twin.gold.mv_sensor_uptime` | Sensor availability and uptime metrics |
# MAGIC | 5 | `water_digital_twin.gold.mv_dma_rag_summary` | RAG status summary across all DMAs |
# MAGIC | 6 | `water_digital_twin.gold.mv_incident_duration` | Duration metrics for active and resolved incidents |
# MAGIC | 7 | `water_digital_twin.gold.mv_properties_affected` | Properties affected by active incidents |
# MAGIC | 8 | `water_digital_twin.gold.mv_reservoir_levels` | Current reservoir levels and trends |
# MAGIC 
# MAGIC ### Silver Dimension Tables
# MAGIC 
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 9 | `water_digital_twin.silver.dim_dma` | DMA reference data (codes, names, centroids, boundaries) |
# MAGIC | 10 | `water_digital_twin.silver.dim_sensor` | Sensor registry (IDs, types, locations, DMA assignments) |
# MAGIC | 11 | `water_digital_twin.silver.dim_properties` | Property register (residential, schools, hospitals, etc.) |
# MAGIC | 12 | `water_digital_twin.silver.dim_assets` | Infrastructure assets (pumps, valves, treatment works) |
# MAGIC 
# MAGIC ### Gold Operational Tables
# MAGIC 
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 13 | `water_digital_twin.gold.dma_status` | Current RAG status for every DMA |
# MAGIC | 14 | `water_digital_twin.gold.dma_rag_history` | Historical RAG snapshots with pressure comparisons |
# MAGIC | 15 | `water_digital_twin.gold.anomaly_scores` | Sensor-level anomaly scores (sigma-based) |
# MAGIC | 16 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Set the System Prompt
# MAGIC 
# MAGIC Paste the following system prompt into the Genie Space configuration:
# MAGIC 
# MAGIC ```
# MAGIC You are a water network operations assistant for Water Utilities. You help operators and engineers understand the real-time state of the distribution network.
# MAGIC 
# MAGIC Key domain concepts:
# MAGIC - DMA (District Metered Area): A discrete zone of the water distribution network with metered inflows and outflows. Each DMA has a unique code (e.g., DEMO_DMA_01).
# MAGIC - PMA (Pressure Management Area): A sub-zone within a DMA where pressure is actively managed via PRVs (Pressure Reducing Valves).
# MAGIC - RAG Status: Red/Amber/Green health classification for each DMA.
# MAGIC   - RED: Significant pressure drop or supply interruption detected. Immediate attention required.
# MAGIC   - AMBER: Pressure trending below normal thresholds. Monitoring required.
# MAGIC   - GREEN: Operating within normal parameters.
# MAGIC - Pressure: Measured in metres head (m). Normal range is typically 15-60 m. Below 10 m indicates likely supply loss.
# MAGIC - Flow rate: Measured in litres per second (l/s).
# MAGIC - Anomaly score: Expressed in standard deviations (sigma). Scores above 3 sigma indicate statistically significant deviations warranting investigation.
# MAGIC - Sensitive properties: Schools, hospitals, and care homes require priority attention during supply interruptions.
# MAGIC 
# MAGIC Data model notes:
# MAGIC - Telemetry data is in fact_telemetry (silver schema) with sensor_id, metric, value, unit, event_ts.
# MAGIC - DMA-to-asset relationships use bridge tables: dim_asset_dma_feed, dim_reservoir_dma_feed.
# MAGIC - Spatial queries use ST_Distance on centroid geometry columns in dim_dma.
# MAGIC - The demo snapshot time is 2026-04-07 05:30:00 UTC. CURRENT_TIMESTAMP() reflects this.
# MAGIC 
# MAGIC When answering:
# MAGIC - Always include units (m, l/s, sigma, %, etc.).
# MAGIC - When referencing DMAs, include both the code and name.
# MAGIC - For pressure questions, clarify whether the value is current or historical.
# MAGIC - Flag any RED status DMAs prominently.
# MAGIC - For sensitive property queries, always break down by property type (school, hospital, care home).
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Add Sample Questions
# MAGIC 
# MAGIC Add these 10 sample questions to help users discover the Space's capabilities:
# MAGIC 
# MAGIC 1. Which DMAs had the biggest pressure drop in the last 6 hours?
# MAGIC 2. How many hospitals and schools are in DEMO_DMA_01?
# MAGIC 3. Show pressure trend for DEMO_SENSOR_01 over last 24 hours
# MAGIC 4. Which pump stations feed DMAs that are currently red?
# MAGIC 5. Current reservoir level for red/amber DMAs?
# MAGIC 6. All DMAs within 5 km of DEMO_DMA_01
# MAGIC 7. Properties without supply for more than 3 hours?
# MAGIC 8. Schools in affected DMAs
# MAGIC 9. Flow rate at DEMO_DMA_01 entry at 2 am vs now?
# MAGIC 10. Sensors in DEMO_DMA_01 with anomaly scores above 3 sigma?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Add Verified Queries
# MAGIC 
# MAGIC Verified queries ensure Genie returns exact, tested SQL for critical questions. These are stored in `genie/operator_verified_queries.sql` in the project repo.
# MAGIC 
# MAGIC For each verified query:
# MAGIC 
# MAGIC 1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
# MAGIC 2. Click **Add verified query**.
# MAGIC 3. Enter the **natural language question** as the trigger phrase.
# MAGIC 4. Paste the corresponding SQL from `genie/operator_verified_queries.sql`.
# MAGIC 5. Add a brief description of the expected result.
# MAGIC 6. Click **Save**.
# MAGIC 
# MAGIC Repeat for all 10 queries (Q1-Q10).
# MAGIC 
# MAGIC > **Tip:** Verified queries take priority over Genie-generated SQL. If a user's question closely matches a verified query trigger, Genie will use the verified SQL verbatim.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC 
# MAGIC Test the Space by asking each of the 10 sample questions and confirming results match expected values:
# MAGIC 
# MAGIC | Question | Key expected result |
# MAGIC |---|---|
# MAGIC | Q1 - Biggest pressure drop | DEMO_DMA_01: ~45 m to ~8 m |
# MAGIC | Q2 - Hospitals/schools in DMA_01 | 2+ schools, 1+ hospital |
# MAGIC | Q3 - Pressure trend SENSOR_01 | ~45-55 m until 02:00, then ~5-10 m |
# MAGIC | Q4 - Pump stations feeding red DMAs | DEMO_PUMP_01 |
# MAGIC | Q5 - Reservoir levels | DEMO_SR_01 at ~43% |
# MAGIC | Q6 - Nearby DMAs | Neighbours within 5 km |
# MAGIC | Q7 - Properties >3h without supply | 312+ properties |
# MAGIC | Q8 - Schools in affected DMAs | 2+ in DEMO_DMA_01 |
# MAGIC | Q9 - Flow rate 2am vs now | ~45 l/s to ~12 l/s |
# MAGIC | Q10 - Anomaly scores >3 sigma | DEMO_SENSOR_01 + others |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC 
# MAGIC Proceed to [04 -- Genie Executive Guide](04_genie_executive_guide.md) to create the executive Genie Space.
