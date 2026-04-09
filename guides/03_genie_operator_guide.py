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
# MAGIC
# MAGIC > **Best practice:** Test all configuration changes in a **cloned Space** before applying them to the production Space. In the Genie sidebar, select your Space → **Clone** to create a safe copy.

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
# MAGIC ## Step 2 -- Configure Unity Catalog Metadata
# MAGIC
# MAGIC Quality metadata is critical for Genie accuracy. Before adding tables to the Space, verify the following in Unity Catalog:
# MAGIC
# MAGIC ### Primary/Foreign Key Constraints
# MAGIC
# MAGIC Set PK/FK constraints so Genie understands table relationships and generates correct joins:
# MAGIC
# MAGIC ```sql
# MAGIC -- Primary keys
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ADD CONSTRAINT pk_dim_dma PRIMARY KEY (dma_code);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ADD CONSTRAINT pk_dim_sensor PRIMARY KEY (sensor_id);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ADD CONSTRAINT pk_dim_properties PRIMARY KEY (property_id);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ADD CONSTRAINT pk_dim_assets PRIMARY KEY (asset_id);
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT pk_dim_incidents PRIMARY KEY (incident_id);
# MAGIC
# MAGIC -- Foreign keys (Genie uses these to plan joins)
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ADD CONSTRAINT fk_sensor_dma FOREIGN KEY (dma_code) REFERENCES water_digital_twin.silver.dim_dma(dma_code);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ADD CONSTRAINT fk_property_dma FOREIGN KEY (dma_code) REFERENCES water_digital_twin.silver.dim_dma(dma_code);
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT fk_incident_dma FOREIGN KEY (dma_code) REFERENCES water_digital_twin.silver.dim_dma(dma_code);
# MAGIC ```
# MAGIC
# MAGIC ### Column Descriptions
# MAGIC
# MAGIC Review AI-generated column descriptions in the Genie Space UI. Override any that are vague or incorrect — Genie uses these to disambiguate user questions. Pay special attention to:
# MAGIC
# MAGIC - `value` vs `flow_rate` in `fact_telemetry` — Genie must know `value` is pressure (metres head) and `flow_rate` is flow (l/s)
# MAGIC - `anomaly_sigma` in `anomaly_scores` — clarify this is standard deviations, not a raw score
# MAGIC - `is_sensitive_premise` in `dim_properties` — clarify this flags hospitals, schools, care homes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Add Trusted Assets
# MAGIC
# MAGIC Add the following 19 tables and metric views as trusted assets. These are the only objects Genie will use to answer questions.
# MAGIC
# MAGIC ### Metric Views (gold schema)
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 1 | `water_digital_twin.gold.mv_dma_pressure` | Average, min, and max pressure by DMA over time |
# MAGIC | 2 | `water_digital_twin.gold.mv_flow_anomaly` | Flow rate deviation by DMA entry point |
# MAGIC | 3 | `water_digital_twin.gold.mv_anomaly_scores` | Sensor-level anomaly scores and detection rates |
# MAGIC | 4 | `water_digital_twin.gold.mv_sensor_status` | Sensor availability and uptime by DMA |
# MAGIC | 5 | `water_digital_twin.gold.mv_regulatory_compliance` | RAG status distribution, property counts, sensitive premises |
# MAGIC | 6 | `water_digital_twin.gold.mv_incident_summary` | Incident duration, threshold breaches, and properties affected |
# MAGIC | 7 | `water_digital_twin.gold.mv_reservoir_status` | Reservoir levels, capacity, and estimated hours remaining |
# MAGIC
# MAGIC ### Silver Fact Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 8 | `water_digital_twin.silver.fact_telemetry` | Sensor telemetry readings (pressure in metres head, flow in l/s) |
# MAGIC
# MAGIC ### Silver Dimension Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 9 | `water_digital_twin.silver.dim_dma` | DMA reference data (codes, names, centroids, boundaries) |
# MAGIC | 10 | `water_digital_twin.silver.dim_sensor` | Sensor registry (IDs, types, locations, DMA assignments) |
# MAGIC | 11 | `water_digital_twin.silver.dim_properties` | Property register (residential, schools, hospitals, etc.) |
# MAGIC | 12 | `water_digital_twin.silver.dim_assets` | Infrastructure assets (pumps, valves, treatment works) |
# MAGIC | 13 | `water_digital_twin.silver.dim_reservoirs` | Service reservoir metadata (capacity, current level, demand rate) |
# MAGIC | 14 | `water_digital_twin.silver.dim_asset_dma_feed` | Asset-to-DMA feed relationships (which assets serve which DMAs) |
# MAGIC | 15 | `water_digital_twin.silver.dim_reservoir_dma_feed` | Reservoir-to-DMA feed topology (primary/secondary feeds) |
# MAGIC
# MAGIC ### Gold Operational Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 16 | `water_digital_twin.gold.dma_status` | Current RAG status for every DMA |
# MAGIC | 17 | `water_digital_twin.gold.dma_rag_history` | Historical RAG snapshots with pressure comparisons |
# MAGIC | 18 | `water_digital_twin.gold.anomaly_scores` | Sensor-level anomaly scores (sigma-based) |
# MAGIC | 19 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Configure Knowledge Store (SQL Expressions)
# MAGIC
# MAGIC SQL expressions have the **highest priority** in Genie's instruction hierarchy (above example SQL and text instructions). Navigate to **Configure → Context → SQL Expressions** and add the following.
# MAGIC
# MAGIC > **Limit:** Up to 200 SQL expressions per Space. Use these for business-critical definitions.
# MAGIC
# MAGIC ### Measures
# MAGIC
# MAGIC | Name | Expression | Description |
# MAGIC |---|---|---|
# MAGIC | `avg_pressure_m` | `AVG(avg_pressure)` | Average pressure in metres head across sensors in a DMA |
# MAGIC | `min_pressure_m` | `MIN(min_pressure)` | Minimum pressure — low values indicate supply issues |
# MAGIC | `avg_flow_rate_ls` | `AVG(flow_rate)` | Average flow rate in litres per second |
# MAGIC | `anomaly_count` | `SUM(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END)` | Number of readings flagged as anomalous (> 2.5 sigma) |
# MAGIC | `sensor_uptime_pct` | `ROUND(SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)` | Percentage of sensors that are operational |
# MAGIC | `properties_affected` | `SUM(total_properties_affected)` | Total properties affected by incidents |
# MAGIC | `reservoir_hours_remaining` | `MIN(hours_remaining)` | Worst-case hours of supply remaining across reservoirs |
# MAGIC
# MAGIC ### Filters
# MAGIC
# MAGIC | Name | Expression | Description |
# MAGIC |---|---|---|
# MAGIC | `red_dmas` | `rag_status = 'RED'` | DMAs in RED status — immediate attention required |
# MAGIC | `amber_dmas` | `rag_status = 'AMBER'` | DMAs in AMBER status — monitoring required |
# MAGIC | `high_anomaly` | `anomaly_sigma > 3` | Readings with anomaly score above 3 sigma |
# MAGIC | `active_incidents` | `status = 'active'` | Currently active incidents |
# MAGIC | `sensitive_premises` | `property_type IN ('school', 'hospital', 'care_home', 'dialysis_home')` | Sensitive premises requiring priority response |
# MAGIC | `pressure_sensors` | `sensor_type = 'pressure'` | Pressure measurement sensors only |
# MAGIC | `flow_sensors` | `sensor_type = 'flow'` | Flow measurement sensors only |
# MAGIC
# MAGIC ### Dimensions
# MAGIC
# MAGIC | Name | Expression | Description |
# MAGIC |---|---|---|
# MAGIC | `dma_code` | `dma_code` | District Metered Area identifier (e.g., DEMO_DMA_01) |
# MAGIC | `sensor_type` | `sensor_type` | Sensor type: pressure or flow |
# MAGIC | `rag_status` | `rag_status` | RED/AMBER/GREEN health classification |
# MAGIC | `property_type` | `property_type` | Property classification: domestic, school, hospital, care_home, etc. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Set the System Prompt
# MAGIC
# MAGIC Paste the following into the **General Instructions** section of the Genie Space configuration. Keep text instructions minimal — SQL expressions (Step 4) handle metric definitions.
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
# MAGIC - Telemetry data is in fact_telemetry (silver schema) with columns: sensor_id, dma_code, timestamp, sensor_type, value, total_head_pressure, flow_rate, quality_flag.
# MAGIC - For pressure sensors: use the 'value' column (metres head). For flow sensors: use 'flow_rate' (l/s).
# MAGIC - DMA-to-asset relationships use bridge tables: dim_asset_dma_feed, dim_reservoir_dma_feed.
# MAGIC - Anomaly scores use 'anomaly_sigma' (not 'anomaly_score') and 'timestamp' (not 'scored_ts').
# MAGIC - Incidents use 'start_timestamp' / 'end_timestamp' (not 'detected_ts' / 'resolved_ts') and 'total_properties_affected' (not 'properties_affected').
# MAGIC - The demo snapshot time is 2026-04-07 05:30:00 UTC. CURRENT_TIMESTAMP() reflects this.
# MAGIC
# MAGIC Geospatial data:
# MAGIC - All geometry data uses WGS84 coordinate reference system (SRID 4326).
# MAGIC - Always use geometry columns (centroid in dim_dma) for spatial operations — never use raw centroid_latitude / centroid_longitude columns directly.
# MAGIC - ST_Point takes longitude as the first argument: ST_Point(longitude, latitude).
# MAGIC - ST_Distance returns metres when used with geometry columns.
# MAGIC - Use ST_Transform(geometry, target_srid) when mixing coordinate reference systems.
# MAGIC
# MAGIC Clarification rules:
# MAGIC - When users ask about pressure without specifying a DMA or sensor, ask: "Which DMA or sensor would you like pressure data for, or should I show all DMAs?"
# MAGIC - When users ask about "recent" data without a timeframe, assume last 24 hours but confirm: "Showing the last 24 hours — would you like a different timeframe?"
# MAGIC - When users mention "affected properties" without specifying an incident, use the most recent active incident.
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
# MAGIC ## Step 6 -- Add Sample Questions
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
# MAGIC ## Step 7 -- Add Verified Queries
# MAGIC
# MAGIC Verified queries ensure Genie returns exact, tested SQL for critical questions. These are stored in `genie/operator_verified_queries.sql` in the project repo.
# MAGIC
# MAGIC For each verified query:
# MAGIC
# MAGIC 1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
# MAGIC 2. Click **Add verified query**.
# MAGIC 3. Enter the **natural language question** as the trigger phrase — use natural phrasing for better prompt matching.
# MAGIC 4. Paste the corresponding SQL from `genie/operator_verified_queries.sql`.
# MAGIC 5. Add a brief description of the expected result.
# MAGIC 6. Click **Save**.
# MAGIC
# MAGIC Repeat for all 10 queries (Q1-Q10).
# MAGIC
# MAGIC ### Parameterized Queries
# MAGIC
# MAGIC Where a query takes a dynamic value (DMA code, sensor ID, timeframe), use `:parameter_name` syntax to make it reusable. When Genie matches a parameterized query, the response gets the **"Trusted"** label.
# MAGIC
# MAGIC Examples:
# MAGIC
# MAGIC | Trigger phrase | Parameter | Type |
# MAGIC |---|---|---|
# MAGIC | "How many hospitals and schools are in `:dma_code`?" | `:dma_code` | String |
# MAGIC | "Pressure trend for `:sensor_id` over last 24 hours" | `:sensor_id` | String |
# MAGIC | "Sensors in `:dma_code` with anomaly scores above 3 sigma" | `:dma_code` | String |
# MAGIC
# MAGIC > **Tip:** Verified queries take priority over Genie-generated SQL. If a user's question closely matches a verified query trigger, Genie will use the verified SQL verbatim.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 -- Build Benchmark Evaluation Set
# MAGIC
# MAGIC Benchmarks are the primary tool for measuring and improving Genie accuracy. Each Space supports up to **500 benchmark questions**.
# MAGIC
# MAGIC ### Create Benchmarks
# MAGIC
# MAGIC 1. In the Genie Space, click **Benchmarks**.
# MAGIC 2. Click **Add benchmark**.
# MAGIC 3. Enter a test question.
# MAGIC 4. Paste the gold-standard SQL answer (from the verified queries file).
# MAGIC 5. Click **Run** to verify results, then **Save**.
# MAGIC
# MAGIC ### Add Rephrasings
# MAGIC
# MAGIC For each of the 10 sample questions, add **2-3 rephrasings** to test robustness. Users ask the same question in different ways — the Space must handle all of them.
# MAGIC
# MAGIC | Original question | Rephrasings |
# MAGIC |---|---|
# MAGIC | Q1 — Biggest pressure drop in 6h | "Which DMAs lost the most pressure recently?", "Where has pressure fallen the most?" |
# MAGIC | Q2 — Hospitals/schools in DMA_01 | "Sensitive premises in Crystal Palace South?", "How many schools and hospitals are in DMA 01?" |
# MAGIC | Q3 — Pressure trend SENSOR_01 | "What happened to pressure at sensor 01?", "Graph pressure for DEMO_SENSOR_01 today" |
# MAGIC | Q4 — Pumps feeding red DMAs | "Which pumps serve the red zones?", "Pump stations connected to DMAs that are red" |
# MAGIC | Q5 — Reservoir levels | "How full are the reservoirs for affected DMAs?", "Water levels for red and amber areas" |
# MAGIC | Q6 — Nearby DMAs | "What's near DEMO_DMA_01?", "DMAs close to Crystal Palace South" |
# MAGIC | Q7 — Properties >3h no supply | "How many homes have been without water for over 3 hours?", "Properties exceeding the Ofwat threshold" |
# MAGIC | Q8 — Schools in affected DMAs | "Which schools are impacted?", "Schools in red or amber DMAs" |
# MAGIC | Q9 — Flow rate 2am vs now | "Compare flow at DMA 01 entry overnight", "What was flow like before the incident vs now?" |
# MAGIC | Q10 — Anomaly >3σ in DMA_01 | "High anomaly sensors in Crystal Palace?", "Which sensors have unusual readings in DMA 01?" |
# MAGIC
# MAGIC ### Run and Evaluate
# MAGIC
# MAGIC 1. Click **Run Benchmarks** to evaluate all questions at once.
# MAGIC 2. Review results: **Good** means Genie's SQL matches the gold standard. **Bad** means the response diverged.
# MAGIC 3. For each **Bad** result, examine the generated SQL to understand what Genie misinterpreted.
# MAGIC 4. Fix by adding/refining SQL expressions (Step 4), example queries (Step 7), or text instructions (Step 5) — in that priority order.
# MAGIC 5. Re-run benchmarks after every change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 -- Monitor and Iterate
# MAGIC
# MAGIC After sharing the Space with users, use the **Monitoring** tab to continuously improve accuracy.
# MAGIC
# MAGIC ### Weekly Review Checklist
# MAGIC
# MAGIC 1. **Review user questions** — check the Monitoring tab for questions that produced unexpected results or errors.
# MAGIC 2. **Check "Ask for Review" flags** — users can flag responses they're unsure about. Review these first.
# MAGIC 3. **Add failing questions as benchmarks** — any question that produces incorrect SQL should be added to the benchmark set with the correct SQL answer.
# MAGIC 4. **Refine instructions** — if a pattern of failures emerges (e.g., Genie misunderstanding "pressure drop"), add a targeted SQL expression or example query.
# MAGIC 5. **Re-run benchmarks** — after any change, run the full benchmark suite to confirm the fix didn't break other queries.
# MAGIC
# MAGIC ### Audit Order
# MAGIC
# MAGIC When investigating accuracy issues, audit in this order:
# MAGIC
# MAGIC 1. **SQL expressions** — are the measure/filter/dimension definitions correct and complete?
# MAGIC 2. **Example SQL queries** — does a verified query cover this question?
# MAGIC 3. **General instructions** — is there missing context Genie needs?
# MAGIC
# MAGIC > **Tip:** SQL expressions have the highest priority. If you can fix an issue with a SQL expression, prefer that over adding text instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference: Expected Results
# MAGIC
# MAGIC Use these expected values when building benchmarks and validating results:
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
# MAGIC Proceed to [04 -- Genie Executive Guide](04_genie_executive_guide.py) to create the executive Genie Space.
