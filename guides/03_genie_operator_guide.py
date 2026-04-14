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
# MAGIC Add the following 20 tables and metric views as trusted assets. These are the only objects Genie will use to answer questions.
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
# MAGIC | 16 | `water_digital_twin.silver.customer_complaints` | Customer complaints by type (no_water, low_pressure, discoloured_water), DMA, channel, and resolution status |
# MAGIC
# MAGIC ### Gold Operational Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 17 | `water_digital_twin.gold.dma_status` | Current RAG status for every DMA |
# MAGIC | 18 | `water_digital_twin.gold.dma_rag_history` | Historical RAG snapshots with pressure comparisons |
# MAGIC | 19 | `water_digital_twin.gold.anomaly_scores` | Sensor-level anomaly scores (sigma-based) |
# MAGIC | 20 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records |

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
# MAGIC > **Important:** `avg_pressure` exists in multiple tables (`dma_status`, `dma_rag_history`, `vw_dma_pressure`, `dma_summary`). Use table-qualified expressions so Genie picks the right source.
# MAGIC
# MAGIC Each measure has four fields in the UI: **Name**, **Code** (SQL expression), **Synonyms** (comma-separated), and **Instructions**.
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `current_avg_pressure_m` | `AVG(dma_status.avg_pressure)` | current pressure, live pressure, pressure now | Use for "what's the pressure now" questions. Returns metres head. Source: dma_status (real-time snapshot). |
# MAGIC | `historical_avg_pressure_m` | `AVG(dma_rag_history.avg_pressure)` | past pressure, pressure history, pressure at time | Use for "what was the pressure at 2am" or trend questions. Returns metres head. Source: dma_rag_history (15-min snapshots). |
# MAGIC | `min_pressure_m` | `MIN(dma_status.min_pressure)` | lowest pressure, worst pressure, minimum pressure | Minimum pressure across sensors in a DMA. Low values (< 15m) indicate supply loss. Returns metres head. |
# MAGIC | `avg_flow_rate_ls` | `AVG(fact_telemetry.flow_rate)` | flow, flow rate, water flow | Average flow rate at DMA entry points. Returns litres per second (l/s). |
# MAGIC | `anomaly_count` | `SUM(CASE WHEN anomaly_scores.is_anomaly = true THEN 1 ELSE 0 END)` | anomalies, unusual readings, flagged readings | Count of readings exceeding 2.5 sigma threshold. Use for anomaly rates and detection summaries. |
# MAGIC | `sensor_uptime_pct` | `ROUND(SUM(CASE WHEN mv_sensor_latest.status = 'active' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)` | sensor availability, uptime, sensor health | Percentage of sensors that are operational (status = active). Returns 0-100%. |
# MAGIC | `properties_affected` | `SUM(dim_incidents.total_properties_affected)` | homes affected, customers impacted, properties impacted | Total properties affected by incidents. Count of distinct properties, not a percentage. |
# MAGIC | `reservoir_hours_remaining` | `MIN(vw_reservoir_status.hours_remaining)` | supply remaining, reservoir time left, hours of water left | Worst-case hours of supply remaining across reservoirs feeding a DMA. Use MIN to show the most urgent reservoir. |
# MAGIC
# MAGIC ### Filters
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `red_dmas` | `dma_status.rag_status = 'RED'` | critical zones, red zones, supply loss areas | RED = min_pressure < 15m. Immediate attention required — likely supply interruption. |
# MAGIC | `amber_dmas` | `dma_status.rag_status = 'AMBER'` | warning zones, amber zones, degraded areas | AMBER = min_pressure < 25m. Monitoring required — pressure below normal but supply maintained. |
# MAGIC | `high_anomaly` | `anomaly_scores.anomaly_sigma > 3` | high sigma, unusual sensors, 3 sigma readings | Readings more than 3 standard deviations from the 7-day rolling baseline. Warrants investigation. |
# MAGIC | `active_incidents` | `dim_incidents.status = 'active'` | ongoing incidents, current incidents, open incidents | Incidents that are currently active (not yet resolved). |
# MAGIC | `sensitive_premises` | `dim_properties.property_type IN ('school', 'hospital', 'care_home', 'dialysis_home')` | vulnerable properties, priority premises, sensitive properties | Properties requiring priority response during supply interruptions. Excludes domestic and commercial. |
# MAGIC | `pressure_sensors` | `dim_sensor.sensor_type = 'pressure'` | pressure monitors, pressure gauges | Filters to pressure sensors only. Reading unit: metres head (m). |
# MAGIC | `flow_sensors` | `dim_sensor.sensor_type = 'flow'` | flow meters, flow monitors | Filters to flow sensors only. Reading unit: litres per second (l/s). |
# MAGIC
# MAGIC ### Dimensions
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `dma_code` | `dim_dma.dma_code` | zone, district, area, DMA, metered area | District Metered Area identifier (e.g., DEMO_DMA_01). Primary grouping for all network data. |
# MAGIC | `sensor_type` | `dim_sensor.sensor_type` | sensor kind, measurement type | Two values: pressure (metres head) or flow (litres per second). |
# MAGIC | `rag_status` | `dma_status.rag_status` | health status, traffic light, RAG, status colour | RED/AMBER/GREEN health classification. RED is worst. |
# MAGIC | `property_type` | `dim_properties.property_type` | building type, premises type, property category | Values: domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account. |

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
# MAGIC ## Step 6 -- Configure Data Settings
# MAGIC
# MAGIC These settings improve Genie's ability to match natural language to specific column values, reduce noise, and join tables confidently.
# MAGIC
# MAGIC ### 6a -- Enable Entity Matching
# MAGIC
# MAGIC Navigate to **Configure → Data** and enable entity matching on these columns:
# MAGIC
# MAGIC | Column | Table(s) | Values |
# MAGIC |---|---|---|
# MAGIC | `rag_status` | `dma_status`, `dma_rag_history` | GREEN, AMBER, RED |
# MAGIC | `status` (incidents) | `dim_incidents` | active, resolved |
# MAGIC | `severity` | `dim_incidents` | low, medium, high, critical |
# MAGIC | `property_type` | `dim_properties` | domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account |
# MAGIC | `sensor_type` | `dim_sensor`, `fact_telemetry` | pressure, flow |
# MAGIC | `asset_type` | `dim_assets` | pump_station, trunk_main, isolation_valve, prv |
# MAGIC | `feed_type` | `dim_asset_dma_feed`, `dim_reservoir_dma_feed` | primary, secondary |
# MAGIC | `complaint_type` | `customer_complaints` | no_water, low_pressure, discoloured_water, other |
# MAGIC
# MAGIC > **Note:** Entity matching limits: max 120 columns per space, max 1,024 distinct values per column.
# MAGIC
# MAGIC ### 6b -- Hide Irrelevant Columns
# MAGIC
# MAGIC In **Configure → Data**, uncheck these columns:
# MAGIC
# MAGIC - `geometry_wkt` (all tables) — WKT text, not useful for NL queries
# MAGIC - `h3_index` (`dim_dma`, `dim_properties`) — internal spatial index
# MAGIC - `centroid_latitude`, `centroid_longitude` (`dim_dma`) — redundant with centroid geometry column
# MAGIC - `quality_flag` (`fact_telemetry`) — rarely queried, adds token noise
# MAGIC
# MAGIC ### 6c -- Add Join Definitions
# MAGIC
# MAGIC In **Configure → Data → Join Definitions**, add these joins. Even though PK/FK exists in UC, explicit join definitions give Genie highest confidence.
# MAGIC
# MAGIC | Left Table | Right Table | Left Column | Right Column | Relationship Type | Instructions |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `dim_incidents` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join incidents to DMA reference data for names and geographic context. |
# MAGIC | `dim_properties` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join properties to their DMA for area-level aggregation. |
# MAGIC | `dim_sensor` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join sensors to their DMA for area-level grouping. |
# MAGIC | `dim_asset_dma_feed` | `dim_assets` | `asset_id` | `asset_id` | Many to One | Join feed topology to asset details (name, type, status). |
# MAGIC | `dim_asset_dma_feed` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join feed topology to DMA for "which DMAs does this asset serve?" queries. |
# MAGIC | `dim_reservoir_dma_feed` | `dim_reservoirs` | `reservoir_id` | `reservoir_id` | Many to One | Join feed topology to reservoir details (capacity, level, hours remaining). |
# MAGIC | `dim_reservoir_dma_feed` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join feed topology to DMA for "which reservoirs feed this DMA?" queries. |
# MAGIC | `anomaly_scores` | `dim_sensor` | `sensor_id` | `sensor_id` | Many to One | Join anomaly scores to sensor metadata for type, location, and DMA context. |
# MAGIC | `fact_telemetry` | `dim_sensor` | `sensor_id` | `sensor_id` | Many to One | Join telemetry readings to sensor metadata. |
# MAGIC | `fact_telemetry` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join telemetry readings to DMA reference data. |
# MAGIC | `customer_complaints` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join complaints to DMA for area-level complaint analysis. |
# MAGIC | `customer_complaints` | `dim_properties` | `property_id` | `property_id` | Many to One | Join complaints to property details for type and location context. |
# MAGIC
# MAGIC ### 6d -- Add Column Synonyms
# MAGIC
# MAGIC In **Configure → Data → Column Synonyms**, add:
# MAGIC
# MAGIC | Table | Column | Synonyms |
# MAGIC |---|---|---|
# MAGIC | `dim_dma` | `dma_code` | zone, district, area, DMA, metered area, supply zone |
# MAGIC | `dma_status` | `rag_status` | health status, traffic light, RAG, status colour |
# MAGIC | `anomaly_scores` | `anomaly_sigma` | anomaly score, deviation, sigma, z-score |
# MAGIC | `dim_sensor` | `sensor_type` | sensor kind, measurement type |
# MAGIC | `dim_incidents` | `total_properties_affected` | homes affected, properties impacted, customers affected |
# MAGIC | `vw_dma_summary` | `sensitive_premises_count` | vulnerable properties, priority premises |
# MAGIC | `dim_properties` | `property_type` | building type, premises type, property category |
# MAGIC | `customer_complaints` | `complaint_type` | complaint reason, issue type, customer issue |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 -- Add Agent Questions
# MAGIC
# MAGIC Agent questions require Genie to run multiple queries and synthesize an analytical answer. These showcase multi-step reasoning.
# MAGIC
# MAGIC Add these as sample questions alongside the standard SQL questions:
# MAGIC
# MAGIC 1. What's causing the low pressure in DEMO_DMA_01?
# MAGIC 2. Give me a full situation report for the active incident
# MAGIC 3. Is the pressure situation getting better or worse?
# MAGIC 4. Which other DMAs might be affected if DEMO_PUMP_01 stays down?
# MAGIC 5. Are there early warning signs of problems in other DMAs?
# MAGIC
# MAGIC > **Tip:** Agent questions work best when UC metadata (table/column descriptions, PK/FK constraints) is complete. Ensure Steps 2 and 6 are done before testing agent questions.
# MAGIC
# MAGIC ### Agent Question Rephrasings
# MAGIC
# MAGIC Add rephrasings for benchmarks:
# MAGIC
# MAGIC | Agent question | Rephrasings |
# MAGIC |---|---|
# MAGIC | Root cause analysis | "Why is pressure low in DMA 01?", "Diagnose the DEMO_DMA_01 issue" |
# MAGIC | Situation report | "Situation report for DEMO_DMA_01", "What's the status of the current incident?" |
# MAGIC | Trajectory analysis | "Is the incident improving?", "Pressure trend analysis for the active incident" |
# MAGIC | Cascading risk | "Impact assessment if DEMO_PUMP_01 stays offline", "Downstream risk analysis" |
# MAGIC | Early warnings | "Network-wide anomaly scan", "Which DMAs show emerging problems?" |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 -- Add Sample Questions
# MAGIC
# MAGIC Add these 12 sample questions to help users discover the Space's capabilities:
# MAGIC
# MAGIC 1. Which DMAs had the biggest pressure drop in the last 6 hours?
# MAGIC 2. How many hospitals and schools are in DEMO_DMA_01?
# MAGIC 3. Show pressure trend for DEMO_SENSOR_01 over last 24 hours
# MAGIC 4. Which pump stations feed DMAs that are currently red?
# MAGIC 5. Current reservoir level for red/amber DMAs?
# MAGIC 6. Which DMAs share a reservoir or pump station with DEMO_DMA_01?
# MAGIC 7. Properties without supply for more than 3 hours?
# MAGIC 8. Schools in affected DMAs
# MAGIC 9. Flow into DEMO_DMA_01 at 2 am vs now?
# MAGIC 10. Any unusual sensor readings in DEMO_DMA_01?
# MAGIC 11. How many DMAs are red, amber, and green right now?
# MAGIC 12. When did the pressure drop start in DEMO_DMA_01?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 -- Add Verified Queries
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
# MAGIC Repeat for all 12 queries (Q1-Q12).
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
# MAGIC | "Unusual sensor readings in `:dma_code`" | `:dma_code` | String |
# MAGIC | "When did the pressure drop start in `:dma_code`?" | `:dma_code` | String |
# MAGIC
# MAGIC > **Tip:** Verified queries take priority over Genie-generated SQL. If a user's question closely matches a verified query trigger, Genie will use the verified SQL verbatim.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 -- Build Benchmark Evaluation Set
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
# MAGIC For each of the 12 sample questions, add **2-3 rephrasings** to test robustness. Users ask the same question in different ways — the Space must handle all of them.
# MAGIC
# MAGIC | Original question | Rephrasings |
# MAGIC |---|---|
# MAGIC | Q1 — Biggest pressure drop in 6h | "Which DMAs lost the most pressure recently?", "Where has pressure fallen the most?" |
# MAGIC | Q2 — Hospitals/schools in DMA_01 | "Sensitive premises in Crystal Palace South?", "How many schools and hospitals are in DMA 01?" |
# MAGIC | Q3 — Pressure trend SENSOR_01 | "What happened to pressure at sensor 01?", "Graph pressure for DEMO_SENSOR_01 today" |
# MAGIC | Q4 — Pumps feeding red DMAs | "Which pumps serve the red zones?", "Pump stations connected to DMAs that are red" |
# MAGIC | Q5 — Reservoir levels | "How full are the reservoirs for affected DMAs?", "Water levels for red and amber areas" |
# MAGIC | Q6 — Shared-feed DMAs | "What's connected to DEMO_DMA_01?", "Which zones share infrastructure with Crystal Palace South?" |
# MAGIC | Q7 — Properties >3h no supply | "How many homes have been without water for over 3 hours?", "Properties exceeding the Ofwat threshold" |
# MAGIC | Q8 — Schools in affected DMAs | "Which schools are impacted?", "Schools in red or amber DMAs" |
# MAGIC | Q9 — Flow 2am vs now | "What was the flow into DMA 01 before the incident?", "Compare overnight flow vs current for DMA 01" |
# MAGIC | Q10 — Unusual readings in DMA_01 | "Which sensors are flagged in DMA 01?", "Anything alarming in Crystal Palace South?" |
# MAGIC | Q11 — Network status | "RAG summary", "How's the network looking?" |
# MAGIC | Q12 — Pressure drop timeline | "When did pressure fall in Crystal Palace South?", "Pressure history for DMA 01 today" |
# MAGIC
# MAGIC ### Run and Evaluate
# MAGIC
# MAGIC 1. Click **Run Benchmarks** to evaluate all questions at once.
# MAGIC 2. Review results: **Good** means Genie's SQL matches the gold standard. **Bad** means the response diverged.
# MAGIC 3. For each **Bad** result, examine the generated SQL to understand what Genie misinterpreted.
# MAGIC 4. Fix by adding/refining SQL expressions (Step 4), example queries (Step 9), or text instructions (Step 5) — in that priority order.
# MAGIC 5. Re-run benchmarks after every change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 -- Monitor and Iterate
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
# MAGIC | Q6 - Shared-feed DMAs | DMAs sharing reservoirs or pump stations |
# MAGIC | Q7 - Properties >3h without supply | 312+ properties |
# MAGIC | Q8 - Schools in affected DMAs | 2+ in DEMO_DMA_01 |
# MAGIC | Q9 - Flow rate 2am vs now | ~45 l/s to ~12 l/s |
# MAGIC | Q10 - Unusual readings | DEMO_SENSOR_01 + others with anomaly_sigma > 3 |
# MAGIC | Q11 - Network RAG status | Mostly GREEN, 1+ RED (DEMO_DMA_01) |
# MAGIC | Q12 - Pressure drop timeline | Drop visible around 02:00 on 2026-04-07 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Proceed to [04 -- Genie Executive Guide](04_genie_executive_guide.py) to create the executive Genie Space.
