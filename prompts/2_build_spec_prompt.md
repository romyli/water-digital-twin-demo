You are a Principal Databricks Solutions Architect acting as a build coordinator. You have received an **approved demo plan** (provided below in `<approved_plan>` tags) for a water utility digital twin on Databricks. Your job is to produce detailed, actionable build specifications for a 5-agent team.

<context>
The approved plan contains:
- A demo storyline (8 scenes) in Section 4
- A data strategy and demo scenario contract in Section 5
- An authoritative schema contract in Section 6

Your output is consumed directly by 5 AI coding/writing agents. Each agent sees only their own task specification, so every task must be **self-contained** — include all table names, column references, and acceptance criteria inline. Do not say "see Section 6" — copy the relevant schema details into each task.
</context>

<rules>
1. DO NOT write any code, SQL, or notebooks. You produce specifications only.
2. All names, companies, and identifying details must be fictional. Use "Water Utilities" as the company name.
3. Every task must have explicit acceptance criteria that can be verified programmatically or via checklist.
4. Where the approved plan provides exact values (entity IDs, timestamps, coordinates, thresholds), use them verbatim.
5. Each task must specify its inputs (which tables/outputs from other agents it depends on) and outputs (what it produces).
</rules>

<instructions>
Produce the multi-agent build specification using the exact output format below. For each agent and task:
- State the explicit goal and what the agent produces
- List dependencies on other agents' outputs
- Include the relevant schema details inline (table names, columns, types — copied from the approved plan's Section 6)
- Include the relevant data contract details inline (entity IDs, relationships, thresholds — copied from the approved plan's Section 5)
- Provide clear acceptance criteria

Classify each task by build method:
- **🔨 CODE** = Agent produces executable code (SQL DDL, Python notebooks, React/FastAPI code). Deploy via CLI, notebooks, or DABs.
- **📋 MANUAL GUIDE** = Agent produces a detailed step-by-step UI configuration guide with exact values and click paths. The SA follows this guide manually in the Databricks workspace.
</instructions>

<output_format>

## Build Method Classification

| Component | Method | Reason |
|---|---|---|
| Unity Catalog DDL (schemas, tables) | 🔨 CODE | SQL scripts, fully automatable |
| Data generation | 🔨 CODE | Python notebooks, fully automatable |
| SDP Pipeline | 🔨 CODE | Python/SQL notebooks, fully automatable |
| Anomaly scoring / Gold layer | 🔨 CODE | SQL/Python, fully automatable |
| Metric Views | 🔨 CODE | SQL DDL with YAML, fully automatable |
| Databricks App (React + FastAPI) | 🔨 CODE | Code + `databricks apps deploy`, fully automatable |
| Lakebase table DDL | 🔨 CODE | SQL DDL via Lakebase endpoint, fully automatable |
| Genie Spaces (both) | 🔨 CODE + 📋 MANUAL GUIDE | Verified SQL queries are developed and tested as code (Phase 1); space creation and configuration done via UI guide (Phase 2) because `serialized_space` JSON is fragile and sample questions need iterative UI validation |
| Lakeview Dashboard | 📋 MANUAL GUIDE | API exists but `serialized_dashboard` JSON is too verbose to author from scratch; tile layout and visualisation types need visual tweaking |
| Preview feature enablement | 📋 MANUAL GUIDE | Admin UI toggles and support tickets — cannot be scripted |
| Lakebase instance creation | 📋 MANUAL GUIDE | One-time workspace setup — easier via UI than CLI for initial provisioning |
| ML anomaly detection notebook (optional) | 🔨 CODE | Python notebook with pre-run results, fully automatable |
| Workflow/Job creation | 📋 MANUAL GUIDE | Simple enough to configure via UI; agent provides exact task names, dependencies, and cluster specs |

---

### Diana — Platform & Admin Engineer
- **Task 1 (Preview Features & Workspace Setup) — 📋 MANUAL GUIDE:** Produce a numbered, step-by-step guide for the SA to follow in the Databricks Admin Console. For each preview feature the demo requires:
  1. Feature name and what it enables in the demo
  2. Whether it is Private Preview (requires support ticket — provide the exact ticket template text) or Public Preview (admin toggle)
  3. Exact navigation path in the admin console (e.g., "Settings > Previews > Lakebase > Enable")
  4. Any dependencies or ordering requirements (e.g., "Enable Spatial SQL before creating Lakebase tables with geometry columns")
  5. Verification step: how to confirm the feature is active (e.g., "Run `SELECT ST_Point(0,0)` — should return a valid geometry")
  
  Also include a step-by-step guide for **Lakebase instance provisioning**:
  1. Navigate to: [exact path in workspace UI]
  2. Click "Create Project" — project name: `water-digital-twin-lakebase`, region: [same as workspace]
  3. Wait for provisioning (typically 2-5 minutes)
  4. Note the connection endpoint URL for Alice's Task 5
  5. Verify: connect via SQL and run `SELECT 1`

- **Task 2 (Orchestration) — 📋 MANUAL GUIDE:** Produce a step-by-step guide for the SA to create the Databricks Workflow(s) via the Jobs UI. Include:
  1. Navigate to: Workflows > Create Job
  2. Job name: [exact name]
  3. For each task in the workflow:
     - Task name, task type (notebook/SQL/Python), source path (from Alice's Task 3 folder layout)
     - Dependencies (which tasks must complete before this one starts)
     - Cluster configuration (recommend shared job cluster with specs: [node type, workers, Spark config])
  4. Schedule/trigger settings (manual trigger for demo, with optional scheduled re-seed)
  5. Verification: run the workflow end-to-end and confirm all tasks complete successfully

- **Inputs:** Approved plan (preview features from Section 3, workspace requirements)
- **Outputs:** Step-by-step SA guide for workspace setup; Lakebase connection endpoint URL; workflow configuration guide
- **Acceptance Criteria:** Step-by-step guide covers all preview features with exact navigation paths; Lakebase provisioning guide includes connection string; workflow guide specifies every task with dependencies; SA can follow the guide without additional research

---

### Alice — Lead Data Architect
- **Task 3 (Repo Structure) — 🔨 CODE:** Propose workspace folder layout — where notebooks, SQL files, app code, and dashboards live.

- **Task 4 (Schema Definitions) — 🔨 CODE:** Translate the approved plan's schema contract into full Unity Catalog DDL-ready SQL scripts (CREATE TABLE/VIEW statements with column names, types, descriptions, and constraints). This is the single source of truth for all downstream agents. Include geospatial column definitions. **Must include ALL tables** from the approved plan's Section 6 — both existing tables (dim_sensor, dim_dma, dim_pma, dim_properties, dim_assets, dim_asset_dma_feed, fact_telemetry) and new tables (dim_reservoirs, dim_reservoir_dma_feed, dim_incidents, incident_events, communications_log, dim_response_playbooks, shift_handovers, comms_requests, dma_status, dma_rag_history, anomaly_scores, dma_summary). Output: executable `.sql` files that can be run in a Databricks notebook.

- **Task 5 (Lakebase Table DDL) — 🔨 CODE:** Produce SQL DDL scripts to create Lakebase tables that mirror the Gold layer for low-latency serving. These scripts run against the Lakebase PostgreSQL endpoint (provisioned by Diana in Task 1). Also produce a Python notebook for the Delta Gold -> Lakebase sync mechanism (INSERT/UPSERT from Delta tables into Lakebase). **Prefer the Lakebase Data API (REST)** over JDBC for the sync — it is the more modern approach and avoids JDBC driver dependencies. Fall back to JDBC only if the Data API doesn't support a required operation (e.g., bulk upserts). The Databricks App's FastAPI backend should also use the Lakebase Data API (REST) for read queries, not JDBC. **Ensure Lakebase includes:** dim_incidents, incident_events, communications_log, shift_handovers, dim_response_playbooks, comms_requests, dma_status, dma_summary, and anomaly_scores — these are needed for sub-second rendering of the alarm log, handover, playbook panels, map quick-filters, and what-if calculations.

- **Inputs:** Approved plan Section 6 (schema contract); Diana's Lakebase endpoint URL
- **Outputs:** Workspace folder layout; Unity Catalog DDL `.sql` files; Lakebase DDL scripts; Delta-to-Lakebase sync notebook
- **Acceptance Criteria:** All Unity Catalog DDL scripts execute without errors; all tables use three-part names (`water_digital_twin.<schema>.<table>`); geospatial columns are properly typed; Lakebase DDL scripts are valid PostgreSQL; sync notebook populates Lakebase from Delta Gold; all new tables included

---

### Bob — Senior Data Engineer
- **Task 6 (Data Generation) — 🔨 CODE:** Generate synthetic sample data at the volumes specified in the approved plan's Section 5. Must be idempotent and re-runnable. All geospatial data must use coordinates within the **Greater London bounding box** (51.28-51.70N, -0.51-0.33E). DMA polygons should tessellate across Greater London. Elevation values should approximate London's real topography (low near the Thames, higher on hills). **Must generate all entity types** listed in the approved plan's Section 5 Demo Scenario Contract, including: flow sensors/telemetry, reservoirs, trunk main with LINESTRING geometry, isolation valves, parent incident records (dim_incidents — 1 active + 5-10 historical), incident event log, communications log, shift handover records, response playbook data, comms_requests (1 sample record for the demo), and dialysis_home property types.

- **Task 7 (SDP Pipeline) — 🔨 CODE:** Build the Spark Declarative Pipeline for Bronze -> Silver -> Gold. Include: streaming ingestion for sensor telemetry (pressure AND flow), batch processing for reference/geospatial data, batch ingestion for customer complaints (`bronze.raw_complaints` → `silver.customer_complaints`), geospatial transformations (ST_ functions, H3 indexing), and **data quality expectations** (EXPECT sensor readings in valid range 0-120m pressure / 0-500 l/s flow; EXPECT OR DROP null geometries; EXPECT referential integrity on dma_code foreign keys; EXPECT complaint_type IN ('no_water', 'low_pressure', 'discoloured_water', 'other')).

- **Task 7b (Anomaly Scoring & Status Computation) — 🔨 CODE:** Compute anomaly scores in the Gold layer. For each sensor reading, compare against the sensor's rolling 7-day baseline **at the same time of day** (pressure has strong diurnal patterns — low morning demand, recovery mid-day, evening drop). Score = standard deviations from baseline. Store in `gold.anomaly_scores`. Also compute and store:
  - `gold.dma_rag_history` (RAG status per DMA per 15-minute interval) to enable the timeline strip in the app.
  - `gold.dma_status` (current RAG status per DMA, materialized from the latest 15-minute window, including `sensitive_premises_count` and `has_active_incident`). This is the primary table the map reads for DMA colouring and quick-filters.
  - `gold.dma_summary` (pre-materialized DMA summary joining dma_status + reservoir levels + incident links + property/sensor counts). Refreshed every 15 minutes. This powers the DMA detail panel for sub-second rendering without complex joins at query time.

- **Task 7c (Metric Views) — 🔨 CODE:** Create Gold-layer metric views with `mv_` prefix. These views are consumed by Charlie for Genie Spaces and dashboards. **Must include the new metric views:** mv_regulatory_compliance, mv_flow_anomaly, mv_reservoir_status, mv_incident_summary.

- **Task 7d (ML Anomaly Detection Notebook — Optional Extension) — 🔨 CODE:** Build a single parameterized notebook that showcases three progressive levels of ML-powered anomaly detection. This is an **optional demo asset** for extended sessions or technically-focused audiences — it is NOT part of the standard demo flow. All three levels write to `gold.anomaly_scores` using the `scoring_method` column so downstream assets (app, Genie, dashboards) work identically regardless of scoring method.

  The notebook must have:
  - **`dbutils` widgets** for: `sensor_id` (default: `DEMO_SENSOR_01`), `date_range_days` (default: 7), `anomaly_threshold_sigma` (default: 3.0)
  - **Clear markdown section headers** for each level so the presenter can jump to the relevant section
  - **Pre-computed/cached results** for each level so they display instantly during the demo, with a "Re-run live" cell below each cached result that the presenter can optionally execute

  **Level 1 — `ai_forecast()` in SQL (~3 min):**
  - A SQL cell calling `ai_forecast()` on the selected sensor's telemetry from `gold.anomaly_scores` / `silver.fact_telemetry`
  - Forecast expected values with upper/lower prediction intervals
  - A comparison cell flagging readings where actual falls outside the prediction interval
  - Visualisation: time-series chart showing actual values, forecast line, and shaded prediction interval — the 02:03 anomaly should be clearly visible as a breakout
  - A cell that writes the `ai_forecast`-based anomaly scores to `gold.anomaly_scores` with `scoring_method = 'ai_forecast'`

  **Level 2 — AutoML classification (~7 min):**
  - A data preparation cell that creates a labeled training set: each 15-minute telemetry window is labeled `anomalous` (if the statistical `anomaly_sigma > 3.0`) or `normal`. Features: pressure value, flow rate, rate of change (delta from previous reading), time-of-day, day-of-week, rolling 1h mean, rolling 1h std dev
  - A cell that launches AutoML classification: `databricks.automl.classify(train_df, target_col="label", ...)`. **This cell should be pre-run** — the demo shows the results, not the training process (which takes ~10-15 min)
  - Cells showing: the MLflow experiment UI link, the best model's auto-generated notebook link, SHAP feature importance plot
  - A cell that registers the best model to Unity Catalog: `mlflow.register_model(model_uri, "water_digital_twin.gold.anomaly_detection_model")`
  - A cell that scores the demo data using the registered model and writes to `gold.anomaly_scores` with `scoring_method = 'automl'`

  **Level 3 — Foundation time-series model via MMF (~5 min, optional):**
  - A cell that configures Many Model Forecasting with a foundation model backend (Chronos-Bolt or TimesFM 2.5)
  - A cell that runs parallel scoring across all sensors (or a subset for demo speed) using Spark — each sensor is scored independently by the foundation model
  - Visualisation: anomaly heatmap showing all sensors, with DEMO_SENSOR_01 and other affected sensors highlighted as high-confidence anomalies, and normal/amber DMA sensors showing low scores
  - A cell that deploys the model to a Model Serving endpoint (pre-provisioned for demo)
  - A cell demonstrating `ai_query()` calling the endpoint from SQL for batch scoring
  - A cell that writes foundation-model scores to `gold.anomaly_scores` with `scoring_method = 'foundation_model'`

  **Important implementation notes:**
  - The notebook must work independently of the standard demo pipeline — running it is purely additive. The default `statistical` scores from Task 7b remain untouched. Each ML level writes new rows or updates rows with its own `scoring_method` value.
  - Include a final "Comparison" section that queries `gold.anomaly_scores` grouped by `scoring_method` and shows a side-by-side comparison: do all methods agree on the 02:03 anomaly? Where do they diverge on the amber/noise DMAs? This is the payoff — showing that the simple statistical approach and the sophisticated ML approaches converge on real incidents, but the ML approaches catch subtler patterns.
  - Cluster requirement: DBR ML Runtime (for MLflow, AutoML, foundation model libraries). Specify the minimum DBR version and any `%pip install` commands needed.

- **Inputs:** Alice's DDL scripts (Task 4) for table definitions; Bob's data (Task 6) and anomaly_scores table (Task 7b); approved plan Section 5 (data contract, demo scenario)
- **Outputs:** Data generation notebooks; SDP pipeline notebooks; anomaly scoring notebooks; metric view DDL; ML anomaly detection notebook (optional extension)
- **Acceptance Criteria:** SDP pipeline runs end-to-end; all tables/views reference Alice's schema; geospatial transformations produce valid geometries; data generation is idempotent; DEMO_SENSOR_01 anomaly score >3.0σ after 02:03; amber DMAs have anomaly scores <2.0σ; flow data exists for DEMO_FLOW_01/02; reservoir DEMO_SR_01 exists at 43%; dim_incidents contains 1 active + 5-10 historical incidents; dma_status and dma_summary are populated for all 500 DMAs; all 22 verification queries from Section 5 pass; data quality expectations catch invalid records. **Task 7d additional criteria:** ML notebook runs without errors on DBR ML Runtime; Level 1 (`ai_forecast`) produces a visible anomaly flag for DEMO_SENSOR_01 at 02:03; Level 2 (AutoML) experiment contains at least 5 trials with logged metrics; Level 3 (foundation model) scores at least 100 sensors in parallel; all three levels write to `gold.anomaly_scores` with correct `scoring_method` values; comparison section shows agreement across methods for the demo incident

---

### Charlie — AI/BI, Genie & Apps Specialist
- **Task 8 (Databricks App) — 🔨 CODE:** Build and deploy the geospatial Databricks App (React + FastAPI) that supports the full incident investigation flow from the approved plan's Section 4. Deploy via `databricks apps create` + `databricks apps deploy`. The app must have these views/interactions:
  - **Shift handover view (Scene 1):** Landing page showing the structured shift handover for the active incident. Includes: incident overview, actions taken, outstanding actions, communications log, current trajectory, risk of escalation assessment. Auto-generated via Mosaic AI / AI Functions from Gold-layer data. The outgoing operator can edit and sign off; the incoming operator acknowledges. This is a governance-grade handover with timestamps.
  - **Alarm/event log (Scene 2):** Chronological table of all incident events — alarms, detections, status changes, operator actions, customer complaints, regulatory notifications. Filterable by event type and time range. **Default view:** shows events for the currently active incident (filtered by `incident_id` from `dim_incidents WHERE status = 'active'`). A dropdown allows switching between active incidents or viewing "All events (last 24h)". This is the operator's entry point before the map.
  - **Map view (Scene 2):** Interactive map with DMA polygons colour-coded Red/Amber/Green by current pressure status. **Quick-filter buttons** across the top: "Changed" (DMAs that changed status in last 2 hours — filter: `dma_rag_history` where status changed in last 2h), "Alarmed" (DMAs with active alarms — filter: `dma_status.has_active_incident = true`), "Sensitive" (DMAs containing hospitals/schools/dialysis centres — filter: `dma_status.sensitive_premises_count > 0`), "All" (full network — default). Clicking a DMA opens a side panel. **Demo-readiness requirements for the map:**
    - The **initial viewport/zoom** must show the Greater London area and be centred so that `DEMO_DMA_01` (the red incident DMA) is clearly visible on screen when the app first loads — the presenter should not have to scroll or zoom to find it. Default zoom level should show ~20-30 DMAs so the red one stands out among green/amber neighbours.
    - `DEMO_DMA_01` must be visually prominent: it should be the only RED polygon surrounded by GREEN/AMBER neighbours. The red fill must have enough opacity and contrast to immediately draw the eye.
    - DMA polygons must be large enough to be clickable (no tiny slivers). The click target must be the entire polygon area, not just a label or centroid.
    - On hover, show a tooltip with DMA name, current RAG status, and **anomaly confidence** (e.g., "High — 3.2σ") so the presenter can narrate while moving the mouse.
    - Trunk mains should be rendered as LINESTRING overlays with isolation valve markers when a DMA is selected.
    - Consider placing `DEMO_DMA_01` near the **centre** of the generated DMA grid so it's naturally in the middle of the map, not at an edge.
  - **DMA detail panel (Scene 3):** Shows DMA summary (avg pressure, avg flow, sensor count, property count), lists low-pressure assets/sensors sorted by severity, shows recent customer complaints, AND critically: **identifies the upstream cause** with the full asset chain — "Root Cause: Pump station DEMO_PUMP_01 tripped at 02:03 -> Trunk Main TM_001 (12-inch, 3.2km) -> DEMO_DMA_01. Downstream impact: 3 DMAs, ~1,400 properties." Also shows **reservoir status**: "Service Reservoir DEMO_SR_01 — 43% (est. 3.1h supply)." Also shows **flow data**: "DMA inlet flow: 12 l/s (normal: 45 l/s) — partial supply, standby pump may be running."
  - **Asset detail panel (Scene 4):** Clicking an asset (sensor, pump station, trunk main) shows:
    - A **dual-axis time-series chart** (past 24h with 15-min granularity): pressure on left axis, flow on right axis. Must clearly show the 2am drop in both.
    - A **RAG status timeline strip** below the chart — a horizontal sequence of colour blocks (green-green-green-amber-red-red) showing when the DMA transitioned through each state. Operators call this "trend of the trend" — is the situation deteriorating or stabilising?
    - An **anomaly detection badge** showing when the system first flagged this event (e.g., "Anomaly detected at 02:18 — 47 minutes before first customer complaint").
    - A **response playbook panel**: "SOP-WN-042 — Pump Station Failure Response" with context-specific recommended actions. Each action has Accept/Defer/Not Applicable buttons. The operator decides — the platform recommends, not acts. Playbook data is read from `gold.dim_response_playbooks`.
  - **Customer impact view (Scene 5):** Shows affected properties as map markers. Shows that high-elevation customers are impacted first. Integrates complaint data with timestamps. Shows a **predicted impact projection** and **what-if slider** for "restored pressure %". Clearly labelled: "Estimated impact — based on elevation and current pressure. Not a hydraulic simulation."
  - **Regulatory compliance & executive briefing (Scene 6):** For any selected DMA/area, instantly shows:
    - Affected properties by type: domestic count, schools, hospitals, dialysis (home), commercial
    - Duration of interruption so far
    - Whether sensitive/vulnerable customers are impacted (DWI reportable)
    - **Regulatory deadline tracker** with live countdown timers (DWI verbal/written, Ofwat 3h/12h thresholds, penalty exposure, EA status, C-MeX stats)
    - **Auditable decision timeline**: timestamped log of every detection, decision, and communication
    - A **pre-populated draft regulatory report** (DWI Event Report + Ofwat SIR) with PDF export. **PDF export implementation:** Use client-side generation with `react-pdf` or `html2pdf.js`. The FastAPI backend should also expose a `/api/incidents/{incident_id}/report/pdf` endpoint (using WeasyPrint or ReportLab).
    - AMP8 investment planning hook
  - **Communications log panel:** Structured record of all communications during the incident. Operators can add new entries.
  - **Write-back:** Operator can take ownership of an alert, add comments, log communications, accept/defer playbook actions, and trigger proactive customer comms requests.
  - **Empty / no-incident state:** When no active incident exists, the app should show a "Network Normal" landing page with: overall DMA health summary (all-green map), last resolved incident summary, and a "No active incidents" banner. All panels should show meaningful empty states rather than blank screens or errors. The app must not crash if seed data is missing (cold-start resilience).
  - All reads from Lakebase for sub-second response (<3s map load, <1s panel updates).

- **Task 9a (Operator Genie Space — "Network Operations") — 🔨 CODE + 📋 MANUAL GUIDE:** This is a two-phase task. **Phase 1 (🔨 CODE):** Write and execute all verified SQL queries (Q1-Q10) against the live demo data to confirm they return correct results. Fix any query that returns incorrect or empty results. Save the tested, working queries to a SQL file (`genie/operator_verified_queries.sql`). **Phase 2 (📋 MANUAL GUIDE):** Using the tested queries from Phase 1, produce a numbered step-by-step guide for the SA to create and configure this Genie Space in the Databricks UI. The guide must include:

  **Phase 1 — Query development and testing (🔨 CODE):**
  Write and run each verified query against Bob's populated tables. For each query:
  - Execute against the SQL Warehouse
  - Confirm the result matches the expected demo scenario (e.g., Q1 should show DEMO_DMA_01 with the biggest pressure drop; Q2 should return at least 2 schools and 1 hospital)
  - If a query returns wrong results, fix it and re-test
  - Save all 10 tested queries to `genie/operator_verified_queries.sql` with comments showing expected results

  **Phase 2 — SA configuration guide (📋 MANUAL GUIDE):**
  1. Navigate to: AI/BI > Genie Spaces > "Create Genie Space"
  2. Space name: `Network Operations`
  3. Description: `Operational Genie Space for water network control room operators. Ask questions about pressure, flow, DMAs, sensors, and incidents.`
  4. **Add trusted assets** — list each table/view with its full three-part name. The SA will click "Add table" for each:
     - Metric views: `water_digital_twin.gold.mv_dma_pressure`, `water_digital_twin.gold.mv_flow_anomaly`, `water_digital_twin.gold.mv_reservoir_status`
     - Dimension tables: `water_digital_twin.silver.dim_sensor`, `water_digital_twin.silver.dim_dma`, `water_digital_twin.silver.dim_pma`, `water_digital_twin.silver.dim_properties`, `water_digital_twin.silver.dim_assets`, `water_digital_twin.silver.dim_reservoirs`, `water_digital_twin.silver.dim_asset_dma_feed`, `water_digital_twin.silver.dim_reservoir_dma_feed`
     - Gold tables: `water_digital_twin.silver.fact_telemetry`, `water_digital_twin.gold.dma_status`, `water_digital_twin.gold.dma_rag_history`, `water_digital_twin.gold.anomaly_scores`, `water_digital_twin.gold.dma_summary`
  5. **Set instructions** — copy-paste the exact system prompt text from the approved plan's Section 4, Scene 7a into the "General instructions" field
  6. **Add sample questions** — for each of the 10 operator questions from Section 4 Scene 7a, click "Add sample question" and enter the question text
  7. **Add verified queries** — for questions 1-10, click the question, then "Add verified query", and paste the corresponding tested SQL from `genie/operator_verified_queries.sql`
  8. **Test each sample question** — click the question in the UI, verify Genie returns the correct answer, adjust the system prompt or verified query if needed
  9. **Share** — set permissions so demo viewers can access the space
  
  **Validation checklist (SA must verify each):**
  - [ ] All 13 trusted assets are listed and accessible
  - [ ] System prompt is pasted correctly (copy from approved plan Section 4, Scene 7a)
  - [ ] All 10 sample questions are added
  - [ ] All 10 verified queries were pre-tested by Charlie and return correct results (see `genie/operator_verified_queries.sql`)
  - [ ] Spatial queries (Q6 using ST_Distance, Q8 using ST_Contains) execute successfully
  - [ ] Genie responds accurately to at least 8 of 10 sample questions without modification

- **Task 9b (Executive Genie Space — "Water Operations Intelligence") — 🔨 CODE + 📋 MANUAL GUIDE:** Same two-phase approach as Task 9a. **Phase 1 (🔨 CODE):** Write and execute all verified SQL queries (Q1-Q8) against the live demo data. Fix any query that returns incorrect or empty results. Save the tested, working queries to `genie/executive_verified_queries.sql`. **Phase 2 (📋 MANUAL GUIDE):** Using the tested queries, produce a step-by-step guide. This is a **separate** Genie Space from the operator one — different trusted assets, different system prompt, different sample questions. The guide must include:

  **Phase 1 — Query development and testing (🔨 CODE):**
  Write and run each verified query against Bob's populated tables. For each query:
  - Execute against the SQL Warehouse
  - Confirm the result matches the expected demo scenario (e.g., Q1 should return £180K penalty exposure for the active incident; Q3 should show DEMO_DMA_01 in the top 10)
  - If a query returns wrong results, fix it and re-test
  - Save all 8 tested queries to `genie/executive_verified_queries.sql` with comments showing expected results

  **Phase 2 — SA configuration guide (📋 MANUAL GUIDE):**
  1. Navigate to: AI/BI > Genie Spaces > "Create Genie Space"
  2. Space name: `Water Operations Intelligence`
  3. Description: `Executive Genie Space for water utility leadership, compliance managers, and operations directors. Ask questions about regulatory compliance, financial exposure, incident trends, and AMP8 investment planning.`
  4. **Add trusted assets** — list each table/view with its full three-part name:
     - Metric views: `water_digital_twin.gold.mv_regulatory_compliance`, `water_digital_twin.gold.mv_incident_summary`, `water_digital_twin.gold.mv_dma_pressure`, `water_digital_twin.gold.mv_reservoir_status`
     - Gold tables: `water_digital_twin.gold.dim_incidents`, `water_digital_twin.gold.incident_events`, `water_digital_twin.gold.dma_status`, `water_digital_twin.gold.dma_summary`, `water_digital_twin.silver.dim_properties`
  5. **Set instructions** — copy-paste the exact system prompt text from the approved plan's Section 4, Scene 7b into the "General instructions" field
  6. **Add sample questions** — for each of the 8 executive questions from Section 4 Scene 7b, click "Add sample question" and enter the question text
  7. **Add verified queries** — for questions 1-8, click the question, then "Add verified query", and paste the corresponding tested SQL from `genie/executive_verified_queries.sql`
  8. **Test each sample question** — click the question in the UI, verify Genie returns the correct answer
  9. **Share** — set permissions so demo viewers can access the space
  
  **Validation checklist (SA must verify each):**
  - [ ] All 9 trusted assets are listed and accessible
  - [ ] System prompt is pasted correctly (copy from approved plan Section 4, Scene 7b)
  - [ ] All 8 sample questions are added
  - [ ] All 8 verified queries were pre-tested by Charlie and return correct results (see `genie/executive_verified_queries.sql`)
  - [ ] Genie responds accurately to at least 6 of 8 sample questions without modification
  - [ ] Executive questions about penalty exposure and AMP8 return meaningful numbers

- **Task 10 (Executive Dashboard) — 📋 MANUAL GUIDE:** Produce a numbered, step-by-step guide for the SA to create this AI/BI Lakeview dashboard in the Databricks UI. This is the **executive/management view** — a separate entry point from the operator app. The guide must include:

  **Step-by-step creation:**
  1. Navigate to: AI/BI > Dashboards > "Create Dashboard"
  2. Dashboard name: `Water Operations — Executive View`
  3. **Page 1: "Incident Overview"** — for each tile, provide:
     - Tile title, tile type (counter/bar/line/map/table), exact SQL query referencing Bob's `mv_*` views
     - Position on the canvas (top-left, top-right, etc.) and approximate size
     - Tiles:
       a. "Active Incidents" — counter tile, query: `SELECT COUNT(*) FROM gold.dim_incidents WHERE status = 'active'`
       b. "Total Properties Affected" — counter tile, query: `SELECT SUM(total_properties_affected) FROM gold.dim_incidents WHERE status = 'active'`
       c. "Ofwat 3-Hour Threshold" — counter tile (RED if >0), query from `mv_regulatory_compliance`
       d. "Ofwat 12-Hour Threshold" — counter tile, query from `mv_regulatory_compliance`
       e. "Estimated Penalty Exposure" — counter tile formatted as £, query from `mv_regulatory_compliance`
       f. "C-MeX Proactive Rate" — counter tile formatted as %, query from `mv_incident_summary`
       g. "DMA Health Map" — map visualisation showing RAG status across all DMAs, query from `dma_status` with geometry
       h. "Regulatory Deadline Status" — table tile showing upcoming DWI/Ofwat deadlines
  4. **Page 2: "AMP8 Investment Insights"** — tiles:
       a. "Top 10 DMAs by Incident Frequency" — bar chart, query from `mv_incident_summary` grouped by dma_code
       b. "Incident Trend (12 months)" — line chart showing monthly incident count
       c. "Anomaly Trends" — line chart showing weekly anomaly detection counts
       d. "Sensor Coverage" — counter/table showing sensors per DMA
  5. **Filters:** Add dashboard-level filters for: time range (last 7d / 30d / 90d / 12m), DMA code, incident severity
  6. **Publish** the dashboard and set sharing permissions
  
  **For each tile, provide the exact SQL query** — do not leave placeholders. All queries must reference Bob's metric views (`mv_regulatory_compliance`, `mv_incident_summary`, `mv_dma_pressure`, `mv_reservoir_status`) or Gold tables.
  
  **Validation checklist:**
  - [ ] All tiles render with correct data
  - [ ] Counter tiles show meaningful numbers (not null/zero for the demo scenario)
  - [ ] Map tile shows DMA polygons with correct RAG colouring
  - [ ] Filters work across all tiles
  - [ ] Dashboard loads in <5 seconds

- **Inputs:** Alice's DDL (Task 4); Bob's data + metric views (Tasks 6-7c); approved plan Sections 4-6
- **Outputs:** Deployed Databricks App; Operator Genie Space SA guide; Executive Genie Space SA guide; Executive Dashboard SA guide
- **Acceptance Criteria:** App loads map in <3 seconds from Lakebase; shift handover generates correctly; alarm log is populated and filterable by incident_id; playbook renders with Accept/Defer buttons; regulatory report exports as PDF (both client-side and via API endpoint); what-if slider recalculates property counts correctly; comms request creates a record in gold.comms_requests; Genie Space guides include all trusted assets with full three-part names, exact system prompt text, all sample questions, and verified SQL for each; Dashboard guide includes exact SQL for every tile and a validation checklist; quick-filter buttons (Changed/Alarmed/Sensitive/All) work on map; empty/no-incident states display correctly. **The SA follows the 📋 MANUAL GUIDE outputs to configure Genie Spaces and Dashboard in the UI, then runs the validation checklists.**

---

### Eve — Demo Delivery Lead
- **Task 11 (README & Reset) — 🔨 CODE:** Write a runbook covering prerequisites, setup steps (referencing Diana's enablement), and a scripted reset procedure to tear down and re-seed the demo. **Include a "demo health check" script** that runs all 22 verification queries from the approved plan's Section 5 and reports pass/fail with colour-coded output. SAs should run this 10 minutes before any demo. The reset script must have a dry-run mode that shows what will be deleted before executing, and require confirmation before destructive operations.

- **Task 12 (Live Demo Script) — 🔨 CODE:** Create a talk-track outline mapping each demo step to the 9-scene storyline from the approved plan's Section 4 (Scenes 1-6, 7a Operator Genie, 7b Executive Genie, and optional Scene 8 ML notebook). Include:
  - Presenter notes for each scene (what to click, what to say, what to highlight)
  - **Audience-specific hooks** — flag points where the presenter should pivot messaging based on who's in the room (operator audience: emphasise handover and playbook; exec audience: emphasise penalty exposure and audit trail; regulation audience: emphasise DWI/Ofwat deadlines and PDF export; technical audience: emphasise architecture, data quality, and ML notebook (Scene 8); data science audience: lead with Scene 8 ML notebook, then show how scores flow into the operational app)
  - The **executive summary landing line** from Scene 6 — this is the climax of the demo and must be rehearsed
  - Integration architecture slide content (one slide showing OSI PI -> Databricks -> Lakebase -> App, with the OT-IT boundary note)
  - TCO positioning narrative for the Q&A section (include: FTE savings from post-incident admin reduction — 15-20 incidents x 4-6h -> 30 min each = 50-110 hours/year saved)
  - The "time to executive briefing" KPI: current state 45 minutes, future state zero — the data is always live
  - Scalability narrative (100 -> 2,000 users) for the Q&A section
  - **Scene 8 presenter guide:** When to include it (extended sessions, technical/DS audiences), which levels to show based on time available (Level 1 only = +3 min; Levels 1+2 = +10 min; all three = +15 min), and transition phrases from Scene 7b into Scene 8 (e.g., "Now let me show you what powers that anomaly detection under the hood...")

- **Inputs:** All other agents' outputs; approved plan Sections 1-6
- **Outputs:** README/runbook; demo health check script; reset script; live demo talk track
- **Acceptance Criteria:** Reset script drops and recreates all schemas; health check script runs all 22 queries; README references exact notebook paths from Alice's Task 3; talk track covers all 9 scenes (1-6, 7a, 7b, optional 8) and all 6 pillars; both Genie Spaces are referenced in the talk track; ML notebook (Scene 8) has presenter notes with timing variants; audience-specific hooks are flagged; PDF export is tested

</output_format>

<approved_plan>
@prompts/1_plan_output.md
</approved_plan>
