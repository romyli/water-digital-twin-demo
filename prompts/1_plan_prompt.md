You are a Principal Databricks Solutions Architect. I have drafted a detailed demo plan for a water utility digital twin use case on Databricks. Your job is to **validate, refine, and complete** this plan — not design it from scratch.

<context>
Below you will find:
1. An anonymized **use case brief** (in `<use_case_brief>` tags) summarizing a real water utility engagement.
2. A **detailed draft plan** embedded in the output format — including a storyline, data strategy, schema contract, and agent delegation structure. Sections marked with `[brackets]` are gaps for you to fill. Sections with full content are my draft — validate them against your Databricks platform expertise and improve where needed.

This demo is intended for **field use** — it must be reusable, self-contained, and showcase Databricks geospatial capabilities in a water utility context. It is NOT a customer-specific demo; it is an industry demo inspired by a real use case.

You must rely on the use case brief, the draft plan content, and your expert knowledge of the Databricks Data Intelligence Platform (including Unity Catalog, Spark Declarative Pipelines, Databricks SQL, Lakebase, Databricks Apps, Spatial SQL, Metric Views, AI/BI Genie Spaces, Mosaic AI, etc.).
</context>

<rules>
1. DO NOT write any code, SQL, or Databricks notebooks in this step.
2. This output is a blueprint. Another AI agent will use your exact output to implement the demo, so your instructions, build order, and asset lists must be highly specific, actionable, and leave no room for guessing.
3. All names, companies, and identifying details must be fictional. Use "Water Utilities" as the company name. Use generic role titles instead of real names.
4. You must actively use your Glean search tool to find the latest Databricks features currently in Public or Private Preview. Only include them if they directly solve a challenge identified in the use case brief.
5. Where I have provided specific narratives, TCO figures, or KPIs (e.g., the TCO positioning, the "47 minutes before first customer complaint" metric, the FTE savings), use them verbatim — these are validated and intentional. Your role is to refine the surrounding structure and fill gaps, not to recalculate or second-guess owned content.
</rules>

<instructions>
Step 1: Deep-Dive Analysis
Analyze the use case brief and identify: the primary personas (roles, decision power, technical depth), their top prioritized pains/challenges around geospatial data, and their key aspirations ("what good looks like").

**Personas must include at minimum:**
- Control room operator (day-to-day user, incident investigation)
- Operations manager / Head of Operations (shift oversight, resource allocation)
- Head of Regulation / Compliance (DWI/Ofwat reporting, performance commitments)
- C-level executive (CTO/COO — strategic value, ROI, board reporting)
- Data/GIS engineer (technical architecture, data quality)

Step 2: Preview Feature Discovery (via Glean)
Search Glean for the newest Databricks capabilities currently in preview relevant to geospatial processing — e.g., Spatial SQL functions (ST_ functions, H3), Lakebase GA status, Databricks Apps features, Metric View enhancements, Genie improvements, Mosaic AI Model Serving, AI Functions. Select 1-3 preview features that represent forward-looking value specifically for this geospatial water utility use case.

Step 3: Demo Plan Validation & Refinement
Review the draft plan I have provided in the output format below. For each section:
- **Validate** that the Databricks features referenced are real and correctly described.
- **Fill gaps** where I have left `[bracketed placeholders]`.
- **Flag issues** if any part of my draft is technically incorrect, architecturally unsound, or misuses a Databricks capability. Correct inline and add a `**[REFINED]**` marker so I can see what you changed.
- **Preserve** content I have provided verbatim (especially the demo storyline scenes, TCO positioning, and data contract) unless it is technically wrong.

The demo must showcase these five pillars in an integrated storyline:

1. **Spark Declarative Pipelines (SDP)** — Ingest and transform raw sensor, asset, and geospatial data through a medallion architecture (Bronze -> Silver -> Gold). Show how SDP handles streaming sensor telemetry (pressure AND flow) and batch geospatial reference data in a single pipeline. Include data quality expectations (EXPECT, EXPECT OR DROP, EXPECT OR FAIL) for: sensor readings within valid range (0-120m pressure, 0-500 l/s flow), no null geometries, referential integrity between sensor.dma_code and dim_dma.dma_code.

2. **Delta Tables + Lakebase with Geospatial Transformations** — Show Delta tables storing geospatial data (WKT/WKB polygons for districts, point geometries for sensors/properties, LINESTRING for trunk mains). Show Spatial SQL transformations (ST_Point, ST_Contains, ST_Distance, ST_Buffer, H3 indexing). Then show Lakebase as the low-latency serving layer for the app — replacing the need for an external SQL Managed Instance.

3. **Databricks Apps with Geospatial Visualisation** — A React + FastAPI app deployed on Databricks Apps that displays an interactive map (e.g., using Mapbox/Leaflet/DeckGL). The app reads from Lakebase for sub-second map tile loading. The app supports the full incident lifecycle: alarm review, map investigation, root cause tracing, customer impact assessment, regulatory reporting, shift handover, and decision support. Show: DMA/PMA polygon overlays colour-coded by pressure status (RAG), sensor locations, property locations, trunk main traces, time-series pressure/flow charts on click, and operational panels.

4. **Metric Views on the Gold Layer** — Create well-structured metric views (mv_ prefix) on the Gold layer that define reusable business metrics: average pressure by DMA, sensor reading counts, anomaly rates, customer impact counts, regulatory compliance metrics (properties >3h without supply, properties >12h without supply, sensitive premises affected), and flow anomaly metrics. These metric views are the single source of truth consumed by both Genie Spaces (Operator and Executive), dashboards, and the app's executive view.

5. **Two Curated Genie Spaces — Operator + Executive** — Two separate AI/BI Genie Spaces, each tailored to a distinct persona:
   - **Operator Genie Space ("Network Operations"):** Backed by telemetry views, dimension tables, and operational metric views (mv_dma_pressure, mv_flow_anomaly, mv_reservoir_status). System prompt explains water network domain terminology. 8-10 sample questions focused on incident investigation, sensor data, spatial queries, and operational decisions (see Section 4, Scene 7a).
   - **Executive Genie Space ("Water Operations Intelligence"):** Backed by regulatory metric views, incident summary views, and aggregated Gold-layer tables (mv_regulatory_compliance, mv_incident_summary, mv_dma_pressure, dma_summary). System prompt explains regulatory frameworks (Ofwat OPA, DWI, C-MeX) and business metrics. 6-8 sample questions focused on financial exposure, regulatory compliance, AMP8 investment planning, and portfolio-level performance (see Section 4, Scene 7b).

6. **ML-Powered Anomaly Detection (Optional Extension)** — An optional notebook-based walkthrough that upgrades the default statistical anomaly scoring (Pillar 1 / Task 7b) with trained ML models. This is **not required for the standard 50-minute demo** — it is an extension for longer sessions (adds ~10-15 min) or technically-focused audiences. The notebook showcases three progressive levels of ML sophistication, all writing to the same `gold.anomaly_scores` table (via the `scoring_method` column) so the rest of the demo (app, Genie, dashboards) works identically regardless of which approach produced the scores:

   **Level 1 — "Any analyst can do this" (~3 min):** `ai_forecast()` in pure SQL. Forecast expected pressure/flow values for DEMO_SENSOR_01 using the built-in time-series forecasting AI function, then flag readings outside the prediction interval as anomalies. Zero model training, zero Python — anomaly detection in a few lines of SQL. This is the "wow" opener.

   **Level 2 — "Data science team takes over" (~7 min):** AutoML classification. Frame the problem as binary classification (label historical telemetry windows as anomalous vs. normal using the existing sigma scores as ground truth), then run AutoML. Show: the experiment UI with trial comparison, the auto-generated best-model notebook with SHAP explainability (which features drove the anomaly — pressure drop + flow correlation + time-of-day), and one-click registration to Unity Catalog model registry. Punchline: "Glass-box ML — every trial logged, every decision explainable, the generated notebook is yours to customise."

   **Level 3 — "State of the art" (~5 min, optional):** Foundation time-series models (Chronos-Bolt or TimesFM) via Many Model Forecasting (MMF). Zero-shot anomaly detection across all 10,000 sensors in parallel using Spark — no training data required, pretrained transformers handle unseen time series. Deploy the best model to a Model Serving endpoint, then score from SQL with `ai_query()`. Punchline: "Pretrained transformer, no training data needed, parallel scoring across 10,000 sensors."

   The notebook is parameterized with `dbutils` widgets (sensor_id, date range, threshold) and has pre-computed results cached so each level loads instantly during a demo, with the option to re-run live if time permits. See Section 4, Scene 8 for the talk track.

Ensure you incorporate an elevator pitch, timing, technical prerequisites, build order, practical framing for a field demo audience, and a brief TCO/ROI positioning narrative.
</instructions>

<output_format>
Please format your response using the exact structure below:

<analysis>
- Primary Persona(s): [Details — include all five personas listed above with their decision power, technical depth, and what "success" looks like for each]
- Top Pains/Challenges: [Prioritized list with brief explanations]
- Key Aspirations: [Future state / what good looks like]
</analysis>

<demo_plan>
## 1. Executive Summary & Strategy
- Elevator Pitch: [1-2 sentences summarizing the demo's value for water utility geospatial operations]
- Estimated Demo Timing: [e.g., 50 mins total: 10 min context + architecture, 30 min live demo, 10 min Q&A]
- Target Audience Framing: [How to position this demo for different audiences — technical engineers vs. leadership vs. operations vs. regulation. Include specific hooks for each persona.]
- TCO Positioning: [**Use the following narrative verbatim** — these figures are validated. Current state: ArcGIS licence (capped at 100 users), SQL Managed Instance (shared, contention), Azure App Service middleware, plus FTE cost of manual incident response and regulatory reporting. Future state: Databricks consumption (Lakebase, Apps, SQL Warehouse) — no separate GIS licence, no SQL MI, no middleware API, no per-user licence cap. **Indicative FTE savings:** A typical water utility has 15-20 major incidents per year, each requiring 4-6 hours of senior operator post-incident admin (regulatory report collation, evidence gathering, log assembly). That's 60-120 hours/year of senior operator time on admin. The platform cuts this to ~30 minutes per incident — saving 50-110 hours/year of senior operator time, plus eliminating the risk of missed deadlines. Position as: "Even at cost-neutral, the elimination of 3 separate systems and the reduction in incident response time from 15 minutes to 30 seconds creates a quantifiable benefit. Add the regulatory penalty avoidance, the FTE savings on post-incident admin, and the business case writes itself."]
- Scalability Narrative: [Address the 100 -> 2,000 user scaling path. Databricks Apps auto-scales, Lakebase handles concurrent queries with performance isolation (unlike SQL MI), no per-user licensing. Marginal cost per additional user is near-zero for the platform layer.]
- Integration Architecture Context: [One slide/section showing how Databricks fits into the existing ecosystem: OSI PI -> Databricks (SDP ingestion), SAP/Salesforce -> Databricks (batch ingestion), Databricks -> Lakebase -> Databricks App. Note: "Sensor data is ingested via the existing OSI PI historian — the platform reads from PI, not directly from SCADA. No OT network exposure." This pre-empts CISO concerns about OT-IT boundary.]

## 2. Main Messaging Pillars (3-5 Pillars)
*For each pillar include:*
- Message: [One-line message]
- Relevance: [Why a water utility operations persona cares]
- Proof Point: [Which specific part of the demo proves this]

## 3. Key Features & Preview Capabilities Mapped to Value
- [Standard Feature 1, e.g., Unity Catalog]: [Explicitly map to a pain/aspiration identified in your analysis]
- [Standard Feature 2]: [Map to pain/aspiration]
- [Mosaic AI / AI Functions]: [Map to decision support, shift handover generation, and incident classification capabilities]
- [ai_forecast() AI Function]: [Map to anomaly detection — pure SQL time-series forecasting with prediction intervals, zero model training required. Enables Level 1 of the optional ML section (Scene 8).] | **Stage:** Public Preview
- [AutoML]: [Map to ML-powered anomaly detection — binary classification on labeled telemetry, auto-generated notebooks with SHAP explainability, MLflow experiment tracking, Unity Catalog model registry. Enables Level 2 of the optional ML section (Scene 8).] | **Stage:** GA
- [Many Model Forecasting + Foundation Time Series Models (Chronos-Bolt/TimesFM)]: [Map to zero-shot anomaly detection at scale — pretrained transformers scoring 10K sensors in parallel via Spark, no training required. Enables Level 3 of the optional ML section (Scene 8).] | **Stage:** GA (MMF); models available via Databricks-hosted serving
- [Model Serving + ai_query()]: [Map to real-time and batch ML inference — deploy trained models as REST endpoints, score from SQL. Closes the loop from model training to production scoring in the Gold layer pipeline.] | **Stage:** GA (Model Serving), Public Preview (ai_query)
- 🚀 [Glean Preview Feature 1]: [Exactly how it solves a geospatial edge-case or future goal] | **Stage:** [Private/Public Preview/Beta] | **Timeline:** [e.g., GA expected Q4]
- 🚀 [Glean Preview Feature 2]: [Map to pain/aspiration] | **Stage:** [Private/Public Preview/Beta] | **Timeline:** [Expected timeline/GA date]

## 4. Demo Storyline & Talk Track
*The demo follows a single, continuous incident investigation story. A control room operator arrives at 5:30am for the day shift. The night shift has been managing a pressure incident that started at 2am — a pump station tripped, causing low pressure across 3 DMAs. The operator inherits the incident via a structured shift handover, then investigates the current state and manages the response. The demo walks through this full lifecycle.*

**IMPORTANT NARRATIVE FRAMING:** The 5:30am operator is NOT discovering the incident — they are INHERITING it. The night shift has been managing it for 3.5 hours. This is operationally realistic and sets up the shift handover scene naturally.

**The narrative arc (the app is the centrepiece):**

**Scene 1 — "Inherit the incident — structured shift handover"**
The operator arrives at 5:30am. Instead of a scribbled whiteboard note or a rushed verbal briefing, they open the Databricks App and see a **structured shift handover summary** for the active incident. This is the first thing the operator checks — before the map, before anything else.

The handover summary includes:
- **Incident overview:** "Pump station DEMO_PUMP_01 tripped at 02:03. 3 DMAs affected (DEMO_DMA_01 RED, DEMO_DMA_02 AMBER, DEMO_DMA_03 AMBER). Anomaly auto-detected at 02:18 — 47 minutes before first customer complaint."
- **Actions already taken:** "Crew dispatched to DEMO_PUMP_01 at 03:15, ETA 04:00. DWI verbal notification made at 04:10. Proactive customer comms triggered for 423 domestic properties at 04:30."
- **Outstanding actions:** "Awaiting crew confirmation of pump status. DWI written Event Report due by 14:03. Ofwat 3-hour property count rising — currently 312 properties. Proactive comms NOT yet sent to commercial customers."
- **Communications log:** "03:20 — Network Manager notified (phone). 04:10 — DWI duty officer notified (phone). 04:30 — Customer comms team notified (email). 05:00 — Executive on-call briefed (phone)."
- **Current trajectory:** "Pressure in DEMO_DMA_01 stable at 8m since 03:00. No further DMAs degraded. Complaint rate declining (12 in last hour vs. 18 in previous hour). Risk of escalation: LOW — situation stabilising but supply not restored."

The outgoing operator has reviewed and signed off this summary. The incoming operator acknowledges receipt. This is a governance-grade handover — every action timestamped, every communication logged, every outstanding item visible.

**Demo talking point:** "This handover was auto-generated from the platform's incident timeline, communications log, and action records. The outgoing operator reviewed and edited it in 2 minutes. Compare this to the current process: a verbal briefing, a whiteboard, and hope that nothing gets missed. Information lost at shift handover extended an incident by 6 hours last winter — this prevents that."

**Databricks features:** Mosaic AI / AI Functions generate the natural-language summary from structured Gold-layer data (incident events, actions, comms log). Lakebase serves the data. Databricks Apps renders the handover UI with sign-off workflow.

**Scene 2 — "Check the alarm log, then open the map"**
After reviewing the handover, the operator checks the **alarm/event log** — a chronological table showing all events since the incident began:
- `02:03` — DEMO_PUMP_01 TRIPPED (source: SCADA/PI)
- `02:18` — Anomaly detected: DEMO_DMA_01 pressure 3.2σ below baseline (source: platform)
- `02:30` — DEMO_DMA_01 RAG status changed: GREEN -> AMBER -> RED
- `02:30` — DEMO_DMA_02, DEMO_DMA_03 RAG status changed: GREEN -> AMBER
- `03:05` — First customer complaint received (source: Salesforce)
- `03:15` — Crew dispatched to DEMO_PUMP_01 (source: operator action)
- `04:10` — DWI verbal notification filed (source: operator action)

This event log is the operator's starting point — not the map. The map is the investigation tool; the event log is the situational awareness tool. The operator scans the log, confirms nothing new has happened since the handover, then clicks through to the map.

**Quick-filter buttons** across the top of the map: **"Changed"** (DMAs that changed status in last 2 hours), **"Alarmed"** (DMAs with active alarms), **"Sensitive"** (DMAs containing hospitals/schools/dialysis centres), **"All"** (full network). The presenter clicks "Changed" — the map instantly highlights only 3 DMAs (the affected ones), cutting through the "wall of green" far more effectively than relying on the audience to spot one red polygon among 500. Then clicks "All" to show the full network context.

The operator sees the network map. DMA polygons are colour-coded Red/Amber/Green by current pressure status. Most of the 500 DMAs are green — this is a normal night. A few are amber (planned maintenance, demand spikes — normal noise). But one cluster stands out: `DEMO_DMA_01` is red, flanked by two amber neighbours. The red DMA has an **anomaly confidence badge** (e.g., "High — 3.2σ deviation from 7-day baseline") that distinguishes it from the amber DMAs that are just routine fluctuation.

**Scene 3 — "Trace upstream to the cause"**
The operator clicks the red DMA. The side panel shows DMA summary stats, but also highlights the **upstream asset chain that caused the problem**:

"**Root Cause:** Pump station DEMO_PUMP_01 tripped at 02:03 -> Trunk Main TM_001 (12-inch, 3.2km) -> DEMO_DMA_01 (primary feed), DEMO_DMA_02, DEMO_DMA_03 (secondary feed). Downstream impact: 3 DMAs, ~1,400 properties."

The trunk main is rendered on the map as a LINESTRING connecting the pump station to the DMA. Isolation valves along the trunk main are shown as markers (DEMO_VALVE_01, DEMO_VALVE_02) — the operator can see where the network can be isolated if needed.

The panel also shows:
- **Reservoir status:** "Fed by Service Reservoir DEMO_SR_01 — Current level: 43% (estimated 3.1 hours supply at current demand)." This is the single data point that changes urgency — if the reservoir were at 90%, the operator would have 8+ hours. At 43%, the clock is ticking.
- **Flow data:** DMA inlet flow rate has dropped from 45 l/s (normal) to 12 l/s (reduced), confirming partial supply loss — not complete loss. This tells the operator the standby pump may have partially kicked in.

This is the real investigative flow — a DMA going red is a symptom; the operator needs to find the upstream cause (a pump trip, a burst trunk main, a stuck PRV). The sensor in the DMA merely *reports* the failure happening upstream.

**Scene 4 — "See the timeline and the recommended response"**
The operator clicks on the pump station asset. A detail panel opens with:
- A **dual-axis time-series chart** (past 24h, 15-min intervals) showing BOTH pressure (left axis) and flow (right axis). Pressure: stable at ~50m, then sharp drop to ~8m at 02:03. Flow: stable at ~45 l/s, then drop to ~12 l/s at 02:03. Operators always look at both — flow tells you things pressure doesn't (e.g., whether the standby pump kicked in).
- A **RAG status timeline strip** along the bottom: a sequence of green-green-green-amber-red-red blocks, showing exactly when the DMA transitioned through each state. Operators call this "trend of the trend" — is it getting worse or stabilising? In this case, the red blocks are flat (not deepening), suggesting the burst has stabilised but supply is still interrupted.
- An **anomaly detection flag**: the system detected this event automatically at 02:18 (15 minutes after the drop, on the next sensor reading cycle) — well before the first customer complaint at 03:05. This is the "golden hour" advantage: the platform alerts you, not the customer.
- A **response playbook panel** showing: "Standard Operating Procedure: SOP-WN-042 — Pump Station Failure Response. Suggested actions based on incident context:"
  1. "Verify crew status at DEMO_PUMP_01" [checkbox — already actioned by night shift]
  2. "Check standby pump status — flow data suggests partial operation" [checkbox]
  3. "Assess rezoning: can DEMO_DMA_01 be temporarily fed from adjacent DMA via interconnection valves?" [checkbox]
  4. "Check downstream reservoir DEMO_SR_01 — current level 43%, ~3.1h supply remaining" [checkbox]
  5. "Notify DWI — sensitive premises affected (1 hospital, 2 schools). Written report due 14:03." [checkbox — verbal notification already done]
  6. "Trigger proactive customer comms for commercial customers (12 properties, not yet notified)" [checkbox]

  Each action has an "Accept", "Defer", or "Not applicable" option. The operator decides — the platform recommends, it does not act autonomously. The playbook is sourced from a `dim_response_playbooks` table that operations managers can edit — not hardcoded by the vendor.

  **Demo talking point:** "Every water company has SOPs in binder files or PDFs. When a pump trips at 3am and a junior operator is on shift alone, they fumble through a 40-page PDF. This platform surfaces the relevant SOP, pre-populated with the specific incident context — which DMAs, which customers, which reservoirs. The knowledge of your most experienced operator, digitised and available to everyone."

**Scene 5 — "Understand customer impact — and model what's coming"**
The operator switches to the **customer view**. Customer complaints (from Salesforce data) started coming in at ~3am — about an hour after the pressure drop. The map shows affected properties as markers. Crucially, the affected customers are all at **higher elevation** than the pump station — because lower pressure affects high-altitude customers first (pressure = total_head_pressure - customer_height).

The panel also shows a **predicted impact projection**: "If pressure remains at current level, an estimated 1,200 properties will be affected within 2 hours (including 1 hospital, 3 dialysis patients on home dialysis)." This is a simple threshold model — given current pressure at the DMA level and the elevation distribution of properties, which additional properties will lose effective supply if pressure doesn't recover? This turns the exec conversation from "how bad is it now" to "how bad will it get."

**What-if scenario modelling (lightweight):** The operator adjusts a **pressure restoration slider**: "What if we restore 30% flow via bypass valve PRV_03?" The map dynamically recalculates: "Estimated properties recovering supply: 340 (all below 35m elevation). Properties still affected: 860 (above 35m). Hospitals/schools still affected: 1 hospital, 2 schools."

**What-if calculation logic (for the app agent):** This is computed **client-side** in the React app, not via a backend API. The formula:
1. Fetch all properties in the affected DMA(s) from Lakebase: `uprn, customer_height, property_type, latitude, longitude`
2. Fetch current DMA-level `total_head_pressure` from `dma_summary`
3. The slider represents "restored pressure %" (0-100%). Compute `simulated_pressure = current_total_head_pressure + (normal_total_head_pressure - current_total_head_pressure) * (slider_pct / 100)`
4. For each property: `effective_pressure = simulated_pressure - customer_height`. If `effective_pressure < 10` (minimum service threshold in metres), the property is "affected."
5. Re-render the map markers and update the count summary.
This is a simple threshold model. `normal_total_head_pressure` is the sensor's 7-day average (available in `anomaly_scores.baseline_value`).

**IMPORTANT: Position this as a "rapid operational impact assessment" — not a hydraulic simulation. Label clearly in the UI: "Estimated impact — based on elevation and current pressure data. Not a hydraulic model." Water companies use dedicated hydraulic modelling tools (e.g., Synergi, InfoWorks) for full simulations. This gives a 90-second indicative answer while the full model runs.**

**Scene 6 — "Brief the executives — regulatory compliance and financial exposure"**
An executive calls asking about impact. The operator switches to the **regulatory compliance and executive briefing** view. This is where the demo must land with the force of a board paper.

The view shows:

**Incident Impact Summary:**
- Affected properties by type: **domestic (423), schools (2), hospitals (1), dialysis centres (0 — but 3 home dialysis patients flagged), commercial (12)**
- Duration of interruption so far: **3h 27m**
- Sensitive/vulnerable customers impacted: **Yes — 1 hospital, 2 schools, 3 home dialysis patients** (DWI reportable)

**Regulatory Deadline Tracker:**
- **DWI Verbal Notification:** DONE at 04:10 (requirement: "as soon as reasonably practicable" for events affecting sensitive premises)
- **DWI Written Event Report:** DUE in 7h 50m (deadline: 14:03). Status: Draft auto-generated, pending operator review.
- **DWI Regulation 35 Risk Assessment:** FLAGGED — "Low pressure event: assess contamination ingress risk. Has water quality been confirmed safe at the boundary of the affected zone?"
- **Ofwat Supply Interruption — 3-hour threshold:** **312 properties** currently >3h without supply (this drives the OPA performance commitment score)
- **Ofwat Supply Interruption — 12-hour threshold:** **0 properties** (escalation risk if unresolved by 14:03)
- **Estimated Ofwat OPA penalty exposure:** **£180K** based on current trajectory (312 properties x projected duration above 3h threshold)
- **EA (Environment Agency) Notification:** NOT REQUIRED (no environmental discharge detected)
- **Proactive Customer Comms (C-MeX):** 423 domestic properties notified at 04:30 — **31 notifications sent BEFORE first customer complaint.** C-MeX benefit: proactive notification improves customer satisfaction score.

**Auditable Decision Timeline:**
A timestamped log of every detection, decision, and communication:
`02:03 — Pump trip detected (SCADA) | 02:18 — Anomaly flagged (platform) | 02:25 — Night operator acknowledged alert | 02:30 — DMA status escalated to RED | 03:05 — First customer complaint | 03:15 — Crew dispatched | 04:10 — DWI verbal notification | 04:30 — Proactive comms triggered | 05:30 — Shift handover completed`

This audit trail evidences timely decision-making — it's what keeps the company out of a DWI Section 19 investigation.

**Pre-populated regulatory report:** A draft DWI Event Report and Ofwat SIR form with all mandatory fields pre-filled from the platform data: properties affected, duration, sensitive customers, cause, timeline, actions taken. The operator reviews, edits, and exports as PDF. What normally takes 4-6 hours of manual collation is pre-filled in seconds.

**The executive summary line (for the talk track):**
"This incident has been active for 3 hours 27 minutes. It has affected 441 properties including 1 hospital, 2 schools, and 3 home dialysis patients. Properties exceeding the 3-hour Ofwat threshold: 312 (performance commitment impact). Properties approaching the 12-hour penalty trigger: 0 (deadline in 8h 33m). Estimated Ofwat OPA penalty exposure: £180K. DWI written report deadline: 7h 50m. Customer complaints: 47 received, but 423 proactive notifications were sent before the first complaint. Time from detection to executive briefing data on screen: zero — it's always live. The platform detected this incident 47 minutes before any customer called. A typical large water utility experiences 300-400 pressure incidents per year — each one is a potential regulatory event where this platform's early detection advantage compounds."

**Key before/after KPI (for the talk track):**
"Time from incident detection to a complete executive briefing: current state — 45 minutes of manual data collation across 3 systems. With this platform — zero. The data the CEO needs is always live on screen. That's the difference between briefing the executive during the golden hour versus after it."

**AMP8 Investment Planning hook (brief, for the strategic audience):**
"This DMA — DEMO_DMA_01 — has had 12 incidents in the last 6 months, affecting 2,400 properties cumulatively. It should be prioritised for mains replacement in the AMP8 capital programme. The platform doesn't just manage today's incident — it builds the evidence base for where to invest tomorrow."

**ESG / Environmental hook (one line):**
"Faster burst detection reduces average leak duration. This platform's 47-minute early detection advantage, applied across the network, contributes an estimated X megalitres/year to leakage reduction targets — directly supporting PR24 performance commitments and ESG reporting."

**Brief touch point — "Request Proactive Comms" (10 seconds, still in Scene 6):**
Before moving to Genie, the presenter clicks a **"Request Proactive Comms"** button. The app shows a pre-drafted customer notification with: affected postcode areas, estimated restoration time (set manually by the operator — operators hate auto-generated ETAs they can't control), and a templated message. One click sends this to the customer communications team (NOT directly to customers — the app creates a request record, the comms team handles distribution). The C-MeX scoring is shown: "Proactive notification before complaint reduces C-MeX penalty impact." This is a 10-second touch point that completes the incident lifecycle: **detect -> investigate -> respond -> report -> communicate** — all in one platform. No current tool chain does all five.

**Scene 7a — "Operator explores with Genie (Network Operations space)"**
For ad-hoc questions that go beyond the app's pre-built views, the operator opens the **Network Operations Genie Space** and asks natural-language questions. This Genie Space is configured for operational investigation.

**System prompt for Operator Genie Space (must be included in the Genie Space configuration):**
> You are an AI assistant for water network control room operators. You answer questions about the clean water distribution network for Water Utilities, serving the Greater London area.
>
> Key domain terminology:
> - **DMA** = District Metered Area (a geographic zone bounded by a polygon, the primary unit of analysis)
> - **PMA** = Pressure Management Area (a subset of a DMA)
> - **RAG status** = Red/Amber/Green health indicator. RED = pressure below critical threshold. AMBER = pressure below warning threshold or planned maintenance. GREEN = normal.
> - **Pressure** is measured in **metres head (m)**. Normal pressure is typically 40-60m. Below 15m is a supply interruption.
> - **Total head pressure** = absolute pressure at the sensor. **Property-level pressure** = total_head_pressure - customer_height (elevation adjustment). High-elevation customers lose supply first.
> - **Flow rate** is measured in **litres per second (l/s)**.
> - **Anomaly sigma (σ)** = number of standard deviations from the 7-day same-time-of-day baseline. >3.0σ = high-confidence anomaly. <2.0σ = normal fluctuation.
> - **Reservoir level** determines urgency: hours_remaining = (level_pct / 100 × capacity) / hourly_demand_rate.
> - Sensor readings arrive every **15 minutes**. Always use the most recent complete 15-minute window.
> - When asked about "affected" properties, calculate effective pressure (total_head_pressure - customer_height) and flag properties where this falls below the minimum threshold (typically 10m).
> - Sensitive premises = hospitals, schools, dialysis_home. These require DWI notification.
>
> Available tables: dim_sensor, dim_dma, dim_pma, dim_properties, dim_assets, dim_reservoirs, dim_asset_dma_feed, dim_reservoir_dma_feed, fact_telemetry, dma_status, dma_rag_history, anomaly_scores, dma_summary. Metric views: mv_dma_pressure, mv_flow_anomaly, mv_reservoir_status.

**Operator sample questions (8-10, mix of tabular and geospatial):**
1. "Which DMAs had the biggest pressure drop in the last 6 hours?" (tabular — verified query required)
2. "How many hospitals and schools are in DEMO_DMA_01?" (cross-domain join — verified query required)
3. "Show pressure trend for sensor DEMO_SENSOR_01 over the last 24 hours" (time-series — verified query required)
4. "Which pump stations feed DMAs that are currently red?" (upstream analysis — verified query required)
5. "What's the current reservoir level for DMAs that are red or amber?" (operational — verified query required)
6. "Show me all DMAs within 5km of DEMO_DMA_01" (spatial proximity — uses ST_Distance — verified query required)
7. "How many properties have been without supply for more than 3 hours?" (regulatory threshold — verified query required)
8. "Show me all schools within affected DMAs" (sensitive premises — uses ST_Contains — verified query required)
9. "What was the flow rate at the DEMO_DMA_01 entry point at 2am vs now?" (flow comparison)
10. "Which sensors in DEMO_DMA_01 have anomaly scores above 3 sigma?" (anomaly investigation)

**Scene 7b — "Executive explores with Genie (Water Operations Intelligence space)"**
The presenter switches to the **Water Operations Intelligence Genie Space** — a separate Genie Space configured for executive and regulatory audiences. This demonstrates that different personas get different self-service analytics without any code changes.

**System prompt for Executive Genie Space (must be included in the Genie Space configuration):**
> You are an AI assistant for water utility executives, regulatory compliance managers, and operations directors at Water Utilities.
>
> Key business context:
> - **Ofwat** is the economic regulator. The **OPA (Outcome Performance Assessment)** penalises supply interruptions. Key thresholds: properties without supply for **>3 hours** count towards the annual performance commitment; **>12 hours** triggers escalated penalties. Penalties are calculated as: number of affected properties × duration above threshold × per-property-hour rate.
> - **DWI (Drinking Water Inspectorate)** requires notification for events affecting **sensitive premises** (hospitals, schools, dialysis centres). Written Event Reports are required within hours. **Regulation 35** requires a contamination ingress risk assessment during low-pressure events.
> - **C-MeX (Customer Measure of Experience)** scores improve when customers receive **proactive notification** before they complain.
> - **AMP8** (Asset Management Period 2025-2030) — water companies allocate capital investment based on asset condition and incident history. DMAs with frequent incidents are candidates for mains replacement in the capital programme.
> - **ESG** — faster burst detection reduces average leak duration, contributing to leakage reduction targets.
> - When asked about financial exposure, use the formula: estimated_penalty = properties_exceeding_3h_threshold × projected_additional_hours × penalty_rate_per_property_hour. Use £580/property/hour as the default Ofwat penalty rate unless told otherwise.
>
> Available tables: dim_incidents, incident_events, dma_status, dma_summary, dim_properties, mv_regulatory_compliance, mv_incident_summary, mv_dma_pressure, mv_reservoir_status.

**Executive sample questions (6-8, focused on business outcomes and regulatory compliance):**
1. "What's our total Ofwat penalty exposure across all active incidents right now?" (financial — verified query required)
2. "How many properties have exceeded the 3-hour supply interruption threshold this month?" (regulatory — verified query required)
3. "Which DMAs have had the most incidents this year? Show the top 10 with total properties affected." (AMP8 investment planning — verified query required)
4. "Show me all incidents in the last 30 days where more than 100 properties were affected" (historical — verified query required)
5. "What percentage of affected customers received proactive notification before complaining, for the current incident?" (C-MeX — verified query required)
6. "Which DMAs contain hospitals or schools that have been affected by supply interruptions this quarter?" (sensitive premises — verified query required)
7. "What's the average time from incident detection to DWI notification across all incidents this year?" (regulatory performance)
8. "Compare incident frequency this AMP period vs. last AMP period for the top 10 worst-performing DMAs" (capital planning)

**Scene 8 (OPTIONAL — extended sessions only) — "Under the hood: ML-powered anomaly detection"**
This scene is for **longer sessions (~60-65 min) or technically-focused audiences** (data engineers, data scientists, architecture teams). It is NOT part of the standard 50-minute demo. The presenter opens a Databricks notebook and walks through the progressive ML approach.

**Narrative framing:** "The statistical approach you've seen — comparing readings against a rolling baseline — catches known failure patterns like our pump trip. A sharp sigma deviation is hard to miss. But what about a slow-developing burst where pressure drops 1m per hour over 12 hours? A PRV drifting out of calibration over weeks? Correlated failures across DMAs that individually look normal? That's where trained models add value. Let me show you three levels of sophistication — all available on this platform."

**Level 1 — `ai_forecast()` in SQL (~3 min):**
The presenter runs a SQL cell that calls `ai_forecast()` on DEMO_SENSOR_01's telemetry. The function returns forecasted values with upper/lower prediction intervals. A simple comparison — actual vs. interval — flags the 02:03 anomaly. No model training, no Python, no data science team required.

**Demo talking point:** "This is anomaly detection in 4 lines of SQL. Any analyst who can write a SELECT statement can do this. The function handles seasonality, trend, and prediction intervals automatically. For 80% of your anomaly detection use cases, this is enough — and it's available today in every SQL Warehouse."

**Databricks features:** `ai_forecast()` AI Function (Public Preview), SQL Warehouse.

**Level 2 — AutoML classification (~7 min):**
The presenter shows a pre-run AutoML experiment (classification — labelling telemetry windows as anomalous vs. normal). Walk through:
1. The experiment UI — trial comparison table showing algorithms tested (LightGBM, XGBoost, random forest), metrics (F1, precision, recall)
2. Click into the best model's auto-generated notebook — show the code is transparent and editable, not a black box
3. SHAP explainability: which features drove the anomaly classification (pressure magnitude, rate of change, flow correlation, time-of-day deviation)
4. One-click registration to Unity Catalog model registry — show the model as a governed asset with lineage back to the training data

**Demo talking point:** "AutoML tried 10 algorithms in 15 minutes and generated a production-ready notebook with full explainability. Your data science team gets a head start — they can customise the generated code, not start from scratch. And because the model is registered in Unity Catalog, it has the same governance, access control, and lineage as every other data asset in the lakehouse."

**Databricks features:** AutoML (GA), MLflow experiment tracking (GA), Unity Catalog model registry (GA), SHAP explainability.

**Level 3 — Foundation time-series models (~5 min, optional):**
The presenter shows a notebook using Many Model Forecasting (MMF) with a foundation time-series model (Chronos-Bolt or TimesFM). These are pretrained transformer models that perform zero-shot anomaly detection — they have never seen this sensor data before, yet they detect the anomaly because they understand time-series patterns from pretraining.
1. Point MMF at the telemetry table — it scores all 10,000 sensors in parallel using Spark
2. Show the anomaly scores: DEMO_SENSOR_01 is flagged with high confidence, while normal amber DMAs score low
3. Deploy the model to a Model Serving endpoint for real-time scoring
4. Score from SQL using `ai_query('anomaly-endpoint', ...)` — the Gold layer pipeline can call this endpoint to populate `anomaly_scores` with ML-generated scores

**Demo talking point:** "This is a pretrained transformer that understands time-series patterns — seasonal cycles, trend breaks, level shifts — without any training on your data. It scored 10,000 sensors in parallel in under 2 minutes. For a water company with complex, diverse sensor patterns, this is the state of the art — and it runs natively on Databricks."

**Databricks features:** Many Model Forecasting (GA), Foundation time-series models (Chronos-Bolt/TimesFM), Model Serving (GA), `ai_query()` (Public Preview).

**Key integration point:** All three levels write to the same `gold.anomaly_scores` table using the `scoring_method` column ('ai_forecast', 'automl', 'foundation_model'). The app, Genie Spaces, and dashboards work identically regardless of which method produced the scores — the ML section is a drop-in upgrade, not a parallel path.

- The Problem: Business impact of their current state (latency, fragmented geospatial systems, 15 minutes to pull incident data manually, no anomaly detection, manual regulatory reporting, information loss at shift handover, no decision support for junior operators, no financial exposure visibility).
- The Solution: How the Databricks Lakehouse with geospatial capabilities solves this — from automated anomaly detection to root cause tracing to shift handover to executive briefing and regulatory report in under 60 seconds.
- Step-by-Step Live Demo: [Follow the 9 scenes above (1, 2, 3, 4, 5, 6, 7a, 7b, and optionally 8 for extended sessions), mapping each to the underlying Databricks features: Mosaic AI for handover generation and response playbooks, SDP for data freshness and anomaly scoring, Delta+Lakebase for sub-second queries, the App for the interactive investigation, Metric Views for the aggregated stats and regulatory metrics, Operator Genie for operational ad-hoc exploration, Executive Genie for strategic/regulatory ad-hoc exploration, ML notebook for progressive anomaly detection (ai_forecast, AutoML, foundation models)]
- The Outcome: "Day in the life" after adoption — the incoming operator inherits the incident via a structured handover, checks the alarm log, traces the root cause in seconds, sees the reservoir level and flow data, gets contextual SOP guidance, predicts escalation with a what-if model, briefs the exec with financial exposure numbers, has a draft regulatory report and audit trail ready, and triggers proactive customer comms — the full detect-investigate-respond-report-communicate lifecycle in one platform, all within the golden hour. The platform detected the incident 47 minutes before the first customer called, the response playbook guided a junior operator through the night, the shift handover ensured zero information loss, and the time from detection to executive briefing dropped from 45 minutes to zero — the data is always live on screen. Post-incident admin that used to consume 4-6 hours of senior operator time is completed in 30 minutes.

## 5. Sample Data Strategy
*Note: All table names, column names, and data types referenced in this section are defined authoritatively in Section 6 (Global Schema Contract). This section specifies **what data to generate and the relationships between entities**. For exact column definitions, see Section 6.*

- Entities & Fields: [sensors, DMAs, PMAs, properties, pressure telemetry, flow telemetry, upstream assets (pump stations, trunk mains, isolation valves, PRVs), service reservoirs, customer contacts/complaints, anomaly_scores, dma_rag_history, incident_events, communications_log, response_playbooks — see Section 6 for full column definitions of each table]
- Volume & Realism: [Recommended time range, scale, and noise level. Default: ~500K pressure readings, ~100K flow readings, 500 DMAs, 10K sensors (pressure + flow), 50K properties, 20 service reservoirs, 12 months of history. Override only if the use case demands different scale.]
- **Demo Scenario Contract (CRITICAL — hardcoded consistency requirements):**
  The synthetic data MUST include one pre-baked incident scenario. This is not aspirational — the data generation agent must produce **exactly** these entities with **exactly** these relationships. If any link is broken, the demo fails.

  **The incident DMA: `DEMO_DMA_01`**
  - This DMA must have RAG status = RED at the demo timestamp (pressure below red threshold)
  - It must contain at least **20 sensors** (18 pressure, 2 flow), of which **at least 8 pressure sensors** show pressure below the low-pressure threshold after 2am, and **both flow sensors** show reduced flow (<15 l/s vs. normal ~45 l/s)
  - It must contain **at least 800 properties**, distributed as:
    - ~750 domestic
    - At least 2 schools (property_type = 'school')
    - At least 1 hospital (property_type = 'hospital')
    - At least 3 homes with dialysis patients (property_type = 'dialysis_home')
    - At least 15 commercial (property_type = 'commercial')
  - All properties must have `dma_code = 'DEMO_DMA_01'` — this join must work
  - All properties must have valid `latitude`, `longitude`, and `geometry_wkt` **within the polygon** of DEMO_DMA_01

  **The upstream cause: pump station `DEMO_PUMP_01`**
  - This is the **root cause asset** — a pump station that feeds DEMO_DMA_01 (and partially feeds DEMO_DMA_02, DEMO_DMA_03)
  - It must exist in a `dim_assets` table with: `asset_id = 'DEMO_PUMP_01'`, `asset_type = 'pump_station'`, `status = 'tripped'`, lat/lon placing it geographically **upstream** (slightly north/west) of DEMO_DMA_01
  - It must have a `trip_timestamp = '2026-04-07 02:03:00'`
  - The demo story: a DMA going red is a *symptom*. The cause is this upstream pump trip. The app must link the DMA back to the feeding asset so the operator can trace cause, not just observe effect.
  - The pump station should be connected to the affected DMAs via a `dim_asset_dma_feed` mapping table (asset_id -> dma_code, with feed_type = 'primary' or 'secondary')

  **The trunk main: `DEMO_TM_001`**
  - A trunk main (12-inch, 3.2km) connecting DEMO_PUMP_01 to DEMO_DMA_01
  - Must exist in `dim_assets` with: `asset_id = 'DEMO_TM_001'`, `asset_type = 'trunk_main'`, `geometry_wkt` as a LINESTRING from the pump station location to the DMA entry point
  - Two isolation valves along the trunk main: `DEMO_VALVE_01` and `DEMO_VALVE_02`, stored in `dim_assets` with `asset_type = 'isolation_valve'`, `status = 'open'`, and point geometries along the trunk main LINESTRING

  **The service reservoir: `DEMO_SR_01`**
  - A service reservoir that DEMO_DMA_01 draws from
  - Must exist in `dim_reservoirs` with: `reservoir_id = 'DEMO_SR_01'`, `capacity_ml = 5.0` (megalitres), `current_level_pct = 43` (at demo timestamp), `geometry_wkt` as a POINT near DEMO_DMA_01
  - Linked to DEMO_DMA_01 via a `dim_reservoir_dma_feed` mapping table
  - The 43% level gives approximately 3.1 hours of supply at current demand — this creates urgency in the demo narrative

  **The reporting sensor: `DEMO_SENSOR_01` in `DEMO_DMA_01`**
  - This sensor *reports* the impact of the pump trip — it doesn't fail itself
  - It must show **normal pressure (~45-55m)** from midnight to 1:59am
  - At **2:03am** (matching the pump trip time), pressure drops sharply to **~5-10m**
  - Pressure stays low through 6am (the demo moment)
  - The sensor must have a known `latitude`, `longitude` and low `elevation` (e.g., 15m)

  **Flow sensors in DEMO_DMA_01:**
  - At least 2 flow sensors (`DEMO_FLOW_01`, `DEMO_FLOW_02`) at the DMA entry point
  - Normal flow: ~45 l/s. After 02:03: drops to ~12 l/s (partial flow — standby pump partially running)
  - This flow data is critical: it tells the operator the supply is reduced but not zero, which changes the response

  **Anomaly detection scores:**
  - The Gold layer must include an `anomaly_scores` table/view with a pre-computed anomaly score for each sensor reading
  - The anomaly model is simple: compare each reading against the sensor's **rolling 7-day baseline at the same time-of-day** (to account for diurnal pressure patterns — low morning demand, recovery mid-day, evening drop). Score = number of standard deviations from that baseline.
  - DEMO_SENSOR_01's readings after 02:03 must score **>3.0σ** (flagged as high-confidence anomaly)
  - The system must have "detected" the anomaly at **02:18** (the next 15-minute reading cycle after the 02:03 trip) — this is ~47 minutes before the first customer complaint at 03:05, proving the platform catches it first
  - Normal amber DMAs (from planned maintenance or demand spikes) should have anomaly scores **<2.0σ** — this is how the platform distinguishes real incidents from noise ("wall of green" problem)

  **RAG status timeline:**
  - The Gold layer must include a `dma_rag_history` table that stores RAG status per DMA per 15-minute interval
  - For DEMO_DMA_01, the history must show: GREEN until 02:00, AMBER at 02:15, RED from 02:30 onward
  - For DEMO_DMA_02/03 (neighbours): GREEN until 02:15, AMBER from 02:30 onward (they stay amber, not red)
  - This enables the "trend of the trend" RAG timeline strip in the app UI

  **Elevation coherence:**
  - The failing sensor/asset is at **low elevation** (~15m)
  - Affected properties (especially schools/hospital) must be at **higher elevation** (customer_height 40-80m)
  - This means their effective pressure (total_head_pressure - customer_height) goes **negative or near-zero** during the incident — proving the "high-altitude customers lose water first" story
  - At least 50% of properties in DEMO_DMA_01 must have customer_height > 35m

  **Customer complaints:**
  - At least **30 complaint records** with `dma_code = 'DEMO_DMA_01'`
  - `complaint_timestamp` must be between **3:00am and 5:30am** (1 hour lag after pressure drop)
  - Complaints must be linked to properties that are at **high elevation** (customer_height > 35m)
  - Complaint types: 'no_water', 'low_pressure'

  **Incident event log (for Scene 2 and audit trail):**
  - A `gold.incident_events` table with at least the following pre-baked events for the demo scenario:
    - `02:03` | `asset_trip` | `scada` | "DEMO_PUMP_01 tripped — duty pump failure"
    - `02:18` | `anomaly_detected` | `platform` | "DEMO_DMA_01 pressure anomaly: 3.2σ below baseline"
    - `02:25` | `alert_acknowledged` | `operator` | "Night operator acknowledged DEMO_DMA_01 alert"
    - `02:30` | `rag_change` | `platform` | "DEMO_DMA_01 status: GREEN -> RED"
    - `02:30` | `rag_change` | `platform` | "DEMO_DMA_02, DEMO_DMA_03 status: GREEN -> AMBER"
    - `03:05` | `customer_complaint` | `salesforce` | "First customer complaint received — low pressure"
    - `03:15` | `crew_dispatched` | `operator` | "Field crew dispatched to DEMO_PUMP_01"
    - `04:10` | `regulatory_notification` | `operator` | "DWI verbal notification filed — sensitive premises affected"
    - `04:30` | `customer_comms` | `operator` | "Proactive comms triggered for 423 domestic properties"
    - `05:30` | `shift_handover` | `platform` | "Shift handover completed — day shift operator acknowledged"

  **Communications log (for Scene 1 handover and Scene 6 audit trail):**
  - A `gold.communications_log` table with pre-baked records:
    - `03:20` | `Network Manager` | `phone` | "Briefed on pump trip and affected DMAs" | "Agreed to escalate crew priority"
    - `04:10` | `DWI Duty Officer` | `phone` | "Event notification: sensitive premises affected" | "Written report due by 14:03"
    - `04:30` | `Customer Comms Team` | `email` | "Triggered proactive notification for 423 domestic properties" | "Comms sent by 04:45"
    - `05:00` | `Executive On-Call` | `phone` | "Briefed: 441 properties affected, 1 hospital, 3 dialysis patients, est. penalty £180K" | "Acknowledged, requested hourly updates"

  **Parent incident record:**
  - A `gold.dim_incidents` table with at least one pre-baked incident for the demo:
    - `incident_id = 'INC-2026-0407-001'`, `dma_code = 'DEMO_DMA_01'`, `root_cause_asset_id = 'DEMO_PUMP_01'`, `start_timestamp = '2026-04-07 02:03:00'`, `end_timestamp = NULL` (ongoing), `status = 'active'`, `severity = 'high'`, `total_properties_affected = 441`, `sensitive_premises_affected = true`
  - All `incident_events`, `communications_log`, and `shift_handovers` records for the demo scenario must reference `incident_id = 'INC-2026-0407-001'`
  - Additionally, generate **5-10 historical incidents** (resolved, spanning the last 6 months) to support the executive Genie Space questions about incident frequency, AMP8 investment planning, and "top 10 worst DMAs". At least 2 historical incidents should be in DEMO_DMA_01 (to support the "12 incidents in 6 months" AMP8 hook).

  **Response playbook data:**
  - A `gold.dim_response_playbooks` table with at least one entry for the demo:
    - `incident_type = 'pump_station_trip'`, `sop_reference = 'SOP-WN-042'`, `action_steps` as a structured JSON array containing the 6 recommended actions from Scene 4

  **Neighbouring DMAs and spatial layout:**
  - 2-3 adjacent DMAs (`DEMO_DMA_02`, `DEMO_DMA_03`) should be AMBER (slightly low pressure but not critical) — this makes the red DMA stand out on the map
  - Remaining DMAs should be GREEN (normal operations)
  - **`DEMO_DMA_01` must be placed near the geographic centre of the Greater London DMA grid** (roughly central-south London, ~51.45N, -0.05E — a hilly area where the elevation story is plausible). It must not be at an edge or corner. This ensures it's visible in the default map viewport without panning. Surround it with the AMBER DMAs, then GREEN DMAs further out.

  **Normal baseline:**
  - All sensors (including DEMO_SENSOR_01) must have **at least 7 days of normal data** before the incident, so the 2am drop is clearly visible as an anomaly in time-series charts
  - The demo timestamp (when the operator "opens the app") is **5:30am on the incident day**

  **Verification queries (Bob must run these after data generation):**
  ```
  -- 1. DEMO_DMA_01 exists and is RED
  SELECT dma_code, rag_status FROM gold.dma_status WHERE dma_code = 'DEMO_DMA_01'
  -- Expected: RED

  -- 2. Property type distribution in DEMO_DMA_01
  SELECT property_type, COUNT(*) FROM silver.dim_properties WHERE dma_code = 'DEMO_DMA_01' GROUP BY property_type
  -- Expected: domestic >= 750, school >= 2, hospital >= 1, dialysis_home >= 3, commercial >= 15

  -- 3. Pressure drop is visible
  SELECT timestamp, value FROM silver.fact_telemetry WHERE sensor_id = 'DEMO_SENSOR_01' AND timestamp BETWEEN '2026-04-07 00:00' AND '2026-04-07 06:00' ORDER BY timestamp
  -- Expected: ~45-55m until 2am, then ~5-10m after

  -- 4. Flow drop is visible
  SELECT timestamp, flow_rate FROM silver.fact_telemetry WHERE sensor_id = 'DEMO_FLOW_01' AND timestamp BETWEEN '2026-04-07 00:00' AND '2026-04-07 06:00' ORDER BY timestamp
  -- Expected: ~45 l/s until 2am, then ~12 l/s after

  -- 5. Complaints lag pressure drop by ~1 hour
  SELECT MIN(complaint_timestamp), MAX(complaint_timestamp), COUNT(*) FROM silver.customer_complaints WHERE dma_code = 'DEMO_DMA_01'
  -- Expected: earliest ~3am, count >= 30

  -- 6. Elevation coherence
  SELECT AVG(p.customer_height) as avg_customer_elevation FROM silver.dim_properties p JOIN silver.customer_complaints c ON p.uprn = c.uprn WHERE c.dma_code = 'DEMO_DMA_01'
  -- Expected: avg > 35m (high elevation customers complaining)

  -- 7. Upstream cause exists and is linked
  SELECT asset_id, asset_type, status, trip_timestamp FROM silver.dim_assets WHERE asset_id = 'DEMO_PUMP_01'
  -- Expected: asset_type = 'pump_station', status = 'tripped', trip_timestamp = '2026-04-07 02:03:00'

  -- 8. Pump feeds the affected DMAs
  SELECT asset_id, dma_code, feed_type FROM silver.dim_asset_dma_feed WHERE asset_id = 'DEMO_PUMP_01'
  -- Expected: 3 rows — DEMO_DMA_01 (primary), DEMO_DMA_02 (secondary), DEMO_DMA_03 (secondary)

  -- 9. Trunk main exists with LINESTRING geometry
  SELECT asset_id, asset_type, geometry_wkt FROM silver.dim_assets WHERE asset_id = 'DEMO_TM_001'
  -- Expected: asset_type = 'trunk_main', geometry_wkt starts with 'LINESTRING('

  -- 10. Isolation valves exist along trunk main
  SELECT asset_id, asset_type, status FROM silver.dim_assets WHERE asset_type = 'isolation_valve' AND asset_id LIKE 'DEMO_VALVE%'
  -- Expected: 2 rows, both status = 'open'

  -- 11. Reservoir exists and is linked
  SELECT reservoir_id, capacity_ml, current_level_pct FROM silver.dim_reservoirs WHERE reservoir_id = 'DEMO_SR_01'
  -- Expected: capacity_ml = 5.0, current_level_pct = 43

  -- 12. Anomaly score is high for incident, low for noise
  SELECT sensor_id, timestamp, anomaly_sigma FROM gold.anomaly_scores WHERE sensor_id = 'DEMO_SENSOR_01' AND timestamp = '2026-04-07 02:15:00'
  -- Expected: anomaly_sigma > 3.0

  -- 13. Amber DMAs are low anomaly (not real incidents)
  SELECT s.sensor_id, MAX(a.anomaly_sigma) FROM silver.dim_sensor s JOIN gold.anomaly_scores a ON s.sensor_id = a.sensor_id WHERE s.dma_code = 'DEMO_DMA_02' AND a.timestamp BETWEEN '2026-04-07 02:00' AND '2026-04-07 06:00' GROUP BY s.sensor_id
  -- Expected: all max anomaly_sigma < 2.0

  -- 14. RAG timeline history exists
  SELECT dma_code, timestamp, rag_status FROM gold.dma_rag_history WHERE dma_code = 'DEMO_DMA_01' AND timestamp BETWEEN '2026-04-07 01:00' AND '2026-04-07 04:00' ORDER BY timestamp
  -- Expected: GREEN until ~02:00, AMBER at ~02:15, RED from ~02:30 onward

  -- 15. Incident event log is populated
  SELECT timestamp, event_type, source, description FROM gold.incident_events WHERE timestamp BETWEEN '2026-04-07 02:00' AND '2026-04-07 06:00' ORDER BY timestamp
  -- Expected: at least 10 events matching the pre-baked scenario

  -- 16. Communications log is populated
  SELECT timestamp, contact_role, method, summary FROM gold.communications_log WHERE timestamp BETWEEN '2026-04-07 03:00' AND '2026-04-07 06:00' ORDER BY timestamp
  -- Expected: at least 4 records

  -- 17. Response playbook exists
  SELECT incident_type, sop_reference FROM gold.dim_response_playbooks WHERE incident_type = 'pump_station_trip'
  -- Expected: 1 row with sop_reference = 'SOP-WN-042'

  -- 18. Active incident exists in dim_incidents
  SELECT incident_id, dma_code, root_cause_asset_id, status, severity, total_properties_affected FROM gold.dim_incidents WHERE status = 'active'
  -- Expected: 1 row — INC-2026-0407-001, DEMO_DMA_01, DEMO_PUMP_01, active, high, 441

  -- 19. Historical incidents exist for executive Genie Space
  SELECT COUNT(*) as total, COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved FROM gold.dim_incidents
  -- Expected: total >= 6 (1 active + at least 5 historical), resolved >= 5

  -- 20. dma_status is populated for all DMAs
  SELECT COUNT(*) as dma_count, COUNT(CASE WHEN rag_status = 'RED' THEN 1 END) as red, COUNT(CASE WHEN rag_status = 'AMBER' THEN 1 END) as amber FROM gold.dma_status
  -- Expected: dma_count = 500, red = 1 (DEMO_DMA_01), amber = 2 (DEMO_DMA_02, DEMO_DMA_03)

  -- 21. dma_summary is populated with reservoir and incident links
  SELECT dma_code, reservoir_level_pct, reservoir_hours_remaining, active_incident_id, sensitive_premises_count FROM gold.dma_summary WHERE dma_code = 'DEMO_DMA_01'
  -- Expected: reservoir_level_pct = 43, reservoir_hours_remaining ~ 3.1, active_incident_id = 'INC-2026-0407-001', sensitive_premises_count >= 6

  -- 22. Sensitive premises count is correct (for map quick-filter)
  SELECT dma_code, sensitive_premises_count FROM gold.dma_status WHERE sensitive_premises_count > 0 AND dma_code = 'DEMO_DMA_01'
  -- Expected: sensitive_premises_count >= 6 (2 schools + 1 hospital + 3 dialysis_home)
  ```
- **Geospatial Realism — London area (MANDATORY):**
  - All generated coordinates must fall within the **Greater London area**. Bounding box: latitude **51.28N to 51.70N**, longitude **-0.51W to 0.33E**.
  - `DEMO_DMA_01` (the incident DMA) should be centred roughly around **central-south London** (e.g., ~51.45N, -0.05E) so it's in the middle of the map and visually prominent.
  - DMA polygons should be generated as **hexagonal or Voronoi-style tessellations** covering the Greater London area. Each polygon should be roughly 2-5 km across — large enough to be clearly visible and clickable on a map.
  - Sensor locations must be **inside** their assigned DMA polygon. Property locations must be **inside** their assigned DMA polygon. Use ST_Contains or H3-based assignment to verify.
  - Elevation data should roughly reflect London's real topography: low areas near the Thames (~5-15m), higher areas on hills like Highgate, Crystal Palace, Shooters Hill (~60-100m). `DEMO_DMA_01` should be in a hilly area so the elevation story (high-altitude customers lose water first) is geographically plausible.
  - Geometry types: DMA/PMA boundaries as WKT POLYGON, sensors and properties as WKT POINT, trunk mains as WKT LINESTRING, plus separate lat/lon DOUBLE columns for point geometries.
  - H3 resolution: use resolution 7 or 8 for DMA-level spatial indexing. H3 is used for two purposes: (1) **spatial joins** — assigning sensors/properties to DMAs via H3 cell membership is faster than ST_Contains for bulk operations during data generation; (2) **aggregation** — H3 cells enable heatmap-style visualisations in the app (e.g., pressure anomaly density). The canonical DMA assignment (sensor.dma_code, property.dma_code) should be verified with ST_Contains during data generation but can use H3 for the fast path.
- Generation Method: [Synthetic vs. pattern-based, and generation assumptions]
- Reset & Re-seed Strategy: [How the demo environment can be torn down and rebuilt from scratch. Must be scripted, not manual. Include a "demo health check" script that runs all verification queries above and reports pass/fail — SAs should run this 10 minutes before any demo.]

## 6. Global Schema Contract (Strict Data Dictionary)
*This is the **single authoritative source** for all table names, column names, and data types. Sections 4 and 5 reference entities by name — this section defines them. Downstream coding agents will rely on this as the absolute source of truth to ensure their scripts connect properly. If any other section appears to contradict this schema, this section wins.*

**Naming conventions (mandatory):**
- Use three-part names: `<catalog>.<schema>.<table>` (default catalog: `water_digital_twin`)
- Follow medallion architecture schemas: `bronze`, `silver`, `gold`
  - `water_digital_twin.bronze.*` — raw ingested data
  - `water_digital_twin.silver.*` — cleansed, conformed tables with geospatial columns
  - `water_digital_twin.gold.*` — business-level aggregates, feature tables, and metric views (`mv_` prefix)
- Geospatial columns should use STRING type with WKT format for portability, plus DOUBLE lat/lon columns for point geometries

**New tables required (in addition to existing schema):**

**Silver layer — new dimension tables:**
- `silver.dim_reservoirs` — reservoir_id (STRING PK), name (STRING), capacity_ml (DOUBLE, megalitres), current_level_pct (DOUBLE, 0-100), hourly_demand_rate_ml (DOUBLE, megalitres/hour — used to compute estimated hours of supply: `(current_level_pct / 100 * capacity_ml) / hourly_demand_rate_ml`), latitude (DOUBLE), longitude (DOUBLE), geometry_wkt (STRING, WKT POINT)
- `silver.dim_reservoir_dma_feed` — reservoir_id (STRING FK), dma_code (STRING FK), feed_type (STRING, enum: 'primary', 'secondary')

**Gold layer — incident management tables:**
- `gold.dim_incidents` — incident_id (STRING PK), dma_code (STRING FK — primary affected DMA), root_cause_asset_id (STRING FK to dim_assets), start_timestamp (TIMESTAMP), end_timestamp (TIMESTAMP, NULL if ongoing), status (STRING, enum: 'active', 'resolved', 'closed'), severity (STRING, enum: 'low', 'medium', 'high', 'critical'), total_properties_affected (INT), sensitive_premises_affected (BOOLEAN). **This is the parent table for incident_events, communications_log, and shift_handovers. All reference incident_id as a foreign key.**
- `gold.incident_events` — event_id (STRING PK), incident_id (STRING FK to dim_incidents), timestamp (TIMESTAMP), event_type (STRING, enum: 'asset_trip', 'anomaly_detected', 'alert_acknowledged', 'rag_change', 'customer_complaint', 'crew_dispatched', 'regulatory_notification', 'customer_comms', 'shift_handover'), source (STRING, enum: 'scada', 'platform', 'operator', 'salesforce'), description (STRING), operator_id (STRING, nullable)
- `gold.communications_log` — log_id (STRING PK), incident_id (STRING FK to dim_incidents), timestamp (TIMESTAMP), contact_role (STRING), method (STRING, enum: 'phone', 'email', 'teams'), summary (STRING), action_agreed (STRING), operator_id (STRING)
- `gold.dim_response_playbooks` — playbook_id (STRING PK), incident_type (STRING, enum: 'pump_station_trip', 'trunk_main_burst', 'prv_failure', 'water_quality'), sop_reference (STRING), action_steps (STRING, JSON array — each step has: step_number, description, is_mandatory, typical_timeframe), last_updated_by (STRING), last_updated_at (TIMESTAMP)
- `gold.shift_handovers` — handover_id (STRING PK), incident_id (STRING FK to dim_incidents), outgoing_operator (STRING), incoming_operator (STRING), generated_summary (STRING, auto-generated by Mosaic AI from incident_events + communications_log + current Gold-layer state), risk_of_escalation (STRING, enum: 'low', 'medium', 'high' — computed from: pressure trend direction, reservoir level vs. demand, complaint rate trend, and whether supply has been restored), current_trajectory (STRING, auto-generated narrative: "stabilising", "deteriorating", "recovering"), operator_edits (STRING, any manual changes the outgoing operator made to the generated summary), signed_off_at (TIMESTAMP), acknowledged_at (TIMESTAMP)
- `gold.comms_requests` — request_id (STRING PK), incident_id (STRING FK to dim_incidents), dma_code (STRING FK), requested_by (STRING, operator_id), requested_at (TIMESTAMP), message_template (STRING), affected_postcodes (STRING, comma-separated), estimated_restoration_time (STRING, manually set by operator), customer_count (INT), status (STRING, enum: 'pending', 'sent', 'cancelled')

**Gold layer — computed status tables:**
- `gold.dma_status` — dma_code (STRING PK), rag_status (STRING, enum: 'GREEN', 'AMBER', 'RED'), avg_pressure (DOUBLE), min_pressure (DOUBLE), sensor_count (INT), property_count (INT), sensitive_premises_count (INT — count of properties with property_type IN ('hospital', 'school', 'dialysis_home')), has_active_incident (BOOLEAN), last_updated (TIMESTAMP). **Materialized from the latest 15-minute telemetry window. This is the primary table the map reads for DMA colouring.**
- `gold.dma_rag_history` — dma_code (STRING), timestamp (TIMESTAMP, 15-minute intervals), rag_status (STRING, enum: 'GREEN', 'AMBER', 'RED'), avg_pressure (DOUBLE), min_pressure (DOUBLE). **Composite PK: (dma_code, timestamp). Enables the RAG timeline strip in the app.**
- `gold.anomaly_scores` — sensor_id (STRING FK), timestamp (TIMESTAMP), anomaly_sigma (DOUBLE, number of standard deviations from the 7-day same-time-of-day baseline), baseline_value (DOUBLE, the expected value), actual_value (DOUBLE, the observed value), is_anomaly (BOOLEAN, true if anomaly_sigma > 2.5), scoring_method (STRING, enum: 'statistical', 'ai_forecast', 'automl', 'foundation_model' — default 'statistical'; identifies which approach produced the score; downstream consumers ignore this column unless explicitly querying by method). **Composite PK: (sensor_id, timestamp).**
- `gold.dma_summary` — dma_code (STRING PK), dma_name (STRING), rag_status (STRING), avg_pressure (DOUBLE), avg_flow (DOUBLE), property_count (INT), sensor_count (INT), sensitive_premises_count (INT), feeding_reservoir_id (STRING FK), reservoir_level_pct (DOUBLE), reservoir_hours_remaining (DOUBLE, computed: `(current_level_pct / 100 * capacity_ml) / hourly_demand_rate_ml`), active_incident_id (STRING FK, nullable), active_complaints_count (INT), last_updated (TIMESTAMP). **Pre-materialized summary for sub-second DMA detail panel rendering. Refreshed every 15 minutes via SDP.**

For each table, provide:
| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.bronze.example` | col_name | STRING | ... |

**Metric Views (Gold layer):**
For each metric view, provide the full YAML-style definition:
- name, source table, joins, dimensions (with display names and formats), measures (with expressions and formats)

**New metric views required (in addition to existing):**
- `mv_regulatory_compliance` — properties >3h without supply, properties >12h without supply, sensitive premises count, estimated OPA penalty exposure
- `mv_flow_anomaly` — flow rate deviation by DMA entry point, DMA-level flow vs. expected
- `mv_reservoir_status` — current level %, estimated hours of supply, feed DMA mapping
- `mv_incident_summary` — active incidents, total properties affected, regulatory deadlines, actions outstanding

</demo_plan>
</output_format>

**IMPORTANT:** Do NOT include multi-agent build instructions or task delegation in this output. That will be handled in a separate build spec prompt (`2_build_spec_prompt.md`) that consumes your plan output. Your output ends at Section 6.

**OUTPUT FILE:** Save your complete output to `prompts/1_plan_output.md` in the project directory.

<use_case_brief>
## Water Utility Digital Twin — Anonymized Use Case Brief

### Background
A large water utility operates a **digital twin platform** ("Smart Visualiser") for their clean water network. The platform enables real-time incident response for control room operators — when a trunk main bursts, every second counts to reduce environmental risk, customer interruptions, and regulatory fines.

- **Scale:** 100 control room users launched April 2026, scaling to 2,000+ by April 2027
- **Data volume:** ~200GB time series database, 10K+ pressure sensors reporting every 15 minutes, 4.5M properties in service area
- **Latency requirement:** GIS layers must load in <3 seconds (ideal <1 second)
- **"Golden hour" concept:** In the first hour of an incident, operators have the best shot at reducing impact. Currently a skilled operator takes 15 minutes to pull the right data from an incident — the platform reduces this to 30 seconds.

### Current Architecture
- **Data sources:** OSI PI (pressure/flow sensors), Syrinx (acoustic sensors), Hydroguard, Permanet, GIS data, SAP, Salesforce (customer contacts)
- **Data platform:** Databricks workspace with EDP 2.0 (Enterprise Data Platform) for ingestion. Medallion architecture: Bronze -> Silver -> Gold in Delta tables. SQL Warehouse for query serving.
- **Geospatial database:** SQL Managed Instance (SQL MI) hosts three key tables:
  - **CleanWaterAssetRAG** — asset status with Red/Amber/Green (RAG) colour coding
  - **CleanWaterAssetUN** — utility network model (pipe geometry, connections)
  - **CustomerContact** — customer locations linked to DMAs, sourced from SAP/Salesforce
- **Geospatial data refresh:** Asset/pipe geometry is daily (static network). Customer contact data is hourly (map needs to show where calls are coming from during incidents).
- **Frontend:** ArcGIS Experience Builder (ESRI) on a Cloud VM. Has 100-user licence cap and customisation limitations.
- **API layer:** Backend API Service on Azure App Service connects SQL Warehouse to frontend. Adds an extra hop and latency.
- **GIS infrastructure:** Separate geospatial team maintains a GIS Geometric Model (end-of-life, migrating to cloud-based Utility Network Model). The geospatial team has their own database and architecture — there's an ambition to unify but it's a 3-4 year program.

### Pains & Challenges (Geospatial Focus)
1. **Latency from middleware:** Backend API Service adds an extra hop between Databricks and the ESRI frontend. Performance is critical — apart from latency, loading time for GIS layers must be <3 seconds.
2. **SQL MI contention:** SQL Managed Instance is shared infrastructure with contention from other teams. No performance isolation.
3. **ArcGIS limitations:** 100-user licence cap, limited customisation of Experience Builder, can't easily integrate AI/LLM features.
4. **Fragmented geospatial data:** Geospatial data lives in SQL MI (separate from Databricks). Some geospatial data stored as files on a VM. No unified geospatial processing.
5. **No write-back:** Current setup is read-only. Need write-back for alert ownership, operator comments, and data science feedback on model outputs.
6. **Authentication fragmentation:** No unified auth across API Service, SQL MI, and ESRI.
7. **Information loss at shift handover:** Incidents spanning shift changes rely on verbal briefings and whiteboard notes. Critical information — actions taken, communications made, outstanding tasks — is routinely lost or incomplete.
8. **Manual regulatory reporting:** DWI Event Reports and Ofwat SIR submissions require hours of manual data collation from multiple systems. Regulatory deadline tracking is done on spreadsheets.
9. **No decision support for junior operators:** Institutional knowledge lives in experienced operators' heads. When a senior operator retires, their knowledge of "what to do when pump X trips" walks out the door. Night shift junior operators have no guidance beyond thick SOP binder files.
10. **No financial exposure visibility:** During an incident, no one can answer "what's our estimated Ofwat penalty if this continues?" The financial impact is calculated retrospectively, weeks later.

### Aspirations (POC Goals)
The team wants to prove:
1. **Lakebase replaces SQL MI** for the geospatial serving layer — sub-second latency, no contention, geospatial data stays in Databricks
2. **Databricks Apps replaces ArcGIS WebExperience + API Service** — simplify architecture (remove 2 middleware layers), unified authentication, no licence cap, and enable write-back
3. **Geospatial transformations natively in Databricks** — use Spatial SQL (ST_ functions) to do what's currently done in SQL MI or ESRI, keeping all processing in the lakehouse
4. **Self-service analytics for every persona** — Two Genie Spaces: one for operators (pressure, DMAs, sensors, spatial queries) and one for executives (regulatory compliance, financial exposure, AMP8 investment planning) — natural-language questions without writing SQL, tailored to each audience
5. **Automated shift handover** — platform-generated, structured, auditable handover briefs that capture the full incident timeline
6. **Real-time regulatory compliance tracking** — live DWI/Ofwat deadline countdowns, auto-populated regulatory reports, auditable decision trails
7. **Contextual decision support** — digitised SOPs surfaced at the right moment with incident-specific context, empowering junior operators
8. **Financial exposure visibility** — real-time estimated Ofwat penalty exposure during incidents, not retrospective analysis weeks later

### Data Model (from existing Genie Space)
The team already has a working Genie Space ("DMA Pressure Metric View") with these entities:
- **dim_sensor** — sensor metadata (sensor_id, name, type, location, DMA assignment, **sensor_type** (STRING, enum: 'pressure' or 'flow' — must be added to distinguish pressure sensors from flow sensors))
- **dim_properties** — property metadata (uprn, customer_height, dma_code, pma_code). **NOTE: The existing schema is missing key fields needed for the demo.** The demo must enrich this table with: `property_type` (enum: domestic, school, hospital, commercial, nursery, key_account, dialysis_home), `address` (street address string), `latitude` (DOUBLE), `longitude` (DOUBLE), and `geometry_wkt` (STRING, WKT point). These fields are available in the real environment via the CustomerContact table (SAP/Salesforce) but weren't exposed in the original Genie space. The property type and location data are essential for the executive briefing scenario (counting schools, hospitals, dialysis patients etc. in an affected area) and for rendering properties on the map.
- **dim_pma** — Pressure Management Areas (pma_code, pma_name, geometry)
- **dim_dma** — District Metered Areas (dma_code, dma_name, dma_area_code, geometry)
- **fact_telemetry** — sensor telemetry readings (sensor_id, timestamp, value, total_head_pressure, sensor_type, flow_rate). **NOTE: The demo must extend this table to include flow telemetry in the SAME table (do NOT create a separate flow table).** Add columns: `sensor_type` (STRING, enum: 'pressure' or 'flow') and `flow_rate` (DOUBLE, l/s — populated only when sensor_type = 'flow', NULL for pressure sensors). For pressure sensors, `value` holds pressure in metres head and `flow_rate` is NULL. For flow sensors, `flow_rate` holds flow in l/s and `value` is NULL. This single-table approach simplifies joins and Genie Space queries.
- **vw_dma_pressure** — aggregated DMA-level pressure (joins telemetry + DMA dims)
- **vw_pma_pressure** — aggregated PMA-level pressure
- **vw_property_pressure** — property-level pressure (computed as: total_head_pressure - customer_height)
- **vw_dma_pressure_sensor** — sensor-level pressure within DMAs

**Existing Metric View definition (DMA Pressure):**
- Source: vw_dma_pressure
- Joins: dim_dma ON dma_code = dim_dma.dma_area_code
- Dimensions: telemetry_time (timestamp), dma_code, dma_name (from dim_dma)
- Measures: avg_pressure (AVG(value)), max_pressure (MAX(value)), min_pressure (MIN(value)), avg_total_head_pressure (AVG(total_head_pressure)), reading_count (COUNT(tag_name))

### Key Domain Concepts
- **DMA (District Metered Area):** Geographic zone for water distribution, bounded by polygon. Primary unit of analysis.
- **PMA (Pressure Management Area):** Subset of DMA for pressure regulation. Has PRVs (Pressure Reducing Valves).
- **RAG Status:** Red/Amber/Green health indicator on assets and DMA polygons. Driven by pressure thresholds.
- **Total Head Pressure:** Absolute pressure at a point. Property-level pressure = total_head_pressure - customer_height (elevation adjustment).
- **TMPOI (Trunk Main Point of Interest):** Alerts from ML model detecting pressure anomalies that could indicate a trunk main burst.
- **Customer impact:** During an incident, the map shows affected properties (black diamonds = no water, violet diamonds = leak). Operators need to know how many customers are impacted.
- **Pressure time filters:** Operators typically filter by 2 days, 7 days, or 30 days of pressure data.
- **Service Reservoir:** Large storage tank feeding one or more DMAs. Reservoir level determines how long supply can continue after a source failure — this is the single most important data point for assessing urgency.
- **Minimum Night Flow:** At ~2-4am, water demand is lowest. Abnormally high flow at this time suggests a burst (leaking water). Abnormally low flow suggests a supply interruption (no water coming in). These are very different incidents requiring different responses.
- **DWI (Drinking Water Inspectorate):** Regulator for water quality. Requires notification for events affecting sensitive premises (hospitals, schools, dialysis centres). Written Event Reports required within hours. Regulation 35 requires risk assessment for potential contamination ingress during low-pressure events.
- **Ofwat:** Economic regulator. OPA (Outcome Performance Assessment) penalises supply interruptions >3 hours and >12 hours. Financial penalties scale with properties affected and duration. C-MeX (Customer Measure of Experience) scores are affected by proactive vs. reactive customer communication.
- **EA (Environment Agency):** Environmental regulator. Must be notified if there is environmental discharge (e.g., leakage from a burst main).
- **AMP8:** The current Asset Management Period (2025-2030). Water companies allocate capital investment based on asset condition and incident history. DMAs with frequent incidents are candidates for infrastructure replacement.
</use_case_brief>
