# Water Digital Twin — Demo Plan (Validated & Refined)

<analysis>

## Primary Personas

### 1. Control Room Operator
- **Role:** Day-to-day incident investigation, alarm management, shift handover, first-line response coordination
- **Decision power:** Tactical — dispatches crews, acknowledges alarms, triggers proactive customer comms, decides whether to escalate
- **Technical depth:** Moderate — comfortable with maps, tables, time-series charts. Not a SQL user. Needs information surfaced, not queried.
- **What "success" looks like:** "I inherited a 3-hour incident at shift change and within 60 seconds I knew exactly what happened, what's been done, and what I need to do next — without phoning anyone."

### 2. Operations Manager / Head of Operations
- **Role:** Shift oversight, resource allocation, crew management, decision escalation
- **Decision power:** Operational — approves crew deployment, authorises rezoning, decides escalation to executives
- **Technical depth:** Low-to-moderate — dashboard consumer, needs aggregated views not raw data
- **What "success" looks like:** "I can see all active incidents, their severity trajectory, and resource allocation on one screen. I don't need to phone three people to understand the current state of the network."

### 3. Head of Regulation / Compliance
- **Role:** DWI Event Reports, Ofwat SIR submissions, regulatory deadline tracking, audit evidence
- **Decision power:** Compliance gate — must sign off regulatory notifications, controls the reporting timeline
- **Technical depth:** Low — needs pre-populated reports and deadline countdowns. Cares about completeness, auditability, and timeliness.
- **What "success" looks like:** "The DWI Event Report is 90% pre-filled from platform data. I review, edit, and submit in 30 minutes instead of 4 hours. Every decision has a timestamped audit trail. We never miss a deadline."

### 4. C-Level Executive (CTO / COO)
- **Role:** Strategic direction, board reporting, AMP8 capital investment, regulatory penalty management
- **Decision power:** Strategic — approves capital programmes, accountable for regulatory performance
- **Technical depth:** Very low — needs executive summaries, financial exposure, trend lines. Asks "how bad is it?" and "where should we invest?"
- **What "success" looks like:** "During an incident, I have live financial exposure on screen — no waiting for someone to collate data. For board reporting, I can see which DMAs need AMP8 investment based on 12 months of incident data, not anecdotes."

### 5. Data / GIS Engineer
- **Role:** Platform architecture, data quality, pipeline management, geospatial data modelling
- **Decision power:** Technical architecture — defines data models, pipeline design, integration patterns
- **Technical depth:** High — SQL, Python, Spark, geospatial libraries. Evaluates platforms on technical merit.
- **What "success" looks like:** "Geospatial processing happens in the same platform as my Delta tables. No more syncing WKT strings to SQL MI, no more maintaining a separate GIS database. One platform, one governance model, one set of spatial functions."

## Top Pains / Challenges (Prioritised)

1. **Fragmented geospatial architecture:** Geospatial data lives in SQL MI (separate from Databricks), some on a VM. No unified processing. Every query crosses system boundaries.
2. **Latency from middleware:** Backend API Service adds an extra hop between Databricks and the ESRI frontend. GIS layers must load in <3 seconds but contention on SQL MI makes this unreliable.
3. **Information loss at shift handover:** Incidents spanning shift changes rely on verbal briefings and whiteboard notes. A single lost detail extended an incident by 6 hours last winter.
4. **Manual regulatory reporting:** DWI Event Reports and Ofwat SIR submissions require 4-6 hours of manual data collation. Deadline tracking is done on spreadsheets.
5. **No decision support for junior operators:** Institutional knowledge lives in experienced operators' heads. Night shift junior operators have no contextual guidance beyond SOP binder files.
6. **No financial exposure visibility:** During an incident, no one can answer "what's our estimated Ofwat penalty?" — the impact is calculated retrospectively, weeks later.
7. **ArcGIS licence cap and customisation limits:** 100-user cap, limited Experience Builder customisation, no AI/LLM integration path.
8. **SQL MI contention:** Shared infrastructure with no performance isolation. Other teams' queries degrade control room performance.
9. **No write-back:** Current setup is read-only. Operators cannot record alert ownership, actions taken, or model feedback.
10. **Authentication fragmentation:** No unified auth across API Service, SQL MI, and ESRI.

## Key Aspirations (Future State / What Good Looks Like)

1. **Single-platform geospatial processing:** All spatial transformations (ST_Contains, ST_Distance, ST_Buffer, H3 indexing) run natively in Databricks. No external GIS database.
2. **Sub-second map performance:** Lakebase replaces SQL MI as the serving layer — no contention, no middleware hop, PostGIS-grade spatial queries at sub-second latency.
3. **Full incident lifecycle in one app:** Detect → Investigate → Respond → Report → Communicate — all in a single Databricks App, replacing ArcGIS + API Service + manual processes.
4. **Structured, auditable shift handover:** AI-generated handover summaries with every action timestamped, every communication logged, every outstanding item visible.
5. **Real-time regulatory compliance tracking:** Live DWI/Ofwat deadline countdowns, auto-populated regulatory reports, auditable decision trails.
6. **Contextual decision support:** Digitised SOPs surfaced at the right moment with incident-specific context, empowering junior operators.
7. **Live financial exposure:** Real-time Ofwat penalty estimates during incidents, not retrospective analysis.
8. **Self-service analytics for every persona:** Natural-language questions via Genie Spaces — operators ask about pressure and DMAs, executives ask about penalties and investment.
9. **Scale from 100 to 2,000+ users** without licence caps, middleware bottlenecks, or per-user cost scaling.

</analysis>

<demo_plan>

## 1. Executive Summary & Strategy

### Elevator Pitch
"A real-time water network digital twin on Databricks — from automated anomaly detection to root cause tracing to regulatory reporting in under 60 seconds — replacing three separate systems with one platform that detected the incident 47 minutes before any customer called."

### Estimated Demo Timing
**Standard demo: 50 minutes total**
- 10 min — Context setting, architecture overview, persona framing
- 30 min — Live demo (Scenes 1-7b)
- 10 min — Q&A, TCO discussion, next steps

**Extended demo: 60-65 minutes** (add Scene 8 — ML anomaly detection notebook)

### Target Audience Framing

**For Operations / Control Room audiences (Scenes 1-5 emphasis):**
Hook: "Your operators currently take 15 minutes to pull incident data from three systems. This platform does it in 30 seconds — and it detected the problem 47 minutes before any customer called."
Emphasis: Shift handover, map investigation, upstream root cause tracing, reservoir urgency, response playbooks, customer impact projection.

**For Regulatory / Compliance audiences (Scene 6 emphasis):**
Hook: "Your DWI Event Report takes 4-6 hours to compile from spreadsheets and phone logs. This platform pre-fills it in seconds, with an auditable decision timeline that keeps you out of a Section 19 investigation."
Emphasis: Regulatory deadline tracker, pre-populated reports, audit trail, sensitive premises identification, Ofwat penalty exposure, proactive comms C-MeX benefit.

**For C-level / Strategy audiences (Scene 6 + 7b emphasis):**
Hook: "During an incident, the data your CEO needs is always live on screen. No 45-minute collation exercise. And the same platform builds the evidence base for your AMP8 capital programme."
Emphasis: Financial exposure, AMP8 investment planning, ESG leakage reduction, scalability narrative (100 → 2,000 users), TCO positioning.

**For Technical / Architecture audiences (all scenes + Scene 8):**
Hook: "One platform replaces SQL MI, ArcGIS, the API middleware layer, and the manual reporting stack. Native geospatial SQL, a Postgres-compatible serving layer, and ML anomaly detection — all governed by Unity Catalog."
Emphasis: SDP pipelines, Spatial SQL functions, Lakebase with PostGIS, native GEOMETRY types, Delta medallion architecture, AutoML, foundation time-series models, Model Serving.

### TCO Positioning
Current state: ArcGIS licence (capped at 100 users), SQL Managed Instance (shared, contention), Azure App Service middleware, plus FTE cost of manual incident response and regulatory reporting. Future state: Databricks consumption (Lakebase, Apps, SQL Warehouse) — no separate GIS licence, no SQL MI, no middleware API, no per-user licence cap. **Indicative FTE savings:** A typical water utility has 15-20 major incidents per year, each requiring 4-6 hours of senior operator post-incident admin (regulatory report collation, evidence gathering, log assembly). That's 60-120 hours/year of senior operator time on admin. The platform cuts this to ~30 minutes per incident — saving 50-110 hours/year of senior operator time, plus eliminating the risk of missed deadlines. Position as: "Even at cost-neutral, the elimination of 3 separate systems and the reduction in incident response time from 15 minutes to 30 seconds creates a quantifiable benefit. Add the regulatory penalty avoidance, the FTE savings on post-incident admin, and the business case writes itself."

### Scalability Narrative
The current ArcGIS deployment is hard-capped at 100 users. The target is 2,000+ by April 2027. Databricks Apps auto-scales horizontally — there is no per-user licence. Lakebase provides performance isolation (unlike the shared SQL MI, where other teams' queries degrade control room latency). The SQL Warehouse scales independently for Genie Space and dashboard queries. Marginal cost per additional user is near-zero for the platform layer — the primary cost drivers are compute (SQL Warehouse, Lakebase instance size) and storage, both of which scale with data volume and query concurrency, not user count. At 2,000 users, the per-user economics are dramatically better than ArcGIS + SQL MI + API Service.

### Integration Architecture Context
**One slide showing how Databricks fits into the existing ecosystem:**

```
OSI PI (pressure/flow sensors) ──► Databricks SDP (Bronze → Silver → Gold)
SAP / Salesforce (customer data) ──► Databricks SDP (batch ingestion)
                                          │
                                          ▼
                                    Delta Tables (Silver/Gold)
                                          │
                                    ┌─────┴─────┐
                                    ▼           ▼
                              Lakebase      SQL Warehouse
                            (serving layer)  (analytics)
                                    │           │
                                    ▼           ▼
                            Databricks App   Genie Spaces
                            (map + panels)   (self-service)
```

**Key pre-emptive note for CISO concerns:** "Sensor data is ingested via the existing OSI PI historian — the platform reads from PI, not directly from SCADA. No OT network exposure. The OT-IT boundary is preserved exactly as it is today."

**What this replaces:**
- SQL MI → Lakebase (geospatial serving)
- ArcGIS Experience Builder → Databricks App (React + FastAPI)
- Azure App Service API → eliminated (App reads directly from Lakebase)
- Manual reporting processes → automated from Gold layer data

---

## 2. Main Messaging Pillars

### Pillar 1 — "Detect before the customer calls"
- **Message:** Automated anomaly detection catches incidents 47 minutes before the first customer complaint.
- **Relevance:** Every water utility measures "time to detect." The golden hour — the first 60 minutes — determines whether an incident is a minor event or a major regulatory failure. Currently, the first alert is often a customer phone call.
- **Proof Point:** Scene 2 — the alarm log shows the platform flagged the anomaly at 02:18 (3.2σ deviation from baseline). The first customer complaint arrived at 03:05. The platform bought the operator 47 minutes of response time.

### Pillar 2 — "One platform, full lifecycle"
- **Message:** Detect → Investigate → Respond → Report → Communicate — the complete incident lifecycle in one platform, not five systems.
- **Relevance:** Today's incident lifecycle spans ArcGIS (map), SQL MI (data), spreadsheets (regulatory tracking), phone calls (comms), and binder files (SOPs). Information is lost at every handoff. The demo shows all five stages in a single application.
- **Proof Point:** Scenes 1-6 — from shift handover (Scene 1) through map investigation (Scenes 2-4), customer impact (Scene 5), and regulatory reporting + proactive comms (Scene 6) — all without leaving the Databricks App.

### Pillar 3 — "The knowledge of your best operator, available to everyone"
- **Message:** Contextual SOPs and AI-generated handovers digitise institutional knowledge so a junior night-shift operator performs like a 20-year veteran.
- **Relevance:** Water utilities face a demographic cliff — experienced operators are retiring, and their knowledge of "what to do when pump X trips at 3am" walks out the door. Night-shift junior operators currently fumble through 40-page PDF SOPs.
- **Proof Point:** Scene 1 (structured shift handover — zero information loss) and Scene 4 (response playbook panel with incident-specific SOP actions pre-populated from the platform data).

### Pillar 4 — "Regulatory confidence, not regulatory anxiety"
- **Message:** Live deadline countdowns, auto-populated regulatory reports, and an auditable decision timeline that evidences timely action.
- **Relevance:** DWI investigations hinge on evidence of timely decision-making. Ofwat penalties scale with properties affected × duration. Currently, regulatory reports take 4-6 hours to compile and deadline tracking is manual.
- **Proof Point:** Scene 6 — regulatory deadline tracker (DWI verbal ✓, written report due in 7h 50m, Ofwat 3-hour threshold count live at 312 properties), pre-populated DWI Event Report draft, auditable decision timeline with every action timestamped.

### Pillar 5 — "Self-service analytics for every persona"
- **Message:** Operators and executives ask questions in natural language — each gets answers tailored to their world, without writing SQL.
- **Relevance:** Today, ad-hoc questions ("how many hospitals in the affected area?", "what's our penalty exposure this quarter?") require a data team to write a query. Genie Spaces give operators and executives direct access to governed, verified answers.
- **Proof Point:** Scene 7a (Operator Genie — "Which pump stations feed DMAs that are currently red?") and Scene 7b (Executive Genie — "What's our total Ofwat penalty exposure across all active incidents?").

---

## 3. Key Features & Preview Capabilities Mapped to Value

### Standard Features (GA)

**Unity Catalog**
Maps to: *Fragmented geospatial data, authentication fragmentation, no governance across systems.*
Unity Catalog provides a single governance layer for all data assets — Delta tables, geospatial dimensions, ML models, metric views, and Genie Spaces. Every table in the demo (bronze through gold) is registered in Unity Catalog with column-level access controls. The ML models from Scene 8 are registered as governed assets with lineage back to training data. This replaces the current state where geospatial data governance is split across SQL MI, ArcGIS, and the Databricks workspace with no unified access control. | **Stage:** GA

**Spark Declarative Pipelines (SDP)**
Maps to: *Data freshness, pipeline reliability, data quality enforcement.*
SDP handles the full medallion pipeline — streaming sensor telemetry (pressure AND flow from OSI PI) and batch geospatial reference data (DMA polygons, property locations from SAP/Salesforce) in a single declarative pipeline. Data quality expectations enforce: sensor readings within valid ranges (EXPECT pressure BETWEEN 0 AND 120, EXPECT flow_rate BETWEEN 0 AND 500), no null geometries (EXPECT OR DROP geometry_wkt IS NOT NULL), and referential integrity (EXPECT OR FAIL sensor.dma_code IN dim_dma.dma_code). The pipeline also materialises the Gold layer: `dma_status`, `dma_rag_history`, `anomaly_scores`, and `dma_summary` — refreshed every 15 minutes. | **Stage:** GA

**Delta Tables (Medallion Architecture)**
Maps to: *Data reliability, time travel for incident investigation, unified batch + streaming.*
Delta provides ACID transactions, schema enforcement, and time travel across all layers. Time travel is operationally valuable: "Show me the DMA status 2 hours ago" is a simple `VERSION AS OF` query — critical for incident post-mortem. The medallion architecture (Bronze → Silver → Gold) separates raw ingestion from cleansed dimensions from business aggregates, enabling the data/GIS engineer to reason about data quality at each tier. | **Stage:** GA

**Lakebase (Postgres-compatible serving layer)**
Maps to: *SQL MI contention, middleware latency, no write-back capability.*
Lakebase replaces SQL MI as the geospatial serving layer. It is Postgres-compatible and supports the PostGIS extension natively — providing spatial data types (geometry, geography), GiST-based R-tree spatial indexes, and PostGIS functions (ST_Contains, ST_DWithin, ST_Transform) at the serving layer. This means the Databricks App queries Lakebase with sub-second latency using standard Postgres spatial queries — no WKT string parsing, no middleware hop. Lakebase also enables write-back: operators can record alert acknowledgements, action updates, and shift handover sign-offs directly. Performance isolation eliminates the SQL MI contention problem. | **Stage:** GA (AWS and Azure)

**Databricks Apps**
Maps to: *ArcGIS licence cap, limited customisation, no AI/LLM integration, no write-back.*
Databricks Apps hosts the React + FastAPI application directly on the Databricks platform. No separate Azure App Service. No ArcGIS licence. Authentication is handled via workspace SSO — users log in once and the app inherits their identity. The app auto-scales horizontally (no 100-user cap). Write-back is native (the FastAPI backend writes to Lakebase). The app can call Mosaic AI endpoints for shift handover generation and response playbook contextualisation. | **Stage:** GA

**Mosaic AI / AI Functions**
Maps to: *Decision support, shift handover generation, incident classification, response playbook contextualisation.*
AI Functions (`ai_query()` with foundation models) power three capabilities in the demo: (1) Shift handover summary generation — the platform composes a structured narrative from Gold-layer incident events, communications log, and current DMA status; (2) Response playbook contextualisation — generic SOP steps are enriched with incident-specific details (which DMAs, which customers, which reservoir); (3) Incident classification and severity assessment. These are called from the FastAPI backend via `ai_query()` against Databricks-hosted foundation models (e.g., `databricks-meta-llama-3-3-70b-instruct`). | **Stage:** GA (Model Serving), `ai_query()` GA

**`ai_forecast()` AI Function**
Maps to: *Anomaly detection — pure SQL time-series forecasting with prediction intervals, zero model training required. Enables Level 1 of the optional ML section (Scene 8).*
`ai_forecast()` provides built-in time-series forecasting directly in SQL. For the water utility use case, it forecasts expected pressure/flow values for any sensor based on historical patterns (handling seasonality, trend, and diurnal cycles automatically), then returns upper/lower prediction intervals. Readings outside the interval are flagged as anomalies. This is the "any analyst can do this" entry point — anomaly detection in 4 lines of SQL, no Python, no model training. | **Stage:** Public Preview

**AutoML**
Maps to: *ML-powered anomaly detection — binary classification on labelled telemetry, auto-generated notebooks with SHAP explainability, MLflow experiment tracking, Unity Catalog model registry. Enables Level 2 of the optional ML section (Scene 8).*
AutoML frames anomaly detection as binary classification (labelling telemetry windows as anomalous vs. normal using existing sigma scores as ground truth), then runs multiple algorithms (LightGBM, XGBoost, random forest) in parallel. The auto-generated best-model notebook includes SHAP explainability — showing which features drove the classification (pressure magnitude, rate of change, flow correlation, time-of-day). The model is registered in Unity Catalog with full lineage. | **Stage:** GA

**Many Model Forecasting + Foundation Time Series Models (Chronos-Bolt / TimesFM)**
Maps to: *Zero-shot anomaly detection at scale — pretrained transformers scoring 10K sensors in parallel via Spark, no training required. Enables Level 3 of the optional ML section (Scene 8).*
Foundation time-series models perform zero-shot anomaly detection — they have never seen this sensor data, yet they detect anomalies because they understand time-series patterns from pretraining on billions of time series. Many Model Forecasting (MMF) parallelises scoring across all 10,000 sensors using Spark, completing in under 2 minutes. | **Stage:** GA (MMF); foundation models available via Databricks-hosted serving

**Model Serving + `ai_query()`**
Maps to: *Real-time and batch ML inference — deploy trained models as REST endpoints, score from SQL. Closes the loop from model training to production scoring in the Gold layer pipeline.*
Model Serving deploys the trained anomaly detection model (from AutoML or foundation model) as a REST endpoint. `ai_query('anomaly-endpoint', ...)` calls this endpoint from SQL, enabling the Gold layer SDP pipeline to populate `anomaly_scores` with ML-generated scores alongside the default statistical scores. The same endpoint can be called from the FastAPI backend for real-time scoring of incoming telemetry. | **Stage:** GA (Model Serving), `ai_query()` GA

### Preview Capabilities (from Glean Research)

🚀 **Spatial SQL with Native GEOMETRY / GEOGRAPHY Types**
Spatial SQL brings 80+ geospatial functions (ST_Point, ST_Contains, ST_Distance, ST_Buffer, ST_Area, ST_Intersection, ST_Union, ST_Within, ST_Intersects, etc.) plus native GEOMETRY and GEOGRAPHY data types directly to the Databricks platform. For this demo, Spatial SQL enables: (1) storing DMA boundaries as native GEOMETRY columns rather than WKT strings — with automatic spatial indexing for fast spatial joins (17x faster out-of-the-box vs. WKT-based approaches); (2) computing property-to-DMA containment (`ST_Contains(dma.geometry, property.geometry)`) during data generation; (3) spatial proximity queries in Genie ("show all DMAs within 5km of DEMO_DMA_01" using `ST_Distance`); (4) buffer analysis for impact assessment (`ST_Buffer` to find properties near a burst main). **[REFINED]** The demo should use WKT STRING columns for maximum portability (Lakebase/PostGIS compatibility, Genie readability), but leverage native GEOMETRY types in the SDP pipeline for performant spatial joins during data transformation. The Silver layer stores both: `geometry_wkt` (STRING) for serving and a transient `geometry` (GEOMETRY) column used only within SDP spatial joins. | **Stage:** Public Preview | **Timeline:** GA expected H2 2026

🚀 **Lakebase PostGIS Extension for Geospatial Serving**
Lakebase runs on native PostgreSQL and supports the PostGIS extension out of the box. This means the Databricks App's FastAPI backend can query Lakebase using standard PostGIS functions (ST_Contains, ST_DWithin, ST_Transform) with GiST-based R-tree spatial indexes — delivering sub-second geospatial queries for map tile loading without any custom spatial middleware. For the water utility demo, this replaces the current SQL MI geospatial serving layer with a Databricks-native solution that has spatial indexing, coordinate system transformation, and distance calculations built in. The app queries like `SELECT * FROM dim_properties WHERE ST_DWithin(geometry, ST_MakePoint(-0.05, 51.45)::geography, 5000)` run with spatial index support at the serving tier. This is the single most impactful architectural simplification: no more syncing WKT strings from Databricks to an external geospatial database. | **Stage:** GA (Lakebase), PostGIS extension available natively | **Timeline:** Available now

🚀 **Metric Views with Semantic Metadata for LLM Accuracy**
Metric Views define reusable, governed business metrics as Unity Catalog objects. The semantic metadata capability (descriptions, display names, format hints on dimensions and measures) improves Genie Space accuracy by giving the LLM explicit context about what each metric means, how it should be aggregated, and what units it uses. For the water utility demo, this means: `mv_dma_pressure` has a measure `avg_pressure` with semantic metadata `{description: "Average pressure in metres head across all sensors in the DMA", unit: "m", aggregation: "AVG"}` — so when an operator asks Genie "what's the average pressure in DEMO_DMA_01?", the LLM knows to use this metric view, apply the correct aggregation, and format the result in metres. This eliminates the ambiguity that causes incorrect Genie responses (e.g., accidentally summing instead of averaging). | **Stage:** Public Preview | **Timeline:** GA expected H2 2026

---

## 4. Demo Storyline & Talk Track

*The demo follows a single, continuous incident investigation story. A control room operator arrives at 5:30am for the day shift. The night shift has been managing a pressure incident that started at 2am — a pump station tripped, causing low pressure across 3 DMAs. The operator inherits the incident via a structured shift handover, then investigates the current state and manages the response. The demo walks through this full lifecycle.*

**IMPORTANT NARRATIVE FRAMING:** The 5:30am operator is NOT discovering the incident — they are INHERITING it. The night shift has been managing it for 3.5 hours. This is operationally realistic and sets up the shift handover scene naturally.

**The narrative arc (the app is the centrepiece):**

### Scene 1 — "Inherit the incident — structured shift handover"
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

### Scene 2 — "Check the alarm log, then open the map"
After reviewing the handover, the operator checks the **alarm/event log** — a chronological table showing all events since the incident began:
- `02:03` — DEMO_PUMP_01 TRIPPED (source: SCADA/PI)
- `02:18` — Anomaly detected: DEMO_DMA_01 pressure 3.2σ below baseline (source: platform)
- `02:30` — DEMO_DMA_01 RAG status changed: GREEN → AMBER → RED
- `02:30` — DEMO_DMA_02, DEMO_DMA_03 RAG status changed: GREEN → AMBER
- `03:05` — First customer complaint received (source: Salesforce)
- `03:15` — Crew dispatched to DEMO_PUMP_01 (source: operator action)
- `04:10` — DWI verbal notification filed (source: operator action)

This event log is the operator's starting point — not the map. The map is the investigation tool; the event log is the situational awareness tool. The operator scans the log, confirms nothing new has happened since the handover, then clicks through to the map.

**Quick-filter buttons** across the top of the map: **"Changed"** (DMAs that changed status in last 2 hours), **"Alarmed"** (DMAs with active alarms), **"Sensitive"** (DMAs containing hospitals/schools/dialysis centres), **"All"** (full network). The presenter clicks "Changed" — the map instantly highlights only 3 DMAs (the affected ones), cutting through the "wall of green" far more effectively than relying on the audience to spot one red polygon among 500. Then clicks "All" to show the full network context.

The operator sees the network map. DMA polygons are colour-coded Red/Amber/Green by current pressure status. Most of the 500 DMAs are green — this is a normal night. A few are amber (planned maintenance, demand spikes — normal noise). But one cluster stands out: `DEMO_DMA_01` is red, flanked by two amber neighbours. The red DMA has an **anomaly confidence badge** (e.g., "High — 3.2σ deviation from 7-day baseline") that distinguishes it from the amber DMAs that are just routine fluctuation.

### Scene 3 — "Trace upstream to the cause"
The operator clicks the red DMA. The side panel shows DMA summary stats, but also highlights the **upstream asset chain that caused the problem**:

"**Root Cause:** Pump station DEMO_PUMP_01 tripped at 02:03 → Trunk Main TM_001 (12-inch, 3.2km) → DEMO_DMA_01 (primary feed), DEMO_DMA_02, DEMO_DMA_03 (secondary feed). Downstream impact: 3 DMAs, ~1,400 properties."

The trunk main is rendered on the map as a LINESTRING connecting the pump station to the DMA. Isolation valves along the trunk main are shown as markers (DEMO_VALVE_01, DEMO_VALVE_02) — the operator can see where the network can be isolated if needed.

The panel also shows:
- **Reservoir status:** "Fed by Service Reservoir DEMO_SR_01 — Current level: 43% (estimated 3.1 hours supply at current demand)." This is the single data point that changes urgency — if the reservoir were at 90%, the operator would have 8+ hours. At 43%, the clock is ticking.
- **Flow data:** DMA inlet flow rate has dropped from 45 l/s (normal) to 12 l/s (reduced), confirming partial supply loss — not complete loss. This tells the operator the standby pump may have partially kicked in.

This is the real investigative flow — a DMA going red is a symptom; the operator needs to find the upstream cause (a pump trip, a burst trunk main, a stuck PRV). The sensor in the DMA merely *reports* the failure happening upstream.

### Scene 4 — "See the timeline and the recommended response"
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

### Scene 5 — "Understand customer impact — and model what's coming"
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

### Scene 6 — "Brief the executives — regulatory compliance and financial exposure"
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
- **Estimated Ofwat OPA penalty exposure:** **£180K** based on current trajectory (312 properties × projected duration above 3h threshold)
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
Before moving to Genie, the presenter clicks a **"Request Proactive Comms"** button. The app shows a pre-drafted customer notification with: affected postcode areas, estimated restoration time (set manually by the operator — operators hate auto-generated ETAs they can't control), and a templated message. One click sends this to the customer communications team (NOT directly to customers — the app creates a request record, the comms team handles distribution). The C-MeX scoring is shown: "Proactive notification before complaint reduces C-MeX penalty impact." This is a 10-second touch point that completes the incident lifecycle: **detect → investigate → respond → report → communicate** — all in one platform. No current tool chain does all five.

### Scene 7a — "Operator explores with Genie (Network Operations space)"
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

**Operator sample questions (10, mix of tabular and geospatial):**
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

### Scene 7b — "Executive explores with Genie (Water Operations Intelligence space)"
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

**Executive sample questions (8, focused on business outcomes and regulatory compliance):**
1. "What's our total Ofwat penalty exposure across all active incidents right now?" (financial — verified query required)
2. "How many properties have exceeded the 3-hour supply interruption threshold this month?" (regulatory — verified query required)
3. "Which DMAs have had the most incidents this year? Show the top 10 with total properties affected." (AMP8 investment planning — verified query required)
4. "Show me all incidents in the last 30 days where more than 100 properties were affected" (historical — verified query required)
5. "What percentage of affected customers received proactive notification before complaining, for the current incident?" (C-MeX — verified query required)
6. "Which DMAs contain hospitals or schools that have been affected by supply interruptions this quarter?" (sensitive premises — verified query required)
7. "What's the average time from incident detection to DWI notification across all incidents this year?" (regulatory performance)
8. "Compare incident frequency this AMP period vs. last AMP period for the top 10 worst-performing DMAs" (capital planning)

### Scene 8 (OPTIONAL — extended sessions only) — "Under the hood: ML-powered anomaly detection"
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

**Databricks features:** Many Model Forecasting (GA), Foundation time-series models (Chronos-Bolt/TimesFM), Model Serving (GA), `ai_query()` (GA).

**Key integration point:** All three levels write to the same `gold.anomaly_scores` table using the `scoring_method` column ('ai_forecast', 'automl', 'foundation_model'). The app, Genie Spaces, and dashboards work identically regardless of which method produced the scores — the ML section is a drop-in upgrade, not a parallel path.

### Demo Flow Summary

- **The Problem:** Business impact of their current state (latency, fragmented geospatial systems, 15 minutes to pull incident data manually, no anomaly detection, manual regulatory reporting, information loss at shift handover, no decision support for junior operators, no financial exposure visibility).
- **The Solution:** How the Databricks Lakehouse with geospatial capabilities solves this — from automated anomaly detection to root cause tracing to shift handover to executive briefing and regulatory report in under 60 seconds.
- **Step-by-Step Live Demo:** Follow the 9 scenes above (1, 2, 3, 4, 5, 6, 7a, 7b, and optionally 8 for extended sessions), mapping each to the underlying Databricks features: Mosaic AI for handover generation and response playbooks, SDP for data freshness and anomaly scoring, Delta+Lakebase for sub-second queries, the App for the interactive investigation, Metric Views for the aggregated stats and regulatory metrics, Operator Genie for operational ad-hoc exploration, Executive Genie for strategic/regulatory ad-hoc exploration, ML notebook for progressive anomaly detection (ai_forecast, AutoML, foundation models).
- **The Outcome:** "Day in the life" after adoption — the incoming operator inherits the incident via a structured handover, checks the alarm log, traces the root cause in seconds, sees the reservoir level and flow data, gets contextual SOP guidance, predicts escalation with a what-if model, briefs the exec with financial exposure numbers, has a draft regulatory report and audit trail ready, and triggers proactive customer comms — the full detect-investigate-respond-report-communicate lifecycle in one platform, all within the golden hour. The platform detected the incident 47 minutes before the first customer called, the response playbook guided a junior operator through the night, the shift handover ensured zero information loss, and the time from detection to executive briefing dropped from 45 minutes to zero — the data is always live on screen. Post-incident admin that used to consume 4-6 hours of senior operator time is completed in 30 minutes.

---

## 5. Sample Data Strategy

*Note: All table names, column names, and data types referenced in this section are defined authoritatively in Section 6 (Global Schema Contract). This section specifies **what data to generate and the relationships between entities**. For exact column definitions, see Section 6.*

### Entities & Fields
sensors, DMAs, PMAs, properties, pressure telemetry, flow telemetry, upstream assets (pump stations, trunk mains, isolation valves, PRVs), service reservoirs, customer contacts/complaints, anomaly_scores, dma_rag_history, incident_events, communications_log, response_playbooks, shift_handovers, comms_requests, dim_incidents — see Section 6 for full column definitions of each table.

### Volume & Realism
- **Time range:** 12 months of history (April 2025 — April 2026). The demo timestamp is **2026-04-07 05:30:00** (the moment the day-shift operator opens the app).
- **Scale:**
  - 500 DMAs (covering Greater London area)
  - ~100 PMAs (subset of DMAs with PRVs)
  - 10,000 sensors (8,000 pressure + 2,000 flow)
  - 50,000 properties (distributed across all DMAs)
  - 20 service reservoirs
  - ~500K pressure readings (7 days high-resolution at 15-min intervals for demo sensors, daily aggregates for historical)
  - ~100K flow readings (same cadence)
  - 30+ upstream assets (pump stations, trunk mains, isolation valves, PRVs)
- **Noise level:** Normal sensors should show realistic diurnal pressure patterns — higher at night (~55m), lower during morning demand (~40m), recovering mid-day. Add ±2-3m Gaussian noise. Flow sensors show similar demand-driven patterns. Amber DMAs should have mild pressure dips (25-35m) from planned maintenance or demand spikes — enough to trigger AMBER but not anomalous (sigma <2.0).

### Demo Scenario Contract (CRITICAL — hardcoded consistency requirements)

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
- The pump station should be connected to the affected DMAs via a `dim_asset_dma_feed` mapping table (asset_id → dma_code, with feed_type = 'primary' or 'secondary')

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
  - `02:30` | `rag_change` | `platform` | "DEMO_DMA_01 status: GREEN → RED"
  - `02:30` | `rag_change` | `platform` | "DEMO_DMA_02, DEMO_DMA_03 status: GREEN → AMBER"
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
- The demo timestamp (when the operator "opens the app") is **5:30am on the incident day (2026-04-07)**

### Verification Queries (must run after data generation — all must pass)

```sql
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

### Geospatial Realism — London area (MANDATORY)

- All generated coordinates must fall within the **Greater London area**. Bounding box: latitude **51.28N to 51.70N**, longitude **-0.51W to 0.33E**.
- `DEMO_DMA_01` (the incident DMA) should be centred roughly around **central-south London** (e.g., ~51.45N, -0.05E — Crystal Palace / Sydenham Hill area) so it's in the middle of the map and visually prominent. **[REFINED]** Crystal Palace / Sydenham Hill (~51.42N, -0.07W) is ideal — it's a genuinely hilly area (elevation 90-110m at the ridge) with lower-lying areas nearby, making the elevation-driven customer impact story geographically authentic.
- DMA polygons should be generated as **hexagonal or Voronoi-style tessellations** covering the Greater London area. Each polygon should be roughly 2-5 km across — large enough to be clearly visible and clickable on a map.
- Sensor locations must be **inside** their assigned DMA polygon. Property locations must be **inside** their assigned DMA polygon. Use ST_Contains or H3-based assignment to verify.
- Elevation data should roughly reflect London's real topography: low areas near the Thames (~5-15m), higher areas on hills like Highgate, Crystal Palace, Shooters Hill (~60-100m). `DEMO_DMA_01` should be in a hilly area so the elevation story (high-altitude customers lose water first) is geographically plausible.
- Geometry types: DMA/PMA boundaries as WKT POLYGON, sensors and properties as WKT POINT, trunk mains as WKT LINESTRING, plus separate lat/lon DOUBLE columns for point geometries.
- H3 resolution: use resolution 7 or 8 for DMA-level spatial indexing. H3 is used for two purposes: (1) **spatial joins** — assigning sensors/properties to DMAs via H3 cell membership is faster than ST_Contains for bulk operations during data generation; (2) **aggregation** — H3 cells enable heatmap-style visualisations in the app (e.g., pressure anomaly density). The canonical DMA assignment (sensor.dma_code, property.dma_code) should be verified with ST_Contains during data generation but can use H3 for the fast path.

### Generation Method
**Synthetic, deterministic, seed-based generation.** All data is generated programmatically using Python (Polars or PySpark) with fixed random seeds for reproducibility. The generation follows a "scaffolding" approach:

1. **Geographic scaffolding first:** Generate 500 DMA hexagonal polygons tessellating the Greater London bounding box. Assign DEMO_DMA_01/02/03 to the Crystal Palace area with specific centroid coordinates. All subsequent entity generation is constrained by these polygons.
2. **Dimension tables second:** Generate sensors (placed inside DMA polygons), properties (placed inside DMA polygons with elevation derived from DMA centroid elevation ± noise), assets (pump stations, trunk mains, valves placed at appropriate geographic positions), reservoirs.
3. **Fact tables third:** Generate 7+ days of 15-min telemetry for all sensors. Apply the incident scenario (pressure/flow drop at 02:03) to DEMO_DMA_01 sensors. Apply mild dips to DEMO_DMA_02/03 sensors.
4. **Gold layer last:** Compute anomaly_scores (rolling 7-day baseline comparison), dma_status (current RAG from latest telemetry), dma_rag_history (RAG per 15-min interval), dma_summary (aggregated with reservoir + incident links). Insert pre-baked incident events, communications, playbooks, and handover records.

### Reset & Re-seed Strategy
A single idempotent script (`scripts/reset_demo.py`) handles full teardown and rebuild:
1. **Drop** all tables in `water_digital_twin.bronze`, `water_digital_twin.silver`, `water_digital_twin.gold` schemas
2. **Re-run** data generation notebooks/scripts in dependency order (geography → dimensions → facts → gold)
3. **Run** all 22 verification queries and report pass/fail as a summary table
4. **Refresh** Lakebase tables (sync Gold layer to Lakebase serving tables)
5. Total reset time target: <15 minutes

**Demo health check script** (`scripts/demo_health_check.py`): Runs all 22 verification queries and reports pass/fail. SAs should run this 10 minutes before any demo. If any query fails, the script outputs the failing query, expected result, and actual result for quick diagnosis.

---

## 6. Global Schema Contract (Strict Data Dictionary)

*This is the **single authoritative source** for all table names, column names, and data types. Sections 4 and 5 reference entities by name — this section defines them. Downstream coding agents will rely on this as the absolute source of truth to ensure their scripts connect properly. If any other section appears to contradict this schema, this section wins.*

**Naming conventions (mandatory):**
- Use three-part names: `<catalog>.<schema>.<table>` (default catalog: `water_digital_twin`)
- Follow medallion architecture schemas: `bronze`, `silver`, `gold`
  - `water_digital_twin.bronze.*` — raw ingested data
  - `water_digital_twin.silver.*` — cleansed, conformed tables with geospatial columns
  - `water_digital_twin.gold.*` — business-level aggregates, feature tables, and metric views (`mv_` prefix)
- Geospatial columns should use STRING type with WKT format for portability, plus DOUBLE lat/lon columns for point geometries

### Bronze Layer Tables

| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.bronze.raw_telemetry` | sensor_id | STRING | Sensor identifier from OSI PI |
| | timestamp | TIMESTAMP | Reading timestamp (UTC) |
| | value | DOUBLE | Raw sensor reading value (pressure in metres head, or flow in l/s depending on sensor type) |
| | quality_flag | STRING | PI quality indicator: 'good', 'suspect', 'bad' |
| | source_system | STRING | Always 'osi_pi' for this demo |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |
| `water_digital_twin.bronze.raw_assets` | asset_id | STRING | Asset identifier |
| | asset_type | STRING | Asset type from source system |
| | name | STRING | Asset display name |
| | status | STRING | Raw status string |
| | latitude | DOUBLE | Latitude (WGS84) |
| | longitude | DOUBLE | Longitude (WGS84) |
| | geometry_wkt | STRING | WKT geometry (POINT, LINESTRING, or POLYGON) |
| | metadata_json | STRING | Additional properties as JSON |
| | source_system | STRING | Source: 'gis', 'scada', 'sap' |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |
| `water_digital_twin.bronze.raw_customer_contacts` | uprn | STRING | Unique Property Reference Number |
| | address | STRING | Full address string |
| | postcode | STRING | UK postcode |
| | latitude | DOUBLE | Property latitude |
| | longitude | DOUBLE | Property longitude |
| | property_type | STRING | Raw property classification |
| | dma_code | STRING | DMA assignment from source |
| | customer_height | DOUBLE | Property elevation in metres |
| | source_system | STRING | 'sap' or 'salesforce' |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |
| `water_digital_twin.bronze.raw_complaints` | complaint_id | STRING | Complaint identifier |
| | uprn | STRING | Property UPRN |
| | dma_code | STRING | DMA code |
| | complaint_timestamp | TIMESTAMP | When complaint was received |
| | complaint_type | STRING | Raw complaint type |
| | source_system | STRING | Always 'salesforce' |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |
| `water_digital_twin.bronze.raw_dma_boundaries` | dma_code | STRING | DMA identifier |
| | dma_name | STRING | DMA display name |
| | dma_area_code | STRING | DMA area code (may differ from dma_code in source) |
| | geometry_wkt | STRING | WKT POLYGON boundary |
| | source_system | STRING | 'gis' |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |
| `water_digital_twin.bronze.raw_pma_boundaries` | pma_code | STRING | PMA identifier |
| | pma_name | STRING | PMA display name |
| | dma_code | STRING | Parent DMA code |
| | geometry_wkt | STRING | WKT POLYGON boundary |
| | source_system | STRING | 'gis' |
| | ingested_at | TIMESTAMP | SDP ingestion timestamp |

### Silver Layer Tables

| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.silver.dim_sensor` | sensor_id | STRING | **PK.** Sensor identifier (e.g., 'DEMO_SENSOR_01', 'DEMO_FLOW_01') |
| | name | STRING | Sensor display name |
| | sensor_type | STRING | **Enum: 'pressure', 'flow'.** Distinguishes pressure from flow sensors. |
| | dma_code | STRING | **FK → dim_dma.dma_code.** DMA assignment. |
| | pma_code | STRING | FK → dim_pma.pma_code. PMA assignment (nullable). |
| | latitude | DOUBLE | Sensor latitude (WGS84) |
| | longitude | DOUBLE | Sensor longitude (WGS84) |
| | elevation | DOUBLE | Sensor elevation in metres above sea level |
| | geometry_wkt | STRING | WKT POINT geometry |
| | h3_index | STRING | H3 cell index at resolution 8 |
| | is_active | BOOLEAN | Whether sensor is currently reporting |
| | installed_date | DATE | Installation date |
| `water_digital_twin.silver.dim_dma` | dma_code | STRING | **PK.** DMA identifier (e.g., 'DEMO_DMA_01') |
| | dma_name | STRING | DMA display name (e.g., 'Crystal Palace South') |
| | dma_area_code | STRING | DMA area code (used in joins with existing metric views) |
| | geometry_wkt | STRING | WKT POLYGON boundary |
| | centroid_latitude | DOUBLE | Polygon centroid latitude |
| | centroid_longitude | DOUBLE | Polygon centroid longitude |
| | avg_elevation | DOUBLE | Average ground elevation in metres |
| | h3_index | STRING | H3 cell index of centroid at resolution 7 |
| | pressure_red_threshold | DOUBLE | Pressure below this = RED (default: 15.0m) |
| | pressure_amber_threshold | DOUBLE | Pressure below this = AMBER (default: 25.0m) |
| `water_digital_twin.silver.dim_pma` | pma_code | STRING | **PK.** PMA identifier |
| | pma_name | STRING | PMA display name |
| | dma_code | STRING | **FK → dim_dma.dma_code.** Parent DMA. |
| | geometry_wkt | STRING | WKT POLYGON boundary |
| | centroid_latitude | DOUBLE | Polygon centroid latitude |
| | centroid_longitude | DOUBLE | Polygon centroid longitude |
| `water_digital_twin.silver.dim_properties` | uprn | STRING | **PK.** Unique Property Reference Number |
| | address | STRING | Street address |
| | postcode | STRING | UK postcode |
| | property_type | STRING | **Enum: 'domestic', 'school', 'hospital', 'commercial', 'nursery', 'key_account', 'dialysis_home'.** |
| | dma_code | STRING | **FK → dim_dma.dma_code.** DMA assignment. |
| | pma_code | STRING | FK → dim_pma.pma_code. PMA assignment (nullable). |
| | customer_height | DOUBLE | Property elevation in metres above sea level |
| | latitude | DOUBLE | Property latitude (WGS84) |
| | longitude | DOUBLE | Property longitude (WGS84) |
| | geometry_wkt | STRING | WKT POINT geometry |
| | h3_index | STRING | H3 cell index at resolution 8 |
| `water_digital_twin.silver.dim_assets` | asset_id | STRING | **PK.** Asset identifier (e.g., 'DEMO_PUMP_01', 'DEMO_TM_001') |
| | asset_type | STRING | **Enum: 'pump_station', 'trunk_main', 'isolation_valve', 'prv', 'treatment_works'.** |
| | name | STRING | Asset display name |
| | status | STRING | **Enum: 'operational', 'tripped', 'failed', 'maintenance', 'decommissioned'.** |
| | latitude | DOUBLE | Asset latitude (for point assets; NULL for linear assets) |
| | longitude | DOUBLE | Asset longitude (for point assets; NULL for linear assets) |
| | geometry_wkt | STRING | WKT geometry — POINT for stations/valves, LINESTRING for trunk mains |
| | diameter_inches | INT | Pipe diameter (for trunk_main only, NULL otherwise) |
| | length_km | DOUBLE | Asset length in km (for trunk_main only, NULL otherwise) |
| | trip_timestamp | TIMESTAMP | When the asset tripped/failed (NULL if operational) |
| | installed_date | DATE | Installation date |
| `water_digital_twin.silver.dim_asset_dma_feed` | asset_id | STRING | **FK → dim_assets.asset_id.** Upstream asset. |
| | dma_code | STRING | **FK → dim_dma.dma_code.** Fed DMA. |
| | feed_type | STRING | **Enum: 'primary', 'secondary'.** |
| `water_digital_twin.silver.dim_reservoirs` | reservoir_id | STRING | **PK.** Reservoir identifier (e.g., 'DEMO_SR_01') |
| | name | STRING | Reservoir display name |
| | capacity_ml | DOUBLE | Total capacity in megalitres |
| | current_level_pct | DOUBLE | Current fill level as percentage (0-100) |
| | hourly_demand_rate_ml | DOUBLE | Current demand rate in megalitres/hour. Used to compute hours remaining: `(current_level_pct / 100 * capacity_ml) / hourly_demand_rate_ml` |
| | latitude | DOUBLE | Reservoir latitude (WGS84) |
| | longitude | DOUBLE | Reservoir longitude (WGS84) |
| | geometry_wkt | STRING | WKT POINT geometry |
| `water_digital_twin.silver.dim_reservoir_dma_feed` | reservoir_id | STRING | **FK → dim_reservoirs.reservoir_id.** |
| | dma_code | STRING | **FK → dim_dma.dma_code.** |
| | feed_type | STRING | **Enum: 'primary', 'secondary'.** |
| `water_digital_twin.silver.fact_telemetry` | sensor_id | STRING | **FK → dim_sensor.sensor_id.** |
| | timestamp | TIMESTAMP | Reading timestamp (15-min intervals, UTC) |
| | sensor_type | STRING | **Enum: 'pressure', 'flow'.** Denormalised from dim_sensor for query performance. |
| | value | DOUBLE | Pressure reading in metres head. **Populated for sensor_type = 'pressure', NULL for 'flow'.** |
| | total_head_pressure | DOUBLE | Absolute pressure at sensor (= value for pressure sensors). Used in property-level effective pressure calculation. **NULL for flow sensors.** |
| | flow_rate | DOUBLE | Flow rate in litres/second. **Populated for sensor_type = 'flow', NULL for 'pressure'.** |
| | quality_flag | STRING | Data quality: 'good', 'suspect', 'bad'. Inherited from Bronze, refined by SDP expectations. |
| `water_digital_twin.silver.customer_complaints` | complaint_id | STRING | **PK.** Complaint identifier |
| | uprn | STRING | **FK → dim_properties.uprn.** Complaining property. |
| | dma_code | STRING | **FK → dim_dma.dma_code.** |
| | complaint_timestamp | TIMESTAMP | When complaint was received |
| | complaint_type | STRING | **Enum: 'no_water', 'low_pressure', 'discoloured_water', 'other'.** |

### Gold Layer — Computed Status Tables

| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.gold.dma_status` | dma_code | STRING | **PK. FK → dim_dma.dma_code.** |
| | rag_status | STRING | **Enum: 'GREEN', 'AMBER', 'RED'.** Current status computed from latest 15-min telemetry window. |
| | avg_pressure | DOUBLE | Average pressure across all sensors in the DMA (metres head) |
| | min_pressure | DOUBLE | Minimum sensor pressure in the DMA |
| | sensor_count | INT | Number of active sensors in the DMA |
| | property_count | INT | Total properties in the DMA |
| | sensitive_premises_count | INT | Count of properties with property_type IN ('hospital', 'school', 'dialysis_home') |
| | has_active_incident | BOOLEAN | TRUE if the DMA has an active incident in dim_incidents |
| | last_updated | TIMESTAMP | When this row was last refreshed |
| `water_digital_twin.gold.dma_rag_history` | dma_code | STRING | **Composite PK (dma_code, timestamp). FK → dim_dma.** |
| | timestamp | TIMESTAMP | 15-minute interval timestamp |
| | rag_status | STRING | **Enum: 'GREEN', 'AMBER', 'RED'.** RAG status at this interval. |
| | avg_pressure | DOUBLE | Average pressure at this interval |
| | min_pressure | DOUBLE | Minimum pressure at this interval |
| `water_digital_twin.gold.anomaly_scores` | sensor_id | STRING | **Composite PK (sensor_id, timestamp). FK → dim_sensor.** |
| | timestamp | TIMESTAMP | Telemetry reading timestamp |
| | anomaly_sigma | DOUBLE | Standard deviations from 7-day same-time-of-day baseline. >3.0 = high-confidence anomaly. |
| | baseline_value | DOUBLE | The expected value (7-day average for this time-of-day) |
| | actual_value | DOUBLE | The observed sensor reading |
| | is_anomaly | BOOLEAN | TRUE if anomaly_sigma > 2.5 |
| | scoring_method | STRING | **Enum: 'statistical', 'ai_forecast', 'automl', 'foundation_model'.** Default: 'statistical'. Identifies which approach produced the score. |
| `water_digital_twin.gold.dma_summary` | dma_code | STRING | **PK. FK → dim_dma.dma_code.** |
| | dma_name | STRING | DMA display name (denormalised) |
| | rag_status | STRING | Current RAG status (denormalised from dma_status) |
| | avg_pressure | DOUBLE | Average pressure in metres head |
| | avg_flow | DOUBLE | Average inlet flow rate in l/s |
| | property_count | INT | Total properties in DMA |
| | sensor_count | INT | Active sensor count |
| | sensitive_premises_count | INT | Hospital + school + dialysis_home count |
| | feeding_reservoir_id | STRING | FK → dim_reservoirs.reservoir_id. Primary feeding reservoir. |
| | reservoir_level_pct | DOUBLE | Current reservoir fill level % (denormalised) |
| | reservoir_hours_remaining | DOUBLE | Estimated hours of supply: `(current_level_pct / 100 * capacity_ml) / hourly_demand_rate_ml` |
| | active_incident_id | STRING | FK → dim_incidents.incident_id. NULL if no active incident. |
| | active_complaints_count | INT | Number of complaints in last 24 hours |
| | last_updated | TIMESTAMP | Last refresh timestamp |

### Gold Layer — Incident Management Tables

| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.gold.dim_incidents` | incident_id | STRING | **PK.** Format: 'INC-YYYY-MMDD-NNN' |
| | dma_code | STRING | **FK → dim_dma.dma_code.** Primary affected DMA. |
| | root_cause_asset_id | STRING | FK → dim_assets.asset_id. The asset that caused the incident. |
| | start_timestamp | TIMESTAMP | Incident start time |
| | end_timestamp | TIMESTAMP | Incident end time. **NULL if ongoing.** |
| | status | STRING | **Enum: 'active', 'resolved', 'closed'.** |
| | severity | STRING | **Enum: 'low', 'medium', 'high', 'critical'.** |
| | total_properties_affected | INT | Total properties impacted |
| | sensitive_premises_affected | BOOLEAN | TRUE if hospitals/schools/dialysis_home are in the affected area |
| `water_digital_twin.gold.incident_events` | event_id | STRING | **PK.** Auto-generated unique ID. |
| | incident_id | STRING | **FK → dim_incidents.incident_id.** |
| | timestamp | TIMESTAMP | Event timestamp |
| | event_type | STRING | **Enum: 'asset_trip', 'anomaly_detected', 'alert_acknowledged', 'rag_change', 'customer_complaint', 'crew_dispatched', 'regulatory_notification', 'customer_comms', 'shift_handover'.** |
| | source | STRING | **Enum: 'scada', 'platform', 'operator', 'salesforce'.** |
| | description | STRING | Human-readable event description |
| | operator_id | STRING | Operator who triggered the event (NULL for automated events) |
| `water_digital_twin.gold.communications_log` | log_id | STRING | **PK.** Auto-generated unique ID. |
| | incident_id | STRING | **FK → dim_incidents.incident_id.** |
| | timestamp | TIMESTAMP | Communication timestamp |
| | contact_role | STRING | Who was contacted (e.g., 'Network Manager', 'DWI Duty Officer') |
| | method | STRING | **Enum: 'phone', 'email', 'teams'.** |
| | summary | STRING | Brief description of the communication |
| | action_agreed | STRING | What was agreed/decided |
| | operator_id | STRING | Operator who made the communication |
| `water_digital_twin.gold.dim_response_playbooks` | playbook_id | STRING | **PK.** Auto-generated unique ID. |
| | incident_type | STRING | **Enum: 'pump_station_trip', 'trunk_main_burst', 'prv_failure', 'water_quality'.** |
| | sop_reference | STRING | SOP document reference (e.g., 'SOP-WN-042') |
| | action_steps | STRING | JSON array. Each element: `{step_number: INT, description: STRING, is_mandatory: BOOLEAN, typical_timeframe: STRING}` |
| | last_updated_by | STRING | Who last modified the playbook |
| | last_updated_at | TIMESTAMP | Last modification timestamp |
| `water_digital_twin.gold.shift_handovers` | handover_id | STRING | **PK.** Auto-generated unique ID. |
| | incident_id | STRING | **FK → dim_incidents.incident_id.** |
| | outgoing_operator | STRING | Operator handing over |
| | incoming_operator | STRING | Operator receiving |
| | generated_summary | STRING | Auto-generated narrative from Mosaic AI (incident events + comms log + current Gold-layer state) |
| | risk_of_escalation | STRING | **Enum: 'low', 'medium', 'high'.** Computed from: pressure trend, reservoir level vs. demand, complaint rate trend, supply restoration status. |
| | current_trajectory | STRING | Auto-generated: 'stabilising', 'deteriorating', 'recovering' |
| | operator_edits | STRING | Manual changes the outgoing operator made to the generated summary |
| | signed_off_at | TIMESTAMP | When outgoing operator signed off |
| | acknowledged_at | TIMESTAMP | When incoming operator acknowledged |
| `water_digital_twin.gold.comms_requests` | request_id | STRING | **PK.** Auto-generated unique ID. |
| | incident_id | STRING | **FK → dim_incidents.incident_id.** |
| | dma_code | STRING | **FK → dim_dma.dma_code.** |
| | requested_by | STRING | Operator ID who triggered the request |
| | requested_at | TIMESTAMP | Request timestamp |
| | message_template | STRING | Pre-drafted customer notification text |
| | affected_postcodes | STRING | Comma-separated list of affected postcodes |
| | estimated_restoration_time | STRING | Manually set by operator (operators control ETAs) |
| | customer_count | INT | Number of customers to be notified |
| | status | STRING | **Enum: 'pending', 'sent', 'cancelled'.** |

### Gold Layer — Views (for Genie Space consumption)

| Full Table Name | Description | Source | Key Joins |
|---|---|---|---|
| `water_digital_twin.gold.vw_dma_pressure` | Aggregated DMA-level pressure | fact_telemetry WHERE sensor_type = 'pressure' | JOIN dim_dma ON dma_code |
| `water_digital_twin.gold.vw_pma_pressure` | Aggregated PMA-level pressure | fact_telemetry WHERE sensor_type = 'pressure' | JOIN dim_pma ON pma_code |
| `water_digital_twin.gold.vw_property_pressure` | Property-level effective pressure (total_head_pressure - customer_height) | fact_telemetry | JOIN dim_properties ON dma_code, compute effective_pressure |
| `water_digital_twin.gold.vw_dma_pressure_sensor` | Sensor-level pressure within DMAs | fact_telemetry WHERE sensor_type = 'pressure' | JOIN dim_sensor ON sensor_id |

### Metric Views (Gold Layer)

**`water_digital_twin.gold.mv_dma_pressure`**
```yaml
name: mv_dma_pressure
description: "Average, min, and max pressure by DMA over time. Primary metric view for operator Genie Space."
source: "SELECT * FROM water_digital_twin.gold.vw_dma_pressure"
joins:
  - table: water_digital_twin.silver.dim_dma
    on: "dma_code = dim_dma.dma_area_code"
    type: LEFT
dimensions:
  - name: telemetry_time
    column: timestamp
    display_name: "Reading Time"
    format: "yyyy-MM-dd HH:mm"
    description: "Timestamp of the 15-minute telemetry reading window"
  - name: dma_code
    column: dma_code
    display_name: "DMA Code"
    description: "District Metered Area identifier"
  - name: dma_name
    column: dim_dma.dma_name
    display_name: "DMA Name"
    description: "District Metered Area display name"
measures:
  - name: avg_pressure
    expression: "AVG(value)"
    display_name: "Avg Pressure (m)"
    format: "#,##0.0"
    description: "Average pressure in metres head across all sensors in the DMA"
    is_measure: true
  - name: max_pressure
    expression: "MAX(value)"
    display_name: "Max Pressure (m)"
    format: "#,##0.0"
    description: "Maximum pressure reading in the DMA"
    is_measure: true
  - name: min_pressure
    expression: "MIN(value)"
    display_name: "Min Pressure (m)"
    format: "#,##0.0"
    description: "Minimum pressure reading in the DMA — low values indicate supply issues"
    is_measure: true
  - name: avg_total_head_pressure
    expression: "AVG(total_head_pressure)"
    display_name: "Avg Total Head (m)"
    format: "#,##0.0"
    description: "Average absolute pressure (total head) in metres"
    is_measure: true
  - name: reading_count
    expression: "COUNT(sensor_id)"
    display_name: "Reading Count"
    format: "#,##0"
    description: "Number of sensor readings in the time window"
    is_measure: true
```

**`water_digital_twin.gold.mv_flow_anomaly`**
```yaml
name: mv_flow_anomaly
description: "Flow rate deviation by DMA entry point. Detects supply interruptions (low flow) and potential bursts (abnormally high flow)."
source: "SELECT * FROM water_digital_twin.silver.fact_telemetry WHERE sensor_type = 'flow'"
joins:
  - table: water_digital_twin.silver.dim_sensor
    on: "fact_telemetry.sensor_id = dim_sensor.sensor_id"
    type: INNER
  - table: water_digital_twin.silver.dim_dma
    on: "dim_sensor.dma_code = dim_dma.dma_code"
    type: LEFT
dimensions:
  - name: telemetry_time
    column: timestamp
    display_name: "Reading Time"
    format: "yyyy-MM-dd HH:mm"
    description: "Timestamp of the 15-minute telemetry reading"
  - name: dma_code
    column: dim_sensor.dma_code
    display_name: "DMA Code"
    description: "District Metered Area identifier"
  - name: dma_name
    column: dim_dma.dma_name
    display_name: "DMA Name"
    description: "District Metered Area display name"
  - name: sensor_id
    column: dim_sensor.sensor_id
    display_name: "Flow Sensor"
    description: "Flow sensor identifier at DMA entry point"
measures:
  - name: avg_flow_rate
    expression: "AVG(flow_rate)"
    display_name: "Avg Flow (l/s)"
    format: "#,##0.0"
    description: "Average flow rate in litres per second at the DMA entry point"
    is_measure: true
  - name: min_flow_rate
    expression: "MIN(flow_rate)"
    display_name: "Min Flow (l/s)"
    format: "#,##0.0"
    description: "Minimum flow rate — low values indicate supply interruption"
    is_measure: true
  - name: max_flow_rate
    expression: "MAX(flow_rate)"
    display_name: "Max Flow (l/s)"
    format: "#,##0.0"
    description: "Maximum flow rate — high values may indicate a burst"
    is_measure: true
  - name: flow_reading_count
    expression: "COUNT(flow_rate)"
    display_name: "Reading Count"
    format: "#,##0"
    description: "Number of flow readings in the time window"
    is_measure: true
```

**`water_digital_twin.gold.mv_reservoir_status`**
```yaml
name: mv_reservoir_status
description: "Service reservoir current status, estimated hours of supply remaining, and fed DMA mapping. Critical for incident urgency assessment."
source: "SELECT * FROM water_digital_twin.silver.dim_reservoirs"
joins:
  - table: water_digital_twin.silver.dim_reservoir_dma_feed
    on: "dim_reservoirs.reservoir_id = dim_reservoir_dma_feed.reservoir_id"
    type: LEFT
  - table: water_digital_twin.silver.dim_dma
    on: "dim_reservoir_dma_feed.dma_code = dim_dma.dma_code"
    type: LEFT
dimensions:
  - name: reservoir_id
    column: dim_reservoirs.reservoir_id
    display_name: "Reservoir ID"
    description: "Service reservoir identifier"
  - name: reservoir_name
    column: dim_reservoirs.name
    display_name: "Reservoir Name"
    description: "Service reservoir display name"
  - name: fed_dma_code
    column: dim_reservoir_dma_feed.dma_code
    display_name: "Fed DMA Code"
    description: "DMA code of a district fed by this reservoir"
  - name: fed_dma_name
    column: dim_dma.dma_name
    display_name: "Fed DMA Name"
    description: "DMA name of a district fed by this reservoir"
  - name: feed_type
    column: dim_reservoir_dma_feed.feed_type
    display_name: "Feed Type"
    description: "Whether this is the primary or secondary feed for the DMA"
measures:
  - name: current_level_pct
    expression: "MAX(dim_reservoirs.current_level_pct)"
    display_name: "Current Level (%)"
    format: "#,##0.0"
    description: "Current reservoir fill level as a percentage (0-100)"
    is_measure: true
  - name: capacity_ml
    expression: "MAX(dim_reservoirs.capacity_ml)"
    display_name: "Capacity (ML)"
    format: "#,##0.0"
    description: "Total reservoir capacity in megalitres"
    is_measure: true
  - name: hours_remaining
    expression: "MAX((dim_reservoirs.current_level_pct / 100.0 * dim_reservoirs.capacity_ml) / dim_reservoirs.hourly_demand_rate_ml)"
    display_name: "Est. Hours Remaining"
    format: "#,##0.0"
    description: "Estimated hours of supply remaining at current demand rate"
    is_measure: true
```

**`water_digital_twin.gold.mv_regulatory_compliance`**
```yaml
name: mv_regulatory_compliance
description: "Regulatory compliance metrics: properties exceeding Ofwat thresholds, sensitive premises counts, estimated OPA penalty exposure. For executive Genie Space and regulatory dashboard."
source: >
  SELECT
    ds.dma_code,
    ds.rag_status,
    ds.property_count,
    ds.sensitive_premises_count,
    di.incident_id,
    di.start_timestamp,
    di.total_properties_affected,
    di.sensitive_premises_affected,
    TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) as hours_since_start,
    CASE WHEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) > 3
         THEN di.total_properties_affected ELSE 0 END as properties_exceeding_3h,
    CASE WHEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) > 12
         THEN di.total_properties_affected ELSE 0 END as properties_exceeding_12h
  FROM water_digital_twin.gold.dma_status ds
  LEFT JOIN water_digital_twin.gold.dim_incidents di
    ON ds.dma_code = di.dma_code AND di.status = 'active'
joins: []
dimensions:
  - name: dma_code
    column: dma_code
    display_name: "DMA Code"
    description: "District Metered Area identifier"
  - name: incident_id
    column: incident_id
    display_name: "Incident ID"
    description: "Active incident identifier (NULL if no active incident)"
  - name: rag_status
    column: rag_status
    display_name: "RAG Status"
    description: "Current Red/Amber/Green status"
measures:
  - name: properties_exceeding_3h
    expression: "SUM(properties_exceeding_3h)"
    display_name: "Properties >3h"
    format: "#,##0"
    description: "Properties exceeding the 3-hour Ofwat supply interruption threshold"
    is_measure: true
  - name: properties_exceeding_12h
    expression: "SUM(properties_exceeding_12h)"
    display_name: "Properties >12h"
    format: "#,##0"
    description: "Properties exceeding the 12-hour escalated penalty threshold"
    is_measure: true
  - name: sensitive_premises_total
    expression: "SUM(sensitive_premises_count)"
    display_name: "Sensitive Premises"
    format: "#,##0"
    description: "Total hospitals, schools, and dialysis homes in affected DMAs"
    is_measure: true
  - name: estimated_opa_penalty
    expression: "SUM(properties_exceeding_3h * (hours_since_start - 3) * 580)"
    display_name: "Est. OPA Penalty (£)"
    format: "£#,##0"
    description: "Estimated Ofwat OPA penalty: properties_exceeding_3h × additional_hours × £580/property/hour"
    is_measure: true
  - name: total_properties_affected
    expression: "SUM(total_properties_affected)"
    display_name: "Total Affected"
    format: "#,##0"
    description: "Total properties affected across all active incidents"
    is_measure: true
```

**`water_digital_twin.gold.mv_incident_summary`**
```yaml
name: mv_incident_summary
description: "Active and historical incident summary: total properties affected, regulatory deadlines, actions outstanding. For executive Genie Space."
source: "SELECT * FROM water_digital_twin.gold.dim_incidents"
joins:
  - table: water_digital_twin.gold.dma_summary
    on: "dim_incidents.dma_code = dma_summary.dma_code"
    type: LEFT
dimensions:
  - name: incident_id
    column: dim_incidents.incident_id
    display_name: "Incident ID"
    description: "Incident identifier"
  - name: dma_code
    column: dim_incidents.dma_code
    display_name: "DMA Code"
    description: "Primary affected DMA"
  - name: status
    column: dim_incidents.status
    display_name: "Status"
    description: "Incident status: active, resolved, or closed"
  - name: severity
    column: dim_incidents.severity
    display_name: "Severity"
    description: "Incident severity: low, medium, high, critical"
  - name: start_date
    column: dim_incidents.start_timestamp
    display_name: "Start Time"
    format: "yyyy-MM-dd HH:mm"
    description: "When the incident began"
measures:
  - name: total_properties_affected
    expression: "SUM(dim_incidents.total_properties_affected)"
    display_name: "Total Properties Affected"
    format: "#,##0"
    description: "Sum of properties affected across incidents"
    is_measure: true
  - name: active_incident_count
    expression: "COUNT(CASE WHEN dim_incidents.status = 'active' THEN 1 END)"
    display_name: "Active Incidents"
    format: "#,##0"
    description: "Number of currently active incidents"
    is_measure: true
  - name: incidents_with_sensitive_premises
    expression: "COUNT(CASE WHEN dim_incidents.sensitive_premises_affected = TRUE THEN 1 END)"
    display_name: "Incidents w/ Sensitive Premises"
    format: "#,##0"
    description: "Incidents affecting hospitals, schools, or dialysis centres (DWI reportable)"
    is_measure: true
  - name: avg_duration_hours
    expression: "AVG(TIMESTAMPDIFF(HOUR, dim_incidents.start_timestamp, COALESCE(dim_incidents.end_timestamp, CURRENT_TIMESTAMP())))"
    display_name: "Avg Duration (hrs)"
    format: "#,##0.0"
    description: "Average incident duration in hours"
    is_measure: true
```

</demo_plan>
