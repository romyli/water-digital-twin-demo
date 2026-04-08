You are a Principal Databricks Solutions Architect. Your goal is to design a high-impact geospatial demo plan for a water utility digital twin use case on Databricks.

<context>
Below, in the <use_case_brief> tags, I have provided an anonymized summary of a real water utility engagement. You must rely strictly on the information provided combined with your expert knowledge of the Databricks Data Intelligence Platform (including Unity Catalog, Spark Declarative Pipelines, Databricks SQL, Lakebase, Databricks Apps, Spatial SQL, Metric Views, AI/BI Genie Spaces, etc.).

This demo is intended for **field use** — it must be reusable, self-contained, and showcase Databricks geospatial capabilities in a water utility context. It is NOT a customer-specific demo; it is an industry demo inspired by a real use case.
</context>

<rules>
1. DO NOT write any code, SQL, or Databricks notebooks in this step.
2. This output is a blueprint. Another AI agent will use your exact output to implement the demo, so your instructions, build order, and asset lists must be highly specific, actionable, and leave no room for guessing.
3. All names, companies, and identifying details must be fictional. Use "Water Utilities" as the company name. Use generic role titles instead of real names.
4. You must actively use your Glean search tool to find the latest Databricks features currently in Public or Private Preview. Only include them if they directly solve a challenge identified in the use case brief.
</rules>

<instructions>
Step 1: Deep-Dive Analysis
Analyze the use case brief and identify: the primary personas (roles, decision power, technical depth), their top prioritized pains/challenges around geospatial data, and their key aspirations ("what good looks like").

Step 2: Preview Feature Discovery (via Glean)
Search Glean for the newest Databricks capabilities currently in preview relevant to geospatial processing — e.g., Spatial SQL functions (ST_ functions, H3), Lakebase GA status, Databricks Apps features, Metric View enhancements, Genie improvements. Select 1-3 preview features that represent forward-looking value specifically for this geospatial water utility use case.

Step 3: Demo Plan Construction
Based on your analysis and Glean research, construct a comprehensive Databricks demo plan. The demo must showcase these five pillars in an integrated storyline:

1. **Spark Declarative Pipelines (SDP)** — Ingest and transform raw sensor, asset, and geospatial data through a medallion architecture (Bronze → Silver → Gold). Show how SDP handles streaming sensor telemetry and batch geospatial reference data in a single pipeline.

2. **Delta Tables + Lakebase with Geospatial Transformations** — Show Delta tables storing geospatial data (WKT/WKB polygons for districts, point geometries for sensors/properties). Show Spatial SQL transformations (ST_Point, ST_Contains, ST_Distance, ST_Buffer, H3 indexing). Then show Lakebase as the low-latency serving layer for the app — replacing the need for an external SQL Managed Instance.

3. **Databricks Apps with Geospatial Visualisation** — A React + FastAPI app deployed on Databricks Apps that displays an interactive map (e.g., using Mapbox/Leaflet/DeckGL). The app reads from Lakebase for sub-second map tile loading. Show: DMA/PMA polygon overlays colour-coded by pressure status (RAG), sensor locations, property locations, and time-series pressure charts on click.

4. **Metric Views on the Gold Layer** — Create well-structured metric views (mv_ prefix) on the Gold layer that define reusable business metrics: average pressure by DMA, sensor reading counts, anomaly rates, customer impact counts. These metric views are the single source of truth consumed by both the Genie Space and dashboards.

5. **Curated Genie Space for Geospatial Data** — A well-configured AI/BI Genie Space backed by the metric views and dimension tables. Include: a focused system prompt for water network operations, 5-7 sample questions that showcase geospatial awareness (e.g., "Which DMAs have average pressure below threshold?", "How many customers are in DMA X?", "Show pressure trend for sensor Y over the last 7 days"), and verified queries for the most common questions.

Ensure you incorporate an elevator pitch, timing, technical prerequisites, build order, and practical framing for a field demo audience.
</instructions>

<output_format>
Please format your response using the exact structure below:

<analysis>
- Primary Persona(s): [Details]
- Top Pains/Challenges: [Prioritized list with brief explanations]
- Key Aspirations: [Future state / what good looks like]
</analysis>

<demo_plan>
## 1. Executive Summary & Strategy
- Elevator Pitch: [1-2 sentences summarizing the demo's value for water utility geospatial operations]
- Estimated Demo Timing: [e.g., 45 mins total: 10 min context, 25 min live demo, 10 min Q&A]
- Target Audience Framing: [How to position this demo for different audiences — technical engineers vs. leadership vs. operations]

## 2. Main Messaging Pillars (3-5 Pillars)
*For each pillar include:*
- Message: [One-line message]
- Relevance: [Why a water utility operations persona cares]
- Proof Point: [Which specific part of the demo proves this]

## 3. Key Features & Preview Capabilities Mapped to Value
- [Standard Feature 1, e.g., Unity Catalog]: [Explicitly map to a pain/aspiration identified in your analysis]
- [Standard Feature 2]: [Map to pain/aspiration]
- 🚀 [Glean Preview Feature 1]: [Exactly how it solves a geospatial edge-case or future goal] | **Stage:** [Private/Public Preview/Beta] | **Timeline:** [e.g., GA expected Q4]
- 🚀 [Glean Preview Feature 2]: [Map to pain/aspiration] | **Stage:** [Private/Public Preview/Beta] | **Timeline:** [Expected timeline/GA date]

## 4. Demo Storyline & Talk Track
*The demo follows a single, continuous incident investigation story. A control room operator arrives at 5am to find a pressure anomaly that started at 2am. The demo walks through how they investigate and respond using the platform.*

**The narrative arc (the app is the centrepiece):**

**Scene 1 — "Open the map, see the problem — but not too many problems"**
The operator opens the Databricks App and sees the network map. DMA polygons are colour-coded Red/Amber/Green by current pressure status. Most of the 500 DMAs are green — this is a normal night. A few are amber (planned maintenance, demand spikes — normal noise). But one cluster stands out: `DEMO_DMA_01` is red, flanked by two amber neighbours. Crucially, the red DMA has an **anomaly confidence badge** (e.g., "High — 3.2σ deviation from 7-day baseline") that distinguishes it from the amber DMAs that are just routine fluctuation. This is the "wall of green" problem solved — the platform cuts through alert fatigue and tells the operator which red actually matters.

**Scene 2 — "Trace upstream to the cause"**
The operator clicks the red DMA. The side panel shows DMA summary stats, but also highlights the **upstream asset that caused the problem**: a pump station (`DEMO_PUMP_01`) that feeds this DMA via a trunk main section. The panel shows: "Pump station DEMO_PUMP_01 tripped at 02:03. Downstream impact: 3 DMAs affected." This is the real investigative flow — a DMA going red is a symptom; the operator needs to find the upstream cause (a pump trip, a burst trunk main, a stuck PRV). The sensor in the DMA merely *reports* the failure happening upstream.

**Scene 3 — "See the timeline, not just the current state"**
The operator clicks on the pump station asset. A detail panel opens with:
- A **pressure time-series chart** (past 24h, 15-min intervals) showing stable pressure at ~50m, then a sharp drop to ~8m at 02:03
- A **RAG status timeline strip** along the bottom: a sequence of green-green-green-amber-red-red blocks, showing exactly when the DMA transitioned through each state. Operators call this "trend of the trend" — is it getting worse or stabilising? In this case, the red blocks are flat (not deepening), suggesting the burst has stabilised but supply is still interrupted.
- An **anomaly detection flag**: the system detected this event automatically at 02:18 (15 minutes after the drop, on the next sensor reading cycle) — well before the first customer complaint at 03:05. This is the "golden hour" advantage: the platform alerts you, not the customer.

**Scene 4 — "Understand customer impact — and predict what's coming"**
The operator switches to the **customer view**. Customer complaints (from Salesforce data) started coming in at ~3am — about an hour after the pressure drop. The map shows affected properties as markers. Crucially, the affected customers are all at **higher elevation** than the pump station — because lower pressure affects high-altitude customers first (pressure = total_head_pressure - customer_height).

The panel also shows a **predicted impact projection**: "If pressure remains at current level, an estimated 1,200 properties will be affected within 2 hours (including 1 hospital)." This is a simple threshold model — given current pressure at the DMA level and the elevation distribution of properties, which additional properties will lose effective supply if pressure doesn't recover? This turns the exec conversation from "how bad is it now" to "how bad will it get."

**Scene 5 — "Brief the executives — and draft the regulatory report"**
An executive calls asking about impact. The operator can instantly see:
- Affected properties by type: **domestic (423), schools (2), hospitals (1), commercial (12)**
- Duration of interruption so far: **3h 27m**
- Whether vulnerable/sensitive customers are impacted: **Yes — 1 hospital, 2 schools** (these are DWI reportable)
- A **pre-populated draft incident summary** with the mandatory fields for DWI (Drinking Water Inspectorate) and Ofwat reporting: number of properties affected, duration, sensitive customers, cause (pump trip), and timeline. The operator can review, edit, and export — what normally takes hours of manual collation is pre-filled in seconds.

**Scene 6 — "Explore further with Genie"**
For ad-hoc questions that go beyond the app's pre-built views, the operator opens the Genie Space and asks natural-language questions like "Which DMAs had the biggest pressure drop in the last 6 hours?", "How many hospitals are in DMA X?", or "Show me all incidents in the last 30 days where more than 100 properties were affected."

- The Problem: Business impact of their current state (latency, fragmented geospatial systems, 15 minutes to pull incident data manually, no anomaly detection, manual regulatory reporting).
- The Solution: How the Databricks Lakehouse with geospatial capabilities solves this — from automated anomaly detection to root cause to executive briefing and regulatory report in under 60 seconds.
- Step-by-Step Live Demo: [Follow the 6 scenes above, mapping each to the underlying Databricks features: SDP for data freshness and anomaly scoring, Delta+Lakebase for sub-second queries, the App for the interactive investigation, Metric Views for the aggregated stats, Genie for ad-hoc exploration]
- The Outcome: "Day in the life" after adoption — the platform detects the incident before the first customer calls, the operator traces the root cause in seconds, predicts escalation, briefs the exec with numbers, and has a draft regulatory report ready — all within the golden hour.

## 5. Sample Data Strategy
- Entities & Fields: [sensors, DMAs, PMAs, properties (with property_type: domestic/school/hospital/commercial and customer_height/elevation), pressure telemetry, **upstream assets** (pump stations, trunk mains — with asset_type, status, trip_timestamp, and a feed mapping to downstream DMAs), customer contacts/complaints (with complaint_timestamp and source=Salesforce), **anomaly_scores** (per sensor reading, σ deviation from 7-day time-of-day baseline), **dma_rag_history** (RAG status per DMA per 15-min interval)]
- Volume & Realism: [Recommended time range, scale, and noise level. Default: ~500K pressure readings, 500 DMAs, 10K sensors, 50K properties, 12 months of history. Override only if the use case demands different scale.]
- **Demo Scenario Contract (CRITICAL — hardcoded consistency requirements):**
  The synthetic data MUST include one pre-baked incident scenario. This is not aspirational — the data generation agent must produce **exactly** these entities with **exactly** these relationships. If any link is broken, the demo fails.

  **The incident DMA: `DEMO_DMA_01`**
  - This DMA must have RAG status = RED at the demo timestamp (pressure below red threshold)
  - It must contain at least **20 sensors**, of which **at least 8** show pressure below the low-pressure threshold after 2am
  - It must contain **at least 800 properties**, distributed as:
    - ~750 domestic
    - At least 2 schools (property_type = 'school')
    - At least 1 hospital (property_type = 'hospital')
    - At least 15 commercial (property_type = 'commercial')
  - All properties must have `dma_code = 'DEMO_DMA_01'` — this join must work
  - All properties must have valid `latitude`, `longitude`, and `geometry_wkt` **within the polygon** of DEMO_DMA_01

  **The upstream cause: pump station `DEMO_PUMP_01`**
  - This is the **root cause asset** — a pump station that feeds DEMO_DMA_01 (and partially feeds DEMO_DMA_02, DEMO_DMA_03)
  - It must exist in a `dim_assets` table with: `asset_id = 'DEMO_PUMP_01'`, `asset_type = 'pump_station'`, `status = 'tripped'`, lat/lon placing it geographically **upstream** (slightly north/west) of DEMO_DMA_01
  - It must have a `trip_timestamp = '2026-04-07 02:03:00'`
  - The demo story: a DMA going red is a *symptom*. The cause is this upstream pump trip. The app must link the DMA back to the feeding asset so the operator can trace cause, not just observe effect.
  - The pump station should be connected to the affected DMAs via a `dim_asset_dma_feed` mapping table (asset_id → dma_code, with feed_type = 'primary' or 'secondary')

  **The reporting sensor: `DEMO_SENSOR_01` in `DEMO_DMA_01`**
  - This sensor *reports* the impact of the pump trip — it doesn't fail itself
  - It must show **normal pressure (~45-55m)** from midnight to 1:59am
  - At **2:03am** (matching the pump trip time), pressure drops sharply to **~5-10m**
  - Pressure stays low through 6am (the demo moment)
  - The sensor must have a known `latitude`, `longitude` and low `elevation` (e.g., 15m)

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
  -- Expected: domestic >= 750, school >= 2, hospital >= 1, commercial >= 15

  -- 3. Pressure drop is visible
  SELECT timestamp, value FROM silver.fact_telemetry WHERE sensor_id = 'DEMO_SENSOR_01' AND timestamp BETWEEN '2026-04-07 00:00' AND '2026-04-07 06:00' ORDER BY timestamp
  -- Expected: ~45-55m until 2am, then ~5-10m after

  -- 4. Complaints lag pressure drop by ~1 hour
  SELECT MIN(complaint_timestamp), MAX(complaint_timestamp), COUNT(*) FROM silver.customer_complaints WHERE dma_code = 'DEMO_DMA_01'
  -- Expected: earliest ~3am, count >= 30

  -- 5. Elevation coherence
  SELECT AVG(p.customer_height) as avg_customer_elevation FROM silver.dim_properties p JOIN silver.customer_complaints c ON p.uprn = c.uprn WHERE c.dma_code = 'DEMO_DMA_01'
  -- Expected: avg > 35m (high elevation customers complaining)

  -- 6. Upstream cause exists and is linked
  SELECT asset_id, asset_type, status, trip_timestamp FROM silver.dim_assets WHERE asset_id = 'DEMO_PUMP_01'
  -- Expected: asset_type = 'pump_station', status = 'tripped', trip_timestamp = '2026-04-07 02:03:00'

  -- 7. Pump feeds the affected DMAs
  SELECT asset_id, dma_code, feed_type FROM silver.dim_asset_dma_feed WHERE asset_id = 'DEMO_PUMP_01'
  -- Expected: 3 rows — DEMO_DMA_01 (primary), DEMO_DMA_02 (secondary), DEMO_DMA_03 (secondary)

  -- 8. Anomaly score is high for incident, low for noise
  SELECT sensor_id, timestamp, anomaly_sigma FROM gold.anomaly_scores WHERE sensor_id = 'DEMO_SENSOR_01' AND timestamp = '2026-04-07 02:15:00'
  -- Expected: anomaly_sigma > 3.0

  -- 9. Amber DMAs are low anomaly (not real incidents)
  SELECT s.sensor_id, MAX(a.anomaly_sigma) FROM silver.dim_sensor s JOIN gold.anomaly_scores a ON s.sensor_id = a.sensor_id WHERE s.dma_code = 'DEMO_DMA_02' AND a.timestamp BETWEEN '2026-04-07 02:00' AND '2026-04-07 06:00' GROUP BY s.sensor_id
  -- Expected: all max anomaly_sigma < 2.0

  -- 10. RAG timeline history exists
  SELECT dma_code, timestamp, rag_status FROM gold.dma_rag_history WHERE dma_code = 'DEMO_DMA_01' AND timestamp BETWEEN '2026-04-07 01:00' AND '2026-04-07 04:00' ORDER BY timestamp
  -- Expected: GREEN until ~02:00, AMBER at ~02:15, RED from ~02:30 onward
  ```
- **Geospatial Realism — London area (MANDATORY):**
  - All generated coordinates must fall within the **Greater London area**. Bounding box: latitude **51.28N to 51.70N**, longitude **-0.51W to 0.33E**.
  - `DEMO_DMA_01` (the incident DMA) should be centred roughly around **central-south London** (e.g., ~51.45N, -0.05E) so it's in the middle of the map and visually prominent.
  - DMA polygons should be generated as **hexagonal or Voronoi-style tessellations** covering the Greater London area. Each polygon should be roughly 2-5 km across — large enough to be clearly visible and clickable on a map.
  - Sensor locations must be **inside** their assigned DMA polygon. Property locations must be **inside** their assigned DMA polygon. Use ST_Contains or H3-based assignment to verify.
  - Elevation data should roughly reflect London's real topography: low areas near the Thames (~5-15m), higher areas on hills like Highgate, Crystal Palace, Shooters Hill (~60-100m). `DEMO_DMA_01` should be in a hilly area so the elevation story (high-altitude customers lose water first) is geographically plausible.
  - Geometry types: DMA/PMA boundaries as WKT POLYGON, sensors and properties as WKT POINT, plus separate lat/lon DOUBLE columns.
  - H3 resolution: use resolution 7 or 8 for DMA-level spatial indexing.
- Generation Method: [Synthetic vs. pattern-based, and generation assumptions]
- Reset & Re-seed Strategy: [How the demo environment can be torn down and rebuilt from scratch. Must be scripted, not manual.]

## 6. Global Schema Contract (Strict Data Dictionary)
*Provide exact table names, column names, and data types. Downstream coding agents will rely on this as the absolute source of truth to ensure their scripts connect properly.*

**Naming conventions (mandatory):**
- Use three-part names: `<catalog>.<schema>.<table>` (default catalog: `water_digital_twin`)
- Follow medallion architecture schemas: `bronze`, `silver`, `gold`
  - `water_digital_twin.bronze.*` — raw ingested data
  - `water_digital_twin.silver.*` — cleansed, conformed tables with geospatial columns
  - `water_digital_twin.gold.*` — business-level aggregates, feature tables, and metric views (`mv_` prefix)
- Geospatial columns should use STRING type with WKT format for portability, plus DOUBLE lat/lon columns for point geometries

For each table, provide:
| Full Table Name | Column | Type | Description |
|---|---|---|---|
| `water_digital_twin.bronze.example` | col_name | STRING | ... |

**Metric Views (Gold layer):**
For each metric view, provide the full YAML-style definition:
- name, source table, joins, dimensions (with display names and formats), measures (with expressions and formats)

## 7. Multi-Agent Build Instructions & Delegation
*Divide the required demo assets into tasks for the downstream 5-agent team. Each task maps directly to a named agent in the build prompt. For each task, provide the explicit goal and acceptance criteria.*

### Diana — Platform & Admin Engineer
- **Task 1 (Preview Features & Workspace Setup):** List every preview feature the demo requires. For each, specify whether it is Private Preview (requires support ticket) or Public Preview (admin toggle). Include exact admin steps.
- **Task 2 (Orchestration):** Describe the Databricks Workflow(s) needed to run the demo end-to-end — job name, task dependencies, triggers.
- Acceptance Criteria: [e.g., All preview features documented with enablement steps; Lakebase instance provisioned; workflow runs ingestion → transformation → serving in order]

### Alice — Lead Data Architect
- **Task 3 (Repo Structure):** Propose workspace folder layout — where notebooks, SQL files, app code, and dashboards live.
- **Task 4 (Schema Definitions):** Translate the Section 6 schema contract into full Unity Catalog DDL-ready definitions (table, column, type, description). This is the single source of truth for all downstream agents. Include geospatial column definitions.
- **Task 5 (Lakebase Setup):** Create the Lakebase database and tables that mirror the Gold layer for low-latency serving. Define the sync mechanism from Delta Gold → Lakebase.
- Acceptance Criteria: [e.g., All tables use three-part names from Section 6; geospatial columns are properly typed; Lakebase tables are populated and queryable]

### Bob — Senior Data Engineer
- **Task 6 (Data Generation):** Generate synthetic sample data at the volumes specified in Section 5. Must be idempotent and re-runnable. All geospatial data must use coordinates within the **Greater London bounding box** (51.28-51.70N, -0.51-0.33E). DMA polygons should tessellate across Greater London. Elevation values should approximate London's real topography (low near the Thames, higher on hills).
- **Task 7 (SDP Pipeline):** Build the Spark Declarative Pipeline for Bronze → Silver → Gold. Include: streaming ingestion for sensor telemetry, batch processing for reference/geospatial data, geospatial transformations (ST_ functions, H3 indexing), and quality expectations.
- **Task 7b (Anomaly Scoring):** Compute anomaly scores in the Gold layer. For each sensor reading, compare against the sensor's rolling 7-day baseline **at the same time of day** (pressure has strong diurnal patterns — low morning demand, recovery mid-day, evening drop). Score = standard deviations from baseline. Store in `gold.anomaly_scores`. Also compute and store `gold.dma_rag_history` (RAG status per DMA per 15-minute interval) to enable the timeline strip in the app.
- **Task 7c (Metric Views):** Create Gold-layer metric views with `mv_` prefix. These views are consumed by Charlie for Genie Spaces and dashboards.
- Acceptance Criteria: [e.g., SDP pipeline runs end-to-end; all tables/views reference Alice's schema; geospatial transformations produce valid geometries; data generation is idempotent; DEMO_SENSOR_01 anomaly score >3.0σ after 02:03; amber DMAs have anomaly scores <2.0σ]

### Charlie — AI/BI, Genie & Apps Specialist
- **Task 8 (Databricks App):** Build and deploy the geospatial Databricks App (React + FastAPI) that supports the full incident investigation flow from Section 4. The app must have these views/interactions:
  - **Map view:** Interactive map with DMA polygons colour-coded Red/Amber/Green by current pressure status. Clicking a DMA opens a side panel. **Demo-readiness requirements for the map:**
    - The **initial viewport/zoom** must show the Greater London area and be centred so that `DEMO_DMA_01` (the red incident DMA) is clearly visible on screen when the app first loads — the presenter should not have to scroll or zoom to find it. Default zoom level should show ~20-30 DMAs so the red one stands out among green/amber neighbours.
    - `DEMO_DMA_01` must be visually prominent: it should be the only RED polygon surrounded by GREEN/AMBER neighbours. The red fill must have enough opacity and contrast to immediately draw the eye.
    - DMA polygons must be large enough to be clickable (no tiny slivers). The click target must be the entire polygon area, not just a label or centroid.
    - On hover, show a tooltip with DMA name, current RAG status, and **anomaly confidence** (e.g., "High — 3.2σ") so the presenter can narrate while moving the mouse. This helps distinguish real incidents from routine amber noise.
    - Consider placing `DEMO_DMA_01` near the **centre** of the generated DMA grid so it's naturally in the middle of the map, not at an edge.
  - **DMA detail panel:** Shows DMA summary (avg pressure, sensor count, property count), lists low-pressure assets/sensors sorted by severity, shows recent customer complaints, AND critically: **identifies the upstream cause** — the feeding asset (pump station `DEMO_PUMP_01`) that tripped, with a link to its detail view. The panel should show "Cause: Pump station DEMO_PUMP_01 tripped at 02:03. Downstream impact: 3 DMAs." A DMA going red is a symptom; the operator needs to see the cause.
  - **Asset detail panel:** Clicking an asset (sensor or pump station) shows:
    - A **pressure time-series chart** (past 24h with 15-min granularity). Must clearly show the 2am drop.
    - A **RAG status timeline strip** below the chart — a horizontal sequence of colour blocks (green-green-green-amber-red-red) showing when the DMA transitioned through each state. Operators call this "trend of the trend" — is the situation deteriorating or stabilising?
    - An **anomaly detection badge** showing when the system first flagged this event (e.g., "Anomaly detected at 02:18 — 47 minutes before first customer complaint"). This proves the platform catches incidents before customers report them.
  - **Customer impact view:** Shows affected properties as map markers, with colour/icon distinguishing complaint type. Shows that high-elevation customers (where pressure = total_head_pressure - customer_height falls below threshold) are impacted first. Integrates Salesforce-sourced complaint data with timestamps. Also shows a **predicted impact projection**: "If pressure remains at current level, an estimated X additional properties will lose supply within 2 hours" — a simple threshold model based on current pressure vs. property elevation distribution.
  - **Executive summary & regulatory report:** For any selected DMA/area, instantly shows:
    - Affected properties by type: domestic count, schools, hospitals, commercial
    - Duration of interruption so far
    - Whether sensitive/vulnerable customers are impacted (DWI reportable)
    - A **pre-populated draft incident summary** with mandatory fields for DWI and Ofwat reporting (properties affected, duration, sensitive customers, cause, timeline). The operator can review, edit, and export. What normally takes hours of manual collation is pre-filled in seconds.
  - **Write-back:** Operator can take ownership of an alert and add comments.
  - All reads from Lakebase for sub-second response (<3s map load, <1s panel updates).
- **Task 9 (Genie Space):** Configure an AI/BI Genie Space for natural-language exploration of water network geospatial data. Specify: trusted assets (Bob's `mv_*` views + dimension tables), system prompt/instructions tailored to water network operations, and 5-7 pre-populated sample questions that map to the incident investigation story — e.g., "Which DMAs have average pressure below threshold?", "How many customers are affected in DMA X?", "Show pressure trend for sensor Y over the last 24 hours", "How many schools and hospitals are in the affected area?", "When did customer complaints start in DMA X?". Include verified queries for the most common questions.
- **Task 10 (Dashboard):** Design an AI/BI Lakeview dashboard with tiles showing: network-wide pressure overview, DMA health summary, sensor coverage map, anomaly trends, and customer impact metrics. All tiles reference Bob's metric views.
- Acceptance Criteria: [e.g., App loads map in <3 seconds from Lakebase; Genie Space answers geospatial questions accurately; Dashboard tiles all reference mv_ views; sample questions return correct results]

### Eve — Demo Delivery Lead
- **Task 11 (README & Reset):** Write a runbook covering prerequisites, setup steps (referencing Diana's enablement), and a scripted reset procedure to tear down and re-seed the demo.
- **Task 12 (Live Demo Script):** Create a talk-track outline mapping each demo step to the storyline from Section 4.
- Acceptance Criteria: [e.g., Reset script drops and recreates all schemas; README references exact notebook paths from Alice's Task 3; talk track covers all 5 pillars]
</demo_plan>
</output_format>

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
- **Data platform:** Databricks workspace with EDP 2.0 (Enterprise Data Platform) for ingestion. Medallion architecture: Bronze → Silver → Gold in Delta tables. SQL Warehouse for query serving.
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

### Aspirations (POC Goals)
The team wants to prove:
1. **Lakebase replaces SQL MI** for the geospatial serving layer — sub-second latency, no contention, geospatial data stays in Databricks
2. **Databricks Apps replaces ArcGIS WebExperience + API Service** — simplify architecture (remove 2 middleware layers), unified authentication, no licence cap, and enable write-back
3. **Geospatial transformations natively in Databricks** — use Spatial SQL (ST_ functions) to do what's currently done in SQL MI or ESRI, keeping all processing in the lakehouse
4. **Self-service geospatial analytics** — Genie Space where operators can ask natural-language questions about pressure, DMAs, customers, sensors without writing SQL

### Data Model (from existing Genie Space)
The team already has a working Genie Space ("DMA Pressure Metric View") with these entities:
- **dim_sensor** — sensor metadata (sensor_id, name, type, location, DMA assignment)
- **dim_properties** — property metadata (uprn, customer_height, dma_code, pma_code). **NOTE: The existing schema is missing key fields needed for the demo.** The demo must enrich this table with: `property_type` (enum: domestic, school, hospital, commercial, nursery, key_account), `address` (street address string), `latitude` (DOUBLE), `longitude` (DOUBLE), and `geometry_wkt` (STRING, WKT point). These fields are available in the real environment via the CustomerContact table (SAP/Salesforce) but weren't exposed in the original Genie space. The property type and location data are essential for the executive briefing scenario (counting schools, hospitals, etc. in an affected area) and for rendering properties on the map.
- **dim_pma** — Pressure Management Areas (pma_code, pma_name, geometry)
- **dim_dma** — District Metered Areas (dma_code, dma_name, dma_area_code, geometry)
- **fact_telemetry** — sensor pressure readings (sensor_id, timestamp, value, total_head_pressure)
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
</use_case_brief>
