# Water Digital Twin -- Live Demo Talk Track

> **Company:** Water Utilities
> **Duration:** 45-60 minutes (Scenes 1-7b core; Scene 8 optional ML extension)
> **Demo timestamp:** 2026-04-07 05:30:00 (day shift handover)
> **Active incident:** INC-2026-0407-001 -- pump trip at DEMO_PUMP_01, started 02:03

---

## Audience Hooks (use throughout)

Weave these hooks into your narration depending on who is in the room:

| Icon | Audience | Hook Examples |
|---|---|---|
| Operator | Operator / Control Room | Handover narrative, playbook, flow/pressure data, alarm log |
| Exec | Executive / Board | Penalty exposure, audit trail, AMP8 investment trends |
| Regulation | Regulatory / Compliance | DWI deadlines, Ofwat thresholds, PDF export, timestamp governance |
| Technical | Technical / IT | Architecture diagram, data quality, Spatial SQL, Unity Catalog |
| Data Science | Data Science / Analytics | ML notebook, anomaly score flow, AutoML, foundation models |

---

## Scene 1 -- Shift Handover (5 min)

### Click

1. Open the app landing page
2. Navigate to the **Handover View**

### Say

> "It's 5:30 in the morning. The day shift is just starting at Water Utilities. Before anyone picks up a radio or opens a spreadsheet, the platform has automatically generated a shift handover summary."
>
> "This narrative was produced by Mosaic AI -- it reads across every alarm, telemetry anomaly, incident update, and customer complaint from the past 12 hours, and distils it into a human-readable briefing."
>
> "Notice the governance timestamps at the bottom -- every data source used in this narrative is traced back to its Unity Catalog lineage. The operator knows exactly what data fed this summary and when it was last refreshed."

### Highlight

- **Auto-generated narrative:** No manual report writing; AI summarises the night shift
- **Governance timestamps:** Full auditability -- every table, every refresh time
- **Mosaic AI:** Foundation model serving via Databricks, no external API keys required

### Audience hooks

- **Operator:** "Your team no longer spends the first 30 minutes of a shift reading through logs."
- **Exec:** "This is a compliance artifact -- auditors can trace every statement back to source data."
- **Technical:** "The narrative is generated via a Mosaic AI model endpoint, with prompt engineering stored in Unity Catalog as a registered model."

---

## Scene 2 -- Alarm Log & Map (5 min)

### Click

1. Switch to the **Alarm Log** tab
2. Then switch to the **Map View**

### Say

> "Let's see what happened overnight. The alarm log shows a chronological timeline of every event -- sensor alerts, customer complaints, operational actions -- all correlated to the same incident."
>
> "Now let's look at the map. Every DMA in the network is coloured by its RAG status -- green means normal, amber means elevated risk, red means active incident."
>
> "One red DMA stands out immediately. That's DEMO_DMA_01. Two amber DMAs sit on either side -- DEMO_DMA_02 and DEMO_DMA_03. The rest of the network is green."

### Highlight

- **Chronological event log:** Every event timestamped and linked to an incident ID
- **RAG-coloured map:** 500 DMAs, instant visual triage (1 RED, 2 AMBER, 497 GREEN)
- **Quick filters:** Filter by severity, time range, asset type

### Audience hooks

- **Operator:** "No more flipping between SCADA, GIS, and the CRM -- everything is unified."
- **Technical:** "The map renders DMA polygons stored as WKT in Unity Catalog. Spatial joins power the colouring."

---

## Scene 3 -- DMA Investigation (5 min)

### Click

1. Click on the **DEMO_DMA_01** polygon on the map
2. The **DMA Detail Panel** opens

### Say

> "Let's drill into the red zone. DEMO_DMA_01 has 441 properties affected, including 1 hospital, 2 schools, and 3 dialysis patients. Those sensitive premises are flagged immediately -- this is critical for regulatory reporting."
>
> "The panel shows the upstream root cause chain: a pump trip at DEMO_PUMP_01 at 02:03 caused pressure to drop across three DMAs fed by that pump. The trunk main DEMO_TM_001 connects them."
>
> "Notice the reservoir countdown -- DEMO_SR_01 is at 43% of its 5 megalitres capacity. At current draw-down rate, the operator can see exactly how much time they have before the situation escalates."

### Highlight

- **Upstream root cause chain:** Pump -> trunk main -> DMAs (automated causal linking)
- **Sensitive premises:** Hospital, schools, dialysis patients flagged immediately
- **Reservoir countdown:** Real-time level with capacity context

### Audience hooks

- **Regulation:** "DWI requires you to report sensitive premises within specific timeframes. This panel gives you the numbers instantly."
- **Exec:** "441 properties, 1 hospital, 3 dialysis patients -- that's the penalty exposure in one glance."

---

## Scene 4 -- Asset Deep Dive (5 min)

### Click

1. Click on the **Sensor Chart** for DEMO_SENSOR_01
2. Open the **Playbook Panel**

### Say

> "Here's the pressure trace for DEMO_SENSOR_01. You can see the normal baseline, then the cliff-edge drop at 02:03 when the pump tripped."
>
> "The anomaly was detected at 02:18 -- that's our AI scoring engine flagging the deviation at over 3 sigma. The first customer complaint didn't arrive until after 03:00. That's **47 minutes of early warning** before any customer picked up the phone."
>
> "On the right, the playbook panel has automatically loaded SOP-WN-042 -- the standard operating procedure for a water network loss of supply event. The operator doesn't need to search for the procedure; it's surfaced automatically based on the incident type."

### Highlight

- **Dual-axis chart:** Pressure and flow overlaid, with anomaly threshold line
- **Anomaly badge:** 3+ sigma flagged visually; timestamp of first detection
- **SOP playbook:** Auto-surfaced based on incident classification

### Audience hooks

- **Operator:** "47 minutes before the first call. That's the difference between proactive and reactive."
- **Data Science:** "The anomaly scoring runs as a batch job in notebooks/07_anomaly_scoring.py. The sigma threshold is configurable."
- **Technical:** "The playbook is stored in gold.dim_playbook -- it's just another governed table in Unity Catalog."

---

## Scene 5 -- Customer Impact (5 min)

### Click

1. Navigate to the **Customer Impact View**
2. Interact with the **What-If Slider**

### Say

> "Now let's look at who is actually affected. The platform models customer impact using property elevation data. High-elevation customers lost supply first -- they're at the top of the pressure gradient."
>
> "The average elevation for complainants in DEMO_DMA_01 is above 35 metres. That's not a coincidence -- it's physics. The digital twin understands the hydraulic model."
>
> "Use the what-if slider to simulate scenarios: what if we open isolation valve DEMO_VALVE_01? What if we increase pump output from the backup? The model recalculates the impact zone in real time."

### Highlight

- **Elevation model:** Customers mapped by height above sea level; hydraulic physics built in
- **What-if simulation:** Interactive scenario planning for operators and managers
- **Complaint correlation:** Complaints overlaid on the pressure timeline to show the lag

### Audience hooks

- **Operator:** "You can test a valve operation before you send a crew."
- **Exec:** "This is the TCO story: 50 to 110 hours per year saved on manual impact assessments."
- **Technical:** "The elevation data comes from gold.dim_property. The what-if model is a parameterised Spark query."

---

## Scene 6 -- Regulatory & Executive Summary (5 min)

> **This is the climax of the demo. Rehearse this line.**

### Say

> "Let me pull up the executive summary. This incident has been active for **3 hours and 27 minutes**. **441 properties** are affected, including **1 hospital**, **2 schools**, and **3 dialysis patients**. Based on current Ofwat penalty thresholds, the estimated financial exposure is **GBP 180,000**."
>
> "The DWI reporting clock started at 02:03. The platform tracks every deadline automatically -- initial notification, 24-hour update, final report. If a deadline is approaching, the system escalates."
>
> "All of this is exportable. One click generates a PDF with the full incident timeline, actions taken, and regulatory data -- ready to send to the regulator. Every action, every timestamp, every data point is part of a complete audit trail."

### Highlight

- **DWI deadlines:** Automated tracking of regulatory notification windows
- **Ofwat penalty thresholds:** Financial exposure calculated in real time
- **PDF export:** One-click regulatory report generation
- **Audit trail:** Every user action, system event, and data change is logged

### Audience hooks

- **Regulation:** "This is your DWI submission, auto-generated. No more scrambling for data at 3am."
- **Exec:** "GBP 180K exposure, and the clock is ticking. This is why early detection matters."
- **Technical:** "The audit trail is append-only in Delta Lake. Immutable, time-travel enabled."

---

## Scene 7a -- Genie Operator (5 min)

### Click

1. Open the **Network Operations** Genie Space
2. Type natural language queries

### Sample Queries

> "Which DMAs are currently in RED status?"

> "Show me all sensors in DEMO_DMA_01 with anomaly sigma above 2.0"

> "What is the trunk main geometry for DEMO_TM_001?"

> "List all complaints in the last 3 hours for DEMO_DMA_01, ordered by time"

### Say

> "Genie is Databricks' natural language SQL interface. The operator doesn't need to know SQL -- they ask questions in plain English and get verified, governed answers."
>
> "Behind the scenes, Genie translates the question into Spatial SQL against the metric views we created. Every query is audited and the results respect Unity Catalog permissions."

### Highlight

- **Spatial SQL:** Genie can write ST_Contains, ST_Distance queries from natural language
- **Metric views:** Purpose-built views in gold schema optimise Genie's query generation
- **Verified queries:** Curated Q&A pairs ensure consistent answers for common questions

### Audience hooks

- **Operator:** "Ask your question. No SQL, no training, no ticket to the data team."
- **Technical:** "The metric views are defined in notebooks/08_metric_views.sql. You control exactly what Genie can see."

---

## Scene 7b -- Genie Executive (5 min)

### Click

1. Open the **Executive** Genie Space
2. Ask executive-level questions

### Sample Queries

> "What is our total penalty exposure across all active incidents?"

> "Show me the AMP8 investment trend for network resilience over the past 3 years"

> "How many sensitive premises are in DMAs with active incidents?"

> "Compare our supply interruption KPIs to Ofwat targets"

### Say

> "This is a separate Genie Space configured for executive users. The system prompt includes business context -- AMP8 investment cycles, Ofwat performance commitments, penalty frameworks."
>
> "The executive doesn't see raw sensor data. They see KPIs, trends, and financial exposure. Same underlying data, different lens -- all governed by Unity Catalog."

### Highlight

- **Business context in system prompt:** Genie understands regulatory frameworks without per-query prompting
- **Role-based access:** Executives see aggregated KPIs; operators see granular sensor data
- **Same platform:** One Unity Catalog, multiple audiences

### Audience hooks

- **Exec:** "You can ask your own questions of the data. No analyst in the loop."
- **Regulation:** "The audit trail shows who asked what and when. Full transparency."

---

## Scene 8 -- ML Extension (Optional, 3-15 min)

Adjust depth based on audience interest and remaining time.

### Level 1: ai_forecast (3 min)

> "Databricks has a built-in SQL function called `ai_forecast`. In one line of SQL, we can project the reservoir level forward 24 hours. No ML expertise required."

```sql
SELECT * FROM ai_forecast(
  TABLE gold.fact_telemetry,
  horizon => '24 hours',
  time_col => 'event_timestamp',
  value_col => 'value',
  group_col => 'sensor_id'
)
WHERE sensor_id = 'DEMO_SR_01'
```

### Level 2: AutoML (7 min)

> "For a more rigorous model, we use Databricks AutoML. Point it at historical telemetry, and it trains, evaluates, and registers the best model -- all tracked in MLflow."
>
> Show the AutoML experiment UI, the model comparison chart, and the registered model in Unity Catalog.

### Level 3: Foundation Model Fine-Tuning (5 min)

> "For the most advanced use case, we fine-tune a foundation model on historical incident narratives. The model learns to generate shift handovers, root cause analyses, and regulatory summaries in the voice of your operations team."
>
> Show the fine-tuning job, training metrics, and a comparison of base vs fine-tuned output.

### Audience hooks

- **Data Science:** "MLflow tracks every experiment. Unity Catalog governs the model. One platform."
- **Exec:** "We go from a SQL function to a production model without leaving Databricks."
- **Technical:** "The model serving endpoint auto-scales. No infrastructure to manage."

---

## Key Moments to Rehearse

These are the lines and transitions that make or break the demo. Practice them out loud.

### 1. The Executive Summary Line (Scene 6)

> "This incident has been active for 3 hours and 27 minutes. 441 properties, 1 hospital, 2 schools, 3 dialysis patients. Estimated penalty: GBP 180,000."

Deliver this with gravitas. Pause after "dialysis patients" before the penalty number.

### 2. The 47-Minute Early Warning (Scene 4)

> "The anomaly was detected at 02:18. The first customer complaint arrived after 03:00. That's 47 minutes of early warning."

Let the number land. This is the ROI moment.

### 3. The Architecture Pivot (any scene)

When a technical audience member asks "how does this work?":

> "Everything you see runs on one platform. The data flows from OSI PI through Bronze, Silver, Gold in Unity Catalog. The app reads from Lakebase -- that's Databricks' built-in Postgres -- for sub-millisecond response. The AI features use Mosaic AI model endpoints. No external services, no data leaving the platform."

### 4. The TCO Positioning

> "Our customers in the water sector tell us this kind of platform saves 50 to 110 hours per year on manual reporting and incident analysis alone. And that's before you factor in the early warning value."

### 5. The Scalability Story

> "Right now you're looking at a demo with 500 DMAs. But this architecture scales linearly. We have customers running this pattern across 2,000 DMAs with thousands of concurrent users. The platform handles it."

---

## Closing & Next Steps

> "What you've seen today is a digital twin built entirely on the Databricks platform. Real-time telemetry, anomaly detection, incident management, regulatory reporting, natural language queries -- all governed by Unity Catalog, all on one platform."
>
> "The next step is a technical deep-dive with your team to map this architecture to your specific data sources and regulatory requirements. Shall we schedule that?"

---

## Pre-Demo Checklist

- [ ] Health check passed (22/22) -- run `scripts/demo_health_check.py`
- [ ] App loads in browser
- [ ] Both Genie spaces respond to test queries
- [ ] Browser tabs pre-loaded: App, Genie (Operator), Genie (Executive), Dashboard
- [ ] Demo timestamp set: 2026-04-07 05:30:00
- [ ] Know your audience composition (adjust hooks accordingly)
- [ ] Rehearsed Scene 6 executive summary line
- [ ] Backup plan: if app is down, demo via Genie + Dashboard only
