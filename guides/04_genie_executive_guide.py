# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Executive Space Guide
# MAGIC
# MAGIC **Water Utilities -- Digital Twin Demo**
# MAGIC
# MAGIC Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
# MAGIC CLI profile: `adb-98`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC This guide creates the **"Water Operations Intelligence"** Genie Space for executive stakeholders. It surfaces regulatory exposure, penalty estimates, incident trends, and customer experience metrics using natural language queries.
# MAGIC
# MAGIC > **Best practice:** Test all configuration changes in a **cloned Space** before applying them to the production Space. In the Genie sidebar, select your Space → **Clone** to create a safe copy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Create the Genie Space
# MAGIC
# MAGIC 1. In the workspace sidebar, navigate to **Genie**.
# MAGIC 2. Click **New** (or **Create Genie Space**).
# MAGIC 3. Set the name: **Water Operations Intelligence**.
# MAGIC 4. Set the description:
# MAGIC    > Executive intelligence for Water Utilities leadership. Query Ofwat penalty exposure, regulatory compliance, incident trends, C-MeX performance, and AMP8 investment metrics.

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
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT pk_dim_incidents PRIMARY KEY (incident_id);
# MAGIC ALTER TABLE water_digital_twin.gold.incident_notifications ADD CONSTRAINT pk_incident_notifications PRIMARY KEY (incident_id);
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ADD CONSTRAINT pk_regulatory_notifications PRIMARY KEY (notification_id);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ADD CONSTRAINT pk_dim_properties PRIMARY KEY (property_id);
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ADD CONSTRAINT pk_dim_dma PRIMARY KEY (dma_code);
# MAGIC
# MAGIC -- Foreign keys (Genie uses these to plan joins)
# MAGIC ALTER TABLE water_digital_twin.gold.incident_notifications ADD CONSTRAINT fk_notif_incident FOREIGN KEY (incident_id) REFERENCES water_digital_twin.gold.dim_incidents(incident_id);
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ADD CONSTRAINT fk_reg_incident FOREIGN KEY (incident_id) REFERENCES water_digital_twin.gold.dim_incidents(incident_id);
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT fk_incident_dma FOREIGN KEY (dma_code) REFERENCES water_digital_twin.silver.dim_dma(dma_code);
# MAGIC ```
# MAGIC
# MAGIC ### Column Descriptions
# MAGIC
# MAGIC Review AI-generated column descriptions in the Genie Space UI. Override any that are vague or incorrect. Pay special attention to:
# MAGIC
# MAGIC - `total_properties_affected` in `dim_incidents` — clarify this is the count of properties impacted, not a percentage
# MAGIC - `proactive_notifications` vs `reactive_complaints` in `incident_notifications` — Genie must understand proactive = sent before customer contact, reactive = complaint-driven
# MAGIC - `dwi_notified_ts` in `regulatory_notifications` — clarify this is the timestamp DWI was formally notified

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Add Trusted Assets
# MAGIC
# MAGIC Add the following 8 tables and metric views as trusted assets.
# MAGIC
# MAGIC ### Metric Views (gold schema)
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 1 | `water_digital_twin.gold.mv_incident_summary` | Incident duration, threshold breaches, and properties affected |
# MAGIC | 2 | `water_digital_twin.gold.mv_penalty_exposure` | Ofwat OPA penalty exposure (£580/property/hour beyond 3h) |
# MAGIC | 3 | `water_digital_twin.gold.mv_regulatory_compliance` | RAG status distribution, property counts, sensitive premises |
# MAGIC
# MAGIC ### Gold Operational Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 4 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records with properties affected |
# MAGIC | 5 | `water_digital_twin.gold.incident_notifications` | Proactive notifications and reactive complaint counts per incident |
# MAGIC | 6 | `water_digital_twin.gold.regulatory_notifications` | DWI and Ofwat notification timestamps and compliance tracking |
# MAGIC
# MAGIC ### Silver Reference Tables
# MAGIC
# MAGIC | # | Asset | Description |
# MAGIC |---|---|---|
# MAGIC | 7 | `water_digital_twin.silver.dim_properties` | Property register with types (residential, school, hospital, etc.) |
# MAGIC | 8 | `water_digital_twin.silver.dim_dma` | DMA reference data for geographic context |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Configure Knowledge Store (SQL Expressions)
# MAGIC
# MAGIC SQL expressions have the **highest priority** in Genie's instruction hierarchy. Navigate to **Configure → Context → SQL Expressions** and add the following.
# MAGIC
# MAGIC > **Critical:** The penalty formula and regulatory thresholds must be defined as SQL expressions — Genie-generated SQL may not apply the correct formula from text instructions alone.
# MAGIC
# MAGIC ### Measures
# MAGIC
# MAGIC Each measure has four fields in the UI: **Name**, **Code** (SQL expression), **Synonyms** (comma-separated), and **Instructions**.
# MAGIC
# MAGIC > **Tip:** If using the pre-joined `vw_incident_executive` view, you can reference its columns directly (e.g., `vw_incident_executive.total_properties_affected`). The expressions below use the individual source tables.
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `penalty_gbp` | `SUM(dim_incidents.total_properties_affected * GREATEST(TIMESTAMPDIFF(HOUR, dim_incidents.start_timestamp, COALESCE(dim_incidents.end_timestamp, CURRENT_TIMESTAMP())) - 3, 0) * 580)` | fine, penalty, Ofwat penalty, OPA penalty, financial exposure | Ofwat OPA penalty: properties × hours beyond 3h threshold × £580/property/hour. Always express in GBP (£). Show formula breakdown when explaining. |
# MAGIC | `proactive_pct` | `ROUND(SUM(incident_notifications.proactive_notifications) * 100.0 / NULLIF(SUM(incident_notifications.proactive_notifications + incident_notifications.reactive_complaints), 0), 1)` | C-MeX rate, proactive rate, notification rate, customer comms rate | Percentage of affected customers who received proactive notification before complaining. Higher is better for C-MeX. Returns 0-100%. |
# MAGIC | `avg_duration_hours` | `AVG(TIMESTAMPDIFF(HOUR, dim_incidents.start_timestamp, COALESCE(dim_incidents.end_timestamp, CURRENT_TIMESTAMP())))` | average incident length, mean duration, typical incident time | Average incident duration. Active incidents use CURRENT_TIMESTAMP as end. Returns hours. |
# MAGIC | `total_properties_affected` | `SUM(dim_incidents.total_properties_affected)` | homes affected, customers impacted, properties impacted | Total count of distinct properties affected by incidents. Not a percentage. |
# MAGIC | `incident_count` | `COUNT(DISTINCT dim_incidents.incident_id)` | number of incidents, incident total, how many incidents | Count of distinct incidents. Use with time filters (this_quarter, amp8_period) for period comparisons. |
# MAGIC | `detection_to_dwi_hours` | `ROUND(AVG(TIMESTAMPDIFF(MINUTE, dim_incidents.start_timestamp, regulatory_notifications.dwi_notified_ts)) / 60.0, 1)` | DWI response time, notification lag, regulator notification time | Average hours from incident detection to DWI formal notification. DWI verbal deadline is 1 hour. Lower is better. Requires join between dim_incidents and regulatory_notifications. |
# MAGIC
# MAGIC ### Filters
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `active_incidents` | `dim_incidents.status = 'active'` | ongoing incidents, current incidents, open incidents | Incidents that are currently active (not yet resolved). Default filter when user asks about "current" exposure. |
# MAGIC | `over_3h_threshold` | `TIMESTAMPDIFF(HOUR, dim_incidents.start_timestamp, COALESCE(dim_incidents.end_timestamp, CURRENT_TIMESTAMP())) > 3` | Ofwat threshold, 3 hour breach, penalty threshold | Incidents exceeding the 3-hour Ofwat supply interruption threshold. Penalty accrual begins after this point. |
# MAGIC | `over_12h_threshold` | `TIMESTAMPDIFF(HOUR, dim_incidents.start_timestamp, COALESCE(dim_incidents.end_timestamp, CURRENT_TIMESTAMP())) > 12` | Category 1, escalation threshold, 12 hour breach | Incidents exceeding 12-hour Category 1 escalation. Flag prominently — requires board-level notification. |
# MAGIC | `sensitive_premises` | `dim_properties.property_type IN ('school', 'hospital', 'care_home', 'dialysis_home')` | vulnerable properties, priority premises, sensitive properties | Properties requiring priority response. Tracked separately for Ofwat regulatory reporting. |
# MAGIC | `this_quarter` | `dim_incidents.start_timestamp >= DATE_TRUNC('quarter', CURRENT_DATE())` | quarterly, this quarter, Q1/Q2/Q3/Q4 | Incidents starting in the current calendar quarter. |
# MAGIC | `this_year` | `dim_incidents.start_timestamp >= DATE_TRUNC('year', CURRENT_DATE())` | this year, annual, year to date, YTD | Incidents starting in the current calendar year. |
# MAGIC | `amp8_period` | `dim_incidents.start_timestamp >= '2025-04-01'` | AMP8, current AMP, current regulatory period | AMP8 regulatory period (April 2025 – March 2030). Default when user says "this period" without specifying. |
# MAGIC | `amp7_period` | `dim_incidents.start_timestamp BETWEEN '2020-04-01' AND '2025-03-31'` | AMP7, previous AMP, last regulatory period | AMP7 regulatory period (April 2020 – March 2025). Use for historical comparison against AMP8. |
# MAGIC
# MAGIC ### Dimensions
# MAGIC
# MAGIC | Name | Code | Synonyms | Instructions |
# MAGIC |---|---|---|---|
# MAGIC | `dma_code` | `dim_dma.dma_code` | zone, district, area, DMA, supply zone | District Metered Area identifier (e.g., DEMO_DMA_01). Primary geographic grouping. |
# MAGIC | `incident_severity` | `dim_incidents.severity` | priority, criticality, severity level | Values: low, medium, high, critical. Critical = Category 1 escalation (>12h). |
# MAGIC | `incident_status` | `dim_incidents.status` | incident state, active/resolved | Values: active (ongoing, penalty accruing) or resolved (closed, penalty frozen). |
# MAGIC | `property_type` | `dim_properties.property_type` | building type, premises type, property category | Values: domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Set the System Prompt
# MAGIC
# MAGIC Paste the following into the **General Instructions** section. Keep text instructions minimal — SQL expressions (Step 4) handle metric definitions and the penalty formula.
# MAGIC
# MAGIC ```
# MAGIC You are a water industry executive intelligence assistant for Water Utilities. You help senior leaders, regulatory affairs teams, and board members understand operational risk, regulatory compliance, and customer experience metrics.
# MAGIC
# MAGIC Key regulatory and industry context:
# MAGIC
# MAGIC - Ofwat: The economic regulator of the water sector in England and Wales. Sets performance commitments and financial penalties.
# MAGIC - OPA (Outcome Performance Assessment): Ofwat's framework for measuring water company performance against commitments. Penalties are calculated per property, per hour of interruption beyond thresholds.
# MAGIC - Penalty calculation: Defined in SQL expressions — use the penalty_gbp measure rather than computing manually.
# MAGIC - DWI (Drinking Water Inspectorate): The regulator responsible for drinking water quality. Companies must notify DWI promptly when incidents affect water quality or supply.
# MAGIC - C-MeX (Customer Measure of Experience): Ofwat's customer satisfaction metric. Proactive notification before customers complain positively impacts C-MeX scores.
# MAGIC - AMP8 (Asset Management Period 8): The current 5-year regulatory period (2025-2030). AMP7 covered 2020-2025.
# MAGIC - ESG (Environmental, Social, and Governance): Water companies report ESG metrics including supply resilience, leakage reduction, and environmental compliance.
# MAGIC - Sensitive properties: Schools, hospitals, and care homes are tracked separately for regulatory reporting and prioritised during incident response.
# MAGIC
# MAGIC Data model notes:
# MAGIC - Incidents use 'start_timestamp' / 'end_timestamp' (not detected_ts / resolved_ts) and 'total_properties_affected' (not properties_affected).
# MAGIC - Notification data separates proactive_notifications (sent before customer contact) from reactive_complaints (customer-initiated) in incident_notifications table.
# MAGIC - Regulatory notifications track dwi_notified_ts and ofwat_notified_ts in regulatory_notifications table.
# MAGIC - The demo snapshot time is 2026-04-07 05:30:00 UTC.
# MAGIC
# MAGIC Clarification rules:
# MAGIC - When users ask about penalty exposure without specifying active or all incidents, assume active incidents only but confirm: "Showing penalty exposure for active incidents — include resolved incidents too?"
# MAGIC - When users ask about "this period" without specifying AMP7 or AMP8, assume AMP8 (current period).
# MAGIC - When users reference "compliance" without context, clarify: "Are you asking about Ofwat penalty compliance, DWI notification compliance, or C-MeX customer satisfaction?"
# MAGIC
# MAGIC When answering:
# MAGIC - Always express monetary values in GBP (£) with appropriate formatting.
# MAGIC - When discussing penalties, show the formula and intermediate values.
# MAGIC - Express time thresholds clearly (e.g., "3-hour Ofwat threshold", "12-hour escalation").
# MAGIC - Compare current AMP8 performance against AMP7 baselines when relevant.
# MAGIC - Flag any properties exceeding the 12-hour supply interruption threshold (Category 1 escalation).
# MAGIC - For C-MeX metrics, express as percentages with proactive vs reactive breakdown.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- Configure Data Settings
# MAGIC
# MAGIC Navigate to **Configure → Data** to fine-tune how Genie interprets your tables.
# MAGIC
# MAGIC ### 6a — Enable Entity Matching
# MAGIC
# MAGIC Enable entity matching on these columns so Genie recognises categorical values in natural language:
# MAGIC
# MAGIC | Column | Table(s) | Values |
# MAGIC |---|---|---|
# MAGIC | `rag_status` | `mv_regulatory_compliance` | GREEN, AMBER, RED |
# MAGIC | `status` | `dim_incidents` (or `vw_incident_executive`) | active, resolved |
# MAGIC | `severity` | `dim_incidents` (or `vw_incident_executive`) | low, medium, high, critical |
# MAGIC | `property_type` | `dim_properties` | domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account |
# MAGIC | `has_active_incident` | `mv_regulatory_compliance` | true, false |
# MAGIC
# MAGIC > **Limits:** max 120 columns per space, max 1,024 distinct values per column.
# MAGIC
# MAGIC ### 6b — Hide Irrelevant Columns
# MAGIC
# MAGIC In **Configure → Data**, uncheck these columns to reduce token consumption:
# MAGIC
# MAGIC - `geometry_wkt` (`dim_dma`) — WKT text, not useful for NL queries
# MAGIC - `h3_index` (`dim_dma`) — internal spatial index
# MAGIC - `centroid_latitude`, `centroid_longitude` (`dim_dma`) — redundant with centroid geometry
# MAGIC - `sop_reference` (`dim_incidents`) — internal reference, rarely queried by executives
# MAGIC - `created_at`, `updated_at` (`dim_incidents`) — audit columns, rarely relevant
# MAGIC
# MAGIC ### 6c — Add Join Definitions
# MAGIC
# MAGIC In **Configure → Data → Join Definitions**, add:
# MAGIC
# MAGIC | Left Table | Right Table | Left Column | Right Column | Relationship Type | Instructions |
# MAGIC |---|---|---|---|---|---|
# MAGIC | `dim_incidents` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join incidents to DMA reference data for names and geographic context. |
# MAGIC | `dim_incidents` | `incident_notifications` | `incident_id` | `incident_id` | One to One | Join incidents to proactive/reactive notification counts for C-MeX analysis. |
# MAGIC | `dim_incidents` | `regulatory_notifications` | `incident_id` | `incident_id` | One to Many | Join incidents to DWI/Ofwat notification timestamps. One incident may have multiple regulatory notifications. |
# MAGIC | `dim_properties` | `dim_dma` | `dma_code` | `dma_code` | Many to One | Join properties to DMA for area-level aggregation and sensitive premises counts. |
# MAGIC
# MAGIC > **Alternative:** If using the pre-joined `vw_incident_executive` view (recommended), fewer join definitions are needed since the view already pre-joins incidents with DMA names, notification metrics, and regulatory timestamps.
# MAGIC
# MAGIC ### 6d — Add Column Synonyms
# MAGIC
# MAGIC | Table | Column | Synonyms |
# MAGIC |---|---|---|
# MAGIC | `dim_dma` | `dma_code` | zone, district, area, DMA, supply zone |
# MAGIC | `dim_incidents` | `total_properties_affected` | homes affected, properties impacted, customers affected |
# MAGIC | `incident_notifications` | `proactive_notifications` | proactive comms, advance warnings, pre-notifications |
# MAGIC | `incident_notifications` | `reactive_complaints` | customer complaints, inbound complaints |
# MAGIC | `dim_incidents` | `severity` | priority, criticality, severity level |
# MAGIC | `dim_incidents` | `start_timestamp` | incident start, detection time |
# MAGIC | `regulatory_notifications` | `dwi_notified_ts` | DWI notification time, regulator notification |
# MAGIC
# MAGIC ### 6e — Consider the Pre-Joined Executive View
# MAGIC
# MAGIC Notebook `10_uc_metadata.py` creates `gold.vw_incident_executive` — a pre-joined view combining incidents + DMA names + notification metrics + regulatory timestamps. Consider replacing the 3 separate tables (`dim_incidents`, `incident_notifications`, `regulatory_notifications`) with this single view to eliminate join inference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 -- Add Agent Questions
# MAGIC
# MAGIC Agent questions require Genie to run multiple queries and synthesize analytical answers. These are the most impressive demo questions.
# MAGIC
# MAGIC Add these as sample questions alongside the standard SQL questions:
# MAGIC
# MAGIC 1. Give me an executive summary of our current risk position
# MAGIC 2. How does this incident compare to our worst incidents historically?
# MAGIC 3. What regulatory deadlines are we approaching or have we missed?
# MAGIC 4. Should we escalate the current incident to Category 1?
# MAGIC 5. What would our penalty be if this incident runs for 12 more hours?
# MAGIC
# MAGIC > **Tip:** Agent questions work best when UC metadata is complete. Ensure Steps 2 and 6 are done before testing.
# MAGIC
# MAGIC ### Agent Question Rephrasings
# MAGIC
# MAGIC | Agent question | Rephrasings |
# MAGIC |---|---|
# MAGIC | Risk summary | "Board-level risk briefing", "What should I tell the CEO?" |
# MAGIC | Historical comparison | "How bad is this compared to past incidents?", "Worst incidents in AMP8" |
# MAGIC | Regulatory deadlines | "Regulatory compliance status", "Have we missed any notification windows?" |
# MAGIC | Escalation decision | "Do we need to escalate?", "Category 1 assessment" |
# MAGIC | Penalty projection | "Projected penalty if unresolved for 12 hours", "Worst-case financial exposure" |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 -- Add Sample Questions
# MAGIC
# MAGIC Add these 8 sample questions to help executives discover the Space's capabilities:
# MAGIC
# MAGIC 1. What is our total Ofwat penalty exposure right now?
# MAGIC 2. How many properties have exceeded the 3-hour supply interruption threshold this month?
# MAGIC 3. Which areas are giving us the most trouble this year?
# MAGIC 4. Show incidents in the last 30 days affecting more than 100 properties
# MAGIC 5. What percentage of customers received proactive notification before complaining?
# MAGIC 6. Which hospitals and schools are affected by supply interruptions this quarter?
# MAGIC 7. What is the average time from incident detection to DWI notification?
# MAGIC 8. Are we seeing more or fewer incidents than in AMP7?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 -- Add Verified Queries
# MAGIC
# MAGIC Verified queries ensure precise, tested SQL for executive-critical questions. These are stored in `genie/executive_verified_queries.sql` in the project repo.
# MAGIC
# MAGIC For each verified query:
# MAGIC
# MAGIC 1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
# MAGIC 2. Click **Add verified query**.
# MAGIC 3. Enter the **natural language question** as the trigger phrase — use natural phrasing for better prompt matching.
# MAGIC 4. Paste the corresponding SQL from `genie/executive_verified_queries.sql`.
# MAGIC 5. Add a brief description of the expected result.
# MAGIC 6. Click **Save**.
# MAGIC
# MAGIC Repeat for all 8 queries (Q1-Q8).
# MAGIC
# MAGIC ### Parameterized Queries
# MAGIC
# MAGIC Where a query takes a dynamic value, use `:parameter_name` syntax. Parameterized queries earn the **"Trusted"** label when matched.
# MAGIC
# MAGIC | Trigger phrase | Parameter | Type |
# MAGIC |---|---|---|
# MAGIC | "Penalty exposure for `:dma_code`" | `:dma_code` | String |
# MAGIC | "Incidents since `:start_date` affecting more than `:min_properties` properties" | `:start_date`, `:min_properties` | Date, Numeric |
# MAGIC | "Hospitals and schools in `:dma_code`" | `:dma_code` | String |
# MAGIC
# MAGIC > **Tip:** Executive verified queries are especially important because penalty calculations and regulatory metrics must be exact — Genie-generated SQL may not apply the correct formula (e.g., the 3-hour offset in penalty calculations).

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
# MAGIC For each of the 8 sample questions, add **2-3 rephrasings** to test robustness:
# MAGIC
# MAGIC | Original question | Rephrasings |
# MAGIC |---|---|
# MAGIC | Q1 — Total penalty exposure | "How much are we liable for in Ofwat fines?", "Current OPA penalty estimate" |
# MAGIC | Q2 — Properties >3h threshold | "Homes without water over 3 hours this month?", "How many properties have breached the Ofwat threshold?" |
# MAGIC | Q3 — Worst-performing areas | "Top 10 DMAs by incident count this year?", "Where are we seeing repeat problems?" |
# MAGIC | Q4 — Incidents >100 properties | "Major incidents in the last month?", "Large-scale supply disruptions recently?" |
# MAGIC | Q5 — Proactive notification % | "What's our C-MeX proactive rate?", "Were customers notified before they called in?" |
# MAGIC | Q6 — Hospitals/schools affected | "Sensitive premises impacted this quarter?", "Which schools and hospitals lost supply?" |
# MAGIC | Q7 — Detection to DWI time | "How fast are we notifying DWI?", "Average DWI notification lag" |
# MAGIC | Q8 — AMP8 vs AMP7 trend | "Compare incident frequency across AMPs", "Are incidents improving since AMP7?" |
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
# MAGIC 4. **Refine instructions** — if a pattern of failures emerges (e.g., Genie miscalculating penalties), add a targeted SQL expression or example query.
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
# MAGIC > **Tip:** For executive Spaces, penalty and regulatory calculations must be exact. If Genie generates incorrect penalty SQL, add it as a SQL expression rather than relying on text instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference: Expected Results
# MAGIC
# MAGIC Use these expected values when building benchmarks and validating results:
# MAGIC
# MAGIC | Question | Key expected result |
# MAGIC |---|---|
# MAGIC | Q1 - Penalty exposure | ~£180K total |
# MAGIC | Q2 - Properties >3h | 441 properties |
# MAGIC | Q3 - Worst-performing areas | DEMO_DMA_01 in top results |
# MAGIC | Q4 - Incidents >100 properties | Active incident with 441 properties |
# MAGIC | Q5 - Proactive notification % | High % (~423 proactive vs ~47 complaints) |
# MAGIC | Q6 - Hospitals/schools affected | DEMO_DMA_01: 1 hospital, 2 schools |
# MAGIC | Q7 - Detection to DWI time | A few hours |
# MAGIC | Q8 - AMP8 vs AMP7 | Annualised comparison for top 10 DMAs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Proceed to [05 -- Dashboard Guide](05_dashboard_guide.py) to create the executive Lakeview dashboard.
