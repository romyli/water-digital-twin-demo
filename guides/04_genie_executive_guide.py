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
# MAGIC ## Step 2 -- Add Trusted Assets
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
# MAGIC ## Step 3 -- Set the System Prompt
# MAGIC 
# MAGIC Paste the following system prompt into the Genie Space configuration:
# MAGIC 
# MAGIC ```
# MAGIC You are a water industry executive intelligence assistant for Water Utilities. You help senior leaders, regulatory affairs teams, and board members understand operational risk, regulatory compliance, and customer experience metrics.
# MAGIC 
# MAGIC Key regulatory and industry context:
# MAGIC 
# MAGIC - Ofwat: The economic regulator of the water sector in England and Wales. Sets performance commitments and financial penalties.
# MAGIC - OPA (Outcome Performance Assessment): Ofwat's framework for measuring water company performance against commitments. Penalties are calculated per property, per hour of interruption beyond thresholds.
# MAGIC - Penalty calculation: For supply interruptions exceeding 3 hours, the penalty is approximately £580 per property per hour beyond the 3-hour threshold.
# MAGIC - DWI (Drinking Water Inspectorate): The regulator responsible for drinking water quality. Companies must notify DWI promptly when incidents affect water quality or supply.
# MAGIC - C-MeX (Customer Measure of Experience): Ofwat's customer satisfaction metric. Proactive notification before customers complain positively impacts C-MeX scores.
# MAGIC - AMP8 (Asset Management Period 8): The current 5-year regulatory period (2025-2030). Investment and performance targets are set per AMP cycle. AMP7 covered 2020-2025.
# MAGIC - ESG (Environmental, Social, and Governance): Water companies report ESG metrics including supply resilience, leakage reduction, and environmental compliance.
# MAGIC - Sensitive properties: Schools, hospitals, and care homes are tracked separately for regulatory reporting and prioritised during incident response.
# MAGIC 
# MAGIC Data model notes:
# MAGIC - Incidents track detected_ts, resolved_ts, status, properties_affected, and dma_code.
# MAGIC - Notification data separates proactive_notifications (sent before customer contact) from reactive_complaints (customer-initiated).
# MAGIC - Regulatory notifications track dwi_notified_ts for DWI compliance reporting.
# MAGIC - The demo snapshot time is 2026-04-07 05:30:00 UTC.
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
# MAGIC ## Step 4 -- Add Sample Questions
# MAGIC 
# MAGIC Add these 8 sample questions to help executives discover the Space's capabilities:
# MAGIC 
# MAGIC 1. What is our total Ofwat penalty exposure right now?
# MAGIC 2. How many properties have exceeded the 3-hour supply interruption threshold this month?
# MAGIC 3. Which are the top 10 DMAs by incident count this year?
# MAGIC 4. Show incidents in the last 30 days affecting more than 100 properties
# MAGIC 5. What percentage of customers received proactive notification before complaining?
# MAGIC 6. Which hospitals and schools are affected by supply interruptions this quarter?
# MAGIC 7. What is the average time from incident detection to DWI notification?
# MAGIC 8. Compare incident frequency for top 10 DMAs between AMP8 and AMP7

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Add Verified Queries
# MAGIC 
# MAGIC Verified queries ensure precise, tested SQL for executive-critical questions. These are stored in `genie/executive_verified_queries.sql` in the project repo.
# MAGIC 
# MAGIC For each verified query:
# MAGIC 
# MAGIC 1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
# MAGIC 2. Click **Add verified query**.
# MAGIC 3. Enter the **natural language question** as the trigger phrase.
# MAGIC 4. Paste the corresponding SQL from `genie/executive_verified_queries.sql`.
# MAGIC 5. Add a brief description of the expected result.
# MAGIC 6. Click **Save**.
# MAGIC 
# MAGIC Repeat for all 8 queries (Q1-Q8).
# MAGIC 
# MAGIC > **Tip:** Executive verified queries are especially important because penalty calculations and regulatory metrics must be exact -- Genie-generated SQL may not apply the correct formula (e.g., the 3-hour offset in penalty calculations).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC 
# MAGIC Test the Space by asking each of the 8 sample questions and confirming results match expected values:
# MAGIC 
# MAGIC | Question | Key expected result |
# MAGIC |---|---|
# MAGIC | Q1 - Penalty exposure | ~£180K total |
# MAGIC | Q2 - Properties >3h | 441 properties |
# MAGIC | Q3 - Top 10 DMAs by incidents | DEMO_DMA_01 in top results |
# MAGIC | Q4 - Incidents >100 properties | Active incident with 441 properties |
# MAGIC | Q5 - Proactive notification % | High % (~423 proactive vs ~47 complaints) |
# MAGIC | Q6 - Hospitals/schools affected | DEMO_DMA_01: 1 hospital, 2 schools |
# MAGIC | Q7 - Detection to DWI time | A few hours |
# MAGIC | Q8 - AMP8 vs AMP7 | Comparison data for top 10 DMAs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC 
# MAGIC Proceed to [05 -- Dashboard Guide](05_dashboard_guide.py) to create the executive Lakeview dashboard.
