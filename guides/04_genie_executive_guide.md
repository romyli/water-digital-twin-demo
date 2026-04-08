# Genie Executive Space Guide

**Water Utilities -- Digital Twin Demo**

Workspace: `https://adb-984752964297111.11.azuredatabricks.net/`
CLI profile: `adb-98`

---

## Overview

This guide creates the **"Water Operations Intelligence"** Genie Space for executive stakeholders. It surfaces regulatory exposure, penalty estimates, incident trends, and customer experience metrics using natural language queries.

---

## Step 1 -- Create the Genie Space

1. In the workspace sidebar, navigate to **Genie**.
2. Click **New** (or **Create Genie Space**).
3. Set the name: **Water Operations Intelligence**.
4. Set the description:
   > Executive intelligence for Water Utilities leadership. Query Ofwat penalty exposure, regulatory compliance, incident trends, C-MeX performance, and AMP8 investment metrics.

---

## Step 2 -- Add Trusted Assets

Add the following 9 tables and metric views as trusted assets.

### Metric Views (gold schema)

| # | Asset | Description |
|---|---|---|
| 1 | `water_digital_twin.gold.mv_incident_duration` | Duration metrics for incidents |
| 2 | `water_digital_twin.gold.mv_properties_affected` | Properties affected by incidents |
| 3 | `water_digital_twin.gold.mv_penalty_exposure` | Estimated Ofwat penalty calculations |
| 4 | `water_digital_twin.gold.mv_proactive_notification_rate` | Proactive vs reactive customer contact rates |

### Gold Operational Tables

| # | Asset | Description |
|---|---|---|
| 5 | `water_digital_twin.gold.dim_incidents` | Active and historical incident records with properties affected |
| 6 | `water_digital_twin.gold.incident_notifications` | Proactive notifications and reactive complaint counts per incident |
| 7 | `water_digital_twin.gold.regulatory_notifications` | DWI and Ofwat notification timestamps and compliance tracking |

### Silver Reference Tables

| # | Asset | Description |
|---|---|---|
| 8 | `water_digital_twin.silver.dim_properties` | Property register with types (residential, school, hospital, etc.) |
| 9 | `water_digital_twin.silver.dim_dma` | DMA reference data for geographic context |

---

## Step 3 -- Set the System Prompt

Paste the following system prompt into the Genie Space configuration:

```
You are a water industry executive intelligence assistant for Water Utilities. You help senior leaders, regulatory affairs teams, and board members understand operational risk, regulatory compliance, and customer experience metrics.

Key regulatory and industry context:

- Ofwat: The economic regulator of the water sector in England and Wales. Sets performance commitments and financial penalties.
- OPA (Outcome Performance Assessment): Ofwat's framework for measuring water company performance against commitments. Penalties are calculated per property, per hour of interruption beyond thresholds.
- Penalty calculation: For supply interruptions exceeding 3 hours, the penalty is approximately £580 per property per hour beyond the 3-hour threshold.
- DWI (Drinking Water Inspectorate): The regulator responsible for drinking water quality. Companies must notify DWI promptly when incidents affect water quality or supply.
- C-MeX (Customer Measure of Experience): Ofwat's customer satisfaction metric. Proactive notification before customers complain positively impacts C-MeX scores.
- AMP8 (Asset Management Period 8): The current 5-year regulatory period (2025-2030). Investment and performance targets are set per AMP cycle. AMP7 covered 2020-2025.
- ESG (Environmental, Social, and Governance): Water companies report ESG metrics including supply resilience, leakage reduction, and environmental compliance.
- Sensitive properties: Schools, hospitals, and care homes are tracked separately for regulatory reporting and prioritised during incident response.

Data model notes:
- Incidents track detected_ts, resolved_ts, status, properties_affected, and dma_code.
- Notification data separates proactive_notifications (sent before customer contact) from reactive_complaints (customer-initiated).
- Regulatory notifications track dwi_notified_ts for DWI compliance reporting.
- The demo snapshot time is 2026-04-07 05:30:00 UTC.

When answering:
- Always express monetary values in GBP (£) with appropriate formatting.
- When discussing penalties, show the formula and intermediate values.
- Express time thresholds clearly (e.g., "3-hour Ofwat threshold", "12-hour escalation").
- Compare current AMP8 performance against AMP7 baselines when relevant.
- Flag any properties exceeding the 12-hour supply interruption threshold (Category 1 escalation).
- For C-MeX metrics, express as percentages with proactive vs reactive breakdown.
```

---

## Step 4 -- Add Sample Questions

Add these 8 sample questions to help executives discover the Space's capabilities:

1. What is our total Ofwat penalty exposure right now?
2. How many properties have exceeded the 3-hour supply interruption threshold this month?
3. Which are the top 10 DMAs by incident count this year?
4. Show incidents in the last 30 days affecting more than 100 properties
5. What percentage of customers received proactive notification before complaining?
6. Which hospitals and schools are affected by supply interruptions this quarter?
7. What is the average time from incident detection to DWI notification?
8. Compare incident frequency for top 10 DMAs between AMP8 and AMP7

---

## Step 5 -- Add Verified Queries

Verified queries ensure precise, tested SQL for executive-critical questions. These are stored in `genie/executive_verified_queries.sql` in the project repo.

For each verified query:

1. In the Genie Space, navigate to **Verified Queries** (or **Curated Queries**).
2. Click **Add verified query**.
3. Enter the **natural language question** as the trigger phrase.
4. Paste the corresponding SQL from `genie/executive_verified_queries.sql`.
5. Add a brief description of the expected result.
6. Click **Save**.

Repeat for all 8 queries (Q1-Q8).

> **Tip:** Executive verified queries are especially important because penalty calculations and regulatory metrics must be exact -- Genie-generated SQL may not apply the correct formula (e.g., the 3-hour offset in penalty calculations).

---

## Verification

Test the Space by asking each of the 8 sample questions and confirming results match expected values:

| Question | Key expected result |
|---|---|
| Q1 - Penalty exposure | ~£180K total |
| Q2 - Properties >3h | 441 properties |
| Q3 - Top 10 DMAs by incidents | DEMO_DMA_01 in top results |
| Q4 - Incidents >100 properties | Active incident with 441 properties |
| Q5 - Proactive notification % | High % (~423 proactive vs ~47 complaints) |
| Q6 - Hospitals/schools affected | DEMO_DMA_01: 1 hospital, 2 schools |
| Q7 - Detection to DWI time | A few hours |
| Q8 - AMP8 vs AMP7 | Comparison data for top 10 DMAs |

---

## Next Step

Proceed to [05 -- Dashboard Guide](05_dashboard_guide.md) to create the executive Lakeview dashboard.
