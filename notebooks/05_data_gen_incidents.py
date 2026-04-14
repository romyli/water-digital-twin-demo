# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Incident & Operational Data Generation
# MAGIC
# MAGIC Generates incidents, event logs, communications, response playbooks,
# MAGIC shift handovers, and comms requests for the gold layer.
# MAGIC
# MAGIC **Outputs:**
# MAGIC | Layer | Table |
# MAGIC |-------|-------|
# MAGIC | Gold | `gold.dim_incidents` |
# MAGIC | Gold | `gold.incident_events` |
# MAGIC | Gold | `gold.communications_log` |
# MAGIC | Gold | `gold.dim_response_playbooks` |
# MAGIC | Gold | `gold.shift_handovers` |
# MAGIC | Gold | `gold.comms_requests` |
# MAGIC | Gold | `gold.incident_notifications` |
# MAGIC | Gold | `gold.regulatory_notifications` |

# COMMAND ----------

# DBTITLE 1,Configuration
import random
import json
import pandas as pd
from datetime import datetime, timedelta

CATALOG = "water_digital_twin"
SEED = 42
random.seed(SEED + 7000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incidents

# COMMAND ----------

# DBTITLE 1,Generate Incident Records

incident_records = []

# --- Active incident: INC-2026-0407-001 ---
incident_records.append({
    "incident_id": "INC-2026-0407-001",
    "dma_code": "DEMO_DMA_01",
    "root_cause_asset_id": "DEMO_PUMP_01",
    "root_cause_asset_type": "pump_station",
    "incident_type": "pump_station_trip",
    "title": "Crystal Palace Booster Station Trip — Low Pressure Event",
    "description": (
        "DEMO_PUMP_01 (Crystal Palace Booster Station) tripped at 02:03 on 2026-04-07, "
        "causing a significant pressure drop across DEMO_DMA_01 (Crystal Palace South). "
        "Multiple pressure sensors recording below 15m head. Adjacent DMAs (Sydenham Hill, "
        "Norwood Junction) experiencing mild pressure dips. Customer complaints received "
        "from high-elevation properties. Sensitive premises (schools, hospital) affected."
    ),
    "start_timestamp": "2026-04-07 02:03:00",
    "end_timestamp": None,
    "status": "active",
    "severity": "high",
    "total_properties_affected": 441,
    "sensitive_premises_affected": True,
    "priority_score": 92,
    "escalation_risk": "High — reservoir at 43% with morning demand surge approaching. If pump not restored by 07:30, recommend Ofwat pre-notification and tanker deployment to sensitive premises.",
    "trajectory": "Pressure stabilising at 5-12m head but not recovering. Reservoir draw-down rate 0.694 ML/hr will accelerate after 06:00 morning demand surge. Without pump restoration, full DMA supply loss expected by ~08:30.",
    "first_complaint_time": "2026-04-07 03:05:00",
    "assigned_team": "Network Operations",
    "sop_reference": "SOP-WN-042",
    "created_at": "2026-04-07 02:18:00",
    "updated_at": "2026-04-07 05:30:00",
})

# --- 5-10 historical incidents (Oct 2025 - Mar 2026) ---
historical_incidents = [
    {
        "incident_id": "INC-2025-1103-001",
        "dma_code": "DEMO_DMA_01",
        "root_cause_asset_id": "DEMO_PUMP_01",
        "root_cause_asset_type": "pump_station",
        "incident_type": "pump_station_trip",
        "title": "Crystal Palace Booster Station Trip — Brief Interruption",
        "description": (
            "DEMO_PUMP_01 tripped due to electrical fault. Auto-restart successful after 12 minutes. "
            "Minor pressure dip recorded, no customer complaints received."
        ),
        "start_timestamp": "2025-11-03 14:22:00",
        "end_timestamp": "2025-11-03 14:34:00",
        "status": "resolved",
        "severity": "low",
        "total_properties_affected": 0,
        "sensitive_premises_affected": False,
        "priority_score": 25,
        "escalation_risk": "Resolved — auto-restart successful, no customer impact.",
        "trajectory": "Brief pressure dip recovered within 12 minutes of auto-restart.",
        "first_complaint_time": None,
        "assigned_team": "Network Operations",
        "sop_reference": "SOP-WN-042",
        "created_at": "2025-11-03 14:22:00",
        "updated_at": "2025-11-03 15:00:00",
    },
    {
        "incident_id": "INC-2026-0115-001",
        "dma_code": "DEMO_DMA_01",
        "root_cause_asset_id": "DEMO_TM_001",
        "root_cause_asset_type": "trunk_main",
        "incident_type": "burst_main",
        "title": "Crystal Palace Trunk Main Leak — Repaired",
        "description": (
            "Small leak detected on DEMO_TM_001 near junction of Sydenham Road. "
            "Repair crew dispatched, isolation and repair completed in 4 hours. "
            "Temporary pressure reduction in DEMO_DMA_01 during repair."
        ),
        "start_timestamp": "2026-01-15 08:45:00",
        "end_timestamp": "2026-01-15 12:50:00",
        "status": "resolved",
        "severity": "medium",
        "total_properties_affected": 120,
        "sensitive_premises_affected": False,
        "priority_score": 55,
        "escalation_risk": "Resolved — leak isolated and repaired within 4 hours.",
        "trajectory": "Pressure reduced to 30m during repair, recovered to normal within 30 mins of completion.",
        "first_complaint_time": "2026-01-15 09:20:00",
        "assigned_team": "Repair & Maintenance",
        "sop_reference": "SOP-WN-018",
        "created_at": "2026-01-15 08:50:00",
        "updated_at": "2026-01-15 13:00:00",
    },
    {
        "incident_id": "INC-2025-1020-001",
        "dma_code": "DMA_00010",
        "root_cause_asset_id": "PUMP_002",
        "root_cause_asset_type": "pump_station",
        "incident_type": "pump_station_trip",
        "title": "Highgate Booster Station Electrical Fault",
        "description": (
            "Highgate booster station tripped due to power supply fluctuation. "
            "Backup generator activated within 5 minutes. Full power restored after 45 minutes."
        ),
        "start_timestamp": "2025-10-20 23:15:00",
        "end_timestamp": "2025-10-21 00:00:00",
        "status": "resolved",
        "severity": "medium",
        "total_properties_affected": 85,
        "sensitive_premises_affected": False,
        "priority_score": 48,
        "escalation_risk": "Resolved — backup generator maintained supply during main power restoration.",
        "trajectory": "Brief supply disruption, backup generator activated within 5 mins. Full power restored in 45 mins.",
        "first_complaint_time": "2025-10-20 23:40:00",
        "assigned_team": "Network Operations",
        "sop_reference": "SOP-WN-042",
        "created_at": "2025-10-20 23:18:00",
        "updated_at": "2025-10-21 00:15:00",
    },
    {
        "incident_id": "INC-2025-1208-001",
        "dma_code": "DMA_00030",
        "root_cause_asset_id": "TM_004",
        "root_cause_asset_type": "trunk_main",
        "incident_type": "burst_main",
        "title": "Wimbledon Trunk Main Burst",
        "description": (
            "Major burst on 14-inch trunk main in Wimbledon area. Surface flooding reported. "
            "Emergency isolation valves closed. Repair completed after 8 hours."
        ),
        "start_timestamp": "2025-12-08 06:30:00",
        "end_timestamp": "2025-12-08 14:45:00",
        "status": "resolved",
        "severity": "high",
        "total_properties_affected": 320,
        "sensitive_premises_affected": True,
        "priority_score": 85,
        "escalation_risk": "Resolved — major burst required 8-hour repair. DWI and Ofwat both notified.",
        "trajectory": "Surface flooding contained within 2 hours. Pressure restored progressively as isolation valves reopened.",
        "first_complaint_time": "2025-12-08 07:00:00",
        "assigned_team": "Emergency Response",
        "sop_reference": "SOP-WN-018",
        "created_at": "2025-12-08 06:35:00",
        "updated_at": "2025-12-08 15:00:00",
    },
    {
        "incident_id": "INC-2026-0212-001",
        "dma_code": "DMA_00050",
        "root_cause_asset_id": "PRV_005",
        "root_cause_asset_type": "prv",
        "incident_type": "prv_failure",
        "title": "Hampstead PRV Malfunction — Overpressure",
        "description": (
            "PRV failure at Hampstead resulted in temporary overpressure in downstream DMA. "
            "Manual override applied. Valve replaced within 3 hours."
        ),
        "start_timestamp": "2026-02-12 11:00:00",
        "end_timestamp": "2026-02-12 14:10:00",
        "status": "resolved",
        "severity": "medium",
        "total_properties_affected": 65,
        "sensitive_premises_affected": False,
        "priority_score": 42,
        "escalation_risk": "Resolved — PRV replaced within 3 hours. Overpressure contained by manual override.",
        "trajectory": "Downstream pressure spiked to 75m, capped by manual override at 60m. Normal after valve replacement.",
        "first_complaint_time": None,
        "assigned_team": "Repair & Maintenance",
        "sop_reference": "SOP-WN-031",
        "created_at": "2026-02-12 11:05:00",
        "updated_at": "2026-02-12 14:30:00",
    },
    {
        "incident_id": "INC-2026-0305-001",
        "dma_code": "DMA_00080",
        "root_cause_asset_id": "PUMP_008",
        "root_cause_asset_type": "pump_station",
        "incident_type": "pump_station_trip",
        "title": "Brixton Pump Station Trip — Resolved",
        "description": (
            "Brixton pump station tripped during high-demand period. "
            "Manual restart performed by on-site crew. Service restored in 25 minutes."
        ),
        "start_timestamp": "2026-03-05 07:45:00",
        "end_timestamp": "2026-03-05 08:10:00",
        "status": "resolved",
        "severity": "low",
        "total_properties_affected": 40,
        "sensitive_premises_affected": False,
        "priority_score": 30,
        "escalation_risk": "Resolved — manual restart successful within 25 minutes.",
        "trajectory": "Pressure dipped to 28m during trip, recovered to normal within 10 minutes of restart.",
        "first_complaint_time": None,
        "assigned_team": "Network Operations",
        "sop_reference": "SOP-WN-042",
        "created_at": "2026-03-05 07:48:00",
        "updated_at": "2026-03-05 08:30:00",
    },
    {
        "incident_id": "INC-2026-0319-001",
        "dma_code": "DMA_00060",
        "root_cause_asset_id": "TM_007",
        "root_cause_asset_type": "trunk_main",
        "incident_type": "burst_main",
        "title": "Dulwich Trunk Main Joint Failure",
        "description": (
            "Joint failure on Dulwich trunk main caused gradual pressure loss. "
            "Detected via telemetry anomaly. Repair completed overnight."
        ),
        "start_timestamp": "2026-03-19 20:00:00",
        "end_timestamp": "2026-03-20 04:30:00",
        "status": "resolved",
        "severity": "medium",
        "total_properties_affected": 150,
        "sensitive_premises_affected": False,
        "priority_score": 52,
        "escalation_risk": "Resolved — joint failure repaired overnight. DWI notified as precaution.",
        "trajectory": "Gradual pressure loss detected by anomaly engine. Stabilised at 32m during repair, full recovery by 05:00.",
        "first_complaint_time": "2026-03-19 21:15:00",
        "assigned_team": "Repair & Maintenance",
        "sop_reference": "SOP-WN-018",
        "created_at": "2026-03-19 20:10:00",
        "updated_at": "2026-03-20 05:00:00",
    },
]

incident_records.extend(historical_incidents)
print(f"Total incidents: {len(incident_records)}")
print(f"  Active: {sum(1 for i in incident_records if i['status'] == 'active')}")
print(f"  Resolved: {sum(1 for i in incident_records if i['status'] == 'resolved')}")
print(f"  DEMO_DMA_01 incidents: {sum(1 for i in incident_records if i['dma_code'] == 'DEMO_DMA_01')}")

# COMMAND ----------

# DBTITLE 1,Write gold.dim_incidents

df_incidents = spark.createDataFrame(pd.DataFrame(incident_records))
df_incidents.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.dim_incidents")
print(f"Wrote {df_incidents.count()} rows to {CATALOG}.gold.dim_incidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incident Event Log

# COMMAND ----------

# DBTITLE 1,Generate Incident Event Log for INC-2026-0407-001

event_records = [
    {
        "event_id": "EVT-001",
        "incident_id": "INC-2026-0407-001",
        "event_type": "asset_trip",
        "event_timestamp": "2026-04-07 02:03:00",
        "description": "DEMO_PUMP_01 (Crystal Palace Booster Station) tripped. SCADA alarm triggered. Pump motor overcurrent protection activated.",
        "actor": "SCADA_SYSTEM",
        "metadata": json.dumps({"asset_id": "DEMO_PUMP_01", "alarm_code": "PMP-OC-001", "motor_current_amps": 125.3}),
    },
    {
        "event_id": "EVT-002",
        "incident_id": "INC-2026-0407-001",
        "event_type": "anomaly_detected",
        "event_timestamp": "2026-04-07 02:18:00",
        "description": "Anomaly detection system flagged sustained pressure drop across 12 sensors in DEMO_DMA_01. Multiple sensors below RED threshold (15m).",
        "actor": "ANOMALY_ENGINE",
        "metadata": json.dumps({"sensors_affected": 12, "avg_pressure_m": 8.4, "threshold_breached": "RED"}),
    },
    {
        "event_id": "EVT-003",
        "incident_id": "INC-2026-0407-001",
        "event_type": "alert_acknowledged",
        "event_timestamp": "2026-04-07 02:25:00",
        "description": "Night shift operator OP_NIGHT_01 acknowledged the alert and initiated incident response protocol SOP-WN-042.",
        "actor": "OP_NIGHT_01",
        "metadata": json.dumps({"operator_id": "OP_NIGHT_01", "sop_initiated": "SOP-WN-042"}),
    },
    {
        "event_id": "EVT-004",
        "incident_id": "INC-2026-0407-001",
        "event_type": "rag_change",
        "event_timestamp": "2026-04-07 02:30:00",
        "description": "DEMO_DMA_01 RAG status changed to RED. Average pressure across DMA below 15m threshold.",
        "actor": "RAG_ENGINE",
        "metadata": json.dumps({"dma_code": "DEMO_DMA_01", "previous_rag": "GREEN", "new_rag": "RED", "trigger": "avg_pressure_below_15m"}),
    },
    {
        "event_id": "EVT-005",
        "incident_id": "INC-2026-0407-001",
        "event_type": "rag_change",
        "event_timestamp": "2026-04-07 02:30:00",
        "description": "DEMO_DMA_02 and DEMO_DMA_03 RAG status changed to AMBER. Mild pressure dips detected in adjacent DMAs.",
        "actor": "RAG_ENGINE",
        "metadata": json.dumps({"dma_codes": ["DEMO_DMA_02", "DEMO_DMA_03"], "previous_rag": "GREEN", "new_rag": "AMBER", "trigger": "avg_pressure_below_25m"}),
    },
    {
        "event_id": "EVT-006",
        "incident_id": "INC-2026-0407-001",
        "event_type": "customer_complaint",
        "event_timestamp": "2026-04-07 03:05:00",
        "description": "First customer complaints received. 5 reports of no water or low pressure from high-elevation properties in Crystal Palace area.",
        "actor": "CONTACT_CENTRE",
        "metadata": json.dumps({"complaint_count": 5, "complaint_types": ["no_water", "low_pressure"]}),
    },
    {
        "event_id": "EVT-007",
        "incident_id": "INC-2026-0407-001",
        "event_type": "crew_dispatched",
        "event_timestamp": "2026-04-07 03:15:00",
        "description": "Emergency repair crew dispatched to DEMO_PUMP_01 site. ETA 30 minutes. Crew reference: CREW-NIGHT-03.",
        "actor": "OP_NIGHT_01",
        "metadata": json.dumps({"crew_id": "CREW-NIGHT-03", "eta_minutes": 30, "destination": "DEMO_PUMP_01"}),
    },
    {
        "event_id": "EVT-008",
        "incident_id": "INC-2026-0407-001",
        "event_type": "regulatory_notification",
        "event_timestamp": "2026-04-07 04:10:00",
        "description": "DWI (Drinking Water Inspectorate) Duty Officer notified of potential supply interruption affecting sensitive premises in Crystal Palace area.",
        "actor": "OP_NIGHT_01",
        "metadata": json.dumps({"regulator": "DWI", "contact": "Duty Officer", "sensitive_premises": True, "notification_ref": "DWI-NOT-2026-0407-001"}),
    },
    {
        "event_id": "EVT-009",
        "incident_id": "INC-2026-0407-001",
        "event_type": "customer_comms",
        "event_timestamp": "2026-04-07 04:30:00",
        "description": "Customer Communications Team activated. Automated SMS and email notifications prepared for 423 affected properties.",
        "actor": "COMMS_TEAM",
        "metadata": json.dumps({"properties_notified": 423, "channels": ["sms", "email"], "message_template": "TPL-SUPPLY-INT-001"}),
    },
    {
        "event_id": "EVT-010",
        "incident_id": "INC-2026-0407-001",
        "event_type": "shift_handover",
        "event_timestamp": "2026-04-07 05:30:00",
        "description": "Shift handover from night team (OP_NIGHT_01) to day team (OP_DAY_01). Incident status, actions taken, and outstanding risks briefed.",
        "actor": "OP_NIGHT_01",
        "metadata": json.dumps({"outgoing": "OP_NIGHT_01", "incoming": "OP_DAY_01", "handover_ref": "HO-2026-0407-0530"}),
    },
]

print(f"Incident events: {len(event_records)}")

# COMMAND ----------

# DBTITLE 1,Write gold.incident_events

df_events = spark.createDataFrame(pd.DataFrame(event_records))
df_events.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.incident_events")
print(f"Wrote {df_events.count()} rows to {CATALOG}.gold.incident_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Communications Log

# COMMAND ----------

# DBTITLE 1,Generate Communications Log

comms_records = [
    {
        "comms_id": "COMM-001",
        "incident_id": "INC-2026-0407-001",
        "comms_timestamp": "2026-04-07 03:20:00",
        "recipient_role": "Network Manager",
        "recipient_name": "On-Call Network Manager",
        "channel": "phone",
        "direction": "outbound",
        "summary": "Briefed Network Manager on DEMO_PUMP_01 trip and pressure drop across Crystal Palace zone. Confirmed crew dispatch in progress. Manager approved escalation to DWI if not resolved within 2 hours.",
        "status": "completed",
    },
    {
        "comms_id": "COMM-002",
        "incident_id": "INC-2026-0407-001",
        "comms_timestamp": "2026-04-07 04:10:00",
        "recipient_role": "DWI Duty Officer",
        "recipient_name": "DWI Duty Officer",
        "channel": "phone",
        "direction": "outbound",
        "summary": "Regulatory notification to DWI regarding supply interruption in Crystal Palace area. Confirmed sensitive premises affected including schools and hospital. DWI acknowledged and requested 4-hourly updates.",
        "status": "completed",
    },
    {
        "comms_id": "COMM-003",
        "incident_id": "INC-2026-0407-001",
        "comms_timestamp": "2026-04-07 04:30:00",
        "recipient_role": "Customer Comms Team",
        "recipient_name": "Customer Communications Duty Team",
        "channel": "email",
        "direction": "outbound",
        "summary": "Requested activation of customer notification workflow for 423 properties in Crystal Palace area. Template TPL-SUPPLY-INT-001 approved. SMS and email channels confirmed.",
        "status": "completed",
    },
    {
        "comms_id": "COMM-004",
        "incident_id": "INC-2026-0407-001",
        "comms_timestamp": "2026-04-07 05:00:00",
        "recipient_role": "Executive On-Call",
        "recipient_name": "Executive On-Call Director",
        "channel": "phone",
        "direction": "outbound",
        "summary": "Executive escalation briefing. Incident ongoing for 3 hours, 441 properties affected including sensitive premises. Crew on site, pump under investigation. Reservoir level at 43% with approximately 3 hours supply remaining. Day shift handover imminent.",
        "status": "completed",
    },
]

print(f"Communications log entries: {len(comms_records)}")

# COMMAND ----------

# DBTITLE 1,Write gold.communications_log

df_comms = spark.createDataFrame(pd.DataFrame(comms_records))
df_comms.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.communications_log")
print(f"Wrote {df_comms.count()} rows to {CATALOG}.gold.communications_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Response Playbook

# COMMAND ----------

# DBTITLE 1,Generate Response Playbook

action_steps = [
    {
        "step": 1,
        "action": "Verify pump trip alarm",
        "description": "Confirm SCADA alarm is genuine. Check for communication faults. Verify pump motor status and protection relay flags.",
        "responsible": "Control Room Operator",
        "sla_minutes": 5,
    },
    {
        "step": 2,
        "action": "Assess downstream impact",
        "description": "Review pressure telemetry in affected DMA and adjacent DMAs. Identify sensors below RED/AMBER thresholds. Check reservoir levels.",
        "responsible": "Control Room Operator",
        "sla_minutes": 15,
    },
    {
        "step": 3,
        "action": "Attempt remote restart",
        "description": "If safe to do so, attempt SCADA remote restart of pump. Monitor for 5 minutes. If restart fails or pump re-trips, proceed to Step 4.",
        "responsible": "Control Room Operator",
        "sla_minutes": 10,
    },
    {
        "step": 4,
        "action": "Dispatch repair crew",
        "description": "Dispatch nearest available crew to pump station for manual inspection and restart. Confirm ETA with crew leader.",
        "responsible": "Duty Manager",
        "sla_minutes": 15,
    },
    {
        "step": 5,
        "action": "Activate customer communications",
        "description": "If pressure impact persists beyond 30 minutes, activate customer notification workflow. Identify sensitive premises for priority contact.",
        "responsible": "Customer Comms Team",
        "sla_minutes": 30,
    },
    {
        "step": 6,
        "action": "Regulatory notification",
        "description": "If supply interruption affects sensitive premises or exceeds 2 hours, notify DWI Duty Officer and record notification reference.",
        "responsible": "Duty Manager",
        "sla_minutes": 120,
    },
]

playbook_records = [
    {
        "playbook_id": "PB-SOP-WN-042",
        "sop_reference": "SOP-WN-042",
        "incident_type": "pump_station_trip",
        "title": "Pump Station Trip Response Procedure",
        "description": (
            "Standard operating procedure for responding to pump station trip events. "
            "Covers alarm verification, impact assessment, restart attempts, crew dispatch, "
            "customer communications, and regulatory notification."
        ),
        "action_steps": json.dumps(action_steps),
        "total_steps": len(action_steps),
        "max_sla_minutes": max(s["sla_minutes"] for s in action_steps),
        "version": "3.1",
        "effective_date": "2025-09-01",
        "review_date": "2026-09-01",
        "owner": "Head of Network Operations",
        "created_at": "2025-09-01 00:00:00",
    }
]

print(f"Playbook records: {len(playbook_records)}")
print(f"Action steps in SOP-WN-042: {len(action_steps)}")

# COMMAND ----------

# DBTITLE 1,Write gold.dim_response_playbooks

df_playbooks = spark.createDataFrame(pd.DataFrame(playbook_records))
df_playbooks.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.dim_response_playbooks")
print(f"Wrote {df_playbooks.count()} rows to {CATALOG}.gold.dim_response_playbooks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shift Handover

# COMMAND ----------

# DBTITLE 1,Generate Shift Handover

handover_summary = (
    "SHIFT HANDOVER — Night to Day, 2026-04-07 05:30\n\n"
    "INCIDENT SUMMARY:\n"
    "At 02:03 DEMO_PUMP_01 (Crystal Palace Booster Station) tripped with motor overcurrent "
    "protection activated. Cause is suspected mechanical fault on pump bearing assembly — crew "
    "on site report vibration damage and are awaiting replacement parts.\n\n"
    "IMPACT:\n"
    "- DEMO_DMA_01 (Crystal Palace South): RED status. All 18 pressure sensors below 15m head. "
    "441 properties affected including 2 schools, 1 hospital, and 3 dialysis-dependent homes.\n"
    "- DEMO_DMA_02 (Sydenham Hill): AMBER status. Pressure dipped to 25-35m range.\n"
    "- DEMO_DMA_03 (Norwood Junction): AMBER status. Similar mild pressure reduction.\n\n"
    "ACTIONS TAKEN:\n"
    "1. Remote restart attempted at 02:10 — failed (pump re-tripped immediately)\n"
    "2. Crew CREW-NIGHT-03 dispatched at 03:15, arrived on site 03:45\n"
    "3. DWI notified at 04:10 (ref: DWI-NOT-2026-0407-001) — 4-hourly updates required\n"
    "4. Customer comms activated at 04:30 — 423 properties notified via SMS/email\n"
    "5. Executive On-Call briefed at 05:00\n\n"
    "RESERVOIR STATUS:\n"
    "DEMO_SR_01 (Crystal Palace): 43% capacity (2.15 ML), ~3.1 hours supply at current demand "
    "rate of 0.694 ML/hr. Adjacent reservoirs not yet supplementing — consider opening "
    "interconnection valves if pump repair exceeds 2 hours.\n\n"
    "OUTSTANDING RISKS:\n"
    "- Reservoir depletion if pump not restored by ~08:30\n"
    "- Morning demand surge (06:00-09:00) will accelerate pressure drop\n"
    "- School opening at 08:30 — St Mary's Primary and Crystal Palace Academy both affected\n\n"
    "RECOMMENDED ACTIONS FOR DAY SHIFT:\n"
    "- Chase replacement parts delivery (expected 07:00)\n"
    "- Pre-position tankers for sensitive premises if pump not restored by 07:30\n"
    "- Consider opening DEMO_VALVE_01/02 to allow flow from adjacent trunk mains\n"
    "- Send DWI 4-hourly update at 08:10\n"
    "- Brief Head of Operations at 08:00 daily standup"
)

handover_records = [
    {
        "handover_id": "HO-2026-0407-0530",
        "incident_id": "INC-2026-0407-001",
        "outgoing_operator": "OP_NIGHT_01",
        "incoming_operator": "OP_DAY_01",
        "handover_timestamp": "2026-04-07 05:30:00",
        "signed_off_timestamp": "2026-04-07 05:25:00",
        "acknowledged_timestamp": "2026-04-07 05:30:00",
        "risk_of_escalation": "low",
        "trajectory": "stabilising",
        "summary": handover_summary,
        "status": "acknowledged",
    }
]

print(f"Handover records: {len(handover_records)}")

# COMMAND ----------

# DBTITLE 1,Write gold.shift_handovers

df_handovers = spark.createDataFrame(pd.DataFrame(handover_records))
df_handovers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.shift_handovers")
print(f"Wrote {df_handovers.count()} rows to {CATALOG}.gold.shift_handovers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outstanding Actions

# COMMAND ----------

# DBTITLE 1,Generate & Write Outstanding Actions

# These are pending tasks for the incoming shift — the things that still need doing.
# Without this table, the Shift Handover "Outstanding Actions" section is always empty.

outstanding_actions = [
    {
        "action_id": "ACT-2026-0407-001",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Chase replacement bearing assembly delivery for DEMO_PUMP_01 — supplier confirmed dispatch, expected arrival 07:00",
        "priority": "critical",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 07:30:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Supplier: Grundfos UK, order ref GRF-2026-04071. Contact: 0800 123 4567",
    },
    {
        "action_id": "ACT-2026-0407-002",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Pre-position emergency water tankers at St Mary's Primary and Crystal Palace Academy if pump not restored by 07:30",
        "priority": "high",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 07:30:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "2 x 10,000L tankers available at Croydon depot. Schools open at 08:30.",
    },
    {
        "action_id": "ACT-2026-0407-003",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Evaluate opening DEMO_VALVE_01 and DEMO_VALVE_02 to allow supplementary flow from adjacent trunk mains",
        "priority": "high",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 07:00:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Risk: may reduce pressure in DEMO_DMA_02/03 further. Model impact before opening.",
    },
    {
        "action_id": "ACT-2026-0407-004",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Send 4-hourly DWI update (ref: DWI-NOT-2026-0407-001) — next update due 08:10",
        "priority": "high",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 08:10:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "DWI Duty Officer acknowledged initial notification. Include reservoir levels and ETA for repair.",
    },
    {
        "action_id": "ACT-2026-0407-005",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Brief Head of Operations at 08:00 daily standup on incident status and resource requirements",
        "priority": "medium",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 08:00:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Include: customer impact count, reservoir depletion timeline, crew status, parts ETA.",
    },
    {
        "action_id": "ACT-2026-0407-006",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Monitor DEMO_SR_01 reservoir level — currently at 43%, projected depletion ~08:30 if pump not restored",
        "priority": "critical",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 08:30:00",
        "status": "in_progress",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Capacity 5.0 ML, current volume 2.15 ML, demand rate 0.694 ML/hr. Morning surge will accelerate.",
    },
    {
        "action_id": "ACT-2026-0407-007",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Contact 3 dialysis-dependent homes to confirm alternative water arrangements are in place",
        "priority": "high",
        "assigned_to": "COMMS_TEAM",
        "assigned_role": "Customer Communications",
        "due_by": "2026-04-07 07:00:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Dialysis patients need guaranteed supply. Coordinate with local NHS trust if needed.",
    },
    {
        "action_id": "ACT-2026-0407-008",
        "incident_id": "INC-2026-0407-001",
        "action_description": "Prepare Ofwat 12-hour escalation report if incident not resolved by 14:03",
        "priority": "medium",
        "assigned_to": "OP_DAY_01",
        "assigned_role": "Day Shift Operator",
        "due_by": "2026-04-07 13:00:00",
        "status": "pending",
        "created_at": "2026-04-07 05:30:00",
        "created_by": "OP_NIGHT_01",
        "notes": "Ofwat 12-hour threshold at 14:03 (incident start + 12h). Draft report in advance.",
    },
]

df_actions = spark.createDataFrame(pd.DataFrame(outstanding_actions))
df_actions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.incident_outstanding_actions")
print(f"Wrote {df_actions.count()} outstanding action records to {CATALOG}.gold.incident_outstanding_actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comms Requests

# COMMAND ----------

# DBTITLE 1,Generate & Write Comms Requests

comms_request_records = [
    {
        "request_id": "CR-2026-0407-001",
        "incident_id": "INC-2026-0407-001",
        "dma_code": "DEMO_DMA_01",
        "request_timestamp": "2026-04-07 04:30:00",
        "customer_count": 423,
        "channels": json.dumps(["sms", "email"]),
        "message_template": "TPL-SUPPLY-INT-001",
        "message_summary": (
            "We are aware of a water supply issue in the Crystal Palace area. "
            "Our engineers are on site working to restore normal service. "
            "We apologise for any inconvenience."
        ),
        "status": "sent",
        "sent_timestamp": "2026-04-07 04:35:00",
        "delivery_rate_pct": 98.2,
        "requested_by": "OP_NIGHT_01",
        "approved_by": "COMMS_DUTY_MANAGER",
    }
]

df_comms_req = spark.createDataFrame(pd.DataFrame(comms_request_records))
df_comms_req.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.comms_requests")
print(f"Wrote {df_comms_req.count()} rows to {CATALOG}.gold.comms_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incident Notifications

# COMMAND ----------

# DBTITLE 1,Generate & Write Incident Notifications

# Per-incident summary of proactive notifications vs reactive complaints.
# Used by the executive Genie Space for C-MeX / proactive notification metrics.

notification_records = [
    {
        "incident_id": "INC-2026-0407-001",
        "proactive_notifications": 423,
        "reactive_complaints": 47,
    },
    {
        "incident_id": "INC-2025-1103-001",
        "proactive_notifications": 0,
        "reactive_complaints": 0,
    },
    {
        "incident_id": "INC-2026-0115-001",
        "proactive_notifications": 95,
        "reactive_complaints": 12,
    },
    {
        "incident_id": "INC-2025-1020-001",
        "proactive_notifications": 60,
        "reactive_complaints": 8,
    },
    {
        "incident_id": "INC-2025-1208-001",
        "proactive_notifications": 280,
        "reactive_complaints": 25,
    },
    {
        "incident_id": "INC-2026-0212-001",
        "proactive_notifications": 50,
        "reactive_complaints": 5,
    },
    {
        "incident_id": "INC-2026-0305-001",
        "proactive_notifications": 30,
        "reactive_complaints": 3,
    },
    {
        "incident_id": "INC-2026-0319-001",
        "proactive_notifications": 120,
        "reactive_complaints": 10,
    },
]

df_notifications = spark.createDataFrame(pd.DataFrame(notification_records))
df_notifications.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.incident_notifications")
print(f"Wrote {df_notifications.count()} rows to {CATALOG}.gold.incident_notifications")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regulatory Notifications

# COMMAND ----------

# DBTITLE 1,Generate & Write Regulatory Notifications

# DWI and Ofwat notification timestamps per incident.
# Only incidents exceeding 2 hours or affecting sensitive premises require DWI notification.

regulatory_records = [
    {
        "notification_id": "REG-2026-0407-001",
        "incident_id": "INC-2026-0407-001",
        "regulator": "DWI",
        "dwi_notified_ts": "2026-04-07 04:10:00",
        "ofwat_notified_ts": "2026-04-07 05:15:00",
        "notification_ref": "DWI-NOT-2026-0407-001",
        "status": "acknowledged",
    },
    {
        "notification_id": "REG-2025-1208-001",
        "incident_id": "INC-2025-1208-001",
        "regulator": "DWI",
        "dwi_notified_ts": "2025-12-08 08:30:00",
        "ofwat_notified_ts": "2025-12-08 09:00:00",
        "notification_ref": "DWI-NOT-2025-1208-001",
        "status": "acknowledged",
    },
    {
        "notification_id": "REG-2026-0115-001",
        "incident_id": "INC-2026-0115-001",
        "regulator": "DWI",
        "dwi_notified_ts": "2026-01-15 11:00:00",
        "ofwat_notified_ts": None,
        "notification_ref": "DWI-NOT-2026-0115-001",
        "status": "acknowledged",
    },
    {
        "notification_id": "REG-2026-0319-001",
        "incident_id": "INC-2026-0319-001",
        "regulator": "DWI",
        "dwi_notified_ts": "2026-03-19 22:15:00",
        "ofwat_notified_ts": None,
        "notification_ref": "DWI-NOT-2026-0319-001",
        "status": "acknowledged",
    },
]

df_regulatory = spark.createDataFrame(pd.DataFrame(regulatory_records))
df_regulatory.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.regulatory_notifications")
print(f"Wrote {df_regulatory.count()} rows to {CATALOG}.gold.regulatory_notifications")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Regulatory Rules & Config

# COMMAND ----------

# DBTITLE 1,Generate & Write Regulatory Rules

# Centralises all hardcoded business rules so the app can read them from a table
# instead of embedding magic numbers in frontend code.

regulatory_rules = [
    {
        "rule_id": "OFWAT_PENALTY_RATE",
        "category": "penalty",
        "rule_name": "Ofwat Supply Interruption Penalty Rate",
        "description": "Penalty per property per hour for supply interruptions exceeding the grace period",
        "value_numeric": 580.0,
        "value_text": None,
        "unit": "GBP/property/hour",
        "effective_from": "2025-04-01",
        "effective_to": None,
        "source": "Ofwat PR24 Final Determination",
    },
    {
        "rule_id": "OFWAT_GRACE_PERIOD",
        "category": "penalty",
        "rule_name": "Ofwat Significant Interruption Threshold",
        "description": "Hours before supply interruption is classified as significant and penalties apply",
        "value_numeric": 3.0,
        "value_text": None,
        "unit": "hours",
        "effective_from": "2025-04-01",
        "effective_to": None,
        "source": "Ofwat PR24 Final Determination",
    },
    {
        "rule_id": "DWI_VERBAL_DEADLINE",
        "category": "regulatory_deadline",
        "rule_name": "DWI Verbal Notification Deadline",
        "description": "Hours after incident start by which DWI must be verbally notified",
        "value_numeric": 1.0,
        "value_text": None,
        "unit": "hours",
        "effective_from": "2020-01-01",
        "effective_to": None,
        "source": "DWI Regulation 35 Reporting Requirements",
    },
    {
        "rule_id": "DWI_WRITTEN_DEADLINE",
        "category": "regulatory_deadline",
        "rule_name": "DWI Written Report Deadline",
        "description": "Hours after incident start by which a formal written report must be submitted to DWI",
        "value_numeric": 24.0,
        "value_text": None,
        "unit": "hours",
        "effective_from": "2020-01-01",
        "effective_to": None,
        "source": "DWI Regulation 35 Reporting Requirements",
    },
    {
        "rule_id": "OFWAT_ESCALATION_THRESHOLD",
        "category": "regulatory_deadline",
        "rule_name": "Ofwat Escalation Threshold",
        "description": "Hours after incident start triggering the second Ofwat reporting threshold",
        "value_numeric": 12.0,
        "value_text": None,
        "unit": "hours",
        "effective_from": "2025-04-01",
        "effective_to": None,
        "source": "Ofwat PR24 Final Determination",
    },
    {
        "rule_id": "PRESSURE_RED_THRESHOLD",
        "category": "threshold",
        "rule_name": "Pressure RED Threshold",
        "description": "Pressure (metres head) below which a sensor is flagged RED",
        "value_numeric": 15.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Internal SOP-WN-001",
    },
    {
        "rule_id": "PRESSURE_AMBER_THRESHOLD",
        "category": "threshold",
        "rule_name": "Pressure AMBER Threshold",
        "description": "Pressure (metres head) below which a sensor is flagged AMBER",
        "value_numeric": 25.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Internal SOP-WN-001",
    },
    {
        "rule_id": "DEFAULT_BASE_PRESSURE",
        "category": "threshold",
        "rule_name": "Default Base Pressure Fallback",
        "description": "Fallback base pressure (metres head) when dim_properties.base_pressure is NULL",
        "value_numeric": 25.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Engineering estimate",
    },
    {
        "rule_id": "IMPACT_HIGH_THRESHOLD",
        "category": "threshold",
        "rule_name": "High Impact Effective Pressure Threshold",
        "description": "Effective pressure at or below which a property is classified as HIGH impact (no water)",
        "value_numeric": 0.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Internal SOP-WN-001",
    },
    {
        "rule_id": "IMPACT_MEDIUM_THRESHOLD",
        "category": "threshold",
        "rule_name": "Medium Impact Effective Pressure Threshold",
        "description": "Effective pressure at or below which a property is classified as MEDIUM impact (very low pressure)",
        "value_numeric": 5.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Internal SOP-WN-001",
    },
    {
        "rule_id": "IMPACT_LOW_THRESHOLD",
        "category": "threshold",
        "rule_name": "Low Impact Effective Pressure Threshold",
        "description": "Effective pressure at or below which a property is classified as LOW impact (reduced pressure)",
        "value_numeric": 10.0,
        "value_text": None,
        "unit": "m",
        "effective_from": "2024-01-01",
        "effective_to": None,
        "source": "Internal SOP-WN-001",
    },
    {
        "rule_id": "CMEX_GREEN_THRESHOLD",
        "category": "kpi",
        "rule_name": "C-MeX Proactive Comms Rate — Green Threshold",
        "description": "Proactive comms rate percentage at or above which the indicator is GREEN",
        "value_numeric": 70.0,
        "value_text": None,
        "unit": "percent",
        "effective_from": "2025-04-01",
        "effective_to": None,
        "source": "Internal KPI Target — PR24 aligned",
    },
    {
        "rule_id": "CMEX_AMBER_THRESHOLD",
        "category": "kpi",
        "rule_name": "C-MeX Proactive Comms Rate — Amber Threshold",
        "description": "Proactive comms rate percentage at or above which the indicator is AMBER (below this = RED)",
        "value_numeric": 40.0,
        "value_text": None,
        "unit": "percent",
        "effective_from": "2025-04-01",
        "effective_to": None,
        "source": "Internal KPI Target — PR24 aligned",
    },
]

# Ensure all-NULL columns are typed as string (not void) so Spark/Lakebase sync works
rules_df = pd.DataFrame(regulatory_rules)
rules_df["effective_to"] = pd.array([None] * len(rules_df), dtype=pd.StringDtype())
rules_df["value_text"] = pd.array([None] * len(rules_df), dtype=pd.StringDtype())
df_rules = spark.createDataFrame(rules_df)
df_rules.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.gold.dim_regulatory_rules")
print(f"Wrote {df_rules.count()} regulatory rules to {CATALOG}.gold.dim_regulatory_rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Metadata — PK/FK Constraints & Comments
# MAGIC
# MAGIC Apply table/column comments and PK/FK constraints for Genie Space accuracy.

# COMMAND ----------

# DBTITLE 1,PK/FK Constraints (idempotent)
constraint_statements = [
    # Primary keys
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ADD CONSTRAINT pk_dim_incidents PRIMARY KEY (incident_id) NOT ENFORCED",
    f"ALTER TABLE {CATALOG}.gold.incident_notifications ADD CONSTRAINT pk_incident_notifications PRIMARY KEY (incident_id) NOT ENFORCED",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ADD CONSTRAINT pk_regulatory_notifications PRIMARY KEY (notification_id) NOT ENFORCED",
    # Foreign keys
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ADD CONSTRAINT fk_incident_dma FOREIGN KEY (dma_code) REFERENCES {CATALOG}.silver.dim_dma(dma_code) NOT ENFORCED",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ADD CONSTRAINT fk_incident_asset FOREIGN KEY (root_cause_asset_id) REFERENCES {CATALOG}.silver.dim_assets(asset_id) NOT ENFORCED",
    f"ALTER TABLE {CATALOG}.gold.incident_notifications ADD CONSTRAINT fk_notif_incident FOREIGN KEY (incident_id) REFERENCES {CATALOG}.gold.dim_incidents(incident_id) NOT ENFORCED",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ADD CONSTRAINT fk_reg_incident FOREIGN KEY (incident_id) REFERENCES {CATALOG}.gold.dim_incidents(incident_id) NOT ENFORCED",
]

for sql in constraint_statements:
    try:
        spark.sql(sql)
        print(f"OK:   {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]}")
    except Exception as e:
        if "already exists" in str(e).lower() or "CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"SKIP: {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} (already exists)")
        else:
            print(f"ERR:  {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} — {e}")

# COMMAND ----------

# DBTITLE 1,Table & Column Comments — dim_incidents
spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dim_incidents IS
  'Active and historical incident records. Each incident has a root cause asset, affected DMA, severity, property impact count, and timeline (start_timestamp / end_timestamp). Use total_properties_affected for the count of impacted properties. Use status = active to filter to ongoing incidents.'""")

comment_stmts = [
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN incident_id COMMENT 'Unique incident identifier (format: INC-YYYY-MMDD-NNN). Primary key.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN dma_code COMMENT 'Primary affected DMA code. Foreign key to dim_dma.dma_code.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN root_cause_asset_id COMMENT 'Asset that caused the incident. Foreign key to dim_assets.asset_id.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN root_cause_asset_type COMMENT 'Type of the root cause asset: pump_station, trunk_main, prv, etc.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN incident_type COMMENT 'Incident classification: pump_station_trip, burst_main, prv_failure, etc.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN title COMMENT 'Short descriptive title of the incident.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN description COMMENT 'Detailed narrative description of the incident, impact, and response.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN start_timestamp COMMENT 'Timestamp when the incident started (UTC). Use this column — not detected_ts — for duration and penalty calculations.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN end_timestamp COMMENT 'Timestamp when the incident was resolved (UTC). NULL for active incidents — use COALESCE(end_timestamp, CURRENT_TIMESTAMP()) for duration calculations.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN status COMMENT 'Incident lifecycle status: active (ongoing) or resolved (closed). Filter on active to see current incidents.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN severity COMMENT 'Incident severity classification: low, medium, high, or critical.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN total_properties_affected COMMENT 'Count of properties impacted by this incident. This is an integer count (not a percentage). Use SUM(total_properties_affected) for aggregate queries across incidents.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN sensitive_premises_affected COMMENT 'Boolean: TRUE if the incident affects any sensitive premises (schools, hospitals, care homes, dialysis-dependent homes).'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN priority_score COMMENT 'Computed priority score (0-100) reflecting severity, property count, sensitive premises, and reservoir risk.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN escalation_risk COMMENT 'Narrative assessment of escalation risk and recommended actions.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN trajectory COMMENT 'Current trajectory of the incident: improving, stable, or deteriorating.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN first_complaint_time COMMENT 'Timestamp of the first customer complaint related to this incident (UTC). NULL if no complaints received.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN assigned_team COMMENT 'Operations team assigned to manage the incident response.'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN sop_reference COMMENT 'Standard Operating Procedure reference for the incident type (e.g. SOP-WN-042).'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN created_at COMMENT 'Timestamp when the incident record was created in the system (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.dim_incidents ALTER COLUMN updated_at COMMENT 'Timestamp of the most recent update to the incident record (UTC).'",
]
for sql in comment_stmts:
    spark.sql(sql)
print("dim_incidents: table + column comments applied")

# COMMAND ----------

# DBTITLE 1,Table & Column Comments — incident_notifications
spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.incident_notifications IS
  'Per-incident summary of customer notification metrics. proactive_notifications = customers contacted before they called in; reactive_complaints = customer-initiated contacts. Used for C-MeX proactive notification rate calculations.'""")

for sql in [
    f"ALTER TABLE {CATALOG}.gold.incident_notifications ALTER COLUMN incident_id COMMENT 'Incident identifier. Primary key and foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.incident_notifications ALTER COLUMN proactive_notifications COMMENT 'Number of customers proactively notified by the company BEFORE they contacted the company. Proactive notifications positively impact C-MeX scores.'",
    f"ALTER TABLE {CATALOG}.gold.incident_notifications ALTER COLUMN reactive_complaints COMMENT 'Number of customer-initiated contacts (complaints) received about this incident. These are reactive — the customer called before the company notified them.'",
]:
    spark.sql(sql)
print("incident_notifications: table + column comments applied")

# COMMAND ----------

# DBTITLE 1,Table & Column Comments — regulatory_notifications
spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.regulatory_notifications IS
  'Regulatory notification tracking for DWI (Drinking Water Inspectorate) and Ofwat. Records the timestamp each regulator was formally notified and the notification reference. Use dwi_notified_ts to measure time-to-notification from incident start.'""")

for sql in [
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN notification_id COMMENT 'Unique regulatory notification identifier (format: REG-YYYY-MMDD-NNN). Primary key.'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN regulator COMMENT 'Regulatory body notified: DWI (Drinking Water Inspectorate) or Ofwat.'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN dwi_notified_ts COMMENT 'Timestamp when DWI was formally notified of the incident (UTC). DWI should be notified within 1 hour for incidents affecting sensitive premises.'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN ofwat_notified_ts COMMENT 'Timestamp when Ofwat was formally notified (UTC). NULL if not required or not yet sent.'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN notification_ref COMMENT 'External reference number for the regulatory notification (e.g. DWI-NOT-2026-0407-001).'",
    f"ALTER TABLE {CATALOG}.gold.regulatory_notifications ALTER COLUMN status COMMENT 'Notification status: acknowledged (regulator confirmed receipt) or pending.'",
]:
    spark.sql(sql)
print("regulatory_notifications: table + column comments applied")

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("=== Incident & Operational Data Generation Complete ===")
for table in [
    "gold.dim_incidents",
    "gold.incident_events",
    "gold.communications_log",
    "gold.dim_response_playbooks",
    "gold.shift_handovers",
    "gold.incident_outstanding_actions",
    "gold.comms_requests",
    "gold.incident_notifications",
    "gold.regulatory_notifications",
    "gold.dim_regulatory_rules",
]:
    count = spark.table(f"{CATALOG}.{table}").count()
    print(f"  {table}: {count} rows")

display(spark.sql(f"SELECT incident_id, status, severity, total_properties_affected FROM {CATALOG}.gold.dim_incidents"))
display(spark.sql(f"SELECT event_id, event_type, event_timestamp, actor FROM {CATALOG}.gold.incident_events ORDER BY event_timestamp"))
