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

# COMMAND ----------

# DBTITLE 1,Configuration
import random
import json
from datetime import datetime, timedelta

CATALOG = "water_digital_twin"
SEED = 42
random.seed(SEED + 7000)

# Ensure gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

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

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, FloatType
)

incident_schema = StructType([
    StructField("incident_id", StringType(), False),
    StructField("dma_code", StringType(), False),
    StructField("root_cause_asset_id", StringType(), True),
    StructField("root_cause_asset_type", StringType(), True),
    StructField("incident_type", StringType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), False),
    StructField("start_timestamp", StringType(), False),
    StructField("end_timestamp", StringType(), True),
    StructField("status", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("total_properties_affected", IntegerType(), False),
    StructField("sensitive_premises_affected", BooleanType(), False),
    StructField("priority_score", IntegerType(), False),
    StructField("assigned_team", StringType(), False),
    StructField("sop_reference", StringType(), True),
    StructField("created_at", StringType(), False),
    StructField("updated_at", StringType(), False),
])

incident_rows = [
    (
        i["incident_id"], i["dma_code"], i["root_cause_asset_id"],
        i["root_cause_asset_type"], i["incident_type"], i["title"],
        i["description"], i["start_timestamp"], i["end_timestamp"],
        i["status"], i["severity"], i["total_properties_affected"],
        i["sensitive_premises_affected"], i["priority_score"],
        i["assigned_team"], i["sop_reference"],
        i["created_at"], i["updated_at"],
    )
    for i in incident_records
]

df_incidents = spark.createDataFrame(incident_rows, schema=incident_schema)
(
    df_incidents.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dim_incidents")
)
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

event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("incident_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("description", StringType(), False),
    StructField("actor", StringType(), False),
    StructField("metadata", StringType(), True),
])

event_rows = [
    (e["event_id"], e["incident_id"], e["event_type"], e["event_timestamp"], e["description"], e["actor"], e["metadata"])
    for e in event_records
]

df_events = spark.createDataFrame(event_rows, schema=event_schema)
(
    df_events.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.incident_events")
)
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

comms_schema = StructType([
    StructField("comms_id", StringType(), False),
    StructField("incident_id", StringType(), False),
    StructField("comms_timestamp", StringType(), False),
    StructField("recipient_role", StringType(), False),
    StructField("recipient_name", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("direction", StringType(), False),
    StructField("summary", StringType(), False),
    StructField("status", StringType(), False),
])

comms_rows = [
    (c["comms_id"], c["incident_id"], c["comms_timestamp"], c["recipient_role"],
     c["recipient_name"], c["channel"], c["direction"], c["summary"], c["status"])
    for c in comms_records
]

df_comms = spark.createDataFrame(comms_rows, schema=comms_schema)
(
    df_comms.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.communications_log")
)
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

playbook_schema = StructType([
    StructField("playbook_id", StringType(), False),
    StructField("sop_reference", StringType(), False),
    StructField("incident_type", StringType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), False),
    StructField("action_steps", StringType(), False),
    StructField("total_steps", IntegerType(), False),
    StructField("max_sla_minutes", IntegerType(), False),
    StructField("version", StringType(), False),
    StructField("effective_date", StringType(), False),
    StructField("review_date", StringType(), False),
    StructField("owner", StringType(), False),
    StructField("created_at", StringType(), False),
])

playbook_rows = [
    (
        p["playbook_id"], p["sop_reference"], p["incident_type"], p["title"],
        p["description"], p["action_steps"], p["total_steps"],
        p["max_sla_minutes"], p["version"], p["effective_date"],
        p["review_date"], p["owner"], p["created_at"],
    )
    for p in playbook_records
]

df_playbooks = spark.createDataFrame(playbook_rows, schema=playbook_schema)
(
    df_playbooks.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.dim_response_playbooks")
)
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

handover_schema = StructType([
    StructField("handover_id", StringType(), False),
    StructField("incident_id", StringType(), False),
    StructField("outgoing_operator", StringType(), False),
    StructField("incoming_operator", StringType(), False),
    StructField("handover_timestamp", StringType(), False),
    StructField("signed_off_timestamp", StringType(), False),
    StructField("acknowledged_timestamp", StringType(), False),
    StructField("risk_of_escalation", StringType(), False),
    StructField("trajectory", StringType(), False),
    StructField("summary", StringType(), False),
    StructField("status", StringType(), False),
])

handover_rows = [
    (
        h["handover_id"], h["incident_id"], h["outgoing_operator"],
        h["incoming_operator"], h["handover_timestamp"],
        h["signed_off_timestamp"], h["acknowledged_timestamp"],
        h["risk_of_escalation"], h["trajectory"], h["summary"], h["status"],
    )
    for h in handover_records
]

df_handovers = spark.createDataFrame(handover_rows, schema=handover_schema)
(
    df_handovers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.shift_handovers")
)
print(f"Wrote {df_handovers.count()} rows to {CATALOG}.gold.shift_handovers")

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

comms_req_schema = StructType([
    StructField("request_id", StringType(), False),
    StructField("incident_id", StringType(), False),
    StructField("dma_code", StringType(), False),
    StructField("request_timestamp", StringType(), False),
    StructField("customer_count", IntegerType(), False),
    StructField("channels", StringType(), False),
    StructField("message_template", StringType(), False),
    StructField("message_summary", StringType(), False),
    StructField("status", StringType(), False),
    StructField("sent_timestamp", StringType(), True),
    StructField("delivery_rate_pct", FloatType(), True),
    StructField("requested_by", StringType(), False),
    StructField("approved_by", StringType(), True),
])

comms_req_rows = [
    (
        cr["request_id"], cr["incident_id"], cr["dma_code"],
        cr["request_timestamp"], cr["customer_count"], cr["channels"],
        cr["message_template"], cr["message_summary"], cr["status"],
        cr["sent_timestamp"], float(cr["delivery_rate_pct"]),
        cr["requested_by"], cr["approved_by"],
    )
    for cr in comms_request_records
]

df_comms_req = spark.createDataFrame(comms_req_rows, schema=comms_req_schema)
(
    df_comms_req.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.comms_requests")
)
print(f"Wrote {df_comms_req.count()} rows to {CATALOG}.gold.comms_requests")

# COMMAND ----------

# DBTITLE 1,Validation Summary
print("=== Incident & Operational Data Generation Complete ===")
for table in [
    "gold.dim_incidents",
    "gold.incident_events",
    "gold.communications_log",
    "gold.dim_response_playbooks",
    "gold.shift_handovers",
    "gold.comms_requests",
]:
    count = spark.table(f"{CATALOG}.{table}").count()
    print(f"  {table}: {count} rows")

display(spark.sql(f"SELECT incident_id, status, severity, total_properties_affected FROM {CATALOG}.gold.dim_incidents"))
display(spark.sql(f"SELECT event_id, event_type, event_timestamp, actor FROM {CATALOG}.gold.incident_events ORDER BY event_timestamp"))
