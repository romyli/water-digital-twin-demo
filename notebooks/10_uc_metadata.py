# Databricks notebook source

# MAGIC %md
# MAGIC # 10 — Unity Catalog Metadata for Genie Spaces
# MAGIC
# MAGIC Applies table descriptions, column descriptions, and PK/FK constraints
# MAGIC to gold-layer tables that are **not** managed by the SDP pipeline.
# MAGIC
# MAGIC SDP-managed tables (bronze, silver, and gold views in `06_sdp_pipeline.py`)
# MAGIC already have inline `schema=` and `comment=` metadata. This notebook covers:
# MAGIC
# MAGIC - **Notebook 05 tables** without comments: `incident_events`, `communications_log`,
# MAGIC   `dim_response_playbooks`, `shift_handovers`, `incident_outstanding_actions`,
# MAGIC   `comms_requests`, `dim_regulatory_rules`
# MAGIC - **Notebook 07 tables**: `anomaly_scores`, `dma_rag_history`, `dma_status`, `dma_summary`
# MAGIC - **Pre-joined executive view**: `vw_incident_executive`
# MAGIC
# MAGIC > **Note:** Notebook 05 already applies PK/FK + comments for `dim_incidents`,
# MAGIC > `incident_notifications`, and `regulatory_notifications` — those are not repeated here.
# MAGIC
# MAGIC **Run after:** Notebooks 05 and 07 (tables must exist).

# COMMAND ----------

CATALOG = "water_digital_twin"

def apply_comments(statements):
    """Run a list of SQL statements, printing status for each."""
    for sql in statements:
        try:
            spark.sql(sql)
        except Exception as e:
            print(f"ERR: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 05 Tables — Missing Comments

# COMMAND ----------

# MAGIC %md
# MAGIC ### incident_events

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.incident_events IS
  'Chronological event log for incidents. Each row is one event (alarm, acknowledgement, dispatch, comms, handover). Use event_type to filter by category. metadata column contains JSON with event-specific details. Grain: event_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN event_id COMMENT 'Unique event identifier (format: EVT-NNN).'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN incident_id COMMENT 'Parent incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN event_type COMMENT 'Event category: asset_trip, anomaly_detected, alert_acknowledged, rag_change, customer_complaint, crew_dispatched, regulatory_notification, customer_comms, shift_handover.'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN event_timestamp COMMENT 'Timestamp when the event occurred (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN description COMMENT 'Human-readable description of what happened.'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN actor COMMENT 'Who or what triggered the event: operator ID (e.g. OP_NIGHT_01), system (SCADA_SYSTEM, ANOMALY_ENGINE, RAG_ENGINE), or team (CONTACT_CENTRE, COMMS_TEAM).'",
    f"ALTER TABLE {CATALOG}.gold.incident_events ALTER COLUMN metadata COMMENT 'JSON object with event-specific details (alarm codes, sensor counts, crew IDs, etc.).'",
])
print("incident_events: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### communications_log

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.communications_log IS
  'Log of all communications sent during incident response. Tracks who was contacted (regulators, managers, comms teams), via which channel, and the outcome. Grain: comms_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN comms_id COMMENT 'Unique communication record identifier (format: COMM-NNN).'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN comms_timestamp COMMENT 'Timestamp when the communication was sent (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN recipient_role COMMENT 'Role of the recipient: Network Manager, DWI Duty Officer, Customer Comms Team, Executive On-Call.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN recipient_name COMMENT 'Name or title of the recipient.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN channel COMMENT 'Communication channel: phone, email, or sms.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN direction COMMENT 'Direction: outbound (company to external) or inbound.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN summary COMMENT 'Free-text summary of the communication content and outcome.'",
    f"ALTER TABLE {CATALOG}.gold.communications_log ALTER COLUMN status COMMENT 'Communication status: completed, pending, or failed.'",
])
print("communications_log: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_response_playbooks

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dim_response_playbooks IS
  'Standard Operating Procedure (SOP) playbooks for incident response. Each playbook has ordered action steps with SLA targets. action_steps column is a JSON array. Grain: playbook_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN playbook_id COMMENT 'Unique playbook identifier (format: PB-SOP-WN-NNN).'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN sop_reference COMMENT 'SOP reference code (e.g. SOP-WN-042). Matches dim_incidents.sop_reference.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN incident_type COMMENT 'Incident type this playbook addresses: pump_station_trip, burst_main, prv_failure, etc.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN title COMMENT 'Playbook title.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN description COMMENT 'Summary of what this playbook covers.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN action_steps COMMENT 'JSON array of ordered action steps. Each step has: step number, action, description, responsible role, and SLA in minutes.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN total_steps COMMENT 'Total number of action steps in the playbook.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN max_sla_minutes COMMENT 'Maximum SLA target in minutes across all steps.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN version COMMENT 'Playbook version number.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN effective_date COMMENT 'Date this version became effective (YYYY-MM-DD).'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN review_date COMMENT 'Next scheduled review date (YYYY-MM-DD).'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN owner COMMENT 'Role responsible for maintaining this playbook.'",
    f"ALTER TABLE {CATALOG}.gold.dim_response_playbooks ALTER COLUMN created_at COMMENT 'Record creation timestamp (UTC).'",
])
print("dim_response_playbooks: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### shift_handovers

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.shift_handovers IS
  'Shift handover records for incident management. Contains the handover summary, risk assessment, and acknowledgement status between outgoing and incoming operators. Grain: handover_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN handover_id COMMENT 'Unique handover identifier (format: HO-YYYY-MMDD-HHMM).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN outgoing_operator COMMENT 'Operator ID of the person handing over (e.g. OP_NIGHT_01).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN incoming_operator COMMENT 'Operator ID of the person receiving the handover (e.g. OP_DAY_01).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN handover_timestamp COMMENT 'Timestamp of the handover (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN signed_off_timestamp COMMENT 'Timestamp when the outgoing operator signed off (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN acknowledged_timestamp COMMENT 'Timestamp when the incoming operator acknowledged the handover (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN risk_of_escalation COMMENT 'Assessed escalation risk at handover time: low, medium, or high.'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN trajectory COMMENT 'Incident trajectory at handover: improving, stabilising, or deteriorating.'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN summary COMMENT 'Full handover briefing text covering incident status, actions taken, outstanding risks, and recommended next actions.'",
    f"ALTER TABLE {CATALOG}.gold.shift_handovers ALTER COLUMN status COMMENT 'Handover status: acknowledged (incoming confirmed receipt) or pending.'",
])
print("shift_handovers: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### incident_outstanding_actions

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.incident_outstanding_actions IS
  'Pending and in-progress tasks assigned during incident response. Each action has a priority, assignee, due time, and status. Use status = pending to find incomplete work. Grain: action_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN action_id COMMENT 'Unique action identifier (format: ACT-YYYY-MMDD-NNN).'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN action_description COMMENT 'What needs to be done.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN priority COMMENT 'Action priority: critical, high, medium, or low.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN assigned_to COMMENT 'Operator or team ID assigned to this action.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN assigned_role COMMENT 'Role description of the assignee (e.g. Day Shift Operator, Customer Communications).'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN due_by COMMENT 'Deadline for completing this action (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN status COMMENT 'Action status: pending, in_progress, or completed.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN created_at COMMENT 'Timestamp when the action was created (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN created_by COMMENT 'Operator ID who created this action.'",
    f"ALTER TABLE {CATALOG}.gold.incident_outstanding_actions ALTER COLUMN notes COMMENT 'Additional context, supplier contacts, or reference numbers.'",
])
print("incident_outstanding_actions: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### comms_requests

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.comms_requests IS
  'Customer communication requests triggered by incidents. Tracks how many customers were notified, via which channels, and the delivery success rate. Grain: request_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN request_id COMMENT 'Unique communication request identifier (format: CR-YYYY-MMDD-NNN).'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN dma_code COMMENT 'Target DMA for the communication. Foreign key to dim_dma.dma_code.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN request_timestamp COMMENT 'Timestamp when the communication request was raised (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN customer_count COMMENT 'Number of customers targeted by this communication.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN channels COMMENT 'JSON array of notification channels used: sms, email, phone.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN message_template COMMENT 'Template reference used for the customer message.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN message_summary COMMENT 'Summary of the message sent to customers.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN status COMMENT 'Request status: sent, pending, or failed.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN sent_timestamp COMMENT 'Timestamp when notifications were dispatched (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN delivery_rate_pct COMMENT 'Percentage of notifications successfully delivered (0-100).'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN requested_by COMMENT 'Operator ID who raised the request.'",
    f"ALTER TABLE {CATALOG}.gold.comms_requests ALTER COLUMN approved_by COMMENT 'Manager who approved the customer communication.'",
])
print("comms_requests: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_regulatory_rules

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dim_regulatory_rules IS
  'Centralised configuration of regulatory thresholds, penalty rates, and KPI targets. Avoids hardcoded values in application code. Categories: penalty, regulatory_deadline, threshold, kpi. Grain: rule_id.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN rule_id COMMENT 'Unique rule identifier (e.g. OFWAT_PENALTY_RATE, DWI_VERBAL_DEADLINE). Primary key.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN category COMMENT 'Rule category: penalty, regulatory_deadline, threshold, or kpi.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN rule_name COMMENT 'Human-readable rule name.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN description COMMENT 'Detailed description of the rule and when it applies.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN value_numeric COMMENT 'Numeric value of the rule (e.g. 580 for penalty rate, 3 for grace period hours).'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN value_text COMMENT 'Text value for non-numeric rules. NULL for numeric rules.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN unit COMMENT 'Unit of measurement: GBP/property/hour, hours, m, percent, etc.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN effective_from COMMENT 'Date from which this rule version is effective (YYYY-MM-DD).'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN effective_to COMMENT 'Date until which this rule version is effective (YYYY-MM-DD). NULL if currently active.'",
    f"ALTER TABLE {CATALOG}.gold.dim_regulatory_rules ALTER COLUMN source COMMENT 'Authoritative source for the rule (e.g. Ofwat PR24 Final Determination, DWI Regulation 35).'",
])
print("dim_regulatory_rules: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Notebook 07 Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### anomaly_scores

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.anomaly_scores IS
  'Per-sensor statistical anomaly scores computed from a rolling 7-day same-time-of-day baseline. anomaly_sigma measures standard deviations from the baseline. is_anomaly = TRUE when sigma > 2.5. Join to dim_sensor on sensor_id for DMA assignment and sensor type. Grain: sensor_id + timestamp.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN sensor_id COMMENT 'Sensor identifier. Foreign key to dim_sensor.sensor_id.'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN timestamp COMMENT 'Timestamp of the scored reading (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN anomaly_sigma COMMENT 'Standard deviations from the 7-day rolling baseline at the same time of day. Above 2.5 = flagged as anomaly. Above 3.0 = high-confidence anomaly.'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN baseline_value COMMENT 'Expected value from the 7-day historical baseline (same time of day, +/- 15 min). Units depend on sensor type: metres head (pressure) or litres per second (flow).'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN actual_value COMMENT 'Actual measured value at this timestamp. Units depend on sensor type: metres head (pressure) or litres per second (flow).'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN is_anomaly COMMENT 'Boolean: TRUE if anomaly_sigma exceeds the 2.5 threshold. Use for counting anomalies and calculating anomaly rates.'",
    f"ALTER TABLE {CATALOG}.gold.anomaly_scores ALTER COLUMN scoring_method COMMENT 'Algorithm used to compute the score. Currently: statistical (z-score based).'",
])
print("anomaly_scores: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dma_rag_history

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dma_rag_history IS
  'Historical RAG (Red/Amber/Green) status per DMA per 15-minute interval. RED: min_pressure < 15m (supply loss). AMBER: min_pressure < 25m (degraded). GREEN: normal. Use for: RAG timeline analysis, pressure trend correlation, incident timeline reconstruction. Grain: dma_code + timestamp.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.dma_rag_history ALTER COLUMN dma_code COMMENT 'District Metered Area identifier. Foreign key to dim_dma.dma_code.'",
    f"ALTER TABLE {CATALOG}.gold.dma_rag_history ALTER COLUMN timestamp COMMENT 'Timestamp of the 15-minute assessment window (UTC).'",
    f"ALTER TABLE {CATALOG}.gold.dma_rag_history ALTER COLUMN rag_status COMMENT 'Health classification for this window. RED: min_pressure < 15m (immediate attention). AMBER: min_pressure < 25m (monitoring required). GREEN: operating normally.'",
    f"ALTER TABLE {CATALOG}.gold.dma_rag_history ALTER COLUMN avg_pressure COMMENT 'Average pressure across all pressure sensors in the DMA (metres head).'",
    f"ALTER TABLE {CATALOG}.gold.dma_rag_history ALTER COLUMN min_pressure COMMENT 'Minimum pressure reading across all pressure sensors in the DMA (metres head). Drives the RAG classification.'",
])
print("dma_rag_history: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dma_status

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dma_status IS
  'Current RAG status per DMA — one row per DMA from the latest 15-minute window. Enriched with sensor counts, property counts, sensitive premises counts, and active incident flag. Use for: real-time DMA health overview, map colouring, dashboard KPIs. Grain: dma_code.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN dma_code COMMENT 'District Metered Area identifier. Foreign key to dim_dma.dma_code.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN rag_status COMMENT 'Current health classification. RED: min_pressure < 15m (supply loss). AMBER: min_pressure < 25m (degraded). GREEN: normal.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN avg_pressure COMMENT 'Current average pressure across all sensors in the DMA (metres head).'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN min_pressure COMMENT 'Current minimum pressure in the DMA (metres head).'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN sensor_count COMMENT 'Number of active sensors deployed in this DMA.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN property_count COMMENT 'Total number of properties (residential and non-residential) in the DMA.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN sensitive_premises_count COMMENT 'Number of sensitive premises (hospitals, schools, care homes, dialysis homes) in the DMA.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN has_active_incident COMMENT 'Boolean: TRUE if there is at least one active (ongoing) incident in this DMA.'",
    f"ALTER TABLE {CATALOG}.gold.dma_status ALTER COLUMN last_updated COMMENT 'Timestamp when this status record was last computed (UTC).'",
])
print("dma_status: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dma_summary

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.dma_summary IS
  'Pre-materialised DMA summary joining status, reservoir levels, incident links, flow rates, and complaint counts. Powers the DMA detail panel for sub-second rendering. One row per DMA. Grain: dma_code.'""")

apply_comments([
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN dma_code COMMENT 'District Metered Area identifier.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN dma_name COMMENT 'Human-readable DMA display name.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN rag_status COMMENT 'Current RAG status: RED, AMBER, or GREEN.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN avg_pressure COMMENT 'Current average pressure in metres head.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN avg_flow COMMENT 'Average flow rate across flow sensors in litres per second.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN property_count COMMENT 'Total properties in the DMA.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN sensor_count COMMENT 'Number of active sensors.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN sensitive_premises_count COMMENT 'Number of sensitive premises (hospitals, schools, care homes, dialysis homes).'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN feeding_reservoir_id COMMENT 'Primary feeding reservoir identifier.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN reservoir_level_pct COMMENT 'Current fill level of the primary feeding reservoir (0-100%).'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN reservoir_hours_remaining COMMENT 'Estimated hours of supply remaining from the primary reservoir at current demand.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN active_incident_id COMMENT 'Incident ID of the active incident in this DMA. NULL if no active incident.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN active_complaints_count COMMENT 'Number of customer complaints in the last 24 hours.'",
    f"ALTER TABLE {CATALOG}.gold.dma_summary ALTER COLUMN last_updated COMMENT 'Timestamp when this summary was last computed (UTC).'",
])
print("dma_summary: table + column comments applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pre-Joined Executive View

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_incident_executive
# MAGIC
# MAGIC Pre-joins incidents with DMA names, notification metrics, and regulatory timestamps.
# MAGIC Add this view to the executive Genie space in place of the 3 separate tables
# MAGIC (`dim_incidents`, `incident_notifications`, `regulatory_notifications`) to eliminate
# MAGIC join inference — every join Genie must infer is a potential failure point.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.gold.vw_incident_executive AS
SELECT
    i.incident_id,
    i.dma_code,
    d.dma_name,
    i.root_cause_asset_id,
    i.root_cause_asset_type,
    i.incident_type,
    i.title,
    i.description,
    i.start_timestamp,
    i.end_timestamp,
    i.status,
    i.severity,
    i.total_properties_affected,
    i.sensitive_premises_affected,
    i.priority_score,
    i.escalation_risk,
    i.trajectory,
    i.first_complaint_time,
    i.assigned_team,
    i.sop_reference,
    n.proactive_notifications,
    n.reactive_complaints,
    r.dwi_notified_ts,
    r.ofwat_notified_ts,
    r.notification_ref
FROM {CATALOG}.gold.dim_incidents i
LEFT JOIN {CATALOG}.silver.dim_dma d ON i.dma_code = d.dma_code
LEFT JOIN {CATALOG}.gold.incident_notifications n ON i.incident_id = n.incident_id
LEFT JOIN {CATALOG}.gold.regulatory_notifications r ON i.incident_id = r.incident_id
""")
print("Created gold.vw_incident_executive")

# COMMAND ----------

spark.sql(f"""COMMENT ON TABLE {CATALOG}.gold.vw_incident_executive IS
  'Pre-joined executive view of incidents with DMA names, C-MeX notification metrics (proactive vs reactive), and regulatory notification timestamps (DWI/Ofwat). Use this single view instead of joining dim_incidents + incident_notifications + regulatory_notifications. Grain: incident_id.'""")
print("vw_incident_executive: table comment applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Validation

# COMMAND ----------

# Verify table comments are set
for table in [
    "gold.incident_events",
    "gold.communications_log",
    "gold.dim_response_playbooks",
    "gold.shift_handovers",
    "gold.incident_outstanding_actions",
    "gold.comms_requests",
    "gold.dim_regulatory_rules",
    "gold.anomaly_scores",
    "gold.dma_rag_history",
    "gold.dma_status",
    "gold.dma_summary",
    "gold.vw_incident_executive",
]:
    comment = spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{table}").filter("col_name = 'Comment'").collect()
    has_comment = len(comment) > 0 and comment[0]["data_type"] not in (None, "")
    status = "OK" if has_comment else "MISSING"
    print(f"  {status}: {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Follow the Genie Space guides to configure entity matching, column synonyms,
# MAGIC column hiding, and join definitions:
# MAGIC - [03 — Genie Operator Guide](../guides/03_genie_operator_guide.py)
# MAGIC - [04 — Genie Executive Guide](../guides/04_genie_executive_guide.py)
