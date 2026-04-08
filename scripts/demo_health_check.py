# Databricks notebook source

# MAGIC %md
# MAGIC # Water Digital Twin -- Demo Health Check
# MAGIC
# MAGIC Runs **22 verification queries** against `water_digital_twin` to confirm the demo environment
# MAGIC is correctly seeded and ready for a live walkthrough.
# MAGIC
# MAGIC **Usage:** Run all cells. The final cell prints a summary with pass/fail for each check.

# COMMAND ----------

CATALOG = "water_digital_twin"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class HealthCheck:
    """Single health-check definition."""
    id: int
    name: str
    query: str
    validate: str  # Python expression evaluated against `result` (first row as dict)
    expect_description: str


checks: list[HealthCheck] = [
    # ------------------------------------------------------------------
    # 1. DEMO_DMA_01 exists and is RED
    # ------------------------------------------------------------------
    HealthCheck(
        id=1,
        name="DEMO_DMA_01 is RED",
        query="""
            SELECT rag_status
            FROM gold.dma_status
            WHERE dma_code = 'DEMO_DMA_01'
        """,
        validate="result['rag_status'] == 'RED'",
        expect_description="rag_status = 'RED'",
    ),
    # ------------------------------------------------------------------
    # 2. Property type distribution in DEMO_DMA_01
    # ------------------------------------------------------------------
    HealthCheck(
        id=2,
        name="Property type distribution (DEMO_DMA_01)",
        query="""
            SELECT
                SUM(CASE WHEN property_type = 'domestic' THEN 1 ELSE 0 END) AS domestic,
                SUM(CASE WHEN property_type = 'school' THEN 1 ELSE 0 END) AS school,
                SUM(CASE WHEN property_type = 'hospital' THEN 1 ELSE 0 END) AS hospital,
                SUM(CASE WHEN property_type = 'dialysis_home' THEN 1 ELSE 0 END) AS dialysis_home
            FROM gold.dim_property
            WHERE dma_code = 'DEMO_DMA_01'
        """,
        validate=(
            "result['domestic'] >= 750 and result['school'] >= 2 "
            "and result['hospital'] >= 1 and result['dialysis_home'] >= 3"
        ),
        expect_description=">=750 domestic, >=2 school, >=1 hospital, >=3 dialysis_home",
    ),
    # ------------------------------------------------------------------
    # 3. Pressure drop after 02:03
    # ------------------------------------------------------------------
    HealthCheck(
        id=3,
        name="Pressure drop visible (DEMO_SENSOR_01)",
        query="""
            SELECT MIN(value) AS min_pressure
            FROM gold.fact_telemetry
            WHERE sensor_id = 'DEMO_SENSOR_01'
              AND event_timestamp > '2026-04-07 02:03:00'
        """,
        validate="result['min_pressure'] is not None and result['min_pressure'] < 15",
        expect_description="min pressure < 15 after 02:03",
    ),
    # ------------------------------------------------------------------
    # 4. Flow drop after 02:03
    # ------------------------------------------------------------------
    HealthCheck(
        id=4,
        name="Flow drop visible (DEMO_FLOW_01)",
        query="""
            SELECT MIN(flow_rate) AS min_flow
            FROM gold.fact_telemetry
            WHERE sensor_id = 'DEMO_FLOW_01'
              AND event_timestamp > '2026-04-07 02:03:00'
        """,
        validate="result['min_flow'] is not None and result['min_flow'] < 20",
        expect_description="min flow_rate < 20 after 02:03",
    ),
    # ------------------------------------------------------------------
    # 5. Complaints lag pressure
    # ------------------------------------------------------------------
    HealthCheck(
        id=5,
        name="Complaints lag pressure (>= 30 after 03:00)",
        query="""
            SELECT
                MIN(complaint_timestamp) AS first_complaint,
                COUNT(*) AS complaint_count
            FROM gold.fact_complaint
            WHERE dma_code = 'DEMO_DMA_01'
              AND complaint_timestamp > '2026-04-07 03:00:00'
        """,
        validate=(
            "result['first_complaint'] is not None "
            "and str(result['first_complaint']) > '2026-04-07 03:00:00' "
            "and result['complaint_count'] >= 30"
        ),
        expect_description="first complaint after 03:00, count >= 30",
    ),
    # ------------------------------------------------------------------
    # 6. Elevation coherence
    # ------------------------------------------------------------------
    HealthCheck(
        id=6,
        name="Elevation coherence (avg > 35m for complainants)",
        query="""
            SELECT AVG(p.customer_height) AS avg_height
            FROM gold.fact_complaint c
            JOIN gold.dim_property p ON c.property_id = p.property_id
            WHERE c.dma_code = 'DEMO_DMA_01'
        """,
        validate="result['avg_height'] is not None and result['avg_height'] > 35",
        expect_description="avg customer_height > 35m for complainants in DEMO_DMA_01",
    ),
    # ------------------------------------------------------------------
    # 7. Upstream cause: pump trip
    # ------------------------------------------------------------------
    HealthCheck(
        id=7,
        name="Upstream cause (DEMO_PUMP_01 tripped)",
        query="""
            SELECT status, trip_timestamp
            FROM gold.dim_asset
            WHERE asset_id = 'DEMO_PUMP_01'
        """,
        validate=(
            "result['status'] == 'tripped' "
            "and str(result['trip_timestamp']) == '2026-04-07 02:03:00'"
        ),
        expect_description="status='tripped', trip_timestamp='2026-04-07 02:03:00'",
    ),
    # ------------------------------------------------------------------
    # 8. Pump feeds 3 DMAs
    # ------------------------------------------------------------------
    HealthCheck(
        id=8,
        name="Pump feeds 3 DMAs",
        query="""
            SELECT COUNT(*) AS cnt
            FROM gold.dim_asset_dma_feed
            WHERE asset_id = 'DEMO_PUMP_01'
        """,
        validate="result['cnt'] == 3",
        expect_description="3 rows in dim_asset_dma_feed for DEMO_PUMP_01",
    ),
    # ------------------------------------------------------------------
    # 9. Trunk main LINESTRING
    # ------------------------------------------------------------------
    HealthCheck(
        id=9,
        name="Trunk main geometry (DEMO_TM_001)",
        query="""
            SELECT geometry_wkt
            FROM gold.dim_asset
            WHERE asset_id = 'DEMO_TM_001'
        """,
        validate=(
            "result['geometry_wkt'] is not None "
            "and str(result['geometry_wkt']).startswith('LINESTRING(')"
        ),
        expect_description="geometry_wkt starts with 'LINESTRING('",
    ),
    # ------------------------------------------------------------------
    # 10. Isolation valves
    # ------------------------------------------------------------------
    HealthCheck(
        id=10,
        name="Isolation valves (DEMO_VALVE_01/02 open)",
        query="""
            SELECT asset_id, status
            FROM gold.dim_asset
            WHERE asset_id IN ('DEMO_VALVE_01', 'DEMO_VALVE_02')
            ORDER BY asset_id
        """,
        validate="result['status'] == 'open'",
        expect_description="2 valves, both status='open'",
    ),
    # ------------------------------------------------------------------
    # 11. Reservoir level
    # ------------------------------------------------------------------
    HealthCheck(
        id=11,
        name="Reservoir (DEMO_SR_01 at ~43%, capacity 5.0)",
        query="""
            SELECT level_pct, capacity_ml
            FROM gold.dim_asset
            WHERE asset_id = 'DEMO_SR_01'
        """,
        validate=(
            "result['level_pct'] is not None "
            "and abs(result['level_pct'] - 43) < 5 "
            "and result['capacity_ml'] == 5.0"
        ),
        expect_description="level_pct ~ 43%, capacity_ml = 5.0",
    ),
    # ------------------------------------------------------------------
    # 12. High anomaly after 02:03
    # ------------------------------------------------------------------
    HealthCheck(
        id=12,
        name="High anomaly (DEMO_SENSOR_01 sigma > 3.0)",
        query="""
            SELECT MAX(anomaly_sigma) AS max_sigma
            FROM gold.fact_anomaly_score
            WHERE sensor_id = 'DEMO_SENSOR_01'
              AND scored_at > '2026-04-07 02:03:00'
        """,
        validate="result['max_sigma'] is not None and result['max_sigma'] > 3.0",
        expect_description="anomaly_sigma > 3.0 after 02:03",
    ),
    # ------------------------------------------------------------------
    # 13. Amber DMAs low anomaly
    # ------------------------------------------------------------------
    HealthCheck(
        id=13,
        name="Amber DMAs low anomaly (DEMO_DMA_02 sigma < 2.0)",
        query="""
            SELECT MAX(a.anomaly_sigma) AS max_sigma
            FROM gold.fact_anomaly_score a
            JOIN gold.dim_sensor s ON a.sensor_id = s.sensor_id
            WHERE s.dma_code = 'DEMO_DMA_02'
        """,
        validate="result['max_sigma'] is not None and result['max_sigma'] < 2.0",
        expect_description="max anomaly sigma < 2.0 for DEMO_DMA_02 sensors",
    ),
    # ------------------------------------------------------------------
    # 14. RAG timeline
    # ------------------------------------------------------------------
    HealthCheck(
        id=14,
        name="RAG timeline (GREEN, AMBER, RED for DEMO_DMA_01)",
        query="""
            SELECT COLLECT_SET(rag_status) AS statuses
            FROM gold.fact_rag_timeline
            WHERE dma_code = 'DEMO_DMA_01'
        """,
        validate=(
            "result['statuses'] is not None "
            "and 'GREEN' in result['statuses'] "
            "and 'AMBER' in result['statuses'] "
            "and 'RED' in result['statuses']"
        ),
        expect_description="DEMO_DMA_01 has GREEN, AMBER, and RED entries",
    ),
    # ------------------------------------------------------------------
    # 15. Event log
    # ------------------------------------------------------------------
    HealthCheck(
        id=15,
        name="Event log (>= 10 events for INC-2026-0407-001)",
        query="""
            SELECT COUNT(*) AS cnt
            FROM gold.fact_event_log
            WHERE incident_id = 'INC-2026-0407-001'
        """,
        validate="result['cnt'] >= 10",
        expect_description=">= 10 events for INC-2026-0407-001",
    ),
    # ------------------------------------------------------------------
    # 16. Comms log
    # ------------------------------------------------------------------
    HealthCheck(
        id=16,
        name="Comms log (>= 4 records for INC-2026-0407-001)",
        query="""
            SELECT COUNT(*) AS cnt
            FROM gold.fact_comms_log
            WHERE incident_id = 'INC-2026-0407-001'
        """,
        validate="result['cnt'] >= 4",
        expect_description=">= 4 comms records for INC-2026-0407-001",
    ),
    # ------------------------------------------------------------------
    # 17. Playbook exists
    # ------------------------------------------------------------------
    HealthCheck(
        id=17,
        name="Playbook SOP-WN-042 exists",
        query="""
            SELECT COUNT(*) AS cnt
            FROM gold.dim_playbook
            WHERE sop_code = 'SOP-WN-042'
        """,
        validate="result['cnt'] >= 1",
        expect_description="SOP-WN-042 exists in dim_playbook",
    ),
    # ------------------------------------------------------------------
    # 18. Active incident
    # ------------------------------------------------------------------
    HealthCheck(
        id=18,
        name="Active incident (INC-2026-0407-001)",
        query="""
            SELECT status, severity, properties_affected
            FROM gold.fact_incident
            WHERE incident_id = 'INC-2026-0407-001'
        """,
        validate=(
            "result['status'] == 'active' "
            "and result['severity'] == 'high' "
            "and result['properties_affected'] == 441"
        ),
        expect_description="status='active', severity='high', properties_affected=441",
    ),
    # ------------------------------------------------------------------
    # 19. Historical incidents
    # ------------------------------------------------------------------
    HealthCheck(
        id=19,
        name="Historical incidents (>= 6 total, >= 5 resolved)",
        query="""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) AS resolved
            FROM gold.fact_incident
        """,
        validate="result['total'] >= 6 and result['resolved'] >= 5",
        expect_description="total >= 6, resolved >= 5",
    ),
    # ------------------------------------------------------------------
    # 20. DMA status distribution
    # ------------------------------------------------------------------
    HealthCheck(
        id=20,
        name="DMA status (500 DMAs: 1 RED, 2 AMBER, 497 GREEN)",
        query="""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN rag_status = 'RED' THEN 1 ELSE 0 END) AS red,
                SUM(CASE WHEN rag_status = 'AMBER' THEN 1 ELSE 0 END) AS amber,
                SUM(CASE WHEN rag_status = 'GREEN' THEN 1 ELSE 0 END) AS green
            FROM gold.dma_status
        """,
        validate=(
            "result['total'] == 500 "
            "and result['red'] == 1 "
            "and result['amber'] == 2 "
            "and result['green'] == 497"
        ),
        expect_description="500 total (1 RED, 2 AMBER, 497 GREEN)",
    ),
    # ------------------------------------------------------------------
    # 21. DMA summary for DEMO_DMA_01
    # ------------------------------------------------------------------
    HealthCheck(
        id=21,
        name="DMA summary (DEMO_DMA_01 reservoir ~43%, incident linked)",
        query="""
            SELECT reservoir_level_pct, active_incident_id
            FROM gold.dma_status
            WHERE dma_code = 'DEMO_DMA_01'
        """,
        validate=(
            "result['reservoir_level_pct'] is not None "
            "and abs(result['reservoir_level_pct'] - 43) < 5 "
            "and result['active_incident_id'] == 'INC-2026-0407-001'"
        ),
        expect_description="reservoir ~43%, active_incident_id = 'INC-2026-0407-001'",
    ),
    # ------------------------------------------------------------------
    # 22. Sensitive premises in DEMO_DMA_01
    # ------------------------------------------------------------------
    HealthCheck(
        id=22,
        name="Sensitive premises (DEMO_DMA_01 >= 6)",
        query="""
            SELECT COUNT(*) AS cnt
            FROM gold.dim_property
            WHERE dma_code = 'DEMO_DMA_01'
              AND property_type IN ('hospital', 'school', 'dialysis_home', 'care_home')
        """,
        validate="result['cnt'] >= 6",
        expect_description=">= 6 sensitive premises in DEMO_DMA_01",
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Checks

# COMMAND ----------

results = []

for check in checks:
    try:
        df = spark.sql(check.query)
        rows = df.collect()
        if len(rows) == 0:
            passed = False
            actual = "NO ROWS RETURNED"
        else:
            result = rows[0].asDict()
            passed = bool(eval(check.validate))
            actual = str(result)
    except Exception as e:
        passed = False
        actual = f"ERROR: {e}"

    results.append({
        "id": check.id,
        "name": check.name,
        "passed": passed,
        "expected": check.expect_description,
        "actual": actual,
        "query": check.query.strip(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

pass_count = sum(1 for r in results if r["passed"])
fail_count = len(results) - pass_count

html_parts = []
html_parts.append("<style>")
html_parts.append("""
    .hc-table { border-collapse: collapse; width: 100%; font-family: monospace; font-size: 13px; }
    .hc-table th { background: #1b1b2f; color: #fff; padding: 8px 12px; text-align: left; }
    .hc-table td { padding: 6px 12px; border-bottom: 1px solid #ddd; vertical-align: top; }
    .hc-pass { color: #2e7d32; font-weight: bold; }
    .hc-fail { color: #c62828; font-weight: bold; }
    .hc-query { color: #555; font-size: 11px; white-space: pre-wrap; }
    .hc-summary-pass { font-size: 20px; color: #2e7d32; font-weight: bold; padding: 16px 0; }
    .hc-summary-fail { font-size: 20px; color: #c62828; font-weight: bold; padding: 16px 0; }
""")
html_parts.append("</style>")

html_parts.append("<table class='hc-table'>")
html_parts.append("<tr><th>#</th><th>Status</th><th>Check</th><th>Expected</th><th>Actual</th></tr>")

for r in results:
    if r["passed"]:
        status_html = "<span class='hc-pass'>&#x2705; PASS</span>"
    else:
        status_html = "<span class='hc-fail'>&#x274C; FAIL</span>"

    html_parts.append(
        f"<tr>"
        f"<td>{r['id']}</td>"
        f"<td>{status_html}</td>"
        f"<td>{r['name']}<br/><span class='hc-query'>{r['query']}</span></td>"
        f"<td>{r['expected']}</td>"
        f"<td>{r['actual']}</td>"
        f"</tr>"
    )

html_parts.append("</table>")

if fail_count == 0:
    html_parts.append(f"<div class='hc-summary-pass'>&#x2705; {pass_count}/{len(results)} passed -- Demo is READY</div>")
else:
    html_parts.append(
        f"<div class='hc-summary-fail'>&#x274C; {pass_count}/{len(results)} passed -- "
        f"{fail_count} FAILED</div>"
    )

displayHTML("".join(html_parts))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plain-Text Summary (for logs)

# COMMAND ----------

print("=" * 60)
print("WATER DIGITAL TWIN -- HEALTH CHECK SUMMARY")
print("=" * 60)
for r in results:
    icon = "PASS" if r["passed"] else "FAIL"
    print(f"  [{icon}] {r['id']:>2}. {r['name']}")
    if not r["passed"]:
        print(f"         Expected: {r['expected']}")
        print(f"         Actual:   {r['actual']}")
print("=" * 60)
if fail_count == 0:
    print(f"RESULT: {pass_count}/{len(results)} passed -- Demo is READY")
else:
    print(f"RESULT: {pass_count}/{len(results)} passed -- {fail_count} FAILED")
print("=" * 60)
