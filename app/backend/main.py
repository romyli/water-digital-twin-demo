"""
Water Digital Twin Demo - FastAPI Backend
Serves data from Lakebase (PostgreSQL) via Databricks SDK Data API.
Company: Water Utilities (anonymized demo)
"""

import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lakebase Client
# ---------------------------------------------------------------------------

class LakebaseClient:
    """Wraps HTTP calls to Lakebase Data API with Databricks workspace auth."""

    def __init__(self):
        self._ws = WorkspaceClient()
        self._project = os.environ.get("LAKEBASE_PROJECT", "water-digital-twin-lakebase")
        self._base_url: Optional[str] = None
        self._headers: Optional[dict] = None

    def _ensure_connection(self):
        """Lazily resolve Lakebase endpoint and auth headers."""
        if self._base_url is not None:
            return
        host = self._ws.config.host.rstrip("/")
        self._base_url = f"{host}/api/2.0/lakebase/projects/{self._project}"
        token = self._ws.config.authenticate()
        self._headers = {
            "Authorization": f"Bearer {token.get('access_token', token.get('token', ''))}",
            "Content-Type": "application/json",
        }

    async def _refresh_auth(self):
        """Refresh auth headers if token may have expired."""
        token = self._ws.config.authenticate()
        tok_value = token if isinstance(token, str) else token.get("access_token", token.get("token", ""))
        self._headers = {
            "Authorization": f"Bearer {tok_value}",
            "Content-Type": "application/json",
        }

    async def execute(self, sql: str, params: Optional[list] = None) -> list[dict[str, Any]]:
        """Execute a SQL query against Lakebase and return rows as dicts."""
        self._ensure_connection()
        await self._refresh_auth()
        url = f"{self._base_url}/sql"
        body: dict[str, Any] = {"statement": sql}
        if params:
            body["parameters"] = params
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, json=body, headers=self._headers)
            if resp.status_code == 401:
                await self._refresh_auth()
                resp = await client.post(url, json=body, headers=self._headers)
            if resp.status_code != 200:
                logger.error("Lakebase query failed: %s %s", resp.status_code, resp.text)
                raise HTTPException(status_code=502, detail=f"Lakebase query error: {resp.status_code}")
            data = resp.json()
        columns = [col["name"] for col in data.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = data.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row)) for row in rows]

    async def execute_one(self, sql: str, params: Optional[list] = None) -> Optional[dict[str, Any]]:
        """Execute and return the first row, or None."""
        rows = await self.execute(sql, params)
        return rows[0] if rows else None


db = LakebaseClient()

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Water Utilities Digital Twin API",
    version="1.0.0",
    description="Backend API for Water Utilities digital twin demo on Databricks.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

class CommsEntry(BaseModel):
    channel: str
    recipient: Optional[str] = None
    message: str
    sent_by: Optional[str] = "operator"

class PlaybookAction(BaseModel):
    action_id: str
    decision: str  # Accept, Defer, Not Applicable
    note: Optional[str] = None

class CommsRequest(BaseModel):
    channel: str
    message_template: str
    target_audience: Optional[str] = None

# ---------------------------------------------------------------------------
# 1. Health
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {"status": "ok", "service": "water-digital-twin", "company": "Water Utilities"}

# ---------------------------------------------------------------------------
# 2. Incidents
# ---------------------------------------------------------------------------

@app.get("/api/incidents/active")
async def active_incidents():
    rows = await db.execute(
        "SELECT * FROM dim_incidents WHERE status = 'active' ORDER BY created_at DESC"
    )
    return {"incidents": rows}


@app.get("/api/incidents/{incident_id}")
async def incident_detail(incident_id: str):
    incident = await db.execute_one(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [incident_id]
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    events = await db.execute(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time DESC", [incident_id]
    )
    comms = await db.execute(
        "SELECT * FROM communications_log WHERE incident_id = $1 ORDER BY sent_at DESC", [incident_id]
    )
    return {"incident": incident, "events": events, "communications": comms}


@app.get("/api/incidents/{incident_id}/events")
async def incident_events(incident_id: str):
    rows = await db.execute(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time DESC", [incident_id]
    )
    return {"events": rows}


@app.get("/api/incidents/{incident_id}/handover")
async def shift_handover(incident_id: str):
    incident = await db.execute_one(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [incident_id]
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")
    events = await db.execute(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time ASC", [incident_id]
    )
    comms = await db.execute(
        "SELECT * FROM communications_log WHERE incident_id = $1 ORDER BY sent_at DESC", [incident_id]
    )
    outstanding = await db.execute(
        "SELECT * FROM incident_events WHERE incident_id = $1 AND status = 'pending' ORDER BY event_time ASC",
        [incident_id],
    )
    return {
        "incident": incident,
        "actions_taken": [e for e in events if e.get("status") == "completed"],
        "outstanding_actions": outstanding,
        "communications": comms,
    }

# ---------------------------------------------------------------------------
# 3. DMA
# ---------------------------------------------------------------------------

@app.get("/api/dma/status")
async def dma_status(rag_status: Optional[str] = Query(None)):
    sql = "SELECT * FROM dma_status"
    params = []
    if rag_status:
        sql += " WHERE rag_status = $1"
        params.append(rag_status)
    sql += " ORDER BY dma_code"
    rows = await db.execute(sql, params or None)
    return {"dma_statuses": rows}


@app.get("/api/dma/{dma_code}/polygons")
async def dma_polygons(dma_code: str):
    row = await db.execute_one(
        "SELECT dma_code, dma_name, ST_AsGeoJSON(geom) AS geojson FROM dim_dma WHERE dma_code = $1",
        [dma_code],
    )
    if not row:
        raise HTTPException(status_code=404, detail="DMA not found")
    geojson = json.loads(row["geojson"]) if isinstance(row.get("geojson"), str) else row.get("geojson")
    return {
        "type": "Feature",
        "properties": {"dma_code": row["dma_code"], "dma_name": row["dma_name"]},
        "geometry": geojson,
    }


@app.get("/api/dma/{dma_code}/detail")
async def dma_detail(dma_code: str):
    row = await db.execute_one(
        "SELECT * FROM dma_summary WHERE dma_code = $1", [dma_code]
    )
    if not row:
        raise HTTPException(status_code=404, detail="DMA summary not found")
    return row


@app.get("/api/dma/{dma_code}/sensors")
async def dma_sensors(dma_code: str):
    rows = await db.execute(
        "SELECT * FROM dim_sensor WHERE dma_code = $1 ORDER BY sensor_id", [dma_code]
    )
    return {"sensors": rows}


@app.get("/api/dma/{dma_code}/properties")
async def dma_properties(dma_code: str):
    rows = await db.execute(
        "SELECT * FROM dim_properties WHERE dma_code = $1 ORDER BY property_id", [dma_code]
    )
    return {"properties": rows}


@app.get("/api/dma/{dma_code}/complaints")
async def dma_complaints(dma_code: str):
    rows = await db.execute(
        "SELECT * FROM customer_complaints WHERE dma_code = $1 ORDER BY reported_at DESC LIMIT 50",
        [dma_code],
    )
    return {"complaints": rows}


@app.get("/api/dma/{dma_code}/assets")
async def dma_assets(dma_code: str):
    rows = await db.execute(
        """
        SELECT a.* FROM dim_assets a
        JOIN dim_asset_dma_feed f ON a.asset_id = f.asset_id
        WHERE f.dma_code = $1
        ORDER BY a.asset_type, a.asset_id
        """,
        [dma_code],
    )
    return {"assets": rows}


@app.get("/api/dma/{dma_code}/rag-history")
async def dma_rag_history(
    dma_code: str,
    hours: int = Query(24, ge=1, le=168),
):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = await db.execute(
        "SELECT * FROM dma_rag_history WHERE dma_code = $1 AND recorded_at >= $2 ORDER BY recorded_at ASC",
        [dma_code, since],
    )
    return {"rag_history": rows}

# ---------------------------------------------------------------------------
# 4. Sensors & Telemetry
# ---------------------------------------------------------------------------

@app.get("/api/sensor/{sensor_id}/telemetry")
async def sensor_telemetry(sensor_id: str, hours: int = Query(24, ge=1, le=168)):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = await db.execute(
        "SELECT * FROM fact_telemetry WHERE sensor_id = $1 AND ts >= $2 ORDER BY ts ASC",
        [sensor_id, since],
    )
    return {"telemetry": rows}


@app.get("/api/sensor/{sensor_id}/anomalies")
async def sensor_anomalies(sensor_id: str):
    rows = await db.execute(
        "SELECT * FROM anomaly_scores WHERE sensor_id = $1 ORDER BY scored_at DESC LIMIT 100",
        [sensor_id],
    )
    return {"anomalies": rows}

# ---------------------------------------------------------------------------
# 5. Map / GeoJSON
# ---------------------------------------------------------------------------

@app.get("/api/map/geojson")
async def map_geojson():
    rows = await db.execute(
        """
        SELECT d.dma_code, d.dma_name, ST_AsGeoJSON(d.geom) AS geojson,
               s.rag_status, s.avg_pressure, s.anomaly_confidence
        FROM dim_dma d
        LEFT JOIN dma_status s ON d.dma_code = s.dma_code
        ORDER BY d.dma_code
        """
    )
    features = []
    for r in rows:
        geom = json.loads(r["geojson"]) if isinstance(r.get("geojson"), str) else r.get("geojson")
        features.append({
            "type": "Feature",
            "properties": {
                "dma_code": r["dma_code"],
                "dma_name": r.get("dma_name"),
                "rag_status": r.get("rag_status", "GREEN"),
                "avg_pressure": r.get("avg_pressure"),
                "anomaly_confidence": r.get("anomaly_confidence"),
            },
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}


@app.get("/api/map/assets/{dma_code}")
async def map_assets(dma_code: str):
    rows = await db.execute(
        """
        SELECT a.asset_id, a.asset_type, a.asset_name,
               ST_AsGeoJSON(a.geom) AS geojson
        FROM dim_assets a
        JOIN dim_asset_dma_feed f ON a.asset_id = f.asset_id
        WHERE f.dma_code = $1
        """,
        [dma_code],
    )
    features = []
    for r in rows:
        geom = json.loads(r["geojson"]) if isinstance(r.get("geojson"), str) else r.get("geojson")
        features.append({
            "type": "Feature",
            "properties": {
                "asset_id": r["asset_id"],
                "asset_type": r.get("asset_type"),
                "asset_name": r.get("asset_name"),
            },
            "geometry": geom,
        })
    return {"type": "FeatureCollection", "features": features}

# ---------------------------------------------------------------------------
# 6. Playbooks
# ---------------------------------------------------------------------------

@app.get("/api/playbooks/{incident_type}")
async def playbook(incident_type: str):
    rows = await db.execute(
        "SELECT * FROM dim_response_playbooks WHERE incident_type = $1 ORDER BY step_order ASC",
        [incident_type],
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Playbook not found")
    return {"incident_type": incident_type, "steps": rows}

# ---------------------------------------------------------------------------
# 7. Reservoirs
# ---------------------------------------------------------------------------

@app.get("/api/reservoirs/{dma_code}")
async def reservoirs(dma_code: str):
    rows = await db.execute(
        """
        SELECT r.* FROM dim_reservoirs r
        JOIN dim_asset_dma_feed f ON r.reservoir_id = f.asset_id
        WHERE f.dma_code = $1
        """,
        [dma_code],
    )
    return {"reservoirs": rows}

# ---------------------------------------------------------------------------
# 8. Communications
# ---------------------------------------------------------------------------

@app.get("/api/comms/{incident_id}")
async def get_comms(incident_id: str):
    rows = await db.execute(
        "SELECT * FROM communications_log WHERE incident_id = $1 ORDER BY sent_at DESC",
        [incident_id],
    )
    return {"communications": rows}


@app.post("/api/comms/{incident_id}")
async def add_comms(incident_id: str, entry: CommsEntry):
    await db.execute(
        """
        INSERT INTO communications_log (incident_id, channel, recipient, message, sent_by, sent_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        [incident_id, entry.channel, entry.recipient, entry.message, entry.sent_by,
         datetime.now(timezone.utc).isoformat()],
    )
    return {"status": "created"}

# ---------------------------------------------------------------------------
# 9. Playbook Actions (write-back)
# ---------------------------------------------------------------------------

@app.post("/api/playbook-actions/{incident_id}")
async def save_playbook_actions(incident_id: str, actions: list[PlaybookAction]):
    for action in actions:
        await db.execute(
            """
            INSERT INTO playbook_action_log (incident_id, action_id, decision, note, decided_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            [incident_id, action.action_id, action.decision, action.note,
             datetime.now(timezone.utc).isoformat()],
        )
    return {"status": "saved", "count": len(actions)}

# ---------------------------------------------------------------------------
# 10. Proactive Comms Request
# ---------------------------------------------------------------------------

@app.post("/api/comms-request/{incident_id}")
async def create_comms_request(incident_id: str, req: CommsRequest):
    await db.execute(
        """
        INSERT INTO comms_requests (incident_id, channel, message_template, target_audience, created_at)
        VALUES ($1, $2, $3, $4, $5)
        """,
        [incident_id, req.channel, req.message_template, req.target_audience,
         datetime.now(timezone.utc).isoformat()],
    )
    return {"status": "created"}

# ---------------------------------------------------------------------------
# 11. PDF Report Generation
# ---------------------------------------------------------------------------

@app.post("/api/incidents/{incident_id}/report/pdf")
async def generate_pdf_report(incident_id: str):
    incident = await db.execute_one(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [incident_id]
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    events = await db.execute(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time ASC", [incident_id]
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=20 * mm, bottomMargin=20 * mm)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title2", parent=styles["Title"], fontSize=16, spaceAfter=12)
    normal = styles["Normal"]

    story = []
    story.append(Paragraph("Water Utilities — Regulatory Incident Report", title_style))
    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph(f"<b>Incident ID:</b> {incident.get('incident_id', 'N/A')}", normal))
    story.append(Paragraph(f"<b>Type:</b> {incident.get('incident_type', 'N/A')}", normal))
    story.append(Paragraph(f"<b>Status:</b> {incident.get('status', 'N/A')}", normal))
    story.append(Paragraph(f"<b>Created:</b> {incident.get('created_at', 'N/A')}", normal))
    story.append(Paragraph(f"<b>Description:</b> {incident.get('description', 'N/A')}", normal))
    story.append(Spacer(1, 6 * mm))

    story.append(Paragraph("<b>Event Timeline</b>", styles["Heading2"]))
    if events:
        table_data = [["Time", "Type", "Description", "Status"]]
        for e in events:
            table_data.append([
                str(e.get("event_time", "")),
                str(e.get("event_type", "")),
                str(e.get("description", "")),
                str(e.get("status", "")),
            ])
        t = Table(table_data, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#003366")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ]))
        story.append(t)
    else:
        story.append(Paragraph("No events recorded.", normal))

    story.append(Spacer(1, 6 * mm))
    story.append(Paragraph(
        f"<i>Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} — Water Utilities</i>",
        normal,
    ))

    doc.build(story)
    buf.seek(0)
    filename = f"incident_{incident_id}_report.pdf"
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

# ---------------------------------------------------------------------------
# 12. Regulatory
# ---------------------------------------------------------------------------

@app.get("/api/regulatory/{incident_id}")
async def regulatory_data(incident_id: str):
    incident = await db.execute_one(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [incident_id]
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    properties = await db.execute(
        """
        SELECT p.property_type, COUNT(*) as count
        FROM dim_properties p
        JOIN dim_incidents i ON p.dma_code = i.dma_code
        WHERE i.incident_id = $1
        GROUP BY p.property_type
        """,
        [incident_id],
    )

    total_properties = sum(int(p.get("count", 0)) for p in properties)
    created_at = incident.get("created_at", datetime.now(timezone.utc).isoformat())
    hours_elapsed = 0.0
    try:
        if isinstance(created_at, str):
            from dateutil import parser as dtparser
            start = dtparser.parse(created_at)
        else:
            start = created_at
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        hours_elapsed = (datetime.now(timezone.utc) - start).total_seconds() / 3600
    except Exception:
        hours_elapsed = 0.0

    penalty_hours = max(0, hours_elapsed - 3)
    penalty = total_properties * penalty_hours * 580

    return {
        "incident_id": incident_id,
        "incident": incident,
        "affected_properties": properties,
        "total_properties": total_properties,
        "hours_elapsed": round(hours_elapsed, 2),
        "deadlines": {
            "dwi_verbal": {"label": "DWI Verbal Notification", "hours": 1, "status": "DONE" if hours_elapsed >= 1 else "PENDING"},
            "dwi_written": {"label": "DWI Written Report", "hours": 24, "status": "DONE" if hours_elapsed >= 24 else "DUE"},
            "ofwat_3h": {"label": "Ofwat 3h Threshold", "hours": 3, "status": "BREACHED" if hours_elapsed >= 3 else "OK"},
            "ofwat_12h": {"label": "Ofwat 12h Threshold", "hours": 12, "status": "BREACHED" if hours_elapsed >= 12 else "OK"},
        },
        "penalty_calculation": {
            "formula": "properties x (hours - 3) x GBP 580",
            "properties": total_properties,
            "penalty_hours": round(penalty_hours, 2),
            "rate_per_property_hour": 580,
            "estimated_penalty_gbp": round(penalty, 2),
        },
    }


# ---------------------------------------------------------------------------
# Serve React frontend (production build)
# ---------------------------------------------------------------------------

from fastapi.staticfiles import StaticFiles
import pathlib

frontend_build = pathlib.Path(__file__).resolve().parent.parent / "frontend" / "build"
if frontend_build.exists():
    app.mount("/", StaticFiles(directory=str(frontend_build), html=True), name="frontend")
