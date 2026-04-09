/**
 * Water Digital Twin Demo — AppKit Backend
 *
 * Plugins:
 *   server   — Express HTTP server + frontend serving
 *   lakebase — PostgreSQL connection to Lakebase (auto OAuth refresh)
 *   genie    — Network Operations Genie Space integration
 */

import { createApp, server, lakebase, genie } from "@databricks/appkit";

async function setupRoutes(appkit: any) {
  async function query(sql: string, params?: any[]): Promise<any[]> {
    try {
      const result = await appkit.lakebase.query(sql, params);
      return result.rows ?? result;
    } catch (err: any) {
      console.error("[lakebase query error]", err.message || err, "SQL:", sql.slice(0, 200));
      throw err;
    }
  }

  async function queryOne(sql: string, params?: any[]): Promise<any | null> {
    const rows = await query(sql, params);
    return rows[0] ?? null;
  }

  // Communications: Delta-synced seed (communications_log) + app writes (app_communications_log)
  // Actual columns: comms_id, incident_id, comms_timestamp, recipient_role, recipient_name, channel, direction, summary, status
  const COMMS_UNION = `(
    SELECT incident_id, channel, recipient_name, summary, direction, comms_timestamp::text AS comms_timestamp FROM communications_log
    UNION ALL
    SELECT incident_id, channel, recipient AS recipient_name, message AS summary, sent_by AS direction, sent_at::text AS comms_timestamp FROM app_communications_log
  ) AS all_comms`;

  appkit.server.extend((app: any) => {
  // ---- Health -----------------------------------------------------------
  app.get("/api/health", (_req: any, res: any) => {
    res.json({ status: "ok", service: "water-digital-twin", company: "Water Utilities" });
  });

  // ---- Incidents --------------------------------------------------------
  app.get("/api/incidents/active", async (_req: any, res: any) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_incidents WHERE status = 'active' ORDER BY created_at DESC"
      );
      res.json({ incidents: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id", async (req: any, res: any) => {
    try {
      const { id } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [id]
      );
      if (!incident) return res.status(404).json({ error: "Incident not found" });
      const [events, comms] = await Promise.all([
        query("SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_timestamp DESC", [id]),
        query(`SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY comms_timestamp DESC`, [id]),
      ]);
      res.json({ incident, events, communications: comms });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/events/recent", async (req: any, res: any) => {
    try {
      // Try real-time window first; fall back to most recent events if demo data is static
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const since = new Date(Date.now() - hours * 3600_000).toISOString();
      let rows = await query(
        "SELECT * FROM incident_events WHERE event_timestamp >= $1 ORDER BY event_timestamp DESC LIMIT 200",
        [since]
      );
      if (rows.length === 0) {
        // Static demo data — return most recent events regardless of time window
        rows = await query(
          "SELECT * FROM incident_events ORDER BY event_timestamp DESC LIMIT 200"
        );
      }
      res.json({ events: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id/events", async (req: any, res: any) => {
    try {
      const rows = await query(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_timestamp DESC",
        [req.params.id]
      );
      res.json({ events: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id/handover", async (req: any, res: any) => {
    try {
      const { id } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [id]
      );
      if (!incident) return res.status(404).json({ error: "Incident not found" });
      const [events, comms] = await Promise.all([
        query("SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_timestamp ASC", [id]),
        query(`SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY comms_timestamp DESC`, [id]),
      ]);
      // The incident_events table has no "status" column — all events are completed facts.
      // Outstanding actions come from the playbook, not from events.
      res.json({
        incident,
        actions_taken: events,
        outstanding_actions: [],
        communications: comms,
      });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- DMA --------------------------------------------------------------
  app.get("/api/dma/status", async (req: any, res: any) => {
    try {
      const ragStatus = req.query.rag_status as string | undefined;
      let sql = "SELECT * FROM dma_status";
      const params: any[] = [];
      if (ragStatus) {
        sql += " WHERE rag_status = $1";
        params.push(ragStatus);
      }
      sql += " ORDER BY dma_code";
      const rows = await query(sql, params.length ? params : undefined);
      res.json({ dma_statuses: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/polygons", async (req: any, res: any) => {
    try {
      const row = await queryOne(
        "SELECT dma_code, dma_name, ST_AsGeoJSON(geom) AS geojson FROM dim_dma WHERE dma_code = $1",
        [req.params.code]
      );
      if (!row) return res.status(404).json({ error: "DMA not found" });
      const geojson = typeof row.geojson === "string" ? JSON.parse(row.geojson) : row.geojson;
      res.json({
        type: "Feature",
        properties: { dma_code: row.dma_code, dma_name: row.dma_name },
        geometry: geojson,
      });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/detail", async (req: any, res: any) => {
    try {
      const row = await queryOne(
        "SELECT * FROM dma_summary WHERE dma_code = $1", [req.params.code]
      );
      if (!row) return res.status(404).json({ error: "DMA summary not found" });
      res.json(row);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/sensors", async (req: any, res: any) => {
    try {
      // Two fast queries instead of one slow LATERAL join
      const [sensorRows, latestRows] = await Promise.all([
        query("SELECT * FROM dim_sensor WHERE dma_code = $1 ORDER BY sensor_id", [req.params.code]),
        query(
          `SELECT DISTINCT ON (sensor_id) sensor_id, value AS latest_value, sensor_type
           FROM fact_telemetry
           WHERE dma_code = $1
           ORDER BY sensor_id, timestamp DESC`,
          [req.params.code]
        ),
      ]);
      // Index latest readings by sensor_id
      const latest: Record<string, any> = {};
      for (const r of latestRows) latest[r.sensor_id] = r;
      // Merge and sort low-pressure first
      const sensors = sensorRows.map((s: any) => {
        const reading = latest[s.sensor_id];
        return {
          ...s,
          latest_pressure: s.sensor_type === "pressure" ? reading?.latest_value ?? null : null,
          latest_flow: s.sensor_type === "flow" ? reading?.latest_value ?? null : null,
        };
      });
      sensors.sort((a: any, b: any) => (a.latest_pressure ?? 999) - (b.latest_pressure ?? 999));
      res.json({ sensors });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/properties", async (req: any, res: any) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_properties WHERE dma_code = $1 ORDER BY property_id", [req.params.code]
      );
      res.json({ properties: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/complaints", async (req: any, res: any) => {
    try {
      const rows = await query(
        "SELECT * FROM customer_complaints WHERE dma_code = $1 ORDER BY complaint_timestamp DESC LIMIT 50",
        [req.params.code]
      );
      res.json({ complaints: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/assets", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT a.* FROM dim_assets a
         JOIN dim_asset_dma_feed f ON a.asset_id = f.asset_id
         WHERE f.dma_code = $1
         ORDER BY a.asset_type, a.asset_id`,
        [req.params.code]
      );
      res.json({ assets: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/rag-history", async (req: any, res: any) => {
    try {
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const since = new Date(Date.now() - hours * 3600_000).toISOString();
      let rows = await query(
        "SELECT dma_code, timestamp AS recorded_at, rag_status, avg_pressure, min_pressure FROM dma_rag_history WHERE dma_code = $1 AND timestamp >= $2 ORDER BY timestamp ASC",
        [req.params.code, since]
      );
      if (rows.length === 0) {
        // Static demo data — return the last 24h of history (96 entries at 15-min intervals)
        // so the RAG timeline strip shows meaningful RED/AMBER/GREEN proportions
        rows = await query(
          `SELECT dma_code, timestamp AS recorded_at, rag_status, avg_pressure, min_pressure
           FROM dma_rag_history WHERE dma_code = $1
           ORDER BY timestamp DESC LIMIT 96`,
          [req.params.code]
        );
        rows.reverse();
      }
      res.json({ rag_history: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Sensors & Telemetry ----------------------------------------------
  // fact_telemetry columns: sensor_id, sensor_type, dma_code, timestamp, value, quality_flag, anomaly_flag
  app.get("/api/sensor/:id/telemetry", async (req: any, res: any) => {
    try {
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const since = new Date(Date.now() - hours * 3600_000).toISOString();
      let rows = await query(
        `SELECT sensor_id, sensor_type, timestamp AS ts, value
         FROM fact_telemetry WHERE sensor_id = $1 AND timestamp >= $2 ORDER BY timestamp ASC`,
        [req.params.id, since]
      );
      if (rows.length === 0) {
        rows = await query(
          `SELECT sensor_id, sensor_type, timestamp AS ts, value
           FROM fact_telemetry WHERE sensor_id = $1 ORDER BY timestamp DESC LIMIT 500`,
          [req.params.id]
        );
        rows.reverse();
      }
      // Pivot: the frontend expects { ts, pressure, flow } per row
      // Group by timestamp, merge pressure + flow values
      const byTs: Record<string, any> = {};
      for (const r of rows) {
        const key = r.ts;
        if (!byTs[key]) byTs[key] = { ts: key, pressure: null, flow: null };
        if (r.sensor_type === "pressure") byTs[key].pressure = r.value;
        else if (r.sensor_type === "flow") byTs[key].flow = r.value;
      }
      res.json({ telemetry: Object.values(byTs) });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // anomaly_scores columns: sensor_id, timestamp, anomaly_sigma, baseline_value, actual_value, is_anomaly, scoring_method
  app.get("/api/sensor/:id/anomalies", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT sensor_id, timestamp AS scored_at, anomaly_sigma AS score, baseline_value, actual_value, is_anomaly
         FROM anomaly_scores WHERE sensor_id = $1 ORDER BY timestamp DESC LIMIT 100`,
        [req.params.id]
      );
      res.json({ anomalies: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Map / GeoJSON ----------------------------------------------------
  app.get("/api/map/geojson", async (_req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT d.dma_code, d.dma_name, ST_AsGeoJSON(d.geom) AS geojson,
                s.rag_status, s.avg_pressure
         FROM dim_dma d
         LEFT JOIN dma_status s ON d.dma_code = s.dma_code
         ORDER BY d.dma_code`
      );
      const features = rows.map((r: any) => {
        const geom = typeof r.geojson === "string" ? JSON.parse(r.geojson) : r.geojson;
        return {
          type: "Feature",
          properties: {
            dma_code: r.dma_code,
            dma_name: r.dma_name,
            rag_status: r.rag_status ?? "GREEN",
            avg_pressure: r.avg_pressure,
          },
          geometry: geom,
        };
      });
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/map/assets/:dmaCode", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT a.asset_id, a.asset_type, a.asset_name,
                ST_AsGeoJSON(a.geom) AS geojson
         FROM dim_assets a
         JOIN dim_asset_dma_feed f ON a.asset_id = f.asset_id
         WHERE f.dma_code = $1`,
        [req.params.dmaCode]
      );
      const features = rows.map((r: any) => {
        const geom = typeof r.geojson === "string" ? JSON.parse(r.geojson) : r.geojson;
        return {
          type: "Feature",
          properties: { asset_id: r.asset_id, asset_type: r.asset_type, asset_name: r.asset_name },
          geometry: geom,
        };
      });
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Map overlays: sensors, complaints, sensitive premises --------------
  app.get("/api/map/sensors", async (_req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT s.sensor_id, s.sensor_type, s.latitude, s.longitude, s.dma_code,
                d.rag_status, d.avg_pressure
         FROM dim_sensor s
         LEFT JOIN dma_status d ON s.dma_code = d.dma_code
         WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL`
      );
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          sensor_id: r.sensor_id,
          sensor_type: r.sensor_type,
          dma_code: r.dma_code,
          rag_status: r.rag_status ?? "GREEN",
          avg_pressure: r.avg_pressure != null ? Number(r.avg_pressure) : null,
        },
        geometry: { type: "Point", coordinates: [Number(r.longitude), Number(r.latitude)] },
      }));
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/map/complaints/:dmaCode", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT c.complaint_type, c.complaint_timestamp, c.property_id,
                p.latitude, p.longitude
         FROM customer_complaints c
         JOIN dim_properties p ON c.property_id = p.property_id
         WHERE c.dma_code = $1 AND p.latitude IS NOT NULL
         ORDER BY c.complaint_timestamp DESC LIMIT 100`,
        [req.params.dmaCode]
      );
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          complaint_type: r.complaint_type,
          complaint_timestamp: r.complaint_timestamp,
          property_id: r.property_id,
        },
        geometry: { type: "Point", coordinates: [Number(r.longitude), Number(r.latitude)] },
      }));
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/map/sensitive/:dmaCode", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT property_id, property_type, latitude, longitude,
                sensitive_premise_type, customer_height_m
         FROM dim_properties
         WHERE dma_code = $1 AND sensitive_premise_type IS NOT NULL
           AND latitude IS NOT NULL`,
        [req.params.dmaCode]
      );
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          property_id: r.property_id,
          property_type: r.property_type,
          sensitive_type: r.sensitive_premise_type,
          height: r.customer_height_m != null ? Number(r.customer_height_m) : null,
        },
        geometry: { type: "Point", coordinates: [Number(r.longitude), Number(r.latitude)] },
      }));
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Playbooks --------------------------------------------------------
  app.get("/api/playbooks/:type", async (req: any, res: any) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_response_playbooks WHERE incident_type = $1 ORDER BY step_order ASC",
        [req.params.type]
      );
      if (!rows.length) return res.status(404).json({ error: "Playbook not found" });
      res.json({ incident_type: req.params.type, steps: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Reservoirs -------------------------------------------------------
  app.get("/api/reservoirs/:dmaCode", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT r.* FROM dim_reservoirs r
         JOIN dim_asset_dma_feed f ON r.reservoir_id = f.asset_id
         WHERE f.dma_code = $1`,
        [req.params.dmaCode]
      );
      res.json({ reservoirs: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Communications ---------------------------------------------------
  app.get("/api/comms/:incidentId", async (req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY comms_timestamp DESC`,
        [req.params.incidentId]
      );
      res.json({ communications: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post("/api/comms/:incidentId", async (req: any, res: any) => {
    try {
      const { incidentId } = req.params;
      const { channel, recipient, message, sent_by = "operator" } = req.body;
      await appkit.lakebase.query(
        `INSERT INTO app_communications_log (incident_id, channel, recipient, message, sent_by, sent_at)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [incidentId, channel, recipient, message, sent_by, new Date().toISOString()]
      );
      res.json({ status: "created" });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Playbook Actions (write-back) ------------------------------------
  app.post("/api/playbook-actions/:incidentId", async (req: any, res: any) => {
    try {
      const { incidentId } = req.params;
      const actions = req.body as Array<{ action_id: string; decision: string; note?: string }>;
      for (const action of actions) {
        await appkit.lakebase.query(
          `INSERT INTO app_playbook_action_log (incident_id, action_id, decision, note, decided_at)
           VALUES ($1, $2, $3, $4, $5)`,
          [incidentId, action.action_id, action.decision, action.note ?? null, new Date().toISOString()]
        );
      }
      res.json({ status: "saved", count: actions.length });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Proactive Comms Request ------------------------------------------
  app.post("/api/comms-request/:incidentId", async (req: any, res: any) => {
    try {
      const { incidentId } = req.params;
      const { channel, message_template, target_audience } = req.body;
      await appkit.lakebase.query(
        `INSERT INTO app_comms_requests (incident_id, channel, message_template, target_audience, created_at)
         VALUES ($1, $2, $3, $4, $5)`,
        [incidentId, channel, message_template, target_audience, new Date().toISOString()]
      );
      res.json({ status: "created" });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Regulatory -------------------------------------------------------
  app.get("/api/regulatory/:incidentId", async (req: any, res: any) => {
    try {
      const { incidentId } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1", [incidentId]
      );
      if (!incident) return res.status(404).json({ error: "Incident not found" });

      const properties = await query(
        `SELECT p.property_type, COUNT(*) as count
         FROM dim_properties p
         JOIN dim_incidents i ON p.dma_code = i.dma_code
         WHERE i.incident_id = $1
         GROUP BY p.property_type`,
        [incidentId]
      );

      const totalProperties = properties.reduce(
        (sum: number, p: any) => sum + Number(p.count || 0), 0
      );

      let hoursElapsed = 0;
      const createdAt = incident.created_at || incident.start_timestamp;
      if (createdAt) {
        hoursElapsed = (Date.now() - new Date(createdAt).getTime()) / 3_600_000;
      }

      const penaltyHours = Math.max(0, hoursElapsed - 3);
      const penalty = totalProperties * penaltyHours * 580;

      // Check if proactive comms have been requested for this incident
      const commsRequests = await query(
        "SELECT COUNT(*) AS cnt FROM app_comms_requests WHERE incident_id = $1",
        [incidentId]
      );
      const commsRequested = Number(commsRequests[0]?.cnt || 0) > 0;

      res.json({
        incident_id: incidentId,
        incident,
        affected_properties: properties,
        total_properties: totalProperties,
        proactive_comms_requested: commsRequested,
        hours_elapsed: Math.round(hoursElapsed * 100) / 100,
        deadlines: {
          dwi_verbal:  { label: "DWI Verbal Notification", hours: 1,  status: hoursElapsed >= 1  ? "DONE" : "PENDING" },
          dwi_written: { label: "DWI Written Report",      hours: 24, status: hoursElapsed >= 24 ? "DONE" : "DUE" },
          ofwat_3h:    { label: "Ofwat 3h Threshold",      hours: 3,  status: hoursElapsed >= 3  ? "BREACHED" : "OK" },
          ofwat_12h:   { label: "Ofwat 12h Threshold",     hours: 12, status: hoursElapsed >= 12 ? "BREACHED" : "OK" },
        },
        penalty_calculation: {
          formula: "properties x (hours - 3) x GBP 580",
          properties: totalProperties,
          penalty_hours: Math.round(penaltyHours * 100) / 100,
          rate_per_property_hour: 580,
          estimated_penalty_gbp: Math.round(penalty * 100) / 100,
        },
      });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });
  });
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const plugins: any[] = [
  server({ autoStart: false }),
  lakebase(),
];

if (process.env.DATABRICKS_GENIE_SPACE_ID) {
  plugins.push(genie());
}

createApp({ plugins })
  .then(async (appkit) => {
    await setupRoutes(appkit);
    await appkit.server.start();
    console.log("Water Digital Twin API started");
  })
  .catch(console.error);
