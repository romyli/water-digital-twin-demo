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
  // ---------------------------------------------------------------------------
  // Helper: run a query and return rows
  // ---------------------------------------------------------------------------

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

  // Communications and comms_requests are split across two tables each:
  // - Delta-synced seed data: communications_log / comms_requests
  // - App write-back data:    app_communications_log / app_comms_requests
  // Read queries UNION both; writes go to the app_* tables only.

  const COMMS_UNION = `(
    SELECT incident_id, channel, recipient, message, sent_by, sent_at FROM communications_log
    UNION ALL
    SELECT incident_id, channel, recipient, message, sent_by, sent_at FROM app_communications_log
  ) AS all_comms`;

  // ---------------------------------------------------------------------------
  // Custom Routes
  // ---------------------------------------------------------------------------

  appkit.server.extend((app: any) => {
  // ---- Health -----------------------------------------------------------
  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok", service: "water-digital-twin", company: "Water Utilities" });
  });

  // ---- Incidents --------------------------------------------------------
  app.get("/api/incidents/active", async (_req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_incidents WHERE status = 'active' ORDER BY created_at DESC"
      );
      res.json({ incidents: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id", async (req, res) => {
    try {
      const { id } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1",
        [id]
      );
      if (!incident) return res.status(404).json({ error: "Incident not found" });
      const [events, comms] = await Promise.all([
        query("SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time DESC", [id]),
        query(`SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY sent_at DESC`, [id]),
      ]);
      res.json({ incident, events, communications: comms });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id/events", async (req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time DESC",
        [req.params.id]
      );
      res.json({ events: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/:id/handover", async (req, res) => {
    try {
      const { id } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1",
        [id]
      );
      if (!incident) return res.status(404).json({ error: "Incident not found" });
      const [events, comms, outstanding] = await Promise.all([
        query("SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_time ASC", [id]),
        query(`SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY sent_at DESC`, [id]),
        query(
          "SELECT * FROM incident_events WHERE incident_id = $1 AND status = 'pending' ORDER BY event_time ASC",
          [id]
        ),
      ]);
      res.json({
        incident,
        actions_taken: events.filter((e: any) => e.status === "completed"),
        outstanding_actions: outstanding,
        communications: comms,
      });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- DMA --------------------------------------------------------------
  app.get("/api/dma/status", async (req, res) => {
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

  app.get("/api/dma/:code/polygons", async (req, res) => {
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

  app.get("/api/dma/:code/detail", async (req, res) => {
    try {
      const row = await queryOne(
        "SELECT * FROM dma_summary WHERE dma_code = $1",
        [req.params.code]
      );
      if (!row) return res.status(404).json({ error: "DMA summary not found" });
      res.json(row);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/sensors", async (req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_sensor WHERE dma_code = $1 ORDER BY sensor_id",
        [req.params.code]
      );
      res.json({ sensors: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/properties", async (req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM dim_properties WHERE dma_code = $1 ORDER BY property_id",
        [req.params.code]
      );
      res.json({ properties: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/complaints", async (req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM customer_complaints WHERE dma_code = $1 ORDER BY reported_at DESC LIMIT 50",
        [req.params.code]
      );
      res.json({ complaints: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/assets", async (req, res) => {
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

  app.get("/api/dma/:code/rag-history", async (req, res) => {
    try {
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const since = new Date(Date.now() - hours * 3600_000).toISOString();
      const rows = await query(
        "SELECT * FROM dma_rag_history WHERE dma_code = $1 AND recorded_at >= $2 ORDER BY recorded_at ASC",
        [req.params.code, since]
      );
      res.json({ rag_history: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Sensors & Telemetry ----------------------------------------------
  app.get("/api/sensor/:id/telemetry", async (req, res) => {
    try {
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const since = new Date(Date.now() - hours * 3600_000).toISOString();
      const rows = await query(
        "SELECT * FROM fact_telemetry WHERE sensor_id = $1 AND ts >= $2 ORDER BY ts ASC",
        [req.params.id, since]
      );
      res.json({ telemetry: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/sensor/:id/anomalies", async (req, res) => {
    try {
      const rows = await query(
        "SELECT * FROM anomaly_scores WHERE sensor_id = $1 ORDER BY scored_at DESC LIMIT 100",
        [req.params.id]
      );
      res.json({ anomalies: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Map / GeoJSON ----------------------------------------------------
  app.get("/api/map/geojson", async (_req, res) => {
    try {
      const rows = await query(
        `SELECT d.dma_code, d.dma_name, ST_AsGeoJSON(d.geom) AS geojson,
                s.rag_status, s.avg_pressure, s.anomaly_confidence
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
            anomaly_confidence: r.anomaly_confidence,
          },
          geometry: geom,
        };
      });
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/map/assets/:dmaCode", async (req, res) => {
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
          properties: {
            asset_id: r.asset_id,
            asset_type: r.asset_type,
            asset_name: r.asset_name,
          },
          geometry: geom,
        };
      });
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Playbooks --------------------------------------------------------
  app.get("/api/playbooks/:type", async (req, res) => {
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
  app.get("/api/reservoirs/:dmaCode", async (req, res) => {
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
  app.get("/api/comms/:incidentId", async (req, res) => {
    try {
      const rows = await query(
        `SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY sent_at DESC`,
        [req.params.incidentId]
      );
      res.json({ communications: rows });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post("/api/comms/:incidentId", async (req, res) => {
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
  app.post("/api/playbook-actions/:incidentId", async (req, res) => {
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
  app.post("/api/comms-request/:incidentId", async (req, res) => {
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
  app.get("/api/regulatory/:incidentId", async (req, res) => {
    try {
      const { incidentId } = req.params;
      const incident = await queryOne(
        "SELECT * FROM dim_incidents WHERE incident_id = $1",
        [incidentId]
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
        (sum: number, p: any) => sum + Number(p.count || 0),
        0
      );

      let hoursElapsed = 0;
      const createdAt = incident.created_at;
      if (createdAt) {
        const start = new Date(createdAt);
        hoursElapsed = (Date.now() - start.getTime()) / 3_600_000;
      }

      const penaltyHours = Math.max(0, hoursElapsed - 3);
      const penalty = totalProperties * penaltyHours * 580;

      res.json({
        incident_id: incidentId,
        incident,
        affected_properties: properties,
        total_properties: totalProperties,
        hours_elapsed: Math.round(hoursElapsed * 100) / 100,
        deadlines: {
          dwi_verbal: {
            label: "DWI Verbal Notification",
            hours: 1,
            status: hoursElapsed >= 1 ? "DONE" : "PENDING",
          },
          dwi_written: {
            label: "DWI Written Report",
            hours: 24,
            status: hoursElapsed >= 24 ? "DONE" : "DUE",
          },
          ofwat_3h: {
            label: "Ofwat 3h Threshold",
            hours: 3,
            status: hoursElapsed >= 3 ? "BREACHED" : "OK",
          },
          ofwat_12h: {
            label: "Ofwat 12h Threshold",
            hours: 12,
            status: hoursElapsed >= 12 ? "BREACHED" : "OK",
          },
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

// Only enable Genie if a space ID is configured
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
