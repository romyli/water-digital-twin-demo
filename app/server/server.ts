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

  // --- Regulatory Rules (loaded once at startup, fallback to defaults) ------
  const DEFAULT_RULES: Record<string, number> = {
    OFWAT_PENALTY_RATE: 580, OFWAT_GRACE_PERIOD: 3,
    DWI_VERBAL_DEADLINE: 1, DWI_WRITTEN_DEADLINE: 24,
    OFWAT_ESCALATION_THRESHOLD: 12,
    PRESSURE_RED_THRESHOLD: 15, PRESSURE_AMBER_THRESHOLD: 25,
    DEFAULT_BASE_PRESSURE: 25,
    IMPACT_HIGH_THRESHOLD: 0, IMPACT_MEDIUM_THRESHOLD: 5, IMPACT_LOW_THRESHOLD: 10,
    CMEX_GREEN_THRESHOLD: 70, CMEX_AMBER_THRESHOLD: 40,
  };

  let rulesMap: Record<string, { value_numeric: number; value_text: string | null; unit: string; rule_name: string }> = {};
  try {
    const rulesRows = await query(
      "SELECT rule_id, rule_name, value_numeric, value_text, unit FROM dim_regulatory_rules WHERE effective_to IS NULL"
    );
    for (const r of rulesRows) {
      rulesMap[r.rule_id] = { value_numeric: Number(r.value_numeric), value_text: r.value_text, unit: r.unit, rule_name: r.rule_name };
    }
    console.log(`[rules] Loaded ${Object.keys(rulesMap).length} regulatory rules from dim_regulatory_rules`);
  } catch (err: any) {
    console.warn("[rules] dim_regulatory_rules not available, using defaults:", err.message);
  }

  function rule(id: string): number {
    return rulesMap[id]?.value_numeric ?? DEFAULT_RULES[id] ?? 0;
  }

  // --- Demo scenario state (in-memory) ------------------------------------
  let demoScenarioActive = false;
  let demoActivatedAt: Date | null = null;
  let demoTimeOffset = 0; // ms offset: real_now - demo_world_now

  /** Current time in demo-world (for server-side calculations and time-windowed queries) */
  function demoNow(): number {
    return demoScenarioActive && demoTimeOffset ? Date.now() - demoTimeOffset : Date.now();
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

  // ---- Regulatory Rules Config ------------------------------------------
  app.get("/api/config/rules", (_req: any, res: any) => {
    res.json({ rules: rulesMap });
  });

  // ---- Demo Scenario Control --------------------------------------------
  app.get("/api/demo/status", (_req: any, res: any) => {
    res.json({ scenarioActive: demoScenarioActive, activatedAt: demoActivatedAt?.toISOString() ?? null, timeOffset: demoTimeOffset });
  });

  app.post("/api/demo/activate", async (_req: any, res: any) => {
    try {
      // Find the latest event timestamp — this becomes "now" so the incident
      // start naturally appears in the past (e.g. 3.5h ago for the handover demo)
      const row = await queryOne(
        `SELECT i.incident_id,
                (SELECT MAX(event_timestamp) FROM incident_events WHERE incident_id = i.incident_id) AS latest_event
         FROM dim_incidents i WHERE i.status = 'active' ORDER BY i.created_at ASC LIMIT 1`
      );
      if (!row) return res.status(400).json({ error: "No active incident in dataset" });
      const anchor = new Date(row.latest_event);
      demoActivatedAt = new Date();
      demoTimeOffset = demoActivatedAt.getTime() - anchor.getTime();
      demoScenarioActive = true;
      console.log(`[demo] Scenario activated. Anchor: ${row.latest_event}, Offset: ${Math.round(demoTimeOffset / 60_000)} min`);
      res.json({ scenarioActive: true, activatedAt: demoActivatedAt.toISOString() });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.post("/api/demo/reset", async (_req: any, res: any) => {
    try {
      demoScenarioActive = false;
      demoActivatedAt = null;
      demoTimeOffset = 0;
      // Clear app-written data for a fresh demo run
      const results = await Promise.allSettled([
        appkit.lakebase.query("DELETE FROM app_communications_log"),
        appkit.lakebase.query("DELETE FROM app_playbook_action_log"),
        appkit.lakebase.query("DELETE FROM app_comms_requests"),
      ]);
      const failed = results.filter((r) => r.status === "rejected");
      if (failed.length) console.warn("[demo] Some tables failed to clear:", failed.map((r: any) => r.reason?.message));
      console.log("[demo] Scenario reset. App write-back tables cleared.");
      res.json({ scenarioActive: false, activatedAt: null });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // ---- Incidents --------------------------------------------------------
  app.get("/api/incidents/active", async (_req: any, res: any) => {
    if (!demoScenarioActive) return res.json({ incidents: [] });
    try {
      const [rows, dmaSummary] = await Promise.all([
        query("SELECT * FROM dim_incidents WHERE status = 'active' ORDER BY created_at DESC"),
        queryOne(
          `SELECT COUNT(DISTINCT s.dma_code) AS affected_dma_count,
                  COALESCE(SUM(sm.total_properties), 0) AS total_properties,
                  COALESCE(SUM(sm.sensitive_premises_count), 0) AS sensitive_site_count
           FROM dma_status s
           LEFT JOIN vw_dma_summary sm ON s.dma_code = sm.dma_code
           WHERE s.rag_status IN ('RED', 'AMBER')`
        ),
      ]);
      const enriched = rows.map((inc: any) => ({
        ...inc,
        affected_dma_count: Number(dmaSummary?.affected_dma_count) || 0,
        affected_properties: Number(dmaSummary?.total_properties) || 0,
        sensitive_site_count: Number(dmaSummary?.sensitive_site_count) || 0,
      }));
      res.json({ incidents: enriched });
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
      res.json({
        incident,
        events,
        communications: comms,
      });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/incidents/events/recent", async (req: any, res: any) => {
    if (!demoScenarioActive) return res.json({ events: [] });
    try {
      // Try real-time window first; fall back to most recent events if demo data is static
      const hours = Math.min(168, Math.max(1, Number(req.query.hours) || 24));
      const now = demoNow();
      const since = new Date(now - hours * 3600_000).toISOString();
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
      const [events, comms, dmaSummary] = await Promise.all([
        query("SELECT * FROM incident_events WHERE incident_id = $1 ORDER BY event_timestamp ASC", [id]),
        query(`SELECT * FROM ${COMMS_UNION} WHERE incident_id = $1 ORDER BY comms_timestamp DESC`, [id]),
        queryOne(
          `SELECT COUNT(DISTINCT s.dma_code) AS affected_dma_count,
                  COALESCE(SUM(sm.total_properties), 0) AS total_properties,
                  COALESCE(SUM(sm.sensitive_premises_count), 0) AS sensitive_site_count
           FROM dma_status s
           LEFT JOIN vw_dma_summary sm ON s.dma_code = sm.dma_code
           WHERE s.rag_status IN ('RED', 'AMBER')`
        ),
      ]);

      // Outstanding actions — separate query with fallback if table not synced
      let outstandingActions: any[] = [];
      try {
        outstandingActions = await query(
          `SELECT action_id, action_description, priority, assigned_to, assigned_role,
                  due_by, status, notes
           FROM incident_outstanding_actions
           WHERE incident_id = $1 AND status != 'completed'
           ORDER BY CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
                    due_by`,
          [id]
        );
      } catch { /* table may not be synced yet */ }

      // Anomaly lead time: first_complaint_time - start_timestamp
      const anomalyLeadMinutes = incident.first_complaint_time && incident.start_timestamp
        ? Math.round((new Date(incident.first_complaint_time).getTime() - new Date(incident.start_timestamp).getTime()) / 60_000)
        : null;

      // Enrich incident with computed counts
      const enriched = {
        ...incident,
        affected_dma_count: Number(dmaSummary?.affected_dma_count) || 0,
        affected_properties: Number(dmaSummary?.total_properties) || 0,
        sensitive_site_count: Number(dmaSummary?.sensitive_site_count) || 0,
        anomaly_lead_minutes: anomalyLeadMinutes,
      };
      res.json({
        incident: enriched,
        actions_taken: events,
        outstanding_actions: outstandingActions,
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
      if (!demoScenarioActive) {
        const normalised = rows.map((r: any) => ({ ...r, rag_status: "GREEN", avg_pressure: r.avg_pressure ?? rule("DEFAULT_BASE_PRESSURE") }));
        return res.json({ dma_statuses: normalised });
      }
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
        "SELECT * FROM vw_dma_summary WHERE dma_code = $1", [req.params.code]
      );
      if (!row) return res.status(404).json({ error: "DMA summary not found" });
      res.json(row);
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  app.get("/api/dma/:code/sensors", async (req: any, res: any) => {
    try {
      // Use pre-computed mv_sensor_latest instead of dim_sensor + fact_telemetry join
      let sensors: any[];
      try {
        const rows = await query(
          `SELECT sensor_id, dma_code, sensor_type, timestamp,
                  total_head_pressure AS latest_pressure, flow_rate AS latest_flow,
                  quality_flag, latitude, longitude, elevation_m, status
           FROM mv_sensor_latest WHERE dma_code = $1
           ORDER BY total_head_pressure ASC NULLS LAST`,
          [req.params.code]
        );
        sensors = rows;
      } catch {
        // Fallback: mv_sensor_latest not synced yet
        const [sensorRows, latestRows] = await Promise.all([
          query("SELECT * FROM dim_sensor WHERE dma_code = $1 ORDER BY sensor_id", [req.params.code]),
          query(
            `SELECT DISTINCT ON (sensor_id) sensor_id, value AS latest_value, sensor_type
             FROM fact_telemetry WHERE dma_code = $1
             ORDER BY sensor_id, timestamp DESC`,
            [req.params.code]
          ),
        ]);
        const latest: Record<string, any> = {};
        for (const r of latestRows) latest[r.sensor_id] = r;
        sensors = sensorRows.map((s: any) => {
          const reading = latest[s.sensor_id];
          return {
            ...s,
            latest_pressure: s.sensor_type === "pressure" ? reading?.latest_value ?? null : null,
            latest_flow: s.sensor_type === "flow" ? reading?.latest_value ?? null : null,
          };
        });
        sensors.sort((a: any, b: any) => (a.latest_pressure ?? 999) - (b.latest_pressure ?? 999));
      }
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
      const now = demoNow();
      const since = new Date(now - hours * 3600_000).toISOString();
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
      // When demo is active, shift the time window back so it hits the original data range
      const now = demoNow();
      const since = new Date(now - hours * 3600_000).toISOString();
      let rows = await query(
        `SELECT sensor_id, sensor_type, timestamp AS ts, value
         FROM fact_telemetry WHERE sensor_id = $1 AND timestamp >= $2 ORDER BY timestamp ASC LIMIT 1000`,
        [req.params.id, since]
      );
      if (rows.length === 0) {
        // Fallback: use a narrower query with just the sensor filter + limit
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
         FROM anomaly_scores WHERE sensor_id = $1 AND is_anomaly = true ORDER BY timestamp DESC LIMIT 100`,
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
                s.rag_status, s.avg_pressure,
                COALESCE(sm.sensitive_premises_count, 0) AS sensitive_premises_count
         FROM dim_dma d
         LEFT JOIN dma_status s ON d.dma_code = s.dma_code
         LEFT JOIN vw_dma_summary sm ON d.dma_code = sm.dma_code
         ORDER BY d.dma_code`
      );
      const features = rows.map((r: any) => {
        const geom = typeof r.geojson === "string" ? JSON.parse(r.geojson) : r.geojson;
        return {
          type: "Feature",
          properties: {
            dma_code: r.dma_code,
            dma_name: r.dma_name,
            rag_status: demoScenarioActive ? (r.rag_status ?? "GREEN") : "GREEN",
            avg_pressure: demoScenarioActive ? r.avg_pressure : (r.avg_pressure ?? rule("DEFAULT_BASE_PRESSURE")),
            sensitive_premises_count: Number(r.sensitive_premises_count) || 0,
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
      // Try mv_sensor_latest first (includes lat/lng + latest reading)
      let rows: any[];
      try {
        rows = await query(
          `SELECT m.sensor_id, m.sensor_type, m.latitude, m.longitude, m.dma_code,
                  d.rag_status, m.total_head_pressure AS avg_pressure
           FROM mv_sensor_latest m
           LEFT JOIN dma_status d ON m.dma_code = d.dma_code
           WHERE m.latitude IS NOT NULL AND m.longitude IS NOT NULL`
        );
      } catch {
        rows = await query(
          `SELECT s.sensor_id, s.sensor_type, s.latitude, s.longitude, s.dma_code,
                  d.rag_status, d.avg_pressure
           FROM dim_sensor s
           LEFT JOIN dma_status d ON s.dma_code = d.dma_code
           WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL`
        );
      }
      const basePressure = rule("DEFAULT_BASE_PRESSURE");
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          sensor_id: r.sensor_id,
          sensor_type: r.sensor_type,
          dma_code: r.dma_code,
          rag_status: demoScenarioActive ? (r.rag_status ?? "GREEN") : "GREEN",
          avg_pressure: demoScenarioActive ? (r.avg_pressure != null ? Number(r.avg_pressure) : null) : basePressure,
        },
        geometry: { type: "Point", coordinates: [Number(r.longitude), Number(r.latitude)] },
      }));
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Global complaints overlay (all DMAs, last 7 days)
  app.get("/api/map/complaints", async (_req: any, res: any) => {
    if (!demoScenarioActive) return res.json({ type: "FeatureCollection", features: [] });
    try {
      const now = demoNow();
      const since = new Date(now - 7 * 24 * 3600_000).toISOString();
      const rows = await query(
        `SELECT c.complaint_type, c.complaint_timestamp, c.property_id, c.dma_code,
                p.latitude, p.longitude
         FROM customer_complaints c
         JOIN dim_properties p ON c.property_id = p.property_id
         WHERE p.latitude IS NOT NULL AND c.complaint_timestamp >= $1
         ORDER BY c.complaint_timestamp DESC LIMIT 500`,
        [since]
      );
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          complaint_type: r.complaint_type,
          complaint_timestamp: r.complaint_timestamp,
          property_id: r.property_id,
          dma_code: r.dma_code,
        },
        geometry: { type: "Point", coordinates: [Number(r.longitude), Number(r.latitude)] },
      }));
      res.json({ type: "FeatureCollection", features });
    } catch (e: any) {
      res.status(500).json({ error: e.message });
    }
  });

  // Global sensitive premises overlay (all DMAs)
  app.get("/api/map/sensitive", async (_req: any, res: any) => {
    try {
      const rows = await query(
        `SELECT property_id, property_type, latitude, longitude,
                sensitive_premise_type, customer_height_m, dma_code
         FROM dim_properties
         WHERE sensitive_premise_type IS NOT NULL AND latitude IS NOT NULL`
      );
      const features = rows.map((r: any) => ({
        type: "Feature",
        properties: {
          property_id: r.property_id,
          property_type: r.property_type,
          sensitive_type: r.sensitive_premise_type,
          height: r.customer_height_m != null ? Number(r.customer_height_m) : null,
          dma_code: r.dma_code,
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

  // ---- Map Centre (computed from DMA centroids) --------------------------
  app.get("/api/map/centre", async (_req: any, res: any) => {
    try {
      const row = await queryOne(
        `SELECT AVG(centroid_latitude) AS lat, AVG(centroid_longitude) AS lng
         FROM vw_dma_summary
         WHERE centroid_latitude IS NOT NULL`
      );
      if (row?.lat != null && row?.lng != null) {
        res.json({ latitude: Number(row.lat), longitude: Number(row.lng) });
      } else {
        // Fallback: compute from dim_dma geometry centroids
        const fallback = await queryOne(
          `SELECT AVG(ST_Y(ST_Centroid(geom))) AS lat, AVG(ST_X(ST_Centroid(geom))) AS lng FROM dim_dma`
        );
        res.json({
          latitude: Number(fallback?.lat) || 51.49,
          longitude: Number(fallback?.lng) || -0.08,
        });
      }
    } catch (e: any) {
      // Ultimate fallback — hardcoded London
      res.json({ latitude: 51.49, longitude: -0.08 });
    }
  });

  // ---- Playbooks --------------------------------------------------------
  app.get("/api/playbooks/:type", async (req: any, res: any) => {
    try {
      // dim_response_playbooks has one row per playbook with action_steps as a JSON column
      const row = await queryOne(
        "SELECT * FROM dim_response_playbooks WHERE incident_type = $1",
        [req.params.type]
      );
      if (!row) return res.status(404).json({ error: "Playbook not found" });
      // Parse action_steps JSON into individual steps
      let steps: any[] = [];
      try {
        steps = typeof row.action_steps === "string" ? JSON.parse(row.action_steps) : row.action_steps ?? [];
      } catch { steps = []; }
      // Map to frontend-expected shape: step_order, action, description
      const mappedSteps = steps.map((s: any, i: number) => ({
        step_order: s.step ?? i + 1,
        action_id: `step-${s.step ?? i + 1}`,
        action: s.action,
        description: s.description,
        responsible: s.responsible,
        sla_minutes: s.sla_minutes,
      }));
      res.json({ incident_type: req.params.type, steps: mappedSteps });
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
        [incidentId, channel, recipient, message, sent_by, new Date(demoNow()).toISOString()]
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
          [incidentId, action.action_id, action.decision, action.note ?? null, new Date(demoNow()).toISOString()]
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
        [incidentId, channel, message_template, target_audience, new Date(demoNow()).toISOString()]
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
        hoursElapsed = (demoNow() - new Date(createdAt).getTime()) / 3_600_000;
      }

      const gracePeriod = rule("OFWAT_GRACE_PERIOD");
      const penaltyRate = rule("OFWAT_PENALTY_RATE");
      const penaltyHours = Math.max(0, hoursElapsed - gracePeriod);
      const penalty = totalProperties * penaltyHours * penaltyRate;

      const dwiVerbal = rule("DWI_VERBAL_DEADLINE");
      const dwiWritten = rule("DWI_WRITTEN_DEADLINE");
      const ofwat3h = rule("OFWAT_GRACE_PERIOD");
      const ofwat12h = rule("OFWAT_ESCALATION_THRESHOLD");

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
        rules: {
          penalty_rate: penaltyRate,
          grace_period: gracePeriod,
          cmex_green: rule("CMEX_GREEN_THRESHOLD"),
          cmex_amber: rule("CMEX_AMBER_THRESHOLD"),
        },
        deadlines: {
          dwi_verbal:  { label: "DWI Verbal Notification", hours: dwiVerbal,  status: hoursElapsed >= dwiVerbal  ? "DONE" : "PENDING" },
          dwi_written: { label: "DWI Written Report",      hours: dwiWritten, status: hoursElapsed >= dwiWritten ? "DONE" : "DUE" },
          ofwat_3h:    { label: "Ofwat 3h Threshold",      hours: ofwat3h,    status: hoursElapsed >= ofwat3h    ? "BREACHED" : "OK" },
          ofwat_12h:   { label: "Ofwat 12h Threshold",     hours: ofwat12h,   status: hoursElapsed >= ofwat12h   ? "BREACHED" : "OK" },
        },
        penalty_calculation: {
          formula: `properties x (hours - ${gracePeriod}) x GBP ${penaltyRate}`,
          properties: totalProperties,
          penalty_hours: Math.round(penaltyHours * 100) / 100,
          rate_per_property_hour: penaltyRate,
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
