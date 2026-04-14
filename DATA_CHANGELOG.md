# Data Model Changes — April 2026

Summary of data improvements for the frontend developer to consume.

---

## New Tables

### `gold.dim_regulatory_rules`

Replaces all hardcoded business rules in the app. Query by `rule_id` to get the current value.

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | STRING | Unique key (e.g., `OFWAT_PENALTY_RATE`) |
| `category` | STRING | `penalty`, `regulatory_deadline`, `threshold`, `kpi` |
| `rule_name` | STRING | Human-readable name |
| `description` | STRING | What the rule governs |
| `value_numeric` | DOUBLE | The numeric value |
| `value_text` | STRING | For non-numeric rules (currently all NULL) |
| `unit` | STRING | `GBP/property/hour`, `hours`, `m`, `percent` |
| `effective_from` | STRING | Date the rule took effect |
| `effective_to` | STRING | NULL = still active |
| `source` | STRING | Regulatory source reference |

**Available rule IDs:**

| `rule_id` | Value | Replaces hardcoded... |
|-----------|-------|----------------------|
| `OFWAT_PENALTY_RATE` | 580.0 GBP/property/hour | `£580` in penalty calculation |
| `OFWAT_GRACE_PERIOD` | 3.0 hours | `3 hours` grace before penalties |
| `DWI_VERBAL_DEADLINE` | 1.0 hours | `1 hour` DWI verbal notification deadline |
| `DWI_WRITTEN_DEADLINE` | 24.0 hours | `24 hours` DWI written report deadline |
| `OFWAT_ESCALATION_THRESHOLD` | 12.0 hours | `12 hours` Ofwat escalation threshold |
| `PRESSURE_RED_THRESHOLD` | 15.0 m | `10m` / `15m` RED threshold |
| `PRESSURE_AMBER_THRESHOLD` | 25.0 m | `15m` / `25m` AMBER threshold |
| `DEFAULT_BASE_PRESSURE` | 25.0 m | `25m` fallback for NULL base_pressure |
| `IMPACT_HIGH_THRESHOLD` | 0.0 m | `0m` effective pressure = no water |
| `IMPACT_MEDIUM_THRESHOLD` | 5.0 m | `5m` effective pressure = very low |
| `IMPACT_LOW_THRESHOLD` | 10.0 m | `10m` effective pressure = reduced |
| `CMEX_GREEN_THRESHOLD` | 70.0 % | `70%` comms rate = GREEN |
| `CMEX_AMBER_THRESHOLD` | 40.0 % | `40%` comms rate = AMBER |

**Suggested usage:**
```sql
-- Load all rules at app startup as a key-value map
SELECT rule_id, value_numeric, value_text, unit
FROM water_digital_twin.gold.dim_regulatory_rules
WHERE effective_to IS NULL
```

---

### `gold.incident_outstanding_actions`

Provides pending actions for the Shift Handover page. Previously this was always empty (`[]`).

| Column | Type | Description |
|--------|------|-------------|
| `action_id` | STRING | e.g., `ACT-2026-0407-001` |
| `incident_id` | STRING | FK to `dim_incidents` |
| `action_description` | STRING | What needs to be done |
| `priority` | STRING | `critical`, `high`, `medium`, `low` |
| `assigned_to` | STRING | Operator/team ID (e.g., `OP_DAY_01`) |
| `assigned_role` | STRING | Human-readable role (e.g., `Day Shift Operator`) |
| `due_by` | STRING (timestamp) | Deadline |
| `status` | STRING | `pending`, `in_progress`, `completed` |
| `created_at` | STRING (timestamp) | When the action was created |
| `created_by` | STRING | Who created it |
| `notes` | STRING | Additional context (supplier contacts, quantities, etc.) |

**Current data:** 8 actions for `INC-2026-0407-001` (2 critical, 4 high, 2 medium).

**Suggested query for Shift Handover:**
```sql
SELECT action_id, action_description, priority, assigned_to, assigned_role, due_by, status, notes
FROM water_digital_twin.gold.incident_outstanding_actions
WHERE incident_id = :incident_id AND status != 'completed'
ORDER BY
  CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END,
  due_by
```

---

### `gold.mv_sensor_latest`

Pre-computed latest reading per sensor. Use this instead of the slow `LATERAL JOIN fact_telemetry ORDER BY timestamp DESC LIMIT 1` pattern.

| Column | Type | Description |
|--------|------|-------------|
| `sensor_id` | STRING | FK to `dim_sensor` |
| `dma_code` | STRING | DMA the sensor belongs to |
| `timestamp` | STRING | Timestamp of the latest reading |
| `sensor_type` | STRING | `pressure` or `flow` |
| `value` | DOUBLE | Raw reading value |
| `total_head_pressure` | DOUBLE | Pressure in metres head (NULL for flow sensors) |
| `flow_rate` | DOUBLE | Flow in l/s (NULL for pressure sensors) |
| `quality_flag` | STRING | `good`, `suspect`, etc. |
| `latitude` | DOUBLE | Sensor location |
| `longitude` | DOUBLE | Sensor location |
| `elevation_m` | DOUBLE | Sensor elevation |
| `status` | STRING | Sensor status (`active`, `maintenance`) |

**Replaces:** Any query that joins `dim_sensor` with `fact_telemetry` to get the latest reading.

---

### `gold.vw_dma_summary`

Pre-aggregated DMA-level statistics. Use for root cause chain display and DMA overview panels.

| Column | Type | Description |
|--------|------|-------------|
| `dma_code` | STRING | DMA identifier |
| `dma_name` | STRING | Human-readable name |
| `dma_area_code` | STRING | Borough/area |
| `avg_elevation` | DOUBLE | Average elevation (m) |
| `centroid_latitude` | DOUBLE | For map centering |
| `centroid_longitude` | DOUBLE | For map centering |
| `total_properties` | LONG | Count of properties in DMA |
| `sensitive_premises_count` | LONG | Schools, hospitals, dialysis, care homes |
| `downstream_dma_count` | LONG | DMAs with secondary feed dependency |

**Replaces:** On-the-fly `COUNT(*)` joins against `dim_properties` for totals.

---

## Modified Tables

### `gold.dim_incidents` — 3 new columns

| Column | Type | Description | Previously |
|--------|------|-------------|------------|
| `escalation_risk` | STRING | Risk assessment text | App showed "Monitoring - assess at next review point" |
| `trajectory` | STRING | Incident trend/outlook text | App showed "Pressure stabilising but below normal..." |
| `first_complaint_time` | STRING (timestamp) | When first customer complaint was received | Anomaly lead-time calculation was skipped |

- **Active incident** (`INC-2026-0407-001`): Has real escalation risk ("High — reservoir at 43%..."), trajectory ("Pressure stabilising at 5-12m head but not recovering..."), and first complaint time (`2026-04-07 03:05:00`).
- **Resolved incidents**: Escalation risk starts with "Resolved —", trajectory describes what happened. `first_complaint_time` is NULL where no complaints were received.
- **Anomaly lead time** can now be calculated: `first_complaint_time - start_timestamp` gives the window between anomaly detection and first customer impact.

---

### `silver.dim_properties` — 2 new columns

| Column | Type | Description | Previously |
|--------|------|-------------|------------|
| `base_pressure` | DOUBLE | Expected normal pressure at the property (metres head) | App fell back to 25m |
| `sensitive_premise_type` | STRING | `school`, `hospital`, `dialysis_home`, `care_home`, or NULL | Only `is_sensitive_premise` boolean existed |

- **`base_pressure`** is computed from real physics: DMA supply pressure (derived from reservoir elevation) minus head loss from property elevation. Range: 5.0–63.7m, mean: 48.7m. All 50,000 properties populated — no NULLs.
- **`sensitive_premise_type`** enables filtering by type (e.g., "show me all dialysis homes") rather than just boolean sensitive/not-sensitive.
- **`is_sensitive_premise`** still exists and is unchanged — no breaking changes.

**Customer impact calculator improvement:**
```sql
-- Before: effective_pressure = network_pressure - 25  (hardcoded fallback)
-- After:  effective_pressure = network_pressure - base_pressure  (real per-property value)
SELECT
  p.property_id,
  p.base_pressure,
  s.value AS current_network_pressure,
  s.value - p.base_pressure AS effective_pressure_delta
FROM silver.dim_properties p
JOIN gold.mv_sensor_latest s ON s.dma_code = p.dma_code AND s.sensor_type = 'pressure'
```

---

### `gold.vw_property_pressure` — 1 new column

| Column | Type | Description |
|--------|------|-------------|
| `base_pressure` | DOUBLE | Now passed through from `dim_properties` |

Previously only had `customer_height_m` for pressure impact. Now includes the real base pressure for more accurate impact modelling.

---

## What the Frontend Should Change

### 1. Read thresholds from `dim_regulatory_rules` instead of hardcoding
- Load rules once at app startup (13 rows, trivial query)
- Replace every hardcoded `580`, `3`, `1`, `24`, `12`, `15`, `25`, `70`, `40` with the table values
- Rules have `effective_from`/`effective_to` so the app can support historical rule lookups

### 2. Show Outstanding Actions on Shift Handover
- Query `incident_outstanding_actions WHERE incident_id = X AND status != 'completed'`
- Display as a prioritised checklist with due times, assigned roles, and notes
- Colour-code by priority: critical (red), high (amber), medium (yellow)

### 3. Show real escalation risk and trajectory on Shift Handover / Incident Detail
- `dim_incidents.escalation_risk` replaces the "Monitoring — assess at next review point" placeholder
- `dim_incidents.trajectory` replaces the "Pressure stabilising but below normal..." placeholder
- `dim_incidents.first_complaint_time` enables anomaly lead-time display

### 4. Use `base_pressure` for customer impact calculations
- Replace the hardcoded `25m` fallback with `dim_properties.base_pressure`
- Impact = `base_pressure - current_effective_pressure` (positive = pressure loss)
- All 50K properties have real values — no fallback needed

### 5. Use `mv_sensor_latest` for sensor lists / DMA detail panels
- Replaces the slow `LATERAL JOIN fact_telemetry` pattern
- Single table with latest reading + sensor metadata — one clean query

### 6. Use `vw_dma_summary` for root cause and DMA overview
- `total_properties` for "X properties affected" display
- `sensitive_premises_count` for sensitive premise alerts
- `downstream_dma_count` for cascade impact chain

### 7. Use `sensitive_premise_type` for filtering
- Enable dropdowns: "Show all hospitals", "Show dialysis homes"
- Currently only boolean `is_sensitive_premise` is used

### 8. Compute map centre from data (P2)
- `SELECT avg(centroid_latitude), avg(centroid_longitude) FROM gold.vw_dma_summary`
- Replaces the hardcoded `(-0.08, 51.49)` — auto-centres on whatever geography the data covers
