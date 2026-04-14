# Data Improvement Brief

**Water Utilities Digital Twin Demo**
**For:** Data Engineer
**Purpose:** Improve demo realism by replacing hardcoded values, filling data gaps, and adding missing datasets

---

## 1. Hardcoded Business Rules (should be a config table)

These values are currently hardcoded in the app. Ideally they'd live in a `dim_config` or `dim_regulatory_rules` table so they can be adjusted without code changes.

| Value | What it is | Where used | Suggestion |
|-------|-----------|------------|------------|
| **GBP 580** per property per hour | Ofwat penalty rate (Supply Interruption penalty) | Regulatory penalty calculation | Store in `dim_regulatory_rules` with effective dates |
| **3 hours** grace period before penalties | Ofwat threshold for "significant" interruption | Penalty calc + deadline tracker | Same config table |
| **1 hour** DWI verbal notification deadline | Drinking Water Inspectorate verbal report deadline | Deadline tracker | Same config table |
| **24 hours** DWI written report deadline | DWI formal report deadline | Deadline tracker | Same config table |
| **12 hours** Ofwat escalation threshold | Second Ofwat reporting threshold | Deadline tracker | Same config table |
| **10m** pressure RED threshold | Pressure below which a sensor is flagged RED | Sensor list, DMA detail panel | Already in `dim_dma` (`pressure_red_threshold`) but not used by app |
| **15m** pressure AMBER threshold | Pressure below which a sensor is flagged AMBER | Not currently shown | Exists in `dim_dma` (`pressure_amber_threshold`) |
| **25m** default base pressure | Fallback when `dim_properties.base_pressure` is null | Customer impact what-if calculator | Populate `base_pressure` on every property |
| **0m / 5m / 10m** impact thresholds | Effective pressure thresholds for high/medium/low impact | Customer impact classification | Store in config or derive from property type |
| **70% / 40%** C-MeX comms rate thresholds | Colour thresholds for proactive comms rate bar | Regulatory view | Store in config |

---

## 2. Missing or Incomplete Data in Existing Tables

### `dim_incidents` - missing columns the app falls back on

| Column | Current state | What the app shows instead | Fix |
|--------|-------------|---------------------------|-----|
| `escalation_risk` | NULL | "Monitoring - assess at next review point" | Add real escalation risk assessment per incident |
| `trajectory` | NULL | "Pressure stabilising but below normal..." | Add incident trajectory/trend text |
| `first_complaint_time` | NULL | Anomaly lead-time calculation skipped | Populate from `customer_complaints` join |

### `dim_properties` - missing columns for impact modelling

| Column | Current state | What happens | Fix |
|--------|-------------|-------------|-----|
| `base_pressure` | NULL (likely) | Falls back to 25m | Compute from DMA normal pressure + property elevation |
| `elevation_m` | May be partially populated | Falls back to 0 (breaks impact calc) | Populate from SRTM/OS Terrain data for all properties |
| `sensitive_premise_type` | Only set on some | Sensitive premise filter shows subset | Ensure all hospitals, schools, care homes, dialysis centres are tagged |

### `dim_sensor` - no live readings

| Column | Current state | What happens | Fix |
|--------|-------------|-------------|-----|
| `latest_pressure` / `latest_flow` | Doesn't exist | App joins with `fact_telemetry` LATERAL (slow) | Add a `dim_sensor_latest` materialised view with latest reading per sensor |

### `dim_reservoirs` - sparse data

| Column | Current state | What happens | Fix |
|--------|-------------|-------------|-----|
| `level_pct` | May be NULL | Falls back to 50% (looks fake) | Populate from telemetry or a reservoir_readings fact table |
| `est_supply_hours` | May be NULL | Silently hidden | Compute from level + draw-down rate |

### `dma_summary` - potentially missing fields

| Column | Needed by app | Fix |
|--------|-------------|-----|
| `downstream_dma_count` | Root cause chain display | Compute from DMA topology/graph |
| `total_properties` | Root cause impact text | Aggregate from `dim_properties` |

---

## 3. Outstanding Actions / Playbook Integration

The **Shift Handover** page has an "Outstanding Actions" section that is **always empty**. The app returns `[]` because `incident_events` only contains completed facts.

**What's needed:** A table like `incident_outstanding_actions` with:
- `incident_id`, `action_description`, `priority`, `assigned_to`, `due_by`, `status` (pending/in_progress/completed)

This would show what the incoming shift operator still needs to do.

---

## 4. Data That Would Make the Demo Richer

### Real-time simulation data

The demo snapshot is frozen at **2026-04-07 05:30 UTC**. All time-based queries use fallbacks because `now() - 24h` returns nothing. Options:

1. **Shift timestamps forward** — update all timestamps in the data gen notebooks to be relative to `CURRENT_TIMESTAMP()` so data always looks "fresh"
2. **Add a streaming simulator** — a notebook that continuously generates new telemetry, shifting the incident forward in time

### Additional datasets that would improve the demo

| Dataset | What it enables | Suggested table |
|---------|----------------|-----------------|
| **Crew locations & ETAs** | Show repair crew positions on map, ETA to incident site | `fact_crew_locations` |
| **Weather data** | Correlate pressure events with freeze/thaw or demand spikes | `fact_weather` |
| **Historical incidents** | Trend analysis, recurrence patterns | Already have 3 in `dim_incidents` but only 1 is active |
| **Meter readings** | DMA leakage calculation (inflow vs consumption) | `fact_meter_readings` |
| **Pipe network geometry** | Show actual pipe routes on map (not just DMA polygons) | `dim_pipe_network` with LineString geometry |
| **Customer contact history** | Show individual complaint details, callback status | Extend `customer_complaints` with resolution fields |

### Map centre & initial view

Currently hardcoded to **(-0.08, 51.49)** with zoom 11 (south-east London). Should be computed from the centroid of all DMAs in the dataset, or stored in a config table — so the map auto-centres on whatever geography the demo uses.

---

## 5. Query Performance Considerations

These queries could be slow at scale and would benefit from materialised views or pre-computed tables:

| Query | Current approach | Better approach |
|-------|-----------------|----------------|
| Latest telemetry per sensor | `LATERAL JOIN fact_telemetry ORDER BY timestamp DESC LIMIT 1` | Pre-computed `mv_sensor_latest` updated by SDP pipeline |
| DMA GeoJSON for map | `ST_AsGeoJSON(geom)` on 500 DMAs per request | Cache in a `dma_geojson` table or use app-level cache |
| Regulatory penalty calculation | Computed on-the-fly from `dim_properties` + `dim_incidents` | Pre-computed in a `mv_regulatory_status` view |

---

## Summary Priority

| Priority | Item | Impact |
|----------|------|--------|
| **P0** | Populate `dim_properties.base_pressure` and `elevation_m` | Customer impact calculator is meaningless without these |
| **P0** | Add `incident_outstanding_actions` table | Shift handover is incomplete without pending actions |
| **P1** | Create `dim_config` / `dim_regulatory_rules` table | Removes all hardcoded business rules |
| **P1** | Add `escalation_risk`, `trajectory`, `first_complaint_time` to `dim_incidents` | Handover page shows placeholder text |
| **P1** | Populate `dim_reservoirs.level_pct` and `est_supply_hours` | Reservoir status shows fake 50% fallback |
| **P2** | Compute map centre from data | Map works for London but breaks for other geographies |
| **P2** | Create `mv_sensor_latest` materialised view | Faster sensor list loading |
| **P2** | Add pipe network geometry | Richer map visualisation |
| **P3** | Time-shift demo data to be relative | Eliminates all static-data fallback hacks |
| **P3** | Add crew locations, weather, meter readings | Premium demo features |
