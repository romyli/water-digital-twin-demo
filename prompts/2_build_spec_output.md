# Water Digital Twin — Multi-Agent Build Specification

**Workspace:** `https://adb-984752964297111.11.azuredatabricks.net/` (CLI profile: `adb-98`)
**Asset Root:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo`
**Catalog:** `water_digital_twin`
**Schemas:** `bronze`, `silver`, `gold`
**Demo Timestamp:** `2026-04-07 05:30:00` (the moment the day-shift operator opens the app)

---

## Build Method Classification

| Component | Method | Reason |
|---|---|---|
| Unity Catalog DDL (schemas, tables) | 🔨 CODE | SQL scripts, fully automatable |
| Data generation | 🔨 CODE | Python notebooks, fully automatable |
| SDP Pipeline | 🔨 CODE | Python/SQL notebooks, fully automatable |
| Anomaly scoring / Gold layer | 🔨 CODE | SQL/Python, fully automatable |
| Metric Views | 🔨 CODE | SQL DDL with YAML, fully automatable |
| Databricks App (React + FastAPI) | 🔨 CODE | Code + `databricks apps deploy`, fully automatable |
| Lakebase table DDL | 🔨 CODE | SQL DDL via Lakebase endpoint, fully automatable |
| Genie Spaces (both) | 🔨 CODE + 📋 MANUAL GUIDE | Verified SQL queries developed/tested as code (Phase 1); space creation/config done via UI guide (Phase 2) — `serialized_space` JSON is fragile and sample questions need iterative UI validation |
| Lakeview Dashboard | 📋 MANUAL GUIDE | `serialized_dashboard` JSON too verbose to author from scratch; tile layout and visualisation types need visual tweaking |
| Preview feature enablement | 📋 MANUAL GUIDE | Admin UI toggles and support tickets — cannot be scripted |
| Lakebase instance creation | 📋 MANUAL GUIDE | One-time workspace setup — easier via UI than CLI for initial provisioning |
| ML anomaly detection notebook (optional) | 🔨 CODE | Python notebook with pre-run results, fully automatable |
| Workflow/Job creation | 📋 MANUAL GUIDE | Simple enough to configure via UI; agent provides exact task names, dependencies, and cluster specs |

---

## Agent 1: Diana — Platform & Admin Engineer

### Task 1 — Preview Features & Workspace Setup (📋 MANUAL GUIDE)

**Goal:** Produce a numbered, step-by-step guide for the SA to enable all required preview features and provision infrastructure in the Databricks workspace at `https://adb-984752964297111.11.azuredatabricks.net/`.

**Outputs:** A markdown document at `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides/01_workspace_setup_guide.md` containing the full SA walkthrough.

#### Part A — Preview Feature Enablement

For each preview feature below, the guide must include: (1) feature name and what it enables, (2) whether it's Private Preview (support ticket required — provide ticket template) or Public Preview (admin toggle), (3) exact navigation path, (4) dependencies/ordering, (5) verification step.

**Features to enable:**

1. **Spatial SQL with Native GEOMETRY/GEOGRAPHY Types**
   - What it enables: 80+ geospatial functions (ST_Point, ST_Contains, ST_Distance, ST_Buffer, ST_Area, etc.) plus native GEOMETRY and GEOGRAPHY data types. Required for SDP spatial joins, Genie spatial queries, and map data generation.
   - Stage: **Public Preview**
   - Navigation: Workspace Admin Console → Settings → Previews → search "Spatial" → enable "Spatial SQL"
   - Dependencies: Must be enabled before running Alice's schema DDL (Task 4) or Bob's data generation (Task 6)
   - Verification: Open a SQL Warehouse query editor and run:
     ```sql
     SELECT ST_Point(-0.05, 51.45) AS test_geometry
     ```
     Expected: returns a valid geometry value (not an error)

2. **Metric Views with Semantic Metadata**
   - What it enables: Reusable governed business metrics as Unity Catalog objects with semantic metadata (descriptions, display names, format hints). Improves Genie Space accuracy by giving the LLM explicit context about metrics.
   - Stage: **Public Preview**
   - Navigation: Workspace Admin Console → Settings → Previews → search "Metric Views" → enable
   - Dependencies: Must be enabled before Bob creates metric views (Task 7c)
   - Verification: Run:
     ```sql
     CREATE OR REPLACE METRIC VIEW water_digital_twin.gold.mv_test AS
     SELECT 1 AS test_value;
     DROP METRIC VIEW IF EXISTS water_digital_twin.gold.mv_test;
     ```
     Expected: both statements succeed without error

3. **`ai_forecast()` AI Function**
   - What it enables: Built-in time-series forecasting directly in SQL. Used in Bob's optional ML notebook (Task 7d, Level 1).
   - Stage: **Public Preview**
   - Navigation: Workspace Admin Console → Settings → Previews → search "ai_forecast" → enable
   - Dependencies: Requires a SQL Warehouse with the feature enabled
   - Verification: Run:
     ```sql
     SELECT ai_forecast(ARRAY(1.0, 2.0, 3.0, 4.0, 5.0), 3) AS forecast_test
     ```
     Expected: returns forecast values (not a function-not-found error)

#### Part B — Lakebase Instance Provisioning

1. Navigate to: `https://adb-984752964297111.11.azuredatabricks.net/` → left sidebar → "Lakebase" (or "SQL" → "Lakebase")
2. Click **"Create Project"**
   - Project name: `water-digital-twin-lakebase`
   - Region: same as workspace (check workspace settings — should be the Azure region where the workspace is deployed)
3. Wait for provisioning (typically 2-5 minutes). The status will change from "Provisioning" to "Running".
4. Once running, note the **connection endpoint URL** — this will be in the format: `<hostname>:<port>/<database>`. Record this for Alice's Task 5 (Lakebase DDL).
5. Verify connection: Open a notebook attached to any cluster and run:
   ```python
   # Using Lakebase Data API (preferred)
   import requests
   # Or via SQL: connect to the Lakebase endpoint and run SELECT 1
   ```
   Or from the Lakebase SQL editor: `SELECT 1` — should return `1`.

#### Part C — Unity Catalog Setup

1. Navigate to: Catalog Explorer → click "+" → "Create Catalog"
2. Catalog name: `water_digital_twin`
3. Create three schemas within the catalog:
   ```sql
   CREATE SCHEMA IF NOT EXISTS water_digital_twin.bronze;
   CREATE SCHEMA IF NOT EXISTS water_digital_twin.silver;
   CREATE SCHEMA IF NOT EXISTS water_digital_twin.gold;
   ```
4. Verify: navigate to Catalog Explorer → `water_digital_twin` → confirm `bronze`, `silver`, `gold` schemas exist.

**Inputs:** Approved plan Section 3 (preview features), workspace URL
**Outputs:** Step-by-step SA guide; Lakebase connection endpoint URL; confirmation of catalog/schema creation
**Acceptance Criteria:**
- [ ] Guide covers all 3 preview features with exact navigation paths
- [ ] Each preview feature has a verification step that the SA can run
- [ ] Lakebase provisioning guide includes connection string
- [ ] Unity Catalog `water_digital_twin` with `bronze`/`silver`/`gold` schemas exists
- [ ] SA can follow the guide without additional research

---

### Task 2 — Orchestration (📋 MANUAL GUIDE)

**Goal:** Produce a step-by-step guide for the SA to create the Databricks Workflow via the Jobs UI that orchestrates the full data pipeline.

**Outputs:** A markdown document at `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides/02_orchestration_guide.md`.

**Step-by-step creation:**

1. Navigate to: `https://adb-984752964297111.11.azuredatabricks.net/` → left sidebar → "Workflows" → "Create Job"
2. Job name: `Water Digital Twin — Full Pipeline`
3. **Shared job cluster configuration:**
   - Cluster name: `water-dt-pipeline`
   - Node type: `Standard_DS3_v2` (or equivalent — 4 cores, 14GB RAM)
   - Workers: 2-4 (auto-scaling)
   - Spark config: `spark.databricks.delta.preview.enabled true`
   - DBR version: 15.4 LTS or later (must support Spatial SQL)
   - For Task 7d (ML notebook only): separate cluster with **DBR ML Runtime 15.4+**

4. **Task dependency graph:**

   | Task Name | Type | Source Path | Depends On | Notes |
   |---|---|---|---|---|
   | `01_schema_ddl` | SQL notebook | `/Water Digital Twin Demo/notebooks/01_schema_ddl.sql` | — | Alice's Task 4 — creates all tables |
   | `02_data_gen_geography` | Python notebook | `/Water Digital Twin Demo/notebooks/02_data_gen_geography.py` | `01_schema_ddl` | Bob's Task 6 — DMA polygons first |
   | `03_data_gen_dimensions` | Python notebook | `/Water Digital Twin Demo/notebooks/03_data_gen_dimensions.py` | `02_data_gen_geography` | Bob's Task 6 — sensors, properties, assets |
   | `04_data_gen_facts` | Python notebook | `/Water Digital Twin Demo/notebooks/04_data_gen_facts.py` | `03_data_gen_dimensions` | Bob's Task 6 — telemetry, complaints |
   | `05_data_gen_incidents` | Python notebook | `/Water Digital Twin Demo/notebooks/05_data_gen_incidents.py` | `03_data_gen_dimensions` | Bob's Task 6 — incidents, events, comms, playbooks |
   | `06_sdp_pipeline` | SDP Pipeline | `/Water Digital Twin Demo/notebooks/06_sdp_pipeline.py` | `04_data_gen_facts`, `05_data_gen_incidents` | Bob's Task 7 — Bronze→Silver→Gold |
   | `07_anomaly_scoring` | Python notebook | `/Water Digital Twin Demo/notebooks/07_anomaly_scoring.py` | `06_sdp_pipeline` | Bob's Task 7b — anomaly scores, RAG, DMA status/summary |
   | `08_metric_views` | SQL notebook | `/Water Digital Twin Demo/notebooks/08_metric_views.sql` | `07_anomaly_scoring` | Bob's Task 7c — mv_ metric views |
   | `09_lakebase_sync` | Python notebook | `/Water Digital Twin Demo/notebooks/09_lakebase_sync.py` | `07_anomaly_scoring` | Alice's Task 5 — Delta Gold → Lakebase |

5. Schedule/trigger: **Manual trigger** for demo setup. Optionally add a scheduled trigger for periodic re-seed (e.g., weekly).
6. Verification: Run the workflow end-to-end. All 9 tasks should complete successfully. Check the final task's output for confirmation.

**Inputs:** Alice's folder layout (Task 3); all notebook paths from Alice and Bob
**Outputs:** Workflow configuration guide with exact task names, paths, dependencies, and cluster specs
**Acceptance Criteria:**
- [ ] Guide specifies every task with correct notebook paths
- [ ] Task dependencies form a valid DAG (no cycles)
- [ ] Cluster specs are appropriate for the workload
- [ ] SA can follow the guide to create the workflow without additional research

---

## Agent 2: Alice — Lead Data Architect

### Task 3 — Repo Structure (🔨 CODE)

**Goal:** Create the workspace folder layout for all demo assets.

**Workspace root:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo`

**Folder structure:**
```
Water Digital Twin Demo/
├── notebooks/
│   ├── 01_schema_ddl.sql              # Alice Task 4 — Unity Catalog DDL
│   ├── 02_data_gen_geography.py       # Bob Task 6 — DMA polygon generation
│   ├── 03_data_gen_dimensions.py      # Bob Task 6 — sensors, properties, assets, reservoirs
│   ├── 04_data_gen_facts.py           # Bob Task 6 — telemetry, complaints
│   ├── 05_data_gen_incidents.py       # Bob Task 6 — incidents, events, comms, playbooks, handovers
│   ├── 06_sdp_pipeline.py             # Bob Task 7 — SDP Bronze→Silver→Gold
│   ├── 07_anomaly_scoring.py          # Bob Task 7b — anomaly scores, RAG history, DMA status/summary
│   ├── 08_metric_views.sql            # Bob Task 7c — metric view DDL
│   ├── 09_lakebase_sync.py            # Alice Task 5 — Delta Gold → Lakebase sync
│   └── 10_ml_anomaly_detection.py     # Bob Task 7d (optional) — ML notebook
├── app/
│   ├── app.yaml                       # Databricks App config
│   ├── backend/
│   │   ├── main.py                    # FastAPI backend
│   │   ├── requirements.txt
│   │   └── ...
│   └── frontend/
│       ├── package.json
│       ├── src/
│       │   └── ...                    # React app
│       └── ...
├── genie/
│   ├── operator_verified_queries.sql  # Charlie Task 9a Phase 1 — tested operator Genie queries
│   └── executive_verified_queries.sql # Charlie Task 9b Phase 1 — tested executive Genie queries
├── guides/
│   ├── 01_workspace_setup_guide.md    # Diana Task 1 — preview features + Lakebase
│   ├── 02_orchestration_guide.md      # Diana Task 2 — workflow creation
│   ├── 03_genie_operator_guide.md     # Charlie Task 9a Phase 2 — operator Genie Space
│   ├── 04_genie_executive_guide.md    # Charlie Task 9b Phase 2 — executive Genie Space
│   └── 05_dashboard_guide.md          # Charlie Task 10 — Lakeview dashboard
├── scripts/
│   ├── reset_demo.py                  # Eve Task 11 — teardown + re-seed
│   └── demo_health_check.py           # Eve Task 11 — 22-query health check
├── docs/
│   ├── README.md                      # Eve Task 11 — runbook
│   └── demo_talk_track.md             # Eve Task 12 — live demo script
└── dashboards/
    └── (placeholder — dashboard created via UI per Charlie Task 10)
```

**Deploy via CLI:**
```bash
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/app" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/genie" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/scripts" --profile adb-98
databricks workspace mkdirs "/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/docs" --profile adb-98
```

**Acceptance Criteria:**
- [ ] All directories exist in the workspace
- [ ] Folder layout matches the structure above
- [ ] All downstream agents reference these exact paths

---

### Task 4 — Schema Definitions / Unity Catalog DDL (🔨 CODE)

**Goal:** Create executable SQL scripts that define ALL tables across Bronze, Silver, and Gold layers in Unity Catalog. This is the single source of truth for all downstream agents.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/01_schema_ddl.sql`

**All tables use three-part names under catalog `water_digital_twin`.** The script must be idempotent (use `CREATE TABLE IF NOT EXISTS` or `CREATE OR REPLACE`).

#### Bronze Layer DDL

```sql
-- Catalog and schemas
CREATE CATALOG IF NOT EXISTS water_digital_twin;
CREATE SCHEMA IF NOT EXISTS water_digital_twin.bronze;
CREATE SCHEMA IF NOT EXISTS water_digital_twin.silver;
CREATE SCHEMA IF NOT EXISTS water_digital_twin.gold;

-- Bronze: raw_telemetry
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_telemetry (
  sensor_id STRING COMMENT 'Sensor identifier from OSI PI',
  timestamp TIMESTAMP COMMENT 'Reading timestamp (UTC)',
  value DOUBLE COMMENT 'Raw sensor reading value (pressure in metres head, or flow in l/s)',
  quality_flag STRING COMMENT 'PI quality indicator: good, suspect, bad',
  source_system STRING COMMENT 'Always osi_pi for this demo',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw sensor telemetry ingested from OSI PI';

-- Bronze: raw_assets
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_assets (
  asset_id STRING COMMENT 'Asset identifier',
  asset_type STRING COMMENT 'Asset type from source system',
  name STRING COMMENT 'Asset display name',
  status STRING COMMENT 'Raw status string',
  latitude DOUBLE COMMENT 'Latitude (WGS84)',
  longitude DOUBLE COMMENT 'Longitude (WGS84)',
  geometry_wkt STRING COMMENT 'WKT geometry (POINT, LINESTRING, or POLYGON)',
  metadata_json STRING COMMENT 'Additional properties as JSON',
  source_system STRING COMMENT 'Source: gis, scada, sap',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw asset data from GIS/SCADA/SAP';

-- Bronze: raw_customer_contacts
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_customer_contacts (
  uprn STRING COMMENT 'Unique Property Reference Number',
  address STRING COMMENT 'Full address string',
  postcode STRING COMMENT 'UK postcode',
  latitude DOUBLE COMMENT 'Property latitude',
  longitude DOUBLE COMMENT 'Property longitude',
  property_type STRING COMMENT 'Raw property classification',
  dma_code STRING COMMENT 'DMA assignment from source',
  customer_height DOUBLE COMMENT 'Property elevation in metres',
  source_system STRING COMMENT 'sap or salesforce',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw customer/property data from SAP/Salesforce';

-- Bronze: raw_complaints
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_complaints (
  complaint_id STRING COMMENT 'Complaint identifier',
  uprn STRING COMMENT 'Property UPRN',
  dma_code STRING COMMENT 'DMA code',
  complaint_timestamp TIMESTAMP COMMENT 'When complaint was received',
  complaint_type STRING COMMENT 'Raw complaint type',
  source_system STRING COMMENT 'Always salesforce',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw customer complaints from Salesforce';

-- Bronze: raw_dma_boundaries
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_dma_boundaries (
  dma_code STRING COMMENT 'DMA identifier',
  dma_name STRING COMMENT 'DMA display name',
  dma_area_code STRING COMMENT 'DMA area code',
  geometry_wkt STRING COMMENT 'WKT POLYGON boundary',
  source_system STRING COMMENT 'Always gis',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw DMA boundary polygons from GIS';

-- Bronze: raw_pma_boundaries
CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_pma_boundaries (
  pma_code STRING COMMENT 'PMA identifier',
  pma_name STRING COMMENT 'PMA display name',
  dma_code STRING COMMENT 'Parent DMA code',
  geometry_wkt STRING COMMENT 'WKT POLYGON boundary',
  source_system STRING COMMENT 'Always gis',
  ingested_at TIMESTAMP COMMENT 'SDP ingestion timestamp'
) COMMENT 'Raw PMA boundary polygons from GIS';
```

#### Silver Layer DDL

```sql
-- Silver: dim_sensor
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_sensor (
  sensor_id STRING COMMENT 'PK. Sensor identifier (e.g. DEMO_SENSOR_01, DEMO_FLOW_01)',
  name STRING COMMENT 'Sensor display name',
  sensor_type STRING COMMENT 'Enum: pressure, flow',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  pma_code STRING COMMENT 'FK → dim_pma.pma_code (nullable)',
  latitude DOUBLE COMMENT 'Sensor latitude (WGS84)',
  longitude DOUBLE COMMENT 'Sensor longitude (WGS84)',
  elevation DOUBLE COMMENT 'Sensor elevation in metres above sea level',
  geometry_wkt STRING COMMENT 'WKT POINT geometry',
  h3_index STRING COMMENT 'H3 cell index at resolution 8',
  is_active BOOLEAN COMMENT 'Whether sensor is currently reporting',
  installed_date DATE COMMENT 'Installation date'
) COMMENT 'Cleansed sensor dimension — 10,000 sensors (8,000 pressure + 2,000 flow)';

-- Silver: dim_dma
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_dma (
  dma_code STRING COMMENT 'PK. DMA identifier (e.g. DEMO_DMA_01)',
  dma_name STRING COMMENT 'DMA display name (e.g. Crystal Palace South)',
  dma_area_code STRING COMMENT 'DMA area code',
  geometry_wkt STRING COMMENT 'WKT POLYGON boundary',
  centroid_latitude DOUBLE COMMENT 'Polygon centroid latitude',
  centroid_longitude DOUBLE COMMENT 'Polygon centroid longitude',
  avg_elevation DOUBLE COMMENT 'Average ground elevation in metres',
  h3_index STRING COMMENT 'H3 cell index of centroid at resolution 7',
  pressure_red_threshold DOUBLE COMMENT 'Pressure below this = RED (default 15.0m)',
  pressure_amber_threshold DOUBLE COMMENT 'Pressure below this = AMBER (default 25.0m)'
) COMMENT 'Cleansed DMA dimension — 500 DMAs covering Greater London';

-- Silver: dim_pma
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_pma (
  pma_code STRING COMMENT 'PK. PMA identifier',
  pma_name STRING COMMENT 'PMA display name',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  geometry_wkt STRING COMMENT 'WKT POLYGON boundary',
  centroid_latitude DOUBLE COMMENT 'Polygon centroid latitude',
  centroid_longitude DOUBLE COMMENT 'Polygon centroid longitude'
) COMMENT 'Cleansed PMA dimension — ~100 PMAs';

-- Silver: dim_properties
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_properties (
  uprn STRING COMMENT 'PK. Unique Property Reference Number',
  address STRING COMMENT 'Street address',
  postcode STRING COMMENT 'UK postcode',
  property_type STRING COMMENT 'Enum: domestic, school, hospital, commercial, nursery, key_account, dialysis_home',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  pma_code STRING COMMENT 'FK → dim_pma.pma_code (nullable)',
  customer_height DOUBLE COMMENT 'Property elevation in metres above sea level',
  latitude DOUBLE COMMENT 'Property latitude (WGS84)',
  longitude DOUBLE COMMENT 'Property longitude (WGS84)',
  geometry_wkt STRING COMMENT 'WKT POINT geometry',
  h3_index STRING COMMENT 'H3 cell index at resolution 8'
) COMMENT 'Cleansed property dimension — 50,000 properties';

-- Silver: dim_assets
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_assets (
  asset_id STRING COMMENT 'PK. Asset identifier (e.g. DEMO_PUMP_01, DEMO_TM_001)',
  asset_type STRING COMMENT 'Enum: pump_station, trunk_main, isolation_valve, prv, treatment_works',
  name STRING COMMENT 'Asset display name',
  status STRING COMMENT 'Enum: operational, tripped, failed, maintenance, decommissioned',
  latitude DOUBLE COMMENT 'Asset latitude (NULL for linear assets)',
  longitude DOUBLE COMMENT 'Asset longitude (NULL for linear assets)',
  geometry_wkt STRING COMMENT 'WKT geometry — POINT for stations/valves, LINESTRING for trunk mains',
  diameter_inches INT COMMENT 'Pipe diameter (trunk_main only, NULL otherwise)',
  length_km DOUBLE COMMENT 'Asset length in km (trunk_main only, NULL otherwise)',
  trip_timestamp TIMESTAMP COMMENT 'When the asset tripped/failed (NULL if operational)',
  installed_date DATE COMMENT 'Installation date'
) COMMENT 'Cleansed asset dimension — pump stations, trunk mains, valves, PRVs';

-- Silver: dim_asset_dma_feed
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_asset_dma_feed (
  asset_id STRING COMMENT 'FK → dim_assets.asset_id',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  feed_type STRING COMMENT 'Enum: primary, secondary'
) COMMENT 'Mapping: upstream assets → DMAs they feed';

-- Silver: dim_reservoirs
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_reservoirs (
  reservoir_id STRING COMMENT 'PK. Reservoir identifier (e.g. DEMO_SR_01)',
  name STRING COMMENT 'Reservoir display name',
  capacity_ml DOUBLE COMMENT 'Total capacity in megalitres',
  current_level_pct DOUBLE COMMENT 'Current fill level as percentage (0-100)',
  hourly_demand_rate_ml DOUBLE COMMENT 'Current demand rate in megalitres/hour',
  latitude DOUBLE COMMENT 'Reservoir latitude (WGS84)',
  longitude DOUBLE COMMENT 'Reservoir longitude (WGS84)',
  geometry_wkt STRING COMMENT 'WKT POINT geometry'
) COMMENT 'Service reservoir dimension — 20 reservoirs';

-- Silver: dim_reservoir_dma_feed
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_reservoir_dma_feed (
  reservoir_id STRING COMMENT 'FK → dim_reservoirs.reservoir_id',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  feed_type STRING COMMENT 'Enum: primary, secondary'
) COMMENT 'Mapping: reservoirs → DMAs they feed';

-- Silver: fact_telemetry
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.fact_telemetry (
  sensor_id STRING COMMENT 'FK → dim_sensor.sensor_id',
  timestamp TIMESTAMP COMMENT 'Reading timestamp (15-min intervals, UTC)',
  sensor_type STRING COMMENT 'Enum: pressure, flow. Denormalised from dim_sensor.',
  value DOUBLE COMMENT 'Pressure reading in metres head (NULL for flow sensors)',
  total_head_pressure DOUBLE COMMENT 'Absolute pressure at sensor (NULL for flow sensors)',
  flow_rate DOUBLE COMMENT 'Flow rate in l/s (NULL for pressure sensors)',
  quality_flag STRING COMMENT 'Data quality: good, suspect, bad'
) COMMENT 'Cleansed sensor telemetry — 15-minute intervals';

-- Silver: customer_complaints
CREATE TABLE IF NOT EXISTS water_digital_twin.silver.customer_complaints (
  complaint_id STRING COMMENT 'PK. Complaint identifier',
  uprn STRING COMMENT 'FK → dim_properties.uprn',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  complaint_timestamp TIMESTAMP COMMENT 'When complaint was received',
  complaint_type STRING COMMENT 'Enum: no_water, low_pressure, discoloured_water, other'
) COMMENT 'Cleansed customer complaints';
```

#### Gold Layer DDL — Computed Status Tables

```sql
-- Gold: dma_status
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_status (
  dma_code STRING COMMENT 'PK. FK → dim_dma.dma_code',
  rag_status STRING COMMENT 'Enum: GREEN, AMBER, RED. Current status from latest 15-min window.',
  avg_pressure DOUBLE COMMENT 'Average pressure across all sensors in the DMA (metres head)',
  min_pressure DOUBLE COMMENT 'Minimum sensor pressure in the DMA',
  sensor_count INT COMMENT 'Number of active sensors in the DMA',
  property_count INT COMMENT 'Total properties in the DMA',
  sensitive_premises_count INT COMMENT 'Count of hospital + school + dialysis_home properties',
  has_active_incident BOOLEAN COMMENT 'TRUE if DMA has an active incident in dim_incidents',
  last_updated TIMESTAMP COMMENT 'When this row was last refreshed'
) COMMENT 'Current RAG status per DMA — primary table for map colouring and quick-filters';

-- Gold: dma_rag_history
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_rag_history (
  dma_code STRING COMMENT 'Composite PK (dma_code, timestamp). FK → dim_dma.',
  timestamp TIMESTAMP COMMENT '15-minute interval timestamp',
  rag_status STRING COMMENT 'Enum: GREEN, AMBER, RED. RAG status at this interval.',
  avg_pressure DOUBLE COMMENT 'Average pressure at this interval',
  min_pressure DOUBLE COMMENT 'Minimum pressure at this interval'
) COMMENT 'Historical RAG status per DMA per 15-min interval — enables timeline strip';

-- Gold: anomaly_scores
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.anomaly_scores (
  sensor_id STRING COMMENT 'Composite PK (sensor_id, timestamp). FK → dim_sensor.',
  timestamp TIMESTAMP COMMENT 'Telemetry reading timestamp',
  anomaly_sigma DOUBLE COMMENT 'Std devs from 7-day same-time-of-day baseline. >3.0 = high-confidence anomaly.',
  baseline_value DOUBLE COMMENT 'Expected value (7-day average for this time-of-day)',
  actual_value DOUBLE COMMENT 'Observed sensor reading',
  is_anomaly BOOLEAN COMMENT 'TRUE if anomaly_sigma > 2.5',
  scoring_method STRING COMMENT 'Enum: statistical, ai_forecast, automl, foundation_model. Default: statistical.'
) COMMENT 'Anomaly scores for each sensor reading — supports multiple scoring methods';

-- Gold: dma_summary
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_summary (
  dma_code STRING COMMENT 'PK. FK → dim_dma.dma_code',
  dma_name STRING COMMENT 'DMA display name (denormalised)',
  rag_status STRING COMMENT 'Current RAG status (denormalised from dma_status)',
  avg_pressure DOUBLE COMMENT 'Average pressure in metres head',
  avg_flow DOUBLE COMMENT 'Average inlet flow rate in l/s',
  property_count INT COMMENT 'Total properties in DMA',
  sensor_count INT COMMENT 'Active sensor count',
  sensitive_premises_count INT COMMENT 'Hospital + school + dialysis_home count',
  feeding_reservoir_id STRING COMMENT 'FK → dim_reservoirs.reservoir_id',
  reservoir_level_pct DOUBLE COMMENT 'Current reservoir fill level %',
  reservoir_hours_remaining DOUBLE COMMENT 'Estimated hours of supply',
  active_incident_id STRING COMMENT 'FK → dim_incidents.incident_id (NULL if no active incident)',
  active_complaints_count INT COMMENT 'Number of complaints in last 24 hours',
  last_updated TIMESTAMP COMMENT 'Last refresh timestamp'
) COMMENT 'Pre-materialised DMA summary — powers DMA detail panel for sub-second rendering';
```

#### Gold Layer DDL — Incident Management Tables

```sql
-- Gold: dim_incidents
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dim_incidents (
  incident_id STRING COMMENT 'PK. Format: INC-YYYY-MMDD-NNN',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code. Primary affected DMA.',
  root_cause_asset_id STRING COMMENT 'FK → dim_assets.asset_id',
  start_timestamp TIMESTAMP COMMENT 'Incident start time',
  end_timestamp TIMESTAMP COMMENT 'Incident end time (NULL if ongoing)',
  status STRING COMMENT 'Enum: active, resolved, closed',
  severity STRING COMMENT 'Enum: low, medium, high, critical',
  total_properties_affected INT COMMENT 'Total properties impacted',
  sensitive_premises_affected BOOLEAN COMMENT 'TRUE if hospitals/schools/dialysis_home affected'
) COMMENT 'Incident dimension — 1 active + 5-10 historical';

-- Gold: incident_events
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.incident_events (
  event_id STRING COMMENT 'PK. Auto-generated unique ID',
  incident_id STRING COMMENT 'FK → dim_incidents.incident_id',
  timestamp TIMESTAMP COMMENT 'Event timestamp',
  event_type STRING COMMENT 'Enum: asset_trip, anomaly_detected, alert_acknowledged, rag_change, customer_complaint, crew_dispatched, regulatory_notification, customer_comms, shift_handover',
  source STRING COMMENT 'Enum: scada, platform, operator, salesforce',
  description STRING COMMENT 'Human-readable event description',
  operator_id STRING COMMENT 'Operator who triggered the event (NULL for automated)'
) COMMENT 'Chronological incident event log — for alarm log and audit trail';

-- Gold: communications_log
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.communications_log (
  log_id STRING COMMENT 'PK. Auto-generated unique ID',
  incident_id STRING COMMENT 'FK → dim_incidents.incident_id',
  timestamp TIMESTAMP COMMENT 'Communication timestamp',
  contact_role STRING COMMENT 'Who was contacted (e.g. Network Manager, DWI Duty Officer)',
  method STRING COMMENT 'Enum: phone, email, teams',
  summary STRING COMMENT 'Brief description of the communication',
  action_agreed STRING COMMENT 'What was agreed/decided',
  operator_id STRING COMMENT 'Operator who made the communication'
) COMMENT 'Structured communications log during incidents';

-- Gold: dim_response_playbooks
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dim_response_playbooks (
  playbook_id STRING COMMENT 'PK. Auto-generated unique ID',
  incident_type STRING COMMENT 'Enum: pump_station_trip, trunk_main_burst, prv_failure, water_quality',
  sop_reference STRING COMMENT 'SOP document reference (e.g. SOP-WN-042)',
  action_steps STRING COMMENT 'JSON array: [{step_number, description, is_mandatory, typical_timeframe}]',
  last_updated_by STRING COMMENT 'Who last modified the playbook',
  last_updated_at TIMESTAMP COMMENT 'Last modification timestamp'
) COMMENT 'Response playbooks with SOP action steps';

-- Gold: shift_handovers
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.shift_handovers (
  handover_id STRING COMMENT 'PK. Auto-generated unique ID',
  incident_id STRING COMMENT 'FK → dim_incidents.incident_id',
  outgoing_operator STRING COMMENT 'Operator handing over',
  incoming_operator STRING COMMENT 'Operator receiving',
  generated_summary STRING COMMENT 'Auto-generated narrative from Mosaic AI',
  risk_of_escalation STRING COMMENT 'Enum: low, medium, high',
  current_trajectory STRING COMMENT 'Enum: stabilising, deteriorating, recovering',
  operator_edits STRING COMMENT 'Manual changes by outgoing operator',
  signed_off_at TIMESTAMP COMMENT 'When outgoing operator signed off',
  acknowledged_at TIMESTAMP COMMENT 'When incoming operator acknowledged'
) COMMENT 'Structured shift handover records';

-- Gold: comms_requests
CREATE TABLE IF NOT EXISTS water_digital_twin.gold.comms_requests (
  request_id STRING COMMENT 'PK. Auto-generated unique ID',
  incident_id STRING COMMENT 'FK → dim_incidents.incident_id',
  dma_code STRING COMMENT 'FK → dim_dma.dma_code',
  requested_by STRING COMMENT 'Operator ID who triggered the request',
  requested_at TIMESTAMP COMMENT 'Request timestamp',
  message_template STRING COMMENT 'Pre-drafted customer notification text',
  affected_postcodes STRING COMMENT 'Comma-separated list of affected postcodes',
  estimated_restoration_time STRING COMMENT 'Manually set by operator',
  customer_count INT COMMENT 'Number of customers to be notified',
  status STRING COMMENT 'Enum: pending, sent, cancelled'
) COMMENT 'Proactive customer comms requests';
```

#### Gold Layer DDL — Views

```sql
-- Gold: vw_dma_pressure
CREATE OR REPLACE VIEW water_digital_twin.gold.vw_dma_pressure AS
SELECT
  s.dma_code,
  t.timestamp,
  AVG(t.value) AS avg_pressure,
  MAX(t.value) AS max_pressure,
  MIN(t.value) AS min_pressure,
  AVG(t.total_head_pressure) AS avg_total_head_pressure,
  COUNT(t.sensor_id) AS reading_count
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s ON t.sensor_id = s.sensor_id
WHERE t.sensor_type = 'pressure'
GROUP BY s.dma_code, t.timestamp;

-- Gold: vw_pma_pressure
CREATE OR REPLACE VIEW water_digital_twin.gold.vw_pma_pressure AS
SELECT
  s.pma_code,
  t.timestamp,
  AVG(t.value) AS avg_pressure,
  MAX(t.value) AS max_pressure,
  MIN(t.value) AS min_pressure,
  COUNT(t.sensor_id) AS reading_count
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s ON t.sensor_id = s.sensor_id
WHERE t.sensor_type = 'pressure' AND s.pma_code IS NOT NULL
GROUP BY s.pma_code, t.timestamp;

-- Gold: vw_property_pressure
CREATE OR REPLACE VIEW water_digital_twin.gold.vw_property_pressure AS
SELECT
  p.uprn,
  p.dma_code,
  p.property_type,
  p.customer_height,
  t.timestamp,
  t.total_head_pressure,
  (t.total_head_pressure - p.customer_height) AS effective_pressure
FROM water_digital_twin.silver.dim_properties p
JOIN water_digital_twin.silver.dim_sensor s ON p.dma_code = s.dma_code AND s.sensor_type = 'pressure'
JOIN water_digital_twin.silver.fact_telemetry t ON s.sensor_id = t.sensor_id;

-- Gold: vw_dma_pressure_sensor
CREATE OR REPLACE VIEW water_digital_twin.gold.vw_dma_pressure_sensor AS
SELECT
  s.sensor_id,
  s.dma_code,
  s.name AS sensor_name,
  s.latitude,
  s.longitude,
  t.timestamp,
  t.value AS pressure,
  t.total_head_pressure
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s ON t.sensor_id = s.sensor_id
WHERE t.sensor_type = 'pressure';
```

**Inputs:** Approved plan Section 6 (schema contract)
**Outputs:** Single executable `.sql` notebook at the path above
**Acceptance Criteria:**
- [ ] All DDL statements execute without errors on the workspace
- [ ] All tables use three-part names (`water_digital_twin.<schema>.<table>`)
- [ ] All Bronze tables exist: `raw_telemetry`, `raw_assets`, `raw_customer_contacts`, `raw_complaints`, `raw_dma_boundaries`, `raw_pma_boundaries`
- [ ] All Silver tables exist: `dim_sensor`, `dim_dma`, `dim_pma`, `dim_properties`, `dim_assets`, `dim_asset_dma_feed`, `dim_reservoirs`, `dim_reservoir_dma_feed`, `fact_telemetry`, `customer_complaints`
- [ ] All Gold tables exist: `dma_status`, `dma_rag_history`, `anomaly_scores`, `dma_summary`, `dim_incidents`, `incident_events`, `communications_log`, `dim_response_playbooks`, `shift_handovers`, `comms_requests`
- [ ] All Gold views exist: `vw_dma_pressure`, `vw_pma_pressure`, `vw_property_pressure`, `vw_dma_pressure_sensor`
- [ ] Script is idempotent — can be re-run without errors

---

### Task 5 — Lakebase Table DDL & Sync (🔨 CODE)

**Goal:** Create SQL DDL scripts for Lakebase tables (PostgreSQL/PostGIS) that mirror Gold layer tables for low-latency serving, plus a Python notebook for Delta Gold → Lakebase sync using the **Lakebase Data API (REST)**.

**Outputs:**
- Lakebase DDL within the sync notebook
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/09_lakebase_sync.py`

**Lakebase tables to create** (these power the app's sub-second rendering):

| Lakebase Table | Source Delta Table | Purpose |
|---|---|---|
| `dim_incidents` | `gold.dim_incidents` | Alarm log, incident context |
| `incident_events` | `gold.incident_events` | Alarm log, audit trail |
| `communications_log` | `gold.communications_log` | Handover, audit trail |
| `shift_handovers` | `gold.shift_handovers` | Handover view |
| `dim_response_playbooks` | `gold.dim_response_playbooks` | Playbook panel |
| `comms_requests` | `gold.comms_requests` | Proactive comms |
| `dma_status` | `gold.dma_status` | Map DMA colouring, quick-filters |
| `dma_summary` | `gold.dma_summary` | DMA detail panel |
| `anomaly_scores` | `gold.anomaly_scores` | Anomaly badges, timeline |
| `dim_dma` | `silver.dim_dma` | DMA polygons for map (with PostGIS geometry) |
| `dim_properties` | `silver.dim_properties` | Customer impact view |
| `dim_assets` | `silver.dim_assets` | Trunk main overlays, valve markers |
| `dim_sensor` | `silver.dim_sensor` | Sensor positions on map |
| `dim_reservoirs` | `silver.dim_reservoirs` | Reservoir status |
| `fact_telemetry` | `silver.fact_telemetry` | Time-series charts (last 7 days only) |

**Lakebase DDL (PostgreSQL + PostGIS):**

Each table must include PostGIS geometry columns where applicable. Example for `dim_dma`:
```sql
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS dim_dma (
  dma_code TEXT PRIMARY KEY,
  dma_name TEXT,
  dma_area_code TEXT,
  geometry_wkt TEXT,
  geom GEOMETRY(Polygon, 4326),  -- PostGIS native geometry
  centroid_latitude DOUBLE PRECISION,
  centroid_longitude DOUBLE PRECISION,
  avg_elevation DOUBLE PRECISION,
  pressure_red_threshold DOUBLE PRECISION DEFAULT 15.0,
  pressure_amber_threshold DOUBLE PRECISION DEFAULT 25.0
);

CREATE INDEX IF NOT EXISTS idx_dim_dma_geom ON dim_dma USING GIST(geom);
```

For point tables (`dim_sensor`, `dim_properties`, `dim_assets`, `dim_reservoirs`), add:
```sql
geom GEOMETRY(Point, 4326)
```
with a GiST spatial index.

For `dim_assets` with LINESTRING (trunk mains), use `GEOMETRY` (generic) since assets can be POINT or LINESTRING.

**Sync notebook logic:**

1. Read each Gold/Silver Delta table using Spark
2. For each table, use the **Lakebase Data API (REST)** to:
   - TRUNCATE the Lakebase table (full refresh for demo simplicity)
   - INSERT rows in batches
3. For geometry columns: convert WKT to PostGIS geometry using `ST_GeomFromText(geometry_wkt, 4326)` in the INSERT statement
4. For `fact_telemetry`: only sync the last 7 days to keep Lakebase lean
5. The Databricks App's FastAPI backend should also use the **Lakebase Data API (REST)** for read queries, not JDBC

**Inputs:** Alice's DDL (Task 4) for table definitions; Diana's Lakebase endpoint URL (Task 1)
**Outputs:** Lakebase DDL with PostGIS extensions; Delta-to-Lakebase sync notebook
**Acceptance Criteria:**
- [ ] All 15 Lakebase tables created with valid PostgreSQL DDL
- [ ] PostGIS geometry columns and GiST indexes exist for spatial tables
- [ ] Sync notebook populates all Lakebase tables from Delta Gold/Silver
- [ ] `SELECT COUNT(*) FROM dim_dma` returns 500 after sync
- [ ] `SELECT ST_IsValid(geom) FROM dim_dma LIMIT 5` returns all true
- [ ] Sync uses Lakebase Data API (REST), not JDBC

---

## Agent 3: Bob — Senior Data Engineer

### Task 6 — Data Generation (🔨 CODE)

**Goal:** Generate all synthetic sample data at the volumes and with the exact relationships specified in the demo scenario contract. Must be idempotent and re-runnable with deterministic output (fixed random seeds).

**Outputs:** Three Python notebooks:
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/02_data_gen_geography.py`
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/03_data_gen_dimensions.py`
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/04_data_gen_facts.py`
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/05_data_gen_incidents.py`

#### Notebook 1: Geographic Scaffolding (`02_data_gen_geography.py`)

Generate 500 DMA hexagonal/Voronoi polygons tessellating the Greater London bounding box:
- **Bounding box:** latitude 51.28N to 51.70N, longitude -0.51W to 0.33E
- **Polygon size:** roughly 2-5 km across — large enough to be clickable on a map
- **DEMO_DMA_01** must be centred near Crystal Palace / Sydenham Hill (~51.42N, -0.07W) — a genuinely hilly area (elevation 90-110m). Place it near the **geographic centre** of the DMA grid, NOT at an edge.
- **DEMO_DMA_02** and **DEMO_DMA_03** must be immediately adjacent to DEMO_DMA_01 (sharing polygon edges)
- Generate ~100 PMAs as subdivisions of selected DMAs (subset of DMAs that have PRVs)
- Assign realistic elevation values: low near the Thames (~5-15m), higher on hills like Highgate, Crystal Palace, Shooters Hill (~60-100m)
- Use H3 resolution 7 for DMA centroid indexing, resolution 8 for entity-level indexing

**Output tables:** Write to `water_digital_twin.bronze.raw_dma_boundaries`, `water_digital_twin.bronze.raw_pma_boundaries`. Also write cleansed versions directly to `water_digital_twin.silver.dim_dma`, `water_digital_twin.silver.dim_pma`.

#### Notebook 2: Dimension Tables (`03_data_gen_dimensions.py`)

**Sensors — 10,000 total (8,000 pressure + 2,000 flow):**
- Each sensor placed **inside** its assigned DMA polygon (verify with ST_Contains or H3)
- DEMO_DMA_01 must have at least **20 sensors**: 18 pressure (including `DEMO_SENSOR_01`) + 2 flow (`DEMO_FLOW_01`, `DEMO_FLOW_02`)
- `DEMO_SENSOR_01`: `sensor_type = 'pressure'`, latitude/longitude inside DEMO_DMA_01, `elevation = 15` (low elevation)
- `DEMO_FLOW_01`, `DEMO_FLOW_02`: `sensor_type = 'flow'`, positioned at DMA entry points
- Write to `water_digital_twin.silver.dim_sensor`

**Properties — 50,000 total distributed across all 500 DMAs:**
- Each property placed **inside** its assigned DMA polygon
- **DEMO_DMA_01 must have at least 800 properties:**
  - ~750 domestic (`property_type = 'domestic'`)
  - At least 2 schools (`property_type = 'school'`)
  - At least 1 hospital (`property_type = 'hospital'`)
  - At least 3 dialysis patients (`property_type = 'dialysis_home'`)
  - At least 15 commercial (`property_type = 'commercial'`)
- All properties must have `dma_code = 'DEMO_DMA_01'` — this join MUST work
- All properties must have valid `latitude`, `longitude`, `geometry_wkt` within DEMO_DMA_01's polygon
- **Elevation coherence:** At least 50% of DEMO_DMA_01 properties must have `customer_height > 35m`. Schools/hospital should be at higher elevation (40-80m) to support the "high-altitude customers lose water first" story.
- Write to `water_digital_twin.silver.dim_properties` (also insert raw to `bronze.raw_customer_contacts`)

**Assets — 30+ upstream assets:**
- **`DEMO_PUMP_01`:** `asset_type = 'pump_station'`, `status = 'tripped'`, `trip_timestamp = '2026-04-07 02:03:00'`, positioned **upstream** (slightly north/west) of DEMO_DMA_01 (~51.44N, -0.08W)
- **`DEMO_TM_001`:** `asset_type = 'trunk_main'`, `diameter_inches = 12`, `length_km = 3.2`, `geometry_wkt` as LINESTRING from DEMO_PUMP_01 to DEMO_DMA_01 entry point
- **`DEMO_VALVE_01`, `DEMO_VALVE_02`:** `asset_type = 'isolation_valve'`, `status = 'open'`, point geometries along DEMO_TM_001 LINESTRING
- Additional assets: ~10 more pump stations, ~10 trunk mains, ~10 PRVs, scattered across the network
- Write to `water_digital_twin.silver.dim_assets` (also to `bronze.raw_assets`)

**Asset-DMA feed mapping:**
- `DEMO_PUMP_01` → `DEMO_DMA_01` (feed_type = 'primary')
- `DEMO_PUMP_01` → `DEMO_DMA_02` (feed_type = 'secondary')
- `DEMO_PUMP_01` → `DEMO_DMA_03` (feed_type = 'secondary')
- Additional mappings for other pump stations to their DMAs
- Write to `water_digital_twin.silver.dim_asset_dma_feed`

**Reservoirs — 20 total:**
- **`DEMO_SR_01`:** `capacity_ml = 5.0`, `current_level_pct = 43`, `hourly_demand_rate_ml` such that `(43/100 * 5.0) / hourly_demand_rate_ml ≈ 3.1 hours` → `hourly_demand_rate_ml ≈ 0.694`. Positioned near DEMO_DMA_01.
- Linked to DEMO_DMA_01 via `dim_reservoir_dma_feed` (feed_type = 'primary')
- Other reservoirs distributed across the network with levels 60-95%
- Write to `water_digital_twin.silver.dim_reservoirs`, `water_digital_twin.silver.dim_reservoir_dma_feed`

#### Notebook 3: Fact Tables (`04_data_gen_facts.py`)

**Telemetry — ~500K pressure readings + ~100K flow readings:**
- **Time range:** At least 7 days of 15-minute interval data (2026-03-31 to 2026-04-07 06:00)
- **Normal pressure pattern (diurnal):** Higher at night (~55m ± 2-3m noise), lower during morning demand (~40m ± 2-3m), recovering mid-day. Apply Gaussian noise ±2-3m.
- **Normal flow pattern:** Similar demand-driven patterns, ~45 l/s baseline ± 5 l/s noise
- **DEMO_SENSOR_01 incident data:**
  - Normal (~45-55m) from midnight to 01:59
  - Sharp drop to ~5-10m at **02:03** (matching pump trip)
  - Stays low through 06:00
- **DEMO_FLOW_01, DEMO_FLOW_02 incident data:**
  - Normal (~45 l/s) until 02:03
  - Drop to ~12 l/s after 02:03 (partial flow — standby pump partially running)
- **At least 8 pressure sensors in DEMO_DMA_01** must show pressure below low-pressure threshold after 02:03
- **Amber DMA sensors (DEMO_DMA_02, DEMO_DMA_03):** Mild pressure dips to 25-35m (enough for AMBER status, but anomaly_sigma < 2.0)
- **All other DMAs:** Normal green patterns
- Write to `water_digital_twin.bronze.raw_telemetry` (raw) and `water_digital_twin.silver.fact_telemetry` (cleansed)

**Customer complaints — at least 30 for DEMO_DMA_01:**
- `complaint_timestamp` between **03:00 and 05:30** (1-hour lag after pressure drop)
- Complaints linked to properties at **high elevation** (`customer_height > 35m`)
- `complaint_type`: mix of `'no_water'` and `'low_pressure'`
- Write to `water_digital_twin.bronze.raw_complaints` and `water_digital_twin.silver.customer_complaints`

#### Notebook 4: Incident & Operational Data (`05_data_gen_incidents.py`)

**Active incident — `INC-2026-0407-001`:**
- `dma_code = 'DEMO_DMA_01'`
- `root_cause_asset_id = 'DEMO_PUMP_01'`
- `start_timestamp = '2026-04-07 02:03:00'`
- `end_timestamp = NULL` (ongoing)
- `status = 'active'`, `severity = 'high'`
- `total_properties_affected = 441`
- `sensitive_premises_affected = true`

**Historical incidents — 5-10 resolved:**
- Spanning last 6 months (October 2025 — March 2026)
- At least 2 historical incidents in DEMO_DMA_01 (supports "12 incidents in 6 months" AMP8 hook)
- Various severities (low, medium, high)
- Various root causes (pump_station_trip, trunk_main_burst, prv_failure)
- Each with realistic `total_properties_affected` (50-500 range)

**Incident event log — pre-baked for `INC-2026-0407-001`:**

| timestamp | event_type | source | description |
|---|---|---|---|
| 2026-04-07 02:03:00 | asset_trip | scada | DEMO_PUMP_01 tripped — duty pump failure |
| 2026-04-07 02:18:00 | anomaly_detected | platform | DEMO_DMA_01 pressure anomaly: 3.2σ below baseline |
| 2026-04-07 02:25:00 | alert_acknowledged | operator | Night operator acknowledged DEMO_DMA_01 alert |
| 2026-04-07 02:30:00 | rag_change | platform | DEMO_DMA_01 status: GREEN → RED |
| 2026-04-07 02:30:00 | rag_change | platform | DEMO_DMA_02, DEMO_DMA_03 status: GREEN → AMBER |
| 2026-04-07 03:05:00 | customer_complaint | salesforce | First customer complaint received — low pressure |
| 2026-04-07 03:15:00 | crew_dispatched | operator | Field crew dispatched to DEMO_PUMP_01 |
| 2026-04-07 04:10:00 | regulatory_notification | operator | DWI verbal notification filed — sensitive premises affected |
| 2026-04-07 04:30:00 | customer_comms | operator | Proactive comms triggered for 423 domestic properties |
| 2026-04-07 05:30:00 | shift_handover | platform | Shift handover completed — day shift operator acknowledged |

Write to `water_digital_twin.gold.incident_events`

**Communications log — pre-baked for `INC-2026-0407-001`:**

| timestamp | contact_role | method | summary | action_agreed |
|---|---|---|---|---|
| 2026-04-07 03:20:00 | Network Manager | phone | Briefed on pump trip and affected DMAs | Agreed to escalate crew priority |
| 2026-04-07 04:10:00 | DWI Duty Officer | phone | Event notification: sensitive premises affected | Written report due by 14:03 |
| 2026-04-07 04:30:00 | Customer Comms Team | email | Triggered proactive notification for 423 domestic properties | Comms sent by 04:45 |
| 2026-04-07 05:00:00 | Executive On-Call | phone | Briefed: 441 properties affected, 1 hospital, 3 dialysis patients, est. penalty £180K | Acknowledged, requested hourly updates |

Write to `water_digital_twin.gold.communications_log`

**Response playbook — pump station trip:**
- `incident_type = 'pump_station_trip'`
- `sop_reference = 'SOP-WN-042'`
- `action_steps` (JSON array):
  ```json
  [
    {"step_number": 1, "description": "Verify crew status at DEMO_PUMP_01", "is_mandatory": true, "typical_timeframe": "15 minutes"},
    {"step_number": 2, "description": "Check standby pump status — flow data suggests partial operation", "is_mandatory": true, "typical_timeframe": "10 minutes"},
    {"step_number": 3, "description": "Assess rezoning: can DEMO_DMA_01 be temporarily fed from adjacent DMA via interconnection valves?", "is_mandatory": false, "typical_timeframe": "30 minutes"},
    {"step_number": 4, "description": "Check downstream reservoir DEMO_SR_01 — current level 43%, ~3.1h supply remaining", "is_mandatory": true, "typical_timeframe": "5 minutes"},
    {"step_number": 5, "description": "Notify DWI — sensitive premises affected (1 hospital, 2 schools). Written report due 14:03.", "is_mandatory": true, "typical_timeframe": "20 minutes"},
    {"step_number": 6, "description": "Trigger proactive customer comms for commercial customers (12 properties, not yet notified)", "is_mandatory": false, "typical_timeframe": "10 minutes"}
  ]
  ```
- Write to `water_digital_twin.gold.dim_response_playbooks`

**Shift handover — pre-baked for `INC-2026-0407-001`:**
- `outgoing_operator = 'OP_NIGHT_01'`, `incoming_operator = 'OP_DAY_01'`
- `risk_of_escalation = 'low'`, `current_trajectory = 'stabilising'`
- `signed_off_at = '2026-04-07 05:25:00'`, `acknowledged_at = '2026-04-07 05:30:00'`
- `generated_summary`: The full handover narrative from Scene 1 of the approved plan
- Write to `water_digital_twin.gold.shift_handovers`

**Comms request — 1 sample for the demo:**
- `incident_id = 'INC-2026-0407-001'`, `dma_code = 'DEMO_DMA_01'`
- `requested_by = 'OP_NIGHT_01'`, `requested_at = '2026-04-07 04:30:00'`
- `customer_count = 423`, `status = 'sent'`
- Write to `water_digital_twin.gold.comms_requests`

**Inputs:** Alice's DDL (Task 4) for table definitions
**Outputs:** Four data generation notebooks
**Acceptance Criteria:**
- [ ] All data generation is idempotent (re-runnable with same output)
- [ ] All coordinates within Greater London bounding box (51.28-51.70N, -0.51-0.33E)
- [ ] DEMO_DMA_01 centred near Crystal Palace (~51.42N, -0.07W)
- [ ] All 22 verification queries from Section 5 pass (see Eve's Task 11 for the full list)
- [ ] All entity relationships are intact (FK joins work)
- [ ] Elevation coherence: DEMO_DMA_01 properties with complaints have avg elevation > 35m

---

### Task 7 — SDP Pipeline (🔨 CODE)

**Goal:** Build the Spark Declarative Pipeline for Bronze → Silver → Gold transformation.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/06_sdp_pipeline.py`

**Pipeline stages:**

1. **Bronze → Silver (streaming for telemetry, batch for reference):**
   - Stream `bronze.raw_telemetry` → `silver.fact_telemetry`: parse sensor readings, add `sensor_type` (join with `dim_sensor`), split `value` into `value`/`total_head_pressure` (for pressure) and `flow_rate` (for flow)
   - Batch `bronze.raw_dma_boundaries` → `silver.dim_dma`: add centroid lat/lon, avg_elevation, H3 index, default thresholds
   - Batch `bronze.raw_pma_boundaries` → `silver.dim_pma`: add centroid lat/lon
   - Batch `bronze.raw_assets` → `silver.dim_assets`: normalise status enum, parse geometry
   - Batch `bronze.raw_customer_contacts` → `silver.dim_properties`: normalise property_type enum, add H3 index
   - Batch `bronze.raw_complaints` → `silver.customer_complaints`: validate complaint_type enum

2. **Data quality expectations:**
   - `EXPECT sensor_value_range`: pressure BETWEEN 0 AND 120, flow_rate BETWEEN 0 AND 500
   - `EXPECT OR DROP null_geometry`: `geometry_wkt IS NOT NULL` for spatial tables
   - `EXPECT referential_integrity_dma`: `sensor.dma_code IN (SELECT dma_code FROM dim_dma)`
   - `EXPECT complaint_type_valid`: `complaint_type IN ('no_water', 'low_pressure', 'discoloured_water', 'other')`

3. **Geospatial transformations within SDP:**
   - Use native GEOMETRY types for performant spatial joins during transformation
   - Compute H3 indexes: `h3_pointash3(latitude, longitude, 8)` for entities, resolution 7 for DMA centroids
   - Verify spatial containment: sensors and properties are inside their assigned DMA polygon

**Inputs:** Alice's DDL (Task 4); Bob's generated Bronze data (Task 6)
**Outputs:** SDP pipeline notebook
**Acceptance Criteria:**
- [ ] Pipeline runs end-to-end without errors
- [ ] All Silver tables populated from Bronze sources
- [ ] Data quality expectations catch invalid records (drop null geometries, flag out-of-range readings)
- [ ] Geospatial transformations produce valid geometries
- [ ] H3 indexes are correctly computed

---

### Task 7b — Anomaly Scoring & Status Computation (🔨 CODE)

**Goal:** Compute anomaly scores, RAG status history, and DMA status/summary tables in the Gold layer.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/07_anomaly_scoring.py`

**Computations:**

1. **`gold.anomaly_scores`** — for each sensor reading in `silver.fact_telemetry`:
   - Compute the **rolling 7-day baseline at the same time of day**: for a reading at 02:15, the baseline is the average of all readings at 02:15 (±15 min) over the previous 7 days. This accounts for diurnal patterns (low morning demand, recovery mid-day, evening drop).
   - `anomaly_sigma` = (actual_value - baseline_value) / std_dev_of_baseline. Use absolute value for the deviation magnitude.
   - `is_anomaly` = TRUE if `anomaly_sigma > 2.5`
   - `scoring_method = 'statistical'` (default)
   - **Critical:** `DEMO_SENSOR_01` readings after 02:03 on 2026-04-07 must score **>3.0σ**. The reading at 02:18 (next 15-min cycle after the trip) should score ~3.2σ.
   - **Critical:** Sensors in amber DMAs (DEMO_DMA_02/03) must have `anomaly_sigma < 2.0` — they are routine fluctuation, not real incidents.

2. **`gold.dma_rag_history`** — RAG status per DMA per 15-minute interval:
   - For each DMA at each 15-min timestamp, compute avg and min pressure from `fact_telemetry` (pressure sensors only)
   - RAG logic:
     - `RED` if `min_pressure < pressure_red_threshold` (default 15.0m from `dim_dma`)
     - `AMBER` if `min_pressure < pressure_amber_threshold` (default 25.0m) but >= red threshold
     - `GREEN` otherwise
   - **DEMO_DMA_01 history:** GREEN until ~02:00, AMBER at ~02:15, RED from ~02:30 onward
   - **DEMO_DMA_02/03:** GREEN until ~02:15, AMBER from ~02:30 onward (stay amber, not red)

3. **`gold.dma_status`** — current RAG status per DMA (materialized from latest 15-min window):
   - One row per DMA
   - `rag_status`: from the latest `dma_rag_history` entry
   - `avg_pressure`, `min_pressure`: from latest telemetry
   - `sensor_count`: count of active sensors in this DMA
   - `property_count`: total properties in this DMA
   - `sensitive_premises_count`: count of properties with `property_type IN ('hospital', 'school', 'dialysis_home')`
   - `has_active_incident`: TRUE if the DMA has a matching `dim_incidents` record with `status = 'active'`
   - **At demo timestamp:** DEMO_DMA_01 = RED (1 row), DEMO_DMA_02/03 = AMBER (2 rows), remaining 497 = GREEN

4. **`gold.dma_summary`** — pre-materialised DMA summary:
   - Joins dma_status + reservoir levels (via `dim_reservoir_dma_feed`) + incident links + property/sensor counts
   - Refreshed every 15 minutes
   - **DEMO_DMA_01 summary:** `reservoir_level_pct = 43`, `reservoir_hours_remaining ≈ 3.1`, `active_incident_id = 'INC-2026-0407-001'`, `sensitive_premises_count >= 6`
   - Powers the DMA detail panel for sub-second rendering without complex joins

**Inputs:** Alice's DDL (Task 4); Bob's data (Task 6)
**Outputs:** Anomaly scoring notebook
**Acceptance Criteria:**
- [ ] `DEMO_SENSOR_01` anomaly_sigma > 3.0 after 02:03 on 2026-04-07
- [ ] Amber DMAs have max anomaly_sigma < 2.0
- [ ] `dma_rag_history` shows correct GREEN→AMBER→RED progression for DEMO_DMA_01
- [ ] `dma_status` has 500 rows: 1 RED, 2 AMBER, 497 GREEN
- [ ] `dma_status` for DEMO_DMA_01 has `sensitive_premises_count >= 6`
- [ ] `dma_summary` for DEMO_DMA_01 has `reservoir_hours_remaining ≈ 3.1`

---

### Task 7c — Metric Views (🔨 CODE)

**Goal:** Create Gold-layer metric views with `mv_` prefix for consumption by Genie Spaces and dashboards.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/08_metric_views.sql`

**Metric views to create:**

#### `water_digital_twin.gold.mv_dma_pressure`
```yaml
name: mv_dma_pressure
description: "Average, min, and max pressure by DMA over time. Primary metric view for operator Genie Space."
source: "SELECT * FROM water_digital_twin.gold.vw_dma_pressure"
joins:
  - table: water_digital_twin.silver.dim_dma
    on: "dma_code = dim_dma.dma_area_code"
    type: LEFT
dimensions:
  - name: telemetry_time
    column: timestamp
    display_name: "Reading Time"
    format: "yyyy-MM-dd HH:mm"
    description: "Timestamp of the 15-minute telemetry reading window"
  - name: dma_code
    column: dma_code
    display_name: "DMA Code"
    description: "District Metered Area identifier"
  - name: dma_name
    column: dim_dma.dma_name
    display_name: "DMA Name"
    description: "District Metered Area display name"
measures:
  - name: avg_pressure
    expression: "AVG(value)"
    display_name: "Avg Pressure (m)"
    format: "#,##0.0"
    description: "Average pressure in metres head across all sensors in the DMA"
  - name: max_pressure
    expression: "MAX(value)"
    display_name: "Max Pressure (m)"
    format: "#,##0.0"
  - name: min_pressure
    expression: "MIN(value)"
    display_name: "Min Pressure (m)"
    format: "#,##0.0"
    description: "Minimum pressure reading in the DMA — low values indicate supply issues"
  - name: avg_total_head_pressure
    expression: "AVG(total_head_pressure)"
    display_name: "Avg Total Head (m)"
    format: "#,##0.0"
  - name: reading_count
    expression: "COUNT(sensor_id)"
    display_name: "Reading Count"
    format: "#,##0"
```

#### `water_digital_twin.gold.mv_flow_anomaly`
```yaml
name: mv_flow_anomaly
description: "Flow rate deviation by DMA entry point. Detects supply interruptions (low flow) and bursts (high flow)."
source: "SELECT * FROM water_digital_twin.silver.fact_telemetry WHERE sensor_type = 'flow'"
joins:
  - table: water_digital_twin.silver.dim_sensor
    on: "fact_telemetry.sensor_id = dim_sensor.sensor_id"
    type: INNER
  - table: water_digital_twin.silver.dim_dma
    on: "dim_sensor.dma_code = dim_dma.dma_code"
    type: LEFT
dimensions:
  - name: telemetry_time (column: timestamp)
  - name: dma_code (column: dim_sensor.dma_code)
  - name: dma_name (column: dim_dma.dma_name)
  - name: sensor_id (column: dim_sensor.sensor_id)
measures:
  - name: avg_flow_rate — expression: "AVG(flow_rate)" — display: "Avg Flow (l/s)"
  - name: min_flow_rate — expression: "MIN(flow_rate)" — display: "Min Flow (l/s)"
  - name: max_flow_rate — expression: "MAX(flow_rate)" — display: "Max Flow (l/s)"
  - name: flow_reading_count — expression: "COUNT(flow_rate)" — display: "Reading Count"
```

#### `water_digital_twin.gold.mv_reservoir_status`
```yaml
name: mv_reservoir_status
description: "Service reservoir status, estimated hours remaining, and fed DMA mapping."
source: "SELECT * FROM water_digital_twin.silver.dim_reservoirs"
joins:
  - table: water_digital_twin.silver.dim_reservoir_dma_feed (on reservoir_id)
  - table: water_digital_twin.silver.dim_dma (on dma_code)
dimensions:
  - reservoir_id, reservoir_name, fed_dma_code, fed_dma_name, feed_type
measures:
  - current_level_pct — "MAX(dim_reservoirs.current_level_pct)" — "Current Level (%)"
  - capacity_ml — "MAX(dim_reservoirs.capacity_ml)" — "Capacity (ML)"
  - hours_remaining — "MAX((current_level_pct / 100.0 * capacity_ml) / hourly_demand_rate_ml)" — "Est. Hours Remaining"
```

#### `water_digital_twin.gold.mv_regulatory_compliance`
```yaml
name: mv_regulatory_compliance
description: "Regulatory compliance metrics: Ofwat thresholds, sensitive premises, OPA penalty exposure."
source: >
  SELECT
    ds.dma_code, ds.rag_status, ds.property_count, ds.sensitive_premises_count,
    di.incident_id, di.start_timestamp, di.total_properties_affected, di.sensitive_premises_affected,
    TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) as hours_since_start,
    CASE WHEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) > 3
         THEN di.total_properties_affected ELSE 0 END as properties_exceeding_3h,
    CASE WHEN TIMESTAMPDIFF(HOUR, di.start_timestamp, CURRENT_TIMESTAMP()) > 12
         THEN di.total_properties_affected ELSE 0 END as properties_exceeding_12h
  FROM water_digital_twin.gold.dma_status ds
  LEFT JOIN water_digital_twin.gold.dim_incidents di
    ON ds.dma_code = di.dma_code AND di.status = 'active'
dimensions: dma_code, incident_id, rag_status
measures:
  - properties_exceeding_3h — "SUM(properties_exceeding_3h)" — "Properties >3h"
  - properties_exceeding_12h — "SUM(properties_exceeding_12h)" — "Properties >12h"
  - sensitive_premises_total — "SUM(sensitive_premises_count)" — "Sensitive Premises"
  - estimated_opa_penalty — "SUM(properties_exceeding_3h * (hours_since_start - 3) * 580)" — "Est. OPA Penalty (£)"
  - total_properties_affected — "SUM(total_properties_affected)" — "Total Affected"
```

#### `water_digital_twin.gold.mv_incident_summary`
```yaml
name: mv_incident_summary
description: "Active and historical incident summary for executive Genie Space."
source: "SELECT * FROM water_digital_twin.gold.dim_incidents"
joins:
  - table: water_digital_twin.gold.dma_summary (on dma_code)
dimensions: incident_id, dma_code, status, severity, start_date
measures:
  - total_properties_affected — "SUM(dim_incidents.total_properties_affected)"
  - active_incident_count — "COUNT(CASE WHEN status = 'active' THEN 1 END)"
  - incidents_with_sensitive_premises — "COUNT(CASE WHEN sensitive_premises_affected = TRUE THEN 1 END)"
  - avg_duration_hours — "AVG(TIMESTAMPDIFF(HOUR, start_timestamp, COALESCE(end_timestamp, CURRENT_TIMESTAMP())))"
```

**Inputs:** Alice's DDL (Task 4); Bob's data (Tasks 6-7b)
**Outputs:** Metric view DDL notebook
**Acceptance Criteria:**
- [ ] All 4 metric views create successfully
- [ ] `mv_dma_pressure` returns data for DEMO_DMA_01 with correct pressure values
- [ ] `mv_flow_anomaly` returns flow data for DEMO_FLOW_01/02
- [ ] `mv_reservoir_status` shows DEMO_SR_01 at 43% with ~3.1h remaining
- [ ] `mv_regulatory_compliance` shows penalty exposure for active incident
- [ ] `mv_incident_summary` shows 1 active + 5+ historical incidents

---

### Task 7d — ML Anomaly Detection Notebook (Optional Extension) (🔨 CODE)

**Goal:** Build a single parameterised notebook showcasing three progressive levels of ML-powered anomaly detection for extended/technical demo sessions.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/notebooks/10_ml_anomaly_detection.py`

**Cluster requirement:** DBR ML Runtime 15.4+ (for MLflow, AutoML, foundation model libraries). Include `%pip install` cells for any additional packages.

**Notebook structure:**

**Widgets:**
- `sensor_id` (default: `DEMO_SENSOR_01`)
- `date_range_days` (default: `7`)
- `anomaly_threshold_sigma` (default: `3.0`)

**Level 1 — `ai_forecast()` in SQL (~3 min):**
- SQL cell calling `ai_forecast()` on selected sensor's telemetry from `gold.anomaly_scores` / `silver.fact_telemetry`
- Forecast expected values with upper/lower prediction intervals
- Comparison cell flagging readings where actual falls outside prediction interval
- Visualisation: time-series chart (actual values, forecast line, shaded prediction interval). The 02:03 anomaly must be clearly visible as a breakout.
- Cell writing `ai_forecast`-based scores to `gold.anomaly_scores` with `scoring_method = 'ai_forecast'`
- **Pre-computed/cached results** that display instantly, with a "Re-run live" cell below

**Level 2 — AutoML classification (~7 min):**
- Data preparation cell: labeled training set from 15-min telemetry windows. Features: pressure value, flow rate, rate of change (delta from previous), time-of-day, day-of-week, rolling 1h mean, rolling 1h std dev. Label: `anomalous` if `anomaly_sigma > 3.0`, else `normal`.
- Cell launching AutoML: `databricks.automl.classify(train_df, target_col="label", ...)`. **Pre-run** — show results, not training.
- Cells showing: MLflow experiment UI link, best model notebook link, SHAP feature importance plot
- Cell registering best model: `mlflow.register_model(model_uri, "water_digital_twin.gold.anomaly_detection_model")`
- Cell scoring demo data and writing to `gold.anomaly_scores` with `scoring_method = 'automl'`
- **Pre-computed/cached results**

**Level 3 — Foundation time-series model via MMF (~5 min, optional):**
- Cell configuring Many Model Forecasting with Chronos-Bolt or TimesFM 2.5
- Cell running parallel scoring across all sensors (or subset) using Spark
- Visualisation: anomaly heatmap — DEMO_SENSOR_01 highlighted as high-confidence, normal/amber sensors low scores
- Cell deploying model to Model Serving endpoint (pre-provisioned)
- Cell demonstrating `ai_query()` calling endpoint from SQL for batch scoring
- Cell writing scores to `gold.anomaly_scores` with `scoring_method = 'foundation_model'`

**Comparison section:**
- Query `gold.anomaly_scores` grouped by `scoring_method`
- Side-by-side comparison: do all methods agree on the 02:03 anomaly? Where do they diverge on amber/noise DMAs?

**Inputs:** Alice's DDL (Task 4); Bob's data (Task 6) and anomaly_scores (Task 7b)
**Outputs:** ML anomaly detection notebook
**Acceptance Criteria:**
- [ ] Notebook runs without errors on DBR ML Runtime
- [ ] Level 1: `ai_forecast` produces visible anomaly flag for DEMO_SENSOR_01 at 02:03
- [ ] Level 2: AutoML experiment contains at least 5 trials with logged metrics
- [ ] Level 3: foundation model scores at least 100 sensors in parallel
- [ ] All three levels write to `gold.anomaly_scores` with correct `scoring_method` values
- [ ] Comparison section shows agreement across methods for the demo incident

---

## Agent 4: Charlie — AI/BI, Genie & Apps Specialist

### Task 8 — Databricks App (🔨 CODE)

**Goal:** Build and deploy the geospatial Databricks App (React + FastAPI) supporting the full incident investigation flow.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/app/`

**Deploy via:**
```bash
databricks apps create water-digital-twin --profile adb-98
databricks apps deploy water-digital-twin --source-code-path ./app --profile adb-98
```

#### App Views & Interactions

**1. Shift Handover View (Scene 1) — Landing Page:**
- Shows structured handover for active incident (query `gold.shift_handovers` JOIN `gold.dim_incidents WHERE status = 'active'`)
- Displays: incident overview, actions taken, outstanding actions, communications log (from `gold.communications_log`), current trajectory, risk of escalation
- Auto-generated summary via Mosaic AI / AI Functions (`ai_query('databricks-meta-llama-3-3-70b-instruct', ...)`) from Gold-layer data
- Outgoing operator can edit and sign off; incoming operator acknowledges (write-back to `gold.shift_handovers` via Lakebase)
- Governance-grade: timestamps on all actions

**2. Alarm/Event Log (Scene 2):**
- Chronological table from `gold.incident_events`
- Columns: timestamp, event_type, source, description
- **Default view:** filtered by `incident_id` from `gold.dim_incidents WHERE status = 'active'`
- Dropdown to switch between incidents or "All events (last 24h)"
- Filterable by event_type and time range

**3. Map View (Scene 2):**
- Interactive map (use Mapbox GL JS or Leaflet) with DMA polygons colour-coded Red/Amber/Green
- DMA polygons from Lakebase: `SELECT dma_code, dma_name, geom, rag_status FROM dma_status JOIN dim_dma USING (dma_code)`
- **Quick-filter buttons:**
  - "Changed": DMAs with status change in last 2h (query: `dma_rag_history` where status differs between now and 2h ago)
  - "Alarmed": DMAs with `has_active_incident = true` (from `dma_status`)
  - "Sensitive": DMAs with `sensitive_premises_count > 0` (from `dma_status`)
  - "All": full network (default)
- **Initial viewport:** Centred on Greater London, zoom level showing ~20-30 DMAs. DEMO_DMA_01 (RED) must be clearly visible without scrolling.
- **DEMO_DMA_01** styling: RED fill with sufficient opacity/contrast to stand out among GREEN/AMBER neighbours. Only RED polygon.
- **DMA polygons** must be large enough to be clickable (full polygon area as click target, not just label/centroid)
- **Hover tooltip:** DMA name, current RAG status, anomaly confidence (e.g., "High — 3.2σ")
- **Trunk main overlays:** Rendered as LINESTRING when a DMA is selected, with isolation valve markers
- **Map load target:** < 3 seconds from Lakebase

**4. DMA Detail Panel (Scene 3):**
- Triggered by clicking a DMA polygon
- Shows from `gold.dma_summary` JOIN `gold.dim_incidents`:
  - DMA summary: avg pressure, avg flow, sensor count, property count
  - Low-pressure sensors sorted by severity
  - Recent customer complaints (from `silver.customer_complaints`)
  - **Upstream root cause chain:** "Root Cause: Pump station DEMO_PUMP_01 tripped at 02:03 → Trunk Main TM_001 (12-inch, 3.2km) → DEMO_DMA_01. Downstream impact: 3 DMAs, ~1,400 properties."
    - Query: `dim_assets` JOIN `dim_asset_dma_feed` WHERE `dma_code = selected_dma`
  - **Reservoir status:** "Service Reservoir DEMO_SR_01 — 43% (est. 3.1h supply)"
    - Query: `dma_summary.reservoir_level_pct`, `dma_summary.reservoir_hours_remaining`
  - **Flow data:** "DMA inlet flow: 12 l/s (normal: 45 l/s)"
    - Query: latest `fact_telemetry` for flow sensors in DMA

**5. Asset Detail Panel (Scene 4):**
- Triggered by clicking an asset (sensor, pump station, trunk main)
- **Dual-axis time-series chart** (past 24h, 15-min granularity):
  - Left axis: pressure (from `fact_telemetry WHERE sensor_type = 'pressure'`)
  - Right axis: flow (from `fact_telemetry WHERE sensor_type = 'flow'` for DMA entry sensors)
  - Must clearly show the 2am drop in both
- **RAG status timeline strip:** Horizontal sequence of colour blocks (GREEN-GREEN-AMBER-RED-RED) from `gold.dma_rag_history`
- **Anomaly detection badge:** "Anomaly detected at 02:18 — 47 minutes before first customer complaint" (from `gold.anomaly_scores`)
- **Response playbook panel:** "SOP-WN-042 — Pump Station Failure Response" from `gold.dim_response_playbooks WHERE incident_type = 'pump_station_trip'`
  - Parse `action_steps` JSON and render each step with Accept/Defer/Not Applicable buttons
  - Write-back: operator choices saved to Lakebase

**6. Customer Impact View (Scene 5):**
- Affected properties as map markers from `silver.dim_properties WHERE dma_code IN (affected DMAs)`
- Shows high-elevation customers impacted first
- Complaint data with timestamps from `silver.customer_complaints`
- **Predicted impact projection:** count properties where `effective_pressure < 10m`
- **What-if slider** (0-100% restored pressure): computed **client-side** in React:
  1. Fetch properties: `uprn, customer_height, property_type, latitude, longitude` from Lakebase
  2. Fetch `total_head_pressure` from `dma_summary`
  3. `simulated_pressure = current_total_head_pressure + (normal - current) * (slider_pct / 100)`
  4. For each property: `effective_pressure = simulated_pressure - customer_height`. Affected if < 10m.
  5. Re-render map markers and count summary
- Label: "Estimated impact — based on elevation and current pressure. Not a hydraulic simulation."

**7. Regulatory Compliance & Executive Briefing (Scene 6):**
- Affected properties by type: domestic, schools, hospitals, dialysis (home), commercial
  - Query: `SELECT property_type, COUNT(*) FROM dim_properties WHERE dma_code = ?`
- Duration of interruption: `TIMESTAMPDIFF(HOUR, start_timestamp, CURRENT_TIMESTAMP())` from `dim_incidents`
- Sensitive/vulnerable customers impacted: from `dma_status.sensitive_premises_count`
- **Regulatory deadline tracker** with live countdown timers:
  - DWI verbal notification: DONE at 04:10
  - DWI written Event Report: DUE (deadline: start_timestamp + 12h = 14:03)
  - Ofwat 3-hour threshold: from `mv_regulatory_compliance.properties_exceeding_3h`
  - Ofwat 12-hour threshold: from `mv_regulatory_compliance.properties_exceeding_12h`
  - Estimated OPA penalty: `properties_exceeding_3h × (hours - 3) × £580`
  - C-MeX proactive rate: proactive notifications sent / total complaints
- **Auditable decision timeline:** from `gold.incident_events` ordered by timestamp
- **Pre-populated draft regulatory report:** DWI Event Report + Ofwat SIR with all fields from Gold-layer data
- **PDF export:** Client-side via `react-pdf` or `html2pdf.js`. Backend endpoint: `POST /api/incidents/{incident_id}/report/pdf` (WeasyPrint or ReportLab)
- **"Request Proactive Comms" button:** Creates a `comms_requests` record in Lakebase

**8. Communications Log Panel:**
- Structured records from `gold.communications_log` for the active incident
- Operators can add new entries (write-back to Lakebase)

**9. Write-back Capabilities:**
- Alert ownership, comments, comms log entries, playbook action Accept/Defer → all written to Lakebase via **Lakebase Data API (REST)**

**10. Empty / No-Incident State:**
- "Network Normal" landing page: all-green DMA map, last resolved incident summary, "No active incidents" banner
- All panels show meaningful empty states (not blank/errors)
- App must not crash if seed data is missing (cold-start resilience)

**All reads from Lakebase** for sub-second response. FastAPI backend uses **Lakebase Data API (REST)** for all database operations.

**Inputs:** Alice's DDL (Task 4); Alice's Lakebase DDL (Task 5); Bob's data (Tasks 6-7c)
**Outputs:** Deployed Databricks App
**Acceptance Criteria:**
- [ ] App loads map in < 3 seconds from Lakebase
- [ ] DMA polygons render with correct RAG colouring
- [ ] DEMO_DMA_01 is RED and visually prominent
- [ ] Quick-filter buttons (Changed/Alarmed/Sensitive/All) work
- [ ] Shift handover generates correctly from Gold-layer data
- [ ] Alarm log populated and filterable by incident_id
- [ ] DMA detail panel shows upstream root cause chain
- [ ] Asset detail panel shows dual-axis chart with 2am drop visible
- [ ] RAG timeline strip renders correctly
- [ ] Playbook renders with Accept/Defer buttons and write-back works
- [ ] What-if slider recalculates property counts correctly
- [ ] Regulatory report exports as PDF (client-side and API endpoint)
- [ ] Comms request creates a record in Lakebase
- [ ] Empty/no-incident states display correctly
- [ ] Hover tooltip shows DMA name, RAG, anomaly confidence
- [ ] Panel updates < 1 second

---

### Task 9a — Operator Genie Space "Network Operations" (🔨 CODE + 📋 MANUAL GUIDE)

**Goal:** Phase 1: Write and test 10 verified SQL queries against live demo data. Phase 2: Produce SA guide for Genie Space creation.

**Outputs:**
- Phase 1: `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/genie/operator_verified_queries.sql`
- Phase 2: `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides/03_genie_operator_guide.md`

#### Phase 1 — Query Development & Testing (🔨 CODE)

Write and execute each query against the SQL Warehouse. Confirm results match the demo scenario. Save all 10 tested queries.

**Q1:** "Which DMAs had the biggest pressure drop in the last 6 hours?"
```sql
-- Expected: DEMO_DMA_01 with the biggest drop (~45m → ~8m)
SELECT dma_code, dma_name,
  MAX(CASE WHEN timestamp < '2026-04-07 00:00:00' THEN avg_pressure END) as pressure_before,
  MIN(CASE WHEN timestamp >= '2026-04-07 02:00:00' THEN avg_pressure END) as pressure_after,
  MAX(CASE WHEN timestamp < '2026-04-07 00:00:00' THEN avg_pressure END) -
  MIN(CASE WHEN timestamp >= '2026-04-07 02:00:00' THEN avg_pressure END) as pressure_drop
FROM water_digital_twin.gold.dma_rag_history h
JOIN water_digital_twin.silver.dim_dma d USING (dma_code)
WHERE h.timestamp >= '2026-04-06 23:30:00'
GROUP BY dma_code, dma_name
ORDER BY pressure_drop DESC
LIMIT 10;
```

**Q2:** "How many hospitals and schools are in DEMO_DMA_01?"
```sql
-- Expected: at least 2 schools, at least 1 hospital
SELECT property_type, COUNT(*) as count
FROM water_digital_twin.silver.dim_properties
WHERE dma_code = 'DEMO_DMA_01' AND property_type IN ('hospital', 'school')
GROUP BY property_type;
```

**Q3:** "Show pressure trend for sensor DEMO_SENSOR_01 over the last 24 hours"
```sql
-- Expected: ~45-55m until 2am, then ~5-10m
SELECT timestamp, value as pressure_m
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id = 'DEMO_SENSOR_01'
  AND timestamp >= '2026-04-06 06:00:00'
ORDER BY timestamp;
```

**Q4:** "Which pump stations feed DMAs that are currently red?"
```sql
-- Expected: DEMO_PUMP_01
SELECT a.asset_id, a.name, a.status, f.dma_code, f.feed_type
FROM water_digital_twin.silver.dim_assets a
JOIN water_digital_twin.silver.dim_asset_dma_feed f ON a.asset_id = f.asset_id
JOIN water_digital_twin.gold.dma_status s ON f.dma_code = s.dma_code
WHERE a.asset_type = 'pump_station' AND s.rag_status = 'RED';
```

**Q5:** "What's the current reservoir level for DMAs that are red or amber?"
```sql
-- Expected: DEMO_SR_01 at 43%
SELECT r.reservoir_id, r.name, r.current_level_pct,
  (r.current_level_pct / 100.0 * r.capacity_ml) / r.hourly_demand_rate_ml as hours_remaining,
  rf.dma_code, s.rag_status
FROM water_digital_twin.silver.dim_reservoirs r
JOIN water_digital_twin.silver.dim_reservoir_dma_feed rf ON r.reservoir_id = rf.reservoir_id
JOIN water_digital_twin.gold.dma_status s ON rf.dma_code = s.dma_code
WHERE s.rag_status IN ('RED', 'AMBER');
```

**Q6:** "Show me all DMAs within 5km of DEMO_DMA_01" (spatial — uses ST_Distance)
```sql
-- Expected: DEMO_DMA_02, DEMO_DMA_03 and several nearby DMAs
SELECT b.dma_code, b.dma_name,
  ST_Distance(
    ST_Point(a.centroid_longitude, a.centroid_latitude),
    ST_Point(b.centroid_longitude, b.centroid_latitude)
  ) * 111.32 as approx_distance_km
FROM water_digital_twin.silver.dim_dma a
CROSS JOIN water_digital_twin.silver.dim_dma b
WHERE a.dma_code = 'DEMO_DMA_01' AND b.dma_code != 'DEMO_DMA_01'
  AND ST_Distance(
    ST_Point(a.centroid_longitude, a.centroid_latitude),
    ST_Point(b.centroid_longitude, b.centroid_latitude)
  ) * 111.32 < 5.0
ORDER BY approx_distance_km;
```

**Q7:** "How many properties have been without supply for more than 3 hours?"
```sql
-- Expected: 312+ properties (from the approved plan's narrative)
SELECT COUNT(DISTINCT p.uprn) as properties_without_supply_3h
FROM water_digital_twin.silver.dim_properties p
JOIN water_digital_twin.gold.dma_status s ON p.dma_code = s.dma_code
JOIN water_digital_twin.gold.dim_incidents i ON s.dma_code = i.dma_code
WHERE s.rag_status = 'RED'
  AND i.status = 'active'
  AND TIMESTAMPDIFF(HOUR, i.start_timestamp, TIMESTAMP '2026-04-07 05:30:00') > 3;
```

**Q8:** "Show me all schools within affected DMAs" (uses ST_Contains or join)
```sql
-- Expected: at least 2 schools in DEMO_DMA_01
SELECT p.uprn, p.address, p.dma_code, p.latitude, p.longitude, s.rag_status
FROM water_digital_twin.silver.dim_properties p
JOIN water_digital_twin.gold.dma_status s ON p.dma_code = s.dma_code
WHERE p.property_type = 'school' AND s.rag_status IN ('RED', 'AMBER');
```

**Q9:** "What was the flow rate at the DEMO_DMA_01 entry point at 2am vs now?"
```sql
-- Expected: ~45 l/s at 2am, ~12 l/s at 5:30am
SELECT sensor_id, timestamp, flow_rate
FROM water_digital_twin.silver.fact_telemetry
WHERE sensor_id IN ('DEMO_FLOW_01', 'DEMO_FLOW_02')
  AND timestamp IN (
    (SELECT MAX(timestamp) FROM water_digital_twin.silver.fact_telemetry WHERE sensor_id = 'DEMO_FLOW_01' AND timestamp <= '2026-04-07 02:00:00'),
    (SELECT MAX(timestamp) FROM water_digital_twin.silver.fact_telemetry WHERE sensor_id = 'DEMO_FLOW_01' AND timestamp <= '2026-04-07 05:30:00')
  )
ORDER BY sensor_id, timestamp;
```

**Q10:** "Which sensors in DEMO_DMA_01 have anomaly scores above 3 sigma?"
```sql
-- Expected: DEMO_SENSOR_01 and several other pressure sensors
SELECT a.sensor_id, a.timestamp, a.anomaly_sigma, a.actual_value, a.baseline_value
FROM water_digital_twin.gold.anomaly_scores a
JOIN water_digital_twin.silver.dim_sensor s ON a.sensor_id = s.sensor_id
WHERE s.dma_code = 'DEMO_DMA_01'
  AND a.anomaly_sigma > 3.0
  AND a.timestamp >= '2026-04-07 02:00:00'
ORDER BY a.anomaly_sigma DESC;
```

#### Phase 2 — SA Configuration Guide (📋 MANUAL GUIDE)

1. Navigate to: `https://adb-984752964297111.11.azuredatabricks.net/` → AI/BI → Genie Spaces → "Create Genie Space"
2. Space name: `Network Operations`
3. Description: `Operational Genie Space for water network control room operators. Ask questions about pressure, flow, DMAs, sensors, and incidents.`
4. **Add trusted assets** — click "Add table" for each:
   - `water_digital_twin.gold.mv_dma_pressure`
   - `water_digital_twin.gold.mv_flow_anomaly`
   - `water_digital_twin.gold.mv_reservoir_status`
   - `water_digital_twin.silver.dim_sensor`
   - `water_digital_twin.silver.dim_dma`
   - `water_digital_twin.silver.dim_pma`
   - `water_digital_twin.silver.dim_properties`
   - `water_digital_twin.silver.dim_assets`
   - `water_digital_twin.silver.dim_reservoirs`
   - `water_digital_twin.silver.dim_asset_dma_feed`
   - `water_digital_twin.silver.dim_reservoir_dma_feed`
   - `water_digital_twin.silver.fact_telemetry`
   - `water_digital_twin.gold.dma_status`
   - `water_digital_twin.gold.dma_rag_history`
   - `water_digital_twin.gold.anomaly_scores`
   - `water_digital_twin.gold.dma_summary`
5. **Set instructions** — paste this exact system prompt into "General instructions":

   > You are an AI assistant for water network control room operators. You answer questions about the clean water distribution network for Water Utilities, serving the Greater London area.
   >
   > Key domain terminology:
   > - **DMA** = District Metered Area (a geographic zone bounded by a polygon, the primary unit of analysis)
   > - **PMA** = Pressure Management Area (a subset of a DMA)
   > - **RAG status** = Red/Amber/Green health indicator. RED = pressure below critical threshold. AMBER = pressure below warning threshold or planned maintenance. GREEN = normal.
   > - **Pressure** is measured in **metres head (m)**. Normal pressure is typically 40-60m. Below 15m is a supply interruption.
   > - **Total head pressure** = absolute pressure at the sensor. **Property-level pressure** = total_head_pressure - customer_height (elevation adjustment). High-elevation customers lose supply first.
   > - **Flow rate** is measured in **litres per second (l/s)**.
   > - **Anomaly sigma (σ)** = number of standard deviations from the 7-day same-time-of-day baseline. >3.0σ = high-confidence anomaly. <2.0σ = normal fluctuation.
   > - **Reservoir level** determines urgency: hours_remaining = (level_pct / 100 × capacity) / hourly_demand_rate.
   > - Sensor readings arrive every **15 minutes**. Always use the most recent complete 15-minute window.
   > - When asked about "affected" properties, calculate effective pressure (total_head_pressure - customer_height) and flag properties where this falls below the minimum threshold (typically 10m).
   > - Sensitive premises = hospitals, schools, dialysis_home. These require DWI notification.

6. **Add sample questions** — for each of the 10 operator questions listed above, click "Add sample question"
7. **Add verified queries** — for Q1-Q10, click the question, then "Add verified query", paste the tested SQL from `genie/operator_verified_queries.sql`
8. **Test each sample question** — click it in the UI, verify Genie returns correct answer
9. **Share** — set permissions for demo viewers

**Validation checklist:**
- [ ] All 16 trusted assets listed and accessible
- [ ] System prompt pasted correctly
- [ ] All 10 sample questions added
- [ ] All 10 verified queries pre-tested and return correct results
- [ ] Spatial queries (Q6 ST_Distance, Q8 ST_Contains) execute successfully
- [ ] Genie responds accurately to at least 8 of 10 sample questions

---

### Task 9b — Executive Genie Space "Water Operations Intelligence" (🔨 CODE + 📋 MANUAL GUIDE)

**Goal:** Phase 1: Write and test 8 verified SQL queries. Phase 2: SA guide for executive Genie Space creation.

**Outputs:**
- Phase 1: `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/genie/executive_verified_queries.sql`
- Phase 2: `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides/04_genie_executive_guide.md`

#### Phase 1 — Query Development & Testing (🔨 CODE)

**Q1:** "What's our total Ofwat penalty exposure across all active incidents right now?"
```sql
-- Expected: ~£180K for the active incident
SELECT
  SUM(CASE WHEN TIMESTAMPDIFF(HOUR, di.start_timestamp, TIMESTAMP '2026-04-07 05:30:00') > 3
       THEN di.total_properties_affected * (TIMESTAMPDIFF(HOUR, di.start_timestamp, TIMESTAMP '2026-04-07 05:30:00') - 3) * 580
       ELSE 0 END) as total_penalty_exposure_gbp
FROM water_digital_twin.gold.dim_incidents di
WHERE di.status = 'active';
```

**Q2:** "How many properties have exceeded the 3-hour supply interruption threshold this month?"
```sql
-- Expected: 441 (total from the active incident, since it's >3h duration)
SELECT SUM(di.total_properties_affected) as properties_exceeding_3h
FROM water_digital_twin.gold.dim_incidents di
WHERE di.status = 'active'
  AND di.start_timestamp >= '2026-04-01'
  AND TIMESTAMPDIFF(HOUR, di.start_timestamp, TIMESTAMP '2026-04-07 05:30:00') > 3;
```

**Q3:** "Which DMAs have had the most incidents this year? Show the top 10 with total properties affected."
```sql
-- Expected: DEMO_DMA_01 in top 10 (3+ incidents including historical)
SELECT dma_code, COUNT(*) as incident_count, SUM(total_properties_affected) as total_affected
FROM water_digital_twin.gold.dim_incidents
WHERE start_timestamp >= '2026-01-01'
GROUP BY dma_code
ORDER BY incident_count DESC
LIMIT 10;
```

**Q4:** "Show me all incidents in the last 30 days where more than 100 properties were affected"
```sql
-- Expected: at least the active incident (441 properties)
SELECT incident_id, dma_code, start_timestamp, end_timestamp, status, severity, total_properties_affected
FROM water_digital_twin.gold.dim_incidents
WHERE start_timestamp >= DATEADD(DAY, -30, TIMESTAMP '2026-04-07 05:30:00')
  AND total_properties_affected > 100
ORDER BY start_timestamp DESC;
```

**Q5:** "What percentage of affected customers received proactive notification before complaining?"
```sql
-- Expected: high % (423 notified proactively vs 47 complaints)
SELECT
  cr.customer_count as proactive_notifications,
  (SELECT COUNT(*) FROM water_digital_twin.silver.customer_complaints
   WHERE dma_code = 'DEMO_DMA_01' AND complaint_timestamp >= '2026-04-07 02:00:00') as complaints_received,
  ROUND(cr.customer_count * 100.0 /
    NULLIF((SELECT COUNT(*) FROM water_digital_twin.silver.customer_complaints
     WHERE dma_code = 'DEMO_DMA_01' AND complaint_timestamp >= '2026-04-07 02:00:00'), 0), 1) as proactive_rate_pct
FROM water_digital_twin.gold.comms_requests cr
WHERE cr.incident_id = 'INC-2026-0407-001' AND cr.status = 'sent';
```

**Q6:** "Which DMAs contain hospitals or schools affected by supply interruptions this quarter?"
```sql
-- Expected: DEMO_DMA_01 with 1 hospital, 2 schools
SELECT p.dma_code, p.property_type, COUNT(*) as count
FROM water_digital_twin.silver.dim_properties p
JOIN water_digital_twin.gold.dma_status s ON p.dma_code = s.dma_code
WHERE p.property_type IN ('hospital', 'school')
  AND s.rag_status IN ('RED', 'AMBER')
GROUP BY p.dma_code, p.property_type
ORDER BY p.dma_code;
```

**Q7:** "What's the average time from incident detection to DWI notification across all incidents this year?"
```sql
-- Expected: a few hours (based on the demo scenario)
SELECT AVG(TIMESTAMPDIFF(MINUTE,
  (SELECT MIN(ie2.timestamp) FROM water_digital_twin.gold.incident_events ie2 WHERE ie2.incident_id = ie.incident_id AND ie2.event_type = 'anomaly_detected'),
  ie.timestamp)) / 60.0 as avg_hours_to_dwi
FROM water_digital_twin.gold.incident_events ie
WHERE ie.event_type = 'regulatory_notification'
  AND ie.timestamp >= '2026-01-01';
```

**Q8:** "Compare incident frequency this AMP period vs. last AMP period for the top 10 worst DMAs"
```sql
-- Expected: meaningful comparison data
SELECT dma_code,
  COUNT(CASE WHEN start_timestamp >= '2025-04-01' THEN 1 END) as amp8_incidents,
  COUNT(CASE WHEN start_timestamp < '2025-04-01' AND start_timestamp >= '2020-04-01' THEN 1 END) as amp7_incidents
FROM water_digital_twin.gold.dim_incidents
GROUP BY dma_code
ORDER BY amp8_incidents DESC
LIMIT 10;
```

#### Phase 2 — SA Configuration Guide (📋 MANUAL GUIDE)

1. Navigate to: AI/BI → Genie Spaces → "Create Genie Space"
2. Space name: `Water Operations Intelligence`
3. Description: `Executive Genie Space for water utility leadership, compliance managers, and operations directors. Ask questions about regulatory compliance, financial exposure, incident trends, and AMP8 investment planning.`
4. **Add trusted assets:**
   - `water_digital_twin.gold.mv_regulatory_compliance`
   - `water_digital_twin.gold.mv_incident_summary`
   - `water_digital_twin.gold.mv_dma_pressure`
   - `water_digital_twin.gold.mv_reservoir_status`
   - `water_digital_twin.gold.dim_incidents`
   - `water_digital_twin.gold.incident_events`
   - `water_digital_twin.gold.dma_status`
   - `water_digital_twin.gold.dma_summary`
   - `water_digital_twin.silver.dim_properties`
5. **Set instructions** — paste this exact system prompt:

   > You are an AI assistant for water utility executives, regulatory compliance managers, and operations directors at Water Utilities.
   >
   > Key business context:
   > - **Ofwat** is the economic regulator. The **OPA (Outcome Performance Assessment)** penalises supply interruptions. Key thresholds: properties without supply for **>3 hours** count towards the annual performance commitment; **>12 hours** triggers escalated penalties. Penalties are calculated as: number of affected properties × duration above threshold × per-property-hour rate.
   > - **DWI (Drinking Water Inspectorate)** requires notification for events affecting **sensitive premises** (hospitals, schools, dialysis centres). Written Event Reports are required within hours. **Regulation 35** requires a contamination ingress risk assessment during low-pressure events.
   > - **C-MeX (Customer Measure of Experience)** scores improve when customers receive **proactive notification** before they complain.
   > - **AMP8** (Asset Management Period 2025-2030) — water companies allocate capital investment based on asset condition and incident history. DMAs with frequent incidents are candidates for mains replacement in the capital programme.
   > - **ESG** — faster burst detection reduces average leak duration, contributing to leakage reduction targets.
   > - When asked about financial exposure, use the formula: estimated_penalty = properties_exceeding_3h_threshold × projected_additional_hours × penalty_rate_per_property_hour. Use £580/property/hour as the default Ofwat penalty rate unless told otherwise.

6. **Add sample questions** — all 8 executive questions listed above
7. **Add verified queries** — paste tested SQL from `genie/executive_verified_queries.sql`
8. **Test each sample question** in UI
9. **Share** — set permissions

**Validation checklist:**
- [ ] All 9 trusted assets listed and accessible
- [ ] System prompt pasted correctly
- [ ] All 8 sample questions added
- [ ] All 8 verified queries pre-tested and return correct results
- [ ] Executive penalty exposure question returns ~£180K
- [ ] AMP8 question returns meaningful data
- [ ] Genie responds accurately to at least 6 of 8 sample questions

---

### Task 10 — Executive Dashboard (📋 MANUAL GUIDE)

**Goal:** Produce a step-by-step SA guide for creating the AI/BI Lakeview dashboard as the executive/management view.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/guides/05_dashboard_guide.md`

**Step-by-step creation:**

1. Navigate to: `https://adb-984752964297111.11.azuredatabricks.net/` → AI/BI → Dashboards → "Create Dashboard"
2. Dashboard name: `Water Operations — Executive View`

**Page 1: "Incident Overview"**

| Position | Tile Title | Type | SQL Query |
|---|---|---|---|
| Top-left | Active Incidents | Counter | `SELECT COUNT(*) as value FROM water_digital_twin.gold.dim_incidents WHERE status = 'active'` |
| Top-center-left | Total Properties Affected | Counter | `SELECT SUM(total_properties_affected) as value FROM water_digital_twin.gold.dim_incidents WHERE status = 'active'` |
| Top-center-right | Properties >3h Threshold | Counter (RED if >0) | `SELECT SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, CURRENT_TIMESTAMP()) > 3 THEN total_properties_affected ELSE 0 END) as value FROM water_digital_twin.gold.dim_incidents WHERE status = 'active'` |
| Top-right | Est. Penalty Exposure | Counter (£) | `SELECT SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, CURRENT_TIMESTAMP()) > 3 THEN total_properties_affected * (TIMESTAMPDIFF(HOUR, start_timestamp, CURRENT_TIMESTAMP()) - 3) * 580 ELSE 0 END) as value FROM water_digital_twin.gold.dim_incidents WHERE status = 'active'` |
| Middle-left (wide) | DMA Health Map | Map | `SELECT d.dma_code, d.dma_name, d.geometry_wkt, s.rag_status, s.avg_pressure, s.sensitive_premises_count FROM water_digital_twin.gold.dma_status s JOIN water_digital_twin.silver.dim_dma d ON s.dma_code = d.dma_code` |
| Middle-right | Regulatory Deadline Status | Table | `SELECT di.incident_id, 'DWI Written Report' as deadline_type, TIMESTAMPADD(HOUR, 12, di.start_timestamp) as deadline, TIMESTAMPDIFF(MINUTE, CURRENT_TIMESTAMP(), TIMESTAMPADD(HOUR, 12, di.start_timestamp)) as minutes_remaining FROM water_digital_twin.gold.dim_incidents di WHERE di.status = 'active' UNION ALL SELECT di.incident_id, 'Ofwat 12h Threshold', TIMESTAMPADD(HOUR, 12, di.start_timestamp), TIMESTAMPDIFF(MINUTE, CURRENT_TIMESTAMP(), TIMESTAMPADD(HOUR, 12, di.start_timestamp)) FROM water_digital_twin.gold.dim_incidents di WHERE di.status = 'active'` |
| Bottom-left | C-MeX Proactive Rate | Counter (%) | `SELECT ROUND(MAX(cr.customer_count) * 100.0 / NULLIF(COUNT(cc.complaint_id), 0), 1) as value FROM water_digital_twin.gold.comms_requests cr JOIN water_digital_twin.gold.dim_incidents di ON cr.incident_id = di.incident_id LEFT JOIN water_digital_twin.silver.customer_complaints cc ON di.dma_code = cc.dma_code WHERE di.status = 'active'` |
| Bottom-right | Properties >12h Threshold | Counter | `SELECT SUM(CASE WHEN TIMESTAMPDIFF(HOUR, start_timestamp, CURRENT_TIMESTAMP()) > 12 THEN total_properties_affected ELSE 0 END) as value FROM water_digital_twin.gold.dim_incidents WHERE status = 'active'` |

**Page 2: "AMP8 Investment Insights"**

| Position | Tile Title | Type | SQL Query |
|---|---|---|---|
| Top-left (wide) | Top 10 DMAs by Incident Frequency | Bar Chart | `SELECT dma_code, COUNT(*) as incident_count, SUM(total_properties_affected) as total_affected FROM water_digital_twin.gold.dim_incidents WHERE start_timestamp >= '2025-04-01' GROUP BY dma_code ORDER BY incident_count DESC LIMIT 10` |
| Top-right | Incident Trend (12 months) | Line Chart | `SELECT DATE_TRUNC('month', start_timestamp) as month, COUNT(*) as incident_count FROM water_digital_twin.gold.dim_incidents WHERE start_timestamp >= DATEADD(MONTH, -12, CURRENT_TIMESTAMP()) GROUP BY DATE_TRUNC('month', start_timestamp) ORDER BY month` |
| Bottom-left | Anomaly Trends | Line Chart | `SELECT DATE_TRUNC('week', timestamp) as week, COUNT(CASE WHEN is_anomaly = true THEN 1 END) as anomaly_count FROM water_digital_twin.gold.anomaly_scores WHERE timestamp >= DATEADD(MONTH, -3, CURRENT_TIMESTAMP()) GROUP BY DATE_TRUNC('week', timestamp) ORDER BY week` |
| Bottom-right | Sensor Coverage | Table | `SELECT s.dma_code, d.dma_name, COUNT(*) as sensor_count, SUM(CASE WHEN s.sensor_type = 'pressure' THEN 1 ELSE 0 END) as pressure_sensors, SUM(CASE WHEN s.sensor_type = 'flow' THEN 1 ELSE 0 END) as flow_sensors FROM water_digital_twin.silver.dim_sensor s JOIN water_digital_twin.silver.dim_dma d ON s.dma_code = d.dma_code GROUP BY s.dma_code, d.dma_name ORDER BY sensor_count DESC LIMIT 20` |

**Dashboard-level filters:**
- Time range: last 7d / 30d / 90d / 12m (apply to relevant tiles)
- DMA code: dropdown filter
- Incident severity: dropdown filter (low / medium / high / critical)

5. **Publish** the dashboard and set sharing permissions for demo viewers.

**Validation checklist:**
- [ ] All tiles render with correct data
- [ ] Counter tiles show meaningful numbers (not null/zero)
- [ ] Map tile shows DMA polygons with RAG colouring
- [ ] Bar chart shows DEMO_DMA_01 in top 10
- [ ] Filters work across all tiles
- [ ] Dashboard loads in < 5 seconds

---

## Agent 5: Eve — Demo Delivery Lead

### Task 11 — README & Reset (🔨 CODE)

**Goal:** Write a runbook, demo health check script, and reset procedure.

**Outputs:**
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/docs/README.md`
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/scripts/demo_health_check.py`
- `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/scripts/reset_demo.py`

#### README/Runbook Content:
- **Prerequisites:** Workspace URL, CLI profile `adb-98`, preview features enabled (reference Diana's guide), Lakebase provisioned
- **Setup steps:** Reference Diana's Task 1 guide → run Alice's schema DDL → run Bob's data gen → run Bob's pipeline → run Alice's Lakebase sync → deploy Charlie's app → follow Charlie's Genie/Dashboard guides
- **Asset paths:** Reference exact notebook paths from Alice's Task 3 folder layout
- **Demo preparation:** Run health check 10 min before demo

#### Demo Health Check Script (`demo_health_check.py`):
- Runs all **22 verification queries** from Section 5 of the approved plan
- Reports pass/fail with colour-coded output (green ✓ / red ✗)
- For each query: shows the query, expected result, actual result
- Summary: "22/22 passed" or "20/22 passed — 2 FAILED" with details

**The 22 verification queries** (inline — the health check script must include all of these):

1. DEMO_DMA_01 exists and is RED → `SELECT dma_code, rag_status FROM gold.dma_status WHERE dma_code = 'DEMO_DMA_01'`
2. Property type distribution → `SELECT property_type, COUNT(*) FROM silver.dim_properties WHERE dma_code = 'DEMO_DMA_01' GROUP BY property_type`
3. Pressure drop visible → `SELECT timestamp, value FROM silver.fact_telemetry WHERE sensor_id = 'DEMO_SENSOR_01'...`
4. Flow drop visible → `SELECT timestamp, flow_rate FROM silver.fact_telemetry WHERE sensor_id = 'DEMO_FLOW_01'...`
5. Complaints lag pressure drop → `SELECT MIN(complaint_timestamp), COUNT(*) FROM silver.customer_complaints WHERE dma_code = 'DEMO_DMA_01'`
6. Elevation coherence → avg customer_height > 35m for complainants
7. Upstream cause exists → `DEMO_PUMP_01` with status 'tripped', trip_timestamp '2026-04-07 02:03:00'
8. Pump feeds affected DMAs → 3 rows (primary + 2 secondary)
9. Trunk main with LINESTRING → `DEMO_TM_001` geometry starts with 'LINESTRING('
10. Isolation valves → 2 rows, both status 'open'
11. Reservoir exists → `DEMO_SR_01` at 43%, capacity 5.0
12. Anomaly score high → `DEMO_SENSOR_01` at 02:15 has anomaly_sigma > 3.0
13. Amber DMAs low anomaly → DEMO_DMA_02 sensors all max sigma < 2.0
14. RAG timeline → GREEN→AMBER→RED progression for DEMO_DMA_01
15. Incident event log → at least 10 events
16. Communications log → at least 4 records
17. Response playbook → SOP-WN-042 exists
18. Active incident → INC-2026-0407-001, status active, severity high, 441 properties
19. Historical incidents → total >= 6, resolved >= 5
20. dma_status populated → 500 DMAs (1 RED, 2 AMBER)
21. dma_summary has reservoir/incident links → DEMO_DMA_01 reservoir ~43%, incident linked
22. Sensitive premises count → DEMO_DMA_01 >= 6

#### Reset Script (`reset_demo.py`):
- **Dry-run mode:** `--dry-run` flag shows what will be deleted without executing
- **Requires confirmation** before destructive operations
- Steps:
  1. DROP all tables in `water_digital_twin.bronze`, `silver`, `gold` schemas
  2. Re-run data generation notebooks in order (geography → dimensions → facts → incidents)
  3. Re-run SDP pipeline, anomaly scoring, metric views
  4. Re-run Lakebase sync
  5. Run all 22 verification queries and report pass/fail
- Target reset time: < 15 minutes

**Inputs:** All other agents' outputs; approved plan
**Outputs:** README, health check script, reset script
**Acceptance Criteria:**
- [ ] Reset script drops and recreates all schemas (with dry-run and confirmation)
- [ ] Health check runs all 22 queries and reports pass/fail
- [ ] README references exact notebook paths from Task 3
- [ ] SA can set up the entire demo by following the README

---

### Task 12 — Live Demo Script (🔨 CODE)

**Goal:** Create a talk-track outline mapping each demo step to the 9-scene storyline.

**Output:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo/docs/demo_talk_track.md`

**Structure:**

For each of the 9 scenes (1-6, 7a, 7b, optional 8):
- **What to click:** Exact navigation in the app/workspace
- **What to say:** Key talking points and transition phrases
- **What to highlight:** Features, data points, before/after comparisons
- **Audience-specific hooks** (flagged inline):
  - 🔧 Operator audience: emphasise handover, playbook, flow data
  - 📊 Exec audience: emphasise penalty exposure, audit trail, AMP8
  - 📋 Regulation audience: emphasise DWI/Ofwat deadlines, PDF export
  - 🏗️ Technical audience: emphasise architecture, data quality, Spatial SQL, ML
  - 🔬 Data science audience: lead with Scene 8, show score flow into app

**Key moments to rehearse:**
- **Scene 6 executive summary landing line** (the demo climax): "This incident has been active for 3 hours 27 minutes. It has affected 441 properties including 1 hospital, 2 schools, and 3 home dialysis patients..."
- **Integration architecture slide** (OSI PI → Databricks → Lakebase → App, OT-IT boundary note)
- **TCO positioning:** FTE savings from post-incident admin reduction (15-20 incidents × 4-6h → 30 min = 50-110 hours/year saved)
- **"Time to executive briefing" KPI:** current 45 min → future zero
- **Scalability narrative:** 100 → 2,000 users, no per-user licence cap

**Scene 8 presenter guide:**
- When to include: extended sessions (60-65 min), technical/DS audiences
- Level 1 only: +3 min | Levels 1+2: +10 min | All three: +15 min
- Transition from Scene 7b: "Now let me show you what powers that anomaly detection under the hood..."

**Inputs:** All other agents' outputs; approved plan Sections 1-6
**Outputs:** Demo talk track document
**Acceptance Criteria:**
- [ ] Covers all 9 scenes (1-6, 7a, 7b, optional 8)
- [ ] References all 6 messaging pillars
- [ ] Both Genie Spaces referenced in the talk track
- [ ] ML notebook (Scene 8) has timing variants
- [ ] Audience-specific hooks flagged throughout
- [ ] PDF export tested and referenced
- [ ] Integration architecture content included
- [ ] TCO and scalability narratives for Q&A section

---

## Agent Dependency Graph

```
Diana Task 1 (Workspace Setup)
  ↓
Alice Task 3 (Repo Structure) ──→ Alice Task 4 (Schema DDL)
                                       ↓
                              Bob Task 6 (Data Generation)
                                       ↓
                              Bob Task 7 (SDP Pipeline)
                                       ↓
                              Bob Task 7b (Anomaly Scoring)
                                       ↓
                        ┌──────────────┼──────────────┐
                        ↓              ↓              ↓
               Bob Task 7c     Alice Task 5    Bob Task 7d (optional)
              (Metric Views)  (Lakebase Sync)  (ML Notebook)
                        ↓              ↓
                        └──────┬───────┘
                               ↓
                    Charlie Task 8 (App)
                    Charlie Task 9a/9b (Genie)
                    Charlie Task 10 (Dashboard)
                               ↓
                    Eve Task 11 (README/Reset)
                    Eve Task 12 (Demo Script)
```

**Parallelism opportunities:**
- Diana Task 1 and Alice Task 3 can start immediately (no dependencies)
- Alice Task 4 can start once Task 3 is done
- Bob Tasks 7c, Alice Task 5, and Bob Task 7d can run in parallel after Task 7b
- Charlie Tasks 8, 9a, 9b, 10 can start once Bob's data and Alice's Lakebase sync are complete
- Eve Tasks 11 and 12 run last (depend on all other outputs)
