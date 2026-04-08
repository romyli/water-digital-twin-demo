-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Water Digital Twin — Unity Catalog DDL
-- MAGIC
-- MAGIC This notebook creates the full schema for the Water Digital Twin demo.
-- MAGIC All statements are idempotent (`CREATE ... IF NOT EXISTS` / `CREATE OR REPLACE`).

-- COMMAND ----------

-- ============================================================
-- CATALOG & SCHEMAS
-- ============================================================

CREATE CATALOG IF NOT EXISTS water_digital_twin
COMMENT 'Water utility digital twin demo — bronze/silver/gold medallion architecture';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.bronze
COMMENT 'Raw ingestion layer — data as-is from source systems';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.silver
COMMENT 'Curated layer — cleansed dimensions and facts';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS water_digital_twin.gold
COMMENT 'Consumption layer — aggregated views, incidents, and operational tables';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Tables

-- COMMAND ----------

-- ============================================================
-- BRONZE TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_telemetry (
  sensor_id       STRING    COMMENT 'Unique identifier for the sensor',
  timestamp       TIMESTAMP COMMENT 'Reading timestamp from the source system',
  value           DOUBLE    COMMENT 'Raw sensor reading value',
  quality_flag    STRING    COMMENT 'Data quality indicator from the source (e.g. GOOD, SUSPECT)',
  source_system   STRING    COMMENT 'Originating telemetry system name',
  ingested_at     TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw sensor telemetry readings ingested from SCADA and IoT systems';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_assets (
  asset_id        STRING    COMMENT 'Unique identifier for the infrastructure asset',
  asset_type      STRING    COMMENT 'Type of asset (e.g. pump_station, trunk_main, prv)',
  name            STRING    COMMENT 'Human-readable asset name',
  status          STRING    COMMENT 'Current operational status of the asset',
  latitude        DOUBLE    COMMENT 'WGS-84 latitude of the asset',
  longitude       DOUBLE    COMMENT 'WGS-84 longitude of the asset',
  geometry_wkt    STRING    COMMENT 'Well-Known Text geometry representation',
  metadata_json   STRING    COMMENT 'Additional asset metadata stored as JSON string',
  source_system   STRING    COMMENT 'Originating asset management system name',
  ingested_at     TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw infrastructure asset records from the asset management system';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_customer_contacts (
  uprn            STRING    COMMENT 'Unique Property Reference Number',
  address         STRING    COMMENT 'Full postal address of the property',
  postcode        STRING    COMMENT 'Postal code of the property',
  latitude        DOUBLE    COMMENT 'WGS-84 latitude of the property',
  longitude       DOUBLE    COMMENT 'WGS-84 longitude of the property',
  property_type   STRING    COMMENT 'Type of property (e.g. domestic, commercial)',
  dma_code        STRING    COMMENT 'District Metered Area code the property belongs to',
  customer_height DOUBLE    COMMENT 'Elevation of the customer property in metres above datum',
  source_system   STRING    COMMENT 'Originating CRM or GIS system name',
  ingested_at     TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw customer property contact records from the CRM system';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_complaints (
  complaint_id        STRING    COMMENT 'Unique identifier for the customer complaint',
  uprn                STRING    COMMENT 'Unique Property Reference Number of the complainant',
  dma_code            STRING    COMMENT 'District Metered Area code associated with the complaint',
  complaint_timestamp TIMESTAMP COMMENT 'Timestamp when the complaint was lodged',
  complaint_type      STRING    COMMENT 'Category of the complaint (e.g. low pressure, no water)',
  source_system       STRING    COMMENT 'Originating complaints management system name',
  ingested_at         TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw customer complaint records from the complaints management system';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_dma_boundaries (
  dma_code        STRING    COMMENT 'Unique code for the District Metered Area',
  dma_name        STRING    COMMENT 'Human-readable name for the DMA',
  dma_area_code   STRING    COMMENT 'Parent area grouping code for the DMA',
  geometry_wkt    STRING    COMMENT 'Well-Known Text polygon boundary of the DMA',
  source_system   STRING    COMMENT 'Originating GIS system name',
  ingested_at     TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw District Metered Area boundary polygons from GIS';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.bronze.raw_pma_boundaries (
  pma_code        STRING    COMMENT 'Unique code for the Pressure Management Area',
  pma_name        STRING    COMMENT 'Human-readable name for the PMA',
  dma_code        STRING    COMMENT 'Parent DMA code that this PMA belongs to',
  geometry_wkt    STRING    COMMENT 'Well-Known Text polygon boundary of the PMA',
  source_system   STRING    COMMENT 'Originating GIS system name',
  ingested_at     TIMESTAMP COMMENT 'Timestamp when the record was ingested into the lakehouse'
)
COMMENT 'Raw Pressure Management Area boundary polygons from GIS';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Tables

-- COMMAND ----------

-- ============================================================
-- SILVER TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_sensor (
  sensor_id       STRING    COMMENT 'Unique identifier for the sensor',
  name            STRING    COMMENT 'Human-readable sensor name',
  sensor_type     STRING    COMMENT 'Type of measurement: pressure or flow',
  dma_code        STRING    COMMENT 'District Metered Area the sensor is located in',
  pma_code        STRING    COMMENT 'Pressure Management Area the sensor is located in (nullable)',
  latitude        DOUBLE    COMMENT 'WGS-84 latitude of the sensor',
  longitude       DOUBLE    COMMENT 'WGS-84 longitude of the sensor',
  elevation       DOUBLE    COMMENT 'Elevation of the sensor in metres above datum',
  geometry_wkt    STRING    COMMENT 'Well-Known Text point geometry of the sensor',
  h3_index        STRING    COMMENT 'H3 hexagonal spatial index for the sensor location',
  is_active       BOOLEAN   COMMENT 'Whether the sensor is currently active',
  installed_date  DATE      COMMENT 'Date the sensor was installed'
)
COMMENT 'Dimension table of pressure and flow sensors across the network';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_dma (
  dma_code                  STRING    COMMENT 'Unique code for the District Metered Area',
  dma_name                  STRING    COMMENT 'Human-readable name for the DMA',
  dma_area_code             STRING    COMMENT 'Parent area grouping code for the DMA',
  geometry_wkt              STRING    COMMENT 'Well-Known Text polygon boundary of the DMA',
  centroid_latitude         DOUBLE    COMMENT 'Latitude of the DMA polygon centroid',
  centroid_longitude        DOUBLE    COMMENT 'Longitude of the DMA polygon centroid',
  avg_elevation             DOUBLE    COMMENT 'Average ground elevation across the DMA in metres',
  h3_index                  STRING    COMMENT 'H3 hexagonal spatial index for the DMA centroid',
  pressure_red_threshold    DOUBLE    DEFAULT 15.0 COMMENT 'Pressure threshold (metres head) below which DMA status is RED',
  pressure_amber_threshold  DOUBLE    DEFAULT 25.0 COMMENT 'Pressure threshold (metres head) below which DMA status is AMBER'
)
COMMENT 'Dimension table of District Metered Areas with boundaries and RAG thresholds';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_pma (
  pma_code            STRING    COMMENT 'Unique code for the Pressure Management Area',
  pma_name            STRING    COMMENT 'Human-readable name for the PMA',
  dma_code            STRING    COMMENT 'Parent DMA code that this PMA belongs to',
  geometry_wkt        STRING    COMMENT 'Well-Known Text polygon boundary of the PMA',
  centroid_latitude   DOUBLE    COMMENT 'Latitude of the PMA polygon centroid',
  centroid_longitude  DOUBLE    COMMENT 'Longitude of the PMA polygon centroid'
)
COMMENT 'Dimension table of Pressure Management Areas within DMAs';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_properties (
  uprn            STRING    COMMENT 'Unique Property Reference Number',
  address         STRING    COMMENT 'Full postal address of the property',
  postcode        STRING    COMMENT 'Postal code of the property',
  property_type   STRING    COMMENT 'Type of property: domestic, school, hospital, commercial, nursery, key_account, or dialysis_home',
  dma_code        STRING    COMMENT 'District Metered Area the property belongs to',
  pma_code        STRING    COMMENT 'Pressure Management Area the property belongs to (nullable)',
  customer_height DOUBLE    COMMENT 'Elevation of the customer property in metres above datum',
  latitude        DOUBLE    COMMENT 'WGS-84 latitude of the property',
  longitude       DOUBLE    COMMENT 'WGS-84 longitude of the property',
  geometry_wkt    STRING    COMMENT 'Well-Known Text point geometry of the property',
  h3_index        STRING    COMMENT 'H3 hexagonal spatial index for the property location'
)
COMMENT 'Dimension table of customer properties with location, type, and elevation';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_assets (
  asset_id          STRING    COMMENT 'Unique identifier for the infrastructure asset',
  asset_type        STRING    COMMENT 'Type of asset: pump_station, trunk_main, isolation_valve, prv, or treatment_works',
  name              STRING    COMMENT 'Human-readable asset name',
  status            STRING    COMMENT 'Operational status: operational, tripped, failed, maintenance, or decommissioned',
  latitude          DOUBLE    COMMENT 'WGS-84 latitude of the asset',
  longitude         DOUBLE    COMMENT 'WGS-84 longitude of the asset',
  geometry_wkt      STRING    COMMENT 'Well-Known Text geometry representation of the asset',
  diameter_inches   INT       COMMENT 'Pipe or valve diameter in inches (where applicable)',
  length_km         DOUBLE    COMMENT 'Length of the asset in kilometres (where applicable)',
  trip_timestamp    TIMESTAMP COMMENT 'Timestamp of the most recent trip event (nullable)',
  installed_date    DATE      COMMENT 'Date the asset was installed or commissioned'
)
COMMENT 'Dimension table of network infrastructure assets (pumps, mains, valves, PRVs, treatment works)';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_asset_dma_feed (
  asset_id    STRING    COMMENT 'Identifier of the feeding infrastructure asset',
  dma_code    STRING    COMMENT 'District Metered Area being fed by this asset',
  feed_type   STRING    COMMENT 'Feed relationship type: primary or secondary'
)
COMMENT 'Mapping of which infrastructure assets feed which DMAs';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_reservoirs (
  reservoir_id            STRING    COMMENT 'Unique identifier for the reservoir',
  name                    STRING    COMMENT 'Human-readable reservoir name',
  capacity_ml             DOUBLE    COMMENT 'Total storage capacity in megalitres',
  current_level_pct       DOUBLE    COMMENT 'Current fill level as a percentage of capacity',
  hourly_demand_rate_ml   DOUBLE    COMMENT 'Current hourly demand draw in megalitres',
  latitude                DOUBLE    COMMENT 'WGS-84 latitude of the reservoir',
  longitude               DOUBLE    COMMENT 'WGS-84 longitude of the reservoir',
  geometry_wkt            STRING    COMMENT 'Well-Known Text geometry representation of the reservoir'
)
COMMENT 'Dimension table of service reservoirs with capacity and current levels';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.dim_reservoir_dma_feed (
  reservoir_id  STRING    COMMENT 'Identifier of the feeding reservoir',
  dma_code      STRING    COMMENT 'District Metered Area being fed by this reservoir',
  feed_type     STRING    COMMENT 'Feed relationship type: primary or secondary'
)
COMMENT 'Mapping of which reservoirs feed which DMAs';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.fact_telemetry (
  sensor_id             STRING    COMMENT 'Unique identifier for the sensor',
  timestamp             TIMESTAMP COMMENT 'Reading timestamp',
  sensor_type           STRING    COMMENT 'Type of measurement: pressure or flow',
  value                 DOUBLE    COMMENT 'Calibrated sensor reading value',
  total_head_pressure   DOUBLE    COMMENT 'Total head pressure accounting for sensor elevation (metres head)',
  flow_rate             DOUBLE    COMMENT 'Calculated flow rate (where applicable)',
  quality_flag          STRING    COMMENT 'Data quality indicator after cleansing'
)
COMMENT 'Cleansed fact table of sensor telemetry with derived pressure and flow metrics';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.silver.customer_complaints (
  complaint_id        STRING    COMMENT 'Unique identifier for the customer complaint',
  uprn                STRING    COMMENT 'Unique Property Reference Number of the complainant',
  dma_code            STRING    COMMENT 'District Metered Area code associated with the complaint',
  complaint_timestamp TIMESTAMP COMMENT 'Timestamp when the complaint was lodged',
  complaint_type      STRING    COMMENT 'Category of the complaint (e.g. low pressure, no water)'
)
COMMENT 'Cleansed customer complaints enriched with DMA mapping';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Tables

-- COMMAND ----------

-- ============================================================
-- GOLD TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_status (
  dma_code                  STRING    COMMENT 'District Metered Area code',
  rag_status                STRING    COMMENT 'Current RAG status: RED, AMBER, or GREEN',
  avg_pressure              DOUBLE    COMMENT 'Current average pressure across the DMA in metres head',
  min_pressure              DOUBLE    COMMENT 'Current minimum pressure across the DMA in metres head',
  sensor_count              INT       COMMENT 'Number of active sensors in the DMA',
  property_count            INT       COMMENT 'Total number of properties in the DMA',
  sensitive_premises_count  INT       COMMENT 'Number of sensitive premises (hospitals, schools, nurseries, dialysis homes)',
  has_active_incident       BOOLEAN   COMMENT 'Whether there is an active incident in this DMA',
  last_updated              TIMESTAMP COMMENT 'Timestamp of the last status calculation'
)
COMMENT 'Current operational status of each DMA with RAG rating and key metrics';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_rag_history (
  dma_code      STRING    COMMENT 'District Metered Area code',
  timestamp     TIMESTAMP COMMENT 'Timestamp of the RAG status snapshot',
  rag_status    STRING    COMMENT 'RAG status at this point in time: RED, AMBER, or GREEN',
  avg_pressure  DOUBLE    COMMENT 'Average pressure at this point in time in metres head',
  min_pressure  DOUBLE    COMMENT 'Minimum pressure at this point in time in metres head'
)
COMMENT 'Historical time-series of DMA RAG status changes for trend analysis';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.anomaly_scores (
  sensor_id       STRING    COMMENT 'Unique identifier for the sensor',
  timestamp       TIMESTAMP COMMENT 'Timestamp of the anomaly evaluation',
  anomaly_sigma   DOUBLE    COMMENT 'Number of standard deviations from the baseline',
  baseline_value  DOUBLE    COMMENT 'Expected baseline value from the model',
  actual_value    DOUBLE    COMMENT 'Actual observed sensor reading',
  is_anomaly      BOOLEAN   COMMENT 'Whether this reading is classified as anomalous',
  scoring_method  STRING    COMMENT 'Algorithm used for anomaly scoring (e.g. z-score, isolation_forest)'
)
COMMENT 'Anomaly detection scores for sensor readings against learned baselines';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dma_summary (
  dma_code                    STRING    COMMENT 'District Metered Area code',
  dma_name                    STRING    COMMENT 'Human-readable name for the DMA',
  rag_status                  STRING    COMMENT 'Current RAG status: RED, AMBER, or GREEN',
  avg_pressure                DOUBLE    COMMENT 'Current average pressure in metres head',
  avg_flow                    DOUBLE    COMMENT 'Current average flow rate across the DMA',
  property_count              INT       COMMENT 'Total number of properties in the DMA',
  sensor_count                INT       COMMENT 'Number of active sensors in the DMA',
  sensitive_premises_count    INT       COMMENT 'Number of sensitive premises in the DMA',
  feeding_reservoir_id        STRING    COMMENT 'Primary reservoir feeding the DMA',
  reservoir_level_pct         DOUBLE    COMMENT 'Current fill level of the feeding reservoir as percentage',
  reservoir_hours_remaining   DOUBLE    COMMENT 'Estimated hours of supply remaining in the feeding reservoir',
  active_incident_id          STRING    COMMENT 'ID of the active incident in this DMA (nullable)',
  active_complaints_count     INT       COMMENT 'Number of active customer complaints in this DMA',
  last_updated                TIMESTAMP COMMENT 'Timestamp of the last summary calculation'
)
COMMENT 'Comprehensive DMA summary combining pressure, flow, reservoir, incident, and complaint data';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dim_incidents (
  incident_id                 STRING    COMMENT 'Unique identifier for the incident',
  dma_code                    STRING    COMMENT 'District Metered Area affected by the incident',
  root_cause_asset_id         STRING    COMMENT 'Asset identified as the root cause of the incident',
  start_timestamp             TIMESTAMP COMMENT 'Timestamp when the incident was opened',
  end_timestamp               TIMESTAMP COMMENT 'Timestamp when the incident was closed (nullable)',
  status                      STRING    COMMENT 'Current incident status (e.g. open, investigating, resolved)',
  severity                    STRING    COMMENT 'Incident severity level (e.g. P1, P2, P3)',
  total_properties_affected   INT       COMMENT 'Total number of properties impacted by the incident',
  sensitive_premises_affected BOOLEAN   COMMENT 'Whether any sensitive premises are affected'
)
COMMENT 'Incident dimension tracking pressure and supply incidents across DMAs';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.incident_events (
  event_id      STRING    COMMENT 'Unique identifier for the event',
  incident_id   STRING    COMMENT 'Parent incident this event belongs to',
  timestamp     TIMESTAMP COMMENT 'Timestamp of the event',
  event_type    STRING    COMMENT 'Type of event (e.g. detection, escalation, mitigation, resolution)',
  source        STRING    COMMENT 'Source of the event (e.g. telemetry, operator, AI)',
  description   STRING    COMMENT 'Free-text description of the event',
  operator_id   STRING    COMMENT 'Identifier of the operator who recorded the event (nullable)'
)
COMMENT 'Chronological event log for incidents capturing detection, actions, and resolution steps';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.communications_log (
  log_id          STRING    COMMENT 'Unique identifier for the communication log entry',
  incident_id     STRING    COMMENT 'Parent incident this communication relates to',
  timestamp       TIMESTAMP COMMENT 'Timestamp of the communication',
  contact_role    STRING    COMMENT 'Role of the person contacted (e.g. field_engineer, customer, manager)',
  method          STRING    COMMENT 'Communication method (e.g. phone, email, radio)',
  summary         STRING    COMMENT 'Summary of the communication',
  action_agreed   STRING    COMMENT 'Actions agreed during the communication',
  operator_id     STRING    COMMENT 'Identifier of the operator who made the communication'
)
COMMENT 'Log of all communications made during incident management';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.dim_response_playbooks (
  playbook_id       STRING    COMMENT 'Unique identifier for the response playbook',
  incident_type     STRING    COMMENT 'Type of incident this playbook applies to',
  sop_reference     STRING    COMMENT 'Standard Operating Procedure document reference',
  action_steps      STRING    COMMENT 'Ordered action steps for the response (stored as text or JSON)',
  last_updated_by   STRING    COMMENT 'Identifier of the person who last updated the playbook',
  last_updated_at   TIMESTAMP COMMENT 'Timestamp when the playbook was last updated'
)
COMMENT 'Standard response playbooks and SOPs for different incident types';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.shift_handovers (
  handover_id           STRING    COMMENT 'Unique identifier for the shift handover',
  incident_id           STRING    COMMENT 'Incident being handed over',
  outgoing_operator     STRING    COMMENT 'Operator handing over the shift',
  incoming_operator     STRING    COMMENT 'Operator receiving the shift',
  generated_summary     STRING    COMMENT 'AI-generated summary of incident status at handover',
  risk_of_escalation    STRING    COMMENT 'Assessed risk of escalation (e.g. low, medium, high)',
  current_trajectory    STRING    COMMENT 'Current trajectory of the incident (e.g. improving, stable, worsening)',
  operator_edits        STRING    COMMENT 'Manual edits or notes added by the outgoing operator',
  signed_off_at         TIMESTAMP COMMENT 'Timestamp when the outgoing operator signed off',
  acknowledged_at       TIMESTAMP COMMENT 'Timestamp when the incoming operator acknowledged the handover'
)
COMMENT 'Shift handover records for incident continuity between operators';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS water_digital_twin.gold.comms_requests (
  request_id                STRING    COMMENT 'Unique identifier for the communications request',
  incident_id               STRING    COMMENT 'Parent incident this request relates to',
  dma_code                  STRING    COMMENT 'District Metered Area affected',
  requested_by              STRING    COMMENT 'Operator who initiated the communications request',
  requested_at              TIMESTAMP COMMENT 'Timestamp when the request was made',
  message_template          STRING    COMMENT 'Template text for customer communications',
  affected_postcodes        STRING    COMMENT 'Comma-separated list of affected postcodes',
  estimated_restoration_time STRING   COMMENT 'Estimated time of service restoration',
  customer_count            INT       COMMENT 'Number of customers to be contacted',
  status                    STRING    COMMENT 'Request status (e.g. draft, approved, sent)'
)
COMMENT 'Customer communications requests generated during incidents';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Views

-- COMMAND ----------

-- ============================================================
-- GOLD VIEWS
-- ============================================================

CREATE OR REPLACE VIEW water_digital_twin.gold.vw_dma_pressure
COMMENT 'Aggregated pressure metrics per DMA per timestamp for dashboard consumption'
AS
SELECT
  s.dma_code,
  t.timestamp,
  AVG(t.value)               AS avg_pressure,
  MAX(t.value)               AS max_pressure,
  MIN(t.value)               AS min_pressure,
  AVG(t.total_head_pressure) AS avg_total_head_pressure,
  COUNT(*)                   AS reading_count
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s
  ON t.sensor_id = s.sensor_id
WHERE s.sensor_type = 'pressure'
GROUP BY s.dma_code, t.timestamp;

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.vw_pma_pressure
COMMENT 'Aggregated pressure metrics per PMA per timestamp for dashboard consumption'
AS
SELECT
  s.pma_code,
  t.timestamp,
  AVG(t.value)               AS avg_pressure,
  MAX(t.value)               AS max_pressure,
  MIN(t.value)               AS min_pressure,
  AVG(t.total_head_pressure) AS avg_total_head_pressure,
  COUNT(*)                   AS reading_count
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s
  ON t.sensor_id = s.sensor_id
WHERE s.sensor_type = 'pressure'
  AND s.pma_code IS NOT NULL
GROUP BY s.pma_code, t.timestamp;

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.vw_property_pressure
COMMENT 'Per-property effective pressure accounting for customer elevation'
AS
SELECT
  p.uprn,
  p.dma_code,
  p.property_type,
  p.customer_height,
  t.timestamp,
  t.total_head_pressure,
  t.total_head_pressure - p.customer_height AS effective_pressure
FROM water_digital_twin.silver.dim_properties p
JOIN water_digital_twin.silver.dim_sensor s
  ON p.dma_code = s.dma_code
JOIN water_digital_twin.silver.fact_telemetry t
  ON s.sensor_id = t.sensor_id
WHERE s.sensor_type = 'pressure';

-- COMMAND ----------

CREATE OR REPLACE VIEW water_digital_twin.gold.vw_dma_pressure_sensor
COMMENT 'Per-sensor pressure readings with DMA context for map visualisation'
AS
SELECT
  s.sensor_id,
  s.dma_code,
  s.name            AS sensor_name,
  s.latitude,
  s.longitude,
  t.timestamp,
  t.value           AS pressure,
  t.total_head_pressure
FROM water_digital_twin.silver.fact_telemetry t
JOIN water_digital_twin.silver.dim_sensor s
  ON t.sensor_id = s.sensor_id
WHERE s.sensor_type = 'pressure';
