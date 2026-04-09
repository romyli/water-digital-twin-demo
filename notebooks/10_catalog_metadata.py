# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 10 — Catalog Metadata
# MAGIC
# MAGIC Applies Unity Catalog metadata (PK/FK constraints, table comments, column comments)
# MAGIC to all Silver and Gold tables referenced by the Genie Spaces. This metadata enables
# MAGIC Genie to understand table relationships, disambiguate column semantics, and generate
# MAGIC correct SQL.
# MAGIC
# MAGIC **Catalog:** `water_digital_twin`
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Primary Key Constraints
# MAGIC 2. Foreign Key Constraints
# MAGIC 3. Table Comments
# MAGIC 4. Column Comments — Silver Tables
# MAGIC 5. Column Comments — Gold Tables
# MAGIC 6. Column Comments — Gold Metric Views (table-level only; metric view columns are defined in YAML)
# MAGIC 7. Validation
# MAGIC
# MAGIC **Idempotency:** Constraint additions are wrapped in try/except to handle already-exists errors.
# MAGIC Comment statements are inherently idempotent (overwrite previous values).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Primary Key Constraints

# COMMAND ----------

# DBTITLE 1,Add Primary Key Constraints (idempotent)
# NOTE: Silver dimension PKs (dim_dma, dim_sensor, dim_properties, dim_assets, dim_reservoirs)
# are defined at creation time via the `schema` parameter in 06_sdp_pipeline.py.
# Only Gold tables created by saveAsTable() need ALTER TABLE constraints.
pk_statements = [
    # Gold tables (created by saveAsTable in notebooks 05/07)
    "ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT pk_dim_incidents PRIMARY KEY (incident_id) NOT ENFORCED",
    "ALTER TABLE water_digital_twin.gold.incident_notifications ADD CONSTRAINT pk_incident_notifications PRIMARY KEY (incident_id) NOT ENFORCED",
    "ALTER TABLE water_digital_twin.gold.regulatory_notifications ADD CONSTRAINT pk_regulatory_notifications PRIMARY KEY (notification_id) NOT ENFORCED",
]

for sql in pk_statements:
    try:
        spark.sql(sql)
        print(f"OK:   {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]}")
    except Exception as e:
        if "already exists" in str(e).lower() or "CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"SKIP: {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} (already exists)")
        else:
            print(f"ERR:  {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Foreign Key Constraints

# COMMAND ----------

# DBTITLE 1,Add Foreign Key Constraints (idempotent)
# NOTE: Silver FK constraints (sensor→dma, properties→dma, complaints→dma/properties,
# asset_feed→assets/dma, reservoir_feed→reservoirs/dma) are defined at creation time
# via the `schema` parameter in 06_sdp_pipeline.py.
# Only Gold table FKs need ALTER TABLE.
fk_statements = [
    # dim_incidents → dim_dma
    "ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT fk_incident_dma FOREIGN KEY (dma_code) REFERENCES water_digital_twin.silver.dim_dma(dma_code) NOT ENFORCED",
    # dim_incidents → dim_assets (root cause asset)
    "ALTER TABLE water_digital_twin.gold.dim_incidents ADD CONSTRAINT fk_incident_asset FOREIGN KEY (root_cause_asset_id) REFERENCES water_digital_twin.silver.dim_assets(asset_id) NOT ENFORCED",
    # incident_notifications → dim_incidents
    "ALTER TABLE water_digital_twin.gold.incident_notifications ADD CONSTRAINT fk_notif_incident FOREIGN KEY (incident_id) REFERENCES water_digital_twin.gold.dim_incidents(incident_id) NOT ENFORCED",
    # regulatory_notifications → dim_incidents
    "ALTER TABLE water_digital_twin.gold.regulatory_notifications ADD CONSTRAINT fk_reg_incident FOREIGN KEY (incident_id) REFERENCES water_digital_twin.gold.dim_incidents(incident_id) NOT ENFORCED",
]

for sql in fk_statements:
    try:
        spark.sql(sql)
        print(f"OK:   {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]}")
    except Exception as e:
        if "already exists" in str(e).lower() or "CONSTRAINT_ALREADY_EXISTS" in str(e):
            print(f"SKIP: {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} (already exists)")
        else:
            print(f"ERR:  {sql.split('ADD CONSTRAINT ')[1].split(' ')[0]} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table Comments

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SILVER LAYER
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.fact_telemetry IS
# MAGIC   'Sensor telemetry readings at 15-minute intervals. Pressure sensors report in the `value` column (metres head); flow sensors report in the `flow_rate` column (litres per second). Join to dim_sensor on sensor_id for sensor metadata and DMA assignment.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_dma IS
# MAGIC   'District Metered Area (DMA) reference data. Each DMA is a discrete, boundary-metered zone of the water distribution network with a unique code (e.g. DEMO_DMA_01). Contains centroid coordinates, H3 index, average elevation, and configurable pressure RED/AMBER thresholds.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_sensor IS
# MAGIC   'Sensor registry for all pressure and flow sensors deployed across the network. Contains sensor type, DMA assignment, geographic coordinates, installation date, calibration history, and operational status (active or maintenance).';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_properties IS
# MAGIC   'Property register for all residential and non-residential premises in the network. Includes property type classification (domestic, school, hospital, care_home, dialysis_home, commercial, nursery, key_account), sensitive premise flags, elevation, and expected base pressure.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_assets IS
# MAGIC   'Infrastructure asset register covering pump stations, trunk mains, isolation valves, and pressure reducing valves (PRVs). Contains asset status, geographic location, and maintenance history. Use dim_asset_dma_feed to find which DMAs an asset serves.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_reservoirs IS
# MAGIC   'Service reservoir metadata including total capacity, current fill level, hourly demand rate, and estimated hours of supply remaining. Use dim_reservoir_dma_feed to find which DMAs a reservoir feeds.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_asset_dma_feed IS
# MAGIC   'Bridge table mapping infrastructure assets (pumps, mains, valves) to the DMAs they serve. Feed type indicates whether the asset is a primary or secondary supply source for the DMA. Use this table to find which pump stations feed a given DMA or which DMAs are affected when an asset trips.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.dim_reservoir_dma_feed IS
# MAGIC   'Bridge table mapping service reservoirs to the DMAs they supply. Feed type indicates primary (direct) or secondary (supplementary) supply relationship. Use this table to find reservoir levels for a given DMA.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.silver.customer_complaints IS
# MAGIC   'Customer complaints received via contact centre. Each row is a single complaint with type (no_water, low_pressure, discoloured_water, other), DMA assignment, contact channel, and resolution status. Use complaint_timestamp for time-based analysis.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GOLD LAYER — Operational Tables
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.dma_status IS
# MAGIC   'Current RAG status snapshot for every DMA in the network. One row per DMA with latest pressure readings, sensor/property/sensitive-premises counts, and active incident flag. RED = significant pressure drop or supply interruption; AMBER = pressure trending below normal; GREEN = operating normally.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.dma_rag_history IS
# MAGIC   'Historical RAG status timeline for each DMA at 15-minute intervals. Records average and minimum pressure along with the computed RAG status at each interval. Use this table for pressure trend analysis and to reconstruct the timeline of an incident.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.anomaly_scores IS
# MAGIC   'Per-sensor statistical anomaly scores computed against a rolling 7-day same-time-of-day baseline. anomaly_sigma represents standard deviations from the baseline — readings above 2.5 sigma are flagged as anomalies. Join to dim_sensor on sensor_id for DMA and sensor type context.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.dim_incidents IS
# MAGIC   'Active and historical incident records. Each incident has a root cause asset, affected DMA, severity, property impact count, and timeline (start_timestamp / end_timestamp). Use total_properties_affected for the count of impacted properties. Use status = active to filter to ongoing incidents.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.incident_notifications IS
# MAGIC   'Per-incident summary of customer notification metrics. proactive_notifications = customers contacted before they called in; reactive_complaints = customer-initiated contacts. Used for C-MeX (Customer Measure of Experience) proactive notification rate calculations.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.regulatory_notifications IS
# MAGIC   'Regulatory notification tracking for DWI (Drinking Water Inspectorate) and Ofwat. Records the timestamp each regulator was formally notified and the notification reference. Use dwi_notified_ts to measure time-to-notification from incident start.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.vw_dma_pressure IS
# MAGIC   'Pre-aggregated pressure metrics per DMA per 15-minute interval. Contains average, maximum, and minimum pressure readings plus total head pressure and reading count. Joined with DMA name from dim_dma for display purposes.';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.vw_reservoir_status IS
# MAGIC   'Service reservoir status joined with DMA feed topology. Shows current fill level, total capacity, hourly demand rate, and estimated hours remaining for each reservoir-DMA feed relationship.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GOLD LAYER — Metric Views
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_dma_pressure IS
# MAGIC   'Metric view: average, min, and max pressure by DMA over time. Source: vw_dma_pressure. Use for pressure trend queries in the operator Genie Space. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_flow_anomaly IS
# MAGIC   'Metric view: flow rate deviation by DMA entry point. Source: fact_telemetry (flow sensors only). Detects supply interruptions (low flow) and bursts (high flow). Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_reservoir_status IS
# MAGIC   'Metric view: reservoir levels, capacity, and estimated hours of supply remaining by DMA. Source: vw_reservoir_status. Use for reservoir depletion queries. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_regulatory_compliance IS
# MAGIC   'Metric view: RAG status distribution, property counts, and sensitive premises across all DMAs. Source: dma_status. Used by both operator and executive Genie Spaces. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_incident_summary IS
# MAGIC   'Metric view: incident duration, Ofwat threshold breaches (3h and 12h), and properties affected. Source: dim_incidents. Used by both operator and executive Genie Spaces. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_penalty_exposure IS
# MAGIC   'Metric view: Ofwat OPA penalty exposure in GBP. Formula: properties x MAX(0, hours - 3) x 580. Source: dim_incidents. Used by the executive Genie Space. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_anomaly_scores IS
# MAGIC   'Metric view: sensor-level anomaly scores and detection rates. Source: anomaly_scores. Join with dim_sensor on sensor_id for DMA-level aggregation. Measures must be wrapped in MEASURE().';
# MAGIC
# MAGIC COMMENT ON TABLE water_digital_twin.gold.mv_sensor_status IS
# MAGIC   'Metric view: sensor availability and operational uptime by DMA and sensor type. Source: dim_sensor. Use for sensor fleet health queries. Measures must be wrapped in MEASURE().';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Column Comments — Silver Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.fact_telemetry
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN sensor_id COMMENT 'Unique sensor identifier (e.g. DEMO_SENSOR_01, PS_000019). Join to dim_sensor for sensor metadata.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN dma_code COMMENT 'District Metered Area code the sensor belongs to. Join to dim_dma for DMA metadata.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN timestamp COMMENT 'Timestamp of the 15-minute telemetry reading window (UTC).';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN sensor_type COMMENT 'Type of sensor: pressure or flow. Determines which value column to use.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN value COMMENT 'Pressure reading in metres head (m). Only populated for pressure sensors (sensor_type = pressure). NULL for flow sensors. Normal range: 15-60 m; below 10 m indicates likely supply loss.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN total_head_pressure COMMENT 'Total head pressure in metres head (m), including the elevation component. Only populated for pressure sensors. Use this column when accounting for elevation differences across the DMA.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN flow_rate COMMENT 'Flow rate in litres per second (l/s). Only populated for flow sensors (sensor_type = flow). NULL for pressure sensors. Normal DMA entry flow: 30-60 l/s.';
# MAGIC ALTER TABLE water_digital_twin.silver.fact_telemetry ALTER COLUMN quality_flag COMMENT 'Data quality indicator for the reading. Use to filter out suspect or invalid readings before analysis.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_dma
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN dma_code COMMENT 'Unique District Metered Area identifier (e.g. DEMO_DMA_01, DMA_00010). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN dma_name COMMENT 'Human-readable DMA name (e.g. Crystal Palace South, Sydenham Hill).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN dma_area_code COMMENT 'Parent area grouping code for the DMA.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN geometry_wkt COMMENT 'DMA boundary polygon in WKT format (WGS84 / SRID 4326). Use ST_GeomFromWKT() for spatial operations.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN centroid_latitude COMMENT 'Latitude of the DMA centroid (WGS84). For display only — use geometry for spatial queries.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN centroid_longitude COMMENT 'Longitude of the DMA centroid (WGS84). For display only — use geometry for spatial queries.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN avg_elevation COMMENT 'Average ground elevation of the DMA in metres above ordnance datum (m AOD). Higher elevation DMAs require more pressure to maintain supply.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN h3_index COMMENT 'Uber H3 spatial index at resolution 7 for the DMA centroid. Use for geospatial joins and proximity queries.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN pressure_red_threshold COMMENT 'Minimum pressure threshold (metres head) below which the DMA is classified RED. Default: 15.0 m.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_dma ALTER COLUMN pressure_amber_threshold COMMENT 'Minimum pressure threshold (metres head) below which the DMA is classified AMBER. Default: 25.0 m.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_sensor
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN sensor_id COMMENT 'Unique sensor identifier. Primary key. Naming convention: DEMO_SENSOR_01 for demo sensors, PS_NNNNNN for pressure, FS_NNNNNN for flow.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN sensor_type COMMENT 'Sensor measurement type: pressure (measures metres head) or flow (measures litres per second).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN dma_code COMMENT 'DMA code where the sensor is physically located. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN latitude COMMENT 'Sensor latitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN longitude COMMENT 'Sensor longitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN elevation_m COMMENT 'Sensor elevation in metres above ordnance datum (m AOD). Affects pressure-to-head conversion.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN install_date COMMENT 'Date the sensor was installed (YYYY-MM-DD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN manufacturer COMMENT 'Sensor hardware manufacturer (e.g. Siemens, ABB, Endress+Hauser).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN model COMMENT 'Sensor model identifier (e.g. SITRANS P320, AquaMaster4).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN status COMMENT 'Current operational status: active (reporting telemetry) or maintenance (offline for calibration/repair).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_sensor ALTER COLUMN last_calibration COMMENT 'Date of most recent sensor calibration (YYYY-MM-DD).';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_properties
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN property_id COMMENT 'Unique property identifier (format: PROP_NNNNNN). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN address COMMENT 'Street address of the property.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN postcode COMMENT 'UK postcode of the property.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN property_type COMMENT 'Normalised property classification: domestic, commercial, school, hospital, care_home, dialysis_home, nursery, or key_account. Use for filtering sensitive premises.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN dma_code COMMENT 'DMA code the property is located in. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN customer_height_m COMMENT 'Height of the customer tap point above ordnance datum in metres. Used to calculate effective pressure at the property — higher properties lose more pressure.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN elevation_m COMMENT 'Ground elevation of the property in metres above ordnance datum (m AOD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN base_pressure COMMENT 'Expected normal pressure at the property tap point in metres head (m), accounting for DMA supply pressure and property elevation. Used as a baseline for anomaly detection.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN latitude COMMENT 'Property latitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN longitude COMMENT 'Property longitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN occupants COMMENT 'Number of occupants at the property. Residential: 1-6 typical; non-residential: 10-500+.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN is_sensitive_premise COMMENT 'Boolean flag: TRUE if the property is a school, hospital, care home, or dialysis-dependent home. These premises require priority response during supply interruptions and are tracked for regulatory reporting.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN sensitive_premise_type COMMENT 'Type of sensitive premise: school, hospital, care_home, or dialysis_home. NULL for non-sensitive properties. Use to break down sensitive premises by category.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN geometry_wkt COMMENT 'Property location as WKT POINT geometry (WGS84 / SRID 4326).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_properties ALTER COLUMN h3_index COMMENT 'Uber H3 spatial index at resolution 8 for the property location. Use for spatial joins and proximity analysis.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_assets
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN asset_id COMMENT 'Unique infrastructure asset identifier (e.g. DEMO_PUMP_01, TM_003, PRV_005). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN asset_type COMMENT 'Asset classification: pump_station, trunk_main, isolation_valve, or prv (pressure reducing valve).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN asset_name COMMENT 'Human-readable asset name (e.g. Crystal Palace Booster Station).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN dma_code COMMENT 'Primary DMA the asset is associated with. For assets serving multiple DMAs, use dim_asset_dma_feed for the full mapping.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN status COMMENT 'Current operational status: operational, tripped, failed, maintenance, or decommissioned. Tripped assets have an associated trip_timestamp.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN latitude COMMENT 'Asset latitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN longitude COMMENT 'Asset longitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN elevation_m COMMENT 'Asset elevation in metres above ordnance datum (m AOD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN geometry_wkt COMMENT 'Asset geometry in WKT format (WGS84). POINT for pump stations/valves/PRVs; LINESTRING for trunk mains.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN diameter_inches COMMENT 'Pipe diameter in inches. Applicable to trunk mains, isolation valves, and PRVs. NULL for pump stations.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN length_km COMMENT 'Pipeline length in kilometres. Applicable to trunk mains only. NULL for other asset types.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN trip_timestamp COMMENT 'Timestamp when the asset tripped (UTC). NULL if the asset has not tripped. Only applicable when status = tripped.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN install_date COMMENT 'Date the asset was installed or commissioned (YYYY-MM-DD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN manufacturer COMMENT 'Asset manufacturer (e.g. Grundfos, Saint-Gobain, AVK).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN model COMMENT 'Asset model or specification (e.g. CRE 64-2-1, Ductile Iron DN300).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN capacity_kw COMMENT 'Pump motor capacity in kilowatts (kW). Applicable to pump stations only. NULL for other asset types.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN last_maintenance COMMENT 'Date of most recent maintenance activity (YYYY-MM-DD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_assets ALTER COLUMN notes COMMENT 'Free-text notes about the asset, including its role in the network.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_reservoirs
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN reservoir_id COMMENT 'Unique service reservoir identifier (e.g. DEMO_SR_01, SR_002). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN reservoir_name COMMENT 'Human-readable reservoir name (e.g. Crystal Palace Service Reservoir).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN latitude COMMENT 'Reservoir latitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN longitude COMMENT 'Reservoir longitude (WGS84).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN elevation_m COMMENT 'Reservoir elevation in metres above ordnance datum (m AOD). Higher reservoirs provide more gravity-fed head pressure.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN capacity_ml COMMENT 'Total reservoir storage capacity in megalitres (ML).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN current_level_pct COMMENT 'Current fill level as a percentage of total capacity (0-100%). DEMO_SR_01 at 43% during the active incident.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN current_volume_ml COMMENT 'Current stored volume in megalitres (ML). Calculated as capacity_ml x current_level_pct / 100.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN hourly_demand_rate_ml COMMENT 'Current hourly draw-down rate in megalitres per hour (ML/hr). Increases during morning demand surge (06:00-09:00).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN hours_remaining COMMENT 'Estimated hours of supply remaining at the current demand rate. Calculated as current_volume_ml / hourly_demand_rate_ml.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN status COMMENT 'Reservoir operational status: active or offline.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN last_inspection COMMENT 'Date of most recent reservoir inspection (YYYY-MM-DD).';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoirs ALTER COLUMN notes COMMENT 'Free-text notes about the reservoir and its service area.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_asset_dma_feed
# MAGIC ALTER TABLE water_digital_twin.silver.dim_asset_dma_feed ALTER COLUMN asset_id COMMENT 'Asset identifier. Foreign key to dim_assets.asset_id.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_asset_dma_feed ALTER COLUMN dma_code COMMENT 'DMA code served by this asset. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_asset_dma_feed ALTER COLUMN feed_type COMMENT 'Feed relationship type: primary (main supply source) or secondary (supplementary/interconnection). When an asset trips, primary-fed DMAs are most affected.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_asset_dma_feed ALTER COLUMN notes COMMENT 'Description of the feed relationship.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.dim_reservoir_dma_feed
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoir_dma_feed ALTER COLUMN reservoir_id COMMENT 'Reservoir identifier. Foreign key to dim_reservoirs.reservoir_id.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoir_dma_feed ALTER COLUMN dma_code COMMENT 'DMA code fed by this reservoir. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.silver.dim_reservoir_dma_feed ALTER COLUMN feed_type COMMENT 'Feed relationship type: primary (direct supply) or secondary (supplementary). Primary-fed DMAs are most dependent on this reservoir.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver.customer_complaints
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN complaint_id COMMENT 'Unique complaint identifier.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN property_id COMMENT 'Property that lodged the complaint. Foreign key to dim_properties.property_id.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN dma_code COMMENT 'DMA code where the complaining property is located. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN complaint_timestamp COMMENT 'Timestamp when the complaint was received (UTC).';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN complaint_type COMMENT 'Normalised complaint category: no_water (complete supply loss), low_pressure (reduced flow), discoloured_water (water quality), or other.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN description COMMENT 'Free-text description of the complaint as provided by the customer.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN contact_channel COMMENT 'Channel through which the complaint was received (e.g. phone, web, email).';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN customer_height_m COMMENT 'Customer tap height in metres above ordnance datum. Copied from the property record for analysis convenience.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN property_type COMMENT 'Property type of the complainant (domestic, school, hospital, etc.). Copied from property record.';
# MAGIC ALTER TABLE water_digital_twin.silver.customer_complaints ALTER COLUMN status COMMENT 'Complaint resolution status (e.g. open, resolved, closed).';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Column Comments — Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.dma_status
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN dma_code COMMENT 'District Metered Area identifier. One row per DMA. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN rag_status COMMENT 'Current RED/AMBER/GREEN health classification. RED: min pressure below 15 m (supply loss risk). AMBER: min pressure below 25 m (monitoring required). GREEN: operating normally.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN avg_pressure COMMENT 'Average pressure across all pressure sensors in the DMA in metres head (m). Computed from the latest 15-minute telemetry window.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN min_pressure COMMENT 'Minimum pressure reading across all pressure sensors in the DMA in metres head (m). Drives the RAG status classification.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN sensor_count COMMENT 'Number of active sensors deployed in this DMA.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN property_count COMMENT 'Total number of properties (residential and non-residential) in this DMA.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN sensitive_premises_count COMMENT 'Count of sensitive premises (hospitals, schools, care homes, dialysis-dependent homes) in this DMA. These require priority response during incidents.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN has_active_incident COMMENT 'Boolean: TRUE if there is a currently active incident affecting this DMA.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_status ALTER COLUMN last_updated COMMENT 'Timestamp when this status row was last computed (UTC).';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.dma_rag_history
# MAGIC ALTER TABLE water_digital_twin.gold.dma_rag_history ALTER COLUMN dma_code COMMENT 'District Metered Area identifier. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_rag_history ALTER COLUMN timestamp COMMENT 'Timestamp of the 15-minute interval (UTC).';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_rag_history ALTER COLUMN rag_status COMMENT 'RAG classification at this interval: RED (min pressure < 15 m), AMBER (min pressure < 25 m), or GREEN (normal).';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_rag_history ALTER COLUMN avg_pressure COMMENT 'Average pressure across all pressure sensors in the DMA at this interval, in metres head (m).';
# MAGIC ALTER TABLE water_digital_twin.gold.dma_rag_history ALTER COLUMN min_pressure COMMENT 'Minimum pressure across all pressure sensors in the DMA at this interval, in metres head (m). This value determines the RAG status.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.anomaly_scores
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN sensor_id COMMENT 'Sensor identifier. Join to dim_sensor for DMA code, sensor type, and location.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN timestamp COMMENT 'Timestamp of the scored telemetry reading (UTC). Use this column — not scored_ts — for time-based queries.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN anomaly_sigma COMMENT 'Anomaly score in standard deviations (sigma) from the rolling 7-day same-time-of-day baseline. NOT a raw score or percentage. Values above 2.5 sigma are flagged as anomalies; above 3.0 sigma warrants investigation.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN baseline_value COMMENT 'Expected baseline value computed from the rolling 7-day same-time-of-day window. Units match the sensor type: metres head for pressure, litres per second for flow.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN actual_value COMMENT 'Actual observed sensor reading. Units: metres head (m) for pressure sensors, litres per second (l/s) for flow sensors.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN is_anomaly COMMENT 'Boolean: TRUE if anomaly_sigma exceeds the 2.5 sigma threshold. Use this column for counting anomalous readings.';
# MAGIC ALTER TABLE water_digital_twin.gold.anomaly_scores ALTER COLUMN scoring_method COMMENT 'Algorithm used to compute the anomaly score. Currently always statistical (rolling 7-day baseline comparison).';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.dim_incidents
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN incident_id COMMENT 'Unique incident identifier (format: INC-YYYY-MMDD-NNN). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN dma_code COMMENT 'Primary affected DMA code. Foreign key to dim_dma.dma_code. For incidents affecting multiple DMAs, this is the most impacted DMA.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN root_cause_asset_id COMMENT 'Asset that caused the incident. Foreign key to dim_assets.asset_id.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN root_cause_asset_type COMMENT 'Type of the root cause asset: pump_station, trunk_main, prv, etc.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN incident_type COMMENT 'Incident classification: pump_station_trip, burst_main, prv_failure, etc.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN title COMMENT 'Short descriptive title of the incident.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN description COMMENT 'Detailed narrative description of the incident, impact, and response.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN start_timestamp COMMENT 'Timestamp when the incident started (UTC). Use this column — not detected_ts — for duration and penalty calculations.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN end_timestamp COMMENT 'Timestamp when the incident was resolved (UTC). NULL for active incidents — use COALESCE(end_timestamp, CURRENT_TIMESTAMP()) for duration calculations.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN status COMMENT 'Incident lifecycle status: active (ongoing) or resolved (closed). Filter on active to see current incidents.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN severity COMMENT 'Incident severity classification: low, medium, high, or critical.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN total_properties_affected COMMENT 'Count of properties impacted by this incident. This is an integer count (not a percentage). Use SUM(total_properties_affected) for aggregate queries across incidents.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN sensitive_premises_affected COMMENT 'Boolean: TRUE if the incident affects any sensitive premises (schools, hospitals, care homes, dialysis-dependent homes).';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN priority_score COMMENT 'Computed priority score (0-100) reflecting severity, property count, sensitive premises, and reservoir risk.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN escalation_risk COMMENT 'Narrative assessment of escalation risk and recommended actions.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN trajectory COMMENT 'Current trajectory of the incident: whether conditions are improving, stable, or deteriorating.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN first_complaint_time COMMENT 'Timestamp of the first customer complaint related to this incident (UTC). NULL if no complaints received.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN assigned_team COMMENT 'Operations team assigned to manage the incident response.';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN sop_reference COMMENT 'Standard Operating Procedure reference for the incident type (e.g. SOP-WN-042).';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN created_at COMMENT 'Timestamp when the incident record was created in the system (UTC).';
# MAGIC ALTER TABLE water_digital_twin.gold.dim_incidents ALTER COLUMN updated_at COMMENT 'Timestamp of the most recent update to the incident record (UTC).';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.incident_notifications
# MAGIC ALTER TABLE water_digital_twin.gold.incident_notifications ALTER COLUMN incident_id COMMENT 'Incident identifier. Primary key and foreign key to dim_incidents.incident_id.';
# MAGIC ALTER TABLE water_digital_twin.gold.incident_notifications ALTER COLUMN proactive_notifications COMMENT 'Number of customers proactively notified by the company BEFORE they contacted the company. Proactive notifications positively impact C-MeX scores.';
# MAGIC ALTER TABLE water_digital_twin.gold.incident_notifications ALTER COLUMN reactive_complaints COMMENT 'Number of customer-initiated contacts (complaints) received about this incident. These are reactive — the customer called before the company notified them.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.regulatory_notifications
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN notification_id COMMENT 'Unique regulatory notification identifier (format: REG-YYYY-MMDD-NNN). Primary key.';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN incident_id COMMENT 'Related incident identifier. Foreign key to dim_incidents.incident_id.';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN regulator COMMENT 'Regulatory body notified: DWI (Drinking Water Inspectorate) or Ofwat.';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN dwi_notified_ts COMMENT 'Timestamp when DWI was formally notified of the incident (UTC). Used to calculate notification lag: TIMESTAMPDIFF(MINUTE, i.start_timestamp, r.dwi_notified_ts). DWI should be notified within 1 hour for incidents affecting sensitive premises.';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN ofwat_notified_ts COMMENT 'Timestamp when Ofwat was formally notified of the incident (UTC). NULL if Ofwat notification was not required or has not yet been sent.';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN notification_ref COMMENT 'External reference number for the regulatory notification (e.g. DWI-NOT-2026-0407-001).';
# MAGIC ALTER TABLE water_digital_twin.gold.regulatory_notifications ALTER COLUMN status COMMENT 'Notification status: acknowledged (regulator confirmed receipt) or pending.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.vw_dma_pressure
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN dma_code COMMENT 'District Metered Area identifier. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN timestamp COMMENT 'Timestamp of the 15-minute telemetry aggregation window (UTC).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN avg_pressure COMMENT 'Average pressure across all pressure sensors in the DMA at this interval, in metres head (m).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN max_pressure COMMENT 'Maximum pressure reading in the DMA at this interval, in metres head (m).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN min_pressure COMMENT 'Minimum pressure reading in the DMA at this interval, in metres head (m). Low values indicate supply issues.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN avg_total_head_pressure COMMENT 'Average total head pressure in metres head (m), including elevation component.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN reading_count COMMENT 'Number of individual sensor readings aggregated in this interval.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_dma_pressure ALTER COLUMN dma_name COMMENT 'Human-readable DMA name, joined from dim_dma.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold.vw_reservoir_status
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN reservoir_id COMMENT 'Unique service reservoir identifier. Foreign key to dim_reservoirs.reservoir_id.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN reservoir_name COMMENT 'Human-readable reservoir name.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN dma_code COMMENT 'DMA code of the area fed by this reservoir. Foreign key to dim_dma.dma_code.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN fed_dma_name COMMENT 'Human-readable name of the DMA fed by this reservoir.';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN feed_type COMMENT 'Feed relationship: primary (direct supply source) or secondary (supplementary).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN current_level_pct COMMENT 'Current reservoir fill level as a percentage of total capacity (0-100%).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN capacity_ml COMMENT 'Total reservoir storage capacity in megalitres (ML).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN hourly_demand_rate_ml COMMENT 'Current hourly draw-down rate in megalitres per hour (ML/hr).';
# MAGIC ALTER TABLE water_digital_twin.gold.vw_reservoir_status ALTER COLUMN hours_remaining COMMENT 'Estimated hours of supply remaining at the current demand rate.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Column Comments — Gold Metric Views
# MAGIC
# MAGIC Metric views (`mv_*`) have their column semantics defined in the YAML measure/dimension
# MAGIC definitions (see `08_metric_views.sql`). Table-level comments were applied in Section 3
# MAGIC above. No additional column-level comments are needed for metric views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation
# MAGIC
# MAGIC Verify that comments and constraints were applied correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify table comments on a sample of tables
# MAGIC SELECT table_catalog, table_schema, table_name, comment
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'water_digital_twin'
# MAGIC   AND table_schema IN ('silver', 'gold')
# MAGIC   AND comment IS NOT NULL
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify column comments on fact_telemetry (critical disambiguations)
# MAGIC SELECT column_name, comment
# MAGIC FROM system.information_schema.columns
# MAGIC WHERE table_catalog = 'water_digital_twin'
# MAGIC   AND table_schema = 'silver'
# MAGIC   AND table_name = 'fact_telemetry'
# MAGIC   AND comment IS NOT NULL
# MAGIC ORDER BY ordinal_position;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify PK/FK constraints
# MAGIC SELECT constraint_catalog, constraint_schema, constraint_name, constraint_type
# MAGIC FROM system.information_schema.table_constraints
# MAGIC WHERE constraint_catalog = 'water_digital_twin'
# MAGIC   AND constraint_schema IN ('silver', 'gold')
# MAGIC ORDER BY constraint_type, constraint_name;
