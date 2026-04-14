# Water Digital Twin Demo

## Overview
Industry demo built on Databricks showcasing a water utility digital twin use case. Based on a real customer engagement — all content must be fully anonymized.

## Anonymization Rules (CRITICAL)
- **No real customer names** — use fictional names (e.g., "Water Utilities") if a company name is needed
- **No real person names** — use fake names or roles only
- **No identifying details** — obscure locations, contract values, or any detail that could identify the customer
- When the user shares raw notes, treat them as confidential source material. Extract the technical patterns and requirements, but never commit identifying information.

## Project Structure
- `notes/` — local-only directory for raw customer notes (gitignored, never committed)
- `prompts/` — plan and build prompts for the multi-agent demo build

## Databricks Workspace
- **Workspace:** `https://adb-984752964297111.11.azuredatabricks.net/` (CLI profile: `adb-98`)
- **Asset Root:** `/Workspace/Users/romy.li@databricks.com/Water Digital Twin Demo`
- **Catalog:** `water_digital_twin`

## Tech Stack
- Platform: Databricks
- Language: Python

## Databricks Demo Build Guidelines

These rules apply to any Databricks demo build. They are generic and reusable.

### Serverless-first compute
All notebooks and pipelines must use **serverless** compute unless there is a specific reason not to.
- Create DataFrames from local data via Pandas: `spark.createDataFrame(pd.DataFrame(records))` — never `spark.createDataFrame(list_of_tuples, schema=StructType(...))` (fails on Spark Connect serialization).
- Omit `.format("delta")` on writes — Delta is the default.
- Never use `.cache()` or `.persist()` — not supported on serverless (Spark Connect).
- **SDP pipelines:** Set `"serverless": true` in the pipeline JSON config. Do **not** include a `"clusters"` array — serverless pipelines manage their own compute.

### SDP API
Use `from pyspark import pipelines as dp` — **not** the legacy `import dlt` module.

### Metric Views
Use `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$` — **not** `CREATE METRIC VIEW`. Both dimensions and measures require an `expr` field.
- **Querying metric views:** Measure columns must be wrapped in `MEASURE()` and dimensions must appear in `GROUP BY`. Example: `SELECT dim_col, MEASURE(measure_col) AS m FROM mv GROUP BY dim_col`. Selecting a measure column without `MEASURE()` raises `METRIC_VIEW_MISSING_MEASURE_FUNCTION`.

### Notebook types
- **All text content** (guides, walkthroughs, runbooks) must be Python notebooks with `# MAGIC %md` markdown cells — never plain `.md` files. This ensures they render in Databricks and can be attached to clusters.
- **SDP/DLT pipelines** are notebook source files but must be deployed as a **Pipeline resource** (not run as a notebook task). The orchestration workflow references them as Pipeline tasks, not Notebook tasks.

### Medallion architecture — file-based ingestion
Data gen notebooks write JSON files to a **Unity Catalog Volume** (landing zone). The SDP pipeline handles the entire medallion flow:

| Layer | Owned by | Method |
|-------|----------|--------|
| Catalog + schemas + volume | Schema DDL notebook | `CREATE CATALOG/SCHEMA/VOLUME IF NOT EXISTS` |
| Landing zone files | Data gen notebooks | `df.write.format("json").mode("overwrite").save("/Volumes/.../landing_zone/<dataset>/")` |
| Bronze streaming tables | SDP pipeline | `@dp.table` + Auto Loader (`cloudFiles`) from Volume paths |
| Silver materialized views | SDP pipeline | `@dp.materialized_view` reading from bronze tables within the pipeline |
| Gold materialized views | SDP pipeline | `@dp.materialized_view` reading from silver tables within the pipeline |
| Gold computed tables (incidents etc.) | The notebook that populates them | `df.write.saveAsTable()` creates implicitly |
| Metric views (`mv_*`) | A dedicated views notebook | `CREATE OR REPLACE METRIC VIEW` |

**Key rules:**
- Data gen notebooks **never** write to tables — only to Volume files. This ensures the SDP pipeline DAG shows the full bronze → silver → gold flow.
- The SDP pipeline is the **single owner** of all bronze, silver, and gold analytical tables.
- Cross-notebook dependencies read from Volume files, not from tables (e.g., NB03 reads DMA JSON from Volume, not `spark.table()`).
- DDL notebook creates **only** catalog, schemas, and the landing zone volume — no table definitions.

### Data realism
- **Never generate synthetic geometry** (hex grids, random polygons). Always search for real public boundary datasets and use those. Prefer government open data portals with permissive licences (e.g., OGL, CC-BY).
- **Never use simplified mathematical models** for physical data (elevation, terrain, climate). Use real observational data from public APIs (e.g., SRTM elevation, weather APIs). If an API is available, use it instead of approximating.
- **Use real place names** from source datasets where possible. Anonymization rules apply to customer-identifying details, not to publicly available geographic names.
- Document all external data sources (URL, licence, access method) in a dedicated section of the build spec or a `DATA_SOURCES.md` file — not in CLAUDE.md.

### Databricks Apps
- Always use the **AppKit skill** (`/databricks-apps`) before building any Databricks app. It contains up-to-date scaffolding, configuration, and deployment guidelines.
- **Scaffold first:** Run `databricks apps init` to generate the canonical AppKit project structure before writing custom code. Match its layout exactly (`server/server.ts`, `client/src/`, `databricks.yml`, split tsconfigs). Do not invent your own directory structure.
- **Verify table schemas before writing SQL.** Never assume column names — read the data generation notebooks or run `SELECT * FROM <table> LIMIT 1` to confirm. Column name mismatches are the #1 cause of 500 errors at runtime.
- **UNION ALL columns must have matching types.** If one source has `TEXT` and another has `TIMESTAMP`, explicitly cast both sides (e.g., `col::text`) to avoid `UNION types ... cannot be matched` errors.
- **App write-back tables must be separate from sync-managed tables.** Lakebase sync notebooks typically `DROP TABLE + CREATE TABLE`, which wipes all data. Tables that receive writes from the app must use a distinct name (e.g., `app_*` prefix) and be created with `CREATE TABLE IF NOT EXISTS` so re-syncs don't destroy app data.
- **Grant the app service principal access to Lakebase tables.** The app runs as an auto-created SP (`service_principal_client_id` from `databricks apps get`). After syncing tables, grant it access: `databricks_create_role(sp_id, 'SERVICE_PRINCIPAL')` + `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public`.
- **React 19 + TypeScript: do not `import React from "react"`.** The JSX transform handles it automatically. Bare React imports trigger `TS6133: declared but never read`. Use named imports only when needed (e.g., `import { useState } from "react"`).
- **Tailwind CSS v4 syntax:** Use `@import "tailwindcss"` + `@theme { }` for custom colours — not the v3 `@tailwind base/components/utilities` directives. Add `@source "../../src/**/*.tsx"` if the Vite plugin doesn't auto-detect component files.
- **Use MapLibre GL (free) instead of Mapbox GL** for map visualizations. MapLibre is an API-compatible open-source fork that requires no access token. Use the CARTO Positron style (`https://basemaps.cartocdn.com/gl/positron-gl-style/style.json`) for a clean base map.
- **Use `react-map-gl/maplibre` (declarative) instead of raw `maplibregl` (imperative).** Imperative `new maplibregl.Map()` in `useEffect` fails silently in Databricks Apps (CSS loading, CSP, lifecycle issues). The `react-map-gl` wrapper handles initialization, CSS, and cleanup reliably. Import `{ Map, Source, Layer, Popup, NavigationControl } from "react-map-gl/maplibre"` and `import "maplibre-gl/dist/maplibre-gl.css"`. Add `react-map-gl` to dependencies alongside `maplibre-gl`.
- **Make optional plugins conditional.** If a plugin (e.g., Genie) requires a resource that may not be configured yet, guard it: `if (process.env.VAR) plugins.push(plugin())`. Otherwise the app crashes on startup with `ConfigurationError: Missing required resources`.
- **Track all hardcoded values and data gaps.** While building the app, maintain a `DATA_IMPROVEMENT_BRIEF.md` listing every hardcoded business rule (penalty rates, thresholds, deadlines), every fallback default that masks missing data, and every dataset that would enrich the demo. Categorise as P0-P3 so a data engineer can prioritise. This document is the handoff artifact — the app developer knows what's faked, the data engineer knows what to fix.

### Deployment
- All files must be uploaded to the workspace at the Asset Root after local changes.
- Use `-p <profile>` (short flag) for the Databricks CLI, not `--profile <profile>`.
- Notebooks: use `workspace import <path_without_extension> --format SOURCE --language PYTHON --file <local.py>` (or `SQL`). Always use `--file`, never stdin redirect. Non-notebook files (app code, config) upload with `--format RAW --file`.

## Self-Correction Protocol
When the user corrects a mistake in any demo asset (notebook, guide, app code, config), immediately:
1. **Fix the asset** as requested.
2. **Add a generic guideline** to the Databricks Demo Build Guidelines section above so the mistake is never repeated — keep rules demo-agnostic so they apply to future builds.
3. **Update MEMORY** if the correction applies beyond this project.

This ensures the user never needs to give the same correction twice.
