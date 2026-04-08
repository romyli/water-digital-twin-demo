# Data Sources — Water Digital Twin Demo

External datasets used by the data generation notebooks.

## DMA Polygons (Ward Boundaries)

| Field | Value |
|-------|-------|
| Source | ONS Open Geography Portal — Electoral Wards (Dec 2025), Generalised Clipped Boundaries |
| Service | ArcGIS FeatureServer: `WD_DEC_2025_UK_BGC` |
| Filter | `LAD25CD LIKE 'E09%'` (London boroughs only) → 704 wards, trimmed to 500 most central |
| Format | GeoJSON (WGS84) |
| Fields used | `WD25CD`, `WD25NM`, `LAD25NM`, `LONG`, `LAT`, `Shape__Area`, geometry |
| Licence | Open Government Licence v3.0 |
| Used in | `notebooks/02_data_gen_geography.py` |

## Elevation

| Field | Value |
|-------|-------|
| Source | Open-Meteo Elevation API (SRTM ~30m resolution) |
| URL | `https://api.open-meteo.com/v1/elevation` |
| Batch size | Up to 100 coordinates per request |
| Auth | None required |
| Licence | Free for commercial and non-commercial use |
| Used in | `notebooks/02_data_gen_geography.py`, `notebooks/03_data_gen_dimensions.py` |
