# Architecture

## Data flow

```
Public APIs          Synthetic Generation     Public Documents
(Open-Meteo,         (ETL, calibrated to      (ZGL QM, Payments
 Frankfurter,         documented ranges)        Booklet 2026)
 Overpass, Stats NZ)
       │                      │                       │
       └──────────────┬───────┴───────────────────────┘
                      ↓
              ETL Pipeline (Python)
           03_etl_pipeline/api_feed.py
                      ↓
         SQLite Database (Star Schema)
    02_data_processed/kiwifruit_export.db
                      ↓
           ┌──────────┴──────────┐
           ↓                     ↓
     Analysis Layer         Risk Model (APO v4)
       04_analysis/            05_models/
           ↓                     ↓
           └──────────┬──────────┘
                      ↓
             JSON Payload Export
    07_reports/api_payloads/payload_live.json
                      ↓
          Frontend Application
  06_simulator/apophenia_v4_executive.html
                      ↓
        ┌─────────────┼─────────────┐
        ↓             ↓             ↓
    Chart.js      Mapbox GL JS   Scenario
   (analytics)    (geospatial)   simulator
```

## Components

### Backend

- **ETL pipeline** — ingests public APIs, generates synthetic operational variables, writes to SQLite
- **Risk model** — multi-variable regression scoring, confidence-interval forecast generation
- **SQL analysis layer** — six documented research queries answering business questions
- **Payload exporter** — serialises latest run to JSON for the frontend

### Frontend

- **Hero band** — executive recommendation derived from current scenario
- **Evidence triad** — Where (corridor), When (peak risk week), Cost (margin exposure)
- **Detailed analysis** — Strategic Monitor, Regional Intelligence (map), Forecast Lab, Executive Vault
- **Scenario controls drawer** — 10 operational sliders for live recalculation
- **Process band** — methodology overview ("How APOPHENIA works")
- **Footer** — legal, methodology, data sources, contact

## Public APIs in use

| API | Purpose | Refresh |
|-----|---------|---------|
| Open-Meteo | BOP weather forecasts | Hourly |
| Frankfurter | FX rates (NZD/EUR, NZD/JPY) | Daily |
| Overpass / OSM | SH2 corridor geometry | One-time |
| Mapbox | Basemap + live traffic | ~5 min cadence (managed by provider) |
