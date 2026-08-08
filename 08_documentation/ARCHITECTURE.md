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
        06_simulator/index.html
                      ↓
        ┌─────────────┼─────────────┐
        ↓             ↓             ↓
    Chart.js      Mapbox GL JS   Scenario
   (analytics)    (geospatial)   simulator
```

## Two databases

`02_data_processed/star_schema/` holds two SQLite files. They are not a
migration in progress — each is authoritative for a different part of
the project, and both stay:

- **`kiwifruit_export.db` — the operational database.** Built by
  `03_transform.py` / `04_load.py` from the shared ETL pipeline shown
  above. It's the only database that carries the risk-model context
  variables (`congestion_index`, `rainfall_mm_7d`, `reg_index`,
  `vol_index`, `risk_score`), so it's what `api_feed.py` reads to feed
  the **live deployed simulator**, what `05_sql_analysis.py`'s v1
  queries (`04_analysis/legacy_queries/`) run against, and what
  `05_models/06_risk_model_validation.py` backtests against. Nothing
  in this path is optional to keep.
- **`apophenia_star.db` — a separate, redesigned star schema.** Built
  by its own loader, `04_analysis/star_schema/load_star_schema.py`,
  reading directly from the raw synthetic CSVs rather than from the
  pipeline above. It exists to demonstrate normalised star-schema
  design and window-function SQL (`04_analysis/sql_queries/`, q7–q11)
  and powers the Power BI dashboard. It does not carry the risk-model
  fields the simulator needs, so it cannot replace
  `kiwifruit_export.db` without redesigning it to add them — which
  would undo the point of it being a simplified, derived schema.

See `04_analysis/legacy_queries/README.md` and
`04_analysis/sql_queries/README.md` for how the two query sets divide
along this same line.

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
