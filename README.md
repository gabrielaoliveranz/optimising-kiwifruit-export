# APOPHENIA™

**Operational Risk Intelligence Simulator for NZ Kiwifruit Export**

> Independent portfolio project demonstrating end-to-end data engineering, predictive modelling, geospatial visualisation, and executive dashboard design.

[![Status](https://img.shields.io/badge/status-demonstration_prototype-006338)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue)]()
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

---

## Overview

APOPHENIA is an interactive **operational risk simulator** for the New Zealand kiwifruit export supply chain. Users adjust ten operational variables (climate, logistics, fruit quality, regulatory load) and the system recalculates risk scores, cost-of-delay, and forecast projections in real time, returning an executive recommendation grounded in the underlying data.

The product targets executive audiences — designed for boardroom projection, daily desktop work, and tablet review.

**Live demonstration:** https://apophenia-nz.vercel.app/

---

## What it demonstrates

- **Data engineering** — Python ETL pipeline processing synthetic operational datasets calibrated against publicly available industry standards
- **Predictive modelling** — multi-variable risk regression with 90% confidence intervals, 26-week forecast arc, and 90-day scenario projection
- **Geospatial visualisation** — Mapbox-powered Bay of Plenty corridor map with live traffic overlay
- **Executive dashboard design** — editorial visual system with hero narrative, evidence triad, KPI strip, and exportable PDF briefings
- **Data ethics in practice** — clear separation between synthetic and public data sources, transparent methodology disclaimers

---

## Architecture

```
01_data_raw/           ← Public industry PDFs (ZGL Quality Manual, Grower Payments Booklet)
02_data_processed/     ← SQLite database (kiwifruit_export.db)
03_etl_pipeline/       ← Python ETL scripts (synthetic data generation, API ingestion)
04_analysis/           ← SQL queries, Jupyter notebooks, Power BI visualisations
05_models/             ← APO v4 risk model (multi-variable regression)
06_simulator/          ← Web frontend (HTML/CSS/JS, Chart.js, Mapbox GL JS)
07_reports/            ← API payloads, presentation slides
08_documentation/      ← Architecture, methodology, data dictionary
00_project_management/ ← Sprint logs, design decisions
```

---

## Data sources

| Source | Type | Purpose |
|--------|------|---------|
| Open-Meteo | Public API (live) | Bay of Plenty rainfall and weather forecasts |
| Frankfurter | Public API (live) | NZD/EUR and NZD/JPY exchange rates |
| Overpass / OpenStreetMap | Public API (live) | SH2 corridor geometry verification |
| Mapbox | Public API (live) | Vector basemap and live traffic overlay |
| Stats NZ | Public dataset | Horticulture Survey volume aggregates |
| ZGL Quality Manual 2026 | Public PDF | Calibration of synthetic dry-matter and MTS thresholds |
| Grower Payments Booklet 2026 | Public PDF | Payment rate calibration |
| **Operational inventory** | **Synthetic** | Stochastically generated within documented industry ranges |

No proprietary data has been accessed. All operational variables are synthetic, calibrated against the public sources above.

---

## Technology stack

**Backend & Data**
- Python 3.10+, pandas, SQLite
- ETL pipelines, synthetic data generation
- Multi-variable risk regression with logistic transform

**Frontend**
- HTML5, CSS3, vanilla JavaScript
- Chart.js for time-series and analytic charts
- Mapbox GL JS for geospatial visualisation
- Fraunces (serif) + Inter (sans) typography

**Workflow**
- AI-augmented development (Claude Code)
- Git/GitHub for version control
- Vercel deployment

---

## Local setup

### Prerequisites

- Python 3.10 or higher
- A modern browser (Chrome, Edge, Firefox, Safari)
- A free Mapbox account ([mapbox.com](https://mapbox.com)) for the access token

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/gabrielaoliveranz/optimising-kiwifruit-export.git
   cd optimising-kiwifruit-export
   ```

2. Configure your Mapbox token:
   ```bash
   cd 06_simulator
   cp config.local.example.js config.local.js
   ```
   Edit `config.local.js` and replace `YOUR_MAPBOX_TOKEN_HERE` with your real public token.

3. *(Optional)* Run the ETL pipeline to regenerate the database:
   ```bash
   cd ..
   python 03_etl_pipeline/api_feed.py
   ```

4. Serve the simulator:
   ```bash
   cd 06_simulator
   python -m http.server 8000
   ```

5. Open in your browser: `http://localhost:8000/apophenia_v4_executive.html`

---

## Project structure

```
optimising-kiwifruit-export/
├── README.md                           # This file
├── .gitignore
│
├── 00_project_management/
│   ├── README.md
│   └── sprint_logs/                    # Chronological development logs
│
├── 01_data_raw/                        # Public industry references
├── 02_data_processed/
│   └── kiwifruit_export.db             # SQLite star schema database
│
├── 03_etl_pipeline/
│   └── api_feed.py                     # Live API ingestion + synthetic data
│
├── 04_analysis/
│   ├── 05_sql_analysis.py              # SQL query orchestrator
│   ├── notebooks/                      # Jupyter EDA
│   ├── sql_queries/                    # 6 documented research queries
│   └── visualisations/
│       ├── powerbi/                    # Power BI dashboard
│       └── python/                     # Custom matplotlib charts
│
├── 05_models/                          # APO v4 risk model
│
├── 06_simulator/                       # Frontend product
│   ├── apophenia_v4_executive.html
│   ├── config.local.example.js         # Token template (commit-safe)
│   ├── config.local.js                 # Local token (.gitignored)
│   ├── assets/                         # Images
│   └── README.md
│
├── 07_reports/
│   ├── api_payloads/                   # Generated payload_live.json
│   └── presentation_slides/
│
└── 08_documentation/
    ├── ARCHITECTURE.md
    ├── METHODOLOGY.md
    └── DATA_DICTIONARY.md
```

---

## Methodology highlights

- **Synthetic data**: stochastically generated within ranges documented in ZGL Quality Manual 2026 and Grower Payments Booklet 2026.
- **Risk model (APO v4)**: multi-variable regression weighting dry-matter (35%), pest pressure (25%), congestion (15%), rainfall (15%), and regulatory load (10%).
- **Validation**: 3-season backtest against the synthetic dataset, R² = 0.82, OTIF projection accuracy ±8% within a 14-day horizon.

See [`08_documentation/METHODOLOGY.md`](08_documentation/METHODOLOGY.md) for the full methodology.

---

## Author

**Gabriela Olivera** — Data Analyst · Operational Analytics · Tauranga, NZ

20+ years of professional experience, with the last 14+ across operations, procurement, and data analytics — between Argentina and New Zealand. Specialised in turning operational complexity into data-driven decisions.

[LinkedIn](https://www.linkedin.com/in/gabriela-olivera-nz) · [GitHub](https://github.com/gabrielaoliveranz) · [Kaggle](https://kaggle.com/gabrielaoliveranz)

---

## Commercial availability

This is a demonstration prototype. Commercial deployment, customisation, and integration with proprietary data sources are available on request.

For enquiries: please use the **Commercial inquiries** link in the live application or message via LinkedIn.

---

## Disclaimer

APOPHENIA is an independent project. Not affiliated with, endorsed by, or commissioned by Zespri Group Limited. All references to ZGL documents are public-document citations.

Recommendations generated by the simulator are illustrative and should not be used as the sole basis for binding operational decisions.

---

## Licence

MIT — see [LICENSE](LICENSE).

## Commercial use

This software is licensed under MIT — you are free to use, modify, and 
distribute it. However, **the APOPHENIA name, branding, and methodology 
documentation are proprietary**. If you want to deploy a commercial 
service based on this work, please get in touch first.
