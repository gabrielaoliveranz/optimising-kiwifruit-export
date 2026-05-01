# Methodology

## Data ethics — synthetic vs public

APOPHENIA processes only **publicly available data** and **locally generated synthetic datasets**. No proprietary or personal data is collected, transmitted, or stored.

### Public data

- **Open-Meteo** — Bay of Plenty rainfall and weather forecasts
- **Overpass / OpenStreetMap** — SH2 state-highway corridor geometry
- **Frankfurter** — NZD foreign-exchange rates
- **Stats NZ Horticulture Survey** — volume baselines (~120M trays seasonal)
- **Mapbox** — vector basemap and TomTom-sourced live traffic
- **ZGL Quality Manual 2026** (public PDF) — MTS thresholds, dry-matter ranges, CCP triggers
- **Grower Payments Booklet 2026** (public PDF) — submit rates, taste-payment formulae

### Synthetic data

Operational variables — packhouse inventory, dry-matter readings, MTS pass rates, OTIF percentages — are stochastically generated within ranges documented in the public sources above. Specifically:

- Dry-matter readings: normal distribution per subzone, μ and σ derived from QM 2026 (e.g. Ōpōtiki: μ=16.37%, σ=0.75%)
- Tray volumes: scaled to a 120M baseline (Stats NZ aggregate)
- Climate-event narratives: calibrated to documented events (Cyclone Gabrielle 2024/25, El Niño 2023/24)

**No proprietary ZGL operational data has been accessed.** This is a methodological exercise in calibrated synthesis.

---

## Risk model (APO v4)

A multi-variable regression with logistic transform combining five operational dimensions:

| Variable | Weight | Source |
|----------|--------|--------|
| Dry-matter % | 35% | Synthetic (calibrated to QM 2026) |
| Pest pressure | 25% | Synthetic (calibrated to CCP triggers) |
| SH2 congestion | 15% | Live (Mapbox traffic) + synthetic baseline |
| Rainfall | 15% | Live (Open-Meteo) |
| Regulatory load | 10% | Synthetic |

### Validation

- **Backtest horizon**: 3 simulated seasons (2022/23 → 2024/25)
- **R² on synthetic backtest**: 0.82
- **OTIF projection accuracy**: ±8% within a 14-day horizon
- **Cost-of-delay estimate**: ±12% under stable conditions
- **26-week risk arc**: 90% confidence intervals

### Known boundaries

- Validated only against synthetic data; no real production validation has been performed
- Black-swan event performance (cyclones, port strikes) is illustrative
- Subzone granularity below packhouse level is not supported
- Pricing inputs are assumed constant within forecast windows

---

## Recommendation logic

Executive recommendations are derived deterministically from the dominant sensitivity in the current scenario. No machine learning is used at the recommendation stage — the rules are explicit, auditable, and documented in the codebase.

---

## Disclaimer

Recommendations are illustrative. They should not be used as the sole basis for binding operational decisions and must be validated by an Operations Lead in any real-world context.
