# SQL Queries v2 — Star Schema

Queries refactored against the star schema (`02_data_processed/star_schema/apophenia_star.db`).
Demonstrates JOINs, CTEs, and window functions across 5 dimensions + 1 fact table.

## Schema reference

| Table | Role |
|---|---|
| `fact_export_transactions` | Central fact table — one row per consignment |
| `dim_season` | Season metadata (variety, year, MTS thresholds) |
| `dim_packhouse` | Packhouse → subzone → corridor mapping |
| `dim_fruit_quality` | DM %, MTS pass/fail per subzone per season |
| `dim_route` | SH2 corridor segments, dwell baselines |
| `dim_time` | Pack week calendar, ISO week, season position |

## Query catalogue

Queries are numbered `q1_…sql` through `q6_…sql` (to be added). Each file contains:
- Business question header comment
- CTE or JOIN structure using star schema tables
- Window function(s) where applicable

## Legacy queries

v1 queries (written against the monolithic `kiwifruit_export.db`) are preserved in `../legacy_queries/`.
