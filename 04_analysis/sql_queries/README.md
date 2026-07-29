# SQL Queries v2 — Star Schema

Queries refactored against the star schema (`02_data_processed/star_schema/apophenia_star.db`).
Demonstrates JOINs, CTEs, and window functions across 5 dimensions + 1 fact table.

## Schema reference

| Table | Role |
|---|---|
| `fact_pallet_submissions` | Central fact table — one row per pallet submission |
| `dim_date` | Calendar date, pack week, season, milestones |
| `dim_grower` | Grower identity, subzone, region, certification status |
| `dim_corridor` | SH2 corridor segments, origin subzone, destination port, distance |
| `dim_variety` | Kiwifruit variety, tier, MTS threshold |
| `dim_packhouse` | Packhouse identity, PTE mode/score, capacity |

> **Note:** an earlier draft of this README described a different table set
> (`fact_export_transactions`, `dim_season`, `dim_route`, `dim_fruit_quality`) —
> that was the schema of the original `kiwifruit_export.db` build (documented in
> `transform_report.md`, same folder), not the current `apophenia_star.db`.
> `apophenia_star.db` is a simplified, derived version of that earlier star
> schema, rebuilt for the Power BI dashboard currently in use. The v1 queries
> written against the original schema are preserved in `../legacy_queries/` and
> still have documentation value — they show the schema's evolution across the
> project.

## Query catalogue

Queries are numbered continuing on from `../legacy_queries/` (`q1`–`q6`), starting at `q7_…sql`. Each file contains:
- Business question header comment
- CTE or JOIN structure using star schema tables
- Window function(s) where applicable

| ID | Question |
|----|----------|
| Q7 | General KPIs across all pallet submissions (Power BI top card row) |
| Q8 | Which SH2 corridor carries the highest delay risk? |
| Q9 | How does dry matter compliance and payment per tray vary by variety? |
| Q10 | How does OTIF performance trend across the season? |
| Q11 | Which growers carry the highest lateness risk? |

`sql_practice_window_functions.sql` is a separate, non-numbered file — curated
`ROW_NUMBER`/`RANK`/`DENSE_RANK` practice queries against `apophenia_star.db`,
not part of the dashboard query set.

## Legacy queries

v1 queries (written against the monolithic `kiwifruit_export.db`) are preserved in `../legacy_queries/`.
