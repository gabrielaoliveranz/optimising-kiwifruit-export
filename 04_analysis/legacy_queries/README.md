# Legacy SQL Queries (v1)

> **Superseded.** These are v1 queries against the monolithic fact table (`kiwifruit_export.db` / `fact_export_transactions`).
> See `../sql_queries/` for v2 queries refactored against the star schema (`apophenia_star.db`).

Six documented research queries against `02_data_processed/kiwifruit_export.db`. Each query answers a specific business question relevant to operational risk in NZ kiwifruit export.

## Queries

| ID | Question |
|----|----------|
| Q1 | What % of BOP production falls below MTS Green (15.5%)? How does it vary by season and variety? |
| Q2 | In which pack weeks does SH2 congestion cause greatest OTIF degradation? |
| Q3 | What is the NZD elasticity of dry-matter percentage in grower payments? |
| Q4 | Which BOP subzone has the highest DM variance between seasons? |
| Q5 | Does the composite Risk Score predict OTIF < 88% episodes? |
| Q6 | What was the highest-risk pack week in the dataset and what caused it? |

## Run the queries

```bash
cd ..
python 04_analysis/05_sql_analysis.py
```

Output is saved to `query_results.md` in this folder (kept isolated from `../sql_queries/` so re-running this script never overwrites the v2 dashboard queries), with full markdown tables and interpretation notes.
