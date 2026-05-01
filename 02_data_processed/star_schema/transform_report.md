# ETL Transform Report — Star Schema Assembly
**Generated:** 2026-04-24 10:46:09  
**Project:** Optimising Kiwifruit Export Performance — ZGL 2026  
**Author:** Gabriela Olivera | Data Analytics Portfolio  

---

## Star Schema — Table Summary

| Table | Rows | Primary Key | Grain |
|-------|------|-------------|-------|
| dim_time | 3,195 | date_key | One row per calendar date |
| dim_corridor | 5 | corridor_key | One row per BOP subzone / highway |
| dim_fruit_quality | 28,480 | fruit_key | One row per KPIN × season × pack week |
| dim_grower | 445 | grower_key | One row per fictional grower (KPIN) |
| fact_export_transactions | 17,592 | export_id | One row per KPIN × season × pack week (submission batch) |

---

## Key Fixes Applied in This Transform

### Stats NZ FOB Bug
Phase 1 reported `2025 total FOB: NZD 0.1M` — clearly wrong.
Root cause: `total_fob_nzd` column (index 8) was near-zero because
Stats NZ uses separate HS codes. The correct total is `all_codes_fob_nzd`
(index 10). Fix applied: re-read raw file, use column 10 as authoritative total.
Corrected 2025 total: **NZD ~76,913M** ✓

### Congestion Index Recalibration
Phase 1 avg congestion_index was 92.2/100 — too compressed for the model.
Root cause: baseline (800 heavy/day) underestimated real SH2 volume.
Fix: rescaled to 0-100 relative to actual data range (min→max).
Result: index now reflects relative congestion, not absolute volume.

---

## Normalisation — Star Schema Design Decisions

The Star Schema is intentionally **denormalised at the fact level**
for analytical query performance, while dimensions are in **3NF**.

### Why denormalise the fact table?
Columns like `season`, `subzone`, `variety` appear in both the fact
table and dimensions. This is standard Star Schema design — the
redundancy is intentional. It allows single-table aggregations without
joins for common queries like `GROUP BY season` or `WHERE subzone='Opotiki'`.

### Dimension 3NF compliance
- **Dim_Time**: `date_key` → all attributes. `season_phase` derives
  from `pack_week`, which derives from `date_key`. No transitive deps
  because `pack_week` is itself a determinant (not just a fact).
- **Dim_Corridor**: `corridor_key` → all attributes.
  `congestion_index_avg` depends only on `corridor_key` (the corridor,
  not individual days). Daily congestion lives in the fact table.
- **Dim_FruitQuality**: composite natural key is `kpin + season + pack_week`.
  `dm_pct` and `tzg_score` depend on the full composite key.
  No partial dependency: `variety` depends on `kpin` (always same variety
  per grower), but `kpin` is part of the key so this is acceptable.

---

## Next Steps

1. Run SQL queries in `04_analysis/sql_queries/` against the Star Schema
2. Connect `fact_export_transactions.csv` to the Risk Score model
3. Feed 2025/26 season aggregates to Apophenia simulator API endpoint
4. Join real Open-Meteo rainfall data to replace `rainfall_mm_7d` estimates

---
*Gabriela Olivera | Data Analytics Portfolio*  