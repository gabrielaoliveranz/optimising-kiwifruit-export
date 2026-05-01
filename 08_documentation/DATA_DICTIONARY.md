# Data Dictionary

## Star schema overview

The SQLite database `02_data_processed/kiwifruit_export.db` follows a star schema with one fact table and four dimension tables.

---

## Fact table

### `fact_export_transactions`

| Column | Type | Description |
|--------|------|-------------|
| transaction_id | INTEGER PK | Surrogate key |
| date_key | INTEGER FK | Reference to dim_time |
| subzone | TEXT | Bay of Plenty subzone (Katikati, Te Puke, Pongakawa, Tauranga, Ōpōtiki) |
| variety | TEXT | Kiwifruit variety (Green, SunGold, Organic SunGold, Organic Green) |
| season | TEXT | Season label (e.g. 2024/25) |
| pack_week | INTEGER | ISO pack week number |
| dm_pct_avg | REAL | Average dry-matter percentage |
| mts_pass | INTEGER | MTS threshold flag (0 = fail, 1 = pass) |
| otif_pct | REAL | On-Time-In-Full delivery percentage |
| congestion_index | REAL | SH2 corridor congestion index (0–100) |
| rainfall_mm_7d | REAL | 7-day rolling rainfall (mm) |
| risk_score | REAL | Composite risk score (0–100) |
| trays_submitted | INTEGER | Tray count submitted |
| trays_exported | INTEGER | Tray count exported |
| submit_payment_nzd | REAL | Submit-rate payment |
| taste_payment_nzd | REAL | TZG taste-payment bonus |
| total_return_nzd | REAL | Total grower return |
| margin_erosion_pct | REAL | Margin erosion vs baseline |

---

## Dimension tables

### `dim_time`

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER PK | Surrogate key |
| date | TEXT | Calendar date |
| season | TEXT | Season label |
| pack_week | INTEGER | ISO pack week number |
| season_phase | TEXT | Pre-pack / MainPack / Post-pack |

### `dim_fruit_quality`

| Column | Type | Description |
|--------|------|-------------|
| subzone | TEXT | Bay of Plenty subzone |
| season | TEXT | Season label |
| dm_pct | REAL | Individual dry-matter reading |
| mts_status | TEXT | PASS / FAIL |
| pest_index | REAL | Pest pressure index |

### `dim_logistics`

| Column | Type | Description |
|--------|------|-------------|
| corridor | TEXT | Corridor name |
| distance_km | REAL | Road distance to port (km) |
| congestion_baseline | REAL | Baseline congestion index |
| port | TEXT | Export port name |

### `dim_payment_structure`

| Column | Type | Description |
|--------|------|-------------|
| variety | TEXT | Kiwifruit variety |
| mts_threshold | REAL | MTS minimum dry-matter threshold (%) |
| submit_rate_nzd | REAL | Submit rate per tray (NZD) |
| taste_max_nzd | REAL | Maximum taste-payment bonus per tray (NZD) |

---

## Variable units

| Variable | Unit | Range |
|----------|------|-------|
| Dry-matter | percent (%) | 14.0 – 20.0 |
| Rainfall | millimetres (mm) | 0 – 200 |
| Congestion | index | 0 – 100 |
| OTIF | percent (%) | 0 – 100 |
| Risk score | index | 0 – 100 |
| Returns | NZD | varies |
| Tray volume | trays | varies |

All percentages stored as decimals scaled 0–100 (not 0–1).
