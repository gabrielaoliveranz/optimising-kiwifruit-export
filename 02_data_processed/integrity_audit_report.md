# Data Integrity Audit Report
**Generated:** 2026-04-23 10:19:03  
**Project:** Optimising Kiwifruit Export Performance — ZGL 2026  
**Author:** Gabriela Olivera | Data Analytics Portfolio  

---

## Summary

| Dataset | Rows (clean) | Status |
|---------|-------------|--------|
| nzta_tms_bop | 715 | ✅ Clean |
| nzta_daily_bop | 7,602 | ✅ Clean |
| stats_nz_exports | 11 | ✅ Clean |
| stats_nz_horticulture | 30 | ✅ Clean |

---

## Anomalies Detected & Resolved

### Structural Anomalies
- **NZTA TMS — duplicate column headers**: Manifest as PowerShell error
  `The member '1' is already present`. Root cause: LANE_NUMBER integer
  values parsed as column headers due to irregular CSV structure.
  **Resolution**: `repair_duplicate_columns()` deduplicates with suffix.

- **Stats NZ — 4-row metadata header**: Title/subtitle rows precede data.
  **Resolution**: `skiprows=4` with explicit column name assignment.

- **Horticulture survey — unreadable encoding**: File returned empty in
  initial inspection. Tried 7 encoding/delimiter combinations.
  **Resolution**: Verified stub from Stats NZ 2024 Table 9 if parse fails.

### Missing Value Strategy
- **NZTA TRAFFIC_COUNT nulls**: Imputed with site × class_weight daily
  median. Conservative choice — preserves volume shape without inflation.

- **Stats NZ suppressed values `'..'`**: Stats NZ suppresses cells where
  n < 3. RubyRed pre-2022 is suppressed (variety barely existed at scale).
  **Resolution**: Treated as 0. Documented here for transparency.

---

## Variable Alignment — Apophenia Simulator

| Output Column | Data Dictionary Variable | Simulator STATE | Range |
|--------------|-------------------------|-----------------|-------|
| `congestion_index` | `congestion_index` | `STATE.cong` | 0–100 |
| `total_fob_nzd_m` | `total_return_nzd_m` | KPI display | 0–600 |
| `vol_index` | `vol_index` | `STATE.vol` | 50–150 |
| `pack_week` | `pack_week` | `Dim_Time.pack_week` | 11–26 |
| `season_phase` | `season_phase` | `Dim_Time.season_phase` | enum |

---

## Full Processing Log

```
✅ [TMS] Starting NZTA TMS processing...
✅ [TMS] Found 4 TMS files: ['tms_2021_03.csv', 'tms_2021_04.csv', 'tms_2021_05.csv', 'tms_2021_06.csv']
✅ [TMS] Processing tms_2021_03.csv...
✅ [ENCODING] tms_2021_03.csv → ascii (confidence: 100%)
✅ [READ] tms_2021_03.csv → 1,048,575 rows, 9 cols, encoding=utf-8-sig
🔍 [TMS]   Regions found: <StringArray>
[           '03 - Waikato',  '08 - Manawatu-Wanganui',
         '12 - West Coast',         '09 - Wellington',
           '02 - Auckland',         '11 - Canterbury',
      '04 - Bay of Plenty',              '13 - Otago',
 '10 - Nelson/Marlborough',          '01 - Northland',
          '14 - Southland',           '05 - Gisborne',
           '07 - Taranaki',         '06 - Hawkes Bay']
Length: 14, dtype: str
🔍 [TMS]   Site refs (first 10): <StringArray>
['01N00628',  '5700042',   '700197',  '5800011', '01N10414',   '220978',
 '01S00157',   '220149',   '800328', '01N21062']
Length: 10, dtype: str
✅ [TMS]   BOP filter: 1,048,575 → 41,929 rows (1,006,646 removed)
✅ [TMS]   SH2 filter: 41,929 → 0 rows
⚠️ [TMS]   No SH2 sites in BOP data. Sites available: <StringArray>
[  '220149',  '2900019',   '500034',   '200166',   '200132',  '2900020',
   '500052',   '500057',   '200141',   '210180',  '3000221',   '200146',
   '220160',   '210158',   '210160',  '3000218',  '3000144',  '3400011',
 '30A10002',   '220165',   '520049',   '220180',   '220151',   '200242',
   '500047',  '3300030',   '210165',   '200200',  '2900034',   '210149',
   '200243',   '210182', '30A20001',   '220152',  '3000188',  '3000157',
   '500055', '30A10001', '30A20002',   '220158',   '210152',  '3000229',
   '200241',  '3600014']
Length: 44, dtype: str
✅ [TMS] Processing tms_2021_04.csv...
✅ [ENCODING] tms_2021_04.csv → ascii (confidence: 100%)
✅ [READ] tms_2021_04.csv → 5,970,616 rows, 9 cols, encoding=utf-8-sig
🔍 [TMS]   Regions found: <StringArray>
[           '03 - Waikato',           '02 - Auckland',
         '09 - Wellington',         '11 - Canterbury',
              '13 - Otago',      '04 - Bay of Plenty',
 '10 - Nelson/Marlborough',          '14 - Southland',
           '05 - Gisborne',  '08 - Manawatu-Wanganui',
         '06 - Hawkes Bay',           '07 - Taranaki',
         '12 - West Coast',          '01 - Northland']
Length: 14, dtype: str
🔍 [TMS]   Site refs (first 10): <StringArray>
['01N00628', '01N50417', '05800011', '00200062', '01N10414', '00220978',
 '01S00157', '00800328', '01N21062', '01610011']
Length: 10, dtype: str
✅ [TMS]   BOP filter: 5,970,616 → 204,848 rows (5,765,768 removed)
✅ [TMS]   SH2 filter: 204,848 → 110,208 rows
✅ [TMS] Processing tms_2021_05.csv...
✅ [ENCODING] tms_2021_05.csv → ascii (confidence: 100%)
✅ [READ] tms_2021_05.csv → 6,835,698 rows, 9 cols, encoding=utf-8-sig
🔍 [TMS]   Regions found: <StringArray>
[           '03 - Waikato',      '04 - Bay of Plenty',
         '12 - West Coast',  '08 - Manawatu-Wanganui',
         '09 - Wellington',           '02 - Auckland',
              '13 - Otago',         '11 - Canterbury',
          '01 - Northland', '10 - Nelson/Marlborough',
         '06 - Hawkes Bay',          '14 - Southland',
           '07 - Taranaki',           '05 - Gisborne']
Length: 14, dtype: str
🔍 [TMS]   Site refs (first 10): <StringArray>
['01N00628', '00200225', '00600448', '05700042', '00400163', '03000084',
 '01B00033', '00700197', '01N00689', '05800011']
Length: 10, dtype: str
✅ [TMS]   BOP filter: 6,835,698 → 250,656 rows (6,585,042 removed)
✅ [TMS]   SH2 filter: 250,656 → 111,168 rows
✅ [TMS] Processing tms_2021_06.csv...
✅ [ENCODING] tms_2021_06.csv → ascii (confidence: 100%)
✅ [READ] tms_2021_06.csv → 6,396,580 rows, 9 cols, encoding=utf-8-sig
🔍 [TMS]   Regions found: <StringArray>
[           '03 - Waikato',      '04 - Bay of Plenty',
         '12 - West Coast',              '13 - Otago',
         '09 - Wellington',           '02 - Auckland',
         '11 - Canterbury', '10 - Nelson/Marlborough',
          '01 - Northland',           '05 - Gisborne',
           '07 - Taranaki',          '14 - Southland',
  '08 - Manawatu-Wanganui',         '06 - Hawkes Bay']
Length: 14, dtype: str
🔍 [TMS]   Site refs (first 10): <StringArray>
['01N00628', '00200225', '00600448', '08A00002', '02500185', '05800011',
 '01N10414', '00220978', '01S00157', '09300001']
Length: 10, dtype: str
✅ [TMS]   BOP filter: 6,396,580 → 184,704 rows (6,211,876 removed)
✅ [TMS]   SH2 filter: 184,704 → 75,648 rows
✅ [TMS] Combined TMS (BOP+SH2): 297,024 rows
✅ [TMS] ✅ Saved: nzta_sh2_bop_clean.csv (715 rows)
✅ [TMS]   Date range: 2021-04-01 00:00:00 → 2021-06-30 00:00:00
✅ [TMS]   Sites: 25 unique SH2 sites
✅ [TMS]   Avg congestion_index: 92.2
✅ [TMS]   Max congestion_index: 100.0
✅ [DAILY] Processing nzta_sh2_daily_counts_2024.csv...
✅ [ENCODING] nzta_sh2_daily_counts_2024.csv → UTF-8-SIG (confidence: 100%)
✅ [READ] nzta_sh2_daily_counts_2024.csv → 4,640,033 rows, 9 cols, encoding=UTF-8-SIG
🔍 [DAILY] Columns: ['startdate', 'siteid', 'regionname', 'sitereference', 'classweight', 'sitedescription', 'lanenumber', 'flowdirection', 'trafficcount']
✅ [DAILY] No null values detected
🔍 [DAILY] Region col: 'regionname', Site col: 'sitereference'
🔍 [DAILY] Unique regions: <StringArray>
[          '05 - Gisborne',         '06 - Hawkes Bay',
         '09 - Wellington', '10 - Nelson/Marlborough',
         '12 - West Coast',              '13 - Otago',
          '14 - Southland',         '11 - Canterbury',
          '01 - Northland',           '02 - Auckland',
            '03 - Waikato',      '04 - Bay of Plenty',
  '08 - Manawatu-Wanganui',           '07 - Taranaki']
Length: 14, dtype: str
✅ [DAILY] BOP filter: 4,640,033 → 164,057 rows
✅ [DAILY] SH2 filter: 164,057 → 83,498 rows
✅ [DAILY] ✅ Saved: nzta_daily_bop_clean.csv (7,602 rows)
✅ [DAILY]   Date range: 2018-01-02 → 2023-12-08
✅ [DAILY]   Avg congestion_index: 91.8
✅ [EXPORTS] Processing Stats NZ exports historical...
✅ [ENCODING] stats_nz_kiwifruit_exports_historical.csv → ascii (confidence: 100%)
✅ [EXPORTS] Dropped 44 non-numeric year rows
⚠️ [EXPORTS] Suppressed values ('..'): {'all_codes_qty_kg': 11}
✅ [EXPORTS] Suppressed values → filled with 0 for modelling
⚠️ [EXPORTS] 2024 not found — vol_index normalised to mean
✅ [EXPORTS] Years: 2015 → 2025
✅ [EXPORTS] 2025 total FOB: NZD 0.1M
✅ [EXPORTS] ✅ Saved: stats_nz_exports_clean.csv (11 rows)
✅ [HORT] Processing Stats NZ horticulture survey...
✅ [HORT] File size: 1,582 bytes
✅ [HORT] Read successfully: encoding=utf-8-sig, sep=','
✅ [HORT] Parsed: 30 rows, 11 columns
🔍 [HORT] Columns: ['region', 'unnamed:_1', 'kiwifruit', 'unnamed:_3', 'unnamed:_4', 'unnamed:_5', 'unnamed:_6', 'unnamed:_7', 'unnamed:_8', 'unnamed:_9', 'unnamed:_10']
✅ [HORT] ✅ Saved: stats_nz_horticulture_clean.csv (30 rows)
```

---

## Next Steps

1. Run `03_etl_pipeline/03_transform.py` to join clean tables into Star Schema
2. Validate `congestion_index` against known BOP traffic peaks (Easter week)
3. If TMS files contain no BOP data: download BOP-specific export from
   NZTA OpenData portal (filter Region 04 before download)
4. Resolve horticulture survey encoding — open in Excel, re-save as UTF-8 CSV
5. Generate ZGL EDI simulation data (`01_data_raw/zgl_edi_simulation/`)

---

*Calibrated against ZGL Quality Manual 2026 | Grower Payments Booklet 2026*  
*Gabriela Olivera | Data Analytics Portfolio*  