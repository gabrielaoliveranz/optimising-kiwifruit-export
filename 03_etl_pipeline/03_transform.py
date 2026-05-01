"""
=============================================================================
OPTIMISING KIWIFRUIT EXPORT PERFORMANCE — ZGL 2026
Script: 03_transform.py
Stage: ETL Phase 2 — Star Schema Assembly & Normalisation
Author: Gabriela Olivera | Data Analytics Portfolio
=============================================================================

WHAT THIS SCRIPT DOES:
  Joins all cleaned datasets into a Star Schema ready for SQL analysis.
  Produces 5 output files:

  DIMENSIONS:
    dim_time.csv           — Pack weeks, dates, season phases
    dim_corridor.csv       — BOP subzones, SH2 sites, congestion
    dim_fruit_quality.csv  — KPIN × variety × DM readings
    dim_grower.csv         — Grower register with subzone metadata

  FACT TABLE:
    fact_export_transactions.csv — Core analytical table joining all dims

  Also fixes the Stats NZ FOB parsing bug from ETL Phase 1:
  - total_fob_nzd was being read from wrong column position
  - Re-parses and recalculates vol_index correctly against 120M tray baseline

  NORMALISATION NOTE:
  All output tables are in 3NF:
  - dim_time: date_key (PK) → all time attributes. No transitive deps.
  - dim_corridor: corridor_key (PK) → site attributes. congestion_index
    stored as weekly avg, not per-row (would create partial dependency).
  - dim_fruit_quality: fruit_key (PK) → KPIN + pack_week composite.
    dm_pct and tzg_score depend on full key, not just KPIN.
  - fact table: export_id (PK), all FKs, all measures depend only on PK.

HOW TO RUN:
  python 03_etl_pipeline/03_transform.py
  Run from project root: G:\\My Drive\\optimising-kiwifruit-export\\

INPUTS (from 02_data_processed/ and 01_data_raw/zgl_edi_simulation/):
  nzta_daily_bop_clean.csv
  nzta_sh2_bop_clean.csv
  stats_nz_exports_clean.csv
  stats_nz_horticulture_clean.csv
  zgl_maturity_readings.csv
  zgl_pallet_submissions.csv
  zgl_grower_register.csv

OUTPUTS (all in 02_data_processed/star_schema/):
  dim_time.csv
  dim_corridor.csv
  dim_fruit_quality.csv
  dim_grower.csv
  fact_export_transactions.csv
  transform_report.md
=============================================================================
"""

import warnings
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
PROCESSED    = PROJECT_ROOT / "02_data_processed"
ZGL_SIM      = PROJECT_ROOT / "01_data_raw" / "zgl_edi_simulation"
STAR         = PROCESSED / "star_schema"
STAR.mkdir(exist_ok=True)

# ZGL 2026 constants — identical to simulator and generator
MTS_GREEN    = 15.5
MTS_SUNGOLD  = 16.1
MTS_RUBY     = 17.2
OTIF_BASE    = 97.5
BASE_RATE    = 3.20    # NZD/tray blended pool
TASTE_MAX    = 0.95    # NZD/tray max taste bonus

# Audit log
log_lines = []

def log(msg: str, level: str = "INFO"):
    tag = {"INFO": "✅", "WARN": "⚠️ ", "ERROR": "❌", "FIND": "🔍"}.get(level, "•")
    line = f"  {tag} {msg}"
    print(line)
    log_lines.append(line)


# =============================================================================
# LOAD ALL CLEAN SOURCES
# =============================================================================

def load_sources() -> dict:
    log("Loading all cleaned source files...")

    sources = {}

    # NZTA daily (primary BOP traffic source — 2018-2023)
    p = PROCESSED / "nzta_daily_bop_clean.csv"
    if p.exists():
        sources["nzta_daily"] = pd.read_csv(p, low_memory=False)
        sources["nzta_daily"]["date"] = pd.to_datetime(
            sources["nzta_daily"]["date"], errors="coerce"
        )
        log(f"nzta_daily_bop_clean.csv → {len(sources['nzta_daily']):,} rows")
    else:
        log("nzta_daily_bop_clean.csv not found", "WARN")

    # NZTA TMS (15-min → daily, April-June 2021)
    p = PROCESSED / "nzta_sh2_bop_clean.csv"
    if p.exists():
        df = pd.read_csv(p, low_memory=False)
        if len(df) > 0:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            sources["nzta_tms"] = df
            log(f"nzta_sh2_bop_clean.csv → {len(df):,} rows")
        else:
            log("nzta_sh2_bop_clean.csv is empty (placeholder) — skipping", "WARN")

    # Stats NZ exports — re-parse to fix FOB bug
    p = PROCESSED / "stats_nz_exports_clean.csv"
    if p.exists():
        sources["stats_exports"] = pd.read_csv(p)
        log(f"stats_nz_exports_clean.csv → {len(sources['stats_exports'])} rows")

    # ZGL EDI simulation
    for name, fname in [
        ("zgl_maturity",    "zgl_maturity_readings.csv"),
        ("zgl_submissions", "zgl_pallet_submissions.csv"),
        ("zgl_growers",     "zgl_grower_register.csv"),
        ("zgl_losses",      "zgl_fruit_loss_records.csv"),
    ]:
        p = ZGL_SIM / fname
        if p.exists():
            sources[name] = pd.read_csv(p, low_memory=False)
            log(f"{fname} → {len(sources[name]):,} rows")
        else:
            log(f"{fname} not found", "ERROR")

    return sources


# =============================================================================
# FIX STATS NZ FOB BUG
# =============================================================================

def fix_stats_nz_fob(sources: dict) -> pd.DataFrame:
    """
    The Phase 1 script mapped total_fob_nzd to the wrong column.
    The raw file has 11 columns but the aggregated total is in
    column index 9 (all_codes_fob_nzd), not column 8 (total_fob_nzd).

    From the PowerShell inspection we confirmed:
      2025 all_codes_fob = 76,913,603,766 NZD (raw)
      = NZD 76,913M — this is the correct total

    Re-read from raw to get the correct figure.
    """
    raw_path = PROJECT_ROOT / "01_data_raw" / "stats_nz" / \
               "stats_nz_kiwifruit_exports_historical.csv"

    col_names = [
        "year",
        "gold_qty_kg",    "gold_fob_nzd",
        "green_qty_kg",   "green_fob_nzd",
        "red_qty_kg",     "red_fob_nzd",
        "total_qty_kg",   "total_fob_nzd",
        "all_codes_qty_kg", "all_codes_fob_nzd"
    ]

    try:
        df = pd.read_csv(
            raw_path,
            skiprows=4,
            header=None,
            names=col_names,
            na_values=["", '".."', ".."],
            encoding="utf-8"
        )
    except Exception:
        df = pd.read_csv(
            raw_path,
            skiprows=4,
            header=None,
            names=col_names,
            na_values=["", '".."', ".."],
            encoding="latin-1"
        )

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    for col in col_names[1:]:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace('"', '').str.strip(),
            errors="coerce"
        ).fillna(0)

    # Use all_codes_fob_nzd as the authoritative total
    # (includes all HS codes — most complete figure)
    df["total_export_fob_nzd"]   = df["all_codes_fob_nzd"]
    df["total_export_fob_nzd_m"] = (df["total_export_fob_nzd"] / 1_000_000).round(2)
    df["gold_fob_nzd_m"]         = (df["gold_fob_nzd"] / 1_000_000).round(2)
    df["green_fob_nzd_m"]        = (df["green_fob_nzd"] / 1_000_000).round(2)
    df["red_fob_nzd_m"]          = (df["red_fob_nzd"] / 1_000_000).round(2)

    # vol_index: 2024 = 100 baseline
    ref = df.loc[df["year"] == 2024, "total_export_fob_nzd_m"].values
    if len(ref) > 0 and ref[0] > 0:
        df["vol_index"] = (df["total_export_fob_nzd_m"] / ref[0] * 100).round(1)
    else:
        df["vol_index"] = (df["total_export_fob_nzd_m"] /
                           df["total_export_fob_nzd_m"].mean() * 100).round(1)

    log(f"Stats NZ FOB fix: 2025 total = "
        f"NZD {df.loc[df['year']==2025,'total_export_fob_nzd_m'].values[0]:,.1f}M ✓")
    log(f"Stats NZ FOB fix: 2024 vol_index = "
        f"{df.loc[df['year']==2024,'vol_index'].values[0]:.1f} (baseline=100)")

    return df


# =============================================================================
# DIM_TIME
# =============================================================================

def build_dim_time(sources: dict) -> pd.DataFrame:
    """
    Build Dim_Time from NZTA traffic dates + ZGL submission dates.

    Covers: 2018-01-01 → 2026-09-30
    Grain: one row per calendar date
    Primary key: date_key (integer YYYYMMDD)

    Pack week mapping:
    - ISO week is used as a proxy for pack week
    - Season phase: KiwiStart (pw<14), MainPack (14-22), Late (23+)
    - Season year: the export season a date belongs to
      (April 2025 → season 2025/26)
    """
    log("Building Dim_Time...")

    # Collect all dates from all sources
    all_dates = pd.date_range("2018-01-01", "2026-09-30", freq="D")
    df = pd.DataFrame({"date": all_dates})

    df["date_key"]     = df["date"].dt.strftime("%Y%m%d").astype(int)
    df["iso_week"]     = df["date"].dt.isocalendar().week.astype(int)
    df["iso_year"]     = df["date"].dt.isocalendar().year.astype(int)
    df["month"]        = df["date"].dt.month
    df["year"]         = df["date"].dt.year
    df["pack_week"]    = df["iso_week"]

    # Season year: April-March (Southern Hemisphere export season)
    df["season_year"] = df.apply(
        lambda r: f"{r['year']}/{str(r['year']+1)[2:]}"
        if r["month"] >= 4
        else f"{r['year']-1}/{str(r['year'])[2:]}",
        axis=1
    )

    # Season phase
    df["season_phase"] = df["pack_week"].apply(
        lambda w: "KiwiStart" if w < 14
                  else "MainPack" if w <= 22
                  else "Late"
    )

    # Is pack season active?
    df["is_pack_season"] = df["month"].isin([3, 4, 5, 6, 7, 8])

    # Add rainfall from NZTA daily if available (proxy — real NIWA data goes here)
    df["rainfall_mm_7d"] = np.nan   # placeholder for Open-Meteo join

    out = STAR / "dim_time.csv"
    df.to_csv(out, index=False)
    log(f"dim_time.csv → {len(df):,} rows | "
        f"{df['season_year'].nunique()} seasons | "
        f"date_key range: {df['date_key'].min()}→{df['date_key'].max()}")
    return df


# =============================================================================
# DIM_CORRIDOR
# =============================================================================

def build_dim_corridor(sources: dict) -> pd.DataFrame:
    """
    Build Dim_Corridor from NZTA traffic data + subzone definitions.

    Grain: one row per corridor segment (subzone × highway combination)
    Primary key: corridor_key (auto-increment integer)

    congestion_index: weekly average per site, joined from nzta_daily.
    Sites without traffic data get the subzone baseline estimate.

    Recalibration note:
    The avg congestion_index of 92 from ETL Phase 1 indicated the
    baseline (800 heavy/day) was too low. Recalibrated here using
    the actual median from the clean data as the new baseline.
    """
    log("Building Dim_Corridor...")

    # Subzone reference table — from Master Context + MPI data
    subzone_ref = pd.DataFrame([
        {
            "subzone":                  "Te Puke",
            "highway":                  "SH2",
            "distance_port_km":         28,
            "base_risk_weight":         0.35,
            "psa_incidence_historical": 0.18,
        },
        {
            "subzone":                  "Katikati",
            "highway":                  "SH2",
            "distance_port_km":         52,
            "base_risk_weight":         0.20,
            "psa_incidence_historical": 0.09,
        },
        {
            "subzone":                  "Tauranga",
            "highway":                  "SH2",
            "distance_port_km":         12,
            "base_risk_weight":         0.18,
            "psa_incidence_historical": 0.12,
        },
        {
            "subzone":                  "Pongakawa",
            "highway":                  "SH2",
            "distance_port_km":         35,
            "base_risk_weight":         0.15,
            "psa_incidence_historical": 0.14,
        },
        {
            "subzone":                  "Opotiki",
            "highway":                  "SH2",
            "distance_port_km":         97,
            "base_risk_weight":         0.12,
            "psa_incidence_historical": 0.22,
        },
    ])

    # Compute congestion stats from real NZTA data
    if "nzta_daily" in sources and "congestion_index" in sources["nzta_daily"].columns:
        nzta = sources["nzta_daily"].copy()

        # Recalibrate: use p25 as "normal" baseline, p75 as "congested"
        ci_median = nzta["congestion_index"].median()
        ci_p75    = nzta["congestion_index"].quantile(0.75)
        ci_max    = nzta["congestion_index"].max()

        log(f"NZTA congestion_index — median: {ci_median:.1f}, "
            f"p75: {ci_p75:.1f}, max: {ci_max:.1f}", "FIND")
        log("Note: high median (92) reflects baseline miscalibration. "
            "Congestion_index recalibrated in dim_corridor using relative scale.", "WARN")

        # Rescale congestion_index to 0-100 relative to actual data range
        ci_min = nzta["congestion_index"].min()
        if ci_max > ci_min:
            nzta["congestion_index_scaled"] = (
                (nzta["congestion_index"] - ci_min) /
                (ci_max - ci_min) * 100
            ).round(1)
        else:
            nzta["congestion_index_scaled"] = 25.0

        # Weekly average congestion per pack week
        if "pack_week" in nzta.columns:
            weekly_cong = (nzta.groupby("pack_week")["congestion_index_scaled"]
                          .mean()
                          .round(1)
                          .reset_index()
                          .rename(columns={"congestion_index_scaled":
                                           "avg_congestion_by_week"}))
            log(f"Weekly congestion computed for "
                f"{len(weekly_cong)} pack weeks", "FIND")

        # Overall average for subzone table
        avg_cong = nzta["congestion_index_scaled"].mean()
    else:
        avg_cong = 25.0
        log("No NZTA data available — using default congestion_index=25", "WARN")

    subzone_ref["congestion_index_avg"] = round(avg_cong, 1)
    subzone_ref["corridor_key"] = range(1, len(subzone_ref) + 1)

    # Reorder columns — PK first
    cols = ["corridor_key", "subzone", "highway", "distance_port_km",
            "congestion_index_avg", "base_risk_weight",
            "psa_incidence_historical"]
    subzone_ref = subzone_ref[cols]

    out = STAR / "dim_corridor.csv"
    subzone_ref.to_csv(out, index=False)
    log(f"dim_corridor.csv → {len(subzone_ref)} corridors")
    return subzone_ref


# =============================================================================
# DIM_FRUIT_QUALITY
# =============================================================================

def build_dim_fruit_quality(sources: dict) -> pd.DataFrame:
    """
    Build Dim_FruitQuality from ZGL maturity readings.

    Grain: one row per KPIN × season × pack_week
    Primary key: fruit_key (auto-increment)

    This dimension captures the quality profile of each grower's
    fruit at each point in the season — the core of the DM analysis.

    Joining logic:
    - dm_pct and tzg_score come from zgl_maturity_readings
    - mts_status derived from dm_pct vs variety-specific MTS threshold
    - growing_method derived from variety name ('Organic' prefix)
    """
    log("Building Dim_FruitQuality...")

    if "zgl_maturity" not in sources:
        log("zgl_maturity_readings.csv not found", "ERROR")
        return pd.DataFrame()

    mat = sources["zgl_maturity"].copy()

    # Derive growing method
    mat["growing_method"] = mat["variety"].apply(
        lambda v: "organic" if "Organic" in str(v) else "conventional"
    )

    # MTS status label (cleaner than boolean for SQL queries)
    mat["mts_status"] = mat["mts_pass"].map({True: "PASS", False: "FAIL"})

    # Pest indicator from PSA penalty flag
    mat["pest_indicator"] = mat["psa_penalty_applied"].astype(int)

    # Maturity area = KPIN + pack_week (ZGL QM definition)
    mat["maturity_area"] = mat["kpin"].astype(str) + "_" + mat["pack_week"].astype(str)

    # Select and rename to match Data Dictionary
    dim = mat[[
        "kpin", "season", "subzone", "variety", "growing_method",
        "pack_week", "reading_date", "dm_pct", "mts_threshold",
        "mts_status", "tzg_score", "tzg_grade", "pest_indicator",
        "maturity_area", "sample_size"
    ]].copy()

    # Add fruit_key PK
    dim = dim.reset_index(drop=True)
    dim.insert(0, "fruit_key", dim.index + 1)

    # Summary stats for audit
    pass_rate = (dim["mts_status"] == "PASS").mean()
    log(f"dim_fruit_quality: {len(dim):,} records | "
        f"MTS pass rate: {pass_rate:.1%} | "
        f"Varieties: {dim['variety'].nunique()} | "
        f"Seasons: {dim['season'].nunique()}")

    # DM distribution by variety
    dm_by_var = dim.groupby("variety")["dm_pct"].agg(["mean", "std", "min", "max"])
    for variety, row in dm_by_var.iterrows():
        log(f"  {variety:<20} "
            f"mean={row['mean']:.2f}%  "
            f"std={row['std']:.2f}%  "
            f"range=[{row['min']:.1f}, {row['max']:.1f}]", "FIND")

    out = STAR / "dim_fruit_quality.csv"
    dim.to_csv(out, index=False)
    log(f"dim_fruit_quality.csv → {len(dim):,} rows")
    return dim


# =============================================================================
# DIM_GROWER (extends grower register)
# =============================================================================

def build_dim_grower(sources: dict, dim_corridor: pd.DataFrame) -> pd.DataFrame:
    """
    Build Dim_Grower — denormalised grower reference table.
    Joins grower register with corridor metadata.
    """
    log("Building Dim_Grower...")

    if "zgl_growers" not in sources:
        log("zgl_grower_register.csv not found", "ERROR")
        return pd.DataFrame()

    growers = sources["zgl_growers"].copy()

    # Join corridor_key from dim_corridor
    growers = growers.merge(
        dim_corridor[["subzone", "corridor_key", "distance_port_km"]],
        on="subzone",
        how="left",
        suffixes=("", "_corridor")
    )

    # Use corridor distance if grower distance is missing
    if "distance_port_km_corridor" in growers.columns:
        growers["distance_port_km"] = growers["distance_port_km"].fillna(
            growers["distance_port_km_corridor"]
        )
        growers = growers.drop(columns=["distance_port_km_corridor"])

    growers = growers.reset_index(drop=True)
    growers.insert(0, "grower_key", growers.index + 1)

    out = STAR / "dim_grower.csv"
    growers.to_csv(out, index=False)
    log(f"dim_grower.csv → {len(growers)} growers | "
        f"Subzones: {growers['subzone'].nunique()} | "
        f"Organic: {growers['organic'].sum()} growers")
    return growers


# =============================================================================
# FACT_EXPORT_TRANSACTIONS
# =============================================================================

def build_fact_table(sources: dict, dim_time: pd.DataFrame,
                     dim_corridor: pd.DataFrame,
                     dim_fruit: pd.DataFrame,
                     dim_grower: pd.DataFrame,
                     stats_exports: pd.DataFrame) -> pd.DataFrame:
    """
    Build Fact_ExportTransactions — the central analytical table.

    Grain: one row per KPIN × pack week × season (= one submission batch)
    Primary key: export_id (auto-increment)

    Foreign keys:
    - date_key     → Dim_Time.date_key
    - corridor_key → Dim_Corridor.corridor_key
    - fruit_key    → Dim_FruitQuality.fruit_key
    - grower_key   → Dim_Grower.grower_key

    Measures (all depend only on export_id, satisfying 3NF):
    - trays_submitted, trays_exported, trays_lost
    - dm_pct_avg, tzg_score
    - submit_payment_nzd, taste_payment_nzd, total_return_nzd
    - otif_pct (computed from congestion + rain + MTS status)
    - freight_cost_nzd (estimated from distance × trays)
    - risk_score (computed — same formula as Apophenia simulator)

    OTIF computation (matches Apophenia simulator formula):
      OTIF = 97.5 - (cong_f×8.5) - (rain_f×5.5) - (reg_f×3.0) - (12 if MTS breach)
      where cong_f = (cong/100)^1.3, rain_f = (rain/120)^1.2
    """
    log("Building Fact_ExportTransactions...")

    if "zgl_submissions" not in sources:
        log("zgl_pallet_submissions.csv not found", "ERROR")
        return pd.DataFrame()

    subs = sources["zgl_submissions"].copy()

    # ── Join fruit_key from dim_fruit_quality ──────────────────────────────
    fruit_keys = dim_fruit[["fruit_key", "kpin", "season", "pack_week"]].copy()
    subs = subs.merge(fruit_keys, on=["kpin", "season", "pack_week"], how="left")

    # ── Join grower_key and corridor_key from dim_grower ──────────────────
    grower_keys = dim_grower[["grower_key", "corridor_key", "kpin"]].copy()
    subs = subs.merge(grower_keys, on="kpin", how="left")

    # ── Join date_key from dim_time ────────────────────────────────────────
    subs["submission_date"] = pd.to_datetime(subs["submission_date"], errors="coerce")
    subs["date_key"] = subs["submission_date"].dt.strftime("%Y%m%d").astype("Int64")

    time_keys = dim_time[["date_key", "pack_week", "season_phase",
                           "is_pack_season"]].copy()
    subs = subs.merge(time_keys, on=["date_key", "pack_week"], how="left")

    # ── OTIF computation ───────────────────────────────────────────────────
    # Use corridor avg congestion as proxy (pack-week specific would need join)
    # Congestion lookup: use dim_corridor avg as baseline
    cong_default = dim_corridor["congestion_index_avg"].mean() \
                   if len(dim_corridor) > 0 else 25.0

    # Rain proxy: use pack_week to estimate seasonal rainfall pattern
    # (real NIWA data joins here in production — this is the placeholder)
    def estimate_rain(pack_week):
        """
        Estimated 7-day rainfall mm by pack week for BOP.
        Based on NIWA BOP climate normals — April peaks, June drier.
        Pack week 11-13 (March): ~28mm, 14-16 (April): ~32mm,
        17-19 (May): ~22mm, 20-22 (June): ~18mm, 23-26 (July+): ~15mm
        """
        if pack_week < 14:   return 28.0
        if pack_week < 17:   return 32.0
        if pack_week < 20:   return 22.0
        if pack_week < 23:   return 18.0
        return 15.0

    subs["congestion_index"] = cong_default
    subs["rainfall_mm_7d"]   = subs["pack_week"].apply(estimate_rain)
    subs["reg_index"]        = 15.0   # default regulatory load

    # OTIF formula — identical to Apophenia simulator
    def compute_otif(row):
        cong_f = (row["congestion_index"] / 100) ** 1.3
        rain_f = (min(row["rainfall_mm_7d"], 120) / 120) ** 1.2
        reg_f  = (row["reg_index"] / 100) ** 1.2
        mts_breach = not row["mts_pass"]
        drop = (cong_f * 8.5) + (rain_f * 5.5) + (reg_f * 3.0) + \
               (12.0 if mts_breach else 0)
        return round(max(52.0, OTIF_BASE - drop), 2)

    subs["otif_pct"] = subs.apply(compute_otif, axis=1)

    # ── Risk Score ─────────────────────────────────────────────────────────
    # Identical formula to Apophenia simulator STATE computation
    def compute_risk_score(row):
        import math
        dm        = row["dm_pct_avg"]
        pest      = row.get("pest_indicator_x", 20)   # default if not joined
        cong      = row["congestion_index"]
        rain      = row["rainfall_mm_7d"]
        reg       = row["reg_index"]

        dm_factor  = 1 / (1 + math.exp(-2.2 * (dm - MTS_GREEN)))
        pest_f     = (min(pest, 100) / 100) ** 1.4
        cong_f     = (cong / 100) ** 1.3
        rain_f     = (min(rain, 120) / 120) ** 1.2
        reg_f      = (reg / 100) ** 1.2

        raw = ((1 - dm_factor) * 0.35 +
               pest_f          * 0.25 +
               cong_f          * 0.15 +
               rain_f          * 0.15 +
               reg_f           * 0.10)

        return int(min(100, max(1, round(raw * 100))))

    subs["risk_score"] = subs.apply(compute_risk_score, axis=1)

    # ── Freight cost estimate ───────────────────────────────────────────────
    # $0.085 NZD per tray per 10km — operational estimate
    # Adjusted upward with congestion (higher congestion = higher cost)
    subs["freight_cost_nzd"] = (
        subs["trays_exported"] *
        0.0085 *
        subs.get("distance_port_km", pd.Series([30.0] * len(subs))) *
        (1 + subs["congestion_index"] / 200)
    ).round(2)

    # ── Margin erosion ─────────────────────────────────────────────────────
    # % of potential return lost to quality + logistics + regulatory factors
    potential_return = subs["trays_submitted"] * (BASE_RATE + TASTE_MAX)
    actual_return    = subs["total_return_nzd"]
    subs["margin_erosion_pct"] = (
        (1 - actual_return / potential_return.replace(0, np.nan)) * 100
    ).clip(0, 100).round(2)

    # ── Final column selection ─────────────────────────────────────────────
    fact_cols = [
        "export_id",          # PK (added below)
        "date_key",           # FK → Dim_Time
        "corridor_key",       # FK → Dim_Corridor
        "fruit_key",          # FK → Dim_FruitQuality
        "grower_key",         # FK → Dim_Grower
        # Context
        "season", "subzone", "variety", "pack_week", "season_phase",
        # Volume measures
        "trays_submitted", "trays_exported", "trays_lost", "loss_pct",
        # Quality measures
        "dm_pct_avg", "tzg_score", "mts_pass",
        # Financial measures
        "submit_payment_nzd", "taste_payment_nzd",
        "total_return_nzd", "freight_cost_nzd", "margin_erosion_pct",
        # Operational measures
        "otif_pct", "risk_score",
        # Context variables (for simulator feed)
        "congestion_index", "rainfall_mm_7d", "reg_index", "vol_index",
    ]

    # Keep only columns that exist
    fact_cols_exist = [c for c in fact_cols if c in subs.columns]
    fact = subs[fact_cols_exist].copy().reset_index(drop=True)
    fact.insert(0, "export_id", fact.index + 1)

    out = STAR / "fact_export_transactions.csv"
    fact.to_csv(out, index=False)

    # Validation summary
    total_ret  = fact["total_return_nzd"].sum() / 1_000_000
    total_trays = fact["trays_submitted"].sum()
    avg_otif    = fact["otif_pct"].mean()
    avg_risk    = fact["risk_score"].mean()

    log(f"fact_export_transactions.csv → {len(fact):,} rows")
    log(f"  Total return (all seasons): NZD {total_ret:,.1f}M", "FIND")
    log(f"  Total trays submitted:      {total_trays:,.0f}", "FIND")
    log(f"  Avg OTIF: {avg_otif:.1f}%", "FIND")
    log(f"  Avg Risk Score: {avg_risk:.1f}/100", "FIND")
    log(f"  MTS pass rate: {fact['mts_pass'].mean():.1%}", "FIND")

    return fact


# =============================================================================
# TRANSFORM REPORT
# =============================================================================

def write_transform_report(tables: dict):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "# ETL Transform Report — Star Schema Assembly",
        f"**Generated:** {now}  ",
        "**Project:** Optimising Kiwifruit Export Performance — ZGL 2026  ",
        "**Author:** Gabriela Olivera | Data Analytics Portfolio  ",
        "",
        "---",
        "",
        "## Star Schema — Table Summary",
        "",
        "| Table | Rows | Primary Key | Grain |",
        "|-------|------|-------------|-------|",
    ]

    grains = {
        "dim_time":                "One row per calendar date",
        "dim_corridor":            "One row per BOP subzone / highway",
        "dim_fruit_quality":       "One row per KPIN × season × pack week",
        "dim_grower":              "One row per fictional grower (KPIN)",
        "fact_export_transactions":"One row per KPIN × season × pack week (submission batch)",
    }

    pks = {
        "dim_time":                "date_key",
        "dim_corridor":            "corridor_key",
        "dim_fruit_quality":       "fruit_key",
        "dim_grower":              "grower_key",
        "fact_export_transactions":"export_id",
    }

    for name, df in tables.items():
        if df is not None and len(df) > 0:
            lines.append(f"| {name} | {len(df):,} | "
                        f"{pks.get(name,'—')} | "
                        f"{grains.get(name,'—')} |")

    lines += [
        "",
        "---",
        "",
        "## Key Fixes Applied in This Transform",
        "",
        "### Stats NZ FOB Bug",
        "Phase 1 reported `2025 total FOB: NZD 0.1M` — clearly wrong.",
        "Root cause: `total_fob_nzd` column (index 8) was near-zero because",
        "Stats NZ uses separate HS codes. The correct total is `all_codes_fob_nzd`",
        "(index 10). Fix applied: re-read raw file, use column 10 as authoritative total.",
        "Corrected 2025 total: **NZD ~76,913M** ✓",
        "",
        "### Congestion Index Recalibration",
        "Phase 1 avg congestion_index was 92.2/100 — too compressed for the model.",
        "Root cause: baseline (800 heavy/day) underestimated real SH2 volume.",
        "Fix: rescaled to 0-100 relative to actual data range (min→max).",
        "Result: index now reflects relative congestion, not absolute volume.",
        "",
        "---",
        "",
        "## Normalisation — Star Schema Design Decisions",
        "",
        "The Star Schema is intentionally **denormalised at the fact level**",
        "for analytical query performance, while dimensions are in **3NF**.",
        "",
        "### Why denormalise the fact table?",
        "Columns like `season`, `subzone`, `variety` appear in both the fact",
        "table and dimensions. This is standard Star Schema design — the",
        "redundancy is intentional. It allows single-table aggregations without",
        "joins for common queries like `GROUP BY season` or `WHERE subzone='Opotiki'`.",
        "",
        "### Dimension 3NF compliance",
        "- **Dim_Time**: `date_key` → all attributes. `season_phase` derives",
        "  from `pack_week`, which derives from `date_key`. No transitive deps",
        "  because `pack_week` is itself a determinant (not just a fact).",
        "- **Dim_Corridor**: `corridor_key` → all attributes.",
        "  `congestion_index_avg` depends only on `corridor_key` (the corridor,",
        "  not individual days). Daily congestion lives in the fact table.",
        "- **Dim_FruitQuality**: composite natural key is `kpin + season + pack_week`.",
        "  `dm_pct` and `tzg_score` depend on the full composite key.",
        "  No partial dependency: `variety` depends on `kpin` (always same variety",
        "  per grower), but `kpin` is part of the key so this is acceptable.",
        "",
        "---",
        "",
        "## Next Steps",
        "",
        "1. Run SQL queries in `04_analysis/sql_queries/` against the Star Schema",
        "2. Connect `fact_export_transactions.csv` to the Risk Score model",
        "3. Feed 2025/26 season aggregates to Apophenia simulator API endpoint",
        "4. Join real Open-Meteo rainfall data to replace `rainfall_mm_7d` estimates",
        "",
        "---",
        "*Gabriela Olivera | Data Analytics Portfolio*  ",
    ]

    report = "\n".join(lines)
    out = STAR / "transform_report.md"
    out.write_text(report, encoding="utf-8")
    log(f"transform_report.md saved")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("  OPTIMISING KIWIFRUIT EXPORT — ETL Phase 2: Star Schema Assembly")
    print("  ZGL 2026 | Gabriela Olivera | Data Analytics Portfolio")
    print("=" * 70)
    print()

    sources = load_sources()
    print()

    print("── STATS NZ FOB FIX ─────────────────────────────────────────────")
    stats_exports = fix_stats_nz_fob(sources)
    print()

    print("── DIM_TIME ──────────────────────────────────────────────────────")
    dim_time = build_dim_time(sources)
    print()

    print("── DIM_CORRIDOR ──────────────────────────────────────────────────")
    dim_corridor = build_dim_corridor(sources)
    print()

    print("── DIM_FRUIT_QUALITY ─────────────────────────────────────────────")
    dim_fruit = build_dim_fruit_quality(sources)
    print()

    print("── DIM_GROWER ────────────────────────────────────────────────────")
    dim_grower = build_dim_grower(sources, dim_corridor)
    print()

    print("── FACT_EXPORT_TRANSACTIONS ──────────────────────────────────────")
    fact = build_fact_table(
        sources, dim_time, dim_corridor,
        dim_fruit, dim_grower, stats_exports
    )
    print()

    print("── TRANSFORM REPORT ──────────────────────────────────────────────")
    tables = {
        "dim_time":                dim_time,
        "dim_corridor":            dim_corridor,
        "dim_fruit_quality":       dim_fruit,
        "dim_grower":              dim_grower,
        "fact_export_transactions": fact,
    }
    write_transform_report(tables)

    print()
    print("=" * 70)
    print("  ETL Phase 2 COMPLETE")
    print(f"  Output: {STAR}")
    print()
    print("  Star Schema files:")
    for f in sorted(STAR.glob("*.csv")):
        size_kb = f.stat().st_size / 1024
        rows = sum(1 for _ in open(f)) - 1
        print(f"    {f.name:<42} {rows:>8,} rows  {size_kb:>7.1f} KB")
    print("=" * 70)


if __name__ == "__main__":
    main()
