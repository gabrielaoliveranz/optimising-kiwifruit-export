"""
=============================================================================
OPTIMISING KIWIFRUIT EXPORT PERFORMANCE — ZGL 2026
Script: generate_zgl_edi_simulation.py  [v2 — recalibrated]
Stage: ETL Phase 0 — Synthetic ZGL EDI Data Generation
Author: Gabriela Olivera | Data Analytics Portfolio
=============================================================================

CALIBRATION CHANGES v1 → v2:
  1. MTS fail rate: 33% → target 6-12%
     - Reduced dm_season_std from 0.85-1.10 → 0.45-0.65
     - Increased dm_season_avg for difficult years to stay above MTS floor
     - Rationale: ZGL publicly reports ~5-10% reject rates at maturity gate

  2. Total returns: NZD 4-6M → target ~280-320M per season
     - Recalibrated yield_trays_per_ha to reflect real BOP productivity
     - Increased weekly_yield_frac range and harvest window coverage
     - Added trays_per_submission scalar to reach 120M tray season baseline

  3. Subzone DM variance now differentiated:
     - Katikati: lower std (more consistent, sheltered microclimate)
     - Opotiki: higher std (exposed, variable — greater PSA/weather risk)
     - This makes subzone comparison analytically meaningful

SEASON LOGIC:
  Each season has a base DM average and stress modifier.
  Within a season, each grower has a persistent quality effect (±0.25%)
  and each subzone has a mean offset and its own std modifier.
  This produces realistic within-season AND between-season variation.

OUTPUTS (01_data_raw/zgl_edi_simulation/):
  zgl_grower_register.csv      445 growers, 5 BOP subzones
  zgl_maturity_readings.csv    28,480 DM readings (4 seasons × 445 × 16 weeks)
  zgl_pallet_submissions.csv   ~180K-220K submission records
  zgl_fruit_loss_records.csv   Loss events with ZGL CCP cause codes
=============================================================================
"""

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

# =============================================================================
# CONFIGURATION
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR   = PROJECT_ROOT / "01_data_raw" / "zgl_edi_simulation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RANDOM_SEED = 42
rng = np.random.default_rng(RANDOM_SEED)

# =============================================================================
# ZGL 2026 CALIBRATION CONSTANTS
# =============================================================================

MTS = {
    "Green":          15.5,
    "SunGold":        16.1,
    "SunGold_sz39":   16.6,
    "SweetGreen":     16.2,
    "OrganicGreen":   15.5,
    "OrganicSunGold": 16.1,
    "RubyRed":        17.2,
}

SUBMIT_RATES = {
    "SunGold":        3.60,
    "OrganicSunGold": 4.25,
    "Green":          2.75,
    "Green_sz42":     2.55,
    "OrganicGreen":   3.30,
    "SweetGreen":     2.95,
    "RubyRed":        5.10,
}

TASTE_MAX_BONUS  = 0.95          # NZD/tray at max TZG
TOTAL_TRAYS_REF  = 120_000_000   # 120M trays — BOP season baseline (Stats NZ)
SEASON_WEEKS_REF = 12            # active harvest weeks (weeks 13-24 peak)

# =============================================================================
# SUBZONE DEFINITIONS — recalibrated variance
# =============================================================================

SUBZONES = {
    "Te Puke": {
        "base_risk_weight":          0.35,
        "psa_incidence_historical":  0.18,
        "dm_mean_offset":            0.00,   # reference subzone
        "dm_std_modifier":           1.00,   # baseline std multiplier
        "distance_port_km":          28,
        "kpin_count":                180,
        "variety_mix": {
            "SunGold": 0.62, "Green": 0.28,
            "OrganicSunGold": 0.05, "OrganicGreen": 0.03,
            "SweetGreen": 0.01, "RubyRed": 0.01
        }
    },
    "Katikati": {
        "base_risk_weight":          0.20,
        "psa_incidence_historical":  0.09,   # historically lower PSA
        "dm_mean_offset":           +0.18,   # higher DM average
        "dm_std_modifier":           0.80,   # MORE consistent — key analytical finding
        "distance_port_km":          52,
        "kpin_count":                95,
        "variety_mix": {
            "SunGold": 0.55, "Green": 0.35,
            "OrganicSunGold": 0.04, "OrganicGreen": 0.04,
            "SweetGreen": 0.01, "RubyRed": 0.01
        }
    },
    "Tauranga": {
        "base_risk_weight":          0.18,
        "psa_incidence_historical":  0.12,
        "dm_mean_offset":           -0.05,
        "dm_std_modifier":           0.90,
        "distance_port_km":          12,
        "kpin_count":                75,
        "variety_mix": {
            "SunGold": 0.58, "Green": 0.30,
            "OrganicSunGold": 0.06, "OrganicGreen": 0.04,
            "SweetGreen": 0.01, "RubyRed": 0.01
        }
    },
    "Pongakawa": {
        "base_risk_weight":          0.15,
        "psa_incidence_historical":  0.14,
        "dm_mean_offset":           +0.10,
        "dm_std_modifier":           1.05,
        "distance_port_km":          35,
        "kpin_count":                55,
        "variety_mix": {
            "SunGold": 0.60, "Green": 0.30,
            "OrganicSunGold": 0.03, "OrganicGreen": 0.05,
            "SweetGreen": 0.01, "RubyRed": 0.01
        }
    },
    "Opotiki": {
        "base_risk_weight":          0.12,
        "psa_incidence_historical":  0.22,   # highest PSA — geographically exposed
        "dm_mean_offset":           -0.22,   # lower DM due to climate/PSA exposure
        "dm_std_modifier":           1.35,   # MOST variable — key finding for research Q4
        "distance_port_km":          97,     # furthest from port
        "kpin_count":                40,
        "variety_mix": {
            "SunGold": 0.45, "Green": 0.42,
            "OrganicSunGold": 0.03, "OrganicGreen": 0.06,
            "SweetGreen": 0.02, "RubyRed": 0.02
        }
    },
}

# =============================================================================
# SEASON DEFINITIONS — recalibrated for realistic MTS fail rates
# =============================================================================

SEASONS = {
    "2022/23": {
        "start_date":        date(2023, 3, 13),
        "dm_season_avg":     16.55,   # moderate-good year
        "dm_season_std":     0.48,    # REDUCED from 0.85 — target 8% MTS fail
        "climate_stress":    0.10,    # La Niña tail — some wet weather
        "pest_pressure_base": 22,
        "vol_index":         95,
        "note": "Moderate season. La Niña residual. Some early-season rain."
    },
    "2023/24": {
        "start_date":        date(2024, 3, 11),
        "dm_season_avg":     16.85,   # strong DM year — El Niño
        "dm_season_std":     0.42,    # tight distribution — consistent conditions
        "climate_stress":    0.04,    # El Niño — drier, warmer, ideal DM development
        "pest_pressure_base": 14,
        "vol_index":         102,
        "note": "Best DM year in dataset. El Niño conditions. Low pest pressure."
    },
    "2024/25": {
        "start_date":        date(2025, 3, 10),
        "dm_season_avg":     16.20,   # CHALLENGING year — late rains, Cyclone residual
        "dm_season_std":     0.65,    # wider spread — most variable season
        "climate_stress":    0.24,    # highest stress — Cyclone Gabrielle aftermath
        "pest_pressure_base": 33,
        "vol_index":         97,
        "note": "Most difficult season. Cyclone residual. High pest. Widest DM variance."
    },
    "2025/26": {
        "start_date":        date(2026, 3, 9),
        "dm_season_avg":     16.65,
        "dm_season_std":     0.45,
        "climate_stress":    0.08,
        "pest_pressure_base": 19,
        "vol_index":         104,
        "note": "Recovery season. Strong SunGold. Low pest. Reference for 2026 analysis."
    },
}

PACK_WEEKS = list(range(11, 27))   # weeks 11–26

# Harvest window per variety (which pack weeks are active)
HARVEST_WINDOW = {
    "SunGold":        list(range(13, 23)),
    "OrganicSunGold": list(range(14, 22)),
    "Green":          list(range(11, 21)),
    "OrganicGreen":   list(range(11, 20)),
    "SweetGreen":     list(range(12, 22)),
    "RubyRed":        list(range(15, 24)),
}

# Yield calibrated to approach 120M tray season baseline across 445 growers
# Real BOP: ~12,000 ha × ~10,000 trays/ha season = 120M trays
# Per grower per active week: depends on ha and weekly rhythm
YIELD_TRAYS_PER_HA_WEEK = {
    "SunGold":        780,    # high-yielding, long season
    "OrganicSunGold": 650,
    "Green":          720,
    "OrganicGreen":   600,
    "SweetGreen":     700,
    "RubyRed":        480,    # lower volume variety
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def pack_week_to_date(season_start: date, pack_week: int) -> date:
    offset_weeks = pack_week - 11
    return season_start + timedelta(weeks=offset_weeks)


def dm_seasonal_arc(pack_week: int, season_avg: float,
                    season_std: float, climate_stress: float,
                    subzone_std_modifier: float) -> float:
    """
    Generate a single DM% reading for a given pack week.

    Seasonal arc shape:
    - Weeks 11-12: slightly below season avg (immature fruit, early harvest)
    - Weeks 13-16: peak window — highest DM, optimal maturity
    - Weeks 17-21: gradual decline as main crop completes
    - Weeks 22-26: late-season, post-optimal, cold storage impact

    Climate stress compresses the peak and adds downward pressure on mean.
    Subzone std modifier differentiates how consistent each area is.
    """
    pos = (pack_week - 11) / 15.0  # 0.0 → 1.0

    # Seasonal arc: Gaussian peak around week 15 (pos ≈ 0.27)
    arc = 1.0 + 0.035 * np.exp(-7.0 * (pos - 0.27) ** 2) - 0.025 * pos

    # Climate stress: pulls mean down, adds noise in stressed weeks
    stress_pull  = climate_stress * float(rng.beta(2, 5)) * 0.6
    stress_noise = climate_stress * 0.20

    # Effective std combines season std, subzone modifier, and stress
    effective_std = (season_std * subzone_std_modifier) + stress_noise

    dm = rng.normal(season_avg * arc - stress_pull, effective_std)
    return round(float(np.clip(dm, 12.0, 22.0)), 2)


def compute_tzg(dm: float, variety: str) -> float:
    """
    Compute Taste Zespri Grade score (0.0 → 1.0 Green, 0.0 → 0.86 SunGold).
    Returns 0.0 if dm < MTS — no taste payment on failed fruit.
    Calibrated to ZGL Grower Payments Booklet 2026.
    """
    if "SunGold" in variety or "Gold" in variety:
        mts_key, max_tzg = "SunGold", 0.86
    elif "Ruby" in variety:
        mts_key, max_tzg = "RubyRed", 1.0
    else:
        mts_key, max_tzg = "Green", 1.0

    mts = MTS.get(mts_key, 15.5)
    if dm < mts:
        return 0.0

    return round(float(np.clip((dm - mts) / (20.0 - mts) * max_tzg, 0, max_tzg)), 3)


def tzg_to_grade(tzg: float, mts_pass: bool) -> str:
    if not mts_pass:
        return "F"
    if tzg >= 0.80: return "A+"
    if tzg >= 0.60: return "A"
    if tzg >= 0.40: return "B"
    if tzg >= 0.20: return "C"
    return "D"


# =============================================================================
# TABLE 1 — GROWER REGISTER
# =============================================================================

def generate_grower_register() -> pd.DataFrame:
    """
    445 fictional growers across 5 BOP subzones.
    KPINs: 100000–100444 (fictional, sequential).
    Each grower assigned primary variety weighted by subzone variety mix.
    """
    rows = []
    kpin = 100000

    for subzone, cfg in SUBZONES.items():
        varieties = list(cfg["variety_mix"].keys())
        probs     = list(cfg["variety_mix"].values())

        for _ in range(cfg["kpin_count"]):
            variety = str(rng.choice(varieties, p=probs))
            rows.append({
                "kpin":                     kpin,
                "subzone":                  subzone,
                "primary_variety":          variety,
                "organic":                  "Organic" in variety,
                "orchard_ha":               round(float(rng.uniform(2.0, 22.0)), 1),
                "distance_port_km":         cfg["distance_port_km"],
                "psa_history":              bool(rng.random() < cfg["psa_incidence_historical"]),
                "base_risk_weight":         cfg["base_risk_weight"],
                "dm_std_modifier":          cfg["dm_std_modifier"],
            })
            kpin += 1

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "zgl_grower_register.csv", index=False)
    print(f"  ✅ zgl_grower_register.csv — {len(df)} growers | "
          f"PSA history: {df['psa_history'].mean():.1%} of growers")
    return df


# =============================================================================
# TABLE 2 — MATURITY READINGS
# =============================================================================

def generate_maturity_readings(growers: pd.DataFrame) -> pd.DataFrame:
    """
    One DM% reading per KPIN × pack week × season = 28,480 records.

    Each reading = pool composite of 90 fruit (ZGL QM 2026 spec).

    Grower persistent effect: drawn once per grower, applied every season.
    This models real-world grower management quality — some consistently
    produce higher DM, others consistently lower.

    PSA history: reduces DM by 0.15–0.45% in affected weeks (probabilistic).
    """
    # Persistent grower effect — drawn once, stable across seasons
    grower_effects = {
        row["kpin"]: float(rng.normal(0, 0.25))
        for _, row in growers.iterrows()
    }

    rows = []

    for season, s in SEASONS.items():
        for _, g in growers.iterrows():
            kpin    = g["kpin"]
            subzone = g["subzone"]
            variety = g["primary_variety"]
            sz_cfg  = SUBZONES[subzone]

            psa_penalty = 0.0
            if g["psa_history"] and rng.random() < 0.30:
                psa_penalty = float(rng.uniform(0.15, 0.45))

            for pw in PACK_WEEKS:
                dm_raw = dm_seasonal_arc(
                    pw,
                    s["dm_season_avg"] + sz_cfg["dm_mean_offset"],
                    s["dm_season_std"],
                    s["climate_stress"],
                    sz_cfg["dm_std_modifier"]
                )
                dm = round(float(np.clip(
                    dm_raw + grower_effects[kpin] - psa_penalty, 12.0, 22.0
                )), 2)

                if "SunGold" in variety or "Gold" in variety:
                    mts_val = MTS["SunGold"]
                elif "Ruby" in variety:
                    mts_val = MTS["RubyRed"]
                else:
                    mts_val = MTS["Green"]

                mts_pass = dm >= mts_val
                tzg      = compute_tzg(dm, variety)

                rows.append({
                    "season":               season,
                    "kpin":                 kpin,
                    "subzone":              subzone,
                    "variety":              variety,
                    "pack_week":            pw,
                    "reading_date":         pack_week_to_date(s["start_date"], pw),
                    "dm_pct":               dm,
                    "mts_threshold":        mts_val,
                    "mts_pass":             mts_pass,
                    "tzg_score":            tzg,
                    "tzg_grade":            tzg_to_grade(tzg, mts_pass),
                    "sample_size":          90,
                    "psa_penalty_applied":  psa_penalty > 0,
                    "grower_effect":        round(grower_effects[kpin], 3),
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "zgl_maturity_readings.csv", index=False)
    fail_rate = (~df["mts_pass"]).mean()
    print(f"  ✅ zgl_maturity_readings.csv — {len(df):,} readings | "
          f"MTS fail rate: {fail_rate:.1%} (target: 6-12%)")
    return df


# =============================================================================
# TABLE 3 — PALLET SUBMISSIONS
# =============================================================================

def generate_pallet_submissions(growers: pd.DataFrame,
                                maturity: pd.DataFrame) -> pd.DataFrame:
    """
    Submission records — one per KPIN × active pack week × season.

    Volume calibration:
    445 growers × avg 8.5 ha × ~780 trays/ha/active week × 10 active weeks
    ≈ 2.96M trays per season simulated.

    The simulator uses vol_index (50-150) to scale this against the
    120M tray reference — so the absolute tray count here represents
    a sample of the BOP grower pool, not the full population.
    Financial outputs are scaled accordingly in 03_transform.py.

    Payment logic (ZGL Grower Payments Booklet 2026):
    - Submit payment: always calculated on trays_submitted
    - MTS fail: submit payment reversed (total_return = 0)
    - Taste payment: TZG × $0.95 × trays_exported (only if MTS pass)
    """
    SIZE_CODES  = ["18", "22", "25", "30", "33", "36", "39", "42"]
    SIZE_PROBS  = [0.05, 0.12, 0.25, 0.22, 0.16, 0.10, 0.07, 0.03]

    rows = []

    for season, s in SEASONS.items():
        season_mat = maturity[maturity["season"] == season].copy()
        mat_idx    = season_mat.set_index(["kpin", "pack_week"])

        for _, g in growers.iterrows():
            kpin    = g["kpin"]
            variety = g["primary_variety"]
            ha      = g["orchard_ha"]
            subzone = g["subzone"]
            hw      = HARVEST_WINDOW.get(variety, list(range(11, 22)))
            y_week  = YIELD_TRAYS_PER_HA_WEEK.get(variety, 700)

            for pw in hw:
                key = (kpin, pw)
                if key not in mat_idx.index:
                    continue

                mat_row  = mat_idx.loc[key]
                dm       = float(mat_row["dm_pct"])
                mts_pass = bool(mat_row["mts_pass"])
                tzg      = float(mat_row["tzg_score"])

                # Weekly trays — drawn from yield distribution
                weekly_frac  = float(rng.uniform(0.10, 0.18))
                trays_sub    = max(50, int(ha * y_week * weekly_frac
                                          * (s["vol_index"] / 100)))

                # Fruit loss
                base_loss    = 0.030
                dm_loss      = max(0.0, (15.5 - dm) * 0.025) if not mts_pass else 0.0
                pest_loss    = float(rng.beta(1.5, 30)) * 0.08
                loss_pct     = float(np.clip(
                    base_loss + dm_loss + pest_loss + rng.normal(0, 0.004),
                    0.005, 0.42
                ))
                trays_lost   = int(trays_sub * loss_pct)
                trays_exp    = trays_sub - trays_lost

                # Size code
                size = str(rng.choice(SIZE_CODES, p=SIZE_PROBS))

                # Payments
                rate = SUBMIT_RATES["Green_sz42"] if (variety == "Green" and size == "42") \
                       else SUBMIT_RATES.get(variety, SUBMIT_RATES["Green"])

                submit_pay = round(rate * trays_sub, 2)

                if mts_pass:
                    taste_pay   = round(tzg * TASTE_MAX_BONUS * trays_exp, 2)
                    total_ret   = round(submit_pay + taste_pay, 2)
                    otif_impact = 0.0
                else:
                    taste_pay   = 0.0
                    total_ret   = 0.0    # submit reversed on MTS breach
                    otif_impact = 12.0   # OTIF penalty from model

                rows.append({
                    "season":             season,
                    "kpin":               kpin,
                    "subzone":            subzone,
                    "variety":            variety,
                    "pack_week":          pw,
                    "submission_date":    pack_week_to_date(s["start_date"], pw),
                    "size_code":          size,
                    "trays_submitted":    trays_sub,
                    "trays_exported":     trays_exp,
                    "trays_lost":         trays_lost,
                    "loss_pct":           round(loss_pct * 100, 3),
                    "dm_pct_avg":         dm,
                    "tzg_score":          tzg,
                    "mts_pass":           mts_pass,
                    "submit_payment_nzd": submit_pay,
                    "taste_payment_nzd":  taste_pay,
                    "total_return_nzd":   total_ret,
                    "otif_impact":        otif_impact,
                    "vol_index":          s["vol_index"],
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "zgl_pallet_submissions.csv", index=False)

    total_trays = df["trays_submitted"].sum()
    total_ret_m = df["total_return_nzd"].sum() / 1_000_000
    mts_fail    = (~df["mts_pass"]).mean()
    print(f"  ✅ zgl_pallet_submissions.csv — {len(df):,} submissions | "
          f"{total_trays:,.0f} trays | "
          f"NZD {total_ret_m:,.1f}M | MTS fail: {mts_fail:.1%}")
    return df


# =============================================================================
# TABLE 4 — FRUIT LOSS RECORDS
# =============================================================================

def generate_fruit_loss_records(submissions: pd.DataFrame) -> pd.DataFrame:
    """
    Individual loss event records for batches where trays_lost > 5.

    CCP cause codes from ZGL Quality Manual 2026:
    - PEST_CCP2/2A: pest index 0-40% (increased sampling)
    - PEST_CCP3:    pest index >40% Japan, >60% EU/US (market block)
    - MTS_BREACH:   DM below threshold — clearance rejection
    - COOLSTORE_CCP5: temperature excursion in coolstore
    - LOADOUT_CCP6: loadout verification failure
    - MECHANICAL:   packhouse or transit physical damage
    - WEATHER:      harvest-day rain/wind event
    """
    CAUSE_WEIGHTS = {
        "MTS_BREACH":     0.22,
        "MECHANICAL":     0.20,
        "PEST_CCP2":      0.18,
        "COOLSTORE_CCP5": 0.15,
        "WEATHER":        0.12,
        "PEST_CCP3":      0.08,
        "LOADOUT_CCP6":   0.03,
        "OTHER":          0.02,
    }
    causes = list(CAUSE_WEIGHTS.keys())
    probs  = list(CAUSE_WEIGHTS.values())

    loss_events = submissions[submissions["trays_lost"] > 5].copy()

    rows = []
    for _, row in loss_events.iterrows():
        # MTS breach always gets its cause — not random
        if not row["mts_pass"]:
            cause = "MTS_BREACH"
        else:
            cause = str(rng.choice(causes, p=probs))
            # Avoid MTS_BREACH for passed fruit
            if cause == "MTS_BREACH":
                cause = "MECHANICAL"

        ccp_triggered    = cause in ["PEST_CCP2", "PEST_CCP3",
                                     "COOLSTORE_CCP5", "LOADOUT_CCP6"]
        market_block_risk = cause == "PEST_CCP3"

        fin_impact = (row["submit_payment_nzd"]
                      if not row["mts_pass"]
                      else row["trays_lost"] * 3.20)

        rows.append({
            "season":               row["season"],
            "kpin":                 row["kpin"],
            "subzone":              row["subzone"],
            "variety":              row["variety"],
            "pack_week":            row["pack_week"],
            "event_date":           row["submission_date"],
            "trays_lost":           row["trays_lost"],
            "loss_pct":             row["loss_pct"],
            "primary_cause":        cause,
            "ccp_triggered":        ccp_triggered,
            "market_block_risk":    market_block_risk,
            "dm_at_event":          row["dm_pct_avg"],
            "mts_pass":             row["mts_pass"],
            "financial_impact_nzd": round(fin_impact, 2),
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "zgl_fruit_loss_records.csv", index=False)
    mts_count = (df["primary_cause"] == "MTS_BREACH").sum()
    print(f"  ✅ zgl_fruit_loss_records.csv — {len(df):,} loss events | "
          f"MTS breach: {mts_count:,} | "
          f"Market block risk: {df['market_block_risk'].sum():,}")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("  ZGL EDI SIMULATION — Data Generation  [v2 — recalibrated]")
    print("  ZGL Quality Manual 2026 | Grower Payments Booklet 2026")
    print("  Seasons: 2022/23 → 2025/26 | Pack weeks 11–26")
    print("  Author: Gabriela Olivera | Data Analytics Portfolio")
    print("=" * 70)
    print()

    print("── TABLE 1: Grower Register ─────────────────────────────────────")
    growers = generate_grower_register()

    print("\n── TABLE 2: Maturity Readings ───────────────────────────────────")
    maturity = generate_maturity_readings(growers)

    print("\n── TABLE 3: Pallet Submissions ──────────────────────────────────")
    submissions = generate_pallet_submissions(growers, maturity)

    print("\n── TABLE 4: Fruit Loss Records ──────────────────────────────────")
    losses = generate_fruit_loss_records(submissions)

    # ── VALIDATION SUMMARY ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  VALIDATION SUMMARY")
    print("=" * 70)

    print("\n  MTS Compliance by Season (target: 88-94%):")
    mts_season = maturity.groupby("season")["mts_pass"].mean() * 100
    for s, pct in mts_season.items():
        flag = "✅" if 85 <= pct <= 97 else "⚠️ "
        print(f"    {flag} {s}: {pct:.1f}% pass")

    print("\n  DM% by Subzone — mean ± std (all seasons):")
    dm_sz = maturity.groupby("subzone")["dm_pct"].agg(["mean", "std"])
    dm_sz = dm_sz.sort_values("std")
    for sz, row in dm_sz.iterrows():
        bar = "▓" * int(row["std"] * 20)
        print(f"    {sz:<12}  mean={row['mean']:.2f}%  std={row['std']:.2f}%  {bar}")
    print("    (lower std = more consistent = lower risk)")

    print("\n  Total Returns by Season (NZD M):")
    ret_season = submissions.groupby("season")["total_return_nzd"].sum() / 1_000_000
    trays_season = submissions.groupby("season")["trays_submitted"].sum()
    for s in ret_season.index:
        print(f"    {s}: NZD {ret_season[s]:,.1f}M  |  "
              f"{trays_season[s]:,.0f} trays submitted")

    print("\n  Loss Cause Distribution:")
    cause_dist = losses["primary_cause"].value_counts()
    total_loss = len(losses)
    for cause, count in cause_dist.items():
        pct = count / total_loss * 100
        print(f"    {cause:<22} {count:>5,}  ({pct:.1f}%)")

    print("\n  Files generated:")
    for f in sorted(OUTPUT_DIR.glob("*.csv")):
        size_kb = f.stat().st_size / 1024
        rows = sum(1 for _ in open(f)) - 1
        print(f"    {f.name:<42} {rows:>8,} rows  {size_kb:>7.1f} KB")

    print()
    print("  ⚠️  All KPINs are fictional. No real Zespri grower data used.")
    print("  ⚠️  Distributions calibrated from ZGL 2026 public standards only.")
    print("=" * 70)


if __name__ == "__main__":
    main()
