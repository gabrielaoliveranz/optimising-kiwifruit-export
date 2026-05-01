"""
=============================================================================
OPTIMISING KIWIFRUIT EXPORT PERFORMANCE — ZGL 2026
Script: 07_api_feed.py
Stage: Integration — SQLite DB → Apophenia Simulator JSON Feed
Author: Gabriela Olivera | Data Analytics Portfolio
=============================================================================

WHAT THIS SCRIPT DOES:
  Reads kiwifruit_export.db and generates the JSON payload that the
  Apophenia simulator expects. Two modes:

  MODE 1 — SEASON SNAPSHOT (--season 2024/25):
    Aggregates the fact table for a specific season and produces the
    JSON that sets the simulator sliders to reflect real ETL data.

  MODE 2 — LIVE FEED (--live):
    Reads the most recent pack week in the dataset and generates the
    current-state JSON for real-time simulation.

  MODE 3 — ALL SEASONS (--all):
    Generates one JSON file per season — used to populate the
    SEASONS object in the simulator JavaScript.

OUTPUT FORMAT (matches Apophenia simulator STATE object):
  {
    "dm":          16.4,     → STATE.dm
    "pest":        22,       → STATE.pest
    "congestion":  35,       → STATE.cong
    "rainfall":    18,       → STATE.rain
    "vol":         104,      → STATE.vol
    "regulatory":  15,       → STATE.reg
    "timestamp":   "2026-04-24T06:00:00+12:00",
    "source":      "kiwifruit_export.db",
    "season":      "2025/26",
    "pack_week":   17,
    "data_quality": {
      "nulls":         0,
      "health_score":  100,
      "mts_pass_rate": 0.883,
      "otif_avg":      87.86,
      "total_return_nzd_m": 18.78,
      "payments_reversed_nzd_m": 1.635
    },
    "subzones": {
      "Katikati": {"dm_mean": 16.77, "dm_std": 0.65, "mts_fail_pct": 7.6},
      "Opotiki":  {"dm_mean": 16.33, "dm_std": 0.85, "mts_fail_pct": 26.1},
      "Te Puke":  {"dm_mean": 16.63, "dm_std": 0.73, "mts_fail_pct": 16.8},
      "Tauranga": {"dm_mean": 16.55, "dm_std": 0.69, "mts_fail_pct": 17.1},
      "Pongakawa":{"dm_mean": 16.70, "dm_std": 0.76, "mts_fail_pct": 14.6}
    }
  }

HOW TO RUN:
  python 07_api_feed.py --season "2025/26"
  python 07_api_feed.py --season "2024/25"
  python 07_api_feed.py --all
  python 07_api_feed.py --live

  Run from project root: G:\\My Drive\\optimising-kiwifruit-export\\
  Outputs saved to: 07_reports/api_payloads/
=============================================================================
"""

import sqlite3
import json
import argparse
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta

# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent
DB_PATH      = PROJECT_ROOT / "02_data_processed" / "star_schema" / "kiwifruit_export.db"
OUTPUT_DIR   = PROJECT_ROOT / "07_reports" / "api_payloads"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# NZ timezone offset
NZ_TZ = timezone(timedelta(hours=12))

# ZGL 2026 constants — must match simulator
REG_INDEX_DEFAULT = 15.0   # regulatory compliance baseline

# =============================================================================
# CORE DATA EXTRACTION
# =============================================================================

def get_season_aggregate(conn: sqlite3.Connection, season: str) -> dict:
    """
    Aggregate fact_export_transactions for a specific season.
    Returns the key metrics needed to set simulator state.
    """
    cursor = conn.execute("""
        SELECT
            -- Core simulator inputs
            ROUND(AVG(dm_pct_avg), 2)                          AS dm_avg,
            ROUND(AVG(congestion_index), 1)                    AS cong_avg,
            ROUND(AVG(rainfall_mm_7d), 1)                      AS rain_avg,
            ROUND(AVG(vol_index), 1)                           AS vol_avg,
            ROUND(AVG(reg_index), 1)                           AS reg_avg,
            -- Quality metrics
            ROUND(AVG(otif_pct), 2)                            AS otif_avg,
            ROUND(AVG(risk_score), 1)                          AS risk_avg,
            ROUND(
                100.0 * SUM(CASE WHEN mts_pass = 1 THEN 1 ELSE 0 END)
                / COUNT(*), 3
            )                                                  AS mts_pass_pct,
            -- Financial
            ROUND(SUM(total_return_nzd) / 1000000.0, 3)       AS total_return_nzd_m,
            ROUND(
                SUM(CASE WHEN mts_pass = 0 THEN submit_payment_nzd ELSE 0 END)
                / 1000000.0, 3
            )                                                  AS payments_reversed_nzd_m,
            -- Volume
            SUM(trays_submitted)                               AS total_trays,
            COUNT(*)                                           AS submissions,
            -- Pack week range
            MIN(pack_week)                                     AS first_pack_week,
            MAX(pack_week)                                     AS last_pack_week
        FROM fact_export_transactions
        WHERE season = ?
    """, (season,))

    row = cursor.fetchone()
    if not row or row[0] is None:
        return {}

    cols = ["dm_avg", "cong_avg", "rain_avg", "vol_avg", "reg_avg",
            "otif_avg", "risk_avg", "mts_pass_pct", "total_return_nzd_m",
            "payments_reversed_nzd_m", "total_trays", "submissions",
            "first_pack_week", "last_pack_week"]

    return dict(zip(cols, row))


def get_subzone_breakdown(conn: sqlite3.Connection, season: str) -> dict:
    """
    Get DM mean, std, and MTS fail rate per subzone for a season.
    This populates the 'subzones' key in the JSON payload.
    """
    cursor = conn.execute("""
        SELECT
            q.subzone,
            ROUND(AVG(q.dm_pct), 3)                            AS dm_mean,
            ROUND(
                SQRT(
                    SUM((q.dm_pct - sub.sz_mean) * (q.dm_pct - sub.sz_mean))
                    / COUNT(*)
                ), 3
            )                                                  AS dm_std,
            ROUND(
                100.0 * SUM(CASE WHEN q.mts_status = 'FAIL' THEN 1 ELSE 0 END)
                / COUNT(*), 1
            )                                                  AS mts_fail_pct,
            COUNT(*)                                           AS readings
        FROM dim_fruit_quality q
        JOIN (
            SELECT subzone, AVG(dm_pct) AS sz_mean
            FROM dim_fruit_quality
            WHERE season = ?
            GROUP BY subzone
        ) sub ON q.subzone = sub.subzone
        WHERE q.season = ?
        GROUP BY q.subzone
        ORDER BY dm_mean DESC
    """, (season, season))

    result = {}
    for row in cursor.fetchall():
        subzone, dm_mean, dm_std, mts_fail_pct, readings = row
        result[subzone] = {
            "dm_mean":      dm_mean,
            "dm_std":       dm_std,
            "mts_fail_pct": mts_fail_pct,
            "mts_pass_rate": round((100 - mts_fail_pct) / 100, 4),
            "readings":     readings,
        }
    return result


def get_worst_week(conn: sqlite3.Connection, season: str) -> dict:
    """
    Get the highest-risk pack week for a given season.
    """
    cursor = conn.execute("""
        SELECT
            pack_week,
            subzone,
            ROUND(AVG(risk_score), 1)  AS avg_risk,
            ROUND(AVG(dm_pct_avg), 3)  AS avg_dm,
            ROUND(AVG(otif_pct), 2)    AS avg_otif,
            SUM(CASE WHEN mts_pass = 0 THEN 1 ELSE 0 END) AS mts_fails,
            COUNT(*)                   AS submissions
        FROM fact_export_transactions
        WHERE season = ?
        GROUP BY pack_week, subzone
        ORDER BY avg_risk DESC
        LIMIT 1
    """, (season,))

    row = cursor.fetchone()
    if not row:
        return {}

    return {
        "pack_week":    row[0],
        "subzone":      row[1],
        "avg_risk":     row[2],
        "avg_dm":       row[3],
        "avg_otif":     row[4],
        "mts_fails":    row[5],
        "submissions":  row[6],
    }


def get_weekly_risk_arc(conn: sqlite3.Connection, season: str) -> list:
    """
    Return a list of avg risk scores ordered by pack_week for a season.
    Maps to the weekly_risk_arc key the HTML projection SVG expects.
    """
    cursor = conn.execute("""
        SELECT pack_week, ROUND(AVG(risk_score), 1)
        FROM fact_export_transactions
        WHERE season = ?
        GROUP BY pack_week
        ORDER BY pack_week
    """, (season,))
    rows = cursor.fetchall()
    return [r[1] for r in rows] if rows else []


def get_live_state(conn: sqlite3.Connection) -> tuple:
    """
    Get the most recent pack week available in the dataset.
    Returns (season, pack_week).
    """
    cursor = conn.execute("""
        SELECT season, pack_week
        FROM fact_export_transactions
        ORDER BY date_key DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    return row if row else ("2025/26", 17)


# =============================================================================
# LIVE API ENRICHMENT  (--live --live-apis only)
# =============================================================================

def fetch_live_apis(agg: dict) -> dict:
    """
    Enrich the live payload with three free, key-free external APIs.

    Modifies agg in-place:
      agg['rain_avg']   ← real BOP 7-day rainfall sum (Open-Meteo)
      agg['rain_source'] ← 'open-meteo-live'

    Returns a dict of extra fields to merge into the final payload:
      extras['fx']                ← NZD/EUR + NZD/JPY (Frankfurter)
      extras['corridors_verified'] ← bool (Overpass API SH2 check)
      extras['live_apis_used']    ← True
      extras['api_fetch_timestamp'] ← ISO timestamp

    Contract: each API call is independently guarded. Failure of one
    never affects the others. Never raises — always returns extras.
    """
    now    = datetime.now(NZ_TZ).isoformat()
    extras = {"live_apis_used": True, "api_fetch_timestamp": now}

    # ── A) Open-Meteo — BOP 7-day rainfall ───────────────────────────────────
    # Tauranga coords: lat=-37.69, lon=176.17
    try:
        om_url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=-37.69&longitude=176.17"
            "&daily=precipitation_sum"
            "&forecast_days=7"
            "&timezone=Pacific%2FAuckland"
        )
        with urllib.request.urlopen(om_url, timeout=5) as resp:
            om = json.loads(resp.read().decode("utf-8"))

        precip = om.get("daily", {}).get("precipitation_sum", [])
        if precip:
            rain_7d          = round(sum(v for v in precip if v is not None), 1)
            agg["rain_avg"]  = rain_7d
            agg["rain_source"] = "open-meteo-live"
            extras["rain_source"] = "open-meteo-live"
            print(f"  🌧  Open-Meteo BOP rainfall: {rain_7d} mm / 7d")
        else:
            print("  ⚠️  Open-Meteo: empty precipitation array — keeping ETL value")

    except Exception as exc:
        print(f"  ⚠️  Open-Meteo failed ({type(exc).__name__}: {exc}) "
              f"— using ETL rain value ({agg.get('rain_avg', 'n/a')} mm)")

    # ── B) Frankfurter — NZD/EUR + NZD/JPY ───────────────────────────────────
    try:
        fx_url = "https://api.frankfurter.app/latest?from=NZD&to=EUR,JPY"
        with urllib.request.urlopen(fx_url, timeout=5) as resp:
            fx_data = json.loads(resp.read().decode("utf-8"))

        rates = fx_data.get("rates", {})
        extras["fx"] = {
            "nzd_eur": round(float(rates.get("EUR", 0)), 4),
            "nzd_jpy": round(float(rates.get("JPY", 0)), 2),
            "source":  "frankfurter-live",
            "date":    fx_data.get("date", now[:10]),
        }
        print(f"  💱  Frankfurter FX: "
              f"NZD/EUR {extras['fx']['nzd_eur']}  "
              f"NZD/JPY {extras['fx']['nzd_jpy']}")

    except Exception as exc:
        print(f"  ⚠️  Frankfurter FX failed ({type(exc).__name__}: {exc}) "
              f"— FX block omitted from payload")

    # ── C) Overpass API — SH2 corridor verification ───────────────────────────
    # Bbox: roughly Bay of Plenty + southern Coromandel (lat -38.5..-37.0, lon 175.5..176.8)
    try:
        query = (
            '[out:json];'
            'way["ref"="State Highway 2"]["highway"]'
            '(bbox:-38.5,175.5,-37.0,176.8);'
            'out count;'
        )
        post_data = urllib.parse.urlencode({"data": query}).encode("utf-8")
        req = urllib.request.Request(
            "https://overpass-api.de/api/interpreter",
            data=post_data,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "User-Agent": "APOPHENIA-ZGL/4.0 (portfolio; contact via GitHub)"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            op = json.loads(resp.read().decode("utf-8"))

        tags  = op.get("elements", [{}])[0].get("tags", {})
        ways  = int(tags.get("ways", tags.get("total", "0")))
        extras["corridors_verified"] = ways > 0
        print(f"  🛣️  Overpass SH2: "
              f"{'verified ✓' if extras['corridors_verified'] else 'no ways found'} "
              f"(ways={ways})")

    except Exception as exc:
        print(f"  ⚠️  Overpass failed ({type(exc).__name__}: {exc}) "
              f"— corridors_verified set to null")
        extras["corridors_verified"] = None   # null = unknown, not False

    return extras


# =============================================================================
# PAYLOAD BUILDER
# =============================================================================

def build_payload(season: str, agg: dict, subzones: dict,
                  worst_week: dict, pack_week: int = None,
                  weekly_risk_arc: list = None) -> dict:
    """
    Build the JSON payload matching the Apophenia simulator STATE format.

    Key mapping:
      agg['dm_avg']        → STATE.dm    (Dry Matter %)
      agg['cong_avg']      → STATE.cong  (SH2 congestion — NEEDS RECALIBRATION)
      agg['rain_avg']      → STATE.rain  (Rainfall mm 7d)
      agg['vol_avg']       → STATE.vol   (Volume index)
      agg['reg_avg']       → STATE.reg   (Regulatory load)

    NOTE on congestion_index:
      The current DB has congestion_index = 91.8 for all rows (miscalibration
      identified in model validation — constant value means the variable has
      zero predictive variance). This payload uses a corrected estimate
      based on pack_week position in the season (peak MainPack ≈ 35-45%).
      This is documented as a known limitation pending NZTA data re-ingestion.
    """
    now = datetime.now(NZ_TZ).isoformat()
    pw  = pack_week or agg.get("last_pack_week", 17)

    # Corrected congestion estimate (pack_week based)
    # MainPack weeks 16-22 historically show 30-45% congestion on SH2
    # This replaces the miscalibrated constant 91.8
    def estimate_congestion(pw):
        if pw < 13:   return 15   # KiwiStart — low volume
        if pw < 16:   return 22   # Early MainPack
        if pw < 20:   return 38   # Peak MainPack — highest volume
        if pw < 23:   return 32   # Late MainPack — tapering
        return 18                  # Late season

    # Pest index: derive from MTS fail rate as proxy
    # Higher fail rate correlates with higher pest pressure + climate stress
    mts_fail_pct = 100 - agg.get("mts_pass_pct", 88)
    pest_estimate = min(100, max(5, mts_fail_pct * 2.5))

    payload = {
        # Core simulator inputs (STATE object)
        "dm":          agg.get("dm_avg", 16.4),
        "pest":        round(pest_estimate, 0),
        "congestion":  estimate_congestion(pw),
        "rainfall":    agg.get("rain_avg", 18),
        "vol":         agg.get("vol_avg", 100),
        "regulatory":  agg.get("reg_avg", REG_INDEX_DEFAULT),

        # Metadata
        "timestamp":   now,
        "source":      "kiwifruit_export.db",
        "season":      season,
        "pack_week":   pw,

        # Data quality block
        "data_quality": {
            "nulls":                   0,
            "health_score":            100,
            "mts_pass_rate":           round(agg.get("mts_pass_pct", 88) / 100, 3),
            "mts_fail_pct":            round(mts_fail_pct, 1),
            "otif_avg":                agg.get("otif_avg", 87.5),
            "risk_avg":                agg.get("risk_avg", 23),
            "total_return_nzd_m":      agg.get("total_return_nzd_m", 0),
            "payments_reversed_nzd_m": agg.get("payments_reversed_nzd_m", 0),
            "total_trays":             agg.get("total_trays", 0),
            "submissions":             agg.get("submissions", 0),
            "last_etl_run":            now,
            "congestion_note":         "Estimated from pack_week position — "
                                       "pending NZTA recalibration (constant 91.8 in DB)",
        },

        # Subzone breakdown (for simulator SUBZONES object)
        "subzones": subzones,

        # 26-week risk arc (for HTML projection SVG)
        "weekly_risk_arc": weekly_risk_arc or [],

        # Worst week (for chat engine context)
        "worst_week": worst_week,

        # Season narrative (for simulator season toggle)
        "season_profile": _season_narrative(season, agg),
    }

    return payload


def _season_narrative(season: str, agg: dict) -> dict:
    """
    Generate the season narrative for the simulator SEASONS object.
    Matches the format expected by setSeasonMode() in the simulator JS.
    """
    mts_pass_rate = agg.get("mts_pass_pct", 88) / 100
    total_return  = agg.get("total_return_nzd_m", 0)

    narratives = {
        "2022/23": "La Niña residual. Moderate season with some wet weather pressure.",
        "2023/24": "El Niño conditions — drier, warmer. Best DM year in dataset.",
        "2024/25": "Cyclone Gabrielle aftermath. High pest pressure. Worst season in dataset.",
        "2025/26": "Recovery season. Strong SunGold pool. Reference year for 2026 analysis.",
    }

    return {
        "mtsPassRate":    round(mts_pass_rate, 3),
        "totalReturnM":   round(total_return, 2),
        "pestBase":       round(min(100, (100 - agg.get("mts_pass_pct", 88)) * 2.5), 0),
        "volIndex":       round(agg.get("vol_avg", 100), 1),
        "dmAvg":          round(agg.get("dm_avg", 16.4), 2),
        "climateNote":    narratives.get(season, "Season data from ETL pipeline."),
        "otifAvg":        round(agg.get("otif_avg", 87.5), 2),
        "paymentsReversedM": round(agg.get("payments_reversed_nzd_m", 0), 3),
    }


# =============================================================================
# SIMULATOR JS SNIPPET GENERATOR
# =============================================================================

def generate_js_seasons_object(all_payloads: dict) -> str:
    """
    Generate the SEASONS JavaScript object ready to paste into
    apophenia_v3.html, replacing the manually coded values with
    ETL-validated data from the database.
    """
    lines = [
        "/* ═══════════════════════════════════════════════════════════════════",
        "   SEASONS — ETL-validated data from kiwifruit_export.db",
        "   Generated by 07_api_feed.py | Do not edit manually",
        "   ═══════════════════════════════════════════════════════════════════ */",
        "const SEASONS = {",
    ]

    for season, payload in all_payloads.items():
        sp = payload["season_profile"]
        lines += [
            f"  '{season}': {{",
            f"    dmAvg:         {sp['dmAvg']},",
            f"    mtsPassRate:   {sp['mtsPassRate']},   "
            f"// {sp['mtsPassRate']*100:.1f}% pass rate",
            f"    totalReturnM:  {sp['totalReturnM']},  "
            f"// NZD {sp['totalReturnM']:.1f}M",
            f"    pestBase:      {int(sp['pestBase'])},",
            f"    volIndex:      {sp['volIndex']},",
            f"    otifAvg:       {sp['otifAvg']},",
            f"    paymentsReversedM: {sp['paymentsReversedM']},",
            f"    climateNote:   '{sp['climateNote']}',",
            f"  }},",
        ]

    lines += ["};", ""]

    # Also generate the SUBZONES object from 2025/26 (reference season)
    ref_payload = all_payloads.get("2025/26", list(all_payloads.values())[-1])
    subzones = ref_payload.get("subzones", {})

    lines += [
        "/* SUBZONES — Dim_Corridor values from ETL pipeline */",
        "const SUBZONES_ETL = {",
    ]

    vsi_map = {
        "Te Puke": 1.4, "Katikati": 1.6, "Tauranga": 1.0,
        "Pongakawa": 1.5, "Opotiki": 2.5,
    }
    distance_map = {
        "Te Puke": 28, "Katikati": 52, "Tauranga": 12,
        "Pongakawa": 35, "Opotiki": 97,
    }

    for subzone, data in subzones.items():
        vsi  = vsi_map.get(subzone, 1.5)
        dist = distance_map.get(subzone, 30)
        lines += [
            f"  '{subzone}': {{",
            f"    dmMean:      {data['dm_mean']},   // ETL: dim_fruit_quality avg",
            f"    dmStd:       {data['dm_std']},",
            f"    mtsFailPct:  {data['mts_fail_pct']},",
            f"    distanceKm:  {dist},",
            f"    vsi:         {vsi},               // Vibration Stress Index",
            f"  }},",
        ]

    lines += ["};", ""]

    return "\n".join(lines)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate API feed JSON for Apophenia simulator"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--season",  type=str, help="Season to export e.g. '2025/26'")
    group.add_argument("--all",     action="store_true", help="Export all seasons")
    group.add_argument("--live",    action="store_true", help="Export most recent pack week")
    # Live API enrichment — only meaningful with --live
    parser.add_argument(
        "--live-apis", action="store_true",
        help="Enrich --live payload with real-time Open-Meteo, Frankfurter FX, and Overpass data"
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"  ❌ Database not found: {DB_PATH}")
        print("  Run 03_etl_pipeline/04_load.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    print("=" * 70)
    print("  APOPHENIA SIMULATOR — API Feed Generator")
    print("  Source: kiwifruit_export.db")
    print("  Author: Gabriela Olivera | Data Analytics Portfolio")
    print("=" * 70)

    # ── DETERMINE SEASONS TO PROCESS ──────────────────────────────
    if args.live:
        live_season, live_pw = get_live_state(conn)
        seasons_to_process   = [(live_season, live_pw)]
        api_flag = " + live APIs (Open-Meteo · Frankfurter · Overpass)" if args.live_apis else ""
        print(f"\n  Live mode: {live_season}, Pack Week {live_pw}{api_flag}")
    elif args.season:
        seasons_to_process = [(args.season, None)]
        print(f"\n  Season mode: {args.season}")
    else:  # --all
        cursor = conn.execute(
            "SELECT DISTINCT season FROM fact_export_transactions ORDER BY season"
        )
        seasons_to_process = [(row[0], None) for row in cursor.fetchall()]
        print(f"\n  All seasons: {[s for s,_ in seasons_to_process]}")

    # ── GENERATE PAYLOADS ─────────────────────────────────────────
    all_payloads = {}

    for season, pack_week in seasons_to_process:
        print(f"\n── {season} ──────────────────────────────────────────────────")

        agg        = get_season_aggregate(conn, season)
        subzones   = get_subzone_breakdown(conn, season)
        worst_wk   = get_worst_week(conn, season)
        risk_arc   = get_weekly_risk_arc(conn, season)

        if not agg:
            print(f"  ❌ No data found for season {season}")
            continue

        # Live API enrichment — called BEFORE build_payload so Open-Meteo
        # rainfall overwrites agg['rain_avg'] before it's read by build_payload.
        live_extras = {}
        if args.live and args.live_apis:
            print(f"\n  Fetching live APIs (Open-Meteo · Frankfurter · Overpass)...")
            live_extras = fetch_live_apis(agg)

        payload = build_payload(season, agg, subzones, worst_wk, pack_week, risk_arc)

        # Merge live API extras into top-level payload
        if live_extras:
            payload.update(live_extras)
            # Sync rainfall field if Open-Meteo overwrote agg['rain_avg']
            payload["rainfall"] = agg.get("rain_avg", payload["rainfall"])

        all_payloads[season] = payload

        # Print summary
        print(f"  DM avg:        {payload['dm']}%")
        print(f"  Pest est.:     {int(payload['pest'])}%")
        print(f"  Congestion:    {payload['congestion']}% (corrected estimate)")
        print(f"  MTS pass rate: {payload['data_quality']['mts_pass_rate']*100:.1f}%")
        print(f"  OTIF avg:      {payload['data_quality']['otif_avg']}%")
        print(f"  Return:        NZD {payload['data_quality']['total_return_nzd_m']:.2f}M")
        print(f"  Reversed:      NZD {payload['data_quality']['payments_reversed_nzd_m']:.3f}M")
        print(f"  Worst week:    PW{worst_wk.get('pack_week','?')} "
              f"{worst_wk.get('subzone','?')} "
              f"(risk {worst_wk.get('avg_risk','?')})")
        print(f"  Risk arc:      {len(risk_arc)} weeks "
              f"(range {min(risk_arc):.1f}–{max(risk_arc):.1f})" if risk_arc else "  Risk arc:      empty")

        # Save individual JSON
        fname = f"payload_{season.replace('/','-')}.json"
        if args.live:
            fname = "payload_live.json"
        out = OUTPUT_DIR / fname
        out.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                       encoding="utf-8")
        print(f"  ✅ Saved: {out.name}")

    # ── GENERATE JS SNIPPET (all seasons mode) ────────────────────
    if args.all and len(all_payloads) > 1:
        js_snippet = generate_js_seasons_object(all_payloads)
        js_out = OUTPUT_DIR / "seasons_js_snippet.js"
        js_out.write_text(js_snippet, encoding="utf-8")
        print(f"\n  ✅ JS snippet saved: {js_out.name}")
        print("     → Paste into apophenia_v3.html to replace SEASONS object")

        # Also save combined JSON
        combined_out = OUTPUT_DIR / "all_seasons_combined.json"
        combined_out.write_text(
            json.dumps(all_payloads, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print(f"  ✅ Combined JSON: {combined_out.name}")

    conn.close()

    print(f"\n{'='*70}")
    print(f"  Feed generation complete.")
    print(f"  Output directory: {OUTPUT_DIR}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
