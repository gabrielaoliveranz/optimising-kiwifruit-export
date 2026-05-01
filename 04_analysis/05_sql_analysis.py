"""
=============================================================================
OPTIMISING KIWIFRUIT EXPORT PERFORMANCE — ZGL 2026
Script: 05_sql_analysis.py
Stage: Analysis — 6 SQL Queries against kiwifruit_export.db
Author: Gabriela Olivera | Data Analytics Portfolio
=============================================================================

RESEARCH QUESTIONS ANSWERED:
  Q1. MTS Compliance      — What % of BOP production falls below MTS Green
                            (15.5%) and how does it vary by season?
  Q2. SH2 Corridor        — Which pack weeks show highest OTIF degradation?
                            Is congestion impact non-linear above a threshold?
  Q3. TZG & Return        — What is the NZD elasticity of DM%?
                            How much is each 0.1% DM worth in Taste Payment?
  Q4. BOP Subzones        — Which subzone has highest DM variance between
                            seasons? Does Katikati show lower variance than
                            Ōpōtiki in the data?
  Q5. Risk Score          — Does the composite Risk Score predict OTIF < 88%
                            episodes? What's the confusion matrix?
  Q6. Worst Week          — What was the highest-risk pack week in the dataset
                            and what combination of factors caused it?

HOW TO RUN:
  python 04_analysis/05_sql_analysis.py
  Run from project root: G:\\My Drive\\optimising-kiwifruit-export\\

OUTPUT:
  Prints formatted results to terminal.
  Saves: 04_analysis/sql_queries/query_results.md
=============================================================================
"""

import re
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH      = PROJECT_ROOT / "02_data_processed" / "star_schema" / "kiwifruit_export.db"
SQL_DIR      = PROJECT_ROOT / "04_analysis" / "sql_queries"
SQL_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# QUERIES
# Each entry: (query_id, title, research_question, sql, interpretation_note)
# =============================================================================

QUERIES = [

# ─────────────────────────────────────────────────────────────────────────────
("Q1", "MTS Compliance Rate by Season & Variety",
"""
Research Question 1: What % of BOP production falls below MTS Green (15.5%)?
How does it vary by season and variety?
""",
"""
SELECT
    f.season,
    f.variety,
    COUNT(*)                                                    AS total_submissions,
    SUM(CASE WHEN f.mts_pass = 0 THEN 1 ELSE 0 END)            AS mts_fails,
    ROUND(
        100.0 * SUM(CASE WHEN f.mts_pass = 0 THEN 1 ELSE 0 END)
        / COUNT(*), 2
    )                                                           AS mts_fail_pct,
    ROUND(AVG(f.dm_pct_avg), 3)                                 AS avg_dm_pct,
    ROUND(MIN(f.dm_pct_avg), 2)                                 AS min_dm_pct,
    ROUND(
        SUM(CASE WHEN f.mts_pass = 0 THEN f.submit_payment_nzd ELSE 0 END)
        / 1000.0, 2
    )                                                           AS payment_reversed_nzd_k
FROM fact_export_transactions f
GROUP BY f.season, f.variety
ORDER BY f.season, mts_fail_pct DESC
""",
"""
INTERPRETATION: MTS fail rate should be 6-12% in a normal season.
2024/25 will show the highest fail rate due to climate stress.
payment_reversed_nzd_k shows the direct financial cost of MTS breaches.
Varieties with MTS thresholds closest to mean DM (e.g. SunGold at 16.1%)
will show higher fail rates than Green (threshold 15.5%).
"""),

# ─────────────────────────────────────────────────────────────────────────────
("Q2", "OTIF Degradation by Pack Week — SH2 Corridor Analysis",
"""
Research Question 2: In which pack weeks does SH2 congestion cause greatest
OTIF degradation? Is there a non-linear threshold effect?
""",
"""
SELECT
    f.pack_week,
    t.season_phase,
    ROUND(AVG(f.otif_pct), 2)                                   AS avg_otif,
    ROUND(MIN(f.otif_pct), 2)                                   AS min_otif,
    ROUND(AVG(f.congestion_index), 2)                           AS avg_congestion,
    ROUND(AVG(f.rainfall_mm_7d), 1)                             AS avg_rainfall_mm,
    COUNT(*)                                                    AS submissions,
    SUM(CASE WHEN f.otif_pct < 88 THEN 1 ELSE 0 END)           AS below_target_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.otif_pct < 88 THEN 1 ELSE 0 END)
        / COUNT(*), 1
    )                                                           AS below_target_pct
FROM fact_export_transactions f
JOIN dim_time t ON f.date_key = t.date_key
GROUP BY f.pack_week, t.season_phase
ORDER BY f.pack_week
""",
"""
INTERPRETATION: Look for pack weeks where below_target_pct spikes.
MainPack weeks (14-22) are the critical window — highest volume,
highest SH2 pressure. The non-linear effect appears when congestion
crosses ~40% (congFactor = 0.4^1.3 ≈ 0.30, causing >2.5pt OTIF drop).
"""),

# ─────────────────────────────────────────────────────────────────────────────
("Q3", "DM% Elasticity — TZG Payment & Return per 0.1% DM",
"""
Research Question 3: What is the NZD elasticity of DM%?
How much is each additional 0.1% DM worth in Taste Zespri Payment?
""",
"""
SELECT
    ROUND(f.dm_pct_avg, 1)                                      AS dm_pct_band,
    COUNT(*)                                                    AS submissions,
    ROUND(AVG(f.tzg_score), 3)                                  AS avg_tzg,
    ROUND(AVG(f.taste_payment_nzd / NULLIF(f.trays_exported,0)), 4)
                                                                AS avg_taste_per_tray,
    ROUND(AVG(f.total_return_nzd / NULLIF(f.trays_submitted,0)), 4)
                                                                AS avg_return_per_tray,
    ROUND(SUM(f.total_return_nzd) / 1000000.0, 2)              AS total_return_nzd_m,
    ROUND(AVG(f.margin_erosion_pct), 2)                         AS avg_margin_erosion_pct
FROM fact_export_transactions f
WHERE f.mts_pass = 1                          -- MTS pass only (failed = $0)
GROUP BY dm_pct_band
ORDER BY dm_pct_band
""",
"""
INTERPRETATION: The elasticity = change in avg_return_per_tray
per 0.1% DM band. Above MTS (15.5%) the relationship is linear —
each 0.1% DM adds approximately TASTE_MAX/45 = $0.021/tray.
The cliff effect at MTS makes the marginal value of DM near the
threshold much higher than anywhere else in the range.
"""),

# ─────────────────────────────────────────────────────────────────────────────
("Q4", "BOP Subzone DM Variance Analysis — Katikati vs Ōpōtiki",
"""
Research Question 4: Which BOP subzone has the highest DM variance
between seasons? Does Katikati show lower variance than Ōpōtiki?
""",
"""
SELECT
    q.subzone,
    q.season,
    COUNT(*)                                                    AS readings,
    ROUND(AVG(q.dm_pct), 3)                                     AS dm_mean,
    -- SQLite has no STDDEV — calculate manually as sqrt of variance
    ROUND(
        SQRT(
            SUM((q.dm_pct - sub.season_mean) * (q.dm_pct - sub.season_mean))
            / COUNT(*)
        ), 3
    )                                                           AS dm_std,
    ROUND(MIN(q.dm_pct), 2)                                     AS dm_min,
    ROUND(MAX(q.dm_pct), 2)                                     AS dm_max,
    ROUND(MAX(q.dm_pct) - MIN(q.dm_pct), 2)                    AS dm_range,
    ROUND(
        100.0 * SUM(CASE WHEN q.mts_status = 'FAIL' THEN 1 ELSE 0 END)
        / COUNT(*), 1
    )                                                           AS mts_fail_pct
FROM dim_fruit_quality q
JOIN (
    SELECT subzone, season, AVG(dm_pct) AS season_mean
    FROM dim_fruit_quality
    GROUP BY subzone, season
) sub ON q.subzone = sub.subzone AND q.season = sub.season
GROUP BY q.subzone, q.season
ORDER BY q.subzone, q.season
""",
"""
INTERPRETATION: Compare dm_std across subzones. Katikati (target: 0.65%)
should show consistently lower std than Ōpōtiki (target: 0.85%).
This is Research Question 4 — the key finding for the portfolio.
A 31% higher variance in Ōpōtiki translates directly to higher
MTS fail risk and less predictable grower payments.
"""),

# ─────────────────────────────────────────────────────────────────────────────
("Q5", "Risk Score Predictive Power — Does It Predict OTIF < 88%?",
"""
Research Question 5: Does the composite Risk Score predict OTIF < 88%
episodes? What is the precision and recall?
""",
"""
SELECT
    risk_band,
    total,
    otif_below_88,
    ROUND(100.0 * otif_below_88 / total, 1)                    AS detection_rate_pct,
    otif_above_88,
    ROUND(100.0 * otif_above_88 / total, 1)                    AS false_alarm_rate_pct
FROM (
    SELECT
        CASE
            WHEN risk_score < 30 THEN '1_LOW (0-29)'
            WHEN risk_score < 45 THEN '2_MODERATE (30-44)'
            WHEN risk_score < 60 THEN '3_ELEVATED (45-59)'
            ELSE                      '4_HIGH (60+)'
        END                                                     AS risk_band,
        COUNT(*)                                                AS total,
        SUM(CASE WHEN otif_pct < 88 THEN 1 ELSE 0 END)         AS otif_below_88,
        SUM(CASE WHEN otif_pct >= 88 THEN 1 ELSE 0 END)        AS otif_above_88
    FROM fact_export_transactions
    GROUP BY risk_band
)
ORDER BY risk_band
""",
"""
INTERPRETATION: A good Risk Score should show that HIGH risk bands
have high detection_rate_pct (true positives — correctly flagging
OTIF failures) and LOW risk bands have low detection_rate (few
false alarms). If HIGH risk band has >80% OTIF<88, the model
has strong predictive power. This validates the weight calibration:
DM 35% | Pest 25% | Congestion 15% | Rain 15% | Reg 10%.
"""),

# ─────────────────────────────────────────────────────────────────────────────
("Q6", "Worst Week — Highest Risk Pack Week in 4-Season Dataset",
"""
Research Question 6: What was the highest-risk pack week in the dataset
and what combination of factors caused it?
""",
"""
SELECT
    f.season,
    f.pack_week,
    t.season_phase,
    f.subzone,
    ROUND(AVG(f.risk_score), 1)                                 AS avg_risk_score,
    ROUND(MAX(f.risk_score), 0)                                 AS max_risk_score,
    ROUND(AVG(f.dm_pct_avg), 3)                                 AS avg_dm,
    ROUND(AVG(f.otif_pct), 2)                                   AS avg_otif,
    ROUND(AVG(f.congestion_index), 1)                           AS avg_congestion,
    ROUND(AVG(f.rainfall_mm_7d), 1)                             AS avg_rainfall,
    COUNT(*)                                                    AS submissions,
    SUM(CASE WHEN f.mts_pass = 0 THEN 1 ELSE 0 END)            AS mts_fails,
    ROUND(SUM(f.total_return_nzd) / 1000000.0, 3)              AS return_nzd_m,
    ROUND(
        SUM(CASE WHEN f.mts_pass = 0 THEN f.submit_payment_nzd ELSE 0 END)
        / 1000.0, 2
    )                                                           AS payments_reversed_nzd_k
FROM fact_export_transactions f
JOIN dim_time t ON f.date_key = t.date_key
GROUP BY f.season, f.pack_week, t.season_phase, f.subzone
ORDER BY avg_risk_score DESC
LIMIT 20
""",
"""
INTERPRETATION: The top rows show the worst season × pack_week × subzone
combinations. Expect 2024/25 to dominate (climate stress year).
Ōpōtiki rows should appear frequently due to high PSA incidence
and distance penalties. The 'what caused it' answer is in the
combination of avg_dm (near MTS floor), avg_congestion, and
avg_rainfall for those rows.
"""),

]

# =============================================================================
# RUNNER
# =============================================================================

def _df_to_md(df: pd.DataFrame) -> str:
    """Generate markdown table without tabulate dependency."""
    cols = list(df.columns)
    header = "| " + " | ".join(str(c) for c in cols) + " |"
    sep    = "| " + " | ".join("---" for _ in cols) + " |"
    rows   = []
    for _, row in df.iterrows():
        vals = []
        for v in row:
            if isinstance(v, float):
                vals.append(f"{v:.3f}")
            else:
                vals.append(str(v))
        rows.append("| " + " | ".join(vals) + " |")
    return "\n".join([header, sep] + rows)


def run_queries():
    print("=" * 70)
    print("  OPTIMISING KIWIFRUIT EXPORT — SQL Analysis")
    print("  6 Research Questions | kiwifruit_export.db")
    print("  ZGL 2026 | Gabriela Olivera | Data Analytics Portfolio")
    print("=" * 70)

    if not DB_PATH.exists():
        print(f"\n  ❌ Database not found: {DB_PATH}")
        print("  Run 03_etl_pipeline/04_load.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")

    md_lines = [
        "# SQL Analysis Results",
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        "**Database:** kiwifruit_export.db  ",
        "**Author:** Gabriela Olivera | Data Analytics Portfolio  ",
        "",
        "---",
        "",
    ]

    for qid, title, rq, sql, interpretation in QUERIES:

        print(f"\n{'─'*70}")
        print(f"  {qid} — {title}")
        print(f"{'─'*70}")

        try:
            df = pd.read_sql_query(sql, conn)

            # Terminal output
            pd.set_option('display.max_columns', 20)
            pd.set_option('display.width', 120)
            pd.set_option('display.float_format', '{:.3f}'.format)
            print(df.to_string(index=False))
            print(f"\n  → {len(df)} rows returned")

            # Key stats
            if len(df) > 0:
                print(f"\n  KEY FINDINGS:")
                for col in df.columns:
                    if df[col].dtype in ['float64', 'int64']:
                        print(f"    {col}: min={df[col].min():.3f}  "
                              f"max={df[col].max():.3f}  "
                              f"avg={df[col].mean():.3f}")

            # Markdown output
            md_lines += [
                f"## {qid} — {title}",
                "",
                f"**Research Question:**{rq}",
                "",
                "**SQL:**",
                "```sql" + sql + "```",
                "",
                "**Results:**",
                "",
                _df_to_md(df) if len(df) > 0 else "_No rows returned_",
                "",
                f"**Interpretation:** {interpretation}",
                "",
                "---",
                "",
            ]

        except Exception as e:
            print(f"  ❌ Query failed: {e}")
            md_lines.append(f"## {qid} — ERROR: {e}\n\n---\n")

    conn.close()

    # Save markdown report
    md_path = SQL_DIR / "query_results.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    # Save individual SQL files
    for qid, title, rq, sql, interpretation in QUERIES:
        safe_title = re.sub(r'[^a-z0-9_]', '', title[:30].lower().replace(' ', '_').replace('—', ''))
        sql_file = SQL_DIR / f"{qid.lower()}_{safe_title}.sql"
        sql_content = f"-- {qid}: {title}\n-- Author: Gabriela Olivera | Data Analytics Portfolio\n-- DB: kiwifruit_export.db\n{sql}"
        sql_file.write_text(sql_content, encoding="utf-8")

    print(f"\n{'='*70}")
    print(f"  Analysis complete.")
    print(f"  Results saved: {SQL_DIR / 'query_results.md'}")
    print(f"  SQL files saved: {SQL_DIR}")
    print(f"{'='*70}")


if __name__ == "__main__":
    run_queries()
