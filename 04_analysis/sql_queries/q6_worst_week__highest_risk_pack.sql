-- Q6: Worst Week — Highest Risk Pack Week in 4-Season Dataset
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

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
