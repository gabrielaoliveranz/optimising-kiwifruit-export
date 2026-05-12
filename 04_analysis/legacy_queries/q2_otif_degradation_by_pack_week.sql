-- Q2: OTIF Degradation by Pack Week — SH2 Corridor Analysis
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

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
