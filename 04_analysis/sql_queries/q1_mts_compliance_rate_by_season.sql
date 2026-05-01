-- Q1: MTS Compliance Rate by Season & Variety
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

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
