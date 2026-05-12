-- Q3: DM% Elasticity — TZG Payment & Return per 0.1% DM
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

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
