-- Q9: Variety Performance — Dry Matter Compliance & Average Payment per Tray
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

SELECT
    v.variety_name,
    v.tier,
    v.mts_threshold,
    COUNT(*)                                            AS n_submissions,
    ROUND(AVG(f.dry_matter_pct), 2)                     AS avg_dry_matter_pct,
    ROUND(100.0 * SUM(f.mts_passed) / COUNT(*), 1)      AS pct_mts_passed,
    ROUND(AVG(f.payment_per_tray_nzd), 2)               AS avg_payment_per_tray_nzd
FROM fact_pallet_submissions f
JOIN dim_variety v ON v.variety_id = f.variety_id
GROUP BY v.variety_name, v.tier, v.mts_threshold
ORDER BY v.tier
