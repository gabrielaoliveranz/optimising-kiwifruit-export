-- Q10: OTIF Trend by Season
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

SELECT
    d.season,
    d.month,
    COUNT(*)                                            AS n_submissions,
    ROUND(AVG(f.delay_hours), 2)                        AS avg_delay_hours,
    ROUND(100.0 * SUM(CASE WHEN f.otif_status = 'OnTime' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_otif,
    ROUND(SUM(f.total_value_nzd), 2)                    AS total_value_nzd
FROM fact_pallet_submissions f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.season, d.month
ORDER BY d.season, d.month
