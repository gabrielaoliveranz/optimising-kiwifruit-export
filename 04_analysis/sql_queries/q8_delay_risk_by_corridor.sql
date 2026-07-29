-- Q8: Delay Risk by Corridor
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

SELECT
    c.corridor_name,
    c.distance_km,
    COUNT(*)                                            AS n_submissions,
    ROUND(AVG(f.delay_hours), 2)                        AS avg_delay_hours,
    ROUND(100.0 * SUM(CASE WHEN f.otif_status = 'Late' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late,
    ROUND(100.0 * SUM(f.mts_passed) / COUNT(*), 1)      AS pct_mts_passed,
    ROUND(SUM(f.total_value_nzd), 2)                    AS total_value_nzd
FROM fact_pallet_submissions f
JOIN dim_corridor c ON c.corridor_id = f.corridor_id
GROUP BY c.corridor_name, c.distance_km
ORDER BY pct_late DESC
