-- Q11: Highest-Risk Growers
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

SELECT
    g.grower_name,
    g.subzone,
    COUNT(*)                                            AS n_submissions,
    ROUND(100.0 * SUM(CASE WHEN f.otif_status = 'Late' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late,
    -- Rank growers by lateness percentage, limited to growers with 15+ submissions
    RANK() OVER (ORDER BY 100.0 * SUM(CASE WHEN f.otif_status = 'Late' THEN 1 ELSE 0 END) / COUNT(*) DESC) AS risk_rank
FROM fact_pallet_submissions f
JOIN dim_grower g ON g.grower_id = f.grower_id
GROUP BY g.grower_name, g.subzone
HAVING COUNT(*) >= 15
ORDER BY risk_rank
LIMIT 15
