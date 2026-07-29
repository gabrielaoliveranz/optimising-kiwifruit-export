-- Q7: General KPIs (Power BI top card row)
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

SELECT
    COUNT(*)                                           AS total_submissions,
    SUM(pallet_count)                                  AS total_pallets,
    SUM(tray_count)                                    AS total_trays,
    ROUND(SUM(total_value_nzd), 2)                     AS total_value_nzd,
    ROUND(AVG(dry_matter_pct), 2)                      AS avg_dry_matter_pct,
    ROUND(AVG(delay_hours), 2)                         AS avg_delay_hours,
    -- Percentage of shipments delivered on time
    ROUND(100.0 * SUM(CASE WHEN otif_status = 'OnTime' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_otif,
    -- Percentage of shipments that passed the Minimum Taste Standard (MTS) check
    ROUND(100.0 * SUM(mts_passed) / COUNT(*), 1)       AS pct_mts_passed
FROM fact_pallet_submissions
