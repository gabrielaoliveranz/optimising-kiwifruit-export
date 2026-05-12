-- Q5: Risk Score Predictive Power — Does It Predict OTIF < 88%?
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

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
