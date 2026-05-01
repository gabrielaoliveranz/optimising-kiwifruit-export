-- Q4: BOP Subzone DM Variance Analysis — Katikati vs Ōpōtiki
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: kiwifruit_export.db

SELECT
    q.subzone,
    q.season,
    COUNT(*)                                                    AS readings,
    ROUND(AVG(q.dm_pct), 3)                                     AS dm_mean,
    -- SQLite has no STDDEV — calculate manually as sqrt of variance
    ROUND(
        SQRT(
            SUM((q.dm_pct - sub.season_mean) * (q.dm_pct - sub.season_mean))
            / COUNT(*)
        ), 3
    )                                                           AS dm_std,
    ROUND(MIN(q.dm_pct), 2)                                     AS dm_min,
    ROUND(MAX(q.dm_pct), 2)                                     AS dm_max,
    ROUND(MAX(q.dm_pct) - MIN(q.dm_pct), 2)                    AS dm_range,
    ROUND(
        100.0 * SUM(CASE WHEN q.mts_status = 'FAIL' THEN 1 ELSE 0 END)
        / COUNT(*), 1
    )                                                           AS mts_fail_pct
FROM dim_fruit_quality q
JOIN (
    SELECT subzone, season, AVG(dm_pct) AS season_mean
    FROM dim_fruit_quality
    GROUP BY subzone, season
) sub ON q.subzone = sub.subzone AND q.season = sub.season
GROUP BY q.subzone, q.season
ORDER BY q.subzone, q.season
