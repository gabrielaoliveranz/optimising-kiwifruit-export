-- Practice: Window Functions — ROW_NUMBER, RANK, DENSE_RANK
-- Author: Gabriela Olivera | Data Analytics Portfolio
-- DB: apophenia_star.db

-- 1. ROW_NUMBER: rank each grower's submissions by total_value_nzd, highest first
-- PARTITION BY resets the count for every grower
SELECT
    submission_id,
    grower_id,
    total_value_nzd,
    ROW_NUMBER() OVER (
        PARTITION BY grower_id
        ORDER BY total_value_nzd DESC
    ) AS rank_value
FROM fact_pallet_submissions;


-- 2. RANK vs DENSE_RANK on delay_hours (Packhouse 1), where ties occur (12.0 hrs)
-- RANK() gives tied rows the same position but skips the next rank number;
-- DENSE_RANK() gives tied rows the same position and continues consecutively
SELECT
    submission_id,
    packhouse_id,
    delay_hours,
    RANK() OVER (
        PARTITION BY packhouse_id
        ORDER BY delay_hours DESC
    ) AS ranking_with_gaps,
    DENSE_RANK() OVER (
        PARTITION BY packhouse_id
        ORDER BY delay_hours DESC
    ) AS ranking_dense
FROM fact_pallet_submissions
WHERE packhouse_id = 1
ORDER BY delay_hours DESC;


-- 3. ROW_NUMBER / RANK / DENSE_RANK side by side on total_value_nzd (Packhouse 1)
-- Shows how each function handles the same tied values differently
SELECT
    submission_id,
    packhouse_id,
    total_value_nzd,
    ROW_NUMBER() OVER (
        PARTITION BY packhouse_id
        ORDER BY total_value_nzd DESC
    ) AS rn,
    RANK() OVER (
        PARTITION BY packhouse_id
        ORDER BY total_value_nzd DESC
    ) AS rk,
    DENSE_RANK() OVER (
        PARTITION BY packhouse_id
        ORDER BY total_value_nzd DESC
    ) AS drk
FROM fact_pallet_submissions
WHERE packhouse_id = 1
LIMIT 20;


-- 4. Top-3 podium per packhouse using DENSE_RANK
-- DENSE_RANK avoids gaps in the podium when values tie (e.g. two 2nd places still leave a 3rd place)
SELECT *
FROM (
    SELECT
        submission_id,
        packhouse_id,
        total_value_nzd,
        DENSE_RANK() OVER (
            PARTITION BY packhouse_id
            ORDER BY total_value_nzd DESC
        ) AS rank_pallets
    FROM fact_pallet_submissions
) sub
WHERE packhouse_id = 1
  AND rank_pallets <= 3
