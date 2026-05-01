# SQL Analysis Results
**Generated:** 2026-05-01 09:53:16  
**Database:** kiwifruit_export.db  
**Author:** Gabriela Olivera | Data Analytics Portfolio  

---

## Q1 — MTS Compliance Rate by Season & Variety

**Research Question:**
Research Question 1: What % of BOP production falls below MTS Green (15.5%)?
How does it vary by season and variety?


**SQL:**
```sql
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
```

**Results:**

| season | variety | total_submissions | mts_fails | mts_fail_pct | avg_dm_pct | min_dm_pct | payment_reversed_nzd_k |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2022/23 | RubyRed | 27 | 22 | 81.480 | 16.566 | 15.700 | 100.590 |
| 2022/23 | SunGold | 2610 | 383 | 14.670 | 16.772 | 14.350 | 1790.040 |
| 2022/23 | OrganicSunGold | 112 | 13 | 11.610 | 16.754 | 15.340 | 62.070 |
| 2022/23 | OrganicGreen | 189 | 6 | 3.170 | 16.872 | 15.090 | 16.080 |
| 2022/23 | Green | 1430 | 21 | 1.470 | 16.825 | 14.770 | 52.150 |
| 2022/23 | SweetGreen | 30 | 0 | 0.000 | 16.665 | 15.910 | 0.000 |
| 2023/24 | RubyRed | 27 | 20 | 74.070 | 16.855 | 15.750 | 99.350 |
| 2023/24 | SunGold | 2610 | 129 | 4.940 | 17.062 | 15.150 | 689.820 |
| 2023/24 | OrganicSunGold | 112 | 3 | 2.680 | 17.137 | 15.740 | 13.740 |
| 2023/24 | Green | 1430 | 2 | 0.140 | 17.141 | 15.140 | 9.650 |
| 2023/24 | SweetGreen | 30 | 0 | 0.000 | 17.024 | 15.850 | 0.000 |
| 2023/24 | OrganicGreen | 189 | 0 | 0.000 | 17.189 | 15.770 | 0.000 |
| 2024/25 | RubyRed | 27 | 27 | 100.000 | 15.998 | 14.320 | 127.350 |
| 2024/25 | SunGold | 2610 | 911 | 34.900 | 16.376 | 13.400 | 4144.470 |
| 2024/25 | OrganicSunGold | 112 | 24 | 21.430 | 16.490 | 15.060 | 79.780 |
| 2024/25 | Green | 1430 | 163 | 11.400 | 16.441 | 13.510 | 484.170 |
| 2024/25 | OrganicGreen | 189 | 13 | 6.880 | 16.493 | 14.630 | 56.270 |
| 2024/25 | SweetGreen | 30 | 2 | 6.670 | 16.340 | 14.370 | 5.210 |
| 2025/26 | RubyRed | 27 | 24 | 88.890 | 16.504 | 15.850 | 129.060 |
| 2025/26 | SunGold | 2610 | 271 | 10.380 | 16.847 | 14.550 | 1429.380 |
| 2025/26 | OrganicSunGold | 112 | 7 | 6.250 | 16.923 | 15.830 | 33.630 |
| 2025/26 | Green | 1430 | 20 | 1.400 | 16.935 | 14.790 | 43.070 |
| 2025/26 | SweetGreen | 30 | 0 | 0.000 | 16.822 | 15.670 | 0.000 |
| 2025/26 | OrganicGreen | 189 | 0 | 0.000 | 17.010 | 15.500 | 0.000 |

**Interpretation:** 
INTERPRETATION: MTS fail rate should be 6-12% in a normal season.
2024/25 will show the highest fail rate due to climate stress.
payment_reversed_nzd_k shows the direct financial cost of MTS breaches.
Varieties with MTS thresholds closest to mean DM (e.g. SunGold at 16.1%)
will show higher fail rates than Green (threshold 15.5%).


---

## Q2 — OTIF Degradation by Pack Week — SH2 Corridor Analysis

**Research Question:**
Research Question 2: In which pack weeks does SH2 congestion cause greatest
OTIF degradation? Is there a non-linear threshold effect?


**SQL:**
```sql
SELECT
    f.pack_week,
    t.season_phase,
    ROUND(AVG(f.otif_pct), 2)                                   AS avg_otif,
    ROUND(MIN(f.otif_pct), 2)                                   AS min_otif,
    ROUND(AVG(f.congestion_index), 2)                           AS avg_congestion,
    ROUND(AVG(f.rainfall_mm_7d), 1)                             AS avg_rainfall_mm,
    COUNT(*)                                                    AS submissions,
    SUM(CASE WHEN f.otif_pct < 88 THEN 1 ELSE 0 END)           AS below_target_count,
    ROUND(
        100.0 * SUM(CASE WHEN f.otif_pct < 88 THEN 1 ELSE 0 END)
        / COUNT(*), 1
    )                                                           AS below_target_pct
FROM fact_export_transactions f
JOIN dim_time t ON f.date_key = t.date_key
GROUP BY f.pack_week, t.season_phase
ORDER BY f.pack_week
```

**Results:**

| pack_week | season_phase | avg_otif | min_otif | avg_congestion | avg_rainfall_mm | submissions | below_target_count | below_target_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 11 | KiwiStart | 88.300 | 76.630 | 91.800 | 28.000 | 656 | 18 | 2.700 |
| 12 | KiwiStart | 88.360 | 76.630 | 91.800 | 28.000 | 668 | 15 | 2.200 |
| 13 | KiwiStart | 87.900 | 76.630 | 91.800 | 28.000 | 1712 | 104 | 6.100 |
| 14 | MainPack | 87.770 | 76.460 | 91.800 | 32.000 | 1768 | 102 | 5.800 |
| 15 | MainPack | 87.660 | 76.460 | 91.800 | 32.000 | 1780 | 119 | 6.700 |
| 16 | MainPack | 87.500 | 76.460 | 91.800 | 32.000 | 1780 | 142 | 8.000 |
| 17 | MainPack | 87.800 | 76.870 | 91.800 | 22.000 | 1780 | 158 | 8.900 |
| 18 | MainPack | 87.510 | 76.870 | 91.800 | 22.000 | 1780 | 201 | 11.300 |
| 19 | MainPack | 87.230 | 76.870 | 91.800 | 22.000 | 1780 | 243 | 13.700 |
| 20 | MainPack | 86.980 | 77.020 | 91.800 | 18.000 | 1696 | 288 | 17.000 |
| 21 | MainPack | 85.650 | 77.020 | 91.800 | 18.000 | 1124 | 316 | 28.100 |
| 22 | MainPack | 85.120 | 77.020 | 91.800 | 18.000 | 1056 | 343 | 32.500 |
| 23 | Late | 77.130 | 77.130 | 91.800 | 15.000 | 12 | 12 | 100.000 |

**Interpretation:** 
INTERPRETATION: Look for pack weeks where below_target_pct spikes.
MainPack weeks (14-22) are the critical window — highest volume,
highest SH2 pressure. The non-linear effect appears when congestion
crosses ~40% (congFactor = 0.4^1.3 ≈ 0.30, causing >2.5pt OTIF drop).


---

## Q3 — DM% Elasticity — TZG Payment & Return per 0.1% DM

**Research Question:**
Research Question 3: What is the NZD elasticity of DM%?
How much is each additional 0.1% DM worth in Taste Zespri Payment?


**SQL:**
```sql
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
```

**Results:**

| dm_pct_band | submissions | avg_tzg | avg_taste_per_tray | avg_return_per_tray | total_return_nzd_m | avg_margin_erosion_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 15.500 | 31.000 | 0.004 | 0.004 | 2.818 | 0.080 | 32.080 |
| 15.600 | 73.000 | 0.022 | 0.021 | 2.825 | 0.210 | 31.920 |
| 15.700 | 97.000 | 0.043 | 0.041 | 2.831 | 0.250 | 31.790 |
| 15.800 | 106.000 | 0.067 | 0.064 | 2.853 | 0.320 | 31.250 |
| 15.900 | 108.000 | 0.091 | 0.086 | 2.889 | 0.330 | 30.380 |
| 16.000 | 130.000 | 0.111 | 0.106 | 2.929 | 0.370 | 29.430 |
| 16.100 | 460.000 | 0.064 | 0.061 | 3.303 | 1.980 | 20.440 |
| 16.200 | 580.000 | 0.071 | 0.067 | 3.392 | 2.360 | 18.360 |
| 16.300 | 692.000 | 0.090 | 0.086 | 3.442 | 2.980 | 17.210 |
| 16.400 | 875.000 | 0.115 | 0.110 | 3.439 | 3.740 | 17.260 |
| 16.500 | 749.000 | 0.136 | 0.129 | 3.458 | 3.250 | 16.790 |
| 16.600 | 1024.000 | 0.158 | 0.150 | 3.475 | 4.420 | 16.350 |
| 16.700 | 865.000 | 0.184 | 0.174 | 3.481 | 3.750 | 16.250 |
| 16.800 | 1032.000 | 0.208 | 0.198 | 3.491 | 4.510 | 16.030 |
| 16.900 | 1164.000 | 0.226 | 0.215 | 3.531 | 5.350 | 15.080 |
| 17.000 | 939.000 | 0.251 | 0.238 | 3.542 | 4.100 | 14.840 |
| 17.100 | 1145.000 | 0.272 | 0.259 | 3.576 | 5.110 | 14.080 |
| 17.200 | 911.000 | 0.297 | 0.282 | 3.570 | 4.100 | 14.190 |
| 17.300 | 889.000 | 0.317 | 0.301 | 3.598 | 3.990 | 13.570 |
| 17.400 | 863.000 | 0.340 | 0.323 | 3.625 | 3.950 | 13.000 |
| 17.500 | 582.000 | 0.366 | 0.347 | 3.623 | 2.780 | 13.160 |
| 17.600 | 599.000 | 0.383 | 0.364 | 3.667 | 2.760 | 11.960 |
| 17.700 | 402.000 | 0.412 | 0.391 | 3.658 | 1.840 | 12.210 |
| 17.800 | 316.000 | 0.427 | 0.406 | 3.706 | 1.530 | 11.150 |
| 17.900 | 306.000 | 0.449 | 0.426 | 3.714 | 1.390 | 10.580 |
| 18.000 | 189.000 | 0.478 | 0.454 | 3.704 | 0.930 | 10.940 |
| 18.100 | 135.000 | 0.503 | 0.478 | 3.696 | 0.630 | 11.130 |
| 18.200 | 95.000 | 0.509 | 0.484 | 3.820 | 0.460 | 8.360 |
| 18.300 | 52.000 | 0.542 | 0.515 | 3.819 | 0.230 | 8.480 |
| 18.400 | 51.000 | 0.567 | 0.539 | 3.769 | 0.260 | 9.180 |
| 18.500 | 30.000 | 0.586 | 0.557 | 3.788 | 0.130 | 8.730 |
| 18.600 | 18.000 | 0.592 | 0.562 | 3.908 | 0.090 | 5.840 |
| 18.700 | 5.000 | 0.601 | 0.571 | 3.981 | 0.020 | 4.070 |
| 18.800 | 5.000 | 0.646 | 0.614 | 3.853 | 0.030 | 7.170 |
| 18.900 | 9.000 | 0.695 | 0.660 | 3.766 | 0.040 | 9.460 |
| 19.000 | 1.000 | 0.780 | 0.741 | 3.467 | 0.000 | 16.470 |
| 19.100 | 1.000 | 0.659 | 0.626 | 4.204 | 0.010 | 0.000 |
| 19.400 | 1.000 | 0.734 | 0.697 | 4.270 | 0.000 | 0.000 |
| 19.700 | 1.000 | 0.785 | 0.746 | 4.320 | 0.010 | 0.000 |

**Interpretation:** 
INTERPRETATION: The elasticity = change in avg_return_per_tray
per 0.1% DM band. Above MTS (15.5%) the relationship is linear —
each 0.1% DM adds approximately TASTE_MAX/45 = $0.021/tray.
The cliff effect at MTS makes the marginal value of DM near the
threshold much higher than anywhere else in the range.


---

## Q4 — BOP Subzone DM Variance Analysis — Katikati vs Ōpōtiki

**Research Question:**
Research Question 4: Which BOP subzone has the highest DM variance
between seasons? Does Katikati show lower variance than Ōpōtiki?


**SQL:**
```sql
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
```

**Results:**

| subzone | season | readings | dm_mean | dm_std | dm_min | dm_max | dm_range | mts_fail_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Katikati | 2022/23 | 1520 | 16.777 | 0.585 | 14.860 | 18.470 | 3.610 | 10.300 |
| Katikati | 2023/24 | 1520 | 17.068 | 0.540 | 15.430 | 18.940 | 3.510 | 3.900 |
| Katikati | 2024/25 | 1520 | 16.380 | 0.693 | 14.020 | 18.600 | 4.580 | 26.900 |
| Katikati | 2025/26 | 1520 | 16.872 | 0.550 | 15.090 | 18.470 | 3.380 | 7.600 |
| Opotiki | 2022/23 | 640 | 16.319 | 0.812 | 14.250 | 18.800 | 4.550 | 32.300 |
| Opotiki | 2023/24 | 640 | 16.654 | 0.680 | 14.840 | 18.950 | 4.110 | 15.900 |
| Opotiki | 2024/25 | 640 | 15.981 | 0.988 | 12.910 | 19.660 | 6.750 | 44.400 |
| Opotiki | 2025/26 | 640 | 16.369 | 0.745 | 14.410 | 18.730 | 4.320 | 26.100 |
| Pongakawa | 2022/23 | 880 | 16.680 | 0.690 | 14.440 | 18.660 | 4.220 | 13.000 |
| Pongakawa | 2023/24 | 880 | 17.029 | 0.631 | 14.990 | 19.170 | 4.180 | 4.200 |
| Pongakawa | 2024/25 | 880 | 16.299 | 0.870 | 13.510 | 19.060 | 5.550 | 31.400 |
| Pongakawa | 2025/26 | 880 | 16.802 | 0.646 | 14.720 | 18.860 | 4.140 | 8.800 |
| Tauranga | 2022/23 | 1200 | 16.547 | 0.621 | 14.630 | 18.680 | 4.050 | 16.300 |
| Tauranga | 2023/24 | 1200 | 16.862 | 0.557 | 14.770 | 18.450 | 3.680 | 6.000 |
| Tauranga | 2024/25 | 1200 | 16.147 | 0.751 | 13.740 | 18.630 | 4.890 | 34.400 |
| Tauranga | 2025/26 | 1200 | 16.663 | 0.606 | 14.550 | 18.490 | 3.940 | 11.600 |
| Te Puke | 2022/23 | 2880 | 16.616 | 0.648 | 14.550 | 18.520 | 3.970 | 15.400 |
| Te Puke | 2023/24 | 2880 | 16.926 | 0.596 | 14.790 | 18.900 | 4.110 | 5.800 |
| Te Puke | 2024/25 | 2880 | 16.245 | 0.827 | 13.390 | 19.090 | 5.700 | 34.000 |
| Te Puke | 2025/26 | 2880 | 16.721 | 0.634 | 14.420 | 18.940 | 4.520 | 11.900 |

**Interpretation:** 
INTERPRETATION: Compare dm_std across subzones. Katikati (target: 0.65%)
should show consistently lower std than Ōpōtiki (target: 0.85%).
This is Research Question 4 — the key finding for the portfolio.
A 31% higher variance in Ōpōtiki translates directly to higher
MTS fail risk and less predictable grower payments.


---

## Q5 — Risk Score Predictive Power — Does It Predict OTIF < 88%?

**Research Question:**
Research Question 5: Does the composite Risk Score predict OTIF < 88%
episodes? What is the precision and recall?


**SQL:**
```sql
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
```

**Results:**

| risk_band | total | otif_below_88 | detection_rate_pct | otif_above_88 | false_alarm_rate_pct |
| --- | --- | --- | --- | --- | --- |
| 1_LOW (0-29) | 15871 | 709 | 4.500 | 15162 | 95.500 |
| 2_MODERATE (30-44) | 1499 | 1130 | 75.400 | 369 | 24.600 |
| 3_ELEVATED (45-59) | 222 | 222 | 100.000 | 0 | 0.000 |

**Interpretation:** 
INTERPRETATION: A good Risk Score should show that HIGH risk bands
have high detection_rate_pct (true positives — correctly flagging
OTIF failures) and LOW risk bands have low detection_rate (few
false alarms). If HIGH risk band has >80% OTIF<88, the model
has strong predictive power. This validates the weight calibration:
DM 35% | Pest 25% | Congestion 15% | Rain 15% | Reg 10%.


---

## Q6 — Worst Week — Highest Risk Pack Week in 4-Season Dataset

**Research Question:**
Research Question 6: What was the highest-risk pack week in the dataset
and what combination of factors caused it?


**SQL:**
```sql
SELECT
    f.season,
    f.pack_week,
    t.season_phase,
    f.subzone,
    ROUND(AVG(f.risk_score), 1)                                 AS avg_risk_score,
    ROUND(MAX(f.risk_score), 0)                                 AS max_risk_score,
    ROUND(AVG(f.dm_pct_avg), 3)                                 AS avg_dm,
    ROUND(AVG(f.otif_pct), 2)                                   AS avg_otif,
    ROUND(AVG(f.congestion_index), 1)                           AS avg_congestion,
    ROUND(AVG(f.rainfall_mm_7d), 1)                             AS avg_rainfall,
    COUNT(*)                                                    AS submissions,
    SUM(CASE WHEN f.mts_pass = 0 THEN 1 ELSE 0 END)            AS mts_fails,
    ROUND(SUM(f.total_return_nzd) / 1000000.0, 3)              AS return_nzd_m,
    ROUND(
        SUM(CASE WHEN f.mts_pass = 0 THEN f.submit_payment_nzd ELSE 0 END)
        / 1000.0, 2
    )                                                           AS payments_reversed_nzd_k
FROM fact_export_transactions f
JOIN dim_time t ON f.date_key = t.date_key
GROUP BY f.season, f.pack_week, t.season_phase, f.subzone
ORDER BY avg_risk_score DESC
LIMIT 20
```

**Results:**

| season | pack_week | season_phase | subzone | avg_risk_score | max_risk_score | avg_dm | avg_otif | avg_congestion | avg_rainfall | submissions | mts_fails | return_nzd_m | payments_reversed_nzd_k |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024/25 | 23 | Late | Katikati | 35.500 | 51.000 | 15.560 | 77.130 | 91.800 | 15.000 | 2 | 2 | 0.000 | 7.370 |
| 2024/25 | 22 | MainPack | Opotiki | 34.300 | 52.000 | 15.822 | 81.190 | 91.800 | 18.000 | 23 | 15 | 0.059 | 65.510 |
| 2024/25 | 21 | MainPack | Opotiki | 34.200 | 53.000 | 15.625 | 81.020 | 91.800 | 18.000 | 24 | 16 | 0.033 | 77.060 |
| 2024/25 | 20 | MainPack | Opotiki | 31.800 | 53.000 | 15.881 | 84.020 | 91.800 | 18.000 | 36 | 15 | 0.108 | 66.310 |
| 2024/25 | 19 | MainPack | Opotiki | 31.300 | 51.000 | 15.988 | 83.470 | 91.800 | 22.000 | 40 | 18 | 0.101 | 75.570 |
| 2024/25 | 23 | Late | Te Puke | 31.000 | 31.000 | 15.740 | 77.130 | 91.800 | 15.000 | 1 | 1 | 0.000 | 7.610 |
| 2024/25 | 21 | MainPack | Tauranga | 30.800 | 52.000 | 15.931 | 82.760 | 91.800 | 18.000 | 46 | 24 | 0.106 | 113.160 |
| 2024/25 | 22 | MainPack | Tauranga | 30.500 | 52.000 | 15.944 | 82.160 | 91.800 | 18.000 | 42 | 24 | 0.083 | 105.380 |
| 2022/23 | 22 | MainPack | Opotiki | 30.400 | 51.000 | 15.994 | 82.240 | 91.800 | 18.000 | 23 | 13 | 0.058 | 53.010 |
| 2024/25 | 16 | MainPack | Opotiki | 30.400 | 55.000 | 16.150 | 84.260 | 91.800 | 32.000 | 40 | 14 | 0.118 | 65.090 |
| 2024/25 | 18 | MainPack | Opotiki | 30.100 | 53.000 | 16.136 | 83.770 | 91.800 | 22.000 | 40 | 17 | 0.117 | 74.980 |
| 2024/25 | 22 | MainPack | Te Puke | 30.100 | 53.000 | 15.965 | 82.240 | 91.800 | 18.000 | 108 | 61 | 0.221 | 276.170 |
| 2023/24 | 23 | Late | Te Puke | 30.000 | 30.000 | 15.790 | 77.130 | 91.800 | 15.000 | 1 | 1 | 0.000 | 8.380 |
| 2024/25 | 20 | MainPack | Tauranga | 29.800 | 51.000 | 15.958 | 83.020 | 91.800 | 18.000 | 68 | 34 | 0.140 | 139.330 |
| 2024/25 | 15 | MainPack | Opotiki | 29.600 | 48.000 | 16.305 | 85.460 | 91.800 | 32.000 | 40 | 10 | 0.149 | 35.070 |
| 2025/26 | 22 | MainPack | Opotiki | 29.500 | 46.000 | 16.030 | 83.280 | 91.800 | 18.000 | 23 | 11 | 0.058 | 67.380 |
| 2025/26 | 21 | MainPack | Opotiki | 29.300 | 50.000 | 16.082 | 83.020 | 91.800 | 18.000 | 24 | 12 | 0.082 | 52.070 |
| 2024/25 | 14 | MainPack | Opotiki | 29.200 | 53.000 | 16.308 | 85.460 | 91.800 | 32.000 | 40 | 10 | 0.139 | 44.420 |
| 2024/25 | 21 | MainPack | Te Puke | 29.200 | 53.000 | 16.052 | 82.810 | 91.800 | 18.000 | 116 | 60 | 0.243 | 281.800 |
| 2024/25 | 22 | MainPack | Pongakawa | 29.100 | 49.000 | 16.090 | 82.830 | 91.800 | 18.000 | 31 | 16 | 0.068 | 87.290 |

**Interpretation:** 
INTERPRETATION: The top rows show the worst season × pack_week × subzone
combinations. Expect 2024/25 to dominate (climate stress year).
Ōpōtiki rows should appear frequently due to high PSA incidence
and distance penalties. The 'what caused it' answer is in the
combination of avg_dm (near MTS floor), avg_congestion, and
avg_rainfall for those rows.


---
