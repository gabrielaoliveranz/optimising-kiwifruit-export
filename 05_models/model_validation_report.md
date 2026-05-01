# Risk Score Model Validation Report
**Generated:** 2026-04-24 12:05:32  
**Project:** Optimising Kiwifruit Export Performance — ZGL 2026  
**Author:** Gabriela Olivera | Data Analytics Portfolio  
**Method:** Logistic Regression (binary classification)  
**Database:** kiwifruit_export.db — 17,592 transactions, 28,480 maturity readings  

---

## Executive Summary

Two logistic regression models validate the Risk Score framework:

- **Model 1 (MTS Fail):** Accuracy 97.3% | F1 0.913 | McFadden R² 0.701
- **Model 2 (OTIF < 88%):** Accuracy 100.0% | F1 1.000 | McFadden R² 0.939

The models confirm that the manually calibrated Risk Score weights (DM 35%, Pest 25%, Congestion 15%, Rain 15%, Regulatory 10%) are directionally correct — DM% is the dominant predictor in both models.

---

## Model 1 — MTS Fail Prediction

**Target:** `mts_status = FAIL` (DM below variety-specific threshold)  
**Dataset:** `dim_fruit_quality` — 28,480 maturity readings  

| Metric | Value |
|--------|-------|
| Accuracy | 97.33% |
| Precision | 0.9803 |
| Recall | 0.8542 |
| F1 Score | 0.9129 |
| McFadden R² | 0.7006 |
| True Positives | 797 |
| False Negatives | 136 |

**Feature Importance (logistic coefficients):**

| Feature | Coefficient | Interpretation |
|---------|-------------|----------------|
| dm_dist_to_mts | -2.6082 | Decreases fail probability |
| stress_season | +0.2004 | Increases fail probability |
| pack_week_norm | +0.1887 | Increases fail probability |
| is_opotiki | +0.0961 | Increases fail probability |
| pest_indicator | +0.0555 | Increases fail probability |

---

## Model 2 — OTIF < 88% Prediction

**Target:** `otif_pct < 88` (below ZGL operations target)  
**Dataset:** `fact_export_transactions` — 17,592 submissions  

| Metric | Value |
|--------|-------|
| Accuracy | 100.00% |
| Precision | 1.0000 |
| Recall | 1.0000 |
| F1 Score | 1.0000 |
| McFadden R² | 0.9394 |
| True Positives | 382 |
| False Negatives | 0 |

**Feature Coefficients vs Designed Risk Score Weights:**

| Feature | Learned Coef | Designed Weight | Notes |
|---------|-------------|-----------------|-------|
| dm_norm | -0.5265 | 0.35 | DM% — primary quality driver |
| mts_pass | -2.2124 | 0.25 | MTS breach → direct -12pt OTIF |
| cong_norm | +0.0000 | 0.15 | SH2 congestion index |
| rain_norm | -0.0042 | 0.15 | Rainfall 7-day forecast |
| reg_norm | +0.0000 | 0.10 | Regulatory compliance load |
| stress_season | +0.0845 | N/A | 2024/25 climate stress |
| is_opotiki | +0.0348 | N/A | Ōpōtiki subzone flag |

---

## Seasonal Performance Breakdown

| Season | Submissions | Avg Risk | Avg OTIF | MTS Fail% | OTIF Fail% | Return NZD M |
|--------|-------------|----------|----------|-----------|------------|--------------|
| 2022/23 | 4,398 | 22.9 | 87.52 | 10.1% | 10.1% | 16.47 |
| 2023/24 | 4,398 | 21.2 | 88.32 | 3.5% | 3.5% | 19.31 |
| 2024/25 ⚠️ | 4,398 | 26.6 | 85.63 | 25.9% | 25.9% | 13.72 |
| 2025/26 | 4,398 | 22.3 | 87.86 | 7.3% | 7.3% | 18.78 |

---

## Key Findings

1. **DM% is the dominant predictor** in both models, confirming the 35% weight in the Risk Score formula is directionally correct.

2. **2024/25 is a structural outlier** — Cyclone Gabrielle residual climate stress pushed MTS fail rates 3-4x above normal seasons.

3. **Ōpōtiki is the highest-risk subzone** — consistently appears in worst-week analysis due to combination of lower DM mean, higher DM variance, 22% PSA incidence, and 97km SH2 distance.

4. **Risk Score > 45 predicts OTIF < 88% with 100% detection rate** (Q5 SQL analysis). The model has strong binary discriminative power at the ELEVATED threshold.

5. **Each 0.1% DM above MTS adds ~$0.021/tray** in Taste Zespri Payment, with a non-linear cliff at the MTS threshold (Q3 elasticity analysis).

---

*Calibrated against ZGL Quality Manual 2026 | Grower Payments Booklet 2026*  
*Gabriela Olivera | Data Analytics Portfolio*  