# =============================================================================
# APOPHENIA — Unit tests for api_feed.py
# Script: tests/test_api_feed.py
# Stage:  Testing
# Author: Gabriela Olivera | Data Analytics Portfolio
# =============================================================================
"""
Covers the pure logic in 03_etl_pipeline/api_feed.py: estimate_congestion(),
estimate_pest(), build_payload(), and _season_narrative(). Uses only
small in-memory dicts — never reads kiwifruit_export.db or
apophenia_star.db, so this runs on a clean clone with no database
present.
"""

from typing import Any

import pytest

from api_feed import (
    build_payload,
    estimate_congestion,
    estimate_pest,
    _season_narrative,
)


# =============================================================================
# estimate_congestion() — boundary behaviour
# =============================================================================


@pytest.mark.parametrize(
    "pack_week, expected",
    [
        (1, 15),  # well inside KiwiStart
        (12, 15),  # one below the pw < 13 boundary
        (13, 22),  # exactly on the boundary — first Early MainPack week
        (15, 22),  # one below the pw < 16 boundary
        (16, 38),  # exactly on the boundary — first Peak MainPack week
        (19, 38),  # one below the pw < 20 boundary
        (20, 32),  # exactly on the boundary — first Late MainPack week
        (22, 32),  # one below the pw < 23 boundary
        (23, 18),  # exactly on the boundary — first Late season week
        (26, 18),  # well inside Late season
    ],
)
def test_estimate_congestion_boundaries(pack_week: int, expected: int) -> None:
    assert estimate_congestion(pack_week) == expected


# =============================================================================
# estimate_pest() — clamped to [5, 100]
# =============================================================================


def test_estimate_pest_clamps_to_floor_of_5() -> None:
    # mts_fail_pct=0 -> 0*2.5=0, well below the floor
    assert estimate_pest(0) == 5


def test_estimate_pest_just_below_floor_still_clamps() -> None:
    # mts_fail_pct=1 -> 2.5, still below the floor of 5
    assert estimate_pest(1) == 5


def test_estimate_pest_exactly_at_floor_is_unclamped() -> None:
    # mts_fail_pct=2 -> exactly 5.0
    assert estimate_pest(2) == 5.0


def test_estimate_pest_mid_range_is_unclamped() -> None:
    # mts_fail_pct=20 -> 50.0, well inside [5, 100]
    assert estimate_pest(20) == 50.0


def test_estimate_pest_exactly_at_ceiling_is_unclamped() -> None:
    # mts_fail_pct=40 -> exactly 100.0
    assert estimate_pest(40) == 100.0


def test_estimate_pest_clamps_to_ceiling_of_100() -> None:
    # mts_fail_pct=50 -> 125, well above the ceiling
    assert estimate_pest(50) == 100


# =============================================================================
# build_payload() — defaults on empty agg
# =============================================================================


def test_build_payload_falls_back_to_documented_defaults_on_empty_agg() -> None:
    payload = build_payload("2022/23", {}, {}, {})

    assert payload["dm"] == 16.4
    assert payload["rainfall"] == 18
    assert payload["vol"] == 100
    assert payload["regulatory"] == 15.0
    assert payload["season"] == "2022/23"
    assert payload["pack_week"] == 17  # agg.get("last_pack_week", 17)
    # congestion at pw=17 (the default pack_week) falls in the Peak
    # MainPack band (16 <= pw < 20) -> 38
    assert payload["congestion"] == 38
    # mts_fail_pct default: 100 - 88 = 12 -> pest = min(100, max(5, 30)) = 30
    assert payload["pest"] == 30.0

    dq = payload["data_quality"]
    assert dq["mts_pass_rate"] == 0.88
    assert dq["mts_fail_pct"] == 12.0
    assert dq["otif_avg"] == 87.5
    assert dq["risk_avg"] == 23
    assert dq["total_return_nzd_m"] == 0
    assert dq["payments_reversed_nzd_m"] == 0
    assert dq["total_trays"] == 0
    assert dq["submissions"] == 0

    assert payload["subzones"] == {}
    assert payload["weekly_risk_arc"] == []
    assert payload["worst_week"] == {}


# =============================================================================
# build_payload() — exact structure from a known input
# =============================================================================


def test_build_payload_produces_exact_structure_from_known_input() -> None:
    agg: dict[str, Any] = {
        "dm_avg": 16.88,
        "cong_avg": 91.8,
        "rain_avg": 18.0,
        "vol_avg": 104.0,
        "reg_avg": 15.0,
        "otif_avg": 87.86,
        "risk_avg": 23.0,
        "mts_pass_pct": 92.7,
        "total_return_nzd_m": 18.78,
        "payments_reversed_nzd_m": 1.635,
        "total_trays": 5754062,
        "submissions": 4398,
        "first_pack_week": 11,
        "last_pack_week": 23,
    }
    subzones = {
        "Katikati": {
            "dm_mean": 16.77,
            "dm_std": 0.65,
            "mts_fail_pct": 7.6,
            "mts_pass_rate": 0.924,
            "readings": 1520,
        }
    }
    worst_week = {
        "pack_week": 22,
        "subzone": "Opotiki",
        "avg_risk": 29.5,
        "avg_dm": 16.2,
        "avg_otif": 82.1,
        "mts_fails": 12,
        "submissions": 45,
    }
    risk_arc = [21.4, 22.0, 23.1]

    payload = build_payload(
        "2025/26", agg, subzones, worst_week, pack_week=23, weekly_risk_arc=risk_arc
    )

    # Top-level structure
    assert set(payload.keys()) == {
        "dm",
        "pest",
        "congestion",
        "rainfall",
        "vol",
        "regulatory",
        "timestamp",
        "source",
        "season",
        "pack_week",
        "data_quality",
        "subzones",
        "weekly_risk_arc",
        "worst_week",
        "season_profile",
    }

    assert payload["dm"] == 16.88
    assert payload["pest"] == 18.0  # (100-92.7)*2.5 = 18.25 -> round(.,0)
    assert payload["congestion"] == 18  # pw=23 -> Late season band
    assert payload["rainfall"] == 18.0
    assert payload["vol"] == 104.0
    assert payload["regulatory"] == 15.0
    assert payload["source"] == "kiwifruit_export.db"
    assert payload["season"] == "2025/26"
    assert payload["pack_week"] == 23  # explicit param takes priority
    assert isinstance(payload["timestamp"], str)

    dq = payload["data_quality"]
    assert set(dq.keys()) == {
        "nulls",
        "health_score",
        "mts_pass_rate",
        "mts_fail_pct",
        "otif_avg",
        "risk_avg",
        "total_return_nzd_m",
        "payments_reversed_nzd_m",
        "total_trays",
        "submissions",
        "last_etl_run",
        "congestion_note",
    }
    assert dq["nulls"] == 0
    assert dq["health_score"] == 100
    assert dq["mts_pass_rate"] == 0.927
    assert dq["mts_fail_pct"] == 7.3
    assert dq["otif_avg"] == 87.86
    assert dq["risk_avg"] == 23.0
    assert dq["total_return_nzd_m"] == 18.78
    assert dq["payments_reversed_nzd_m"] == 1.635
    assert dq["total_trays"] == 5754062
    assert dq["submissions"] == 4398

    # Pass-through fields unchanged
    assert payload["subzones"] == subzones
    assert payload["weekly_risk_arc"] == risk_arc
    assert payload["worst_week"] == worst_week

    assert set(payload["season_profile"].keys()) == {
        "mtsPassRate",
        "totalReturnM",
        "pestBase",
        "volIndex",
        "dmAvg",
        "climateNote",
        "otifAvg",
        "paymentsReversedM",
    }


# =============================================================================
# _season_narrative() — known season vs unknown season
# =============================================================================


def test_season_narrative_known_season_returns_correct_shape_and_values() -> None:
    agg = {
        "mts_pass_pct": 92.7,
        "total_return_nzd_m": 18.78,
        "vol_avg": 104.0,
        "dm_avg": 16.88,
        "otif_avg": 87.86,
        "payments_reversed_nzd_m": 1.635,
    }

    narrative = _season_narrative("2025/26", agg)

    assert set(narrative.keys()) == {
        "mtsPassRate",
        "totalReturnM",
        "pestBase",
        "volIndex",
        "dmAvg",
        "climateNote",
        "otifAvg",
        "paymentsReversedM",
    }
    assert narrative["mtsPassRate"] == 0.927
    assert narrative["totalReturnM"] == 18.78
    assert narrative["pestBase"] == 18.0
    assert narrative["volIndex"] == 104.0
    assert narrative["dmAvg"] == 16.88
    assert narrative["otifAvg"] == 87.86
    assert narrative["paymentsReversedM"] == 1.635
    assert narrative["climateNote"] == (
        "Recovery season. Strong SunGold pool. Reference year for 2026 analysis."
    )


def test_season_narrative_unknown_season_falls_back_to_generic_note() -> None:
    narrative = _season_narrative("1999/00", {})

    assert set(narrative.keys()) == {
        "mtsPassRate",
        "totalReturnM",
        "pestBase",
        "volIndex",
        "dmAvg",
        "climateNote",
        "otifAvg",
        "paymentsReversedM",
    }
    assert narrative["climateNote"] == "Season data from ETL pipeline."
    # Defaults still apply for an unknown season with an empty agg
    assert narrative["mtsPassRate"] == 0.88
    assert narrative["dmAvg"] == 16.4
