from datetime import UTC, datetime

from scripts.report_wide_ee3f_venue_reachable_diagnosis import (
    STICKY_FINGERPRINT,
    STICKY_WALLET,
    build_diagnosis,
)


def _cell(wallet, fingerprint, *, reachable, unreachable, pnl=10.0):
    return {
        "identity": {"wallet": wallet},
        "wide_policy_fingerprint": fingerprint,
        "venue_executable_full_stream_rescore": {
            "resolved": reachable,
            "venue_executable_resolved": reachable,
            "venue_unreachable_resolved": unreachable,
            "venue_reachable_share_pct": round(
                100 * reachable / (reachable + unreachable), 6
            ),
            "venue_reachable_share_min_pct": 40.0,
            "post_fee_pnl_usd": pnl,
            "concentration_admissible": True,
            "first_half": {"post_fee_pnl_usd": 1.0},
            "second_half": {"post_fee_pnl_usd": 2.0},
        },
    }


def _row(wallet, fingerprint):
    return {
        "wallet": wallet,
        "wide_policy_fingerprint": fingerprint,
        "checks": {
            "own_evidenced_policy_available": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
        },
        "active_temporal": {"classification": "WEEKDAY-ONLY"},
        "cooloff_scope": {"active": False},
    }


def test_structural_projection_and_lawful_alternative_do_not_retarget():
    other_wallet = "0xother"
    other_fp = "other-fp"
    report = build_diagnosis(
        fingerprint_evidence={
            "generated_at": "2026-07-31T16:00:00Z",
            "cells": [
                _cell(STICKY_WALLET, STICKY_FINGERPRINT, reachable=134, unreachable=1600),
                _cell(other_wallet, other_fp, reachable=240, unreachable=60, pnl=20.0),
            ],
        },
        candidate_evidence={
            "rows": [
                _row(STICKY_WALLET, STICKY_FINGERPRINT),
                _row(other_wallet, other_fp),
            ]
        },
        now=datetime(2026, 7, 31, 16, 1, tzinfo=UTC),
    )

    assert report["diagnosis"] == "VENUE_REACHABLE_STRUCTURAL"
    assert report["venue_share_definition"]["numerator_value"] == 134
    assert report["venue_share_definition"]["denominator_value"] == 1734
    assert report["residual_zero_projection"]["projected_share_pct"] < 40
    assert report["highest_share_lawful_alternative_for_fable_disposition_only"]["wallet"] == other_wallet
    assert report["retarget_applied"] is False


def test_rescore_history_is_bounded_and_reports_trend():
    prior = {
        "sample_stability": {
            "rescore_history": [
                {"generated_at": f"t{i}", "venue_reachable_share_pct": float(20 + i)}
                for i in range(12)
            ]
        }
    }
    report = build_diagnosis(
        fingerprint_evidence={
            "generated_at": "new",
            "cells": [
                _cell(STICKY_WALLET, STICKY_FINGERPRINT, reachable=134, unreachable=1600)
            ],
        },
        candidate_evidence={"rows": []},
        prior=prior,
    )
    stability = report["sample_stability"]
    assert stability["rescore_count"] == 10
    assert stability["rescore_history"][-1]["generated_at"] == "new"
    assert stability["trend"] == "DOWN"
