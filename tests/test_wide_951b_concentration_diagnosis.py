from scripts.report_wide_951b_concentration_diagnosis import (
    FOCUS_FP,
    RUNNER_UP_FP,
    WALLET,
    build_report,
)


def _cell(fp, *, resolved, pnl, excluding, first=1.0, second=2.0):
    return {
        "identity": {"wallet": WALLET},
        "wide_policy_fingerprint": fp,
        "venue_executable_full_stream_rescore": {
            "resolved": resolved,
            "post_fee_pnl_usd": pnl,
            "pnl_excluding_top_1_market": excluding,
            "top_1_market_pnl_usd": pnl - excluding,
            "top_1_market_share_pct": 190.0,
            "venue_reachable_share_pct": 69.0,
            "concentration_admissible": False,
            "concentration_deficits": ["top_1_market_share_lt_50pct"],
            "first_half": {"resolved": resolved // 2 + resolved % 2, "post_fee_pnl_usd": first},
            "second_half": {"resolved": resolved // 2, "post_fee_pnl_usd": second},
        },
    }


def test_thin_sample_projection_and_runner_up_are_evidence_only():
    report = build_report(
        {
            "cells": [
                _cell(FOCUS_FP, resolved=49, pnl=3.140927, excluding=-2.959851),
                _cell(RUNNER_UP_FP, resolved=23, pnl=7.524907, excluding=1.424129),
            ]
        }
    )
    assert report["diagnosis"] == "THIN_SAMPLE_PLAUSIBLY_CLEARS_AT_RESIDUAL_ZERO"
    assert report["focus"]["residual_zero_clearance_projection"][
        "plausibly_clears_at_residual_zero"
    ] is True
    assert report["runner_up_for_conditional_fable_disposition_only"][
        "wide_policy_fingerprint"
    ] == RUNNER_UP_FP
    assert report["retarget_applied"] is False


def test_mature_failure_is_structural():
    report = build_report(
        {"cells": [_cell(FOCUS_FP, resolved=200, pnl=3.0, excluding=-2.0)]}
    )
    assert report["diagnosis"] == "CONCENTRATION_STRUCTURAL_AT_F1_SAMPLE"
