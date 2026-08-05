from datetime import UTC, datetime

from scripts import report_wide_all_pass_seat_path as seat_path

build_report = seat_path.build_report


def test_production_sticky_focus_identity_is_empty_after_deadline() -> None:
    assert seat_path.STICKY_FOCUS_IDENTITY is None


def _row(wallet, *, pnl, classification, fails, resolved, own=True):
    checks = {
        "f1_measured_positive_regime_cell": True,
        "f1_venue_reachable_admissible": True,
        "f1_walk_forward_admissible": True,
        "f2_fresh_rows_and_own_policy_copyable": True,
        "f3_not_enabled_or_cooloff_or_fading": True,
        "f4_external_liveness": True,
        "own_evidenced_policy_available": own,
        "active_temporal_not_proven_negative": True,
        "active_temporal_regime_cell_measured": True,
        "not_terminal_park_red_clock_or_measured_loser": True,
        "both_resolved_halves_positive": True,
        "f1_concentration_admissible": True,
    }
    for name in fails:
        checks[name] = False
    return {
        "wallet": wallet,
        "wide_policy_fingerprint": f"fp-{wallet[-4:]}",
        "source_generation": "generation",
        "checks": checks,
        "evidence_deficits": list(fails),
        "active_temporal": {
            "label": "PROVEN-POSITIVE",
            "classification": classification,
            "pnl_usd": pnl,
            "roi_pct": 1.0,
            "resolved_trades": 500,
        },
        "direct_source": {
            "attempts": 10,
            "copyable": 2,
            "policy_depth_pass": 2,
            "latest_receipt_at": "2026-07-31T14:00:00Z",
        },
        "f1_slice_basis": {"resolved_signals": resolved},
        "cooloff_scope": {"active": False},
        "paper_policy_id": "paper",
    }


def test_seat_path_ranks_deficits_and_selects_non_fading_focus():
    fading = _row(
        "0x3048d65321be3497164cdfc2996f94f98a2e7537",
        pnl=45549.0,
        classification="FADING",
        fails=["f1_walk_forward_admissible", "f3_not_enabled_or_cooloff_or_fading"],
        resolved=212,
    )
    focus = _row(
        "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        pnl=1529.0,
        classification="WEEKDAY-ONLY",
        fails=[
            "both_resolved_halves_positive",
            "f1_concentration_admissible",
            "f1_measured_positive_regime_cell",
            "f1_walk_forward_admissible",
        ],
        resolved=95,
    )
    report = build_report(
        frontier={
            "candidate_count": 2,
            "eligible_count": 0,
            "nearest_frontier": [fading, focus],
            "gap_closing_lane": {
                "lane": "wide_exact_policy_paper",
                "status": "ACTIVE_PAPER_ONLY",
                "source_state": "paper.json",
                "covers_wallets": [focus["wallet"]],
                "live_authority": False,
            },
        },
        candidate_evidence={"rows": [fading, focus]},
        fingerprint_evidence={
            "walk_forward_best_by_wallet": {
                fading["wallet"]: {
                    "wide_policy_fingerprint": fading["wide_policy_fingerprint"],
                    "venue_executable_full_stream_rescore": {
                        "f1_walk_forward_admissible": False,
                        "first_half": {
                            "resolved": 225,
                            "f1_pass": True,
                            "pnl_excluding_top_1_market": -1.0,
                        },
                        "second_half": {
                            "resolved": 224,
                            "f1_pass": True,
                            "concentration_admissible": False,
                        },
                    },
                }
            }
        },
        now=datetime(2026, 7, 31, 14, 0, tzinfo=UTC),
    )

    assert report["top_row"]["wallet"] == fading["wallet"]
    assert report["top_row"]["binding_deficit"] == "f1_walk_forward_admissible"
    assert report["top_row"]["admit_ready"] is False
    assert report["walk_forward_diagnosis"]["true_temporal_fading"] is True
    assert report["walk_forward_diagnosis"]["seat_narrative_disposition"] == "DEMOTE_TRUE_FADING_NO_OVERRIDE"
    assert report["sole_accrual_focus"]["wallet"] == focus["wallet"]
    assert report["sole_accrual_focus"]["after"]["f1_resolved_signals"] == 95
    assert report["sole_accrual_focus"]["paper_lane"]["covers_wallet"] is True
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False


def test_focus_uses_one_fingerprint_clock_and_excludes_other_manifest_identity():
    focus = _row(
        "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        pnl=1529.0,
        classification="WEEKDAY-ONLY",
        fails=["f1_walk_forward_admissible"],
        resolved=95,
    )
    report = build_report(
        frontier={
            "candidate_count": 1,
            "eligible_count": 0,
            "nearest_frontier": [focus],
            "gap_closing_lane": {"covers_wallets": [focus["wallet"]], "live_authority": False},
        },
        candidate_evidence={"rows": [focus]},
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {"wallet": focus["wallet"]},
                    "wide_policy_fingerprint": focus["wide_policy_fingerprint"],
                    "venue_executable_full_stream_rescore": {
                        "resolved": 97,
                        "first_half": {"resolved": 49, "post_fee_pnl_usd": 1},
                        "second_half": {"resolved": 48, "post_fee_pnl_usd": 2},
                        "f1_walk_forward_admissible": False,
                    },
                }
            ]
        },
        paper_state={
            "manifest": {
                "wallet_policy_identities": {
                    focus["wallet"]: {"wide_policy_fingerprint": "other-fingerprint"}
                }
            },
            "wallets": {
                focus["wallet"]: {
                    "attempted_exact_policy_buys": 99,
                    "copyable_exact_policy_buys": 88,
                }
            },
        },
    )

    sole = report["sole_accrual_focus"]
    assert sole["single_fingerprint_f1"]["resolved"] == 97
    assert sole["after"]["f1_residual_to_200"] == 103
    assert sole["walk_forward_diagnosis"]["classification"] == "INSUFFICIENT_FORWARD_SAMPLE"
    assert sole["walk_forward_diagnosis"]["wide_policy_fingerprint"] == focus["wide_policy_fingerprint"]
    assert sole["paper_lane"]["focus_identity_active"] is False
    assert sole["exact_policy_paper"]["attempted_exact_policy_buys"] == 0


def test_no_row_is_admit_ready_until_every_canonical_check_passes():
    row = _row(
        "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b",
        pnl=1.0,
        classification="WEEKDAY-ONLY",
        fails=["f1_walk_forward_admissible"],
        resolved=199,
    )
    report = build_report(
        frontier={"candidate_count": 1, "eligible_count": 0, "nearest_frontier": [row]},
        candidate_evidence={"rows": [row]},
        fingerprint_evidence={},
    )
    assert report["top_row"]["all_pass"] is False
    assert report["top_row"]["admit_ready"] is False
    assert report["admission_applied"] is False


def test_sticky_focus_emits_residual_zero_venue_projection():
    wallet = "0x951bd740ef681d05891ca35440232488271d433e"
    fingerprint = "57d944ade6d26e904f6df3360e3e4425f528222ed82b7ec63fe27a1be756d9f0"
    row = _row(wallet, pnl=10.0, classification="WEEKDAY-ONLY", fails=[], resolved=49)
    row["wide_policy_fingerprint"] = fingerprint
    report = build_report(
        frontier={"candidate_count": 1, "eligible_count": 0, "nearest_frontier": [row]},
        candidate_evidence={"rows": [row]},
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {"wallet": wallet},
                    "wide_policy_fingerprint": fingerprint,
                    "venue_executable_full_stream_rescore": {
                        "resolved": 49,
                        "venue_executable_resolved": 49,
                        "venue_unreachable_resolved": 22,
                        "venue_reachable_share_min_pct": 40.0,
                    },
                }
            ]
        },
    )
    projection = report["sole_accrual_focus"]["venue_residual_zero_projection"]
    assert projection["admissible"] is True
    assert projection["projected_share_pct"] > 40.0


def test_completed_generation_preserves_identity_scoped_paper_delta_after_reset():
    wallet = "0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b"
    fingerprint = "ee3f44cad1e78f050d38408b03c4301a03cd0ffb0835ae4c71faaefcde1e08cb"
    row = _row(
        wallet,
        pnl=10.0,
        classification="WEEKDAY-ONLY",
        fails=["f1_walk_forward_admissible"],
        resolved=134,
    )
    row["wide_policy_fingerprint"] = fingerprint
    report = build_report(
        frontier={
            "candidate_count": 1,
            "eligible_count": 0,
            "nearest_frontier": [row],
            "gap_closing_lane": {"covers_wallets": [wallet], "live_authority": False},
        },
        candidate_evidence={"rows": [row]},
        fingerprint_evidence={
            "cells": [
                {
                    "identity": {"wallet": wallet},
                    "wide_policy_fingerprint": fingerprint,
                    "venue_executable_full_stream_rescore": {"resolved": 134},
                }
            ]
        },
        paper_state={
            "manifest": {
                "manifest_id": "new-manifest",
                "wallet_policy_identities": {
                    wallet: {"wide_policy_fingerprint": fingerprint}
                },
            },
            "wallets": {wallet: {"attempted_exact_policy_buys": 0}},
        },
        completed_paper_generation={
            "_run_id": "wide_done",
            "generated_at": "2026-07-31T16:21:00Z",
            "standings": [
                {
                    "wallet": wallet,
                    "wide_policy_fingerprint": fingerprint,
                    "attempted_exact_policy_buys": 32,
                    "copyable_exact_policy_buys": 1,
                    "resolved_orders": 0,
                }
            ],
        },
        completed_paper_generations=[
            {
                "_run_id": "wide_before",
                "generated_at": "2026-07-31T15:51:00Z",
                "standings": [
                    {
                        "wallet": wallet,
                        "wide_policy_fingerprint": fingerprint,
                        "attempted_exact_policy_buys": 20,
                        "copyable_exact_policy_buys": 1,
                        "resolved_orders": 0,
                    }
                ],
            },
            {
                "_run_id": "wide_done",
                "generated_at": "2026-07-31T16:21:00Z",
                "standings": [
                    {
                        "wallet": wallet,
                        "wide_policy_fingerprint": fingerprint,
                        "attempted_exact_policy_buys": 32,
                        "copyable_exact_policy_buys": 1,
                        "resolved_orders": 0,
                    }
                ],
            },
        ],
    )

    paper = report["sole_accrual_focus"]["exact_policy_paper"]
    assert paper["after"]["copyable_exact_policy_buys"] == 1
    assert paper["completed_generation"]["run_id"] == "wide_done"
    assert paper["current_active_generation"]["copyable_exact_policy_buys"] == 0
    assert [row["run_id"] for row in paper["generation_history"]] == [
        "wide_before",
        "wide_done",
    ]
    assert paper["generation_history"][-1]["delta_vs_previous_generation"][
        "attempted_exact_policy_buys"
    ] == 12
    assert report["sole_accrual_focus"]["residual_velocity"][
        "signals_per_completed_generation"
    ] == 0.0
