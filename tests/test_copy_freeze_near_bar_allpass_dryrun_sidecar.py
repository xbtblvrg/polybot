from scripts.build_copy_freeze_near_bar_allpass_dryrun_sidecar import build_sidecar


WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
FP = "e009"


def _evidence(
    resolved=187,
    pnl=18.82,
    roi=10.06,
    first_half=10.0,
    second_half=8.82,
):
    return {
        "generated_at": "2026-07-26T02:00:00Z",
        "cells": [
            {
                "identity": {"wallet": WALLET},
                "wide_policy_fingerprint": FP,
                "evidence_authority": "venue_executable_full_stream_rescore",
                "venue_executable_full_stream_rescore": {
                    "resolved": resolved,
                    "post_fee_pnl_usd": pnl,
                    "roi_pct": roi,
                    "first_half_post_fee_pnl_usd": first_half,
                    "second_half_post_fee_pnl_usd": second_half,
                },
            }
        ],
    }


def _deadman(cooloffs=None):
    row = {
        "wallet": WALLET,
        "wide_policy_fingerprint": FP,
        "fresh_own_source_buy_rows_30m": 96,
        "checks": {
            "active_temporal_not_proven_negative": True,
            "f2_fresh_rows_and_own_policy_copyable": True,
            "f3_not_enabled_or_cooloff_or_fading": not bool(cooloffs),
            "f4_external_liveness": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
            "own_evidenced_policy_available": True,
        },
    }
    return {
        "checked_at": "2026-07-26T02:01:00Z",
        "policy_choke_rung_b_cooloffs": cooloffs or {},
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "gate_digits": {
                        "f2_min_fresh_own_source_buy_rows_30m": 10
                    },
                    "nearest_frontier": [row],
                }
            }
        },
    }


def _shadow():
    return {
        "generated_at": "2026-07-26T01:59:00Z",
        "primary": {"wallet": WALLET, "wide_policy_fingerprint": FP},
    }


def test_near_bar_packet_waits_for_f1_without_live_authority():
    packet = build_sidecar(_evidence(), _deadman(), _shadow())
    assert packet["status"] == "WAIT_F1_RESOLVED"
    assert packet["primary"]["remaining_resolved"] == 13
    assert packet["checks"] == {
        "f1_measured_positive_regime_cell": False,
        "f2_fresh_rows_and_own_policy_copyable": True,
        "f3_not_enabled_or_cooloff_or_fading": True,
        "f4_external_liveness": True,
        "active_temporal_not_proven_negative": True,
        "all_pass": False,
        "fresh_own_source_buy_rows_30m": 96,
        "f2_minimum": 10,
        "cooloff_until": None,
    }
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["actuator_contract"]["eligible_to_invoke"] is False


def test_all_pass_arms_existing_direct_deadman_path():
    packet = build_sidecar(_evidence(resolved=200), _deadman(), _shadow())
    assert packet["status"] == "ALL_PASS_READY"
    assert packet["checks"]["all_pass"] is True
    assert packet["actuator_contract"]["eligible_to_invoke"] is True
    assert packet["actuator_contract"]["supply_rung"] == "DIRECT"
    assert packet["actuator_contract"]["selection_pin_refresh_allowed"] is False


def test_unmatched_pin_fingerprint_is_loud_and_never_invokable():
    deadman = _deadman()
    row = deadman["policy_choke"]["actuator"]["candidate_evidence"][
        "nearest_frontier"
    ][0]
    row["wide_policy_fingerprint"] = "supply-fingerprint"

    packet = build_sidecar(_evidence(resolved=200), deadman, _shadow())

    assert packet["status"] == "WAIT_PIN_UNRESOLVED"
    assert packet["checks"]["all_pass"] is False
    assert packet["checks"]["pin_fingerprint_unmatched_in_supply"] is True
    assert packet["checks"]["supply_fingerprints_for_pinned_wallet"] == [
        "supply-fingerprint"
    ]
    assert packet["actuator_contract"]["eligible_to_invoke"] is False


def test_cooloff_refuses_all_pass_even_when_f1_is_green():
    until = "2026-07-27T01:27:36Z"
    packet = build_sidecar(
        _evidence(resolved=200),
        _deadman({WALLET: until}),
        _shadow(),
    )
    assert packet["status"] == "WAIT_OTHER_GATE"
    assert packet["checks"]["f3_not_enabled_or_cooloff_or_fading"] is False
    assert packet["checks"]["all_pass"] is False


def test_active_temporal_negative_refuses_all_pass():
    deadman = _deadman()
    row = deadman["policy_choke"]["actuator"]["candidate_evidence"][
        "nearest_frontier"
    ][0]
    row["checks"]["active_temporal_not_proven_negative"] = False

    packet = build_sidecar(_evidence(resolved=200), deadman, _shadow())

    assert packet["status"] == "WAIT_OTHER_GATE"
    assert packet["checks"]["active_temporal_not_proven_negative"] is False
    assert packet["checks"]["all_pass"] is False
    assert packet["actuator_contract"]["eligible_to_invoke"] is False


def test_negative_chronological_half_refuses_f1_at_sample_bar():
    packet = build_sidecar(
        _evidence(resolved=200, first_half=20.0, second_half=-1.0),
        _deadman(),
        _shadow(),
    )
    assert packet["status"] == "WAIT_F1_RESOLVED"
    assert packet["checks"]["f1_measured_positive_regime_cell"] is False
    assert packet["checks"]["all_pass"] is False


def test_direction_climb_override_replaces_completed_legacy_shadow_primary():
    directed_wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    directed_fp = "34bd"
    evidence = _evidence(resolved=352)
    evidence["freeze_overrides"] = {
        directed_wallet: {
            "wide_policy_fingerprint": directed_fp,
            "reason": "climb_priority_exact_fp_both_halves_positive_f1_open_paper_feedstock",
            "f1": {
                "resolved": 55,
                "post_fee_pnl_usd": 52.5,
                "first_half_post_fee_pnl_usd": 16.9,
                "second_half_post_fee_pnl_usd": 35.6,
            },
        }
    }
    evidence["cells"].append(
        {
            "identity": {"wallet": directed_wallet},
            "wide_policy_fingerprint": directed_fp,
            "evidence_authority": "venue_executable_full_stream_rescore",
            "venue_executable_full_stream_rescore": {
                "resolved": 55,
                "post_fee_pnl_usd": 52.5,
                "roi_pct": 95.5,
                "first_half_post_fee_pnl_usd": 16.9,
                "second_half_post_fee_pnl_usd": 35.6,
            },
        }
    )

    packet = build_sidecar(evidence, _deadman(), _shadow())

    assert packet["primary"]["wallet"] == directed_wallet
    assert packet["primary"]["wide_policy_fingerprint"] == directed_fp
    assert packet["primary"]["resolved"] == 55
    assert packet["status"] == "NOT_NEAR_BAR"
    assert packet["generation_fence"]["primary_selection_source"] == (
        "direction_climb_freeze_override"
    )
