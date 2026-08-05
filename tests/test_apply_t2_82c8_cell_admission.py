from scripts import apply_t2_82c8_cell_admission as subject


def _evidence():
    return {
        "cells": [
            {
                "wide_policy_fingerprint": subject.FINGERPRINT,
                "identity": {
                    "wallet": subject.WALLET,
                    "move_slice_keys": ["120-180|>0.75"],
                },
                "evidence_authority": "venue_executable_full_stream_rescore",
                "venue_executable_full_stream_rescore": {
                    "f1_pass": True,
                    "resolved": 1491,
                    "post_fee_pnl_usd": 200.851087,
                    "roi_pct": 13.470898,
                    "first_half_post_fee_pnl_usd": 76.311184,
                    "second_half_post_fee_pnl_usd": 124.539903,
                },
            }
        ]
    }


def _terminal(*, bound=False):
    return {
        "execution_status": "PARK_COMMITTED",
        "checks": {
            "wallet_present": bound,
            "source_binding_exact": bound,
            "clock_start_exact": bound,
        },
    }


def test_t2_packet_clears_only_unbound_red_clock():
    packet = subject.build_packet(
        _evidence(),
        _terminal(),
        generated_at="2026-07-28T18:00:00Z",
    )
    assert packet["status"] == "PASS"
    assert packet["blocker_sweep"]["classification"] == "CONVENIENT"
    assert packet["member"]["max_order_usd"] == 1.0
    assert packet["member"]["policy"]["maker_min_share_funding_cap_usd"] == 2.5
    assert packet["member"]["policy"]["maker_min_share_base_request_cap_usd"] == 1.0
    assert packet["member"]["cell_scoped_admission"]["first_slice_kill"] == {
        "min_resolved_fills": 3,
        "pnl_lte_usd": 0.0,
        "action": "AUTO_DISABLE_CELL",
    }


def test_t2_packet_refuses_bound_red_clock():
    packet = subject.build_packet(
        _evidence(),
        _terminal(bound=True),
        generated_at="2026-07-28T18:00:00Z",
    )
    assert packet["status"] == "DEFECT"
    assert "terminal_red_clock_has_binding_evidence" in packet["defects"]
