from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from scripts.execute_a689_82c8_ready_shadow_cut import CUT_AT, build_spec, execute_cut
from scripts.run_ready_wallet_shadow_lanes import (
    A689_HOT_STANDBY_WALLET,
    RANK1_SUCCESSOR_SOURCE_BINDING,
    RANK1_SUCCESSOR_WALLET,
    build_state,
)


def inputs() -> tuple[dict, dict]:
    state = {
        "paper_only": True,
        "live_orders_allowed": False,
        "lanes": [
            {
                "wallet": A689_HOT_STANDBY_WALLET,
                "standby_evidence_started_at": "2026-07-21T18:22:07.136412Z",
                "resolved_paper_fills": 0,
                "in_lane_post_fee_pnl_usd": 0.0,
                "source_binding_status": "WIRED",
            }
        ],
    }
    queue = {
        "ranked_members": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "replay": {
                    "policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "paper_orders": 676,
                    "resolved_orders": 323,
                    "paper_pnl_usd": 133.947283,
                },
            }
        ]
    }
    return state, queue


def test_cut_spec_is_live_ready_but_not_due_early() -> None:
    state, queue = inputs()
    spec = build_spec(state, queue, now=CUT_AT - timedelta(seconds=1))
    assert spec["cut_due"] is False
    assert spec["decision"] == "TERMINATE_A689_EMPTY_AND_BIND_82C8"
    assert spec["successor"]["liveness_probe"]["recommended_query_key"] == "user"
    assert RANK1_SUCCESSOR_WALLET in spec["successor"]["liveness_probe"]["command"]
    with pytest.raises(ValueError, match="not due"):
        execute_cut(state, spec, now=CUT_AT - timedelta(seconds=1))


def test_due_cut_atomically_terminalizes_a689_and_binds_fresh_82c8() -> None:
    state, queue = inputs()
    spec = build_spec(state, queue, now=CUT_AT)
    updated = execute_cut(state, spec, now=CUT_AT)
    wallets = {row["wallet"] for row in updated["lanes"]}
    assert A689_HOT_STANDBY_WALLET not in wallets
    assert RANK1_SUCCESSOR_WALLET in wallets
    lane = next(row for row in updated["lanes"] if row["wallet"] == RANK1_SUCCESSOR_WALLET)
    assert lane["source_binding"] == RANK1_SUCCESSOR_SOURCE_BINDING
    assert lane["standby_evidence_elapsed_h"] == 0.0
    assert lane["resolved_paper_fills"] == 0
    assert lane["resolved_fill_gap"] == 30
    assert lane["source_liveness"]["recommended_query_key"] == "user"
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert updated["standby_adjudications"][-1]["status"] == "A689_FILL_BACKED_CUT_FAIL_EMPTY_SEAT_REBOUND"


def test_successor_binding_survives_normal_ready_shadow_refresh() -> None:
    state, queue = inputs()
    rebound = execute_cut(state, build_spec(state, queue, now=CUT_AT), now=CUT_AT)
    refreshed = build_state(
        queue=queue,
        limit=5,
        gate=50,
        previous_state=rebound,
        now=CUT_AT + timedelta(hours=1),
    )
    lanes = [row for row in refreshed["lanes"] if row["wallet"] == RANK1_SUCCESSOR_WALLET]
    assert len(lanes) == 1
    assert lanes[0]["source_binding"] == RANK1_SUCCESSOR_SOURCE_BINDING
    assert lanes[0]["standby_evidence_elapsed_h"] == 1.0
    assert lanes[0]["resolved_paper_fills"] == 0


def test_successor_cut_prevents_a689_reenrollment_from_stale_ruling() -> None:
    state, queue = inputs()
    rebound = execute_cut(state, build_spec(state, queue, now=CUT_AT), now=CUT_AT)
    refreshed = build_state(
        queue=queue,
        limit=5,
        gate=50,
        previous_state=rebound,
        watch_tier_shadow_ev={
            "wallets": [
                {
                    "source_wallet": A689_HOT_STANDBY_WALLET,
                    "eligible_signals": 90,
                    "resolved_signals": 60,
                    "pnl_usd": 10.0,
                }
            ]
        },
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": A689_HOT_STANDBY_WALLET,
                    "ruling": "HOT_STANDBY_PENDING_LIVENESS",
                }
            ]
        },
        now=CUT_AT + timedelta(minutes=10),
    )
    wallets = [row["wallet"] for row in refreshed["lanes"]]
    assert A689_HOT_STANDBY_WALLET not in wallets
    assert wallets.count(RANK1_SUCCESSOR_WALLET) == 1
    assert refreshed["a689_82c8_cut"]["status"] == "EXECUTED_ATOMIC_STATE_REBIND"
    assert refreshed["summary"]["a689_cut_terminalized"] is True
