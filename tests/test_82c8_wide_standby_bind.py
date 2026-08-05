from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from scripts.execute_82c8_wide_standby_bind import (
    EXPECTED_PACKET_CHECKSUM,
    EXPECTED_RUN_ID,
    EXPECTED_SOURCE_GENERATION,
    build_binding,
    execute_terminal_outcome,
    precommit_existing_terminal,
)
from scripts.run_ready_wallet_shadow_lanes import (
    RANK1_SUCCESSOR_WALLET,
    WIDE_STANDBY_SOURCE_BINDING,
    build_state,
)


NOW = datetime(2026, 7, 29, 6, 45, tzinfo=UTC)


def snapshot() -> dict:
    return {
        "captured_at": "2026-07-29T06:31:17Z",
        "source_generation": EXPECTED_SOURCE_GENERATION,
        "identity": {"run_id": EXPECTED_RUN_ID},
        "input_equals_terminal": True,
        "rows": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "attempt_id": "widepaper_latest",
                "row_identity": "receipt_latest",
                "source_event_id": "1839",
                "transaction_hash": "0xabc",
                "recorded_at": "2026-07-29T06:31:04Z",
                "f1_f4_terminal": {
                    "F3_receipt_freshness": "PASS",
                    "terminal": "REFUSED_ALPHA_PROFILE_FILTER",
                },
            }
        ],
    }


def test_binding_is_non_backdated_and_paper_only() -> None:
    state, artifact = build_binding(
        {"paper_only": True, "live_orders_allowed": False, "lanes": []},
        snapshot(),
        {"source_checksum": EXPECTED_PACKET_CHECKSUM},
        now=NOW,
    )
    lane = state["lanes"][0]
    assert lane["source_binding"] == WIDE_STANDBY_SOURCE_BINDING
    assert lane["standby_evidence_started_at"] == "2026-07-29T06:45:00Z"
    assert lane["source_binding_evidence"]["receipt_recorded_at"] == "2026-07-29T06:31:04Z"
    assert lane["paper_only"] is True
    assert lane["live_orders_allowed"] is False
    assert lane["resolved_paper_fills"] == 0
    assert artifact["status"] == "BOUND_WIDE_RECEIPT_NON_BACKDATED"
    terminal = lane["terminal_outcome_on_deadline"]
    assert terminal["status"] == "PARK_SEAT_UNFED_CLOCK"
    assert terminal["deadline_at"] == "2026-07-31T06:45:00Z"
    assert terminal["terminal"] is True
    assert terminal["stop_writer"] is True
    assert terminal["live_authority"] is False


def test_existing_binding_terminal_precommit_preserves_clock() -> None:
    state = {
        "lanes": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "source_binding": WIDE_STANDBY_SOURCE_BINDING,
                "standby_evidence_started_at": "2026-07-29T06:44:50.331867Z",
            }
        ]
    }
    artifact = {
        "execution_status": "EXECUTED",
        "binding": dict(state["lanes"][0]),
    }

    updated, migrated = precommit_existing_terminal(state, artifact)

    terminal = migrated["binding"]["terminal_outcome_on_deadline"]
    assert terminal["deadline_at"] == "2026-07-31T06:44:50.331867Z"
    assert updated["lanes"][0]["standby_evidence_started_at"] == (
        "2026-07-29T06:44:50.331867Z"
    )
    assert updated["lanes"][0]["terminal_outcome_on_deadline"] == terminal


def test_precommitted_terminal_executes_at_deadline_and_releases_lane() -> None:
    state = {
        "lanes": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "source_binding": WIDE_STANDBY_SOURCE_BINDING,
            }
        ],
        "standby_adjudications": [],
    }
    terminal = {
        "status": "PARK_SEAT_UNFED_CLOCK",
        "deadline_at": "2026-07-31T06:44:50.331867Z",
        "reason": "UNFED_CLOCK_CANNOT_MATURE",
        "terminal": True,
        "stop_writer": True,
        "deadline_extension_allowed": False,
        "promotion_authority": False,
        "live_authority": False,
    }
    artifact = {"binding": {"terminal_outcome_on_deadline": terminal}}

    updated, executed = execute_terminal_outcome(
        state,
        artifact,
        now=datetime(2026, 7, 31, 6, 44, 51, tzinfo=UTC),
    )

    assert updated["lanes"] == []
    assert updated["terminal_82c8_wide_seat_decision"]["status"] == "PARK_SEAT_UNFED_CLOCK"
    assert updated["standby_adjudications"][0]["stop_writer"] is True
    assert executed["status"] == "PARK_SEAT_UNFED_CLOCK"
    assert executed["execution_status"] == "PARK_COMMITTED"


def test_binding_refuses_wrong_packet_checksum() -> None:
    with pytest.raises(ValueError, match="PACKET_CHECKSUM_MISMATCH"):
        build_binding({}, snapshot(), {"source_checksum": "wrong"}, now=NOW)


def test_binding_refuses_to_reset_existing_immutable_clock() -> None:
    clock_start = "2026-07-29T06:44:50.331867Z"
    terminal = {
        "status": "PARK_SEAT_UNFED_CLOCK",
        "deadline_at": "2026-07-31T06:44:50.331867Z",
    }
    state = {
        "lanes": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "source_binding": WIDE_STANDBY_SOURCE_BINDING,
                "standby_evidence_started_at": clock_start,
                "terminal_outcome_on_deadline": terminal,
            }
        ]
    }

    with pytest.raises(ValueError, match="WIDE_BIND_REFUSED_CLOCK_RESET"):
        build_binding(
            state,
            snapshot(),
            {"source_checksum": EXPECTED_PACKET_CHECKSUM},
            now=NOW + timedelta(days=1),
        )

    assert state["lanes"][0]["standby_evidence_started_at"] == clock_start
    assert state["lanes"][0]["terminal_outcome_on_deadline"] == terminal


def test_wide_binding_survives_refresh_and_counts_only_post_bind_orders() -> None:
    state, _artifact = build_binding(
        {"paper_only": True, "live_orders_allowed": False, "lanes": []},
        snapshot(),
        {"source_checksum": EXPECTED_PACKET_CHECKSUM},
        now=NOW,
    )
    wide = {
        "orders": [
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "recorded_at": "2026-07-29T06:44:59Z",
                "resolved": True,
                "pre_fee_pnl_usd": 99,
                "post_fee_pnl_usd": 99,
                "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
            },
            {
                "wallet": RANK1_SUCCESSOR_WALLET,
                "recorded_at": "2026-07-29T06:46:00Z",
                "resolved": True,
                "pre_fee_pnl_usd": 1.0,
                "post_fee_pnl_usd": 0.9,
                "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
            },
        ]
    }
    refreshed = build_state(
        queue={"ranked_members": []},
        limit=5,
        gate=50,
        previous_state=state,
        wide_exact_state=wide,
        now=NOW + timedelta(hours=1),
    )
    lane = next(row for row in refreshed["lanes"] if row["wallet"] == RANK1_SUCCESSOR_WALLET)
    assert lane["source_binding"] == WIDE_STANDBY_SOURCE_BINDING
    assert lane["paper_orders"] == 1
    assert lane["resolved_paper_fills"] == 1
    assert lane["in_lane_post_fee_pnl_usd"] == 0.9
