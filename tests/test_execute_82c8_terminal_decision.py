from __future__ import annotations

from datetime import timedelta

import pytest

from scripts.execute_82c8_terminal_decision import (
    CLOCK_START,
    DECISION_AT,
    PARK,
    SOURCE_BINDING,
    WALLET,
    apply_terminal_decision,
    build_decision,
)


def state(rows: list[dict] | None = None) -> dict:
    return {
        "paper_only": True,
        "live_orders_allowed": False,
        "lanes": [
            {
                "wallet": WALLET,
                "source_binding": SOURCE_BINDING,
                "standby_evidence_started_at": CLOCK_START.isoformat(),
                "post_bind_resolved_rows": rows or [],
                "feed_baseline_resolved_signals": 323,
                "resolved_paper_fills": len(rows or []),
            }
        ],
        "standby_adjudications": [],
    }


def positive_rows(count: int = 30) -> list[dict]:
    return [
        {
            "order_id": f"order-{index}",
            "source_wallet": WALLET,
            "resolved_at": (CLOCK_START + timedelta(minutes=index + 1)).isoformat(),
            "resolved": True,
            "copyintent_parity": True,
            "post_fee_pnl_usd": 0.1,
        }
        for index in range(count)
    ]


def test_before_deadline_refuses_execution_without_mutation() -> None:
    original = state()
    decision = build_decision(original, now=DECISION_AT - timedelta(microseconds=1))
    assert decision["status"] == "NOT_DUE"
    with pytest.raises(ValueError, match="not due"):
        apply_terminal_decision(original, decision, now=DECISION_AT - timedelta(seconds=1))
    assert len(original["lanes"]) == 1


def test_exact_deadline_parks_zero_of_thirty_and_releases_capacity() -> None:
    original = state()
    decision = build_decision(original, now=DECISION_AT)
    assert decision["status"] == PARK
    assert decision["evidence"]["resolved"] == 0
    updated = apply_terminal_decision(original, decision, now=DECISION_AT)
    assert updated["lanes"] == []
    assert updated["terminal_82c8_decision"]["capacity_claim_released"] is True
    assert updated["readiness_regeneration"]["status"] == "COMPLETE_AFTER_TERMINAL_RELEASE"
    assert updated["summary"]["lane_count"] == 0


def test_historical_and_foreign_rows_never_satisfy_the_gate() -> None:
    rows = positive_rows()
    rows[0]["resolved_at"] = (CLOCK_START - timedelta(seconds=1)).isoformat()
    rows[1]["source_wallet"] = "0x" + "11" * 20
    decision = build_decision(state(rows), now=DECISION_AT)
    assert decision["evidence"]["resolved"] == 28
    assert len(decision["evidence"]["rejected_rows"]) == 2
    assert decision["status"] == PARK


def test_post_deadline_rows_never_enter_the_frozen_set() -> None:
    rows = positive_rows()
    rows[-1]["resolved_at"] = (DECISION_AT + timedelta(microseconds=1)).isoformat()
    decision = build_decision(state(rows), now=DECISION_AT + timedelta(hours=2))
    assert decision["evidence"]["resolved"] == 29
    assert decision["status"] == PARK
    assert any("outside_immutable_clock" in reason for reason in decision["evidence"]["rejected_rows"])


def test_identical_duplicate_is_counted_once() -> None:
    rows = positive_rows()
    rows.append(dict(rows[0]))
    decision = build_decision(state(rows), now=DECISION_AT)
    assert decision["evidence"]["resolved"] == 30
    assert decision["status"] == "READY_FOR_FABLE_PROMOTION_HANDOFF"


def test_conflicting_duplicate_fails_closed_for_that_identity() -> None:
    rows = positive_rows()
    duplicate = dict(rows[0])
    duplicate["post_fee_pnl_usd"] = 100.0
    rows.append(duplicate)
    decision = build_decision(state(rows), now=DECISION_AT)
    assert decision["evidence"]["resolved"] == 29
    assert decision["status"] == PARK
    assert any("conflicting_duplicate" in reason for reason in decision["evidence"]["rejected_rows"])


def test_all_pass_is_handoff_only_and_never_auto_promotes() -> None:
    original = state(positive_rows())
    decision = build_decision(original, now=DECISION_AT)
    assert decision["status"] == "READY_FOR_FABLE_PROMOTION_HANDOFF"
    assert decision["all_pass"] is True
    assert apply_terminal_decision(original, decision, now=DECISION_AT) == original
    assert original["live_orders_allowed"] is False


def test_terminal_park_is_idempotent() -> None:
    original = state()
    decision = build_decision(original, now=DECISION_AT)
    once = apply_terminal_decision(original, decision, now=DECISION_AT)
    twice = apply_terminal_decision(once, decision, now=DECISION_AT + timedelta(minutes=1))
    assert twice == once
    assert len(once["standby_adjudications"]) == 1


def test_decision_is_independent_of_order_flow_incident_fields() -> None:
    baseline = state()
    firing = {**baseline, "order_flow_deadman": {"status": "INCIDENT_POLICY_CHOKE"}}
    clear = {**baseline, "order_flow_deadman": {"status": "CLEAR"}}
    left = build_decision(firing, now=DECISION_AT)
    right = build_decision(clear, now=DECISION_AT)
    assert left["status"] == right["status"] == PARK
    assert left["checks"] == right["checks"]
