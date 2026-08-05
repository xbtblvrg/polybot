from datetime import UTC, datetime

from scripts.report_regime_seat_selection import (
    acceptance_share_rows,
    build_packet,
    policy_choke_rung_a,
)
from scripts.report_routing_shadow_validation import _intent_row


def test_weekend_rung_a_uses_weekend_slice_and_rule() -> None:
    incumbent = "0x" + "a" * 40
    alternate = "0x" + "b" * 40
    now_s = datetime(2026, 8, 1, 0, 30, tzinfo=UTC).timestamp()
    rows = acceptance_share_rows(
        guard={
            "active_set_runtime": {
                "members": [
                    {"source_wallet": incumbent},
                    {"source_wallet": alternate},
                ]
            }
        },
        temporal={
            "wallets": [
                {
                    "wallet": incumbent,
                    "slice_labels": {
                        "weekday": {"label": "PROVEN-POSITIVE"},
                        "weekend": {
                            "label": "PROVEN-NEGATIVE",
                            "resolved_trades": 538,
                            "pnl_usd": -275.149683,
                            "roi_pct": -6.931507,
                        },
                    },
                },
                {
                    "wallet": alternate,
                    "slice_labels": {
                        "weekend": {"label": "PROVEN-POSITIVE"}
                    },
                },
            ]
        },
        hot_history={
            "events": [
                {
                    "source_wallet": alternate,
                    "action": "BUY",
                    "observed_ts": now_s,
                    "event_id": "weekend-buy",
                }
            ]
        },
        routing_shadow={"fee_gated_measurement_rows": []},
        ledger={
            "orders": [
                {
                    "source_wallet": alternate,
                    "status": "FILLED",
                    "submitted_at": "2026-08-01T00:30:00+00:00",
                    "order_id": "weekend-fill",
                }
            ]
        },
        now_s=now_s,
        lookback_s=1800.0,
        regime="weekend",
    )
    rung_a = policy_choke_rung_a(rows, incumbent, regime="weekend")

    incumbent_row = next(row for row in rows if row["wallet"] == incumbent)
    assert incumbent_row["regime_slice_label"] == "PROVEN-NEGATIVE"
    assert incumbent_row["regime_slice_pnl_usd"] == -275.149683
    assert "weekend-positive" in rung_a["rule"]
    assert "weekday" not in rung_a["rule"]

    packet = build_packet(
        temporal={
            "wallets": [
                {"wallet": incumbent, "slice_labels": {"weekend": {
                    "label": "PROVEN-NEGATIVE", "resolved_trades": 538,
                    "pnl_usd": -275.149683, "roi_pct": -6.931507,
                }}},
                {"wallet": alternate, "slice_labels": {
                    "weekend": {"label": "PROVEN-POSITIVE"}
                }},
            ]
        },
        guard={"active_set_runtime": {
            "selected_member": {"source_wallet": incumbent},
            "members": [{"source_wallet": incumbent}, {"source_wallet": alternate}],
        }},
        ready_shadow={"lanes": []},
        readmission={"rulings": []},
        hot_history={"events": [{
            "source_wallet": alternate, "action": "BUY",
            "observed_ts": now_s, "event_id": "weekend-buy",
        }]},
        routing_shadow={"fee_gated_measurement_rows": []},
        ledger={"orders": [{
            "source_wallet": alternate, "status": "FILLED",
            "submitted_at": "2026-08-01T00:30:00+00:00",
            "order_id": "weekend-fill",
        }]},
        regime="weekend",
        generated_at="2026-08-01T00:30:00Z",
    )
    assert "weekend-positive" in packet["acceptance_share_30m"]["rule"]
    assert "weekday" not in packet["acceptance_share_30m"]["rule"]


def _profile(wallet: str, roi: float) -> dict:
    return {
        "wallet": wallet,
        "classification": "WEEKDAY-ONLY",
        "slice_labels": {
            "weekday": {
                "label": "PROVEN-POSITIVE",
                "resolved_trades": 20,
                "pnl_usd": roi,
                "roi_pct": roi,
            }
        },
    }


def test_report_keeps_paper_evidence_leader_out_of_live_winner() -> None:
    live = "0x" + "a" * 40
    paper = "0x" + "b" * 40
    packet = build_packet(
        temporal={"wallets": [_profile(live, 2.0), _profile(paper, 12.0)]},
        guard={
            "active_set_runtime": {
                "selected_member": {"source_wallet": live},
                "members": [{"source_wallet": live, "candidate_id": "live", "enabled": True}],
            }
        },
        ready_shadow={
            "lanes": [
                {
                    "wallet": paper,
                    "candidate_id": "paper",
                    "paper_only": True,
                    "live_orders_allowed": False,
                }
            ]
        },
        readmission={"rulings": []},
        regime="weekday",
        generated_at="2026-07-20T15:00:00Z",
    )

    assert packet["winner_wallet"] == live
    assert packet["regime_evidence_leader_wallet"] == paper
    assert packet["evidence_leader_is_live_eligible"] is False
    assert packet["live_mutation"] is False


def test_disabled_runtime_member_cannot_win() -> None:
    enabled = "0x" + "c" * 40
    disabled = "0x" + "d" * 40
    packet = build_packet(
        temporal={"wallets": [_profile(enabled, 1.0), _profile(disabled, 20.0)]},
        guard={
            "active_set_runtime": {
                "selected_member": {"source_wallet": enabled},
                "members": [
                    {"source_wallet": enabled, "enabled": True},
                    {"source_wallet": disabled, "enabled": True},
                ],
                "external_liveness_sweep": {
                    "reports": [
                        {
                            "disabled_members": [
                                {"source_wallet": disabled, "reason": "external_liveness_stale"}
                            ]
                        }
                    ]
                },
            }
        },
        ready_shadow={"lanes": []},
        readmission={"rulings": []},
        regime="weekday",
        generated_at="2026-07-20T15:00:00Z",
    )

    assert packet["winner_wallet"] == enabled
    row = next(row for row in packet["candidates"] if row["wallet"] == disabled)
    assert row["live_seat_eligible_existing_rules"] is False
    assert "external_liveness_stale" in row["exclusion_reasons"]


def test_policy_choke_rung_a_prefers_nonzero_weekday_acceptance() -> None:
    incumbent = "0x" + "a" * 40
    alternate = "0x" + "b" * 40
    now_s = 1_784_560_200.0
    packet = build_packet(
        temporal={"wallets": [_profile(incumbent, 2.0), _profile(alternate, 3.0)]},
        guard={
            "active_set_runtime": {
                "selected_member": {"source_wallet": incumbent},
                "members": [
                    {"source_wallet": incumbent, "enabled": True},
                    {"source_wallet": alternate, "enabled": True},
                ],
            }
        },
        ready_shadow={"lanes": []},
        readmission={"rulings": []},
        hot_history={
            "events": [
                {"source_wallet": incumbent, "action": "BUY", "observed_ts": now_s - 10, "event_id": "i1"},
                {"source_wallet": alternate, "action": "BUY", "observed_ts": now_s - 10, "event_id": "a1"},
            ]
        },
        routing_shadow={
            "fee_gated_measurement_rows": [
                {"source_wallet": alternate, "observed_ts": now_s - 5, "intent_id": "ci1", "dominant_skip_reason": "eligible"}
            ]
        },
        ledger={
            "orders": [
                {"source_wallet": alternate, "status": "FILLED", "updated_at": "2026-07-20T15:09:55Z", "order_id": "lo1"}
            ]
        },
        regime="weekday",
        generated_at="2026-07-20T15:10:00Z",
    )

    seat_read = packet["acceptance_share_30m"]
    assert seat_read["action"] == "RUNG_A_RESELECT"
    assert seat_read["target_wallet"] == alternate


def test_acceptance_share_emits_source_identity_difference_set() -> None:
    wallet = "0x" + "a" * 40
    now_s = 1_784_560_200.0
    rows = acceptance_share_rows(
        guard={"active_set_runtime": {"members": [{"source_wallet": wallet}]}},
        temporal={"wallets": []},
        hot_history={"events": [
            {"source_wallet": wallet, "action": "BUY", "observed_ts": now_s, "event_id": "we-1"},
            {"source_wallet": wallet, "action": "BUY", "observed_ts": now_s, "event_id": "we-2"},
        ]},
        routing_shadow={"fee_gated_measurement_rows": [{
            "source_wallet": wallet,
                "observed_ts": now_s,
                "source_event_id": "we-1",
                "source_row_event_id": "we-1",
            "intent_id": "ci-1",
            "dominant_skip_reason": "eligible",
        }]},
        now_s=now_s,
        lookback_s=1800.0,
    )

    assert rows[0]["policy_eligible_intents"] == 1
    assert rows[0]["source_row_identity_coverage"] == 1.0
    assert rows[0]["unmatched_source_row_count"] == 1
    assert rows[0]["unmatched_source_row_ids"] == ["we-2"]


def test_real_routing_builder_propagates_source_row_identity() -> None:
    wallet = "0x" + "a" * 40
    now_s = 1_784_560_200.0
    routing_row = _intent_row(
        intent={
            "intent_id": "ci-1",
            "source_event_id": "we-1",
            "source_row_event_id": "we-1",
            "observed_ts": now_s,
            "market_slug": "btc-updown-5m-1784560200",
            "metadata": {"inventory_v2": {"dominant_skip_reason": "eligible"}},
        },
        state={},
        member={"source_wallet": wallet},
        route_denied_reason="",
        final_gate_rank=0,
    )
    rows = acceptance_share_rows(
        guard={"active_set_runtime": {"members": [{"source_wallet": wallet}]}},
        temporal={"wallets": []},
        hot_history={"events": [{
            "source_wallet": wallet,
            "action": "BUY",
            "observed_ts": now_s,
            "event_id": "we-1",
        }]},
        routing_shadow={"fee_gated_measurement_rows": [routing_row]},
        now_s=now_s,
        lookback_s=1800.0,
    )

    assert routing_row["source_row_event_id"] == "we-1"
    assert rows[0]["policy_eligible_intents"] == 1
    assert rows[0]["source_row_identity_coverage"] == 1.0
