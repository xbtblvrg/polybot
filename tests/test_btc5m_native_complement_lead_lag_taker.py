import json
from datetime import datetime, timezone
from pathlib import Path

from scripts import run_btc5m_native_complement_lead_lag_taker as lane
from scripts.arbitrate_btc5m_promoted_cells import build_arbiter


def book(*, bid=0.40, ask=0.42, bid_size=500, ask_size=500, micro=0.41):
    return {
        "status": "PASS",
        "best_bid": bid,
        "best_ask": ask,
        "best_bid_size": bid_size,
        "best_ask_size": ask_size,
        "microprice": micro,
    }


def sell_rows():
    return [
        {
            "transactionHash": "a",
            "timestamp": 101,
            "asset": "leader",
            "side": "SELL",
            "price": 0.40,
            "size": 100,
        },
        {
            "transactionHash": "b",
            "timestamp": 101,
            "asset": "leader",
            "side": "SELL",
            "price": 0.39,
            "size": 100,
        },
    ]


def snapshots():
    return [
        {
            "outcome": "Up",
            "observed_at_s": 100.5,
            "book": book(bid_size=500, micro=0.44),
        },
        {
            "outcome": "Down",
            "observed_at_s": 100.5,
            "book": book(ask=0.35, micro=0.56),
        },
    ]


def test_leader_sell_requires_all_frozen_native_features():
    features, blockers = lane.leader_sell_features(
        sell_rows(),
        leader_outcome="Up",
        leader_token="leader",
        opposite_outcome="Down",
        now=102,
        snapshots=snapshots(),
        current_leader_book=book(micro=0.40),
        current_opposite_book=book(ask=0.35, micro=0.56),
    )
    assert blockers == []
    assert features is not None
    assert features["leader_sell_levels"] == 2
    assert features["leader_sell_notional_usd"] == 79.0
    assert features["leader_visible_bid_consumption"] == 0.4


def test_leader_sell_rejects_repriced_complement_and_identity_conflict():
    rows = [*sell_rows(), {**sell_rows()[0], "price": 0.38}]
    features, blockers = lane.leader_sell_features(
        rows,
        leader_outcome="Up",
        leader_token="leader",
        opposite_outcome="Down",
        now=102,
        snapshots=snapshots(),
        current_leader_book=book(micro=0.40),
        current_opposite_book=book(ask=0.39, micro=0.56),
    )
    assert features is None
    assert "conflicting_trade_identity:a" in blockers
    assert "opposite_ask_already_repriced" in blockers


def test_costed_signal_and_exact_paper_intent():
    features, _ = lane.leader_sell_features(
        sell_rows(),
        leader_outcome="Up",
        leader_token="leader",
        opposite_outcome="Down",
        now=102,
        snapshots=snapshots(),
        current_leader_book=book(micro=0.40),
        current_opposite_book=book(ask=0.35, micro=0.56),
    )
    signal, blockers = lane.choose_signal(
        features, opposite_book=book(ask=0.35, micro=0.56), elapsed_s=60
    )
    assert blockers == []
    assert signal["net_edge_per_share"] > 0
    intent = lane.build_intent(
        condition_id="c",
        slug="btc-updown-5m-300",
        token_id="opposite",
        observed_ts=360,
        signal=signal,
    ).asdict()
    assert intent["copy_size_usd"] == 1.0
    assert intent["order_type"] == "FAK"
    assert intent["outcome"] == "Down"
    assert intent["live_orders_allowed"] is False


def _terminal(window, intent=None):
    return {
        "window_start_s": window,
        "raw_clock_complete": True,
        "intent": intent,
        "signal": (intent or {}).get("metadata", {}).get("features"),
        "blockers": ["no_signal"] if intent is None else [],
    }


def test_two_zero_intent_windows_park_and_twelve_clock_is_satisfiable():
    parked = lane.reduce_generation(
        [_terminal(lane.FORWARD_START_S), _terminal(lane.FORWARD_START_S + 300)],
        [],
        {},
    )
    assert parked["status"] == "PARK_ZERO_INTENT_GENERATION"
    intent = {
        "shares": 2.5,
        "metadata": {
            "sequence_disagreement": 0,
            "lookahead_violations": 0,
            "identity_disagreements": 0,
            "parity_disagreement": 0,
            "features": {"executable_ask_depth": 10},
        },
    }
    twelve = lane.reduce_generation(
        [
            _terminal(lane.FORWARD_START_S + index * 300, intent)
            for index in range(12)
        ],
        [],
        {},
    )
    assert twelve["status"] == "PARK_FAILED_GATE_BY_TWELVE_WINDOWS"
    assert twelve["reducer_reproducibility"]["equal"] is True
    assert (
        twelve["reducer_reproducibility"]["first_output_checksum"]
        == twelve["reducer_reproducibility"]["second_output_checksum"]
    )


def test_integrity_is_derived_from_immutable_receipts():
    trades = lane.immutable_trade_receipts(sell_rows(), receipt_timestamp_s=102)
    books = [
        {
            "outcome": outcome,
            "receipt_sequence": 1,
            "receipt_timestamp_s": 102,
            "book_hash": lane._canonical_checksum({"outcome": outcome}),
        }
        for outcome in ("Up", "Down")
    ]
    integrity = lane.measured_raw_integrity(
        trade_receipts=trades,
        book_receipts=books,
        terminal_recorded_at_s=102,
    )
    assert integrity["identity_conflicts"] == 0
    assert integrity["lookahead_violations"] == 0
    assert integrity["continuity_disagreements"] == 0
    assert integrity["trade_receipts_checksum"] == lane._canonical_checksum(trades)

    conflicting = [*trades, {**trades[0], "payload_hash": "different"}]
    measured = lane.measured_raw_integrity(
        trade_receipts=conflicting,
        book_receipts=books[:1],
        terminal_recorded_at_s=101,
    )
    assert measured["identity_conflicts"] == 1
    assert measured["lookahead_violations"] == len(conflicting)
    assert measured["continuity_disagreements"] == 1


def test_matched_baseline_and_raw_reconciliation_are_derived():
    terminal = _terminal(lane.FORWARD_START_S)
    terminal.update(
        {
            "generation_checksum": lane.CHECKSUM,
            "raw_integrity": {
                "sequence_disagreements": 0,
                "continuity_disagreements": 0,
                "lookahead_violations": 0,
                "identity_conflicts": 0,
            },
            "trade_receipts": [],
            "book_receipts": [],
            "raw_evidence_checksum": lane._canonical_checksum(
                {"trade_receipts": [], "book_receipts": []}
            ),
            "matched_no_trade_cohort": [
                {"cash_flows_usd": [-0.02, 0.03], "orders_executed": 0}
            ],
        }
    )
    reduced = lane.reduce_generation([terminal], [], {})
    assert reduced["matched_no_trade_pnl_usd"] == 0.01
    assert reduced["matched_no_trade_cash_flow_count"] == 2
    assert reduced["gate_checks"]["raw_input_equals_terminal"] is True
    assert reduced["gate_checks"]["two_run_checksum_idempotence"] is True


def _all_pass_payload():
    return {
        "generated_at": "2026-07-25T17:30:00+00:00",
        "resolved_orders": 10,
        "post_cost_pnl_usd": 2,
        "first_half_post_cost_pnl_usd": 1,
        "second_half_post_cost_pnl_usd": 1,
        "gate_checks": {
            "resolved_gte_10": True,
            "post_cost_positive": True,
            "first_half_positive": True,
            "second_half_positive": True,
            "incremental_vs_no_trade_positive": True,
            "actual_executable_depth": True,
            "raw_input_equals_terminal": True,
            "zero_sequence_lookahead_identity_parity_disagreement": True,
            "two_run_checksum_idempotence": True,
        },
    }


def test_pre_gate_selector_cannot_publish_live_candidate():
    payload = _all_pass_payload()
    payload["gate_checks"]["resolved_gte_10"] = False
    selector = lane._selector(payload, {"checksum": "p"})
    assert selector["selected"] is None
    assert selector["status"] == "NO_GATE_COMPLETE_CELL"


def test_terminal_park_selector_is_nonactionable_tombstone():
    payload = _all_pass_payload()
    payload["status"] = "PARK_ZERO_INTENT_GENERATION"
    selector = lane._selector(payload, {"checksum": "p"})
    assert selector["status"] == "TERMINAL_PARK_ZERO_INTENT_GENERATION"
    assert selector["selected"] is None
    assert selector["cells"] == []
    assert selector["stop_writer"] is True
    assert selector["active_capacity"] is False
    assert selector["due"] is False
    assert selector["historical_arbiter_only"] is True


def test_all_pass_reaches_legacy_watched_arbiter_path(tmp_path: Path, monkeypatch):
    state = tmp_path / "state.json"
    selector_path = tmp_path / "selector.json"
    output = tmp_path / "btc5m_cross_exchange_promoted_cell_latest.json"
    monkeypatch.setattr(lane, "STATE", str(state))
    prereg_body = {
        **lane.CONFIG,
        "kind": "btc5m_native_complement_lead_lag_taker_preregistration",
        "generation_checksum": lane.CHECKSUM,
        "model_checksum": lane.CHECKSUM,
        "execution_mode": "taker",
        "registered_before_forward_outcome_inspection": True,
        "immutable": True,
    }
    prereg = {
        **prereg_body,
        "checksum": lane._arbiter_checksum(prereg_body),
    }
    state.write_text(
        json.dumps(
            {
                "generation_checksum": lane.CHECKSUM,
                "frozen_model": {"checksum": lane.CHECKSUM},
                "preregistration": prereg,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    )
    selector_path.write_text(json.dumps(lane._selector(_all_pass_payload(), prereg)))
    result = build_arbiter(
        source_paths=[str(selector_path)],
        output_path=str(output),
        now=datetime(2026, 7, 25, 17, 30, 1, tzinfo=timezone.utc),
    )
    assert result["status"] == "PROMOTED_CELL_READY"
    assert result["selected"]["generation_checksum"] == lane.CHECKSUM
    assert result["selected"]["activation_id"].startswith("promoted-arbiter-")
    assert json.loads(output.read_text()) == result


def test_stale_or_mixed_generation_evidence_fails_closed(tmp_path: Path, monkeypatch):
    state = tmp_path / "state.json"
    selector_path = tmp_path / "selector.json"
    monkeypatch.setattr(lane, "STATE", str(state))
    prereg = {"checksum": "wrong", "generation_checksum": "mixed", "model_checksum": "mixed"}
    state.write_text(
        json.dumps(
            {
                "generation_checksum": "mixed",
                "frozen_model": {"checksum": "mixed"},
                "preregistration": prereg,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )
    )
    selector_path.write_text(json.dumps(lane._selector(_all_pass_payload(), prereg)))
    result = build_arbiter(
        source_paths=[str(selector_path)],
        output_path=str(tmp_path / "legacy.json"),
        now=datetime(2026, 7, 25, 17, 31, tzinfo=timezone.utc),
        max_age_s=30,
    )
    assert result["status"] == "NO_GATE_COMPLETE_CELL"
    assert result["candidate_count"] == 0
