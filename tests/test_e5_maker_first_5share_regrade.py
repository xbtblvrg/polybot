from __future__ import annotations

import json

from scripts import regrade_e5_maker_first_5share as regrade


def _order(*, quote_id: str = "q1", price: float = 0.40, age_s: float = 30.0) -> dict:
    return {
        "order_id": f"o-{quote_id}",
        "intent_id": f"i-{quote_id}",
        "market_slug": "btc-updown-5m-1000",
        "condition_id": "condition",
        "outcome": "Up",
        "limit_price": price,
        "maker_quote": {
            "quote_id": quote_id,
            "market_slug": "btc-updown-5m-1000",
            "condition_id": "condition",
            "outcome": "Up",
            "token_id": "token-up",
            "quote_price": price,
            "quote_ts": 1010.0,
            "window_end_s": 1300.0,
            "cancel_before_close_s": 30.0,
            "enforced_no_fallback_book": True,
            "book_evidence_mode": "enforced_no_fallback",
            "signal_gate": {"source_signal_age_s": age_s},
            "top_of_book": {
                "status": "OK",
                "book_hash": "hash",
                "route_report": {"route_class": "DIRECT_PASS"},
            },
        },
    }


def test_candidate_is_exact_five_share_and_freshness_fails_closed() -> None:
    selected = regrade.select_candidates({"orders": [_order(price=0.49)]})
    assert selected[0]["size_shares"] == 5.0
    assert selected[0]["size_usd"] == 2.45
    assert selected[0]["sizing_policy_id"] == "fixed_shares_5"
    assert regrade.select_candidates({"orders": [_order(age_s=60.000001)]}) == []


def test_replay_accumulates_partial_sell_volume_and_cancels_residual() -> None:
    candidate = regrade.select_candidates({"orders": [_order()]})[0]
    events = [
        {
            "event_id": "before",
            "market_slug": candidate["market_slug"],
            "outcome": "Up",
            "token_id": "token-up",
            "side": "SELL",
            "price": 0.39,
            "size": 9.0,
            "observed_ts": 1009.0,
            "event_ts": 1009.0,
            "transaction_hash": "a",
        },
        {
            "event_id": "p1",
            "market_slug": candidate["market_slug"],
            "outcome": "Up",
            "token_id": "token-up",
            "side": "SELL",
            "price": 0.40,
            "size": 1.5,
            "observed_ts": 1011.0,
            "event_ts": 1011.0,
            "transaction_hash": "b",
        },
        {
            "event_id": "p2",
            "market_slug": candidate["market_slug"],
            "outcome": "Up",
            "token_id": "token-up",
            "side": "SELL",
            "price": 0.39,
            "size": 2.0,
            "observed_ts": 1012.0,
            "event_ts": 1012.0,
            "transaction_hash": "c",
        },
    ]
    replayed = regrade.replay_candidate(candidate, events)
    assert replayed["execution_status"] == "PARTIAL_FILL_CANCEL_RESIDUAL"
    assert replayed["filled_shares"] == 3.5
    assert replayed["residual_cancelled_shares"] == 1.5
    assert [row["event_id"] for row in replayed["fill_events"]] == ["p1", "p2"]


def test_build_packet_gate_requires_every_quality_check(tmp_path, monkeypatch) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(
        json.dumps(
            {
                "event": "rtds_trade_event",
                "event_id": "coverage-start",
                "market_slug": "btc-updown-5m-1000",
                "outcome": "Down",
                "asset": "token-down",
                "side": "BUY",
                "price": 0.50,
                "size": 1.0,
                "received_at_s": 1000.0,
                "event_ts": 1000.0,
            }
        )
        + "\n"
        + json.dumps(
            {
                "event": "rtds_trade_event",
                "event_id": "fill",
                "market_slug": "btc-updown-5m-1000",
                "asset": "token-up",
                "side": "SELL",
                "price": 0.40,
                "size": 5.0,
                "received_at_s": 1011.0,
                "event_ts": 1011.0,
                "transaction_hash": "tx",
                "raw": {"outcome": "Up"},
            }
        )
        + "\n"
        + json.dumps(
            {
                "event": "rtds_trade_event",
                "event_id": "coverage",
                "market_slug": "btc-updown-5m-1300",
                "outcome": "Up",
                "asset": "other",
                "side": "BUY",
                "price": 0.50,
                "size": 1.0,
                "received_at_s": 1271.0,
                "event_ts": 1271.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(regrade, "MIN_RESOLVED_EXECUTIONS", 1)
    monkeypatch.setattr(regrade, "MIN_TERMINAL_FILL_RATE_PCT", 90.0)
    resolutions = {
        "condition": {
            "direction": "Up",
            "expiry_unix_ts": 1300,
            "source": "polymarket_gamma",
            "window_type": "5m",
        }
    }
    packet = regrade.build_packet(
        paper_state={"updated_at": "now", "orders": [_order()]},
        resolutions=resolutions,
        source_paths=[source],
    )
    assert packet["gate"]["decision"] == "PASS_AUTO_PROMOTE_FIXED_SHARES_5"
    assert packet["summary"]["resolved_distinct_executions"] == 1
    assert packet["summary"]["resolved_post_fee_pnl_usd"] > 0
    assert packet["summary"]["terminal_maker_fill_rate_pct"] == 100.0
