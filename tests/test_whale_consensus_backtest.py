from __future__ import annotations

import json
from argparse import Namespace

from scripts.backtest_whale_consensus_v1 import FillEvent, run_backtest
from scripts.run_whale_consensus_paper_lane import (
    WhaleConsensusFeedEvent,
    _default_params,
    _feed_event_from_rtds,
    _load_signal_event_evidence,
    build_live_consensus_signals,
    consensus_signal_to_intent,
)
from src.wallet_copy.fill_model import FillModelConfig, estimate_executable_fill


def _event(wallet: str, window: int, outcome: str, winner: str, price: float, size: float = 10.0) -> FillEvent:
    return FillEvent(
        wallet=wallet,
        market_slug=f"btc-updown-5m-{window}",
        outcome=outcome,
        side="BUY",
        price=price,
        size=size,
        event_ts=float(window + 30),
        winner=winner,
    )


def test_whale_consensus_uses_only_prior_positive_profiles() -> None:
    events = [
        _event("0xaaa0000000000000000000000000000000000000", 1000, "Up", "Up", 0.4),
        _event("0xaaa0000000000000000000000000000000000000", 1300, "Down", "Down", 0.4),
    ]

    report = run_backtest(
        events,
        thresholds=[1.0],
        pmax_values=[0.5],
        top_k=20,
        order_usd=1.0,
        max_entry_offset_s=240.0,
    )

    assert report["coverage"]["windows"] == 2
    assert report["coverage"]["windows_without_prior_positive_profile"] == 1
    assert report["best"]["trades"] == 1
    assert report["best"]["wins"] == 1
    assert report["best"]["roi_pct"] == 150.0


def test_whale_consensus_pmax_prevents_overpriced_entries() -> None:
    events = [
        _event("0xaaa0000000000000000000000000000000000000", 1000, "Up", "Up", 0.4),
        _event("0xaaa0000000000000000000000000000000000000", 1300, "Down", "Down", 0.8),
    ]

    report = run_backtest(
        events,
        thresholds=[1.0],
        pmax_values=[0.5],
        top_k=20,
        order_usd=1.0,
        max_entry_offset_s=240.0,
    )

    assert report["best"]["trades"] == 0
    assert report["best"]["roi_pct"] == 0.0


def _feed_event(wallet: str, price: float = 0.4) -> WhaleConsensusFeedEvent:
    return WhaleConsensusFeedEvent(
        source_wallet=wallet,
        market_slug="btc-updown-5m-2000",
        condition_id="0xcondition",
        outcome="Up",
        side="BUY",
        price=price,
        size=10.0,
        event_ts=2030.0,
        observed_ts=2030.25,
        token_id="token-up",
        transaction_hash="0xtx",
        event_id=f"rt_{wallet[-6:]}",
    )


def test_live_whale_consensus_signal_uses_profile_weights() -> None:
    signals, diagnostics = build_live_consensus_signals(
        [_feed_event("0xaaa0000000000000000000000000000000000000")],
        [{"wallet": "0xaaa0000000000000000000000000000000000000", "weight": 2.0}],
        threshold=5.0,
        pmax=0.45,
        top_k=20,
        order_usd=1.0,
        max_entry_offset_s=240.0,
    )

    assert diagnostics["signals"] == 1
    assert signals[0]["copy_model"] == "consensus"
    assert signals[0]["outcome"] == "Up"
    assert signals[0]["signal_strength"] == 20.0


def test_live_whale_consensus_signal_respects_pmax() -> None:
    signals, diagnostics = build_live_consensus_signals(
        [_feed_event("0xaaa0000000000000000000000000000000000000", price=0.7)],
        [{"wallet": "0xaaa0000000000000000000000000000000000000", "weight": 2.0}],
        threshold=5.0,
        pmax=0.45,
        top_k=20,
        order_usd=1.0,
        max_entry_offset_s=240.0,
    )

    assert signals == []
    assert diagnostics["trigger_price_above_pmax"] == 1


def test_consensus_signal_to_intent_is_paper_clob_backed() -> None:
    signals, _diagnostics = build_live_consensus_signals(
        [_feed_event("0xaaa0000000000000000000000000000000000000")],
        [{"wallet": "0xaaa0000000000000000000000000000000000000", "weight": 2.0}],
        threshold=5.0,
        pmax=0.45,
        top_k=20,
        order_usd=1.0,
        max_entry_offset_s=240.0,
    )
    clob_book = {
        "status": "OK",
        "instant_fill_status": "PASS",
        "blocking_reason": "none",
        "token_id": "token-up",
        "asset_id": "token-up",
        "book_hash": "hash",
        "best_bid": 0.39,
        "best_ask": 0.4,
        "spread": 0.01,
        "source_price": 0.4,
        "max_copy_price": 0.41,
        "copy_size_usd": 1.0,
        "fillable_usd": 1.0,
        "fillable_shares": 2.5,
        "remaining_usd": 0.0,
        "avg_fill_price": 0.4,
        "fill_ratio": 1.0,
        "levels_used": 1,
    }

    intent = consensus_signal_to_intent(signals[0], clob_book=clob_book)
    fill = estimate_executable_fill(
        intent,
        FillModelConfig(model_id="test", allow_fallback_without_book=False),
    )

    assert intent.mode == "paper"
    assert intent.live_orders_allowed is False
    assert intent.source_wallet == "CONSENSUS"
    assert intent.strategy_family == "whale_consensus_v1"
    assert intent.metadata["copy_model"] == "consensus"
    assert fill["source"] == "clob_book_evidence"
    assert fill["status"] == "FILLED"


def test_rtds_feed_event_uses_raw_outcome_fallback() -> None:
    event = _feed_event_from_rtds(
        {
            "event": "rtds_trade_event",
            "market_slug": "btc-updown-5m-2000",
            "source_wallet": "0xaaa0000000000000000000000000000000000000",
            "side": "BUY",
            "price": 0.4,
            "size": 10,
            "event_ts": 2030,
            "received_at_s": 2030.2,
            "asset": "token-up",
            "raw": {"outcome": "Up", "conditionId": "0xcondition"},
        }
    )

    assert event is not None
    assert event.outcome == "Up"
    assert event.condition_id == "0xcondition"


def _paper_lane_args(**overrides: float) -> Namespace:
    values = {
        "threshold": 0.0,
        "pmax": 0.0,
        "top_k": 0,
        "order_usd": 0.0,
        "max_entry_offset_s": 0.0,
        "slippage_bps": 250.0,
        "min_fill_ratio": 0.999,
    }
    values.update(overrides)
    return Namespace(**values)


def test_paper_lane_defaults_persist_prior_relaxed_params() -> None:
    backtest = {
        "best": {"threshold": 0.5, "pmax": 0.45},
        "parameters": {"top_k": 20, "order_usd": 1.0, "max_entry_offset_s": 240.0},
    }
    prior_state = {"parameters": {"threshold": 0.5, "pmax": 0.55}}
    stale_prior_state = {"parameters": {"threshold": 0.5, "pmax": 0.45}}

    params = _default_params(backtest, _paper_lane_args(), prior_state=prior_state)
    stale_params = _default_params(backtest, _paper_lane_args(), prior_state=stale_prior_state)
    cli_params = _default_params(backtest, _paper_lane_args(pmax=0.6), prior_state=prior_state)

    assert params["threshold"] == 0.5
    assert params["pmax"] == 0.55
    assert stale_params["pmax"] == 0.55
    assert cli_params["pmax"] == 0.6


def test_signal_event_evidence_counts_cumulative_relaxed_signals(tmp_path) -> None:
    signal_log = tmp_path / "signals.jsonl"
    rows = [
        {
            "event": "whale_consensus_signal",
            "signal_id": "base",
            "generated_at": "2026-07-05T15:00:00Z",
            "market_slug": "btc-updown-5m-1",
            "pmax": 0.45,
            "threshold": 0.5,
        },
        {
            "event": "whale_consensus_signal",
            "signal_id": "relaxed",
            "generated_at": "2026-07-05T15:34:10Z",
            "market_slug": "btc-updown-5m-2",
            "pmax": 0.55,
            "threshold": 0.5,
            "clob_instant_fill_status": "PASS",
        },
    ]
    signal_log.write_text("\n".join(json.dumps(row) for row in rows) + "\n")

    evidence = _load_signal_event_evidence(str(signal_log), baseline={"threshold": 0.5, "pmax": 0.45})

    assert evidence["signal_events"] == 2
    assert evidence["relaxed_signal_events"] == 1
    assert evidence["fillable_signal_events"] == 1
    assert evidence["latest_relaxed_signal"]["signal_id"] == "relaxed"
