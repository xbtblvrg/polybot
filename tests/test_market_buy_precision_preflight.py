from scripts import run_wallet_copy_live_execution as live
from scripts.report_market_buy_precision_counterfactual import build_report
from src.wallet_copy.execution import intent_to_trade_executor_decision
from src.wallet_copy.models import CopyIntent


def _intent(price=0.4633, *, size=1.0, source_price=None):
    metadata = {
        "copy_model": "drip",
        "wallet_copy_policy": {"effective_live_cap_usd": 1.0},
    }
    if source_price is not None:
        metadata["drift_buffer"] = {"source_limit_price": source_price}
    return CopyIntent(
        source_wallet="0xf418d3a1a941292f9c8707d62a14980c5beb95a3", wallet_name="f418", source_event_id="we_1",
        condition_id="0xcond", market_slug="btc-updown-5m-1784630000", outcome="Up", side="YES",
        limit_price=price, wallet_usdc_size=10.0, copy_size_usd=size, shares=size / price,
        observed_ts=1784630001.0, event_ts=1784630001.0, api_latency_s=0.1, token_id="tok",
        metadata=metadata,
    )


def test_preflight_suppresses_prime_price_and_records_nearest_tick():
    accepted, summary = live._apply_market_buy_precision_preflight(
        [_intent()], min_order_usd=1.0, hard_max_buy_price=0.5,
        max_chase_ticks=1, chase_max_price=0.5, chase_tick_size=0.01,
    )
    assert accepted == []
    row = summary["filtered_intents_detail"][0]
    assert row["effective_chase_price"] == 0.4733
    assert row["precision_safe_amount_usd"] == 1.41
    assert row["nearest_executable_tick"] == 0.48
    assert row["nearest_executable_amount_usd"] == 1.02


def test_preflight_suppresses_cap_clamped_passive_below_venue_five_share_minimum():
    accepted, summary = live._apply_market_buy_precision_preflight(
        [_intent(price=0.49, size=2.45, source_price=0.38)],
        min_order_usd=1.0,
        hard_max_buy_price=0.5,
        max_chase_ticks=1,
        chase_max_price=0.5,
        chase_tick_size=0.01,
        enable_maker_fallback=True,
        passive_at_source_sealed=False,
    )

    assert accepted == []
    assert summary["blocked_intents"] == 1
    assert summary["passive_source_intents"] == 0


def test_preflight_routes_cap_clamped_passive_at_or_above_venue_minimum():
    accepted, summary = live._apply_market_buy_precision_preflight(
        [_intent(price=0.49, size=2.45, source_price=0.19)],
        min_order_usd=1.0,
        hard_max_buy_price=0.5,
        max_chase_ticks=1,
        chase_max_price=0.5,
        chase_tick_size=0.01,
        enable_maker_fallback=True,
        passive_at_source_sealed=False,
    )

    assert len(accepted) == 1
    intent = accepted[0]
    assert intent.limit_price == 0.19
    assert intent.shares == 5.27
    assert intent.copy_size_usd == 1.0013
    assert summary["blocked_intents"] == 0
    assert summary["passive_source_intents"] == 1
    capsule = intent.metadata["precision_requires_passive_source"]
    assert capsule["pre_clamp_copy_size_usd"] == 2.45
    assert capsule["effective_cap_usd"] == 1.0
    decision = intent_to_trade_executor_decision(intent, clob_token_ids=["tok", "no"])
    assert decision["order_type"] == "POLICY_TERMINAL"
    assert decision["post_only_strict"] is False
    assert decision["allow_passive_precision_below_min_shares"] is False
    assert (
        decision["strategy_reason"]
        == "entry_price_band_closed_negative_holdout"
    )
    assert (
        decision["terminal_stage"]
        == "entry_price_band_closed_negative_holdout"
    )
    assert decision["entry_price_band_closed_negative_holdout"]["entry_price"] == 0.19


def test_in_band_passive_source_still_closes_at_passive_policy_gate():
    intent = _intent(price=0.30, size=1.50)
    intent = CopyIntent.from_dict(
        {
            **intent.asdict(),
            "metadata": {
                **intent.metadata,
                "precision_requires_passive_source": {
                    "status": "unit",
                    "execution_path": "direct_post_only_gtc_at_source",
                    "passive_price": 0.30,
                    "original_source_price": 0.30,
                    "buffered_limit_price": 0.31,
                    "max_copy_price": 0.32,
                    "best_ask": 0.31,
                },
            },
        }
    )

    decision = intent_to_trade_executor_decision(intent, clob_token_ids=["tok", "no"])

    assert decision["order_type"] == "POLICY_TERMINAL"
    assert decision["post_only_strict"] is False
    assert decision["strategy_reason"] == "passive_at_source_lane_closed"
    assert decision["terminal_stage"] == "passive_at_source_lane_closed"


def test_sealed_passive_holdout_keeps_sub_five_share_01a_as_taker():
    intent = _intent(price=0.30, size=1.24, source_price=0.30)
    intent = CopyIntent.from_dict(
        {
            **intent.asdict(),
            "metadata": {
                **intent.metadata,
                "wallet_copy_policy": {
                    "effective_live_cap_usd": 1.24,
                    "maker_min_share_funding_cap_usd": 2.45,
                    "maker_min_share_original_policy_cap_usd": 4.0,
                },
            },
        }
    )

    accepted, summary = live._apply_market_buy_precision_preflight(
        [intent],
        min_order_usd=1.0,
        hard_max_buy_price=0.32,
        max_chase_ticks=1,
        chase_max_price=0.32,
        chase_tick_size=0.01,
        enable_maker_fallback=True,
        passive_at_source_sealed=True,
    )

    assert len(accepted) == 1
    emitted = accepted[0]
    assert emitted.limit_price == intent.limit_price
    assert emitted.copy_size_usd == intent.copy_size_usd
    assert emitted.shares == intent.shares
    assert "precision_requires_passive_source" not in emitted.metadata
    assert emitted.metadata["precision_taker_first_passive_sealed"]["execution_path"] == (
        "taker_first_existing_limit"
    )
    assert summary["taker_eligible_emitted"] == 1
    assert summary["passive_source_intents"] == 0
    decision = intent_to_trade_executor_decision(emitted, clob_token_ids=["tok", "no"])
    assert decision["order_type"] != "POLICY_TERMINAL"
    assert decision["strategy_reason"] != "passive_at_source_lane_closed"


def test_entry_band_gate_is_min_inclusive_and_max_exclusive():
    for price in (0.25, 0.319999):
        decision = intent_to_trade_executor_decision(
            _intent(price=price), clob_token_ids=["tok", "no"]
        )
        assert decision["strategy_reason"] != "entry_price_band_closed_negative_holdout"

    for price in (0.249999, 0.32, 0.50):
        decision = intent_to_trade_executor_decision(
            _intent(price=price), clob_token_ids=["tok", "no"]
        )
        assert decision["order_type"] == "POLICY_TERMINAL"
        assert decision["strategy_reason"] == "entry_price_band_closed_negative_holdout"
        assert decision["entry_price_band_closed_negative_holdout"]["entry_price"] == price


def test_preflight_keeps_infeasible_non_source_price_intent_suppressed():
    accepted, summary = live._apply_market_buy_precision_preflight(
        [_intent(price=0.62, size=2.45, source_price=0.62)],
        min_order_usd=1.0,
        hard_max_buy_price=0.7,
        max_chase_ticks=1,
        chase_max_price=0.7,
        chase_tick_size=0.01,
        enable_maker_fallback=True,
    )

    assert accepted == []
    assert summary["passive_source_intents"] == 0
    assert summary["blocked_intents"] == 1


def test_counterfactual_waits_for_boundary_and_prices_nearest_tick_post_fee():
    event = {
        "event": "wallet_copy_live_market_buy_precision_infeasible_reject", "intent_id": "ci_1",
        "market_slug": "btc-updown-5m-1", "outcome": "Up", "nearest_executable_tick": 0.48,
        "nearest_executable_amount_usd": 1.02, "nearest_tick_price_delta": 0.0067,
    }
    report = build_report(
        event_rows=[event, event], resolution_rows=[{"market_slug": "btc-updown-5m-1", "direction": "UP"}],
        generated_at="2026-07-21T11:00:00Z", min_resolved=2,
    )
    assert report["unique_suppressed_intents"] == 1
    assert report["resolved_suppressed_windows"] == 1
    assert report["post_fee_counterfactual_pnl_usd"] > 0
    assert report["status"] == "ACCRUING"
