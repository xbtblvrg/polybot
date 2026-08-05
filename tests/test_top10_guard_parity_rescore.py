from scripts.build_top10_guard_parity_rescore import build_report, classify_reject


def _reject_order(reason: str, *, source_price: float = 0.47, best_ask: float = 0.6) -> dict:
    return {
        "final_status": "REJECTED",
        "market_slug": "btc-updown-5m-1783376100",
        "source_intent": {
            "event_ts": 1783375989.0,
            "observed_ts": 1783375989.2,
            "market_slug": "btc-updown-5m-1783376100",
        },
        "fill_estimate": {
            "status": "REJECTED",
            "reject_details": {
                "blocking_reason": reason,
                "source_price": source_price,
                "best_ask": best_ask,
                "book_timestamp": "1783376165179",
            },
        },
    }


def test_slippage_reject_reports_required_bps_and_deep_bucket() -> None:
    row = classify_reject(_reject_order("price_above_slippage_cap"))

    assert row["category"] == "deep_slippage"
    assert row["required_slippage_bps"] > row["marginal_threshold_bps"]
    assert row["parity_taker_fillable"] is False
    assert row["configured_drift_buffer_price"] == 0.05
    assert row["applied_drift_buffer_price"] == 0.01


def test_no_book_live_at_source_is_maker_maybe_and_replay_artifact() -> None:
    row = classify_reject(
        {
            "final_status": "REJECTED",
            "market_slug": "btc-updown-5m-1783376100",
            "source_intent": {
                "event_ts": 1783375989.0,
                "market_slug": "btc-updown-5m-1783376100",
            },
            "fill_estimate": {
                "status": "REJECTED",
                "reject_details": {
                    "blocking_reason": "no_ask_liquidity",
                    "source_price": 0.4,
                    "best_ask": 0.0,
                    "book_timestamp": None,
                },
            },
        }
    )

    assert row["category"] == "maker_fallback_fillable"
    assert row["maker_fallback_maybe_fill"] is True
    assert row["maker_fallback_candidate"] is True
    assert row["replay_artifact_book_expired"] is True


def test_report_keeps_maker_maybe_separate_from_taker_fillability() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    report = build_report(
        replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_a",
                    "paper_replay": {
                        "replay_orders": [
                            {"final_status": "FILLED", "fill_estimate": {"status": "FILLED"}},
                            _reject_order("price_above_slippage_cap", source_price=0.5, best_ask=0.515),
                            _reject_order("no_ask_liquidity", source_price=0.4, best_ask=0.0),
                        ]
                    },
                }
            ]
        },
        clearance_summary={
            "rows": [
                {
                    "wallet": wallet,
                    "clearance_status": "ANALYZE_PARTIAL_SAMPLE",
                    "copyable_rate_pct": 10.0,
                    "copyable_buy_events": 1,
                    "buy_events": 3,
                }
            ]
        },
    )

    row = report["rows"][0]
    assert row["strict_replay_fills"] == 1
    assert row["parity_taker_fillable_orders"] == 1
    assert row["parity_maker_maybe_fill_orders"] == 1
    assert row["maker_fallback_candidate_rejects"] == 1
    assert row["reject_category_counts"]["marginal_slippage"] == 1
    assert row["reject_category_counts"]["maker_fallback_fillable"] == 1
    assert report["summary"]["all_rejects_classified"] is True
    assert report["parity_semantics"]["configured_max_drift_buffer_price"] == 0.05
    assert report["parity_semantics"]["applied_inventory_tick_buffer_price"] == 0.01
