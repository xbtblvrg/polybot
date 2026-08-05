import scripts.report_pinned_tranche_economics as report
import pytest


def test_pinned_tranche_economics_aggregates_only_pinned_resolved_fills(tmp_path):
    live_state = {
        "orders": [
            {
                "order_id": "pin-win",
                "intent_id": "i1",
                "market_slug": "btc-updown-5m-1",
                "submitted_at": "2026-07-16T08:00:00Z",
                "final_status": "FILLED",
                "requested_size_usd": 2.4,
                "limit_price": 0.49,
                "source_intent": {
                    "metadata": {
                        "min_live_floor_pin": True,
                        "min_live_floor_pin_usd": 2.4,
                        "min_live_floor_pin_direction_id": report.PIN_DIRECTION_ID,
                    }
                },
            },
            {
                "order_id": "pin-loss",
                "intent_id": "i2",
                "market_slug": "btc-updown-5m-2",
                "submitted_at": "2026-07-16T08:05:00Z",
                "final_status": "FILLED",
                "requested_size_usd": 2.35,
                "limit_price": 0.48,
                "source_intent": {"metadata": {"min_live_floor_pin": True}},
            },
            {
                "order_id": "pin-reject",
                "market_slug": "btc-updown-5m-3",
                "submitted_at": "2026-07-16T08:10:00Z",
                "final_status": "REJECTED",
                "requested_size_usd": 2.4,
                "source_intent": {"metadata": {"min_live_floor_pin": True}},
            },
            {
                "order_id": "plain",
                "market_slug": "btc-updown-5m-4",
                "submitted_at": "2026-07-16T08:15:00Z",
                "final_status": "FILLED",
                "requested_size_usd": 2.4,
                "source_intent": {"metadata": {"min_live_floor_pin": False}},
            },
        ]
    }
    scorecard = {
        "kind": "wallet_copy_daily_scorecard",
        "generated_at": "2026-07-16T08:20:00Z",
        "day_utc": "2026-07-16",
        "canonical_pnl_truth": {
            "events": [
                {
                    "order_id": "pin-win",
                    "market_slug": "btc-updown-5m-1",
                    "submitted_at": "2026-07-16T08:00:00Z",
                    "status": "FILLED",
                    "resolved": True,
                    "cost_usd": 2.4,
                    "payout_usd": 5.0,
                    "pnl_usd": 2.6,
                    "price_bucket": "01_25_50",
                },
                {
                    "order_id": "pin-loss",
                    "market_slug": "btc-updown-5m-2",
                    "submitted_at": "2026-07-16T08:05:00Z",
                    "status": "FILLED",
                    "resolved": True,
                    "cost_usd": 2.35,
                    "payout_usd": 0.0,
                    "pnl_usd": -2.35,
                    "price_bucket": "01_25_50",
                },
                {
                    "order_id": "plain",
                    "market_slug": "btc-updown-5m-4",
                    "submitted_at": "2026-07-16T08:15:00Z",
                    "status": "FILLED",
                    "resolved": True,
                    "cost_usd": 2.4,
                    "payout_usd": 5.0,
                    "pnl_usd": 2.6,
                    "price_bucket": "01_25_50",
                },
            ]
        },
    }

    out = report.build_report(
        live_state=live_state,
        scorecard=scorecard,
        scorecard_path=tmp_path / "scorecard.json",
        live_state_path=tmp_path / "live.json",
        output_path=tmp_path / "out.json",
        trigger_n=2,
        probe_trigger_usd=-8.0,
    )

    assert out["status"] == "TRIGGER_MET_PACKET_READY"
    assert out["summary"]["pinned_orders"] == 3
    assert out["summary"]["pinned_status_counts"] == {"FILLED": 2, "REJECTED": 1}
    assert out["summary"]["resolved_pinned_fills"] == 2
    assert out["summary"]["pnl_usd"] == 0.25
    assert out["summary"]["wins"] == 1
    assert out["summary"]["losses"] == 1
    assert out["summary"]["win_rate_pct"] == 50.0
    assert out["summary"]["requested_payout_usd"] == pytest.approx(9.895833)
    assert out["summary"]["breakeven_win_rate_pct"] == pytest.approx(48.000002)
    assert out["summary"]["wilson_95_lower_bound_win_rate_pct"] == pytest.approx(9.453121)
    assert out["summary"]["wilson_lower_bound_gt_breakeven"] is False
    assert out["summary"]["distance_to_probe_trigger_usd"] == 8.25
    assert out["summary"]["sizing_gate_20_30z"]["resolved_pinned_n_gte_50"] is False
    assert out["summary"]["sizing_gate_20_30z"]["all_criteria_met"] is False
    assert out["summary"]["worst_fill"]["order_id"] == "pin-loss"
