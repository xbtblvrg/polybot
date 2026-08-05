from datetime import datetime, timezone

import pytest

from scripts.report_two_arm_concentration_decomposition import (
    LIVE_CELL_82C8,
    SEAT_CLOCK_START,
    WALLET_31C2,
    WALLET_82C8,
    _summarize,
    apply_seat_maturity,
    build_report,
)


def _history_event(
    event_id: str, *, market: int, price: float, outcome: str
) -> dict:
    return {
        "source_wallet": WALLET_31C2,
        "event_id": event_id,
        "transaction_hash": event_id,
        "token_id": f"token-{event_id}",
        "condition_id": f"condition-{event_id}",
        "market_slug": f"btc-updown-5m-{market}",
        "event_ts": market + 30,
        "price": price,
        "outcome": outcome,
        "action": "BUY",
        "asset": "BTC",
        "duration": "5m",
    }


def _resolution(event_id: str, market: int, direction: str) -> dict:
    return {
        "condition_id": f"condition-{event_id}",
        "market_slug": f"btc-updown-5m-{market}",
        "yes_token": f"token-{event_id}" if direction == "UP" else "other",
        "no_token": f"token-{event_id}" if direction == "DOWN" else "other",
        "direction": direction,
    }


def _seat_order(
    order_id: str, *, market: int, price: float, pnl: float
) -> dict:
    return {
        "wallet": WALLET_82C8,
        "order_id": order_id,
        "recorded_at": SEAT_CLOCK_START,
        "market_slug": f"btc-updown-5m-{market}",
        "fill_price": price,
        "post_fee_pnl_usd": pnl,
        "source_event_ts": market + 30,
        "resolved": True,
        "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
    }


def test_summary_uses_net_pnl_denominator_like_951b_audit() -> None:
    summary = _summarize(
        [
            {
                "market_slug": "btc-updown-5m-1",
                "fill_price": 0.2,
                "post_fee_pnl_usd": 9.0,
            },
            {
                "market_slug": "btc-updown-5m-2",
                "fill_price": 0.4,
                "post_fee_pnl_usd": -1.0,
            },
        ]
    )

    assert summary["post_fee_pnl_usd"] == 8.0
    assert summary["top_1_market_share_of_total_pnl_pct"] == 112.5
    assert summary["single_best_trade_share_of_total_pnl_pct"] == 112.5
    assert summary["distinct_markets"] == 2


def test_summary_does_not_publish_signed_share_for_negative_total() -> None:
    summary = _summarize(
        [
            {
                "market_slug": "btc-updown-5m-1",
                "fill_price": 0.2,
                "post_fee_pnl_usd": 1.0,
            },
            {
                "market_slug": "btc-updown-5m-2",
                "fill_price": 0.4,
                "post_fee_pnl_usd": -2.0,
            },
        ]
    )

    assert summary["post_fee_pnl_usd"] == -1.0
    assert summary["top_1_market_share_of_total_pnl_pct"] is None


def test_report_marks_concentrated_seat_and_precommits_park() -> None:
    history = {
        "events": [
            _history_event("win-a", market=1000, price=0.4, outcome="Up"),
            _history_event("win-b", market=2000, price=0.4, outcome="Up"),
            _history_event("loss", market=3000, price=0.4, outcome="Up"),
        ]
    }
    resolutions = [
        _resolution("win-a", 1000, "UP"),
        _resolution("win-b", 2000, "UP"),
        _resolution("loss", 3000, "DOWN"),
    ]
    seat_orders = [
        *[
            _seat_order(
                f"seat-win-{index}", market=4000, price=0.2, pnl=3.9
            )
            for index in range(3)
        ],
        _seat_order("seat-loss", market=5000, price=0.6, pnl=-1.0),
    ]

    report = build_report(
        history=history,
        resolutions=resolutions,
        wide_state={"orders": seat_orders},
        runtime_state={
            "runtime_active_set": {
                "total_loss_auto_disable": {
                    "disabled_members": [
                        {"candidate_id": LIVE_CELL_82C8}
                    ]
                }
            }
        },
    )

    seat = next(
        arm for arm in report["arms"] if arm["arm"] == "82c8_forward_seat_clock"
    )
    assert seat["base"]["resolved"] == 4
    assert seat["base"]["top_1_market_share_of_total_pnl_pct"] > 50.0
    assert seat["verdict"]["classification"] == "CONCENTRATION_ARTEFACT"
    precommit = report["seat_82c8_maturity_precommit"]
    assert precommit["decision"] == "PARK_AT_MATURITY"
    assert precommit["third_48h_clock_allowed"] is False

    ready_state = {
        "lanes": [
            {
                "wallet": WALLET_82C8,
                "standby_evidence_started_at": SEAT_CLOCK_START,
            },
            {"wallet": "0x" + "1" * 40},
        ],
        "standby_adjudications": [],
    }
    decision_at = datetime(2026, 7, 31, 6, 44, 50, tzinfo=timezone.utc)
    updated = apply_seat_maturity(ready_state, report, now=decision_at)
    assert [row["wallet"] for row in updated["lanes"]] == ["0x" + "1" * 40]
    assert (
        updated["terminal_82c8_forward_seat_decision"]["status"]
        == "PARK_CONCENTRATED_PAPER_SEAT"
    )
    assert apply_seat_maturity(updated, report, now=decision_at) == updated


def test_forward_seat_refuses_predeadline_mutation() -> None:
    report = {
        "seat_82c8_maturity_precommit": {
            "decision_at": "2026-07-31T06:44:50Z",
            "decision": "PARK_AT_MATURITY",
        }
    }
    with pytest.raises(ValueError, match="not due"):
        apply_seat_maturity(
            {"lanes": []},
            report,
            now=datetime(2026, 7, 31, 6, 44, 49, tzinfo=timezone.utc),
        )


def test_price_floors_exclude_sub_floor_rows() -> None:
    summary = _summarize(
        [
            {
                "market_slug": "btc-updown-5m-1",
                "fill_price": 0.01,
                "post_fee_pnl_usd": 98.0,
            },
            {
                "market_slug": "btc-updown-5m-2",
                "fill_price": 0.2,
                "post_fee_pnl_usd": -1.0,
            },
        ],
        min_price=0.05,
    )

    assert summary["resolved"] == 1
    assert summary["post_fee_pnl_usd"] == -1.0
    assert summary["price_distribution"]["count_lt_0_05"] == 0
