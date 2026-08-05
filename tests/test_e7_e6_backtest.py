from __future__ import annotations

import json
from argparse import Namespace
from types import SimpleNamespace

from scripts.backtest_e7_spot_open_and_e6_exit import _run_e7_book_verified, compare_e6_exit_orders


def _args(**overrides: object) -> Namespace:
    values = {
        "order_usd": 1.0,
        "e5_paper_state": "",
        "e6_exit_min_flow_usd": 20.0,
        "e6_exit_min_dominance": 0.6,
        "tick_size": 0.01,
    }
    values.update(overrides)
    return Namespace(**values)


def test_e7_book_verified_uses_persisted_depth_and_matching_side(tmp_path) -> None:
    state_path = tmp_path / "e5_state.json"
    state_path.write_text(
        json.dumps(
            {
                "orders": [
                    {
                        "market_slug": "btc-updown-5m-1780000000",
                        "outcome": "Up",
                        "source_intent": {
                            "metadata": {
                                "e5_maker_first_btc5m_v1": {
                                    "market_slug": "btc-updown-5m-1780000000",
                                    "outcome": "Up",
                                    "quote_ts": 1780000230.0,
                                    "top_of_book": {
                                        "status": "OK",
                                        "instant_fill_status": "PASS",
                                        "book_hash": "book",
                                        "avg_fill_price": 0.5,
                                        "best_ask": 0.5,
                                        "fillable_usd": 1.0,
                                    },
                                }
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    report = _run_e7_book_verified(
        _args(e5_paper_state=str(state_path)),
        resolutions={
            "btc-updown-5m-1780000000": {"market_slug": "btc-updown-5m-1780000000", "direction": "UP"}
        },
        klines={1780000000: [1780000000, "100.0"], 1780000240: [1780000240, "100.2"]},
        thresholds_bps=[5.0],
        entry_prices=[0.7],
    )

    assert report["status"] == "BOOK_VERIFIED_COMPLETE"
    assert report["best"]["trades"] == 1
    assert report["best"]["pnl_usd"] == 1.0


def test_e6_exit_comparison_uses_persisted_paper_orders() -> None:
    order = {
        "status": "FILLED",
        "final_status": "FILLED",
        "market_slug": "btc-updown-5m-1780000000",
        "outcome": "Up",
        "condition_id": "condition",
        "filled_size_usd": 8.0,
        "filled_shares": 20.0,
        "source_intent": {
            "metadata": {
                "e6_whale_net_flow_v1": {
                    "market_slug": "btc-updown-5m-1780000000",
                    "outcome": "Up",
                    "event_ts": 1780000010.0,
                }
            }
        },
    }
    events = [
        SimpleNamespace(
            market_slug="btc-updown-5m-1780000000",
            outcome="Down",
            side="BUY",
            price=0.2,
            size=100.0,
            source_usd=20.0,
            event_ts=1780000020.0,
            observed_ts=1780000020.1,
            event_id="rt_exit",
        )
    ]

    report = compare_e6_exit_orders(
        orders=[order],
        events=events,
        resolution_index={"slug_start:1780000000": {"direction": "DOWN", "window_type": "5m"}},
        args=_args(e6_exit_min_flow_usd=20.0, e6_exit_min_dominance=0.6, tick_size=0.01),
    )

    assert report["status"] == "COMPARISON_COMPLETE"
    assert report["hold_summary"]["pnl_usd"] == -8.0
    assert report["exit_hits"] == 1
    assert report["exit_summary"]["pnl_usd"] == 7.8
    assert report["delta_exit_minus_hold_pnl_usd"] == 15.8
