from scripts.report_f418_size_clamp_fee_leak_shadow import F418, build_packet


def _filled_order(*, index: int, intended: float, cost: float, fee: float, win: bool) -> dict:
    price = 0.5
    shares = cost / price
    return {
        "source_wallet": F418,
        "final_status": "FILLED",
        "market_slug": f"btc-updown-5m-{1784592000 + 300 * index}",
        "submitted_at": "2026-07-21T00:10:00Z",
        "limit_price": price,
        "side": "YES",
        "trade_result": {
            "response_filled_size_usd": cost,
            "response_fill_size_shares": shares,
            "market_order_amount_adjustment_usd": intended - cost,
        },
        "expected_vs_realized_fee": {"response_expected_fee_usd": fee},
    }


def test_packet_is_report_only_and_does_not_double_subtract_fee() -> None:
    ledger = {"orders": [_filled_order(index=1, intended=1.0, cost=1.04, fee=0.04, win=False)]}
    resolutions = {
        "slug_start:1784592300": {
            "market_slug": "btc-updown-5m-1784592300",
            "direction": "DOWN",
        }
    }
    packet = build_packet(
        ledger=ledger,
        resolutions=resolutions,
        generated_at="2026-07-21T02:30:00Z",
    )
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["live_mutation"] is False
    assert packet["micro_1usd"]["n"] == 1
    assert packet["micro_1usd"]["post_fee_pnl_usd"] == -1.04
    assert packet["micro_1usd"]["expected_fee_usd"] == 0.04
    assert packet["status"] == "ACCRUE_PREREGISTERED_SAMPLE"


def test_primary_drag_gate_requires_sample_and_significant_worse_micro_ev() -> None:
    orders = []
    resolutions = {}
    for index in range(80):
        micro = index < 40
        intended = 1.0 if micro else 2.5
        cost = 1.04 if micro else 2.6
        fee = 0.06 if micro else 0.10
        win = not micro
        orders.append(_filled_order(index=index, intended=intended, cost=cost, fee=fee, win=win))
        slug = f"btc-updown-5m-{1784592000 + 300 * index}"
        resolutions[f"slug_start:{1784592000 + 300 * index}"] = {
            "market_slug": slug,
            "direction": "UP" if win else "DOWN",
        }
    packet = build_packet(
        ledger={"orders": orders},
        resolutions=resolutions,
        generated_at="2026-07-21T12:00:00Z",
    )
    assert packet["micro_1usd"]["n"] == 40
    assert packet["standing_2_to_2_5usd"]["n"] == 40
    assert packet["comparison"]["sample_gate_crossed"] is True
    assert packet["comparison"]["micro_ev_significantly_worse"] is True
    assert packet["comparison"]["primary_drag_gate_crossed"] is True
    assert packet["status"] == "PASS_FEE_LEAK_PRIMARY_DRAG_READY_FOR_FABLE_HOLDOUT_RULING"
