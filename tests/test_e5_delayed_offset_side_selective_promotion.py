from scripts.report_e5_delayed_offset_side_selective_promotion import build_packet


def test_delayed_offset_cells_never_pool_and_old_inventory_fails_closed() -> None:
    orders = []
    scored = []
    for index in range(50):
        order_id = f"o{index}"
        orders.append(
            {
                "order_id": order_id,
                "final_status": "FILLED",
                "outcome": "Up",
                "limit_price": 0.4,
                "filled_shares": 5,
                "source_intent": {"intent_id": order_id},
                "maker_quote": {
                    "quote_ts": 1100 + index,
                    "window_start_s": 1000,
                    "window_end_s": 1300,
                    "enforced_no_fallback_book": True,
                    "top_of_book": {"status": "OK", "book_hash": f"h{index}"},
                },
            }
        )
        scored.append(
            {
                "order_id": order_id,
                "resolved": True,
                "shares": 5,
                "pnl_usd": 1,
                "resolution": {"source": "gamma"},
            }
        )
    orders.append(
        {
            "order_id": "old",
            "final_status": "FILLED",
            "outcome": "Down",
            "limit_price": 0.4,
            "maker_quote": {
                "quote_ts": 1200,
                "window_start_s": 1000,
                "window_end_s": 1300,
                "enforced_no_fallback_book": True,
                "top_of_book": {"status": "OK", "book_hash": "old"},
            },
        }
    )
    packet = build_packet(
        {
            "orders": orders,
            "resolution_scoring": {"scored_orders": scored},
            "book_aware_summary": {"copyintent_parity_violations": 0},
        },
        now_s=2000,
    )
    by_cell = {row["cell"]: row for row in packet["cells"]}
    assert by_cell["60_180_UP"]["pass"] is True
    assert by_cell["180_270_DOWN"]["unresolved_old_fills"] == 1
    assert by_cell["180_270_DOWN"]["pass"] is False
    assert packet["selected_cell"] == "60_180_UP"
