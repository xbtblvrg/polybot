from scripts.report_copy_event_triggered_cycle_scheduler_closure import build_closure_packet


def test_scheduler_closure_confirms_negative_raw_and_missing_book_evidence():
    state = {
        "kind": "copy_event_triggered_cycle_scheduler_paper_lane",
        "generated_at": "2026-07-17T01:45:00Z",
        "status": "PAPER_CLOCK_MATURE_NO_POSITIVE_RESOLVED_WINDOWS",
        "summary": {
            "paper_clock_rows_landed": 3,
            "paper_clock_rows_resolved": 2,
            "paper_clock_post_fee_would_pnl_usd": -0.5,
        },
        "paper_clock_accumulator": {
            "win": {
                "candidate_id": "candidate_a",
                "source_wallet": "0xabc",
                "market_slug": "btc-updown-5m-1784073600",
                "price": 0.5,
                "paper_order_size_usd": 1.0,
                "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
                "post_fee_would_pnl_usd": 1.0,
            },
            "loss": {
                "candidate_id": "candidate_a",
                "source_wallet": "0xabc",
                "market_slug": "btc-updown-5m-1784073900",
                "price": 0.5,
                "paper_order_size_usd": 1.0,
                "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
                "post_fee_would_pnl_usd": -1.5,
            },
            "pending": {
                "post_fee_would_pnl_status": "PENDING_RESOLUTION_OR_JOIN",
                "post_fee_would_pnl_usd": None,
            },
        },
    }

    packet = build_closure_packet(state, generated_at="2026-07-17T08:00:00Z", price_haircut_ticks=(0.01,))

    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["guard_code_touched"] is False
    assert packet["closure_verdict"] == "R18_FAIL_CONFIRMED_RAW_NEGATIVE"
    assert packet["raw_vs_haircut"]["raw_gate_metric"]["rows"] == 2
    assert packet["raw_vs_haircut"]["raw_gate_metric"]["would_pnl_usd"] == -0.5
    assert packet["raw_vs_haircut"]["adverse_price_haircuts"][0]["would_pnl_usd"] < -0.5
    assert packet["top_of_book_depth_evidence"]["status"] == "NOT_PERSISTED"
    assert packet["top_of_book_depth_evidence"]["missing_book_evidence_rows"] == 2


def test_scheduler_closure_counts_depth_backed_rows():
    state = {
        "summary": {"paper_clock_post_fee_would_pnl_usd": 1.0},
        "paper_clock_accumulator": {
            "booked": {
                "source_wallet": "0xabc",
                "market_slug": "btc-updown-5m-1784073600",
                "price": 0.5,
                "paper_order_size_usd": 1.0,
                "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
                "post_fee_would_pnl_usd": 1.0,
                "top_of_book": {
                    "status": "OK",
                    "book_hash": "hash",
                    "fillable_usd": 1.25,
                },
            }
        },
    }

    packet = build_closure_packet(state, generated_at="2026-07-17T08:00:00Z")

    assert packet["top_of_book_depth_evidence"]["status"] == "DEPTH_EVIDENCE_PRESENT"
    assert packet["top_of_book_depth_evidence"]["book_evidence_rows"] == 1
    assert packet["top_of_book_depth_evidence"]["fillable_at_requested_size_rows"] == 1
    assert packet["top_of_book_depth_evidence"]["depth_backed_raw_would_pnl_usd"] == 1.0
