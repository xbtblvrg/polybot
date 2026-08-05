from scripts.run_top10_broad_paper_lane import _shadow_scoring_fields


def test_shadow_scoring_fields_include_book_age_and_fillability() -> None:
    fields = _shadow_scoring_fields(
        normalized={"event_ts": 1_000_000.0},
        result={
            "status": "REJECTED",
            "book": {
                "top_of_book": {
                    "book_timestamp": "1000002500000",
                    "best_ask": 0.46,
                }
            },
        },
        source_price=0.44,
        fetch_s=1003.0,
        receipt_to_fetch_latency_ms=3000.0,
    )

    assert fields["source_ts"] == 1_000_000.0
    assert fields["book_ts"] == 1_000_002.5
    assert fields["book_age_s"] == 2.5
    assert fields["needed_bps"] == 454.545455
    assert fields["taker_fillable"] is False
    assert fields["drift_buffer_taker_fillable"] is True
    assert fields["parity_fillable"] is True


def test_shadow_scoring_fields_mark_maker_candidate_separately() -> None:
    fields = _shadow_scoring_fields(
        normalized={"event_ts": 1000.0},
        result={"status": "REJECTED", "book": {"top_of_book": {"best_ask": 0.56}}},
        source_price=0.44,
        fetch_s=1001.0,
        receipt_to_fetch_latency_ms=1000.0,
    )

    assert fields["taker_fillable"] is False
    assert fields["maker_fallback_candidate"] is True
    assert fields["parity_fillable"] is True
    assert fields["parity_limit_price"] == 0.49
