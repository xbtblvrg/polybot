from scripts.report_fee_edge_decomposition import EXPERIMENT_ID, build_report


def _row(window: int, *, price: float = 0.5, pnl: float = 1.0, wallet: str = "0xabc") -> dict:
    return {
        "market_slug": f"btc-updown-5m-{window}",
        "window_start_s": float(window),
        "observed_ts": float(window + 100),
        "winning_observed_ts": float(window + 100),
        "intent_id": f"i-{window}",
        "winning_intent_id": f"i-{window}",
        "source_wallet": wallet,
        "winning_source_wallet": wallet,
        "limit_price": price,
        "shares": 2.0,
        "expected_fee_usd": 0.1,
        "extra_would_submit_window": True,
        "realized_paper_outcome": {"status": "RESOLVED", "paper_pnl_usd": pnl},
    }


def test_build_report_finds_preregistered_positive_slice_at_100_windows() -> None:
    rows = [_row(1000 + index * 300) for index in range(100)]
    report = build_report(
        {"generated_at": "2026-07-20T16:00:00Z", "fee_gated_measurement_rows": rows, "rows": rows},
        {"experiment_id": EXPERIMENT_ID, "registered_at": "2026-07-20T15:59:00Z"},
    )
    assert report["verdict"] == "WINNER"
    assert report["winner_count"] == 8
    assert all(item["n_resolved_windows"] == 100 for item in report["winners"])
    assert report["measurement_only"] is True
    assert report["live_mutation"] is False


def test_build_report_dedupes_same_slice_window_and_excludes_invalid_time() -> None:
    first = _row(1000, pnl=2.0)
    duplicate = {**first, "observed_ts": 1101.0, "winning_observed_ts": 1101.0, "intent_id": "later"}
    invalid = _row(1300)
    invalid["observed_ts"] = 1700.0
    invalid["winning_observed_ts"] = 1700.0
    report = build_report(
        {"fee_gated_measurement_rows": [duplicate, first, invalid], "rows": []},
        {"registered_at": "2026-07-20T15:59:00Z"},
    )
    cohort = report["cohorts"]["fee_cal_measured"]
    price_slice = next(item for item in cohort["axes"]["entry_price_band"] if item["slice"] == "[0.4,0.6)")
    assert price_slice["n_resolved_windows"] == 1
    assert price_slice["pre_fee_pnl_usd"] == 2.0
    assert cohort["excluded_rows"] == {"invalid_seconds_remaining": 1}
    assert report["verdict"] == "NONE"
