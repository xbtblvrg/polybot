from scripts.report_early_01a_executable_subset import (
    SUSPECTED_TWO_SIDED_QUOTING,
    attach_nearest_books,
    build_report,
    canonical_pairs,
)


def test_same_window_multiwallet_pool_refuses_independence_grade() -> None:
    rows = []
    for wallet in ("0x" + "a" * 40, "0x" + "b" * 40):
        rows.append(
            {
                "wallet": wallet,
                "slug": "btc-updown-5m-1000",
                "asset": "asset",
                "timestamp": 1020.0,
                "price": 0.30,
                "size": 1.0,
                "outcome": "UP",
                "winning_outcome": "UP",
                "resolved": True,
            }
        )
    attached = attach_nearest_books(rows, [{"asset_id": "asset", "captured_at_s": 1019.5, "best_bid": 0.30, "best_ask": 0.31}])
    report = build_report(attached, generated_at="now")
    assert report["n_resolved"] == 2
    assert report["n_distinct_windows"] == 1
    assert report["outcome_correlation_ratio"] == 2.0
    assert report["maker_fill_flat_1usd_roi_pct"] == 233.333333
    assert report["taker_best_ask_flat_1usd_roi_pct"] == 222.580645
    assert report["mean_taker_slippage_vs_maker_fill_pp"] == 1.0
    assert report["verdict"] == "OUTCOME_CORRELATED_POOL_NOT_INDEPENDENT"


def test_canonical_pair_is_earliest_and_obeys_fixed_cut() -> None:
    wallet = "0x" + "a" * 40
    base = {"side": "BUY", "slug": "btc-updown-5m-1000", "asset": "asset", "price": 0.30, "size": 1.0, "outcome": "Up"}
    rows = [
        {**base, "timestamp": 1030, "transactionHash": "later"},
        {**base, "timestamp": 1020, "transactionHash": "early"},
        {**base, "timestamp": 1040, "transactionHash": "after-cut"},
    ]
    pairs = canonical_pairs({wallet: rows}, resolutions={"btc-updown-5m-1000": "UP"}, cut_ts=1035)
    assert len(pairs) == 1
    assert pairs[0]["transaction_hash"] == "early"


def test_book_join_is_causal_and_ignores_cheaper_future_snapshot() -> None:
    pair = {
        "wallet": "0x" + "a" * 40,
        "slug": "btc-updown-5m-1000",
        "asset": "asset",
        "timestamp": 1020.0,
        "price": 0.30,
        "size": 1.0,
        "outcome": "UP",
        "winning_outcome": "UP",
        "resolved": True,
    }
    attached = attach_nearest_books(
        [pair],
        [
            {"asset_id": "asset", "captured_at_s": 1019.25, "best_bid": 0.29, "best_ask": 0.31},
            {"asset_id": "asset", "captured_at_s": 1020.25, "best_bid": 0.20, "best_ask": 0.25},
        ],
    )
    assert attached[0]["book_matched"] is True
    assert attached[0]["book_join_direction"] == "causal"
    assert attached[0]["book_lag_s"] == -0.75
    assert attached[0]["best_ask"] == 0.31
    report = build_report(attached, generated_at="now")
    assert report["causal_join_violation_fraction"] == 0.0
    assert report["mean_taker_slippage_vs_maker_fill_pp"] == 1.0
    assert report["lane_decision_status"] == "UNDERPOWERED_RETROSPECTIVE_CAUSAL_SAMPLE"


def test_causal_join_violation_suppresses_roi_fields() -> None:
    row = {
        "wallet": "0x" + "a" * 40,
        "slug": "btc-updown-5m-1000",
        "asset": "asset",
        "timestamp": 1020.0,
        "price": 0.30,
        "size": 1.0,
        "outcome": "UP",
        "winning_outcome": "UP",
        "resolved": True,
        "book_matched": True,
        "book_lag_s": -0.2,
        "book_join_direction": "causal",
        "best_bid": 0.20,
        "best_ask": 0.25,
        "executable": True,
    }
    report = build_report([row], generated_at="now")
    assert report["verdict"] == "CAUSAL_JOIN_VIOLATION"
    assert report["publication_status"] == "QUARANTINED_NON_CAUSAL_BOOK_JOIN"
    assert "taker_best_ask_flat_1usd_roi_pct" not in report


def test_report_discloses_quoting_included_and_excluded_cohorts() -> None:
    quoting_wallet = next(iter(SUSPECTED_TWO_SIDED_QUOTING))
    rows = [
        {
            "wallet": quoting_wallet,
            "slug": "btc-updown-5m-1000",
            "asset": "a",
            "timestamp": 1020.0,
            "price": 0.30,
            "size": 1.0,
            "outcome": "UP",
            "winning_outcome": "UP",
            "resolved": True,
            "book_matched": True,
            "book_lag_s": -1.0,
            "book_join_direction": "causal",
            "best_bid": 0.30,
            "best_ask": 0.31,
            "executable": True,
            "suspected_two_sided_quoting": True,
        }
    ]
    report = build_report(rows, generated_at="now")
    assert report["cohorts"]["including_suspected_two_sided_quoting"]["executable_pairs"] == 1
    assert report["cohorts"]["excluding_suspected_two_sided_quoting"]["executable_pairs"] == 0
