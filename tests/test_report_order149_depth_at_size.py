from scripts.report_order149_depth_at_size import build_report


def test_depth_at_size_reports_full_and_capped_fillability() -> None:
    rows = [{
        "asset_id": "a",
        "asks": [{"price": 0.4, "size": 25}, {"price": 0.41, "size": 100}],
    }, {
        "asset_id": "b",
        "asks": [{"price": 0.6, "size": 100}],
    }]
    report = build_report(rows, targets=(12.0,))
    assert report["snapshot_count"] == 1
    assert report["asset_count"] == 1
    target = report["targets"][0]
    assert target["full_book_fill_rate"] == 1.0
    assert target["within_250bps_of_best_ask_fill_rate"] == 1.0
    assert target["vwap_impact_bps_vs_best_ask"]["p50"] > 0


def test_depth_at_size_refuses_empty_band() -> None:
    report = build_report([{"asset_id": "a", "asks": [{"price": 0.6, "size": 100}]}])
    assert report["verdict"] == "NO_IN_BAND_SNAPSHOTS"
