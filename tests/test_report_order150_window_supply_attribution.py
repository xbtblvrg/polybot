from scripts.report_order150_window_supply_attribution import build_report


def test_order150_attributes_all_288_windows_exactly_once() -> None:
    start = 1_800_000_000
    slugs = [f"btc-updown-5m-{start + offset * 300}" for offset in range(4)]
    report = build_report(
        day_start_s=start,
        now_s=start + 1200,
        ledger={"orders": [{"market_slug": slugs[0]}]},
        participation={
            slugs[1]: {"skip_reasons": {"no_eligible_signal": 1}},
        },
        book_windows={
            slugs[2]: {"snapshot_count": 2, "assets": {"a"}, "in_band_snapshot_count": 2, "target_fillable_snapshot_count": 1},
        },
    )
    assert report["integrity"]["exactly_one_category_per_window"] is True
    assert report["category_counts"] == {
        "BOOK_OBSERVED_NO_GUARD_REASON": 1,
        "FUTURE_PENDING": 284,
        "NAMED_GUARD_ABSTAIN": 1,
        "NO_BOOK_OR_GUARD_EVIDENCE": 1,
        "SUBMITTED": 1,
    }
    assert report["book_observed_unreasoned_windows_with_12p138_fillability"] == 1
    assert report["rows"][2]["target_fillable_rate"] == 0.5
