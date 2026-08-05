from datetime import UTC, datetime

from scripts.report_supply_foreclosure_attribution import build_money_surface, build_report


def test_supply_foreclosures_keep_counts_and_code_provenance() -> None:
    report = build_report(
        {
            "generated_at": "2026-07-29T21:44:09Z",
            "summary": {
                "windows_total": 288,
                "submitted_windows": 187,
                "zero_submission_windows": 101,
                "abstaining_predicates_ranked": [
                    {"predicate": "no_eligible_signal", "windows_foreclosed": 36},
                    {
                        "predicate": "inventory_confirmed_unchanged_no_edge",
                        "windows_foreclosed": 35,
                    },
                ],
            },
        },
        generated_at="now",
    )

    assert [row["windows_foreclosed"] for row in report["foreclosures"]] == [36, 35]
    assert report["combined_windows_foreclosed"] == 71
    assert report["combined_share_of_288_pct"] == 24.652778
    assert all("source" in key for row in report["foreclosures"] for key in row if key.endswith("source"))
    assert report["live_mutation"] is False


def test_money_surface_prices_generation_taxonomy_by_whole_utc_days() -> None:
    predicates = (
        "inventory_residual_gap_below_min_order",
        "drip_min_tranche_exceeds_window_budget",
        "hard_entry_floor_skip",
    )
    rows = []
    resolutions = {}
    for day_offset, day in enumerate(("2026-07-20", "2026-07-21", "2026-07-22")):
        window_start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())
        for predicate_index, predicate in enumerate(predicates):
            slug = f"btc-updown-5m-{window_start + predicate_index * 300}"
            rows.append(
                {
                    "dominant_skip_reason": predicate,
                    "window_start_s": window_start + predicate_index * 300,
                    "market_slug": slug,
                    "condition_id": f"condition-{day_offset}-{predicate_index}",
                    "source_wallet": "0xabc",
                    "outcome": "Up",
                    "source_inventory_vwap": 0.5,
                    "guard_sized_copy_usd": 1.0,
                    "set_generation_id": f"generation-{day_offset}",
                }
            )
            resolutions[f"condition-{day_offset}-{predicate_index}"] = {
                "direction": "UP" if day_offset != 1 else "DOWN",
                "source": "canonical-test",
            }

    surface = build_money_surface(
        {"window_participation": {"rows": rows}},
        resolutions,
        generated_at="2026-07-23T12:00:00+00:00",
    )

    assert surface["generation_taxonomy_rows"] == 9
    assert surface["dev_days"] == ["2026-07-20", "2026-07-21"]
    assert surface["holdout_days"] == ["2026-07-22"]
    assert surface["latest_complete_day"] == "2026-07-22"
    assert len(surface["day_bounded_slices"]) == 9
    for row in surface["predicates"]:
        assert row["generation"]["generation_rows"] == 3
        assert row["generation"]["resolved_rows"] == 3
        assert row["dev"]["would_be_realized_pnl_usd"] == 0.0
        assert row["holdout"]["would_be_realized_pnl_usd"] == 1.0
        assert row["latest_complete_day"]["would_be_realized_pnl_usd"] == 1.0
    assert surface["measurement_only"] is True
    assert surface["live_mutation"] is False
