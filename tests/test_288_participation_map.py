from scripts.report_288_participation_map import build_map


def test_rollover_map_joins_four_canonical_fills_inside_six_elapsed_windows():
    start = 1784937600
    events = [
        {
            "status": "FILLED",
            "resolved": True,
            "market_slug": f"btc-updown-5m-{start + offset * 300}",
            "pnl_usd": 0.1,
        }
        for offset in (0, 1, 3, 5)
    ]
    scorecard = {
        "generated_at": "2026-07-25T00:30:00Z",
        "canonical_pnl_truth": {"events": events},
        "volume_kpi": {
            "canonical_daily": {"windows_filled": 4, "windows_submitted": 4},
            "rows": [
                {
                    "window_start_s": start + offset * 300,
                    "wallet_eligible_orders": 1,
                    "our_submits": 0,
                    "our_fills": 0,
                }
                for offset in range(6)
            ],
        },
    }
    report = build_map(scorecard, day="2026-07-25")
    assert report["summary"]["elapsed_windows"] == 6
    assert report["summary"]["traded_windows"] == 4
    assert report["summary"]["canonical_unique_filled_windows"] == 4
    assert report["summary"]["filled_window_reconciliation_status"] == "PASS"


def test_elapsed_uncovered_windows_are_explicitly_unmeasured() -> None:
    report = build_map(
        {
            "generated_at": "2026-07-25T00:10:00Z",
            "canonical_pnl_truth": {"events": []},
            "volume_kpi": {"rows": []},
        },
        day="2026-07-25",
    )

    elapsed = report["rows"][:2]
    assert all(row["measurement_status"] == "UNMEASURED" for row in elapsed)
    assert all(row["measured_reason"] == "UNMEASURED" for row in elapsed)
    assert all(row["participation_predicate"] for row in elapsed)
    assert report["summary"]["elapsed_unmeasured_windows"] == 2
    assert report["summary"]["elapsed_uncovered_without_classification"] == 0


def test_unmeasured_elapsed_windows_carry_retention_provenance() -> None:
    start = 1784937600
    report = build_map(
        {
            "generated_at": "2026-07-25T00:20:00Z",
            "canonical_pnl_truth": {"events": []},
            "volume_kpi": {
                "source": "data/research/wallet_copy_live_guard_state.json",
                "rows": [{"window_start_s": start + 300}],
            },
        },
        day="2026-07-25",
    )

    unmeasured = [
        row
        for row in report["rows"]
        if row["lifecycle"] == "elapsed" and row["measurement_status"] == "UNMEASURED"
    ]
    assert unmeasured
    assert all(row["row_source_producer"] for row in unmeasured)
    assert all(row["retention_window_start"] for row in unmeasured)
    assert all(row["retention_window_end"] for row in unmeasured)
    assert all(
        row["participation_predicate"]
        in report["summary"]["unmeasured_predicate_closed_set"]
        for row in unmeasured
    )


def test_unmeasured_predicate_concentration_is_reported_and_bounded() -> None:
    start = 1784937600
    report = build_map(
        {
            "generated_at": "2026-07-25T00:20:00Z",
            "canonical_pnl_truth": {"events": []},
            "volume_kpi": {"rows": [{"window_start_s": start + 300}]},
        },
        day="2026-07-25",
    )

    summary = report["summary"]
    assert summary["unmeasured_predicate_counts"] == {
        "producer_ran_and_wrote_no_row": 3,
        "window_precedes_producer_first_run": 1,
    }
    assert summary["max_unmeasured_predicate_concentration_pct"] == 75.0
    assert summary["unmeasured_predicate_concentration_bounded"] is True
