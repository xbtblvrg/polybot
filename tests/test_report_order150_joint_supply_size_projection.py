from scripts.report_order150_joint_supply_size_projection import build_report


def test_joint_projection_uses_only_recovered_window_fillability() -> None:
    attribution = {
        "elapsed_windows": 4,
        "rows": [
            {"category": "BOOK_OBSERVED_NO_GUARD_REASON", "target_fillable_rate": 1.0},
            {"category": "BOOK_OBSERVED_NO_GUARD_REASON", "target_fillable_rate": 0.5},
            {"category": "NAMED_GUARD_ABSTAIN", "target_fillable_rate": 1.0},
            {"category": "SUBMITTED", "target_fillable_rate": 1.0},
        ],
    }
    depth = {"targets": [{
        "target_usd": 12.138,
        "vwap_impact_bps_vs_best_ask": {"p50": 10.0},
    }]}
    report = build_report(attribution=attribution, depth=depth)
    assert report["recovered_windows_observed"] == 2
    assert report["effective_fillable_window_equivalents_observed"] == 1.5
    assert report["projected_recovered_windows_per_288"] == 144.0
    assert report["projected_effective_fillable_windows_per_288"] == 108.0
    assert report["median_impact_bps"] == 10.0
    assert report["impact_adjusted_gross_roi_bps"] == 276.06
    assert "net_edge_bps" not in report
    assert report["active_live_cap_usd"] == 2.5
    assert report["fee_usd_per_fill"] == 0.04441418
