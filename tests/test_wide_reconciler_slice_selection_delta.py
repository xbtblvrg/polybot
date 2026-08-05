from scripts.report_wide_reconciler_slice_selection_delta import build_report


WALLET = "0x0484e64092ba4108c2786b61e6fc052d3bf41b1a"


def test_report_names_slice_adds_and_drops_without_profile_double_veto():
    baseline = {
        "capture_watch_wallets": [
            {"wallet": WALLET, "move_slice_keys": ["060-120|0.50-0.75"]}
        ]
    }
    alpha = {
        "execution_profiles": {
            "profiles_by_wallet": {
                WALLET: {
                    "eligible": False,
                    "move_slices": [
                        {
                            "move_slice_key": "060-120|<=0.25",
                            "mean_edge": 0.02,
                            "median_edge": 0.01,
                            "copyable_rate_pct": 71.0,
                        },
                        {
                            "move_slice_key": "060-120|>0.75",
                            "mean_edge": 0.03,
                            "median_edge": 0.01,
                            "copyable_rate_pct": 77.0,
                        },
                    ],
                }
            }
        }
    }

    report = build_report(
        baseline_manifest=baseline,
        alpha=alpha,
        baseline_path="before.json",
        alpha_path="after.json",
    )

    row = report["rows"][0]
    assert row["slice_count_before"] == 1
    assert row["slice_count_after"] == 2
    assert row["adds"] == ["060-120|<=0.25", "060-120|>0.75"]
    assert row["drops"] == ["060-120|0.50-0.75"]
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
