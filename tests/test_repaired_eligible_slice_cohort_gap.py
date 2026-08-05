from scripts.report_repaired_eligible_slice_cohort_gap import build_report


WALLET = "0x31393e2f8333d4a6e2e31ca6424defe8b6cb9e2e"


def test_reports_repairable_outside_cohort_bindings_without_authority() -> None:
    report = build_report(
        alpha={
            "execution_profiles": {
                "profiles_by_wallet": {
                    WALLET: {
                        "move_slices": [{
                            "eligible": True,
                            "move_slice_key": "060-120|>0.75",
                            "fill_sample": 26,
                            "copyable_rate_pct": 73.076923,
                            "mean_edge": 0.01,
                        }]
                    }
                }
            }
        },
        cohort_manifest={"manifest_id": "m1", "capture_watch_wallets": []},
        temporal={"rows": []},
        frontier_checksum="frontier1",
        source_packet_sha256="packet1",
    )

    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["promotion_authority"] is False
    assert len(report["rows"]) == 3
    assert {row["binding_exclusion"] for row in report["rows"]} == {
        "not_in_cohort_manifest",
        "no_direct_source_generation",
        "no_temporal_row",
    }
    assert all(row["repairable_by_next_wide_manifest"] for row in report["rows"])
    assert all(row["frontier_checksum"] == "frontier1" for row in report["rows"])
    assert report["summary"] == {
        "eligible_outside_cohort_wallets": 1,
        "binding_rows": 3,
        "repairable_rows": 3,
        "terminal_rows": 0,
        "all_rows_emitted": True,
    }


def test_excludes_eligible_wallet_already_in_cohort() -> None:
    report = build_report(
        alpha={
            "execution_profiles": {
                "profiles_by_wallet": {
                    WALLET: {"move_slices": [{"eligible": True}]}
                }
            }
        },
        cohort_manifest={"capture_watch_wallets": [{"wallet": WALLET}]},
        temporal={"rows": []},
        frontier_checksum="frontier1",
        source_packet_sha256="packet1",
    )

    assert report["rows"] == []
    assert report["summary"]["eligible_outside_cohort_wallets"] == 0
