from scripts.report_order146_own_policy_replay import build_report


def test_order146_stops_on_missing_replay_fields() -> None:
    wallet = "0x" + "1" * 40
    deadman = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "nearest_frontier": [
                        {
                            "wallet": wallet,
                            "source_generation": "g1",
                            "wide_policy_fingerprint": "own",
                            "f2_copyable_policy_id": "base",
                            "direct_source": {
                                "attempts_by_continuity_window": [2, 3],
                                "copyables_by_continuity_window": [0, 1],
                            },
                        }
                    ]
                }
            }
        }
    }
    packets = {
        "g1": {
            "rows": [
                {
                    "wallet": wallet,
                    "f1_f4_terminal": {"terminal": "REFUSED_ALPHA_PROFILE_FILTER"},
                }
            ]
        }
    }
    report = build_report(deadman=deadman, packets=packets)
    assert report["status"] == "ORDER146_MISSING_FIELDS_STOP"
    assert report["pre_registered_branch"] == "E3''''''"
    assert "market_slug" in report["missing_field_list"]
    assert report["wallets"][0]["requested_recorded_attempts"] == 5
    assert report["wallets"][0]["base_observed_copyables"] == 1
    assert report["wallets"][0]["own_policy_replay_copyables"] is None
