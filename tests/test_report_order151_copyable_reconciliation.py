from scripts.report_order151_copyable_reconciliation import build_report


def test_names_scope_mismatch_and_loser():
    audit = {"source_run_ids": ["a", "b"], "candidates": [{"wallet": "0x1", "wide_policy_fingerprint": "f", "own_policy_replay_copyables": 3}]}
    deadman = {"policy_choke": {"actuator": {"candidate_evidence": {"nearest_frontier": [{"wallet": "0x1", "wide_policy_fingerprint": "f", "direct_source": {"copyable": 0}}]}}}}
    report = build_report(audit=audit, deadman=deadman)
    assert report["wrong_report"] == "order149_rotation_qualification_latest.json"
    assert report["rows"][0]["same_definition_and_window"] is False
    assert report["rows"][0]["historical_run_count"] == 2
