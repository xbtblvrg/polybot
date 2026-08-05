from scripts.report_order149_token_metadata_backfill import build_report


def test_order149_metadata_join_reports_partial_without_synthesis() -> None:
    wallet = "0x" + "4" * 40
    qualification = {"candidates": [{"wallet": wallet, "wide_policy_fingerprint": "fp"}]}
    envelopes = [{"rows": [
        {"wallet": wallet, "attempt_id": "a1", "token_id": "t1"},
        {"wallet": wallet, "attempt_id": "a2", "token_id": "t2"},
    ]}]
    report = build_report(
        qualification=qualification,
        envelopes=envelopes,
        metadata={"t1": {"condition_id": "c", "market_slug": "s", "outcome": "Up"}},
    )
    assert report["pre_registered_branch"] == "E2¹⁰"
    assert report["metadata_resolved"] == 1
    assert report["metadata_unresolved"] == 1
    assert report["unresolved_token_ids"] == ["t2"]
    assert "book" in report["forbidden_fields"]
