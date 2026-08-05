from scripts.report_order149_gamma_metadata_recovery import build_report, candidate_starts


def test_gamma_recovery_is_fenced_and_publishes_residual() -> None:
    starts = candidate_starts(
        unresolved={"t1"},
        envelopes=[{"rows": [{"token_id": "t1", "source_event_ts": 601.0}]}],
    )
    assert 600 in starts
    report = build_report(
        unresolved={"t1", "t2"},
        recovered={"t1": {"condition_id": "c", "market_slug": "s", "outcome": "Up", "price": 0.5}},
        starts=starts,
    )
    assert report["recovered"] == {"t1": {"condition_id": "c", "market_slug": "s", "outcome": "Up"}}
    assert report["residual_token_ids"] == ["t2"]
    assert "price" not in report["recovered"]["t1"]
