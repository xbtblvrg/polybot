from scripts.prepare_a3e0_weekday_probe import ARM_AT, CANDIDATE_ID, WALLET, prepare


def _packet(*, verdict: str = "PROBE_READY_PENDING_FABLE_AUDIT") -> dict:
    return {
        "wallet": WALLET,
        "history_depth": {"status": "COMPLETE_TO_PREREGISTERED_LOOKBACK"},
        "decision": {"p1_pass": verdict == "PROBE_READY_PENDING_FABLE_AUDIT", "verdict": verdict},
        "temporal_hour_match": {"status": "PASS", "candidate_classification": "WEEKDAY-ONLY"},
        "inputs": {"history_sha256": "abc"},
    }


def test_prepare_is_idempotent_and_preregisters_probe_terms() -> None:
    overlay, evidence = prepare(overlay={"members": []}, packet=_packet(), generated_at="2026-07-20T06:00:00Z")
    overlay, _ = prepare(overlay=overlay, packet=_packet(), generated_at="2026-07-20T06:01:00Z")
    rows = [row for row in overlay["members"] if row["source_wallet"] == WALLET]
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == CANDIDATE_ID
    assert rows[0]["max_order_usd"] == 1.0
    assert rows[0]["activate_not_before_utc"] == ARM_AT
    assert evidence["evaluation_clock"]["target_weekday_hours"] == 72
    assert evidence["paper_shadow_parity"]["status"] == "PREREGISTERED"


def test_prepare_refuses_regressed_deep_packet() -> None:
    try:
        prepare(overlay={"members": []}, packet=_packet(verdict="P1_FAIL_NO_ROTATION"), generated_at="x")
    except ValueError as exc:
        assert "p1_verdict_regressed" in str(exc)
    else:
        raise AssertionError("regressed deep packet must not arm")
