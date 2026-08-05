import pytest

from scripts.prepare_ee888f_weekday_admission import ACTIVATE_AT, CANDIDATE_ID, WALLET, prepare


def _candidate() -> dict:
    return {
        "wallet": WALLET,
        "candidate_id": CANDIDATE_ID,
        "history_completeness": "complete",
        "copyable_buy_events": 495,
        "resolved_copyable_events": 495,
        "paper_pnl_usd": 62.781846,
        "roi_pct": 6.214365,
        "denylist_cells": [],
        "source_active": {
            "status": "POLICY_ELIGIBLE_PASS",
            "source_active_tally_status": "PASS",
            "policy_eligible_tally_status": "PASS",
            "source_active_windows": 2,
            "policy_eligible_windows": 2,
        },
        "external_liveness": {"status": "PASS", "reason": "external_liveness_pass"},
        "recommendation": "ADMISSION_PACKET_READY",
        "hour_match_status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
        "temporal_evidence": {
            "status": "PASS_PROVEN_POSITIVE_ACTIVE_SLICE",
            "classification": "FADING",
            "matched_slice": {
                "slice": "weekday",
                "label": "PROVEN-POSITIVE",
                "resolved_trades": 632,
                "roi_pct": 3.8942,
                "pnl_usd": 48.855835,
            },
        },
    }


def _packet() -> dict:
    return {"top_four_way_candidate": _candidate(), "packets": []}


def test_prepare_is_idempotent_and_preserves_a689_pin() -> None:
    a689 = {
        "candidate_id": "runtime_auto_degrade_a6896d11f7",
        "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
        "enabled": True,
    }
    pin = {
        "candidate_id": "runtime_auto_degrade_a6896d11f7",
        "source_wallet": "0xa6896d11f76dfa2820662c1f441496f51553559b",
        "enabled": True,
    }
    overlay = {"members": [a689], "selection_pin": pin}

    updated, evidence = prepare(overlay=overlay, admission=_packet(), generated_at="2026-07-20T17:20:00Z")
    updated, _ = prepare(overlay=updated, admission=_packet(), generated_at="2026-07-20T17:21:00Z")

    ee888f_rows = [row for row in updated["members"] if row["source_wallet"] == WALLET]
    assert len(ee888f_rows) == 1
    member = ee888f_rows[0]
    assert member["candidate_id"] == CANDIDATE_ID
    assert member["activate_not_before_utc"] == ACTIVATE_AT
    assert member["enabled"] is True
    assert "never forced seat" in member["summary"]["activation_scope"]
    assert updated["selection_pin"] == pin
    assert [row for row in updated["members"] if row["source_wallet"] == a689["source_wallet"]] == [a689]
    assert evidence["status"] == "ARMED_AWAITING_WEEKDAY_ACTIVATION"
    assert evidence["a689_pin_unchanged"] is True


def test_prepare_requires_top_four_way_candidate() -> None:
    packet = {"packets": [_candidate()]}
    with pytest.raises(ValueError, match="top_four_way_candidate missing"):
        prepare(overlay={"members": []}, admission=packet, generated_at="x")
