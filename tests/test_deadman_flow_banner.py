from __future__ import annotations

from datetime import datetime, timezone
import json

from scripts.read_deadman_flow_banner import build_banner_state


def test_deadman_banner_reads_fresh_state_without_reexecution(tmp_path):
    state = tmp_path / "order_flow_deadman_state.json"
    state.write_text(
        json.dumps({"status": "INCIDENT_ORDER_FLOW_DEAD", "checked_at": "2026-08-03T00:10:00Z"}),
        encoding="utf-8",
    )

    payload = build_banner_state(
        state_path=state,
        now=datetime(2026, 8, 3, 0, 20, tzinfo=timezone.utc),
        max_age_s=900,
    )

    assert payload["status"] == "INCIDENT_ORDER_FLOW_DEAD"
    assert payload["deadman_banner_age_s"] == 600
    assert payload["deadman_banner_freshness_status"] == "PASS_FRESH_STATE"


def test_deadman_banner_does_not_emit_large_nested_payloads(tmp_path):
    state = tmp_path / "order_flow_deadman_state.json"
    state.write_text(
        json.dumps(
            {
                "status": "INCIDENT_ORDER_FLOW_DEAD",
                "checked_at": "2026-08-03T00:10:00Z",
                "rows": [{"large": "x" * 1000}],
                "policy_choke": {
                    "wallet_policy_diagnostic": "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
                },
                "selected_identity": {
                    "status": "PASS",
                    "source_wallet": "0xabc",
                    "candidate_id": "candidate",
                    "generated_at": "2026-08-03T00:09:00Z",
                    "source": "runtime_member_submittability_crosschecked_top_level",
                    "large_nested_field": ["x" * 1000],
                },
                "selected_identity_resolved": True,
                "active_set_runtime": {
                    "qualified_member_count": 1,
                    "selected_member": {
                        "candidate_id": "candidate",
                        "source_wallet": "0xabc",
                        "policy_id": "policy",
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    payload = build_banner_state(
        state_path=state,
        now=datetime(2026, 8, 3, 0, 20, tzinfo=timezone.utc),
        max_age_s=900,
    )

    assert "rows" not in payload
    assert "policy_choke" not in payload
    assert payload["wallet_policy_diagnostic"] == "WALLET_POLICY_CHOKE_NO_ADMISSIBLE_TARGET"
    assert payload["selected_candidate_id"] == "candidate"
    assert payload["selected_identity_resolved"] is True
    assert payload["selected_identity"]["status"] == "PASS"
    assert "large_nested_field" not in payload["selected_identity"]


def test_deadman_banner_fails_closed_on_stale_state(tmp_path):
    state = tmp_path / "order_flow_deadman_state.json"
    state.write_text(
        json.dumps({"status": "OK", "checked_at": "2026-08-03T00:00:00Z"}),
        encoding="utf-8",
    )

    payload = build_banner_state(
        state_path=state,
        now=datetime(2026, 8, 3, 0, 20, tzinfo=timezone.utc),
        max_age_s=900,
    )

    assert payload["status"] == "INCIDENT_ORDER_FLOW_DEAD_UNVERIFIED"
    assert payload["fail_closed_reason"] == "state_stale"


def test_deadman_banner_fails_closed_on_missing_state(tmp_path):
    payload = build_banner_state(
        state_path=tmp_path / "missing.json",
        now=datetime(2026, 8, 3, 0, 20, tzinfo=timezone.utc),
        max_age_s=900,
    )

    assert payload["status"] == "INCIDENT_ORDER_FLOW_DEAD_UNVERIFIED"
    assert payload["fail_closed_reason"] == "state_missing_or_unreadable"
