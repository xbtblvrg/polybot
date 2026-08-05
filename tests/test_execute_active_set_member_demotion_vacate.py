from __future__ import annotations

import pytest

from scripts.execute_active_set_member_demotion_vacate import build_vacate


TARGET = "0x" + "a6" * 20
CANDIDATE = "policy_choke_rung_direct_a6a6a6a6a6"


def _overlay() -> dict:
    return {
        "members": [
            {
                "candidate_id": CANDIDATE,
                "source_wallet": TARGET,
                "enabled": False,
                "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
            }
        ],
        "selection_pin": {
            "candidate_id": CANDIDATE,
            "source_wallet": TARGET,
            "enabled": False,
            "created_at": "2026-07-26T00:42:54Z",
            "expires_at": "2026-07-26T01:42:54Z",
            "disabled_at": "2026-07-26T00:55:56Z",
        },
        "latest_mechanical_temporal_loss_demotion": {
            "status": "APPLIED",
            "target_wallet": TARGET,
        },
    }


def test_build_vacate_restores_original_pin_and_removes_only_false_cooloff():
    keep = "0x" + "13" * 20
    updated, deadman, report = build_vacate(
        overlay=_overlay(),
        deadman={"policy_choke_rung_b_cooloffs": {TARGET: "false", keep: "true"}},
        target_wallet=TARGET,
        candidate_id=CANDIDATE,
        direction_id="direction",
        evidence="resolved=true;pnl_usd=3.346899",
        generated_at="2026-07-26T01:08:00Z",
    )

    member = updated["members"][0]
    assert member["enabled"] is True
    assert updated["selection_pin"]["enabled"] is True
    assert updated["selection_pin"]["expires_at"] == "2026-07-26T01:42:54Z"
    assert "disabled_at" not in updated["selection_pin"]
    assert updated["latest_mechanical_temporal_loss_demotion"]["status"] == "VACATED_FALSE_NEGATIVE"
    assert TARGET not in deadman["policy_choke_rung_b_cooloffs"]
    assert deadman["policy_choke_rung_b_cooloffs"][keep] == "true"
    assert report["pin_expiry_unchanged"] is True


def test_build_vacate_refuses_expired_pin():
    with pytest.raises(ValueError, match="already expired"):
        build_vacate(
            overlay=_overlay(),
            deadman={},
            target_wallet=TARGET,
            candidate_id=CANDIDATE,
            direction_id="direction",
            evidence="positive",
            generated_at="2026-07-26T01:42:54Z",
        )
