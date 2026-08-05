from scripts.vacate_generation_invalidated_selection_pin import build_vacate


WALLET = "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"
CANDIDATE = "policy_choke_rung_direct_97a18b5b82"
FINGERPRINT = "d277e7fcd160ead7a5cf014f7d516d5dbec64aa74a0c32ed52c074d0c3a3d7f4"


def test_build_vacate_disables_only_enabled_exact_member_and_archives_pin():
    overlay = {
        "selection_pin": {
            "enabled": True,
            "source_wallet": WALLET,
            "candidate_id": CANDIDATE,
            "created_at": "2026-07-27T15:09:43Z",
            "expires_at": "2026-07-27T16:09:43Z",
        },
        "previous_selection_pins": [{"candidate_id": "older"}],
        "members": [
            {
                "source_wallet": WALLET,
                "candidate_id": CANDIDATE,
                "enabled": False,
                "status": "HISTORICAL_DISABLED",
            },
            {
                "source_wallet": WALLET,
                "candidate_id": CANDIDATE,
                "enabled": True,
                "status": "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION",
            },
            {
                "source_wallet": "0x1111111111111111111111111111111111111111",
                "candidate_id": "other",
                "enabled": True,
            },
        ],
    }

    updated, report = build_vacate(
        overlay,
        wallet=WALLET,
        candidate_id=CANDIDATE,
        fingerprint=FINGERPRINT,
        direction_id="fable-2026-07-27T15:17:37Z",
        evidence="n=1113 post=-111.85",
        residual_order_id="0xorder",
        generated_at="2026-07-27T15:18:00Z",
    )

    assert updated["selection_pin"]["enabled"] is False
    assert updated["selection_pin"]["disabled_wide_policy_fingerprint"] == FINGERPRINT
    target = [
        row
        for row in updated["members"]
        if row.get("source_wallet") == WALLET and row.get("enabled") is True
    ]
    assert target == []
    assert updated["members"][0]["status"] == "HISTORICAL_DISABLED"
    assert updated["members"][2]["enabled"] is True
    assert updated["previous_selection_pins"][-1]["enabled"] is False
    assert report["disabled_members"] == 1
    assert report["residual_order_action"] == "HOLD_NO_CANCEL_NO_CHASE"
