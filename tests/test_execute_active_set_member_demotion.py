from __future__ import annotations

import pytest

from scripts.execute_active_set_member_demotion import build_demotion


def test_build_demotion_disables_only_target_and_clears_target_pin():
    target = "0x" + "a6" * 20
    keep_a = "0x" + "32" * 20
    keep_b = "0x" + "c5" * 20
    overlay = {
        "members": [
            {"candidate_id": "target", "source_wallet": target, "enabled": True},
            {"candidate_id": "keep-a", "source_wallet": keep_a, "enabled": True},
            {"candidate_id": "keep-b", "source_wallet": keep_b, "enabled": True},
        ],
        "selection_pin": {"enabled": True, "source_wallet": target},
    }

    updated, report = build_demotion(
        overlay=overlay,
        target_wallet=target,
        preserve_wallets={keep_a, keep_b},
        direction_id="direction",
        reason="proven negative",
        evidence="weekend=-1",
        generated_at="2026-07-25T02:00:00Z",
        canonical_resolved=True,
        canonical_pnl_usd=-1.0,
        resolution_winner="DOWN",
    )

    by_wallet = {row["source_wallet"]: row for row in updated["members"]}
    assert by_wallet[target]["enabled"] is False
    assert by_wallet[target]["status"] == "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
    assert by_wallet[keep_a]["enabled"] is True
    assert by_wallet[keep_b]["enabled"] is True
    assert updated["selection_pin"]["enabled"] is False
    assert report["replacement_selected"] is False
    assert report["single_submitter_preserved"] is True


def test_build_demotion_fails_if_preserved_wallet_is_absent():
    target = "0x" + "a6" * 20
    with pytest.raises(ValueError, match="preserved wallets absent"):
        build_demotion(
            overlay={"members": [{"source_wallet": target, "enabled": True}]},
            target_wallet=target,
            preserve_wallets={"0x" + "32" * 20},
            direction_id="direction",
            reason="proven negative",
            evidence="weekend=-1",
            generated_at="2026-07-25T02:00:00Z",
            canonical_resolved=True,
            canonical_pnl_usd=-1.0,
            resolution_winner="DOWN",
        )


def test_build_demotion_refuses_positive_resolved_fill():
    target = "0x" + "a6" * 20
    with pytest.raises(ValueError, match="pnl_usd < 0"):
        build_demotion(
            overlay={"members": [{"source_wallet": target, "enabled": True}]},
            target_wallet=target,
            preserve_wallets=set(),
            direction_id="direction",
            reason="cost misread as loss",
            evidence="UP winner; cost=1.648473; pnl=3.346899",
            generated_at="2026-07-26T01:00:00Z",
            canonical_resolved=True,
            canonical_pnl_usd=3.346899,
            resolution_winner="UP",
        )


def test_build_demotion_refuses_missing_resolution_winner():
    target = "0x" + "a6" * 20
    with pytest.raises(ValueError, match="resolution winner"):
        build_demotion(
            overlay={"members": [{"source_wallet": target, "enabled": True}]},
            target_wallet=target,
            preserve_wallets=set(),
            direction_id="direction",
            reason="unresolved",
            evidence="no winner",
            generated_at="2026-07-26T01:00:00Z",
            canonical_resolved=True,
            canonical_pnl_usd=-1.0,
            resolution_winner="",
        )


def test_build_demotion_refuses_duplicate_wallet_without_identity_scope():
    target = "0x" + "a6" * 20
    overlay = {
        "members": [
            {
                "source_wallet": target,
                "enabled": False,
                "policy_id": "wide_fp_2163fe3eeb215902c0fd0e90",
                "policy": {
                    "wide_policy_fingerprint": "2163fe3eeb215902c0fd0e90010e82430e67542cd6751699bbab3e51863a7449"
                },
            },
            {
                "source_wallet": target,
                "enabled": True,
                "policy_id": "wide_fp_1f11bb72759c0741fd49a70f",
                "policy": {
                    "wide_policy_fingerprint": "1f11bb72759c0741fd49a70feb4f4700dca15c67278edf64c66e3785bab47461"
                },
            },
        ]
    }
    with pytest.raises(ValueError, match="duplicate target wallet members require"):
        build_demotion(
            overlay=overlay,
            target_wallet=target,
            preserve_wallets=set(),
            direction_id="direction",
            reason="first loss",
            evidence="pnl=-2.45",
            generated_at="2026-07-27T04:40:00Z",
            canonical_resolved=True,
            canonical_pnl_usd=-2.45,
            resolution_winner="DOWN",
        )


def test_build_demotion_identity_scopes_duplicate_wallet_members():
    target = "0x" + "a6" * 20
    fp_old = "2163fe3eeb215902c0fd0e90010e82430e67542cd6751699bbab3e51863a7449"
    fp_new = "1f11bb72759c0741fd49a70feb4f4700dca15c67278edf64c66e3785bab47461"
    overlay = {
        "members": [
            {
                "candidate_id": "shared",
                "source_wallet": target,
                "enabled": False,
                "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
                "policy_id": "wide_fp_2163fe3eeb215902c0fd0e90",
                "policy": {"wide_policy_fingerprint": fp_old},
            },
            {
                "candidate_id": "shared",
                "source_wallet": target,
                "enabled": True,
                "status": "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION",
                "policy_id": "wide_fp_1f11bb72759c0741fd49a70f",
                "policy": {"wide_policy_fingerprint": fp_new},
            },
        ],
        "selection_pin": {
            "enabled": True,
            "source_wallet": target,
            "candidate_id": "shared",
            "pin_id": "policy-choke-rung-b",
        },
    }

    updated, report = build_demotion(
        overlay=overlay,
        target_wallet=target,
        preserve_wallets=set(),
        direction_id="2026-07-27T04:42:00Z-fable-00033f-1f11-first-loss",
        reason="FIRST_CANONICAL_NEGATIVE_RESOLUTION_UNDER_DIRECT_PIN",
        evidence="pnl=-2.45 winner=DOWN",
        generated_at="2026-07-27T04:42:00Z",
        canonical_resolved=True,
        canonical_pnl_usd=-2.45,
        resolution_winner="DOWN",
        target_wide_policy_fingerprint=fp_new,
        cooloff_until="2026-07-28T04:42:00Z",
    )

    members = [
        row
        for row in updated["members"]
        if isinstance(row, dict) and row.get("source_wallet") == target
    ]
    assert len(members) == 2
    by_fp = {
        (row.get("policy") or {}).get("wide_policy_fingerprint"): row for row in members
    }
    assert by_fp[fp_old]["enabled"] is False
    assert by_fp[fp_old]["status"] == "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
    assert by_fp[fp_new]["enabled"] is False
    assert by_fp[fp_new]["status"] == "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
    assert (
        by_fp[fp_new]["mechanical_temporal_loss_demotion"]["wide_policy_fingerprint"]
        == fp_new
    )
    assert updated["selection_pin"]["enabled"] is False
    assert report["wide_policy_fingerprint"] == fp_new
    assert report["target_selection_pin_disabled"] is True
    assert report["cooloff_until"] == "2026-07-28T04:42:00Z"
    assert report["single_submitter_preserved"] is True


def test_build_demotion_selects_only_enabled_duplicate_identity():
    target = "0x" + "a7" * 20
    fingerprint = "5d5a524301303bd4badd9410f002a3be6629587b2d29d4cb9f2ce529d4a79325"
    policy_id = "wide_fp_5d5a524301303bd4badd9410"
    historical = {
        "candidate_id": "shared",
        "source_wallet": target,
        "enabled": False,
        "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
        "policy_id": policy_id,
        "policy": {"wide_policy_fingerprint": fingerprint},
        "mechanical_temporal_loss_demotion": {"direction_id": "older"},
    }
    live = {
        "candidate_id": "shared",
        "source_wallet": target,
        "enabled": True,
        "status": "POLICY_CHOKE_RUNG_DIRECT_EMERGENCY_ADMISSION",
        "policy_id": policy_id,
        "policy": {"wide_policy_fingerprint": fingerprint},
    }
    overlay = {
        "members": [historical, live],
        "selection_pin": {
            "enabled": True,
            "source_wallet": target,
            "candidate_id": "shared",
        },
    }

    updated, report = build_demotion(
        overlay=overlay,
        target_wallet=target,
        preserve_wallets=set(),
        direction_id="fable-first-loss",
        reason="FIRST_CANONICAL_NEGATIVE_RESOLUTION_UNDER_DIRECT_PIN",
        evidence="pnl=-2.40 winner=UP",
        generated_at="2026-07-27T06:52:30Z",
        canonical_resolved=True,
        canonical_pnl_usd=-2.4,
        resolution_winner="UP",
        target_wide_policy_fingerprint=fingerprint,
        target_policy_id=policy_id,
        cooloff_until="2026-07-28T06:52:30Z",
    )

    assert updated["members"][0] == historical
    assert updated["members"][1]["enabled"] is False
    assert updated["members"][1]["status"] == "DEMOTED_FABLE_SUBSTITUTE_ROTATION"
    assert updated["selection_pin"]["enabled"] is False
    assert report["target_was_enabled"] is True
