from scripts.build_frozen_fingerprint_f2_prewarm_shadow import (
    build_shadow,
    select_auto_primary,
)


PRIMARY_WALLET = "0xc1b4bfdc36eaa5c09e7231c61e5e44c5463f624f"
PRIMARY_FP = "c8ee"


def _cell(
    wallet: str,
    fingerprint: str,
    resolved: int,
    pnl: float,
    roi: float,
    *,
    first_half: float | None = None,
    second_half: float | None = None,
):
    return {
        "wide_policy_fingerprint": fingerprint,
        "identity": {"wallet": wallet},
        "evidence_authority": "venue_executable_full_stream_rescore",
        "venue_executable_full_stream_rescore": {
            "resolved": resolved,
            "post_fee_pnl_usd": pnl,
            "roi_pct": roi,
            "first_half_post_fee_pnl_usd": (
                pnl + 1 if first_half is None else first_half
            ),
            "second_half_post_fee_pnl_usd": (
                1 if second_half is None else second_half
            ),
        },
    }


def _frontier(wallet: str, fingerprint: str, f2: int):
    return {
        "wallet": wallet,
        "wide_policy_fingerprint": fingerprint,
        "fresh_own_source_buy_rows_30m": f2,
        "checks": {
            "f4_external_liveness": True,
            "own_evidenced_policy_available": True,
            "not_terminal_park_red_clock_or_measured_loser": True,
        },
    }


def test_shadow_prewarms_primary_and_ranks_non_cooloff_backup_first():
    backup = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
    cooloff = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    evidence = {
        "generated_at": "2026-07-25T23:50:00Z",
        "cells": [
            _cell(PRIMARY_WALLET, PRIMARY_FP, 198, 11.68, 5.9),
            _cell(backup, "bf33", 125, 32.26, 25.8),
            _cell(cooloff, "d308", 228, 21.25, 9.3),
        ],
        "freeze_overrides": {
            PRIMARY_WALLET: {"wide_policy_fingerprint": PRIMARY_FP},
            backup: {"wide_policy_fingerprint": "bf33"},
            cooloff: {"wide_policy_fingerprint": "d308"},
        },
    }
    deadman = {
        "checked_at": "2026-07-25T23:51:00Z",
        "policy_choke_rung_b_cooloffs": {cooloff: "2026-07-26T22:38:30Z"},
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "gate_digits": {"f2_min_fresh_own_source_buy_rows_30m": 10},
                    "rows": [
                        _frontier(PRIMARY_WALLET, PRIMARY_FP, 492),
                        _frontier(backup, "bf33", 245),
                        _frontier(cooloff, "d308", 500),
                    ],
                }
            }
        },
    }

    result = build_shadow(
        evidence,
        deadman,
        primary_wallet=PRIMARY_WALLET,
        primary_fingerprint=PRIMARY_FP,
    )

    assert result["paper_only"] is True
    assert result["live_orders_allowed"] is False
    assert result["primary"]["fresh_own_source_buy_rows_30m"] == 492
    assert result["primary"]["f2_prewarmed"] is True
    assert result["primary"]["live_eligible_from_shadow"] is False
    assert result["backup_rank"][0]["wallet"] == backup
    assert result["backup_rank"][0]["deficits"] == ["f1_resolved_gte_200:125/200"]
    assert result["backup_rank"][1]["wallet"] == cooloff
    assert any(
        item.startswith("f3_cooloff_until:")
        for item in result["backup_rank"][1]["deficits"]
    )
    assert [row["wallet"] for row in result["lawful_freeze_backup_rank"]] == [
        backup
    ]


def test_shadow_excludes_non_positive_cells_and_marks_missing_frontier():
    backup = "0x00033f1089ff061813850e5135483bed39ce3b49"
    evidence = {
        "cells": [
            _cell(PRIMARY_WALLET, PRIMARY_FP, 198, 11.68, 5.9),
            _cell(backup, "positive", 82, 13.3, 16.2),
            _cell("0xf9b7", "negative", 250, -1.0, -1.0),
        ]
    }
    deadman = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "rows": [_frontier(PRIMARY_WALLET, PRIMARY_FP, 10)]
                }
            }
        }
    }

    result = build_shadow(
        evidence,
        deadman,
        primary_wallet=PRIMARY_WALLET,
        primary_fingerprint=PRIMARY_FP,
    )

    assert result["backup_count"] == 1
    assert result["backup_rank"][0]["wallet"] == backup
    assert "f4_or_frontier_evidence_absent" in result["backup_rank"][0]["deficits"]


def test_shadow_uses_wallet_scoped_f2_f4_from_another_fingerprint_row():
    evidence = {
        "cells": [
            _cell(PRIMARY_WALLET, PRIMARY_FP, 163, 16.85, 10.33),
        ]
    }
    deadman = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "rows": [_frontier(PRIMARY_WALLET, "different-fingerprint", 187)]
                }
            }
        }
    }

    result = build_shadow(
        evidence,
        deadman,
        primary_wallet=PRIMARY_WALLET,
        primary_fingerprint=PRIMARY_FP,
    )

    assert result["frontier_evidence_scope"] == "wallet_scoped_f2_f4"
    assert result["primary"]["fresh_own_source_buy_rows_30m"] == 187
    assert result["primary"]["f2_prewarmed"] is True
    assert result["primary"]["live_eligible_from_shadow"] is False


def test_shadow_uses_source_roster_nearest_frontier_wallet_authority():
    evidence = {
        "cells": [
            _cell(PRIMARY_WALLET, PRIMARY_FP, 163, 16.85, 10.33),
        ]
    }
    deadman = {
        "policy_choke": {
            "actuator": {
                "candidate_evidence": {
                    "rows": [_frontier(PRIMARY_WALLET, "", 0)]
                }
            },
            "source_roster_drought": {
                "candidate_evidence": {
                    "nearest_frontier": [
                        _frontier(PRIMARY_WALLET, "observed-fingerprint", 187)
                    ]
                }
            },
        }
    }

    result = build_shadow(
        evidence,
        deadman,
        primary_wallet=PRIMARY_WALLET,
        primary_fingerprint=PRIMARY_FP,
    )

    assert result["primary"]["fresh_own_source_buy_rows_30m"] == 187
    assert result["primary"]["f2_prewarmed"] is True


def test_auto_retarget_drops_negative_half_and_selects_freeze_override_ladder():
    old_wallet = "0x4c9497941333332d29f1c235dd23200f3623ffad"
    next_wallet = "0x1015bb260154f51e5f432cb0a3227c1619fcbac8"
    lower_wallet = "0x1160696549a6e3e5d9b4a7ccaed0403902307d8a"
    evidence = {
        "cells": [
            _cell(
                old_wallet,
                "old",
                117,
                10.99,
                9.39,
                first_half=18.49,
                second_half=-7.50,
            ),
            _cell(next_wallet, "next", 97, 24.08, 24.83),
            _cell(lower_wallet, "lower", 65, 33.14, 50.98),
        ],
        "freeze_overrides": {
            old_wallet: {"wide_policy_fingerprint": "old"},
            next_wallet: {"wide_policy_fingerprint": "next"},
            lower_wallet: {"wide_policy_fingerprint": "lower"},
        },
    }
    wallet, fingerprint, decision = select_auto_primary(
        evidence,
        {},
        {"primary": {"wallet": old_wallet, "wide_policy_fingerprint": "old"}},
    )

    assert (wallet, fingerprint) == (next_wallet, "next")
    assert decision["action"] == "RETARGET_UNHEALTHY_PRIMARY"


def test_auto_retarget_holds_healthy_primary_without_reranking():
    evidence = {
        "cells": [_cell(PRIMARY_WALLET, PRIMARY_FP, 97, 24.08, 24.83)],
        "freeze_overrides": {
            PRIMARY_WALLET: {"wide_policy_fingerprint": PRIMARY_FP}
        },
    }
    wallet, fingerprint, decision = select_auto_primary(
        evidence,
        {},
        {
            "primary": {
                "wallet": PRIMARY_WALLET,
                "wide_policy_fingerprint": PRIMARY_FP,
            }
        },
    )

    assert (wallet, fingerprint) == (PRIMARY_WALLET, PRIMARY_FP)
    assert decision["action"] == "HOLD_HEALTHY_PRIMARY"


def test_auto_retarget_excludes_terminal_primary_and_uses_directed_paper_priority(
    monkeypatch,
):
    banned = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    preferred = "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"
    monkeypatch.setattr(
        "scripts.build_frozen_fingerprint_f2_prewarm_shadow.FABLE_PAPER_FREEZE_PRIORITY",
        (preferred,),
    )
    evidence = {
        "cells": [
            _cell(banned, "banned", 1000, 60.0, 6.0),
            _cell(preferred, "preferred", 340, 79.0, 23.0),
        ],
        "freeze_overrides": {
            banned: {"wide_policy_fingerprint": "banned"},
            preferred: {"wide_policy_fingerprint": "preferred"},
        },
    }
    banned_frontier = _frontier(banned, "banned", 100)
    banned_frontier["checks"][
        "not_terminal_park_red_clock_or_measured_loser"
    ] = False
    deadman = {
        "policy_choke_rung_b_cooloffs": {
            preferred: "2026-07-27T18:44:05Z"
        },
        "policy_choke": {
            "source_roster_drought": {
                "candidate_evidence": {
                    "nearest_frontier": [
                        banned_frontier,
                        _frontier(preferred, "preferred", 100),
                    ]
                }
            }
        },
    }

    wallet, fingerprint, decision = select_auto_primary(
        evidence,
        deadman,
        {"primary": {"wallet": banned, "wide_policy_fingerprint": "banned"}},
    )

    assert (wallet, fingerprint) == (preferred, "preferred")
    assert decision["action"] == "RETARGET_FABLE_PAPER_FREEZE_PRIORITY"
    assert decision["paper_only"] is True
    assert decision["live_eligibility_granted"] is False
    assert decision["paper_cooloff_ignored"] is True
