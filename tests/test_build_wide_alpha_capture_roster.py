from scripts import build_wide_alpha_capture_roster as roster_builder

build_roster = roster_builder.build_roster


READY = "0x0000000000000000000000000000000000000001"
ELIGIBLE = "0x0000000000000000000000000000000000000002"
DEMOTED = "0x0000000000000000000000000000000000000003"
TOP = "0x0000000000000000000000000000000000000004"
CLIMB = "0xb27bc932bf8110d8f78e55da7d5f0497a18b5b82"


def test_build_roster_keeps_ready_queue_and_excludes_prior_demotion() -> None:
    report = build_roster(
        {
            "ranked_queue": [
                {"wallet": READY, "admission_status": "READY_QUEUE"},
                {"wallet": DEMOTED, "admission_status": "READY_QUEUE"},
            ],
            "top_wallets": [
                {
                    "wallet": DEMOTED,
                    "admission_status": "PRIOR_LIVE_DEMOTION_REQUIRES_FRESH_READMISSION",
                }
            ],
        },
        {
            "execution_profiles": {
                "profiles": [
                    {"wallet": ELIGIBLE, "eligible": True},
                    {"wallet": DEMOTED, "eligible": True},
                ]
            }
        },
        {"counters": {"top_live_ready_wallet": TOP}},
        {},
    )

    selected = {row["address"] for row in report["wallets"]}
    assert READY in selected
    assert ELIGIBLE in selected
    assert TOP in selected
    assert DEMOTED not in selected
    assert report["excluded_prior_live_demotion_wallets"] == [DEMOTED]
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False


def test_build_roster_excludes_disabled_loss_overlay_member() -> None:
    report = build_roster(
        {"ranked_queue": [{"wallet": READY, "admission_status": "READY_QUEUE"}]},
        {},
        {},
        {
            "members": [
                {
                    "source_wallet": READY,
                    "enabled": False,
                    "status": "DEMOTED_NEGATIVE_ROLLING_LOSS",
                }
            ]
        },
    )

    selected = {row["address"] for row in report["wallets"]}
    assert READY not in selected
    assert "0x224a89dbe0db0d6124b335edabd15b3f877da3d5" in selected
    assert report["source_counts"]["prior_live_demotion_excluded"] == 1


def test_build_roster_unions_manifest_capture_and_direction_climb(monkeypatch) -> None:
    monkeypatch.setattr(
        roster_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        ((CLIMB, "test-fingerprint"),),
    )
    manifest_wallet = "0x0000000000000000000000000000000000000005"
    report = build_roster(
        {},
        {},
        {},
        {},
        {
            "capture_watch_wallets": [
                {
                    "wallet": manifest_wallet,
                    "paper_measurement_only": True,
                    "promotion_authority": False,
                }
            ]
        },
    )

    rows = {row["address"]: row for row in report["wallets"]}
    assert manifest_wallet in rows
    assert "manifest_capture_watch" in rows[manifest_wallet]["tags"]
    assert CLIMB in rows
    assert "direct_climb_priority" in rows[CLIMB]["tags"]
    assert report["source_counts"]["manifest_capture_watch"] == 1
    assert report["source_counts"]["direct_climb_priority"] == 1
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False


def test_build_roster_adds_positive_copy_pnl_by_weekday_depth() -> None:
    deep = "0x0000000000000000000000000000000000000010"
    shallow = "0x0000000000000000000000000000000000000011"
    report = build_roster(
        {
            "leaderboard": [
                {"wallet": shallow, "copy_replay": {"paper_pnl_usd": 100.0}},
                {"wallet": deep, "copy_replay": {"paper_pnl_usd": 1.0}},
            ]
        },
        {}, {}, {}, None,
        {"wallets": [
            {"wallet": shallow, "slice_labels": {"weekday": {"resolved_trades": 20}}},
            {"wallet": deep, "slice_labels": {"weekday": {"resolved_trades": 400}}},
        ]},
        1,
        {
            "generated_at": "2026-07-31T10:00:00Z",
            "cells": [{
                "identity": {"wallet": deep},
                "venue_executable_full_stream_rescore": {
                    "resolved": 400,
                    "f1_pass": True,
                },
                "f1_walk_forward_admissible": True,
            }],
        },
    )

    rows = {row["address"]: row for row in report["wallets"]}
    assert deep in rows
    assert shallow not in rows
    assert rows[deep]["weekday_resolved_trades"] == 400
    assert rows[deep]["positive_copy_pnl_depth_rank"] == 1
    assert report["source_counts"]["positive_copy_pnl_depth"] == 1
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["evidence_projection"] == {
        "basis": "current fingerprint evidence restricted to expanded paper cohort; newly captured cells appear on later cuts",
        "source_generated_at": "2026-07-31T10:00:00Z",
        "selected_wallets": 3,
        "known_cells": 1,
        "cells_reaching_n_gte_200": 1,
        "f1_pass_cells": 1,
        "walk_forward_admissible_count": 1,
        "walk_forward_blocked_by_half_depth": 0,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def test_positive_copy_pnl_depth_remains_paper_observable_after_demotion() -> None:
    wallet = "0x0000000000000000000000000000000000000012"
    report = build_roster(
        {"leaderboard": [{
            "wallet": wallet,
            "copy_replay": {"paper_pnl_usd": 10.0},
        }]},
        {}, {},
        {"members": [{
            "source_wallet": wallet,
            "enabled": False,
            "status": "DEMOTED_NEGATIVE_ROLLING_LOSS",
        }]},
        None,
        {"wallets": [{
            "wallet": wallet,
            "slice_labels": {"weekday": {"resolved_trades": 300}},
        }]},
        1,
        {"cells": [{
            "identity": {
                "wallet": wallet,
                "wide_policy_fingerprint": "demoted-depth-cell",
            },
            "venue_executable_full_stream_rescore": {
                "resolved": 250,
                "post_fee_pnl_usd": 20.0,
                "roi_pct": 8.0,
                "first_half_post_fee_pnl_usd": 8.0,
                "second_half_post_fee_pnl_usd": 12.0,
                "concentration_admissible": True,
                "venue_reachable_share_pct": 80.0,
                "f1_pass": True,
                "f1_walk_forward_admissible": False,
            },
        }]},
    )

    rows = {row["address"]: row for row in report["wallets"]}
    assert wallet in rows
    assert wallet in report["excluded_prior_live_demotion_wallets"]
    assert "positive_copy_pnl_depth" in rows[wallet]["tags"]
    assert "depth_priority_frontier" in rows[wallet]["tags"]
    assert rows[wallet]["depth_priority_cell"]["promotion_authority"] is False
    assert report["paper_only"] is True


def test_depth_priority_frontier_selects_exact_cell_and_observes_two_cut_rate() -> None:
    wallet = "0x0000000000000000000000000000000000000013"
    fingerprint = "depth-fingerprint"
    report = build_roster(
        {"leaderboard": [{
            "wallet": wallet,
            "copy_replay": {"paper_pnl_usd": 25.0},
        }]},
        {}, {}, {}, None,
        {"wallets": [{
            "wallet": wallet,
            "slice_labels": {"weekday": {"resolved_trades": 65_000}},
        }]},
        0,
        {
            "generated_at": "2026-07-31T10:20:00Z",
            "cells": [{
                "wide_policy_fingerprint": fingerprint,
                "identity": {
                    "wallet": wallet,
                    "wide_policy_fingerprint": fingerprint,
                    "move_slice_keys": ["000-060|0.25-0.50"],
                },
                "venue_executable_full_stream_rescore": {
                    "resolved": 270,
                    "post_fee_pnl_usd": 86.0,
                    "roi_pct": 31.0,
                    "first_half_post_fee_pnl_usd": 70.0,
                    "second_half_post_fee_pnl_usd": 16.0,
                    "concentration_admissible": True,
                    "venue_reachable_share_pct": 73.0,
                    "f1_pass": True,
                    "f1_walk_forward_admissible": False,
                },
            }],
        },
        {
            "generated_at": "2026-07-31T10:00:00Z",
            "cells": [{
                "wide_policy_fingerprint": fingerprint,
                "resolved": 260,
            }],
        },
    )

    row = next(row for row in report["wallets"] if row["address"] == wallet)
    frontier = report["depth_priority_frontier"]
    cell = frontier["cells"][0]
    assert "depth_priority_frontier" in row["tags"]
    assert row["depth_priority_cell"]["wide_policy_fingerprint"] == fingerprint
    assert cell["gap_to_400"] == 130
    assert cell["observed_resolved_signals_per_day"] == 720.0
    assert cell["rate_status"] == "TWO_CUT_RATE_OBSERVED"
    assert frontier["summary"]["non_82c8_cell_count"] == 1
    assert report["evidence_projection"]["walk_forward_blocked_by_half_depth"] == 1


def test_direct_climb_priority_is_fingerprint_scoped_for_prior_demotion(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        roster_builder,
        "DIRECTION_DIRECT_CLIMB_PRIORITY",
        ((CLIMB, "other-fingerprint"),),
    )
    report = build_roster(
        {},
        {},
        {},
        {
            "members": [
                {
                    "source_wallet": CLIMB,
                    "enabled": False,
                    "status": "DEMOTED_FABLE_SUBSTITUTE_ROTATION",
                    "policy_id": "wide_fp_2163fe3eeb215902c0fd0e90",
                }
            ]
        },
    )

    rows = {row["address"]: row for row in report["wallets"]}
    assert CLIMB in rows
    assert "direct_climb_priority" in rows[CLIMB]["tags"]
    assert report["excluded_prior_live_demotion_wallets"] == []
