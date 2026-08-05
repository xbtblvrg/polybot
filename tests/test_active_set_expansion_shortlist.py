from __future__ import annotations

import json

from scripts.build_active_set_expansion_shortlist import _leaderboard_pnl, build_shortlist, load_alpha_report_with_fallback


def test_leaderboard_pnl_prefers_latest_week_note() -> None:
    row = {
        "notes": (
            "Polymarket CRYPTO leaderboard PNL wallet; MONTH rank #10 pnl=100.5 vol=1000; "
            "WEEK rank #20 pnl=12.25 vol=200\n"
            "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #21 pnl=15.75 vol=250"
        )
    }

    pnl = _leaderboard_pnl(row)

    assert pnl["resolved_pnl"] == 15.75
    assert pnl["period"] == "week"
    assert pnl["rank"] == 21


def test_shortlist_excludes_active_set_and_scores_complementary_windows(tmp_path) -> None:
    candidate = "0x1111111111111111111111111111111111111111"
    active = "0x2222222222222222222222222222222222222222"
    other = "0x3333333333333333333333333333333333333333"
    pnl_only = "0x4444444444444444444444444444444444444444"
    alpha_report = {
        "execution_profiles": {
            "eligible_profile_count": 3,
            "profiles_by_wallet": {
                candidate: {
                    "eligible": True,
                    "status": "PASS",
                    "eligible_move_slice_count": 2,
                    "copyable_rate_pct": 80.0,
                    "fill_sample": 40,
                    "mean_edge": 0.01,
                    "median_edge": 0.01,
                    "best_eligible_move_slice": {"move_slice_key": "0-60|<=0.25"},
                },
                active: {
                    "eligible": True,
                    "status": "PASS",
                    "eligible_move_slice_count": 9,
                    "fill_sample": 100,
                },
                other: {
                    "eligible": True,
                    "status": "PASS",
                    "eligible_move_slice_count": 1,
                    "fill_sample": 20,
                },
                pnl_only: {
                    "eligible": True,
                    "status": "PASS",
                    "eligible_move_slice_count": 10,
                    "fill_sample": 0,
                },
            },
        }
    }
    registry = {
        "wallets": [
            {
                "address": candidate,
                "enabled": True,
                "name": "candidate",
                "notes": "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #2 pnl=50.0 vol=1000",
            },
            {
                "address": active,
                "enabled": True,
                "name": "active",
                "notes": "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #1 pnl=500.0 vol=1000",
            },
            {
                "address": other,
                "enabled": True,
                "name": "other",
                "notes": "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #3 pnl=25.0 vol=1000",
            },
            {
                "address": pnl_only,
                "enabled": True,
                "name": "pnl only",
                "notes": "Polymarket CRYPTO leaderboard PNL wallet; WEEK rank #1 pnl=1000.0 vol=1000",
            },
        ]
    }
    guard_state = {
        "active_set": {"members": [{"source_wallet": active}]},
        "window_participation": {
            "window_rollups": [
                {"market_slug": "btc-updown-5m-1000", "our_submits": 1, "our_fills": 1},
            ]
        },
    }
    resolutions = tmp_path / "resolutions.jsonl"
    resolutions.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "asset": "BTC",
                        "window_type": "5m",
                        "direction": "UP",
                        "market_slug": "btc-updown-5m-1000",
                        "window_start_unix_ts": 1000,
                        "condition_id": "0xaaa",
                        "yes_token": "token-a",
                        "no_token": "token-b",
                    }
                ),
                json.dumps(
                    {
                        "asset": "BTC",
                        "window_type": "5m",
                        "direction": "DOWN",
                        "market_slug": "btc-updown-5m-1300",
                        "window_start_unix_ts": 1300,
                        "condition_id": "0xbbb",
                        "yes_token": "token-c",
                        "no_token": "token-d",
                    }
                ),
            ]
        )
        + "\n"
    )
    polygon = tmp_path / "polygon.jsonl"
    polygon.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event": "polygon_orderfilled_log",
                        "selected_wallet": candidate,
                        "registry_wallets": [candidate],
                        "decoded": {"asset": "token-a"},
                    }
                ),
                json.dumps(
                    {
                        "event": "polygon_orderfilled_log",
                        "selected_wallet": candidate,
                        "registry_wallets": [candidate],
                        "decoded": {"asset": "token-c"},
                    }
                ),
            ]
        )
        + "\n"
    )

    report = build_shortlist(
        alpha_report=alpha_report,
        registry=registry,
        guard_state=guard_state,
        polygon_jsonl=str(polygon),
        resolutions=str(resolutions),
        top_n=5,
    )

    wallets = [row["wallet"] for row in report["top_candidates"]]
    row = next(item for item in report["top_candidates"] if item["wallet"] == candidate)
    assert active not in wallets
    assert pnl_only not in wallets
    assert report["pnl_only_no_lane_evidence"][0]["wallet"] == pnl_only
    assert report["summary"]["denominator_limited"] is True
    assert row["resolved_pnl"] == 50.0
    assert row["fills"] == 2
    assert row["complementary_fills"] == 1
    assert row["complementary_window_pct"] == 50.0


def test_alpha_report_loader_falls_back_from_empty_invalid_latest(tmp_path) -> None:
    requested = tmp_path / "alpha_decay_report.json"
    requested.write_text(
        json.dumps(
            {
                "execution_profiles": {
                    "alpha_decay_status": "INVALID_CAPTURE_WINDOW_MISMATCH",
                    "eligible_profile_count": 0,
                    "profiles_by_wallet": {},
                }
            }
        )
    )
    fallback = tmp_path / "alpha_decay_report_pass_20260706T013509Z.json"
    fallback.write_text(
        json.dumps(
            {
                "execution_profiles": {
                    "alpha_decay_status": "PASS",
                    "eligible_profile_count": 2,
                    "profiles_by_wallet": {
                        "0x1111111111111111111111111111111111111111": {"eligible": True},
                        "0x2222222222222222222222222222222222222222": {"eligible": True},
                    },
                },
                "polygon_jsonl": "fills.jsonl",
            }
        )
    )

    selected = load_alpha_report_with_fallback(str(requested))

    assert selected.fallback_used is True
    assert selected.selected_path == str(fallback)
    assert selected.reason.startswith("requested_alpha_report_empty_or_invalid")
    assert selected.report["execution_profiles"]["eligible_profile_count"] == 2
