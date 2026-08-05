import json
from pathlib import Path
from types import SimpleNamespace

from scripts.build_wallet_temporal_profitability_registry import _candidate_score, _candidate_sort_key, _classify_wallet, build_report


def _classification(recent: dict):
    return _classify_wallet(
        all_profile={"resolved_trades": 1000, "roi_pct": 2.0},
        recent_profile=recent,
        regime_profiles={"weekday": {"resolved_trades": 0}, "weekend": {"resolved_trades": 5, "roi_pct": 2.0}},
        dead_band_profile={"resolved_trades": 0}, profitable_hours=[], min_trades=5, min_band_trades=3,
    )[0]


def test_fading_requires_power_floor_and_negative_sigma():
    assert _classification({"resolved_trades": 50, "roi_pct": -4.0, "gap_in_sigma": -0.53}) == "WEEKEND-ONLY"
    assert _classification({"resolved_trades": 200, "roi_pct": -4.0, "gap_in_sigma": -0.99}) == "WEEKEND-ONLY"
    assert _classification({"resolved_trades": 200, "roi_pct": -4.0, "gap_in_sigma": -1.0}) == "FADING"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _args(**overrides):
    values = {
        "registry": "registry.json",
        "history": "history.json",
        "copyability": "copyability.json",
        "dow_profile": "dow.json",
        "resolutions": "resolutions.jsonl",
        "min_trades": 3,
        "min_band_trades": 2,
        "fresh_max_age_h": 1_000_000.0,
        "stale_after_h": 24.0,
        "dead_band_start_hour": 18,
        "dead_band_end_hour": 22,
        "top_candidates": 5,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_temporal_registry_classifies_regimes_and_dead_band_candidates(tmp_path: Path) -> None:
    weekday_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    weekend_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    band_wallet = "0xcccccccccccccccccccccccccccccccccccccccc"

    _write_json(
        tmp_path / "registry.json",
        {
            "wallets": [
                {"address": weekday_wallet, "name": "weekday"},
                {"address": weekend_wallet, "name": "weekend"},
                {"address": band_wallet, "name": "band"},
            ]
        },
    )
    _write_json(
        tmp_path / "copyability.json",
        {
            "leaderboard": [
                {"wallet": weekday_wallet, "copyability_score": 5, "copy_replay": {"paper_pnl_usd": 1}},
                {"wallet": weekend_wallet, "copyability_score": 5, "copy_replay": {"paper_pnl_usd": 1}},
                {"wallet": band_wallet, "copyability_score": 8, "copy_replay": {"paper_pnl_usd": 2}},
            ]
        },
    )
    _write_json(
        tmp_path / "dow.json",
        {
            "generated_at": "2026-07-10T17:00:00Z",
            "profiles": [
                {
                    "wallet": weekday_wallet,
                    "roles": ["active_member"],
                    "trade_count": 3,
                    "expected_active_dow_hour_weights": {"4:03": 1.0, "4:18": 0.0, "4:19": 0.0, "4:20": 0.0, "4:21": 0.0},
                }
            ],
        },
    )

    rows = []
    resolutions = []

    def add(wallet: str, ts: int, outcome: str, winner: str = "UP") -> None:
        slug = f"btc-updown-5m-{ts}"
        rows.append(
            {
                "source_wallet": wallet,
                "action": "BUY",
                "market_slug": slug,
                "condition_id": f"cond-{ts}",
                "outcome": outcome,
                "price": 0.5,
                "size": 20,
                "event_ts": ts + 10,
                "transaction_hash": f"0x{wallet[-4:]}{ts}",
            }
        )
        resolutions.append({"market_slug": slug, "condition_id": f"cond-{ts}", "winner": winner})

    # Friday 18:00/18:05/18:10 UTC, profitable dead band.
    add(weekday_wallet, 1_783_706_400, "UP")
    add(weekday_wallet, 1_783_706_700, "UP")
    add(weekday_wallet, 1_783_707_000, "UP")
    # Saturday 18:00/18:05/18:10 UTC losses for weekday-only.
    add(weekday_wallet, 1_783_792_800, "DOWN")
    add(weekday_wallet, 1_783_793_100, "DOWN")
    add(weekday_wallet, 1_783_793_400, "DOWN")

    # Weekend-only wallet: weekday losses, weekend wins.
    add(weekend_wallet, 1_783_706_400, "DOWN")
    add(weekend_wallet, 1_783_706_700, "DOWN")
    add(weekend_wallet, 1_783_707_000, "DOWN")
    add(weekend_wallet, 1_783_792_800, "UP")
    add(weekend_wallet, 1_783_793_100, "UP")
    add(weekend_wallet, 1_783_793_400, "UP")

    # Dead-band specialist with no broader regime sample.
    add(band_wallet, 1_783_706_400, "UP")
    add(band_wallet, 1_783_706_700, "UP")

    _write_json(tmp_path / "history.json", {"events": rows})
    _write_jsonl(tmp_path / "resolutions.jsonl", resolutions)

    report = build_report(tmp_path, _args())
    by_wallet = {row["wallet"]: row for row in report["wallets"]}
    feed_wallets = [row["wallet"] for row in report["watch_tier_probe_feed"]["candidates"]]

    assert report["dow_weight_verification"]["status"] == "PASS_REAL_EVENT_HOURS"
    assert report["dow_weight_verification"]["current_dead_band_empty_confirmed"] is True
    assert by_wallet[weekday_wallet]["classification"] == "WEEKDAY-ONLY"
    assert by_wallet[weekend_wallet]["classification"] == "WEEKEND-ONLY"
    assert by_wallet[band_wallet]["classification"] == "BAND-SPECIALIST"
    assert by_wallet[weekday_wallet]["all"]["losses"] == 3
    assert by_wallet[weekday_wallet]["all"]["avg_win_per_winner_usd"] == 10.0
    assert by_wallet[weekday_wallet]["all"]["avg_loss_per_loser_abs_usd"] == 10.0
    assert by_wallet[weekday_wallet]["all"]["required_win_rate_at_payoff_shape_pct"] == 50.0
    assert by_wallet[weekday_wallet]["all"]["actual_minus_required_win_rate_pp"] == 0.0
    assert by_wallet[weekday_wallet]["all"]["gap_sigma_pp"] == 20.412415
    assert by_wallet[weekday_wallet]["all"]["gap_in_sigma"] == 0.0
    assert by_wallet[weekday_wallet]["slice_labels"]["weekday"]["label"] == "PROVEN-POSITIVE"
    assert by_wallet[weekday_wallet]["slice_labels"]["weekend"]["label"] == "PROVEN-NEGATIVE"
    assert by_wallet[weekend_wallet]["slice_labels"]["weekday"]["label"] == "PROVEN-NEGATIVE"
    assert by_wallet[weekend_wallet]["slice_labels"]["weekend"]["label"] == "PROVEN-POSITIVE"
    assert by_wallet[band_wallet]["slice_labels"]["weekday"]["label"] == "UNPROVEN"
    assert by_wallet[band_wallet]["slice_labels"]["dead_band_18_22_utc"]["label"] == "PROVEN-POSITIVE"
    assert by_wallet[weekend_wallet]["slice_labels"]["weekend"]["resolved_trades"] == 3
    assert "roi=" in by_wallet[weekend_wallet]["slice_labels"]["weekend"]["reason"]
    assert report["summary"]["slice_label_counts"]["weekend"]["PROVEN-POSITIVE"] == 1
    assert report["summary"]["slice_label_counts"]["weekend"]["PROVEN-NEGATIVE"] == 1
    assert report["summary"]["slice_label_counts"]["weekend"]["UNPROVEN"] == 1
    assert by_wallet[band_wallet]["dead_band_18_22_utc"]["resolved_trades"] == 2
    assert band_wallet in feed_wallets
    assert all(candidate["watch_tier_policy"]["paper_first"] for candidate in report["watch_tier_probe_feed"]["candidates"])
    assert all("slice_labels" in candidate for candidate in report["watch_tier_probe_feed"]["candidates"])
    assert all("staleness" in candidate["fit"] for candidate in report["watch_tier_probe_feed"]["candidates"])
    assert all(
        float((candidate["dead_band_18_22_utc"] or {}).get("roi_pct") or 0.0) >= 0.0
        or candidate["fit"].get("inclusion_reason")
        for candidate in report["watch_tier_probe_feed"]["candidates"]
    )


def test_temporal_feed_excludes_negative_dead_band_roi_and_sorts_stale_after_fresh() -> None:
    negative_row = {
        "dead_band_18_22_utc": {"resolved_trades": 3, "roi_pct": -3.0, "latest_event_age_h": 1.0},
        "all": {"latest_event_age_h": 1.0},
        "copyability": {"copyability_score": 10},
        "profitable_hour_bands": ["weekday:18"],
    }
    eligible, _score, detail = _candidate_score(
        negative_row,
        fresh_max_age_h=72.0,
        stale_after_h=24.0,
        min_band_trades=3,
    )
    assert eligible is False
    assert detail["dead_band_nonnegative_roi"] is False
    assert detail["inclusion_reason"] == ""

    stale_high_score = {
        "wallet": "0xstale",
        "rank_score": 100.0,
        "dead_band_18_22_utc": {"resolved_trades": 50},
        "fit": {"staleness": {"dead_band_stale_gt_threshold": True, "ordering_age_h": 70.0}},
    }
    fresh_lower_score = {
        "wallet": "0xfresh",
        "rank_score": 10.0,
        "dead_band_18_22_utc": {"resolved_trades": 5},
        "fit": {"staleness": {"dead_band_stale_gt_threshold": False, "ordering_age_h": 0.2}},
    }
    assert sorted([stale_high_score, fresh_lower_score], key=_candidate_sort_key)[0]["wallet"] == "0xfresh"


def test_temporal_registry_merges_supplemental_history(tmp_path: Path) -> None:
    wallet = "0xdddddddddddddddddddddddddddddddddddddddd"
    _write_json(tmp_path / "registry.json", {"wallets": []})
    _write_json(tmp_path / "history.json", {"events": []})
    _write_json(tmp_path / "copyability.json", {"leaderboard": []})
    _write_json(tmp_path / "dow.json", {"profiles": []})
    _write_json(
        tmp_path / "supplemental.json",
        {
            "kind": "wallet_copy_history_state",
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "market_slug": "btc-updown-5m-1783965000",
                    "condition_id": "cond-1",
                    "outcome": "UP",
                    "price": 0.5,
                    "size": 20,
                    "event_ts": 1783965010,
                    "transaction_hash": "0xsupp",
                }
            ],
            "wallets": [{"address": wallet, "name": "supplemental"}],
        },
    )
    _write_jsonl(tmp_path / "resolutions.jsonl", [{"market_slug": "btc-updown-5m-1783965000", "winner": "UP"}])

    report = build_report(
        tmp_path,
        _args(
            supplemental_history=["supplemental.json"],
            supplemental_history_glob=[],
            min_trades=1,
            min_band_trades=1,
        ),
    )

    by_wallet = {row["wallet"]: row for row in report["wallets"]}
    assert report["summary"]["supplemental_history_files"] == 1
    assert report["summary"]["supplemental_history_events"] == 1
    assert by_wallet[wallet]["slice_labels"]["weekday"]["label"] == "PROVEN-POSITIVE"


def test_temporal_registry_excludes_price_above_venue_ceiling(tmp_path: Path) -> None:
    wallet = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    _write_json(tmp_path / "registry.json", {"wallets": [{"address": wallet}]})
    _write_json(
        tmp_path / "history.json",
        {
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "market_slug": "btc-updown-5m-1783965000",
                    "outcome": "UP",
                    "price": 0.49,
                    "size": 2,
                    "event_ts": 1783965010,
                    "transaction_hash": "0xreachable",
                },
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "market_slug": "btc-updown-5m-1783965300",
                    "outcome": "UP",
                    "price": 0.51,
                    "size": 2,
                    "event_ts": 1783965310,
                    "transaction_hash": "0xunreachable",
                },
            ]
        },
    )
    _write_json(tmp_path / "copyability.json", {"leaderboard": []})
    _write_json(tmp_path / "dow.json", {"profiles": []})
    _write_jsonl(
        tmp_path / "resolutions.jsonl",
        [
            {"market_slug": "btc-updown-5m-1783965000", "winner": "UP"},
            {"market_slug": "btc-updown-5m-1783965300", "winner": "UP"},
        ],
    )

    report = build_report(
        tmp_path,
        _args(min_trades=1, min_band_trades=1),
    )
    row = next(item for item in report["wallets"] if item["wallet"] == wallet)
    venue = row["venue_executable"]
    assert row["all"]["resolved_trades"] == 2
    assert venue["venue_executable_resolved"] == 1
    assert venue["venue_unreachable_resolved"] == 1
    assert venue["venue_reachable_share_pct"] == 50.0


def test_temporal_registry_emits_required_wallet_without_local_history(tmp_path: Path) -> None:
    wallet = "0x1313131313131313131313131313131313131313"
    _write_json(tmp_path / "registry.json", {"wallets": []})
    _write_json(tmp_path / "history.json", {"events": []})
    _write_json(tmp_path / "copyability.json", {"leaderboard": []})
    _write_json(tmp_path / "dow.json", {"profiles": []})
    _write_jsonl(tmp_path / "resolutions.jsonl", [])

    report = build_report(tmp_path, _args(include_wallet=[wallet]))

    assert report["summary"]["required_coverage_rows_emitted"] == 1
    assert report["wallets"][0]["wallet"] == wallet
    assert report["wallets"][0]["classification"] == "NO_RESOLVED_BTC5M_HISTORY"


def test_temporal_registry_reads_default_supplemental_manifest(tmp_path: Path) -> None:
    wallet = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    _write_json(tmp_path / "registry.json", {"wallets": []})
    _write_json(tmp_path / "history.json", {"events": []})
    _write_json(tmp_path / "copyability.json", {"leaderboard": []})
    _write_json(tmp_path / "dow.json", {"profiles": []})
    _write_json(
        tmp_path / "data" / "research" / "temporal_supplemental_history_manifest.json",
        {"supplemental_history_files": ["supplemental.json"]},
    )
    _write_json(
        tmp_path / "supplemental.json",
        {
            "kind": "wallet_copy_history_state",
            "events": [
                {
                    "source_wallet": wallet,
                    "action": "BUY",
                    "market_slug": "btc-updown-5m-1783965000",
                    "condition_id": "cond-1",
                    "outcome": "UP",
                    "price": 0.5,
                    "size": 20,
                    "event_ts": 1783965010,
                    "transaction_hash": "0xmanifest",
                }
            ],
            "wallets": [{"address": wallet, "name": "manifest"}],
        },
    )
    _write_jsonl(tmp_path / "resolutions.jsonl", [{"market_slug": "btc-updown-5m-1783965000", "winner": "UP"}])

    report = build_report(
        tmp_path,
        _args(
            supplemental_history=[],
            supplemental_history_glob=[],
            min_trades=1,
            min_band_trades=1,
        ),
    )

    by_wallet = {row["wallet"]: row for row in report["wallets"]}
    assert report["summary"]["supplemental_history_files"] == 1
    assert by_wallet[wallet]["slice_labels"]["weekday"]["label"] == "PROVEN-POSITIVE"
