from __future__ import annotations

from scripts.apply_temporal_watch_tier_probe_feed import build_payload


def test_apply_temporal_watch_tier_feed_skips_live_observed_and_adds_novel_wallet() -> None:
    e6db = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
    ac05 = "0xac0586732786905d285959613f1813bc89246729"
    existing_temporal = "0xdf2c0702fc00be90bd795234a86e28f1ed39118a"
    novel = "0xa727aa7c18821d191023561b6e410949215d91b5"
    retained = "0x1111111111111111111111111111111111111111"
    temporal = {
        "watch_tier_probe_feed": {
            "candidates": [
                {
                    "wallet": existing_temporal,
                    "rank_score": 10.0,
                    "classification": "BAND-SPECIALIST",
                    "dead_band_18_22_utc": {"roi_pct": 5.0, "resolved_trades": 3},
                    "fit": {
                        "latest_dead_band_event_age_h": 0.2,
                        "inclusion_reason": "dead_band_positive",
                        "staleness": {"dead_band_stale_gt_threshold": False},
                    },
                    "copyability": {"copyability_score": 1.0, "paper_pnl_usd": 2.0},
                },
                {
                    "wallet": e6db,
                    "rank_score": 9.0,
                    "dead_band_18_22_utc": {"roi_pct": 4.0, "resolved_trades": 3},
                    "fit": {"inclusion_reason": "dead_band_positive", "staleness": {}},
                    "copyability": {},
                },
                {
                    "wallet": ac05,
                    "rank_score": 8.0,
                    "dead_band_18_22_utc": {"roi_pct": 4.0, "resolved_trades": 3},
                    "fit": {"inclusion_reason": "dead_band_positive", "staleness": {}},
                    "copyability": {},
                },
                {
                    "wallet": novel,
                    "rank_score": 7.0,
                    "classification": "WEEKDAY-ONLY",
                    "dead_band_18_22_utc": {"roi_pct": 1.0, "resolved_trades": 3},
                    "fit": {
                        "latest_dead_band_event_age_h": 0.5,
                        "inclusion_reason": "dead_band_positive",
                        "staleness": {"dead_band_stale_gt_threshold": False},
                    },
                    "copyability": {"copyability_score": 1.0, "paper_pnl_usd": 1.0},
                },
            ]
        }
    }
    config = {
        "schema_version": 2,
        "selection_criteria": {"cap": 2},
        "wallets": [
            {"source_wallet": existing_temporal, "selection_reason": "old", "sources": ["old_source"]},
            {"source_wallet": e6db, "selection_reason": "old"},
            {"source_wallet": retained, "selection_reason": "old"},
        ],
    }

    updated, status = build_payload(
        temporal=temporal,
        config=config,
        skip_wallets={e6db, ac05},
        watch_config_path="watch.json",
    )

    wallets = [row["source_wallet"] for row in updated["wallets"]]
    assert e6db not in wallets
    assert ac05 not in wallets
    assert wallets[:2] == [existing_temporal, novel]
    assert retained in wallets
    assert updated["measure_only"] is True
    assert updated["live_orders_allowed"] is False
    assert updated["selection_criteria"]["cap"] == len(updated["wallets"])
    assert updated["wallets"][0]["sources"] == ["old_source", "temporal_profitability_dead_band_feed"]
    assert status["summary"]["added_wallets"] == [novel]
    assert status["summary"]["already_present_wallets"] == [existing_temporal]
    assert len(status["summary"]["skipped_wallets"]) == 2
