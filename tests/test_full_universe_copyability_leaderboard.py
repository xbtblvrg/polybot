import gzip
import json
import time
from argparse import Namespace
from pathlib import Path

from scripts.build_wallet_full_universe_copyability_leaderboard import build_report


def _wallet(address: str) -> dict:
    return {"address": address, "enabled": True, "name": address[-6:], "tags": ["candidate"]}


def _history_event(wallet: str, idx: int, *, outcome: str = "UP", price: float = 0.5) -> dict:
    window_start = int(time.time() // 300 * 300) - (3 - idx) * 300
    return {
        "action": "BUY",
        "source_wallet": wallet,
        "market_slug": f"btc-updown-5m-{window_start}",
        "condition_id": f"cond-{idx}",
        "outcome": outcome,
        "price": price,
        "size": 2.0,
        "usdc_size": 1.0,
        "event_ts": window_start + 20,
    }


def _candidate(
    wallet: str,
    *,
    paper_pnl: float,
    copyable: int,
    clob: int,
    resolved: int,
    windows: int = 3,
) -> dict:
    return {
        "wallet": wallet,
        "candidate_id": f"candidate_{wallet[-4:]}",
        "unique_windows": windows,
        "total_source_usd": 100.0,
        "paper_replay": {
            "eligibility_status": "PASS" if paper_pnl > 0 else "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
            "paper_pnl_usd": paper_pnl,
            "paper_orders": copyable + 5,
            "resolved_orders": resolved,
            "copyable_buy_events": copyable,
            "candidate_clob_backed_orders": clob,
            "unresolved_ratio": 0.1,
            "policy_id": "paper_policy",
            "max_buy_price": 0.5,
            "failure_reasons": [] if paper_pnl > 0 else ["candidate_paper_pnl_not_positive"],
        },
    }


def _replay_orders(*, fills: int, rejects: int, reason: str = "no_ask_liquidity") -> list[dict]:
    return [
        {"final_status": "FILLED"}
        for _ in range(fills)
    ] + [
        {
            "final_status": "REJECTED",
            "fill_estimate": {"reject_details": {"blocking_reason": reason}},
        }
        for _ in range(rejects)
    ]


def test_full_universe_scores_every_registry_wallet_and_stages_positive_sample(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    registry = root / "configs" / "wallet_copy"
    research.mkdir(parents=True)
    registry.mkdir(parents=True)

    good = "0x1111111111111111111111111111111111111111"
    thin = "0x2222222222222222222222222222222222222222"
    bad = "0x3333333333333333333333333333333333333333"
    empty = "0x4444444444444444444444444444444444444444"
    (registry / "wallets.json").write_text(
        json.dumps({"wallets": [_wallet(good), _wallet(thin), _wallet(bad), _wallet(empty)]}),
        encoding="utf-8",
    )

    events = []
    for idx in range(3):
        events.append(_history_event(good, idx))
        events.append(_history_event(thin, idx))
        events.append(_history_event(bad, idx, outcome="DOWN"))
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": events}), encoding="utf-8")
    with (research / "btc_resolutions_test.jsonl").open("w", encoding="utf-8") as handle:
        for idx in range(3):
            handle.write(
                json.dumps(
                    {
                        "market_slug": _history_event(good, idx)["market_slug"],
                        "condition_id": f"cond-{idx}",
                        "direction": "UP",
                    }
                )
                + "\n"
            )

    (research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").write_text(
        json.dumps(
            {
                "candidates": [
                    _candidate(good, paper_pnl=7.5, copyable=21, clob=21, resolved=21),
                    _candidate(thin, paper_pnl=4.0, copyable=2, clob=2, resolved=2),
                    _candidate(bad, paper_pnl=-3.0, copyable=25, clob=25, resolved=25),
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "active_set_expansion_full_pool_shortlist.json").write_text(
        json.dumps(
            {
                "top_candidates": [
                    {
                        "wallet": good,
                        "resolved_pnl": 7.5,
                        "copyable_rate_pct": 80.0,
                        "fill_sample": 21,
                        "mean_edge": 0.01,
                        "median_edge": 0.01,
                        "eligible_profile": {
                            "best_eligible_move_slice": {
                                "move_slice_key": "fast",
                                "seconds_bucket": "0_10",
                                "entry_price_band": "00_50",
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (research / "wallet_copy_followability_leaderboard_latest.json").write_text(
        json.dumps(
            {
                "promotion_grade": True,
                "source_freshness": {"pass": True, "newest_source_event_age_s": 1.0},
                "leaderboard": [
                    {
                        "wallet": good,
                        "followability_score": 12.0,
                        "eligible_windows": 3,
                        "early_side_predictiveness_pct": 100.0,
                        "early_win_rate_pct": 100.0,
                        "avg_continuation_same_side_usd": 2.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    replay_payload = json.loads((research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").read_text())
    replay_payload["source_freshness"] = {"pass": True, "newest_source_event_age_s": 1.0}
    (research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").write_text(json.dumps(replay_payload))

    report = build_report(
        root,
        Namespace(
            registry="configs/wallet_copy/wallets.json",
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            replay="data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json",
            shortlist="data/research/active_set_expansion_full_pool_shortlist.json",
            followability="data/research/wallet_copy_followability_leaderboard_latest.json",
            top_n=10,
            min_paper_pnl_usd=0.0,
            min_copyable_events=20,
            min_clob_backed_orders=20,
            min_resolved_orders=20,
            min_replay_windows=3,
            min_unique_conditions=3,
        ),
    )

    assert report["summary"]["registry_wallets"] == 4
    assert report["summary"]["wallets_scored"] == 4
    assert report["summary"]["ranked_queue_depth"] == 1
    assert report["ranked_queue"][0]["wallet"] == good
    by_wallet = {row["wallet"]: row for row in report["leaderboard"]}
    assert by_wallet[good]["admission_status"] == "READY_QUEUE"
    assert by_wallet[thin]["admission_status"] == "POSITIVE_THIN_COPYABLE_SAMPLE"
    assert by_wallet[bad]["admission_status"] == "NEGATIVE_COPY_PNL"
    assert by_wallet[empty]["admission_status"] == "NO_COPY_REPLAY"
    assert by_wallet[good]["source_history"]["unique_conditions"] == 3
    assert by_wallet[good]["followability"]["score"] == 12.0
    assert report["promotion_grade"] is True
    assert report["inputs"]["newest_source_event_age_s"] < 86400.0

    (research / "wallet_copy_history_state.json").write_text(json.dumps({"events": events}), encoding="utf-8")
    stale_args = Namespace(
        registry="configs/wallet_copy/wallets.json",
        history="data/research/wallet_copy_history_state.json",
        resolutions="data/research/btc_resolutions_test.jsonl",
        replay="data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json",
        shortlist="data/research/active_set_expansion_full_pool_shortlist.json",
        followability="data/research/wallet_copy_followability_leaderboard_latest.json",
        top_n=10,
        min_paper_pnl_usd=0.0,
        min_copyable_events=20,
        min_clob_backed_orders=20,
        min_resolved_orders=20,
        min_replay_windows=3,
        min_unique_conditions=3,
    )
    stale = build_report(root, stale_args)
    assert stale["promotion_grade"] is False
    assert stale["ranked_queue"] == []


def test_prior_live_demotion_is_not_advertised_as_actionable_ready_queue(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    registry = root / "configs" / "wallet_copy"
    research.mkdir(parents=True)
    registry.mkdir(parents=True)
    wallet = "0x1111111111111111111111111111111111111111"
    (registry / "wallets.json").write_text(json.dumps({"wallets": [_wallet(wallet)]}))
    events = [_history_event(wallet, idx) for idx in range(3)]
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": events}))
    (research / "btc_resolutions_test.jsonl").write_text(
        "".join(
            json.dumps({
                "market_slug": _history_event(wallet, idx)["market_slug"],
                "condition_id": f"cond-{idx}",
                "direction": "UP",
            }) + "\n"
            for idx in range(3)
        )
    )
    replay = {
        "source_freshness": {"pass": True, "newest_source_event_age_s": 1.0},
        "candidates": [_candidate(wallet, paper_pnl=7.5, copyable=21, clob=21, resolved=21)],
    }
    (research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").write_text(json.dumps(replay))
    (research / "active_set_expansion_full_pool_shortlist.json").write_text(json.dumps({}))
    (research / "wallet_copy_followability_leaderboard_latest.json").write_text(
        json.dumps({"promotion_grade": True, "source_freshness": {"pass": True}, "leaderboard": []})
    )
    (research / "wallet_copy_active_set_auto_degrade_state.json").write_text(
        json.dumps({"members": [{"source_wallet": wallet, "enabled": False, "status": "DEMOTED_LOSS"}]})
    )
    report = build_report(
        root,
        Namespace(
            registry="configs/wallet_copy/wallets.json",
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            replay="data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json",
            shortlist="data/research/active_set_expansion_full_pool_shortlist.json",
            followability="data/research/wallet_copy_followability_leaderboard_latest.json",
            active_set_overlay="data/research/wallet_copy_active_set_auto_degrade_state.json",
            top_n=10,
            min_paper_pnl_usd=0.0,
            min_copyable_events=20,
            min_clob_backed_orders=20,
            min_resolved_orders=20,
            min_replay_windows=3,
            min_unique_conditions=3,
        ),
    )
    assert report["ranked_queue"] == []
    row = report["leaderboard"][0]
    assert row["admission_status"] == "PRIOR_LIVE_DEMOTION_REQUIRES_FRESH_READMISSION"
    assert row["queue_eligible_before_live_disposition"] is True


def test_reject_feedback_penalizes_canonical_ratio_not_raw_count(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    registry = root / "configs" / "wallet_copy"
    research.mkdir(parents=True)
    registry.mkdir(parents=True)
    high_ratio = "0x1111111111111111111111111111111111111111"
    low_ratio_more_rejects = "0x2222222222222222222222222222222222222222"
    (registry / "wallets.json").write_text(
        json.dumps({"wallets": [_wallet(high_ratio), _wallet(low_ratio_more_rejects)]})
    )
    events = [
        *[_history_event(high_ratio, idx) for idx in range(3)],
        *[_history_event(low_ratio_more_rejects, idx) for idx in range(3)],
    ]
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": events}))
    (research / "btc_resolutions_test.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "market_slug": _history_event(high_ratio, idx)["market_slug"],
                    "condition_id": f"cond-{idx}",
                    "direction": "UP",
                }
            )
            + "\n"
            for idx in range(3)
        )
    )
    candidates = []
    for wallet, fills, rejects in (
        (high_ratio, 5, 15),
        (low_ratio_more_rejects, 337, 163),
    ):
        candidate = _candidate(wallet, paper_pnl=10.0, copyable=25, clob=25, resolved=25)
        candidate["paper_replay"]["replay_orders"] = _replay_orders(fills=fills, rejects=rejects)
        candidates.append(candidate)
    (research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").write_text(
        json.dumps(
            {
                "source_freshness": {"pass": True, "newest_source_event_age_s": 1.0},
                "candidates": candidates,
            }
        )
    )
    (research / "active_set_expansion_full_pool_shortlist.json").write_text("{}")
    (research / "wallet_copy_followability_leaderboard_latest.json").write_text(
        json.dumps({"promotion_grade": True, "source_freshness": {"pass": True}, "leaderboard": []})
    )

    report = build_report(
        root,
        Namespace(
            registry="configs/wallet_copy/wallets.json",
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            replay="data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json",
            shortlist="data/research/active_set_expansion_full_pool_shortlist.json",
            followability="data/research/wallet_copy_followability_leaderboard_latest.json",
            top_n=10,
            min_paper_pnl_usd=0.0,
            min_copyable_events=20,
            min_clob_backed_orders=20,
            min_resolved_orders=20,
            min_replay_windows=3,
            min_unique_conditions=3,
        ),
    )

    by_wallet = {row["wallet"]: row for row in report["leaderboard"]}
    high = by_wallet[high_ratio]["reject_attribution_feedback"]
    low = by_wallet[low_ratio_more_rejects]["reject_attribution_feedback"]
    assert high["penalty_ratio"] == 0.75
    assert low["penalty_ratio"] == 0.326
    assert low["raw_reject_count_not_used_for_penalty"] > high["raw_reject_count_not_used_for_penalty"]
    assert by_wallet[low_ratio_more_rejects]["copyability_score"] > by_wallet[high_ratio]["copyability_score"]


def test_full_universe_reads_compressed_registry(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "data" / "research"
    registry = root / "configs" / "wallet_copy"
    research.mkdir(parents=True)
    registry.mkdir(parents=True)
    wallet = "0x5555555555555555555555555555555555555555"
    with gzip.open(registry / "wallets.json.gz", "wt", encoding="utf-8") as handle:
        json.dump({"wallets": [_wallet(wallet)]}, handle)
    (research / "wallet_copy_live_guard_hot_history_state.json").write_text(json.dumps({"events": []}))
    (research / "btc_resolutions_test.jsonl").write_text("")
    (research / "wallet_copy_discover_live_band_candidates_full_pool_replay.json").write_text(json.dumps({}))
    (research / "active_set_expansion_full_pool_shortlist.json").write_text(json.dumps({}))
    (research / "wallet_copy_followability_leaderboard_latest.json").write_text(json.dumps({}))

    report = build_report(
        root,
        Namespace(
            registry="configs/wallet_copy/wallets.json.gz",
            history="data/research/wallet_copy_live_guard_hot_history_state.json",
            resolutions="data/research/btc_resolutions_test.jsonl",
            replay="data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json",
            shortlist="data/research/active_set_expansion_full_pool_shortlist.json",
            followability="data/research/wallet_copy_followability_leaderboard_latest.json",
            active_set_overlay="data/research/wallet_copy_active_set_auto_degrade_state.json",
            top_n=10,
            min_paper_pnl_usd=0.0,
            min_copyable_events=20,
            min_clob_backed_orders=20,
            min_resolved_orders=20,
            min_replay_windows=3,
            min_unique_conditions=3,
        ),
    )

    assert report["summary"]["registry_wallets"] == 1
    assert report["leaderboard"][0]["wallet"] == wallet
