import json
from pathlib import Path

from scripts.report_member_factory_kpi import build_report


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def test_member_factory_kpi_names_thin_queue_and_member_freshness(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    guard = data / "wallet_copy_live_guard_state.json"
    queue = data / "wallet_copy_full_pool_member_queue.json"
    events = data / "wallet_copy_live_guard_wallet_events.jsonl"
    state = data / "member_factory_kpi_state.json"
    _write_json(
        guard,
        {
            "active_set": {
                "members": [
                    {
                        "candidate_id": "member_a",
                        "source_wallet": "0xaaa",
                        "policy_id": "policy_a",
                        "enabled": True,
                    },
                    {
                        "candidate_id": "member_b",
                        "source_wallet": "0xbbb",
                        "policy_id": "policy_b",
                        "enabled": True,
                    },
                ]
            }
        },
    )
    _write_json(
        queue,
        {
            "summary": {
                "ready_for_live": 0,
                "queue_depth": 1,
                "replay_candidates": 10,
                "replay_promotable": 1,
                "fill_backed_candidates": 2,
            }
        },
    )
    _write_json(
        data / "wallet_copy_daily_scorecard_test.json",
        {
            "kind": "wallet_copy_daily_scorecard",
            "volume_kpi": {"canonical_daily": {"windows_filled": 56}},
        },
    )
    events.write_text(
        json.dumps(
            {
                "source_wallet": "0xaaa",
                "observed_ts": 1000.0,
                "market_slug": "btc-updown-5m-1",
                "price": 0.5,
                "size": 2.0,
            }
        )
        + "\n"
    )
    (data / "polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"event_ts": 900.0, "market_slug": "btc-updown-5m-1", "price": 0.5, "size": 2.0}),
                json.dumps(
                    {
                        "event_ts": 910.0,
                        "market_slug": "bitcoin-up-or-down-hourly-1",
                        "raw": {"title": "Bitcoin Up or Down Hourly"},
                        "price": 0.4,
                        "size": 3.0,
                    }
                ),
                json.dumps({"event_ts": 920.0, "market_slug": "eth-updown-5m-1", "price": 0.6, "size": 4.0}),
            ]
        )
        + "\n"
    )
    _write_json(
        state,
        {
            "snapshots": [
                {
                    "ts": 100.0,
                    "member_ids": ["member_a", "member_c"],
                    "member_count": 2,
                    "ready_for_live": 1,
                    "queue_depth": 2,
                    "replay_candidates": 8,
                    "replay_promotable": 1,
                }
            ]
        },
    )

    report = build_report(
        root=tmp_path,
        guard_state_path=guard,
        queue_path=queue,
        wallet_event_log=events,
        state_path=state,
        max_event_tail_bytes=100_000,
        now_ts=1000.0,
    )

    assert report["queue_depth"]["status"] == "DEFECT"
    assert report["queue_depth"]["named_cause"] == "promotion_gate_clearance"
    assert report["set_trajectory"]["added"] == ["member_b"]
    assert report["set_trajectory"]["removed_or_demoted"] == ["member_c"]
    assert report["member_freshness"]["members"][0]["last_copy_eligible_flow_age_s"] == 0.0
    assert "member_b" in report["member_freshness"]["stale_members"]
    assert report["hour_coverage"]["covered_hours"] == 1
    assert report["series_census"]["series"]["btc_5m"]["unique_windows_24h"] == 1
    assert report["series_census"]["series"]["btc_hourly"]["denominator_windows"] == 24
    assert report["series_census"]["series"]["eth_5m"]["unique_windows_24h"] == 1
    assert report["series_census"]["total_across_series"]["unique_windows_24h"] == 3
    assert report["factory_throughput"]["entered_measurement_delta"] == 2
    assert report["factory_throughput"]["passing_delta"] == -1
    assert any(row["defect"] == "member_factory_ready_queue_below_3" for row in report["defects"])


def test_member_factory_weekend_calendar_protects_expected_quiet_members(tmp_path: Path) -> None:
    data = tmp_path / "data" / "research"
    guard = data / "wallet_copy_live_guard_state.json"
    queue = data / "wallet_copy_full_pool_member_queue.json"
    events = data / "wallet_copy_live_guard_wallet_events.jsonl"
    state = data / "member_factory_kpi_state.json"
    dow_profile = data / "member_dow_profiles_latest.json"
    rolling20 = data / "wallet_copy_member_rolling20_latest.json"
    positive_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    quiet_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    zero_weights = {f"{dow}:{hour:02d}": 0.0 for dow in range(7) for hour in range(24)}
    _write_json(
        guard,
        {
            "active_set": {
                "members": [
                    {"candidate_id": "positive", "source_wallet": positive_wallet, "enabled": True},
                    {"candidate_id": "quiet", "source_wallet": quiet_wallet, "enabled": True},
                ]
            }
        },
    )
    _write_json(queue, {"summary": {"ready_for_live": 3, "queue_depth": 3}})
    _write_json(
        data / "wallet_copy_daily_scorecard_test.json",
        {"kind": "wallet_copy_daily_scorecard", "volume_kpi": {"canonical_daily": {"windows_filled": 80}}},
    )
    events.write_text(
        json.dumps(
            {
                "source_wallet": quiet_wallet,
                "observed_ts": 1_783_641_600.0,
                "market_slug": "btc-updown-5m-1783641600",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _write_json(
        dow_profile,
        {
            "profiles_by_wallet": {
                quiet_wallet: {
                    "wallet": quiet_wallet,
                    "trade_count": 10,
                    "weekend_evidence_status": "HAS_WEEKEND_SAMPLE",
                    "expected_active_dow_hour_weights": zero_weights,
                }
            }
        },
    )
    _write_json(
        rolling20,
        {
            "rows": [
                {"wallet": positive_wallet, "rolling20_pnl_usd": 4.2, "rolling20_n": 20},
                {"wallet": quiet_wallet, "rolling20_pnl_usd": -1.0, "rolling20_n": 2},
            ]
        },
    )

    report = build_report(
        root=tmp_path,
        guard_state_path=guard,
        queue_path=queue,
        wallet_event_log=events,
        state_path=state,
        max_event_tail_bytes=100_000,
        dow_profile_path=dow_profile,
        member_rolling20_path=rolling20,
        now_ts=1_783_771_200.0,
    )

    freshness = report["member_freshness"]
    assert "positive" not in freshness["stale_members"]
    assert "quiet" not in freshness["stale_members"]
    assert "positive" in freshness["weekend_positive_pnl_protected_members"]
    assert "quiet" in freshness["calendar_protected_members"]
    rows = {row["candidate_id"]: row for row in freshness["members"]}
    assert rows["positive"]["weekend_positive_pnl_protected"] is True
    assert rows["quiet"]["calendar_expected_active_age_s"] == 0.0
    assert rows["quiet"]["calendar_aware_stale_gt_24h"] is False
