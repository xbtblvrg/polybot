import json
from datetime import datetime, timezone

from scripts.report_cohort_alive_admission_packets import (
    _history_completeness_by_wallet_from_manifest,
    build_report,
    build_registry_admission_packets,
    build_registry_f1_probe_queue,
)


def test_cohort_alive_admission_packets_join_liveness_temporal_and_denylist():
    now = datetime(2026, 7, 14, 12, 30, tzinfo=timezone.utc)
    pass_wallet = "0x1111111111111111111111111111111111111111"
    unproven_wallet = "0x2222222222222222222222222222222222222222"
    stale_wallet = "0x3333333333333333333333333333333333333333"
    deny_wallet = "0x4444444444444444444444444444444444444444"
    cohort = {
        "generated_at": "2026-07-14T12:00:00Z",
        "live_ready_picks": [
            {"wallet": unproven_wallet, "paper_pnl_usd": 50.0, "resolved_copyable_events": 20},
            {"wallet": pass_wallet, "paper_pnl_usd": 10.0, "resolved_copyable_events": 12},
            {"wallet": stale_wallet, "paper_pnl_usd": 100.0, "resolved_copyable_events": 30},
            {"wallet": deny_wallet, "paper_pnl_usd": 80.0, "resolved_copyable_events": 25},
        ],
    }
    liveness = {
        "generated_at": "2026-07-14T12:00:00Z",
        "rows": [
            {
                "wallet": pass_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 60,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
            {
                "wallet": unproven_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 120,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
            {
                "wallet": stale_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 49 * 3600,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
            {
                "wallet": deny_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 180,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
        ],
    }
    temporal = {
        "generated_at": "2026-07-14T12:00:00Z",
        "wallets": [
            {
                "wallet": pass_wallet,
                "classification": "WEEKDAY-ONLY",
                "slice_labels": {
                    "weekday": {
                        "label": "PROVEN-POSITIVE",
                        "resolved_trades": 12,
                        "roi_pct": 4.5,
                        "pnl_usd": 10.0,
                    }
                },
            }
        ],
    }
    denylist = {
        "cells": [
            {
                "source_wallet": deny_wallet,
                "price_bucket": "01_25_50",
                "deny_rule": "signals_100_roi_le_0",
                "reason": "toxicity_protection",
            }
        ]
    }
    live_guard = {"active_set_runtime": {"members": [{"source_wallet": pass_wallet}]}}

    report = build_report(
        cohort_replay=cohort,
        liveness_probe=liveness,
        temporal=temporal,
        denylist=denylist,
        live_guard_state=live_guard,
        generated_at=now,
        packet_limit=10,
    )

    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["summary"]["raw_live_ready_picks"] == 4
    assert report["summary"]["alive_liveness_pass"] == 3
    assert report["summary"]["liveness_fail_reason_counts"] == {"external_liveness_age_gte_24h": 1}
    assert report["packets"][0]["wallet"] == pass_wallet
    assert report["packets"][0]["hour_match_status"] == "PASS_PROVEN_POSITIVE_ACTIVE_SLICE"
    assert report["packets"][0]["recommendation"] == "ALREADY_ACTIVE_MEASURE_LIVE"
    deny_packet = next(row for row in report["packets"] if row["wallet"] == deny_wallet)
    assert deny_packet["recommendation"] == "HOLD_DENYLIST_CELL_PRESENT"
    assert deny_packet["denylist_cells"][0]["price_bucket"] == "01_25_50"


def test_cohort_alive_admission_packets_can_require_source_active_policy():
    now = datetime(2026, 7, 14, 13, 30, tzinfo=timezone.utc)
    eligible_wallet = "0x1111111111111111111111111111111111111111"
    pending_wallet = "0x2222222222222222222222222222222222222222"
    cohort = {
        "generated_at": "2026-07-14T13:00:00Z",
        "live_ready_picks": [
            {"wallet": pending_wallet, "paper_pnl_usd": 100.0, "resolved_copyable_events": 20},
            {"wallet": eligible_wallet, "paper_pnl_usd": 10.0, "resolved_copyable_events": 12},
        ],
    }
    liveness = {
        "generated_at": "2026-07-14T13:00:00Z",
        "rows": [
            {
                "wallet": eligible_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 60,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
            {
                "wallet": pending_wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 60,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            },
        ],
    }
    temporal = {
        "generated_at": "2026-07-14T13:00:00Z",
        "wallets": [
            {
                "wallet": eligible_wallet,
                "classification": "WEEKDAY-ONLY",
                "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 12, "roi_pct": 5.0}},
            },
            {
                "wallet": pending_wallet,
                "classification": "WEEKDAY-ONLY",
                "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 12, "roi_pct": 5.0}},
            },
        ],
    }
    source_active = {
        "reports": [
            {
                "wallet": eligible_wallet,
                "source_active_tally_status": "PASS",
                "policy_eligible_tally_status": "PASS",
                "source_active_windows": 2,
                "policy_eligible_windows": 1,
            },
            {
                "wallet": pending_wallet,
                "source_active_tally_status": "PASS",
                "policy_eligible_tally_status": "PENDING",
                "source_active_windows": 2,
                "policy_eligible_windows": 0,
            },
        ]
    }

    report = build_report(
        cohort_replay=cohort,
        liveness_probe=liveness,
        temporal=temporal,
        denylist={},
        live_guard_state={},
        source_active_cohort=source_active,
        history_completeness_by_wallet={
            eligible_wallet: {
                "history_completeness": "complete",
                "artifact_count": 1,
                "stop_reason_counts": {"lookback_cutoff_reached": 1},
            },
            pending_wallet: {
                "history_completeness": "truncated",
                "artifact_count": 1,
                "stop_reason_counts": {"pagination_cap_reached": 1},
            },
        },
        require_source_active_policy=True,
        generated_at=now,
        packet_limit=10,
    )

    assert report["summary"]["source_active_policy_eligible"] == 1
    assert report["summary"]["source_active_policy_pending_or_missing"] == 1
    assert report["summary"]["source_active_policy_hour_match_pass"] == 1
    assert report["summary"]["four_way_admission_ready"] == 1
    assert report["summary"]["history_completeness_counts"] == {"complete": 1}
    assert [row["wallet"] for row in report["packets"]] == [eligible_wallet]
    assert report["packets"][0]["recommendation"] == "ADMISSION_PACKET_READY"
    assert report["packets"][0]["history_completeness"] == "complete"
    assert report["packets"][0]["history_completeness_detail"]["stop_reason_counts"] == {
        "lookback_cutoff_reached": 1
    }


def test_history_completeness_manifest_tags_truncated_when_any_replay_caps(tmp_path):
    complete_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    capped_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    complete = tmp_path / "complete.json"
    capped = tmp_path / "capped.json"
    complete.write_text(
        json.dumps(
            {
                "wallets": [{"address": complete_wallet}],
                "replay": {"batch_id": "b1", "stop_reason": "short_page"},
            }
        ),
        encoding="utf-8",
    )
    capped.write_text(
        json.dumps(
            {
                "wallets": [{"address": capped_wallet}],
                "replay": {"batch_id": "b2", "stop_reason": "pagination_cap_reached", "pagination_cap_reached": True},
            }
        ),
        encoding="utf-8",
    )

    rows = _history_completeness_by_wallet_from_manifest(
        {"supplemental_history_files": [str(complete), str(capped)]},
        root=tmp_path,
    )

    assert rows[complete_wallet]["history_completeness"] == "complete"
    assert rows[capped_wallet]["history_completeness"] == "truncated"
    assert rows[capped_wallet]["stop_reason_counts"] == {"pagination_cap_reached": 1}


def test_truncated_history_disarms_four_way_readiness_and_probe_suggestion():
    now = datetime(2026, 7, 20, 5, 0, tzinfo=timezone.utc)
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    report = build_report(
        cohort_replay={"live_ready_picks": [{"wallet": wallet, "paper_pnl_usd": 10.0}]},
        liveness_probe={
            "generated_at": "2026-07-20T05:00:00Z",
            "rows": [{
                "wallet": wallet,
                "status": "PASS",
                "latest_btc5m_trade_ts": now.timestamp() - 60,
                "btc5m_trades_24h": 5,
                "btc5m_buys_24h": 5,
            }],
        },
        temporal={"wallets": [{
            "wallet": wallet,
            "classification": "WEEKDAY-ONLY",
            "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 20, "roi_pct": 5.0}},
        }]},
        denylist={},
        live_guard_state={},
        source_active_cohort={"reports": [{
            "wallet": wallet,
            "source_active_tally_status": "PASS",
            "policy_eligible_tally_status": "PASS",
            "source_active_windows": 2,
            "policy_eligible_windows": 2,
        }]},
        history_completeness_by_wallet={wallet: {
            "history_completeness": "truncated",
            "stop_reason_counts": {"pagination_cap_reached": 1},
        }},
        require_source_active_policy=True,
        generated_at=now,
        packet_limit=10,
    )

    assert report["packets"][0]["recommendation"] == "DEEP_HISTORY_PENDING"
    assert report["summary"]["four_way_admission_ready"] == 0
    assert report["summary"]["deep_history_pending"] == 1
    assert "complete deep history" in report["summary"]["next_action"]
    assert "config reload" not in report["summary"]["next_action"]


def test_registry_screen_keeps_only_fresh_unchanged_weekday_f1_rows():
    now = datetime(2026, 7, 21, 5, 0, tzinfo=timezone.utc)
    winner = "0x1111111111111111111111111111111111111111"
    thin = "0x2222222222222222222222222222222222222222"
    stale = "0x3333333333333333333333333333333333333333"
    report = build_registry_admission_packets(
        registry={"wallets": [{"address": winner}, {"address": thin}, {"address": stale}]},
        liveness_probe={
            "generated_at": "2026-07-21T05:00:00Z",
            "rows": [
                {"wallet": winner, "status": "PASS", "latest_btc5m_trade_ts": now.timestamp() - 60, "btc5m_trades_24h": 5},
                {"wallet": thin, "status": "PASS", "latest_btc5m_trade_ts": now.timestamp() - 60, "btc5m_trades_24h": 5},
                {"wallet": stale, "status": "PASS", "latest_btc5m_trade_ts": now.timestamp() - 25 * 3600, "btc5m_trades_24h": 5},
            ],
        },
        temporal={
            "wallets": [
                {"wallet": winner, "classification": "WEEKDAY-ONLY", "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 250, "pnl_usd": 12.0, "roi_pct": 3.0}}},
                {"wallet": thin, "classification": "WEEKDAY-ONLY", "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 199, "pnl_usd": 20.0, "roi_pct": 4.0}}},
                {"wallet": stale, "classification": "WEEKDAY-ONLY", "slice_labels": {"weekday": {"label": "PROVEN-POSITIVE", "resolved_trades": 300, "pnl_usd": 30.0, "roi_pct": 5.0}}},
            ]
        },
        live_guard_state={},
        generated_at=now,
        packet_limit=50,
    )

    assert report["registry_rows"] == 3
    assert report["registry_unique_addresses"] == 3
    assert report["packet_count"] == 1
    assert report["packets"][0]["wallet"] == winner
    assert report["packets"][0]["live_orders_allowed"] is False
    assert report["packets"][0]["fresh_own_source_buy_rows_30m"] == 1
    assert report["packets"][0]["registry_screen"]["f1_min_resolved_signals"] == 200

    queue = build_registry_f1_probe_queue(
        registry={"wallets": [{"address": winner}, {"address": thin}]},
        temporal={"wallets": [
            {"wallet": winner, "slice_labels": {"weekday": {"resolved_trades": 250, "pnl_usd": 12.0, "roi_pct": 3.0}}},
            {"wallet": thin, "slice_labels": {"weekday": {"resolved_trades": 199, "pnl_usd": 20.0, "roi_pct": 4.0}}},
        ]},
    )
    assert queue["summary"]["ranked_members"] == 1
    assert queue["ranked_members"][0]["wallet"] == winner
    assert queue["live_orders_allowed"] is False
