from __future__ import annotations

import os
from pathlib import Path

import requests

from scripts.build_top10_broad_paper_lane import (
    apply_liquidity_depth_filter,
    build_state,
    load_recent_activity_counts,
    load_recent_activity_profiles,
)
from scripts.report_fill_quality import build_fill_quality_report
from scripts.run_top10_broad_paper_lane import (
    _disable_source_base_overrides,
    _iter_recent_jsonl,
    _normalized_realtime_event,
    _restore_source_base_overrides,
    build_measurement_state,
)
from src.wallet_copy.http_client import PolymarketRouteError
from src.wallet_copy.live_tracker import CLOBMarketClient


class FakeCLOB:
    def __init__(self, books: dict[str, dict]):
        self.books = books
        self.last_route_report = {"status": "PASS", "route_class": "unit"}
        self.calls: list[str] = []

    def get_book(self, token_id: str) -> dict:
        self.calls.append(token_id)
        return self.books[token_id]


class ErrorCLOB(FakeCLOB):
    def __init__(self, exc: Exception):
        super().__init__({})
        self.exc = exc

    def get_book(self, token_id: str) -> dict:
        self.calls.append(token_id)
        raise self.exc


def test_top10_measurement_can_disable_clob_source_base_override(monkeypatch) -> None:
    monkeypatch.setenv("POLYMARKET_CLOB_API_BASE_URL", "http://127.0.0.1:8787/clob")

    prior = _disable_source_base_overrides(True)

    assert prior == {"POLYMARKET_CLOB_API_BASE_URL": "http://127.0.0.1:8787/clob"}
    assert "POLYMARKET_CLOB_API_BASE_URL" not in os.environ

    _restore_source_base_overrides(prior)

    assert os.environ["POLYMARKET_CLOB_API_BASE_URL"] == "http://127.0.0.1:8787/clob"


def _lane_state(wallets: list[str]) -> dict:
    return {
        "ranked_wallets": [
            {
                "rank": index,
                "wallet": wallet,
                "category": "SPORTS",
                "user_name": f"wallet-{index}",
                "leaderboard_pnl_max": 1000.0 - index,
            }
            for index, wallet in enumerate(wallets, start=1)
        ]
    }


def test_top10_lane_builder_honors_bounded_candidate_allowlist() -> None:
    allowed = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    excluded = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    state = build_state(
        leaderboard_state={
            "candidate_wallets": [
                {"address": excluded, "pnl_by_period": {"WEEK": 100.0}},
                {"address": allowed, "pnl_by_period": {"WEEK": 1.0}},
            ]
        },
        profit_state={},
        live_fill_report={},
        measurement_state={
            "wallets": {
                excluded: {
                    "wallet": excluded,
                    "paper_pnl_usd": 5.0,
                    "copyable_rate_pct": 100.0,
                    "buy_events": 5,
                    "copyable_buy_events": 5,
                }
            }
        },
        candidate_allowlist={allowed},
        limit=2,
    )

    assert [row["wallet"] for row in state["ranked_wallets"]] == [allowed]
    assert state["selection"]["candidate_wallets"] == 1
    assert state["selection"]["source"] == "bounded_candidate_allowlist"


def _polygon_row(
    *,
    wallet: str,
    tx: str,
    asset: str,
    price: float = 0.5,
    side: str = "BUY",
    market_slug: str = "",
    source: str = "polygon_ws",
) -> dict:
    taker = "0xcccccccccccccccccccccccccccccccccccccccc"
    row = {
        "event": "polygon_orderfilled_log",
        "source": source,
        "transaction_hash": tx,
        "log_index": 1,
        "maker": wallet,
        "taker": taker,
        "block_ts": 1783093544.0,
        "received_at_s": 1783093545.0,
        "decoded": {
            "decode_status": "OK",
            "maker_side": side,
            "side": side,
            "asset": asset,
            "price": price,
            "size": 40.0,
        },
    }
    if market_slug:
        row["market_slug"] = market_slug
    return row


def _rtds_row(
    *,
    wallet: str,
    tx: str,
    asset: str,
    price: float = 0.5,
    side: str = "BUY",
    market_slug: str = "btc-updown-5m-1783093500",
) -> dict:
    return {
        "event": "rtds_trade_event",
        "source": "rtds_activity",
        "source_wallet": wallet,
        "transaction_hash": tx,
        "asset": asset,
        "condition_id": "0xcond",
        "market_slug": market_slug,
        "outcome": "Up",
        "side": side,
        "price": price,
        "size": 40.0,
        "event_ts": 1783093544.0,
        "received_at_s": 1783093544.5,
        "raw": {"slug": market_slug, "title": "Bitcoin Up or Down"},
    }


def test_top10_measurement_scores_copyable_buys_and_paper_pnl() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
            "token-b": {
                "asset_id": "token-b",
                "timestamp": "1783093545000",
                "hash": "book-b",
                "asks": [{"price": "0.80", "size": "20"}],
                "bids": [{"price": "0.48", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet_a, wallet_b]),
        polygon_rows=[
            _polygon_row(wallet=wallet_a, tx="0xaaa", asset="token-a"),
            _polygon_row(wallet=wallet_b, tx="0xbbb", asset="token-b"),
        ],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
    )

    wallet_a_metrics = state["wallets"][wallet_a]
    wallet_b_metrics = state["wallets"][wallet_b]
    assert state["flow_stage"] == "OBSERVE"
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert len(events) == 2
    assert wallet_a_metrics["copyable_buy_events"] == 1
    assert wallet_a_metrics["copyable_rate_pct"] == 100.0
    assert wallet_a_metrics["paper_pnl_usd"] == -0.039216
    assert wallet_b_metrics["rejected_buy_events"] == 1
    assert wallet_b_metrics["copyable_rate_pct"] == 0.0
    assert wallet_b_metrics["reject_reasons"] == {"price_above_slippage_cap": 1}
    assert state["summary"]["market_categories"]["sports"]["buy_events"] == 2
    assert state["summary"]["market_categories"]["sports"]["copyable_buy_events"] == 1


def test_top10_measurement_accepts_rtds_trade_events() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[_rtds_row(wallet=wallet, tx="0xaaa", asset="token-a")],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
    )

    assert len(events) == 1
    assert events[0]["source"] == "rtds_activity"
    assert events[0]["primary_market_category"] == "btc_5m"
    assert state["source"]["source_counts"] == {"rtds_activity": 1}
    assert state["wallets"][wallet]["copyable_buy_events"] == 1
    assert state["summary"]["market_categories"]["btc_5m"]["buy_events"] == 1


def test_top10_measurement_accepts_http_tail_orderfilled_rows() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(
                wallet=wallet,
                tx="0xtail",
                asset="token-a",
                market_slug="btc-updown-5m-1783093500",
                source="polygon_http_getLogs_tail",
            )
        ],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
        now_s=1783093546.0,
    )

    assert len(events) == 1
    assert events[0]["source"] == "polygon_http_getLogs_tail"
    assert state["source"]["source_counts"] == {"polygon_http_getLogs_tail": 1}
    assert state["wallets"][wallet]["copyable_buy_events"] == 1
    assert state["summary"]["market_categories"]["btc_5m"]["buy_events"] == 1


def test_top10_measurement_classifies_clob_http_status() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    response = requests.Response()
    response.status_code = 429
    request = requests.Request("GET", "https://clob.example/book").prepare()
    response.request = request
    exc = requests.HTTPError("rate limited", response=response)

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(
                wallet=wallet,
                tx="0x429",
                asset="token-a",
                market_slug="btc-updown-5m-1783093500",
                source="polygon_http_getLogs_tail",
            )
        ],
        clob=ErrorCLOB(exc),
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
        now_s=1783093546.0,
    )

    assert state["summary"]["diagnostics"] == {"clob_http_429:HTTPError": 1}
    assert state["wallets"][wallet]["reject_reasons"] == {"clob_http_429:HTTPError": 1}
    assert events[0]["result"]["http_status_code"] == 429
    assert events[0]["result"]["endpoint"] == "https://clob.example/book"


def test_top10_measurement_summarizes_btc5m_event_category_from_slug() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(
                wallet=wallet,
                tx="0xaaa",
                asset="token-a",
                market_slug="btc-updown-5m-1783093500",
            )
        ],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
    )

    assert events[0]["primary_market_category"] == "btc_5m"
    assert state["wallets"][wallet]["market_category_metrics"]["btc_5m"]["buy_events"] == 1
    assert state["summary"]["market_categories"]["btc_5m"]["copyable_buy_events"] == 1


def test_top10_lane_builder_merges_realtime_measurement_metrics() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": wallet_a,
                "categories": ["SPORTS"],
                "user_name": "alpha",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": wallet_b,
                "categories": ["SPORTS"],
                "user_name": "beta",
                "pnl_by_period": {"WEEK": 20.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 2},
                "periods": ["WEEK"],
            },
        ]
    }
    measurement_state = {
        "status": "WATCH",
        "summary": {"wallets_with_buy_sample": 1},
        "wallets": {
            wallet_a: {
                "wallet": wallet_a,
                "paper_pnl_usd": 2.25,
                "realized_pnl_usd": 1.0,
                "unrealized_pnl_usd": 1.25,
                "copyable_rate_pct": 100.0,
                "paper_orders": 3,
                "buy_events": 3,
                "copyable_buy_events": 3,
                "events_seen": 3,
                "sample_status": "HAS_BUY_SAMPLE",
            },
            wallet_b: {
                "wallet": wallet_b,
                "paper_pnl_usd": 0.0,
                "copyable_rate_pct": 0.0,
                "paper_orders": 0,
                "buy_events": 0,
                "events_seen": 0,
                "sample_status": "NO_REALTIME_EVENTS",
            },
        },
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={"orders": 10, "filled": 5, "rejected": 5},
        measurement_state=measurement_state,
        limit=2,
    )

    assert state["flow_stage"] == "OBSERVE"
    assert state["ranked_wallets"][0]["wallet"] == wallet_a
    assert state["ranked_wallets"][0]["evidence_source"] == "polygon_ws_top10_paper_measurement"
    assert state["ranked_wallets"][0]["paper_pnl_usd"] == 2.25
    assert state["ranked_wallets"][0]["copyable_rate_pct"] == 100.0
    assert state["ranked_wallets"][0]["primary_market_category"] == "sports"
    assert state["selection"]["selected_market_category_counts"] == {"sports": 2}
    assert state["selection"]["candidate_market_category_counts"] == {"sports": 2}
    assert "top10_realtime_buy_sample_missing" in state["blockers"]


def test_top10_lane_builder_reports_policy_measurements_side_by_side() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": wallet,
                "categories": ["SPORTS"],
                "user_name": "alpha",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            }
        ]
    }
    primary = {
        "policy_id": "mission_5pct",
        "status": "WATCH",
        "sizing": {"wallet_fraction": 0.05, "max_order_usd": 2.0, "min_order_usd": 1.0},
        "summary": {"buy_events": 3, "copyable_buy_events": 1, "paper_pnl_usd": -0.1},
        "wallets": {
            wallet: {
                "wallet": wallet,
                "paper_pnl_usd": -0.1,
                "copyable_rate_pct": 33.333333,
                "buy_events": 3,
                "copyable_buy_events": 1,
                "events_seen": 3,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }
    segmented = {
        "policy_id": "segmented_25pct",
        "status": "WATCH",
        "sizing": {"wallet_fraction": 0.25, "max_order_usd": 2.0, "min_order_usd": 1.0},
        "summary": {"buy_events": 3, "copyable_buy_events": 2, "paper_pnl_usd": 0.25},
        "wallets": {
            wallet: {
                "wallet": wallet,
                "paper_pnl_usd": 0.25,
                "copyable_rate_pct": 66.666667,
                "buy_events": 3,
                "copyable_buy_events": 2,
                "events_seen": 3,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state=primary,
        comparison_measurement_states=[segmented],
        limit=1,
    )

    row = state["ranked_wallets"][0]
    assert row["wallet"] == wallet
    assert row["paper_pnl_usd_by_policy"] == {"mission_5pct": -0.1, "segmented_25pct": 0.25}
    assert row["copyable_buy_events_by_policy"] == {"mission_5pct": 1, "segmented_25pct": 2}
    assert set(state["paper_measurement"]["policy_summaries"]) == {"mission_5pct", "segmented_25pct"}
    assert row["live_executable_paper_eligible"] is True
    assert row["paper_eligible_policy_ids"] == ["segmented_25pct"]
    assert state["selection"]["paper_positive_copyable_selected_wallets"] == 1


def test_top10_lane_builder_requires_positive_execution_profile_when_enabled() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    measurement_state = {
        "policy_id": "mission_5pct",
        "status": "WATCH",
        "wallets": {
            wallet: {
                "wallet": wallet,
                "paper_pnl_usd": 0.5,
                "copyable_rate_pct": 100.0,
                "buy_events": 2,
                "copyable_buy_events": 2,
                "events_seen": 2,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }

    state = build_state(
        leaderboard_state={
            "candidate_wallets": [
                {
                    "address": wallet,
                    "categories": ["CRYPTO"],
                    "user_name": "alpha",
                    "pnl_by_period": {"WEEK": 10.0},
                    "vol_by_period": {"WEEK": 100.0},
                    "ranks": {"WEEK": 1},
                    "periods": ["WEEK"],
                }
            ]
        },
        profit_state={},
        live_fill_report={},
        measurement_state=measurement_state,
        execution_profiles_state={
            "kind": "wallet_copy_execution_profiles",
            "flow_stage": "LEARN",
            "status": "ANALYZE",
            "latency_horizon_s": 2.0,
            "profile_count": 1,
            "eligible_profile_count": 0,
            "profiles_by_wallet": {
                wallet: {
                    "wallet": wallet,
                    "eligible": False,
                    "fill_sample": 2,
                    "copyable_rate_pct": 50.0,
                    "mean_edge": -0.01,
                    "median_edge": -0.01,
                    "blockers": ["execution_profile_mean_edge_not_positive"],
                }
            },
        },
        limit=1,
    )

    row = state["ranked_wallets"][0]
    assert row["paper_policy_gate"]["eligible"] is True
    assert row["copyability_profile_gate_enabled"] is True
    assert row["copyability_profile_eligible"] is False
    assert row["live_executable_paper_eligible"] is False
    assert "copyability_execution_profile_not_positive_at_latency" in row["blockers"]
    assert "top10_copyability_execution_profile_missing_or_not_positive" in state["blockers"]


def test_top10_lane_builder_prefers_execution_profile_positive_candidate() -> None:
    profile_positive = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    profile_missing = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    state = build_state(
        leaderboard_state={
            "candidate_wallets": [
                {
                    "address": profile_missing,
                    "categories": ["SPORTS"],
                    "user_name": "missing",
                    "pnl_by_period": {"WEEK": 100.0},
                    "vol_by_period": {"WEEK": 100.0},
                    "ranks": {"WEEK": 1},
                    "periods": ["WEEK"],
                },
                {
                    "address": profile_positive,
                    "categories": ["CRYPTO"],
                    "user_name": "profile-positive",
                    "pnl_by_period": {"WEEK": 1.0},
                    "vol_by_period": {"WEEK": 100.0},
                    "ranks": {"WEEK": 10},
                    "periods": ["WEEK"],
                },
            ]
        },
        profit_state={},
        live_fill_report={},
        execution_profiles_state={
            "kind": "wallet_copy_execution_profiles",
            "flow_stage": "LEARN",
            "status": "PASS",
            "latency_horizon_s": 2.0,
            "profile_count": 1,
            "eligible_profile_count": 1,
            "profiles_by_wallet": {
                profile_positive: {
                    "wallet": profile_positive,
                    "eligible": True,
                    "fill_sample": 20,
                    "copyable_rate_pct": 75.0,
                    "mean_edge": 0.02,
                    "median_edge": 0.01,
                    "blockers": [],
                }
            },
        },
        recent_activity_counts={profile_missing: 3, profile_positive: 1},
        limit=2,
    )

    assert state["selection"]["copyability_profile_gate"]["enabled"] is True
    assert state["ranked_wallets"][0]["wallet"] == profile_positive
    assert state["ranked_wallets"][0]["copyability_profile_eligible"] is True
    assert state["ranked_wallets"][1]["wallet"] == profile_missing
    assert "copyability_execution_profile_missing" in state["ranked_wallets"][1]["blockers"]


def test_top10_lane_builder_marks_negative_measured_cohort_not_rotation_ready() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": wallet,
                "categories": ["SPORTS"],
                "user_name": "negative",
                "pnl_by_period": {"WEEK": 100.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            }
        ]
    }
    measurement_state = {
        "policy_id": "mission_5pct",
        "status": "WATCH",
        "sizing": {"wallet_fraction": 0.05, "max_order_usd": 2.0, "min_order_usd": 1.0},
        "summary": {"buy_events": 5, "copyable_buy_events": 2, "paper_pnl_usd": -0.25},
        "wallets": {
            wallet: {
                "wallet": wallet,
                "paper_pnl_usd": -0.25,
                "copyable_rate_pct": 40.0,
                "buy_events": 5,
                "copyable_buy_events": 2,
                "events_seen": 5,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state=measurement_state,
        limit=1,
    )

    row = state["ranked_wallets"][0]
    assert state["status"] == "ANALYZE"
    assert state["selection"]["paper_positive_copyable_selected_wallets"] == 0
    assert state["blockers"] == ["top10_no_positive_copyable_live_executable_policy"]
    assert row["live_executable_paper_eligible"] is False
    assert row["paper_policy_gate"]["has_copyable_buy"] is True
    assert row["paper_policy_gate"]["has_positive_paper_pnl"] is False
    assert "no_positive_copyable_live_executable_policy" in row["blockers"]
    assert "no_positive_paper_pnl_policy" in row["blockers"]
    assert state["next_action"] == "reject this measured cohort for rotation and broaden/retune selector toward positive liquid copyability"


def test_top10_lane_builder_replaces_failed_measured_wallet_with_unmeasured_active_candidate() -> None:
    failed_measured = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    fresh_active = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": failed_measured,
                "categories": ["SPORTS"],
                "user_name": "failed",
                "pnl_by_period": {"WEEK": 1000.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": fresh_active,
                "categories": ["CRYPTO"],
                "user_name": "fresh",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 2},
                "periods": ["WEEK"],
            },
        ]
    }
    measurement_state = {
        "policy_id": "mission_5pct",
        "status": "WATCH",
        "sizing": {"wallet_fraction": 0.05, "max_order_usd": 2.0, "min_order_usd": 1.0},
        "summary": {"buy_events": 5, "copyable_buy_events": 1, "paper_pnl_usd": -0.1},
        "wallets": {
            failed_measured: {
                "wallet": failed_measured,
                "paper_pnl_usd": -0.1,
                "copyable_rate_pct": 20.0,
                "buy_events": 5,
                "copyable_buy_events": 1,
                "events_seen": 5,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state=measurement_state,
        recent_activity_counts={
            fresh_active: {
                "events": 1,
                "buy_events": 1,
                "copy_sized_buy_events": 1,
                "max_source_buy_usd": 30.0,
            }
        },
        min_source_buy_usd=20.0,
        limit=1,
    )

    assert state["ranked_wallets"][0]["wallet"] == fresh_active
    assert state["ranked_wallets"][0]["evidence_source"] == "profit_engine_or_missing"
    assert state["blockers"] == ["top10_parallel_paper_copy_metrics_missing"]
    assert state["next_action"] == "run realtime paper-copy measurement for selected wallets until each has paper PnL at our copy prices and copyable_rate"


def test_top10_lane_builder_replaces_failed_measured_wallet_with_liquid_candidate_first() -> None:
    failed_measured = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    non_liquid_fresh = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    liquid_fresh = "0xcccccccccccccccccccccccccccccccccccccccc"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": failed_measured,
                "categories": ["SPORTS"],
                "user_name": "failed",
                "pnl_by_period": {"WEEK": 1000.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": non_liquid_fresh,
                "categories": ["SPORTS"],
                "user_name": "non-liquid",
                "pnl_by_period": {"WEEK": 900.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 2},
                "periods": ["WEEK"],
            },
            {
                "address": liquid_fresh,
                "categories": ["CRYPTO"],
                "user_name": "liquid",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 20},
                "periods": ["WEEK"],
            },
        ]
    }
    measurement_state = {
        "policy_id": "mission_5pct",
        "status": "WATCH",
        "sizing": {"wallet_fraction": 0.05, "max_order_usd": 2.0, "min_order_usd": 1.0},
        "summary": {"buy_events": 5, "copyable_buy_events": 1, "paper_pnl_usd": -0.1},
        "wallets": {
            failed_measured: {
                "wallet": failed_measured,
                "paper_pnl_usd": -0.1,
                "copyable_rate_pct": 20.0,
                "buy_events": 5,
                "copyable_buy_events": 1,
                "events_seen": 5,
                "sample_status": "HAS_BUY_SAMPLE",
            }
        },
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state=measurement_state,
        recent_activity_counts={
            non_liquid_fresh: {
                "events": 5,
                "buy_events": 5,
                "copy_sized_buy_events": 5,
                "liquid_copy_sized_buy_events": 0,
                "max_source_buy_usd": 30.0,
            },
            liquid_fresh: {
                "events": 1,
                "buy_events": 1,
                "copy_sized_buy_events": 1,
                "liquid_copy_sized_buy_events": 1,
                "max_source_buy_usd": 25.0,
                "max_recent_ask_depth_usd": 5.0,
            },
        },
        min_source_buy_usd=20.0,
        min_ask_depth_usd=1.0,
        liquidity_summary={"enabled": True, "eligible_assets": 1},
        limit=1,
    )

    assert state["selection"]["liquid_copy_sized_active_candidate_wallets"] == 1
    assert state["ranked_wallets"][0]["wallet"] == liquid_fresh
    assert state["ranked_wallets"][0]["recent_liquid_copy_sized_buy_events"] == 1
    assert "top10_liquid_copy_sized_buy_missing" not in state["ranked_wallets"][0]["blockers"]
    assert state["blockers"] == ["top10_parallel_paper_copy_metrics_missing"]


def test_top10_lane_builder_prefers_recent_active_candidates() -> None:
    inactive_high_pnl = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    active_lower_pnl = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": inactive_high_pnl,
                "categories": ["SPORTS"],
                "user_name": "inactive",
                "pnl_by_period": {"WEEK": 1000.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": active_lower_pnl,
                "categories": ["CRYPTO"],
                "user_name": "active",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 20},
                "periods": ["WEEK"],
            },
        ]
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state={},
        recent_activity_counts={active_lower_pnl: 3},
        limit=2,
    )

    assert state["selection"]["active_candidate_wallets"] == 1
    assert state["selection"]["ranking"] == (
        "recent_polygon_activity_desc_then_active_hour_coverage_desc_then_leaderboard_pnl_max_desc_then_best_rank"
    )
    assert state["ranked_wallets"][0]["wallet"] == active_lower_pnl
    assert state["ranked_wallets"][0]["recent_polygon_ws_events"] == 3


def test_top10_lane_builder_prefers_copy_sized_recent_buy_candidates() -> None:
    tiny_active = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    copy_sized_active = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": tiny_active,
                "categories": ["SPORTS"],
                "user_name": "tiny",
                "pnl_by_period": {"WEEK": 1000.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": copy_sized_active,
                "categories": ["CRYPTO"],
                "user_name": "copy-sized",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 20},
                "periods": ["WEEK"],
            },
        ]
    }

    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state={},
        recent_activity_counts={
            tiny_active: {
                "events": 12,
                "buy_events": 12,
                "copy_sized_buy_events": 0,
                "max_source_buy_usd": 5.0,
            },
            copy_sized_active: {
                "events": 1,
                "buy_events": 1,
                "copy_sized_buy_events": 1,
                "max_source_buy_usd": 25.0,
            },
        },
        min_source_buy_usd=20.0,
        limit=2,
    )

    assert state["selection"]["copy_sized_active_candidate_wallets"] == 1
    assert state["selection"]["min_source_buy_usd"] == 20.0
    assert state["ranked_wallets"][0]["wallet"] == copy_sized_active
    assert state["ranked_wallets"][0]["recent_copy_sized_buy_events"] == 1
    assert state["ranked_wallets"][0]["max_recent_source_buy_usd"] == 25.0


def test_top10_lane_builder_prefers_liquid_copy_sized_recent_buy_candidates() -> None:
    empty_book_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    liquid_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    leaderboard_state = {
        "candidate_wallets": [
            {
                "address": empty_book_wallet,
                "categories": ["SPORTS"],
                "user_name": "empty",
                "pnl_by_period": {"WEEK": 1000.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 1},
                "periods": ["WEEK"],
            },
            {
                "address": liquid_wallet,
                "categories": ["CRYPTO"],
                "user_name": "liquid",
                "pnl_by_period": {"WEEK": 10.0},
                "vol_by_period": {"WEEK": 100.0},
                "ranks": {"WEEK": 20},
                "periods": ["WEEK"],
            },
        ]
    }
    profiles = {
        empty_book_wallet: {
            "events": 12,
            "buy_events": 12,
            "copy_sized_buy_events": 12,
            "max_source_buy_usd": 40.0,
            "recent_buy_assets": {
                "token-empty": {
                    "asset": "token-empty",
                    "buy_events": 12,
                    "copy_sized_buy_events": 12,
                    "total_source_buy_usd": 480.0,
                    "max_source_buy_usd": 40.0,
                }
            },
        },
        liquid_wallet: {
            "events": 1,
            "buy_events": 1,
            "copy_sized_buy_events": 1,
            "max_source_buy_usd": 25.0,
            "recent_buy_assets": {
                "token-liquid": {
                    "asset": "token-liquid",
                    "buy_events": 1,
                    "copy_sized_buy_events": 1,
                    "total_source_buy_usd": 25.0,
                    "max_source_buy_usd": 25.0,
                }
            },
        },
    }
    clob = FakeCLOB(
        {
            "token-empty": {
                "asset_id": "token-empty",
                "asks": [],
                "bids": [{"price": "0.49", "size": "100"}],
            },
            "token-liquid": {
                "asset_id": "token-liquid",
                "asks": [{"price": "0.50", "size": "10"}],
                "bids": [{"price": "0.49", "size": "100"}],
            },
        }
    )

    enriched, liquidity_summary = apply_liquidity_depth_filter(
        profiles,
        clob=clob,
        min_ask_depth_usd=1.0,
        max_assets=10,
    )
    state = build_state(
        leaderboard_state=leaderboard_state,
        profit_state={},
        live_fill_report={},
        measurement_state={},
        recent_activity_counts=enriched,
        min_source_buy_usd=20.0,
        min_ask_depth_usd=1.0,
        liquidity_summary=liquidity_summary,
        limit=2,
    )

    assert state["selection"]["liquid_copy_sized_active_candidate_wallets"] == 1
    assert state["selection"]["min_top_of_book_ask_depth_usd"] == 1.0
    assert state["selection"]["liquidity_filter"]["classification_counts"] == {
        "ask_depth_below_threshold": 12,
        "eligible": 1,
    }
    assert state["ranked_wallets"][0]["wallet"] == liquid_wallet
    assert state["ranked_wallets"][0]["recent_liquid_copy_sized_buy_events"] == 1
    assert state["ranked_wallets"][0]["max_recent_ask_depth_usd"] == 5.0
    assert state["ranked_wallets"][1]["wallet"] == empty_book_wallet
    assert "top10_liquid_copy_sized_buy_missing" in state["ranked_wallets"][1]["blockers"]


def test_top10_activity_counts_and_tail_reader_skip_raw_rows(tmp_path: Path) -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    other = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    capture = tmp_path / "polygon.jsonl"
    capture.write_text(
        "\n".join(
            [
                '{"event":"polygon_ws_raw_frame","raw":"ack"}',
                (
                    '{"event":"polygon_orderfilled_log","source":"polygon_ws","maker":"%s",'
                    '"timestamp":"2026-07-05T10:00:00Z","transaction_hash":"0x1",'
                    '"decoded":{"maker_side":"BUY","price":0.5,"size":50}}'
                )
                % wallet,
                (
                    '{"event":"polygon_orderfilled_log","source":"polygon_ws","maker":"%s",'
                    '"timestamp":"2026-07-05T11:00:00Z","transaction_hash":"0x3",'
                    '"decoded":{"maker_side":"BUY","price":0.5,"size":50}}'
                )
                % wallet,
                (
                    '{"event":"polygon_orderfilled_log","source":"polygon_ws","maker":"%s",'
                    '"transaction_hash":"0x2","decoded":{"maker_side":"SELL","price":0.5,"size":2}}'
                )
                % other,
                '{"event":"polygon_ws_subscription_ack","message":{}}',
                '{"event":"polygon_ws_raw_frame","raw":"tail-noise"}',
            ]
        )
        + "\n"
    )

    counts = load_recent_activity_counts(str(capture), limit=10)
    profiles = load_recent_activity_profiles(str(capture), limit=10, min_source_buy_usd=20.0)
    rows = _iter_recent_jsonl(str(capture), limit=10)

    assert counts == {wallet: 2, other: 1}
    assert profiles[wallet]["copy_sized_buy_events"] == 2
    assert profiles[wallet]["active_hour_of_week_count"] == 2
    assert profiles[wallet]["buy_hour_of_week_count"] == 2
    assert profiles[wallet]["copy_sized_buy_hour_of_week_count"] == 2
    assert profiles[wallet]["active_hour_of_week_coverage_pct"] > 0.0
    assert profiles[wallet]["max_source_buy_usd"] == 25.0
    assert profiles[other]["sell_events"] == 1
    assert len(rows) == 3
    assert rows[0]["event"] == "polygon_orderfilled_log"
    assert rows[0]["maker"] == wallet


def test_top10_accepts_isolated_dataapi_wallet_events(tmp_path: Path) -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    capture = tmp_path / "dataapi.jsonl"
    capture.write_text(
        '{"event":"wallet_copy_wallet_event","source_wallet":"%s","action":"BUY",'
        '"token_id":"123","price":0.42,"size":8,"transaction_hash":"0xabc",'
        '"event_id":"we_1","event_ts":100.0,"observed_ts":101.0}\n' % wallet,
        encoding="utf-8",
    )

    rows = _iter_recent_jsonl(str(capture), limit=10)
    normalized = _normalized_realtime_event(rows[0])

    assert len(rows) == 1
    assert normalized is not None
    assert normalized["source"] == "dataapi_poll"
    assert normalized["asset"] == "123"
    assert normalized["received_at_s"] == 101.0


def test_top10_activity_profiles_keep_unknown_side_activity(tmp_path: Path) -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    capture = tmp_path / "polygon.jsonl"
    capture.write_text(
        '{"event":"polygon_orderfilled_log","source":"polygon_ws","maker":"%s","transaction_hash":"0x1"}\n'
        % wallet
    )

    counts = load_recent_activity_counts(str(capture), limit=10)
    profiles = load_recent_activity_profiles(str(capture), limit=10, min_source_buy_usd=20.0)

    assert counts == {wallet: 1}
    assert profiles[wallet]["events"] == 1
    assert profiles[wallet]["buy_events"] == 0
    assert profiles[wallet]["copy_sized_buy_events"] == 0


def test_top10_measurement_caches_clob_books_per_asset() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "200"}],
                "bids": [{"price": "0.49", "size": "200"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(wallet=wallet, tx="0xaaa", asset="token-a"),
            _polygon_row(wallet=wallet, tx="0xbbb", asset="token-a"),
        ],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
    )

    assert len(events) == 2
    assert clob.calls == ["token-a"]
    assert state["source"]["book_fetches"] == 1


def test_top10_measurement_can_floor_buys_to_min_order_and_skip_sells() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.50", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(wallet=wallet, tx="0xaaa", asset="token-a", price=0.5),
            _polygon_row(wallet=wallet, tx="0xbbb", asset="token-a", price=0.5, side="SELL"),
        ],
        clob=clob,
        wallet_fraction=0.01,
        max_order_usd=2.0,
        min_order_usd=1.0,
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
    )

    metrics = state["wallets"][wallet]
    assert len(events) == 1
    assert events[0]["copy_size_usd"] == 1.0
    assert events[0]["copy_size_floored_to_min_order"] is True
    assert metrics["below_min_order_events"] == 0
    assert metrics["copyable_buy_events"] == 1
    assert state["summary"]["diagnostics"]["sell_event_skipped_buy_only"] == 1
    assert clob.calls == ["token-a"]


def test_top10_measurement_can_filter_to_market_category() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.50", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
            "token-b": {
                "asset_id": "token-b",
                "timestamp": "1783093545000",
                "hash": "book-b",
                "asks": [{"price": "0.50", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[
            _polygon_row(wallet=wallet, tx="0xaaa", asset="token-a", market_slug="btc-updown-5m-1783093500"),
            _polygon_row(wallet=wallet, tx="0xbbb", asset="token-b"),
        ],
        clob=clob,
        wallet_fraction=0.01,
        max_order_usd=2.0,
        min_order_usd=1.0,
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
    )

    assert len(events) == 1
    assert events[0]["primary_market_category"] == "btc_5m"
    assert state["summary"]["buy_events"] == 1
    assert state["summary"]["diagnostics"]["market_category_skipped:sports"] == 1
    assert clob.calls == ["token-a"]


def test_top10_measurement_fresh_filter_records_receipt_to_fetch_latency() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    fresh = _rtds_row(wallet=wallet, tx="0xaaa", asset="token-fresh")
    fresh["received_at_s"] = 200.0
    stale = _rtds_row(wallet=wallet, tx="0xbbb", asset="token-stale")
    stale["received_at_s"] = 100.0
    clob = FakeCLOB(
        {
            "token-fresh": {
                "asset_id": "token-fresh",
                "timestamp": "200000",
                "hash": "book-fresh",
                "asks": [{"price": "0.50", "size": "20"}],
                "bids": [{"price": "0.49", "size": "20"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[stale, fresh],
        clob=clob,
        wallet_fraction=0.01,
        max_order_usd=2.0,
        min_order_usd=1.0,
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
        max_receipt_to_fetch_age_s=60.0,
        now_s=230.0,
    )

    assert len(events) == 1
    assert events[0]["asset"] == "token-fresh"
    assert events[0]["receipt_to_fetch_latency_ms"] == 30000.0
    assert state["summary"]["diagnostics"]["receipt_to_fetch_age_gt_cap"] == 1
    assert state["summary"]["receipt_to_fetch_latency"] == {
        "enabled": True,
        "events_with_latency": 1,
        "max_ms": 30000.0,
        "max_receipt_to_fetch_age_s": 60.0,
        "min_ms": 30000.0,
    }
    assert clob.calls == ["token-fresh"]


def test_top10_measurement_rerun_does_not_count_duplicate_events() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    row = _polygon_row(wallet=wallet, tx="0xaaa", asset="token-a")
    clob = FakeCLOB(
        {
            "token-a": {
                "asset_id": "token-a",
                "timestamp": "1783093545000",
                "hash": "book-a",
                "asks": [{"price": "0.51", "size": "200"}],
                "bids": [{"price": "0.49", "size": "200"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[row],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
        policy_id="idempotent",
    )
    state_again, events_again = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[row],
        clob=clob,
        prior_state=state,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
        policy_id="idempotent",
    )

    assert len(events) == 1
    assert events_again == []
    assert state["wallets"][wallet]["events_seen"] == 1
    assert state_again["wallets"][wallet]["events_seen"] == 1
    assert state_again["summary"]["diagnostics"] == {"duplicate_event": 1}


def test_top10_measurement_diagnoses_empty_book_liquidity_truth() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-empty": {
                "asset_id": "token-empty",
                "timestamp": "1783093545000",
                "hash": "book-empty",
                "asks": [],
                "bids": [{"price": "0.49", "size": "200"}],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[_polygon_row(wallet=wallet, tx="0xaaa", asset="token-empty")],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
        diagnose_liquidity_enabled=True,
    )

    assert len(events) == 1
    assert state["wallets"][wallet]["reject_reasons"] == {"no_ask_liquidity": 1}
    diagnosis = state["liquidity_diagnosis"]
    assert diagnosis["status"] == "ANALYZE"
    assert diagnosis["classification_counts"] == {"empty_book_truth": 1}
    assert diagnosis["records"][0]["top_of_book"]["best_ask_depth_usd"] == 0.0


def test_top10_measurement_classifies_null_empty_book_as_unavailable() -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    clob = FakeCLOB(
        {
            "token-closed": {
                "asset_id": "token-closed",
                "timestamp": None,
                "hash": None,
                "asks": [],
                "bids": [],
            },
        }
    )

    state, events = build_measurement_state(
        lane_state=_lane_state([wallet]),
        polygon_rows=[_polygon_row(wallet=wallet, tx="0xaaa", asset="token-closed")],
        clob=clob,
        wallet_fraction=0.05,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
        diagnose_liquidity_enabled=True,
    )

    assert len(events) == 1
    assert events[0]["result"]["reason"] == "book_unavailable_or_market_closed"
    assert state["wallets"][wallet]["reject_reasons"] == {"book_unavailable_or_market_closed": 1}
    diagnosis = state["liquidity_diagnosis"]
    assert diagnosis["classification_counts"] == {"book_unavailable_or_market_closed": 1}
    assert diagnosis["records"][0]["top_of_book"]["book_timestamp"] is None


def test_clob_market_client_falls_back_from_busy_relay_to_direct_empty_book(monkeypatch) -> None:
    calls: list[str] = []

    def fake_get_json_with_route_report(url, *, params, timeout_s, headers, retries=3, request_role):
        calls.append(url)
        if url.startswith("http://127.0.0.1:8787"):
            raise PolymarketRouteError(
                "relay busy",
                route_report={"status": "TRANSPORT_ERROR", "route_class": "RELAY_BUSY"},
            )
        response = requests.Response()
        response.status_code = 404
        response.url = url
        response._content = b'{"error":"No orderbook exists for the requested token id"}'
        response.wallet_copy_route_report = {"status": "HTTP_NON_2XX", "route_class": "DIRECT_404"}
        raise requests.HTTPError("404 Client Error", response=response)

    monkeypatch.setattr(
        "src.wallet_copy.live_tracker._get_json_with_route_report",
        fake_get_json_with_route_report,
    )

    client = CLOBMarketClient(host="http://127.0.0.1:8787/clob", timeout_s=0.75)
    book = client.get_book("token-empty")

    assert calls == ["http://127.0.0.1:8787/clob/book", "https://clob.polymarket.com/book"]
    assert book["empty_book_truth"] is True
    assert book["asks"] == []
    assert book["__walletCopyClobRouteReport"]["status"] == "HTTP_404_EMPTY_BOOK"
    assert book["__walletCopyClobRouteReport"]["fallback_attempts"][0]["route_class"] == "RELAY_BUSY"


def test_fill_quality_report_verifies_post_fix_precision_rejects() -> None:
    ledger = {
        "orders": [
            {
                "submitted_at": "2026-07-03T20:20:00+00:00",
                "final_status": "REJECTED",
                "limit_price": 0.50,
                "requested_size_usd": 1.0,
                "trade_result": {"error_class": "market_buy_precision_below_min_order"},
            },
            {
                "submitted_at": "2026-07-03T21:00:00+00:00",
                "final_status": "REJECTED",
                "limit_price": 0.50,
                "requested_size_usd": 1.0,
                "trade_result": {"error_class": "fak_no_match"},
            },
        ]
    }

    report = build_fill_quality_report(
        ledger,
        {},
        post_fix_since="2026-07-03T20:30:00+00:00",
    )

    verification = report["post_fix_precision_verification"]
    assert verification["enabled"] is True
    assert verification["status"] == "PASS"
    assert verification["orders"] == 1
    assert verification["precision_rejects"] == 0
