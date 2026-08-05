import argparse
import json
import sys
from types import SimpleNamespace

import pytest

from scripts import probe_queue_remote_dataapi_fresh_flow as probe_mod
from scripts import run_wallet_copy_live_guard as guard_mod
from scripts import update_state_digest as digest_mod
from scripts.build_full_pool_member_queue import _fresh_flow_by_wallet, build_queue
from scripts.probe_queue_remote_dataapi_fresh_flow import (
    _fetch_wallet,
    _merge_probe,
    _paper_shadow_enrollments,
    _wallets_from_active_set,
    _wallets_from_active_set_overlay,
    _wallets_from_cohort,
    _wallets_from_queue,
)
from src.wallet_copy.store import atomic_write_json as real_atomic_write_json


def test_remote_probe_fetches_via_user_param_not_proxy_wallet(monkeypatch) -> None:
    # Data API /trades ignores `proxyWallet=` and returns global rows, so the
    # probe must use the user-param fetch (fetch_trades) or every wallet
    # measures zero after identity filtering.
    calls: list[str] = []

    from src.wallet_copy import ingest as ingest_mod

    def fake_fetch_trades(self, *, limit=500, offset=0):
        calls.append("user")
        return []

    def fake_fetch_proxy(self, *, limit=500, offset=0):
        calls.append("proxyWallet")
        return []

    monkeypatch.setattr(ingest_mod.WalletHistoryClient, "fetch_trades", fake_fetch_trades)
    monkeypatch.setattr(ingest_mod.WalletHistoryClient, "fetch_proxy_wallet_trades", fake_fetch_proxy)
    args = argparse.Namespace(
        timeout_s=1.0, retries=0, lookback_hours=24.0, pages=1, limit=10,
        min_price=0.05, max_price=0.95,
    )
    result = _fetch_wallet("0x1111111111111111111111111111111111111111", args, now_s=1_783_600_000.0)

    assert calls == ["user"]
    assert result["status"] == "PASS"


def test_cohort_probe_prioritizes_unseen_live_ready_wallets() -> None:
    wallets = ["0x" + char * 40 for char in "abc"]
    cohort = {"live_ready_picks": [{"wallet": wallet} for wallet in wallets]}

    selected = _wallets_from_cohort(
        cohort,
        offset=0,
        limit=2,
        seen_wallets={wallets[0]},
        unseen_first=True,
    )

    assert selected == wallets[1:]


def test_active_direct_user_trade_wallet_is_durably_enrolled_paper_only() -> None:
    wallet = "0x" + "a" * 40
    rows = {
        wallet: {
            "wallet": wallet,
            "pass_admission_threshold": True,
            "fetched_at": "2026-07-24T12:00:00Z",
            "latest_btc5m_trade_ts": 123.0,
            "btc5m_buys_24h": 5,
        }
    }

    first = _paper_shadow_enrollments({}, rows, generated_at="2026-07-24T12:00:00Z")
    second = _paper_shadow_enrollments(
        {"paper_shadow_enrollments": first},
        rows,
        generated_at="2026-07-24T12:10:00Z",
    )

    assert second[0]["status"] == "DIRECT_USER_TRADES_ACTIVE_PAPER_SHADOW"
    assert second[0]["paper_only"] is True
    assert second[0]["live_orders_allowed"] is False
    assert second[0]["paper_canary_enrolled_at"] == "2026-07-24T12:00:00Z"


def test_remote_probe_flags_saturated_zero_rows_as_censored(monkeypatch) -> None:
    from src.wallet_copy import ingest as ingest_mod

    def fake_fetch_trades(self, *, limit=500, offset=0):
        return [{"id": offset + index} for index in range(limit)]

    def fake_normalize(raw, *, spec, row_type, observed_ts):
        return SimpleNamespace(
            event_ts=1_783_600_000.0 - 60.0,
            asset="ETH",
            duration="5m",
            action="BUY",
            price=0.4,
        )

    monkeypatch.setattr(ingest_mod.WalletHistoryClient, "fetch_trades", fake_fetch_trades)
    monkeypatch.setattr(probe_mod, "normalize_polymarket_wallet_row", fake_normalize)
    args = argparse.Namespace(
        timeout_s=1.0,
        retries=0,
        lookback_hours=24.0,
        pages=1,
        limit=10,
        min_price=0.25,
        max_price=0.50,
    )

    result = _fetch_wallet("0x1212121212121212121212121212121212121212", args, now_s=1_783_600_000.0)

    assert result["status"] == "CENSORED_PAGINATION_CAP"
    assert result["censored"] == "PAGINATION_CAP"
    assert result["remote_rows_saturated"] is True
    assert result["coverage_complete_24h"] is False
    assert result["btc5m_trades_24h"] == 0


def test_remote_probe_counts_btc5m_buys_in_last_30m(monkeypatch) -> None:
    from src.wallet_copy import ingest as ingest_mod

    now_s = 1_783_600_000.0

    def fake_fetch_trades(self, *, limit=500, offset=0):
        return [{"id": 1}, {"id": 2}]

    def fake_normalize(raw, *, spec, row_type, observed_ts):
        return SimpleNamespace(
            event_ts=now_s - (60.0 if raw["id"] == 1 else 3600.0),
            asset="BTC",
            duration="5m",
            action="BUY",
            price=0.4,
        )

    monkeypatch.setattr(ingest_mod.WalletHistoryClient, "fetch_trades", fake_fetch_trades)
    monkeypatch.setattr(probe_mod, "normalize_polymarket_wallet_row", fake_normalize)
    args = argparse.Namespace(timeout_s=1.0, retries=0, lookback_hours=24.0, pages=1, limit=10, min_price=0.25, max_price=0.50)

    result = _fetch_wallet("0x1414141414141414141414141414141414141414", args, now_s=now_s)

    assert result["btc5m_buys_24h"] == 2
    assert result["btc5m_buys_30m"] == 1
    assert result["btc5m_buy_rows_24h_by_price_subband"] == {
        "00_below_25": 0,
        "01a_25_32": 0,
        "01b_32_40": 0,
        "01c_40_50": 2,
    }


def test_remote_probe_flags_high_offset_cap_error_as_censored(monkeypatch) -> None:
    from src.wallet_copy import ingest as ingest_mod

    def fake_fetch_trades(self, *, limit=500, offset=0):
        if offset:
            raise RuntimeError("offset cap")
        return [{"id": index} for index in range(limit)]

    def fake_normalize(raw, *, spec, row_type, observed_ts):
        return SimpleNamespace(
            event_ts=1_783_600_000.0 - 120.0,
            asset="ETH",
            duration="5m",
            action="BUY",
            price=0.4,
        )

    monkeypatch.setattr(ingest_mod.WalletHistoryClient, "fetch_trades", fake_fetch_trades)
    monkeypatch.setattr(probe_mod, "normalize_polymarket_wallet_row", fake_normalize)
    args = argparse.Namespace(
        timeout_s=1.0,
        retries=0,
        lookback_hours=24.0,
        pages=2,
        limit=10,
        min_price=0.25,
        max_price=0.50,
    )

    result = _fetch_wallet("0x1313131313131313131313131313131313131313", args, now_s=1_783_600_000.0)

    assert result["status"] == "CENSORED_PAGINATION_CAP"
    assert result["censored"] == "PAGINATION_CAP"
    assert result["errors"] == ["RuntimeError: offset cap"]
    assert result["raw_rows"] == 10
    assert result["coverage_complete_24h"] is False


def test_remote_probe_selects_clearance_wallets_plus_include() -> None:
    wallets = _wallets_from_queue(
        {
            "ranked_members": [
                {"wallet": "0x1111111111111111111111111111111111111111", "clearance_ready": True},
                {"wallet": "0x2222222222222222222222222222222222222222", "clearance_ready": False},
                {"wallet": "0x3333333333333333333333333333333333333333", "clearance_ready": True},
            ]
        },
        clearance_limit=1,
        ranked_offset=0,
        ranked_limit=0,
        include_wallets=["0x4444444444444444444444444444444444444444"],
    )

    assert wallets == [
        "0x1111111111111111111111111111111111111111",
        "0x4444444444444444444444444444444444444444",
    ]


def test_remote_probe_extracts_live_active_set_wallets() -> None:
    wallets = _wallets_from_active_set(
        {
            "active_set": {
                "members": [
                    {"source_wallet": "0x1111111111111111111111111111111111111111"},
                    {"wallet": "0x2222222222222222222222222222222222222222"},
                    {"source_wallet": "0x1111111111111111111111111111111111111111"},
                    {"source_wallet": "not-a-wallet"},
                ]
            }
        }
    )

    assert wallets == [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]


def test_remote_probe_extracts_enabled_overlay_and_active_pin_wallets() -> None:
    enabled = "0x" + "1" * 40
    disabled = "0x" + "2" * 40
    pin = "0x" + "3" * 40
    wallets = _wallets_from_active_set_overlay(
        {
            "members": [
                {"source_wallet": enabled, "enabled": True},
                {"source_wallet": disabled, "enabled": False},
            ],
            "selection_pin": {
                "source_wallet": pin,
                "enabled": True,
                "expires_at": "2026-08-02T02:00:00Z",
            },
        },
        now_s=1_785_632_400.0,
    )

    assert wallets == [enabled, pin]


def test_remote_probe_excludes_expired_overlay_pin() -> None:
    pin = "0x" + "3" * 40
    assert _wallets_from_active_set_overlay(
        {
            "members": [],
            "selection_pin": {
                "source_wallet": pin,
                "enabled": True,
                "expires_at": "2026-08-02T00:00:00Z",
            },
        },
        now_s=1_785_632_400.0,
    ) == []


def test_remote_probe_clearance_limit_zero_adds_no_clearance_wallets() -> None:
    wallets = _wallets_from_queue(
        {
            "ranked_members": [
                {"wallet": "0x1111111111111111111111111111111111111111", "clearance_ready": True},
                {"wallet": "0x2222222222222222222222222222222222222222", "clearance_ready": False},
                {"wallet": "0x3333333333333333333333333333333333333333", "clearance_ready": True},
            ]
        },
        clearance_limit=0,
        ranked_offset=1,
        ranked_limit=1,
        include_wallets=[],
    )

    assert wallets == ["0x2222222222222222222222222222222222222222"]


def test_remote_probe_merge_preserves_top_level_rows() -> None:
    wallet = "0x5555555555555555555555555555555555555555"
    merged = _merge_probe(
        {"kind": "wallet_copy_corrected_copyability_probe", "ranked_candidates": []},
        {
            wallet: {
                "wallet": wallet,
                "status": "PASS",
                "btc5m_trades_24h": 4,
                "btc5m_buys_24h": 3,
                "policy_compatible_inband_buy_rows_24h": 3,
                "btc5m_buy_rows_24h_by_price_subband": {
                    "00_below_25": 1,
                    "01a_25_32": 2,
                    "01b_32_40": 0,
                    "01c_40_50": 1,
                },
                "latest_trade_age_h": 0.25,
                "pass_admission_threshold": True,
                "coverage_complete_24h": True,
            }
        },
    )

    remote = merged["remote_dataapi_24h"]
    assert remote["wallets"] == [wallet]
    assert remote["pass_admission_threshold_wallets"] == [wallet]
    assert remote["rows"][0]["btc5m_buys_24h"] == 3
    assert merged["summary"]["remote_dataapi_24h_wallets"] == 1
    assert merged["summary"]["remote_dataapi_24h_p1_eligible"] == 1
    assert merged["summary"]["remote_dataapi_24h_btc5m_buys"] == 3
    assert merged["summary"]["btc5m_buy_rows_24h_by_price_subband"]["01a_25_32"] == 2


def test_remote_probe_merge_counts_censored_separately_from_errors() -> None:
    wallet = "0x5656565656565656565656565656565656565656"
    merged = _merge_probe(
        {"kind": "wallet_copy_corrected_copyability_probe", "ranked_candidates": [{"wallet": wallet}]},
        {
            wallet: {
                "wallet": wallet,
                "status": "CENSORED_PAGINATION_CAP",
                "censored": "PAGINATION_CAP",
                "btc5m_trades_24h": 0,
                "btc5m_buys_24h": 0,
                "policy_compatible_inband_buy_rows_24h": 0,
                "latest_trade_age_h": None,
                "pass_admission_threshold": False,
                "remote_rows_saturated": True,
                "coverage_complete_24h": False,
            }
        },
    )

    remote = merged["remote_dataapi_24h"]
    assert remote["error_wallets"] == []
    assert remote["censored_wallets"] == [wallet]
    assert merged["summary"]["remote_dataapi_24h_errors"] == 0
    assert merged["summary"]["remote_dataapi_24h_censored"] == 1
    assert merged["ranked_candidates"][0]["evidence"]["remote_dataapi_24h"]["censored"] == "PAGINATION_CAP"


def test_remote_liveness_consumers_prefer_current_selected_row() -> None:
    wallet = "0x5757575757575757575757575757575757575757"
    state = {
        "rows": [
            {
                "wallet": wallet,
                "latest_btc5m_trade_ts": 100.0,
                "checkpoint_carryover": True,
                "fetched_at_s": 1.0,
            }
        ],
        "selected_rows": [
            {
                "wallet": wallet,
                "latest_btc5m_trade_ts": 200.0,
                "selected_this_run": True,
                "fetched_at_s": 2.0,
            }
        ],
    }

    guard_rows = guard_mod._external_liveness_rows_by_wallet(state)
    digest_rows = digest_mod._external_liveness_rows_by_wallet_from_probe(state)

    assert guard_rows[wallet]["latest_btc5m_trade_ts"] == 200.0
    assert digest_rows[wallet]["latest_btc5m_trade_ts"] == 200.0


def test_remote_liveness_consumers_recover_bf33_timestamp_from_paper_shadow() -> None:
    wallet = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
    state = {
        "rows": [{
            "wallet": wallet,
            "status": "CENSORED_PAGINATION_CAP",
            "censored": "PAGINATION_CAP",
            "latest_btc5m_trade_ts": None,
            "btc5m_trades_24h": 0,
        }],
        "paper_shadow_enrollments": [{
            "wallet": wallet,
            "status": "DIRECT_USER_TRADES_ACTIVE_PAPER_SHADOW",
            "latest_btc5m_trade_ts": 1_785_628_083.0,
            "latest_trade_age_h": 0.01,
            "btc5m_trades_24h": 50,
            "btc5m_buys_24h": 50,
        }],
    }
    now = guard_mod.dt.datetime.fromtimestamp(1_785_632_400.0, guard_mod.dt.timezone.utc)

    guard_rows = guard_mod._external_liveness_rows_by_wallet(state)
    digest_rows = digest_mod._external_liveness_rows_by_wallet_from_probe(state)
    decision = guard_mod._external_liveness_gate_for_wallet(
        wallet, rows_by_wallet=guard_rows, state=state, now=now
    )
    queue_row = _fresh_flow_by_wallet(state, now_ts=now.timestamp())[wallet]

    assert guard_rows[wallet]["latest_btc5m_trade_ts"] == 1_785_628_083.0
    assert digest_rows[wallet]["latest_btc5m_trade_ts"] == 1_785_628_083.0
    assert decision["passed"] is True
    assert decision["latest_trade_age_h"] == pytest.approx(1.199167, abs=1e-6)
    assert queue_row["latest_trade_age_source"] == "computed_from_latest_btc5m_trade_ts"
    assert queue_row.get("source_reported_latest_trade_age_h") is None


def test_remote_liveness_timestamp_precedence_refuses_true_109h_wallet() -> None:
    wallet = "0x00033f1089ff061813850e5135483bed39ce3b49"
    now = guard_mod.dt.datetime(2026, 8, 2, 1, 0, tzinfo=guard_mod.dt.timezone.utc)
    trade_ts = now.timestamp() - 109.26 * 3600
    state = {"rows": [{
        "wallet": wallet,
        "status": "PASS",
        "latest_btc5m_trade_ts": trade_ts,
        "latest_trade_age_h": 0.017549,
        "btc5m_trades_24h": 5,
    }]}
    decision = guard_mod._external_liveness_gate_for_wallet(wallet, state=state, now=now)

    assert decision["passed"] is False
    assert decision["reason"] == "external_liveness_age_gte_24h"
    assert decision["latest_trade_age_h"] == pytest.approx(109.26)


def test_queue_builder_ranks_remote_dataapi_top_level_rows() -> None:
    fresh_wallet = "0x6666666666666666666666666666666666666666"
    stale_wallet = "0x7777777777777777777777777777777777777777"
    replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 2.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": stale_wallet, "resolved_pnl": 10.0, "complementary_hours_utc": [1, 2, 3]},
                {"wallet": fresh_wallet, "resolved_pnl": 2.0, "complementary_hours_utc": [4]},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {"wallet": stale_wallet, "candidate_id": "stale", "paper_replay": replay},
                {"wallet": fresh_wallet, "candidate_id": "fresh", "paper_replay": replay},
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "generated_at": "2099-01-01T00:00:00Z",
            "remote_dataapi_24h": {
                "rows": [
                    {
                        "wallet": fresh_wallet,
                        "btc5m_buys_24h": 3,
                        "btc5m_trades_24h": 4,
                        "policy_compatible_inband_buy_rows_24h": 3,
                        "latest_trade_age_h": 0.5,
                        "pass_admission_threshold": True,
                    }
                ]
            },
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == fresh_wallet
    assert row["fresh_flow_rank"]["rank_source"] == "remote_dataapi_24h"
    assert row["fresh_flow_rank"]["remote_dataapi_btc5m_buys_24h"] == 3


def test_queue_builder_reads_queue_remote_probe_latest_top_level_rows() -> None:
    fresh_wallet = "0x8888888888888888888888888888888888888888"
    stale_wallet = "0x9999999999999999999999999999999999999999"
    replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 2.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": stale_wallet, "resolved_pnl": 10.0, "complementary_hours_utc": [1, 2, 3]},
                {"wallet": fresh_wallet, "resolved_pnl": 2.0, "complementary_hours_utc": [4]},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {"wallet": stale_wallet, "candidate_id": "stale", "paper_replay": replay},
                {"wallet": fresh_wallet, "candidate_id": "fresh", "paper_replay": replay},
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "kind": "queue_remote_dataapi_fresh_flow_probe",
            "generated_at": "2099-01-01T00:00:00Z",
            "rows": [
                {
                    "wallet": fresh_wallet,
                    "btc5m_buys_24h": 3,
                    "btc5m_trades_24h": 4,
                    "policy_compatible_inband_buy_rows_24h": 3,
                    "latest_trade_age_h": 0.5,
                    "pass_admission_threshold": True,
                    "remote_rows_saturated": True,
                }
            ],
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == fresh_wallet
    assert row["fresh_flow_rank"]["source"] == "remote_dataapi_24h"
    assert row["fresh_flow_rank"]["remote_dataapi_policy_compatible_inband_buy_rows_24h"] == 3
    assert row["fresh_flow_rank"]["remote_rows_saturated"] is True


def test_remote_probe_cli_uses_atomic_writes_for_checkpoint_output_and_probe(tmp_path, monkeypatch) -> None:
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "output.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    queue_path.write_text(
        json.dumps({"ranked_members": [{"wallet": wallet, "clearance_ready": True}]}),
        encoding="utf-8",
    )
    probe_path.write_text(json.dumps({"ranked_candidates": []}), encoding="utf-8")
    calls: list[str] = []

    def fake_fetch_wallet(fetch_wallet, args, *, now_s):
        return {
            "wallet": fetch_wallet,
            "status": "PASS",
            "btc5m_trades_24h": 4,
            "btc5m_buys_24h": 3,
            "policy_compatible_inband_buy_rows_24h": 3,
            "btc5m_buy_rows_24h_by_price_subband": {
                "00_below_25": 0,
                "01a_25_32": 2,
                "01b_32_40": 1,
                "01c_40_50": 0,
            },
            "latest_trade_age_h": 0.25,
            "pass_admission_threshold": True,
            "remote_rows_saturated": True,
        }

    def recording_atomic(path, payload, *, compact=False):
        calls.append(str(path))
        real_atomic_write_json(path, payload, compact=compact)

    monkeypatch.setattr(probe_mod, "_fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(probe_mod, "atomic_write_json", recording_atomic)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_queue_remote_dataapi_fresh_flow.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
            "--clearance-limit",
            "1",
            "--include-wallet",
            wallet,
        ],
    )

    assert probe_mod.main() == 0
    assert str(checkpoint_path) in calls
    assert str(output_path) in calls
    assert str(probe_path) in calls
    assert json.loads(output_path.read_text(encoding="utf-8"))["rows"][0]["remote_rows_saturated"] is True
    assert json.loads(output_path.read_text(encoding="utf-8"))["summary"][
        "btc5m_buy_rows_24h_by_price_subband"
    ]["01a_25_32"] >= 2


def test_remote_probe_cli_can_omit_default_include_wallet(tmp_path, monkeypatch) -> None:
    ranked_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "output.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    queue_path.write_text(
        json.dumps(
            {
                "ranked_members": [
                    {"wallet": ranked_wallet, "clearance_ready": False},
                ]
            }
        ),
        encoding="utf-8",
    )
    probe_path.write_text(json.dumps({"ranked_candidates": []}), encoding="utf-8")
    fetched: list[str] = []

    def fake_fetch_wallet(fetch_wallet, args, *, now_s):
        fetched.append(fetch_wallet)
        return {
            "wallet": fetch_wallet,
            "status": "PASS",
            "btc5m_trades_24h": 0,
            "btc5m_buys_24h": 0,
            "policy_compatible_inband_buy_rows_24h": 0,
            "latest_trade_age_h": None,
            "pass_admission_threshold": False,
            "remote_rows_saturated": False,
        }

    monkeypatch.setattr(probe_mod, "_fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_queue_remote_dataapi_fresh_flow.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
            "--clearance-limit",
            "0",
            "--ranked-limit",
            "1",
            "--no-default-include-wallet",
            "--no-include-active-set-wallets",
        ],
    )

    assert probe_mod.main() == 0
    assert fetched == [ranked_wallet]
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["summary"]["wallets"] == 1
    assert output["rows"][0]["wallet"] == ranked_wallet


def test_remote_probe_cli_refreshes_active_set_wallets_by_default(tmp_path, monkeypatch) -> None:
    active_wallet = "0xcccccccccccccccccccccccccccccccccccccccc"
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "output.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    live_guard_state_path = tmp_path / "live_guard_state.json"
    active_set_overlay_path = tmp_path / "active_set_overlay.json"
    queue_path.write_text(json.dumps({"ranked_members": []}), encoding="utf-8")
    probe_path.write_text(json.dumps({"ranked_candidates": []}), encoding="utf-8")
    live_guard_state_path.write_text(
        json.dumps({"active_set": {"members": [{"source_wallet": active_wallet}]}}),
        encoding="utf-8",
    )
    active_set_overlay_path.write_text(json.dumps({"members": []}), encoding="utf-8")
    fetched: list[str] = []

    def fake_fetch_wallet(fetch_wallet, args, *, now_s):
        fetched.append(fetch_wallet)
        return {
            "wallet": fetch_wallet,
            "status": "PASS",
            "btc5m_trades_24h": 4,
            "btc5m_buys_24h": 3,
            "policy_compatible_inband_buy_rows_24h": 3,
            "latest_btc5m_trade_ts": 1_783_600_000.0,
            "latest_trade_age_h": 0.25,
            "pass_admission_threshold": True,
            "remote_rows_saturated": False,
        }

    monkeypatch.setattr(probe_mod, "_fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_queue_remote_dataapi_fresh_flow.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
            "--live-guard-state",
            str(live_guard_state_path),
            "--active-set-overlay",
            str(active_set_overlay_path),
            "--clearance-limit",
            "0",
            "--no-default-include-wallet",
        ],
    )

    assert probe_mod.main() == 0
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert fetched == [active_wallet]
    assert output["criteria"]["active_set_wallets"] == [active_wallet]
    assert output["summary"]["active_set_wallets"] == 1
    assert output["selected_rows"][0]["selected_this_run"] is True
    assert output["selected_rows"][0]["checkpoint_carryover"] is False


def test_remote_probe_cli_can_omit_active_set_wallets(tmp_path, monkeypatch) -> None:
    active_wallet = "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "output.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    live_guard_state_path = tmp_path / "live_guard_state.json"
    queue_path.write_text(json.dumps({"ranked_members": []}), encoding="utf-8")
    probe_path.write_text(json.dumps({"ranked_candidates": []}), encoding="utf-8")
    live_guard_state_path.write_text(
        json.dumps({"active_set": {"members": [{"source_wallet": active_wallet}]}}),
        encoding="utf-8",
    )
    fetched: list[str] = []

    def fake_fetch_wallet(fetch_wallet, args, *, now_s):
        fetched.append(fetch_wallet)
        return {"wallet": fetch_wallet, "status": "PASS"}

    monkeypatch.setattr(probe_mod, "_fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_queue_remote_dataapi_fresh_flow.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
            "--live-guard-state",
            str(live_guard_state_path),
            "--clearance-limit",
            "0",
            "--no-default-include-wallet",
            "--no-include-active-set-wallets",
        ],
    )

    assert probe_mod.main() == 0
    assert fetched == []
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["summary"]["wallets"] == 0
    assert output["criteria"]["active_set_wallets"] == []


def test_remote_probe_cli_outputs_cumulative_checkpoint_rows(tmp_path, monkeypatch) -> None:
    prior_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    ranked_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    queue_path = tmp_path / "queue.json"
    probe_path = tmp_path / "probe.json"
    output_path = tmp_path / "output.json"
    checkpoint_path = tmp_path / "checkpoint.json"
    queue_path.write_text(
        json.dumps({"ranked_members": [{"wallet": ranked_wallet, "clearance_ready": False}]}),
        encoding="utf-8",
    )
    probe_path.write_text(json.dumps({"ranked_candidates": []}), encoding="utf-8")
    checkpoint_path.write_text(
        json.dumps(
            {
                "rows_by_wallet": {
                    prior_wallet: {
                        "wallet": prior_wallet,
                        "status": "PASS",
                        "btc5m_trades_24h": 4,
                        "btc5m_buys_24h": 3,
                        "policy_compatible_inband_buy_rows_24h": 3,
                        "latest_trade_age_h": 0.25,
                        "pass_admission_threshold": True,
                        "remote_rows_saturated": True,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def fake_fetch_wallet(fetch_wallet, args, *, now_s):
        return {
            "wallet": fetch_wallet,
            "status": "PASS",
            "btc5m_trades_24h": 5,
            "btc5m_buys_24h": 4,
            "policy_compatible_inband_buy_rows_24h": 4,
            "latest_trade_age_h": 0.5,
            "pass_admission_threshold": True,
            "remote_rows_saturated": True,
        }

    monkeypatch.setattr(probe_mod, "_fetch_wallet", fake_fetch_wallet)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "probe_queue_remote_dataapi_fresh_flow.py",
            "--queue",
            str(queue_path),
            "--probe",
            str(probe_path),
            "--output",
            str(output_path),
            "--checkpoint",
            str(checkpoint_path),
            "--clearance-limit",
            "0",
            "--ranked-limit",
            "1",
            "--no-default-include-wallet",
            "--no-include-active-set-wallets",
        ],
    )

    assert probe_mod.main() == 0
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["summary"]["wallets"] == 1
    assert output["summary"]["cumulative_wallets"] == 2
    assert output["selected_wallets"] == [ranked_wallet]
    assert {row["wallet"] for row in output["rows"]} == {prior_wallet, ranked_wallet}
    prior = next(row for row in output["rows"] if row["wallet"] == prior_wallet)
    current = next(row for row in output["rows"] if row["wallet"] == ranked_wallet)
    assert prior["checkpoint_carryover"] is True
    assert current["selected_this_run"] is True
