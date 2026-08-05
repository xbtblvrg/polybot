import argparse
import json
import urllib.error
from pathlib import Path

from scripts import wallet_outflow_deadman as outflow_deadman


WALLET = "0xee888fa7b96007f7fa270988e92bddb0ae19ed10"


def _args(tmp_path: Path, *, end_iso: str = "1970-01-01T00:33:20Z", write_handoff: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        ledger=str(tmp_path / "ledger.json"),
        scorecard=str(tmp_path / "scorecard.json"),
        h2_external=str(tmp_path / "h2_external.json"),
        own_redeem_events=str(tmp_path / "own_redeem_events.jsonl"),
        state=str(tmp_path / "wallet_outflow_deadman_state.json"),
        handoff=str(tmp_path / "docs/agents/HANDOFF.md"),
        user=WALLET,
        start_iso="1970-01-01T00:00:00Z",
        end_iso=end_iso,
        first_run_lookback_s=3600.0,
        overlap_s=300.0,
        max_fetch_gap_s=24 * 60 * 60.0,
        max_unmatched_age_s=600.0,
        degraded_notify_threshold=3,
        min_outflow_usd=0.000001,
        polygon_rpc_url="https://polygon.invalid",
        polygon_rpc_fallback_url="https://polygon-fallback.invalid",
        data_api_base_url="https://data-api.polymarket.com",
        blockscout_base_url="https://blockscout.invalid/api",
        transfer_source="blockscout",
        timeout_s=1.0,
        transfer_403_cooldown_s=600.0,
        transfer_5xx_retry_backoff_s=1.0,
        rpc_secondary_chunk_blocks=50,
        rpc_secondary_from_block_safety_blocks=0,
        rpc_secondary_to_block_safety_blocks=0,
        live_guard_state=str(tmp_path / "wallet_copy_live_guard_state.json"),
        live_armed_after_iso="2026-07-13T00:00:00Z",
        live_armed_degraded_incident_threshold=12,
        chunk_blocks=100,
        block_mode="estimate",
        seconds_per_block=2.1,
        data_api_limit=100,
        data_api_max_pages=1,
        enable_data_api_activity=False,
        write_handoff_on_incident=write_handoff,
    )


def _filled_order(tx: str) -> dict:
    return {
        "order_id": f"order-{tx}",
        "final_status": "FILLED",
        "submitted_at": "1970-01-01T00:10:00Z",
        "source_wallet": "0x1111111111111111111111111111111111111111",
        "trade_result": {
            "tx_hashes": [tx],
            "actual_trade_cost_usd": 1.25,
        },
    }


def _filled_order_without_tx(
    order_id: str = "order-maker",
    *,
    amount_usd: float = 2.5,
    updated_at: str = "1970-01-01T00:30:50Z",
) -> dict:
    return {
        "order_id": order_id,
        "intent_id": "ci-maker",
        "final_status": "FILLED",
        "submitted_at": "1970-01-01T00:25:00Z",
        "updated_at": updated_at,
        "source_wallet": "0xd97ae021645712fe5cf73139049383a100cac068",
        "market_slug": "btc-updown-5m-test",
        "condition_id": "0xcondition",
        "outcome": "Up",
        "trade_result": {
            "execution_role": "maker",
            "filled_size_usd": amount_usd,
            "final_status": "filled",
        },
    }


def _backfilled_maker_order_with_tx(tx: str = "0xsettlement") -> dict:
    return {
        "order_id": "order-backfilled-maker",
        "intent_id": "ci-maker-backfill",
        "final_status": "FILLED",
        "submitted_at": "1970-01-01T00:25:00Z",
        "updated_at": "1970-01-01T00:30:50Z",
        "source_wallet": "0xd97ae021645712fe5cf73139049383a100cac068",
        "market_slug": "btc-updown-5m-test",
        "condition_id": "0xcondition",
        "outcome": "Up",
        "actual_trade_cost_usd": 2.5,
        "trade_result": {
            "execution_role": "maker",
            "filled_at": "1970-01-01T00:30:50Z",
            "filled_size_usd": 2.5,
            "fill_backfill": {
                "cost_usd": 2.5,
                "filled_at": "1970-01-01T00:30:50Z",
                "tx_hash": tx,
            },
            "final_status": "filled",
        },
        "lifecycle": [
            {
                "status": "FILL_BACKFILLED",
                "ts": "1970-01-01T00:31:00Z",
                "payload": {
                    "cost_usd": 2.5,
                    "filled_at": "1970-01-01T00:30:50Z",
                    "tx_hash": tx,
                },
            }
        ],
    }


def _transfer(
    tx: str,
    *,
    block_ts: float,
    amount_usd: float,
    direction: str = "OUT",
    log_index: int = 1,
    counterparty: str = "0x2222222222222222222222222222222222222222",
) -> dict:
    if direction == "OUT":
        from_addr = WALLET
        to_addr = counterparty
    else:
        from_addr = counterparty
        to_addr = WALLET
    return {
        "tx": tx,
        "block_number": int(block_ts),
        "block_ts": block_ts,
        "block_iso": f"1970-01-01T00:{int(block_ts // 60):02d}:00Z",
        "log_index": log_index,
        "direction": direction,
        "from": from_addr,
        "to": to_addr,
        "counterparty": to_addr if direction == "OUT" else from_addr,
        "amount_usd": amount_usd,
    }


def test_wallet_outflow_deadman_escalates_old_unmatched_outflow(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, write_handoff=True)
    Path(args.ledger).write_text(json.dumps({"orders": [_filled_order("0xmatched")]}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    transfers = [
        _transfer("0xmatched", block_ts=1000.0, amount_usd=1.25, log_index=1),
        _transfer("0xsteal", block_ts=1000.0, amount_usd=12.5, log_index=2),
        _transfer("0xinbound", block_ts=1000.0, amount_usd=2.0, direction="IN", log_index=3),
    ]
    monkeypatch.setattr(
        outflow_deadman,
        "_fetch_transfers",
        lambda *_args, **_kwargs: (transfers, {"status": "OK", "source": "unit"}, []),
    )

    report = outflow_deadman.build_report(args)

    assert report["status"] == "INCIDENT_UNEXPLAINED_OUTFLOW"
    assert report["incident"] is True
    assert report["summary"]["outflow_rows"] == 2
    assert report["summary"]["matched_order_outflows"] == 1
    assert report["summary"]["unmatched_outflows"] == 1
    assert report["summary"]["incident_outflow_usd"] == 12.5
    assert report["unmatched_outflows"][0]["tx"] == "0xsteal"
    phases = {row["name"]: row for row in report["timing"]["phases"]}
    assert phases["load_previous_window_wallet"]["wallet_present"] is True
    assert phases["load_ledger_and_match_sets"]["ledger_fill_txs"] == 1
    assert phases["fetch_transfers"]["status"] == "OK"
    assert phases["classify_and_match_transfers"]["unmatched"] == 1
    assert report["timing"]["total_s_before_write"] >= 0
    assert "WALLET_OUTFLOW_DEADMAN INCIDENT" in Path(args.handoff).read_text(encoding="utf-8")


def test_wallet_outflow_deadman_gives_fresh_unmatched_outflow_one_cycle(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:33:20Z")
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    monkeypatch.setattr(
        outflow_deadman,
        "_fetch_transfers",
        lambda *_args, **_kwargs: (
            [_transfer("0xfresh", block_ts=1900.0, amount_usd=3.0, log_index=1)],
            {"status": "OK", "source": "unit"},
            [],
        ),
    )

    report = outflow_deadman.build_report(args)

    assert report["status"] == "WATCH_UNMATCHED_OUTFLOW"
    assert report["incident"] is False
    assert report["summary"]["unmatched_outflows"] == 1
    assert report["summary"]["incident_outflows"] == 0
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    assert state["pending_unmatched"]["0xfresh:1"]["amount_usd"] == 3.0


def test_wallet_outflow_deadman_matches_known_settlement_outflow_by_amount_time(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    settlement_counterparty = outflow_deadman.cash_audit.POLYMARKET_CTF.lower()
    Path(args.ledger).write_text(json.dumps({"orders": [_filled_order_without_tx()]}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    monkeypatch.setattr(
        outflow_deadman,
        "_fetch_transfers",
        lambda *_args, **_kwargs: (
            [
                _transfer(
                    "0xsettlement",
                    block_ts=1852.0,
                    amount_usd=2.5,
                    log_index=1,
                    counterparty=settlement_counterparty,
                )
            ],
            {"status": "OK", "source": "unit"},
            [],
        ),
    )

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    assert report["incident"] is False
    assert report["summary"]["matched_order_outflows"] == 1
    assert report["summary"]["unmatched_outflows"] == 0
    evidence = report["recent_outflows"][0]["matched_evidence"]
    assert evidence["match_method"] == "known_settlement_counterparty_amount_time"
    assert evidence["order_id"] == "order-maker"


def test_wallet_outflow_deadman_matches_backfilled_maker_fill_tx_after_submission_window(
    monkeypatch, tmp_path: Path
) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:33:20Z")
    args.start_iso = "1970-01-01T00:30:00Z"
    settlement_counterparty = outflow_deadman.cash_audit.POLYMARKET_CTF.lower()
    Path(args.ledger).write_text(json.dumps({"orders": [_backfilled_maker_order_with_tx()]}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    monkeypatch.setattr(
        outflow_deadman,
        "_fetch_transfers",
        lambda *_args, **_kwargs: (
            [
                _transfer(
                    "0xsettlement",
                    block_ts=1852.0,
                    amount_usd=2.5,
                    log_index=1,
                    counterparty=settlement_counterparty,
                )
            ],
            {"status": "OK", "source": "unit"},
            [],
        ),
    )

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    assert report["summary"]["matched_order_outflows"] == 1
    evidence = report["recent_outflows"][0]["matched_evidence"]
    assert evidence["orders"] == 1
    assert evidence["cost_usd"] == 2.5
    assert evidence["source_wallets"] == ["0xd97ae021645712fe5cf73139049383a100cac068"]


def test_wallet_outflow_deadman_matches_data_api_activity_trade_tx(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.enable_data_api_activity = True
    settlement_counterparty = outflow_deadman.cash_audit.POLYMARKET_CTF.lower()
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    monkeypatch.setattr(
        outflow_deadman,
        "_fetch_transfers",
        lambda *_args, **_kwargs: (
            [
                _transfer(
                    "0xactivitytrade",
                    block_ts=1852.0,
                    amount_usd=2.5,
                    log_index=1,
                    counterparty=settlement_counterparty,
                )
            ],
            {"status": "OK", "source": "unit"},
            [],
        ),
    )
    monkeypatch.setattr(
        outflow_deadman.cash_audit,
        "_fetch_data_api_activity",
        lambda **_kwargs: (
            [
                {
                    "type": "TRADE",
                    "transactionHash": "0xactivitytrade",
                    "timestamp": 1852,
                    "conditionId": "0xcondition",
                    "asset": "123",
                    "usdcSize": 2.5,
                    "size": 5,
                    "price": 0.5,
                    "side": "BUY",
                    "slug": "btc-updown-5m-test",
                    "outcome": "Up",
                }
            ],
            {"status": "OK", "pages": 1, "truncated": False, "urls": ["hidden"]},
        ),
    )

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    assert report["incident"] is False
    assert report["summary"]["matched_order_outflows"] == 1
    assert report["summary"]["unmatched_outflows"] == 0
    evidence = report["recent_outflows"][0]["matched_evidence"]
    assert evidence["source"] == "data_api_activity_trade"
    assert evidence["match_method"] == "data_api_activity_trade_tx"
    assert evidence["market_slug"] == "btc-updown-5m-test"


def test_transfer_fetch_logs_blockscout_403_and_falls_back_to_rpc(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "blockscout"
    args.timeout_s = 1.0
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    sleeps: list[float] = []

    def fail_blockscout(**_kwargs):
        raise urllib.error.HTTPError(
            "https://blockscout.invalid/api",
            403,
            "Forbidden",
            {"Retry-After": "0.01", "X-RateLimit-Remaining": "0"},
            None,
        )

    def ok_rpc(**_kwargs):
        return [], {"status": "OK", "from_block": 1, "to_block": 2, "rows_in_window": 0}

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_blockscout_transfer_rows", fail_blockscout)
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", ok_rpc)
    monkeypatch.setattr(outflow_deadman.time, "sleep", lambda seconds: sleeps.append(seconds))

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    transfers_fetch = report["fetch"]["transfers"]
    attempts = report["fetch"]["transfer_attempts"]
    assert transfers_fetch["source"] == "polygon_rpc_eth_getLogs"
    assert transfers_fetch["fallback_after_blockscout_error"] is True
    assert [row["status"] for row in attempts] == ["ERROR", "OK"]
    assert attempts[0]["source"] == "blockscout_account_tokentx"
    assert attempts[0]["http_status"] == 403
    assert attempts[0]["response_headers"]["retry-after"] == "0.01"
    assert attempts[0]["response_headers"]["x-ratelimit-remaining"] == "0"
    assert attempts[0]["backoff_applied"] is True
    assert attempts[0]["fallback_source"] == "polygon_rpc_eth_getLogs"
    assert sleeps == [0.01]
    assert attempts[1]["source"] == "polygon_rpc_eth_getLogs"
    assert attempts[1]["rows"] == 0


def test_transfer_fetch_preserves_attempts_when_all_sources_fail(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "auto"
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")

    def fail_rpc(**_kwargs):
        raise TimeoutError("rpc unavailable")

    def fail_blockscout(**_kwargs):
        raise urllib.error.HTTPError(
            "https://blockscout.invalid/api",
            403,
            "Forbidden",
            {"Retry-After": "1", "X-RateLimit-Remaining": "0"},
            None,
        )

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", fail_rpc)
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_blockscout_transfer_rows", fail_blockscout)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "DEGRADED_FETCH"
    attempts = report["fetch"]["transfer_attempts"]
    assert [row["source"] for row in attempts] == [
        "blockscout_account_tokentx",
        "polygon_rpc_eth_getLogs",
        "polygon_rpc_secondary_eth_getLogs",
    ]
    assert [row["status"] for row in attempts] == ["ERROR", "ERROR", "ERROR"]
    assert attempts[0]["http_status"] == 403
    assert attempts[0]["response_headers"]["retry-after"] == "1"
    assert attempts[0]["fallback_source"] == "polygon_rpc_eth_getLogs"
    assert attempts[1]["error_type"] == "TimeoutError"
    assert attempts[1]["fallback_source"] == "polygon_rpc_secondary_eth_getLogs"
    assert attempts[2]["error_type"] == "TimeoutError"
    assert report["fetch"]["transfers"]["error_type"] == "TransferFetchError"
    assert report["fetch"]["transfers"]["attempt_count"] == 3
    assert report["transfer_source_cooldowns"]["blockscout"]["http_status"] == 403


def test_fetch_transfers_timing_surfaces_failed_attempts_before_blockscout_success(
    monkeypatch, tmp_path: Path
) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "auto"
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    clock = iter([100.0, 100.8, 101.0, 101.9, 102.0, 102.2])

    def fail_rpc(**_kwargs):
        raise TimeoutError("rpc unavailable")

    def ok_blockscout(**_kwargs):
        return [], {"status": "OK", "source": "blockscout_account_tokentx", "rows_in_window": 0}

    monkeypatch.setattr(outflow_deadman.time, "time", lambda: next(clock))
    monkeypatch.setattr(
        outflow_deadman,
        "_transfer_source_order",
        lambda _source: ["rpc", "rpc_secondary", "blockscout"],
    )
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", fail_rpc)
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_blockscout_transfer_rows", ok_blockscout)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    phase = next(row for row in report["timing"]["phases"] if row["name"] == "fetch_transfers")
    attempts = phase["transfer_attempts"]
    assert [row["source"] for row in attempts] == [
        "polygon_rpc_eth_getLogs",
        "polygon_rpc_secondary_eth_getLogs",
        "blockscout_account_tokentx",
    ]
    assert [row["status"] for row in attempts] == ["ERROR", "ERROR", "OK"]
    assert attempts[0]["error_type"] == "TimeoutError"
    assert attempts[0]["error"] == "rpc unavailable"
    assert attempts[0]["duration_s"] == 0.8
    assert attempts[1]["duration_s"] == 0.9
    assert phase["pre_blockscout_attempt_duration_s"] == 1.7


def test_transfer_fetch_retries_blockscout_5xx_once_before_rotating(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "blockscout"
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    sleeps: list[float] = []
    calls = {"blockscout": 0, "rpc": 0}

    def flaky_blockscout(**_kwargs):
        calls["blockscout"] += 1
        if calls["blockscout"] == 1:
            raise urllib.error.HTTPError(
                "https://blockscout.invalid/api",
                500,
                "Server Error",
                {"X-RateLimit-Remaining": "179"},
                None,
            )
        return [], {"status": "OK", "source": "blockscout_account_tokentx", "rows_in_window": 0}

    def fail_rpc(**_kwargs):
        calls["rpc"] += 1
        raise AssertionError("rpc fallback should not be used after same-source retry succeeds")

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_blockscout_transfer_rows", flaky_blockscout)
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", fail_rpc)
    monkeypatch.setattr(outflow_deadman.time, "sleep", lambda seconds: sleeps.append(seconds))

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    assert calls == {"blockscout": 2, "rpc": 0}
    attempts = report["fetch"]["transfer_attempts"]
    assert [row["source"] for row in attempts] == ["blockscout_account_tokentx", "blockscout_account_tokentx"]
    assert [row["status"] for row in attempts] == ["ERROR", "OK"]
    assert attempts[0]["retry_source"] == "blockscout_account_tokentx"
    assert attempts[0]["http_status"] == 500
    assert attempts[0]["response_headers"]["x-ratelimit-remaining"] == "179"
    assert sleeps == [1.0]


def test_transfer_fetch_skips_403_source_during_cooldown(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "rpc"
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")

    def primary_rpc_403_secondary_ok(**kwargs):
        if kwargs.get("rpc_url") == "https://polygon-fallback.invalid":
            return [], {"status": "OK", "source": "polygon_rpc_secondary_eth_getLogs", "rows_in_window": 0}
        raise urllib.error.HTTPError(
            "https://polygon.invalid",
            403,
            "Forbidden",
            {"Retry-After": "0.01"},
            None,
        )

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", primary_rpc_403_secondary_ok)
    monkeypatch.setattr(
        outflow_deadman.cash_audit,
        "_fetch_blockscout_transfer_rows",
        lambda **_kwargs: ([], {"status": "OK", "source": "blockscout_account_tokentx"}),
    )
    monkeypatch.setattr(outflow_deadman.time, "sleep", lambda _seconds: None)

    first = outflow_deadman.build_report(args)

    assert first["status"] == "OK"
    assert first["transfer_source_cooldowns"]["rpc"]["http_status"] == 403

    args2 = _args(tmp_path, end_iso="1970-01-01T00:25:00Z")
    args2.transfer_source = "rpc"
    calls = {"rpc": 0, "blockscout": 0}

    def rpc_should_skip_primary_then_use_secondary(**kwargs):
        calls["rpc"] += 1
        if kwargs.get("rpc_url") == "https://polygon-fallback.invalid":
            return [], {"status": "OK", "source": "polygon_rpc_secondary_eth_getLogs", "rows_in_window": 0}
        raise AssertionError("primary rpc should be skipped while cooldown is active")

    def ok_blockscout(**_kwargs):
        calls["blockscout"] += 1
        return [], {"status": "OK", "source": "blockscout_account_tokentx"}

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", rpc_should_skip_primary_then_use_secondary)
    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_blockscout_transfer_rows", ok_blockscout)

    second = outflow_deadman.build_report(args2)

    assert second["status"] == "OK"
    assert calls == {"rpc": 1, "blockscout": 0}
    attempts = second["fetch"]["transfer_attempts"]
    assert [row["status"] for row in attempts] == ["SKIPPED_COOLDOWN", "OK"]
    assert attempts[0]["source"] == "polygon_rpc_eth_getLogs"
    assert attempts[0]["fallback_source"] == "polygon_rpc_secondary_eth_getLogs"
    assert attempts[1]["source"] == "polygon_rpc_secondary_eth_getLogs"


def test_transfer_fetch_can_force_rpc_secondary_with_50_block_cap(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="1970-01-01T00:20:00Z")
    args.transfer_source = "rpc_secondary"
    args.chunk_blocks = 7500
    args.rpc_secondary_chunk_blocks = 50
    Path(args.ledger).write_text(json.dumps({"orders": []}), encoding="utf-8")
    Path(args.scorecard).write_text(json.dumps({}), encoding="utf-8")
    Path(args.h2_external).write_text(json.dumps({"rows": []}), encoding="utf-8")
    observed = {}

    def ok_rpc(**kwargs):
        observed.update(kwargs)
        return [], {"status": "OK", "from_block": 1, "to_block": 2, "rows_in_window": 0}

    monkeypatch.setattr(outflow_deadman.cash_audit, "_fetch_transfer_logs", ok_rpc)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "OK"
    assert observed["rpc_url"] == "https://polygon-fallback.invalid"
    assert observed["chunk_blocks"] == 50
    assert observed["from_block_safety_blocks"] == 0
    assert observed["to_block_safety_blocks"] == 0
    assert observed["directions"] == ("out",)
    transfers = report["fetch"]["transfers"]
    assert transfers["source"] == "polygon_rpc_secondary_eth_getLogs"
    assert transfers["source_order"] == ["polygon_rpc_secondary_eth_getLogs"]
    assert transfers["chunk_blocks"] == 50


def test_degraded_fetch_reuses_last_successful_window_anchor(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="2026-07-11T01:00:00Z")
    args.start_iso = ""
    Path(args.state).write_text(
        json.dumps(
            {
                "status": "DEGRADED_FETCH",
                "checked_at": "2026-07-11T00:30:00Z",
                "last_ok_checked_at": "2026-07-11T00:00:00Z",
                "consecutive_degraded_fetches": 1,
            }
        ),
        encoding="utf-8",
    )
    recorded = {}

    def fail_fetch(*_args, **kwargs):
        recorded["start_ts"] = kwargs["start_ts"]
        recorded["end_ts"] = kwargs["end_ts"]
        raise RuntimeError("both transfer sources down")

    monkeypatch.setattr(outflow_deadman, "_fetch_transfers", fail_fetch)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "DEGRADED_FETCH"
    assert report["window"]["source"] == "last_ok_checked_overlap_after_degraded_fetch"
    assert report["window"]["start_iso"] == "2026-07-10T23:55:00Z"
    assert outflow_deadman._iso(recorded["start_ts"]) == "2026-07-10T23:55:00Z"
    assert report["last_ok_checked_at"] == "2026-07-11T00:00:00Z"
    assert report["consecutive_degraded_fetches"] == 2


def test_degraded_fetch_gap_exceeded_caps_window_and_surfaces_status(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="2026-07-11T01:00:00Z")
    args.start_iso = ""
    args.max_fetch_gap_s = 24 * 60 * 60.0
    Path(args.state).write_text(
        json.dumps(
            {
                "status": "DEGRADED_FETCH",
                "checked_at": "2026-07-10T00:30:00Z",
                "last_ok_checked_at": "2026-07-09T00:00:00Z",
                "consecutive_degraded_fetches": 11,
            }
        ),
        encoding="utf-8",
    )
    recorded = {}

    def ok_fetch(*_args, **kwargs):
        recorded["start_ts"] = kwargs["start_ts"]
        recorded["end_ts"] = kwargs["end_ts"]
        return [], {"status": "OK", "source": "unit"}, []

    monkeypatch.setattr(outflow_deadman, "_fetch_transfers", ok_fetch)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "DEGRADED_FETCH_GAP_EXCEEDED"
    assert report["window"]["source"] == "last_ok_gap_exceeded_capped"
    assert report["window"]["fetch_gap_exceeded"] is True
    assert report["window"]["start_iso"] == "2026-07-10T01:00:00Z"
    assert outflow_deadman._iso(recorded["start_ts"]) == "2026-07-10T01:00:00Z"
    assert report["last_ok_checked_at"] == "2026-07-11T01:00:00Z"
    assert report["consecutive_degraded_fetches"] == 0


def test_third_consecutive_degraded_fetch_writes_handoff_notify(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="2026-07-11T01:00:00Z", write_handoff=True)
    args.start_iso = ""
    Path(args.state).write_text(
        json.dumps(
            {
                "status": "DEGRADED_FETCH",
                "checked_at": "2026-07-11T00:30:00Z",
                "last_ok_checked_at": "2026-07-11T00:00:00Z",
                "consecutive_degraded_fetches": 2,
            }
        ),
        encoding="utf-8",
    )

    def fail_fetch(*_args, **_kwargs):
        raise RuntimeError("both transfer sources down")

    monkeypatch.setattr(outflow_deadman, "_fetch_transfers", fail_fetch)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "DEGRADED_FETCH"
    assert report["consecutive_degraded_fetches"] == 3
    handoff = Path(args.handoff).read_text(encoding="utf-8")
    assert "WALLET_OUTFLOW_DEADMAN DEGRADED" in handoff
    assert "consecutive_degraded_fetches=3/3" in handoff
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    assert state["degraded_notify_written_at"] == report["checked_at"]


def test_degraded_fetch_notify_edge_not_consumed_without_written_marker(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="2026-07-11T01:00:00Z", write_handoff=True)
    args.start_iso = ""
    Path(args.state).write_text(
        json.dumps(
            {
                "status": "DEGRADED_FETCH",
                "checked_at": "2026-07-11T00:40:00Z",
                "last_ok_checked_at": "2026-07-11T00:00:00Z",
                "consecutive_degraded_fetches": 3,
            }
        ),
        encoding="utf-8",
    )

    def fail_fetch(*_args, **_kwargs):
        raise RuntimeError("manual run consumed prior threshold edge")

    monkeypatch.setattr(outflow_deadman, "_fetch_transfers", fail_fetch)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "DEGRADED_FETCH"
    assert report["consecutive_degraded_fetches"] == 4
    handoff = Path(args.handoff).read_text(encoding="utf-8")
    assert "WALLET_OUTFLOW_DEADMAN DEGRADED" in handoff
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    assert state["degraded_notify_written_at"] == report["checked_at"]


def test_live_armed_degraded_fetch_escalates_at_threshold(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, end_iso="2026-07-13T01:00:00Z", write_handoff=True)
    args.start_iso = ""
    Path(args.state).write_text(
        json.dumps(
            {
                "status": "DEGRADED_FETCH",
                "checked_at": "2026-07-13T00:50:00Z",
                "last_ok_checked_at": "2026-07-13T00:00:00Z",
                "consecutive_degraded_fetches": 11,
            }
        ),
        encoding="utf-8",
    )
    Path(args.live_guard_state).write_text(
        json.dumps(
            {
                "status": "LIVE_GUARD_BLOCKED",
                "generated_at": "2026-07-13T00:55:00Z",
                "execute_live": True,
                "live_orders_allowed": False,
            }
        ),
        encoding="utf-8",
    )

    def fail_fetch(*_args, **_kwargs):
        raise RuntimeError("all transfer sources blind")

    monkeypatch.setattr(outflow_deadman, "_fetch_transfers", fail_fetch)

    report = outflow_deadman.build_report(args)

    assert report["status"] == "INCIDENT_OUTFLOW_FETCH_BLIND_LIVE_ARMED"
    assert report["incident"] is True
    assert report["live_armed_degraded_fetch_incident"]["active"] is True
    assert report["consecutive_degraded_fetches"] == 12
    handoff = Path(args.handoff).read_text(encoding="utf-8")
    assert "WALLET_OUTFLOW_DEADMAN INCIDENT" in handoff
    assert "live-armed transfer fetch blind" in handoff
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    assert state["live_armed_degraded_fetch_notify_written_at"] == report["checked_at"]
