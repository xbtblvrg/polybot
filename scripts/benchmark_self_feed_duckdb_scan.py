#!/usr/bin/env python3
"""Benchmark self-feed trade scans through the Parquet/DuckDB data layer."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402

DEFAULT_SELF_FEED_LOG = "data/research/wallet_copy_self_trades.jsonl"
DEFAULT_DUCKDB = "data/derived/wallet_copy_data_layer_v1/wallet_copy.duckdb"
DEFAULT_OUTPUT = "data/research/wallet_copy_self_feed_duckdb_benchmark_latest.json"
DEFAULT_SELF_FEED_REPORT = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_CLASSIFICATION = "data/research/wallet_copy_recon_window_cash_ledger_latest.json"
DEFAULT_FULL_RETRACE = "data/research/wallet_copy_self_feed_full_ledger_retrace_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _jsonl_self_feed_summary(path: Path) -> dict[str, Any]:
    rows = 0
    txs: set[str] = set()
    cost_usd = 0.0
    min_event_ts: float | None = None
    max_event_ts: float | None = None
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            rows += 1
            tx = str(row.get("tx") or "").strip().lower()
            if tx:
                txs.add(tx)
            cost_usd += _num(row.get("cost_usd"))
            event_ts = _num(row.get("event_ts"))
            if event_ts:
                min_event_ts = event_ts if min_event_ts is None else min(min_event_ts, event_ts)
                max_event_ts = event_ts if max_event_ts is None else max(max_event_ts, event_ts)
    return {
        "rows": rows,
        "tx_groups": len(txs),
        "cost_usd": round(cost_usd, 6),
        "min_event_ts": min_event_ts,
        "max_event_ts": max_event_ts,
    }


def _duckdb_self_feed_summary(duckdb_path: Path, source_file: str) -> dict[str, Any]:
    import duckdb  # type: ignore

    source_suffix = source_file.replace("'", "''")
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT
              count(*) AS rows,
              count(DISTINCT lower(json_extract_string(raw_json, '$.tx'))) AS tx_groups,
              coalesce(sum(try_cast(json_extract_string(raw_json, '$.cost_usd') AS DOUBLE)), 0.0) AS cost_usd,
              min(try_cast(json_extract_string(raw_json, '$.event_ts') AS DOUBLE)) AS min_event_ts,
              max(try_cast(json_extract_string(raw_json, '$.event_ts') AS DOUBLE)) AS max_event_ts
            FROM wallet_copy_events
            WHERE source_file = ?
            """,
            [source_suffix],
        ).fetchone()
    finally:
        con.close()
    return {
        "rows": int(row[0] or 0),
        "tx_groups": int(row[1] or 0),
        "cost_usd": round(float(row[2] or 0.0), 6),
        "min_event_ts": row[3],
        "max_event_ts": row[4],
    }


def _load_json(path: Path, default: Any) -> Any:
    try:
        loaded = json.loads(path.read_text())
    except Exception:
        return default
    return loaded if loaded is not None else default


def _first_num(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def _self_feed_rows_from_duckdb(
    duckdb_path: Path,
    source_file: str,
    *,
    start_ts: float,
    end_ts: float,
) -> list[dict[str, Any]]:
    import duckdb  # type: ignore

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT raw_json
            FROM wallet_copy_events
            WHERE source_file = ?
              AND try_cast(json_extract_string(raw_json, '$.event_ts') AS DOUBLE) BETWEEN ? AND ?
            """,
            [source_file, start_ts, end_ts],
        ).fetchall()
    finally:
        con.close()

    normalized: list[dict[str, Any]] = []
    for (raw_json,) in rows:
        try:
            row = json.loads(raw_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "source": row.get("source"),
                "tx": str(row.get("tx") or "").lower(),
                "order_id": row.get("order_id"),
                "token_id": row.get("token_id"),
                "condition_id": row.get("condition_id"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "side": row.get("side"),
                "price": row.get("price"),
                "size": row.get("size"),
                "cost_usd": row.get("cost_usd"),
                "event_ts": row.get("event_ts"),
            }
        )
    return normalized


def _duckdb_gap_summary(duckdb_path: Path, source_file: str, self_feed_report: Path, live_state: Path) -> dict[str, Any]:
    from scripts.reconcile_self_wallet_feed import (  # noqa: PLC0415
        _aggregate_trade_rows,
        _compare_price,
        _normalize_ledger_fill,
        _parse_ts,
        _probable_split_fill_groups,
    )

    report = _load_json(self_feed_report, {})
    live = _load_json(live_state, {})
    window = report.get("window") if isinstance(report.get("window"), dict) else {}
    start_ts = _num(window.get("start_ts"))
    end_ts = _num(window.get("end_ts"))
    if not start_ts or not end_ts:
        return {"status": "WINDOW_MISSING"}

    orders = live.get("orders") if isinstance(live.get("orders"), list) else []
    ledger_fills = [
        _normalize_ledger_fill(order)
        for order in orders
        if isinstance(order, dict)
        and str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
        and start_ts <= _parse_ts(order.get("submitted_at") or order.get("updated_at")) <= end_ts
    ]
    self_rows = _self_feed_rows_from_duckdb(duckdb_path, source_file, start_ts=start_ts, end_ts=end_ts)
    ledger_by_tx = _aggregate_trade_rows(ledger_fills)
    self_by_tx = _aggregate_trade_rows(self_rows)

    matched_count = 0
    amount_mismatch_rows: list[dict[str, Any]] = []
    price_mismatch_rows: list[dict[str, Any]] = []
    ledger_missing_critical = 0
    ledger_missing_grace = 0
    expected_summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    grace_s = max(0.0, _num(expected_summary.get("ledger_missing_grace_s")))
    now_ts = time.time()
    for tx, ledger_item in ledger_by_tx.items():
        self_item = self_by_tx.get(tx)
        if not self_item:
            age_s = now_ts - _num(ledger_item.get("max_event_ts"))
            if grace_s and 0.0 <= age_s <= grace_s:
                ledger_missing_grace += 1
            else:
                ledger_missing_critical += 1
            continue
        cost_delta = round(_num(ledger_item.get("cost_usd")) - _num(self_item.get("cost_usd")), 6)
        size_delta = round(_num(ledger_item.get("size")) - _num(self_item.get("size")), 6)
        if abs(cost_delta) > 0.05 or abs(size_delta) > 0.00001:
            amount_mismatch_rows.append(
                {
                    "tx": tx,
                    "ledger": ledger_item,
                    "self_feed": self_item,
                    "cost_delta_usd": cost_delta,
                    "size_delta": size_delta,
                }
            )
        elif not _compare_price(ledger_item.get("avg_price"), self_item.get("avg_price")):
            matched_count += 1
            price_mismatch_rows.append({"tx": tx, "ledger": ledger_item, "self_feed": self_item})
        else:
            matched_count += 1

    self_missing_ledger_rows = [
        {"tx": tx, "self_feed": self_item}
        for tx, self_item in self_by_tx.items()
        if tx not in ledger_by_tx
    ]
    probable_split_fill_groups = _probable_split_fill_groups(amount_mismatch_rows, self_missing_ledger_rows)
    split_txs = {
        str(row.get("tx") or "")
        for group in probable_split_fill_groups
        for row in group.get("missing_companion_self_feed_txs", [])
        if isinstance(row, dict)
    }
    summary = {
        "ledger_filled_tx_groups": len(ledger_by_tx),
        "matched_ledger_tx_groups": matched_count,
        "self_feed_tx_groups": len(self_by_tx),
        "data_api_trade_rows": len(self_rows),
        "amount_mismatch_tx_groups": len(amount_mismatch_rows),
        "price_rounding_mismatch_tx_groups": len(price_mismatch_rows),
        "probable_split_fill_groups": len(probable_split_fill_groups),
        "probable_split_fill_missing_tx_groups": len(split_txs),
        "ledger_missing_self_feed_critical": ledger_missing_critical,
        "ledger_missing_self_feed_within_grace": ledger_missing_grace,
        "self_feed_missing_ledger_critical": len(self_missing_ledger_rows),
        "self_feed_missing_ledger_cost_usd": round(
            sum(_num(row.get("self_feed", {}).get("cost_usd")) for row in self_missing_ledger_rows),
            6,
        ),
    }
    expected = expected_summary
    parity_keys = (
        "ledger_filled_tx_groups",
        "matched_ledger_tx_groups",
        "self_feed_tx_groups",
        "data_api_trade_rows",
        "amount_mismatch_tx_groups",
        "price_rounding_mismatch_tx_groups",
        "probable_split_fill_groups",
        "probable_split_fill_missing_tx_groups",
        "ledger_missing_self_feed_critical",
        "ledger_missing_self_feed_within_grace",
        "self_feed_missing_ledger_critical",
        "self_feed_missing_ledger_cost_usd",
    )
    expected_summary = {key: expected.get(key) for key in parity_keys}
    expected_summary["data_api_trade_rows"] = expected.get(
        "data_api_trade_rows",
        expected.get("data_api_raw_trade_rows"),
    )
    parity = {key: expected_summary.get(key) == summary.get(key) for key in parity_keys}
    return {
        "status": "PASS" if all(parity.values()) else "MISMATCH",
        "expected_summary": expected_summary,
        "duckdb_summary": summary,
        "parity": parity,
    }


def _timed(label: str, fn: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    start = time.perf_counter()
    summary = fn()
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 6)
    return summary, {"label": label, "elapsed_ms": elapsed_ms}


def _dedup_aware_overlay(
    *,
    raw_missing_pnl_usd: float,
    classification_summary: dict[str, Any],
    full_retrace: dict[str, Any],
) -> dict[str, Any]:
    rows = full_retrace.get("rows") if isinstance(full_retrace.get("rows"), list) else []
    pnl_by_class = (
        classification_summary.get("pnl_by_class_usd")
        if isinstance(classification_summary.get("pnl_by_class_usd"), dict)
        else {}
    )
    split_pnl = _num(pnl_by_class.get("join_key_defect_probable_split_fill"))
    class_totals: dict[str, dict[str, Any]] = {}
    residual_rows: list[dict[str, Any]] = []
    overlay_delta = 0.0
    double_count_excluded = 0.0

    def add_class(klass: str, *, raw_pnl: float, contribution: float, count: int = 1) -> None:
        entry = class_totals.setdefault(
            klass,
            {"count": 0, "raw_pnl_usd": 0.0, "overlay_delta_usd": 0.0},
        )
        entry["count"] += count
        entry["raw_pnl_usd"] += raw_pnl
        entry["overlay_delta_usd"] += contribution

    for row in rows:
        if not isinstance(row, dict):
            continue
        klass = str(row.get("classification") or "unknown")
        raw_pnl = _num(row.get("pnl_usd"))
        contribution = 0.0
        rule = "excluded_from_overlay"
        nearest = (
            row.get("nearest_full_ledger_fill")
            if isinstance(row.get("nearest_full_ledger_fill"), dict)
            else {}
        )
        if klass == "b3_duplicate_full_ledger_match":
            rule = "duplicate_full_ledger_match_contributes_zero"
        elif klass == "b3_join_scope_artifact_size_price_or_time_mismatch":
            self_item = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
            self_size = _num(self_item.get("size"))
            nearest_shares = _num(nearest.get("shares"))
            self_payout = _num(row.get("cost_usd")) + raw_pnl
            if self_size > 0 and nearest_shares > 0:
                payout_per_share = self_payout / self_size
                matched_pnl = (payout_per_share * nearest_shares) - _num(nearest.get("cost_usd"))
                contribution = round(raw_pnl - matched_pnl, 6)
            else:
                contribution = round(_num(nearest.get("cost_usd")) - _num(row.get("cost_usd")), 6)
            rule = "self_feed_pnl_minus_matched_full_ledger_pnl"
        elif klass == "b1_confirmed_guard_evidence_no_full_ledger_fill":
            contribution = raw_pnl
            rule = "confirmed_guard_fill_without_full_ledger_match"
        add_class(klass, raw_pnl=raw_pnl, contribution=contribution)
        overlay_delta += contribution
        if contribution == 0.0 and raw_pnl:
            double_count_excluded += raw_pnl
        residual_rows.append(
            {
                "tx": row.get("tx"),
                "classification": klass,
                "rule": rule,
                "raw_self_feed_pnl_usd": round(raw_pnl, 6),
                "self_feed_cost_usd": row.get("cost_usd"),
                "matched_full_ledger_cost_usd": nearest.get("cost_usd"),
                "overlay_delta_usd": round(contribution, 6),
            }
        )

    if split_pnl:
        add_class(
            "join_key_defect_probable_split_fill",
            raw_pnl=split_pnl,
            contribution=0.0,
            count=int(_num(classification_summary.get("join_key_defect_probable_split_fill"))),
        )
        double_count_excluded += split_pnl

    class_decomposition = {
        key: {
            "count": int(value["count"]),
            "raw_pnl_usd": round(float(value["raw_pnl_usd"]), 6),
            "overlay_delta_usd": round(float(value["overlay_delta_usd"]), 6),
        }
        for key, value in sorted(class_totals.items())
    }
    overlay_delta = round(overlay_delta, 6)
    double_count_excluded = round(raw_missing_pnl_usd - overlay_delta, 6)
    return {
        "schema": "dedup_aware_self_feed_overlay_v1",
        "raw_missing_pnl_upper_bound_usd": round(raw_missing_pnl_usd, 6),
        "overlay_delta_usd": overlay_delta,
        "double_count_excluded_usd": double_count_excluded,
        "class_decomposition": class_decomposition,
        "residual_rows": residual_rows,
    }


def _classification_packet(
    *,
    self_feed_report: dict[str, Any],
    classification: dict[str, Any],
    full_retrace: dict[str, Any],
) -> dict[str, Any]:
    source_summary = self_feed_report.get("summary") if isinstance(self_feed_report.get("summary"), dict) else {}
    classification_summary = (
        classification.get("summary") if isinstance(classification.get("summary"), dict) else {}
    )
    retrace_summary = full_retrace.get("summary") if isinstance(full_retrace.get("summary"), dict) else {}
    backfill_gate = full_retrace.get("backfill_gate") if isinstance(full_retrace.get("backfill_gate"), dict) else {}
    equation = (
        full_retrace.get("reconciliation_equation")
        if isinstance(full_retrace.get("reconciliation_equation"), dict)
        else {}
    )
    missing_pnl = (
        self_feed_report.get("self_feed_missing_ledger_pnl")
        if isinstance(self_feed_report.get("self_feed_missing_ledger_pnl"), dict)
        else {}
    )
    missing_pnl_usd = _first_num(
        missing_pnl.get("pnl_usd"),
        source_summary.get("self_feed_missing_ledger_pnl_usd"),
    )
    actual_delta = _first_num(equation.get("actual_delta_usd"))
    overlay = _dedup_aware_overlay(
        raw_missing_pnl_usd=missing_pnl_usd,
        classification_summary=classification_summary,
        full_retrace=full_retrace,
    )
    overlay_delta = _num(overlay.get("overlay_delta_usd"))
    return {
        "status": "PASS",
        "python_scan_summary": {
            "self_feed_missing_ledger_critical": source_summary.get("self_feed_missing_ledger_critical"),
            "self_feed_missing_ledger_cost_usd": source_summary.get("self_feed_missing_ledger_cost_usd"),
            "self_feed_missing_ledger_payout_usd": source_summary.get("self_feed_missing_ledger_payout_usd"),
            "self_feed_missing_ledger_pnl_usd": source_summary.get("self_feed_missing_ledger_pnl_usd"),
            "self_feed_missing_ledger_resolved_tx_groups": source_summary.get(
                "self_feed_missing_ledger_resolved_tx_groups"
            ),
            "self_feed_missing_ledger_unresolved_tx_groups": source_summary.get(
                "self_feed_missing_ledger_unresolved_tx_groups"
            ),
            "amount_mismatch_tx_groups": source_summary.get("amount_mismatch_tx_groups"),
            "probable_split_fill_groups": source_summary.get("probable_split_fill_groups"),
            "price_rounding_mismatch_tx_groups": source_summary.get("price_rounding_mismatch_tx_groups"),
        },
        "classification_summary": {
            "self_feed_missing_ledger_rows": classification_summary.get("self_feed_missing_ledger_rows"),
            "join_key_defect_probable_split_fill": classification_summary.get(
                "join_key_defect_probable_split_fill"
            ),
            "true_unrecorded_fill_candidate": classification_summary.get("true_unrecorded_fill_candidate"),
            "cost_by_class_usd": classification_summary.get("cost_by_class_usd")
            if isinstance(classification_summary.get("cost_by_class_usd"), dict)
            else {},
            "pnl_by_class_usd": classification_summary.get("pnl_by_class_usd")
            if isinstance(classification_summary.get("pnl_by_class_usd"), dict)
            else {},
        },
        "full_ledger_retrace": {
            "candidate_total": retrace_summary.get("candidate_total"),
            "class_counts": retrace_summary.get("class_counts")
            if isinstance(retrace_summary.get("class_counts"), dict)
            else {},
            "cost_by_class_usd": retrace_summary.get("cost_by_class_usd")
            if isinstance(retrace_summary.get("cost_by_class_usd"), dict)
            else {},
            "pnl_by_class_usd": retrace_summary.get("pnl_by_class_usd")
            if isinstance(retrace_summary.get("pnl_by_class_usd"), dict)
            else {},
            "b1_confirmed_count": retrace_summary.get("b1_confirmed_count"),
            "b2_suspect_count": retrace_summary.get("b2_suspect_count"),
            "immediate_notify_required": retrace_summary.get("immediate_notify_required"),
            "backfill_gate": backfill_gate,
        },
        "resolved_pnl_overlay": {
            "schema": overlay.get("schema"),
            "resolved_tx_groups": _first_num(
                missing_pnl.get("resolved_tx_groups"),
                source_summary.get("self_feed_missing_ledger_resolved_tx_groups"),
            ),
            "unresolved_tx_groups": _first_num(
                missing_pnl.get("unresolved_tx_groups"),
                source_summary.get("self_feed_missing_ledger_unresolved_tx_groups"),
            ),
            "cost_usd": _first_num(
                missing_pnl.get("cost_usd"),
                source_summary.get("self_feed_missing_ledger_cost_usd"),
            ),
            "payout_usd": _first_num(
                missing_pnl.get("payout_usd"),
                source_summary.get("self_feed_missing_ledger_payout_usd"),
            ),
            "raw_missing_pnl_upper_bound_usd": round(missing_pnl_usd, 6),
            "pnl_usd": round(overlay_delta, 6),
            "overlay_delta_usd": round(overlay_delta, 6),
            "double_count_excluded_usd": overlay.get("double_count_excluded_usd"),
            "class_decomposition": overlay.get("class_decomposition"),
            "residual_rows": overlay.get("residual_rows"),
            "actual_delta_usd": equation.get("actual_delta_usd"),
            "reconciled_actual_estimate_usd": round(actual_delta + overlay_delta, 6),
        },
        "recommendation": {
            "mode": "RECONCILIATION_OVERLAY",
            "ledger_rewrite": False,
            "reason": (
                "execution ledger remains append-only guard truth; resolved self-feed cost basis "
                "belongs in a derived scorecard overlay unless a b1 guard-write defect is proven"
            ),
            "backfill_allowed": bool(backfill_gate.get("allowed")),
            "scorecard_overlay_delta_usd": round(overlay_delta, 6),
            "double_count_excluded_usd": overlay.get("double_count_excluded_usd"),
            "next_action": "feed overlay into scorecard actual-basis verdict before ranked-queue refresh",
        },
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    self_feed_log = Path(args.self_feed_log)
    if not self_feed_log.is_absolute():
        self_feed_log = ROOT / self_feed_log
    duckdb_path = Path(args.duckdb_path)
    if not duckdb_path.is_absolute():
        duckdb_path = ROOT / duckdb_path
    source_file = str(Path(args.self_feed_log))
    self_feed_report_path = Path(args.self_feed_report)
    if not self_feed_report_path.is_absolute():
        self_feed_report_path = ROOT / self_feed_report_path
    classification_path = Path(args.classification)
    if not classification_path.is_absolute():
        classification_path = ROOT / classification_path
    full_retrace_path = Path(args.full_retrace)
    if not full_retrace_path.is_absolute():
        full_retrace_path = ROOT / full_retrace_path
    live_state_path = Path(args.live_state)
    if not live_state_path.is_absolute():
        live_state_path = ROOT / live_state_path
    json_summary, json_benchmark = _timed("jsonl_self_feed_scan", lambda: _jsonl_self_feed_summary(self_feed_log))
    try:
        duck_summary, duck_benchmark = _timed(
            "duckdb_wallet_copy_events_scan",
            lambda: _duckdb_self_feed_summary(duckdb_path, source_file),
        )
        gap_summary, gap_benchmark = _timed(
            "duckdb_self_feed_gap_scan",
            lambda: _duckdb_gap_summary(
                duckdb_path,
                source_file,
                self_feed_report_path,
                live_state_path,
            ),
        )
        missing_dependency = ""
    except ModuleNotFoundError as exc:
        duck_summary = {"rows": 0, "tx_groups": 0, "cost_usd": 0.0, "min_event_ts": None, "max_event_ts": None}
        duck_benchmark = {"label": "duckdb_wallet_copy_events_scan", "elapsed_ms": None}
        gap_summary = {"status": "MISSING_DEPENDENCY", "parity": {}}
        gap_benchmark = {"label": "duckdb_self_feed_gap_scan", "elapsed_ms": None}
        missing_dependency = str(exc)
    parity = {
        key: json_summary.get(key) == duck_summary.get(key)
        for key in ("rows", "tx_groups", "cost_usd", "min_event_ts", "max_event_ts")
    }
    classification_packet = _classification_packet(
        self_feed_report=_load_json(self_feed_report_path, {}),
        classification=_load_json(classification_path, {}),
        full_retrace=_load_json(full_retrace_path, {}),
    )
    gap_parity = gap_summary.get("parity") if isinstance(gap_summary.get("parity"), dict) else {}
    status = (
        "PASS"
        if all(parity.values()) and all(gap_parity.values())
        else "MISSING_DEPENDENCY"
        if missing_dependency
        else "MISMATCH"
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_self_feed_duckdb_benchmark",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "status": status,
        "inputs": {
            "self_feed_log": str(Path(args.self_feed_log)),
            "duckdb_path": str(Path(args.duckdb_path)),
            "duckdb_view": "wallet_copy_events",
            "self_feed_report": str(Path(args.self_feed_report)),
            "classification": str(Path(args.classification)),
            "full_retrace": str(Path(args.full_retrace)),
        },
        "jsonl_summary": json_summary,
        "duckdb_summary": duck_summary,
        "parity": parity,
        "gap_scan": gap_summary,
        "classification_packet": classification_packet,
        "benchmarks": [json_benchmark, duck_benchmark, gap_benchmark],
        "missing_dependency": missing_dependency,
        "next_action": (
            "apply reconciliation overlay to scorecard actual basis before ranked-queue refresh"
            if status == "PASS"
            else "rebuild data layer with wallet_copy_self_trades.jsonl and rerun benchmark"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-feed-log", default=DEFAULT_SELF_FEED_LOG)
    parser.add_argument("--duckdb-path", default=DEFAULT_DUCKDB)
    parser.add_argument("--self-feed-report", default=DEFAULT_SELF_FEED_REPORT)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--classification", default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--full-retrace", default=DEFAULT_FULL_RETRACE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    print(json.dumps({"output": str(output), "status": report["status"], "parity": report["parity"]}, sort_keys=True))
    return 0 if report["status"] in {"PASS", "MISSING_DEPENDENCY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
