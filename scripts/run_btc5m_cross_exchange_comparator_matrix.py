#!/usr/bin/env python3
"""Run eight isolated paper-only BTC-5m offset/execution comparator cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_btc5m_cross_exchange_passive_conversion_shadow import (
    run_once as run_passive_once,
)
from scripts.run_btc5m_cross_exchange_probability_edge_paper_lane import (
    run_once as run_taker_once,
)
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.promoted_cell import reduce_promoted_cells
from src.wallet_copy.store import atomic_write_json, load_json


OFFSETS = (15, 30, 45, 60)
DEFAULT_STATE = "data/research/btc5m_cross_exchange_comparator_matrix_state.json"
DEFAULT_CACHE = "data/research/btc5m_cross_exchange_probability_edge_binance_1s_cache.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_ACTUATOR = "data/research/btc5m_cross_exchange_probability_edge_live_actuator_latest.json"
DEFAULT_SELECTOR = "data/research/btc5m_cross_exchange_promoted_cell_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--cache", default=DEFAULT_CACHE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--actuator-state", default=DEFAULT_ACTUATOR)
    parser.add_argument("--selector-state", default=DEFAULT_SELECTOR)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--history-hours", type=float, default=24.0)
    parser.add_argument("--interval-s", type=float, default=10.0)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    return parser.parse_args()


def _cell_path(offset: int, mode: str, kind: str, suffix: str) -> str:
    return (
        "data/research/btc5m_cross_exchange_comparator_"
        f"{offset}s_{mode}_{kind}{suffix}"
    )


def _checksum(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _preregistration(offset: int, mode: str, model_checksum: str) -> dict[str, Any]:
    body = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_comparator_preregistration",
        "cell_id": f"cross_exchange_{offset}s_{mode}",
        "signal_offset_s": offset,
        "execution_mode": mode,
        "model_checksum": model_checksum,
        "paper_only": True,
        "live_orders_allowed": False,
        "registered_before_cell_resolution_inspection": True,
        "entry_bounds": [0.25, 0.50],
        "order_usd": 1.0,
        "fee_model": "canonical_embedded_buy_fee",
        "promotion_authority": False,
    }
    return {**body, "checksum": _checksum(body)}


def _write_or_verify_prereg(path: str, expected: dict[str, Any]) -> None:
    prior = load_json(path, default={})
    if prior:
        canonical = {key: value for key, value in prior.items() if key != "checksum"}
        if prior.get("checksum") != _checksum(canonical) or prior != expected:
            raise RuntimeError(f"immutable comparator preregistration mismatch: {path}")
        return
    atomic_write_json(path, expected)


def run_matrix_once(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = float(args.now_ts or time.time())
    cells: list[dict[str, Any]] = []
    for offset in OFFSETS:
        source_state = _cell_path(offset, "taker", "state", ".json")
        taker_args = SimpleNamespace(
            state=source_state,
            event_log=_cell_path(offset, "taker", "events", ".jsonl"),
            intent_log=_cell_path(offset, "taker", "intents", ".jsonl"),
            cache=args.cache,
            resolutions=args.resolutions,
            model=_cell_path(offset, "taker", "model", ".json"),
            terminal_log=_cell_path(offset, "taker", "terminals", ".jsonl"),
            symbol="BTCUSDT",
            history_hours=float(args.history_hours),
            timeout_s=float(args.timeout_s),
            clob_base_url=args.clob_base_url,
            clob_timeout_s=float(args.clob_timeout_s),
            signal_tolerance_s=8.0,
            signal_offset_s=offset,
            now_ts=now_ts,
        )
        try:
            taker = run_taker_once(taker_args)
            model_checksum = str((taker.get("frozen_model") or {}).get("checksum") or "")
            prereg = _preregistration(offset, "taker", model_checksum)
            _write_or_verify_prereg(
                _cell_path(offset, "taker", "preregistration", ".json"),
                prereg,
            )
            cells.append(
                {
                    "cell_id": f"cross_exchange_{offset}s_taker",
                    "signal_offset_s": offset,
                    "execution_mode": "taker",
                    "status": taker.get("status"),
                    "model_checksum": model_checksum,
                    "preregistration_checksum": prereg["checksum"],
                    "prospective": taker.get("prospective_executable_book"),
                    "state_path": source_state,
                    "terminals_path": _cell_path(offset, "taker", "terminals", ".jsonl"),
                    "preregistration_path": _cell_path(offset, "taker", "preregistration", ".json"),
                    "paper_only": True,
                    "productive": True,
                }
            )
        except Exception as exc:
            cells.append(
                {
                    "cell_id": f"cross_exchange_{offset}s_taker",
                    "signal_offset_s": offset,
                    "execution_mode": "taker",
                    "status": "DATA_FAILURE",
                    "error": f"{type(exc).__name__}: {exc}",
                    "state_path": source_state,
                    "terminals_path": _cell_path(offset, "taker", "terminals", ".jsonl"),
                    "preregistration_path": _cell_path(offset, "taker", "preregistration", ".json"),
                    "paper_only": True,
                    "productive": False,
                }
            )
            continue

        passive_state = _cell_path(offset, "passive", "state", ".json")
        passive_args = SimpleNamespace(
            actuator_state=args.actuator_state,
            source_state=source_state,
            state=passive_state,
            event_log=_cell_path(offset, "passive", "events", ".jsonl"),
            resolutions=args.resolutions,
            clob_base_url=args.clob_base_url,
            clob_timeout_s=float(args.clob_timeout_s),
            cancel_before_close_s=30.0,
            signal_offset_s=offset,
            activation_policy="always-paper",
            interval_s=float(args.interval_s),
            now_ts=now_ts,
        )
        try:
            passive = run_passive_once(passive_args)
            prereg = _preregistration(offset, "passive", model_checksum)
            _write_or_verify_prereg(
                _cell_path(offset, "passive", "preregistration", ".json"),
                prereg,
            )
            cells.append(
                {
                    "cell_id": f"cross_exchange_{offset}s_passive",
                    "signal_offset_s": offset,
                    "execution_mode": "passive",
                    "status": passive.get("status"),
                    "model_checksum": model_checksum,
                    "preregistration_checksum": prereg["checksum"],
                    "summary": passive.get("summary"),
                    "state_path": passive_state,
                    "preregistration_path": _cell_path(offset, "passive", "preregistration", ".json"),
                    "paper_only": True,
                    "productive": True,
                }
            )
        except Exception as exc:
            cells.append(
                {
                    "cell_id": f"cross_exchange_{offset}s_passive",
                    "signal_offset_s": offset,
                    "execution_mode": "passive",
                    "status": "DATA_FAILURE",
                    "error": f"{type(exc).__name__}: {exc}",
                    "state_path": passive_state,
                    "preregistration_path": _cell_path(offset, "passive", "preregistration", ".json"),
                    "paper_only": True,
                    "productive": False,
                }
            )
    payload = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_comparator_matrix",
        "generated_at": utc_now_iso(),
        "flow_stage": "LEARN/OBSERVE/PROMOTE/SELF-DEV",
        "status": "PAPER_MATRIX_ACTIVE",
        "offsets_s": list(OFFSETS),
        "execution_modes": ["taker", "passive"],
        "cell_count": len(cells),
        "productive_cell_count": sum(bool(row.get("productive")) for row in cells),
        "isolated_preregistrations": True,
        "isolated_state_event_intent_ledgers": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "cells": cells,
    }
    atomic_write_json(args.state, payload)
    prior_selector = load_json(args.selector_state, default={})
    selector = reduce_promoted_cells(
        payload,
        resolutions_path=args.resolutions,
        prior_activation=(prior_selector.get("selected") or {}) if isinstance(prior_selector, dict) else {},
        prior_cells=(prior_selector.get("cells") or []) if isinstance(prior_selector, dict) else [],
    )
    selector["generated_at"] = payload["generated_at"]
    selector["single_submitter"] = "scripts/run_wallet_copy_live_guard.py"
    selector["live_reload_required"] = selector["status"] == "PROMOTED_CELL_READY"
    atomic_write_json(args.selector_state, selector)
    payload["promoted_cell_selector"] = {
        "state_path": args.selector_state,
        "status": selector["status"],
        "selected": selector.get("selected"),
    }
    atomic_write_json(args.state, payload)
    return payload


def main() -> int:
    args = parse_args()
    while True:
        payload = run_matrix_once(args)
        print(json.dumps(payload, sort_keys=True), flush=True)
        if not args.watch:
            return 0
        time.sleep(max(1.0, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
