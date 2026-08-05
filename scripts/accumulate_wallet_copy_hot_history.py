#!/usr/bin/env python3
"""Accumulate the rolling live-guard history into a durable stable-id store."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_HOT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_DATABASE = "data/derived/wallet_copy_hot_history_accumulator.sqlite3"
DEFAULT_STATE = "data/research/wallet_copy_hot_history_accumulator_state.json"
DEFAULT_COPY_INTENTS_STATE = "data/research/wallet_copy_live_guard_copy_intents_state.json"
DEFAULT_SUPPLEMENTAL_OUTPUT = (
    "data/research/wallet_copy_hot_history_accumulator_supplemental.json"
)
DEFAULT_SUPPLEMENTAL_MANIFEST = (
    "data/research/temporal_supplemental_history_manifest.json"
)
FRESHNESS_LIMIT_S = 86400.0
SUPPLEMENTAL_EVENT_FIELDS = (
    "schema_version",
    "row_type",
    "source",
    "source_wallet",
    "wallet_name",
    "action",
    "market_slug",
    "event_slug",
    "condition_id",
    "market_id",
    "outcome",
    "outcome_index",
    "price",
    "size",
    "usdc_size",
    "event_ts",
    "observed_ts",
    "transaction_hash",
    "event_id",
)
SUPPLEMENTAL_INTENT_FIELDS = (
    "intent_id",
    "source_row_event_id",
    "source_event_id",
    "source_wallet",
    "market_slug",
    "observed_ts",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hot-history", default=DEFAULT_HOT_HISTORY)
    parser.add_argument("--copy-intents-state", default=DEFAULT_COPY_INTENTS_STATE)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--supplemental-output", default="")
    parser.add_argument("--supplemental-manifest", default=DEFAULT_SUPPLEMENTAL_MANIFEST)
    return parser.parse_args()


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _event_ts(row: dict[str, Any]) -> float:
    return _float(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp"))


def _event_id(row: dict[str, Any]) -> str:
    explicit = str(row.get("event_id") or row.get("id") or "").strip()
    if explicit:
        return explicit
    canonical = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "derived_" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _relative(path: Path) -> str:
    resolved = path.resolve()
    return str(resolved.relative_to(ROOT)) if resolved.is_relative_to(ROOT) else str(resolved)


def export_supplemental_history(
    *,
    database: Path,
    output: Path,
    manifest_path: Path,
    generated_at: str,
) -> dict[str, Any]:
    """Export the accumulator additively through the temporal manifest contract."""
    with sqlite3.connect(database, timeout=30.0) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS intents ("
            "intent_id TEXT PRIMARY KEY, observed_ts REAL NOT NULL, payload_json TEXT NOT NULL)"
        )
        rows = connection.execute(
            "SELECT payload_json FROM events ORDER BY event_ts, event_id"
        ).fetchall()
        intent_rows = connection.execute(
            "SELECT payload_json FROM intents ORDER BY observed_ts, intent_id"
        ).fetchall()
    events: list[dict[str, Any]] = []
    wallets: set[str] = set()
    for (payload_json,) in rows:
        try:
            row = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue
        events.append(
            {
                key: row[key]
                for key in SUPPLEMENTAL_EVENT_FIELDS
                if key in row
            }
        )
        wallet = str(row.get("source_wallet") or row.get("wallet") or "").strip().lower()
        if wallet.startswith("0x") and len(wallet) == 42:
            wallets.add(wallet)
    copy_intents: list[dict[str, Any]] = []
    for (payload_json,) in intent_rows:
        try:
            row = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict):
            copy_intents.append(
                {key: row[key] for key in SUPPLEMENTAL_INTENT_FIELDS if key in row}
            )
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_history_state",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "additive_supplemental": True,
        "source": str(database),
        "events": events,
        "copy_intents": copy_intents,
        "wallets": [
            {
                "address": wallet,
                "enabled": True,
                "market_filter": "btc_5m",
                "asset_allowlist": ["BTC"],
                "tags": ["hot_history_accumulator", "paper_only", "additive_supplemental"],
            }
            for wallet in sorted(wallets)
        ],
        "wallet_results": [],
    }
    atomic_write_json(output, payload)

    manifest = load_json(manifest_path, default={})
    manifest = dict(manifest) if isinstance(manifest, dict) else {
        "schema_version": 1,
        "kind": "temporal_supplemental_history_manifest",
        "flow_stage": "LEARN/PROMOTE",
        "supplemental_history_files": list(manifest) if isinstance(manifest, list) else [],
    }
    files = [
        str(path)
        for path in manifest.get("supplemental_history_files") or []
        if str(path or "").strip()
    ]
    files.append(_relative(output))
    manifest["supplemental_history_files"] = sorted(dict.fromkeys(files))
    manifest["updated_at"] = generated_at
    manifest["last_additive_export_at"] = generated_at
    manifest["last_additive_export"] = {
        "path": _relative(output),
        "event_count": len(events),
        "copy_intent_count": len(copy_intents),
        "wallet_count": len(wallets),
        "mode": "additive_only_no_repoint",
    }
    atomic_write_json(manifest_path, manifest)
    return {
        "path": _relative(output),
        "event_count": len(events),
        "copy_intent_count": len(copy_intents),
        "wallet_count": len(wallets),
        "manifest_file_count": len(manifest["supplemental_history_files"]),
    }


def accumulate(
    *,
    hot_history: Path,
    database: Path,
    state_path: Path,
    now_ts: float | None = None,
    copy_intents_state: Path | None = None,
) -> dict[str, Any]:
    now_ts = float(now_ts or time.time())
    payload = load_json(hot_history, default={})
    events = payload.get("events") if isinstance(payload, dict) and isinstance(payload.get("events"), list) else []
    intent_payload = load_json(copy_intents_state, default={}) if copy_intents_state else {}
    copy_intents = (
        intent_payload.get("copy_intents")
        if isinstance(intent_payload, dict) and isinstance(intent_payload.get("copy_intents"), list)
        else payload.get("copy_intents")
        if isinstance(payload, dict) and isinstance(payload.get("copy_intents"), list)
        else []
    )
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database, timeout=30.0) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "event_id TEXT PRIMARY KEY, event_ts REAL NOT NULL, payload_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS intents ("
            "intent_id TEXT PRIMARY KEY, observed_ts REAL NOT NULL, payload_json TEXT NOT NULL)"
        )
        before = int(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        intents_before = int(connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0])
        connection.executemany(
            "INSERT OR IGNORE INTO events(event_id, event_ts, payload_json) VALUES (?, ?, ?)",
            (
                (
                    _event_id(row),
                    _event_ts(row),
                    json.dumps(row, sort_keys=True, separators=(",", ":")),
                )
                for row in events
                if isinstance(row, dict)
            ),
        )
        connection.executemany(
            "INSERT OR IGNORE INTO intents(intent_id, observed_ts, payload_json) VALUES (?, ?, ?)",
            (
                (
                    str(row.get("intent_id") or ""),
                    _float(row.get("observed_ts") or row.get("event_ts")),
                    json.dumps(row, sort_keys=True, separators=(",", ":")),
                )
                for row in copy_intents
                if isinstance(row, dict) and str(row.get("intent_id") or "")
            ),
        )
        connection.commit()
        count, oldest_ts, newest_ts = connection.execute(
            "SELECT COUNT(*), MIN(event_ts), MAX(event_ts) FROM events"
        ).fetchone()
        intent_count = int(connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0])
    newest_age_s = max(0.0, now_ts - float(newest_ts)) if newest_ts else None
    state = {
        "schema_version": 1,
        "kind": "wallet_copy_hot_history_accumulator",
        "flow_stage": "OBSERVE/LEARN",
        "status": "ACCUMULATING_CURRENT_SOURCE"
        if newest_age_s is not None and newest_age_s <= FRESHNESS_LIMIT_S
        else "STALE_SOURCE_FAIL_CLOSED",
        "promotion_grade": False,
        "generated_at": _iso(now_ts),
        "source": str(hot_history),
        "database": str(database),
        "hot_events_scanned": len(events),
        "events_inserted": int(count) - before,
        "hot_copy_intents_scanned": len(copy_intents),
        "copy_intents_inserted": intent_count - intents_before,
        "copy_intent_count": intent_count,
        "event_count": int(count),
        "source_wallet_count": None,
        "oldest_source_event_ts": float(oldest_ts) if oldest_ts else None,
        "newest_source_event_ts": float(newest_ts) if newest_ts else None,
        "oldest_source_event_iso": _iso(oldest_ts),
        "newest_source_event_iso": _iso(newest_ts),
        "span_days": round((float(newest_ts) - float(oldest_ts)) / 86400.0, 6)
        if oldest_ts and newest_ts
        else 0.0,
        "source_freshness": {
            "newest_source_event_age_s": round(newest_age_s, 6) if newest_age_s is not None else None,
            "freshness_limit_s": FRESHNESS_LIMIT_S,
            "pass": bool(newest_age_s is not None and newest_age_s <= FRESHNESS_LIMIT_S),
        },
        "repoint_allowed": False,
        "repoint_gate": (
            "separate Fable-audited commit after span >=3 genuine days AND "
            "accumulator wallet coverage >= registry resolved-history wallet count"
        ),
    }
    atomic_write_json(state_path, state)
    return state


def main() -> int:
    args = parse_args()
    state = accumulate(
        hot_history=Path(args.hot_history),
        database=Path(args.database),
        state_path=Path(args.state),
        copy_intents_state=Path(args.copy_intents_state),
    )
    if str(args.supplemental_output or "").strip():
        export = export_supplemental_history(
            database=Path(args.database),
            output=Path(args.supplemental_output),
            manifest_path=Path(args.supplemental_manifest),
            generated_at=str(state["generated_at"]),
        )
        state["supplemental_export"] = export
        state["source_wallet_count"] = int(export["wallet_count"])
        atomic_write_json(Path(args.state), state)
    print(json.dumps(state, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
