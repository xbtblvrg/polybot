#!/usr/bin/env python3
"""Paper-only parity and guard-consumption lead gate for OrderFilled sidecar."""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import sys
import time
from pathlib import Path
from statistics import quantiles
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mixed-jsonl", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument(
        "--sidecar-jsonl",
        default="data/research/polygon_orderfilled_ws_orderfilled_only.jsonl",
    )
    parser.add_argument(
        "--guard-cursor-state",
        default="data/research/wallet_copy_orderfilled_direct_cursor_state.json",
    )
    parser.add_argument(
        "--state",
        default="data/research/orderfilled_sidecar_parity_shadow_state.json",
    )
    parser.add_argument("--required-identities", type=int, default=100)
    parser.add_argument("--required-p95-lead-s", type=float, default=5.0)
    parser.add_argument("--loss-grace-s", type=float, default=60.0)
    parser.add_argument("--max-records", type=int, default=20_000)
    parser.add_argument("--iterations", type=int, default=0, help="0 runs forever.")
    parser.add_argument("--sleep-s", type=float, default=0.25)
    return parser.parse_args()


def _identity(row: dict[str, Any]) -> str:
    tx_hash = str(row.get("transaction_hash") or "").strip().lower()
    log_index = row.get("log_index")
    return f"{tx_hash}|{log_index}" if tx_hash and isinstance(log_index, int) else ""


def _payload_hash(row: dict[str, Any]) -> str:
    payload = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _read_range(path: Path, start: int, stop: int | None = None) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], max(0, start)
    size = path.stat().st_size
    end = size if stop is None else min(size, max(0, stop))
    cursor = min(max(0, start), end)
    rows: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        handle.seek(cursor)
        while handle.tell() < end:
            raw = handle.readline()
            if not raw or handle.tell() > end:
                break
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict) and row.get("event") == "polygon_orderfilled_log":
                rows.append(row)
        cursor = handle.tell()
    return rows, cursor


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) < 20:
        return max(values)
    return quantiles(values, n=100, method="inclusive")[94]


def initialize_live_cursor_handoff(
    *,
    mixed_cursor_state: str | Path,
    sidecar_jsonl: str | Path,
    sidecar_cursor_state: str | Path,
    max_seen_identities: int = 5000,
) -> dict[str, Any]:
    """Seed the sidecar cursor before a proven mixed-stream identity.

    The earliest occurrence of the newest available old identity is used.
    Starting before that row is intentionally conservative: the retained
    identity set absorbs the boundary replay, while no unseen row can be
    skipped by choosing a later duplicate.
    """
    old_cursor = load_json(str(mixed_cursor_state), default={}, cache_readonly=False)
    old_cursor = old_cursor if isinstance(old_cursor, dict) else {}
    seen_order = [str(value) for value in old_cursor.get("seen_identities") or [] if str(value)]
    sidecar = Path(sidecar_jsonl)
    target_rank = {identity: rank for rank, identity in enumerate(seen_order)}
    found: dict[str, int] = {}
    if sidecar.exists() and target_rank:
        with sidecar.open("rb") as handle:
            while True:
                row_start = handle.tell()
                raw = handle.readline()
                if not raw:
                    break
                try:
                    row = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(row, dict):
                    continue
                identity = _identity(row)
                if identity in target_rank and identity not in found:
                    found[identity] = row_start
    selected_identity = max(found, key=lambda value: target_rank[value]) if found else ""
    selected_offset = int(found[selected_identity]) if selected_identity else 0
    report = {
        "schema_version": 1,
        "flow_stage": "LIVE/OBSERVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "HANDOFF_BEFORE_MATCHED_IDENTITY" if selected_identity else "HANDOFF_FROM_FILE_START",
        "source_jsonl": str(sidecar),
        "next_byte_offset": selected_offset,
        "boundary_identity": selected_identity,
        "boundary_rule": "start_before_earliest_occurrence_of_newest_available_mixed_cursor_identity",
        "boundary_identity_loss": 0,
        "safe_replay_expected": bool(selected_identity),
        "seen_identities": seen_order[-max(1, int(max_seen_identities)) :],
        "old_mixed_next_byte_offset": int(old_cursor.get("next_byte_offset") or 0),
        "identity_rule": "transaction_hash|log_index",
    }
    atomic_write_json(Path(sidecar_cursor_state), report)
    return report


def run_once(args: argparse.Namespace, *, now_s: float | None = None) -> dict[str, Any]:
    now = time.time() if now_s is None else float(now_s)
    state_path = Path(args.state)
    state = load_json(str(state_path), default={}, cache_readonly=False)
    state = state if isinstance(state, dict) else {}
    records = state.get("records") if isinstance(state.get("records"), dict) else {}
    records = {str(key): dict(value) for key, value in records.items() if isinstance(value, dict)}

    sidecar = Path(args.sidecar_jsonl)
    mixed = Path(args.mixed_jsonl)
    guard_cursor = load_json(str(args.guard_cursor_state), default={}, cache_readonly=False)
    guard_offset = int(guard_cursor.get("next_byte_offset") or 0) if isinstance(guard_cursor, dict) else 0
    initialized = bool(state.get("initialized"))
    sidecar_offset = int(state.get("sidecar_offset") or 0)
    mixed_offset = int(state.get("mixed_guard_offset") or 0)
    if not initialized:
        sidecar_offset = sidecar.stat().st_size if sidecar.exists() else 0
        mixed_offset = guard_offset

    sidecar_rows, sidecar_next = _read_range(sidecar, sidecar_offset)
    for row in sidecar_rows:
        identity = _identity(row)
        if not identity:
            continue
        digest = _payload_hash(row)
        record = records.setdefault(digest, {"identity": identity})
        record["sidecar_seen_at_s"] = min(
            float(record.get("sidecar_seen_at_s") or now),
            float(row.get("received_at_s") or now),
        )

    mixed_stop = max(mixed_offset, guard_offset)
    mixed_rows, mixed_next = _read_range(mixed, mixed_offset, mixed_stop)
    for row in mixed_rows:
        identity = _identity(row)
        if not identity:
            continue
        digest = _payload_hash(row)
        record = records.setdefault(digest, {"identity": identity})
        record["mixed_guard_seen_at_s"] = min(float(record.get("mixed_guard_seen_at_s") or now), now)

    ordered = sorted(
        records.items(),
        key=lambda item: max(
            float(item[1].get("sidecar_seen_at_s") or 0.0),
            float(item[1].get("mixed_guard_seen_at_s") or 0.0),
        ),
    )[-max(1, int(args.max_records)) :]
    records = dict(ordered)
    matched = [row for row in records.values() if row.get("sidecar_seen_at_s") and row.get("mixed_guard_seen_at_s")]
    matched_identities = {str(row.get("identity") or "") for row in matched if row.get("identity")}
    leads = [
        float(row["mixed_guard_seen_at_s"]) - float(row["sidecar_seen_at_s"])
        for row in matched
    ]
    overdue_sidecar_only = [
        row
        for row in records.values()
        if row.get("sidecar_seen_at_s")
        and not row.get("mixed_guard_seen_at_s")
        and now - float(row["sidecar_seen_at_s"]) > float(args.loss_grace_s)
    ]
    overdue_mixed_only = [
        row
        for row in records.values()
        if row.get("mixed_guard_seen_at_s")
        and not row.get("sidecar_seen_at_s")
        and now - float(row["mixed_guard_seen_at_s"]) > float(args.loss_grace_s)
    ]
    p95_lead = _p95(leads)
    rss_raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_gib = float(rss_raw) / (1024**3 if sys.platform == "darwin" else 1024**2)
    current_gate_passed = bool(
        len(matched_identities) >= int(args.required_identities)
        and not overdue_sidecar_only
        and not overdue_mixed_only
        and p95_lead is not None
        and p95_lead > float(args.required_p95_lead_s)
        and rss_gib < 1.0
    )
    gate_passed_once = bool(state.get("gate_passed_once")) or current_gate_passed
    first_passed_at = state.get("first_passed_at")
    if current_gate_passed and not first_passed_at:
        first_passed_at = utc_now_iso()
    report = {
        "schema_version": 1,
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "PASS_LATCHED" if gate_passed_once else "ACCRUING",
        "paper_only": True,
        "live_source_wiring_gate_passed": gate_passed_once,
        "current_window_gate_passed": current_gate_passed,
        "gate_passed_once": gate_passed_once,
        "first_passed_at": first_passed_at,
        "identity_rule": "transaction_hash|log_index",
        "payload_rule": "exact canonical JSON SHA-256 equality",
        "initialized": True,
        "sidecar_offset": sidecar_next,
        "mixed_guard_offset": mixed_next,
        "guard_reported_offset": guard_offset,
        "sidecar_rows_read": len(sidecar_rows),
        "mixed_rows_read_through_guard_cursor": len(mixed_rows),
        "unique_exact_payload_matched_identities": len(matched_identities),
        "required_identities": int(args.required_identities),
        "overdue_sidecar_only_payloads": len(overdue_sidecar_only),
        "overdue_mixed_only_payloads": len(overdue_mixed_only),
        "loss_grace_s": float(args.loss_grace_s),
        "p95_sidecar_lead_vs_mixed_guard_s": round(p95_lead, 6) if p95_lead is not None else None,
        "required_p95_lead_s": float(args.required_p95_lead_s),
        "max_rss_gib": round(rss_gib, 6),
        "max_rss_gate_gib": 1.0,
        "records": records,
    }
    atomic_write_json(state_path, report)
    return report


def main() -> int:
    args = parse_args()
    iteration = 0
    while args.iterations <= 0 or iteration < args.iterations:
        run_once(args)
        iteration += 1
        if args.iterations > 0 and iteration >= args.iterations:
            break
        time.sleep(max(0.05, float(args.sleep_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
