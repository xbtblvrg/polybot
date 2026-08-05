#!/usr/bin/env python3
"""Build a compact cumulative report for guard-owned shadow lanes."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _provenance_bucket(provenance: dict[str, Any]) -> str:
    category = str(provenance.get("category") or "unknown")
    if category == "direct_fallback":
        return "direct_fallback"
    if category in {"genuine_book", "book_derived", "top_of_book"}:
        return "book_derived"
    if provenance.get("book_hash_present") and str(provenance.get("top_of_book_status") or "") == "OK":
        return "book_derived"
    return category


def _fallback_reason(provenance: dict[str, Any], book_test: dict[str, Any]) -> str:
    fallback_source = provenance.get("fallback_source") or book_test.get("fallback_source")
    if fallback_source:
        return str(fallback_source)
    if provenance.get("primary_error_present"):
        return "primary_error_present"
    if provenance.get("route_class"):
        return str(provenance.get("route_class"))
    if book_test.get("blocking_reason"):
        return str(book_test.get("blocking_reason"))
    return "unspecified"


def build_rollup(*, events_path: Path, state_path: Path) -> dict[str, Any]:
    events = _load_jsonl(events_path)
    state = load_json(state_path, default={})
    state = state if isinstance(state, dict) else {}
    lanes: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "rows_seen": 0,
            "unique_intents": 0,
            "book_aware_fill_test_counts": {},
            "pricing_provenance_split": {},
            "fallback_reason_histogram": {},
            "would_have_filled_first_seen": 0,
            "orders_submitted": 0,
            "event_counts": {},
        }
    )
    event_counts = Counter()
    evidence_counts = Counter()
    for event in events:
        event_name = str(event.get("event") or "unknown")
        event_counts[event_name] += 1
        if event_name.startswith("shadow_"):
            evidence_counts[event_name] += 1
        lane = str(event.get("lane") or "")
        if lane:
            lanes[lane]["event_counts"][event_name] = lanes[lane]["event_counts"].get(event_name, 0) + 1

    seen_keys: dict[str, set[tuple[str, str]]] = defaultdict(set)
    first_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in state.get("rows") or []:
        if not isinstance(row, dict):
            continue
        lane = str(row.get("lane") or "unknown")
        intent_id = str(row.get("intent_id") or "")
        first_seen_at = str(row.get("first_seen_at") or "")
        key = (intent_id, first_seen_at or intent_id)
        lanes[lane]["rows_seen"] += 1
        lanes[lane]["orders_submitted"] += int(row.get("orders_submitted") or 0)
        if intent_id:
            seen_keys[lane].add(key)
            first_rows.setdefault((lane, intent_id), row)

        book_test = row.get("book_aware_fill_test") if isinstance(row.get("book_aware_fill_test"), dict) else {}
        verdict = str(book_test.get("instant_fill_status") or book_test.get("status") or "missing")
        book_counts = Counter(lanes[lane]["book_aware_fill_test_counts"])
        book_counts[verdict] += 1
        lanes[lane]["book_aware_fill_test_counts"] = dict(sorted(book_counts.items()))

        provenance = row.get("pricing_provenance") if isinstance(row.get("pricing_provenance"), dict) else {}
        bucket = _provenance_bucket(provenance)
        provenance_counts = Counter(lanes[lane]["pricing_provenance_split"])
        provenance_counts[bucket] += 1
        lanes[lane]["pricing_provenance_split"] = dict(sorted(provenance_counts.items()))

        if bucket == "direct_fallback":
            fallback_counts = Counter(lanes[lane]["fallback_reason_histogram"])
            fallback_counts[_fallback_reason(provenance, book_test)] += 1
            lanes[lane]["fallback_reason_histogram"] = dict(sorted(fallback_counts.items()))

    for lane, keys in seen_keys.items():
        lanes[lane]["unique_intents"] = len(keys)
    for (lane, _intent_id), row in first_rows.items():
        if row.get("would_have_filled"):
            lanes[lane]["would_have_filled_first_seen"] += 1

    lane_payload = {lane: lanes[lane] for lane in sorted(lanes)}
    orders_submitted = sum(int(lane.get("orders_submitted") or 0) for lane in lane_payload.values())
    return {
        "schema_version": 1,
        "kind": "wallet_copy_guard_shadow_lanes_rollup",
        "generated_at": utc_now_iso(),
        "source_events": str(events_path),
        "source_state": str(state_path),
        "event_log_size_bytes": events_path.stat().st_size if events_path.exists() else 0,
        "event_counts": dict(sorted(event_counts.items())),
        "evidence_event_counts": dict(sorted(evidence_counts.items())),
        "lanes": lane_payload,
        "zero_live_assertion": {
            "status": "PASS" if orders_submitted == 0 else "INCIDENT",
            "orders_submitted": orders_submitted,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="data/research/wallet_copy_guard_shadow_lanes_events.jsonl")
    parser.add_argument("--state", default="data/research/wallet_copy_guard_shadow_lanes_state.json")
    parser.add_argument("--out", default="data/research/wallet_copy_guard_shadow_lanes_rollup.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rollup = build_rollup(events_path=Path(args.events), state_path=Path(args.state))
    atomic_write_json(args.out, rollup)
    print(json.dumps(rollup, sort_keys=True))


if __name__ == "__main__":
    main()
