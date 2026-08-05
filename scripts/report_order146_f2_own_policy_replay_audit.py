#!/usr/bin/env python3
"""Reproduce ORDER146's exact own-policy replay from immutable captures."""

from __future__ import annotations

import argparse
import bisect
import glob
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.alpha_decay import btc_5m_move_slice_for_values
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.store import atomic_write_json, load_json

ORDER149_IDENTITIES = (
    ("0x4c9497941333332d29f1c235dd23200f3623ffad", "e6d0000861d94c31c1ae3be7ebdfe05c25978ab030413e9ce11c70456c762b0e"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "53791c40b7cc8ba9d6300f068a8e02e0f5bfda41724a151ed079ae5ecddb343e"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "538b6b5a3fe49bd6d82b6eb399873c19af77ab6d43c45e2a958e617920453074"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "6dde1dc38c9f381b586c64977df7848b6ed52630d90217b7d7c874b962bc848f"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "731393132525421d99ecd0a6b19641b5aa9a1a0781496113bb10447412bd0b12"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "81feedc6ac94479ba944d8bea7e9da8f575bc124bc79ff9bf8cb526c4a33027b"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "cf14c0d7eb4a4739c5bd8c090114b30d91a57f1b71c263809e81bfcbda3d0117"),
    ("0x568b079891fcc8bef2e557fa2a8a7ecca1700b3b", "dee3a70eaf4a2404d6435f24bf50a430ad1abdb40b0512632005b1b41da5659e"),
)
MAX_BOOK_JOIN_AGE_S = 120.0
MIN_OWN_POLICY_BOOK_COVERAGE = 0.90


def load_envelopes(path: Path) -> list[dict[str, Any]]:
    return list(_jsonl(path))


def _jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _paths(patterns: Iterable[str]) -> list[Path]:
    return sorted({Path(item) for pattern in patterns for item in glob.glob(pattern)})


def _epoch(value: Any) -> float | None:
    if isinstance(value, (int, float)) and float(value) > 0:
        return float(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def load_source_events(paths: Iterable[Path], desired: set[tuple[str, int]]) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for path in paths:
        for row in _jsonl(path):
            if row.get("event") != "polygon_orderfilled_log":
                continue
            tx = str(row.get("transaction_hash") or "").lower()
            try:
                log_index = int(str(row.get("log_index") or "0"), 0)
            except ValueError:
                continue
            key = (tx, log_index)
            if key not in desired:
                continue
            decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
            if decoded.get("decode_status") not in {None, "", "OK"}:
                continue
            prior = out.get(key)
            if prior is None or str(row.get("source")) == "polygon_ws":
                out[key] = row
    return out


def load_book_snapshots(paths: Iterable[Path], desired_assets: set[str]) -> dict[str, list[tuple[float, dict[str, Any]]]]:
    out: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for path in paths:
        for row in _jsonl(path):
            asset = str(row.get("asset_id") or "")
            captured = _epoch(row.get("captured_at_s") or row.get("captured_at_iso"))
            if asset in desired_assets and captured is not None:
                out.setdefault(asset, []).append((captured, row))
    for rows in out.values():
        rows.sort(key=lambda item: item[0])
    return out


def _nearest_at_or_before(rows: list[tuple[float, dict[str, Any]]], decision_s: float) -> tuple[float, dict[str, Any]] | None:
    index = bisect.bisect_right([item[0] for item in rows], decision_s) - 1
    return rows[index] if index >= 0 else None


def build_report(
    *,
    deadman: dict[str, Any],
    envelopes: list[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
    source_events: dict[tuple[str, int], dict[str, Any]] | None = None,
    book_snapshots: dict[str, list[tuple[float, dict[str, Any]]]] | None = None,
    identities: tuple[tuple[str, str], ...] = ORDER149_IDENTITIES,
) -> dict[str, Any]:
    metadata = metadata or {}
    source_events = source_events or {}
    book_snapshots = book_snapshots or {}
    frontier_rows = (((((deadman.get("policy_choke") or {}).get("actuator") or {}).get("candidate_evidence") or {}).get("nearest_frontier")) or [])
    frontier_by_identity = {
        (str(row.get("wallet") or "").lower(), str(row.get("wide_policy_fingerprint") or "")): row
        for row in frontier_rows
    }
    frontier = [
        {"wallet": wallet, "wide_policy_fingerprint": fingerprint, **frontier_by_identity.get((wallet, fingerprint), {})}
        for wallet, fingerprint in identities
    ]
    rows_by_wallet: dict[str, dict[str, dict[str, Any]]] = {}
    run_ids: set[str] = set()
    for envelope in envelopes:
        run_id = str((envelope.get("identity") or {}).get("run_id") or "")
        if run_id:
            run_ids.add(run_id)
        for row in envelope.get("rows") or []:
            wallet = str(row.get("wallet") or "").lower()
            attempt_id = str(row.get("attempt_id") or row.get("row_identity") or "")
            if wallet and attempt_id:
                rows_by_wallet.setdefault(wallet, {})[attempt_id] = row

    candidates: list[dict[str, Any]] = []
    aggregate_joins: Counter[str] = Counter()
    total_unique = 0
    for rank, candidate in enumerate(frontier, 1):
        wallet = str(candidate.get("wallet") or "").lower()
        rows = list((rows_by_wallet.get(wallet) or {}).values())
        taxonomy = Counter(str((row.get("f1_f4_terminal") or {}).get("terminal") or "UNKNOWN") for row in rows)
        policy_keys = set(((candidate.get("policy") or {}).get("move_slice_keys") or []))
        joins = Counter()
        own_policy_pass = 0
        executable = 0
        join_ages: list[float] = []
        missing = Counter()
        for row in rows:
            token_id = str(row.get("token_id") or "")
            meta = metadata.get(token_id) if isinstance(metadata.get(token_id), dict) else {}
            if not all(meta.get(field) for field in ("condition_id", "market_slug", "outcome")):
                missing["token_metadata_join"] += 1
                continue
            joins["metadata"] += 1
            tx = str(row.get("transaction_hash") or "").lower()
            try:
                source_key = (tx, int(str(row.get("source_event_id") or "0"), 0))
            except ValueError:
                missing["source_event_join"] += 1
                continue
            source = source_events.get(source_key)
            if not source:
                missing["source_event_join"] += 1
                continue
            decoded = source.get("decoded") if isinstance(source.get("decoded"), dict) else {}
            if str(decoded.get("asset") or "") != token_id:
                missing["source_event_asset_mismatch"] += 1
                continue
            joins["source_event"] += 1
            move = btc_5m_move_slice_for_values(
                market_slug=str(meta["market_slug"]),
                event_ts=_epoch(source.get("event_ts")),
                price=float(decoded.get("price") or 0.0),
            )
            if move["move_slice_key"] not in policy_keys:
                continue
            own_policy_pass += 1
            decision_s = _epoch(source.get("received_at_s") or source.get("captured_at_s"))
            nearest = _nearest_at_or_before(book_snapshots.get(token_id, []), decision_s or 0.0)
            if nearest is None:
                missing["book_uncovered"] += 1
                continue
            captured_s, book = nearest
            join_age_s = max(0.0, (decision_s or captured_s) - captured_s)
            if join_age_s > MAX_BOOK_JOIN_AGE_S:
                missing["book_snapshot_stale"] += 1
                continue
            joins["book_snapshot"] += 1
            join_ages.append(join_age_s)
            scored = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=1.0,
                source_price=float(decoded.get("price") or 0.0),
                max_slippage_bps=250.0,
            )
            if scored.get("instant_fill_status") == "PASS" and float(scored.get("fill_ratio") or 0.0) >= 0.999:
                executable += 1
        total_unique += len(rows)
        aggregate_joins.update(joins)
        complete = not missing
        book_coverage = joins["book_snapshot"] / own_policy_pass if own_policy_pass else None
        book_coverage_pass = bool(
            book_coverage is not None
            and book_coverage + 1e-12 >= MIN_OWN_POLICY_BOOK_COVERAGE
        )
        measured_copyables = executable if executable > 0 or complete else None
        replay_status = (
            "MEASURED_LOWER_BOUND"
            if executable > 0 and not complete
            else "MEASURED"
            if complete
            else "JOIN_RATE_FAILURE"
        )
        candidates.append({
            "rank": rank,
            "wallet": wallet,
            "wide_policy_fingerprint": candidate.get("wide_policy_fingerprint"),
            "base_policy_id": candidate.get("f2_copyable_policy_id"),
            "journal_unique_attempts": len(rows),
            "joined_raw_attempts": joins["source_event"],
            "recorded_terminal_taxonomy": dict(sorted(taxonomy.items())),
            "own_move_slice_keys": sorted(policy_keys),
            "join_counts": dict(sorted(joins.items())),
            "join_rates": {key: round(value / len(rows), 6) if rows else None for key, value in sorted(joins.items())},
            "book_join_age_s_max": round(max(join_ages), 6) if join_ages else None,
            "book_join_max_age_s": MAX_BOOK_JOIN_AGE_S,
            "book_coverage_own_policy_pass": round(book_coverage, 6) if book_coverage is not None else None,
            "book_coverage_floor": MIN_OWN_POLICY_BOOK_COVERAGE,
            "book_coverage_pass": book_coverage_pass,
            "own_policy_replay_passes": own_policy_pass,
            "observed_own_policy_copyables": executable,
            "own_policy_replay_copyables": measured_copyables,
            "missing_required_fields": dict(sorted(missing.items())),
            "replay_status": replay_status,
        })
    positive = [row for row in candidates if int(row.get("own_policy_replay_copyables") or 0) > 0]
    unresolved_zero = [row for row in candidates if row.get("own_policy_replay_copyables") is None]
    status = "OWN_POLICY_COPYABLE_FOUND" if positive else "JOIN_RATE_VERDICT_FAIL" if unresolved_zero else "OWN_POLICY_EMPTY_MEASURED"
    return {
        "schema_version": 2,
        "kind": "order146_f2_own_policy_replay_audit",
        "flow_stage": "MEASURE/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": status,
        "verdict": True if positive else None if unresolved_zero else False,
        "f2_gate_comparable": False,
        "measurement_scope": "multi-generation historical counterfactual replay; not the deadman's rolling 30-minute emitted-copyable gate",
        "frontier_candidate_count": len(candidates),
        "frontier_unique_wallet_count": len({row["wallet"] for row in candidates}),
        "journal_unique_attempts": total_unique,
        "aggregate_join_counts": dict(sorted(aggregate_joins.items())),
        "source_run_ids": sorted(run_ids),
        "stopped_before_policy_replay": False,
        "synthesis_permitted": False,
        "candidates": candidates,
        "next_action": "retain as historical supply evidence; do not use as a fresh F2 gate verdict" if positive else "repair exact reported join deficits" if unresolved_zero else "publish measured empty historical pool",
        "reproduction": {
            "producer": "scripts/report_order146_f2_own_policy_replay_audit.py",
            "deadman": "data/research/order_flow_deadman_state.json",
            "journal": "data/research/wide_direct_handoff_journal.jsonl",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deadman", default="data/research/order_flow_deadman_state.json")
    parser.add_argument("--journal", default="data/research/wide_direct_handoff_journal.jsonl")
    parser.add_argument("--metadata", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--source-events", action="append", default=[])
    parser.add_argument("--book-snapshots", action="append", default=[])
    parser.add_argument("--output", default="data/research/order146_f2_own_policy_replay_latest.json")
    args = parser.parse_args()
    envelopes = load_envelopes(Path(args.journal))
    journal_rows = [row for env in envelopes for row in (env.get("rows") or [])]
    desired = set()
    for row in journal_rows:
        try:
            desired.add((str(row.get("transaction_hash") or "").lower(), int(str(row.get("source_event_id") or "0"), 0)))
        except ValueError:
            pass
    assets = {str(row.get("token_id") or "") for row in journal_rows if row.get("token_id")}
    source_paths = _paths(args.source_events)
    book_paths = _paths(args.book_snapshots)
    report = build_report(
        deadman=load_json(args.deadman, default={}),
        envelopes=envelopes,
        metadata=load_json(args.metadata, default={}),
        source_events=load_source_events(source_paths, desired),
        book_snapshots=load_book_snapshots(book_paths, assets),
    )
    report["reproduction"].update({"source_events": [str(path) for path in source_paths], "book_snapshots": [str(path) for path in book_paths]})
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
