#!/usr/bin/env python3
"""Attribute d60c RULING20c market-closed abstains.

Flow stage: LIVE/MEASURE. This is a bounded, read-only reconstruction of the
four d60c market_closed_now rows from the RULING10 probe. It never mutates live
eligibility, rotation, caps, thresholds, or order submission.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/d60c_latency_attribution_latest.json"
DEFAULT_RULING10_PROBE = "data/research/ruling10_abstain_probe_latest.json"
DEFAULT_WALLET_EVENTS = "data/research/wallet_copy_live_guard_wallet_events.jsonl"
D60C_WALLET = "0x40138697bf1a0d655593f3be6237d60c1dc7ab35"
D60C_CANDIDATE_ID = "runtime_auto_degrade_40138697bf"
RULING_ID = "2026-07-17T12:10Z-fable-ruling20c-d60c-latency-attribution"
DEFAULT_TAIL_BYTES = 400_000_000


@dataclass(frozen=True)
class ScanResult:
    rows: list[dict[str, Any]]
    scanned_lines: int
    parsed_rows: int
    parse_errors: int
    file_size_bytes: int
    scan_offset_bytes: int


def _resolve(root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else root / path


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> float | None:
    numeric = _as_float(value)
    if numeric is not None:
        return numeric
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).timestamp()


def _iso_from_ts(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), UTC).isoformat().replace("+00:00", "Z")


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _ruling10_member(probe: dict[str, Any], label: str) -> dict[str, Any]:
    for row in _as_list(probe.get("members")):
        if isinstance(row, dict) and str(row.get("label") or "") == label:
            return row
    return {}


def _target_market_closed_count(member: dict[str, Any]) -> int:
    counts = _as_dict(member.get("all_abstain_reason_counts"))
    try:
        return max(0, int(counts.get("market_closed_now") or 0))
    except (TypeError, ValueError):
        return 0


def _decision_ts(member: dict[str, Any]) -> tuple[float | None, str | None, str]:
    guard_stamp = _as_dict(member.get("guard_stamp"))
    for key, basis in (
        ("probe_generated_at", "ruling10 d60c probe generated_at"),
        ("live_guard_generated_at", "live guard state generated_at"),
        ("guard_cycle_started_at", "live guard cycle_started_at"),
    ):
        raw = guard_stamp.get(key)
        parsed = _parse_ts(raw)
        if parsed is not None:
            return parsed, str(raw), basis
    return None, None, "missing RULING10 guard stamp timestamp"


def _slug_window_start_s(row: dict[str, Any]) -> float | None:
    slug = str(row.get("market_slug") or row.get("event_slug") or "")
    match = re.fullmatch(r"btc-updown-5m-(\d+)", slug)
    if match:
        return float(match.group(1))
    return _as_float(row.get("window_start_s"))


def _window_start_from_slug(value: Any) -> float | None:
    text = str(value or "")
    match = re.fullmatch(r"btc-updown-5m-(\d+)", text)
    if not match:
        return None
    return float(match.group(1))


def _current_window_start_from_ruling10(member: dict[str, Any], decision_ts: float | None) -> float | None:
    for row in _as_list(member.get("participation_rows")):
        if not isinstance(row, dict):
            continue
        window_start = _window_start_from_slug(row.get("market_slug"))
        if window_start is not None:
            return window_start
    if decision_ts is None:
        return None
    return float(int(float(decision_ts) // 300.0) * 300)


def _candidate_closed_window_starts(member: dict[str, Any], decision_ts: float | None) -> list[float]:
    current = _current_window_start_from_ruling10(member, decision_ts)
    if current is None:
        return []
    return [current - 300.0]


def _iter_tail_lines(path: Path, tail_bytes: int) -> Iterable[bytes]:
    with path.open("rb") as handle:
        try:
            handle.seek(0, 2)
            file_size = handle.tell()
            offset = max(0, file_size - max(0, int(tail_bytes)))
            handle.seek(offset)
            if offset > 0:
                handle.readline()
            yield from handle
        except OSError:
            return


def _scan_wallet_events(
    path: Path,
    *,
    tail_bytes: int,
    wallet: str,
    decision_ts: float,
) -> ScanResult:
    file_size = path.stat().st_size if path.exists() else 0
    offset = max(0, file_size - max(0, int(tail_bytes)))
    rows: list[dict[str, Any]] = []
    scanned_lines = 0
    parsed_rows = 0
    parse_errors = 0
    wallet_norm = _norm_wallet(wallet)
    if not path.exists():
        return ScanResult([], 0, 0, 0, file_size, offset)
    for raw_line in _iter_tail_lines(path, tail_bytes):
        scanned_lines += 1
        if wallet_norm.encode() not in raw_line.lower():
            continue
        try:
            row = json.loads(raw_line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        if not isinstance(row, dict):
            continue
        parsed_rows += 1
        if _norm_wallet(row.get("source_wallet")) != wallet_norm:
            continue
        generated_ts = _parse_ts(row.get("generated_at"))
        if generated_ts is not None and generated_ts > decision_ts:
            continue
        rows.append(row)
    return ScanResult(rows, scanned_lines, parsed_rows, parse_errors, file_size, offset)


def _identity_key(row: dict[str, Any]) -> tuple[Any, ...]:
    event_id = str(row.get("event_id") or "").strip()
    if event_id:
        return ("event_id", event_id)
    return (
        "semantic",
        _norm_wallet(row.get("source_wallet")),
        str(row.get("transaction_hash") or "").strip().lower(),
        str(row.get("token_id") or ""),
        str(row.get("outcome") or ""),
        str(row.get("action") or "").upper(),
        row.get("event_ts"),
        row.get("observed_ts"),
        row.get("price"),
    )


def _first_seen_row(existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    existing_generated = _parse_ts(existing.get("generated_at"))
    candidate_generated = _parse_ts(candidate.get("generated_at"))
    if existing_generated is None:
        return candidate
    if candidate_generated is None:
        return existing
    return candidate if candidate_generated < existing_generated else existing


def _dedupe_repeated_log_snapshots(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = _identity_key(row)
        existing = deduped.get(key)
        deduped[key] = row if existing is None else _first_seen_row(existing, row)
    return list(deduped.values())


def _raw_source(row: dict[str, Any]) -> str:
    raw = _as_dict(row.get("raw"))
    return str(raw.get("_walletCopySource") or row.get("source") or "").strip()


def _event_sort_key(row: dict[str, Any]) -> tuple[float, float, str]:
    return (
        float(_as_float(row.get("observed_ts")) or _as_float(row.get("event_ts")) or 0.0),
        float(_as_float(row.get("event_ts")) or 0.0),
        str(row.get("event_id") or ""),
    )


def _closed_candidate_rows(
    rows: list[dict[str, Any]],
    *,
    decision_ts: float,
    closed_window_starts: list[float],
    observed_lookback_s: float,
) -> tuple[list[dict[str, Any]], str]:
    def is_closed_btc5m(row: dict[str, Any]) -> bool:
        if str(row.get("action") or "").upper() != "BUY":
            return False
        window_start = _slug_window_start_s(row)
        if window_start is None:
            return False
        window_close = window_start + 300.0
        observed_ts = _as_float(row.get("observed_ts"))
        return observed_ts is not None and observed_ts <= decision_ts and decision_ts >= window_close

    deduped = _dedupe_repeated_log_snapshots(row for row in rows if is_closed_btc5m(row))
    preferred = [
        row
        for row in deduped
        if _slug_window_start_s(row) in set(closed_window_starts)
    ]
    if preferred:
        return sorted(preferred, key=_event_sort_key, reverse=True), "previous_ruling10_btc5m_window"

    decision_window = float(int(float(decision_ts) // 300.0) * 300)
    bounded = [
        row
        for row in deduped
        if (decision_window - 900.0) <= float(_slug_window_start_s(row) or 0.0) <= (decision_window - 300.0)
    ]
    if bounded:
        return sorted(bounded, key=_event_sort_key, reverse=True), "bounded_prior_15m_btc5m_windows"

    observed_floor = decision_ts - max(0.0, float(observed_lookback_s))
    recent = [
        row
        for row in deduped
        if float(_as_float(row.get("observed_ts")) or 0.0) >= observed_floor
    ]
    return sorted(recent, key=_event_sort_key, reverse=True), "recent_observed_market_closed_rows"


def _classify_row(row: dict[str, Any], *, decision_ts: float) -> dict[str, Any]:
    window_start = _slug_window_start_s(row)
    window_close = None if window_start is None else window_start + 300.0
    observed_ts = _as_float(row.get("observed_ts"))
    event_ts = _as_float(row.get("event_ts"))
    generated_ts = _parse_ts(row.get("generated_at"))
    if observed_ts is not None and window_close is not None and observed_ts <= window_close and decision_ts > window_close:
        latency_class = "A"
        reason = "event_received_before_window_close_but_decided_after"
    elif observed_ts is not None and window_close is not None and observed_ts > window_close:
        latency_class = "B"
        reason = "event_received_after_window_close"
    else:
        latency_class = "UNCLASSIFIED"
        reason = "missing_timing_or_not_market_closed_at_decision"
    return {
        "event_id": row.get("event_id"),
        "source": row.get("source"),
        "raw_source": _raw_source(row),
        "transaction_hash": row.get("transaction_hash"),
        "market_slug": row.get("market_slug"),
        "outcome": row.get("outcome"),
        "action": row.get("action"),
        "price": row.get("price"),
        "usdc_size": row.get("usdc_size"),
        "event_ts": event_ts,
        "event_ts_iso": _iso_from_ts(event_ts),
        "observed_ts": observed_ts,
        "received_ts": observed_ts,
        "received_ts_iso": _iso_from_ts(observed_ts),
        "generated_at": row.get("generated_at"),
        "generated_ts": generated_ts,
        "window_start_s": window_start,
        "window_close_ts": window_close,
        "window_close_ts_iso": _iso_from_ts(window_close),
        "decided_ts": decision_ts,
        "decided_ts_iso": _iso_from_ts(decision_ts),
        "received_minus_close_s": (
            None if observed_ts is None or window_close is None else round(float(observed_ts) - float(window_close), 6)
        ),
        "decision_minus_close_s": (
            None if window_close is None else round(float(decision_ts) - float(window_close), 6)
        ),
        "decision_minus_received_s": (
            None if observed_ts is None else round(float(decision_ts) - float(observed_ts), 6)
        ),
        "latency_class": latency_class,
        "classification_reason": reason,
    }


def _majority_verdict(class_counts: Counter[str], recovered_count: int) -> str:
    if recovered_count <= 0:
        return "NO_ROWS_RECOVERED"
    a_count = int(class_counts.get("A") or 0)
    b_count = int(class_counts.get("B") or 0)
    if a_count > b_count:
        return "MOSTLY_A_DECISION_LATENCY"
    if b_count > a_count:
        return "MOSTLY_B_SOURCE_LATENCY"
    return "MIXED_OR_TIED"


def build_report(
    *,
    root: Path = ROOT,
    ruling10_probe_path: str = DEFAULT_RULING10_PROBE,
    wallet_events_path: str = DEFAULT_WALLET_EVENTS,
    tail_bytes: int = DEFAULT_TAIL_BYTES,
    observed_lookback_s: float = 1800.0,
) -> dict[str, Any]:
    ruling10_path = _resolve(root, ruling10_probe_path)
    wallet_events = _resolve(root, wallet_events_path)
    ruling10 = load_json(ruling10_path, default={})
    ruling10 = ruling10 if isinstance(ruling10, dict) else {}
    member = _ruling10_member(ruling10, "d60c")
    target_count = _target_market_closed_count(member)
    decision_ts, decision_raw, decision_basis = _decision_ts(member)
    closed_window_starts = _candidate_closed_window_starts(member, decision_ts)
    scan = ScanResult([], 0, 0, 0, wallet_events.stat().st_size if wallet_events.exists() else 0, 0)
    selected: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    selection_basis = "missing_decision_ts"
    if decision_ts is not None and target_count > 0:
        scan = _scan_wallet_events(
            wallet_events,
            tail_bytes=int(tail_bytes),
            wallet=D60C_WALLET,
            decision_ts=float(decision_ts),
        )
        candidate_rows, selection_basis = _closed_candidate_rows(
            scan.rows,
            decision_ts=float(decision_ts),
            closed_window_starts=closed_window_starts,
            observed_lookback_s=float(observed_lookback_s),
        )
        selected = candidate_rows[:target_count]
    classified = [_classify_row(row, decision_ts=float(decision_ts or 0.0)) for row in selected]
    unselected = [_classify_row(row, decision_ts=float(decision_ts or 0.0)) for row in candidate_rows[target_count:target_count + 10]]
    class_counts: Counter[str] = Counter(str(row.get("latency_class") or "UNCLASSIFIED") for row in classified)
    recovered_count = len(classified)
    return {
        "schema_version": 1,
        "kind": "d60c_latency_attribution",
        "flow_stage": "LIVE/MEASURE",
        "ruling_id": RULING_ID,
        "generated_at": _iso_now(),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "source_wallet": D60C_WALLET,
        "candidate_id": D60C_CANDIDATE_ID,
        "ruling10_probe_path": str(ruling10_path),
        "ruling10_probe_generated_at": ruling10.get("generated_at"),
        "target": {
            "source": "RULING10 d60c all_abstain_reason_counts.market_closed_now",
            "market_closed_count": target_count,
            "decision_ts": decision_ts,
            "decision_ts_iso": _iso_from_ts(decision_ts),
            "decision_raw": decision_raw,
            "decision_basis": decision_basis,
            "closed_window_starts": closed_window_starts,
        },
        "scan": {
            "wallet_events_path": str(wallet_events),
            "tail_bytes": int(tail_bytes),
            "file_size_bytes": scan.file_size_bytes,
            "scan_offset_bytes": scan.scan_offset_bytes,
            "scanned_lines": scan.scanned_lines,
            "parsed_wallet_rows": scan.parsed_rows,
            "wallet_rows_before_decision": len(scan.rows),
            "parse_errors": scan.parse_errors,
            "selection_basis": selection_basis,
            "candidate_market_closed_rows": len(candidate_rows),
            "observed_lookback_s": float(observed_lookback_s),
            "dedupe_rule": "repeated wallet-event log snapshots are collapsed by event_id before target-count selection",
            "selection_rule": "select the target-count most recent unique BUY BTC-5m market_closed rows from the prior RULING10 window",
        },
        "summary": {
            "target_market_closed_count": target_count,
            "recovered_market_closed_count": recovered_count,
            "exact_target_count_match": recovered_count == target_count,
            "class_counts": dict(sorted(class_counts.items())),
            "majority_verdict": _majority_verdict(class_counts, recovered_count),
            "class_a_definition": "event received before window close but probe decided after close",
            "class_b_definition": "event received after window close",
        },
        "events": classified,
        "unselected_candidate_events": unselected,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--ruling10-probe", default=DEFAULT_RULING10_PROBE)
    parser.add_argument("--wallet-events", default=DEFAULT_WALLET_EVENTS)
    parser.add_argument("--tail-bytes", type=int, default=DEFAULT_TAIL_BYTES)
    parser.add_argument("--observed-lookback-s", type=float, default=1800.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_report(
        ruling10_probe_path=args.ruling10_probe,
        wallet_events_path=args.wallet_events,
        tail_bytes=int(args.tail_bytes),
        observed_lookback_s=float(args.observed_lookback_s),
    )
    output = _resolve(ROOT, args.output)
    atomic_write_json(output, payload)
    summary = payload["summary"]
    print(
        {
            "output": str(output),
            "target": summary["target_market_closed_count"],
            "recovered": summary["recovered_market_closed_count"],
            "class_counts": summary["class_counts"],
            "majority_verdict": summary["majority_verdict"],
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
