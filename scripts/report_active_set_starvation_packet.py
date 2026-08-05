#!/usr/bin/env python3
"""Build the active-set starvation packet requested by Fable.

The report is measure-only: it reads guard events and current state, then
summarizes per enabled member whether recent evidence is source quiet,
late-window dominated, or eligible for a rotation proposal.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_CURRENT_EVENTS = ROOT / "data/research/wallet_copy_live_guard_events.jsonl"
DEFAULT_LOG_ARCHIVES = ROOT / "data/research/log_archives"
DEFAULT_WATCH_RANKING = ROOT / "data/research/watch_tier_expansion_ranking_latest.json"
DEFAULT_QUEUE = ROOT / "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_LIVE_LEDGER_STATE = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/active_set_starvation_packet_latest.json"
LATE_TAGS = {"window_time_gte_180s", "inventory_late_window_guard"}
RECOVERABLE_INVENTORY_REASONS = {
    "inventory_best_ask_above_vwap_plus_buffer",
    "inventory_best_ask_missing",
}


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _round_float(value: Any) -> float | None:
    parsed = _as_float(value)
    return round(parsed, 6) if parsed is not None else None


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * pct
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return round(ordered[lo] * (1 - weight) + ordered[hi] * weight, 6)


def _btc5m_window_start_s(market_slug: Any) -> float | None:
    parts = str(market_slug or "").rsplit("-", 1)
    if len(parts) != 2:
        return None
    try:
        return float(parts[1])
    except ValueError:
        return None


def _jsonl_paths(current_events: Path, archive_dir: Path) -> list[Path]:
    paths: list[Path] = []
    if archive_dir.exists():
        paths.extend(sorted(archive_dir.glob("wallet_copy_live_guard_events_*.jsonl.gz")))
        paths.extend(sorted(archive_dir.glob("wallet_copy_live_guard_events_*.jsonl")))
    if current_events.exists():
        paths.append(current_events)
    return paths


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    try:
        with opener(path, "rt", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    yield row
    except OSError:
        return


def _row_ts(row: dict[str, Any]) -> datetime | None:
    for key in ("generated_at", "checked_at", "ts", "updated_at"):
        dt = _parse_ts(row.get(key))
        if dt:
            return dt
    return None


def _profit_latency(row: dict[str, Any]) -> dict[str, Any]:
    live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
    summary = live_execution.get("profit_latency_suppression")
    if isinstance(summary, dict):
        return summary
    nested = live_execution.get("candidate_intent_summary")
    if isinstance(nested, dict) and isinstance(nested.get("profit_latency_suppression"), dict):
        return nested["profit_latency_suppression"]
    summary = row.get("profit_latency_suppression")
    return summary if isinstance(summary, dict) else {}


def _toxicity(row: dict[str, Any]) -> dict[str, Any]:
    live_execution = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
    summary = live_execution.get("toxicity_protection")
    return summary if isinstance(summary, dict) else {}


def _taxonomy(sample: dict[str, Any]) -> set[str]:
    out = {str(tag) for tag in sample.get("taxonomy_tags") or []}
    for key in ("taxonomy", "reject_reason"):
        value = sample.get(key)
        if value:
            out.update(part for part in str(value).split("+") if part)
    return out


def _active_members(guard: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    members = runtime.get("members") if isinstance(runtime.get("members"), list) else []
    enabled = [m for m in members if isinstance(m, dict) and m.get("enabled", True) and _norm_wallet(m.get("source_wallet"))]
    return enabled, runtime


def _freshest_source_lag_by_wallet(guard: dict[str, Any]) -> dict[str, float]:
    poller = guard.get("active_set_dataapi_poller") if isinstance(guard.get("active_set_dataapi_poller"), dict) else {}
    fetch_meta = poller.get("fetch_meta") if isinstance(poller.get("fetch_meta"), dict) else {}
    out: dict[str, float] = {}
    for wallet, meta in fetch_meta.items():
        normalized = _norm_wallet(wallet)
        if not normalized or not isinstance(meta, dict):
            continue
        by_source = (
            meta.get("freshest_buy_lag_s_by_source")
            if isinstance(meta.get("freshest_buy_lag_s_by_source"), dict)
            else {}
        )
        values = [float(value) for value in by_source.values() if _as_float(value) is not None]
        if values:
            out[normalized] = min(values)
    return out


def _sample_event_ts(sample: dict[str, Any]) -> datetime | None:
    value = _as_float(sample.get("event_ts"))
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


def _add_latency_sample(bucket: dict[str, Any], sample: dict[str, Any]) -> None:
    source_lag, window_offset = _latency_split(sample)
    if source_lag is not None:
        bucket["source_to_detection_lag_s"].append(source_lag)
    if window_offset is not None:
        bucket["window_open_to_source_trade_s"].append(window_offset)


def _latency_split(sample: dict[str, Any]) -> tuple[float | None, float | None]:
    event_ts = _as_float(sample.get("event_ts"))
    detected_ts = (
        _as_float(sample.get("dataapi_first_seen_ts"))
        or _as_float(sample.get("detection_observed_ts"))
        or _as_float(sample.get("observed_ts"))
    )
    source_lag = None
    if event_ts is not None and detected_ts is not None and detected_ts >= event_ts:
        source_lag = round(detected_ts - event_ts, 6)
    window_offset = None
    window_start = _btc5m_window_start_s(sample.get("market_slug"))
    if event_ts is not None and window_start is not None and event_ts >= window_start:
        window_offset = round(event_ts - window_start, 6)
    return source_lag, window_offset


def _intent_id(sample: dict[str, Any]) -> str:
    return str(sample.get("intent_id") or "").strip()


def _source_fingerprint(wallet: str, sample: dict[str, Any]) -> str:
    event_ts = _as_float(sample.get("event_ts"))
    event_key = f"{event_ts:.6f}" if event_ts is not None else ""
    return "|".join([wallet, event_key, str(sample.get("market_slug") or "")])


def _intent_key(wallet: str, sample: dict[str, Any]) -> str:
    intent_id = _intent_id(sample)
    if intent_id:
        return f"intent:{intent_id}"
    return f"source:{_source_fingerprint(wallet, sample)}"


def _sample_taxonomy(sample: dict[str, Any], sample_kind: str) -> str:
    tags = sorted(_taxonomy(sample))
    if tags:
        return "+".join(tags)
    return "profit_latency_pass" if sample_kind == "passed" else sample_kind


def _is_inventory_reject_class(value: Any) -> bool:
    text = str(value or "")
    if text.startswith("window:"):
        text = text.split(":", 1)[1]
    return text.startswith("inventory_")


def _timeline_entry(
    row: dict[str, Any],
    ts: datetime,
    sample: dict[str, Any],
    sample_kind: str,
    *,
    taxonomy_override: str | None = None,
) -> dict[str, Any]:
    live = row.get("live_execution") if isinstance(row.get("live_execution"), dict) else {}
    drought = live.get("drought_funnel") if isinstance(live.get("drought_funnel"), dict) else {}
    toxicity = live.get("toxicity_protection") if isinstance(live.get("toxicity_protection"), dict) else {}
    sample_wallet = _norm_wallet(sample.get("source_wallet"))
    selected = live.get("selected_candidate") if isinstance(live.get("selected_candidate"), dict) else {}
    selected_wallet = _norm_wallet(selected.get("source_wallet"))
    selected_candidate_id = selected.get("candidate_id")
    active_set_runtime = row.get("active_set_runtime") if isinstance(row.get("active_set_runtime"), dict) else {}
    active_set = row.get("active_set") if isinstance(row.get("active_set"), dict) else {}
    set_generation_id = (
        row.get("set_generation_id")
        or live.get("set_generation_id")
        or active_set_runtime.get("set_generation_id")
        or active_set.get("set_generation_id")
    )
    if not selected_wallet:
        for member in active_set.get("members") if isinstance(active_set.get("members"), list) else []:
            if isinstance(member, dict) and member.get("is_current_cycle_member"):
                selected_wallet = _norm_wallet(member.get("source_wallet"))
                selected_candidate_id = selected_candidate_id or member.get("candidate_id")
                break
    source_member: dict[str, Any] = {}
    source_member_container = ""
    for container_key in ("active_set", "active_set_runtime"):
        container = row.get(container_key) if isinstance(row.get(container_key), dict) else {}
        for member in container.get("members") if isinstance(container.get("members"), list) else []:
            if isinstance(member, dict) and _norm_wallet(member.get("source_wallet")) == sample_wallet:
                source_member = member
                source_member_container = container_key
                break
        if source_member:
            break
    sample_generation = sample.get("set_generation_id") or sample.get("active_set_generation_id")
    source_member_generation = (
        source_member.get("set_generation_id")
        or source_member.get("active_set_generation_id")
        or sample_generation
    )
    current_generation = source_member.get("current_set_generation")
    if current_generation is None:
        current_generation = source_member.get("current_generation")
    if current_generation is None and source_member_generation and set_generation_id:
        current_generation = str(source_member_generation) == str(set_generation_id)
    if current_generation is None and source_member_container == "active_set_runtime" and set_generation_id:
        current_generation = True
    execute_live = row.get("execute_live")
    if execute_live is None:
        execute_live = live.get("execute_live")
    live_orders_allowed = row.get("live_orders_allowed")
    if live_orders_allowed is None:
        live_orders_allowed = live.get("live_orders_allowed")
    return {
        "ts": ts.isoformat(),
        "sample_kind": sample_kind,
        "taxonomy": taxonomy_override or _sample_taxonomy(sample, sample_kind),
        "signal_age_s": _round_float(sample.get("signal_age_s")),
        "window_time_s": _round_float(sample.get("window_time_s")),
        "best_ask": _round_float(sample.get("best_ask")),
        "source_inventory_vwap": _round_float(sample.get("source_inventory_vwap")),
        "target_usd_at_vwap": _round_float(sample.get("target_usd_at_vwap")),
        "guard_sized_copy_usd": _round_float(sample.get("guard_sized_copy_usd")),
        "live_execution_status": live.get("status"),
        "orders_submitted": live.get("orders_submitted"),
        "fresh_candidate_intents": live.get("fresh_candidate_intents"),
        "new_live_candidate_intents": live.get("new_live_candidate_intents"),
        "fresh_after_toxicity_protection": drought.get("fresh_after_toxicity_protection"),
        "reject_taxonomy_counts": drought.get("reject_taxonomy_counts") if isinstance(drought.get("reject_taxonomy_counts"), dict) else {},
        "toxicity_blocked_intents": toxicity.get("blocked_intents"),
        "execute_live": execute_live,
        "live_orders_allowed": live_orders_allowed,
        "operator_gate_blockers": live.get("operator_gate_blockers") if isinstance(live.get("operator_gate_blockers"), list) else [],
        "proof_blockers": live.get("proof_blockers") if isinstance(live.get("proof_blockers"), list) else [],
        "intent_blockers": live.get("intent_blockers") if isinstance(live.get("intent_blockers"), list) else [],
        "selected_candidate_id": selected_candidate_id,
        "selected_source_wallet": selected_wallet or None,
        "source_is_selected_member": sample_wallet == selected_wallet if sample_wallet and selected_wallet else None,
        "set_generation_id": set_generation_id,
        "current_generation": current_generation,
        "source_member_candidate_id": source_member.get("candidate_id"),
        "source_member_status": source_member.get("status"),
        "source_member_is_current_cycle": source_member.get("is_current_cycle_member"),
    }


def _pass_sample_ref(sample: dict[str, Any], wallet: str) -> dict[str, Any]:
    return {
        "intent_id": _intent_id(sample),
        "source_wallet": wallet,
        "market_slug": sample.get("market_slug"),
        "condition_id": sample.get("condition_id"),
        "outcome": sample.get("outcome"),
        "limit_price": _round_float(sample.get("limit_price")),
        "copy_size_usd": _round_float(sample.get("copy_size_usd")),
        "event_ts": _round_float(sample.get("event_ts")),
        "observed_ts": _round_float(sample.get("observed_ts")),
        "dataapi_first_seen_ts": _round_float(sample.get("dataapi_first_seen_ts")),
    }


def _record_intent(
    records: dict[str, dict[str, Any]],
    source_intent_ids: dict[str, set[str]],
    row: dict[str, Any],
    ts: datetime,
    sample: dict[str, Any],
    sample_kind: str,
    wallet: str,
) -> None:
    key = _intent_key(wallet, sample)
    intent_id = _intent_id(sample)
    source_key = _source_fingerprint(wallet, sample)
    if intent_id:
        source_intent_ids[source_key].add(intent_id)
    taxonomy_tags = _taxonomy(sample)
    is_late = bool(taxonomy_tags & LATE_TAGS)
    base_taxonomy = _sample_taxonomy(sample, sample_kind)
    record = records.setdefault(
        key,
        {
            "key": key,
            "intent_id": intent_id,
            "source_key": source_key,
            "source_wallet": wallet,
            "first_seen_ts": ts,
            "last_seen_ts": ts,
            "first_class": sample_kind,
            "first_taxonomy": base_taxonomy,
            "first_is_late": is_late,
            "first_event_ts": _sample_event_ts(sample),
            "first_market_slug": sample.get("market_slug"),
            "first_pass_sample": None,
            "latest_source_trade_ts": _sample_event_ts(sample),
            "occurrence_count": 0,
            "classes_seen": set(),
            "taxonomies_seen": set(),
            "passed_seen": False,
            "late_seen": False,
            "toxicity_seen": False,
            "terminal_toxicity_seen": False,
            "terminal_toxicity_ts": None,
            "terminal_resnapshot_seen": False,
            "terminal_resnapshot_occurrences": 0,
            "first_late_source_to_detection_lag_s": None,
            "first_late_window_open_to_source_trade_s": None,
            "timeline": [],
        },
    )
    terminal_ts = record.get("terminal_toxicity_ts")
    terminal_resnapshot = (
        sample_kind == "profit_filtered"
        and is_late
        and isinstance(terminal_ts, datetime)
        and ts >= terminal_ts
    )
    taxonomy = "resnapshot_after_terminal" if terminal_resnapshot else base_taxonomy
    if ts < record["first_seen_ts"]:
        record["first_seen_ts"] = ts
        record["first_class"] = sample_kind
        record["first_taxonomy"] = taxonomy
        record["first_is_late"] = is_late and not terminal_resnapshot
        record["first_event_ts"] = _sample_event_ts(sample)
        record["first_market_slug"] = sample.get("market_slug")
    record["last_seen_ts"] = max(record["last_seen_ts"], ts)
    source_ts = _sample_event_ts(sample)
    if source_ts:
        previous = record.get("latest_source_trade_ts")
        record["latest_source_trade_ts"] = max([v for v in (previous, source_ts) if v is not None], default=source_ts)
    record["occurrence_count"] += 1
    record["classes_seen"].add(sample_kind)
    record["taxonomies_seen"].add(taxonomy)
    if sample_kind == "passed":
        record["passed_seen"] = True
        if not isinstance(record.get("first_pass_sample"), dict):
            record["first_pass_sample"] = _pass_sample_ref(sample, wallet)
    if sample_kind == "toxicity_filtered":
        record["toxicity_seen"] = True
        if taxonomy == "toxicity_protection":
            record["terminal_toxicity_seen"] = True
            current_terminal = record.get("terminal_toxicity_ts")
            if not isinstance(current_terminal, datetime) or ts < current_terminal:
                record["terminal_toxicity_ts"] = ts
    if terminal_resnapshot:
        record["terminal_resnapshot_seen"] = True
        record["terminal_resnapshot_occurrences"] += 1
    if is_late and not terminal_resnapshot:
        record["late_seen"] = True
        if record["first_late_source_to_detection_lag_s"] is None:
            source_lag, window_offset = _latency_split(sample)
            record["first_late_source_to_detection_lag_s"] = source_lag
            record["first_late_window_open_to_source_trade_s"] = window_offset
    record["timeline"].append(
        _timeline_entry(row, ts, sample, sample_kind, taxonomy_override=taxonomy if terminal_resnapshot else None)
    )


def _pass_to_late_trace(record: dict[str, Any], source_intent_ids: dict[str, set[str]]) -> dict[str, Any]:
    timeline = sorted(record.get("timeline") or [], key=lambda item: item.get("ts") or "")
    compact_timeline: list[dict[str, Any]] = []
    seen_entries: set[tuple[Any, Any, Any, Any]] = set()
    for entry in timeline:
        key = (entry.get("ts"), entry.get("sample_kind"), entry.get("taxonomy"), entry.get("window_time_s"))
        if key in seen_entries:
            continue
        seen_entries.add(key)
        compact_timeline.append(entry)
    first_pass = next((entry for entry in timeline if entry.get("sample_kind") == "passed"), {})
    same_row_toxicity = any(
        entry.get("sample_kind") == "toxicity_filtered" and entry.get("ts") == first_pass.get("ts") for entry in timeline
    )
    source_ids = source_intent_ids.get(str(record.get("source_key") or ""), set())
    reject_counts = first_pass.get("reject_taxonomy_counts") if isinstance(first_pass.get("reject_taxonomy_counts"), dict) else {}
    if same_row_toxicity or reject_counts.get("toxicity_protection"):
        mechanism = "downstream_toxicity_gate_then_resnapshot_late"
    elif len(source_ids) > 1:
        mechanism = "intent_reemit_by_source_event"
    elif (first_pass.get("orders_submitted") or 0) > 0:
        mechanism = "submitted_or_attempted_then_late_resnapshot"
    elif (first_pass.get("fresh_candidate_intents") or 0) > 0 and (first_pass.get("new_live_candidate_intents") or 0) == 0:
        mechanism = "downstream_gate_removed_before_submission"
    else:
        mechanism = "guard_not_submitting_passed_intent"
    return {
        "intent_id": record.get("intent_id"),
        "source_wallet": record.get("source_wallet"),
        "market_slug": record.get("first_market_slug"),
        "first_event_ts": record.get("first_event_ts").isoformat() if isinstance(record.get("first_event_ts"), datetime) else None,
        "occurrence_count": record.get("occurrence_count"),
        "distinct_intent_ids_for_source_event": len(source_ids),
        "mechanism": mechanism,
        "timeline": compact_timeline[:6],
    }


def _first_pass_timeline_entry(record: dict[str, Any]) -> dict[str, Any]:
    timeline = sorted(record.get("timeline") or [], key=lambda item: item.get("ts") or "")
    return next((entry for entry in timeline if entry.get("sample_kind") == "passed"), {})


def _same_cycle_toxicity(record: dict[str, Any], first_pass: dict[str, Any]) -> bool:
    first_ts = first_pass.get("ts")
    timeline = record.get("timeline") if isinstance(record.get("timeline"), list) else []
    return any(
        entry.get("sample_kind") == "toxicity_filtered"
        and entry.get("taxonomy") == "toxicity_protection"
        and entry.get("ts") == first_ts
        for entry in timeline
    )


def _dominant_reject_class(first_pass: dict[str, Any]) -> tuple[str, int]:
    counts = first_pass.get("reject_taxonomy_counts")
    if not isinstance(counts, dict) or not counts:
        return "no_reject_taxonomy", 0
    dominant, count = max(
        ((str(key), int(value or 0)) for key, value in counts.items()),
        key=lambda item: (item[1], item[0]),
    )
    return dominant, count


def _norm_reject_reason(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("window:"):
        text = text.split(":", 1)[1]
    return text or "unknown"


def _inventory_reject_counts(first_pass: dict[str, Any]) -> Counter[str]:
    counts = first_pass.get("reject_taxonomy_counts")
    out: Counter[str] = Counter()
    if not isinstance(counts, dict):
        return out
    for reason, count in counts.items():
        normalized = _norm_reject_reason(reason)
        if normalized.startswith("inventory_"):
            out[normalized] += int(count or 0)
    return out


def _first_terminal_gate(record: dict[str, Any], fallback_reason: str) -> str:
    first_pass_seen = False
    for entry in sorted(record.get("timeline") or [], key=lambda item: item.get("ts") or ""):
        if not isinstance(entry, dict):
            continue
        if entry.get("sample_kind") == "passed":
            first_pass_seen = True
            counts = _inventory_reject_counts(entry)
            if counts:
                return counts.most_common(1)[0][0]
            continue
        if not first_pass_seen:
            continue
        taxonomy = _norm_reject_reason(entry.get("taxonomy"))
        if taxonomy and taxonomy not in {"profit_latency_pass", "passed", "unknown"}:
            return taxonomy
    return fallback_reason


def _c5b_decision_status(summary: dict[str, Any], min_n: int) -> str:
    resolved_n = int(summary.get("resolved") or 0)
    roi = _as_float(summary.get("roi_pct"))
    if resolved_n < min_n:
        return "REAL_SUBMIT_SAMPLE_INSUFFICIENT"
    if roi is not None and roi > 0.0:
        return "REAL_SUBMIT_POSITIVE_SHADOW_EV_REQUIRES_FABLE_RULING"
    return "REAL_SUBMIT_GATE_CONFIRMED_NONPOSITIVE"


def _dry_run_starvation_reason(record: dict[str, Any]) -> str:
    first_pass = _first_pass_timeline_entry(record)
    if first_pass.get("source_is_selected_member") is False:
        return "active_set_non_current_member_diagnostic_only"
    if first_pass.get("execute_live") is False or first_pass.get("live_orders_allowed") is False:
        return "row_not_live_armed_for_submission"
    if first_pass.get("operator_gate_blockers"):
        return "operator_gate_blocker_before_submit"
    if first_pass.get("proof_blockers"):
        return "proof_gate_blocker_before_submit"
    if _same_cycle_toxicity(record, first_pass) and first_pass.get("fresh_after_toxicity_protection") == 0:
        return "terminal_toxicity_zeroed_fresh_intent_in_dry_run_cycle"
    if first_pass.get("intent_blockers"):
        return "intent_blocker_before_submit"
    if (first_pass.get("orders_submitted") or 0) > 0:
        return "submitted_elsewhere_in_cycle_but_first_pass_row_marked_dry_run"
    if (first_pass.get("fresh_candidate_intents") or 0) > 0 and (first_pass.get("new_live_candidate_intents") or 0) == 0:
        return "fresh_candidate_not_new_to_live_submit_path"
    if first_pass.get("fresh_after_toxicity_protection") == 0:
        return "downstream_gate_zeroed_fresh_after_toxicity_stage"
    return "dry_run_status_no_real_submit_path_evaluation"


def _dry_run_starvation_trace(record: dict[str, Any], source_intent_ids: dict[str, set[str]]) -> dict[str, Any]:
    first_pass = _first_pass_timeline_entry(record)
    dominant, dominant_count = _dominant_reject_class(first_pass)
    trace = _pass_to_late_trace(record, source_intent_ids)
    trace.update(
        {
            "dominant_reject_class": dominant,
            "dominant_reject_count": dominant_count,
            "why_pass_reached_dry_run_not_submit": _dry_run_starvation_reason(record),
            "first_pass_path_fields": {
                "live_execution_status": first_pass.get("live_execution_status"),
                "execute_live": first_pass.get("execute_live"),
                "live_orders_allowed": first_pass.get("live_orders_allowed"),
                "orders_submitted": first_pass.get("orders_submitted"),
                "fresh_candidate_intents": first_pass.get("fresh_candidate_intents"),
                "new_live_candidate_intents": first_pass.get("new_live_candidate_intents"),
                "fresh_after_toxicity_protection": first_pass.get("fresh_after_toxicity_protection"),
                "source_is_selected_member": first_pass.get("source_is_selected_member"),
                "selected_candidate_id": first_pass.get("selected_candidate_id"),
                "selected_source_wallet": first_pass.get("selected_source_wallet"),
                "source_member_candidate_id": first_pass.get("source_member_candidate_id"),
                "source_member_is_current_cycle": first_pass.get("source_member_is_current_cycle"),
                "operator_gate_blockers": first_pass.get("operator_gate_blockers"),
                "proof_blockers": first_pass.get("proof_blockers"),
                "intent_blockers": first_pass.get("intent_blockers"),
            },
        }
    )
    return trace


def _dry_run_starvation_packet(
    records: list[dict[str, Any]],
    source_intent_ids: dict[str, set[str]],
) -> dict[str, Any]:
    reason_counts = Counter(_dry_run_starvation_reason(record) for record in records)
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        dominant, _ = _dominant_reject_class(_first_pass_timeline_entry(record))
        buckets[dominant].append(record)
    by_class: list[dict[str, Any]] = []
    for dominant, bucket in sorted(buckets.items(), key=lambda item: (-len(item[1]), item[0])):
        representative = max(
            bucket,
            key=lambda record: (
                int(record.get("occurrence_count") or 0),
                str(record.get("first_event_ts") or ""),
            ),
        )
        by_class.append(
            {
                "dominant_reject_class": dominant,
                "record_count": len(bucket),
                "reason_counts": dict(sorted(Counter(_dry_run_starvation_reason(record) for record in bucket).items())),
                "sample_trace": _dry_run_starvation_trace(representative, source_intent_ids),
            }
        )
    return {
        "record_count": len(records),
        "label_explanation": (
            "LIVE_ARMED_DRY_RUN is treated as diagnostic-only path evidence in C5b: the row can still "
            "show execute_live=true, live_orders_allowed=true, and source_is_selected_member=true because "
            "the guard was globally armed for the cycle, but the same-cycle terminal toxicity gate reduced "
            "fresh_after_toxicity_protection to 0 before the real submit path had a C5b-eligible order to score."
        ),
        "reason_counts": dict(sorted(reason_counts.items())),
        "by_dominant_reject_class": by_class,
    }


def _inventory_skip_lifecycle_trace(
    records: list[dict[str, Any]],
    source_intent_ids: dict[str, set[str]],
    *,
    current_generation_id: str | None = None,
) -> dict[str, Any]:
    inventory_records: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    wallet_reason_counts: dict[str, Counter[str]] = defaultdict(Counter)
    recoverable_intent_estimate = 0
    for record in records:
        if record.get("first_class") != "passed":
            continue
        first_pass = _first_pass_timeline_entry(record)
        dominant, dominant_count = _dominant_reject_class(first_pass)
        counts = _inventory_reject_counts(first_pass)
        if not _is_inventory_reject_class(dominant) and not counts:
            continue
        reason = _norm_reject_reason(dominant)
        if not reason.startswith("inventory_") and counts:
            reason = counts.most_common(1)[0][0]
        wallet = str(record.get("source_wallet") or "")
        if not counts:
            counts[reason] += max(1, int(dominant_count or 0))
        count = max(counts.values(), default=max(1, int(dominant_count or 0)))
        for counted_reason, counted in counts.items():
            reason_counts[counted_reason] += counted
            wallet_reason_counts[wallet][counted_reason] += counted
        if (
            first_pass.get("fresh_candidate_intents", 0) > 0
            and first_pass.get("new_live_candidate_intents", 0) == 0
            and first_pass.get("orders_submitted", 0) == 0
            and reason in RECOVERABLE_INVENTORY_REASONS
        ):
            recoverable_intent_estimate += 1
        trace = _pass_to_late_trace(record, source_intent_ids)
        trace.update(
            {
                "inventory_skip_reason": reason,
                "first_terminal_gate": _first_terminal_gate(record, reason),
                "set_generation_id": first_pass.get("set_generation_id"),
                "current_generation": (
                    str(first_pass.get("set_generation_id")) == str(current_generation_id)
                    if current_generation_id and first_pass.get("set_generation_id")
                    else first_pass.get("current_generation")
                ),
                "inventory_reject_counts": dict(sorted(counts.items())),
                "dominant_inventory_reject_class": reason,
                "dominant_reject_count": count,
                "recoverable_inventory_candidate": reason in RECOVERABLE_INVENTORY_REASONS,
                "first_pass_path_fields": {
                    "orders_submitted": first_pass.get("orders_submitted"),
                    "fresh_candidate_intents": first_pass.get("fresh_candidate_intents"),
                    "new_live_candidate_intents": first_pass.get("new_live_candidate_intents"),
                    "fresh_after_toxicity_protection": first_pass.get("fresh_after_toxicity_protection"),
                    "best_ask": first_pass.get("best_ask"),
                    "source_inventory_vwap": first_pass.get("source_inventory_vwap"),
                    "target_usd_at_vwap": first_pass.get("target_usd_at_vwap"),
                    "guard_sized_copy_usd": first_pass.get("guard_sized_copy_usd"),
                },
            }
        )
        inventory_records.append(trace)
    sample_traces = sorted(
        inventory_records,
        key=lambda trace: (
            int(trace.get("dominant_reject_count") or 0),
            int(trace.get("occurrence_count") or 0),
            str(trace.get("first_event_ts") or ""),
        ),
        reverse=True,
    )[:6]
    return {
        "schema_version": 1,
        "measure_only": True,
        "source": "first-pass denied_records with dominant inventory reject taxonomy",
        "record_count": len(inventory_records),
        "skip_reason_counts_24h": dict(sorted(reason_counts.items())),
        "skip_reason_counts_24h_by_wallet": {
            wallet: dict(sorted(counter.items())) for wallet, counter in sorted(wallet_reason_counts.items())
        },
        "skip_reason_counts_by_wallet_24h": {
            wallet: dict(sorted(counter.items())) for wallet, counter in sorted(wallet_reason_counts.items())
        },
        "recoverable_intent_estimate": recoverable_intent_estimate,
        "sample_traces": sample_traces,
        "interpretation": (
            "Counts only first-pass records whose dominant reject class is inventory_*; "
            "recoverable estimates require fresh_candidate_intents>0, new_live_candidate_intents=0, "
            "orders_submitted=0, and ask-missing/ask-above inventory reasons."
        ),
    }


def _shadow_order_from_record(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    sample = record.get("first_pass_sample") if isinstance(record.get("first_pass_sample"), dict) else {}
    price = _as_float(sample.get("limit_price"))
    cost = _as_float(sample.get("copy_size_usd"))
    market_slug = str(sample.get("market_slug") or "")
    outcome = str(sample.get("outcome") or "")
    if price is None or price <= 0:
        return None, "missing_pass_price"
    if cost is None or cost <= 0:
        return None, "missing_pass_copy_size"
    if not market_slug or not outcome:
        return None, "missing_market_or_outcome"
    return (
        {
            "order_id": f"toxicity_shadow:{record.get('intent_id') or record.get('key')}",
            "intent_id": record.get("intent_id"),
            "source_wallet": record.get("source_wallet"),
            "condition_id": sample.get("condition_id") or "",
            "market_slug": market_slug,
            "outcome": outcome,
            "final_status": "FILLED",
            "status": "FILLED",
            "limit_price": price,
            "filled_size_usd": cost,
            "filled_shares": cost / price,
            "source_intent": {
                "market_slug": market_slug,
                "condition_id": sample.get("condition_id") or "",
                "event_ts": sample.get("event_ts"),
            },
        },
        "",
    )


def _shadow_score_summary(records: list[dict[str, Any]], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    scores: list[dict[str, Any]] = []
    missing: dict[str, int] = defaultdict(int)
    for record in records:
        order, reason = _shadow_order_from_record(record)
        if order is None:
            missing[reason] += 1
            continue
        scores.append(score_order(order, resolutions))
    resolved = [score for score in scores if score.get("resolved") and float(score.get("cost_usd") or 0.0) > 0.0]
    wins = [score for score in resolved if score.get("win")]
    cost = sum(float(score.get("cost_usd") or 0.0) for score in resolved)
    pnl = sum(float(score.get("pnl_usd") or 0.0) for score in resolved)
    return {
        "unique_intents": len(records),
        "shadow_orders_scored": len(scores),
        "resolved": len(resolved),
        "unresolved_or_unscored": len(records) - len(resolved),
        "wins": len(wins),
        "hit_rate_pct": round(100.0 * len(wins) / len(resolved), 6) if resolved else None,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost > 0 else None,
        "missing_reason_counts": dict(sorted(missing.items())),
        "sample_scored": scores[:5],
    }


def _score_summary_from_scores(scores: list[dict[str, Any]], *, rolling_n: int) -> dict[str, Any]:
    resolved = [
        score
        for score in scores
        if score.get("resolved") and _as_float(score.get("cost_usd")) is not None and float(score.get("cost_usd") or 0.0) > 0.0
    ]
    rolling = resolved[-rolling_n:] if rolling_n > 0 else resolved
    wins = [score for score in rolling if score.get("win")]
    cost = sum(float(score.get("cost_usd") or 0.0) for score in rolling)
    pnl = sum(float(score.get("pnl_usd") or 0.0) for score in rolling)
    return {
        "status": "PASS" if rolling else "NO_RESOLVED_SAMPLE",
        "rolling_n": rolling_n,
        "orders_scored": len(scores),
        "resolved_available": len(resolved),
        "resolved": len(rolling),
        "wins": len(wins),
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost > 0 else None,
        "hit_rate_pct": round(100.0 * len(wins) / len(rolling), 6) if rolling else None,
    }


def _scoreable_live_order(order: dict[str, Any]) -> dict[str, Any] | None:
    if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
        return None
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    maker_cancel = (
        trade_result.get("wallet_copy_maker_cancel")
        if isinstance(trade_result.get("wallet_copy_maker_cancel"), dict)
        else {}
    )
    cost = (
        _as_float(order.get("filled_size_usd"))
        or _as_float(trade_result.get("filled_size_usd"))
        or _as_float(trade_result.get("response_filled_size_usd"))
        or _as_float(maker_cancel.get("filled_size_usd"))
    )
    shares = (
        _as_float(order.get("filled_shares"))
        or _as_float(trade_result.get("fill_size_shares"))
        or _as_float(trade_result.get("response_fill_size_shares"))
        or _as_float(maker_cancel.get("matched_shares"))
    )
    price = _as_float(order.get("limit_price")) or _as_float(trade_result.get("response_fill_price")) or _as_float(
        trade_result.get("entry_price")
    )
    if cost is None and shares is not None and price is not None:
        cost = shares * price
    if shares is None and cost is not None and price is not None and price > 0:
        shares = cost / price
    if cost is None or cost <= 0 or shares is None or shares <= 0:
        return None
    out = dict(order)
    out["filled_size_usd"] = cost
    out["filled_shares"] = shares
    return out


def _rolling_live_pnl_by_wallet(
    live_ledger_state: Path,
    resolutions: dict[str, dict[str, Any]],
    active_wallets: set[str],
    *,
    rolling_n: int,
) -> dict[str, dict[str, Any]]:
    ledger = load_json(live_ledger_state, default={})
    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for order in ledger.get("orders") if isinstance(ledger.get("orders"), list) else []:
        if not isinstance(order, dict):
            continue
        wallet = _norm_wallet(order.get("source_wallet"))
        if wallet not in active_wallets:
            continue
        scoreable = _scoreable_live_order(order)
        if scoreable is None:
            continue
        score = score_order(scoreable, resolutions)
        score["_sort_ts"] = str(order.get("updated_at") or order.get("submitted_at") or "")
        by_wallet[wallet].append(score)
    return {
        wallet: _score_summary_from_scores(
            sorted(scores, key=lambda score: str(score.get("_sort_ts") or "")),
            rolling_n=rolling_n,
        )
        for wallet, scores in by_wallet.items()
    }


def _rolling_shadow_pnl_summary(
    records: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
    *,
    rolling_n: int,
) -> dict[str, Any]:
    scores: list[dict[str, Any]] = []
    for record in sorted(records, key=lambda item: str(item.get("first_seen_ts") or "")):
        order, _ = _shadow_order_from_record(record)
        if order is None:
            continue
        score = score_order(order, resolutions)
        score["_sort_ts"] = str(record.get("first_seen_ts") or "")
        scores.append(score)
    return _score_summary_from_scores(scores, rolling_n=rolling_n)


def _path_attribution(records: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts: dict[str, int] = defaultdict(int)
    real_submit_path_evaluated = 0
    dry_run_only = 0
    same_cycle_toxicity = 0
    for record in records:
        first_pass = _first_pass_timeline_entry(record)
        status = str(first_pass.get("live_execution_status") or "UNKNOWN")
        status_counts[status] += 1
        if status == "LIVE_ARMED_DRY_RUN":
            dry_run_only += 1
        elif status not in {"", "UNKNOWN", "CORRECTION"}:
            real_submit_path_evaluated += 1
        if _same_cycle_toxicity(record, first_pass):
            same_cycle_toxicity += 1
    if records and real_submit_path_evaluated == 0 and dry_run_only:
        status = "NOT_CONFIRMED_DIAGNOSTIC_DRY_RUN_ONLY"
        owner = "submit_path_starvation_until_real_submit_path_evidence_exists"
    elif real_submit_path_evaluated == len(records):
        status = "REAL_SUBMIT_PATH_CONFIRMED_FOR_ALL_DENIALS"
        owner = "toxicity_gate"
    elif real_submit_path_evaluated:
        status = "MIXED_MOSTLY_DIAGNOSTIC_DRY_RUN"
        owner = "mixed_submit_path_starvation_and_toxicity_gate"
    else:
        status = "INSUFFICIENT_PATH_EVIDENCE"
        owner = "unknown"
    return {
        "status": status,
        "owner": owner,
        "first_pass_live_execution_status_counts": dict(sorted(status_counts.items())),
        "real_submit_path_evaluated": real_submit_path_evaluated,
        "diagnostic_dry_run_only": dry_run_only,
        "same_cycle_toxicity_denials": same_cycle_toxicity,
    }


def _build_toxicity_denial_shadow_ev(args: argparse.Namespace) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    source_intent_ids: dict[str, set[str]] = defaultdict(set)
    rows_scanned = 0
    files_scanned: list[str] = []
    for path in _jsonl_paths(args.current_events, args.log_archives):
        files_scanned.append(str(path))
        for row in _iter_jsonl(path):
            rows_scanned += 1
            ts = _row_ts(row)
            if ts is None:
                continue
            for sample in _profit_latency(row).get("sample_passed_intents") or []:
                if isinstance(sample, dict):
                    wallet = _norm_wallet(sample.get("source_wallet"))
                    if wallet:
                        _record_intent(records, source_intent_ids, row, ts, sample, "passed", wallet)
            for sample in _profit_latency(row).get("sample_filtered_intents") or []:
                if isinstance(sample, dict):
                    wallet = _norm_wallet(sample.get("source_wallet"))
                    if wallet:
                        _record_intent(records, source_intent_ids, row, ts, sample, "profit_filtered", wallet)
            for sample in _toxicity(row).get("sample_filtered_intents") or []:
                if isinstance(sample, dict):
                    wallet = _norm_wallet(sample.get("source_wallet"))
                    if wallet:
                        _record_intent(records, source_intent_ids, row, ts, sample, "toxicity_filtered", wallet)
    first_pass_records = [record for record in records.values() if record.get("first_class") == "passed"]
    denied_records = [record for record in first_pass_records if record.get("terminal_toxicity_seen")]
    accepted_records = [record for record in first_pass_records if not record.get("terminal_toxicity_seen")]
    resolutions = load_resolutions(args.resolutions)
    denied_summary = _shadow_score_summary(denied_records, resolutions)
    accepted_summary = _shadow_score_summary(accepted_records, resolutions)
    real_submit_denied_records = [
        record
        for record in denied_records
        if _first_pass_timeline_entry(record).get("live_execution_status") == "LIVE_EXECUTION_SUBMITTED"
    ]
    dry_run_denied_records = [
        record
        for record in denied_records
        if _first_pass_timeline_entry(record).get("live_execution_status") == "LIVE_ARMED_DRY_RUN"
    ]
    real_submit_summary = _shadow_score_summary(real_submit_denied_records, resolutions)
    resolved_n = int(denied_summary.get("resolved") or 0)
    roi = _as_float(denied_summary.get("roi_pct"))
    if resolved_n < int(args.toxicity_shadow_min_n):
        decision_status = "INSUFFICIENT_SAMPLE"
    elif roi is not None and roi > 0.0:
        decision_status = "POSITIVE_SHADOW_EV_REQUIRES_FABLE_RULING"
    else:
        decision_status = "GATE_CONFIRMED_NONPOSITIVE"
    traces = sorted(
        [_pass_to_late_trace(record, source_intent_ids) for record in denied_records if record.get("terminal_resnapshot_seen")],
        key=lambda trace: (int(trace.get("occurrence_count") or 0), str(trace.get("first_event_ts") or "")),
        reverse=True,
    )[:1]
    return {
        "schema_version": 1,
        "flow_stage": "LEARN/LIVE",
        "measure_only": True,
        "files_scanned": files_scanned,
        "rows_scanned": rows_scanned,
        "first_pass_unique_intents": len(first_pass_records),
        "denied_set": denied_summary,
        "accepted_baseline": accepted_summary,
        "decision_rule": f"denied-set shadow EV > 0 at n>={int(args.toxicity_shadow_min_n)} allows later Fable consideration; <=0 confirms gate",
        "decision_status": decision_status,
        "path_attribution": _path_attribution(denied_records),
        "c5b_real_submit_path_shadow_ev": {
            "restricted_to_first_pass_live_execution_status": "LIVE_EXECUTION_SUBMITTED",
            "denied_set": real_submit_summary,
            "decision_rule": (
                f"real-submit denied-set resolved n>={int(args.toxicity_shadow_min_n)} "
                "and shadow EV > 0 is required before any toxicity threshold consideration"
            ),
            "decision_status": _c5b_decision_status(real_submit_summary, int(args.toxicity_shadow_min_n)),
        },
        "c5b_dry_run_starvation_trace": _dry_run_starvation_packet(dry_run_denied_records, source_intent_ids),
        "sample_traces": traces,
    }


def _next_ranked_candidates(ranking_path: Path, queue_path: Path, active_wallets: set[str], *, limit: int = 8) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    ranking = load_json(ranking_path, default={})
    for idx, row in enumerate(ranking.get("selected") if isinstance(ranking.get("selected"), list) else []):
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet or wallet in active_wallets or wallet in seen:
            continue
        candidates.append(
            {
                "source": "watch_tier_ranking",
                "rank": idx + 1,
                "source_wallet": wallet,
                "selection_reason": row.get("selection_reason"),
                "live_admission": "requires_shadow_ev_bar",
            }
        )
        seen.add(wallet)
        if len(candidates) >= limit:
            return candidates
    queue = load_json(queue_path, default={})
    for idx, row in enumerate(queue.get("ranked_members") if isinstance(queue.get("ranked_members"), list) else []):
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet or wallet in active_wallets or wallet in seen:
            continue
        candidates.append(
            {
                "source": "ready_lane_queue",
                "rank": row.get("queue_rank") or idx + 1,
                "source_wallet": wallet,
                "candidate_id": row.get("name") or row.get("candidate_id"),
                "ready_for_live": bool(row.get("ready_for_live")),
                "live_admission": "requires_shadow_ev_bar",
            }
        )
        seen.add(wallet)
        if len(candidates) >= limit:
            break
    return candidates


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    now = _parse_ts(args.now) if args.now else None
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=float(args.hours))
    guard = load_json(args.guard_state, default={})
    members, runtime = _active_members(guard)
    active_wallets = {_norm_wallet(member.get("source_wallet")) for member in members}
    resolutions = load_resolutions(args.resolutions)
    rolling_pnl_n = int(getattr(args, "rolling_pnl_n", 20) or 20)
    rolling_live_pnl = _rolling_live_pnl_by_wallet(
        getattr(args, "live_ledger_state", DEFAULT_LIVE_LEDGER_STATE),
        resolutions,
        active_wallets,
        rolling_n=rolling_pnl_n,
    )
    freshest_source_lag = _freshest_source_lag_by_wallet(guard)
    latest_eligible_ts_by_wallet: dict[str, datetime] = {}
    intent_records: dict[str, dict[str, Any]] = {}
    source_intent_ids: dict[str, set[str]] = defaultdict(set)
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "eligible_intents_24h": 0,
            "suppressed_intents_24h": 0,
            "late_window_suppressed_intents_24h": 0,
            "toxicity_blocked_intents_24h": 0,
            "unique_intents_24h": 0,
            "sample_occurrences_24h": 0,
            "taxonomy_transitions": {"pass_to_late": 0, "pass_to_terminal_resnapshot": 0},
            "terminal_resnapshot_after_toxicity_occurrences": 0,
            "latest_event_ts": None,
            "latest_source_trade_ts": None,
            "source_to_detection_lag_s": [],
            "window_open_to_source_trade_s": [],
        }
    )
    files_scanned: list[str] = []
    rows_scanned = 0
    rows_in_window = 0
    earliest_scanned_ts: datetime | None = None
    for path in _jsonl_paths(args.current_events, args.log_archives):
        files_scanned.append(str(path))
        for row in _iter_jsonl(path):
            rows_scanned += 1
            ts = _row_ts(row)
            if ts is None or ts > now + timedelta(minutes=5):
                continue
            earliest_scanned_ts = min([v for v in (earliest_scanned_ts, ts) if v is not None], default=ts)
            for sample in _profit_latency(row).get("sample_passed_intents") or []:
                if not isinstance(sample, dict):
                    continue
                wallet = _norm_wallet(sample.get("source_wallet"))
                if wallet not in active_wallets:
                    continue
                latest_eligible_ts_by_wallet[wallet] = max(
                    [v for v in (latest_eligible_ts_by_wallet.get(wallet), ts) if v is not None],
                    default=ts,
                )
                if ts < cutoff:
                    continue
                _record_intent(intent_records, source_intent_ids, row, ts, sample, "passed", wallet)
            if ts < cutoff:
                continue
            rows_in_window += 1
            for sample in _profit_latency(row).get("sample_filtered_intents") or []:
                if not isinstance(sample, dict):
                    continue
                wallet = _norm_wallet(sample.get("source_wallet"))
                if wallet not in active_wallets:
                    continue
                _record_intent(intent_records, source_intent_ids, row, ts, sample, "profit_filtered", wallet)
            for sample in _toxicity(row).get("sample_filtered_intents") or []:
                if not isinstance(sample, dict):
                    continue
                wallet = _norm_wallet(sample.get("source_wallet"))
                if wallet not in active_wallets:
                    continue
                _record_intent(intent_records, source_intent_ids, row, ts, sample, "toxicity_filtered", wallet)
    pass_to_late_traces: list[dict[str, Any]] = []
    pass_to_late_mechanisms: dict[str, int] = defaultdict(int)
    terminal_resnapshot_traces: list[dict[str, Any]] = []
    terminal_resnapshot_mechanisms: dict[str, int] = defaultdict(int)
    for record in intent_records.values():
        wallet = _norm_wallet(record.get("source_wallet"))
        if wallet not in active_wallets:
            continue
        bucket = stats[wallet]
        bucket["unique_intents_24h"] += 1
        bucket["sample_occurrences_24h"] += int(record.get("occurrence_count") or 0)
        latest_ts = record.get("last_seen_ts")
        if isinstance(latest_ts, datetime):
            bucket["latest_event_ts"] = max([v for v in (bucket["latest_event_ts"], latest_ts) if v is not None], default=latest_ts)
        source_ts = record.get("latest_source_trade_ts")
        if isinstance(source_ts, datetime):
            bucket["latest_source_trade_ts"] = max(
                [v for v in (bucket["latest_source_trade_ts"], source_ts) if v is not None],
                default=source_ts,
            )
        first_class = record.get("first_class")
        if first_class == "passed":
            bucket["eligible_intents_24h"] += 1
        elif first_class == "profit_filtered":
            bucket["suppressed_intents_24h"] += 1
            if record.get("first_is_late"):
                bucket["late_window_suppressed_intents_24h"] += 1
                source_lag = record.get("first_late_source_to_detection_lag_s")
                window_offset = record.get("first_late_window_open_to_source_trade_s")
                if source_lag is not None:
                    bucket["source_to_detection_lag_s"].append(float(source_lag))
                if window_offset is not None:
                    bucket["window_open_to_source_trade_s"].append(float(window_offset))
        elif first_class == "toxicity_filtered":
            bucket["toxicity_blocked_intents_24h"] += 1
        bucket["terminal_resnapshot_after_toxicity_occurrences"] += int(
            record.get("terminal_resnapshot_occurrences") or 0
        )
        if record.get("first_class") == "passed" and record.get("late_seen"):
            bucket["taxonomy_transitions"]["pass_to_late"] += 1
            trace = _pass_to_late_trace(record, source_intent_ids)
            pass_to_late_mechanisms[str(trace["mechanism"])] += 1
            pass_to_late_traces.append(trace)
        if record.get("first_class") == "passed" and record.get("terminal_resnapshot_seen"):
            bucket["taxonomy_transitions"]["pass_to_terminal_resnapshot"] += 1
            trace = _pass_to_late_trace(record, source_intent_ids)
            terminal_resnapshot_mechanisms[str(trace["mechanism"])] += 1
            terminal_resnapshot_traces.append(trace)
    next_candidates = _next_ranked_candidates(args.watch_ranking, args.queue, active_wallets)
    proposed_replacement = next_candidates[0] if next_candidates else None
    rows: list[dict[str, Any]] = []
    demotion_candidates: list[dict[str, Any]] = []
    for member in members:
        wallet = _norm_wallet(member.get("source_wallet"))
        row = dict(stats[wallet])
        suppressed = int(row["suppressed_intents_24h"])
        late = int(row["late_window_suppressed_intents_24h"])
        latest_source_trade_ts = row.get("latest_source_trade_ts")
        hours_since_last_source_trade = None
        if isinstance(latest_source_trade_ts, datetime):
            hours_since_last_source_trade = round(max(0.0, (now - latest_source_trade_ts).total_seconds()) / 3600.0, 6)
        elif wallet in freshest_source_lag:
            hours_since_last_source_trade = round(float(freshest_source_lag[wallet]) / 3600.0, 6)
        latest_eligible_ts = latest_eligible_ts_by_wallet.get(wallet)
        if int(row["eligible_intents_24h"]) > 0:
            zero_eligible_age_hours = 0.0
            zero_eligible_age_basis = "eligible_intents_present_in_window"
        elif isinstance(latest_eligible_ts, datetime):
            zero_eligible_age_hours = round(max(0.0, (now - latest_eligible_ts).total_seconds()) / 3600.0, 6)
            zero_eligible_age_basis = "latest_profit_latency_pass_seen_in_scanned_guard_events"
        else:
            zero_eligible_age_hours = (
                round(max(0.0, (now - earliest_scanned_ts).total_seconds()) / 3600.0, 6)
                if isinstance(earliest_scanned_ts, datetime)
                else None
            )
            zero_eligible_age_basis = "lower_bound_no_profit_latency_pass_seen_in_scanned_guard_events"
        late_source_lags = row.get("source_to_detection_lag_s") if isinstance(row.get("source_to_detection_lag_s"), list) else []
        late_source_offsets = (
            row.get("window_open_to_source_trade_s")
            if isinstance(row.get("window_open_to_source_trade_s"), list)
            else []
        )
        row.update(
            {
                "candidate_id": member.get("candidate_id"),
                "source_wallet": wallet,
                "policy_id": member.get("policy_id"),
                "eligible_intents_24h": int(row["eligible_intents_24h"]),
                "suppressed_intents_24h": suppressed,
                "late_window_suppressed_intents_24h": late,
                "unique_intents_24h": int(row["unique_intents_24h"]),
                "sample_occurrences_24h": int(row["sample_occurrences_24h"]),
                "taxonomy_transitions": row["taxonomy_transitions"],
                "terminal_resnapshot_after_toxicity_occurrences": int(
                    row["terminal_resnapshot_after_toxicity_occurrences"]
                ),
                "late_window_suppression_share": round(late / suppressed, 6) if suppressed else None,
                "toxicity_blocked_intents_24h": int(row["toxicity_blocked_intents_24h"]),
                "latest_event_ts": row["latest_event_ts"].isoformat() if row["latest_event_ts"] else None,
                "latest_source_trade_ts": latest_source_trade_ts.isoformat()
                if isinstance(latest_source_trade_ts, datetime)
                else None,
                "hours_since_last_source_trade": hours_since_last_source_trade,
                "zero_eligible_age_hours": zero_eligible_age_hours,
                "zero_eligible_age_basis": zero_eligible_age_basis,
                "zero_eligible_age_is_lower_bound": zero_eligible_age_basis.startswith("lower_bound_"),
                "late_suppression_latency_attribution": {
                    "sample_count": len(late_source_lags),
                    "source_trade_to_detection_lag_s_p50": _percentile(late_source_lags, 0.50),
                    "source_trade_to_detection_lag_s_p90": _percentile(late_source_lags, 0.90),
                    "window_open_to_source_trade_s_p50": _percentile(late_source_offsets, 0.50),
                    "window_open_to_source_trade_s_p90": _percentile(late_source_offsets, 0.90),
                },
            }
        )
        member_shadow_records = [
            record
            for record in intent_records.values()
            if _norm_wallet(record.get("source_wallet")) == wallet and record.get("first_class") == "passed"
        ]
        row["rolling_realized_pnl"] = rolling_live_pnl.get(
            wallet,
            {
                "status": "NO_LIVE_FILLS",
                "rolling_n": rolling_pnl_n,
                "orders_scored": 0,
                "resolved_available": 0,
                "resolved": 0,
                "wins": 0,
                "cost_usd": 0.0,
                "pnl_usd": 0.0,
                "roi_pct": None,
                "hit_rate_pct": None,
            },
        )
        row["rolling_shadow_pnl"] = _rolling_shadow_pnl_summary(
            member_shadow_records,
            resolutions,
            rolling_n=rolling_pnl_n,
        )
        row.pop("source_to_detection_lag_s", None)
        row.pop("window_open_to_source_trade_s", None)
        all_late_candidate = row["eligible_intents_24h"] == 0 and suppressed > 0 and row["late_window_suppression_share"] == 1.0
        abandoned_candidate = (
            row["eligible_intents_24h"] == 0
            and hours_since_last_source_trade is not None
            and hours_since_last_source_trade >= 48.0
        )
        row["demotion_candidate"] = all_late_candidate or abandoned_candidate
        row["demotion_class"] = (
            "all_late_zero_eligible"
            if all_late_candidate
            else "abandoned_source"
            if abandoned_candidate
            else None
        )
        row["proposed_replacement"] = proposed_replacement if row["demotion_candidate"] else None
        if row["demotion_candidate"]:
            demotion_candidates.append(row)
        rows.append(row)
    representative_traces = sorted(
        pass_to_late_traces,
        key=lambda trace: (int(trace.get("occurrence_count") or 0), str(trace.get("first_event_ts") or "")),
        reverse=True,
    )[:1]
    representative_terminal_traces = sorted(
        terminal_resnapshot_traces,
        key=lambda trace: (int(trace.get("occurrence_count") or 0), str(trace.get("first_event_ts") or "")),
        reverse=True,
    )[:1]
    toxicity_shadow_ev = _build_toxicity_denial_shadow_ev(args)
    return {
        "schema_version": 1,
        "kind": "active_set_starvation_packet",
        "flow_stage": "LIVE/LEARN/ROTATE",
        "generated_at": utc_now_iso(),
        "window_hours": float(args.hours),
        "cutoff_ts": cutoff.isoformat(),
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "active_set_runtime_member_count": runtime.get("member_count"),
        "active_set_runtime_qualified_member_count": runtime.get("qualified_member_count"),
        "active_set_snapshot_member_count": (guard.get("active_set") or {}).get("member_count")
        if isinstance(guard.get("active_set"), dict)
        else None,
        "authoritative_member_count_note": "active_set_runtime excludes total-loss auto-disabled members and is the executable live roster",
        "files_scanned": files_scanned,
        "rows_scanned": rows_scanned,
        "rows_in_window": rows_in_window,
        "summary": {
            "enabled_members": len(rows),
            "demotion_candidates": len(demotion_candidates),
            "abandoned_source_candidates": sum(1 for row in rows if row.get("demotion_class") == "abandoned_source"),
            "replacement_candidates": len(next_candidates),
            "late_suppressed_intents_24h": sum(int(row.get("late_window_suppressed_intents_24h") or 0) for row in rows),
            "eligible_intents_24h": sum(int(row.get("eligible_intents_24h") or 0) for row in rows),
            "unique_intents_24h": sum(int(row.get("unique_intents_24h") or 0) for row in rows),
            "sample_occurrences_24h": sum(int(row.get("sample_occurrences_24h") or 0) for row in rows),
            "pass_to_late_transitions_24h": sum(
                int((row.get("taxonomy_transitions") or {}).get("pass_to_late") or 0) for row in rows
            ),
            "pass_to_terminal_resnapshot_24h": sum(
                int((row.get("taxonomy_transitions") or {}).get("pass_to_terminal_resnapshot") or 0) for row in rows
            ),
            "terminal_resnapshot_after_toxicity_occurrences": sum(
                int(row.get("terminal_resnapshot_after_toxicity_occurrences") or 0) for row in rows
            ),
        },
        "members": rows,
        "pass_to_late_lifecycle_trace": {
            "unique_flip_intents": sum(int((row.get("taxonomy_transitions") or {}).get("pass_to_late") or 0) for row in rows),
            "mechanism_counts": dict(sorted(pass_to_late_mechanisms.items())),
            "sample_traces": representative_traces,
            "interpretation": "first-occurrence taxonomy drives counts; pass->late flips are traced separately and do not inflate late counts",
        },
        "terminal_resnapshot_lifecycle_trace": {
            "unique_terminal_resnapshot_intents": sum(
                int((row.get("taxonomy_transitions") or {}).get("pass_to_terminal_resnapshot") or 0) for row in rows
            ),
            "mechanism_counts": dict(sorted(terminal_resnapshot_mechanisms.items())),
            "sample_traces": representative_terminal_traces,
            "interpretation": "C6 terminal toxicity denials tag later late occurrences as resnapshot_after_terminal",
        },
        "inventory_skip_lifecycle_trace": _inventory_skip_lifecycle_trace(
            list(intent_records.values()),
            source_intent_ids,
            current_generation_id=runtime.get("set_generation_id"),
        ),
        "toxicity_denial_shadow_ev": toxicity_shadow_ev,
        "next_ranked_replacement_candidates": next_candidates,
        "proposal": {
            "action": "FABLE_DECIDES_SWAP",
            "demotion_candidates": [
                {
                    "candidate_id": row.get("candidate_id"),
                    "source_wallet": row.get("source_wallet"),
                    "eligible_intents_24h": row.get("eligible_intents_24h"),
                    "late_window_suppression_share": row.get("late_window_suppression_share"),
                    "hours_since_last_source_trade": row.get("hours_since_last_source_trade"),
                    "zero_eligible_age_hours": row.get("zero_eligible_age_hours"),
                    "rolling_realized_pnl": row.get("rolling_realized_pnl"),
                    "rolling_shadow_pnl": row.get("rolling_shadow_pnl"),
                    "demotion_class": row.get("demotion_class"),
                    "proposed_replacement": row.get("proposed_replacement"),
                }
                for row in demotion_candidates
            ],
            "live_admission_rule": "no force-promote below pre-registered shadow-EV bar",
            "fable_swap_ruling": "Fable decides all swaps; packet only proposes measured candidates",
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", type=Path, default=DEFAULT_GUARD_STATE)
    parser.add_argument("--current-events", type=Path, default=DEFAULT_CURRENT_EVENTS)
    parser.add_argument("--log-archives", type=Path, default=DEFAULT_LOG_ARCHIVES)
    parser.add_argument("--watch-ranking", type=Path, default=DEFAULT_WATCH_RANKING)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--resolutions", type=Path, default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--live-ledger-state", type=Path, default=DEFAULT_LIVE_LEDGER_STATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--toxicity-shadow-min-n", type=int, default=20)
    parser.add_argument("--rolling-pnl-n", type=int, default=20)
    parser.add_argument("--now")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps({"output": str(args.output), "summary": report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
