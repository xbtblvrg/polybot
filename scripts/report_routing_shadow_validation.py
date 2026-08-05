#!/usr/bin/env python3
"""Build the Stage-1 shadow report for per-signal wallet routing.

Flow stage: LIVE/LEARN/SELF-DEV. This report never submits orders; it
replays the already-built live/probe CopyIntent diagnostics and records
which runtime member would have owned each window under the shadow router.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = "data/research/routing_shadow_validation_latest.json"
DEFAULT_SHADOW_CANDIDATE_SEATS = "data/research/routing_shadow_candidate_seats.json"
DEFAULT_MIN_VALIDATION_HOURS = 6.0
DEFAULT_RETAIN_ROWS = 5000
DEFAULT_RETAIN_CYCLES = 2500
DEFAULT_CURRENT_STATE_MAX_AGE_S = 180.0
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
EXPECTED_EDGE_NO_INPUT_SOURCE = "not_available_fee_gate_operates_on_fee_vs_fixed_assumption"
EXPECTED_EDGE_NO_INPUT_REASON = (
    "no upstream expected_edge or expected_edge_after_buffer field on intent, metadata, or drift_buffer"
)
EXPECTED_EDGE_FEE_CALIBRATION_BASIS = "realized_paper_pnl_vs_expected_fee_per_member"
ATTRIBUTION_TIEBREAK_RULE = "earliest_observed_ts_then_lexicographic_wallet"
ATTRIBUTION_STABILITY_MIN_INTERVAL_S = 600.0
GAP_MISSING_RESOLUTION = "missing_market_resolution_in_resolution_feed"
GAP_RESOLUTION_NOT_FINAL = "resolution_feed_status_not_resolved"
GAP_MISSING_DIRECTION_FIELDS = "missing_side_or_outcome_on_measurement_row"
GAP_MISSING_PRICE_OR_SIZE = "missing_shares_or_size_on_fee_gated_measurement_row"


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _r7_snapshot_stamp(value: Any) -> str:
    ts = _parse_ts(value) or dt.datetime.now(dt.timezone.utc).timestamp()
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime("%Y%m%dT%H%MZ")


def r7_dated_snapshot_path(out_path: Path, *, generated_at: Any) -> Path:
    return out_path.with_name(f"{out_path.stem}_r7_{_r7_snapshot_stamp(generated_at)}{out_path.suffix}")


def _first_available_snapshot_path(path: Path) -> Path:
    if not path.exists():
        return path
    for counter in range(2, 10_000):
        candidate = path.with_name(f"{path.stem}_{counter}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"no available R7 snapshot path for {path}")


def write_r7_dated_snapshot(out_path: Path, payload: dict[str, Any]) -> Path:
    snapshot_path = _first_available_snapshot_path(
        r7_dated_snapshot_path(out_path, generated_at=payload.get("generated_at"))
    )
    snapshot_payload = dict(payload)
    snapshot_payload["r7_dated_snapshot"] = {
        "enabled": True,
        "source_latest_path": str(out_path.relative_to(ROOT) if out_path.is_relative_to(ROOT) else out_path),
        "snapshot_path": str(
            snapshot_path.relative_to(ROOT) if snapshot_path.is_relative_to(ROOT) else snapshot_path
        ),
        "rule": "R7 harvest writes dated snapshot before refreshing latest routing shadow state",
    }
    atomic_write_json(snapshot_path, snapshot_payload)
    return snapshot_path


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _window_regime(market_slug: Any, window_start: Any) -> dict[str, Any]:
    start = _parse_ts(window_start)
    if start is None:
        parts = str(market_slug or "").rsplit("-", 1)
        if len(parts) == 2:
            start = _parse_ts(parts[-1])
    if start is None:
        return {"regime": "unknown", "day_of_week_utc": None, "utc_hour": None}
    stamp = dt.datetime.fromtimestamp(float(start), tz=dt.timezone.utc)
    return {
        "regime": "weekend" if stamp.weekday() >= 5 else "weekday",
        "day_of_week_utc": stamp.strftime("%a").lower(),
        "utc_hour": stamp.hour,
    }


def _wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _short_wallet(wallet: Any) -> str | None:
    raw = _wallet(wallet)
    if not raw:
        return None
    return raw if len(raw) < 12 else f"{raw[:6]}...{raw[-4:]}"


def _member_wallet(member: dict[str, Any]) -> str:
    return _wallet(member.get("source_wallet") or member.get("wallet"))


def _member_enabled(member: dict[str, Any]) -> bool:
    status = str(member.get("status") or "").upper()
    return bool(member.get("enabled", True)) and not (
        status.startswith("DEMOTED")
        or status.startswith("DISABLED")
        or status.startswith("AUTO_DISABLED")
        or bool(member.get("demoted"))
        or bool(member.get("total_loss_disabled"))
    )


def _normalize_shadow_candidate_seat(row: dict[str, Any]) -> dict[str, Any]:
    wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
    if not wallet:
        return {}
    policy = row.get("policy") if isinstance(row.get("policy"), dict) else {}
    normalized = {
        "source_wallet": wallet,
        "candidate_id": str(row.get("candidate_id") or row.get("name") or wallet[-12:]),
        "policy_id": str(row.get("policy_id") or policy.get("policy_id") or ""),
        "policy": policy,
        "enabled": row.get("enabled") is not False,
        "shadow_only": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "shadow_seat_reason": str(row.get("reason") or row.get("shadow_seat_reason") or "routing_shadow_candidate_seat"),
    }
    for key in ("max_order_usd", "max_price", "wallet_fraction", "direction_id"):
        if key in row:
            normalized[key] = row.get(key)
    return normalized


def load_shadow_candidate_seats(path: str | Path = DEFAULT_SHADOW_CANDIDATE_SEATS) -> list[dict[str, Any]]:
    parsed = Path(path)
    if not parsed.is_absolute():
        parsed = ROOT / parsed
    payload = load_json(parsed, default={})
    if not isinstance(payload, dict) or payload.get("enabled") is False:
        return []
    seats = payload.get("seats") if isinstance(payload.get("seats"), list) else []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for seat in seats:
        if not isinstance(seat, dict):
            continue
        normalized = _normalize_shadow_candidate_seat(seat)
        wallet = _wallet(normalized.get("source_wallet"))
        if not normalized or not wallet or wallet in seen:
            continue
        rows.append(normalized)
        seen.add(wallet)
    return rows


def _runtime_with_shadow_candidate_seats(
    active_set_runtime: dict[str, Any],
    shadow_candidate_members: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    seats = [
        _normalize_shadow_candidate_seat(row)
        for row in (shadow_candidate_members or [])
        if isinstance(row, dict)
    ]
    seats = [row for row in seats if row]
    if not seats:
        return active_set_runtime, []
    runtime = dict(active_set_runtime)
    members = [
        dict(row)
        for row in active_set_runtime.get("members", [])
        if isinstance(row, dict)
    ]
    existing = {
        _wallet(row.get("source_wallet") or row.get("wallet"))
        for row in members
        if _wallet(row.get("source_wallet") or row.get("wallet"))
    }
    added: list[dict[str, Any]] = []
    for seat in seats:
        wallet = _wallet(seat.get("source_wallet"))
        if not wallet or wallet in existing:
            continue
        members.append(seat)
        added.append(seat)
        existing.add(wallet)
    runtime["members"] = members
    runtime["shadow_candidate_seats"] = added
    runtime["shadow_candidate_seat_count"] = len(added)
    return runtime, added


def _priority_frozen_wallets(selection_priority_freeze: dict[str, Any]) -> set[str]:
    wallets = selection_priority_freeze.get("wallets")
    if isinstance(wallets, list):
        return {_wallet(wallet) for wallet in wallets if _wallet(wallet)}
    frozen: set[str] = set()
    for row in selection_priority_freeze.get("rows") or []:
        if isinstance(row, dict) and _wallet(row.get("source_wallet") or row.get("wallet")):
            frozen.add(_wallet(row.get("source_wallet") or row.get("wallet")))
    return frozen


def _runtime_member_rows(
    active_set_runtime: dict[str, Any],
    *,
    selection_priority_freeze: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    members = active_set_runtime.get("members") if isinstance(active_set_runtime.get("members"), list) else []
    frozen_wallets = _priority_frozen_wallets(selection_priority_freeze)
    rows: list[dict[str, Any]] = []
    by_wallet: dict[str, dict[str, Any]] = {}
    for index, member in enumerate(members):
        if not isinstance(member, dict):
            continue
        wallet = _member_wallet(member)
        if not wallet:
            continue
        enabled = _member_enabled(member)
        denied_reason = ""
        if not enabled:
            denied_reason = "runtime_member_disabled_or_demoted"
        elif wallet in frozen_wallets:
            denied_reason = "selection_priority_freeze"
        policy = member.get("policy") if isinstance(member.get("policy"), dict) else {}
        row = {
            "member_index": index,
            "source_wallet": wallet,
            "source_wallet_short": _short_wallet(wallet),
            "candidate_id": str(member.get("candidate_id") or ""),
            "policy_id": str(member.get("policy_id") or policy.get("policy_id") or ""),
            "enabled": enabled,
            "route_denied": bool(denied_reason),
            "route_denied_reason": denied_reason,
        }
        rows.append(row)
        by_wallet[wallet] = row
    return rows, by_wallet


def _candidate_summary(state: dict[str, Any]) -> dict[str, Any]:
    summary = state.get("candidate_intent_summary") if isinstance(state.get("candidate_intent_summary"), dict) else {}
    return summary


def _safe_probe_label(value: str, *, fallback: str = "member") -> str:
    label = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value or ""))
    return label[:80] or fallback


def _probe_path_for_member(member: dict[str, Any]) -> Path:
    candidate_id = str(member.get("candidate_id") or "")
    wallet = str(member.get("source_wallet") or "").strip().lower()
    label = _safe_probe_label(candidate_id or wallet[-12:], fallback="member")
    return ROOT / "data/research" / f"wallet_copy_live_execution_probe_{label}.json"


def _intent_metadata(intent: dict[str, Any]) -> dict[str, Any]:
    metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
    inventory = metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {}
    return inventory


def _intent_market_slug(intent: dict[str, Any]) -> str:
    inventory = _intent_metadata(intent)
    return str(intent.get("market_slug") or inventory.get("market_slug") or intent.get("source_market_slug") or "")


def _intent_window_start(intent: dict[str, Any]) -> float | None:
    inventory = _intent_metadata(intent)
    for key in ("window_start_s",):
        if (value := _as_float(intent.get(key))) is not None:
            return value
        if (value := _as_float(inventory.get(key))) is not None:
            return value
    slug = _intent_market_slug(intent)
    try:
        return float(slug.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return None


def _intent_observed_ts(intent: dict[str, Any]) -> float | None:
    inventory = _intent_metadata(intent)
    for value in (
        intent.get("observed_ts"),
        intent.get("source_detection_observed_ts"),
        intent.get("latest_observed_ts"),
        inventory.get("source_detection_observed_ts"),
        inventory.get("latest_observed_ts"),
        inventory.get("effective_latest_observed_ts"),
    ):
        if (parsed := _as_float(value)) is not None:
            return parsed
    return None


def _intent_source_wallet(intent: dict[str, Any], state: dict[str, Any], member_wallet: str) -> str:
    inventory = _intent_metadata(intent)
    return _wallet(
        intent.get("source_wallet")
        or inventory.get("source_wallet")
        or state.get("source_wallet")
        or member_wallet
    )


def _load_resolution_map(path: str = DEFAULT_RESOLUTIONS) -> dict[str, dict[str, Any]]:
    parsed = Path(path)
    if not parsed.is_absolute():
        parsed = ROOT / parsed
    resolutions: dict[str, dict[str, Any]] = {}
    try:
        handle = parsed.open(encoding="utf-8", errors="replace")
    except OSError:
        return resolutions
    with handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            slug = str(row.get("market_slug") or "")
            if slug:
                resolutions[slug] = row
    return resolutions


def _intent_price(intent: dict[str, Any]) -> float | None:
    inventory = _intent_metadata(intent)
    drip = inventory.get("inventory_v3_drip") if isinstance(inventory.get("inventory_v3_drip"), dict) else {}
    for value in (
        intent.get("limit_price"),
        intent.get("price"),
        intent.get("source_limit_price"),
        inventory.get("intent_limit_price"),
        inventory.get("source_inventory_vwap"),
        drip.get("tranche_limit_price"),
    ):
        if (parsed := _as_float(value)) is not None:
            return parsed
    return None


def _intent_shares(intent: dict[str, Any]) -> float | None:
    inventory = _intent_metadata(intent)
    drip = inventory.get("inventory_v3_drip") if isinstance(inventory.get("inventory_v3_drip"), dict) else {}
    for value in (
        intent.get("shares"),
        intent.get("size"),
        inventory.get("target_shares"),
        inventory.get("gap_shares"),
        drip.get("tranche_shares"),
    ):
        if (parsed := _as_float(value)) is not None:
            return parsed
    return None


def _expected_fee_usd(intent: dict[str, Any], *, fee_rate: float | None) -> float | None:
    metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
    expected_fee_gate = metadata.get("expected_fee_gate") if isinstance(metadata.get("expected_fee_gate"), dict) else {}
    if (parsed := _as_float(expected_fee_gate.get("expected_fee_usd"))) is not None:
        return round(max(0.0, parsed), 6)
    price = _intent_price(intent)
    shares = _intent_shares(intent)
    if fee_rate is None or price is None or shares is None:
        return None
    return round(max(0.0, fee_rate * shares * price * (1.0 - price)), 6)


def _expected_edge(intent: dict[str, Any]) -> tuple[float | None, str, str | None]:
    metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
    drift_buffer = metadata.get("drift_buffer") if isinstance(metadata.get("drift_buffer"), dict) else {}
    for key in ("expected_edge", "expected_edge_after_buffer"):
        if (parsed := _as_float(intent.get(key))) is not None:
            return parsed, key, None
        if (parsed := _as_float(metadata.get(key))) is not None:
            return parsed, f"metadata.{key}", None
        if (parsed := _as_float(drift_buffer.get(key))) is not None:
            return parsed, f"metadata.drift_buffer.{key}", None
    return None, EXPECTED_EDGE_NO_INPUT_SOURCE, EXPECTED_EDGE_NO_INPUT_REASON


def _paper_outcome(intent: dict[str, Any], *, resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    slug = _intent_market_slug(intent)
    resolution = resolutions.get(slug) if slug else None
    if not isinstance(resolution, dict) or not resolution:
        return {"status": "UNRESOLVED", "market_slug": slug, "measurement_gap_reason": GAP_MISSING_RESOLUTION}
    direction = str(resolution.get("direction") or "").upper()
    outcome = str(intent.get("outcome") or "").upper()
    side = str(intent.get("side") or "").upper()
    if direction not in {"UP", "DOWN"}:
        return {
            "status": "UNRESOLVED",
            "market_slug": slug,
            "resolution_status": resolution.get("uma_resolution_status"),
            "measurement_gap_reason": GAP_RESOLUTION_NOT_FINAL,
        }
    has_direction_fields = outcome in {"UP", "DOWN", "YES", "NO"} or side in {"YES", "NO"}
    if not has_direction_fields:
        return {
            "status": "RESOLVED",
            "market_slug": slug,
            "direction": direction,
            "wins": None,
            "paper_pnl_usd": None,
            "resolution_status": resolution.get("uma_resolution_status"),
            "source": resolution.get("source"),
            "measurement_gap_reason": GAP_MISSING_DIRECTION_FIELDS,
        }
    wins = (direction == "UP" and outcome in {"UP", "YES"}) or (direction == "DOWN" and outcome in {"DOWN", "NO"})
    if outcome not in {"UP", "DOWN", "YES", "NO"}:
        wins = (direction == "UP" and side == "YES") or (direction == "DOWN" and side == "NO")
    price = _intent_price(intent)
    shares = _intent_shares(intent)
    pnl = None
    gap_reason = None
    if price is not None and shares is not None:
        pnl = round(shares * ((1.0 - price) if wins else -price), 6)
    else:
        gap_reason = GAP_MISSING_PRICE_OR_SIZE
    return {
        "status": "RESOLVED",
        "market_slug": slug,
        "direction": direction,
        "wins": wins,
        "paper_pnl_usd": pnl,
        "resolution_status": resolution.get("uma_resolution_status"),
        "source": resolution.get("source"),
        "measurement_gap_reason": gap_reason,
    }


def _intent_row(
    *,
    intent: dict[str, Any],
    state: dict[str, Any],
    member: dict[str, Any],
    route_denied_reason: str,
    final_gate_rank: int,
) -> dict[str, Any]:
    wallet = str(member.get("source_wallet") or "")
    observed_ts = _intent_observed_ts(intent)
    window_start = _intent_window_start(intent)
    market_slug = _intent_market_slug(intent)
    regime = _window_regime(market_slug, window_start)
    source_wallet = _intent_source_wallet(intent, state, wallet)
    parity_conflict = bool(source_wallet and wallet and source_wallet != wallet)
    inventory = _intent_metadata(intent)
    source_row_event_id = str(
        intent.get("source_row_event_id")
        or inventory.get("source_row_event_id")
        or ""
    )
    return {
        "candidate_id": str(member.get("candidate_id") or state.get("candidate_id") or ""),
        "source_wallet": wallet,
        "source_wallet_short": _short_wallet(wallet),
        "policy_id": str(member.get("policy_id") or state.get("policy_id") or ""),
        "intent_id": str(intent.get("intent_id") or ""),
        "source_row_event_id": source_row_event_id or None,
        "market_slug": market_slug,
        "window_start_s": window_start,
        **regime,
        "observed_ts": observed_ts,
        "side": intent.get("side"),
        "outcome": intent.get("outcome"),
        "limit_price": intent.get("limit_price") or intent.get("price") or inventory.get("intent_limit_price"),
        "shares": intent.get("shares") or intent.get("size") or inventory.get("target_shares") or inventory.get("gap_shares"),
        "source_detection_observed_ts": inventory.get("source_detection_observed_ts"),
        "dominant_skip_reason": inventory.get("dominant_skip_reason"),
        "final_gate_rank": final_gate_rank,
        "route_denied_reason": route_denied_reason,
        "copyintent_parity_conflict": parity_conflict,
        "copyintent_source_wallet": source_wallet,
    }


def _signals_from_execution_state(
    state: dict[str, Any],
    *,
    member: dict[str, Any],
    route_denied_reason: str,
    resolutions: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    summary = _candidate_summary(state)
    sample_intents = [row for row in summary.get("sample_intents") or [] if isinstance(row, dict)]
    sample_intents = sorted(
        sample_intents,
        key=lambda row: (
            _intent_observed_ts(row) if _intent_observed_ts(row) is not None else float("inf"),
            str(row.get("intent_id") or ""),
        ),
    )
    after_fee = _as_int(summary.get("fresh_candidate_intents_after_expected_fee_gate"))
    selected = sample_intents[: max(0, min(after_fee, len(sample_intents)))]
    expected_fee_gate = (
        summary.get("expected_fee_capture_gate")
        if isinstance(summary.get("expected_fee_capture_gate"), dict)
        else {}
    )
    fee_rate = _as_float(expected_fee_gate.get("fee_rate"))
    fee_gated = sample_intents[max(0, min(after_fee, len(sample_intents))) :]
    signals: list[dict[str, Any]] = []
    for index, intent in enumerate(selected):
        edge, edge_source, edge_absence_reason = _expected_edge(intent)
        row = _intent_row(
            intent=intent,
            state=state,
            member=member,
            route_denied_reason=route_denied_reason,
            final_gate_rank=index,
        )
        row.update(
            {
                "expected_fee_usd": _expected_fee_usd(intent, fee_rate=fee_rate),
                "expected_fee_rate": fee_rate,
                "expected_edge": edge,
                "expected_edge_source": edge_source,
                "expected_edge_status": "AVAILABLE" if edge is not None else "NOT_AVAILABLE_NO_UPSTREAM_INPUT",
                "expected_edge_absence_reason": edge_absence_reason,
                "expected_edge_calibration_basis": EXPECTED_EDGE_FEE_CALIBRATION_BASIS,
                "realized_paper_outcome": _paper_outcome(intent, resolutions=resolutions),
                "measurement_only": True,
            }
        )
        signals.append(row)
    fee_gated_rows: list[dict[str, Any]] = []
    for index, intent in enumerate(fee_gated):
        edge, edge_source, edge_absence_reason = _expected_edge(intent)
        row = _intent_row(
            intent=intent,
            state=state,
            member=member,
            route_denied_reason=route_denied_reason,
            final_gate_rank=after_fee + index,
        )
        row.update(
            {
                "fee_gate_rank": index,
                "expected_fee_usd": _expected_fee_usd(intent, fee_rate=fee_rate),
                "expected_fee_rate": fee_rate,
                "expected_edge": edge,
                "expected_edge_source": edge_source,
                "expected_edge_status": "AVAILABLE" if edge is not None else "NOT_AVAILABLE_NO_UPSTREAM_INPUT",
                "expected_edge_absence_reason": edge_absence_reason,
                "expected_edge_calibration_basis": EXPECTED_EDGE_FEE_CALIBRATION_BASIS,
                "realized_paper_outcome": _paper_outcome(intent, resolutions=resolutions),
                "measurement_only": True,
            }
        )
        fee_gated_rows.append(row)
    fresh = _as_int(summary.get("fresh_candidate_intents"))
    after_toxicity = _as_int(summary.get("fresh_candidate_intents_after_toxicity_protection"))
    attrition = {
        "fresh_candidate_intents": fresh,
        "fresh_candidate_intents_after_expected_fee_gate": after_fee,
        "fresh_candidate_intents_after_toxicity_protection": after_toxicity,
        "routeable_signals": 0,
        "would_submit": 0,
    }
    evidence = {
        "candidate_id": str(member.get("candidate_id") or state.get("candidate_id") or ""),
        "source_wallet": str(member.get("source_wallet") or state.get("source_wallet") or ""),
        "status": state.get("status"),
        "generated_at": state.get("generated_at"),
        "fresh_candidate_intents": fresh,
        "fresh_candidate_intents_after_toxicity_protection": after_toxicity,
        "fresh_candidate_intents_after_expected_fee_gate": after_fee,
        "sample_intents": len(sample_intents),
        "route_denied_reason": route_denied_reason,
        "filter_attrition": attrition,
    }
    return signals, evidence, fee_gated_rows


def _load_probe_states(
    live_execution_state: dict[str, Any],
    live_probe_result: dict[str, Any],
    *,
    member_rows: list[dict[str, Any]],
    load_json_func: Callable[[Path, Any], Any],
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    states: dict[str, dict[str, Any]] = {}
    current_wallets: set[str] = set()
    selected_wallet = _wallet(live_execution_state.get("source_wallet"))
    if selected_wallet:
        states[selected_wallet] = live_execution_state
        current_wallets.add(selected_wallet)
    for row in live_probe_result.get("rows") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("source_wallet"))
        if not wallet:
            continue
        path_raw = str(row.get("path") or "")
        state: Any = {}
        if path_raw:
            path = Path(path_raw)
            if not path.is_absolute():
                path = ROOT / path
            state = load_json_func(path, {})
        if isinstance(state, dict) and state:
            states[wallet] = state
            current_wallets.add(wallet)
    for member in member_rows:
        wallet = str(member.get("source_wallet") or "")
        if not wallet or wallet in states:
            continue
        path = _probe_path_for_member(member)
        state = load_json_func(path, {})
        if isinstance(state, dict) and state:
            states[wallet] = state
    return states, current_wallets


def _state_age_s(state: dict[str, Any], generated_at: str) -> float | None:
    state_ts = _parse_ts(state.get("generated_at"))
    generated_ts = _parse_ts(generated_at)
    if state_ts is None or generated_ts is None:
        return None
    return max(0.0, generated_ts - state_ts)


def _fresh_counts_from_meta(meta: dict[str, Any]) -> tuple[int, int]:
    fresh_by_source = meta.get("fresh_buy_rows_le_10s_by_source")
    fresh_rows = sum(_as_int(value) for value in fresh_by_source.values()) if isinstance(fresh_by_source, dict) else 0
    policy_feedback = meta.get("policy_feedback") if isinstance(meta.get("policy_feedback"), dict) else {}
    policy_rows = _as_int(
        policy_feedback.get("policy_compatible_fresh_buy_rows_le_30s")
        or meta.get("policy_compatible_fresh_buy_rows_le_30s")
        or meta.get("fresh_policy_compatible_buy_rows_le_30s")
    )
    return fresh_rows, policy_rows


def _structural_exclusion_reason(
    member: dict[str, Any],
    *,
    dataapi_meta: dict[str, Any],
) -> str:
    if member.get("route_denied_reason"):
        return str(member.get("route_denied_reason") or "")
    fresh_rows, policy_rows = _fresh_counts_from_meta(dataapi_meta)
    if fresh_rows <= 0 and policy_rows <= 0:
        return "no_current_fresh_or_policy_compatible_signal_in_dataapi_meta"
    return ""


def _decision_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("cycle_generated_at") or ""),
        str(row.get("window_start_s") or ""),
        str(row.get("winning_intent_id") or ""),
    )


def _window_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("market_slug") or ""),
        str(row.get("window_start_s") or ""),
    )


def _row_has_direction_fields(row: dict[str, Any]) -> bool:
    return str(row.get("side") or "").upper() in {"YES", "NO", "BUY", "SELL"} and str(
        row.get("outcome") or ""
    ).upper() in {"UP", "DOWN", "YES", "NO"}


def _attribution_preference_key(row: dict[str, Any]) -> tuple[int, float, str, str, str]:
    observed_ts = _as_float(row.get("winning_observed_ts") or row.get("observed_ts"))
    return (
        0 if _row_has_direction_fields(row) else 1,
        observed_ts if observed_ts is not None else float("inf"),
        _wallet(row.get("winning_source_wallet") or row.get("source_wallet")),
        str(row.get("winning_intent_id") or row.get("intent_id") or ""),
        str(row.get("cycle_generated_at") or ""),
    )


def _literal_retained_row_key(row: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(row.get("market_slug") or ""),
        str(row.get("window_start_s") or ""),
        str(row.get("runtime_selected_wallet") or row.get("selected_wallet_at_cycle") or ""),
        str(bool(row.get("extra_would_submit_window"))),
        str(row.get("winning_source_wallet") or ""),
        str(row.get("winning_intent_id") or ""),
        str(row.get("winning_observed_ts") or ""),
        str(row.get("side") or ""),
        str(row.get("outcome") or ""),
        str(row.get("limit_price") or ""),
        str(row.get("shares") or ""),
    )


def _dedupe_literal_retained_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_key: dict[tuple[str, ...], dict[str, Any]] = {}
    duplicate_rows: list[dict[str, Any]] = []
    for row in rows:
        key = _literal_retained_row_key(row)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = row
            continue
        duplicate_rows.append(
            {
                "market_slug": row.get("market_slug"),
                "window_start_s": row.get("window_start_s"),
                "winning_source_wallet": row.get("winning_source_wallet"),
                "winning_source_wallet_short": _short_wallet(row.get("winning_source_wallet")),
                "winning_intent_id": row.get("winning_intent_id"),
                "winning_observed_ts": row.get("winning_observed_ts"),
                "dropped_cycle_generated_at": row.get("cycle_generated_at"),
                "kept_cycle_generated_at": existing.get("cycle_generated_at"),
            }
        )
    return sorted(by_key.values(), key=_decision_key), {
        "dedupe_rule": "drop retained .rows duplicates with identical window/wallet/intent/observed_ts/direction/price/size",
        "duplicate_rows_dropped": len(duplicate_rows),
        "duplicate_windows": len({(row.get("market_slug"), row.get("window_start_s")) for row in duplicate_rows}),
        "examples": duplicate_rows[:10],
    }


def _cycle_key(row: dict[str, Any]) -> str:
    return str(row.get("generated_at") or "")


def _fee_gated_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("cycle_generated_at") or ""),
        str(row.get("source_wallet") or ""),
        str(row.get("market_slug") or ""),
        str(row.get("window_start_s") or ""),
        str(row.get("intent_id") or ""),
    )


def _validation_elapsed_hours(cycles: list[dict[str, Any]]) -> float:
    timestamps = [_parse_ts(row.get("generated_at")) for row in cycles if isinstance(row, dict)]
    timestamps = [ts for ts in timestamps if ts is not None]
    if len(timestamps) < 2:
        return 0.0
    return round(max(0.0, max(timestamps) - min(timestamps)) / 3600.0, 6)


def _validation_clock_from_handoff(path: Path) -> float | None:
    if not path.exists():
        return None
    heading_ts: float | None = None
    earliest: float | None = None
    explicit_earliest: float | None = None
    heading_pattern = re.compile(r"^##\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})Z\b")
    elapsed_pattern = re.compile(r"\b(?:elapsed_h|validation_elapsed_hours)=([0-9]+(?:\.[0-9]+)?)")
    explicit_pattern = re.compile(r"\b([0-9]+(?:\.[0-9]+)?)@(\d{2}:\d{2})\b")
    for line in path.read_text(errors="replace").splitlines():
        heading_match = heading_pattern.match(line)
        if heading_match:
            heading_ts = _parse_ts(f"{heading_match.group(1)}:00Z")
            continue
        if heading_ts is None:
            continue
        for explicit_match in explicit_pattern.finditer(line):
            heading_dt = dt.datetime.fromtimestamp(heading_ts, tz=dt.timezone.utc)
            hour, minute = (int(part) for part in explicit_match.group(2).split(":"))
            explicit_dt = heading_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
            implied_start = explicit_dt.timestamp() - float(explicit_match.group(1)) * 3600.0
            explicit_earliest = (
                implied_start if explicit_earliest is None else min(explicit_earliest, implied_start)
            )
        if "routing_shadow" not in line:
            continue
        elapsed_match = elapsed_pattern.search(line)
        if not elapsed_match:
            continue
        implied_start = heading_ts - float(elapsed_match.group(1)) * 3600.0
        earliest = implied_start if earliest is None else min(earliest, implied_start)
    return explicit_earliest if explicit_earliest is not None else earliest


def _validation_clock_started_at(
    *,
    cycles: list[dict[str, Any]],
    previous: dict[str, Any],
    generated_at: str,
    handoff_path: Path | None = None,
) -> tuple[str | None, float]:
    candidates: list[float] = []
    timestamps = [_parse_ts(row.get("generated_at")) for row in cycles if isinstance(row, dict)]
    timestamps = [ts for ts in timestamps if ts is not None]
    if timestamps:
        candidates.append(min(timestamps))
    previous_summary = previous.get("summary") if isinstance(previous.get("summary"), dict) else {}
    previous_started = _parse_ts(previous_summary.get("validation_clock_started_at"))
    if previous_started is not None:
        candidates.append(previous_started)
    previous_generated = _parse_ts(previous.get("generated_at"))
    previous_elapsed = _as_float(previous_summary.get("validation_elapsed_hours"))
    if previous_generated is not None and previous_elapsed is not None and previous_elapsed > 0:
        candidates.append(previous_generated - previous_elapsed * 3600.0)
    handoff_started = _validation_clock_from_handoff(handoff_path or (ROOT / "docs/agents/HANDOFF.md"))
    if handoff_started is not None:
        candidates.append(handoff_started)
    report_ts = _parse_ts(generated_at)
    end_candidates = [ts for ts in timestamps]
    if report_ts is not None:
        end_candidates.append(report_ts)
    if not end_candidates or not candidates:
        return None, 0.0
    current_ts = max(end_candidates)
    started = min(candidates)
    started_iso = dt.datetime.fromtimestamp(started, tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return started_iso, round(max(0.0, current_ts - started) / 3600.0, 6)


def _wallet_transition_count(cycles: list[dict[str, Any]], key: str) -> int:
    wallets: list[str] = []
    for row in sorted(cycles, key=_cycle_key):
        wallet = _wallet(row.get(key) or row.get("selected_wallet_at_cycle"))
        if wallet:
            wallets.append(wallet)
    if len(wallets) < 2:
        return 0
    return sum(1 for previous, current in zip(wallets, wallets[1:]) if previous != current)


def _fee_gate_calibration_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_member: dict[str, dict[str, Any]] = {}
    summary: dict[str, Any] = {
        "fee_gated_intents": len(rows),
        "resolved_intents": 0,
        "unresolved_intents": 0,
        "measurable_resolved_intents": 0,
        "unmeasured_resolved_intents": 0,
        "wins": 0,
        "losses": 0,
        "expected_fee_usd_sum": 0.0,
        "expected_fee_usd_all_rows_sum": 0.0,
        "expected_fee_usd_unresolved_sum": 0.0,
        "expected_fee_usd_unmeasured_resolved_sum": 0.0,
        "expected_edge_sum": 0.0,
        "expected_edge_count": 0,
        "expected_edge_missing_count": 0,
        "expected_edge_status": "NO_FEE_GATED_ROWS",
        "expected_edge_absence_reasons": {},
        "expected_edge_calibration_basis": EXPECTED_EDGE_FEE_CALIBRATION_BASIS,
        "pnl_measurement_gap_reasons": {},
        "regime_counts": {},
        "pre_fee_pnl_usd": 0.0,
        "post_fee_pnl_usd": 0.0,
        "paper_pnl_usd": 0.0,
        "by_member": by_member,
    }

    def record_gap_reason(reason: Any, summary_row: dict[str, Any], member_row: dict[str, Any]) -> None:
        normalized = str(reason or "unknown_measurement_gap").strip() or "unknown_measurement_gap"
        summary_row["pnl_measurement_gap_reasons"][normalized] = (
            int(summary_row["pnl_measurement_gap_reasons"].get(normalized) or 0) + 1
        )
        member_row["pnl_measurement_gap_reasons"][normalized] = (
            int(member_row["pnl_measurement_gap_reasons"].get(normalized) or 0) + 1
        )

    for row in rows:
        wallet = str(row.get("source_wallet") or row.get("winning_source_wallet") or "")
        member = by_member.setdefault(
            wallet,
            {
                "source_wallet_short": _short_wallet(wallet),
                "fee_gated_intents": 0,
                "resolved_intents": 0,
                "unresolved_intents": 0,
                "measurable_resolved_intents": 0,
                "unmeasured_resolved_intents": 0,
                "wins": 0,
                "losses": 0,
                "expected_fee_usd_sum": 0.0,
                "expected_fee_usd_all_rows_sum": 0.0,
                "expected_fee_usd_unresolved_sum": 0.0,
                "expected_fee_usd_unmeasured_resolved_sum": 0.0,
                "expected_edge_sum": 0.0,
                "expected_edge_count": 0,
                "expected_edge_missing_count": 0,
                "expected_edge_status": "NO_FEE_GATED_ROWS",
                "expected_edge_absence_reasons": {},
                "expected_edge_calibration_basis": EXPECTED_EDGE_FEE_CALIBRATION_BASIS,
                "pnl_measurement_gap_reasons": {},
                "regime_counts": {},
                "pre_fee_pnl_usd": 0.0,
                "post_fee_pnl_usd": 0.0,
                "paper_pnl_usd": 0.0,
            },
        )
        member["fee_gated_intents"] += 1
        regime_data = _window_regime(row.get("market_slug"), row.get("window_start_s"))
        regime = str(row.get("regime") or regime_data.get("regime") or "unknown")
        summary["regime_counts"][regime] = int(summary["regime_counts"].get(regime) or 0) + 1
        member["regime_counts"][regime] = int(member["regime_counts"].get(regime) or 0) + 1
        if (expected_fee := _as_float(row.get("expected_fee_usd"))) is not None:
            summary["expected_fee_usd_all_rows_sum"] = round(summary["expected_fee_usd_all_rows_sum"] + expected_fee, 6)
            member["expected_fee_usd_all_rows_sum"] = round(member["expected_fee_usd_all_rows_sum"] + expected_fee, 6)
        if (expected_edge := _as_float(row.get("expected_edge"))) is not None:
            summary["expected_edge_sum"] = round(summary["expected_edge_sum"] + expected_edge, 9)
            summary["expected_edge_count"] += 1
            member["expected_edge_sum"] = round(member["expected_edge_sum"] + expected_edge, 9)
            member["expected_edge_count"] += 1
        else:
            reason = str(row.get("expected_edge_absence_reason") or row.get("expected_edge_source") or "unknown")
            summary["expected_edge_missing_count"] += 1
            member["expected_edge_missing_count"] += 1
            summary["expected_edge_absence_reasons"][reason] = (
                int(summary["expected_edge_absence_reasons"].get(reason) or 0) + 1
            )
            member["expected_edge_absence_reasons"][reason] = (
                int(member["expected_edge_absence_reasons"].get(reason) or 0) + 1
            )
        outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
        if outcome.get("status") == "RESOLVED":
            summary["resolved_intents"] += 1
            member["resolved_intents"] += 1
            if outcome.get("wins") is True:
                summary["wins"] += 1
                member["wins"] += 1
            elif outcome.get("wins") is False:
                summary["losses"] += 1
                member["losses"] += 1
            if (pnl := _as_float(outcome.get("paper_pnl_usd"))) is not None:
                summary["measurable_resolved_intents"] += 1
                member["measurable_resolved_intents"] += 1
                summary["paper_pnl_usd"] = round(summary["paper_pnl_usd"] + pnl, 6)
                member["paper_pnl_usd"] = round(member["paper_pnl_usd"] + pnl, 6)
                summary["pre_fee_pnl_usd"] = round(summary["pre_fee_pnl_usd"] + pnl, 6)
                member["pre_fee_pnl_usd"] = round(member["pre_fee_pnl_usd"] + pnl, 6)
                if expected_fee is not None:
                    summary["expected_fee_usd_sum"] = round(summary["expected_fee_usd_sum"] + expected_fee, 6)
                    member["expected_fee_usd_sum"] = round(member["expected_fee_usd_sum"] + expected_fee, 6)
                    summary["post_fee_pnl_usd"] = round(summary["post_fee_pnl_usd"] + pnl - expected_fee, 6)
                    member["post_fee_pnl_usd"] = round(member["post_fee_pnl_usd"] + pnl - expected_fee, 6)
                else:
                    summary["post_fee_pnl_usd"] = round(summary["post_fee_pnl_usd"] + pnl, 6)
                    member["post_fee_pnl_usd"] = round(member["post_fee_pnl_usd"] + pnl, 6)
            else:
                summary["unmeasured_resolved_intents"] += 1
                member["unmeasured_resolved_intents"] += 1
                if expected_fee is not None:
                    summary["expected_fee_usd_unmeasured_resolved_sum"] = round(
                        summary["expected_fee_usd_unmeasured_resolved_sum"] + expected_fee,
                        6,
                    )
                    member["expected_fee_usd_unmeasured_resolved_sum"] = round(
                        member["expected_fee_usd_unmeasured_resolved_sum"] + expected_fee,
                        6,
                    )
                record_gap_reason(outcome.get("measurement_gap_reason"), summary, member)
        else:
            summary["unresolved_intents"] += 1
            member["unresolved_intents"] += 1
            if expected_fee is not None:
                summary["expected_fee_usd_unresolved_sum"] = round(
                    summary["expected_fee_usd_unresolved_sum"] + expected_fee,
                    6,
                )
                member["expected_fee_usd_unresolved_sum"] = round(
                    member["expected_fee_usd_unresolved_sum"] + expected_fee,
                    6,
                )
            record_gap_reason(outcome.get("measurement_gap_reason") or outcome.get("status"), summary, member)
    summary["expected_edge_avg"] = (
        round(summary["expected_edge_sum"] / summary["expected_edge_count"], 9)
        if summary["expected_edge_count"]
        else None
    )
    if rows:
        if summary["expected_edge_count"] == len(rows):
            summary["expected_edge_status"] = "AVAILABLE"
        elif summary["expected_edge_count"] > 0:
            summary["expected_edge_status"] = "PARTIAL"
        else:
            summary["expected_edge_status"] = "NOT_AVAILABLE_NO_UPSTREAM_INPUT"
            summary["expected_edge_absence_reason"] = EXPECTED_EDGE_NO_INPUT_REASON
    for member in by_member.values():
        member["expected_edge_avg"] = (
            round(member["expected_edge_sum"] / member["expected_edge_count"], 9)
            if member["expected_edge_count"]
            else None
        )
        if member["fee_gated_intents"]:
            if member["expected_edge_count"] == member["fee_gated_intents"]:
                member["expected_edge_status"] = "AVAILABLE"
            elif member["expected_edge_count"] > 0:
                member["expected_edge_status"] = "PARTIAL"
            else:
                member["expected_edge_status"] = "NOT_AVAILABLE_NO_UPSTREAM_INPUT"
                member["expected_edge_absence_reason"] = EXPECTED_EDGE_NO_INPUT_REASON
    return summary


def _add_unique_window_gate_aliases(summary: dict[str, Any]) -> dict[str, Any]:
    summary["unique_windows"] = summary.get("fee_gated_intents", 0)
    summary["measured_unique_windows"] = summary.get("measurable_resolved_intents", 0)
    summary["resolved_unique_windows"] = summary.get("resolved_intents", 0)
    summary["unresolved_unique_windows"] = summary.get("unresolved_intents", 0)
    for member in (summary.get("by_member") or {}).values():
        if not isinstance(member, dict):
            continue
        member["unique_windows"] = member.get("fee_gated_intents", 0)
        member["measured_unique_windows"] = member.get("measurable_resolved_intents", 0)
        member["resolved_unique_windows"] = member.get("resolved_intents", 0)
        member["unresolved_unique_windows"] = member.get("unresolved_intents", 0)
    return summary


def _measured_window_attribution(rows: list[dict[str, Any]]) -> dict[str, str]:
    attribution: dict[str, str] = {}
    for row in rows:
        outcome = row.get("realized_paper_outcome") if isinstance(row.get("realized_paper_outcome"), dict) else {}
        if outcome.get("status") != "RESOLVED" or _as_float(outcome.get("paper_pnl_usd")) is None:
            continue
        slug = str(row.get("market_slug") or "") or f"window:{row.get('window_start_s')}"
        wallet = _wallet(row.get("winning_source_wallet"))
        if slug and wallet:
            attribution[slug] = wallet
    return dict(sorted(attribution.items()))


def _member_split_from_window_attribution(window_attribution: dict[str, str]) -> dict[str, int]:
    split: dict[str, int] = {}
    for wallet in window_attribution.values():
        split[wallet] = split.get(wallet, 0) + 1
    return dict(sorted(split.items()))


def _attribution_stability_check(
    *,
    previous: dict[str, Any],
    generated_at: str,
    window_attribution: dict[str, str],
) -> dict[str, Any]:
    current_ts = _parse_ts(generated_at)
    current_split = _member_split_from_window_attribution(window_attribution)
    previous_summary = previous.get("summary") if isinstance(previous.get("summary"), dict) else {}
    previous_stability = (
        previous_summary.get("attribution_stability")
        if isinstance(previous_summary.get("attribution_stability"), dict)
        else {}
    )
    previous_snapshots = [
        row
        for row in previous_stability.get("snapshots") or []
        if isinstance(row, dict) and isinstance(row.get("measured_window_attribution"), dict)
    ]
    comparison = None
    if current_ts is not None:
        eligible = []
        for snapshot in previous_snapshots:
            snapshot_ts = _parse_ts(snapshot.get("generated_at"))
            if snapshot_ts is None:
                continue
            if current_ts - snapshot_ts >= ATTRIBUTION_STABILITY_MIN_INTERVAL_S:
                eligible.append((snapshot_ts, snapshot))
        if eligible:
            comparison = sorted(eligible, key=lambda item: item[0])[-1][1]
    if comparison is None and previous_snapshots:
        comparison = previous_snapshots[-1]

    status = "PENDING_NO_PRIOR_SNAPSHOT"
    elapsed_s = None
    comparison_attribution: dict[str, str] = {}
    changed_windows: list[dict[str, Any]] = []
    common_window_count = 0
    if comparison is not None:
        comparison_attribution = {
            str(key): _wallet(value)
            for key, value in (comparison.get("measured_window_attribution") or {}).items()
            if _wallet(value)
        }
        comparison_ts = _parse_ts(comparison.get("generated_at"))
        if current_ts is not None and comparison_ts is not None:
            elapsed_s = round(max(0.0, current_ts - comparison_ts), 6)
        common_windows = sorted(set(comparison_attribution) & set(window_attribution))
        common_window_count = len(common_windows)
        changed_windows = [
            {
                "market_slug": slug,
                "previous_wallet": comparison_attribution[slug],
                "current_wallet": window_attribution[slug],
            }
            for slug in common_windows
            if comparison_attribution[slug] != window_attribution[slug]
        ]
        if elapsed_s is not None and elapsed_s < ATTRIBUTION_STABILITY_MIN_INTERVAL_S:
            status = "PENDING_MIN_INTERVAL"
        elif changed_windows:
            status = "FAIL_ATTRIBUTION_CHANGED"
        else:
            status = "PASS"

    snapshot = {
        "generated_at": generated_at,
        "measured_unique_windows": len(window_attribution),
        "measured_member_split": current_split,
        "measured_window_attribution": window_attribution,
    }
    snapshots = (previous_snapshots + [snapshot])[-24:]
    return {
        "status": status,
        "rule": (
            "deterministic per-window attribution must be unchanged on overlapping measured "
            "windows across two report refreshes >=10 minutes apart"
        ),
        "attribution_rule": ATTRIBUTION_TIEBREAK_RULE,
        "min_interval_s": ATTRIBUTION_STABILITY_MIN_INTERVAL_S,
        "current_generated_at": generated_at,
        "comparison_generated_at": comparison.get("generated_at") if isinstance(comparison, dict) else None,
        "comparison_elapsed_s": elapsed_s,
        "current_measured_unique_windows": len(window_attribution),
        "comparison_measured_unique_windows": len(comparison_attribution),
        "common_measured_windows": common_window_count,
        "changed_window_count": len(changed_windows),
        "changed_windows": changed_windows[:20],
        "current_measured_member_split": current_split,
        "comparison_measured_member_split": _member_split_from_window_attribution(comparison_attribution),
        "member_split_identical": current_split == _member_split_from_window_attribution(comparison_attribution)
        if comparison is not None
        else False,
        "snapshots": snapshots,
    }


def _format_usd(value: Any) -> str:
    parsed = _as_float(value)
    if parsed is None:
        return ""
    return f"{parsed:+.6f}"


def _format_gap_reasons(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return ""
    return ", ".join(f"{key}:{value[key]}" for key in sorted(value))


def render_fee_gate_calibration_table(
    report: dict[str, Any],
    *,
    source_artifact: str,
    table_generated_at: str | None = None,
) -> str:
    """Render corrected fee-gate calibration with pre/post-fee semantics."""
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    calibration = (
        summary.get("fee_gate_calibration_retained")
        if isinstance(summary.get("fee_gate_calibration_retained"), dict)
        else {}
    )
    extra_cohort = (
        summary.get("extra_would_submit_post_fee_measurement")
        if isinstance(summary.get("extra_would_submit_post_fee_measurement"), dict)
        else {}
    )
    attribution_stability = (
        summary.get("attribution_stability")
        if isinstance(summary.get("attribution_stability"), dict)
        else {}
    )
    by_member = calibration.get("by_member") if isinstance(calibration.get("by_member"), dict) else {}
    generated_at = table_generated_at or _utc_now_iso()
    source_generated_at = str(report.get("generated_at") or "")
    member_rows = sorted(
        by_member.items(),
        key=lambda item: (
            _as_float(item[1].get("post_fee_pnl_usd")) is None,
            -(_as_float(item[1].get("post_fee_pnl_usd")) or 0.0),
            str(item[0]),
        ),
    )
    lines = [
        f"# Routing Shadow Fee-Cal Realized Table - {generated_at}",
        "",
        "flow_stage: LIVE/LEARN/SELF-DEV",
        f"source_artifact: {source_artifact}",
        f"source_generated_at: {source_generated_at}",
        (
            "basis: fee_gate_calibration_retained; pre_fee_pnl_usd is the sum of "
            "paper_pnl_usd over measurable resolved rows only; "
            "post_fee_pnl_usd = pre_fee_pnl_usd - expected_fee_usd_sum over the same rows."
        ),
        (
            "measurement_gap: unmeasured_resolved_intents are resolved rows whose "
            "paper_pnl_usd is null; their fees are reported separately and never coerced to $0."
        ),
        "",
        "## Aggregate",
        "",
        f"- fee_gated_intents: {calibration.get('fee_gated_intents', 0)}",
        f"- resolved_intents: {calibration.get('resolved_intents', 0)}",
        f"- measurable_resolved_intents: {calibration.get('measurable_resolved_intents', 0)}",
        f"- unmeasured_resolved_intents: {calibration.get('unmeasured_resolved_intents', 0)}",
        f"- unresolved_intents: {calibration.get('unresolved_intents', 0)}",
        f"- expected_fee_usd_sum_measurable: {_format_usd(calibration.get('expected_fee_usd_sum'))}",
        f"- expected_fee_usd_all_rows_sum: {_format_usd(calibration.get('expected_fee_usd_all_rows_sum'))}",
        f"- expected_fee_usd_unmeasured_resolved_sum: {_format_usd(calibration.get('expected_fee_usd_unmeasured_resolved_sum'))}",
        f"- expected_fee_usd_unresolved_sum: {_format_usd(calibration.get('expected_fee_usd_unresolved_sum'))}",
        f"- pre_fee_pnl_usd: {_format_usd(calibration.get('pre_fee_pnl_usd'))}",
        f"- post_fee_pnl_usd: {_format_usd(calibration.get('post_fee_pnl_usd'))}",
        f"- regime_counts: {_format_gap_reasons(calibration.get('regime_counts'))}",
        f"- expected_edge_status: {calibration.get('expected_edge_status')}",
        f"- pnl_measurement_gap_reasons: {_format_gap_reasons(calibration.get('pnl_measurement_gap_reasons'))}",
        "",
        "## Extra Would-Submit Cohort",
        "",
        (
            "basis: retained routing rows where extra_would_submit_window=true; "
            "this is the mechanical Stage-2 promotion gate from Fable 2026-07-10T13:24Z."
        ),
        f"- attribution_rule: {summary.get('attribution_rule') or ATTRIBUTION_TIEBREAK_RULE}",
        f"- attribution_stability_status: {attribution_stability.get('status', '')}",
        f"- attribution_stability_common_windows: {attribution_stability.get('common_measured_windows', 0)}",
        f"- attribution_stability_changed_windows: {attribution_stability.get('changed_window_count', 0)}",
        f"- fee_gated_intents: {extra_cohort.get('fee_gated_intents', 0)}",
        f"- unique_windows: {extra_cohort.get('unique_windows', extra_cohort.get('fee_gated_intents', 0))}",
        f"- resolved_intents: {extra_cohort.get('resolved_intents', 0)}",
        f"- measurable_resolved_intents: {extra_cohort.get('measurable_resolved_intents', 0)}",
        f"- measured_unique_windows: {extra_cohort.get('measured_unique_windows', extra_cohort.get('measurable_resolved_intents', 0))}",
        f"- unmeasured_resolved_intents: {extra_cohort.get('unmeasured_resolved_intents', 0)}",
        f"- unresolved_intents: {extra_cohort.get('unresolved_intents', 0)}",
        f"- expected_fee_usd_sum_measurable: {_format_usd(extra_cohort.get('expected_fee_usd_sum'))}",
        f"- pre_fee_pnl_usd: {_format_usd(extra_cohort.get('pre_fee_pnl_usd'))}",
        f"- post_fee_pnl_usd: {_format_usd(extra_cohort.get('post_fee_pnl_usd'))}",
        f"- regime_counts: {_format_gap_reasons(extra_cohort.get('regime_counts'))}",
        f"- pnl_measurement_gap_reasons: {_format_gap_reasons(extra_cohort.get('pnl_measurement_gap_reasons'))}",
        "- promote_if: post_fee_pnl_usd > 0 and measured_unique_windows >= 30",
        f"- gate_result: {'PROMOTE_ALLOWED' if (_as_float(extra_cohort.get('post_fee_pnl_usd')) or 0.0) > 0.0 and _as_int(extra_cohort.get('measured_unique_windows', extra_cohort.get('measurable_resolved_intents'))) >= 30 else 'EXTEND_SHADOW'}",
        "",
        "### Extra Cohort Per-Member",
        "",
        "| wallet | regime_counts | unique_windows | resolved | measured_unique_windows | unresolved | wins-losses | fee_measured | pre_fee | post_fee |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    extra_by_member = extra_cohort.get("by_member") if isinstance(extra_cohort.get("by_member"), dict) else {}
    for wallet, member in sorted(
        extra_by_member.items(),
        key=lambda item: (
            -(_as_float(item[1].get("post_fee_pnl_usd")) or 0.0),
            str(item[0]),
        ),
    ):
        lines.append(
            "| "
            f"{member.get('source_wallet_short') or _short_wallet(wallet)} | "
            f"{_format_gap_reasons(member.get('regime_counts'))} | "
            f"{member.get('unique_windows', member.get('fee_gated_intents', 0))} | "
            f"{member.get('resolved_intents', 0)} | "
            f"{member.get('measured_unique_windows', member.get('measurable_resolved_intents', 0))} | "
            f"{member.get('unresolved_intents', 0)} | "
            f"{member.get('wins', 0)}-{member.get('losses', 0)} | "
            f"{_format_usd(member.get('expected_fee_usd_sum'))} | "
            f"{_format_usd(member.get('pre_fee_pnl_usd'))} | "
            f"{_format_usd(member.get('post_fee_pnl_usd'))} |"
        )
    lines.extend(
        [
        "",
        "## Per-Member Table",
        "",
        "| wallet | regime_counts | fee_gated | resolved | measured | unmeasured | unresolved | wins-losses | fee_measured | pre_fee | post_fee | fee_unmeasured | fee_unresolved | gap_reasons |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for wallet, member in member_rows:
        lines.append(
            "| "
            f"{member.get('source_wallet_short') or _short_wallet(wallet)} | "
            f"{_format_gap_reasons(member.get('regime_counts'))} | "
            f"{member.get('fee_gated_intents', 0)} | "
            f"{member.get('resolved_intents', 0)} | "
            f"{member.get('measurable_resolved_intents', 0)} | "
            f"{member.get('unmeasured_resolved_intents', 0)} | "
            f"{member.get('unresolved_intents', 0)} | "
            f"{member.get('wins', 0)}-{member.get('losses', 0)} | "
            f"{_format_usd(member.get('expected_fee_usd_sum'))} | "
            f"{_format_usd(member.get('pre_fee_pnl_usd'))} | "
            f"{_format_usd(member.get('post_fee_pnl_usd'))} | "
            f"{_format_usd(member.get('expected_fee_usd_unmeasured_resolved_sum'))} | "
            f"{_format_usd(member.get('expected_fee_usd_unresolved_sum'))} | "
            f"{_format_gap_reasons(member.get('pnl_measurement_gap_reasons'))} |"
        )
    lines.extend(
        [
            "",
            "## Ruling Guardrail",
            "",
            "- Measurement only: no threshold, roster, wallet-count, or live-flip mutation is implied by this artifact.",
            "- Fable ruling 2026-07-10T10:33Z retains the fee gate unchanged until a future primary-Fable direction says otherwise.",
            "",
        ]
    )
    return "\n".join(lines)


def write_fee_gate_calibration_table(
    report: dict[str, Any],
    *,
    out_path: Path,
    source_artifact: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = render_fee_gate_calibration_table(report, source_artifact=source_artifact)
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    tmp_path.write_text(table, encoding="utf-8")
    tmp_path.replace(out_path)


def _runtime_selected_wallet_with_source(
    active_set_runtime: dict[str, Any],
    live_execution_state: dict[str, Any],
) -> tuple[str, str]:
    selected = (
        active_set_runtime.get("selected_member")
        if isinstance(active_set_runtime.get("selected_member"), dict)
        else {}
    )
    selected_wallet = _wallet(selected.get("source_wallet") or selected.get("wallet"))
    if selected_wallet:
        return selected_wallet, "selected_member"

    fresh_selection = (
        active_set_runtime.get("fresh_runtime_member_selection")
        if isinstance(active_set_runtime.get("fresh_runtime_member_selection"), dict)
        else {}
    )
    fresh_wallet = _wallet(fresh_selection.get("selected_wallet"))
    if fresh_wallet:
        return fresh_wallet, "fresh_runtime_member_selection"

    live_wallet = _wallet(live_execution_state.get("source_wallet"))
    if live_wallet:
        return live_wallet, "live_execution_state_fallback"

    return "", "missing"


def build_routing_shadow_validation(
    *,
    active_set_runtime: dict[str, Any],
    live_execution_state: dict[str, Any],
    live_probe_result: dict[str, Any],
    active_set_dataapi_poller: dict[str, Any] | None = None,
    selection_priority_freeze: dict[str, Any],
    previous: dict[str, Any] | None = None,
    generated_at: str | None = None,
    min_validation_hours: float = DEFAULT_MIN_VALIDATION_HOURS,
    retain_rows: int = DEFAULT_RETAIN_ROWS,
    retain_cycles: int = DEFAULT_RETAIN_CYCLES,
    current_state_max_age_s: float = DEFAULT_CURRENT_STATE_MAX_AGE_S,
    load_json_func: Callable[[Path, Any], Any] = load_json,
    last_successful_wallet: str = "",
    resolution_map: dict[str, dict[str, Any]] | None = None,
    shadow_candidate_members: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or _utc_now_iso()
    previous = previous if isinstance(previous, dict) else {}
    active_set_runtime, shadow_seats_added = _runtime_with_shadow_candidate_seats(
        active_set_runtime,
        shadow_candidate_members,
    )
    runtime_selected_wallet, runtime_selected_wallet_source = _runtime_selected_wallet_with_source(
        active_set_runtime,
        live_execution_state,
    )
    live_payload_wallet = _wallet(live_execution_state.get("source_wallet"))
    member_rows, member_by_wallet = _runtime_member_rows(
        active_set_runtime,
        selection_priority_freeze=selection_priority_freeze,
    )
    execution_states, current_state_wallets = _load_probe_states(
        live_execution_state,
        live_probe_result,
        member_rows=member_rows,
        load_json_func=load_json_func,
    )
    active_set_dataapi_poller = active_set_dataapi_poller if isinstance(active_set_dataapi_poller, dict) else {}
    fetch_meta = (
        active_set_dataapi_poller.get("fetch_meta")
        if isinstance(active_set_dataapi_poller.get("fetch_meta"), dict)
        else {}
    )
    resolutions = resolution_map if isinstance(resolution_map, dict) else _load_resolution_map()

    all_signals: list[dict[str, Any]] = []
    fee_gated_measurement_rows: list[dict[str, Any]] = []
    member_evidence: list[dict[str, Any]] = []
    parity_conflicts: list[dict[str, Any]] = []
    for wallet, member in member_by_wallet.items():
        state = execution_states.get(wallet, {})
        state_age = _state_age_s(state, generated_at) if state else None
        dataapi_meta = fetch_meta.get(wallet) if isinstance(fetch_meta.get(wallet), dict) else {}
        structural_exclusion_reason = _structural_exclusion_reason(member, dataapi_meta=dataapi_meta)
        if not state:
            probe_status = "STRUCTURAL_EXCLUSION" if structural_exclusion_reason else "MISSING_CURRENT_EXECUTION_STATE"
            member_evidence.append(
                {
                    **member,
                    "probe_status": probe_status,
                    "structural_exclusion_reason": structural_exclusion_reason,
                    "fresh_candidate_intents_after_expected_fee_gate": 0,
                    "sample_intents": 0,
                    "filter_attrition": {
                        "fresh_candidate_intents": 0,
                        "fresh_candidate_intents_after_expected_fee_gate": 0,
                        "fresh_candidate_intents_after_toxicity_protection": 0,
                        "routeable_signals": 0,
                        "would_submit": 0,
                    },
                }
            )
            continue
        current_state = wallet in current_state_wallets
        if not current_state and state_age is not None and state_age > float(current_state_max_age_s):
            probe_status = "STRUCTURAL_EXCLUSION" if structural_exclusion_reason else "STALE_EXECUTION_STATE"
            summary = _candidate_summary(state)
            member_evidence.append(
                {
                    **member,
                    "probe_status": probe_status,
                    "structural_exclusion_reason": structural_exclusion_reason,
                    "generated_at": state.get("generated_at"),
                    "execution_state_age_s": round(state_age, 6),
                    "fresh_candidate_intents": _as_int(summary.get("fresh_candidate_intents")),
                    "fresh_candidate_intents_after_toxicity_protection": _as_int(
                        summary.get("fresh_candidate_intents_after_toxicity_protection")
                    ),
                    "fresh_candidate_intents_after_expected_fee_gate": _as_int(
                        summary.get("fresh_candidate_intents_after_expected_fee_gate")
                    ),
                    "sample_intents": len(
                        [row for row in summary.get("sample_intents") or [] if isinstance(row, dict)]
                    ),
                    "filter_attrition": {
                        "fresh_candidate_intents": _as_int(summary.get("fresh_candidate_intents")),
                        "fresh_candidate_intents_after_expected_fee_gate": _as_int(
                            summary.get("fresh_candidate_intents_after_expected_fee_gate")
                        ),
                        "fresh_candidate_intents_after_toxicity_protection": _as_int(
                            summary.get("fresh_candidate_intents_after_toxicity_protection")
                        ),
                        "routeable_signals": 0,
                        "would_submit": 0,
                    },
                }
            )
            continue
        route_denied_reason = str(member.get("route_denied_reason") or "")
        signals, evidence, fee_gated_rows = _signals_from_execution_state(
            state,
            member=member,
            route_denied_reason=route_denied_reason,
            resolutions=resolutions,
        )
        fee_gated_measurement_rows.extend(fee_gated_rows)
        member_evidence.append(
            {
                **member,
                **evidence,
                "probe_status": "PASS",
                "current_execution_state": current_state,
                "execution_state_age_s": None if state_age is None else round(state_age, 6),
                "structural_exclusion_reason": str(member.get("route_denied_reason") or ""),
            }
        )
        all_signals.extend(signals)
        parity_conflicts.extend(
            {
                "source_wallet": signal.get("source_wallet"),
                "candidate_id": signal.get("candidate_id"),
                "intent_id": signal.get("intent_id"),
                "copyintent_source_wallet": signal.get("copyintent_source_wallet"),
                "market_slug": signal.get("market_slug"),
            }
            for signal in signals
            if signal.get("copyintent_parity_conflict")
        )

    denied_signals = [signal for signal in all_signals if signal.get("route_denied_reason")]
    routeable_signals = [
        signal
        for signal in all_signals
        if not signal.get("route_denied_reason")
        and not signal.get("copyintent_parity_conflict")
        and signal.get("window_start_s") is not None
        and signal.get("observed_ts") is not None
    ]
    member_index = {row["source_wallet"]: int(row.get("member_index") or 0) for row in member_rows}
    last_successful_wallet = _wallet(last_successful_wallet)
    by_window: dict[tuple[float, str], list[dict[str, Any]]] = {}
    for signal in routeable_signals:
        key = (float(signal["window_start_s"]), str(signal.get("market_slug") or ""))
        by_window.setdefault(key, []).append(signal)

    decisions: list[dict[str, Any]] = []
    suppressed_rows: list[dict[str, Any]] = []
    for (window_start, market_slug), signals in sorted(by_window.items()):
        ordered = sorted(
            signals,
            key=lambda signal: (
                float(signal["observed_ts"]),
                _wallet(signal.get("source_wallet")),
                str(signal.get("intent_id") or ""),
            ),
        )
        winner = ordered[0]
        suppressed = ordered[1:]
        selected_signals = [signal for signal in ordered if signal.get("source_wallet") == runtime_selected_wallet]
        suppressed_rows.extend(
            {
                "cycle_generated_at": generated_at,
                "window_start_s": window_start,
                "market_slug": market_slug,
                "suppressed_source_wallet": signal.get("source_wallet"),
                "suppressed_candidate_id": signal.get("candidate_id"),
                "suppressed_intent_id": signal.get("intent_id"),
                "winning_source_wallet": winner.get("source_wallet"),
                "winning_candidate_id": winner.get("candidate_id"),
                "reason": "routing_shadow_one_position_per_window",
            }
            for signal in suppressed
        )
        decisions.append(
            {
                "cycle_generated_at": generated_at,
                "window_start_s": window_start,
                "market_slug": market_slug,
                "winning_source_wallet": winner.get("source_wallet"),
                "winning_source_wallet_short": _short_wallet(winner.get("source_wallet")),
                "winning_candidate_id": winner.get("candidate_id"),
                "winning_policy_id": winner.get("policy_id"),
                "winning_intent_id": winner.get("intent_id"),
                "winning_observed_ts": winner.get("observed_ts"),
                "limit_price": winner.get("limit_price"),
                "shares": winner.get("shares"),
                "side": winner.get("side"),
                "outcome": winner.get("outcome"),
                "expected_fee_usd": winner.get("expected_fee_usd"),
                "expected_fee_rate": winner.get("expected_fee_rate"),
                "expected_edge": winner.get("expected_edge"),
                "expected_edge_source": winner.get("expected_edge_source"),
                "expected_edge_status": winner.get("expected_edge_status"),
                "expected_edge_absence_reason": winner.get("expected_edge_absence_reason"),
                "expected_edge_calibration_basis": winner.get("expected_edge_calibration_basis"),
                "realized_paper_outcome": winner.get("realized_paper_outcome"),
                "measurement_only": True,
                "runtime_selected_wallet": runtime_selected_wallet,
                "runtime_selected_wallet_source": runtime_selected_wallet_source,
                "runtime_selected_wallet_short": _short_wallet(runtime_selected_wallet),
                "live_payload_wallet": live_payload_wallet,
                "live_payload_wallet_short": _short_wallet(live_payload_wallet),
                "shadow_selected_wallet": winner.get("source_wallet"),
                "shadow_selected_wallet_short": _short_wallet(winner.get("source_wallet")),
                "selected_wallet_at_cycle": runtime_selected_wallet,
                "selected_wallet_signal_present": bool(selected_signals),
                "would_change_selected_wallet": bool(winner.get("source_wallet") != runtime_selected_wallet),
                "extra_would_submit_window": bool(
                    winner.get("source_wallet") != runtime_selected_wallet and not selected_signals
                ),
                "routing_suppressed_signals": len(suppressed),
                "suppressed_source_wallets": sorted({str(signal.get("source_wallet") or "") for signal in suppressed}),
                "tiebreak_rule": ATTRIBUTION_TIEBREAK_RULE,
                "paper_only": True,
                "live_orders_allowed": False,
            }
        )

    routeable_by_wallet: dict[str, int] = {}
    would_submit_by_wallet: dict[str, int] = {}
    for signal in routeable_signals:
        wallet = str(signal.get("source_wallet") or "")
        if wallet:
            routeable_by_wallet[wallet] = routeable_by_wallet.get(wallet, 0) + 1
    for row in decisions:
        wallet = str(row.get("winning_source_wallet") or "")
        if wallet:
            would_submit_by_wallet[wallet] = would_submit_by_wallet.get(wallet, 0) + 1
    for row in member_evidence:
        wallet = str(row.get("source_wallet") or "")
        attrition = row.get("filter_attrition") if isinstance(row.get("filter_attrition"), dict) else {}
        attrition["routeable_signals"] = int(routeable_by_wallet.get(wallet) or 0)
        attrition["would_submit"] = int(would_submit_by_wallet.get(wallet) or 0)
        row["filter_attrition"] = attrition

    previous_rows = [row for row in previous.get("rows") or [] if isinstance(row, dict)]
    rows_by_key = {_decision_key(row): row for row in previous_rows}
    rows_by_key.update({_decision_key(row): row for row in decisions})
    retained_rows = sorted(rows_by_key.values(), key=_decision_key)[-max(1, int(retain_rows)) :]

    for row in retained_rows:
        row["realized_paper_outcome"] = _paper_outcome(row, resolutions=resolutions)
    retained_rows, literal_dedupe = _dedupe_literal_retained_rows(retained_rows)

    previous_suppressed = [row for row in previous.get("routing_suppressed_rows") or [] if isinstance(row, dict)]
    suppressed = (previous_suppressed + suppressed_rows)[-max(1, min(int(retain_rows), 1000)) :]

    for row in fee_gated_measurement_rows:
        row["cycle_generated_at"] = generated_at
    previous_fee_gated = [
        row for row in previous.get("fee_gated_measurement_rows") or [] if isinstance(row, dict)
    ]
    fee_gated_by_key = {_fee_gated_key(row): row for row in previous_fee_gated if _fee_gated_key(row)}
    fee_gated_by_key.update({_fee_gated_key(row): row for row in fee_gated_measurement_rows})
    retained_fee_gated = sorted(fee_gated_by_key.values(), key=_fee_gated_key)[-max(1, int(retain_rows)) :]
    for row in retained_fee_gated:
        row["realized_paper_outcome"] = _paper_outcome(row, resolutions=resolutions)
        row["expected_edge_calibration_basis"] = EXPECTED_EDGE_FEE_CALIBRATION_BASIS
        if _as_float(row.get("expected_edge")) is None:
            if str(row.get("expected_edge_source") or "") == "missing_expected_edge_input":
                row["expected_edge_source"] = EXPECTED_EDGE_NO_INPUT_SOURCE
            row["expected_edge_status"] = "NOT_AVAILABLE_NO_UPSTREAM_INPUT"
            row["expected_edge_absence_reason"] = EXPECTED_EDGE_NO_INPUT_REASON
        else:
            row["expected_edge_status"] = "AVAILABLE"
            row["expected_edge_absence_reason"] = None

    member_split: dict[str, int] = {}
    extra_member_split: dict[str, int] = {}
    for row in retained_rows:
        wallet = str(row.get("winning_source_wallet") or "")
        if wallet:
            member_split[wallet] = member_split.get(wallet, 0) + 1
            if row.get("extra_would_submit_window"):
                extra_member_split[wallet] = extra_member_split.get(wallet, 0) + 1

    non_denied_rows = [row for row in member_evidence if row.get("enabled") and not row.get("route_denied")]
    non_denied_evaluated = [row for row in non_denied_rows if row.get("probe_status") == "PASS"]
    non_denied_accounted = [
        row
        for row in non_denied_rows
        if row.get("probe_status") == "PASS"
        or (row.get("probe_status") == "STRUCTURAL_EXCLUSION" and row.get("structural_exclusion_reason"))
    ]
    non_denied_missing = [
        {
            "source_wallet": row.get("source_wallet"),
            "candidate_id": row.get("candidate_id"),
            "probe_status": row.get("probe_status"),
            "structural_exclusion_reason": row.get("structural_exclusion_reason"),
        }
        for row in non_denied_rows
        if row not in non_denied_accounted
    ]
    aggregate_attrition = {
        "fresh_candidate_intents": sum(
            _as_int((row.get("filter_attrition") or {}).get("fresh_candidate_intents"))
            for row in member_evidence
            if isinstance(row.get("filter_attrition"), dict)
        ),
        "fresh_candidate_intents_after_expected_fee_gate": sum(
            _as_int((row.get("filter_attrition") or {}).get("fresh_candidate_intents_after_expected_fee_gate"))
            for row in member_evidence
            if isinstance(row.get("filter_attrition"), dict)
        ),
        "fresh_candidate_intents_after_toxicity_protection": sum(
            _as_int((row.get("filter_attrition") or {}).get("fresh_candidate_intents_after_toxicity_protection"))
            for row in member_evidence
            if isinstance(row.get("filter_attrition"), dict)
        ),
        "routeable_signals": sum(
            _as_int((row.get("filter_attrition") or {}).get("routeable_signals"))
            for row in member_evidence
            if isinstance(row.get("filter_attrition"), dict)
        ),
        "would_submit": sum(
            _as_int((row.get("filter_attrition") or {}).get("would_submit"))
            for row in member_evidence
            if isinstance(row.get("filter_attrition"), dict)
        ),
    }

    cycle_sample = {
        "generated_at": generated_at,
        "runtime_selected_wallet": runtime_selected_wallet,
        "runtime_selected_wallet_source": runtime_selected_wallet_source,
        "runtime_selected_wallet_short": _short_wallet(runtime_selected_wallet),
        "live_payload_wallet": live_payload_wallet,
        "live_payload_wallet_short": _short_wallet(live_payload_wallet),
        "shadow_selected_wallet": decisions[0].get("shadow_selected_wallet") if decisions else None,
        "shadow_selected_wallet_short": decisions[0].get("shadow_selected_wallet_short") if decisions else None,
        "shadow_selected_wallets": sorted(
            {str(row.get("shadow_selected_wallet") or "") for row in decisions if row.get("shadow_selected_wallet")}
        ),
        "selected_wallet_at_cycle": runtime_selected_wallet,
        "runtime_member_count": len(member_rows),
        "non_denied_runtime_members": len(non_denied_rows),
        "evaluated_member_count": len(non_denied_evaluated),
        "coverage_accounted_member_count": len(non_denied_accounted),
        "routeable_signal_count": len(routeable_signals),
        "denied_signal_count": len(denied_signals),
        "would_submit_windows": len(decisions),
        "extra_would_submit_windows": len([row for row in decisions if row.get("extra_would_submit_window")]),
        "routing_suppressed_signals": len(suppressed_rows),
        "parity_conflicts": len(parity_conflicts),
    }
    cycles_by_key = {
        _cycle_key(row): row
        for row in previous.get("cycle_samples") or []
        if isinstance(row, dict) and _cycle_key(row)
    }
    cycles_by_key[generated_at] = cycle_sample
    cycle_samples = sorted(cycles_by_key.values(), key=_cycle_key)[-max(1, int(retain_cycles)) :]
    buffer_elapsed_hours = _validation_elapsed_hours(cycle_samples)
    validation_clock_started_at, elapsed_hours = _validation_clock_started_at(
        cycles=cycle_samples,
        previous=previous,
        generated_at=generated_at,
    )
    latest_shadow_wallets = cycle_sample["shadow_selected_wallets"]
    latest_shadow_selected_wallet = str(cycle_sample.get("shadow_selected_wallet") or "")
    fee_gate_latest_summary = _add_unique_window_gate_aliases(
        _fee_gate_calibration_summary(fee_gated_measurement_rows)
    )
    fee_gate_retained_summary = _add_unique_window_gate_aliases(
        _fee_gate_calibration_summary(retained_fee_gated)
    )
    # One paper position per window: a real submission fires once, so measure
    # the deterministic attributed row per window instead of one row per cycle.
    # Direction-carrying rows beat legacy side-less rows; otherwise attribution
    # is earliest observed timestamp with lexicographic wallet tie-break.
    extra_rows_by_window: dict[tuple[str, str], dict[str, Any]] = {}
    for row in retained_rows:
        if not row.get("extra_would_submit_window"):
            continue
        key = _window_key(row)
        existing = extra_rows_by_window.get(key)
        if existing is None:
            extra_rows_by_window[key] = row
            continue
        if _attribution_preference_key(row) < _attribution_preference_key(existing):
            extra_rows_by_window[key] = row
    extra_attribution_rows = list(extra_rows_by_window.values())
    extra_would_submit_measurement = _add_unique_window_gate_aliases(
        _fee_gate_calibration_summary(extra_attribution_rows)
    )
    extra_measured_attribution = _measured_window_attribution(extra_attribution_rows)
    attribution_stability = _attribution_stability_check(
        previous=previous,
        generated_at=generated_at,
        window_attribution=extra_measured_attribution,
    )
    parity_conflict_count = len(parity_conflicts)
    all_non_denied_evaluated = cycle_sample["evaluated_member_count"] >= cycle_sample["non_denied_runtime_members"]
    all_non_denied_accounted = (
        cycle_sample["coverage_accounted_member_count"] >= cycle_sample["non_denied_runtime_members"]
    )
    status = "CONFLICT" if parity_conflict_count else "ACCUMULATING"
    if not parity_conflict_count and elapsed_hours >= float(min_validation_hours):
        measured_windows = _as_int(
            extra_would_submit_measurement.get(
                "measured_unique_windows",
                extra_would_submit_measurement.get("measurable_resolved_intents"),
            )
        )
        post_fee_pnl = _as_float(extra_would_submit_measurement.get("post_fee_pnl_usd")) or 0.0
        if measured_windows >= 30 and post_fee_pnl > 0.0:
            status = "READY_FOR_FABLE_STAGE2_RULING"
        else:
            status = "ACCRUING_UNDER_PREREGISTERED_CLOCK"

    summary = {
        "status": status,
        "routing_mode": "shadow",
        "min_validation_hours": float(min_validation_hours),
        "validation_elapsed_hours": elapsed_hours,
        "validation_clock_started_at": validation_clock_started_at,
        "validation_buffer_elapsed_hours": buffer_elapsed_hours,
        "validation_elapsed_basis": "persisted_clock_started_at",
        "cycle_samples": len(cycle_samples),
        "runtime_member_count": len(member_rows),
        "shadow_candidate_seat_count": len(shadow_seats_added),
        "shadow_candidate_seat_wallets": [row.get("source_wallet") for row in shadow_seats_added],
        "non_denied_runtime_members": cycle_sample["non_denied_runtime_members"],
        "evaluated_member_count": cycle_sample["evaluated_member_count"],
        "coverage_accounted_member_count": cycle_sample["coverage_accounted_member_count"],
        "all_non_denied_members_evaluated_this_cycle": all_non_denied_evaluated,
        "all_non_denied_members_accounted_this_cycle": all_non_denied_accounted,
        "missing_non_denied_members": non_denied_missing,
        "would_submit_windows": len(retained_rows),
        "extra_would_submit_windows": len([row for row in retained_rows if row.get("extra_would_submit_window")]),
        "selected_member_would_submit_windows": len(
            [
                row
                for row in retained_rows
                if row.get("winning_source_wallet")
                == (row.get("runtime_selected_wallet") or row.get("selected_wallet_at_cycle"))
            ]
        ),
        "routing_suppressed_signals": len(suppressed),
        "denied_signal_count_latest_cycle": len(denied_signals),
        "one_position_cap_suppressed_windows": len(
            [row for row in retained_rows if int(row.get("routing_suppressed_signals") or 0) > 0]
        ),
        "copyintent_parity_conflicts": parity_conflict_count,
        "copyintent_parity_status": "PASS" if parity_conflict_count == 0 else "CONFLICT",
        "runtime_selected_wallet": runtime_selected_wallet,
        "runtime_selected_wallet_source": runtime_selected_wallet_source,
        "runtime_selected_wallet_short": _short_wallet(runtime_selected_wallet),
        "live_payload_wallet": live_payload_wallet,
        "live_payload_wallet_short": _short_wallet(live_payload_wallet),
        "shadow_selected_wallet": latest_shadow_selected_wallet or None,
        "shadow_selected_wallet_short": _short_wallet(latest_shadow_selected_wallet),
        "shadow_selected_wallets_latest_cycle": latest_shadow_wallets,
        "selection_changes": _wallet_transition_count(cycle_samples, "runtime_selected_wallet"),
        "shadow_selection_changes": _wallet_transition_count(cycle_samples, "shadow_selected_wallet"),
        "selected_wallet_at_cycle": runtime_selected_wallet,
        "selected_wallet_at_cycle_short": _short_wallet(runtime_selected_wallet),
        "member_split": dict(sorted(member_split.items())),
        "extra_member_split": dict(sorted(extra_member_split.items())),
        "attribution_rule": ATTRIBUTION_TIEBREAK_RULE,
        "attribution_literal_dedupe": literal_dedupe,
        "attribution_stability": attribution_stability,
        "filter_attrition_totals_latest_cycle": aggregate_attrition,
        "fee_gate_calibration_latest_cycle": fee_gate_latest_summary,
        "fee_gate_calibration_retained": fee_gate_retained_summary,
        "extra_would_submit_post_fee_measurement": extra_would_submit_measurement,
        "live_flip_allowed": False,
        "next_action": (
            "continue shadow accumulation to >=6h, n>=30, or the 20:30Z freeze"
            if status == "ACCUMULATING"
            else "apply precommitted Stage-2 gate: promote only if extra-cohort post-fee PnL >0 and n>=30; otherwise extend shadow"
            if status == "READY_FOR_FABLE_STAGE2_RULING"
            else "continue pre-registered Stage-2 clock until n>=30 or 20:30Z freeze; no live flip from min-hours alone"
            if status == "ACCRUING_UNDER_PREREGISTERED_CLOCK"
            else "fix CopyIntent parity conflict before live router consideration"
        ),
    }

    return {
        "schema_version": 1,
        "kind": "routing_shadow_validation",
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "generated_at": generated_at,
        "enabled": True,
        "routing_mode": "shadow",
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the only live order submitter; this report submits zero orders",
        "rule": (
            "first eligible member signal by earliest observed_ts wins one position per window; "
            "ties break by lexicographic wallet; later signals are routing-suppressed"
        ),
        "summary": summary,
        "member_evidence": member_evidence,
        "cycle_samples": cycle_samples,
        "rows": retained_rows,
        "routing_suppressed_rows": suppressed,
        "denied_signal_rows": denied_signals[-100:],
        "fee_gated_measurement_rows": retained_fee_gated,
        "copyintent_parity_conflicts": parity_conflicts[:100],
    }


def build_routing_shadow_validation_from_guard(
    guard_state: dict[str, Any],
    *,
    previous: dict[str, Any] | None = None,
    generated_at: str | None = None,
    min_validation_hours: float = DEFAULT_MIN_VALIDATION_HOURS,
    retain_rows: int = DEFAULT_RETAIN_ROWS,
    retain_cycles: int = DEFAULT_RETAIN_CYCLES,
    load_json_func: Callable[[Path, Any], Any] = load_json,
    last_successful_wallet: str = "",
    shadow_candidate_members: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    probe_promotions = (
        guard_state.get("active_set_live_execution_probe_promotions")
        if isinstance(guard_state.get("active_set_live_execution_probe_promotions"), dict)
        else {}
    )
    return build_routing_shadow_validation(
        active_set_runtime=guard_state.get("active_set_runtime")
        if isinstance(guard_state.get("active_set_runtime"), dict)
        else {},
        live_execution_state=guard_state.get("live_execution")
        if isinstance(guard_state.get("live_execution"), dict)
        else {},
        live_probe_result=guard_state.get("active_set_live_execution_probes")
        if isinstance(guard_state.get("active_set_live_execution_probes"), dict)
        else {},
        active_set_dataapi_poller=guard_state.get("active_set_dataapi_poller")
        if isinstance(guard_state.get("active_set_dataapi_poller"), dict)
        else {},
        selection_priority_freeze=probe_promotions.get("selection_priority_freeze")
        if isinstance(probe_promotions.get("selection_priority_freeze"), dict)
        else {},
        previous=previous,
        generated_at=generated_at or str(guard_state.get("generated_at") or "") or None,
        min_validation_hours=min_validation_hours,
        retain_rows=retain_rows,
        retain_cycles=retain_cycles,
        load_json_func=load_json_func,
        last_successful_wallet=last_successful_wallet,
        shadow_candidate_members=shadow_candidate_members
        if shadow_candidate_members is not None
        else load_shadow_candidate_seats(
            guard_state.get("routing_shadow_candidate_seats_state")
            or DEFAULT_SHADOW_CANDIDATE_SEATS
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--out", default=DEFAULT_OUTPUT)
    parser.add_argument("--previous-report", default="")
    parser.add_argument("--fee-cal-table-out", default="")
    parser.add_argument("--shadow-candidate-seats", default=DEFAULT_SHADOW_CANDIDATE_SEATS)
    parser.add_argument("--min-validation-hours", type=float, default=DEFAULT_MIN_VALIDATION_HOURS)
    parser.add_argument("--retain-rows", type=int, default=DEFAULT_RETAIN_ROWS)
    parser.add_argument("--retain-cycles", type=int, default=DEFAULT_RETAIN_CYCLES)
    parser.add_argument(
        "--r7-dated-snapshot",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write <basename>_r7_<UTC>.json before refreshing the latest report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    guard_path = Path(args.guard_state)
    if not guard_path.is_absolute():
        guard_path = ROOT / guard_path
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = ROOT / out_path
    previous_path = Path(args.previous_report) if args.previous_report else out_path
    if not previous_path.is_absolute():
        previous_path = ROOT / previous_path
    guard_state = load_json(guard_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    previous = load_json(previous_path, default={})
    previous = previous if isinstance(previous, dict) else {}
    report = build_routing_shadow_validation_from_guard(
        guard_state,
        previous=previous,
        min_validation_hours=float(args.min_validation_hours),
        retain_rows=int(args.retain_rows),
        retain_cycles=int(args.retain_cycles),
        shadow_candidate_members=load_shadow_candidate_seats(args.shadow_candidate_seats),
    )
    r7_snapshot = write_r7_dated_snapshot(out_path, report) if bool(args.r7_dated_snapshot) else None
    atomic_write_json(out_path, report)
    table_out = ""
    if args.fee_cal_table_out:
        table_path = Path(args.fee_cal_table_out)
        if not table_path.is_absolute():
            table_path = ROOT / table_path
        write_fee_gate_calibration_table(
            report,
            out_path=table_path,
            source_artifact=str(out_path.relative_to(ROOT) if out_path.is_relative_to(ROOT) else out_path),
        )
        table_out = str(table_path.relative_to(ROOT) if table_path.is_relative_to(ROOT) else table_path)
    print(
        json.dumps(
            {
                "out": str(out_path.relative_to(ROOT) if out_path.is_relative_to(ROOT) else out_path),
                "r7_dated_snapshot": str(r7_snapshot.relative_to(ROOT) if r7_snapshot and r7_snapshot.is_relative_to(ROOT) else r7_snapshot or ""),
                "fee_cal_table_out": table_out,
                "summary": report["summary"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
