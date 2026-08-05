#!/usr/bin/env python3
"""Track e6db events rejected only by the 0.50 price band."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from scripts.merge_rtds_wallet_events import _wallet_event_from_rtds  # noqa: E402

DEFAULT_SOURCE_WALLET = "0xe6db20932faf0f9780acf75d95c74c9984407dac"
E6DB = DEFAULT_SOURCE_WALLET
DEFAULT_EVENTS = ROOT / "data/research/wallet_copy_live_guard_wallet_events.jsonl"
DEFAULT_RTDS_EVENTS = ROOT / DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_STATE = ROOT / "data/research/e6db_price_reject_counterfactual_state.json"
DEFAULT_EVENT_LOG = ROOT / "data/research/e6db_price_reject_counterfactual_events.jsonl"
DEFAULT_OVERLAY = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_SINCE = "2026-07-13T12:14:00Z"
DEFAULT_TAIL_BYTES = 8 * 1024 * 1024 * 1024


def _parse_iso_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _iter_jsonl(path: Path, *, tail_bytes: int = 0):
    if tail_bytes > 0 and path.exists() and path.stat().st_size > tail_bytes:
        with path.open("rb") as handle:
            handle.seek(-tail_bytes, os.SEEK_END)
            handle.readline()
            for raw in handle:
                try:
                    yield json.loads(raw.decode("utf-8", errors="ignore"))
                except json.JSONDecodeError:
                    continue
        return
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _event_ts(row: dict[str, Any]) -> float:
    return float(row.get("observed_ts") or row.get("event_ts") or 0.0)


def _short_wallet(source_wallet: str) -> str:
    text = source_wallet.lower().strip()
    return text[-8:] if text.startswith("0x") and len(text) >= 10 else text[:8]


def _wallet_path_slug(source_wallet: str) -> str:
    text = source_wallet.lower().strip()
    return text[2:10] if text.startswith("0x") and len(text) >= 10 else text[:8]


def _wallet_scoped_path(source_wallet: str, suffix: str) -> Path:
    return ROOT / f"data/research/{_wallet_path_slug(source_wallet)}_price_reject_counterfactual_{suffix}"


def _resolve_state_path(source_wallet: str, state_path: Path) -> Path:
    if source_wallet.lower() != DEFAULT_SOURCE_WALLET and state_path == DEFAULT_STATE:
        return _wallet_scoped_path(source_wallet, "state.json")
    return state_path


def _resolve_event_log_path(source_wallet: str, event_log: Path) -> Path:
    if source_wallet.lower() != DEFAULT_SOURCE_WALLET and event_log == DEFAULT_EVENT_LOG:
        return _wallet_scoped_path(source_wallet, "events.jsonl")
    return event_log


def _event_key(row: dict[str, Any], *, source_wallet: str) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return stable_id(
        f"{_short_wallet(source_wallet)}_price_reject",
        {
            "transaction_hash": row.get("transaction_hash") or raw.get("transactionHash"),
            "source_fingerprint": row.get("source_fingerprint"),
            "condition_id": row.get("condition_id"),
            "token_id": row.get("token_id"),
            "outcome": row.get("outcome"),
            "price": round(num(row.get("price")), 8),
            "event_ts": row.get("event_ts"),
        },
        length=24,
    )


def _is_btc5m(row: dict[str, Any]) -> bool:
    slug = str(row.get("market_slug") or row.get("event_slug") or "")
    return slug.startswith("btc-updown-5m-")


def _candidate_rows(
    *,
    source_wallet: str,
    wallet_events: Path,
    rtds_events: Path | None,
    since_ts: float,
    price_floor: float,
    canary_max_price: float,
    tail_bytes: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_key: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    tap_counts = {
        "wallet_source_events": 0,
        "rtds_source_events": 0,
        "source_events_since": 0,
    }
    for row in _iter_jsonl(wallet_events, tail_bytes=tail_bytes):
        if not isinstance(row, dict):
            continue
        row = dict(row)
        row.setdefault("tap_source", "wallet_events")
        rows.append(row)
    if rtds_events is not None:
        for row in _iter_jsonl(rtds_events, tail_bytes=tail_bytes):
            if not isinstance(row, dict):
                continue
            event = _wallet_event_from_rtds(
                row,
                source_wallet=source_wallet,
                wallet_name=f"{_short_wallet(source_wallet)}_rtds_activity",
            )
            if event is None:
                continue
            normalized = event.asdict()
            normalized["tap_source"] = "rtds_activity"
            rows.append(normalized)
    for row in rows:
        if str(row.get("source_wallet") or "").lower() != source_wallet.lower():
            continue
        if str(row.get("tap_source") or "") == "rtds_activity":
            tap_counts["rtds_source_events"] += 1
        else:
            tap_counts["wallet_source_events"] += 1
        if str(row.get("action") or "").upper() != "BUY":
            continue
        if not _is_btc5m(row):
            continue
        event_ts = _event_ts(row)
        if event_ts < since_ts:
            continue
        tap_counts["source_events_since"] += 1
        price = num(row.get("price"))
        if price <= price_floor + 1e-12 or price > canary_max_price + 1e-12:
            continue
        key = _event_key(row, source_wallet=source_wallet)
        current = by_key.get(key)
        if current is None or _event_ts(row) > _event_ts(current):
            by_key[key] = row
    return sorted(by_key.values(), key=_event_ts), tap_counts


def _synthetic_order(row: dict[str, Any], *, source_wallet: str, canary_size_usd: float) -> dict[str, Any]:
    price = max(0.000001, num(row.get("price")))
    cost = max(0.0, float(canary_size_usd))
    return {
        "order_id": _event_key(row, source_wallet=source_wallet),
        "intent_id": _event_key(row, source_wallet=source_wallet),
        "source_wallet": source_wallet,
        "wallet_name": f"{_short_wallet(source_wallet)}_price_reject_counterfactual",
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("market_slug") or row.get("event_slug"),
        "outcome": row.get("outcome"),
        "token_id": row.get("token_id"),
        "final_status": "FILLED",
        "status": "FILLED",
        "filled_size_usd": round(cost, 6),
        "filled_shares": round(cost / price, 9) if price > 0 else 0.0,
        "limit_price": round(price, 8),
        "submitted_at": dt.datetime.fromtimestamp(_event_ts(row), dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_intent": {
            "token_id": row.get("token_id"),
            "market_slug": row.get("market_slug") or row.get("event_slug"),
            "condition_id": row.get("condition_id"),
        },
    }


def _existing_event_keys(path: Path) -> set[str]:
    keys: set[str] = set()
    for row in _iter_jsonl(path):
        if isinstance(row, dict) and row.get("event_key"):
            keys.add(str(row["event_key"]))
    return keys


def _append_new_events(path: Path, rows: list[dict[str, Any]]) -> int:
    existing = _existing_event_keys(path)
    new_rows = [row for row in rows if str(row.get("event_key") or "") not in existing]
    if not new_rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in new_rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    return len(new_rows)


def _event_log_rows(path: Path, *, source_wallet: str) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in _iter_jsonl(path):
        if not isinstance(row, dict) or not row.get("event_key"):
            continue
        if str(row.get("source_wallet") or "").lower() != source_wallet.lower():
            continue
        by_key[str(row["event_key"])] = dict(row)
    return sorted(by_key.values(), key=_event_ts)


def _event_log_wallets(path: Path) -> set[str]:
    wallets: set[str] = set()
    for row in _iter_jsonl(path):
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower().strip()
        if wallet:
            wallets.add(wallet)
    return wallets


def _ensure_wallet_scoped_identity(
    *,
    source_wallet: str,
    state_path: Path,
    event_log: Path,
    using_wallet_scoped_defaults: bool,
) -> None:
    if not using_wallet_scoped_defaults:
        return
    mismatches: list[str] = []
    if state_path.exists():
        existing_state = load_json(state_path, default={})
        existing_wallet = (
            str(existing_state.get("source_wallet") or "").lower().strip()
            if isinstance(existing_state, dict)
            else ""
        )
        if existing_wallet and existing_wallet != source_wallet:
            mismatches.append(f"state source_wallet={existing_wallet}")
    if event_log.exists():
        wallets = _event_log_wallets(event_log)
        other_wallets = sorted(wallet for wallet in wallets if wallet != source_wallet)
        if other_wallets:
            sample = ",".join(other_wallets[:3])
            suffix = ",..." if len(other_wallets) > 3 else ""
            mismatches.append(f"event_log source_wallets={sample}{suffix}")
    if mismatches:
        raise ValueError(
            "wallet-scoped counterfactual path identity mismatch for "
            f"{_wallet_path_slug(source_wallet)}; requested source_wallet={source_wallet}, "
            + "; ".join(mismatches)
            + ". Pass explicit --state/--event-log paths for a different wallet."
        )


def _score_rows(
    rows: list[dict[str, Any]],
    *,
    source_wallet: str,
    resolutions: dict[str, dict[str, Any]],
    canary_size_usd: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        order = _synthetic_order(row, source_wallet=source_wallet, canary_size_usd=canary_size_usd)
        score = score_order(order, resolutions)
        out.append(
            {
                "event_key": _event_key(row, source_wallet=source_wallet),
                "flow_stage": "LIVE/LEARN",
                "source_wallet": source_wallet,
                "market_slug": row.get("market_slug") or row.get("event_slug"),
                "condition_id": row.get("condition_id"),
                "token_id": row.get("token_id"),
                "outcome": row.get("outcome"),
                "price": round(num(row.get("price")), 8),
                "event_ts": row.get("event_ts"),
                "observed_ts": row.get("observed_ts"),
                "generated_at": row.get("generated_at"),
                "transaction_hash": row.get("transaction_hash"),
                "source_fingerprint": row.get("source_fingerprint"),
                "tap_source": row.get("tap_source"),
                "hypothetical_order": order,
                "score": score,
            }
        )
    return out


def _summary(scored_rows: list[dict[str, Any]], *, min_resolved: int) -> dict[str, Any]:
    resolved = [row for row in scored_rows if isinstance(row.get("score"), dict) and row["score"].get("resolved")]
    pnl = round(sum(num(row["score"].get("pnl_usd")) for row in resolved), 6)
    cost = round(sum(num(row["score"].get("cost_usd")) for row in resolved), 6)
    recent_resolved = resolved[-200:]
    recent_pnl = round(sum(num(row["score"].get("pnl_usd")) for row in recent_resolved), 6)
    recent_cost = round(sum(num(row["score"].get("cost_usd")) for row in recent_resolved), 6)
    full_positive = len(resolved) >= int(min_resolved) and pnl > 0.0
    recent_positive = len(recent_resolved) >= min(200, int(min_resolved)) and recent_pnl > 0.0
    return {
        "candidate_events": len(scored_rows),
        "resolved_n": len(resolved),
        "unresolved_n": len(scored_rows) - len(resolved),
        "wins": sum(1 for row in resolved if row["score"].get("win") is True),
        "losses": sum(1 for row in resolved if row["score"].get("win") is False),
        "hypothetical_pnl_usd": pnl,
        "hypothetical_cost_usd": cost,
        "hypothetical_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "min_resolved_for_decision": int(min_resolved),
        "decision_ready": len(resolved) >= int(min_resolved),
        "positive_gate": full_positive,
        "recent_200_resolved_n": len(recent_resolved),
        "recent_200_pnl_usd": recent_pnl,
        "recent_200_cost_usd": recent_cost,
        "recent_200_roi_pct": round((recent_pnl / recent_cost) * 100.0, 6) if recent_cost > 0 else 0.0,
        "recent_200_positive_gate": recent_positive,
        "widening_rearm_gate": full_positive and recent_positive,
        "widening_rearm_requires_explicit_fable": True,
    }


def _refresh_canary_headline_only(
    overlay_path: Path,
    *,
    report: dict[str, Any],
    source_wallet: str,
) -> None:
    """Refresh research headline without mutating any roster or live policy."""
    overlay = load_json(overlay_path, default={})
    if not isinstance(overlay, dict):
        return
    latest_key = f"latest_{_wallet_path_slug(source_wallet)}_price_reject_canary"
    previous = overlay.get(latest_key) if isinstance(overlay.get(latest_key), dict) else {}
    headline = dict(previous)
    headline.update(
        {
            "flow_stage": "LIVE/LEARN",
            "status": report["status"],
            "source_wallet": source_wallet,
            "source_artifact": report["state_path"],
            "summary": report["summary"],
            "canary_max_price": report["filters"]["canary_max_price_inclusive"],
            "canary_max_order_usd": report["filters"]["canary_size_usd"],
            "widening_path": "DISARMED",
            "rearm_rule": "positive ROI on full sample and recent 200 resolved, then explicit ask_fable",
            "direction_id": "2026-07-20T07:58Z-fable-counterfactual-gate-flip-negative",
        }
    )
    overlay[latest_key] = headline
    atomic_write_json(overlay_path, overlay)


def _apply_canary_to_overlay(
    overlay_path: Path,
    *,
    report: dict[str, Any],
    source_wallet: str,
    canary_max_price: float,
    canary_max_order_usd: float,
) -> dict[str, Any]:
    source_wallet = source_wallet.lower()
    wallet_slug = _wallet_path_slug(source_wallet)
    legacy_e6db = source_wallet == DEFAULT_SOURCE_WALLET.lower()
    policy_id = (
        "e6db_price_reject_canary_0.10_cap_2_le_70"
        if legacy_e6db
        else f"{wallet_slug}_price_reject_canary_0.10_cap_2_le_70"
    )
    summary_key = "e6db_price_reject_canary" if legacy_e6db else f"{wallet_slug}_price_reject_canary"
    latest_key = f"latest_{summary_key}"
    direction_id = (
        "2026-07-13T15:46Z-fable-e6db-price-reject-counterfactual"
        if legacy_e6db
        else "2026-07-15T21:16Z-fable-a689-price-band-canary-preauth"
    )
    overlay = load_json(overlay_path, default={})
    overlay = overlay if isinstance(overlay, dict) else {}
    members = [dict(row) for row in overlay.get("members") or [] if isinstance(row, dict)]
    changed = False
    for member in members:
        if str(member.get("source_wallet") or "").lower() != source_wallet:
            continue
        policy = dict(member.get("policy") if isinstance(member.get("policy"), dict) else {})
        old_max_price = num(member.get("max_price") or policy.get("max_price"))
        old_max_order = num(member.get("max_order_usd") or policy.get("max_order_usd"))
        member["max_price"] = round(float(canary_max_price), 6)
        next_max_order = (
            min(old_max_order or canary_max_order_usd, float(canary_max_order_usd))
            if legacy_e6db
            else float(canary_max_order_usd)
        )
        member["max_order_usd"] = round(next_max_order, 6)
        if not legacy_e6db:
            member["fable_cap_max_order_usd"] = round(float(canary_max_order_usd), 6)
            member["drip_min_tranche_usd"] = round(float(canary_max_order_usd), 6)
            member["drip_max_tranche_usd"] = round(float(canary_max_order_usd), 6)
        member["policy_id"] = policy_id
        policy.update(
            {
                "policy_id": member["policy_id"],
                "max_price": member["max_price"],
                "max_order_usd": member["max_order_usd"],
                "wallet_fraction": num(policy.get("wallet_fraction"), 0.1) or 0.1,
                "min_order_usd": num(policy.get("min_order_usd"), 1.0) or 1.0,
            }
        )
        if not legacy_e6db:
            policy.update(
                {
                    "fable_cap_max_order_usd": round(float(canary_max_order_usd), 6),
                    "drip_min_tranche_usd": round(float(canary_max_order_usd), 6),
                    "drip_max_tranche_usd": round(float(canary_max_order_usd), 6),
                    "condition_e_effective_min_submit_cap_usd": round(float(canary_max_order_usd), 6),
                }
            )
        member["policy"] = policy
        summary = dict(member.get("summary") if isinstance(member.get("summary"), dict) else {})
        summary[summary_key] = {
            "flow_stage": "LIVE/PROMOTE",
            "applied_at": report["generated_at"],
            "direction_id": direction_id,
            "source_wallet": source_wallet,
            "source_artifact": _display(Path(report["state_path"])),
            "old_max_price": round(old_max_price, 6),
            "old_max_order_usd": round(old_max_order, 6),
            "canary_max_price": member["max_price"],
            "canary_max_order_usd": member["max_order_usd"],
            "condition_e_effective_min_submit_cap_usd": round(float(canary_max_order_usd), 6),
            "single_submitter_invariant": "scripts/run_wallet_copy_live_guard.py remains the only live order submitter",
            "promotion_basis": "n>=12 resolved price-reject counterfactuals net-positive",
        }
        member["summary"] = summary
        member["status"] = f"{wallet_slug.upper()}_PRICE_REJECT_CANARY_ARMED"
        changed = True
        break
    if not changed:
        return {
            "status": "NO_SOURCE_WALLET_MEMBER",
            "changed": False,
            "source_wallet": source_wallet,
            "overlay_path": _display(overlay_path),
        }
    overlay.update(
        {
            "updated_at": report["generated_at"],
            "direction_id": direction_id,
            "members": members,
            latest_key: {
                "flow_stage": "LIVE/PROMOTE",
                "status": "ARMED",
                "source_wallet": source_wallet,
                "source_artifact": _display(Path(report["state_path"])),
                "summary": report["summary"],
                "canary_max_price": round(float(canary_max_price), 6),
                "canary_max_order_usd": round(float(canary_max_order_usd), 6),
            },
        }
    )
    atomic_write_json(overlay_path, overlay)
    return {
        "status": "CANARY_OVERLAY_UPDATED",
        "changed": True,
        "source_wallet": source_wallet,
        "overlay_path": _display(overlay_path),
    }


def build_report(
    *,
    source_wallet: str = DEFAULT_SOURCE_WALLET,
    wallet_events: Path,
    rtds_events: Path | None,
    resolutions_path: Path,
    state_path: Path,
    event_log: Path,
    overlay_path: Path,
    since: str,
    price_floor: float,
    canary_max_price: float,
    canary_size_usd: float,
    min_resolved: int,
    tail_bytes: int,
    apply_canary: bool,
) -> dict[str, Any]:
    source_wallet = source_wallet.lower()
    using_wallet_scoped_defaults = (
        source_wallet != DEFAULT_SOURCE_WALLET.lower()
        and state_path == DEFAULT_STATE
        and event_log == DEFAULT_EVENT_LOG
    )
    state_path = _resolve_state_path(source_wallet, state_path)
    event_log = _resolve_event_log_path(source_wallet, event_log)
    _ensure_wallet_scoped_identity(
        source_wallet=source_wallet,
        state_path=state_path,
        event_log=event_log,
        using_wallet_scoped_defaults=using_wallet_scoped_defaults,
    )
    since_ts = _parse_iso_ts(since)
    if since_ts is None:
        raise ValueError(f"invalid --since: {since}")
    rows, tap_counts = _candidate_rows(
        source_wallet=source_wallet,
        wallet_events=wallet_events,
        rtds_events=rtds_events,
        since_ts=since_ts,
        price_floor=price_floor,
        canary_max_price=canary_max_price,
        tail_bytes=tail_bytes,
    )
    current_scored = _score_rows(
        rows,
        source_wallet=source_wallet,
        resolutions=load_resolutions(resolutions_path),
        canary_size_usd=canary_size_usd,
    )
    previous_state = load_json(state_path, default={})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    previous_summary = previous_state.get("summary") if isinstance(previous_state.get("summary"), dict) else {}
    appended = _append_new_events(event_log, current_scored)
    ledger_rows = _event_log_rows(event_log, source_wallet=source_wallet)
    scored = _score_rows(
        ledger_rows,
        source_wallet=source_wallet,
        resolutions=load_resolutions(resolutions_path),
        canary_size_usd=canary_size_usd,
    )
    summary = _summary(scored, min_resolved=min_resolved)
    generated_at = utc_now_iso()
    status = "MEASURING"
    if summary["decision_ready"] and summary["widening_rearm_gate"]:
        status = "CANARY_READY"
    elif summary["decision_ready"] and summary["positive_gate"]:
        status = "CANARY_REARM_PENDING_TAIL_NEGATIVE"
    elif summary["decision_ready"]:
        status = "BAND_STANDS_VINDICATED"
    report: dict[str, Any] = {
        "schema_version": 1,
        "kind": "price_reject_counterfactual",
        "flow_stage": "LIVE/LEARN",
        "generated_at": generated_at,
        "status": status,
        "source_wallet": source_wallet,
        "wallet_events": _display(wallet_events),
        "rtds_events": _display(rtds_events) if rtds_events is not None else None,
        "tap_source_mode": "wallet_events_plus_rtds" if rtds_events is not None else "wallet_events_only",
        "tap_counts": tap_counts,
        "tap_provably_carries_events": tap_counts.get("source_events_since", 0) > 0,
        "universe_mode": "append_only_event_log_union",
        "source_snapshot_candidate_events": len(current_scored),
        "event_log_candidate_events": len(scored),
        "resolutions": _display(resolutions_path),
        "state_path": _display(state_path),
        "event_log": _display(event_log),
        "previous_generated_at": previous_state.get("generated_at"),
        "prev_resolved_n": previous_summary.get("resolved_n"),
        "prev_pnl_usd": previous_summary.get("hypothetical_pnl_usd"),
        "resolved_n_delta": (
            summary["resolved_n"] - int(previous_summary["resolved_n"])
            if previous_summary.get("resolved_n") is not None
            else None
        ),
        "pnl_usd_delta": (
            round(summary["hypothetical_pnl_usd"] - num(previous_summary.get("hypothetical_pnl_usd")), 6)
            if previous_summary.get("hypothetical_pnl_usd") is not None
            else None
        ),
        "operator_rule": "negative full-sample ROI vindicates the 0.50 band; widening re-arm requires positive ROI on both full sample and recent 200 resolved, then explicit ask_fable",
        "filters": {
            "since": since,
            "price_floor_exclusive": round(float(price_floor), 6),
            "canary_max_price_inclusive": round(float(canary_max_price), 6),
            "canary_size_usd": round(float(canary_size_usd), 6),
            "tail_bytes": int(tail_bytes),
        },
        "summary": summary,
        "events": scored[-200:],
        "live_orders_allowed": False,
        "paper_only": True,
        "live_path_mutated": False,
    }
    report["event_log_appended"] = appended
    _refresh_canary_headline_only(
        overlay_path,
        report=report,
        source_wallet=source_wallet,
    )
    if status == "CANARY_READY" and summary["widening_rearm_gate"] and apply_canary:
        canary = _apply_canary_to_overlay(
            overlay_path,
            report=report,
            source_wallet=source_wallet,
            canary_max_price=canary_max_price,
            canary_max_order_usd=canary_size_usd,
        )
        report["canary_application"] = canary
        if canary.get("changed"):
            report["status"] = "CANARY_ARMED"
            report["flow_stage"] = "LIVE/PROMOTE"
            report["paper_only"] = False
            report["live_path_mutated"] = True
    atomic_write_json(state_path, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-wallet", default=DEFAULT_SOURCE_WALLET)
    parser.add_argument("--wallet-events", default=str(DEFAULT_EVENTS))
    parser.add_argument("--rtds-events", default=str(DEFAULT_RTDS_EVENTS))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument("--overlay", default=str(DEFAULT_OVERLAY))
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--price-floor", type=float, default=0.50)
    parser.add_argument("--canary-max-price", type=float, default=0.70)
    parser.add_argument("--canary-size-usd", type=float, default=2.0)
    parser.add_argument("--min-resolved", type=int, default=12)
    parser.add_argument("--tail-bytes", type=int, default=DEFAULT_TAIL_BYTES)
    parser.add_argument("--apply-canary", action="store_true")
    args = parser.parse_args(argv)
    report = build_report(
        wallet_events=Path(args.wallet_events),
        rtds_events=Path(args.rtds_events) if str(args.rtds_events or "").strip() else None,
        resolutions_path=Path(args.resolutions),
        state_path=Path(args.state),
        event_log=Path(args.event_log),
        overlay_path=Path(args.overlay),
        since=str(args.since),
        price_floor=float(args.price_floor),
        canary_max_price=float(args.canary_max_price),
        canary_size_usd=float(args.canary_size_usd),
        min_resolved=int(args.min_resolved),
        tail_bytes=int(args.tail_bytes),
        apply_canary=bool(args.apply_canary),
        source_wallet=str(args.source_wallet).lower(),
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
