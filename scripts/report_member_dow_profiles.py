#!/usr/bin/env python3
"""Build day-of-week / UTC-hour activity profiles for active members and queue candidates.

Flow stage: LEARN/ROTATE/SELF-DEV. This is a decision-layer artifact only:
it reads existing local history/event files and never changes live config or
submits orders.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL


DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_QUEUE_STATE = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_OUTPUT = "data/research/member_dow_profiles_latest.json"
DEFAULT_COMBINED_TAIL_LOG = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_WALLET_EVENT_LOGS = ("data/research/wallet_copy_live_guard_wallet_events.jsonl",)
DAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


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


def _event_ts(row: dict[str, Any]) -> float | None:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    for key in (
        "event_ts",
        "observed_ts",
        "timestamp",
        "received_at_s",
        "captured_at_s",
        "block_ts",
    ):
        value = _as_float(row.get(key))
        if value is not None and value > 0:
            return value
    for key in ("timestamp", "received_at_s", "captured_at_s", "block_ts"):
        value = _as_float(raw.get(key))
        if value is not None and value > 0:
            return value
    for key in ("generated_at", "received_at_iso", "captured_at_iso"):
        text = str(row.get(key) or raw.get(key) or "")
        if not text:
            continue
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return None


def _row_wallet(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    candidates = (
        row.get("source_wallet"),
        row.get("selected_wallet"),
        row.get("maker"),
        decoded.get("maker"),
        raw.get("proxyWallet"),
        raw.get("maker"),
    )
    for candidate in candidates:
        wallet = _norm_wallet(candidate)
        if wallet:
            return wallet
    return ""


def _market_slug(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    for key in ("market_slug", "event_slug"):
        value = str(row.get(key) or "")
        if value:
            return value
    for key in ("slug", "eventSlug"):
        value = str(raw.get(key) or "")
        if value:
            return value
    return ""


def _window_key(row: dict[str, Any], slug: str) -> str:
    start = row.get("window_start_s")
    if start not in (None, ""):
        return str(start)
    return slug


def _side(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return str(row.get("action") or row.get("side") or decoded.get("side") or raw.get("side") or "").upper()


def _tail_jsonl(path: Path, *, max_bytes: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - max(1, int(max_bytes))))
            if size > max_bytes:
                handle.readline()
            raw = handle.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _active_targets(guard: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard.get("active_set") if isinstance(guard.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    if not members and isinstance(guard.get("members"), list):
        members = guard["members"]
    rows: list[dict[str, Any]] = []
    for member in members:
        if not isinstance(member, dict):
            continue
        wallet = _norm_wallet(member.get("source_wallet"))
        if not wallet:
            continue
        rows.append(
            {
                "wallet": wallet,
                "candidate_id": member.get("candidate_id") or wallet,
                "policy_id": member.get("policy_id"),
                "role": "active_member",
                "queue_rank": None,
                "ready_for_live": True,
            }
        )
    return rows


def _queue_targets(queue: dict[str, Any], *, top_n: int) -> list[dict[str, Any]]:
    ranked = queue.get("ranked_members") if isinstance(queue.get("ranked_members"), list) else []
    rows: list[dict[str, Any]] = []
    for row in ranked:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        rows.append(
            {
                "wallet": wallet,
                "candidate_id": row.get("name") or row.get("candidate_id") or wallet,
                "policy_id": (row.get("replay") or {}).get("policy_id") if isinstance(row.get("replay"), dict) else row.get("policy_id"),
                "role": "top_queue_candidate",
                "queue_rank": row.get("queue_rank"),
                "ready_for_live": bool(row.get("ready_for_live")),
            }
        )
        if len(rows) >= top_n:
            break
    return rows


def _merge_targets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in rows:
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        current = merged.setdefault(wallet, {**row, "roles": []})
        role = str(row.get("role") or "")
        if role and role not in current["roles"]:
            current["roles"].append(role)
        if current.get("queue_rank") is None and row.get("queue_rank") is not None:
            current["queue_rank"] = row.get("queue_rank")
        if not current.get("candidate_id") and row.get("candidate_id"):
            current["candidate_id"] = row.get("candidate_id")
        current["ready_for_live"] = bool(current.get("ready_for_live") or row.get("ready_for_live"))
    return sorted(
        merged.values(),
        key=lambda row: (
            0 if "active_member" in row.get("roles", []) else 1,
            int(row.get("queue_rank") or 9999),
            str(row.get("wallet") or ""),
        ),
    )


def _candidate_event_paths(root: Path, wallets: set[str], explicit: list[str], combined_tail_log: str) -> list[Path]:
    paths: list[Path] = []
    for item in explicit:
        path = root / item if not Path(item).is_absolute() else Path(item)
        paths.append(path)
    data_dir = root / "data" / "research"
    for wallet in wallets:
        suffixes = {wallet[-12:], wallet[-10:], wallet[-8:]}
        for suffix in suffixes:
            for path in data_dir.glob(f"*{suffix}*_wallet_events.jsonl"):
                paths.append(path)
            for path in data_dir.glob(f"*{suffix}*events.jsonl"):
                if "wallet_events" in path.name:
                    paths.append(path)
    if combined_tail_log:
        paths.append(root / combined_tail_log if not Path(combined_tail_log).is_absolute() else Path(combined_tail_log))
    out: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def _empty_profile(target: dict[str, Any]) -> dict[str, Any]:
    return {
        "wallet": target["wallet"],
        "candidate_id": target.get("candidate_id"),
        "policy_id": target.get("policy_id"),
        "roles": target.get("roles") or [target.get("role")],
        "queue_rank": target.get("queue_rank"),
        "ready_for_live": bool(target.get("ready_for_live")),
        "trade_count": 0,
        "buy_trade_count": 0,
        "sell_trade_count": 0,
        "unique_window_count": 0,
        "btc5m_trade_count": 0,
        "btc5m_unique_window_count": 0,
        "volume_usd": 0.0,
        "first_event_ts": None,
        "last_event_ts": None,
        "first_event_iso": None,
        "last_event_iso": None,
        "_windows": set(),
        "_btc5m_windows": set(),
        "_calendar_dates_weekday": set(),
        "_calendar_dates_weekend": set(),
        "_by_dow_windows": defaultdict(set),
        "_by_hour_windows": defaultdict(set),
        "_by_dow_hour_windows": defaultdict(set),
        "trades_by_dow": Counter(),
        "trades_by_utc_hour": Counter(),
        "trades_by_dow_hour": Counter(),
        "evidence_sources": set(),
    }


def _finalize_profile(profile: dict[str, Any]) -> dict[str, Any]:
    windows = profile.pop("_windows")
    btc_windows = profile.pop("_btc5m_windows")
    weekday_dates = profile.pop("_calendar_dates_weekday")
    weekend_dates = profile.pop("_calendar_dates_weekend")
    by_dow_windows = profile.pop("_by_dow_windows")
    by_hour_windows = profile.pop("_by_hour_windows")
    by_dow_hour_windows = profile.pop("_by_dow_hour_windows")
    sources = sorted(profile.pop("evidence_sources"))
    trades_by_dow = Counter(profile.pop("trades_by_dow"))
    trades_by_hour = Counter(profile.pop("trades_by_utc_hour"))
    trades_by_dow_hour = Counter(profile.pop("trades_by_dow_hour"))

    total_trades = int(profile["trade_count"])
    weekday_trades = sum(trades_by_dow[dow] for dow in range(5))
    weekend_trades = sum(trades_by_dow[dow] for dow in (5, 6))
    weekday_day_count = len(weekday_dates)
    weekend_day_count = len(weekend_dates)
    weekday_avg = weekday_trades / weekday_day_count if weekday_day_count else None
    weekend_avg = weekend_trades / weekend_day_count if weekend_day_count else None
    if weekday_avg and weekend_avg is not None:
        weekend_weight = round(max(0.0, min(1.0, weekend_avg / weekday_avg)), 6)
    else:
        weekend_weight = None

    max_hour_count = max(trades_by_hour.values(), default=0)
    max_dow_hour_count = max(trades_by_dow_hour.values(), default=0)
    hour_weights = {
        str(hour): round((trades_by_hour.get(hour, 0) / max_hour_count), 6) if max_hour_count else None
        for hour in range(24)
    }
    dow_hour_weights = {
        f"{dow}:{hour:02d}": round((trades_by_dow_hour.get((dow, hour), 0) / max_dow_hour_count), 6)
        if max_dow_hour_count
        else None
        for dow in range(7)
        for hour in range(24)
    }

    profile.update(
        {
            "unique_window_count": len(windows),
            "btc5m_unique_window_count": len(btc_windows),
            "volume_usd": round(float(profile.get("volume_usd") or 0.0), 6),
            "weekday_trade_count": weekday_trades,
            "weekend_trade_count": weekend_trades,
            "weekday_sample_days": weekday_day_count,
            "weekend_sample_days": weekend_day_count,
            "weekday_avg_trades_per_sample_day": round(weekday_avg, 6) if weekday_avg is not None else None,
            "weekend_avg_trades_per_sample_day": round(weekend_avg, 6) if weekend_avg is not None else None,
            "weekend_trade_share_pct": round(100.0 * weekend_trades / total_trades, 6) if total_trades else None,
            "weekend_activity_weight_vs_weekday": weekend_weight,
            "weekend_evidence_status": (
                "HAS_WEEKEND_SAMPLE"
                if weekend_day_count
                else "NO_WEEKEND_SAMPLE"
                if total_trades
                else "NO_LOCAL_HISTORY"
            ),
            "trades_by_dow": {
                f"{dow}:{DAY_NAMES[dow]}": {
                    "trades": int(trades_by_dow.get(dow, 0)),
                    "unique_windows": len(by_dow_windows.get(dow, set())),
                }
                for dow in range(7)
            },
            "trades_by_utc_hour": {
                str(hour): {
                    "trades": int(trades_by_hour.get(hour, 0)),
                    "unique_windows": len(by_hour_windows.get(hour, set())),
                    "expected_active_weight": hour_weights[str(hour)],
                }
                for hour in range(24)
            },
            "expected_active_dow_hour_weights": dow_hour_weights,
            "evidence_sources": sources,
        }
    )
    return profile


def build_report(
    *,
    root: Path,
    guard_state: dict[str, Any],
    queue_state: dict[str, Any],
    top_queue: int,
    wallet_event_logs: list[str],
    combined_tail_log: str,
    max_tail_bytes: int,
) -> dict[str, Any]:
    targets = _merge_targets(_active_targets(guard_state) + _queue_targets(queue_state, top_n=top_queue))
    by_wallet = {str(row["wallet"]): _empty_profile(row) for row in targets}
    paths = _candidate_event_paths(root, set(by_wallet), wallet_event_logs, combined_tail_log)
    seen_events: set[tuple[str, str, str, float]] = set()

    for path in paths:
        for row in _tail_jsonl(path, max_bytes=max_tail_bytes):
            wallet = _row_wallet(row)
            if wallet not in by_wallet:
                continue
            ts = _event_ts(row)
            if ts is None:
                continue
            raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
            slug = _market_slug(row)
            key = (
                wallet,
                str(row.get("transaction_hash") or raw.get("transactionHash") or ""),
                slug,
                round(ts, 6),
            )
            if key in seen_events:
                continue
            seen_events.add(key)
            profile = by_wallet[wallet]
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            dow = dt.weekday()
            hour = dt.hour
            window = _window_key(row, slug)
            side = _side(row)
            price = _as_float(row.get("price")) or _as_float(raw.get("price")) or 0.0
            size = _as_float(row.get("size")) or _as_float(raw.get("size")) or 0.0
            profile["trade_count"] += 1
            if side == "BUY":
                profile["buy_trade_count"] += 1
            elif side == "SELL":
                profile["sell_trade_count"] += 1
            if slug.startswith("btc-updown-5m-"):
                profile["btc5m_trade_count"] += 1
                profile["_btc5m_windows"].add(window)
            profile["volume_usd"] += price * size
            profile["_windows"].add(window)
            if dow >= 5:
                profile["_calendar_dates_weekend"].add(dt.date().isoformat())
            else:
                profile["_calendar_dates_weekday"].add(dt.date().isoformat())
            profile["_by_dow_windows"][dow].add(window)
            profile["_by_hour_windows"][hour].add(window)
            profile["_by_dow_hour_windows"][(dow, hour)].add(window)
            profile["trades_by_dow"][dow] += 1
            profile["trades_by_utc_hour"][hour] += 1
            profile["trades_by_dow_hour"][(dow, hour)] += 1
            profile["evidence_sources"].add(str(path.relative_to(root)) if path.is_relative_to(root) else str(path))
            first_ts = profile.get("first_event_ts")
            last_ts = profile.get("last_event_ts")
            if first_ts is None or ts < float(first_ts):
                profile["first_event_ts"] = round(ts, 6)
                profile["first_event_iso"] = dt.isoformat().replace("+00:00", "Z")
            if last_ts is None or ts > float(last_ts):
                profile["last_event_ts"] = round(ts, 6)
                profile["last_event_iso"] = dt.isoformat().replace("+00:00", "Z")

    profiles = [_finalize_profile(profile) for profile in by_wallet.values()]
    no_history = [row["wallet"] for row in profiles if int(row.get("trade_count") or 0) == 0]
    return {
        "schema_version": 1,
        "kind": "member_dow_profiles",
        "flow_stage": "LEARN/ROTATE/SELF-DEV",
        "generated_at": _utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "authority": "Fable DIRECTION 2026-07-10T17:40Z WEEKEND-AWARENESS",
        "rule": "decision/report layer only; no guard, sizing, toxicity, loss-trigger, or live-submit path mutation",
        "calendar_clock_rule": {
            "effective_new_clocks_from": "2026-07-11T00:00:00Z",
            "running_clocks": "rerule_at_expiry_with_this_profile; do not silently rewrite pre-registered clocks",
            "positive_pnl_weekend_inactivity_demotion": "forbidden; loss-trigger remains live",
        },
        "summary": {
            "target_count": len(targets),
            "active_member_count": sum(1 for row in targets if "active_member" in row.get("roles", [])),
            "top_queue_count": sum(1 for row in targets if "top_queue_candidate" in row.get("roles", [])),
            "profiles_with_history": len(profiles) - len(no_history),
            "profiles_without_local_history": len(no_history),
            "no_history_wallets": no_history[:20],
            "source_files_scanned": [str(path.relative_to(root)) if path.is_relative_to(root) else str(path) for path in paths],
        },
        "profiles": profiles,
        "profiles_by_wallet": {str(row["wallet"]): row for row in profiles},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--queue-state", default=DEFAULT_QUEUE_STATE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-queue", type=int, default=12)
    parser.add_argument("--wallet-event-log", action="append", default=list(DEFAULT_WALLET_EVENT_LOGS))
    parser.add_argument("--combined-tail-log", default=DEFAULT_COMBINED_TAIL_LOG)
    parser.add_argument("--max-tail-bytes", type=int, default=80_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    guard_state = _load_json(root / args.guard_state, {})
    queue_state = _load_json(root / args.queue_state, {})
    report = build_report(
        root=root,
        guard_state=guard_state if isinstance(guard_state, dict) else {},
        queue_state=queue_state if isinstance(queue_state, dict) else {},
        top_queue=max(0, int(args.top_queue)),
        wallet_event_logs=list(args.wallet_event_log or []),
        combined_tail_log=str(args.combined_tail_log or ""),
        max_tail_bytes=max(1, int(args.max_tail_bytes)),
    )
    _atomic_write_json(root / args.output, report)
    print(json.dumps({"kind": report["kind"], "generated_at": report["generated_at"], "summary": report["summary"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
