#!/usr/bin/env python3
"""Report member-factory KPIs for active-set growth.

Flow stage: PROMOTE/SELF-DEV. This is deterministic evidence for the supply
side of OP-VOLUME: queue depth, set trajectory, member freshness, and funnel
throughput. It never changes live membership or gates.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL


TARGET_READY_QUEUE = 5
DEFECT_READY_QUEUE_BELOW = 3
STALE_MEMBER_AGE_S = 24 * 60 * 60
SNAPSHOT_RETENTION_S = 3 * 24 * 60 * 60
DEFAULT_DOW_PROFILE = "data/research/member_dow_profiles_latest.json"
DEFAULT_MEMBER_ROLLING20 = "data/research/wallet_copy_member_rolling20_latest.json"
WEEKEND_AWARE_CLOCK_EFFECTIVE_TS = datetime.fromisoformat("2026-07-11T00:00:00+00:00").timestamp()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        pass
    raw = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return 0.0


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def _iter_jsonl_tail(path: Path, *, max_bytes: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        size = handle.tell()
        handle.seek(max(0, size - max_bytes))
        if size > max_bytes:
            handle.readline()
        raw = handle.read().decode("utf-8", errors="replace")
    rows: list[dict[str, Any]] = []
    for line in raw.splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _series_key(slug: str, title: str = "") -> str:
    slug_l = slug.lower()
    title_l = title.lower()
    text = f"{slug_l} {title_l}"
    if slug_l.startswith("btc-updown-5m-"):
        return "btc_5m"
    if slug_l.startswith("eth-updown-5m-") or slug_l.startswith("ethereum-updown-5m-"):
        return "eth_5m"
    if ("btc" in text or "bitcoin" in text) and ("hour" in text or "1h" in text):
        return "btc_hourly"
    if ("eth" in text or "ethereum" in text) and ("hour" in text or "1h" in text):
        return "eth_hourly"
    return "other"


def _series_census(rows: list[dict[str, Any]], *, now_ts: float) -> dict[str, Any]:
    cutoff = now_ts - STALE_MEMBER_AGE_S
    series: dict[str, dict[str, Any]] = {}
    for row in rows:
        ts = _parse_ts(row.get("event_ts") or row.get("received_at_s") or row.get("captured_at_s"))
        if ts < cutoff:
            continue
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        slug = str(row.get("market_slug") or row.get("event_slug") or raw.get("slug") or "")
        if not slug:
            continue
        key = _series_key(slug, str(raw.get("title") or ""))
        if key == "other":
            continue
        item = series.setdefault(
            key,
            {
                "series": key,
                "trade_events_24h": 0,
                "unique_windows_24h": 0,
                "volume_usd_24h": 0.0,
                "sample_slugs": [],
            },
        )
        item["trade_events_24h"] += 1
        item["volume_usd_24h"] = round(
            float(item["volume_usd_24h"]) + float(row.get("size") or 0.0) * float(row.get("price") or 0.0),
            6,
        )
        windows = item.setdefault("_windows", set())
        if isinstance(windows, set):
            windows.add(slug)
        samples = item.get("sample_slugs")
        if isinstance(samples, list) and slug not in samples and len(samples) < 3:
            samples.append(slug)
    total_windows = 0
    for item in series.values():
        windows = item.pop("_windows", set())
        item["unique_windows_24h"] = len(windows) if isinstance(windows, set) else 0
        total_windows += int(item["unique_windows_24h"])
        if item["series"] == "btc_5m":
            item["denominator_windows"] = 288
            item["denominator_rule"] = "OP-BTC5M canonical daily denominator; never blended"
        elif item["series"].endswith("_hourly"):
            item["denominator_windows"] = 24
            item["denominator_rule"] = "own hourly denominator; additive series, not a substitute"
        else:
            item["denominator_windows"] = None
            item["denominator_rule"] = "own series denominator pending listing cadence"
    return {
        "window": "last_24h_from_rtds_tail",
        "series": dict(sorted(series.items())),
        "total_across_series": {
            "unique_windows_24h": total_windows,
            "denominator_rule": "sum of per-series windows; does not change BTC-5m 144/288 target",
        },
    }


def _latest_scorecard(data_dir: Path) -> dict[str, Any]:
    candidates = sorted(
        data_dir.glob("wallet_copy_daily_scorecard_*.json"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
        reverse=True,
    )
    for path in candidates:
        loaded = _load_json(path, {})
        if isinstance(loaded, dict) and loaded.get("kind") == "wallet_copy_daily_scorecard":
            return loaded
    return {}


def _active_members(guard: dict[str, Any]) -> list[dict[str, Any]]:
    active_set = guard.get("active_set") if isinstance(guard.get("active_set"), dict) else {}
    members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    rows = [row for row in members if isinstance(row, dict) and row.get("enabled", True)]
    if rows:
        return rows
    if guard.get("source_wallet"):
        return [
            {
                "candidate_id": guard.get("candidate_id"),
                "source_wallet": guard.get("source_wallet"),
                "policy_id": guard.get("policy_id"),
                "status": "CURRENT_GUARD_MEMBER",
            }
        ]
    return []


def _profile_by_wallet(profile_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(profile_state, dict):
        return {}
    by_wallet = profile_state.get("profiles_by_wallet") if isinstance(profile_state.get("profiles_by_wallet"), dict) else {}
    out = {
        str(wallet).lower(): row
        for wallet, row in by_wallet.items()
        if isinstance(row, dict)
    }
    for row in profile_state.get("profiles") or []:
        if isinstance(row, dict) and row.get("wallet"):
            out[str(row["wallet"]).lower()] = row
    return out


def _rolling20_by_wallet(rolling_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = rolling_state.get("rows") if isinstance(rolling_state.get("rows"), list) else []
    return {
        str(row.get("wallet") or "").lower(): row
        for row in rows
        if isinstance(row, dict) and row.get("wallet")
    }


def _profile_hour_weight(profile: dict[str, Any], dt: datetime) -> float:
    if not profile or int(profile.get("trade_count") or 0) <= 0:
        return 1.0
    if dt.weekday() >= 5 and profile.get("weekend_evidence_status") != "HAS_WEEKEND_SAMPLE":
        return 1.0
    weights = (
        profile.get("expected_active_dow_hour_weights")
        if isinstance(profile.get("expected_active_dow_hour_weights"), dict)
        else {}
    )
    value = weights.get(f"{dt.weekday()}:{dt.hour:02d}")
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 1.0


def _calendar_expected_active_age_s(last_ts: float | None, now_ts: float, profile: dict[str, Any]) -> float | None:
    if last_ts is None or last_ts <= 0.0 or now_ts <= last_ts or not profile:
        return None
    cursor = float(last_ts)
    total = 0.0
    max_steps = 24 * 45
    steps = 0
    while cursor < now_ts and steps < max_steps:
        dt = datetime.fromtimestamp(cursor, tz=timezone.utc)
        next_hour = dt.replace(minute=0, second=0, microsecond=0).timestamp() + 3600.0
        end = min(now_ts, next_hour)
        total += max(0.0, end - cursor) * _profile_hour_weight(profile, dt)
        cursor = end
        steps += 1
    if cursor < now_ts:
        total += now_ts - cursor
    return round(total, 6)


def _snapshot_for_compare(snapshots: list[dict[str, Any]], now_ts: float) -> tuple[dict[str, Any], str]:
    target_ts = now_ts - STALE_MEMBER_AGE_S
    older = [row for row in snapshots if float(row.get("ts") or 0.0) <= target_ts]
    if older:
        return max(older, key=lambda row: float(row.get("ts") or 0.0)), "24h"
    if snapshots:
        return min(snapshots, key=lambda row: float(row.get("ts") or 0.0)), "first_snapshot"
    return {}, "none"


def build_report(
    *,
    root: Path,
    guard_state_path: Path,
    queue_path: Path,
    wallet_event_log: Path,
    state_path: Path,
    max_event_tail_bytes: int,
    dow_profile_path: Path | None = None,
    member_rolling20_path: Path | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    now_ts = float(now_ts if now_ts is not None else time.time())
    data_dir = root / "data" / "research"
    guard = _load_json(guard_state_path, {})
    queue = _load_json(queue_path, {})
    prior = _load_json(state_path, {})
    dow_profiles = _profile_by_wallet(_load_json(dow_profile_path or (root / DEFAULT_DOW_PROFILE), {}))
    rolling20 = _rolling20_by_wallet(_load_json(member_rolling20_path or (root / DEFAULT_MEMBER_ROLLING20), {}))
    scorecard = _latest_scorecard(data_dir)
    queue_summary = queue.get("summary") if isinstance(queue.get("summary"), dict) else {}
    members = _active_members(guard if isinstance(guard, dict) else {})
    member_ids = sorted(str(row.get("candidate_id") or row.get("source_wallet") or "") for row in members)
    member_wallets = {str(row.get("source_wallet") or "").lower(): row for row in members if row.get("source_wallet")}

    latest_by_wallet: dict[str, dict[str, Any]] = {}
    event_rows = _iter_jsonl_tail(wallet_event_log, max_bytes=max_event_tail_bytes)
    active_hours: set[int] = set()
    for event in event_rows:
        wallet = str(event.get("source_wallet") or "").lower()
        if wallet not in member_wallets:
            continue
        ts = _parse_ts(event.get("observed_ts") or event.get("event_ts") or event.get("generated_at"))
        if ts >= now_ts - STALE_MEMBER_AGE_S:
            active_hours.add(datetime.fromtimestamp(ts, tz=timezone.utc).hour)
        if ts <= float(latest_by_wallet.get(wallet, {}).get("ts") or 0.0):
            continue
        latest_by_wallet[wallet] = {
            "ts": ts,
            "market_slug": event.get("market_slug") or event.get("event_slug"),
            "price": event.get("price"),
            "usdc_size": event.get("usdc_size"),
        }

    freshness_rows: list[dict[str, Any]] = []
    stale_members: list[str] = []
    raw_stale_members: list[str] = []
    calendar_protected_members: list[str] = []
    weekend_positive_protected_members: list[str] = []
    current_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    current_is_weekend = current_dt.weekday() >= 5
    for row in members:
        wallet = str(row.get("source_wallet") or "").lower()
        latest = latest_by_wallet.get(wallet, {})
        age_s = round(max(0.0, now_ts - float(latest.get("ts") or 0.0)), 3) if latest else None
        raw_stale = age_s is None or age_s > STALE_MEMBER_AGE_S
        profile = dow_profiles.get(wallet, {})
        expected_age_s = _calendar_expected_active_age_s(
            float(latest.get("ts") or 0.0) if latest else None,
            now_ts,
            profile,
        )
        calendar_stale = (
            expected_age_s is None and raw_stale
        ) or (
            expected_age_s is not None and expected_age_s > STALE_MEMBER_AGE_S
        )
        rolling = rolling20.get(wallet, {})
        rolling_pnl = float(rolling.get("rolling20_pnl_usd") or 0.0) if rolling else 0.0
        weekend_positive_protected = current_is_weekend and rolling_pnl > 0.0
        pre_registered_clock = (
            latest
            and float(latest.get("ts") or 0.0) < WEEKEND_AWARE_CLOCK_EFFECTIVE_TS <= now_ts
            and current_is_weekend
        )
        inactivity_demotion_allowed = bool(calendar_stale and not weekend_positive_protected)
        stale = inactivity_demotion_allowed
        label = str(row.get("candidate_id") or wallet)
        if raw_stale:
            raw_stale_members.append(label)
        if raw_stale and not calendar_stale:
            calendar_protected_members.append(label)
        if weekend_positive_protected:
            weekend_positive_protected_members.append(label)
            stale = False
            inactivity_demotion_allowed = False
        if stale:
            stale_members.append(label)
        freshness_rows.append(
            {
                "candidate_id": row.get("candidate_id"),
                "source_wallet": row.get("source_wallet"),
                "policy_id": row.get("policy_id"),
                "last_copy_eligible_flow_ts": latest.get("ts"),
                "last_copy_eligible_flow_age_s": age_s,
                "last_market_slug": latest.get("market_slug"),
                "raw_stale_gt_24h": raw_stale,
                "calendar_aware_stale_gt_24h": calendar_stale,
                "stale_gt_24h": stale,
                "calendar_expected_active_age_s": expected_age_s,
                "calendar_profile_found": bool(profile),
                "weekend_evidence_status": profile.get("weekend_evidence_status") if profile else None,
                "pre_registered_clock_requires_fable_rerule": bool(pre_registered_clock),
                "rolling20_pnl_usd": rolling.get("rolling20_pnl_usd") if rolling else None,
                "weekend_positive_pnl_protected": weekend_positive_protected,
                "inactivity_demotion_allowed": inactivity_demotion_allowed,
            }
        )

    snapshots = prior.get("snapshots") if isinstance(prior.get("snapshots"), list) else []
    snapshots = [row for row in snapshots if isinstance(row, dict)]
    compare, compare_basis = _snapshot_for_compare(snapshots, now_ts)
    previous_members = set(compare.get("member_ids") or [])
    current_members = set(member_ids)
    added = sorted(current_members - previous_members) if previous_members else []
    removed = sorted(previous_members - current_members) if previous_members else []
    member_count_delta = len(current_members) - len(previous_members) if previous_members else None

    volume = (
        ((scorecard.get("volume_kpi") or {}).get("canonical_daily") or {})
        if isinstance(scorecard.get("volume_kpi"), dict)
        else {}
    )
    windows_filled = int(volume.get("windows_filled") or 0)
    ready_for_live = int(queue_summary.get("ready_for_live") or 0)
    queue_depth = int(queue_summary.get("queue_depth") or 0)
    replay_candidates = int(queue_summary.get("replay_candidates") or 0)
    replay_promotable = int(queue_summary.get("replay_promotable") or 0)
    fill_backed = int(queue_summary.get("fill_backed_candidates") or 0)
    cause = "healthy"
    if ready_for_live < DEFECT_READY_QUEUE_BELOW:
        if queue_depth <= 0:
            cause = "sourcing_or_replay_throughput"
        elif replay_promotable > ready_for_live:
            cause = "promotion_gate_clearance"
        elif replay_candidates > 0:
            cause = "bar_or_measurement_filtering"
        else:
            cause = "sourcing_empty"

    defects: list[dict[str, Any]] = []
    if ready_for_live < DEFECT_READY_QUEUE_BELOW:
        defects.append(
            {
                "defect": "member_factory_ready_queue_below_3",
                "cause": cause,
                "next": "expand sourcing/replay throughput and review filter fallout before accepting thin queue",
            }
        )
    if member_count_delta is not None and member_count_delta <= 0 and windows_filled < 144:
        defects.append(
            {
                "defect": "active_set_flat_or_shrinking_while_volume_below_target",
                "cause": "member_factory_not_expanding_coverage",
                "next": "qualify/add complementary members or name the specific funnel stage limiting adds",
            }
        )
    if stale_members:
        defects.append(
            {
                "defect": "active_members_stale_gt_24h",
                "cause": "stale_member_capacity",
                "members": stale_members,
                "next": "review stale members for swap while preserving evidence on file",
            }
        )

    throughput = {
        "replay_candidates_total": replay_candidates,
        "fill_backed_candidates": fill_backed,
        "replay_promotable": replay_promotable,
        "ready_for_live": ready_for_live,
        "delta_basis": compare_basis,
        "entered_measurement_delta": None,
        "passing_delta": None,
    }
    if compare:
        throughput["entered_measurement_delta"] = replay_candidates - int(compare.get("replay_candidates") or 0)
        throughput["passing_delta"] = ready_for_live - int(compare.get("ready_for_live") or 0)
    rtds_rows = _iter_jsonl_tail(root / DEFAULT_RTDS_ACTIVITY_JSONL, max_bytes=max_event_tail_bytes)
    series_census = _series_census(rtds_rows, now_ts=now_ts)

    snapshot = {
        "ts": now_ts,
        "generated_at": _utc_now_iso(),
        "member_ids": member_ids,
        "member_count": len(member_ids),
        "ready_for_live": ready_for_live,
        "queue_depth": queue_depth,
        "replay_candidates": replay_candidates,
        "replay_promotable": replay_promotable,
    }
    retained = [
        row
        for row in snapshots
        if now_ts - float(row.get("ts") or 0.0) <= SNAPSHOT_RETENTION_S
    ]
    retained.append(snapshot)

    return {
        "schema_version": 1,
        "kind": "member_factory_kpi",
        "flow_stage": "PROMOTE/SELF-DEV",
        "generated_at": snapshot["generated_at"],
        "paper_only": True,
        "live_orders_allowed": False,
        "queue_depth": {
            "ready_for_live": ready_for_live,
            "target_ready_for_live": TARGET_READY_QUEUE,
            "defect_below": DEFECT_READY_QUEUE_BELOW,
            "queue_depth": queue_depth,
            "status": "DEFECT" if ready_for_live < DEFECT_READY_QUEUE_BELOW else "PASS",
            "named_cause": cause,
        },
        "set_trajectory": {
            "member_count": len(member_ids),
            "compare_basis": compare_basis,
            "comparison_member_count": len(previous_members) if previous_members else None,
            "member_count_delta": member_count_delta,
            "added": added,
            "removed_or_demoted": removed,
            "windows_filled": windows_filled,
            "target_windows_filled": 144,
        },
        "member_freshness": {
            "stale_threshold_s": STALE_MEMBER_AGE_S,
            "stale_members": stale_members,
            "raw_stale_members": raw_stale_members,
            "calendar_protected_members": calendar_protected_members,
            "weekend_positive_pnl_protected_members": weekend_positive_protected_members,
            "calendar_clock_effective_new_clocks_from": "2026-07-11T00:00:00Z",
            "members": freshness_rows,
        },
        "hour_coverage": {
            "window": "last_24h_active_member_wallet_events",
            "active_hours_utc": sorted(active_hours),
            "covered_hours": len(active_hours),
            "denominator_hours": 24,
            "coverage_pct": round(100.0 * len(active_hours) / 24.0, 6),
            "target": "source complementary off-peak wallets until portfolio approaches 24/24 coverage",
        },
        "factory_throughput": throughput,
        "series_census": series_census,
        "defects": defects,
        "snapshots": retained,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--queue", default="data/research/wallet_copy_full_pool_member_queue.json")
    parser.add_argument("--wallet-event-log", default="data/research/wallet_copy_live_guard_wallet_events.jsonl")
    parser.add_argument("--state", default="data/research/member_factory_kpi_state.json")
    parser.add_argument("--dow-profile", default=DEFAULT_DOW_PROFILE)
    parser.add_argument("--member-rolling20", default=DEFAULT_MEMBER_ROLLING20)
    parser.add_argument("--max-event-tail-bytes", type=int, default=50_000_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    report = build_report(
        root=root,
        guard_state_path=root / args.guard_state,
        queue_path=root / args.queue,
        wallet_event_log=root / args.wallet_event_log,
        state_path=root / args.state,
        dow_profile_path=root / args.dow_profile,
        member_rolling20_path=root / args.member_rolling20,
        max_event_tail_bytes=int(args.max_event_tail_bytes),
    )
    _atomic_write_json(root / args.state, report)
    print(json.dumps({k: report[k] for k in ("kind", "generated_at", "queue_depth", "set_trajectory")}, sort_keys=True))


if __name__ == "__main__":
    main()
