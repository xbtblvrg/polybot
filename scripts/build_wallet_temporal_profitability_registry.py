#!/usr/bin/env python3
"""Build per-wallet temporal profitability profiles for BTC-5m recruiting.

Flow stage: DISCOVER/LEARN/PROMOTE. This is a report-layer artifact only:
it reads local history/copyability evidence and emits candidates for
watch-tier measurement. It does not change live config or submit orders.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_strategy_decompiler_intake import (  # noqa: E402
    _default_resolutions_path,
    _float,
    _load_resolutions,
    _norm_outcome,
    _parse_ts,
    _utc_now_iso,
)
from src.wallet_copy.store import atomic_write_json  # noqa: E402
from src.wallet_copy.venue_executability import (  # noqa: E402
    row_is_venue_executable,
    venue_minimum_max_price,
)


DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_COPYABILITY = "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_DOW_PROFILE = "data/research/member_dow_profiles_latest.json"
DEFAULT_OUTPUT = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST = "data/research/temporal_supplemental_history_manifest.json"
VOLUME_STANDBY_WALLET = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
DAY_NAMES = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
SLICE_LABELS = ("PROVEN-POSITIVE", "PROVEN-NEGATIVE", "UNPROVEN")
FADING_MIN_RESOLVED_TRADES = 200
FADING_MAX_GAP_IN_SIGMA = -1.0


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _resolve_input_path(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _manifest_supplemental_paths(root: Path, args: argparse.Namespace) -> list[Path]:
    manifest_arg = str(getattr(args, "supplemental_history_manifest", DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST) or "")
    if not manifest_arg:
        return []
    manifest_path = _resolve_input_path(root, manifest_arg)
    payload = _load_json(manifest_path, None)
    if payload is None:
        return []
    raw_paths: list[Any]
    if isinstance(payload, list):
        raw_paths = payload
    elif isinstance(payload, dict):
        raw_paths = (
            payload.get("supplemental_history_files")
            or payload.get("paths")
            or payload.get("files")
            or []
        )
    else:
        raw_paths = []
    return [
        _resolve_input_path(root, str(value))
        for value in raw_paths
        if str(value or "").strip()
    ]


def _supplemental_history_paths(root: Path, args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    explicit_paths = list(getattr(args, "supplemental_history", []) or [])
    explicit_globs = list(getattr(args, "supplemental_history_glob", []) or [])
    for value in explicit_paths:
        text = str(value or "").strip()
        if text:
            paths.append(_resolve_input_path(root, text))
    for value in explicit_globs:
        pattern = str(value or "").strip()
        if not pattern:
            continue
        if Path(pattern).is_absolute():
            matches = Path("/").glob(pattern.lstrip("/"))
        else:
            matches = root.glob(pattern)
        paths.extend(path for path in matches if path.is_file())
    if not paths:
        paths.extend(_manifest_supplemental_paths(root, args))
    out: list[Path] = []
    seen: set[str] = set()
    primary = _resolve_input_path(root, getattr(args, "history", DEFAULT_HISTORY)).resolve()
    for path in paths:
        resolved = path.resolve()
        key = str(resolved)
        if key in seen or resolved == primary or not resolved.exists():
            continue
        seen.add(key)
        out.append(resolved)
    return sorted(out)


def _merge_history_states(root: Path, args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    primary_path = _resolve_input_path(root, args.history)
    primary = _load_json(primary_path, {})
    primary = primary if isinstance(primary, dict) else {}
    supplemental_paths = _supplemental_history_paths(root, args)
    if not supplemental_paths:
        return primary, {
            "primary_history": args.history,
            "supplemental_history_files": [],
            "supplemental_history_events": 0,
            "supplemental_history_wallets": 0,
        }

    merged = dict(primary)
    events = [row for row in primary.get("events") or [] if isinstance(row, dict)]
    copy_intents = [row for row in primary.get("copy_intents") or [] if isinstance(row, dict)]
    wallets = [row for row in primary.get("wallets") or [] if isinstance(row, dict)]
    wallet_results = [row for row in primary.get("wallet_results") or [] if isinstance(row, dict)]
    supplemental_events = 0
    supplemental_wallets: set[str] = set()
    rel_paths: list[str] = []
    for path in supplemental_paths:
        payload = _load_json(path, {})
        if not isinstance(payload, dict):
            continue
        rel_paths.append(str(path.relative_to(root)) if path.is_relative_to(root) else str(path))
        payload_events = [row for row in payload.get("events") or [] if isinstance(row, dict)]
        payload_intents = [row for row in payload.get("copy_intents") or [] if isinstance(row, dict)]
        payload_wallets = [row for row in payload.get("wallets") or [] if isinstance(row, dict)]
        payload_results = [row for row in payload.get("wallet_results") or [] if isinstance(row, dict)]
        events.extend(payload_events)
        copy_intents.extend(payload_intents)
        wallets.extend(payload_wallets)
        wallet_results.extend(payload_results)
        supplemental_events += len(payload_events)
        for row in payload_events:
            wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
            if wallet:
                supplemental_wallets.add(wallet)
        for row in payload_wallets:
            wallet = _norm_wallet(row.get("address") or row.get("wallet"))
            if wallet:
                supplemental_wallets.add(wallet)

    merged.update(
        {
            "schema_version": 1,
            "kind": "wallet_copy_history_state",
            "paper_only": True,
            "live_orders_allowed": False,
            "events": events,
            "copy_intents": copy_intents,
            "wallets": wallets,
            "wallet_results": wallet_results,
        }
    )
    return merged, {
        "primary_history": args.history,
        "supplemental_history_files": rel_paths,
        "supplemental_history_events": supplemental_events,
        "supplemental_history_wallets": len(supplemental_wallets),
    }


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _iso(ts: float | None) -> str | None:
    if not ts:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _wallet_registry(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in payload.get("wallets") or []:
        if not isinstance(item, dict):
            continue
        wallet = _norm_wallet(item.get("address") or item.get("wallet"))
        if not wallet:
            continue
        rows[wallet] = {
            "wallet": wallet,
            "registry_name": str(item.get("name") or ""),
            "registry_enabled": item.get("enabled") is not False,
            "registry_tags": [str(tag) for tag in item.get("tags") or [] if str(tag or "")],
        }
    return rows


def _copyability_by_wallet(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in payload.get("leaderboard") or payload.get("top_wallets") or []:
        if not isinstance(item, dict):
            continue
        wallet = _norm_wallet(item.get("wallet"))
        if wallet:
            rows[wallet] = item
    return rows


def _slug_window_start(slug: Any) -> int | None:
    text = str(slug or "")
    if not text.startswith("btc-updown-5m-"):
        return None
    try:
        return int(text.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _stake(row: dict[str, Any]) -> float:
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    usdc = _float(row.get("usdc_size"), 0.0)
    return usdc if usdc > 0 else price * size if price > 0 and size > 0 else 0.0


def _new_stats() -> dict[str, Any]:
    return {
        "resolved_trades": 0,
        "wins": 0,
        "stake_usd": 0.0,
        "pnl_usd": 0.0,
        "win_pnl_usd": 0.0,
        "loss_abs_pnl_usd": 0.0,
        "first_event_ts": None,
        "latest_event_ts": None,
        "_windows": set(),
    }


def _add_stats(stats: dict[str, Any], *, win: bool, stake: float, pnl: float, ts: float, window: str) -> None:
    stats["resolved_trades"] += 1
    stats["wins"] += int(win)
    stats["stake_usd"] += float(stake)
    stats["pnl_usd"] += float(pnl)
    if win:
        stats["win_pnl_usd"] += max(0.0, float(pnl))
    else:
        stats["loss_abs_pnl_usd"] += max(0.0, -float(pnl))
    stats["_windows"].add(window)
    first = stats.get("first_event_ts")
    latest = stats.get("latest_event_ts")
    stats["first_event_ts"] = ts if first is None else min(float(first), ts)
    stats["latest_event_ts"] = ts if latest is None else max(float(latest), ts)


def _merge_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = _new_stats()
    for stats in rows:
        out["resolved_trades"] += int(stats.get("resolved_trades") or 0)
        out["wins"] += int(stats.get("wins") or 0)
        out["stake_usd"] += float(stats.get("stake_usd") or 0.0)
        out["pnl_usd"] += float(stats.get("pnl_usd") or 0.0)
        out["win_pnl_usd"] += float(stats.get("win_pnl_usd") or 0.0)
        out["loss_abs_pnl_usd"] += float(stats.get("loss_abs_pnl_usd") or 0.0)
        out["_windows"].update(stats.get("_windows") or set())
        first = stats.get("first_event_ts")
        latest = stats.get("latest_event_ts")
        if first is not None:
            out["first_event_ts"] = float(first) if out["first_event_ts"] is None else min(float(out["first_event_ts"]), float(first))
        if latest is not None:
            out["latest_event_ts"] = float(latest) if out["latest_event_ts"] is None else max(float(out["latest_event_ts"]), float(latest))
    return out


def _final_stats(stats: dict[str, Any], *, now_ts: float) -> dict[str, Any]:
    resolved = int(stats.get("resolved_trades") or 0)
    wins = int(stats.get("wins") or 0)
    stake = float(stats.get("stake_usd") or 0.0)
    pnl = float(stats.get("pnl_usd") or 0.0)
    losses = max(0, resolved - wins)
    win_pnl = float(stats.get("win_pnl_usd") or 0.0)
    loss_abs_pnl = float(stats.get("loss_abs_pnl_usd") or 0.0)
    avg_win = win_pnl / wins if wins else None
    avg_loss_abs = loss_abs_pnl / losses if losses else None
    required = None
    gap_pp = None
    sigma_pp = None
    gap_in_sigma = None
    if resolved > 0 and avg_win and avg_loss_abs and avg_win > 0.0 and avg_loss_abs > 0.0:
        required = avg_loss_abs / (avg_loss_abs + avg_win)
        actual = wins / resolved
        gap_pp = 100.0 * (actual - required)
        sigma_pp = math.sqrt(required * (1.0 - required) / resolved) * 100.0
        gap_in_sigma = gap_pp / sigma_pp if sigma_pp > 0.0 else None
    latest = stats.get("latest_event_ts")
    return {
        "resolved_trades": resolved,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(100.0 * wins / resolved, 6) if resolved else None,
        "stake_usd": round(stake, 6),
        "pnl_usd": round(pnl, 6),
        "win_pnl_usd": round(win_pnl, 6),
        "loss_abs_pnl_usd": round(loss_abs_pnl, 6),
        "avg_win_per_winner_usd": round(avg_win, 6) if avg_win is not None else None,
        "avg_loss_per_loser_abs_usd": round(avg_loss_abs, 6) if avg_loss_abs is not None else None,
        "required_win_rate_at_payoff_shape_pct": round(required * 100.0, 6) if required is not None else None,
        "actual_minus_required_win_rate_pp": round(gap_pp, 6) if gap_pp is not None else None,
        "gap_sigma_pp": round(sigma_pp, 6) if sigma_pp is not None else None,
        "gap_in_sigma": round(gap_in_sigma, 6) if gap_in_sigma is not None else None,
        "roi_pct": round(100.0 * pnl / stake, 6) if stake else None,
        "unique_windows": len(stats.get("_windows") or set()),
        "first_event_ts": _iso(stats.get("first_event_ts")),
        "latest_event_ts": _iso(latest),
        "latest_event_age_h": round(max(0.0, now_ts - float(latest)) / 3600.0, 6) if latest else None,
    }


def _is_profitable(profile: dict[str, Any], *, min_trades: int) -> bool:
    return int(profile.get("resolved_trades") or 0) >= int(min_trades) and float(profile.get("roi_pct") or 0.0) > 0.0


def _slice_label(profile: dict[str, Any], *, min_trades: int) -> dict[str, Any]:
    resolved = int(profile.get("resolved_trades") or 0)
    raw_roi = profile.get("roi_pct")
    roi = float(raw_roi) if raw_roi is not None else 0.0
    if resolved < int(min_trades):
        label = "UNPROVEN"
        reason = f"n={resolved} below min_trades={int(min_trades)}; roi={raw_roi}"
    elif roi > 0.0:
        label = "PROVEN-POSITIVE"
        reason = f"n={resolved} >= min_trades={int(min_trades)}; roi={raw_roi} > 0"
    else:
        label = "PROVEN-NEGATIVE"
        reason = f"n={resolved} >= min_trades={int(min_trades)}; roi={raw_roi} <= 0"
    return {
        "label": label,
        "resolved_trades": resolved,
        "roi_pct": raw_roi,
        "pnl_usd": profile.get("pnl_usd"),
        "min_trades": int(min_trades),
        "reason": reason,
    }


def _new_wallet_accumulator(wallet: str, registry_row: dict[str, Any] | None = None) -> dict[str, Any]:
    registry_row = registry_row if isinstance(registry_row, dict) else {}
    return {
        "wallet": wallet,
        "registry": registry_row,
        "all": _new_stats(),
        "regimes": {"weekday": _new_stats(), "weekend": _new_stats()},
        "hours": {hour: _new_stats() for hour in range(24)},
        "regime_hours": {
            "weekday": {hour: _new_stats() for hour in range(24)},
            "weekend": {hour: _new_stats() for hour in range(24)},
        },
        "venue_all": _new_stats(),
        "venue_regimes": {"weekday": _new_stats(), "weekend": _new_stats()},
        "venue_hours": {hour: _new_stats() for hour in range(24)},
        "venue_regime_hours": {
            "weekday": {hour: _new_stats() for hour in range(24)},
            "weekend": {hour: _new_stats() for hour in range(24)},
        },
        "trade_pnls": [],
        "venue_trade_pnls": [],
        "dedupe_count": 0,
    }


def _event_identity(row: dict[str, Any], *, wallet: str, slug: str, outcome: str, ts: float) -> tuple[str, str, str, str, float]:
    tx = str(row.get("transaction_hash") or row.get("transactionHash") or "")
    if not tx:
        tx = str(row.get("event_id") or row.get("source_fingerprint") or "")
    return wallet, tx, slug, outcome, round(float(ts), 3)


def _build_accumulators(
    *,
    registry_rows: dict[str, dict[str, Any]],
    history_state: dict[str, Any],
    winners: dict[str, str],
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    accumulators = {
        wallet: _new_wallet_accumulator(wallet, row)
        for wallet, row in registry_rows.items()
    }
    skipped = defaultdict(int)
    seen: set[tuple[str, str, str, str, float]] = set()
    events = history_state.get("events") if isinstance(history_state.get("events"), list) else []
    for event in events:
        if not isinstance(event, dict):
            continue
        skipped["events_scanned"] += 1
        if str(event.get("action") or "").upper() != "BUY":
            skipped["non_buy"] += 1
            continue
        wallet = _norm_wallet(event.get("source_wallet") or event.get("wallet"))
        if not wallet:
            skipped["missing_wallet"] += 1
            continue
        slug = str(event.get("market_slug") or event.get("event_slug") or "")
        window_start = _slug_window_start(slug)
        if window_start is None:
            skipped["non_btc5m"] += 1
            continue
        winner = winners.get(slug) or winners.get(str(event.get("condition_id") or ""))
        if not winner:
            skipped["unresolved"] += 1
            continue
        outcome = _norm_outcome(event.get("outcome"))
        price = _float(event.get("price"), 0.0)
        stake = _stake(event)
        ts = _parse_ts(event.get("event_ts") or event.get("observed_ts"))
        if not outcome or price <= 0.0 or price >= 1.0 or stake <= 0.0 or ts <= 0.0:
            skipped["bad_price_size_or_ts"] += 1
            continue
        identity = _event_identity(event, wallet=wallet, slug=slug, outcome=outcome, ts=ts)
        if identity in seen:
            skipped["duplicates"] += 1
            accumulators.setdefault(wallet, _new_wallet_accumulator(wallet))["dedupe_count"] += 1
            continue
        seen.add(identity)
        win = outcome == winner
        pnl = ((1.0 / price - 1.0) if win else -1.0) * stake
        dt = datetime.fromtimestamp(ts, tz=UTC)
        regime = "weekend" if dt.weekday() >= 5 else "weekday"
        hour = int(dt.hour)
        window = str(window_start)
        acc = accumulators.setdefault(wallet, _new_wallet_accumulator(wallet))
        _add_stats(acc["all"], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
        _add_stats(acc["regimes"][regime], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
        _add_stats(acc["hours"][hour], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
        _add_stats(acc["regime_hours"][regime][hour], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
        acc["trade_pnls"].append((ts, pnl, stake, win, window, regime, hour))
        if row_is_venue_executable(event, min_order_usd=1.0):
            _add_stats(acc["venue_all"], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
            _add_stats(acc["venue_regimes"][regime], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
            _add_stats(acc["venue_hours"][hour], win=win, stake=stake, pnl=pnl, ts=ts, window=window)
            _add_stats(
                acc["venue_regime_hours"][regime][hour],
                win=win,
                stake=stake,
                pnl=pnl,
                ts=ts,
                window=window,
            )
            acc["venue_trade_pnls"].append(
                (ts, pnl, stake, win, window, regime, hour)
            )
    return accumulators, dict(sorted(skipped.items()))


def _recent_profile(trades: list[tuple[float, float, float, bool, str, str, int]], *, now_ts: float) -> dict[str, Any]:
    if not trades:
        return _final_stats(_new_stats(), now_ts=now_ts)
    trades = sorted(trades, key=lambda row: row[0])
    sample = min(50, max(5, len(trades) // 4))
    stats = _new_stats()
    for ts, pnl, stake, win, window, _regime, _hour in trades[-sample:]:
        _add_stats(stats, win=win, stake=stake, pnl=pnl, ts=ts, window=window)
    return _final_stats(stats, now_ts=now_ts)


def _classify_wallet(
    *,
    all_profile: dict[str, Any],
    recent_profile: dict[str, Any],
    regime_profiles: dict[str, dict[str, Any]],
    dead_band_profile: dict[str, Any],
    profitable_hours: list[str],
    min_trades: int,
    min_band_trades: int,
) -> tuple[str, str]:
    all_positive = _is_profitable(all_profile, min_trades=min_trades)
    recent_negative = (
        all_positive
        and int(recent_profile.get("resolved_trades") or 0) >= FADING_MIN_RESOLVED_TRADES
        and float(recent_profile.get("roi_pct") or 0.0) <= 0.0
        and float(recent_profile.get("gap_in_sigma") or 0.0) <= FADING_MAX_GAP_IN_SIGMA
    )
    if recent_negative:
        return "FADING", "historical ROI positive, recent resolved sample non-positive"
    weekday_positive = _is_profitable(regime_profiles["weekday"], min_trades=min_trades)
    weekend_positive = _is_profitable(regime_profiles["weekend"], min_trades=min_trades)
    if weekday_positive and weekend_positive:
        return "CONTINUOUS", "weekday and weekend regimes are profitable"
    if weekday_positive:
        return "WEEKDAY-ONLY", "weekday regime is profitable; weekend is not proven positive"
    if weekend_positive:
        return "WEEKEND-ONLY", "weekend regime is profitable; weekday is not proven positive"
    if _is_profitable(dead_band_profile, min_trades=min_band_trades) or profitable_hours:
        return "BAND-SPECIALIST", "one or more UTC hour bands are profitable while broad regimes are not"
    if all_positive:
        return "BAND-SPECIALIST", "aggregate profitability exists but regime sample is not broad enough"
    if int(all_profile.get("resolved_trades") or 0) <= 0:
        return "NO_RESOLVED_BTC5M_HISTORY", "no resolved BTC-5m BUY history in the local corpus"
    return "UNPROFITABLE_OR_INSUFFICIENT", "resolved history is not positive at the configured sample bar"


def _copy_metrics(copyability: dict[str, Any]) -> dict[str, Any]:
    replay = copyability.get("copy_replay") if isinstance(copyability.get("copy_replay"), dict) else {}
    followability = copyability.get("followability") if isinstance(copyability.get("followability"), dict) else {}
    source_history = copyability.get("source_history") if isinstance(copyability.get("source_history"), dict) else {}
    return {
        "copyability_score": _float(copyability.get("copyability_score"), 0.0),
        "admission_status": copyability.get("admission_status"),
        "queue_eligible": bool(copyability.get("queue_eligible")),
        "paper_pnl_usd": replay.get("paper_pnl_usd"),
        "copyable_buy_events": replay.get("copyable_buy_events"),
        "resolved_orders": replay.get("resolved_orders"),
        "followability_score": _float(followability.get("score"), 0.0),
        "followability_windows": int(followability.get("eligible_windows") or 0),
        "source_latest_event_ts": source_history.get("latest_event_ts"),
    }


def _finalize_wallet(
    wallet: str,
    acc: dict[str, Any],
    *,
    copyability: dict[str, Any],
    now_ts: float,
    min_trades: int,
    min_band_trades: int,
    dead_band_hours: range,
) -> dict[str, Any]:
    all_profile = _final_stats(acc["all"], now_ts=now_ts)
    recent = _recent_profile(acc["trade_pnls"], now_ts=now_ts)
    regime_profiles = {
        regime: _final_stats(stats, now_ts=now_ts)
        for regime, stats in acc["regimes"].items()
    }
    regime_hour_profiles: dict[str, dict[str, Any]] = {}
    profitable_hours: list[str] = []
    for regime, hours in acc["regime_hours"].items():
        regime_hour_profiles[regime] = {}
        for hour, stats in hours.items():
            profile = _final_stats(stats, now_ts=now_ts)
            if int(profile["resolved_trades"]) <= 0:
                continue
            key = f"{hour:02d}"
            regime_hour_profiles[regime][key] = profile
            if _is_profitable(profile, min_trades=min_band_trades):
                profitable_hours.append(f"{regime}:{key}")
    dead_stats = _merge_stats([acc["hours"][hour] for hour in dead_band_hours])
    dead_profile = _final_stats(dead_stats, now_ts=now_ts)
    venue_all_profile = _final_stats(acc["venue_all"], now_ts=now_ts)
    venue_recent = _recent_profile(acc["venue_trade_pnls"], now_ts=now_ts)
    venue_regime_profiles = {
        regime: _final_stats(stats, now_ts=now_ts)
        for regime, stats in acc["venue_regimes"].items()
    }
    venue_dead_profile = _final_stats(
        _merge_stats([acc["venue_hours"][hour] for hour in dead_band_hours]),
        now_ts=now_ts,
    )
    venue_regime_hour_profiles: dict[str, dict[str, Any]] = {}
    for regime, hours in acc["venue_regime_hours"].items():
        venue_regime_hour_profiles[regime] = {
            f"{hour:02d}": profile
            for hour, stats in hours.items()
            if int(
                (
                    profile := _final_stats(stats, now_ts=now_ts)
                )["resolved_trades"]
            )
            > 0
        }
    classification, reason = _classify_wallet(
        all_profile=all_profile,
        recent_profile=recent,
        regime_profiles=regime_profiles,
        dead_band_profile=dead_profile,
        profitable_hours=sorted(profitable_hours),
        min_trades=min_trades,
        min_band_trades=min_band_trades,
    )
    return {
        "wallet": wallet,
        "registry": acc.get("registry") or {},
        "classification": classification,
        "classification_reason": reason,
        "all": all_profile,
        "recent": recent,
        "regime_profiles": regime_profiles,
        "regime_hour_profiles": regime_hour_profiles,
        "slice_labels": {
            "all": _slice_label(all_profile, min_trades=min_trades),
            "weekday": _slice_label(regime_profiles["weekday"], min_trades=min_trades),
            "weekend": _slice_label(regime_profiles["weekend"], min_trades=min_trades),
            "dead_band_18_22_utc": _slice_label(dead_profile, min_trades=min_band_trades),
        },
        "venue_executable": {
            "min_order_usd": 1.0,
            "max_price": round(venue_minimum_max_price(), 6),
            "all": venue_all_profile,
            "recent": venue_recent,
            "regime_profiles": venue_regime_profiles,
            "regime_hour_profiles": venue_regime_hour_profiles,
            "dead_band_18_22_utc": venue_dead_profile,
            "venue_executable_resolved": venue_all_profile["resolved_trades"],
            "venue_unreachable_resolved": (
                int(all_profile["resolved_trades"])
                - int(venue_all_profile["resolved_trades"])
            ),
            "venue_reachable_share_pct": (
                round(
                    100.0
                    * int(venue_all_profile["resolved_trades"])
                    / int(all_profile["resolved_trades"]),
                    6,
                )
                if int(all_profile["resolved_trades"])
                else None
            ),
        },
        "venue_slice_labels": {
            "all": _slice_label(venue_all_profile, min_trades=min_trades),
            "weekday": _slice_label(
                venue_regime_profiles["weekday"], min_trades=min_trades
            ),
            "weekend": _slice_label(
                venue_regime_profiles["weekend"], min_trades=min_trades
            ),
            "dead_band_18_22_utc": _slice_label(
                venue_dead_profile, min_trades=min_band_trades
            ),
        },
        "dead_band_18_22_utc": dead_profile,
        "profitable_hour_bands": sorted(profitable_hours),
        "copyability": _copy_metrics(copyability),
        "deduped_duplicate_events": int(acc.get("dedupe_count") or 0),
    }


def _candidate_score(
    row: dict[str, Any],
    *,
    fresh_max_age_h: float,
    stale_after_h: float,
    min_band_trades: int,
) -> tuple[bool, float, dict[str, Any]]:
    dead = row["dead_band_18_22_utc"]
    copyability = row["copyability"]
    age = row["all"].get("latest_event_age_h")
    dead_age = dead.get("latest_event_age_h")
    fresh = age is not None and float(age) <= float(fresh_max_age_h)
    paper_pnl = copyability.get("paper_pnl_usd")
    positive_copy = (
        bool(copyability.get("queue_eligible"))
        or _float(paper_pnl, 0.0) > 0.0
        or _float(copyability.get("copyability_score"), 0.0) > 0.0
        or _float(copyability.get("followability_score"), 0.0) > 0.0
    )
    dead_positive = _is_profitable(dead, min_trades=min_band_trades)
    dead_profitable_hour = any(
        band.endswith(tuple(f":{hour:02d}" for hour in range(18, 22)))
        for band in row.get("profitable_hour_bands") or []
    )
    dead_roi = _float(dead.get("roi_pct"), 0.0)
    dead_nonnegative = dead_roi >= 0.0
    inclusion_reason = ""
    if dead_positive:
        inclusion_reason = "dead_band_positive"
    elif dead_profitable_hour and dead_nonnegative:
        inclusion_reason = "dead_profitable_hour_with_nonnegative_dead_band_roi"
    eligible = fresh and positive_copy and dead_nonnegative and bool(inclusion_reason)
    roi = max(0.0, dead_roi)
    n = int(dead.get("resolved_trades") or 0)
    ordering_age = dead_age if dead_age is not None else age
    stale = ordering_age is not None and float(ordering_age) > float(stale_after_h)
    score = (
        roi * math.log1p(max(1, n))
        + 0.05 * _float(copyability.get("copyability_score"), 0.0)
        + 0.02 * _float(copyability.get("followability_score"), 0.0)
        + max(0.0, float(stale_after_h) - float(ordering_age or stale_after_h)) / max(1.0, float(stale_after_h))
    )
    detail = {
        "fresh": fresh,
        "positive_copyability_or_followability": positive_copy,
        "dead_band_positive": dead_positive,
        "dead_profitable_hour": dead_profitable_hour,
        "dead_band_nonnegative_roi": dead_nonnegative,
        "inclusion_reason": inclusion_reason,
        "latest_event_age_h": age,
        "latest_dead_band_event_age_h": dead_age,
        "dead_band_resolved_trades": n,
        "dead_band_roi_pct": dead.get("roi_pct"),
        "staleness": {
            "stale_after_h": float(stale_after_h),
            "ordering_age_h": ordering_age,
            "dead_band_stale_gt_threshold": bool(stale),
            "all_event_stale_gt_threshold": bool(age is not None and float(age) > float(stale_after_h)),
        },
    }
    return eligible, round(score, 6), detail


def _candidate_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    fit = row.get("fit") if isinstance(row.get("fit"), dict) else {}
    staleness = fit.get("staleness") if isinstance(fit.get("staleness"), dict) else {}
    dead = row.get("dead_band_18_22_utc") if isinstance(row.get("dead_band_18_22_utc"), dict) else {}
    ordering_age = staleness.get("ordering_age_h")
    age_sort = float(ordering_age) if ordering_age is not None else 1_000_000.0
    return (
        bool(staleness.get("dead_band_stale_gt_threshold")),
        -float(row.get("rank_score") or 0.0),
        age_sort,
        -int(dead.get("resolved_trades") or 0),
        str(row.get("wallet") or ""),
    )


def _dow_weight_verification(dow_profile: dict[str, Any], *, dead_band_hours: range) -> dict[str, Any]:
    profiles = dow_profile.get("profiles") if isinstance(dow_profile.get("profiles"), list) else []
    active_profiles = [
        row for row in profiles
        if isinstance(row, dict) and "active_member" in set(row.get("roles") or [])
    ]
    nonzero_cells = 0
    active_with_history = 0
    friday_dead_band_weights: dict[str, dict[str, Any]] = {}
    for row in active_profiles:
        if int(row.get("trade_count") or 0) > 0:
            active_with_history += 1
        weights = row.get("expected_active_dow_hour_weights") if isinstance(row.get("expected_active_dow_hour_weights"), dict) else {}
        nonzero_cells += sum(1 for value in weights.values() if _float(value, 0.0) > 0.0)
        friday_dead_band_weights[str(row.get("wallet"))] = {
            f"4:{hour:02d}": weights.get(f"4:{hour:02d}")
            for hour in dead_band_hours
        }
    return {
        "source_generated_at": dow_profile.get("generated_at"),
        "status": "PASS_REAL_EVENT_HOURS" if active_with_history and nonzero_cells else "NO_ACTIVE_WEIGHT_EVIDENCE",
        "active_profiles": len(active_profiles),
        "active_profiles_with_history": active_with_history,
        "nonzero_active_dow_hour_weight_cells": nonzero_cells,
        "dead_band_utc": "18-22",
        "friday_dead_band_weights_by_wallet": friday_dead_band_weights,
        "current_dead_band_empty_confirmed": bool(
            active_profiles
            and all(
                _float(weight, 0.0) == 0.0
                for row in friday_dead_band_weights.values()
                for weight in row.values()
            )
        ),
    }


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    registry_rows = _wallet_registry(_load_json(root / args.registry, {}))
    copyability_rows = _copyability_by_wallet(_load_json(root / args.copyability, {}))
    explicit_coverage = getattr(args, "include_wallet", None)
    requested_coverage_wallets = {
        wallet
        for value in (explicit_coverage or [VOLUME_STANDBY_WALLET])
        if (wallet := _norm_wallet(value))
    }
    if explicit_coverage:
        for wallet in requested_coverage_wallets:
            registry_rows.setdefault(wallet, {"wallet": wallet, "registry_status": "REQUIRED_TEMPORAL_COVERAGE"})
    required_coverage_wallets = requested_coverage_wallets & (set(registry_rows) | set(copyability_rows))
    for wallet in copyability_rows:
        registry_rows.setdefault(wallet, {"wallet": wallet, "registry_status": "UNREGISTERED_EVIDENCE"})
    resolutions_path = root / args.resolutions if args.resolutions else _default_resolutions_path(root)
    history_state, history_inputs = _merge_history_states(root, args)
    accumulators, skipped = _build_accumulators(
        registry_rows=registry_rows,
        history_state=history_state,
        winners=_load_resolutions(resolutions_path),
    )
    now_ts = datetime.now(tz=UTC).timestamp()
    dead_band_hours = range(int(args.dead_band_start_hour), int(args.dead_band_end_hour))
    wallets = [
        _finalize_wallet(
            wallet,
            acc,
            copyability=copyability_rows.get(wallet, {}),
            now_ts=now_ts,
            min_trades=int(args.min_trades),
            min_band_trades=int(args.min_band_trades),
            dead_band_hours=dead_band_hours,
        )
        for wallet, acc in accumulators.items()
    ]
    classification_counts: dict[str, int] = defaultdict(int)
    slice_label_counts: dict[str, dict[str, int]] = {
        "all": defaultdict(int),
        "weekday": defaultdict(int),
        "weekend": defaultdict(int),
        "dead_band_18_22_utc": defaultdict(int),
    }
    for row in wallets:
        classification_counts[str(row["classification"])] += 1
        labels = row.get("slice_labels") if isinstance(row.get("slice_labels"), dict) else {}
        for slice_name, counts in slice_label_counts.items():
            slice_row = labels.get(slice_name) if isinstance(labels.get(slice_name), dict) else {}
            label = str(slice_row.get("label") or "UNPROVEN")
            counts[label] += 1

    candidates: list[dict[str, Any]] = []
    for row in wallets:
        eligible, score, detail = _candidate_score(
            row,
            fresh_max_age_h=float(args.fresh_max_age_h),
            stale_after_h=float(args.stale_after_h),
            min_band_trades=int(args.min_band_trades),
        )
        if not eligible:
            continue
        candidates.append(
            {
                "wallet": row["wallet"],
                "rank_score": score,
                "classification": row["classification"],
                "slice_labels": row["slice_labels"],
                "dead_band_18_22_utc": row["dead_band_18_22_utc"],
                "all": row["all"],
                "copyability": row["copyability"],
                "fit": detail,
                "watch_tier_policy": {
                    "policy_type": "HOUR_BAND_PROBATION",
                    "hours_utc": [int(hour) for hour in dead_band_hours],
                    "paper_first": True,
                    "live_orders_allowed": False,
                    "probe_feed_inclusion_reason": detail.get("inclusion_reason"),
                    "staleness": detail.get("staleness"),
                },
            }
        )
    candidates.sort(key=_candidate_sort_key)
    candidate_wallets = {str(row["wallet"]) for row in candidates[: int(args.top_candidates)]}

    wallets.sort(
        key=lambda row: (
            0 if row["wallet"] in candidate_wallets else 1,
            str(row["classification"]),
            -int(row["all"].get("resolved_trades") or 0),
            row["wallet"],
        )
    )
    emitted_wallets = [
        row
        for row in wallets
        if int(row["all"].get("resolved_trades") or 0) > 0
        or row["wallet"] in candidate_wallets
        or row["wallet"] in required_coverage_wallets
    ]

    return {
        "schema_version": 1,
        "kind": "wallet_temporal_profitability_registry",
        "flow_stage": "DISCOVER/LEARN/PROMOTE",
        "generated_at": _utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "inputs": {
            "registry": args.registry,
            "history": args.history,
            "supplemental_history_files": history_inputs["supplemental_history_files"],
            "copyability": args.copyability,
            "dow_profile": args.dow_profile,
            "resolutions": str(resolutions_path.relative_to(root)) if resolutions_path.is_relative_to(root) else str(resolutions_path),
        },
        "criteria": {
            "min_trades": int(args.min_trades),
            "min_band_trades": int(args.min_band_trades),
            "fading_min_resolved_trades": FADING_MIN_RESOLVED_TRADES,
            "fading_max_gap_in_sigma": FADING_MAX_GAP_IN_SIGMA,
            "fresh_max_age_h": float(args.fresh_max_age_h),
            "stale_after_h": float(args.stale_after_h),
            "dead_band_utc": "18-22",
            "classification_labels": ["CONTINUOUS", "WEEKDAY-ONLY", "WEEKEND-ONLY", "BAND-SPECIALIST", "FADING"],
            "slice_labels": list(SLICE_LABELS),
            "slice_label_rule": (
                "per-slice labels are evidence states: PROVEN-POSITIVE requires n>=min and ROI>0; "
                "PROVEN-NEGATIVE requires n>=min and ROI<=0; UNPROVEN means sample below min, not negative evidence"
            ),
            "empty_wallet_rows_omitted": True,
            "required_coverage_wallets": sorted(required_coverage_wallets),
            "watch_tier_feed_rule": (
                "feed rows must carry staleness metadata and have non-negative dead-band ROI "
                "or a named inclusion reason"
            ),
        },
        "dow_weight_verification": _dow_weight_verification(
            _load_json(root / args.dow_profile, {}),
            dead_band_hours=dead_band_hours,
        ),
        "summary": {
            "wallets_total": len(wallets),
            "wallets_with_resolved_btc5m_history": sum(1 for row in wallets if int(row["all"].get("resolved_trades") or 0) > 0),
            "classification_counts": dict(sorted(classification_counts.items())),
            "slice_label_counts": {
                slice_name: dict(sorted(counts.items()))
                for slice_name, counts in slice_label_counts.items()
            },
            "dead_band_candidate_count": len(candidates),
            "watch_tier_feed_count": min(int(args.top_candidates), len(candidates)),
            "wallet_rows_emitted": len(emitted_wallets),
            "required_coverage_rows_emitted": sum(
                1 for row in emitted_wallets if row["wallet"] in required_coverage_wallets
            ),
            "history_skipped": skipped,
            "supplemental_history_files": len(history_inputs["supplemental_history_files"]),
            "supplemental_history_events": history_inputs["supplemental_history_events"],
            "supplemental_history_wallets": history_inputs["supplemental_history_wallets"],
        },
        "watch_tier_probe_feed": {
            "status": "READY_FOR_WATCH_TIER_PROBE" if candidates else "NO_CANDIDATES_AT_CURRENT_BAR",
            "flow_stage": "DISCOVER/LEARN",
            "candidates": candidates[: int(args.top_candidates)],
        },
        "wallets": emitted_wallets,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--supplemental-history", action="append", default=[])
    parser.add_argument("--supplemental-history-glob", action="append", default=[])
    parser.add_argument("--supplemental-history-manifest", default=DEFAULT_SUPPLEMENTAL_HISTORY_MANIFEST)
    parser.add_argument("--copyability", default=DEFAULT_COPYABILITY)
    parser.add_argument("--dow-profile", default=DEFAULT_DOW_PROFILE)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--min-band-trades", type=int, default=3)
    parser.add_argument("--fresh-max-age-h", type=float, default=72.0)
    parser.add_argument("--stale-after-h", type=float, default=24.0)
    parser.add_argument("--dead-band-start-hour", type=int, default=18)
    parser.add_argument("--dead-band-end-hour", type=int, default=22)
    parser.add_argument("--top-candidates", type=int, default=10)
    parser.add_argument(
        "--include-wallet",
        action="append",
        default=None,
        help="wallet that must have an explicit temporal row; defaults to the volume standby wallet",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(ROOT, args)
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
