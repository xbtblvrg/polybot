"""Promotion and rotation decision evidence for wallet-copy.

This module does not submit orders or rewrite the live guard. It turns persisted
LIVE and OBSERVE evidence into an explicit decision packet for Fable: whether the
current live price-band sample is still acceptable, and whether the broad paper
lane has a positive, live-executable replacement candidate.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS, WATCH


@dataclass(frozen=True)
class PromotionRotationConfig:
    live_max_buy_price: float = 0.50
    min_live_resolved_fills: int = 10
    rolling_rotation_window_fills: int = 20
    rolling_rotation_loss_threshold_usd: float = -8.0
    min_live_fill_rate_pct: float = 40.0
    min_candidate_paper_pnl_usd: float = 0.0
    min_candidate_copyable_buy_events: int = 20
    max_candidates: int = 5
    inactivity_rotation_enabled: bool = True
    live_inactivity_rotation_threshold_s: float = 3 * 60 * 60
    alternate_activity_window_s: float = 3 * 60 * 60
    min_alternate_recent_buy_events: int = 1
    require_leak_rule_1_2_before_rotation_application: bool = True
    live_our_fill_pnl_outranks_paper_for_retention: bool = True
    pause_tripwire_clocks_while_deadman_red_or_unsubmittable: bool = True

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


WEEKEND_AWARE_CLOCK_EFFECTIVE_TS = _ts("2026-07-11T00:00:00+00:00")


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _price_band_window(live_fill_report: dict[str, Any], config: PromotionRotationConfig) -> dict[str, Any]:
    window = live_fill_report.get("price_band_decision_window")
    if isinstance(window, dict) and window.get("enabled") is True:
        return dict(window)

    buckets = live_fill_report.get("buckets") if isinstance(live_fill_report.get("buckets"), dict) else {}
    bucket = buckets.get("01_25_50") if isinstance(buckets.get("01_25_50"), dict) else {}
    if not bucket:
        return {
            "enabled": False,
            "status": "MISSING",
            "max_price": round(float(config.live_max_buy_price), 6),
            "orders": 0,
            "resolved_filled": 0,
            "realized_pnl_usd": 0.0,
            "rotation_triggered": False,
            "blockers": ["live_price_band_decision_window_missing"],
        }

    resolved_filled = int(bucket.get("resolved_filled") or 0)
    realized_pnl = num(bucket.get("realized_pnl_usd"))
    sample_ready = resolved_filled >= int(config.min_live_resolved_fills)
    blockers: list[str] = ["live_price_band_decision_window_missing_using_bucket_fallback"]
    if not sample_ready:
        blockers.append("price_band_resolved_sample_below_threshold")
    if sample_ready and realized_pnl < 0.0:
        blockers.append("fallback_negative_pnl_observed_not_actionable")
    rolling_trigger = {
        "enabled": False,
        "flow_stage": "ROTATE",
        "window_size": int(config.rolling_rotation_window_fills),
        "resolved_filled": resolved_filled,
        "sample_ready": resolved_filled >= int(config.rolling_rotation_window_fills),
        "realized_pnl_usd": round(realized_pnl, 6),
        "loss_threshold_usd": round(float(config.rolling_rotation_loss_threshold_usd), 6),
        "rotation_triggered": False,
    }
    return {
        "enabled": False,
        "status": CORRECTION,
        "evidence_source": "bucket_01_25_50_fallback",
        "max_price": round(float(config.live_max_buy_price), 6),
        "orders": int(bucket.get("orders") or 0),
        "filled": int(bucket.get("filled") or 0),
        "resolved_filled": resolved_filled,
        "min_resolved_filled": int(config.min_live_resolved_fills),
        "realized_pnl_usd": round(realized_pnl, 6),
        "fill_rate_pct": num(bucket.get("fill_rate_pct")),
        "target_fill_rate_pct": round(float(config.min_live_fill_rate_pct), 6),
        "rotation_triggered": False,
        "rolling_rotation_trigger": rolling_trigger,
        "blockers": blockers,
    }


def evaluate_live_rotation(
    live_fill_report: dict[str, Any],
    config: PromotionRotationConfig | None = None,
) -> dict[str, Any]:
    cfg = config or PromotionRotationConfig()
    window = _price_band_window(live_fill_report, cfg)
    resolved_filled = int(window.get("resolved_filled") or 0)
    realized_pnl = num(window.get("realized_pnl_usd"))
    fill_rate = num(window.get("fill_rate_pct"))
    sample_ready = resolved_filled >= int(cfg.min_live_resolved_fills)
    rolling_trigger = window.get("rolling_rotation_trigger") if isinstance(window.get("rolling_rotation_trigger"), dict) else {}
    rolling_triggered = bool(rolling_trigger.get("rotation_triggered"))
    canonical_window_enabled = window.get("enabled") is True
    rotation_triggered = canonical_window_enabled and (
        bool(window.get("rotation_triggered"))
        or (sample_ready and realized_pnl < 0.0)
        or rolling_triggered
    )
    blockers = [str(item) for item in (window.get("blockers") or []) if item]
    if not sample_ready and "price_band_resolved_sample_below_threshold" not in blockers:
        blockers.append("price_band_resolved_sample_below_threshold")
    if rotation_triggered and "price_band_realized_pnl_negative" not in blockers:
        blocker = "rolling_20_resolved_pnl_below_loss_threshold" if rolling_triggered else "price_band_realized_pnl_negative"
        if blocker not in blockers:
            blockers.append(blocker)
    if window.get("enabled") is not True and "live_price_band_decision_window_missing" not in blockers:
        blockers.append("live_price_band_decision_window_missing")

    if not canonical_window_enabled:
        action = "CONTINUE_LIVE_PRICE_BAND_SAMPLE"
        status = CORRECTION
        next_action = "restore the canonical enabled price-band decision window before any rotation decision"
    elif rotation_triggered:
        action = "ROTATE_LIVE_WALLET"
        status = CORRECTION
        next_action = "select a paper-positive live-executable broad-lane candidate and ask Fable to rotate"
    elif not sample_ready:
        action = "CONTINUE_LIVE_PRICE_BAND_SAMPLE"
        status = WATCH
        next_action = "keep the live price-band evidence collection active until enough resolved fills exist"
    elif fill_rate < float(cfg.min_live_fill_rate_pct):
        action = "CONTINUE_LIVE_PRICE_BAND_SAMPLE"
        status = WATCH
        next_action = "keep measuring; realized PnL passed but fill rate is still below target"
        if "price_band_fill_rate_below_target" not in blockers:
            blockers.append("price_band_fill_rate_below_target")
    else:
        action = "KEEP_LIVE_WALLET"
        status = PASS
        next_action = "keep the current live wallet and continue rolling evidence"

    return {
        "flow_stage": "LIVE",
        "status": status,
        "action": action,
        "rotation_triggered": rotation_triggered,
        "sample_ready": sample_ready,
        "max_buy_price": round(float(cfg.live_max_buy_price), 6),
        "resolved_filled": resolved_filled,
        "min_resolved_filled": int(cfg.min_live_resolved_fills),
        "realized_pnl_usd": round(realized_pnl, 6),
        "fill_rate_pct": round(fill_rate, 6),
        "target_fill_rate_pct": round(float(cfg.min_live_fill_rate_pct), 6),
        "decision_window": window,
        "rolling_rotation_trigger": rolling_trigger,
        "blockers": blockers,
        "next_action": next_action,
    }


def _latest_live_order_snapshot(live_execution_state: dict[str, Any]) -> dict[str, Any]:
    orders = live_execution_state.get("orders") if isinstance(live_execution_state.get("orders"), list) else []
    latest: dict[str, Any] = {}
    latest_ts = 0.0
    for order in orders:
        if not isinstance(order, dict):
            continue
        if order.get("paper_only") is True:
            continue
        if order.get("live_orders_allowed") is False:
            continue
        order_ts = _ts(order.get("submitted_at") or order.get("updated_at"))
        if order_ts <= 0:
            lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
            order_ts = max((_ts(row.get("ts")) for row in lifecycle if isinstance(row, dict)), default=0.0)
        if order_ts >= latest_ts:
            latest_ts = order_ts
            latest = order
    return {
        "source_wallet": _norm_wallet(latest.get("source_wallet")) if latest else "",
        "submitted_at": latest.get("submitted_at") if latest else None,
        "order_id": latest.get("order_id") if latest else None,
        "final_status": latest.get("final_status") if latest else None,
        "market_slug": latest.get("market_slug") if latest else None,
        "ts": latest_ts,
    }


def _candidate_recent_activity_events(candidate: dict[str, Any]) -> int:
    return max(
        int(candidate.get("recent_liquid_copy_sized_buy_events") or 0),
        int(candidate.get("recent_copy_sized_buy_events") or 0),
    )


def _profile_for_wallet(dow_profile_state: dict[str, Any] | None, wallet: str) -> dict[str, Any]:
    state = dow_profile_state if isinstance(dow_profile_state, dict) else {}
    by_wallet = state.get("profiles_by_wallet") if isinstance(state.get("profiles_by_wallet"), dict) else {}
    profile = by_wallet.get(wallet)
    if isinstance(profile, dict):
        return profile
    profiles = state.get("profiles") if isinstance(state.get("profiles"), list) else []
    for row in profiles:
        if isinstance(row, dict) and _norm_wallet(row.get("wallet")) == wallet:
            return row
    return {}


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


def _calendar_expected_active_age_s(
    *,
    start_ts: float | None,
    now_ts: float,
    profile: dict[str, Any],
) -> float | None:
    if start_ts is None or start_ts <= 0.0 or now_ts <= start_ts or not profile:
        return None
    cursor = float(start_ts)
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


def _calendar_clock_evidence(
    *,
    wallet: str,
    latest_ts: float | None,
    inactive_age_s: float | None,
    now_ts: float,
    threshold_s: float,
    dow_profile_state: dict[str, Any] | None,
) -> dict[str, Any]:
    profile = _profile_for_wallet(dow_profile_state, wallet)
    expected_age_s = _calendar_expected_active_age_s(start_ts=latest_ts, now_ts=now_ts, profile=profile)
    now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    pre_registered = (
        latest_ts is not None
        and latest_ts > 0.0
        and latest_ts < WEEKEND_AWARE_CLOCK_EFFECTIVE_TS <= now_ts
        and now_dt.weekday() >= 5
    )
    expected_under_threshold = expected_age_s is not None and expected_age_s < float(threshold_s)
    return {
        "flow_stage": "ROTATE/LEARN",
        "enabled": bool(profile),
        "authority": "Fable DIRECTION 2026-07-10T17:40Z/17:55Z weekend-awareness",
        "effective_new_clocks_from": "2026-07-11T00:00:00Z",
        "wallet": wallet,
        "profile_found": bool(profile),
        "weekend_evidence_status": profile.get("weekend_evidence_status") if profile else None,
        "current_is_weekend": now_dt.weekday() >= 5,
        "raw_inactive_age_s": round(float(inactive_age_s), 6) if inactive_age_s is not None else None,
        "expected_active_age_s": expected_age_s,
        "threshold_s": round(float(threshold_s), 6),
        "expected_active_age_below_threshold": expected_under_threshold,
        "pre_registered_clock_requires_fable_rerule": pre_registered,
        "blocks_inactivity_rotation": bool(expected_under_threshold or pre_registered),
        "rule": (
            "new clocks count expected-active hours only; pre-2026-07-11T00:00Z clocks are re-ruled by Fable at expiry"
        ),
    }


def evaluate_inactivity_rotation(
    *,
    live_execution_state: dict[str, Any],
    lane_state: dict[str, Any],
    config: PromotionRotationConfig | None = None,
    now_ts: float | None = None,
    dow_profile_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or PromotionRotationConfig()
    latest = _latest_live_order_snapshot(live_execution_state if isinstance(live_execution_state, dict) else {})
    now_value = _now_ts() if now_ts is None else float(now_ts)
    evaluated_at = datetime.fromtimestamp(now_value, tz=timezone.utc).isoformat()
    inactive_age_s = max(0.0, now_value - float(latest.get("ts") or 0.0)) if latest.get("ts") else None
    candidates = select_promotion_candidates(lane_state if isinstance(lane_state, dict) else {}, cfg)
    active_candidates = [
        candidate
        for candidate in candidates
        if _candidate_recent_activity_events(candidate) >= int(cfg.min_alternate_recent_buy_events)
    ]
    blockers: list[str] = []
    enabled = bool(cfg.inactivity_rotation_enabled)
    if not enabled:
        blockers.append("inactivity_rotation_disabled")
    if inactive_age_s is None:
        blockers.append("live_latest_copy_order_missing")
    elif inactive_age_s < float(cfg.live_inactivity_rotation_threshold_s):
        blockers.append("live_wallet_inactivity_below_threshold")
    if not active_candidates:
        if candidates:
            blockers.append("no_promotable_candidate_active_in_activity_window")
        else:
            blockers.append("no_promotable_candidate_for_inactivity_rotation")
    calendar_clock = _calendar_clock_evidence(
        wallet=str(latest.get("source_wallet") or ""),
        latest_ts=float(latest.get("ts") or 0.0) if latest.get("ts") else None,
        inactive_age_s=inactive_age_s,
        now_ts=now_value,
        threshold_s=float(cfg.live_inactivity_rotation_threshold_s),
        dow_profile_state=dow_profile_state,
    )
    if calendar_clock.get("blocks_inactivity_rotation") is True:
        if calendar_clock.get("pre_registered_clock_requires_fable_rerule") is True:
            blockers.append("pre_registered_inactivity_clock_requires_fable_weekend_rerule")
        elif calendar_clock.get("expected_active_age_below_threshold") is True:
            blockers.append("calendar_expected_active_inactivity_below_threshold")
    rotation_triggered = enabled and inactive_age_s is not None and inactive_age_s >= float(
        cfg.live_inactivity_rotation_threshold_s
    ) and bool(active_candidates) and not bool(calendar_clock.get("blocks_inactivity_rotation"))
    return {
        "flow_stage": "ROTATE",
        "status": CORRECTION if rotation_triggered else WATCH,
        "enabled": enabled,
        "evaluated_at": evaluated_at,
        "rotation_triggered": rotation_triggered,
        "live_source_wallet": latest.get("source_wallet"),
        "latest_live_order": latest,
        "inactive_age_s": round(inactive_age_s, 6) if inactive_age_s is not None else None,
        "threshold_s": round(float(cfg.live_inactivity_rotation_threshold_s), 6),
        "calendar_clock": calendar_clock,
        "alternate_activity_window_s": round(float(cfg.alternate_activity_window_s), 6),
        "promotable_candidate_count": len(candidates),
        "active_promotable_candidate_count": len(active_candidates),
        "best_active_candidate": active_candidates[0] if active_candidates else {},
        "best_active_candidate_wallet": active_candidates[0].get("wallet") if active_candidates else None,
        "considered_active_candidate_wallets": [
            str(candidate.get("wallet") or "") for candidate in active_candidates if str(candidate.get("wallet") or "")
        ][:10],
        "blockers": blockers,
        "next_action": (
            "rotate to the best active profile-positive alternate"
            if rotation_triggered
            else "keep waiting unless the live wallet exceeds inactivity threshold while a promotable alternate is active"
        ),
    }


def select_promotion_candidates(
    lane_state: dict[str, Any],
    config: PromotionRotationConfig | None = None,
) -> list[dict[str, Any]]:
    cfg = config or PromotionRotationConfig()
    ranked = lane_state.get("ranked_wallets") if isinstance(lane_state.get("ranked_wallets"), list) else []
    candidates: list[dict[str, Any]] = []
    for row in ranked:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("address"))
        if not wallet or row.get("live_executable_paper_eligible") is not True:
            continue
        if row.get("copyability_profile_gate_enabled") is True and row.get("copyability_profile_eligible") is not True:
            continue
        gate = row.get("paper_policy_gate") if isinstance(row.get("paper_policy_gate"), dict) else {}
        execution_profile = row.get("execution_profile") if isinstance(row.get("execution_profile"), dict) else {}
        copyability_profile = row.get("copyability_profile") if isinstance(row.get("copyability_profile"), dict) else {}
        policy_ids = [str(item) for item in (row.get("paper_eligible_policy_ids") or gate.get("eligible_policy_ids") or [])]
        best_pnl = num(gate.get("best_policy_paper_pnl_usd"), num(row.get("paper_pnl_usd")))
        best_copyable = int(gate.get("best_policy_copyable_buy_events") or row.get("copyable_buy_events") or 0)
        clearance_ready_duplicate_floor_bypass = bool(
            gate.get("clearance_ready_duplicate_copyable_floor_bypass")
        )
        recent_ask_depth = num(row.get("max_recent_ask_depth_usd"))
        fill_sample = _int(
            execution_profile.get("fill_sample")
            or copyability_profile.get("fill_sample")
            or row.get("copyability_profile_fill_sample")
            or row.get("fill_sample")
        )
        if best_pnl <= float(cfg.min_candidate_paper_pnl_usd):
            continue
        if best_copyable < int(cfg.min_candidate_copyable_buy_events) and not clearance_ready_duplicate_floor_bypass:
            continue
        if recent_ask_depth <= 0.0:
            continue
        if fill_sample <= 0:
            continue
        if not policy_ids:
            continue
        candidates.append(
            {
                "rank": int(row.get("rank") or len(candidates) + 1),
                "wallet": wallet,
                "wallet_name": row.get("wallet_name") or row.get("user_name") or "",
                "status": PASS,
                "ready_for_live": bool(
                    row.get("ready_for_live") is True
                    or gate.get("current_queue_ready_for_live") is True
                ),
                "paper_eligible_policy_ids": policy_ids,
                "best_policy_id": str(gate.get("best_policy_id") or policy_ids[0]),
                "best_policy_paper_pnl_usd": round(best_pnl, 6),
                "best_policy_copyable_rate_pct": gate.get("best_policy_copyable_rate_pct"),
                "best_policy_copyable_buy_events": best_copyable,
                "recent_copy_sized_buy_events": int(row.get("recent_copy_sized_buy_events") or 0),
                "recent_liquid_copy_sized_buy_events": int(row.get("recent_liquid_copy_sized_buy_events") or 0),
                "max_recent_ask_depth_usd": round(recent_ask_depth, 6),
                "copyability_profile": {
                    "enabled": bool(row.get("copyability_profile_gate_enabled")),
                    "eligible": row.get("copyability_profile_eligible"),
                    "latency_horizon_s": execution_profile.get("latency_horizon_s"),
                    "fill_sample": fill_sample,
                    "copyable_rate_pct": execution_profile.get("copyable_rate_pct"),
                    "mean_edge": execution_profile.get("mean_edge"),
                    "median_edge": execution_profile.get("median_edge"),
                },
                "candidate_evidence_bar": {
                    "status": PASS,
                    "min_candidate_copyable_buy_events": int(cfg.min_candidate_copyable_buy_events),
                    "copyable_buy_events": best_copyable,
                    "max_recent_ask_depth_usd": round(recent_ask_depth, 6),
                    "copyability_profile_fill_sample": fill_sample,
                    "clearance_ready_duplicate_copyable_floor_bypass": clearance_ready_duplicate_floor_bypass,
                },
                "evidence_source": row.get("evidence_source") or "",
            }
        )
    return candidates[: max(1, int(cfg.max_candidates))]


def evaluate_paper_promotion(
    lane_state: dict[str, Any],
    config: PromotionRotationConfig | None = None,
) -> dict[str, Any]:
    cfg = config or PromotionRotationConfig()
    candidates = select_promotion_candidates(lane_state, cfg)
    lane_blockers = [str(item) for item in (lane_state.get("blockers") or []) if item]
    blockers: list[str] = []
    if not candidates:
        blockers.append("no_paper_positive_live_executable_candidate")
    if not isinstance(lane_state.get("ranked_wallets"), list):
        blockers.append("paper_lane_ranked_wallets_missing")
    status = PASS if candidates else ANALYZE
    return {
        "flow_stage": "PROMOTE",
        "status": status,
        "candidate_count": len(candidates),
        "best_candidate": candidates[0] if candidates else {},
        "candidates": candidates,
        "lane_status": lane_state.get("status"),
        "lane_blockers": lane_blockers,
        "blockers": blockers,
        "next_action": (
            "ask Fable to promote or rotate to the best paper-positive live-executable candidate"
            if candidates
            else "measure the selected broad-lane wallets until at least one has positive paper PnL and copyable BUYs"
        ),
    }


def _leak_rule_1_2_ready(*payloads: dict[str, Any]) -> bool:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        candidates = [
            payload.get("leak_rule_1_2"),
            payload.get("leak_rules_1_2"),
            payload.get("leak_rule_status"),
            payload.get("rotation_protection_rules"),
        ]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            leak_1 = bool(
                item.get("leak_1_live_our_fill_pnl_outranks_paper")
                or item.get("live_our_fill_pnl_outranks_paper")
                or item.get("live_our_fill_retention_rule")
            )
            leak_2 = bool(
                item.get("leak_2_tripwire_clocks_pause_while_deadman_red")
                or item.get("tripwire_clocks_pause_while_deadman_red")
                or item.get("submittable_windows_only_tripwire_rule")
            )
            if leak_1 and leak_2:
                return True
    return False


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def _state_bool(payload: dict[str, Any], key: str) -> bool | None:
    for source in (
        payload if isinstance(payload, dict) else {},
        payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
    ):
        value = _optional_bool(source.get(key))
        if value is not None:
            return value
    return None


def _rolling_basis_result(rolling: dict[str, Any], basis: str, cfg: PromotionRotationConfig) -> dict[str, Any]:
    bases = rolling.get("bases") if isinstance(rolling.get("bases"), dict) else {}
    source = bases.get(basis) if isinstance(bases.get(basis), dict) else {}
    inherited = not bool(source)
    if inherited and basis == "canonical":
        source = rolling

    window_size = int(source.get("window_size") or rolling.get("window_size") or cfg.rolling_rotation_window_fills)
    resolved_filled = int(source.get("resolved_filled") or 0)
    sample_orders = source.get("sample_orders") if isinstance(source.get("sample_orders"), list) else []
    enabled = bool(source.get("enabled")) if "enabled" in source else bool(rolling.get("enabled"))
    sample_ready_flag = bool(source.get("sample_ready"))
    sample_has_orders = bool(sample_orders)
    sample_ready = (
        enabled
        and sample_ready_flag
        and resolved_filled >= window_size
        and resolved_filled <= window_size
        and sample_has_orders
    )
    pnl = num(source.get("realized_pnl_usd"))
    blockers: list[str] = []
    if not enabled:
        blockers.append(f"{basis}_rolling_disabled_or_missing")
    if not sample_ready_flag or resolved_filled < window_size:
        blockers.append(f"{basis}_rolling_sample_below_window")
    if resolved_filled > window_size:
        blockers.append(f"{basis}_rolling_sample_exceeds_window_size")
    if not sample_has_orders:
        blockers.append(f"{basis}_rolling_sample_orders_missing")
    if sample_ready and pnl <= 0.0:
        blockers.append(f"{basis}_rolling_our_fill_pnl_not_positive")
    return {
        "basis": basis,
        "enabled": enabled,
        "sample_source": (
            f"price_band_decision_window.rolling_rotation_trigger.bases.{basis}"
            if not inherited
            else "price_band_decision_window.rolling_rotation_trigger"
        ),
        "window_size": window_size,
        "resolved_filled": resolved_filled,
        "sample_ready": sample_ready,
        "sample_ready_flag": sample_ready_flag,
        "sample_order_count": len(sample_orders),
        "realized_pnl_usd": round(pnl, 6),
        "blockers": blockers,
    }


def _live_our_fill_retention_rule(live: dict[str, Any], cfg: PromotionRotationConfig) -> dict[str, Any]:
    enabled = bool(cfg.live_our_fill_pnl_outranks_paper_for_retention)
    rolling = live.get("rolling_rotation_trigger") if isinstance(live.get("rolling_rotation_trigger"), dict) else {}
    canonical = _rolling_basis_result(rolling, "canonical", cfg)
    reconciled = _rolling_basis_result(rolling, "reconciled", cfg)
    canonical_positive = bool(canonical.get("sample_ready")) and num(canonical.get("realized_pnl_usd")) > 0.0
    reconciled_positive = bool(reconciled.get("sample_ready")) and num(reconciled.get("realized_pnl_usd")) > 0.0
    protects = enabled and canonical_positive and reconciled_positive
    blockers: list[str] = []
    if not enabled:
        blockers.append("live_our_fill_retention_rule_disabled")
    if not canonical_positive:
        blockers.extend(str(item) for item in canonical.get("blockers", []) if item)
        if bool(canonical.get("sample_ready")) and num(canonical.get("realized_pnl_usd")) <= 0.0:
            blockers.append("canonical_positive_rolling_our_fill_pnl_absent")
    if not reconciled_positive:
        blockers.extend(str(item) for item in reconciled.get("blockers", []) if item)
        if bool(reconciled.get("sample_ready")) and num(reconciled.get("realized_pnl_usd")) <= 0.0:
            blockers.append("reconciled_positive_rolling_our_fill_pnl_absent")
    blockers = list(dict.fromkeys(blockers))
    return {
        "flow_stage": "ROTATE",
        "rule": "LEAK_1",
        "enabled": enabled,
        "sample_source": "price_band_decision_window.rolling_rotation_trigger.bases",
        "sample_ready": bool(canonical.get("sample_ready") and reconciled.get("sample_ready")),
        "resolved_filled": canonical.get("resolved_filled"),
        "required_resolved_filled": int(cfg.rolling_rotation_window_fills),
        "realized_pnl_usd": canonical.get("realized_pnl_usd"),
        "reconciled_realized_pnl_usd": reconciled.get("realized_pnl_usd"),
        "basis_results": {
            "canonical": canonical,
            "reconciled": reconciled,
        },
        "protected_from_non_pnl_demotion": protects,
        "suppresses_non_pnl_rotation": protects,
        "blockers": blockers,
    }


def _tripwire_clock_pause_rule(
    *,
    live_execution_state: dict[str, Any],
    order_flow_deadman_state: dict[str, Any] | None,
    cfg: PromotionRotationConfig,
) -> dict[str, Any]:
    enabled = bool(cfg.pause_tripwire_clocks_while_deadman_red_or_unsubmittable)
    deadman = order_flow_deadman_state if isinstance(order_flow_deadman_state, dict) else {}
    reasons: list[str] = []
    can_trade = _state_bool(live_execution_state if isinstance(live_execution_state, dict) else {}, "can_trade")
    live_allowed = _state_bool(
        live_execution_state if isinstance(live_execution_state, dict) else {},
        "live_orders_allowed",
    )
    deadman_can_trade = _optional_bool(deadman.get("can_trade"))
    deadman_status = str(deadman.get("status") or "")
    if enabled:
        if can_trade is False:
            reasons.append("guard_cannot_submit")
        if live_allowed is False:
            reasons.append("live_orders_not_allowed")
        if deadman_status == "INCIDENT_ORDER_FLOW_DEAD":
            reasons.append("deadman_red")
        if deadman_can_trade is False:
            reasons.append("deadman_cannot_trade")
    return {
        "flow_stage": "ROTATE",
        "rule": "LEAK_2",
        "enabled": enabled,
        "active": bool(enabled and reasons),
        "only_submittable_windows_count": enabled,
        "can_trade": can_trade,
        "live_orders_allowed": live_allowed,
        "deadman_status": deadman_status,
        "deadman_can_trade": deadman_can_trade,
        "pause_reasons": reasons,
    }


def _apply_inactivity_rotation_protections(
    inactivity: dict[str, Any],
    *,
    live_retention_rule: dict[str, Any],
    tripwire_clock_rule: dict[str, Any],
) -> dict[str, Any]:
    protected = dict(inactivity)
    protected["leak_rule_1_live_retention"] = live_retention_rule
    protected["leak_rule_2_tripwire_clock"] = tripwire_clock_rule
    if not bool(protected.get("rotation_triggered")):
        return protected

    reasons: list[str] = []
    if live_retention_rule.get("suppresses_non_pnl_rotation") is True:
        reasons.append("live_positive_rolling_our_fill_pnl_retention_protected")
    if tripwire_clock_rule.get("active") is True:
        reasons.append("tripwire_clock_paused_until_submittable_window")
    if not reasons:
        return protected

    blockers = [str(item) for item in (protected.get("blockers") or []) if item]
    for reason in reasons:
        if reason not in blockers:
            blockers.append(reason)
    protected.update(
        {
            "status": WATCH,
            "rotation_triggered": False,
            "rotation_suppressed": {
                "flow_stage": "ROTATE",
                "original_rotation_triggered": True,
                "reasons": reasons,
            },
            "blockers": blockers,
            "next_action": (
                "keep current live wallet; non-PnL inactivity demotion is suppressed while live "
                "our-fill PnL is retention-positive or tripwire time is non-submittable"
            ),
        }
    )
    return protected


def build_promotion_rotation_state(
    *,
    live_fill_report: dict[str, Any],
    lane_state: dict[str, Any],
    live_execution_state: dict[str, Any] | None = None,
    order_flow_deadman_state: dict[str, Any] | None = None,
    dow_profile_state: dict[str, Any] | None = None,
    config: PromotionRotationConfig | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    cfg = config or PromotionRotationConfig()
    live = evaluate_live_rotation(live_fill_report, cfg)
    raw_inactivity = evaluate_inactivity_rotation(
        live_execution_state=live_execution_state or {},
        lane_state=lane_state,
        config=cfg,
        now_ts=now_ts,
        dow_profile_state=dow_profile_state,
    )
    live_retention_rule = _live_our_fill_retention_rule(live, cfg)
    tripwire_clock_rule = _tripwire_clock_pause_rule(
        live_execution_state=live_execution_state or {},
        order_flow_deadman_state=order_flow_deadman_state,
        cfg=cfg,
    )
    inactivity = _apply_inactivity_rotation_protections(
        raw_inactivity,
        live_retention_rule=live_retention_rule,
        tripwire_clock_rule=tripwire_clock_rule,
    )
    paper = evaluate_paper_promotion(lane_state, cfg)
    price_rotation_triggered = bool(live.get("rotation_triggered"))
    inactivity_rotation_triggered = bool(inactivity.get("rotation_triggered"))
    rotation_triggered = price_rotation_triggered or inactivity_rotation_triggered
    leak_rules_ready = _leak_rule_1_2_ready(live_fill_report, lane_state, live_execution_state or {}) or (
        bool(live_retention_rule.get("enabled")) and bool(tripwire_clock_rule.get("enabled"))
    )
    rotation_application_allowed = not (
        bool(cfg.require_leak_rule_1_2_before_rotation_application)
        and rotation_triggered
        and not leak_rules_ready
    )
    inactivity_candidate = inactivity.get("best_active_candidate") if isinstance(inactivity.get("best_active_candidate"), dict) else {}
    paper_candidate = paper.get("best_candidate") if isinstance(paper.get("best_candidate"), dict) else {}
    best_candidate = inactivity_candidate if inactivity_rotation_triggered else paper_candidate
    has_candidate = bool(best_candidate) or bool(paper.get("candidate_count"))
    destination_ready = bool(best_candidate.get("ready_for_live")) if best_candidate else False
    blockers: list[str] = []
    requires_fable = False
    if rotation_triggered and has_candidate and destination_ready:
        status = PASS
        action = "FABLE_ROTATION_DECISION_READY"
        requires_fable = True
        next_action = "ask Fable to rotate the live guard target to the best active candidate"
    elif rotation_triggered:
        status = CORRECTION
        action = "RETAIN_NO_ELIGIBLE_CANDIDATE"
        blockers.append(
            "rotation_destination_not_ready_in_current_queue"
            if has_candidate
            else "rotation_triggered_without_promotable_candidate"
        )
        next_action = "retain the current live lane while measuring candidates that satisfy copyable-event, depth, and fill-sample evidence bars"
    elif has_candidate and live.get("status") == PASS:
        status = PASS
        action = "PROMOTION_CANDIDATE_READY_LIVE_CAN_STAY"
        requires_fable = True
        next_action = "ask Fable whether the candidate should replace or shadow the current live wallet"
    else:
        status = WATCH
        action = "CONTINUE_BUILD_MEASUREMENT"
        next_action = "continue live price-band evidence and broad-lane paper measurement"

    return {
        "schema_version": 1,
        "kind": "wallet_copy_promotion_rotation_state",
        "flow_stages": ["LIVE", "OBSERVE", "PROMOTE", "ROTATE"],
        "generated_at": utc_now_iso(),
        "status": status,
        "decision": {
            "status": status,
            "action": action,
            "requires_fable_decision": requires_fable,
            "rotation_triggered": rotation_triggered,
            "price_rotation_triggered": price_rotation_triggered,
            "inactivity_rotation_triggered": inactivity_rotation_triggered,
            "rotation_application_allowed": rotation_application_allowed,
            "rotation_application_interlock": {
                "enabled": bool(cfg.require_leak_rule_1_2_before_rotation_application),
                "active": not rotation_application_allowed,
                "reason": (
                    "leak_rules_1_2_absent"
                    if not rotation_application_allowed
                    else "leak_rules_1_2_ready_or_no_rotation"
                ),
                "required_rules": [
                    "live_our_fill_pnl_outranks_paper_for_retention",
                    "tripwire_clocks_pause_while_deadman_red_or_guard_cannot_submit",
                ],
            },
            "paper_candidate_available": has_candidate,
            "destination_ready_for_live": destination_ready,
            "best_candidate_wallet": best_candidate.get("wallet") if best_candidate else None,
            "blockers": blockers,
            "next_action": next_action,
        },
        "config": cfg.asdict(),
        "leak_rules_1_2": {
            "flow_stage": "ROTATE",
            "ready": leak_rules_ready,
            "live_our_fill_pnl_outranks_paper": bool(live_retention_rule.get("enabled")),
            "tripwire_clocks_pause_while_deadman_red": bool(tripwire_clock_rule.get("enabled")),
            "leak_1_live_retention": live_retention_rule,
            "leak_2_tripwire_clock": tripwire_clock_rule,
        },
        "live": live,
        "inactivity_rotation": inactivity,
        "paper_promotion": paper,
    }
