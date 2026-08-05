#!/usr/bin/env python3
"""Build a precomputed active-set member queue from full-pool evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from scripts.update_state_digest import (  # noqa: E402
    _external_liveness_passes_for_digest,
    _external_liveness_rows_by_wallet_from_probe,
    _parse_utc_ts,
)


DEFAULT_SHORTLIST = "data/research/active_set_expansion_full_pool_shortlist.json"
DEFAULT_REPLAY = "data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json"
DEFAULT_ROTATION = "data/research/wallet_copy_promotion_rotation_full_pool_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_full_pool_member_queue.json"
DEFAULT_LIVE_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_ACTIVE_SET_OVERLAY = "data/research/wallet_copy_active_set_auto_degrade_state.json"
DEFAULT_CLEARANCE_GAPS = "data/research/wallet_copy_queue_clearance_gaps.json"
DEFAULT_CLEARANCE_PACKETS = "data/research/ranked_queue_clearance_packets_latest.json"
DEFAULT_FRESH_FLOW_PROBE = "data/research/wallet_copy_corrected_copyability_probe_latest.json"
DEFAULT_EXTERNAL_LIVENESS_PROBE = "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json"
DEFAULT_ACTIVE_SET_POLLER_STATE = "data/research/wallet_copy_active_set_dataapi_poller_state.json"
DEFAULT_DATAAPI_FIRST_SEEN = "data/research/dataapi_first_seen.jsonl"
DEFAULT_BREADTH_DISPOSITIONS = "data/research/wallet_copy_breadth_dispositions.json"
DEFAULT_MARKET_COHORT_REPLAY = "data/research/wallet_market_cohort_replay_latest.json"
DEFAULT_COPYABILITY = "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_COHORT_ADMISSION = "data/research/cohort_alive_admission_packets_latest.json"
DEFAULT_REGISTRY_LIVENESS_PROBE = "data/research/registry_weekday_f1_remote_liveness_probe_latest.json"
DEFAULT_OBSERVATION_ADMISSIONS = "configs/wallet_copy/registry_observation_admissions.json"
PROMOTION_MIN_COPYABLE_BUY_EVENTS = 20
CANONICAL_QUEUE_LIMIT = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shortlist", default=DEFAULT_SHORTLIST)
    parser.add_argument("--replay", default=DEFAULT_REPLAY)
    parser.add_argument("--rotation-state", default=DEFAULT_ROTATION)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--live-guard-state", default=DEFAULT_LIVE_GUARD_STATE)
    parser.add_argument("--active-set-overlay", default=DEFAULT_ACTIVE_SET_OVERLAY)
    parser.add_argument("--clearance-gaps", default=DEFAULT_CLEARANCE_GAPS)
    parser.add_argument("--clearance-packets", default=DEFAULT_CLEARANCE_PACKETS)
    parser.add_argument("--fresh-flow-probe", default=DEFAULT_FRESH_FLOW_PROBE)
    parser.add_argument("--external-liveness-probe", default=DEFAULT_EXTERNAL_LIVENESS_PROBE)
    parser.add_argument("--active-set-dataapi-poller-state", default=DEFAULT_ACTIVE_SET_POLLER_STATE)
    parser.add_argument("--dataapi-first-seen-jsonl", default=DEFAULT_DATAAPI_FIRST_SEEN)
    parser.add_argument("--breadth-dispositions", default=DEFAULT_BREADTH_DISPOSITIONS)
    parser.add_argument("--market-cohort-replay", default=DEFAULT_MARKET_COHORT_REPLAY)
    parser.add_argument("--copyability", default=DEFAULT_COPYABILITY)
    parser.add_argument("--cohort-admission", default=DEFAULT_COHORT_ADMISSION)
    parser.add_argument("--registry-liveness-probe", default=DEFAULT_REGISTRY_LIVENESS_PROBE)
    parser.add_argument("--observation-admissions", default=DEFAULT_OBSERVATION_ADMISSIONS)
    parser.add_argument(
        "--limit",
        type=int,
        default=CANONICAL_QUEUE_LIMIT,
        help="Maximum ranked members to write; <=0 writes the full staged queue.",
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _parse_iso_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _breadth_dispositions_by_wallet(payload: dict[str, Any], *, now_ts: float) -> dict[str, dict[str, Any]]:
    rows = payload.get("dispositions") if isinstance(payload.get("dispositions"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        status = str(row.get("status") or "").upper()
        if not wallet or not status:
            continue
        active_after = _parse_iso_ts(row.get("active_after"))
        expires_at = _parse_iso_ts(row.get("expires_at"))
        if active_after is not None and now_ts < active_after:
            continue
        if expires_at is not None and now_ts >= expires_at:
            continue
        out[wallet] = dict(row)
    return out


def _apply_breadth_dispositions(
    rows: list[dict[str, Any]],
    dispositions: dict[str, dict[str, Any]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    non_live_statuses = {
        "DENIED_READMISSION_TODAY",
        "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE",
        "DEFERRED_POST_ADDRESS_FORM_MAP_REPROBE",
        "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
    }
    next_actions = {
        "DENIED_READMISSION_TODAY": "do not readmit today; only a fresh temporal registry re-measure can reopen this wallet",
        "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE": "do not readmit; re-open only after user= fresh-flow shows >=3 BTC-5m buys within 24h and latest age <=6h",
        "DEFERRED_POST_ADDRESS_FORM_MAP_REPROBE": "defer until address-form map and corrected fresh-flow re-probe complete",
        "MEASUREMENT_ONLY_TEMPORAL_CLOSURE": "run paper/shadow temporal measurement; re-decide at n>=10 resolved simulated copies or the scheduled checkpoint",
    }
    for row in rows:
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        disposition = dispositions.get(wallet)
        if not disposition:
            continue
        status = str(disposition.get("status") or "").upper()
        counts[status] = counts.get(status, 0) + 1
        row["breadth_disposition"] = {
            key: value
            for key, value in disposition.items()
            if key not in {"wallet", "source_wallet"}
        }
        if status in non_live_statuses:
            row["ready_for_live_before_breadth_disposition"] = bool(row.get("ready_for_live"))
            row["ready_for_live"] = False
            row["next_action"] = next_actions.get(status, row.get("next_action") or "")
    return counts


def _replay_by_wallet(replay_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in replay_payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        replay = row.get("paper_replay") if isinstance(row.get("paper_replay"), dict) else {}
        current = out.get(wallet)
        if current is None or num(replay.get("paper_pnl_usd"), -1_000_000.0) > num(
            (current.get("paper_replay") or {}).get("paper_pnl_usd"),
            -1_000_000.0,
        ):
            out[wallet] = row
    return out


def _best_band(row: dict[str, Any]) -> dict[str, Any]:
    profile = row.get("eligible_profile") if isinstance(row.get("eligible_profile"), dict) else {}
    band = profile.get("best_eligible_move_slice") if isinstance(profile.get("best_eligible_move_slice"), dict) else {}
    return {
        "move_slice_key": band.get("move_slice_key") or "",
        "seconds_bucket": band.get("seconds_bucket") or "",
        "entry_price_band": band.get("entry_price_band") or "",
        "latency_horizon_s": band.get("latency_horizon_s"),
        "copyable_rate_pct": band.get("copyable_rate_pct"),
        "fill_sample": band.get("fill_sample"),
        "mean_edge": band.get("mean_edge"),
        "median_edge": band.get("median_edge"),
        "status": band.get("status") or profile.get("status") or "",
    }


def _complementary_hours_score(row: dict[str, Any]) -> int:
    hours = row.get("complementary_hours_utc") or row.get("complementary_hours")
    if isinstance(hours, list):
        return len({str(item) for item in hours})
    return int(row.get("complementary_hours_score") or row.get("off_peak_hours_score") or 0)


def _clearance_by_wallet(clearance_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in clearance_payload.get("candidates") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        classification = str(row.get("classification") or row.get("clearance_status") or "").upper()
        metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
        coverage = row.get("window_coverage") if isinstance(row.get("window_coverage"), dict) else {}
        if classification != "CLEAR":
            continue
        if num(metrics.get("paper_pnl_usd"), 0.0) <= 0.0:
            continue
        if int(metrics.get("copyable_buy_events") or 0) <= 0:
            continue
        if int(metrics.get("candidate_clob_backed_orders") or metrics.get("filled_orders") or 0) <= 0:
            continue
        if int(metrics.get("resolved_orders") or 0) <= 0:
            continue
        if int(metrics.get("unresolved_filled_order_count") or 0) != 0:
            continue
        if int(coverage.get("missing_resolution_market_count") or 0) != 0:
            continue
        out[wallet] = row
    return out


def _packet_cleared_wallets(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    packets = payload.get("packets") if isinstance(payload.get("packets"), list) else [payload]
    out: dict[str, dict[str, Any]] = {}
    for packet in packets:
        if not isinstance(packet, dict):
            continue
        wallet = _norm_wallet(packet.get("wallet"))
        clearance = packet.get("clearance") if isinstance(packet.get("clearance"), dict) else {}
        post_fee = (
            packet.get("exact_policy_post_fee_shadow")
            if isinstance(packet.get("exact_policy_post_fee_shadow"), dict)
            else {}
        )
        if not wallet or packet.get("paper_only") is not True or packet.get("live_orders_allowed") is not False:
            continue
        if clearance.get("paper_disposition") != "ACCRUE_EXACT_POLICY_SHADOW":
            continue
        if clearance.get("failed_gates"):
            continue
        if post_fee.get("gate_pass") is not True:
            continue
        out[wallet] = packet
    return out


def _iter_jsonl(path: str) -> list[dict[str, Any]]:
    target = ROOT / path if not Path(path).is_absolute() else Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _empty_fresh_flow_evidence() -> dict[str, Any]:
    return {
        "fresh_flow": False,
        "p1_promotion_eligible": False,
        "btc5m_buys": 0,
        "btc5m_trades": 0,
        "local_feed_rows": 0,
        "dataapi_first_seen_rows_24h": 0,
        "dataapi_first_seen_rows_per_hour_24h": 0.0,
        "latest_dataapi_first_seen_age_s": None,
        "remote_dataapi_btc5m_trades_24h": 0,
        "remote_dataapi_btc5m_buys_24h": 0,
        "remote_dataapi_policy_compatible_inband_buy_rows_24h": 0,
        "btc5m_buy_rows_24h_by_price_subband": {},
        "remote_dataapi_btc5m_buy_rows_24h_by_price_subband": {},
        "remote_dataapi_latest_trade_age_h": None,
        "remote_dataapi_source_reported_latest_trade_age_h": None,
        "policy_compatible_fresh_buy_rows_le_30s": 0,
        "policy_compatible_recent_rows_per_hour_extrapolated": 0.0,
        "freshest_policy_compatible_buy_lag_s": None,
        "fresh_poll_only_rows_le_30s": 0,
        "fresh_flow_rank_source": "no_fresh_flow_evidence",
    }


def _merge_fresh_flow_evidence(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = {**_empty_fresh_flow_evidence(), **base}
    for key, value in update.items():
        if key in {
            "btc5m_buys",
            "btc5m_trades",
            "local_feed_rows",
            "dataapi_first_seen_rows_24h",
            "remote_dataapi_btc5m_trades_24h",
            "remote_dataapi_btc5m_buys_24h",
            "remote_dataapi_policy_compatible_inband_buy_rows_24h",
            "policy_compatible_fresh_buy_rows_le_30s",
            "fresh_poll_only_rows_le_30s",
        }:
            merged[key] = max(int(merged.get(key) or 0), int(value or 0))
        elif key in {
            "dataapi_first_seen_rows_per_hour_24h",
            "policy_compatible_recent_rows_per_hour_extrapolated",
        }:
            merged[key] = max(num(merged.get(key), 0.0), num(value, 0.0))
        elif key in {
            "latest_dataapi_first_seen_age_s",
            "remote_dataapi_latest_trade_age_h",
            "remote_dataapi_source_reported_latest_trade_age_h",
            "freshest_policy_compatible_buy_lag_s",
            "latest_trade_age_h",
            "source_reported_latest_trade_age_h",
        }:
            if value is None:
                continue
            prior = merged.get(key)
            merged[key] = num(value, 1_000_000.0) if prior is None else min(num(prior, 1_000_000.0), num(value, 1_000_000.0))
        elif key in {"fresh_flow", "p1_promotion_eligible"}:
            merged[key] = bool(merged.get(key) or value)
        elif key == "p1_reject_reasons":
            merged[key] = sorted({str(item) for item in (merged.get(key) or []) + (value or []) if str(item or "")})
        elif value not in (None, "", []):
            merged[key] = value
    if merged.get("source") == "remote_dataapi_24h":
        if int(merged.get("remote_dataapi_btc5m_trades_24h") or 0) <= 0:
            merged["remote_dataapi_btc5m_trades_24h"] = int(merged.get("btc5m_trades") or 0)
        if int(merged.get("remote_dataapi_btc5m_buys_24h") or 0) <= 0:
            merged["remote_dataapi_btc5m_buys_24h"] = int(merged.get("btc5m_buys") or 0)
        if int(merged.get("remote_dataapi_policy_compatible_inband_buy_rows_24h") or 0) <= 0:
            merged["remote_dataapi_policy_compatible_inband_buy_rows_24h"] = int(
                merged.get("policy_compatible_inband_buy_rows_24h") or 0
            )
    if int(merged.get("policy_compatible_fresh_buy_rows_le_30s") or 0) > 0:
        merged["fresh_flow"] = True
        merged["fresh_flow_rank_source"] = "policy_feedback_le_30s"
    elif int(merged.get("remote_dataapi_btc5m_buys_24h") or 0) > 0:
        merged["fresh_flow"] = True
        merged["fresh_flow_rank_source"] = "remote_dataapi_24h"
    elif int(merged.get("dataapi_first_seen_rows_24h") or 0) > 0 and not merged.get("fresh_flow"):
        merged["fresh_flow"] = True
        merged["fresh_flow_rank_source"] = "dataapi_first_seen_24h"
    return merged


def _fresh_flow_source_keys() -> tuple[str, ...]:
    return (
        "remote_dataapi_24h",
        "rows",
        "paper_shadow_enrollments",
        "ranked_candidates",
        "fresh_local_feed_outside_queue",
    )


def _latest_trade_age_h_from_ts(latest_ts: Any, *, now_ts: float) -> float | None:
    ts = num(latest_ts, 0.0)
    if ts <= 0.0:
        return None
    return round(max(0.0, float(now_ts) - ts) / 3600.0, 6)


def _remote_dataapi_passes_admission_threshold(*, btc5m_buys: int, latest_age_h: Any) -> bool:
    age = num(latest_age_h, 1_000_000.0)
    return int(btc5m_buys) >= 3 and age <= 6.0


def _fresh_flow_by_wallet(
    probe_payload: dict[str, Any],
    *,
    active_set_poller: dict[str, Any] | None = None,
    dataapi_first_seen_rows: list[dict[str, Any]] | None = None,
    now_ts: float | None = None,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    now_ts = float(now_ts or time.time())
    for source_key in _fresh_flow_source_keys():
        source_payload = probe_payload.get(source_key) if isinstance(probe_payload, dict) else []
        if isinstance(source_payload, dict):
            rows = source_payload.get("rows") if isinstance(source_payload.get("rows"), list) else []
            if not rows and isinstance(source_payload.get("rows_by_wallet"), dict):
                rows = list(source_payload["rows_by_wallet"].values())
        else:
            rows = source_payload
        for index, row in enumerate(rows if isinstance(rows, list) else [], start=1):
            if not isinstance(row, dict):
                continue
            wallet = _norm_wallet(row.get("wallet"))
            if not wallet:
                continue
            effective_source_key = (
                "remote_dataapi_24h"
                if source_key == "rows"
                and str(probe_payload.get("kind") or "") == "queue_remote_dataapi_fresh_flow_probe"
                else source_key
            )
            btc5m_buys = int(row.get("btc5m_buys") or row.get("btc5m_buys_24h") or 0)
            btc5m_trades = int(row.get("btc5m_trades") or row.get("btc5m_trades_24h") or 0)
            fresh_flow = bool(row.get("fresh_flow")) or btc5m_buys > 0
            latest_trade_ts = row.get("latest_btc5m_trade_ts")
            source_reported_age_h = row.get("latest_trade_age_h")
            if source_key == "paper_shadow_enrollments":
                source_reported_age_h = None
            computed_age_h = _latest_trade_age_h_from_ts(latest_trade_ts, now_ts=now_ts)
            latest_age_h = computed_age_h if computed_age_h is not None else source_reported_age_h
            p1_eligible = bool(row.get("p1_promotion_eligible") or row.get("pass_admission_threshold"))
            if effective_source_key == "remote_dataapi_24h":
                p1_eligible = _remote_dataapi_passes_admission_threshold(
                    btc5m_buys=btc5m_buys,
                    latest_age_h=latest_age_h,
                )
            evidence = {
                "source": effective_source_key,
                "fresh_flow_rank_source": effective_source_key if fresh_flow else "no_fresh_flow_evidence",
                "probe_rank": index,
                "fresh_flow": fresh_flow,
                "p1_promotion_eligible": p1_eligible,
                "btc5m_buys": btc5m_buys,
                "btc5m_trades": btc5m_trades,
                "local_feed_rows": int(row.get("local_feed_rows") or row.get("remote_rows") or 0),
                "inband_025_050_buy_share_pct": row.get("inband_025_050_buy_share_pct"),
                "latest_btc5m_trade_ts": latest_trade_ts,
                "latest_trade_age_h": latest_age_h,
                "source_reported_latest_trade_age_h": source_reported_age_h,
                "latest_trade_age_source": "computed_from_latest_btc5m_trade_ts"
                if computed_age_h is not None
                else "source_reported",
                "remote_rows_saturated": bool(row.get("remote_rows_saturated")),
                "coverage_complete_24h": bool(row.get("coverage_complete_24h")),
                "censored": row.get("censored") or "",
                "median_buy_entry_offset_s": row.get("median_buy_entry_offset_s"),
                "policy_compatible_inband_buy_rows_24h": int(
                    row.get("policy_compatible_inband_buy_rows_24h") or 0
                ),
                "btc5m_buy_rows_24h_by_price_subband": dict(
                    row.get("btc5m_buy_rows_24h_by_price_subband") or {}
                ),
                "p1_reject_reasons": row.get("p1_reject_reasons") or [],
            }
            if effective_source_key == "remote_dataapi_24h":
                evidence.update(
                    {
                        "remote_dataapi_btc5m_trades_24h": btc5m_trades,
                        "remote_dataapi_btc5m_buys_24h": btc5m_buys,
                        "remote_dataapi_policy_compatible_inband_buy_rows_24h": int(
                            row.get("policy_compatible_inband_buy_rows_24h") or 0
                        ),
                        "remote_dataapi_btc5m_buy_rows_24h_by_price_subband": dict(
                            row.get("btc5m_buy_rows_24h_by_price_subband") or {}
                        ),
                        "remote_dataapi_latest_trade_age_h": latest_age_h,
                        "remote_dataapi_source_reported_latest_trade_age_h": source_reported_age_h,
                        "remote_rows_saturated": bool(row.get("remote_rows_saturated")),
                    }
                )
            remote = (row.get("evidence") or {}).get("remote_dataapi_24h") if isinstance(row.get("evidence"), dict) else {}
            if isinstance(remote, dict) and remote:
                remote_buys = int(remote.get("btc5m_buys_24h") or 0)
                remote_trade_ts = remote.get("latest_btc5m_trade_ts")
                remote_source_reported_age_h = remote.get("latest_trade_age_h")
                remote_computed_age_h = _latest_trade_age_h_from_ts(remote_trade_ts, now_ts=now_ts)
                remote_age_h = remote_computed_age_h if remote_computed_age_h is not None else remote_source_reported_age_h
                evidence.update(
                    {
                        "remote_dataapi_btc5m_trades_24h": int(remote.get("btc5m_trades_24h") or 0),
                        "remote_dataapi_btc5m_buys_24h": remote_buys,
                        "remote_dataapi_policy_compatible_inband_buy_rows_24h": int(
                            remote.get("policy_compatible_inband_buy_rows_24h") or 0
                        ),
                        "remote_dataapi_btc5m_buy_rows_24h_by_price_subband": dict(
                            remote.get("btc5m_buy_rows_24h_by_price_subband") or {}
                        ),
                        "remote_dataapi_latest_trade_age_h": remote_age_h,
                        "remote_dataapi_source_reported_latest_trade_age_h": remote_source_reported_age_h,
                        "remote_rows_saturated": bool(remote.get("remote_rows_saturated")),
                        "coverage_complete_24h": bool(remote.get("coverage_complete_24h")),
                        "censored": remote.get("censored") or "",
                        "latest_btc5m_trade_ts": remote_trade_ts or evidence.get("latest_btc5m_trade_ts"),
                        "latest_trade_age_h": remote_age_h if remote_age_h is not None else evidence.get("latest_trade_age_h"),
                        "source_reported_latest_trade_age_h": remote_source_reported_age_h,
                        "latest_trade_age_source": "computed_from_latest_btc5m_trade_ts"
                        if remote_computed_age_h is not None
                        else evidence.get("latest_trade_age_source"),
                    }
                )
                if _remote_dataapi_passes_admission_threshold(btc5m_buys=remote_buys, latest_age_h=remote_age_h):
                    evidence["p1_promotion_eligible"] = True
            current = out.get(wallet)
            merged = _merge_fresh_flow_evidence(current or {}, evidence)
            current_score = _fresh_flow_sort_key(current or {})
            if current is None or _fresh_flow_sort_key(merged) < current_score:
                out[wallet] = merged

    for raw in dataapi_first_seen_rows or []:
        if not isinstance(raw, dict) or raw.get("event") != "dataapi_first_seen" or raw.get("backfill") is True:
            continue
        wallet = _norm_wallet(raw.get("wallet") or raw.get("source_wallet"))
        if not wallet:
            continue
        try:
            captured_at = float(raw.get("captured_at_s") or raw.get("observed_ts") or raw.get("timestamp") or 0.0)
        except (TypeError, ValueError):
            captured_at = 0.0
        if captured_at <= 0 or now_ts - captured_at > 86400.0:
            continue
        age_s = round(max(0.0, now_ts - captured_at), 6)
        current = out.get(wallet) or {}
        count = int(current.get("dataapi_first_seen_rows_24h") or 0) + 1
        out[wallet] = _merge_fresh_flow_evidence(
            current,
            {
                "dataapi_first_seen_rows_24h": count,
                "dataapi_first_seen_rows_per_hour_24h": round(count / 24.0, 6),
                "latest_dataapi_first_seen_age_s": age_s,
            },
        )

    poller = active_set_poller if isinstance(active_set_poller, dict) else {}
    fetch_meta = poller.get("fetch_meta") if isinstance(poller.get("fetch_meta"), dict) else {}
    for wallet_raw, meta in fetch_meta.items():
        wallet = _norm_wallet(wallet_raw)
        if not wallet or not isinstance(meta, dict):
            continue
        feedback = meta.get("policy_feedback") if isinstance(meta.get("policy_feedback"), dict) else {}
        if not feedback:
            continue
        policy_rows = int(feedback.get("policy_compatible_fresh_buy_rows_le_30s") or 0)
        update = {
            "policy_compatible_fresh_buy_rows_le_30s": policy_rows,
            "policy_compatible_recent_rows_per_hour_extrapolated": round(policy_rows * 120.0, 6),
            "freshest_policy_compatible_buy_lag_s": feedback.get("freshest_policy_compatible_buy_lag_s"),
        }
        out[wallet] = _merge_fresh_flow_evidence(out.get(wallet) or {}, update)

    summary = poller.get("summary") if isinstance(poller.get("summary"), dict) else {}
    fresh_poll_only = summary.get("fresh_poll_only_by_wallet") if isinstance(summary.get("fresh_poll_only_by_wallet"), dict) else {}
    for wallet_raw, count in fresh_poll_only.items():
        wallet = _norm_wallet(wallet_raw)
        if wallet:
            out[wallet] = _merge_fresh_flow_evidence(out.get(wallet) or {}, {"fresh_poll_only_rows_le_30s": int(count or 0)})
    return out


def _parse_generated_at(value: Any) -> dt.datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _probe_staleness(probe_payload: dict[str, Any]) -> dict[str, Any]:
    raw_sources: list[tuple[str, Any]] = []
    if isinstance(probe_payload, dict):
        raw_sources.append(("top_level", probe_payload.get("generated_at")))
        remote_dataapi = probe_payload.get("remote_dataapi_24h")
        if isinstance(remote_dataapi, dict):
            raw_sources.append(("remote_dataapi_24h", remote_dataapi.get("generated_at")))
    parsed_sources = [
        (source, generated)
        for source, raw_value in raw_sources
        if (generated := _parse_generated_at(raw_value)) is not None
    ]
    if not parsed_sources:
        return {
            "generated_at": probe_payload.get("generated_at") if isinstance(probe_payload, dict) else None,
            "source": "",
            "age_h": None,
            "stale_gt_6h": True,
            "warning": "fresh_flow_probe_generated_at_missing_or_invalid",
        }
    source, generated = max(parsed_sources, key=lambda item: item[1])
    age_h = max(0.0, (dt.datetime.now(dt.timezone.utc) - generated).total_seconds() / 3600.0)
    return {
        "generated_at": generated.isoformat().replace("+00:00", "Z"),
        "source": source,
        "age_h": round(age_h, 6),
        "stale_gt_6h": age_h > 6.0,
        "warning": "fresh_flow_probe_stale_gt_6h" if age_h > 6.0 else "",
    }


def _fresh_flow_sort_key(evidence: dict[str, Any]) -> tuple[int, int, int, float, int, int, float, float, int]:
    if not evidence:
        return (1, 1, 0, 0.0, 0, 0, 0.0, 1_000_000.0, 1_000_000)
    age = num(evidence.get("latest_trade_age_h"), 1_000_000.0)
    return (
        0 if evidence.get("fresh_flow") else 1,
        0 if evidence.get("p1_promotion_eligible") else 1,
        -int(evidence.get("policy_compatible_fresh_buy_rows_le_30s") or 0),
        -num(evidence.get("policy_compatible_recent_rows_per_hour_extrapolated"), 0.0),
        -int(evidence.get("remote_dataapi_policy_compatible_inband_buy_rows_24h") or 0),
        -int(evidence.get("remote_dataapi_btc5m_buys_24h") or 0),
        -num(evidence.get("dataapi_first_seen_rows_per_hour_24h"), 0.0),
        age,
        int(evidence.get("probe_rank") or 1_000_000),
    )


def _apply_fresh_flow_rank(row: dict[str, Any], fresh_flow_index: dict[str, dict[str, Any]]) -> None:
    evidence = {**_empty_fresh_flow_evidence(), **(fresh_flow_index.get(_norm_wallet(row.get("wallet"))) or {})}
    row["fresh_flow_rank"] = {
        "fresh_flow": bool(evidence.get("fresh_flow")),
        "p1_promotion_eligible": bool(evidence.get("p1_promotion_eligible")),
        "policy_compatible_fresh_buy_rows_le_30s": int(
            evidence.get("policy_compatible_fresh_buy_rows_le_30s") or 0
        ),
        "policy_compatible_recent_rows_per_hour_extrapolated": evidence.get(
            "policy_compatible_recent_rows_per_hour_extrapolated"
        ),
        "freshest_policy_compatible_buy_lag_s": evidence.get("freshest_policy_compatible_buy_lag_s"),
        "dataapi_first_seen_rows_24h": int(evidence.get("dataapi_first_seen_rows_24h") or 0),
        "dataapi_first_seen_rows_per_hour_24h": evidence.get("dataapi_first_seen_rows_per_hour_24h"),
        "latest_dataapi_first_seen_age_s": evidence.get("latest_dataapi_first_seen_age_s"),
        "remote_dataapi_btc5m_trades_24h": int(evidence.get("remote_dataapi_btc5m_trades_24h") or 0),
        "remote_dataapi_btc5m_buys_24h": int(evidence.get("remote_dataapi_btc5m_buys_24h") or 0),
        "remote_dataapi_policy_compatible_inband_buy_rows_24h": int(
            evidence.get("remote_dataapi_policy_compatible_inband_buy_rows_24h") or 0
        ),
        "remote_dataapi_btc5m_buy_rows_24h_by_price_subband": dict(
            evidence.get("remote_dataapi_btc5m_buy_rows_24h_by_price_subband") or {}
        ),
        "remote_dataapi_latest_trade_age_h": evidence.get("remote_dataapi_latest_trade_age_h"),
        "remote_dataapi_source_reported_latest_trade_age_h": evidence.get(
            "remote_dataapi_source_reported_latest_trade_age_h"
        ),
        "remote_rows_saturated": bool(evidence.get("remote_rows_saturated")),
        "coverage_complete_24h": bool(evidence.get("coverage_complete_24h")),
        "censored": evidence.get("censored") or "",
        "fresh_poll_only_rows_le_30s": int(evidence.get("fresh_poll_only_rows_le_30s") or 0),
        "btc5m_buys": int(evidence.get("btc5m_buys") or 0),
        "btc5m_trades": int(evidence.get("btc5m_trades") or 0),
        "local_feed_rows": int(evidence.get("local_feed_rows") or 0),
        "inband_025_050_buy_share_pct": evidence.get("inband_025_050_buy_share_pct"),
        "latest_btc5m_trade_ts": evidence.get("latest_btc5m_trade_ts"),
        "latest_trade_age_h": evidence.get("latest_trade_age_h"),
        "source_reported_latest_trade_age_h": evidence.get("source_reported_latest_trade_age_h"),
        "latest_trade_age_source": evidence.get("latest_trade_age_source"),
        "median_buy_entry_offset_s": evidence.get("median_buy_entry_offset_s"),
        "probe_rank": evidence.get("probe_rank"),
        "source": evidence.get("source") or "",
        "rank_source": evidence.get("fresh_flow_rank_source") or "no_fresh_flow_evidence",
        "p1_reject_reasons": evidence.get("p1_reject_reasons") or [],
    }


def _stamp_external_liveness(
    rows: list[dict[str, Any]],
    probe: dict[str, Any],
    *,
    now_ts: float,
) -> dict[str, int]:
    """Stamp queue rows from the verified external probe used by cohort admission."""
    probe_rows = _external_liveness_rows_by_wallet_from_probe(probe)
    probe_generated_at = _parse_utc_ts(probe.get("generated_at"))
    now = dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc)
    counts = {"PASS": 0, "FAIL": 0, "MISSING": 0}
    for row in rows:
        wallet = _norm_wallet(row.get("wallet") or row.get("source_wallet"))
        probe_row = probe_rows.get(wallet)
        if probe_row is None:
            row["external_liveness_status"] = "MISSING"
            row["external_liveness_reason"] = "external_liveness_row_missing"
            counts["MISSING"] += 1
            continue
        passed, reason, age_h = _external_liveness_passes_for_digest(
            probe_row,
            now=now,
            probe_generated_at=probe_generated_at,
        )
        status = "PASS" if passed else "FAIL"
        row["external_liveness_status"] = status
        row["external_liveness_reason"] = reason
        row["external_latest_trade_age_h"] = None if age_h is None else round(age_h, 6)
        row["external_liveness_max_age_h"] = 24.0
        row["external_liveness_probe"] = {
            "status": status,
            "reason": reason,
            "probe_generated_at": probe.get("generated_at"),
            "probe_row_status": probe_row.get("status"),
            "probe_fetched_at": probe_row.get("fetched_at"),
            "source": "queue_remote_dataapi_fresh_flow_probe",
        }
        counts[status] += 1
    return counts


def _merge_external_liveness_probes(*probes: dict[str, Any]) -> dict[str, Any]:
    rows_by_wallet: dict[str, dict[str, Any]] = {}
    generated_at = ""
    for probe in probes:
        if not isinstance(probe, dict):
            continue
        generated_at = max(generated_at, str(probe.get("generated_at") or ""))
        for wallet, row in _external_liveness_rows_by_wallet_from_probe(probe).items():
            current = rows_by_wallet.get(wallet)
            if current is None or str(row.get("fetched_at") or "") > str(current.get("fetched_at") or ""):
                rows_by_wallet[wallet] = row
    return {"generated_at": generated_at, "rows": list(rows_by_wallet.values())}


def _cohort_admission_observation_rows(
    payload: dict[str, Any], excluded_wallets: set[str], admitted_wallets: set[str] | None = None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for packet in payload.get("packets") or []:
        if not isinstance(packet, dict):
            continue
        wallet = _norm_wallet(packet.get("wallet"))
        if admitted_wallets is not None and wallet not in admitted_wallets:
            continue
        temporal = packet.get("temporal_evidence") if isinstance(packet.get("temporal_evidence"), dict) else {}
        matched = temporal.get("matched_slice") if isinstance(temporal.get("matched_slice"), dict) else {}
        resolved = int(matched.get("resolved_trades") or matched.get("resolved_signals") or 0)
        pnl = num(matched.get("pnl_usd"), 0.0)
        roi = num(matched.get("roi_pct"), 0.0)
        fresh = int(packet.get("fresh_own_source_buy_rows_30m") or 0)
        classification = str(temporal.get("classification") or "").upper()
        if (
            not wallet
            or wallet in excluded_wallets
            or resolved < 200
            or pnl <= 0
            or roi <= 0
            or fresh < 10
            or "FADING" in classification
        ):
            continue
        rows.append(
            {
                "wallet": wallet,
                "name": f"registry_observation_{wallet[-10:]}",
                "candidate_id": packet.get("candidate_id"),
                "queue_source": "registry_admission_observation",
                "observation_member": True,
                "paper_only": True,
                "live_orders_allowed": False,
                "ready_for_live": False,
                "paper_policy_id": packet.get("paper_policy_id"),
                "copy_policy_family": packet.get("paper_policy_id"),
                "fresh_own_source_buy_rows_30m": fresh,
                "regime_slices": {"weekday": {
                    "pnl_usd": pnl,
                    "roi_pct": roi,
                    "resolved_signals": resolved,
                }},
                "resolved_pnl": pnl,
                "external_liveness_status": "PASS",
                "external_latest_trade_age_h": packet.get("latest_trade_age_h"),
                "external_liveness_max_age_h": 24.0,
                "next_action": "observe runtime own-source flow; live seat requires Fable pre-authorized runtime gate",
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["fresh_own_source_buy_rows_30m"]),
            -num((row["regime_slices"]["weekday"]).get("pnl_usd")),
            -num((row["regime_slices"]["weekday"]).get("roi_pct")),
            row["wallet"],
        )
    )
    return rows


def _clearance_evidence(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    coverage = row.get("window_coverage") if isinstance(row.get("window_coverage"), dict) else {}
    return {
        "classification": row.get("classification") or row.get("clearance_status") or "",
        "paper_pnl_usd": num(metrics.get("paper_pnl_usd"), None),
        "copyable_buy_events": int(metrics.get("copyable_buy_events") or 0),
        "candidate_clob_backed_orders": int(metrics.get("candidate_clob_backed_orders") or metrics.get("filled_orders") or 0),
        "resolved_orders": int(metrics.get("resolved_orders") or 0),
        "reject_ratio": metrics.get("reject_ratio"),
        "raw_reject_ratio": metrics.get("raw_reject_ratio", metrics.get("reject_ratio")),
        "attributable_reject_numerator": metrics.get(
            "attributable_reject_numerator", metrics.get("attributable_rejects")
        ),
        "attributable_reject_denominator": metrics.get(
            "attributable_reject_denominator", metrics.get("attributable_denominator")
        ),
        "attributable_reject_ratio": metrics.get("attributable_reject_ratio"),
        "attributable_reject_floor_status": metrics.get(
            "attributable_reject_floor_status", metrics.get("attributable_reject_status")
        ),
        "attributable_reject_floor_min": metrics.get(
            "attributable_reject_floor_min", metrics.get("attributable_sample_floor")
        ),
        "attributable_sample_floor_met": metrics.get("attributable_sample_floor_met"),
        "environment_reject_count": metrics.get("environment_reject_count", metrics.get("environment_rejects")),
        "unresolved_filled_order_count": int(metrics.get("unresolved_filled_order_count") or 0),
        "missing_resolution_market_count": int(coverage.get("missing_resolution_market_count") or 0),
        "queue_rank": row.get("queue_rank"),
    }


def _apply_clearance_ready(row: dict[str, Any], clearance: dict[str, Any] | None) -> None:
    if not clearance or row.get("ready_for_live"):
        return
    row["ready_for_live"] = True
    row["clearance_ready"] = True
    row["clearance"] = _clearance_evidence(clearance)
    row["next_action"] = "eligible_for_half_size_fable_pin_from_clearance"


def _queue_row(
    row: dict[str, Any],
    replay_index: dict[str, dict[str, Any]],
    clearance_index: dict[str, dict[str, Any]],
    *,
    source: str,
) -> dict[str, Any]:
    wallet = _norm_wallet(row.get("wallet"))
    replay_candidate = replay_index.get(wallet, {})
    replay = replay_candidate.get("paper_replay") if isinstance(replay_candidate.get("paper_replay"), dict) else {}
    replay_status = str(replay.get("eligibility_status") or replay.get("status") or "MISSING").upper()
    paper_pnl = num(replay.get("paper_pnl_usd"), None)
    clob_backed_orders = int(replay.get("candidate_clob_backed_orders") or 0)
    copyable_events = int(replay.get("copyable_buy_events") or 0)
    failure_reasons = [str(item) for item in (replay.get("failure_reasons") or []) if str(item or "")]
    meets_copyable_floor = (
        copyable_events >= PROMOTION_MIN_COPYABLE_BUY_EVENTS
        and clob_backed_orders >= PROMOTION_MIN_COPYABLE_BUY_EVENTS
    )
    ready = replay_status == "PASS" and paper_pnl is not None and paper_pnl > 0 and meets_copyable_floor
    next_action = "eligible_for_fable_pin" if ready else "attach resolutions and rerun full-pool replay until PASS"
    if replay_status == "PASS" and not meets_copyable_floor:
        next_action = (
            "keep paper-only; replay PASS needs >=20 copyable/CLOB-backed BUYs before live-ready queue admission"
        )
    if "candidate_unresolved_ratio_above_maximum" in failure_reasons:
        next_action = "refresh/attach market resolutions, then rescore stored replay orders"
    elif "candidate_paper_pnl_not_positive" in failure_reasons:
        next_action = "keep paper-only; do not promote until positive replay PnL"
    elif replay_status == "MISSING":
        next_action = "include wallet in next full-pool replay batch"
    queued = {
        "wallet": wallet,
        "name": row.get("name") or replay_candidate.get("candidate_id") or "",
        "queue_source": source,
        "ready_for_live": ready,
        "best_band": _best_band(row),
        "resolved_pnl": row.get("resolved_pnl"),
        "leaderboard_rank": row.get("leaderboard_rank"),
        "complementary_hours_score": _complementary_hours_score(row),
        "complementary_fills": int(row.get("complementary_fills") or 0),
        "complementary_windows": int(row.get("complementary_windows") or 0),
        "recent_fill_windows": int(row.get("recent_fill_windows") or 0),
        "copyability_profile": {
            "status": (row.get("eligible_profile") or {}).get("status")
            if isinstance(row.get("eligible_profile"), dict)
            else "",
            "copyable_rate_pct": row.get("copyable_rate_pct"),
            "fill_sample": row.get("fill_sample"),
            "eligible_move_slices": int(row.get("eligible_move_slices") or 0),
            "mean_edge": row.get("mean_edge"),
            "median_edge": row.get("median_edge"),
        },
        "replay": {
            "candidate_id": replay_candidate.get("candidate_id") or "",
            "status": replay_status,
            "policy_id": replay.get("policy_id") or "",
            "paper_pnl_usd": paper_pnl,
            "paper_orders": int(replay.get("paper_orders") or 0),
            "resolved_orders": int(replay.get("resolved_orders") or 0),
            "copyable_buy_events": copyable_events,
            "candidate_clob_backed_orders": clob_backed_orders,
            "unresolved_ratio": replay.get("unresolved_ratio"),
            "failure_reasons": failure_reasons,
        },
        "next_action": next_action,
    }
    _apply_clearance_ready(queued, clearance_index.get(wallet))
    return queued


def _strict_replay_pass_rows(
    replay_payload: dict[str, Any],
    existing_wallets: set[str],
    clearance_index: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in replay_payload.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        if not wallet or wallet in existing_wallets:
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        if str(replay.get("eligibility_status") or "").upper() != "PASS":
            continue
        if num(replay.get("paper_pnl_usd"), 0.0) <= 0.0:
            continue
        copyable_events = int(replay.get("copyable_buy_events") or 0)
        clob_backed_orders = int(replay.get("candidate_clob_backed_orders") or 0)
        ready = (
            copyable_events >= PROMOTION_MIN_COPYABLE_BUY_EVENTS
            and clob_backed_orders >= PROMOTION_MIN_COPYABLE_BUY_EVENTS
        )
        queued = {
                "wallet": wallet,
                "name": candidate.get("candidate_id") or f"strict_replay_pass_{wallet[-12:]}",
                "queue_source": "strict_replay_pass",
                "ready_for_live": ready,
                "best_band": {
                    "move_slice_key": "",
                    "seconds_bucket": "",
                    "entry_price_band": f"<={replay.get('max_buy_price') or ''}".rstrip("="),
                    "latency_horizon_s": None,
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "mean_edge": None,
                    "median_edge": None,
                    "status": "PASS_REPLAY_ONLY",
                },
                "resolved_pnl": replay.get("paper_pnl_usd"),
                "leaderboard_rank": candidate.get("leaderboard_rank"),
                "complementary_hours_score": 0,
                "complementary_fills": clob_backed_orders,
                "complementary_windows": int(replay.get("resolved_orders") or 0),
                "recent_fill_windows": int(replay.get("resolved_orders") or 0),
                "copyability_profile": {
                    "status": "PASS_REPLAY_ONLY",
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "eligible_move_slices": 0,
                    "mean_edge": None,
                    "median_edge": None,
                },
                "replay": {
                    "candidate_id": candidate.get("candidate_id") or "",
                    "status": "PASS",
                    "policy_id": replay.get("policy_id") or "",
                    "paper_pnl_usd": num(replay.get("paper_pnl_usd"), None),
                    "paper_orders": int(replay.get("paper_orders") or 0),
                    "resolved_orders": int(replay.get("resolved_orders") or 0),
                    "copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": clob_backed_orders,
                    "unresolved_ratio": replay.get("unresolved_ratio"),
                    "failure_reasons": [],
                },
                "next_action": (
                    "eligible_for_fable_pin"
                    if ready
                    else "keep paper-only; replay PASS needs >=20 copyable/CLOB-backed BUYs before live-ready queue admission"
                ),
            }
        _apply_clearance_ready(queued, clearance_index.get(wallet))
        rows.append(queued)
    return rows


def _fresh_copyability_rows(
    payload: dict[str, Any],
    excluded_wallets: set[str],
) -> list[dict[str, Any]]:
    inputs = payload.get("inputs") if isinstance(payload.get("inputs"), dict) else {}
    dependencies_fresh = bool(
        payload.get("status") == "PASS_CURRENT_SOURCE"
        and payload.get("promotion_grade") is True
        and inputs.get("source_freshness_pass") is True
        and inputs.get("followability_freshness_pass") is True
        and inputs.get("replay_freshness_pass") is True
    )
    if not dependencies_fresh:
        return []
    rows: list[dict[str, Any]] = []
    for source in payload.get("ranked_queue") or []:
        if not isinstance(source, dict):
            continue
        wallet = _norm_wallet(source.get("wallet"))
        if not wallet or wallet in excluded_wallets or source.get("queue_eligible") is not True:
            continue
        replay = source.get("copy_replay") if isinstance(source.get("copy_replay"), dict) else {}
        source_history = source.get("source_history") if isinstance(source.get("source_history"), dict) else {}
        profile = source.get("profile_shortlist") if isinstance(source.get("profile_shortlist"), dict) else {}
        rows.append(
            {
                "wallet": wallet,
                "name": source.get("registry_name") or replay.get("candidate_id") or "",
                "queue_source": "fresh_full_universe_copyability",
                "ready_for_live": False,
                "best_band": profile.get("best_band") or {},
                "resolved_pnl": replay.get("paper_pnl_usd"),
                "leaderboard_rank": source.get("universe_rank") or source.get("queue_rank"),
                "complementary_hours_score": len(source_history.get("activity_hours_utc") or []),
                "complementary_fills": int(replay.get("candidate_clob_backed_orders") or 0),
                "complementary_windows": int(replay.get("unique_windows") or 0),
                "recent_fill_windows": int(replay.get("unique_windows") or 0),
                "copyability_profile": {
                    "status": source.get("admission_status") or "READY_QUEUE",
                    "copyability_score": source.get("copyability_score"),
                    "fill_sample": int(replay.get("copyable_buy_events") or 0),
                    "eligible_move_slices": 0,
                },
                "replay": dict(replay),
                "full_universe_copyability": {
                    "universe_rank": source.get("universe_rank") or source.get("queue_rank"),
                    "copyability_score": source.get("copyability_score"),
                    "wash_filter_status": source.get("wash_filter_status"),
                    "evidence_sources": source.get("evidence_sources") or [],
                },
                "next_action": "run standard paper/live evidence gates; ranked_queue status alone never promotes",
            }
        )
    return rows


def _clearance_ready_rows(
    clearance_index: dict[str, dict[str, Any]],
    replay_index: dict[str, dict[str, Any]],
    existing_wallets: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for wallet, clearance in clearance_index.items():
        if wallet in existing_wallets:
            continue
        replay_candidate = replay_index.get(wallet, {})
        replay = replay_candidate.get("paper_replay") if isinstance(replay_candidate.get("paper_replay"), dict) else {}
        evidence = _clearance_evidence(clearance)
        rows.append(
            {
                "wallet": wallet,
                "name": replay_candidate.get("candidate_id") or f"clearance_ready_{wallet[-12:]}",
                "queue_source": "clearance_ready",
                "ready_for_live": True,
                "clearance_ready": True,
                "clearance": evidence,
                "best_band": {
                    "move_slice_key": "",
                    "seconds_bucket": "",
                    "entry_price_band": f"<={replay.get('max_buy_price') or ''}".rstrip("="),
                    "latency_horizon_s": None,
                    "copyable_rate_pct": None,
                    "fill_sample": evidence["copyable_buy_events"],
                    "mean_edge": None,
                    "median_edge": None,
                    "status": "CLEARANCE_READY",
                },
                "resolved_pnl": evidence["paper_pnl_usd"],
                "leaderboard_rank": replay_candidate.get("leaderboard_rank"),
                "complementary_hours_score": 0,
                "complementary_fills": evidence["candidate_clob_backed_orders"],
                "complementary_windows": evidence["resolved_orders"],
                "recent_fill_windows": evidence["resolved_orders"],
                "copyability_profile": {
                    "status": "CLEARANCE_READY",
                    "copyable_rate_pct": None,
                    "fill_sample": evidence["copyable_buy_events"],
                    "eligible_move_slices": 0,
                    "mean_edge": None,
                    "median_edge": None,
                },
                "replay": {
                    "candidate_id": replay_candidate.get("candidate_id") or "",
                    "status": str(replay.get("eligibility_status") or replay.get("status") or "CLEARANCE_READY"),
                    "policy_id": replay.get("policy_id") or "",
                    "paper_pnl_usd": num(replay.get("paper_pnl_usd"), evidence["paper_pnl_usd"]),
                    "paper_orders": int(replay.get("paper_orders") or 0),
                    "resolved_orders": int(replay.get("resolved_orders") or evidence["resolved_orders"] or 0),
                    "copyable_buy_events": int(replay.get("copyable_buy_events") or evidence["copyable_buy_events"] or 0),
                    "candidate_clob_backed_orders": int(
                        replay.get("candidate_clob_backed_orders") or evidence["candidate_clob_backed_orders"] or 0
                    ),
                    "unresolved_ratio": replay.get("unresolved_ratio"),
                    "failure_reasons": [str(item) for item in (replay.get("failure_reasons") or []) if str(item or "")],
                },
                "next_action": "eligible_for_half_size_fable_pin_from_clearance",
            }
        )
    return rows


def _watch_next_action(failure_reasons: list[str]) -> str:
    if "candidate_unresolved_ratio_above_maximum" in failure_reasons:
        return "refresh/attach market resolutions, then rescore stored replay orders"
    if "candidate_rejected_fill_ratio_above_maximum" in failure_reasons:
        return "keep paper-only; improve fill quality or wait for cleaner CLOB-backed sample"
    if "candidate_missing_clob_fill_evidence" in failure_reasons:
        return "keep paper-only; capture more CLOB-backed source events before live-ready queue admission"
    if "candidate_no_policy_buy_events" in failure_reasons:
        return "keep paper-only; no copyable policy BUY sample yet"
    return "keep paper-only; replay watch candidate needs all live-ready gates before promotion"


def _positive_replay_watch_rows(replay_payload: dict[str, Any], existing_wallets: set[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in replay_payload.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        if not wallet or wallet in existing_wallets:
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        if str(replay.get("eligibility_status") or replay.get("status") or "").upper() == "PASS":
            continue
        paper_pnl = num(replay.get("paper_pnl_usd"), None)
        if paper_pnl is None or paper_pnl <= 0.0:
            continue
        copyable_events = int(replay.get("copyable_buy_events") or 0)
        clob_backed_orders = int(replay.get("candidate_clob_backed_orders") or 0)
        if copyable_events <= 0 and clob_backed_orders <= 0:
            continue
        failure_reasons = [str(item) for item in (replay.get("failure_reasons") or []) if str(item or "")]
        rows.append(
            {
                "wallet": wallet,
                "name": candidate.get("candidate_id") or f"positive_replay_watch_{wallet[-12:]}",
                "queue_source": "positive_replay_watch",
                "ready_for_live": False,
                "best_band": {
                    "move_slice_key": "",
                    "seconds_bucket": "",
                    "entry_price_band": f"<={replay.get('max_buy_price') or ''}".rstrip("="),
                    "latency_horizon_s": None,
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "mean_edge": None,
                    "median_edge": None,
                    "status": "POSITIVE_REPLAY_WATCH",
                },
                "resolved_pnl": paper_pnl,
                "leaderboard_rank": candidate.get("leaderboard_rank"),
                "complementary_hours_score": 0,
                "complementary_fills": clob_backed_orders,
                "complementary_windows": int(replay.get("resolved_orders") or 0),
                "recent_fill_windows": int(replay.get("resolved_orders") or 0),
                "copyability_profile": {
                    "status": "POSITIVE_REPLAY_WATCH",
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "eligible_move_slices": 0,
                    "mean_edge": None,
                    "median_edge": None,
                },
                "replay": {
                    "candidate_id": candidate.get("candidate_id") or "",
                    "status": str(replay.get("eligibility_status") or replay.get("status") or ""),
                    "policy_id": replay.get("policy_id") or "",
                    "paper_pnl_usd": paper_pnl,
                    "paper_orders": int(replay.get("paper_orders") or 0),
                    "resolved_orders": int(replay.get("resolved_orders") or 0),
                    "copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": clob_backed_orders,
                    "unresolved_ratio": replay.get("unresolved_ratio"),
                    "failure_reasons": failure_reasons,
                },
                "next_action": _watch_next_action(failure_reasons),
            }
        )
    return rows


def _market_cohort_replay_window_hours(row: dict[str, Any]) -> float | None:
    first_ts = _parse_iso_ts(row.get("first_trade_ts"))
    latest_ts = _parse_iso_ts(row.get("latest_trade_ts"))
    if first_ts is None or latest_ts is None:
        return None
    return round(max(0.0, latest_ts - first_ts) / 3600.0, 6)


def _market_cohort_bridge_rows(
    cohort_payload: dict[str, Any],
    existing_wallets: set[str],
    excluded_wallets: set[str] | None = None,
    *,
    now_ts: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    defects: list[dict[str, Any]] = []
    excluded_wallets = set(excluded_wallets or set())
    excluded_reason_counts: dict[str, int] = {}
    excluded_reason_wallets: dict[str, list[str]] = {}
    picks = cohort_payload.get("live_ready_picks") if isinstance(cohort_payload, dict) else []
    accounting = {
        "source_live_ready_picks": len(picks) if isinstance(picks, list) else 0,
        "bridged_rows": 0,
        "excluded_rows": 0,
        "excluded_reason_counts": excluded_reason_counts,
        "excluded_reason_wallets": excluded_reason_wallets,
    }
    if not isinstance(cohort_payload, dict) or "live_ready_picks" not in cohort_payload:
        return rows, defects, accounting
    if not isinstance(picks, list):
        accounting["excluded_rows"] = 1
        excluded_reason_counts["live_ready_picks_schema_invalid"] = 1
        defects.append(
            {
                "defect": "market_cohort_replay_live_ready_picks_schema_invalid",
                "attempts": "bridge validation",
                "next": "regenerate wallet_market_cohort_replay_latest.json with list live_ready_picks before rerun",
            }
        )
        return rows, defects, accounting
    required_fields = (
        "paper_pnl_usd",
        "roi_pct",
        "resolved_copyable_events",
        "win_rate_pct",
        "unique_markets",
        "first_trade_ts",
        "latest_trade_ts",
    )
    for pick in picks:
        if not isinstance(pick, dict):
            excluded_reason_counts["pick_schema_invalid"] = excluded_reason_counts.get("pick_schema_invalid", 0) + 1
            defects.append(
                {
                    "defect": "market_cohort_replay_pick_schema_invalid",
                    "wallet": "",
                    "attempts": "bridge validation",
                    "next": "remove or rewrite non-object cohort pick row then rerun bridge",
                }
            )
            continue
        wallet = _norm_wallet(pick.get("wallet"))
        missing = [field for field in required_fields if pick.get(field) in (None, "")]
        if not wallet or missing:
            excluded_reason_counts["pick_missing_required_fields"] = (
                excluded_reason_counts.get("pick_missing_required_fields", 0) + 1
            )
            defects.append(
                {
                    "defect": "market_cohort_replay_pick_schema_invalid",
                    "wallet": wallet or str(pick.get("wallet") or ""),
                    "missing": missing,
                    "attempts": "bridge validation",
                    "next": "fix cohort replay evidence fields then rerun bridge",
                }
            )
            continue
        if wallet in existing_wallets:
            reason = "excluded_wallet_denylist" if wallet in excluded_wallets else "already_in_queue"
            excluded_reason_counts[reason] = excluded_reason_counts.get(reason, 0) + 1
            samples = excluded_reason_wallets.setdefault(reason, [])
            if len(samples) < 5:
                samples.append(wallet)
            continue
        copyable_events = int(pick.get("copyable_buy_events") or 0)
        resolved_events = int(pick.get("resolved_copyable_events") or 0)
        unique_markets = int(pick.get("unique_markets") or pick.get("unique_conditions") or 0)
        unresolved_events = int(pick.get("unresolved_copyable_events") or max(0, copyable_events - resolved_events))
        unresolved_ratio = round(unresolved_events / copyable_events, 6) if copyable_events > 0 else None
        latest_trade_ts_s = _parse_iso_ts(pick.get("latest_trade_ts"))
        days_since_last_trade = (
            round(max(0.0, float(now_ts) - latest_trade_ts_s) / 86400.0, 6)
            if latest_trade_ts_s is not None
            else None
        )
        evidence = {
            "paper_pnl_usd": num(pick.get("paper_pnl_usd"), None),
            "roi_pct": num(pick.get("roi_pct"), None),
            "resolved_copyable_events": resolved_events,
            "win_rate_pct": num(pick.get("win_rate_pct"), None),
            "unique_markets": unique_markets,
            "first_trade_ts": pick.get("first_trade_ts"),
            "latest_trade_ts": pick.get("latest_trade_ts"),
            "last_trade_ts": pick.get("latest_trade_ts"),
            "days_since_last_trade": days_since_last_trade,
            "replay_window_hours": _market_cohort_replay_window_hours(pick),
            "copyable_buy_events": copyable_events,
            "stake_usd": num(pick.get("stake_usd"), None),
            "history_rows_seen": int(pick.get("history_rows_seen") or 0),
            "pagination_cap_reached": bool(pick.get("pagination_cap_reached")),
            "source_generated_at": cohort_payload.get("generated_at"),
            "source_status": pick.get("status") or "",
            "evidence_role": "bench_shadow_accrual_entry_signal_not_promotion_signal",
            "sizing_note": "source replay uses source stakes; live promotion still requires standard queue gates",
        }
        rows.append(
            {
                "wallet": wallet,
                "name": f"market_cohort_replay_{wallet[-12:]}",
                "queue_source": "market_cohort_replay",
                "ready_for_live": False,
                "bench_tier": "market_cohort_shadow_accrual",
                "last_trade_ts": evidence["last_trade_ts"],
                "days_since_last_trade": evidence["days_since_last_trade"],
                "best_band": {
                    "move_slice_key": "",
                    "seconds_bucket": "",
                    "entry_price_band": "market_cohort_replay_observed",
                    "latency_horizon_s": None,
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "mean_edge": None,
                    "median_edge": None,
                    "status": "MARKET_COHORT_REPLAY_ENTRY_SIGNAL",
                },
                "resolved_pnl": evidence["paper_pnl_usd"],
                "leaderboard_rank": pick.get("leaderboard_rank"),
                "complementary_hours_score": 0,
                "complementary_fills": resolved_events,
                "complementary_windows": unique_markets,
                "recent_fill_windows": unique_markets,
                "copyability_profile": {
                    "status": "MARKET_COHORT_REPLAY_ENTRY_SIGNAL",
                    "copyable_rate_pct": None,
                    "fill_sample": copyable_events,
                    "eligible_move_slices": 0,
                    "mean_edge": None,
                    "median_edge": None,
                },
                "replay": {
                    "candidate_id": f"market_cohort_replay_{wallet[-12:]}",
                    "status": pick.get("status") or "LIVE_READY_SHADOW_PICK",
                    "policy_id": "",
                    "paper_pnl_usd": evidence["paper_pnl_usd"],
                    "paper_orders": copyable_events,
                    "resolved_orders": resolved_events,
                    "copyable_buy_events": copyable_events,
                    "candidate_clob_backed_orders": 0,
                    "unresolved_ratio": unresolved_ratio,
                    "failure_reasons": ["bench_tier_entry_signal_not_promotion_signal"],
                },
                "market_cohort_replay": evidence,
                "next_action": (
                    "run standard member queue shadow accrual and clearance gates; "
                    "no live promotion from cohort replay alone"
                ),
            }
        )
        existing_wallets.add(wallet)
    accounting["bridged_rows"] = len(rows)
    accounting["excluded_rows"] = max(0, int(accounting["source_live_ready_picks"] or 0) - len(rows))
    return rows, defects, accounting


def _disabled_or_demoted(member: dict[str, Any]) -> bool:
    status = str(member.get("status") or "").upper()
    return member.get("enabled") is False or status.startswith("DEMOTED") or status.startswith("DISABLED")


def _bench_vintage_key(row: dict[str, Any]) -> str:
    for key in ("recruitment_vintage", "discovery_vintage", "discovery_week"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return str(row.get("queue_source") or "unknown_queue_source")


def _bench_activity_hour(row: dict[str, Any]) -> str:
    fresh = row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}
    trade_ts = fresh.get("latest_btc5m_trade_ts")
    timestamp = num(trade_ts, 0.0)
    if timestamp > 0.0:
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).strftime("%H")
    return "unknown"


def _bench_last_trade_age_h(row: dict[str, Any]) -> float | None:
    fresh = row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}
    for key in ("remote_dataapi_latest_trade_age_h", "latest_trade_age_h"):
        value = fresh.get(key)
        if value is not None:
            return num(value, None)
    return None


def _apply_bench_liveness_purge(rows: list[dict[str, Any]], *, max_alive_age_h: float = 48.0) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    vintage_counts: dict[str, int] = {}
    activity_hour_counts: dict[str, int] = {}
    stale_wallets: list[str] = []
    unknown_wallets: list[str] = []
    for row in rows:
        wallet = _norm_wallet(row.get("wallet"))
        age_h = _bench_last_trade_age_h(row)
        vintage_key = _bench_vintage_key(row)
        activity_hour = _bench_activity_hour(row)
        vintage_counts[vintage_key] = vintage_counts.get(vintage_key, 0) + 1
        activity_hour_counts[activity_hour] = activity_hour_counts.get(activity_hour, 0) + 1
        row["bench_liveness"] = {
            "authority": "Fable DIRECTION 2026-07-13T13:19Z bench purge",
            "last_real_trade_age_h": age_h,
            "last_trade_age_hours": age_h,
            "max_alive_age_h": max_alive_age_h,
            "recruitment_vintage": vintage_key,
            "activity_hour_utc": activity_hour,
        }
        row["last_trade_age_hours"] = age_h
        if age_h is None:
            fresh = row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}
            no_btc5m_observed = int(fresh.get("remote_dataapi_btc5m_trades_24h") or fresh.get("btc5m_trades") or 0) == 0
            zero_is_adjudicated = bool(
                no_btc5m_observed and (fresh.get("coverage_complete_24h") or fresh.get("censored"))
            )
            if zero_is_adjudicated:
                status = "DORMANT_NOT_FRESH"
                row["bench_tier"] = "dormant_not_fresh"
                row["next_action"] = (
                    "keep out of clearance; remote probe found no BTC5m trades after bounded coverage/cap"
                )
                row["bench_liveness"]["zero_btc5m_adjudication"] = {
                    "coverage_complete_24h": bool(fresh.get("coverage_complete_24h")),
                    "censored": fresh.get("censored") or "",
                }
            else:
                status = "UNKNOWN_NO_REMOTE_LIVENESS"
                unknown_wallets.append(wallet)
                row["bench_tier"] = "unknown_remote_liveness"
                if row.get("ready_for_live"):
                    row["next_action"] = (
                        "run external Data API liveness refresh; "
                        "live-ready admission requires <=48h last-real-trade age"
                    )
            if row.get("ready_for_live"):
                row.setdefault("ready_for_live_before_bench_liveness_purge", True)
                row["ready_for_live"] = False
        elif age_h > max_alive_age_h:
            status = "DORMANT_STALE_GT_48H"
            stale_wallets.append(wallet)
            row["ready_for_live_before_bench_liveness_purge"] = bool(row.get("ready_for_live"))
            row["ready_for_live"] = False
            row["bench_tier"] = "dormant"
            row["next_action"] = "keep dormant until external Data API liveness re-sweep shows <=48h last-real-trade age"
        else:
            replay = row.get("replay") if isinstance(row.get("replay"), dict) else {}
            market_cohort_live_ready = (
                row.get("queue_source") == "market_cohort_replay"
                and str(replay.get("status") or "") == "LIVE_READY_SHADOW_PICK"
                and not row.get("ready_for_live")
            )
            if market_cohort_live_ready:
                status = "SHADOW_CLOCK_HELD"
                row["bench_tier"] = "shadow_clock_held"
                row["shadow_clock_held"] = True
                row["next_action"] = (
                    "wait for routing-shadow clock re-adjudication; "
                    "do not promote from market-cohort replay alone"
                )
                row["bench_liveness"]["shadow_clock"] = {
                    "status": "HELD",
                    "rule": "market_cohort_replay_live_ready_rows_wait_for_routing_shadow_clock",
                    "ready_for_live_semantics_changed": False,
                }
            else:
                status = "READY_AND_ALIVE" if row.get("ready_for_live") else "BENCH_ALIVE_NOT_READY"
                row["bench_tier"] = "alive"
        row["bench_liveness"]["status"] = status
        status_counts[status] = status_counts.get(status, 0) + 1
    max_vintage_share = 0.0
    if rows and vintage_counts:
        max_vintage_share = round(max(vintage_counts.values()) / len(rows), 6)
    return {
        "bench_liveness_max_alive_age_h": max_alive_age_h,
        "ready_alive": status_counts.get("READY_AND_ALIVE", 0),
        "hot_standby_ready": status_counts.get("READY_AND_ALIVE", 0),
        "hot_standby_required": 2,
        "hot_standby_gap": max(0, 2 - status_counts.get("READY_AND_ALIVE", 0)),
        "bench_alive_not_ready": status_counts.get("BENCH_ALIVE_NOT_READY", 0),
        "shadow_clock_held": status_counts.get("SHADOW_CLOCK_HELD", 0),
        "dormant_not_fresh": status_counts.get("DORMANT_NOT_FRESH", 0),
        "dormant_stale_gt_48h": status_counts.get("DORMANT_STALE_GT_48H", 0),
        "unknown_remote_liveness": status_counts.get("UNKNOWN_NO_REMOTE_LIVENESS", 0),
        "bench_liveness_status_counts": dict(sorted(status_counts.items())),
        "bench_liveness_stale_wallets": stale_wallets[:20],
        "bench_liveness_unknown_wallets": unknown_wallets[:20],
        "recruitment_vintage_counts": dict(sorted(vintage_counts.items())),
        "recruitment_vintage_max_share": max_vintage_share,
        "recruitment_vintage_rule_pass": max_vintage_share <= 0.5 if rows else True,
        "activity_hour_counts_utc": dict(sorted(activity_hour_counts.items())),
    }


def _excluded_wallets(*payloads: dict[str, Any]) -> set[str]:
    wallets: set[str] = set()
    for payload in payloads:
        active_set = payload.get("active_set") if isinstance(payload.get("active_set"), dict) else payload
        members = active_set.get("members") if isinstance(active_set.get("members"), list) else []
        for member in members:
            if not isinstance(member, dict):
                continue
            wallet = _norm_wallet(member.get("source_wallet") or member.get("wallet"))
            if not wallet:
                continue
            if active_set is not payload or _disabled_or_demoted(member):
                wallets.add(wallet)
    return wallets


def build_queue(
    *,
    shortlist: dict[str, Any],
    replay_payload: dict[str, Any],
    rotation_state: dict[str, Any],
    clearance_payload: dict[str, Any] | None = None,
    clearance_packets_payload: dict[str, Any] | None = None,
    fresh_flow_probe: dict[str, Any] | None = None,
    active_set_poller: dict[str, Any] | None = None,
    dataapi_first_seen_rows: list[dict[str, Any]] | None = None,
    breadth_dispositions: dict[str, Any] | None = None,
    market_cohort_replay: dict[str, Any] | None = None,
    copyability_payload: dict[str, Any] | None = None,
    excluded_wallets: set[str] | None = None,
    now_ts: float | None = None,
    limit: int,
    external_liveness_probe: dict[str, Any] | None = None,
    cohort_admission: dict[str, Any] | None = None,
    active_set_overlay_payload: dict[str, Any] | None = None,
    observation_admissions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_ts = float(now_ts or time.time())
    replay_index = _replay_by_wallet(replay_payload)
    clearance_index = _clearance_by_wallet(clearance_payload or {})
    packet_clearance_index = _packet_cleared_wallets(clearance_packets_payload or {})
    probe_staleness = _probe_staleness(fresh_flow_probe or {})
    fresh_flow_index = _fresh_flow_by_wallet(
        fresh_flow_probe or {},
        active_set_poller=active_set_poller,
        dataapi_first_seen_rows=dataapi_first_seen_rows,
        now_ts=now_ts,
    )
    excluded_wallets = {
        wallet for raw_wallet in (excluded_wallets or set()) if (wallet := _norm_wallet(raw_wallet))
    }
    authoritative_copyability = copyability_payload is not None
    candidates = _fresh_copyability_rows(copyability_payload or {}, excluded_wallets)
    if not authoritative_copyability:
        for row in shortlist.get("top_candidates") or []:
            if isinstance(row, dict) and _norm_wallet(row.get("wallet")) not in excluded_wallets:
                candidates.append(_queue_row(row, replay_index, clearance_index, source="fill_backed_profile_positive"))
        for row in shortlist.get("pnl_only_no_lane_evidence") or []:
            if isinstance(row, dict) and _norm_wallet(row.get("wallet")) not in excluded_wallets:
                candidates.append(_queue_row(row, replay_index, clearance_index, source="profile_positive_pnl_only"))
    existing_wallets = {
        wallet for row in candidates if (wallet := _norm_wallet(row.get("wallet")))
    } | excluded_wallets
    strict_rows = [] if authoritative_copyability else _strict_replay_pass_rows(
        replay_payload, existing_wallets, clearance_index
    )
    candidates.extend(strict_rows)
    existing_wallets.update(str(row.get("wallet") or "") for row in strict_rows)
    clearance_rows = [] if authoritative_copyability else _clearance_ready_rows(
        clearance_index, replay_index, existing_wallets
    )
    candidates.extend(clearance_rows)
    existing_wallets.update(str(row.get("wallet") or "") for row in clearance_rows)
    if authoritative_copyability:
        market_cohort_rows, market_cohort_defects, market_cohort_accounting = [], [], {
            "source_live_ready_picks": 0,
            "bridged_rows": 0,
            "excluded_rows": 0,
            "excluded_reason_counts": {},
            "excluded_reason_wallets": {},
        }
    else:
        market_cohort_rows, market_cohort_defects, market_cohort_accounting = _market_cohort_bridge_rows(
            market_cohort_replay or {},
            existing_wallets,
            excluded_wallets,
            now_ts=now_ts,
        )
    candidates.extend(market_cohort_rows)
    existing_wallets.update(str(row.get("wallet") or "") for row in market_cohort_rows)
    enabled_wallets = {
        wallet
        for row in (active_set_overlay_payload or {}).get("members", [])
        if isinstance(row, dict) and row.get("enabled") is not False
        for wallet in [_norm_wallet(row.get("source_wallet") or row.get("wallet"))]
        if wallet
    }
    admitted_wallets = {
        wallet
        for raw in (observation_admissions or {}).get("wallets", [])
        if (wallet := _norm_wallet(raw))
    }
    observation_rows = _cohort_admission_observation_rows(
        cohort_admission or {}, enabled_wallets, admitted_wallets if admitted_wallets else None
    )
    candidate_by_wallet = {
        wallet: row
        for row in candidates
        if (wallet := _norm_wallet(row.get("wallet")))
    }
    observation_rows_added: list[dict[str, Any]] = []
    for observation in observation_rows:
        wallet = _norm_wallet(observation.get("wallet"))
        existing = candidate_by_wallet.get(wallet)
        if existing is None:
            candidates.append(observation)
            candidate_by_wallet[wallet] = observation
            observation_rows_added.append(observation)
            continue
        existing["observation_member"] = True
        existing["paper_only"] = True
        existing["live_orders_allowed"] = False
        existing["fresh_own_source_buy_rows_30m"] = observation["fresh_own_source_buy_rows_30m"]
        existing["registry_admission_observation"] = {
            "candidate_id": observation.get("candidate_id"),
            "paper_policy_id": observation.get("paper_policy_id"),
            "regime_slices": observation.get("regime_slices"),
            "next_action": observation.get("next_action"),
        }
    existing_wallets.update(str(row.get("wallet") or "") for row in observation_rows_added)
    if not authoritative_copyability:
        candidates.extend(_positive_replay_watch_rows(replay_payload, existing_wallets))
    packet_clearance_applied = 0
    if authoritative_copyability:
        for row in candidates:
            wallet = _norm_wallet(row.get("wallet"))
            packet = packet_clearance_index.get(wallet)
            clearance = clearance_index.get(wallet)
            if not packet or not clearance:
                continue
            _apply_clearance_ready(row, clearance)
            row["clearance_packet"] = {
                "paper_disposition": (packet.get("clearance") or {}).get("paper_disposition"),
                "exact_policy_post_fee_status": (packet.get("exact_policy_post_fee_shadow") or {}).get("status"),
                "live_mutation": False,
            }
            packet_clearance_applied += 1
    for row in candidates:
        _apply_fresh_flow_rank(row, fresh_flow_index)
    breadth_disposition_index = _breadth_dispositions_by_wallet(
        breadth_dispositions or {},
        now_ts=now_ts,
    )
    breadth_disposition_counts = _apply_breadth_dispositions(candidates, breadth_disposition_index)
    candidates.sort(
        key=lambda row: (
            0 if row.get("ready_for_live") else 1,
            0 if row.get("queue_source") == "market_cohort_replay" else 1,
            _fresh_flow_sort_key(row.get("fresh_flow_rank") if isinstance(row.get("fresh_flow_rank"), dict) else {}),
            -int(row.get("complementary_hours_score") or 0),
            -int(row.get("complementary_fills") or 0),
            -num(row.get("resolved_pnl")),
            -int((row.get("copyability_profile") or {}).get("eligible_move_slices") or 0),
            str(row.get("wallet") or ""),
        )
    )
    limit = int(limit)
    ranked = candidates if limit <= 0 else candidates[: max(1, limit)]
    for index, row in enumerate(ranked, start=1):
        row["queue_rank"] = index
    bench_liveness_summary = _apply_bench_liveness_purge(ranked)
    external_liveness_counts = _stamp_external_liveness(
        ranked,
        external_liveness_probe if external_liveness_probe is not None else (fresh_flow_probe or {}),
        now_ts=now_ts,
    )
    decision = rotation_state.get("decision") if isinstance(rotation_state.get("decision"), dict) else {}
    defects = list(market_cohort_defects)
    if decision.get("action") == "RETAIN_NO_ELIGIBLE_CANDIDATE":
        defects.append(
            {
                "defect": "rotation_triggered_without_promotable_candidate",
                "attempts": "full-pool paper-shadow + replay + promotion selector",
                "next": "attach resolutions / rescore stored replay orders and keep full-width paper-shadow running",
            }
        )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_full_pool_member_queue",
        "flow_stage": "PROMOTE/LEARN/ROTATE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "summary": {
            "queue_depth": len(ranked),
            "ready_for_live": sum(1 for row in ranked if row.get("ready_for_live")),
            **bench_liveness_summary,
            "external_liveness_status_counts": external_liveness_counts,
            "profile_positive_pool": int((shortlist.get("summary") or {}).get("pool_after_active_set_exclusion") or 0),
            "fill_backed_candidates": int((shortlist.get("summary") or {}).get("top_count") or 0),
            "replay_candidates": int((replay_payload.get("replay_summary") or {}).get("candidate_count") or 0),
            "replay_promotable": int((replay_payload.get("replay_summary") or {}).get("promotable_replays") or 0),
            "clearance_ready": sum(1 for row in ranked if row.get("clearance_ready")),
            "packet_clearance_applied": packet_clearance_applied,
            "market_cohort_bridge_candidates": len(market_cohort_rows),
            "registry_admission_observation_members": len(observation_rows),
            "market_cohort_bridge_ranked": sum(
                1 for row in ranked if row.get("queue_source") == "market_cohort_replay"
            ),
            "market_cohort_bridge_defects": len(market_cohort_defects),
            "market_cohort_bridge_source_live_ready_picks": market_cohort_accounting.get("source_live_ready_picks"),
            "market_cohort_bridge_bridged": market_cohort_accounting.get("bridged_rows"),
            "market_cohort_bridge_excluded": market_cohort_accounting.get("excluded_rows"),
            "market_cohort_bridge_excluded_reason_counts": market_cohort_accounting.get("excluded_reason_counts"),
            "market_cohort_bridge_excluded_reason_wallets": market_cohort_accounting.get("excluded_reason_wallets"),
            "fresh_flow_probe_generated_at": probe_staleness.get("generated_at"),
            "fresh_flow_probe_generated_at_source": probe_staleness.get("source"),
            "fresh_flow_probe_age_h": probe_staleness.get("age_h"),
            "fresh_flow_probe_stale_gt_6h": probe_staleness.get("stale_gt_6h"),
            "fresh_flow_probe_staleness_warning": probe_staleness.get("warning"),
            "clearance_ready_with_fresh_flow": sum(
                1
                for row in ranked
                if row.get("clearance_ready")
                and isinstance(row.get("fresh_flow_rank"), dict)
                and row["fresh_flow_rank"].get("fresh_flow")
            ),
            "clearance_ready_with_policy_compatible_fresh": sum(
                1
                for row in ranked
                if row.get("clearance_ready")
                and isinstance(row.get("fresh_flow_rank"), dict)
                and int(row["fresh_flow_rank"].get("policy_compatible_fresh_buy_rows_le_30s") or 0) > 0
            ),
            "clearance_ready_with_remote_dataapi_fresh": sum(
                1
                for row in ranked
                if row.get("clearance_ready")
                and isinstance(row.get("fresh_flow_rank"), dict)
                and int(row["fresh_flow_rank"].get("remote_dataapi_btc5m_buys_24h") or 0) > 0
            ),
            "breadth_dispositions_applied": sum(breadth_disposition_counts.values()),
            "breadth_disposition_counts": dict(sorted(breadth_disposition_counts.items())),
            "fresh_flow_ready": sum(
                1
                for row in ranked
                if isinstance(row.get("fresh_flow_rank"), dict) and row["fresh_flow_rank"].get("fresh_flow")
            ),
            "positive_replay_watch": sum(1 for row in ranked if row.get("queue_source") == "positive_replay_watch"),
            "rotation_action": decision.get("action") or "",
            "copyability_authoritative": authoritative_copyability,
            "copyability_status": (copyability_payload or {}).get("status"),
            "copyability_promotion_grade": (copyability_payload or {}).get("promotion_grade") is True,
            "copyability_ranked_queue_input": len((copyability_payload or {}).get("ranked_queue") or []),
        },
        "ranking": {
            "primary": (
                "ready_for_live_then_policy_compatible_fresh_flow_then_dataapi_24h_fresh_flow_"
                "then_corrected_copyability_fresh_flow_then_complementary_hours_then_complementary_fills_then_resolved_pnl"
            ),
            "fresh_flow_rule": (
                "fresh policy-compatible BUY rows from active-set poller feedback rank first; "
                "trailing-24h Data API first-seen flow ranks next when policy fields are unavailable"
            ),
            "promotion_rule": (
                "ready_for_live requires PASS replay with >=20 copyable/CLOB-backed BUYs, or Fable CLEAR clearance for half-size pin"
            ),
            "breadth_disposition_rule": (
                "active BREADTH dispositions can remove a queue row from live-ready status without deleting its evidence packet"
            ),
            "complementary_hours_rule": (
                "higher score means the candidate can add off-peak UTC hours to the active member portfolio"
            ),
        },
        "defects": defects,
        "ranked_members": ranked,
    }


def main() -> int:
    args = parse_args()
    active_set_overlay_payload = load_json(args.active_set_overlay, default={})
    payload = build_queue(
        shortlist=load_json(args.shortlist, default={}),
        replay_payload=load_json(args.replay, default={}),
        rotation_state=load_json(args.rotation_state, default={}),
        clearance_payload=load_json(args.clearance_gaps, default={}),
        clearance_packets_payload=load_json(args.clearance_packets, default={}),
        fresh_flow_probe=load_json(args.fresh_flow_probe, default={}),
        external_liveness_probe=_merge_external_liveness_probes(
            load_json(args.external_liveness_probe, default={}),
            load_json(args.registry_liveness_probe, default={}),
        ),
        cohort_admission=load_json(args.cohort_admission, default={}),
        active_set_overlay_payload=active_set_overlay_payload,
        observation_admissions=load_json(args.observation_admissions, default={}),
        active_set_poller=load_json(args.active_set_dataapi_poller_state, default={}),
        dataapi_first_seen_rows=_iter_jsonl(args.dataapi_first_seen_jsonl),
        breadth_dispositions=load_json(args.breadth_dispositions, default={}),
        market_cohort_replay=load_json(args.market_cohort_replay, default={}),
        copyability_payload=load_json(args.copyability, default={}),
        excluded_wallets=_excluded_wallets(
            load_json(args.live_guard_state, default={}),
            active_set_overlay_payload,
        ),
        limit=int(args.limit),
    )
    atomic_write_json(args.output, payload)
    print(json.dumps(payload["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
