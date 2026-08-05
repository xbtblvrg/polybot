"""Shared paper/live execution adapter for copy intents."""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from dataclasses import dataclass
from typing import Any, Literal

from src.wallet_copy.models import CopyIntent, LifecycleEvent, stable_id, utc_now_iso
from src.wallet_copy.fees import (
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.gate_registry import PRE_SUBMIT_REFUSAL_CLASSES
from src.wallet_copy.paper import PaperWalletCopyEngine
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, json_file_lock, load_json

ENTRY_PRICE_BAND_01A_MIN = 0.25
ENTRY_PRICE_BAND_01A_MAX_EXCLUSIVE = 0.32
ENTRY_PRICE_BAND_CLOSED_REASON = "entry_price_band_closed_negative_holdout"


def _candidate_policy_and_metadata(candidate: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {}
    metadata = candidate.get("metadata") if isinstance(candidate.get("metadata"), dict) else {}
    if not policy:
        policy = {
            "policy_id": candidate.get("policy_id"),
            "sizing_policy_id": candidate.get("sizing_policy_id"),
        }
    metadata = {
        **metadata,
        "source_wallet": metadata.get("source_wallet") or candidate.get("source_wallet"),
    }
    return policy, metadata


def _select_admission_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
    best = payload.get("best_candidate") if isinstance(payload.get("best_candidate"), dict) else {}
    runtime_id = str(decision.get("runtime_admission_candidate_id") or "")
    truth_source = str(decision.get("live_tracker_truth_source") or "")
    candidates: list[dict[str, Any]] = []
    for key in ("runtime_admission_candidate", "forward_runtime_candidate", "best_runtime_candidate"):
        row = payload.get(key)
        if isinstance(row, dict):
            candidates.append(row)
    for row in payload.get("forward_queue_runtime_candidates") or []:
        if isinstance(row, dict):
            candidates.append(row)
    if runtime_id:
        for candidate in candidates:
            if str(candidate.get("candidate_id") or "") == runtime_id:
                return candidate
    if truth_source in {"forward_candidate", "candidate_forward"} or truth_source.startswith("forward_queue_rank_"):
        for key in ("forward_runtime_candidate", "runtime_admission_candidate", "forward_candidate"):
            row = payload.get(key)
            if isinstance(row, dict):
                return row
    return best


def _certificate_copy_truth_status(payload: dict[str, Any], candidate: dict[str, Any]) -> str:
    certificate = (
        payload.get("live_readiness_certificate")
        if isinstance(payload.get("live_readiness_certificate"), dict)
        else {}
    )
    copy_truth = (
        certificate.get("copy_truth")
        if isinstance(certificate.get("copy_truth"), dict)
        else {}
    )
    if certificate.get("live_ready") is not True or certificate.get("blockers"):
        return ""
    if copy_truth.get("candidate_specific_copy_truth_present") is not True:
        return ""
    candidate_id = str(candidate.get("candidate_id") or "")
    truth_candidate_id = str(copy_truth.get("candidate_id") or "")
    if candidate_id and truth_candidate_id and candidate_id != truth_candidate_id:
        return ""
    required = int(copy_truth.get("required_buy_copy_events") or 0)
    clob_filled = int(copy_truth.get("clob_filled_buy_copy_events") or 0)
    dirty_counts = (
        int(copy_truth.get("fallback_filled_buy_copy_events") or 0),
        int(copy_truth.get("rejected_buy_copy_events") or 0),
        int(copy_truth.get("missed_buy_copy_events") or 0),
    )
    if required <= 0 or clob_filled < required or any(count > 0 for count in dirty_counts):
        return ""
    return PASS


def promote_intent_for_live(intent: CopyIntent, *, operator_approval_id: str) -> CopyIntent:
    """Flip a paper CopyIntent into live mode without changing the decision body."""

    if not str(operator_approval_id or "").strip():
        raise ValueError("live wallet-copy intent promotion requires operator approval id")
    metadata = dict(intent.metadata or {})
    metadata["operator_approval_id"] = str(operator_approval_id)
    return CopyIntent.from_dict(
        {
            **intent.asdict(),
            "mode": "live",
            "live_orders_allowed": True,
            "metadata": metadata,
        }
    )


def _paper_equivalent_for_parity(intent: CopyIntent) -> CopyIntent:
    if intent.mode == "paper" and not intent.live_orders_allowed:
        return intent
    metadata = dict(intent.metadata or {})
    metadata.pop("operator_approval_id", None)
    return CopyIntent.from_dict(
        {
            **intent.asdict(),
            "mode": "paper",
            "live_orders_allowed": False,
            "metadata": metadata,
        }
    )


def _parity_payload(intent: CopyIntent) -> dict[str, Any]:
    payload = intent.asdict()
    metadata = dict(payload.get("metadata") or {})
    metadata.pop("operator_approval_id", None)
    payload["metadata"] = metadata
    payload.pop("mode", None)
    payload.pop("live_orders_allowed", None)
    return payload


def _mismatched_keys(left: dict[str, Any], right: dict[str, Any]) -> list[str]:
    return sorted(key for key in set(left) | set(right) if left.get(key) != right.get(key))


def build_copy_intent_parity_capsule(
    intent: CopyIntent,
    *,
    clob_token_ids: list[str],
    operator_approval_id: str,
) -> dict[str, Any]:
    """Build a machine-checkable proof that live uses the paper CopyIntent body."""

    paper_payload = intent.asdict()
    try:
        live_intent = promote_intent_for_live(intent, operator_approval_id=operator_approval_id)
        live_payload = live_intent.asdict()
        live_decision = intent_to_trade_executor_decision(live_intent, clob_token_ids=clob_token_ids)
    except ValueError as exc:
        return {
            "schema_version": 1,
            "status": CORRECTION,
            "blockers": ["copy_intent_parity_build_failed"],
            "error": str(exc),
            "paper_intent_id": intent.intent_id,
            "paper_intent": paper_payload,
            "allowed_differences": ["mode", "live_orders_allowed", "metadata.operator_approval_id"],
        }

    mismatches = _mismatched_keys(_parity_payload(intent), _parity_payload(live_intent))
    decision_wallet_copy = live_decision.get("wallet_copy") if isinstance(live_decision, dict) else None
    decision_matches_live_intent = decision_wallet_copy == live_payload
    blockers: list[str] = []
    if intent.mode != "paper" or intent.live_orders_allowed:
        blockers.append("source_intent_not_paper_only")
    if mismatches:
        blockers.append("paper_live_copy_intent_body_mismatch")
    if not decision_matches_live_intent:
        blockers.append("trade_executor_wallet_copy_payload_mismatch")
    digest_payload = {
        "paper_intent": _parity_payload(intent),
        "live_intent": _parity_payload(live_intent),
        "allowed_differences": ["mode", "live_orders_allowed", "metadata.operator_approval_id"],
        "clob_token_ids": [str(token_id) for token_id in clob_token_ids if token_id],
        "decision_wallet_copy_matches_live_intent": decision_matches_live_intent,
    }
    return {
        "schema_version": 1,
        "status": PASS if not blockers else CORRECTION,
        "blockers": blockers,
        "parity_digest": stable_id("cip", digest_payload, length=32),
        "paper_intent_id": intent.intent_id,
        "live_intent_id": live_intent.intent_id,
        "allowed_differences": ["mode", "live_orders_allowed", "metadata.operator_approval_id"],
        "mismatched_fields": mismatches,
        "decision_wallet_copy_matches_live_intent": decision_matches_live_intent,
        "paper_intent": paper_payload,
        "live_intent": live_payload,
        "live_trade_decision": live_decision,
    }


@dataclass(frozen=True)
class LiveAdmissionSnapshot:
    decision_status: str = ANALYZE
    live_admission_status: str = ANALYZE
    live_orders_allowed: bool = False
    paper_only: bool = True
    live_tracker_truth_status: str = ""
    candidate_type: str = ""
    candidate_policy_id: str = ""
    sizing_policy_id: str = ""
    candidate_source_wallet: str = ""
    operator_approval_id: str = ""
    runtime_live_paused: bool = False
    blockers: tuple[str, ...] = ()

    @classmethod
    def from_profit_state(
        cls,
        payload: dict[str, Any],
        *,
        operator_approval_id: str = "",
        runtime_live_paused: bool = False,
    ) -> "LiveAdmissionSnapshot":
        decision = payload.get("decision") if isinstance(payload.get("decision"), dict) else {}
        live_truth = payload.get("live_tracker_truth") if isinstance(payload.get("live_tracker_truth"), dict) else {}
        effective_truth = (
            payload.get("effective_live_tracker_truth")
            if isinstance(payload.get("effective_live_tracker_truth"), dict)
            else {}
        )
        candidate = _select_admission_candidate(payload)
        policy, metadata = _candidate_policy_and_metadata(candidate)
        blockers = tuple(str(item) for item in (decision.get("live_admission_blockers") or []))
        live_tracker_truth_status = str(effective_truth.get("status") or live_truth.get("status") or "")
        if live_tracker_truth_status != PASS:
            certificate_truth_status = _certificate_copy_truth_status(payload, candidate)
            if certificate_truth_status == PASS:
                live_tracker_truth_status = PASS
        return cls(
            decision_status=str(decision.get("status") or ""),
            live_admission_status=str(decision.get("live_admission_status") or ""),
            live_orders_allowed=bool(decision.get("live_orders_allowed")),
            paper_only=bool(payload.get("paper_only", True)),
            live_tracker_truth_status=live_tracker_truth_status,
            candidate_type=str(candidate.get("candidate_type") or ""),
            candidate_policy_id=str(policy.get("policy_id") or ""),
            sizing_policy_id=str(policy.get("sizing_policy_id") or ""),
            candidate_source_wallet=str(metadata.get("source_wallet") or "").lower(),
            operator_approval_id=str(operator_approval_id or ""),
            runtime_live_paused=bool(runtime_live_paused),
            blockers=blockers,
        )

    def assert_allows(self, intents: list[CopyIntent]) -> None:
        if self.runtime_live_paused:
            raise ValueError("live wallet-copy execution is blocked by runtime live pause")
        if not self.operator_approval_id:
            raise ValueError("live wallet-copy execution requires recorded operator approval id")
        if self.paper_only:
            raise ValueError("live wallet-copy admission snapshot is still paper-only")
        if not self.live_orders_allowed:
            raise ValueError("live wallet-copy admission snapshot does not allow live orders")
        if self.decision_status != PASS or self.live_admission_status != PASS:
            raise ValueError("live wallet-copy admission snapshot is not PASS")
        if self.live_tracker_truth_status and self.live_tracker_truth_status != PASS:
            raise ValueError("live wallet-copy tracker truth is not PASS")
        if self.blockers:
            raise ValueError(f"live wallet-copy admission blockers present: {', '.join(self.blockers)}")
        for intent in intents:
            if intent.mode != "live" or intent.live_orders_allowed is not True:
                raise ValueError("live wallet-copy requires per-intent live admission")
            inventory_candidate = (
                self.candidate_type == "MULTI_WALLET_INVENTORY"
                and (intent.source_wallet == "INVENTORY" or "inventory" in intent.strategy_family)
            )
            if self.candidate_policy_id and intent.policy_id != self.candidate_policy_id and not inventory_candidate:
                raise ValueError("live wallet-copy intent policy does not match admission snapshot")
            if self.sizing_policy_id and intent.sizing_policy_id != self.sizing_policy_id and not inventory_candidate:
                raise ValueError("live wallet-copy intent sizing policy does not match admission snapshot")
            if self.candidate_source_wallet and intent.source_wallet.lower() != self.candidate_source_wallet:
                raise ValueError("live wallet-copy intent source wallet does not match admission snapshot")


@dataclass(frozen=True)
class ExecutionGate:
    mode: Literal["paper", "live"] = "paper"
    explicit_operator_go: bool = False
    live_orders_allowed: bool = False
    dry_run: bool = True
    admission_snapshot: LiveAdmissionSnapshot | None = None

    def assert_live_allowed(self, intents: list[CopyIntent] | None = None) -> None:
        if self.mode != "live":
            return
        if not self.explicit_operator_go:
            raise ValueError("live wallet-copy execution requires explicit operator go")
        if not self.live_orders_allowed:
            raise ValueError("live wallet-copy execution is not allowed")
        if self.dry_run:
            raise ValueError("live wallet-copy execution is still in dry-run")
        if self.admission_snapshot is None:
            raise ValueError("live wallet-copy execution requires a persisted admission snapshot")
        self.admission_snapshot.assert_allows(list(intents or []))


@dataclass(frozen=True)
class LiveExecutionLedgerConfig:
    state_path: str = "data/research/wallet_copy_live_execution_state.json"
    event_log_path: str = "data/research/wallet_copy_live_execution_events.jsonl"
    retain_orders: int = 100_000
    retain_lifecycle_events: int = 300_000


def _empty_live_execution_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "wallet_copy_live_execution_state",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "runtime_permission": {
            "status": "LIVE_RUNTIME_BLOCKED",
            "paper_only": True,
            "live_orders_allowed": False,
            "can_trade": False,
            "blockers": ["live_execution_ledger_has_no_runtime_permission"],
        },
        "orders": [],
        "lifecycle_events": [],
        "summary": {},
    }


def _live_lifecycle(
    status: str,
    order_id: str,
    intent_id: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return LifecycleEvent(
        ts=utc_now_iso(),
        status=status,  # type: ignore[arg-type]
        order_id=order_id,
        intent_id=intent_id,
        message=message,
        payload=payload or {},
    ).asdict()


def _live_final_status(result: dict[str, Any]) -> str:
    status = str(result.get("status") or "").lower()
    final_status = str(result.get("final_status") or "").lower()
    post_status = str(result.get("post_status") or "").lower()
    if status in {"error", "skipped", "unfilled"} or final_status in {
        "error",
        "unfilled",
        "execution_error",
        "balance_guard",
        "post_only_rejected",
        "order_size_hard_cap_exceeded",
        "window_fill_cap",
    }:
        return "REJECTED"
    fill_ratio = float(result.get("fill_ratio") or 0.0)
    filled_size = max(
        float(result.get("filled_size_usd") or 0.0),
        float(result.get("response_filled_size_usd") or 0.0),
        float(result.get("making_amount") or 0.0),
    )
    if (
        status in {"filled", "mock"}
        or final_status == "filled"
        or post_status in {"matched", "filled"}
        or fill_ratio >= 0.999
        or filled_size > 0
    ):
        return "FILLED"
    return "SUBMITTED"


def _live_result_is_skip(result: dict[str, Any]) -> bool:
    return str(result.get("status") or "").lower() == "skipped"


def _occupied_btc_5m_market_counts(state: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in state.get("orders") or []:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        if str(row.get("final_status") or row.get("status") or "").upper() in {"FILLED", "SUBMITTED"}:
            counts[slug] = counts.get(slug, 0) + 1
    return counts


def _raw_clob_reject_payload(result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    keys = (
        "status",
        "final_status",
        "error",
        "error_class",
        "order_id",
        "order_type",
        "fill_ratio",
        "fill_size_shares",
        "filled_size_usd",
        "unfilled_size_usd",
        "wallet_copy_chase",
        "wallet_copy_execute_live_profile",
    )
    payload = {key: result.get(key) for key in keys if result.get(key) is not None}
    raw = result.get("raw")
    if isinstance(raw, (dict, list)):
        payload["raw"] = raw
    return payload


def _latency_delta(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start <= 0 or end <= 0:
        return None
    return round(max(0.0, float(end) - float(start)), 6)


def _wallet_copy_latency_budget(
    intent: CopyIntent,
    *,
    intent_built_ts: float,
    submit_sent_ts: float,
    exchange_ack_ts: float,
) -> dict[str, Any]:
    source_fill_block_ts = float(intent.event_ts) if intent.event_ts is not None else None
    ws_recv_ts = float(intent.observed_ts) if intent.observed_ts is not None else None
    hops = {
        "source_fill_block_to_ws_recv_s": _latency_delta(source_fill_block_ts, ws_recv_ts),
        "ws_recv_to_intent_built_s": _latency_delta(ws_recv_ts, intent_built_ts),
        "intent_built_to_submit_sent_s": _latency_delta(intent_built_ts, submit_sent_ts),
        "submit_sent_to_exchange_ack_s": _latency_delta(submit_sent_ts, exchange_ack_ts),
        "source_fill_block_to_exchange_ack_s": _latency_delta(source_fill_block_ts, exchange_ack_ts),
        "ws_recv_to_exchange_ack_s": _latency_delta(ws_recv_ts, exchange_ack_ts),
    }
    return {
        "schema_version": 1,
        "source": "wallet_copy_live_adapter",
        "source_fill_block_ts": source_fill_block_ts,
        "ws_recv_ts": ws_recv_ts,
        "intent_built_ts": round(float(intent_built_ts), 6),
        "submit_sent_ts": round(float(submit_sent_ts), 6),
        "exchange_ack_ts": round(float(exchange_ack_ts), 6),
        "hops": hops,
        "missing_hops": [key for key, value in hops.items() if value is None],
    }


def _ms_to_seconds(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return round(max(0.0, numeric) / 1000.0, 6)


def _wall_seconds(started: float, ended: float | None = None) -> float:
    return round((time.perf_counter() if ended is None else ended) - started, 6)


def _profile_live_order_execution(
    *,
    intent: CopyIntent,
    role: str,
    order_started: float,
    prepare_order_s: float,
    submit_started: float,
    submit_ended: float,
    result: dict[str, Any],
) -> dict[str, Any]:
    post_submit_book = (
        result.get("wallet_copy_post_submit_book")
        if isinstance(result.get("wallet_copy_post_submit_book"), dict)
        else {}
    )
    sign_s = _ms_to_seconds(result.get("exec_sign_ms"))
    clob_post_s = _ms_to_seconds(result.get("exec_post_ms"))
    fill_poll_s = _ms_to_seconds(post_submit_book.get("fetch_ms"))
    total_submit_s = _ms_to_seconds(result.get("exec_total_ms"))
    submit_wall_s = round(submit_ended - submit_started, 6)
    if total_submit_s is None:
        total_submit_s = submit_wall_s
    if clob_post_s is None:
        clob_post_s = round(max(0.0, submit_wall_s - (sign_s or 0.0)), 6)
    fill_poll_reason = (
        "post_submit_book_fetch"
        if "fetch_ms" in post_submit_book
        else "post_order_response_fill_truth_no_separate_poll"
    )
    if fill_poll_s is None:
        fill_poll_s = 0.0
    missing_substages = {
        "sign_s": sign_s,
        "exec_post_ms": _ms_to_seconds(result.get("exec_post_ms")),
    }
    if fill_poll_reason == "post_submit_book_fetch":
        missing_substages["wallet_copy_post_submit_book.fetch_ms"] = _ms_to_seconds(
            post_submit_book.get("fetch_ms")
        )
    profile = {
        "schema_version": 1,
        "flow_stage": "LIVE/SELF-DEV",
        "intent_id": intent.intent_id,
        "condition_id": intent.condition_id,
        "market_slug": intent.market_slug,
        "outcome": intent.outcome,
        "execution_role": role,
        "status": str(result.get("final_status") or result.get("status") or ""),
        "order_id": str(result.get("order_id") or ""),
        "prepare_order_s": round(float(prepare_order_s), 6),
        "sign_s": sign_s,
        "clob_post_s": clob_post_s,
        "fill_poll_s": fill_poll_s,
        "fill_poll_reason": fill_poll_reason,
        "submit_wall_s": submit_wall_s,
        "submit_path_total_s": submit_wall_s,
        "submit_executor_total_s": total_submit_s,
        "total_s": _wall_seconds(order_started),
        "target_submit_path_p50_s": 0.5,
        "missing_substages": [key for key, value in missing_substages.items() if value is None],
    }
    result["wallet_copy_execute_profile"] = profile
    result["wallet_copy_execute_live_profile"] = profile
    return profile


def _summarize_live_execute_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    def _p50(values: list[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return round(ordered[mid], 6)
        return round((ordered[mid - 1] + ordered[mid]) / 2.0, 6)

    stage_names = (
        "prepare_order_s",
        "sign_s",
        "clob_post_s",
        "fill_poll_s",
        "submit_wall_s",
        "submit_path_total_s",
        "total_s",
    )
    return {
        "schema_version": 1,
        "flow_stage": "LIVE/SELF-DEV",
        "orders_profiled": len(profiles),
        "stage_p50_s": {
            name: _p50([float(row[name]) for row in profiles if row.get(name) is not None])
            for name in stage_names
        },
        "stage_max_s": {
            name: (
                round(max(float(row[name]) for row in profiles if row.get(name) is not None), 6)
                if any(row.get(name) is not None for row in profiles)
                else None
            )
            for name in stage_names
        },
        "profiles": profiles,
    }


def _btc_5m_window_close_ts(market_slug: str) -> float | None:
    slug = str(market_slug or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    marker = slug.rsplit("-", 1)[-1]
    if not marker.isdigit():
        return None
    return float(marker) + 300.0


def _is_fak_no_match_result(result: dict[str, Any]) -> bool:
    return (
        str(result.get("order_type") or "").upper() == "FAK"
        and str(result.get("error_class") or "").lower() == "fak_no_match"
        and str(result.get("final_status") or result.get("status") or "").lower() in {"unfilled", "rejected"}
    )


def _execution_role_from_result(result: dict[str, Any], *, default: str = "taker") -> str:
    role = str(result.get("execution_role") or "").lower()
    if role in {"maker", "taker"}:
        return role
    if result.get("maker") is True:
        return "maker"
    if result.get("maker") is False:
        return "taker"
    return default


def _float_value(*values: Any) -> float:
    for value in values:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(numeric) and numeric > 0.0:
            return numeric
    return 0.0


def _normalized_tx_hashes_from_status(*statuses: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for status in statuses:
        if not isinstance(status, dict):
            continue
        containers = [status]
        raw = status.get("raw")
        if isinstance(raw, dict):
            containers.append(raw)
        for container in containers:
            for key in ("tx_hashes", "transactionsHashes", "transactionHashes"):
                raw_value = container.get(key)
                if isinstance(raw_value, list):
                    values.extend(raw_value)
                elif raw_value:
                    values.append(raw_value)
            for key in ("transaction_hash", "transactionHash", "txHash", "hash"):
                raw_value = container.get(key)
                if raw_value:
                    values.append(raw_value)
            trades = container.get("trades")
            if isinstance(trades, list):
                for trade in trades:
                    if not isinstance(trade, dict):
                        continue
                    for key in ("transaction_hash", "transactionHash", "txHash", "hash"):
                        raw_value = trade.get(key)
                        if raw_value:
                            values.append(raw_value)
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip().lower()
        if text.startswith("0x") and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _expected_fee_reconciliation(
    *,
    metadata: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any] | None:
    expected_fee_gate = metadata.get("expected_fee_gate") if isinstance(metadata, dict) else {}
    if not isinstance(expected_fee_gate, dict) or not expected_fee_gate:
        return None
    response_cost = _float_value(
        result.get("response_filled_size_usd"),
        result.get("filled_size_usd"),
        result.get("making_amount"),
    )
    shares = _float_value(
        result.get("response_fill_size_shares"),
        result.get("fill_size_shares"),
        result.get("taking_amount"),
    )
    price = _float_value(result.get("response_fill_price"))
    if price <= 0.0 and response_cost > 0.0 and shares > 0.0:
        price = response_cost / shares
    expected_from_response = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    modeled_fee = modeled_unvalidated_polymarket_buy_fee_usd(shares=shares, price=price)
    status = "MODEL_DEMOTED_NO_REALIZED_FEE"
    return {
        "status": status,
        "flow_stage": "LIVE/SELF-DEV",
        "pre_submit_expected_fee_usd": expected_fee_gate.get("expected_fee_usd"),
        "response_expected_fee_usd": expected_from_response,
        "response_modeled_unvalidated_fee_usd": modeled_fee,
        "response_cost_usd": round(response_cost, 6),
        "response_fill_price": round(price, 6) if price > 0.0 else 0.0,
        "response_fill_size_shares": round(shares, 6),
        "expected_total_cost_usd": round(response_cost, 6)
        if response_cost > 0.0
        else expected_fee_gate.get("expected_total_cost_usd"),
        "realized_fee_usd": None,
        "realized_fee_source": "tx_receipt_pusd_debit_pending_scorecard",
        "modeled_unvalidated": True,
        "accounting_authority": False,
        "rule": "banked response cost is immutable; receipt premium remains a separate observation",
    }


def _ruled_floor_fill_evidence(
    *,
    intent: CopyIntent,
    result: dict[str, Any],
    latency_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    gate = (
        metadata.get("inventory_best_ask_gate")
        if isinstance(metadata.get("inventory_best_ask_gate"), dict)
        else {}
    )
    cost = _float_value(
        result.get("response_filled_size_usd"),
        result.get("filled_size_usd"),
        result.get("making_amount"),
    )
    shares = _float_value(
        result.get("response_fill_size_shares"),
        result.get("fill_size_shares"),
        result.get("taking_amount"),
    )
    realized_price = _float_value(result.get("response_fill_price"))
    if realized_price <= 0.0 and cost > 0.0 and shares > 0.0:
        realized_price = cost / shares
    gate_probe_best_ask = _float_value(
        gate.get("gate_probe_best_ask"), gate.get("best_ask")
    )
    limit_price = float(intent.limit_price or 0.0)
    observed_at_s = _float_value(gate.get("best_ask_observed_at_s"))
    gate_evaluated_at_s = _float_value(gate.get("gate_evaluated_at_s"))
    effective_latency_budget = latency_budget or result.get(
        "wallet_copy_latency_budget"
    )
    if not isinstance(effective_latency_budget, dict):
        effective_latency_budget = {}
    submit_sent_ts = _float_value(effective_latency_budget.get("submit_sent_ts"))
    book_age_at_submit_sent_s = _latency_delta(observed_at_s, submit_sent_ts)
    gate_to_submit_sent_s = _latency_delta(gate_evaluated_at_s, submit_sent_ts)
    generation_sha256 = str(gate.get("generation_sha256") or "").strip() or None
    evidence = {
        "flow_stage": "LIVE/LEARN/DEFEND",
        "gate_probe_best_ask": round(gate_probe_best_ask, 6)
        if gate_probe_best_ask > 0.0
        else None,
        "best_ask_observed_at_s": round(observed_at_s, 6)
        if observed_at_s > 0.0
        else None,
        "gate_evaluated_at_s": round(gate_evaluated_at_s, 6)
        if gate_evaluated_at_s > 0.0
        else None,
        "gate_probe_best_ask_age_at_gate_s": gate.get(
            "gate_probe_best_ask_age_at_gate_s"
        ),
        "book_age_at_submit_sent_s": book_age_at_submit_sent_s,
        "gate_to_submit_sent_s": gate_to_submit_sent_s,
        "max_gate_probe_best_ask_age_at_gate_s": gate.get(
            "max_gate_probe_best_ask_age_at_gate_s"
        ),
        "book_cache_ttl_s": gate.get("book_cache_ttl_s"),
        "threshold_independent_of_cache_ttl": gate.get(
            "threshold_independent_of_cache_ttl"
        ),
        "generation_sha256": generation_sha256,
        "event_age_s": gate.get("event_age_s"),
        "ruled_entry_floor": ENTRY_PRICE_BAND_01A_MIN,
        "below_ruled_entry_floor": bool(
            gate_probe_best_ask > 0.0
            and gate_probe_best_ask < ENTRY_PRICE_BAND_01A_MIN - 1e-9
        ),
        "realized_entry_price": round(realized_price, 9)
        if realized_price > 0.0
        else None,
        "realized_minus_limit_price": round(realized_price - limit_price, 9)
        if realized_price > 0.0 and limit_price > 0.0
        else None,
        "realized_minus_gate_probe_best_ask": round(
            realized_price - gate_probe_best_ask, 9
        )
        if realized_price > 0.0 and gate_probe_best_ask > 0.0
        else None,
        "realized_entry_classification": (
            "RULED_FLOOR_BREACH"
            if 0.0 < realized_price < ENTRY_PRICE_BAND_01A_MIN - 1e-9
            else "RULED_FLOOR_PASS"
            if realized_price > 0.0
            else "UNMEASURED"
        ),
    }
    return evidence


def _summarize_live_state(state: dict[str, Any]) -> dict[str, Any]:
    orders = [row for row in state.get("orders") or [] if isinstance(row, dict)]
    live_orders_allowed = bool(state.get("live_orders_allowed"))
    paper_only = bool(state.get("paper_only", not live_orders_allowed))
    can_trade = bool(state.get("can_trade") and live_orders_allowed and not paper_only)
    role_counts: dict[str, int] = {}
    for row in orders:
        role = str(row.get("execution_role") or "unknown")
        role_counts[role] = role_counts.get(role, 0) + 1
    return {
        "live_orders": len(orders),
        "submitted_orders": sum(1 for row in orders if row.get("final_status") == "SUBMITTED"),
        "filled_orders": sum(1 for row in orders if row.get("final_status") == "FILLED"),
        "rejected_orders": sum(1 for row in orders if row.get("final_status") == "REJECTED"),
        "execution_role_counts": dict(sorted(role_counts.items())),
        "paper_only": paper_only,
        "live_orders_allowed": live_orders_allowed,
        "can_trade": can_trade,
        "latest_order_ts": max((str(row.get("updated_at") or "") for row in orders), default=None),
    }


def _merge_persisted_order_resolution_fields(state: dict[str, Any], persisted: dict[str, Any]) -> None:
    """Preserve async resolution write-back fields across live guard state saves."""

    if not isinstance(persisted, dict):
        return
    persisted_orders = {
        str(row.get("order_id") or ""): row
        for row in persisted.get("orders") or []
        if isinstance(row, dict) and str(row.get("order_id") or "")
    }
    if not persisted_orders:
        return
    resolution_keys = ("resolved", "pnl_usd", "resolution", "resolution_updated_at")
    for order in state.get("orders") or []:
        if not isinstance(order, dict):
            continue
        persisted_order = persisted_orders.get(str(order.get("order_id") or ""))
        if not persisted_order:
            continue
        if persisted_order.get("resolved") is not True:
            continue
        for key in resolution_keys:
            if persisted_order.get(key) is not None:
                order[key] = persisted_order.get(key)
    if isinstance(persisted.get("resolution_writeback"), dict):
        state["resolution_writeback"] = persisted["resolution_writeback"]


class LiveWalletCopyLifecycle:
    """Durable live-side lifecycle ledger for the same CopyIntent contract used in paper."""

    def __init__(self, config: LiveExecutionLedgerConfig | None = None):
        self.config = config or LiveExecutionLedgerConfig()

    def load_state(self) -> dict[str, Any]:
        state = load_json(self.config.state_path, default=None)
        if not isinstance(state, dict) or state.get("kind") != "wallet_copy_live_execution_state":
            state = _empty_live_execution_state()
        state.setdefault("paper_only", True)
        state.setdefault("can_trade", False)
        state.setdefault("live_orders_allowed", False)
        state.setdefault(
            "runtime_permission",
            {
                "status": "LIVE_RUNTIME_UNKNOWN",
                "paper_only": bool(state.get("paper_only", True)),
                "live_orders_allowed": bool(state.get("live_orders_allowed", False)),
                "can_trade": bool(state.get("can_trade", False)),
                "blockers": [],
            },
        )
        for order in state.get("orders") or []:
            if not isinstance(order, dict) or str(order.get("final_status") or "") != "FILLED":
                continue
            result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
            for key in ("filled_size_usd", "fill_size_shares", "response_filled_size_usd", "response_fill_size_shares"):
                if not order.get(key) and result.get(key):
                    order[key] = result[key]
        return state

    def save_state(self, state: dict[str, Any], events: list[dict[str, Any]]) -> None:
        with json_file_lock(self.config.state_path):
            persisted = load_json(self.config.state_path, default={})
            if isinstance(persisted, dict):
                _merge_persisted_order_resolution_fields(state, persisted)
            orders = [row for row in state.get("orders") or [] if isinstance(row, dict)]
            lifecycle = [row for row in state.get("lifecycle_events") or [] if isinstance(row, dict)]
            state["orders"] = orders[-int(self.config.retain_orders) :]
            state["lifecycle_events"] = lifecycle[-int(self.config.retain_lifecycle_events) :]
            state["summary"] = _summarize_live_state(state)
            atomic_write_json(self.config.state_path, state)
        append_jsonl_many(self.config.event_log_path, events)

    def set_runtime_permission(
        self,
        *,
        paper_only: bool,
        live_orders_allowed: bool,
        can_trade: bool | None = None,
        status: str,
        blockers: list[str] | None = None,
        owner: str = "",
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update current live permission without mutating archived live orders."""

        state = self.load_state()
        resolved_can_trade = bool(live_orders_allowed and not paper_only) if can_trade is None else bool(can_trade)
        state["paper_only"] = bool(paper_only)
        state["live_orders_allowed"] = bool(live_orders_allowed)
        state["can_trade"] = bool(resolved_can_trade and live_orders_allowed and not paper_only)
        state["runtime_permission"] = {
            "status": str(status or ""),
            "paper_only": bool(paper_only),
            "live_orders_allowed": bool(live_orders_allowed),
            "can_trade": bool(state["can_trade"]),
            "blockers": sorted({str(blocker) for blocker in blockers or [] if str(blocker)}),
            "owner": str(owner or ""),
            "updated_at": utc_now_iso(),
            "details": details or {},
        }
        self.save_state(state, [])
        return state

    def record_result(
        self,
        *,
        intent: CopyIntent,
        trade_decision: dict[str, Any],
        result: dict[str, Any],
        parity_capsule: dict[str, Any],
        latency_budget: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        state = self.load_state()
        result = dict(result or {})
        floor_evidence = _ruled_floor_fill_evidence(
            intent=intent,
            result=result,
            latency_budget=latency_budget,
        )
        result["submit_best_ask_evidence"] = floor_evidence
        result["inventory_best_ask_gate"] = dict(
            intent.metadata.get("inventory_best_ask_gate") or {}
        ) if isinstance(intent.metadata, dict) else {}
        result["gate_probe_best_ask"] = floor_evidence["gate_probe_best_ask"]
        result["realized_entry_price"] = floor_evidence["realized_entry_price"]
        result["realized_entry_classification"] = floor_evidence[
            "realized_entry_classification"
        ]
        result["ruled_floor_breach"] = (
            floor_evidence["realized_entry_classification"]
            == "RULED_FLOOR_BREACH"
        )
        state["paper_only"] = False
        state["can_trade"] = True
        state["live_orders_allowed"] = True
        state["runtime_permission"] = {
            "status": "LIVE_RUNTIME_ALLOWED",
            "paper_only": False,
            "live_orders_allowed": True,
            "can_trade": True,
            "blockers": [],
            "owner": "wallet_copy_live_execution",
            "updated_at": utc_now_iso(),
        }
        order_id = str(result.get("order_id") or stable_id("lo", {"intent_id": intent.intent_id, "result": result}))
        final_status = _live_final_status(result)
        execution_role = _execution_role_from_result(result)
        lifecycle = [
            _live_lifecycle("INTENT_RECEIVED", order_id, intent.intent_id, "live copy intent accepted by adapter"),
        ]
        if _live_result_is_skip(result):
            lifecycle.append(
                _live_lifecycle("LIVE_SKIPPED", order_id, intent.intent_id, "live order skipped before submit", result)
            )
        else:
            lifecycle.append(
                _live_lifecycle("LIVE_SUBMITTED", order_id, intent.intent_id, "live order submitted through TradeExecutor")
            )
        if final_status == "FILLED":
            lifecycle.append(
                _live_lifecycle("LIVE_FILLED", order_id, intent.intent_id, "live order reported filled", result)
            )
        elif final_status == "REJECTED" and not _live_result_is_skip(result):
            lifecycle.append(
                _live_lifecycle("LIVE_REJECTED", order_id, intent.intent_id, "live order rejected or unfilled", result)
            )
        ts = utc_now_iso()
        order = {
            "schema_version": 1,
            "order_id": order_id,
            "intent_id": intent.intent_id,
            "source_wallet": intent.source_wallet.lower(),
            "wallet_name": intent.wallet_name,
            "condition_id": intent.condition_id,
            "market_slug": intent.market_slug,
            "outcome": intent.outcome,
            "side": intent.side,
            "requested_size_usd": float(intent.copy_size_usd),
            "requested_shares": float(intent.shares),
            "limit_price": float(intent.limit_price),
            "status": final_status,
            "final_status": final_status,
            "execution_role": execution_role,
            "maker": execution_role == "maker",
            "submitted_at": ts,
            "updated_at": ts,
            "paper_only": False,
            "live_orders_allowed": True,
            "source_intent": intent.asdict(),
            "trade_decision": trade_decision,
            "trade_result": result,
            "latency_budget": latency_budget or result.get("wallet_copy_latency_budget") or {},
            "parity_capsule": parity_capsule,
            "lifecycle": lifecycle,
        }
        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        copy_model = str(metadata.get("copy_model") or "").strip()
        if copy_model:
            order["copy_model"] = copy_model
        expected_fee_gate = metadata.get("expected_fee_gate")
        if isinstance(expected_fee_gate, dict):
            order["expected_fee_gate"] = expected_fee_gate
            reconciliation = _expected_fee_reconciliation(metadata=metadata, result=result)
            if reconciliation:
                order["expected_vs_realized_fee"] = reconciliation
        inventory = metadata.get("inventory_v2")
        if isinstance(inventory, dict):
            order["wallet_copy_inventory"] = inventory
        drip = metadata.get("inventory_v3_drip")
        if isinstance(drip, dict):
            order["wallet_copy_drip"] = drip
            order["signal_tier"] = drip.get("signal_tier")
        maker_fallback = result.get("wallet_copy_maker_fallback")
        if isinstance(maker_fallback, dict):
            order["maker_fallback"] = maker_fallback
        for field in (
            "maker_min_share_effective_cap_usd",
            "maker_min_share_bump_cost_usd",
        ):
            value = result.get(field)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                order[field] = float(value)
        if str(trade_decision.get("execution_lane") or "") == "e5_maker_first_btc5m_v1":
            window_close = _btc_5m_window_close_ts(intent.market_slug)
            if window_close is not None:
                order["maker_cancel"] = {
                    "policy": "cancel_30s_before_btc_5m_window_end",
                    "cancel_after_ts": window_close - 30.0,
                    "execution_lane": "e5_maker_first_btc5m_v1",
                }
        elif str(trade_decision.get("strategy_reason") or "") == "wallet_copy_passive_at_source":
            window_close = _btc_5m_window_close_ts(intent.market_slug)
            if window_close is not None:
                order["maker_cancel"] = {
                    "policy": "cancel_at_btc_5m_window_end",
                    "cancel_after_ts": window_close,
                    "execution_lane": "live_guard",
                    "strategy_reason": "wallet_copy_passive_at_source",
                }
        if final_status == "REJECTED":
            order["raw_clob_reject_payload"] = _raw_clob_reject_payload(result)
        state.setdefault("orders", []).append(order)
        state.setdefault("lifecycle_events", []).extend(lifecycle)
        event = {"event": "wallet_copy_live_order", **order}
        self.save_state(state, [event, *({"event": "wallet_copy_live_lifecycle", **row} for row in lifecycle)])
        return order

    def due_maker_fallback_orders(
        self,
        *,
        now_ts: float | None = None,
        force_e5_demotion: bool = False,
    ) -> list[dict[str, Any]]:
        now = time.time() if now_ts is None else float(now_ts)
        state = self.load_state()
        due: list[dict[str, Any]] = []
        for row in state.get("orders") or []:
            if not isinstance(row, dict):
                continue
            maker_fallback = row.get("maker_fallback") if isinstance(row.get("maker_fallback"), dict) else {}
            maker_cancel = row.get("maker_cancel") if isinstance(row.get("maker_cancel"), dict) else {}
            decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
            is_e5_maker = str(decision.get("execution_lane") or "") == "e5_maker_first_btc5m_v1"
            e5_close = _btc_5m_window_close_ts(str(row.get("market_slug") or "")) if is_e5_maker else None
            cancel_after = float(
                maker_fallback.get("cancel_after_ts")
                or maker_cancel.get("cancel_after_ts")
                or ((e5_close - 30.0) if e5_close is not None else 0.0)
            )
            if (
                str(row.get("execution_role") or "") == "maker"
                and str(row.get("final_status") or "") == "SUBMITTED"
                and (
                    (force_e5_demotion and is_e5_maker)
                    or (cancel_after > 0 and cancel_after <= now)
                )
            ):
                due.append(row)
        return due

    async def cancel_due_maker_fallback_orders(
        self,
        trade_executor: Any,
        *,
        now_ts: float | None = None,
        force_e5_demotion: bool = False,
    ) -> dict[str, Any]:
        due = self.due_maker_fallback_orders(
            now_ts=now_ts,
            force_e5_demotion=force_e5_demotion,
        )
        if not due:
            return {
                "status": "NO_DUE_MAKER_FALLBACKS",
                "due_orders": 0,
                "canceled_orders": 0,
                "filled_orders": 0,
                "errors": [],
            }

        state = self.load_state()
        orders = [row for row in state.get("orders") or [] if isinstance(row, dict)]
        by_order_id = {str(row.get("order_id") or ""): row for row in orders}
        lifecycle_events: list[dict[str, Any]] = []
        event_rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        canceled = 0
        filled = 0
        updated = 0
        for due_order in due:
            order_id = str(due_order.get("order_id") or "")
            row = by_order_id.get(order_id)
            if not row:
                continue
            intent_id = str(row.get("intent_id") or "")
            decision = row.get("trade_decision") if isinstance(row.get("trade_decision"), dict) else {}
            is_e5_maker = str(decision.get("execution_lane") or "") == "e5_maker_first_btc5m_v1"
            try:
                before = await trade_executor.get_order_status(order_id)
                before_status = str(before.get("final_status") or "").lower()
                cancel_sent = False
                if before_status in {"live", "partial", "unknown", "error"}:
                    cancel_sent = bool(await trade_executor.cancel_order(order_id))
                after = await trade_executor.get_order_status(order_id)
                status_probe = after if str(after.get("final_status") or "") not in {"error", "unknown"} else before
                matched_shares = max(float(before.get("size_matched") or 0.0), float(after.get("size_matched") or 0.0))
                avg_price = max(float(before.get("price") or 0.0), float(after.get("price") or 0.0))
                if matched_shares > 0 or str(status_probe.get("final_status") or "").lower() in {"filled", "partial"}:
                    final_status = "FILLED"
                    filled += 1
                    lifecycle_status = "LIVE_MAKER_FILLED"
                    message = "maker order reported filled before or during cancel"
                    filled_usd = matched_shares * (avg_price if avg_price > 0 else float(row.get("limit_price") or 0.0))
                    error_class = ""
                else:
                    final_status = "REJECTED"
                    canceled += 1
                    lifecycle_status = "LIVE_MAKER_CANCELED"
                    message = "maker order canceled before BTC-5m window end without a fill"
                    filled_usd = 0.0
                    error_class = "e5_maker_canceled_unfilled" if is_e5_maker else "maker_fallback_canceled_unfilled"
                cancel_payload = {
                    "status": "CANCELED" if cancel_sent else "STATUS_PROBED",
                    "cancel_sent": cancel_sent,
                    "before": before,
                    "after": after,
                    "matched_shares": round(matched_shares, 6),
                    "avg_price": round(avg_price, 6),
                    "filled_size_usd": round(filled_usd, 6),
                    "updated_at": utc_now_iso(),
                }
                result = row.get("trade_result") if isinstance(row.get("trade_result"), dict) else {}
                tx_hashes = _normalized_tx_hashes_from_status(before, after)
                existing_txs = result.get("tx_hashes") if isinstance(result.get("tx_hashes"), list) else []
                merged_txs = list(dict.fromkeys([str(value).lower() for value in [*existing_txs, *tx_hashes] if value]))
                result = {
                    **result,
                    "final_status": str(status_probe.get("final_status") or "").lower(),
                    "fill_size_shares": round(matched_shares, 6),
                    "filled_size_usd": round(filled_usd, 6),
                    "response_fill_size_shares": round(matched_shares, 6),
                    "response_filled_size_usd": round(filled_usd, 6),
                    "fill_ratio": (
                        round(matched_shares / float(row.get("requested_shares") or 0.0), 6)
                        if float(row.get("requested_shares") or 0.0) > 0
                        else 0.0
                    ),
                    "wallet_copy_maker_cancel": cancel_payload,
                }
                if merged_txs:
                    result["tx_hashes"] = merged_txs
                    row["transaction_hash"] = merged_txs[0]
                    cancel_payload["tx_hashes"] = merged_txs
                if error_class:
                    result["error_class"] = error_class
                row["trade_result"] = result
                row["filled_size_usd"] = round(filled_usd, 6)
                row["fill_size_shares"] = round(matched_shares, 6)
                row["response_filled_size_usd"] = round(filled_usd, 6)
                row["response_fill_size_shares"] = round(matched_shares, 6)
                row["status"] = final_status
                row["final_status"] = final_status
                row["updated_at"] = utc_now_iso()
                lifecycle = _live_lifecycle(lifecycle_status, order_id, intent_id, message, cancel_payload)
                if final_status == "REJECTED":
                    row["raw_clob_reject_payload"] = {
                        "status": lifecycle_status,
                        "error_class": error_class or "maker_fallback_canceled_unfilled",
                        "order_id": order_id,
                        "payload": cancel_payload,
                    }
                row.setdefault("lifecycle", []).append(lifecycle)
                lifecycle_events.append(lifecycle)
                event_rows.append({"event": "wallet_copy_live_order_update", **row})
                event_rows.append({"event": "wallet_copy_live_lifecycle", **lifecycle})
                updated += 1
            except Exception as exc:  # pragma: no cover - defensive around live CLOB edge cases
                errors.append({"order_id": order_id, "error": str(exc)})

        state["orders"] = orders
        state.setdefault("lifecycle_events", []).extend(lifecycle_events)
        self.save_state(state, event_rows)
        return {
            "status": PASS if not errors else ANALYZE,
            "force_e5_demotion": force_e5_demotion,
            "due_orders": len(due),
            "updated_orders": updated,
            "canceled_orders": canceled,
            "filled_orders": filled,
            "errors": errors,
        }


def intent_to_trade_executor_decision(intent: CopyIntent, *, clob_token_ids: list[str]) -> dict[str, Any]:
    token_ids = [str(token_id) for token_id in clob_token_ids if token_id]
    if len(token_ids) < 2:
        raise ValueError("live wallet-copy intent requires YES/NO clob token ids")
    outcome_key = str(intent.outcome or "").strip().lower()
    expected_token = ""
    if outcome_key in {"yes", "up"}:
        expected_token = token_ids[0]
    elif outcome_key in {"no", "down"}:
        expected_token = token_ids[1]
    else:
        raise ValueError(f"wallet-copy intent outcome cannot be mapped to YES/NO token: {intent.outcome}")
    if not str(intent.token_id or "").strip():
        raise ValueError("live wallet-copy intent requires source token_id for CLOB token verification")
    if str(intent.token_id) != str(expected_token):
        raise ValueError("live wallet-copy intent token_id does not match outcome CLOB token mapping")
    if intent.copy_size_usd <= 0 or intent.limit_price <= 0 or intent.shares <= 0:
        raise ValueError("wallet-copy intent has non-positive price or size")
    intent_action = str(intent.action or "BUY").strip().lower()
    if intent_action not in {"buy", "sell"}:
        raise ValueError(f"wallet-copy intent action cannot be executed: {intent.action}")
    decision = {
        "action": intent_action,
        "side": intent.side,
        "limit_price": round(float(intent.limit_price), 4),
        "size_usd": round(float(intent.copy_size_usd), 6),
        "size_shares": round(float(intent.shares), 6),
        "market_id": intent.condition_id,
        "condition_id": intent.condition_id,
        "clob_token_ids": token_ids,
        # Fill-and-kill keeps wallet-copy orders non-resting like FOK, but
        # records partial live fills instead of killing the whole CopyIntent
        # whenever the full size is not immediately available at the limit.
        "order_type": "FAK",
        "post_only_strict": False,
        "execution_lane": "live_guard",
        "strategy_reason": "wallet_copy_intent",
        "strategy_family": intent.strategy_family,
        "wallet_copy": intent.asdict(),
    }
    drift_buffer = intent.metadata.get("drift_buffer") if isinstance(intent.metadata, dict) else None
    if isinstance(drift_buffer, dict):
        decision["wallet_copy_drift_buffer"] = drift_buffer
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    if intent_action == "buy" and not (
        ENTRY_PRICE_BAND_01A_MIN
        <= float(intent.limit_price)
        < ENTRY_PRICE_BAND_01A_MAX_EXCLUSIVE
    ):
        subband_holdout = load_json("data/research/taker_price_subband_holdout_latest.json", default={})
        if not isinstance(subband_holdout, dict):
            subband_holdout = {}
        decision.update(
            {
                "order_type": "POLICY_TERMINAL",
                "post_only_strict": False,
                "execution_lane": "copyintent_policy",
                "strategy_reason": ENTRY_PRICE_BAND_CLOSED_REASON,
                "skip_if_below_min_shares": True,
                "allow_passive_precision_below_min_shares": False,
                "terminal_stage": ENTRY_PRICE_BAND_CLOSED_REASON,
                "entry_price_band_closed_negative_holdout": {
                    "schema_version": 1,
                    "flow_stage": "LIVE/MEASURE/DEFEND",
                    "status": "SKIPPED",
                    "reason": ENTRY_PRICE_BAND_CLOSED_REASON,
                    "entry_price": round(float(intent.limit_price), 6),
                    "allowed_min_inclusive": ENTRY_PRICE_BAND_01A_MIN,
                    "allowed_max_exclusive": ENTRY_PRICE_BAND_01A_MAX_EXCLUSIVE,
                    "allowed_band": "01a_25_32",
                    "closed_bands": ["01b_32_40", "01c_40_50"],
                    "holdout_generated_at": subband_holdout.get("generated_at"),
                    "focus_verdict": subband_holdout.get("focus_verdict"),
                    "live_orders_allowed": False,
                },
            }
        )
    split_sell = (
        metadata.get("complete_set_split_sell")
        if isinstance(metadata.get("complete_set_split_sell"), dict)
        else {}
    )
    if intent_action == "sell" and split_sell:
        if (
            split_sell.get("ctf_split_confirmed_before_submit") is not True
            or abs(float(intent.shares) - 1.0) > 1e-9
        ):
            raise ValueError("complete-set SELL requires a confirmed exact $1 CTF split and one share per leg")
        decision.update(
            {
                "order_type": "FAK",
                "post_only_strict": False,
                "strategy_reason": "bounded_hedge_orphan_exit",
                "allow_sell_below_min_shares": True,
                "execution_lane": "live_guard",
                "complete_set_split_sell": dict(split_sell),
            }
        )
    copy_model = str(metadata.get("copy_model") or "").strip()
    inventory_book_gate = (
        metadata.get("inventory_best_ask_gate")
        if isinstance(metadata.get("inventory_best_ask_gate"), dict)
        else {}
    )
    precision_passive = (
        metadata.get("precision_requires_passive_source")
        if isinstance(metadata.get("precision_requires_passive_source"), dict)
        else {}
    )
    passive_at_source = (
        str(inventory_book_gate.get("execution_path") or "") == "direct_post_only_gtc_at_source"
        or str(precision_passive.get("execution_path") or "") == "direct_post_only_gtc_at_source"
    )
    if passive_at_source and str(decision.get("execution_lane") or "") != "copyintent_policy":
        passive_capsule = precision_passive or inventory_book_gate
        passive_holdout = load_json("data/research/passive_at_source_holdout_latest.json", default={})
        if not isinstance(passive_holdout, dict):
            passive_holdout = {}
        passive_price = float(passive_capsule.get("passive_price") or 0.0)
        original_source_price = float(passive_capsule.get("original_source_price") or 0.0)
        buffered_limit_price = float(
            passive_capsule.get("buffered_limit_price") or intent.limit_price
        )
        max_copy_price = float(passive_capsule.get("max_copy_price") or buffered_limit_price)
        if (
            passive_price <= 0
            or abs(float(intent.limit_price) - passive_price) > 1e-9
            or passive_price > original_source_price + 1e-9
            or passive_price > buffered_limit_price + 1e-9
            or passive_price > max_copy_price + 1e-9
        ):
            raise ValueError("passive-at-source intent price violates protected parity capsule")
        decision.update(
            {
                "order_type": "POLICY_TERMINAL",
                "post_only_strict": False,
                "execution_lane": "copyintent_policy",
                "strategy_reason": "passive_at_source_lane_closed",
                "skip_if_below_min_shares": True,
                "allow_passive_precision_below_min_shares": False,
                "terminal_stage": "passive_at_source_lane_closed",
                "wallet_copy_passive_at_source_closed": {
                    "schema_version": 1,
                    "flow_stage": "LIVE/MEASURE/DEFEND",
                    "status": "SKIPPED",
                    "reason": "passive_at_source_lane_closed",
                    "trigger": precision_passive.get("status") or "inventory_best_ask_gate",
                    "holdout_generated_at": passive_holdout.get("generated_at"),
                    "holdout_verdict": passive_holdout.get("verdict"),
                    "holdout_sample_gate": passive_holdout.get("sample_gate"),
                    "original_source_price": round(original_source_price, 6),
                    "buffered_limit_price": round(buffered_limit_price, 6),
                    "max_copy_price": round(max_copy_price, 6),
                    "best_ask": round(float(passive_capsule.get("best_ask") or 0.0), 6),
                    "passive_price": round(passive_price, 6),
                    "passive_notional_usd": round(float(intent.copy_size_usd), 6),
                    "live_orders_allowed": False,
                    "trade_executor_backstop_retained": True,
                },
            }
        )
    e5_maker = (
        metadata.get("e5_maker_first_btc5m_v1")
        if isinstance(metadata.get("e5_maker_first_btc5m_v1"), dict)
        else {}
    )
    if (
        copy_model == "maker_first_btc5m"
        and e5_maker.get("enforced_no_fallback_book") is True
        and str((e5_maker.get("top_of_book") or {}).get("book_hash") or "")
    ):
        expected_notional = round(5.0 * float(intent.limit_price), 6)
        if (
            abs(float(intent.shares) - 5.0) > 1e-9
            or abs(float(intent.copy_size_usd) - expected_notional) > 1e-9
            or str(intent.sizing_policy_id or "") != "fixed_shares_5"
            or expected_notional > 2.50
        ):
            raise ValueError("E5 maker intent must be exact fixed_shares_5 with size_usd == 5 * limit_price")
        decision.update(
            {
                "order_type": "GTC",
                "post_only_strict": True,
                "execution_lane": "e5_maker_first_btc5m_v1",
                "strategy_reason": "e5_maker_first_live_guard",
                "skip_if_below_min_shares": True,
                "e5_maker_first_live": {
                    "lane": "e5_maker_first_btc5m_v1",
                    "source_tag": "E5_MAKER_FIRST",
                    "book_hash": str(e5_maker["top_of_book"]["book_hash"]),
                    "book_evidence_mode": str(e5_maker.get("book_evidence_mode") or ""),
                    "direct_fallback_allowed": False,
                },
            }
        )
    if intent.source_wallet == "INVENTORY" or "inventory" in intent.strategy_family or copy_model in {"inventory", "drip"}:
        decision["adaptive_mode"] = "wallet_copy_inventory"
    if copy_model == "drip":
        decision["adaptive_mode"] = "wallet_copy_drip_inventory"
    return decision


class CopyExecutionAdapter:
    def __init__(
        self,
        *,
        gate: ExecutionGate | None = None,
        paper_engine: PaperWalletCopyEngine | None = None,
        live_lifecycle: LiveWalletCopyLifecycle | None = None,
        trade_executor: Any = None,
        enable_maker_fallback: bool = False,
        per_window_fill_cap: int = 0,
    ):
        self.gate = gate or ExecutionGate()
        self.paper_engine = paper_engine or PaperWalletCopyEngine()
        self.live_lifecycle = live_lifecycle or LiveWalletCopyLifecycle()
        self.trade_executor = trade_executor
        self.enable_maker_fallback = bool(enable_maker_fallback)
        self.per_window_fill_cap = max(0, int(per_window_fill_cap))

    async def _submit_maker_fallback(
        self,
        *,
        live_intent: CopyIntent,
        decision: dict[str, Any],
        capsule: dict[str, Any],
        fak_result: dict[str, Any],
        intent_built_ts: float,
    ) -> dict[str, Any]:
        order_started = time.perf_counter()
        window_close_ts = _btc_5m_window_close_ts(live_intent.market_slug)
        now_ts = time.time()
        if window_close_ts is None:
            return {
                "status": "SKIPPED",
                "reason": "maker_fallback_window_close_unknown",
                "parent_order_id": fak_result.get("order_id"),
            }
        if window_close_ts <= now_ts:
            return {
                "status": "SKIPPED",
                "reason": "maker_fallback_window_already_closed",
                "parent_order_id": fak_result.get("order_id"),
                "window_close_ts": window_close_ts,
            }
        fallback_meta = {
            "schema_version": 1,
            "flow_stage": "LIVE",
            "status": "SUBMIT_ATTEMPTED",
            "trigger": "fak_no_match",
            "parent_order_id": str(fak_result.get("order_id") or ""),
            "parent_error_class": str(fak_result.get("error_class") or ""),
            "copied_price": round(float(live_intent.limit_price), 6),
            "same_size_shares": round(float(live_intent.shares), 6),
            "same_size_usd": round(float(live_intent.copy_size_usd), 6),
            "window_close_ts": round(float(window_close_ts), 6),
            "cancel_after_ts": round(float(window_close_ts), 6),
            "cancel_policy": "cancel_at_btc_5m_window_end",
        }
        fallback_decision = {
            **decision,
            "order_type": "GTC",
            "post_only_strict": True,
            "strategy_reason": "wallet_copy_fak_miss_maker_fallback",
            "skip_if_below_min_shares": False,
            "wallet_copy_maker_fallback": fallback_meta,
        }
        prepare_order_s = _wall_seconds(order_started)
        submit_sent_ts = time.time()
        submit_started = time.perf_counter()
        fallback_result = self.trade_executor.execute_trade(fallback_decision, size_usd=live_intent.copy_size_usd)
        if inspect.isawaitable(fallback_result):
            fallback_result = await fallback_result
        submit_ended = time.perf_counter()
        exchange_ack_ts = time.time()
        fallback_result = dict(fallback_result or {})
        fallback_latency_budget = _wallet_copy_latency_budget(
            live_intent,
            intent_built_ts=intent_built_ts,
            submit_sent_ts=submit_sent_ts,
            exchange_ack_ts=exchange_ack_ts,
        )
        fallback_meta["status"] = "SUBMITTED" if str(fallback_result.get("status") or "") == "submitted" else "SKIPPED"
        fallback_execute_profile = _profile_live_order_execution(
            intent=live_intent,
            role="maker",
            order_started=order_started,
            prepare_order_s=prepare_order_s,
            submit_started=submit_started,
            submit_ended=submit_ended,
            result=fallback_result,
        )
        fallback_result.update(
            {
                "wallet_copy_latency_budget": fallback_latency_budget,
                "execution_role": "maker",
                "maker": True,
                "wallet_copy_maker_fallback": fallback_meta,
                "fallback_parent_result": {
                    key: fak_result.get(key)
                    for key in (
                        "status",
                        "final_status",
                        "order_id",
                        "order_type",
                        "error_class",
                        "entry_price",
                        "size_usd",
                    )
                },
                "wallet_copy_execute_profile": fallback_execute_profile,
            }
        )
        self.live_lifecycle.record_result(
            intent=live_intent,
            trade_decision=fallback_decision,
            result=fallback_result,
            parity_capsule=capsule,
            latency_budget=fallback_latency_budget,
        )
        return fallback_result

    async def execute_async(
        self,
        intents: list[CopyIntent],
        *,
        clob_token_ids_by_condition: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        if self.gate.mode == "paper":
            return self.paper_engine.apply_intents(intents)
        self.gate.assert_live_allowed(intents)
        if self.trade_executor is None:
            raise ValueError("live wallet-copy execution requires a TradeExecutor instance")
        results = []
        parity_capsules = []
        execute_profiles = []
        token_map = clob_token_ids_by_condition or {}
        btc_5m_window_occupancy = (
            _occupied_btc_5m_market_counts(self.live_lifecycle.load_state())
            if self.per_window_fill_cap > 0
            else {}
        )
        for intent in intents:
            order_started = time.perf_counter()
            capsule = build_copy_intent_parity_capsule(
                _paper_equivalent_for_parity(intent),
                clob_token_ids=token_map.get(intent.condition_id, []),
                operator_approval_id=self.gate.admission_snapshot.operator_approval_id
                if self.gate.admission_snapshot
                else "",
            )
            if capsule.get("status") != PASS:
                raise ValueError(f"live wallet-copy parity proof failed: {capsule.get('blockers')}")
            live_intent = CopyIntent.from_dict(capsule["live_intent"])
            decision = capsule["live_trade_decision"]
            maker_first_live = (
                str(decision.get("order_type") or "").upper() == "GTC"
                and bool(decision.get("post_only_strict"))
            )
            execution_role = "maker" if maker_first_live else "taker"
            intent_built_ts = time.time()
            prepare_order_s = _wall_seconds(order_started)
            parity_capsules.append(
                {
                    key: value
                    for key, value in capsule.items()
                    if key not in {"paper_intent", "live_intent", "live_trade_decision"}
                }
            )
            terminal_reason = str(decision.get("strategy_reason") or "")
            if (
                str(decision.get("execution_lane") or "") == "copyintent_policy"
                and str(decision.get("order_type") or "").upper() == "POLICY_TERMINAL"
                and terminal_reason in PRE_SUBMIT_REFUSAL_CLASSES
            ):
                exchange_ack_ts = time.time()
                latency_budget = _wallet_copy_latency_budget(
                    live_intent,
                    intent_built_ts=intent_built_ts,
                    submit_sent_ts=exchange_ack_ts,
                    exchange_ack_ts=exchange_ack_ts,
                )
                result = {
                    "status": "SKIPPED",
                    "final_status": "copyintent_policy",
                    "reason": terminal_reason,
                    "error_class": terminal_reason,
                    "order_id": "",
                    "market_slug": live_intent.market_slug,
                    "execution_role": execution_role,
                    "maker": False,
                    "wallet_copy_latency_budget": latency_budget,
                    "wallet_copy_passive_at_source_closed": decision.get(
                        "wallet_copy_passive_at_source_closed"
                    ),
                    "entry_price_band_closed_negative_holdout": decision.get(
                        "entry_price_band_closed_negative_holdout"
                    ),
                }
                assert result["error_class"] in PRE_SUBMIT_REFUSAL_CLASSES
                self.live_lifecycle.record_result(
                    intent=live_intent,
                    trade_decision=decision,
                    result=result,
                    parity_capsule=capsule,
                    latency_budget=latency_budget,
                )
                results.append(result)
                continue
            occupied_before = int(btc_5m_window_occupancy.get(live_intent.market_slug, 0))
            if (
                self.per_window_fill_cap > 0
                and live_intent.market_slug.startswith("btc-updown-5m-")
                and occupied_before >= self.per_window_fill_cap
            ):
                exchange_ack_ts = time.time()
                latency_budget = _wallet_copy_latency_budget(
                    live_intent,
                    intent_built_ts=intent_built_ts,
                    submit_sent_ts=exchange_ack_ts,
                    exchange_ack_ts=exchange_ack_ts,
                )
                result = {
                    "status": "SKIPPED",
                    "final_status": "window_fill_cap",
                    "reason": "window_fill_cap",
                    "error_class": "window_fill_cap",
                    "order_id": stable_id(
                        "skip",
                        {
                            "intent_id": live_intent.intent_id,
                            "market_slug": live_intent.market_slug,
                            "reason": "window_fill_cap",
                        },
                    ),
                    "market_slug": live_intent.market_slug,
                    "wallet_copy_latency_budget": latency_budget,
                    "wallet_copy_window_fill_cap": {
                        "schema_version": 1,
                        "flow_stage": "LIVE/DEFEND",
                        "status": "SKIPPED",
                        "reason": "window_fill_cap",
                        "cap_filled_or_submitted_orders_per_btc_5m_window": self.per_window_fill_cap,
                        "occupied_before": occupied_before,
                        "market_slug": live_intent.market_slug,
                    },
                }
                self.live_lifecycle.record_result(
                    intent=live_intent,
                    trade_decision=decision,
                    result=result,
                    parity_capsule=capsule,
                    latency_budget=latency_budget,
                )
                results.append(result)
                continue
            try:
                submit_sent_ts = time.time()
                submit_started = time.perf_counter()
                result = self.trade_executor.execute_trade(decision, size_usd=intent.copy_size_usd)
                if inspect.isawaitable(result):
                    result = await result
                submit_ended = time.perf_counter()
                exchange_ack_ts = time.time()
                result = dict(result or {})
                latency_budget = _wallet_copy_latency_budget(
                    live_intent,
                    intent_built_ts=intent_built_ts,
                    submit_sent_ts=submit_sent_ts,
                    exchange_ack_ts=exchange_ack_ts,
                )
                result["wallet_copy_latency_budget"] = latency_budget
                result["execution_role"] = execution_role
                result["maker"] = maker_first_live
                execute_profile = _profile_live_order_execution(
                    intent=live_intent,
                    role=execution_role,
                    order_started=order_started,
                    prepare_order_s=prepare_order_s,
                    submit_started=submit_started,
                    submit_ended=submit_ended,
                    result=result,
                )
            except Exception as exc:
                exchange_ack_ts = time.time()
                submit_ended = time.perf_counter()
                latency_budget = _wallet_copy_latency_budget(
                    live_intent,
                    intent_built_ts=intent_built_ts,
                    submit_sent_ts=locals().get("submit_sent_ts", exchange_ack_ts),
                    exchange_ack_ts=exchange_ack_ts,
                )
                failure = {
                    "status": "error",
                    "final_status": "execution_exception",
                    "order_id": "",
                    "error": str(exc),
                    "wallet_copy_latency_budget": latency_budget,
                }
                _profile_live_order_execution(
                    intent=live_intent,
                    role=execution_role,
                    order_started=order_started,
                    prepare_order_s=prepare_order_s,
                    submit_started=locals().get("submit_started", order_started),
                    submit_ended=submit_ended,
                    result=failure,
                )
                self.live_lifecycle.record_result(
                    intent=live_intent,
                    trade_decision=decision,
                    result=failure,
                    parity_capsule=capsule,
                    latency_budget=latency_budget,
                )
                raise
            self.live_lifecycle.record_result(
                intent=live_intent,
                trade_decision=decision,
                result=result,
                parity_capsule=capsule,
                latency_budget=latency_budget,
            )
            execute_profiles.append(execute_profile)
            results.append(result)
            if (
                self.per_window_fill_cap > 0
                and live_intent.market_slug.startswith("btc-updown-5m-")
                and _live_final_status(result) in {"FILLED", "SUBMITTED"}
            ):
                btc_5m_window_occupancy[live_intent.market_slug] = (
                    int(btc_5m_window_occupancy.get(live_intent.market_slug, 0)) + 1
                )
            if self.enable_maker_fallback and _is_fak_no_match_result(result):
                fallback_result = await self._submit_maker_fallback(
                    live_intent=live_intent,
                    decision=decision,
                    capsule=capsule,
                    fak_result=result,
                    intent_built_ts=intent_built_ts,
                )
                fallback_profile = fallback_result.get("wallet_copy_execute_profile")
                if isinstance(fallback_profile, dict):
                    execute_profiles.append(fallback_profile)
                results.append(fallback_result)
                if (
                    self.per_window_fill_cap > 0
                    and live_intent.market_slug.startswith("btc-updown-5m-")
                    and _live_final_status(fallback_result) in {"FILLED", "SUBMITTED"}
                ):
                    btc_5m_window_occupancy[live_intent.market_slug] = (
                        int(btc_5m_window_occupancy.get(live_intent.market_slug, 0)) + 1
                    )
        return {
            "schema_version": 1,
            "kind": "wallet_copy_live_execution_results",
            "paper_only": False,
            "live_orders_allowed": True,
            "paper_live_parity_capsules": parity_capsules,
            "execute_live_profile": _summarize_live_execute_profiles(execute_profiles),
            "live_lifecycle_state": self.live_lifecycle.config.state_path,
            "live_lifecycle_event_log": self.live_lifecycle.config.event_log_path,
            "results": results,
        }

    def execute(
        self,
        intents: list[CopyIntent],
        *,
        clob_token_ids_by_condition: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        if self.gate.mode == "paper":
            return self.paper_engine.apply_intents(intents)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.execute_async(intents, clob_token_ids_by_condition=clob_token_ids_by_condition)
            )
        raise RuntimeError("live wallet-copy execution inside an event loop must use execute_async()")
