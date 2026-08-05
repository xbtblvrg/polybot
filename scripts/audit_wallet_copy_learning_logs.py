#!/usr/bin/env python3
"""Audit whether wallet-copy logs are rich enough for autonomous learning."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.mission import mission_contract, mission_contract_check
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json
from src.wallet_copy.tactic_performance import score_tactic_replay_pnl

try:  # pragma: no cover - availability depends on the local Python install.
    import certifi
except Exception:  # pragma: no cover
    certifi = None


_DIRECT_SOURCE_SSL_CONTEXT: ssl.SSLContext | None = None


def _direct_source_ssl_context() -> ssl.SSLContext | None:
    global _DIRECT_SOURCE_SSL_CONTEXT
    if _DIRECT_SOURCE_SSL_CONTEXT is not None:
        return _DIRECT_SOURCE_SSL_CONTEXT
    if certifi is None:
        return None
    _DIRECT_SOURCE_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
    return _DIRECT_SOURCE_SSL_CONTEXT


def _arg(args: argparse.Namespace, name: str, default: Any) -> Any:
    return getattr(args, name, default)


def _jsonl_count(path: str | Path) -> int:
    target = Path(path)
    if not target.exists():
        return 0
    with target.open(encoding="utf-8", errors="ignore") as handle:
        return sum(1 for line in handle if line.strip())


def _file_size_bytes(path: str | Path) -> int:
    target = Path(path)
    if not target.exists():
        return 0
    try:
        return target.stat().st_size
    except OSError:
        return 0


def _jsonl_rows(
    path: str | Path,
    *,
    max_lines: int = 10_000,
    max_bytes: int = 8 * 1024 * 1024,
) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        size = target.stat().st_size
        with target.open("rb") as handle:
            offset = max(0, size - max(0, int(max_bytes)))
            handle.seek(offset)
            chunk = handle.read()
        if offset > 0:
            _, _, chunk = chunk.partition(b"\n")
        lines = chunk.decode("utf-8", errors="ignore").splitlines()[-max_lines:]
    except OSError:
        return []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _get_nested(mapping: dict[str, Any], *keys: str) -> Any:
    value: Any = mapping
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _wallet_report_summary(
    wallet_reports: list[dict[str, Any]],
    *,
    event_log_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "wallet_reports": len(wallet_reports),
        "raw_rows": sum(int(row.get("raw_rows") or 0) for row in wallet_reports),
        "events_seen": sum(int(row.get("events_seen") or 0) for row in wallet_reports),
        "new_events": sum(int(row.get("new_events") or 0) for row in wallet_reports),
        "seen_duplicate_events": sum(int(row.get("seen_duplicate_events") or 0) for row in wallet_reports),
        "pending_duplicate_events": sum(int(row.get("pending_duplicate_events") or 0) for row in wallet_reports),
        "failed_retry_deferred_events": sum(
            int(row.get("failed_retry_deferred_events") or 0) for row in wallet_reports
        ),
        "wallets_with_events": sum(1 for row in wallet_reports if int(row.get("events_seen") or 0) > 0),
        "wallets_with_new_events": sum(1 for row in wallet_reports if int(row.get("new_events") or 0) > 0),
        "dedupe_exhausted_wallets": sum(1 for row in wallet_reports if bool(row.get("dedupe_exhausted"))),
    }
    if event_log_summary:
        summary["event_log_tracked_moves"] = int(event_log_summary.get("tracked_move_events") or 0)
        summary["event_log_source_buy_events"] = int(event_log_summary.get("source_buy_events") or 0)
    return summary


def _parse_clob_token_ids(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(row) for row in raw if row is not None]
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        return _parse_clob_token_ids(parsed)
    return []


def _http_json_get(url: str, *, timeout_s: float) -> tuple[dict[str, Any], Any]:
    started = time.perf_counter()
    request = urllib.request.Request(
        url,
        headers={
            "accept": "application/json",
            "user-agent": "polymarket-agent-wallet-copy-audit/1.0",
        },
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=timeout_s,
            context=_direct_source_ssl_context(),
        ) as response:
            body = response.read(2_000_000)
            status_code = int(getattr(response, "status", 0) or 0)
    except urllib.error.HTTPError as exc:
        return (
            {
                "status": "FAIL",
                "http_status": int(exc.code or 0),
                "duration_s": round(time.perf_counter() - started, 6),
                "error": f"http_error:{exc.code}",
                "error_detail": str(exc),
            },
            None,
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return (
            {
                "status": "FAIL",
                "http_status": None,
                "duration_s": round(time.perf_counter() - started, 6),
                "error": type(exc).__name__,
                "error_detail": str(exc),
            },
            None,
        )
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return (
            {
                "status": "FAIL",
                "http_status": status_code,
                "duration_s": round(time.perf_counter() - started, 6),
                "error": type(exc).__name__,
                "error_detail": str(exc),
            },
            None,
        )
    return (
        {
            "status": "PASS" if status_code == 200 else "FAIL",
            "http_status": status_code,
            "duration_s": round(time.perf_counter() - started, 6),
            "error": None,
        },
        payload,
    )


def _http_json_get_with_retries(
    url: str,
    *,
    timeout_s: float,
    retries: int,
) -> tuple[dict[str, Any], Any]:
    attempts: list[dict[str, Any]] = []
    last_meta: dict[str, Any] = {}
    last_payload: Any = None
    total_started = time.perf_counter()
    for attempt in range(max(1, retries + 1)):
        meta, payload = _http_json_get(url, timeout_s=timeout_s)
        attempts.append(
            {
                "attempt": attempt + 1,
                "status": meta.get("status"),
                "http_status": meta.get("http_status"),
                "duration_s": meta.get("duration_s"),
                "error": meta.get("error"),
                "error_detail": meta.get("error_detail"),
            }
        )
        last_meta = meta
        last_payload = payload
        if meta.get("status") == "PASS":
            break
        if attempt < retries:
            time.sleep(min(0.25 * (attempt + 1), 1.0))
    last_meta = dict(last_meta)
    last_meta["attempt_count"] = len(attempts)
    last_meta["attempts"] = attempts
    last_meta["total_duration_s"] = round(time.perf_counter() - total_started, 6)
    return last_meta, last_payload


def _latest_direct_source_probe_event(path: str | Path, *, max_lines: int = 2_000) -> dict[str, Any]:
    for row in reversed(_jsonl_rows(path, max_lines=max_lines)):
        if row.get("event") != "wallet_copy_live_tracked_move":
            continue
        score = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
        if str(score.get("wallet_action") or row.get("action") or "").upper() != "BUY":
            continue
        if str(score.get("copy_status") or "") != "COPIED_FILLED":
            continue
        fill_source = str(score.get("fill_source") or "")
        if "clob" not in fill_source.lower():
            continue
        wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
        raw_event = wallet_event.get("raw") if isinstance(wallet_event.get("raw"), dict) else {}
        clob_book = _get_nested(row, "tracking_evidence", "clob_book")
        clob_book = clob_book if isinstance(clob_book, dict) else {}
        token_id = str(
            score.get("token_id")
            or wallet_event.get("token_id")
            or raw_event.get("asset")
            or clob_book.get("asset_id")
            or ""
        )
        source_wallet = str(
            score.get("source_wallet")
            or wallet_event.get("source_wallet")
            or raw_event.get("proxyWallet")
            or _get_nested(row, "tracking_evidence", "wallet_api", "requested_wallet")
            or ""
        ).lower()
        market_slug = str(
            score.get("market_slug")
            or wallet_event.get("market_slug")
            or wallet_event.get("event_slug")
            or raw_event.get("eventSlug")
            or row.get("market_slug")
            or ""
        )
        condition_id = str(
            score.get("condition_id")
            or wallet_event.get("condition_id")
            or wallet_event.get("market_id")
            or raw_event.get("conditionId")
            or clob_book.get("book_market")
            or ""
        )
        if not token_id or not (source_wallet or market_slug or condition_id):
            continue
        return {
            "source_event_id": score.get("source_event_id") or row.get("source_event_id") or wallet_event.get("event_id"),
            "source_wallet": source_wallet,
            "market_slug": market_slug,
            "condition_id": condition_id,
            "token_id": token_id,
            "tx_hash": row.get("tx_hash") or wallet_event.get("transaction_hash") or raw_event.get("transactionHash"),
            "event_ts": score.get("source_event_ts") or wallet_event.get("event_ts") or raw_event.get("timestamp"),
            "source_price": score.get("source_price") or wallet_event.get("price") or raw_event.get("price"),
            "outcome": score.get("outcome") or wallet_event.get("outcome") or raw_event.get("outcome"),
            "persisted_book_hash": clob_book.get("book_hash"),
            "persisted_book_market": clob_book.get("book_market"),
            "persisted_book_timestamp": clob_book.get("book_timestamp"),
        }
    return {}


def _direct_data_source_spot_check(
    active_hotlane_event_log: str | Path,
    *,
    enabled: bool,
    timeout_s: float,
    retries: int = 0,
) -> dict[str, Any]:
    if not enabled:
        return {
            "status": "SKIPPED",
            "enabled": False,
            "blockers": ["direct_source_spot_check_disabled"],
            "reason": "disabled for unit callers; CLI audit enables this by default",
        }
    probe = _latest_direct_source_probe_event(active_hotlane_event_log)
    if not probe:
        return {
            "status": "WATCH",
            "enabled": True,
            "blockers": ["no_clob_backed_copied_filled_buy_probe_event"],
            "reason": "direct source verification needs a recent CLOB-backed COPIED_FILLED BUY event",
        }

    blockers: list[str] = []
    result: dict[str, Any] = {
        "status": "WATCH",
        "enabled": True,
        "probe": probe,
        "blockers": blockers,
    }

    source_wallet = str(probe.get("source_wallet") or "").lower()
    market_slug = str(probe.get("market_slug") or "")
    condition_id = str(probe.get("condition_id") or "")
    token_id = str(probe.get("token_id") or "")
    tx_hash = str(probe.get("tx_hash") or "").lower()

    data_url = (
        "https://data-api.polymarket.com/activity?"
        + urllib.parse.urlencode({"user": source_wallet, "limit": 20})
    )
    data_meta, data_payload = _http_json_get_with_retries(data_url, timeout_s=timeout_s, retries=retries)
    data_rows = data_payload if isinstance(data_payload, list) else []
    data_wallet_match = any(
        str(row.get("proxyWallet") or "").lower() == source_wallet
        for row in data_rows
        if isinstance(row, dict)
    )
    data_tx_match = bool(
        tx_hash
        and any(
            str(row.get("transactionHash") or "").lower() == tx_hash
            for row in data_rows
            if isinstance(row, dict)
        )
    )
    if data_meta.get("status") != "PASS" or not data_wallet_match:
        blockers.append("data_api_activity_not_truth_confirmed")
    result["data_api_activity"] = {
        **data_meta,
        "url": data_url,
        "rows": len(data_rows),
        "wallet_identity_match": data_wallet_match,
        "tx_match_in_returned_rows": data_tx_match,
    }

    gamma_url = (
        "https://gamma-api.polymarket.com/markets?"
        + urllib.parse.urlencode({"slug": market_slug, "limit": 1})
    )
    gamma_meta, gamma_payload = _http_json_get_with_retries(gamma_url, timeout_s=timeout_s, retries=retries)
    gamma_rows = gamma_payload if isinstance(gamma_payload, list) else []
    gamma_market = gamma_rows[0] if gamma_rows and isinstance(gamma_rows[0], dict) else {}
    gamma_tokens = _parse_clob_token_ids(gamma_market.get("clobTokenIds"))
    gamma_slug_match = str(gamma_market.get("slug") or "") == market_slug
    gamma_condition_match = (not condition_id) or str(gamma_market.get("conditionId") or "").lower() == condition_id.lower()
    gamma_token_match = (not token_id) or token_id in gamma_tokens
    if gamma_meta.get("status") != "PASS" or not (gamma_slug_match and gamma_condition_match and gamma_token_match):
        blockers.append("gamma_market_not_truth_confirmed")
    result["gamma_market"] = {
        **gamma_meta,
        "url": gamma_url,
        "rows": len(gamma_rows),
        "slug_match": gamma_slug_match,
        "condition_match": gamma_condition_match,
        "token_match": gamma_token_match,
        "clob_token_count": len(gamma_tokens),
    }

    clob_url = "https://clob.polymarket.com/book?" + urllib.parse.urlencode({"token_id": token_id})
    clob_meta, clob_payload = _http_json_get_with_retries(clob_url, timeout_s=timeout_s, retries=retries)
    clob_book = clob_payload if isinstance(clob_payload, dict) else {}
    clob_token_match = str(clob_book.get("asset_id") or "") == token_id
    clob_condition_match = (not condition_id) or str(clob_book.get("market") or "").lower() == condition_id.lower()
    clob_has_book = bool(clob_book.get("hash") and (clob_book.get("bids") is not None) and (clob_book.get("asks") is not None))
    if clob_meta.get("status") != "PASS" or not (clob_token_match and clob_condition_match and clob_has_book):
        blockers.append("clob_book_not_truth_confirmed")
    result["clob_book"] = {
        **clob_meta,
        "url": clob_url,
        "asset_id_match": clob_token_match,
        "condition_match": clob_condition_match,
        "book_hash": clob_book.get("hash"),
        "bids": len(clob_book.get("bids") or []),
        "asks": len(clob_book.get("asks") or []),
        "has_book": clob_has_book,
    }

    result["status"] = "PASS" if not blockers else "WATCH"
    result["reason"] = (
        "active hot-lane evidence was cross-checked against direct Data API activity, "
        "Gamma market metadata, and CLOB book endpoints"
    )
    return result


def _bump(counter: dict[str, int], key: Any) -> None:
    text = str(key or "UNKNOWN")
    counter[text] = counter.get(text, 0) + 1


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return round(ordered[index], 6)


def _live_event_log_summary(path: str | Path) -> dict[str, Any]:
    """Summarize per-poll live tracker evidence persisted in JSONL.

    The live tracking state stores the latest poll summary. Longer tracker
    runs can therefore end with an empty final poll even though earlier polls
    observed and scored wallet moves. The JSONL is the durable learning source.
    """

    rows = _jsonl_rows(path)
    event_ages: list[float] = []
    copyability_reasons: dict[str, int] = {}
    missed_reasons: dict[str, int] = {}
    clob_statuses: dict[str, int] = {}
    mirror_statuses: dict[str, int] = {}
    filter_policies: dict[str, int] = {}
    summary = {
        "tracked_move_events": 0,
        "copy_efficiency_rows": 0,
        "source_buy_events": 0,
        "filtered_buy_events": 0,
        "copyability_filtered_buy_events": 0,
        "profit_filtered_buy_events": 0,
        "clob_book_ok_events": 0,
        "filled_buy_copy_events": 0,
        "clob_filled_buy_copy_events": 0,
        "rejected_buy_copy_events": 0,
        "missed_buy_copy_events": 0,
    }
    for row in rows:
        if row.get("event") == "wallet_copy_live_tracked_move":
            summary["tracked_move_events"] += 1
        score = row.get("copy_efficiency") if isinstance(row.get("copy_efficiency"), dict) else {}
        if not score:
            continue
        summary["copy_efficiency_rows"] += 1
        wallet_action = str(score.get("wallet_action") or "").upper()
        if wallet_action == "BUY":
            summary["source_buy_events"] += 1
        try:
            event_age = float(score.get("event_age_s"))
            event_ages.append(event_age)
        except (TypeError, ValueError):
            pass
        copy_status = str(score.get("copy_status") or "")
        fill_source = str(score.get("fill_source") or "")
        filter_policy = str(score.get("filter_policy") or "")
        mirror_status = str(score.get("mirror_status") or "")
        if filter_policy:
            _bump(filter_policies, filter_policy)
        if mirror_status:
            _bump(mirror_statuses, mirror_status)
        if wallet_action == "BUY" and copy_status == "FILTERED":
            summary["filtered_buy_events"] += 1
        if wallet_action == "BUY" and filter_policy == "copyability":
            summary["copyability_filtered_buy_events"] += 1
        if wallet_action == "BUY" and filter_policy == "profit_policy":
            summary["profit_filtered_buy_events"] += 1
        if wallet_action == "BUY" and copy_status in {"COPIED_FILLED", "FILLED", "MIRRORED"}:
            summary["filled_buy_copy_events"] += 1
        if wallet_action == "BUY" and fill_source.startswith("clob"):
            summary["clob_filled_buy_copy_events"] += 1
        if wallet_action == "BUY" and score.get("reject_reason"):
            summary["rejected_buy_copy_events"] += 1
        if wallet_action == "BUY" and copy_status in {"MISSED", "COPY_REJECTED"} and score.get("missed_copy_reason"):
            summary["missed_buy_copy_events"] += 1
            _bump(missed_reasons, score.get("missed_copy_reason"))
        copyability = row.get("copyability") if isinstance(row.get("copyability"), dict) else {}
        details = copyability.get("details") if isinstance(copyability.get("details"), dict) else {}
        reason = copyability.get("reason") or score.get("copyability_reason")
        if reason:
            _bump(copyability_reasons, reason)
        score_copyability_details = (
            score.get("copyability_details") if isinstance(score.get("copyability_details"), dict) else {}
        )
        clob_status = (
            details.get("clob_book_status")
            or score_copyability_details.get("clob_book_status")
            or score.get("clob_book_status")
        )
        if clob_status:
            _bump(clob_statuses, clob_status)
            if str(clob_status) == "OK":
                summary["clob_book_ok_events"] += 1
    return {
        **summary,
        "event_age_avg_s": _avg(event_ages),
        "event_age_p95_s": _p95(event_ages),
        "event_age_max_s": round(max(event_ages), 6) if event_ages else None,
        "copyability_reason_counts": copyability_reasons,
        "missed_copy_reason_counts": missed_reasons,
        "clob_book_status_counts": clob_statuses,
        "mirror_status_counts": mirror_statuses,
        "filter_policy_counts": filter_policies,
    }


def _status(ok: bool, *, blocked: bool = False) -> str:
    if ok:
        return "PASS"
    return "FAIL" if blocked else "WATCH"


def _has_keys(mapping: dict[str, Any], keys: tuple[str, ...]) -> bool:
    return all(key in mapping for key in keys)


def _operator_onboarding_status(onboarding: dict[str, Any]) -> tuple[str, list[str]]:
    state_status = str(onboarding.get("status") or "")
    command_results = [row for row in onboarding.get("command_results") or [] if isinstance(row, dict)]
    accepted_non_green = {
        (str(row.get("name") or row.get("command") or "command"), int(row.get("returncode") or 0))
        for row in onboarding.get("accepted_non_green_evidence_commands") or []
        if isinstance(row, dict)
    }

    def _is_expected_tracker_evidence(row: dict[str, Any]) -> bool:
        name = str(row.get("name") or row.get("command") or "command")
        return name == "paper_live_tracker" and int(row.get("returncode") or 0) == 2

    nonzero_commands = [
        str(row.get("name") or row.get("command") or "command")
        for row in command_results
        if not bool(row.get("accepted_non_green_evidence"))
        and (str(row.get("name") or row.get("command") or "command"), int(row.get("returncode") or 0))
        not in accepted_non_green
        and not _is_expected_tracker_evidence(row)
        and (row.get("ok") is False or int(row.get("returncode") or 0) != 0)
    ]
    blockers: list[str] = []
    if nonzero_commands:
        blockers.append("operator_onboarding_child_command_failed")
    if state_status in {"FAIL", "FAILED", "ERROR"}:
        blockers.append(f"operator_onboarding_state_{state_status.lower()}")
    if blockers:
        return "FAIL", blockers
    if state_status == "BLOCKED" and onboarding.get("paper_only") is True and onboarding.get("live_orders_allowed") is False:
        return "PASS", []
    if state_status == "PASS":
        return "PASS", []
    if state_status:
        return "WATCH", [f"operator_onboarding_state_{state_status.lower()}"]
    return "WATCH", ["operator_onboarding_state_missing"]


def _copyability_filtered_clob_book_status_counts(copy_efficiency: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    scores = copy_efficiency.get("event_scores") if isinstance(copy_efficiency, dict) else []
    for row in scores or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("filter_policy") or "") != "copyability":
            continue
        details = row.get("copyability_details") if isinstance(row.get("copyability_details"), dict) else {}
        status = details.get("clob_book_status") or row.get("clob_book_status") or "UNKNOWN"
        key = str(status)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _paper_copy_contract_numbers(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": contract.get("status") or "MISSING",
        "blockers": contract.get("blockers") or [],
        "required_buy_copy_events": int(contract.get("required_buy_copy_events") or 0),
        "clob_filled_buy_copy_events": int(contract.get("clob_filled_buy_copy_events") or 0),
        "fallback_filled_buy_copy_events": int(contract.get("fallback_filled_buy_copy_events") or 0),
        "rejected_buy_copy_events": int(contract.get("rejected_buy_copy_events") or 0),
        "missed_buy_copy_events": int(contract.get("missed_buy_copy_events") or 0),
        "paper_copy_fill_rate_pct": contract.get("paper_copy_fill_rate_pct"),
        "paper_only": contract.get("paper_only"),
        "live_orders_allowed": contract.get("live_orders_allowed"),
        "scope": contract.get("scope"),
    }


def _paper_copy_contract_check(contract: dict[str, Any], *, state_path: str, event_log_path: str) -> dict[str, Any]:
    current = contract.get("current_poll") if isinstance(contract.get("current_poll"), dict) else {}
    evidence = contract.get("evidence_window") if isinstance(contract.get("evidence_window"), dict) else {}
    current_numbers = _paper_copy_contract_numbers(current)
    evidence_numbers = _paper_copy_contract_numbers(evidence)
    blockers: list[str] = []

    if not contract:
        blockers.append("paper_copy_contract_missing")
    if evidence_numbers["status"] != "PASS":
        blockers.append("evidence_window_paper_copy_contract_not_pass")
    if evidence_numbers["required_buy_copy_events"] <= 0:
        blockers.append("evidence_window_no_required_copyable_buy_events")
    if evidence_numbers["clob_filled_buy_copy_events"] < evidence_numbers["required_buy_copy_events"]:
        blockers.append("evidence_window_clob_paper_fill_gap")
    if evidence_numbers["fallback_filled_buy_copy_events"] > 0:
        blockers.append("evidence_window_fallback_paper_fill_present")
    if evidence_numbers["rejected_buy_copy_events"] > 0:
        blockers.append("evidence_window_rejected_paper_order_present")
    if evidence_numbers["missed_buy_copy_events"] > 0:
        blockers.append("evidence_window_missed_copy_present")
    if evidence_numbers["paper_only"] is not True:
        blockers.append("evidence_window_not_paper_only")
    if evidence_numbers["live_orders_allowed"] is not False:
        blockers.append("evidence_window_live_orders_allowed")

    current_status = str(current_numbers["status"] or "")
    current_required = int(current_numbers["required_buy_copy_events"] or 0)
    if current_required > 0 and current_status != "PASS":
        blockers.append("current_poll_required_copyable_buy_not_filled")
    if current_required > 0 and current_numbers["clob_filled_buy_copy_events"] < current_required:
        blockers.append("current_poll_clob_paper_fill_gap")
    if current_numbers["fallback_filled_buy_copy_events"] > 0:
        blockers.append("current_poll_fallback_paper_fill_present")
    if current_numbers["rejected_buy_copy_events"] > 0:
        blockers.append("current_poll_rejected_paper_order_present")
    if current_numbers["missed_buy_copy_events"] > 0:
        blockers.append("current_poll_missed_copy_present")

    return {
        "status": "PASS" if not blockers else "FAIL",
        "blockers": blockers,
        "state_path": state_path,
        "event_log_path": event_log_path,
        "current_poll": current_numbers,
        "evidence_window": evidence_numbers,
        "reason": (
            "mechanical paper-copy contract: every required copyable BUY must become a CLOB-backed paper fill; "
            "an empty current poll is ANALYZE, not PASS, and does not override the rolling evidence window"
        ),
    }


def _max_full_state_load_bytes() -> int:
    raw = os.getenv("WALLET_COPY_AUDIT_MAX_FULL_STATE_BYTES", "").strip()
    if not raw:
        return 64 * 1024 * 1024
    try:
        return max(1, int(raw))
    except ValueError:
        return 64 * 1024 * 1024


def _extract_tail_object_for_key(path: str | Path, key: str, *, max_bytes: int = 8 * 1024 * 1024) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    try:
        size = target.stat().st_size
        with target.open("rb") as handle:
            handle.seek(max(0, size - max(1, int(max_bytes))))
            text = handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return {}

    marker = f'"{key}"'
    start = text.rfind(marker)
    if start < 0:
        return {}
    colon = text.find(":", start + len(marker))
    if colon < 0:
        return {}
    index = colon + 1
    while index < len(text) and text[index].isspace():
        index += 1
    if index >= len(text) or text[index] != "{":
        return {}

    depth = 0
    in_string = False
    escaped = False
    for pos in range(index, len(text)):
        char = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(text[index : pos + 1])
                except json.JSONDecodeError:
                    return {}
                return parsed if isinstance(parsed, dict) else {}
    return {}


def _large_state_stub(path: str | Path, *, size_bytes: int) -> dict[str, Any]:
    name = Path(path).name
    summary = _extract_tail_object_for_key(path, "summary")
    stub: dict[str, Any] = {
        "_audit_large_state_stub": True,
        "_file_size_bytes": int(size_bytes),
        "_reason": "state file exceeds bounded audit full-load limit",
    }
    if summary:
        stub["summary"] = summary
    if "history_state" in name:
        # Non-empty placeholders preserve presence checks without allocating the
        # full arrays. Durable JSONL counts remain the source for scale.
        stub["events"] = [{"_audit_large_state_stub": True}]
        stub["copy_intents"] = [{"_audit_large_state_stub": True}]
        stub["wallets"] = [{"_audit_large_state_stub": True}]
    if "paper_state" in name:
        paper_orders = int(summary.get("paper_orders") or summary.get("filled_orders") or 0)
        lifecycle_events = int(summary.get("lifecycle_events") or (paper_orders * 3 if paper_orders else 0))
        if paper_orders > 0:
            stub["orders"] = [{"_audit_large_state_stub": True}]
        if lifecycle_events > 0:
            stub["lifecycle_events"] = [{"_audit_large_state_stub": True}]
    return stub


def _state(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    try:
        size = target.stat().st_size
    except OSError:
        size = 0
    if size > _max_full_state_load_bytes():
        return _large_state_stub(path, size_bytes=size)
    data = load_json(path, default={})
    return data if isinstance(data, dict) else {}


def _read_feedback_entries(path: str | Path, *, max_lines: int = 500) -> list[dict[str, Any]]:
    return _jsonl_rows(path, max_lines=max_lines, max_bytes=4 * 1024 * 1024)


def _fingerprint(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _non_green_check_fingerprints(checks: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for check_name, check in sorted(checks.items()):
        status = str(check.get("status") or "")
        if status == "PASS":
            continue
        blockers: list[str] = []
        for key in (
            "copy_efficiency_blockers",
            "live_admission_blockers",
            "live_tracker_truth_blockers",
            "candidate_forward_truth_blockers",
            "forward_candidate_truth_blockers",
            "live_truth_blockers",
            "best_candidate_blockers",
            "blockers",
        ):
            value = check.get(key)
            if isinstance(value, list) and value:
                blockers = [str(row) for row in value if row]
                break
        if not blockers:
            live_truth_status = str(check.get("live_truth_status") or "")
            if live_truth_status and live_truth_status != "PASS":
                blockers.append(f"live_truth_status_{live_truth_status.lower()}")
            try:
                buy_source_events = int(check.get("buy_source_events") or 0)
                clob_filled = int(check.get("clob_filled_buy_copy_events") or 0)
                rejected = int(check.get("rejected_buy_copy_events") or 0)
                fallback = int(check.get("fallback_filled_buy_copy_events") or 0)
                coverage_violations = int(check.get("coverage_violations") or 0)
                missed_lifecycle = int(check.get("missed_lifecycle_events") or 0)
            except (TypeError, ValueError):
                buy_source_events = clob_filled = rejected = fallback = coverage_violations = missed_lifecycle = 0
            if buy_source_events > clob_filled:
                blockers.append("clob_filled_buy_copy_gap")
            if rejected > 0:
                blockers.append("rejected_buy_copy_events_present")
            if fallback > 0:
                blockers.append("fallback_filled_buy_copy_events_present")
            if coverage_violations > 0:
                blockers.append("coverage_violations_present")
            if missed_lifecycle > 0:
                blockers.append("missed_lifecycle_events_present")
        payload = {
            "check": check_name,
            "status": status,
            "blockers": blockers,
            "reason": check.get("reason"),
        }
        rows.append({**payload, "fingerprint": _fingerprint(payload)})
    return rows


def _source_counter_decreases(
    current: dict[str, int],
    previous: dict[str, Any],
    *,
    min_significant_drop: int = 1,
) -> list[dict[str, Any]]:
    durable_prefixes = (
        "history_",
        "paper_",
        "ml_",
        "leaderboard_",
        "registry_",
        "active_hotlane_registry_",
    )
    durable_suffixes = ("_event_log_lines", "_dataset_rows")
    prior = previous.get("source_counters") if isinstance(previous.get("source_counters"), dict) else {}
    rows: list[dict[str, Any]] = []
    for key, value in sorted(current.items()):
        if not (key.startswith(durable_prefixes) or key.endswith(durable_suffixes)):
            continue
        try:
            old_value = int(prior.get(key) or 0)
            new_value = int(value or 0)
        except (TypeError, ValueError):
            continue
        drop = old_value - new_value
        if drop >= min_significant_drop:
            rows.append({"metric": key, "previous": old_value, "current": new_value, "drop": drop})
    return rows


def _feedback_loop(
    *,
    learning_status: str,
    checks: dict[str, dict[str, Any]],
    next_actions: list[dict[str, str]],
    source_counters: dict[str, int],
    feedback_log: str,
) -> dict[str, Any]:
    entries = _read_feedback_entries(feedback_log)
    previous = entries[-1] if entries else {}
    non_green = _non_green_check_fingerprints(checks)
    prior_counts: dict[str, int] = {}
    for entry in entries:
        for row in entry.get("non_green_checks") or []:
            if isinstance(row, dict) and row.get("fingerprint"):
                prior_counts[str(row["fingerprint"])] = prior_counts.get(str(row["fingerprint"]), 0) + 1
    repeated = [
        {**row, "previous_occurrences": prior_counts.get(str(row.get("fingerprint")), 0)}
        for row in non_green
        if prior_counts.get(str(row.get("fingerprint")), 0) > 0
    ]
    stuck = [row for row in repeated if int(row.get("previous_occurrences") or 0) >= 2]
    decreases = _source_counter_decreases(source_counters, previous)
    green_by_removal_status = "PASS"
    if decreases:
        green_by_removal_status = "FAIL"

    if green_by_removal_status == "FAIL":
        status = "BUG_SUSPECT"
    elif non_green or stuck or green_by_removal_status == "WATCH":
        status = "WATCH"
    else:
        status = "PASS"

    return {
        "status": status,
        "feedback_log": feedback_log,
        "prior_entries_read": len(entries),
        "non_green_checks": non_green,
        "repeated_blockers": repeated,
        "stuck_blockers": stuck,
        "green_by_removal_guard": {
            "status": green_by_removal_status,
            "source_counter_decreases": decreases,
            "rule": "GREEN is forbidden when sources/logs shrink unless a replacement path is evidenced",
        },
        "required_action": (
            "fix_or_deeper_measurement_or_code_level_backlog_required"
            if status != "PASS"
            else "none"
        ),
        "next_actions": next_actions,
    }


def _core_paths(args: argparse.Namespace) -> dict[str, str]:
    return {
        "leaderboard_state": args.leaderboard_state,
        "history_state": args.history_state,
        "history_event_log": args.history_event_log,
        "paper_state": args.paper_state,
        "paper_event_log": args.paper_event_log,
        "research_state": args.research_state,
        "ml_dataset": args.ml_dataset,
        "profit_state": args.profit_state,
        "live_tracking_state": args.live_tracking_state,
        "live_tracking_event_log": args.live_tracking_event_log,
        "active_hotlane_state": _arg(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json"),
        "active_hotlane_registry": _arg(args, "active_hotlane_registry", "data/research/wallet_copy_active_hotlane_registry.json"),
        "active_hotlane_live_tracking_state": _arg(
            args,
            "active_hotlane_live_tracking_state",
            "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
        ),
        "active_hotlane_live_tracking_event_log": _arg(
            args,
            "active_hotlane_live_tracking_event_log",
            "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
        ),
        "active_hotlane_paper_state": _arg(
            args,
            "active_hotlane_paper_state",
            "data/research/wallet_copy_active_hotlane_paper_state.json",
        ),
        "active_hotlane_paper_event_log": _arg(
            args,
            "active_hotlane_paper_event_log",
            "data/research/wallet_copy_active_hotlane_paper_events.jsonl",
        ),
        "active_forward_probe_profit_state": _arg(
            args,
            "active_forward_probe_profit_state",
            "data/research/wallet_copy_active_forward_probe_profit_state.json",
        ),
        "active_forward_probe_live_tracking_state": _arg(
            args,
            "active_forward_probe_live_tracking_state",
            "data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
        ),
        "active_forward_probe_live_tracking_event_log": _arg(
            args,
            "active_forward_probe_live_tracking_event_log",
            "data/research/wallet_copy_active_forward_probe_live_tracking_events.jsonl",
        ),
        "active_forward_probe_paper_event_log": _arg(
            args,
            "active_forward_probe_paper_event_log",
            "data/research/wallet_copy_active_forward_probe_paper_events.jsonl",
        ),
        "active_hotlane_tick_state": _arg(
            args,
            "active_hotlane_tick_state",
            "data/research/wallet_copy_hotlane_tick_state.json",
        ),
        "adaptive_bot_state": _arg(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json"),
        "adaptive_bot_paper_event_log": _arg(
            args,
            "adaptive_bot_paper_event_log",
            "data/research/wallet_copy_adaptive_bot_paper_events.jsonl",
        ),
        "adaptive_single_wallet_exact_copy_paper_event_log": _arg(
            args,
            "adaptive_single_wallet_exact_copy_paper_event_log",
            "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
        ),
        "adaptive_tracker_time_replay_paper_event_log": _arg(
            args,
            "adaptive_tracker_time_replay_paper_event_log",
            "data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
        ),
        "operator_onboarding_state": args.operator_onboarding_state,
    }


def build_learning_log_audit(args: argparse.Namespace) -> dict[str, Any]:
    mission = mission_contract()
    mission_check = mission_contract_check()
    paths = _core_paths(args)
    active_hotlane_state_path = str(_arg(args, "active_hotlane_state", "data/research/wallet_copy_active_hotlane_state.json"))
    active_hotlane_registry_path = str(
        _arg(args, "active_hotlane_registry", "data/research/wallet_copy_active_hotlane_registry.json")
    )
    active_hotlane_live_tracking_state_path = str(
        _arg(
            args,
            "active_hotlane_live_tracking_state",
            "data/research/wallet_copy_active_hotlane_live_tracking_state.json",
        )
    )
    active_hotlane_live_tracking_event_log_path = str(
        _arg(
            args,
            "active_hotlane_live_tracking_event_log",
            "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
        )
    )
    active_hotlane_paper_state_path = str(
        _arg(args, "active_hotlane_paper_state", "data/research/wallet_copy_active_hotlane_paper_state.json")
    )
    active_hotlane_paper_event_log_path = str(
        _arg(
            args,
            "active_hotlane_paper_event_log",
            "data/research/wallet_copy_active_hotlane_paper_events.jsonl",
        )
    )
    active_forward_probe_profit_state_path = str(
        _arg(
            args,
            "active_forward_probe_profit_state",
            "data/research/wallet_copy_active_forward_probe_profit_state.json",
        )
    )
    active_forward_probe_live_tracking_state_path = str(
        _arg(
            args,
            "active_forward_probe_live_tracking_state",
            "data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
        )
    )
    active_forward_probe_live_tracking_event_log_path = str(
        _arg(
            args,
            "active_forward_probe_live_tracking_event_log",
            "data/research/wallet_copy_active_forward_probe_live_tracking_events.jsonl",
        )
    )
    active_forward_probe_paper_event_log_path = str(
        _arg(
            args,
            "active_forward_probe_paper_event_log",
            "data/research/wallet_copy_active_forward_probe_paper_events.jsonl",
        )
    )
    live_tracker_time_replay_paper_event_log_path = str(
        _arg(
            args,
            "live_tracker_time_replay_paper_event_log",
            "data/research/wallet_copy_live_tracker_time_replay_paper_events.jsonl",
        )
    )
    active_hotlane_tracker_time_replay_paper_event_log_path = str(
        _arg(
            args,
            "active_hotlane_tracker_time_replay_paper_event_log",
            "data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
        )
    )
    active_hotlane_single_wallet_exact_copy_paper_event_log_path = str(
        _arg(
            args,
            "active_hotlane_single_wallet_exact_copy_paper_event_log",
            "data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
        )
    )
    active_hotlane_all_order_tactic_replay_paper_event_log_path = str(
        _arg(
            args,
            "active_hotlane_all_order_tactic_replay_paper_event_log",
            "data/research/wallet_copy_active_hotlane_paper_events_all_order_exact_copy_aggressive_tactic_replay.jsonl",
        )
    )
    active_hotlane_all_order_tactic_replay_paper_state_path = str(
        _arg(
            args,
            "active_hotlane_all_order_tactic_replay_paper_state",
            "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy_aggressive_tactic_replay.json",
        )
    )
    resolutions_path = str(_arg(args, "resolutions", "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"))
    active_hotlane_tick_state_path = str(
        _arg(args, "active_hotlane_tick_state", "data/research/wallet_copy_hotlane_tick_state.json")
    )
    active_hotlane_guard_log_arg = getattr(args, "active_hotlane_guard_log", None)
    active_hotlane_guard_log_path = str(
        active_hotlane_guard_log_arg or "data/research/wallet_copy_active_hotlane_guard.out"
    )
    active_hotlane_guard_log_check_enabled = active_hotlane_guard_log_arg is not None
    active_hotlane_guard_log_max_bytes = int(
        _arg(args, "max_active_hotlane_guard_log_bytes", 256 * 1024 * 1024)
    )
    adaptive_bot_state_path = str(_arg(args, "adaptive_bot_state", "data/research/wallet_copy_adaptive_bot_state.json"))
    adaptive_bot_paper_state_path = str(
        _arg(args, "adaptive_bot_paper_state", "data/research/wallet_copy_adaptive_bot_paper_state.json")
    )
    adaptive_bot_paper_event_log_path = str(
        _arg(
            args,
            "adaptive_bot_paper_event_log",
            "data/research/wallet_copy_adaptive_bot_paper_events.jsonl",
        )
    )
    adaptive_single_wallet_exact_copy_paper_event_log_path = str(
        _arg(
            args,
            "adaptive_single_wallet_exact_copy_paper_event_log",
            "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
        )
    )
    adaptive_tracker_time_replay_paper_event_log_path = str(
        _arg(
            args,
            "adaptive_tracker_time_replay_paper_event_log",
            "data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
        )
    )
    pipeline_resume_state_arg = getattr(args, "pipeline_resume_state", None)
    pipeline_resume_state_path = str(pipeline_resume_state_arg or "data/research/wallet_copy_pipeline_resume_state.json")
    leaderboard = _state(args.leaderboard_state)
    history = _state(args.history_state)
    pipeline_resume = _state(pipeline_resume_state_path) if pipeline_resume_state_arg is not None else {}
    paper = _state(args.paper_state)
    research = _state(args.research_state)
    profit = _state(args.profit_state)
    tracker = _state(args.live_tracking_state)
    active_hotlane = _state(active_hotlane_state_path)
    active_hotlane_tracker = _state(active_hotlane_live_tracking_state_path)
    active_forward_probe_profit = _state(active_forward_probe_profit_state_path)
    active_forward_probe_tracker = _state(active_forward_probe_live_tracking_state_path)
    active_hotlane_tick = _state(active_hotlane_tick_state_path)
    adaptive = _state(adaptive_bot_state_path)
    onboarding = _state(args.operator_onboarding_state)

    history_events = history.get("events") if isinstance(history.get("events"), list) else []
    history_intents = history.get("copy_intents") if isinstance(history.get("copy_intents"), list) else []
    history_wallets = history.get("wallets") if isinstance(history.get("wallets"), list) else []
    paper_orders = paper.get("orders") if isinstance(paper.get("orders"), list) else []
    paper_lifecycle = paper.get("lifecycle_events") if isinstance(paper.get("lifecycle_events"), list) else []
    paper_summary = paper.get("summary") if isinstance(paper.get("summary"), dict) else {}
    research_performance = research.get("performance") if isinstance(research.get("performance"), dict) else {}
    lifecycle_realized = (
        research_performance.get("lifecycle_realized")
        if isinstance(research_performance.get("lifecycle_realized"), dict)
        else {}
    )
    profit_decision = profit.get("decision") if isinstance(profit.get("decision"), dict) else {}
    live_tracker_truth = profit.get("live_tracker_truth") if isinstance(profit.get("live_tracker_truth"), dict) else {}
    live_tracker_truth_summary = (
        live_tracker_truth.get("summary") if isinstance(live_tracker_truth.get("summary"), dict) else {}
    )
    candidate_forward_live_tracker_truth = (
        profit.get("candidate_forward_live_tracker_truth")
        if isinstance(profit.get("candidate_forward_live_tracker_truth"), dict)
        else {}
    )
    forward_candidate_live_tracker_truth = (
        profit.get("forward_candidate_live_tracker_truth")
        if isinstance(profit.get("forward_candidate_live_tracker_truth"), dict)
        else {}
    )
    forward_candidate = profit.get("forward_candidate") if isinstance(profit.get("forward_candidate"), dict) else {}
    forward_tracking_queue = (
        profit.get("forward_tracking_queue") if isinstance(profit.get("forward_tracking_queue"), list) else []
    )
    effective_live_tracker_truth = (
        profit.get("effective_live_tracker_truth")
        if isinstance(profit.get("effective_live_tracker_truth"), dict)
        else live_tracker_truth
    )
    best_candidate = profit.get("best_candidate") if isinstance(profit.get("best_candidate"), dict) else {}
    raw_baseline = (
        best_candidate.get("raw_baseline_summary")
        if isinstance(best_candidate.get("raw_baseline_summary"), dict)
        else {}
    )
    tracker_summary = tracker.get("summary") if isinstance(tracker.get("summary"), dict) else {}
    wallet_reports = [row for row in tracker_summary.get("wallet_reports") or [] if isinstance(row, dict)]
    wallet_api_errors = [row.get("api_error") for row in wallet_reports if isinstance(row.get("api_error"), dict)]
    copy_efficiency = (
        tracker_summary.get("copy_efficiency")
        if isinstance(tracker_summary.get("copy_efficiency"), dict)
        else {}
    )
    copy_eff_summary = (
        copy_efficiency.get("summary") if isinstance(copy_efficiency.get("summary"), dict) else {}
    )
    onboarding_status, onboarding_blockers = _operator_onboarding_status(onboarding)
    evidence_counts = (
        tracker_summary.get("evidence_status_counts")
        if isinstance(tracker_summary.get("evidence_status_counts"), dict)
        else {}
    )
    tracker_profit_policy = (
        tracker_summary.get("profit_policy") if isinstance(tracker_summary.get("profit_policy"), dict) else {}
    )
    tracker_latency = tracker_summary.get("latency") if isinstance(tracker_summary.get("latency"), dict) else {}
    wallet_reports = [
        row for row in (tracker_summary.get("wallet_reports") or []) if isinstance(row, dict)
    ]
    jsonl_counts = {
        key: _jsonl_count(path)
        for key, path in {
            "history_event_log": args.history_event_log,
            "paper_event_log": args.paper_event_log,
            "live_tracker_time_replay_paper_event_log": live_tracker_time_replay_paper_event_log_path,
            "ml_dataset": args.ml_dataset,
            "live_tracking_event_log": args.live_tracking_event_log,
            "active_hotlane_live_tracking_event_log": active_hotlane_live_tracking_event_log_path,
            "active_hotlane_paper_event_log": active_hotlane_paper_event_log_path,
            "active_forward_probe_live_tracking_event_log": active_forward_probe_live_tracking_event_log_path,
            "active_forward_probe_paper_event_log": active_forward_probe_paper_event_log_path,
            "active_hotlane_tracker_time_replay_paper_event_log": (
                active_hotlane_tracker_time_replay_paper_event_log_path
            ),
            "active_hotlane_all_order_tactic_replay_paper_event_log": (
                active_hotlane_all_order_tactic_replay_paper_event_log_path
            ),
            "adaptive_bot_paper_event_log": adaptive_bot_paper_event_log_path,
            "adaptive_single_wallet_exact_copy_paper_event_log": (
                adaptive_single_wallet_exact_copy_paper_event_log_path
            ),
            "adaptive_tracker_time_replay_paper_event_log": adaptive_tracker_time_replay_paper_event_log_path,
        }.items()
    }
    live_event_log_summary = _live_event_log_summary(args.live_tracking_event_log)
    wallet_report_summary = _wallet_report_summary(wallet_reports, event_log_summary=live_event_log_summary)

    missing_paths = [name for name, path in paths.items() if not Path(path).exists()]
    active_hotlane_guard_log_size_bytes = (
        _file_size_bytes(active_hotlane_guard_log_path) if active_hotlane_guard_log_check_enabled else 0
    )

    clob_filled = int(copy_eff_summary.get("clob_filled_buy_copy_events") or 0)
    fallback_filled = int(copy_eff_summary.get("fallback_filled_buy_copy_events") or 0)
    rejected = int(copy_eff_summary.get("rejected_buy_copy_events") or 0)
    missed = int(copy_eff_summary.get("missed_buy_copy_events") or 0)
    lifecycle_missed = int(copy_eff_summary.get("lifecycle_missed_events") or 0)
    required_buys = int(copy_eff_summary.get("required_buy_copy_events") or 0)
    copyability_filtered = max(
        int(copy_eff_summary.get("copyability_filtered_buy_events") or 0),
        int(live_event_log_summary.get("copyability_filtered_buy_events") or 0),
    )
    event_log_tracked_moves = int(live_event_log_summary.get("tracked_move_events") or 0)
    event_log_source_buys = int(live_event_log_summary.get("source_buy_events") or 0)
    observed_latency_avg = float(copy_eff_summary.get("event_age_avg_s") or copy_eff_summary.get("api_latency_avg_s") or 0.0)
    if observed_latency_avg <= 0 and live_event_log_summary.get("event_age_avg_s") is not None:
        observed_latency_avg = float(live_event_log_summary.get("event_age_avg_s") or 0.0)
    required_latency_raw = copy_eff_summary.get("required_event_age_avg_s")
    required_latency_avg = float(required_latency_raw) if required_latency_raw is not None else None
    latency_avg = required_latency_avg if required_buys > 0 and required_latency_avg is not None else observed_latency_avg
    latency_basis_for_gate = (
        "required_buy_event_age_s"
        if required_buys > 0 and required_latency_avg is not None
        else "observed_event_age_s"
    )
    copyability_filtered_clob_counts = _copyability_filtered_clob_book_status_counts(copy_efficiency)
    if not copyability_filtered_clob_counts:
        copyability_filtered_clob_counts = live_event_log_summary.get("clob_book_status_counts") or {}
    source_buy_events = int(copy_eff_summary.get("source_buy_events") or live_event_log_summary.get("source_buy_events") or 0)
    fresh_buy_rows_le_10s = int(copy_eff_summary.get("source_fresh_buy_events_le_10s") or 0)
    fresh_buy_rows_le_30s = int(copy_eff_summary.get("source_fresh_buy_events_le_30s") or 0)
    stale_buy_rows_gt_300s = int(copy_eff_summary.get("stale_buy_rows_gt_300s") or 0)
    latest_buy_event_lag_s = copy_eff_summary.get("latest_buy_event_lag_s")
    best_candidate_roi = (
        (best_candidate.get("summary") or {}).get("roi_pct")
        if isinstance(best_candidate.get("summary"), dict)
        else None
    )
    raw_baseline_roi = raw_baseline.get("roi_pct")
    strong_research_candidate = any(
        value is not None and float(value) > 0.0
        for value in (best_candidate_roi, raw_baseline_roi, (research_performance.get("summary") or {}).get("roi_pct") if isinstance(research_performance.get("summary"), dict) else None)
    )
    onboarding_copy_verdict = None
    if strong_research_candidate and source_buy_events > 0 and fresh_buy_rows_le_10s == 0:
        onboarding_copy_verdict = "WALLET_STRONG_BUT_NOT_ADMISSION_READY"
    adaptive_summary = adaptive.get("summary") if isinstance(adaptive.get("summary"), dict) else {}
    adaptive_signals = adaptive.get("signals") if isinstance(adaptive.get("signals"), list) else []
    adaptive_status = str(adaptive.get("status") or "")
    adaptive_embedded_hot_path = (
        adaptive.get("embedded_tracker_hot_path")
        if isinstance(adaptive.get("embedded_tracker_hot_path"), dict)
        else {}
    )
    adaptive_embedded_hot_path_multi_pass = bool(
        adaptive_embedded_hot_path.get("current_poll_multi_wallet_pass") is True
    )
    adaptive_embedded_hot_path_single_wallet_pass = bool(
        adaptive_embedded_hot_path.get("current_poll_single_wallet_exact_copy_pass") is True
    )
    adaptive_intents = int(adaptive_summary.get("intents") or 0)
    adaptive_pass_signals = int(adaptive_summary.get("pass_signals") or 0)
    adaptive_tracker_time_pass_signals = int(adaptive_summary.get("tracker_time_pass_signals") or 0)
    adaptive_runtime_inventory_research_candidates = int(
        adaptive_summary.get("runtime_inventory_research_candidates") or 0
    )
    adaptive_tracker_time_inventory_research_candidates = int(
        adaptive_summary.get("tracker_time_inventory_research_candidates") or 0
    )
    adaptive_tracker_time_replay = (
        adaptive.get("tracker_time_replay") if isinstance(adaptive.get("tracker_time_replay"), dict) else {}
    )
    active_hotlane_summary = active_hotlane.get("summary") if isinstance(active_hotlane.get("summary"), dict) else {}
    active_hotlane_selected = (
        active_hotlane.get("selected_wallets") if isinstance(active_hotlane.get("selected_wallets"), list) else []
    )
    active_hotlane_status = str(active_hotlane.get("status") or "")
    active_hotlane_selected_addresses = {
        str(row.get("address") or row.get("wallet") or "").lower()
        for row in active_hotlane_selected
        if isinstance(row, dict) and (row.get("address") or row.get("wallet"))
    }
    adaptive_source = (
        adaptive.get("source_provenance") if isinstance(adaptive.get("source_provenance"), dict) else {}
    )
    adaptive_source_wallets = {
        str(wallet).lower()
        for wallet in adaptive_source.get("source_wallets") or []
        if wallet
    }
    adaptive_selected_wallet_overlap = len(adaptive_source_wallets & active_hotlane_selected_addresses)
    adaptive_tracker_time_replay_summary = (
        adaptive_tracker_time_replay.get("summary")
        if isinstance(adaptive_tracker_time_replay.get("summary"), dict)
        else {}
    )
    adaptive_tracker_time_replay_intents = int(adaptive_tracker_time_replay_summary.get("intents") or 0)
    adaptive_tracker_time_replay_filled_orders = int(
        adaptive_tracker_time_replay_summary.get("filled_orders") or 0
    )
    adaptive_tracker_time_replay_rejected_orders = int(
        adaptive_tracker_time_replay_summary.get("rejected_orders") or 0
    )
    adaptive_single_wallet = (
        adaptive.get("single_wallet_exact_copy")
        if isinstance(adaptive.get("single_wallet_exact_copy"), dict)
        else {}
    )
    adaptive_single_wallet_summary = (
        adaptive_single_wallet.get("summary")
        if isinstance(adaptive_single_wallet.get("summary"), dict)
        else {}
    )
    adaptive_single_wallet_intents = int(adaptive_single_wallet_summary.get("intents") or 0)
    adaptive_single_wallet_filled_orders = int(adaptive_single_wallet_summary.get("filled_orders") or 0)
    adaptive_single_wallet_rejected_orders = int(adaptive_single_wallet_summary.get("rejected_orders") or 0)
    adaptive_single_wallet_own_pass = bool(
        adaptive
        and adaptive_single_wallet.get("status") == "PASS"
        and adaptive_single_wallet_intents > 0
        and adaptive_single_wallet_filled_orders >= adaptive_single_wallet_intents
        and adaptive_single_wallet_rejected_orders == 0
        and jsonl_counts["adaptive_single_wallet_exact_copy_paper_event_log"] > 0
        and adaptive_source.get("lane") == "active_hotlane"
        and str(adaptive_source.get("live_tracking_state_path") or "") == active_hotlane_live_tracking_state_path
        and str(adaptive_source.get("live_tracking_event_log_path") or "") == active_hotlane_live_tracking_event_log_path
        and adaptive_selected_wallet_overlap > 0
    )
    adaptive_single_wallet_embedded_pass = bool(
        adaptive
        and adaptive_embedded_hot_path_single_wallet_pass
        and adaptive_source.get("lane") == "active_hotlane"
        and str(adaptive_source.get("live_tracking_state_path") or "") == active_hotlane_live_tracking_state_path
        and str(adaptive_source.get("live_tracking_event_log_path") or "") == active_hotlane_live_tracking_event_log_path
        and adaptive_selected_wallet_overlap > 0
        and adaptive_embedded_hot_path.get("paper_only") is True
        and adaptive_embedded_hot_path.get("live_orders_allowed") is False
    )
    adaptive_filled_orders = int(adaptive_summary.get("filled_orders") or 0)
    adaptive_rejected_orders = int(adaptive_summary.get("rejected_orders") or 0)
    adaptive_paper_orders = int(adaptive_summary.get("paper_orders") or 0)
    adaptive_moves_seen = int(adaptive_summary.get("moves_seen") or 0)
    adaptive_eligible_moves = int(adaptive_summary.get("eligible_moves") or 0)
    adaptive_filter_reasons = (
        adaptive_summary.get("filter_reason_counts")
        if isinstance(adaptive_summary.get("filter_reason_counts"), dict)
        else {}
    )
    adaptive_distinct_wallets_max = 0
    adaptive_distinct_markets = {
        str(signal.get("market_slug") or "")
        for signal in adaptive_signals
        if isinstance(signal, dict) and signal.get("market_slug")
    }
    adaptive_opposing_wallet_count = 0
    adaptive_clob_fill_source_counts: dict[str, int] = {}
    for signal in adaptive_signals:
        if not isinstance(signal, dict):
            continue
        agreeing_wallets = signal.get("agreeing_wallets") if isinstance(signal.get("agreeing_wallets"), list) else []
        opposing_wallets = signal.get("opposing_wallets") if isinstance(signal.get("opposing_wallets"), list) else []
        adaptive_distinct_wallets_max = max(adaptive_distinct_wallets_max, len(agreeing_wallets))
        adaptive_opposing_wallet_count = max(adaptive_opposing_wallet_count, len(opposing_wallets))
        evidence = signal.get("chosen_evidence") if isinstance(signal.get("chosen_evidence"), dict) else {}
        clob = evidence.get("clob_book") if isinstance(evidence.get("clob_book"), dict) else {}
        fill_source = str(clob.get("instant_fill_status") or clob.get("status") or "UNKNOWN")
        adaptive_clob_fill_source_counts[fill_source] = adaptive_clob_fill_source_counts.get(fill_source, 0) + 1
    adaptive_own_multi_wallet_pass = bool(
        adaptive
        and adaptive_status == "PASS"
        and adaptive_intents > 0
        and adaptive_pass_signals > 0
        and adaptive_eligible_moves >= adaptive_intents
        and adaptive_paper_orders == adaptive_intents
        and adaptive_filled_orders == adaptive_intents
        and adaptive_rejected_orders == 0
        and adaptive.get("paper_only") is True
        and adaptive.get("live_orders_allowed") is False
        and jsonl_counts["adaptive_bot_paper_event_log"] > 0
    )
    active_hotlane_tracker_summary = (
        active_hotlane_tracker.get("summary") if isinstance(active_hotlane_tracker.get("summary"), dict) else {}
    )
    active_hotlane_tracker_copy_efficiency = (
        active_hotlane_tracker_summary.get("copy_efficiency")
        if isinstance(active_hotlane_tracker_summary.get("copy_efficiency"), dict)
        else {}
    )
    active_hotlane_tracker_copy_eff_summary = (
        active_hotlane_tracker_copy_efficiency.get("summary")
        if isinstance(active_hotlane_tracker_copy_efficiency.get("summary"), dict)
        else {}
    )
    active_hotlane_paper_copy_contract = (
        active_hotlane_tracker_summary.get("paper_copy_contract")
        if isinstance(active_hotlane_tracker_summary.get("paper_copy_contract"), dict)
        else {}
    )
    active_hotlane_paper_copy_contract_check = _paper_copy_contract_check(
        active_hotlane_paper_copy_contract,
        state_path=active_hotlane_live_tracking_state_path,
        event_log_path=active_hotlane_live_tracking_event_log_path,
    )
    active_hotlane_tracker_scope = (
        active_hotlane_tracker_summary.get("tracker_scope")
        if isinstance(active_hotlane_tracker_summary.get("tracker_scope"), dict)
        else {}
    )
    active_hotlane_poll_runtime = (
        active_hotlane_tracker_summary.get("poll_runtime")
        if isinstance(active_hotlane_tracker_summary.get("poll_runtime"), dict)
        else {}
    )
    active_hotlane_hot_path_adaptive = (
        active_hotlane_tracker_summary.get("hot_path_adaptive")
        if isinstance(active_hotlane_tracker_summary.get("hot_path_adaptive"), dict)
        else {}
    )
    active_hotlane_all_order_exact_copy = (
        active_hotlane_tracker_summary.get("all_order_exact_copy")
        if isinstance(active_hotlane_tracker_summary.get("all_order_exact_copy"), dict)
        else {}
    )
    active_hotlane_all_order_tactic_replay = (
        active_hotlane_all_order_exact_copy.get("aggressive_tactic_replay")
        if isinstance(active_hotlane_all_order_exact_copy.get("aggressive_tactic_replay"), dict)
        else {}
    )
    active_hotlane_all_order_tactic_replay_status = str(
        active_hotlane_all_order_tactic_replay.get("status") or ""
    )
    if active_hotlane_all_order_tactic_replay.get("paper_state_path"):
        active_hotlane_all_order_tactic_replay_paper_state_path = str(
            active_hotlane_all_order_tactic_replay.get("paper_state_path")
        )
    if active_hotlane_all_order_tactic_replay.get("paper_event_log_path"):
        active_hotlane_all_order_tactic_replay_paper_event_log_path = str(
            active_hotlane_all_order_tactic_replay.get("paper_event_log_path")
        )
        jsonl_counts["active_hotlane_all_order_tactic_replay_paper_event_log"] = _jsonl_count(
            active_hotlane_all_order_tactic_replay_paper_event_log_path
        )
    active_hotlane_all_order_tactic_replay_paper_state = _state(
        active_hotlane_all_order_tactic_replay_paper_state_path
    )
    active_hotlane_all_order_tactic_replay_paper_summary = (
        active_hotlane_all_order_tactic_replay_paper_state.get("summary")
        if isinstance(active_hotlane_all_order_tactic_replay_paper_state.get("summary"), dict)
        else {}
    )
    active_hotlane_all_order_tactic_replay_paper_orders = [
        row
        for row in active_hotlane_all_order_tactic_replay_paper_state.get("orders") or []
        if isinstance(row, dict)
    ]
    active_hotlane_all_order_tactic_replay_paper_filled = int(
        active_hotlane_all_order_tactic_replay_paper_summary.get("filled_orders") or 0
    )
    active_hotlane_all_order_tactic_replay_paper_rejected = int(
        active_hotlane_all_order_tactic_replay_paper_summary.get("rejected_orders") or 0
    )
    active_hotlane_all_order_tactic_replay_paper_fallback = sum(
        1
        for row in active_hotlane_all_order_tactic_replay_paper_orders
        if str(
            (
                (
                    (row.get("source_intent") or {}).get("fill_estimate")
                    if isinstance(row.get("source_intent"), dict)
                    else {}
                )
                or {}
            ).get("source")
            or ""
        )
        == "source_price_plus_slippage_fallback"
    )
    active_hotlane_all_order_tactic_replay_paper_clob = sum(
        1
        for row in active_hotlane_all_order_tactic_replay_paper_orders
        if str(
            (
                (
                    (row.get("source_intent") or {}).get("fill_estimate")
                    if isinstance(row.get("source_intent"), dict)
                    else {}
                )
                or {}
            ).get("source")
            or ""
        )
        == "clob_book_evidence"
    )
    active_hotlane_all_order_tactic_replay_intents = int(
        active_hotlane_all_order_tactic_replay.get("replay_intents") or 0
    )
    active_hotlane_all_order_tactic_replay_filled = int(
        active_hotlane_all_order_tactic_replay.get("filled_orders") or 0
    )
    active_hotlane_all_order_tactic_replay_rejected = int(
        active_hotlane_all_order_tactic_replay.get("rejected_orders") or 0
    )
    active_hotlane_all_order_tactic_replay_fallback = int(
        active_hotlane_all_order_tactic_replay.get("fallback_filled_orders") or 0
    )
    active_hotlane_all_order_tactic_replay_pass = bool(
        (
            (
                active_hotlane_all_order_tactic_replay
                and active_hotlane_all_order_tactic_replay_status == "PASS"
                and active_hotlane_all_order_tactic_replay_intents > 0
                and active_hotlane_all_order_tactic_replay_filled >= active_hotlane_all_order_tactic_replay_intents
                and active_hotlane_all_order_tactic_replay_rejected == 0
                and active_hotlane_all_order_tactic_replay_fallback == 0
                and active_hotlane_all_order_tactic_replay.get("paper_only") is True
                and active_hotlane_all_order_tactic_replay.get("live_orders_allowed") is False
            )
            or (
                active_hotlane_all_order_tactic_replay_paper_filled > 0
                and active_hotlane_all_order_tactic_replay_paper_rejected == 0
                and active_hotlane_all_order_tactic_replay_paper_fallback == 0
            )
        )
        and jsonl_counts["active_hotlane_all_order_tactic_replay_paper_event_log"] > 0
    )
    active_hotlane_all_order_strict_paper_orders = [
        row
        for row in (
            (
                active_hotlane_all_order_exact_copy.get("paper_state")
                if isinstance(active_hotlane_all_order_exact_copy.get("paper_state"), dict)
                else {}
            ).get("orders")
            or []
        )
        if isinstance(row, dict)
    ]
    active_hotlane_all_order_strict_paper_state_path = str(
        _arg(
            args,
            "active_hotlane_all_order_exact_copy_paper_state",
            "data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
        )
    )
    if not active_hotlane_all_order_strict_paper_orders:
        active_hotlane_all_order_strict_paper_state = _state(active_hotlane_all_order_strict_paper_state_path)
        active_hotlane_all_order_strict_paper_orders = [
            row
            for row in active_hotlane_all_order_strict_paper_state.get("orders") or []
            if isinstance(row, dict)
        ]
    active_hotlane_all_order_tactic_pnl_attribution = score_tactic_replay_pnl(
        active_hotlane_all_order_strict_paper_orders,
        active_hotlane_all_order_tactic_replay_paper_orders,
        load_resolutions(resolutions_path),
    )
    active_hotlane_all_order_tactic_lifecycle_pass = active_hotlane_all_order_tactic_replay_pass
    active_hotlane_all_order_tactic_pnl_pass = (
        active_hotlane_all_order_tactic_pnl_attribution.get("status") == "PASS"
    )
    active_hotlane_all_order_tactic_overall_pass = bool(
        active_hotlane_all_order_tactic_lifecycle_pass and active_hotlane_all_order_tactic_pnl_pass
    )
    active_hotlane_all_order_status = str(active_hotlane_all_order_exact_copy.get("status") or "")
    active_hotlane_all_order_live_truth_status = str(
        active_hotlane_all_order_exact_copy.get("live_truth_status") or ""
    )
    active_hotlane_all_order_source_events = int(
        active_hotlane_all_order_exact_copy.get("source_events") or 0
    )
    active_hotlane_all_order_buy_source_events = int(
        active_hotlane_all_order_exact_copy.get("buy_source_events") or 0
    )
    active_hotlane_all_order_buy_intents = int(
        active_hotlane_all_order_exact_copy.get("buy_intents") or 0
    )
    active_hotlane_all_order_clob_filled = int(
        active_hotlane_all_order_exact_copy.get("clob_filled_buy_copy_events") or 0
    )
    active_hotlane_all_order_fallback_filled = int(
        active_hotlane_all_order_exact_copy.get("fallback_filled_buy_copy_events") or 0
    )
    active_hotlane_all_order_rejected = int(
        active_hotlane_all_order_exact_copy.get("rejected_buy_copy_events") or 0
    )
    active_hotlane_all_order_violations = int(
        active_hotlane_all_order_exact_copy.get("coverage_violations") or 0
    )
    active_hotlane_all_order_missed_lifecycle = int(
        active_hotlane_all_order_exact_copy.get("missed_lifecycle_events") or 0
    )
    active_hotlane_all_order_pass = bool(
        active_hotlane_all_order_exact_copy
        and active_hotlane_all_order_status == "PASS"
        and active_hotlane_all_order_live_truth_status == "PASS"
        and active_hotlane_all_order_source_events > 0
        and active_hotlane_all_order_buy_source_events > 0
        and active_hotlane_all_order_buy_intents == active_hotlane_all_order_buy_source_events
        and active_hotlane_all_order_clob_filled >= active_hotlane_all_order_buy_source_events
        and active_hotlane_all_order_fallback_filled == 0
        and active_hotlane_all_order_rejected == 0
        and active_hotlane_all_order_violations == 0
        and active_hotlane_all_order_missed_lifecycle == 0
        and active_hotlane_all_order_exact_copy.get("paper_only") is True
        and active_hotlane_all_order_exact_copy.get("live_orders_allowed") is False
    )
    active_hotlane_all_order_partial_blocker = bool(
        active_hotlane_all_order_exact_copy
        and not active_hotlane_all_order_pass
    )
    active_hotlane_all_order_idle_only = bool(
        active_hotlane_all_order_exact_copy
        and not active_hotlane_all_order_pass
        and active_hotlane_all_order_source_events == 0
        and active_hotlane_all_order_buy_source_events == 0
        and set(active_hotlane_all_order_exact_copy.get("live_truth_blockers") or []).issubset(
            {
                "no_current_all_order_source_events",
                "no_current_all_order_buy_events",
            }
        )
    )
    active_hotlane_hot_path_blockers = [
        str(row)
        for row in (active_hotlane_hot_path_adaptive.get("blockers") or [])
        if row
    ]
    active_hotlane_hot_path_idle_only = (
        bool(active_hotlane_hot_path_blockers)
        and set(active_hotlane_hot_path_blockers).issubset({"no_current_poll_moves"})
    )
    active_hotlane_tick_summary = (
        active_hotlane_tick.get("summary") if isinstance(active_hotlane_tick.get("summary"), dict) else {}
    )
    active_hotlane_tick_hot_path_pass = bool(
        active_hotlane_tick
        and active_hotlane_tick.get("status") == "PASS"
        and (
            (
                int(active_hotlane_tick_summary.get("best_hot_path_pass_signals") or 0) > 0
                and int(active_hotlane_tick_summary.get("best_hot_path_intents_created") or 0) > 0
                and int(active_hotlane_tick_summary.get("best_hot_path_filled_orders") or 0)
                >= int(active_hotlane_tick_summary.get("best_hot_path_intents_created") or 0)
                and int(active_hotlane_tick_summary.get("best_hot_path_rejected_orders") or 0) == 0
            )
            or (
                int(active_hotlane_tick_summary.get("best_hot_path_inventory_intents_created") or 0) > 0
                and int(active_hotlane_tick_summary.get("best_hot_path_inventory_filled_orders") or 0)
                >= int(active_hotlane_tick_summary.get("best_hot_path_inventory_intents_created") or 0)
                and int(active_hotlane_tick_summary.get("best_hot_path_inventory_rejected_orders") or 0) == 0
            )
        )
    )
    active_hotlane_hot_path_summary = (
        active_hotlane_hot_path_adaptive.get("summary")
        if isinstance(active_hotlane_hot_path_adaptive.get("summary"), dict)
        else {}
    )
    active_hotlane_tick_single_wallet_pass = bool(
        active_hotlane_tick
        and int(active_hotlane_tick_summary.get("best_single_wallet_exact_copy_intents") or 0) > 0
        and int(active_hotlane_tick_summary.get("best_single_wallet_exact_copy_filled_orders") or 0)
        >= int(active_hotlane_tick_summary.get("best_single_wallet_exact_copy_intents") or 0)
        and int(active_hotlane_tick_summary.get("best_single_wallet_exact_copy_rejected_orders") or 0) == 0
    )
    active_hotlane_single_wallet_state_intents = int(
        active_hotlane_hot_path_summary.get("hot_path_single_wallet_exact_copy_intents_created") or 0
    )
    active_hotlane_single_wallet_state_filled = int(
        active_hotlane_hot_path_summary.get("hot_path_single_wallet_exact_copy_filled_orders") or 0
    )
    active_hotlane_single_wallet_state_rejected = int(
        active_hotlane_hot_path_summary.get("hot_path_single_wallet_exact_copy_rejected_orders") or 0
    )
    active_hotlane_single_wallet_tick_intents = int(
        active_hotlane_tick_summary.get("best_single_wallet_exact_copy_intents") or 0
    )
    active_hotlane_single_wallet_tick_filled = int(
        active_hotlane_tick_summary.get("best_single_wallet_exact_copy_filled_orders") or 0
    )
    active_hotlane_single_wallet_tick_rejected = int(
        active_hotlane_tick_summary.get("best_single_wallet_exact_copy_rejected_orders") or 0
    )
    active_hotlane_single_wallet_state_pass = bool(
        active_hotlane_hot_path_adaptive
        and active_hotlane_single_wallet_state_intents > 0
        and active_hotlane_single_wallet_state_filled >= active_hotlane_single_wallet_state_intents
        and active_hotlane_single_wallet_state_rejected == 0
    )
    active_hotlane_single_wallet_blockers: list[str] = []
    if not (active_hotlane_single_wallet_state_pass or active_hotlane_tick_single_wallet_pass):
        if (
            active_hotlane_single_wallet_state_intents <= 0
            and active_hotlane_single_wallet_tick_intents <= 0
        ):
            active_hotlane_single_wallet_blockers.append("no_fresh_single_wallet_exact_copy_intents")
        if active_hotlane_single_wallet_state_rejected > 0 or active_hotlane_single_wallet_tick_rejected > 0:
            active_hotlane_single_wallet_blockers.append("single_wallet_exact_copy_rejected_orders")
        if (
            active_hotlane_single_wallet_state_intents > 0
            and active_hotlane_single_wallet_state_filled < active_hotlane_single_wallet_state_intents
        ) or (
            active_hotlane_single_wallet_tick_intents > 0
            and active_hotlane_single_wallet_tick_filled < active_hotlane_single_wallet_tick_intents
        ):
            active_hotlane_single_wallet_blockers.append(
                "single_wallet_exact_copy_paper_lifecycle_not_fully_filled"
            )
        if not active_hotlane_single_wallet_blockers:
            active_hotlane_single_wallet_blockers.append("single_wallet_exact_copy_evidence_not_pass")
    active_hotlane_hot_path_state_pass = bool(
        active_hotlane_hot_path_adaptive
        and active_hotlane_hot_path_adaptive.get("status") == "PASS"
        and (
            (
                int(active_hotlane_hot_path_summary.get("pass_signals") or 0) > 0
                and int(active_hotlane_hot_path_summary.get("hot_path_intents_created") or 0) > 0
                and int(active_hotlane_hot_path_summary.get("hot_path_filled_orders") or 0)
                >= int(active_hotlane_hot_path_summary.get("hot_path_intents_created") or 0)
                and int(active_hotlane_hot_path_summary.get("hot_path_rejected_orders") or 0) == 0
            )
            or (
                int(active_hotlane_hot_path_summary.get("hot_path_inventory_intents_created") or 0) > 0
                and int(active_hotlane_hot_path_summary.get("hot_path_inventory_filled_orders") or 0)
                >= int(active_hotlane_hot_path_summary.get("hot_path_inventory_intents_created") or 0)
                and int(active_hotlane_hot_path_summary.get("hot_path_inventory_rejected_orders") or 0) == 0
            )
        )
    )
    active_hotlane_hot_path_freshness = (
        active_hotlane_hot_path_summary.get("freshness_diagnostics")
        if isinstance(active_hotlane_hot_path_summary.get("freshness_diagnostics"), dict)
        else {}
    )
    active_hotlane_hot_path_paper = (
        active_hotlane_hot_path_adaptive.get("paper_lifecycle")
        if isinstance(active_hotlane_hot_path_adaptive.get("paper_lifecycle"), dict)
        else {}
    )
    active_hotlane_live_event_log_summary = _live_event_log_summary(active_hotlane_live_tracking_event_log_path)
    direct_source_spot_check_enabled = bool(_arg(args, "direct_source_spot_check", False))
    direct_source_spot_check = _direct_data_source_spot_check(
        active_hotlane_live_tracking_event_log_path,
        enabled=direct_source_spot_check_enabled,
        timeout_s=float(_arg(args, "direct_source_timeout_s", 3.0)),
        retries=int(_arg(args, "direct_source_retries", 2)),
    )
    direct_source_spot_check_gate_pass = (
        not direct_source_spot_check_enabled or direct_source_spot_check.get("status") == "PASS"
    )
    active_hotlane_required_zero_metric_keys = (
        "fallback_filled_buy_copy_events",
        "rejected_buy_copy_events",
        "missed_buy_copy_events",
        "lifecycle_missed_events",
    )
    active_hotlane_required_metric_keys = (
        "source_buy_events",
        "required_buy_copy_events",
        "clob_filled_buy_copy_events",
        *active_hotlane_required_zero_metric_keys,
    )
    active_hotlane_metrics_complete = _has_keys(
        active_hotlane_tracker_copy_eff_summary,
        active_hotlane_required_metric_keys,
    )
    active_hotlane_required_buys = int(active_hotlane_tracker_copy_eff_summary.get("required_buy_copy_events") or 0)
    active_hotlane_clob_filled = int(active_hotlane_tracker_copy_eff_summary.get("clob_filled_buy_copy_events") or 0)
    active_hotlane_fallback_filled = int(active_hotlane_tracker_copy_eff_summary.get("fallback_filled_buy_copy_events") or 0)
    active_hotlane_rejected = int(active_hotlane_tracker_copy_eff_summary.get("rejected_buy_copy_events") or 0)
    active_hotlane_missed = int(active_hotlane_tracker_copy_eff_summary.get("missed_buy_copy_events") or 0)
    active_hotlane_lifecycle_missed = int(active_hotlane_tracker_copy_eff_summary.get("lifecycle_missed_events") or 0)
    active_hotlane_runtime_status = str(active_hotlane_poll_runtime.get("status") or "")
    active_hotlane_forward_copy_efficiency_pass = bool(
        active_hotlane_tracker_summary
        and jsonl_counts["active_hotlane_live_tracking_event_log"] > 0
        and active_hotlane_live_event_log_summary.get("copy_efficiency_rows", 0) >= active_hotlane_required_buys
        and active_hotlane_metrics_complete
        and active_hotlane_tracker_copy_efficiency.get("status") == "PASS"
        and active_hotlane_required_buys > 0
        and active_hotlane_clob_filled >= active_hotlane_required_buys
        and active_hotlane_fallback_filled == 0
        and active_hotlane_rejected == 0
        and active_hotlane_missed == 0
        and active_hotlane_lifecycle_missed == 0
        and active_hotlane_runtime_status == "OK"
        and active_hotlane_tracker_summary.get("paper_only") is True
        and active_hotlane_tracker_summary.get("live_orders_allowed") is False
    )
    active_hotlane_forward_copy_efficiency_partial_blocker = bool(
        active_hotlane_tracker_summary
        and (
            active_hotlane_tracker_copy_efficiency.get("status") not in {None, "PASS"}
            or not active_hotlane_metrics_complete
            or jsonl_counts["active_hotlane_live_tracking_event_log"] <= 0
            or active_hotlane_live_event_log_summary.get("copy_efficiency_rows", 0) < active_hotlane_required_buys
            or active_hotlane_required_buys <= 0
            or active_hotlane_clob_filled < active_hotlane_required_buys
            or active_hotlane_fallback_filled > 0
            or active_hotlane_rejected > 0
            or active_hotlane_missed > 0
            or active_hotlane_lifecycle_missed > 0
            or active_hotlane_runtime_status != "OK"
            or active_hotlane_tracker_summary.get("paper_only") is not True
            or active_hotlane_tracker_summary.get("live_orders_allowed") is not False
        )
    )
    active_hotlane_tracking_partial_blocker = bool(
        active_hotlane_tracker_summary
        and (
            active_hotlane_tracker_copy_efficiency.get("status") != "PASS"
            or active_hotlane_tracker_summary.get("mirror_coverage_status") != "PASS"
            or not active_hotlane_metrics_complete
            or jsonl_counts["active_hotlane_live_tracking_event_log"] <= 0
            or active_hotlane_live_event_log_summary.get("copy_efficiency_rows", 0) < active_hotlane_required_buys
            or active_hotlane_required_buys <= 0
            or active_hotlane_clob_filled < active_hotlane_required_buys
            or active_hotlane_fallback_filled > 0
            or active_hotlane_rejected > 0
            or active_hotlane_missed > 0
            or active_hotlane_lifecycle_missed > 0
            or active_hotlane_runtime_status != "OK"
        )
    )
    active_forward_probe_selected = (
        active_forward_probe_profit.get("selected_probe")
        if isinstance(active_forward_probe_profit.get("selected_probe"), dict)
        else {}
    )
    active_forward_probe_candidates = (
        active_forward_probe_profit.get("ranked_probe_candidates")
        if isinstance(active_forward_probe_profit.get("ranked_probe_candidates"), list)
        else []
    )
    active_forward_probe_tracker_summary = (
        active_forward_probe_tracker.get("summary")
        if isinstance(active_forward_probe_tracker.get("summary"), dict)
        else {}
    )
    active_forward_probe_copy_efficiency = (
        active_forward_probe_tracker_summary.get("copy_efficiency")
        if isinstance(active_forward_probe_tracker_summary.get("copy_efficiency"), dict)
        else {}
    )
    active_forward_probe_copy_eff_summary = (
        active_forward_probe_copy_efficiency.get("summary")
        if isinstance(active_forward_probe_copy_efficiency.get("summary"), dict)
        else {}
    )
    active_forward_probe_current_poll = (
        active_forward_probe_copy_efficiency.get("current_poll")
        if isinstance(active_forward_probe_copy_efficiency.get("current_poll"), dict)
        else {}
    )
    active_forward_probe_current_summary = (
        active_forward_probe_current_poll.get("summary")
        if isinstance(active_forward_probe_current_poll.get("summary"), dict)
        else {}
    )
    active_forward_probe_wallet_reports = (
        active_forward_probe_tracker_summary.get("wallet_reports")
        if isinstance(active_forward_probe_tracker_summary.get("wallet_reports"), list)
        else []
    )
    active_forward_probe_event_log_summary = _live_event_log_summary(
        active_forward_probe_live_tracking_event_log_path
    )
    active_forward_probe_wallet_report_summary = _wallet_report_summary(
        [row for row in active_forward_probe_wallet_reports if isinstance(row, dict)],
        event_log_summary=active_forward_probe_event_log_summary,
    )
    active_forward_probe_required_buys = int(
        active_forward_probe_copy_eff_summary.get("required_buy_copy_events") or 0
    )
    active_forward_probe_clob_filled = int(
        active_forward_probe_copy_eff_summary.get("clob_filled_buy_copy_events") or 0
    )
    active_forward_probe_fallback_filled = int(
        active_forward_probe_copy_eff_summary.get("fallback_filled_buy_copy_events") or 0
    )
    active_forward_probe_rejected = int(
        active_forward_probe_copy_eff_summary.get("rejected_buy_copy_events") or 0
    )
    active_forward_probe_missed = int(active_forward_probe_copy_eff_summary.get("missed_buy_copy_events") or 0)
    active_forward_probe_pass = bool(
        active_forward_probe_selected
        and active_forward_probe_tracker_summary
        and jsonl_counts["active_forward_probe_live_tracking_event_log"] > 0
        and active_forward_probe_copy_efficiency.get("status") == "PASS"
        and active_forward_probe_required_buys > 0
        and active_forward_probe_clob_filled >= active_forward_probe_required_buys
        and active_forward_probe_fallback_filled == 0
        and active_forward_probe_rejected == 0
        and active_forward_probe_missed == 0
        and active_forward_probe_tracker.get("paper_only") is True
        and active_forward_probe_tracker.get("live_orders_allowed") is False
    )
    active_forward_probe_blockers: list[str] = []
    if not active_forward_probe_selected:
        active_forward_probe_blockers.append("no_active_forward_probe_candidate_selected")
    if not active_forward_probe_tracker_summary:
        active_forward_probe_blockers.append("active_forward_probe_tracker_state_missing")
    if active_forward_probe_copy_efficiency.get("status") != "PASS":
        active_forward_probe_blockers.append("copy_efficiency_not_pass")
    if active_forward_probe_required_buys <= 0:
        active_forward_probe_blockers.append("no_required_active_forward_probe_buy_evidence")
    if active_forward_probe_clob_filled < active_forward_probe_required_buys:
        active_forward_probe_blockers.append("active_forward_probe_clob_fill_gap")
    if active_forward_probe_fallback_filled > 0:
        active_forward_probe_blockers.append("active_forward_probe_fallback_fill_present")
    if active_forward_probe_rejected > 0:
        active_forward_probe_blockers.append("active_forward_probe_rejected_buy_present")
    if active_forward_probe_missed > 0:
        active_forward_probe_blockers.append("active_forward_probe_missed_buy_present")
    if active_forward_probe_tracker and active_forward_probe_tracker.get("paper_only") is not True:
        active_forward_probe_blockers.append("active_forward_probe_not_paper_only")
    if active_forward_probe_tracker and active_forward_probe_tracker.get("live_orders_allowed") is not False:
        active_forward_probe_blockers.append("active_forward_probe_live_orders_allowed")
    active_forward_probe_waiting_for_fresh_buy = bool(
        active_forward_probe_selected
        and active_forward_probe_tracker_summary
        and set(active_forward_probe_blockers).issubset(
            {
                "copy_efficiency_not_pass",
                "no_required_active_forward_probe_buy_evidence",
            }
        )
    )
    hotlane_paths = {
        "active_hotlane_live_tracking_state": active_hotlane_live_tracking_state_path,
        "active_hotlane_live_tracking_event_log": active_hotlane_live_tracking_event_log_path,
        "active_hotlane_paper_state": active_hotlane_paper_state_path,
        "active_hotlane_paper_event_log": active_hotlane_paper_event_log_path,
    }
    canonical_paths = {
        "live_tracking_state": str(args.live_tracking_state),
        "live_tracking_event_log": str(args.live_tracking_event_log),
        "paper_state": str(args.paper_state),
        "paper_event_log": str(args.paper_event_log),
    }
    hotlane_path_collisions = [
        {"active_path": active_name, "canonical_path": canonical_name, "path": active_path}
        for active_name, active_path in hotlane_paths.items()
        for canonical_name, canonical_path in canonical_paths.items()
        if str(active_path) == str(canonical_path)
    ]
    copy_efficiency_blockers = list(copy_efficiency.get("blockers") or [])
    if event_log_tracked_moves > 0 and "no_wallet_events_observed" in copy_efficiency_blockers:
        copy_efficiency_blockers = [
            blocker for blocker in copy_efficiency_blockers if blocker != "no_wallet_events_observed"
        ]
        copy_efficiency_blockers.append("final_poll_empty_but_event_log_has_wallet_events")
        if required_buys == 0:
            copy_efficiency_blockers.append("no_fresh_required_buy_copy_evidence")

    checks: dict[str, dict[str, Any]] = {
        "mission_contract": {
            **mission_check,
            "primary_profit_hypothesis": mission.get("primary_profit_hypothesis"),
            "paper_live_contract": mission.get("paper_live_contract"),
            "out_of_scope_as_primary_authority": mission.get("out_of_scope_as_primary_authority"),
        },
        "history_ingest": {
            "status": _status(bool(history_events and history_intents and history_wallets)),
            "wallets": len(history_wallets),
            "events": len(history_events),
            "copy_intents": len(history_intents),
            "event_log_lines": jsonl_counts["history_event_log"],
        },
        "history_resume_coverage": {
            "status": "FAIL"
            if pipeline_resume.get("status") == "CORRECTION"
            else _status(bool(pipeline_resume and pipeline_resume.get("status") == "PASS")),
            "state_path": pipeline_resume_state_path,
            "state_status": pipeline_resume.get("status") or "MISSING",
            "blockers": pipeline_resume.get("blockers") or ["pipeline_resume_state_missing_or_incomplete"],
            "offset": pipeline_resume.get("offset"),
            "next_offset": pipeline_resume.get("next_offset"),
            "current_wallet_count": pipeline_resume.get("current_wallet_count"),
            "slice": pipeline_resume.get("slice") or {},
            "command": {
                "returncode": _get_nested(pipeline_resume, "command", "returncode"),
                "timed_out": _get_nested(pipeline_resume, "command", "timed_out"),
                "timeout_s": _get_nested(pipeline_resume, "command", "timeout_s"),
            },
            "reason": (
                "resumable leaderboard history coverage must make measured progress; timeouts remain visible "
                "and do not advance next_offset without current-run wallet reports"
            ),
        },
        "paper_order_lifecycle": {
            "status": _status(bool(paper_orders and paper_summary)),
            "paper_orders": len(paper_orders),
            "summary_paper_orders": paper_summary.get("paper_orders"),
            "filled_orders": paper_summary.get("filled_orders"),
            "rejected_orders": paper_summary.get("rejected_orders"),
            "lifecycle_events": len(paper_lifecycle),
            "event_log_lines": jsonl_counts["paper_event_log"],
        },
        "research_cross_wallet": {
            "status": _status(bool(research.get("train_rows") and research.get("cross_wallet_windows"))),
            "train_rows": len(research.get("train_rows") or []),
            "cross_wallet_windows": len(research.get("cross_wallet_windows") or []),
            "consensus_signals": len(research.get("consensus_signals") or []),
            "inventory_plans": len(research.get("inventory_plans") or []),
        },
        "ml_dataset": {
            "status": _status(jsonl_counts["ml_dataset"] > 0),
            "rows": jsonl_counts["ml_dataset"],
        },
        "profit_admission": {
            "status": _status(bool(profit_decision and best_candidate and raw_baseline)),
            "decision_status": profit_decision.get("status"),
            "live_admission_status": profit_decision.get("live_admission_status"),
            "live_admission_blockers": profit_decision.get("live_admission_blockers") or [],
            "live_tracker_truth_status": live_tracker_truth.get("status"),
            "live_tracker_truth_blockers": live_tracker_truth.get("blockers") or [],
            "best_candidate_status": best_candidate.get("status"),
            "best_candidate_blockers": best_candidate.get("blockers") or [],
            "raw_baseline_roi_pct": raw_baseline.get("roi_pct"),
            "best_candidate_roi_pct": (best_candidate.get("summary") or {}).get("roi_pct")
            if isinstance(best_candidate.get("summary"), dict)
            else None,
        },
        "live_tracker_copy_efficiency": {
            "status": "FAIL"
            if copy_efficiency.get("status") == "FAIL"
            else _status(
                bool(
                    copy_efficiency
                    and jsonl_counts["live_tracking_event_log"] > 0
                    and copy_efficiency.get("status") == "PASS"
                )
            ),
            "logging_present": bool(copy_efficiency and jsonl_counts["live_tracking_event_log"] > 0),
            "mirror_coverage_status": tracker_summary.get("mirror_coverage_status"),
            "mirror_required_events": tracker_summary.get("mirror_required_events"),
            "mirrored_events": tracker_summary.get("mirrored_events"),
            "coverage_violations": len(tracker_summary.get("coverage_violations") or []),
            "copy_efficiency_status": copy_efficiency.get("status"),
            "copy_efficiency_blockers": copy_efficiency_blockers,
            "buy_execution_status": copy_eff_summary.get("buy_execution_status"),
            "buy_execution_blockers": copy_eff_summary.get("buy_execution_blockers") or [],
            "required_buy_copy_events": required_buys,
            "clob_filled_buy_copy_events": clob_filled,
            "fallback_filled_buy_copy_events": fallback_filled,
            "rejected_buy_copy_events": rejected,
            "missed_buy_copy_events": missed,
            "copyability_filtered_buy_events": copyability_filtered,
            "source_buy_events": source_buy_events,
            "fresh_buy_rows_le_10s": fresh_buy_rows_le_10s,
            "fresh_buy_rows_le_30s": fresh_buy_rows_le_30s,
            "stale_buy_rows_gt_300s": stale_buy_rows_gt_300s,
            "latest_buy_event_lag_s": latest_buy_event_lag_s,
            "onboarding_copy_verdict": onboarding_copy_verdict,
            "copyability_reason_counts": copy_eff_summary.get("copyability_reason_counts")
            or live_event_log_summary.get("copyability_reason_counts")
            or {},
            "api_latency_avg_s": copy_eff_summary.get("api_latency_avg_s"),
            "observed_latency_avg_s": observed_latency_avg,
            "admission_latency_avg_s": latency_avg,
            "admission_latency_basis": latency_basis_for_gate,
            "required_event_age_avg_s": copy_eff_summary.get("required_event_age_avg_s"),
            "required_event_age_p95_s": copy_eff_summary.get("required_event_age_p95_s"),
            "required_event_age_max_s": copy_eff_summary.get("required_event_age_max_s"),
            "event_age_avg_s": copy_eff_summary.get("event_age_avg_s")
            if copy_eff_summary.get("event_age_avg_s") is not None
            else live_event_log_summary.get("event_age_avg_s"),
            "observed_event_age_p95_s": copy_eff_summary.get("observed_event_age_p95_s")
            if copy_eff_summary.get("observed_event_age_p95_s") is not None
            else live_event_log_summary.get("event_age_p95_s"),
            "event_age_p95_s": copy_eff_summary.get("event_age_p95_s")
            if copy_eff_summary.get("event_age_p95_s") is not None
            else live_event_log_summary.get("event_age_p95_s"),
            "event_age_max_s": copy_eff_summary.get("event_age_max_s")
            if copy_eff_summary.get("event_age_max_s") is not None
            else live_event_log_summary.get("event_age_max_s"),
            "latency_basis": copy_eff_summary.get("latency_basis"),
            "tracker_latency": tracker_latency,
            "wallet_api_error_count": len(wallet_api_errors),
            "wallet_api_errors": wallet_api_errors[:10],
            "rejection_reason_counts": copy_eff_summary.get("rejection_reason_counts") or {},
            "fill_blocker_counts": copy_eff_summary.get("fill_blocker_counts") or {},
            "missed_copy_reason_counts": copy_eff_summary.get("missed_copy_reason_counts")
            or live_event_log_summary.get("missed_copy_reason_counts")
            or {},
            "copyability_filtered_clob_book_status_counts": copyability_filtered_clob_counts,
            "fresh_buy_clob_book_status_counts": copy_eff_summary.get("clob_book_status_counts_for_fresh_buys") or {},
            "stale_buy_clob_book_status_counts": copy_eff_summary.get("clob_book_status_counts_for_stale_buys") or {},
            "event_log_lines": jsonl_counts["live_tracking_event_log"],
            "live_event_log_summary": live_event_log_summary,
            "evidence_status_counts": evidence_counts,
            "wallet_report_summary": wallet_report_summary,
            "buy_fill_coverage": tracker_summary.get("buy_fill_coverage") or {},
            "mirror_coverage_by_action": tracker_summary.get("mirror_coverage_by_action") or {},
            "lifecycle_miss_class_counts": tracker_summary.get("lifecycle_miss_class_counts") or {},
        },
        "live_admission_truth": {
            "status": _status(
                bool(
                    profit_decision.get("live_admission_status") == "PASS"
                    and copy_efficiency.get("status") == "PASS"
                    and required_buys > 0
                    and clob_filled >= required_buys
                    and fallback_filled == 0
                    and rejected == 0
                    and missed == 0
                    and latency_avg <= float(args.max_learning_latency_s)
                ),
                blocked=True,
            ),
            "reason": "requires profitable candidate plus CLOB-backed copy-efficiency PASS; fallback fills never count",
            "live_admission_blockers": profit_decision.get("live_admission_blockers") or [],
            "live_tracker_truth_status": live_tracker_truth.get("status"),
            "live_tracker_truth_blockers": live_tracker_truth.get("blockers") or [],
            "candidate_forward_truth_status": candidate_forward_live_tracker_truth.get("status"),
            "candidate_forward_truth_blockers": candidate_forward_live_tracker_truth.get("blockers") or [],
            "forward_candidate_truth_status": forward_candidate_live_tracker_truth.get("status"),
            "forward_candidate_truth_blockers": forward_candidate_live_tracker_truth.get("blockers") or [],
            "forward_candidate_source_wallet": forward_candidate.get("source_wallet"),
            "forward_tracking_queue_size": len(forward_tracking_queue),
            "forward_tracking_queue_wallets": [
                row.get("source_wallet") for row in forward_tracking_queue if isinstance(row, dict)
            ],
            "effective_live_tracker_truth_status": effective_live_tracker_truth.get("status"),
            "live_tracker_truth_source": profit_decision.get("live_tracker_truth_source"),
        },
        "leaderboard_discovery": {
            "status": _status(bool(leaderboard.get("candidate_wallets") or leaderboard.get("leaderboard_rows"))),
            "state_status": leaderboard.get("status"),
            "candidate_wallets": len(leaderboard.get("candidate_wallets") or []),
            "leaderboard_rows": len(leaderboard.get("leaderboard_rows") or []),
            "top_wallets": len(leaderboard.get("top_wallets") or []),
            "pipeline_requested": leaderboard.get("pipeline_requested"),
        },
        "operator_onboarding": {
            "status": onboarding_status,
            "state_status": onboarding.get("status"),
            "blockers": onboarding_blockers,
            "command_results": onboarding.get("command_results") or [],
            "paper_only": onboarding.get("paper_only"),
            "live_orders_allowed": onboarding.get("live_orders_allowed"),
        },
        "active_hotlane_scope": {
            "status": _status(
                bool(
                    active_hotlane
                    and active_hotlane_status == "PASS"
                    and int(active_hotlane_summary.get("selected_wallets") or 0) > 0
                    and Path(active_hotlane_registry_path).exists()
                )
            ),
            "state_status": active_hotlane_status or "MISSING",
            "blockers": active_hotlane.get("blockers") if active_hotlane else ["active_hotlane_state_missing"],
            "registry_path": active_hotlane_registry_path,
            "registry_wallets": active_hotlane_summary.get("registry_wallets"),
            "live_log_rows_read": active_hotlane_summary.get("live_log_rows_read"),
            "scored_wallets": active_hotlane_summary.get("scored_wallets"),
            "selected_wallets": active_hotlane_summary.get("selected_wallets"),
            "top_selected_wallets": [
                {
                    "address": row.get("address"),
                    "score": row.get("score"),
                    "live_score": row.get("live_score"),
                    "history_score": row.get("history_score"),
                    "leaderboard_score": row.get("leaderboard_score"),
                    "profit_score": row.get("profit_score"),
                    "latest_live_event_lag_s": row.get("latest_live_event_lag_s"),
                    "selection_reasons": row.get("selection_reasons"),
                }
                for row in active_hotlane_selected[:10]
                if isinstance(row, dict)
            ],
            "paper_only": active_hotlane.get("paper_only"),
            "live_orders_allowed": active_hotlane.get("live_orders_allowed"),
        },
        "active_hotlane_forward_copy_efficiency": {
            "status": _status(
                active_hotlane_forward_copy_efficiency_pass,
                blocked=active_hotlane_forward_copy_efficiency_partial_blocker,
            ),
            "state_path": active_hotlane_live_tracking_state_path,
            "event_log_path": active_hotlane_live_tracking_event_log_path,
            "event_log_lines": jsonl_counts["active_hotlane_live_tracking_event_log"],
            "copy_efficiency_status": active_hotlane_tracker_copy_efficiency.get("status"),
            "copy_efficiency_blockers": active_hotlane_tracker_copy_efficiency.get("blockers") or [],
            "required_buy_copy_events": active_hotlane_required_buys,
            "clob_filled_buy_copy_events": active_hotlane_clob_filled,
            "fallback_filled_buy_copy_events": active_hotlane_fallback_filled,
            "rejected_buy_copy_events": active_hotlane_rejected,
            "missed_buy_copy_events": active_hotlane_missed,
            "source_buy_events": active_hotlane_tracker_copy_eff_summary.get("source_buy_events"),
            "fresh_buy_rows_le_10s": active_hotlane_tracker_copy_eff_summary.get(
                "source_fresh_buy_events_le_10s"
            ),
            "latest_buy_event_lag_s": active_hotlane_tracker_copy_eff_summary.get("latest_buy_event_lag_s"),
            "required_event_age_p95_s": active_hotlane_tracker_copy_eff_summary.get(
                "required_event_age_p95_s"
            ),
            "required_event_age_max_s": active_hotlane_tracker_copy_eff_summary.get(
                "required_event_age_max_s"
            ),
            "copyability_filtered_buy_events": active_hotlane_tracker_copy_eff_summary.get(
                "copyability_filtered_buy_events"
            ),
            "copyability_reason_counts": active_hotlane_tracker_copy_eff_summary.get(
                "copyability_reason_counts"
            )
            or {},
            "paper_only": active_hotlane_tracker_summary.get("paper_only"),
            "live_orders_allowed": active_hotlane_tracker_summary.get("live_orders_allowed"),
            "role": "forward_active_hotlane_copyability_measurement_not_live_admission_truth",
            "reason": (
                "active hot-lane forward copy-efficiency is tracked separately from canonical candidate "
                "admission: it can PASS as paper-only CLOB-backed copyability evidence while live admission "
                "still stays blocked by profit, walk-forward, and candidate-specific tracker gates"
            ),
        },
        "active_forward_candidate_probe": {
            "status": _status(
                active_forward_probe_pass,
                blocked=bool(active_forward_probe_blockers) and not active_forward_probe_waiting_for_fresh_buy,
            ),
            "profit_state_path": active_forward_probe_profit_state_path,
            "state_path": active_forward_probe_live_tracking_state_path,
            "event_log_path": active_forward_probe_live_tracking_event_log_path,
            "event_log_lines": jsonl_counts["active_forward_probe_live_tracking_event_log"],
            "selected_probe": active_forward_probe_selected,
            "ranked_probe_candidates": len(active_forward_probe_candidates),
            "copy_efficiency_status": active_forward_probe_copy_efficiency.get("status"),
            "copy_efficiency_blockers": active_forward_probe_copy_efficiency.get("blockers") or [],
            "blockers": active_forward_probe_blockers,
            "required_buy_copy_events": active_forward_probe_required_buys,
            "clob_filled_buy_copy_events": active_forward_probe_clob_filled,
            "fallback_filled_buy_copy_events": active_forward_probe_fallback_filled,
            "rejected_buy_copy_events": active_forward_probe_rejected,
            "missed_buy_copy_events": active_forward_probe_missed,
            "source_buy_events": active_forward_probe_copy_eff_summary.get("source_buy_events"),
            "fresh_buy_rows_le_10s": active_forward_probe_copy_eff_summary.get(
                "source_fresh_buy_events_le_10s"
            ),
            "fresh_buy_rows_le_30s": active_forward_probe_copy_eff_summary.get(
                "source_fresh_buy_events_le_30s"
            ),
            "latest_buy_event_lag_s": active_forward_probe_copy_eff_summary.get("latest_buy_event_lag_s"),
            "copyability_filtered_buy_events": active_forward_probe_copy_eff_summary.get(
                "copyability_filtered_buy_events"
            ),
            "copyability_reason_counts": active_forward_probe_copy_eff_summary.get(
                "copyability_reason_counts"
            )
            or active_forward_probe_event_log_summary.get("copyability_reason_counts")
            or {},
            "profit_policy_accepted_buy_events": active_forward_probe_copy_eff_summary.get(
                "profit_policy_accepted_buy_events"
            ),
            "profit_policy_accepted_fresh_buy_events_le_10s": active_forward_probe_copy_eff_summary.get(
                "profit_policy_accepted_fresh_buy_events_le_10s"
            ),
            "profit_policy_rejected_buy_events": active_forward_probe_copy_eff_summary.get(
                "profit_policy_rejected_buy_events"
            ),
            "profit_policy_rejection_reason_counts": active_forward_probe_copy_eff_summary.get(
                "profit_policy_rejection_reason_counts"
            )
            or {},
            "event_age_bucket_counts": active_forward_probe_copy_eff_summary.get("event_age_bucket_counts")
            or {},
            "current_poll_status": active_forward_probe_current_poll.get("status"),
            "current_poll_blockers": active_forward_probe_current_poll.get("blockers") or [],
            "current_poll_source_buy_events": active_forward_probe_current_summary.get("source_buy_events"),
            "current_poll_profit_policy_accepted_buy_events": active_forward_probe_current_summary.get(
                "profit_policy_accepted_buy_events"
            ),
            "current_poll_copyability_reason_counts": active_forward_probe_current_summary.get(
                "copyability_reason_counts"
            )
            or {},
            "live_event_log_summary": active_forward_probe_event_log_summary,
            "wallet_report_summary": active_forward_probe_wallet_report_summary,
            "paper_only": active_forward_probe_tracker.get("paper_only"),
            "live_orders_allowed": active_forward_probe_tracker.get("live_orders_allowed"),
            "role": "active_historic_candidate_forward_probe_not_live_admission_truth",
            "reason": (
                "measures a currently active hot-lane candidate separately from canonical best-candidate "
                "admission, so stale historical candidates stay blocked while the system still learns from "
                "live-active profitable-wallet evidence"
            ),
        },
        "active_hotlane_all_order_exact_copy": {
            "status": _status(
                active_hotlane_all_order_pass,
                blocked=active_hotlane_all_order_partial_blocker and not active_hotlane_all_order_idle_only,
            ),
            "state_path": active_hotlane_live_tracking_state_path,
            "paper_state_path": active_hotlane_all_order_exact_copy.get("paper_state_path"),
            "paper_event_log_path": active_hotlane_all_order_exact_copy.get("paper_event_log_path"),
            "tracker_status": active_hotlane_all_order_status,
            "live_truth_status": active_hotlane_all_order_live_truth_status,
            "live_truth_blockers": active_hotlane_all_order_exact_copy.get("live_truth_blockers") or [],
            "source_events": active_hotlane_all_order_source_events,
            "buy_source_events": active_hotlane_all_order_buy_source_events,
            "buy_intents": active_hotlane_all_order_buy_intents,
            "filled_buy_copy_events": active_hotlane_all_order_exact_copy.get("filled_buy_copy_events"),
            "clob_filled_buy_copy_events": active_hotlane_all_order_clob_filled,
            "fallback_filled_buy_copy_events": active_hotlane_all_order_fallback_filled,
            "rejected_buy_copy_events": active_hotlane_all_order_rejected,
            "coverage_violations": active_hotlane_all_order_violations,
            "missed_lifecycle_events": active_hotlane_all_order_missed_lifecycle,
            "mirror_status_counts": active_hotlane_all_order_exact_copy.get("mirror_status_counts") or {},
            "violation_reason_counts": active_hotlane_all_order_exact_copy.get("violation_reason_counts") or {},
            "lifecycle_miss_class_counts": active_hotlane_all_order_exact_copy.get("lifecycle_miss_class_counts") or {},
            "fill_source_counts": active_hotlane_all_order_exact_copy.get("fill_source_counts") or {},
            "filled_fill_source_counts": active_hotlane_all_order_exact_copy.get("filled_fill_source_counts") or {},
            "rejected_fill_source_counts": active_hotlane_all_order_exact_copy.get("rejected_fill_source_counts") or {},
            "paper_tactic_profile_status_counts": (
                active_hotlane_all_order_exact_copy.get("paper_tactic_profile_status_counts") or {}
            ),
            "paper_tactic_profile_pass_events": active_hotlane_all_order_exact_copy.get(
                "paper_tactic_profile_pass_events"
            )
            or {},
            "execution_corrections": active_hotlane_all_order_exact_copy.get("execution_corrections") or {},
            "current_poll_execution_corrections": active_hotlane_all_order_exact_copy.get(
                "current_poll_execution_corrections"
            )
            or {},
            "cumulative_execution_corrections": active_hotlane_all_order_exact_copy.get(
                "cumulative_execution_corrections"
            )
            or {},
            "execution_tactic_plan": active_hotlane_all_order_exact_copy.get("execution_tactic_plan") or {},
            "micro_batch_all_order_probe": active_hotlane_all_order_exact_copy.get("micro_batch_all_order_probe")
            or {},
            "paper_summary": active_hotlane_all_order_exact_copy.get("paper_summary") or {},
            "paper_only": active_hotlane_all_order_exact_copy.get("paper_only"),
            "live_orders_allowed": active_hotlane_all_order_exact_copy.get("live_orders_allowed"),
            "role": "profit_policy_independent_all_observed_order_copy_truth_for_user_requested_wallet_mirroring",
            "reason": (
                "this check proves whether every observed BTC 5m wallet order in the active hot-lane was copied "
                "into a separate paper ledger; fallback-only fills remain paper research and block live-money readiness"
            ),
        },
        "active_hotlane_all_order_tactic_replay": {
            "status": _status(
                active_hotlane_all_order_tactic_overall_pass,
                blocked=bool(
                    active_hotlane_all_order_tactic_replay
                    and active_hotlane_all_order_tactic_replay_status not in {"NO_ACTION", "PASS"}
                ),
            ),
            "role": "paper_only_aggressive_all_order_tactic_replay_not_live_admission",
            "copy_lifecycle_status": "PASS" if active_hotlane_all_order_tactic_lifecycle_pass else "ANALYZE",
            "pnl_attribution_status": active_hotlane_all_order_tactic_pnl_attribution.get("status"),
            "pnl_attribution_blockers": active_hotlane_all_order_tactic_pnl_attribution.get("blockers") or [],
            "pnl_attribution": active_hotlane_all_order_tactic_pnl_attribution,
            "profile_id": active_hotlane_all_order_tactic_replay.get("profile_id"),
            "recommended_tactic": active_hotlane_all_order_tactic_replay.get("recommended_tactic"),
            "replay_intents": active_hotlane_all_order_tactic_replay_intents,
            "paper_orders": active_hotlane_all_order_tactic_replay.get("paper_orders"),
            "filled_orders": active_hotlane_all_order_tactic_replay_filled,
            "rejected_orders": active_hotlane_all_order_tactic_replay_rejected,
            "clob_filled_orders": active_hotlane_all_order_tactic_replay.get("clob_filled_orders"),
            "fallback_filled_orders": active_hotlane_all_order_tactic_replay_fallback,
            "durable_paper_orders": int(active_hotlane_all_order_tactic_replay_paper_summary.get("paper_orders") or 0),
            "durable_filled_orders": active_hotlane_all_order_tactic_replay_paper_filled,
            "durable_rejected_orders": active_hotlane_all_order_tactic_replay_paper_rejected,
            "durable_clob_filled_orders": active_hotlane_all_order_tactic_replay_paper_clob,
            "durable_fallback_filled_orders": active_hotlane_all_order_tactic_replay_paper_fallback,
            "incremental_filled_orders_vs_strict": active_hotlane_all_order_tactic_replay.get(
                "incremental_filled_orders_vs_strict"
            ),
            "incremental_source_event_ids_vs_strict": active_hotlane_all_order_tactic_replay.get(
                "incremental_source_event_ids_vs_strict"
            )
            or [],
            "event_proofs": active_hotlane_all_order_tactic_replay.get("event_proofs") or [],
            "strict_cost_usd": active_hotlane_all_order_tactic_replay.get("strict_cost_usd"),
            "tactic_cost_usd": active_hotlane_all_order_tactic_replay.get("tactic_cost_usd"),
            "cost_delta_usd": active_hotlane_all_order_tactic_replay.get("cost_delta_usd"),
            "paper_state_path": active_hotlane_all_order_tactic_replay.get("paper_state_path")
            or active_hotlane_all_order_tactic_replay_paper_state_path,
            "paper_event_log_path": active_hotlane_all_order_tactic_replay.get("paper_event_log_path")
            or active_hotlane_all_order_tactic_replay_paper_event_log_path,
            "paper_event_log_lines": jsonl_counts["active_hotlane_all_order_tactic_replay_paper_event_log"],
            "paper_only": active_hotlane_all_order_tactic_replay.get("paper_only"),
            "live_orders_allowed": active_hotlane_all_order_tactic_replay.get("live_orders_allowed"),
            "reason": (
                "this check proves whether an aggressive best-ask tactic recommendation was converted into "
                "durable paper CopyIntent lifecycle evidence and whether that tactic preserves resolved PnL; "
                "it remains research-only until both PnL attribution and normal live-gated copy truth pass"
            ),
        },
        "active_hotlane_tracking_evidence": {
            "status": _status(
                bool(
                    active_hotlane_tracker_summary
                    and active_hotlane_metrics_complete
                    and jsonl_counts["active_hotlane_live_tracking_event_log"] > 0
                    and int(active_hotlane_live_event_log_summary.get("copy_efficiency_rows") or 0)
                    >= active_hotlane_required_buys
                    and int(active_hotlane_tracker_scope.get("wallets_tracked") or active_hotlane_tracker_summary.get("wallets") or 0)
                    > 0
                    and int(active_hotlane_tracker_copy_eff_summary.get("source_buy_events") or 0) > 0
                    and active_hotlane_required_buys > 0
                    and active_hotlane_clob_filled >= active_hotlane_required_buys
                    and active_hotlane_fallback_filled == 0
                    and active_hotlane_rejected == 0
                    and active_hotlane_missed == 0
                    and active_hotlane_lifecycle_missed == 0
                    and active_hotlane_tracker_copy_efficiency.get("status") == "PASS"
                    and active_hotlane_tracker_summary.get("mirror_coverage_status") == "PASS"
                    and active_hotlane_runtime_status == "OK"
                    and direct_source_spot_check_gate_pass
                    and Path(active_hotlane_live_tracking_event_log_path).exists()
                ),
                blocked=(
                    active_hotlane_tracking_partial_blocker
                    or (
                        direct_source_spot_check_enabled
                        and direct_source_spot_check.get("status") != "PASS"
                    )
                ),
            ),
            "state_path": active_hotlane_live_tracking_state_path,
            "event_log_path": active_hotlane_live_tracking_event_log_path,
            "event_log_lines": jsonl_counts["active_hotlane_live_tracking_event_log"],
            "wallets_tracked": active_hotlane_tracker_scope.get("wallets_tracked") or active_hotlane_tracker_summary.get("wallets"),
            "new_wallet_events": active_hotlane_tracker_summary.get("new_wallet_events"),
            "mirror_coverage_status": active_hotlane_tracker_summary.get("mirror_coverage_status"),
            "copy_efficiency_status": active_hotlane_tracker_copy_efficiency.get("status"),
            "copy_efficiency_blockers": active_hotlane_tracker_copy_efficiency.get("blockers") or [],
            "source_buy_events": active_hotlane_tracker_copy_eff_summary.get("source_buy_events"),
            "required_buy_copy_events": active_hotlane_tracker_copy_eff_summary.get("required_buy_copy_events"),
            "clob_filled_buy_copy_events": active_hotlane_tracker_copy_eff_summary.get("clob_filled_buy_copy_events"),
            "fallback_filled_buy_copy_events": active_hotlane_tracker_copy_eff_summary.get("fallback_filled_buy_copy_events"),
            "rejected_buy_copy_events": active_hotlane_tracker_copy_eff_summary.get("rejected_buy_copy_events"),
            "missed_buy_copy_events": active_hotlane_tracker_copy_eff_summary.get("missed_buy_copy_events"),
            "lifecycle_missed_events": active_hotlane_tracker_copy_eff_summary.get("lifecycle_missed_events"),
            "required_metric_keys_present": active_hotlane_metrics_complete,
            "missing_required_metric_keys": [
                key for key in active_hotlane_required_metric_keys if key not in active_hotlane_tracker_copy_eff_summary
            ],
            "lifecycle_miss_class_counts": active_hotlane_tracker_summary.get("lifecycle_miss_class_counts") or {},
            "fresh_buy_rows_le_10s": active_hotlane_tracker_copy_eff_summary.get("source_fresh_buy_events_le_10s"),
            "fresh_buy_rows_le_30s": active_hotlane_tracker_copy_eff_summary.get("source_fresh_buy_events_le_30s"),
            "event_log_tracked_moves": active_hotlane_live_event_log_summary.get("tracked_move_events"),
            "event_log_source_buy_events": active_hotlane_live_event_log_summary.get("source_buy_events"),
            "event_log_copy_efficiency_rows": active_hotlane_live_event_log_summary.get("copy_efficiency_rows"),
            "poll_runtime": active_hotlane_poll_runtime,
            "direct_data_source_spot_check": direct_source_spot_check,
            "paper_only": active_hotlane_tracker_summary.get("paper_only"),
            "live_orders_allowed": active_hotlane_tracker_summary.get("live_orders_allowed"),
            "reason": (
                "active hot-lane must be 100% clean before it can be treated as OK: isolated paths, "
                "copy_efficiency PASS, mirror coverage PASS, required BUYs fully CLOB-filled, zero fallback/reject/miss, "
                "zero lifecycle misses, poll runtime OK, and direct Data API/Gamma/CLOB source spot-check PASS"
            ),
        },
        "direct_data_source_spot_check": direct_source_spot_check,
        "active_hotlane_paper_copy_contract": active_hotlane_paper_copy_contract_check,
        "active_hotlane_hot_path_adaptive": {
            "status": _status(
                bool(active_hotlane_hot_path_state_pass or active_hotlane_tick_hot_path_pass),
                blocked=bool(
                    active_hotlane_hot_path_adaptive
                    and active_hotlane_hot_path_adaptive.get("status") != "PASS"
                    and not active_hotlane_tick_hot_path_pass
                    and not active_hotlane_single_wallet_state_pass
                    and not active_hotlane_tick_single_wallet_pass
                    and not active_hotlane_hot_path_idle_only
                ),
            ),
            "state_path": active_hotlane_live_tracking_state_path,
            "tick_state_path": active_hotlane_tick_state_path,
            "state_status": active_hotlane_hot_path_adaptive.get("status") or "MISSING",
            "tick_status": active_hotlane_tick.get("status") or "MISSING",
            "state_current_poll_pass": active_hotlane_hot_path_state_pass,
            "tick_current_poll_pass": active_hotlane_tick_hot_path_pass,
            "blockers": active_hotlane_hot_path_blockers or ["hot_path_adaptive_missing_or_no_pass"],
            "role": active_hotlane_hot_path_adaptive.get("role"),
            "current_poll_moves": active_hotlane_hot_path_summary.get("current_poll_moves"),
            "eligible_moves": active_hotlane_hot_path_summary.get("eligible_moves"),
            "signals": active_hotlane_hot_path_summary.get("signals"),
            "pass_signals": active_hotlane_hot_path_summary.get("pass_signals"),
            "hot_path_intents_created": active_hotlane_hot_path_summary.get("hot_path_intents_created"),
            "hot_path_new_paper_orders": active_hotlane_hot_path_summary.get("hot_path_new_paper_orders"),
            "hot_path_paper_orders": active_hotlane_hot_path_summary.get("hot_path_paper_orders"),
            "hot_path_filled_orders": active_hotlane_hot_path_summary.get("hot_path_filled_orders"),
            "hot_path_rejected_orders": active_hotlane_hot_path_summary.get("hot_path_rejected_orders"),
            "hot_path_fill_source_counts": active_hotlane_hot_path_summary.get("hot_path_fill_source_counts") or {},
            "hot_path_signal_to_paper_latency_s": active_hotlane_hot_path_summary.get(
                "hot_path_signal_to_paper_latency_s"
            ),
            "hot_path_inventory_intents_created": active_hotlane_hot_path_summary.get(
                "hot_path_inventory_intents_created"
            ),
            "hot_path_inventory_filled_orders": active_hotlane_hot_path_summary.get(
                "hot_path_inventory_filled_orders"
            ),
            "hot_path_inventory_rejected_orders": active_hotlane_hot_path_summary.get(
                "hot_path_inventory_rejected_orders"
            ),
            "hot_path_inventory_fill_source_counts": active_hotlane_hot_path_summary.get(
                "hot_path_inventory_fill_source_counts"
            )
            or {},
            "inventory_paper_lifecycle": active_hotlane_hot_path_adaptive.get("inventory_paper_lifecycle") or {},
            "paper_lifecycle": active_hotlane_hot_path_paper,
            "tick_summary": {
                "ticks_completed": active_hotlane_tick_summary.get("ticks_completed"),
                "cohort_probe_ticks": active_hotlane_tick_summary.get("cohort_probe_ticks"),
                "best_hot_path_pass_signals": active_hotlane_tick_summary.get("best_hot_path_pass_signals"),
                "best_hot_path_intents_created": active_hotlane_tick_summary.get("best_hot_path_intents_created"),
                "best_hot_path_filled_orders": active_hotlane_tick_summary.get("best_hot_path_filled_orders"),
                "best_hot_path_rejected_orders": active_hotlane_tick_summary.get("best_hot_path_rejected_orders"),
                "best_hot_path_inventory_intents_created": active_hotlane_tick_summary.get(
                    "best_hot_path_inventory_intents_created"
                ),
                "best_hot_path_inventory_filled_orders": active_hotlane_tick_summary.get(
                    "best_hot_path_inventory_filled_orders"
                ),
                "best_hot_path_inventory_rejected_orders": active_hotlane_tick_summary.get(
                    "best_hot_path_inventory_rejected_orders"
                ),
                "best_hot_path_runtime_fresh_buy_events_le_cap": active_hotlane_tick_summary.get(
                    "best_hot_path_runtime_fresh_buy_events_le_cap"
                ),
                "best_hot_path_runtime_eligible_wallets": active_hotlane_tick_summary.get(
                    "best_hot_path_runtime_eligible_wallets"
                ),
                "best_hot_path_runtime_signal_blocker_counts": active_hotlane_tick_summary.get(
                    "best_hot_path_runtime_signal_blocker_counts"
                )
                or {},
                "best_hot_path_tracker_time_replay_pass_signals": active_hotlane_tick_summary.get(
                    "best_hot_path_tracker_time_replay_pass_signals"
                ),
                "best_adaptive_tracker_time_replay_intents": active_hotlane_tick_summary.get(
                    "best_adaptive_tracker_time_replay_intents"
                ),
                "best_adaptive_tracker_time_replay_filled_orders": active_hotlane_tick_summary.get(
                    "best_adaptive_tracker_time_replay_filled_orders"
                ),
                "best_single_wallet_exact_copy_intents": active_hotlane_tick_summary.get(
                    "best_single_wallet_exact_copy_intents"
                ),
                "best_single_wallet_exact_copy_filled_orders": active_hotlane_tick_summary.get(
                    "best_single_wallet_exact_copy_filled_orders"
                ),
                "best_single_wallet_exact_copy_rejected_orders": active_hotlane_tick_summary.get(
                    "best_single_wallet_exact_copy_rejected_orders"
                ),
            },
            "runtime_fresh_buy_events_le_cap": active_hotlane_hot_path_freshness.get(
                "runtime_fresh_buy_events_le_cap"
            ),
            "runtime_eligible_wallets": active_hotlane_hot_path_freshness.get("runtime_eligible_wallets"),
            "source_feed_delayed": active_hotlane_hot_path_freshness.get("source_feed_delayed"),
            "latest_buy_event_lag_s": active_hotlane_hot_path_freshness.get("latest_buy_event_lag_s"),
            "runtime_inventory_research_candidates": active_hotlane_hot_path_summary.get(
                "runtime_inventory_research_candidates"
            ),
            "tracker_time_inventory_research_candidates": active_hotlane_hot_path_summary.get(
                "tracker_time_inventory_research_candidates"
            ),
            "top_signals": active_hotlane_hot_path_adaptive.get("top_signals") or [],
            "top_runtime_inventory_candidates": active_hotlane_hot_path_adaptive.get(
                "top_runtime_inventory_candidates"
            )
            or [],
            "paper_only": active_hotlane_hot_path_adaptive.get("paper_only"),
            "live_orders_allowed": active_hotlane_hot_path_adaptive.get("live_orders_allowed"),
            "reason": (
                "current-poll adaptive consensus plus paper-only CopyIntent lifecycle must be visible before any "
                "wallet-derived bot can be promoted; PASS here is measurement only, not live admission"
            ),
        },
        "active_hotlane_single_wallet_exact_copy": {
            "status": _status(bool(active_hotlane_single_wallet_state_pass or active_hotlane_tick_single_wallet_pass)),
            "state_current_poll_single_wallet_pass": active_hotlane_single_wallet_state_pass,
            "tick_single_wallet_pass": active_hotlane_tick_single_wallet_pass,
            "blockers": active_hotlane_single_wallet_blockers,
            "role": "single_wallet_exact_copy_current_poll_measurement_only_not_live_admission",
            "state_intents": active_hotlane_single_wallet_state_intents,
            "state_filled_orders": active_hotlane_single_wallet_state_filled,
            "state_rejected_orders": active_hotlane_single_wallet_state_rejected,
            "state_paper_lifecycle": active_hotlane_hot_path_adaptive.get(
                "single_wallet_exact_copy_paper_lifecycle"
            )
            or {},
            "tick_best_intents": active_hotlane_single_wallet_tick_intents,
            "tick_best_filled_orders": active_hotlane_single_wallet_tick_filled,
            "tick_best_rejected_orders": active_hotlane_single_wallet_tick_rejected,
            "paper_only": active_hotlane_hot_path_adaptive.get("paper_only"),
            "live_orders_allowed": active_hotlane_hot_path_adaptive.get("live_orders_allowed"),
            "reason": (
                "single-wallet current-poll exact-copy is executable paper evidence for 1:1 copyability, "
                "but it remains separate from multi-wallet consensus and live admission"
            ),
        },
        "hotlane_path_isolation": {
            "status": _status(not hotlane_path_collisions, blocked=bool(hotlane_path_collisions)),
            "collisions": hotlane_path_collisions,
            "active_paths": hotlane_paths,
            "canonical_paths": canonical_paths,
            "reason": "active hot-lane live/paper evidence must not overwrite canonical tracker or paper state",
        },
        "active_hotlane_guard_log_retention": {
            "status": _status(
                (not active_hotlane_guard_log_check_enabled)
                or active_hotlane_guard_log_size_bytes <= active_hotlane_guard_log_max_bytes,
                blocked=(
                    active_hotlane_guard_log_check_enabled
                    and active_hotlane_guard_log_size_bytes > active_hotlane_guard_log_max_bytes
                ),
            ),
            "enabled": active_hotlane_guard_log_check_enabled,
            "path": active_hotlane_guard_log_path,
            "size_bytes": active_hotlane_guard_log_size_bytes,
            "max_bytes": active_hotlane_guard_log_max_bytes,
            "reason": (
                "active hot-lane guard output must stay bounded so runtime monitoring remains sustainable; "
                "large logs should be rotated or summarized without deleting source-of-truth JSONL evidence"
            ),
        },
        "adaptive_wallet_derived_bot": {
            "status": _status(
                bool(
                    adaptive
                    and adaptive_status == "PASS"
                    and (adaptive_own_multi_wallet_pass or adaptive_embedded_hot_path_multi_pass)
                    and adaptive_source.get("lane") == "active_hotlane"
                    and str(adaptive_source.get("live_tracking_state_path") or "")
                    == active_hotlane_live_tracking_state_path
                    and str(adaptive_source.get("live_tracking_event_log_path") or "")
                    == active_hotlane_live_tracking_event_log_path
                    and adaptive_selected_wallet_overlap > 0
                )
            ),
            "state_status": adaptive_status or "MISSING",
            "blockers": adaptive.get("blockers") or ["adaptive_bot_state_missing"],
            "embedded_tracker_hot_path": adaptive_embedded_hot_path,
            "embedded_hot_path_current_poll_multi_wallet_pass": adaptive_embedded_hot_path_multi_pass,
            "embedded_hot_path_single_wallet_exact_copy_pass": adaptive_embedded_hot_path_single_wallet_pass,
            "source_provenance": adaptive_source,
            "source_expected_lane": "active_hotlane",
            "source_expected_state_path": active_hotlane_live_tracking_state_path,
            "source_expected_event_log_path": active_hotlane_live_tracking_event_log_path,
            "source_selected_wallet_overlap": adaptive_selected_wallet_overlap,
            "moves_seen": adaptive_moves_seen,
            "eligible_moves": adaptive_eligible_moves,
            "signals": adaptive_summary.get("signals"),
            "pass_signals": adaptive_pass_signals,
            "own_multi_wallet_pass": adaptive_own_multi_wallet_pass,
            "intents": adaptive_intents,
            "paper_orders": adaptive_paper_orders,
            "filled_orders": adaptive_filled_orders,
            "rejected_orders": adaptive_rejected_orders,
            "paper_event_log_lines": jsonl_counts["adaptive_bot_paper_event_log"],
            "paper_only": adaptive.get("paper_only"),
            "live_orders_allowed": adaptive.get("live_orders_allowed"),
            "tracker_time_eligible_moves": adaptive_summary.get("tracker_time_eligible_moves"),
            "tracker_time_signals": adaptive_summary.get("tracker_time_signals"),
            "tracker_time_pass_signals": adaptive_tracker_time_pass_signals,
            "tracker_time_filter_reason_counts": adaptive_summary.get("tracker_time_filter_reason_counts") or {},
            "runtime_inventory_candidates": adaptive_summary.get("runtime_inventory_candidates"),
            "runtime_inventory_research_candidates": adaptive_runtime_inventory_research_candidates,
            "tracker_time_inventory_candidates": adaptive_summary.get("tracker_time_inventory_candidates"),
            "tracker_time_inventory_research_candidates": adaptive_tracker_time_inventory_research_candidates,
            "tracker_time_inventory_modes": adaptive_summary.get("tracker_time_inventory_modes") or {},
            "top_tracker_time_inventory_candidates": adaptive_summary.get("top_tracker_time_inventory_candidates") or [],
            "tracker_time_replay": {
                "status": adaptive_tracker_time_replay.get("status"),
                "blockers": adaptive_tracker_time_replay.get("blockers") or [],
                "role": adaptive_tracker_time_replay.get("role"),
                "intents": adaptive_tracker_time_replay_intents,
                "paper_orders": adaptive_tracker_time_replay_summary.get("paper_orders"),
                "filled_orders": adaptive_tracker_time_replay_filled_orders,
                "rejected_orders": adaptive_tracker_time_replay_rejected_orders,
                "paper_event_log_lines": jsonl_counts["adaptive_tracker_time_replay_paper_event_log"],
                "reason": "tracker-time replay is research evidence only and does not satisfy live-admission truth",
            },
            "intents": adaptive_intents,
            "paper_orders": adaptive_summary.get("paper_orders"),
            "filled_orders": adaptive_filled_orders,
            "rejected_orders": adaptive_rejected_orders,
            "filter_reason_counts": adaptive_filter_reasons,
            "adaptive_dynamic_event_age_p95_s": adaptive_summary.get("dynamic_event_age_p95_s"),
            "adaptive_dynamic_event_age_avg_s": adaptive_summary.get("dynamic_event_age_avg_s"),
            "adaptive_dynamic_event_age_max_s": adaptive_summary.get("dynamic_event_age_max_s"),
            "adaptive_freshness_diagnostics": adaptive_summary.get("freshness_diagnostics") or {},
            "adaptive_distinct_wallets_max": adaptive_distinct_wallets_max,
            "adaptive_distinct_markets": len(adaptive_distinct_markets),
            "adaptive_opposing_wallet_count": adaptive_opposing_wallet_count,
            "adaptive_clob_fill_source_counts": adaptive_clob_fill_source_counts,
            "paper_event_log_lines": jsonl_counts["adaptive_bot_paper_event_log"],
            "paper_only": adaptive.get("paper_only"),
            "live_orders_allowed": adaptive.get("live_orders_allowed"),
            "reason": "own bot must only pass when fresh multi-wallet CLOB-backed consensus emits filled paper CopyIntent evidence",
        },
        "adaptive_single_wallet_exact_copy": {
            "status": _status(bool(adaptive_single_wallet_own_pass or adaptive_single_wallet_embedded_pass)),
            "single_wallet_state_status": adaptive_single_wallet.get("status") or "MISSING",
            "blockers": [] if (adaptive_single_wallet_own_pass or adaptive_single_wallet_embedded_pass) else adaptive_single_wallet.get("blockers") or [],
            "own_single_wallet_pass": adaptive_single_wallet_own_pass,
            "embedded_hot_path_single_wallet_pass": adaptive_single_wallet_embedded_pass,
            "embedded_hot_path_status": adaptive_embedded_hot_path.get("status") or "MISSING",
            "embedded_hot_path_blockers": adaptive_embedded_hot_path.get("blockers") or [],
            "intents": adaptive_single_wallet_intents,
            "filled_orders": adaptive_single_wallet_filled_orders,
            "rejected_orders": adaptive_single_wallet_rejected_orders,
            "embedded_hot_path_intents": adaptive_embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_intents_created"
            ),
            "embedded_hot_path_filled_orders": adaptive_embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_filled_orders"
            ),
            "embedded_hot_path_rejected_orders": adaptive_embedded_hot_path.get(
                "hot_path_single_wallet_exact_copy_rejected_orders"
            ),
            "wallet_count": adaptive_single_wallet_summary.get("wallet_count"),
            "market_outcomes": adaptive_single_wallet_summary.get("market_outcomes") or [],
            "filter_reason_counts": adaptive_single_wallet_summary.get("filter_reason_counts") or {},
            "paper_event_log_lines": jsonl_counts["adaptive_single_wallet_exact_copy_paper_event_log"],
            "paper_only": adaptive.get("paper_only"),
            "live_orders_allowed": adaptive.get("live_orders_allowed"),
            "reason": (
                "fresh single-wallet exact copy is valid paper execution evidence for the 1:1 wallet-copy path, "
                "but it does not satisfy multi-wallet consensus or live-admission profit gates by itself"
            ),
        },
        "lifecycle_realized_pnl": {
            "status": _status(bool(lifecycle_realized)),
            "summary": {k: v for k, v in lifecycle_realized.items() if k != "rows"},
        },
    }

    pass_count = sum(1 for row in checks.values() if row.get("status") == "PASS")
    fail_count = sum(1 for row in checks.values() if row.get("status") == "FAIL")
    watch_count = sum(1 for row in checks.values() if row.get("status") == "WATCH")
    learning_status = "GREEN" if fail_count == 0 and watch_count == 0 and not missing_paths else "WATCH"
    if missing_paths:
        learning_status = "REPAIR"
    if checks["live_admission_truth"]["status"] == "FAIL":
        learning_status = "WATCH"

    onboarding_config = onboarding.get("config") if isinstance(onboarding.get("config"), dict) else {}
    scoped_registry = onboarding.get("scoped_registry") if isinstance(onboarding.get("scoped_registry"), dict) else {}
    tracker_registry_path = str(
        onboarding_config.get("scoped_registry_path")
        or scoped_registry.get("path")
        or "configs/wallet_copy/wallets.json"
    )
    tracker_state_path = str(args.live_tracking_state)
    tracker_event_log_path = str(args.live_tracking_event_log)
    tracker_paper_state_path = str(onboarding_config.get("live_tracker_paper_state") or "data/research/wallet_copy_live_paper_state.json")
    tracker_paper_event_log_path = str(
        onboarding_config.get("live_tracker_paper_event_log")
        or "data/research/wallet_copy_live_paper_orders.jsonl"
    )
    tracker_profit_state_path = str(args.profit_state)
    tracker_history_state_path = str(args.history_state)
    tracker_market_ws_path = str(onboarding_config.get("market_ws_jsonl") or "data/research/clob_market_ws_events.jsonl")
    candidate_forward_state_path = str(
        _arg(
            args,
            "candidate_forward_live_tracker_state",
            "data/research/wallet_copy_candidate_forward_live_tracking_state.json",
        )
    )
    candidate_forward_event_log_path = str(
        _arg(
            args,
            "candidate_forward_live_tracker_event_log",
            "data/research/wallet_copy_candidate_forward_live_tracking_events.jsonl",
        )
    )
    candidate_forward_paper_state_path = str(
        _arg(args, "candidate_forward_paper_state", "data/research/wallet_copy_candidate_forward_paper_state.json")
    )
    candidate_forward_paper_event_log_path = str(
        _arg(
            args,
            "candidate_forward_paper_event_log",
            "data/research/wallet_copy_candidate_forward_paper_events.jsonl",
        )
    )

    next_actions: list[dict[str, str]] = []
    if clob_filled == 0 and required_buys > 0:
        next_actions.append(
            {
                "area": "CLOB evidence",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": "score copy-efficiency with CLOB-enriched CopyIntents and persist per-event book evidence",
                "verify": "python3 scripts/run_wallet_live_tracker.py --enable-clob-books --strict-mirror-coverage ...",
            }
        )
    if fallback_filled > 0:
        next_actions.append(
            {
                "area": "fallback fills",
                "file": "src/wallet_copy/fill_model.py",
                "function": "estimate_executable_fill",
                "action": "keep fallback fills as research-only and require CLOB-backed fills for admission",
                "verify": f"jq '.summary.copy_efficiency.summary' {tracker_state_path}",
            }
        )
    if copyability_filtered > 0:
        next_actions.append(
            {
                "area": "copyability gate",
                "file": "src/wallet_copy/copyability.py",
                "function": "score_copyability",
                "action": "keep stale or unmarketable wallet rows visible as copyability-filtered evidence and prove enough fresh required CLOB-backed copies before admission",
                "verify": (
                    "jq '.summary.copy_efficiency.summary|{copyability_filtered_buy_events,"
                    f"copyability_reason_counts,required_buy_copy_events,clob_filled_buy_copy_events}}' {tracker_state_path}"
                ),
            }
        )
    if checks["active_hotlane_forward_copy_efficiency"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active hotlane forward copy-efficiency",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": (
                    "continue the isolated active-hotlane paper tracker until it records required BUY copies "
                    "with CLOB fills and zero fallback/reject/miss; keep this as forward copyability evidence, "
                    "not live admission truth"
                ),
                "verify": (
                    "jq '.summary.copy_efficiency.summary|{required_buy_copy_events,"
                    "clob_filled_buy_copy_events,fallback_filled_buy_copy_events,rejected_buy_copy_events,"
                    f"missed_buy_copy_events,source_fresh_buy_events_le_10s}}' {active_hotlane_live_tracking_state_path}"
                ),
            }
        )
    if checks["active_forward_candidate_probe"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active forward candidate probe",
                "file": "scripts/select_wallet_copy_active_forward_probe.py",
                "function": "select_active_forward_probe plus scripts/run_wallet_live_tracker.py main",
                "action": (
                    "select a currently active hot-lane single-wallet profit candidate and measure it in an "
                    "isolated paper-only tracker until fresh CLOB-backed required BUY evidence appears; keep "
                    "canonical best-candidate admission blocked while this probe is only research evidence"
                ),
                "verify": (
                    "1) python3 scripts/select_wallet_copy_active_forward_probe.py "
                    f"--profit-state {tracker_profit_state_path} --active-hotlane-state {active_hotlane_state_path} "
                    f"--output {active_forward_probe_profit_state_path}; "
                    "2) python3 scripts/run_wallet_live_tracker.py "
                    f"--registry {active_hotlane_registry_path} --state {active_forward_probe_live_tracking_state_path} "
                    f"--event-log {active_forward_probe_live_tracking_event_log_path} "
                    f"--paper-state data/research/wallet_copy_active_forward_probe_paper_state.json "
                    f"--paper-event-log {active_forward_probe_paper_event_log_path} "
                    f"--profit-policy-state {active_forward_probe_profit_state_path} --track-blocked-profit-policy "
                    f"--seed-history-state {tracker_history_state_path} --seed-before-poll "
                    "--limit 5 --pages 1 --data-api-timeout-s 1.5 --iterations 20 --poll-interval-s 0.5 "
                    "--max-runtime-s 60 --enable-clob-books --admission-mode --strict-mirror-coverage "
                    "--use-profit-search-scope --max-wallets 1"
                ),
            }
        )
    candidate_forward_blockers = [
        str(blocker)
        for blocker in (
            candidate_forward_live_tracker_truth.get("blockers")
            or live_tracker_truth.get("blockers")
            or []
        )
    ]
    stale_candidate_forward = any(
        blocker
        in {
            "stale_wallet_buy_rows_only",
            "candidate_source_wallet_inactive_or_deduped_in_forward_window",
            "candidate_source_wallet_not_tracked",
        }
        for blocker in candidate_forward_blockers
    )
    if "candidate_source_wallet_copy_efficiency_missing" in set(candidate_forward_blockers):
        candidate_policy_id = live_tracker_truth_summary.get("candidate_policy_id")
        if stale_candidate_forward and forward_tracking_queue:
            next_actions.append(
                {
                    "area": "candidate forward rotation",
                    "file": "src/wallet_copy/profit_engine.py",
                    "function": "_forward_tracking_queue plus scripts/run_wallet_copy_autonomous_repair.py _candidate_forward_specs",
                    "action": (
                        "demote the stale best-candidate forward target for measurement, rotate to the freshest "
                        "forward_tracking_queue wallet, and keep live admission ANALYZE/CORRECTION until that candidate produces "
                        "candidate-specific CLOB-backed required BUY evidence"
                    ),
                    "verify": (
                        f"python3 scripts/run_wallet_copy_profit_engine.py --history-state {tracker_history_state_path} "
                        f"--resolutions data/research/btc_resolutions_from_btcusdt_ticks.jsonl "
                        f"--output {tracker_profit_state_path} "
                        f"--live-today-sprint-operator-approval-id OP-LIVE-20260703-BELA; "
                        f"jq '.forward_candidate,.decision.forward_tracking_queue_size,"
                        f".decision.stale_best_candidate_demoted_for_forward_tracking' {tracker_profit_state_path}"
                    ),
                }
            )
        elif tracker_profit_policy.get("status") == "BLOCKED_POLICY_LOADED_FOR_FORWARD_MEASUREMENT_ONLY":
            next_actions.append(
                {
                    "area": "candidate policy forward evidence",
                    "file": "scripts/run_wallet_live_tracker.py",
                    "function": "main",
                    "action": (
                        "continue the explicit paper-only blocked-candidate tracker until it records fresh required "
                        "CLOB-backed BUY copies for the candidate policy_id, then re-run profit admission with that state"
                    ),
                    "verify": (
                        f"python3 scripts/run_wallet_live_tracker.py --registry {tracker_registry_path} "
                        f"--state {candidate_forward_state_path} --event-log {candidate_forward_event_log_path} "
                        f"--paper-state {candidate_forward_paper_state_path} "
                        f"--paper-event-log {candidate_forward_paper_event_log_path} "
                        f"--profit-policy-state {tracker_profit_state_path} --track-blocked-profit-policy "
                        f"--seed-history-state {tracker_history_state_path} --seed-before-poll "
                        f"--market-ws-jsonl {tracker_market_ws_path} --limit 20 --pages 2 --data-api-timeout-s 2 "
                        "--iterations 120 --poll-interval-s 0.5 --max-runtime-s 180 "
                        "--enable-clob-books --admission-mode --strict-mirror-coverage --no-use-profit-search-scope"
                        + (f" # expected policy_id {candidate_policy_id}" if candidate_policy_id else "")
                    ),
                }
            )
        else:
            next_actions.append(
                {
                    "area": "candidate policy forward tracking",
                    "file": "src/wallet_copy/live_tracker.py",
                    "function": "LiveWalletTracker._load_profit_policy",
                    "action": (
                        "add a paper-only mode that forward-measures the best blocked candidate policy without live admission, "
                        "stamps CopyIntents with that candidate policy_id, and feeds candidate-specific CLOB-backed evidence "
                        "back into profit admission"
                    ),
                    "verify": (
                        f"jq '.live_tracker_truth.summary|{{candidate_policy_id,candidate_policy_copy_events,"
                        f"candidate_policy_required_buy_copy_events,copy_efficiency_status}}' {tracker_profit_state_path}"
                        + (f" # expected policy_id {candidate_policy_id}" if candidate_policy_id else "")
                    ),
                }
            )
    if required_buys == 0 and copy_efficiency.get("status") != "PASS":
        if wallet_report_summary["events_seen"] > 0 and wallet_report_summary["new_events"] == 0:
            fresh_action = (
                "selected tracker cohort is returning already-seen wallet rows but no fresh required BUY evidence; "
                "run a longer paper-only tracker or rotate/rank scope toward currently active BTC-5m wallets"
            )
            fresh_verify = (
                "jq '.summary|{tracker_scope,wallet_report_summary:{raw_rows:([.wallet_reports[]?.raw_rows] | add // 0),"
                "events_seen:([.wallet_reports[]?.events_seen] | add // 0),new_events:([.wallet_reports[]?.new_events] | add // 0)},"
                "copy_efficiency:.copy_efficiency.summary|{required_buy_copy_events,clob_filled_buy_copy_events,copyability_filtered_buy_events}}' "
                f"{tracker_state_path}"
            )
        else:
            fresh_action = (
                "run continuous paper-only tracker measurement until fresh required BUY copy evidence appears or "
                "classify the selected wallet cohort as inactive/stale"
            )
            fresh_verify = (
                f"python3 scripts/run_wallet_live_tracker.py --registry {tracker_registry_path} "
                f"--state {tracker_state_path} --event-log {tracker_event_log_path} "
                f"--paper-state {tracker_paper_state_path} --paper-event-log {tracker_paper_event_log_path} "
                f"--profit-policy-state {tracker_profit_state_path} --seed-history-state {tracker_history_state_path} "
                f"--seed-before-poll --market-ws-jsonl {tracker_market_ws_path} "
                "--limit 2 --pages 1 --data-api-timeout-s 2 "
                "--iterations 60 --poll-interval-s 0.5 --max-runtime-s 90 --max-wallets 3 "
                "--enable-clob-books --admission-mode --strict-mirror-coverage --no-use-profit-search-scope"
            )
        next_actions.append(
            {
                "area": "fresh copy evidence",
                "file": "scripts/run_wallet_live_tracker.py",
                "function": "main",
                "action": fresh_action,
                "verify": fresh_verify,
            }
        )
    if wallet_api_errors:
        next_actions.append(
            {
                "area": "wallet API resilience",
                "file": "src/wallet_copy/ingest.py",
                "function": "WalletHistoryClient.fetch_events",
                "action": "keep Data API timeout/errors visible in wallet_reports and add retry/backoff if repeated errors block fresh copy evidence",
                "verify": f"jq '.summary.wallet_reports[]?|select(.api_error)|{{wallet,wallet_name,api_error}}' {tracker_state_path}",
            }
        )
    if rejected > 0:
        next_actions.append(
            {
                "area": "fill model rejects",
                "file": "src/wallet_copy/fill_model.py",
                "function": "estimate_executable_fill",
                "action": "root-cause rejected paper copy orders with CLOB/token/depth evidence and distinguish true no-fill from mapping or marketability defects",
                "verify": (
                    "jq '.summary.copy_efficiency.summary|{rejected_buy_copy_events,rejection_reason_counts,"
                    f"fill_blocker_counts,clob_blocking_reason_counts}}' {tracker_state_path}"
                ),
            }
        )
    if required_buys > 0 and clob_filled < required_buys and rejected == 0 and fallback_filled == 0 and missed == 0:
        next_actions.append(
            {
                "area": "buy copy fill rate",
                "file": "src/wallet_copy/copy_efficiency.py",
                "function": "build_copy_efficiency_report",
                "action": "explain buy_copy_fill_rate_below_100pct with per-event CLOB-backed fill/reject/missed classifications",
                "verify": f"jq '.summary.copy_efficiency.event_scores[]|{{source_event_id,copy_status,fill_source,clob_book_status}}' {tracker_state_path}",
            }
        )
    if lifecycle_missed > 0 or len(tracker_summary.get("coverage_violations") or []) > 0:
        next_actions.append(
            {
                "area": "lifecycle exact-copy",
                "file": "src/wallet_copy/paper.py",
                "function": "PaperWalletCopyEngine._apply_lifecycle_event_to_state",
                "action": "explain and repair SELL/MERGE/REDEEM copy misses by linking lifecycle rows to prior paper positions, backfilled history, or explicit pre-existing-position gaps",
                "verify": f"jq '.summary.coverage_violations[]?|{{action,condition_id,outcome,mirror_result}}' {tracker_state_path}",
            }
        )
    if latency_avg > float(args.max_learning_latency_s):
        fetch_avg = tracker_latency.get("wallet_api_fetch_duration_avg_s")
        freshest_lag = tracker_latency.get("freshest_event_lag_s")
        if fetch_avg is not None and float(fetch_avg or 0.0) <= float(args.max_learning_latency_s):
            latency_action = (
                "event age remains above copy cap after latency split; add continuous low-interval wallet tracker, "
                "Data API cursor tuning, or market preconfirm bridge and verify freshest-event lag instead of treating "
                "fetch duration as the blocker"
            )
        else:
            latency_action = (
                "wallet API fetch duration or source event age exceeds copy cap; instrument per-wallet request timing "
                "and reduce polling/data-source lag"
            )
        next_actions.append(
            {
                "area": "latency",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": latency_action,
                "verify": (
                    "jq '.summary|{latency:.latency, copy_efficiency_latency:.copy_efficiency.summary|"
                    f"{{latency_basis,required_event_age_avg_s,required_event_age_p95_s,required_event_age_max_s,event_age_avg_s,event_age_p95_s,event_age_max_s,api_latency_avg_s}}}}' {tracker_state_path}"
                ),
            }
        )
    if not lifecycle_realized:
        next_actions.append(
            {
                "area": "lifecycle PnL",
                "file": "src/wallet_copy/performance.py",
                "function": "score_paper_state",
                "action": "persist canonical realized PnL for SELL/MERGE/REDEEM reductions",
                "verify": "python3 scripts/analyze_wallet_copy_research.py --history-state ... --paper-state ...",
            }
        )
    if checks["active_hotlane_hot_path_adaptive"]["status"] != "PASS":
        hot_path_blockers = active_hotlane_hot_path_adaptive.get("blockers") or ["hot_path_adaptive_missing_or_no_pass"]
        if "source_feed_delayed" in hot_path_blockers:
            hot_path_action = (
                "current-poll hot path still sees wallet events after the <=10s copy cap; add a fresher "
                "wallet source/preconfirm side-channel or lower hot-lane polling latency before relaxing gates"
            )
            hot_path_file = "src/wallet_copy/ingest.py"
            hot_path_function = "WalletHistoryClient.fetch_events"
        elif "runtime_fresh_but_single_wallet_only" in hot_path_blockers:
            hot_path_action = (
                "current-poll hot path has fresh CLOB-backed BUY evidence but from fewer than two wallets; "
                "rotate smaller active slices or widen concurrent hot-lane scope until multi-wallet agreement appears"
            )
            hot_path_file = "src/wallet_copy/live_tracker.py"
            hot_path_function = "LiveWalletTracker._scoped_specs"
        elif "runtime_inventory_candidate_research_only" in hot_path_blockers:
            hot_path_action = (
                "current-poll hot path found two-sided or biased inventory research candidates; keep them "
                "paper-only and implement explicit adaptive inventory CopyIntent lifecycle before promotion"
            )
            hot_path_file = "src/wallet_copy/adaptive_bot.py"
            hot_path_function = "build_adaptive_signals"
        else:
            hot_path_action = (
                "keep current-poll adaptive evidence visible in tracker summary and continue measuring until "
                "fresh multi-wallet CLOB-backed consensus or a sharper latency/source blocker appears"
            )
            hot_path_file = "src/wallet_copy/live_tracker.py"
            hot_path_function = "LiveWalletTracker.poll_once"
        next_actions.append(
            {
                "area": "active hot-lane current-poll adaptive evidence",
                "file": hot_path_file,
                "function": hot_path_function,
                "action": hot_path_action,
                "verify": (
                    "jq '.summary.hot_path_adaptive|{status,blockers,summary:{current_poll_moves:"
                    ".summary.current_poll_moves,pass_signals:.summary.pass_signals,"
                    "hot_path_intents_created:.summary.hot_path_intents_created,"
                    "hot_path_filled_orders:.summary.hot_path_filled_orders,"
                    "hot_path_rejected_orders:.summary.hot_path_rejected_orders,"
                    "runtime_fresh_buy_events_le_cap:.summary.freshness_diagnostics.runtime_fresh_buy_events_le_cap,"
                    "runtime_eligible_wallets:.summary.freshness_diagnostics.runtime_eligible_wallets,"
                    "source_feed_delayed:.summary.freshness_diagnostics.source_feed_delayed,"
                    "runtime_inventory_research_candidates:.summary.runtime_inventory_research_candidates},"
                    "paper_lifecycle:.paper_lifecycle}' "
                    f"{active_hotlane_live_tracking_state_path}"
                ),
            }
        )
    if checks["active_hotlane_single_wallet_exact_copy"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active hot-lane single-wallet exact copy",
                "file": "scripts/run_wallet_copy_hotlane_tick.py",
                "function": "main",
                "action": (
                    "measure 1:1 single-wallet exact-copy as its own paper-only CopyIntent lane when multi-wallet "
                    "consensus is absent; keep it separate from live admission, but require fresh intents, full "
                    "CLOB-backed fills, and zero rejections before the 1:1 copyability measurement can pass"
                ),
                "verify": (
                    "python3 scripts/run_wallet_copy_hotlane_tick.py "
                    f"--registry {active_hotlane_registry_path} "
                    f"--tracker-state {active_hotlane_live_tracking_state_path} "
                    f"--tracker-event-log {active_hotlane_live_tracking_event_log_path} "
                    f"--paper-state {active_hotlane_paper_state_path} "
                    f"--paper-event-log {active_hotlane_paper_event_log_path} "
                    f"--tracker-time-replay-paper-event-log {active_hotlane_tracker_time_replay_paper_event_log_path} "
                    f"--single-wallet-exact-copy-paper-event-log {active_hotlane_single_wallet_exact_copy_paper_event_log_path} "
                    f"--output {active_hotlane_tick_state_path} --ticks 4 --wallets-per-tick 1 "
                    "--parallel-wallet-fetches 1 --limit 2 --pages 1 --max-runtime-s 20 --max-poll-runtime-s 8"
                ),
            }
        )
    if checks["adaptive_wallet_derived_bot"]["status"] != "PASS":
        adaptive_freshness = (
            adaptive_summary.get("freshness_diagnostics")
            if isinstance(adaptive_summary.get("freshness_diagnostics"), dict)
            else {}
        )
        if adaptive_tracker_time_pass_signals > 0 and adaptive_pass_signals == 0:
            next_actions.append(
                {
                    "area": "adaptive hot-path latency",
                    "file": "src/wallet_copy/live_tracker.py",
                    "function": "LiveWalletTracker.poll_once",
                    "action": (
                        "tracker-time adaptive consensus would pass but runtime signal is stale; move adaptive signal "
                        "construction into the active hot-lane poll loop or trigger it before any slower canonical/profit "
                        "commands, while keeping live execution disabled until runtime_fresh evidence also passes"
                    ),
                    "verify": (
                        "jq '.summary|{pass_signals,tracker_time_pass_signals,"
                        "freshness:.freshness_diagnostics|{runtime_fresh_buy_events_le_cap,"
                        "tracker_fresh_buy_events_le_cap,tracker_fresh_but_runtime_stale,latest_buy_event_lag_s}}' "
                        f"{adaptive_bot_state_path}"
                    ),
                }
            )
        if adaptive_tracker_time_replay_intents > 0 and adaptive_pass_signals == 0:
            next_actions.append(
                {
                    "area": "adaptive tracker-time replay",
                    "file": "src/wallet_copy/adaptive_bot.py",
                    "function": "run_adaptive_bot",
                    "action": (
                        "tracker-time replay has paper lifecycle evidence, but runtime/current-poll consensus still "
                        "does not pass; use the replay ledger to rank wallet cohorts and shrink hot-lane polling lag, "
                        "not to green-light live admission"
                    ),
                    "verify": (
                        "jq '.tracker_time_replay|{status,blockers,summary:{intents:.summary.intents,"
                        "filled:.summary.filled_orders,rejected:.summary.rejected_orders,role}}' "
                        f"{adaptive_bot_state_path}"
                    ),
                }
            )
        elif adaptive_freshness.get("source_feed_delayed") is True:
            next_actions.append(
                {
                    "area": "adaptive source-feed latency",
                    "file": "src/wallet_copy/ingest.py",
                    "function": "WalletHistoryClient.fetch_events",
                    "action": (
                        "active wallets are arriving after the <=10s live-copy cap; improve wallet feed freshness "
                        "with tighter polling, lower page/limit hot-lane queries, or preconfirm/onchain side-channel "
                        "before relaxing any adaptive gate"
                    ),
                    "verify": (
                        "jq '.summary.freshness_diagnostics|{latest_buy_event_lag_s,"
                        "runtime_fresh_buy_events_le_cap,source_feed_delayed,top_filter_reasons_by_wallet}' "
                        f"{adaptive_bot_state_path}"
                    ),
                }
            )
        if adaptive_tracker_time_inventory_research_candidates > 0 and adaptive_runtime_inventory_research_candidates == 0:
            next_actions.append(
                {
                    "area": "adaptive inventory-copy research",
                    "file": "src/wallet_copy/live_tracker.py",
                    "function": "LiveWalletTracker.poll_once",
                    "action": (
                        "tracker-time data shows two-sided or biased wallet inventory candidates, while the current "
                        "poll still lacks fresh runtime inventory evidence; keep tracker-time rows research-only and "
                        "measure whether the hot-lane current-poll inventory path creates, fills, or rejects paper "
                        "intents before promoting any wallet cohort"
                    ),
                    "verify": (
                        "jq '.summary|{hot_path_inventory_intents_created,"
                        "hot_path_inventory_filled_orders,hot_path_inventory_rejected_orders,"
                        "runtime_signal_blocker_counts,top_blocked_runtime_signals}' "
                        f"{active_hotlane_live_tracking_state_path}"
                    ),
                }
            )
        next_actions.append(
            {
                "area": "adaptive wallet-derived bot",
                "file": "src/wallet_copy/adaptive_bot.py",
                "function": "build_adaptive_signals",
                "action": (
                    "keep running the paper-only adaptive bot on live-tracker JSONL; if no PASS signal appears, "
                    "tighten active-wallet discovery/hot-lane polling until fresh CLOB-backed multi-wallet agreement "
                    "is observed instead of calling the bot green"
                ),
                "verify": (
                    "python3 scripts/run_wallet_copy_adaptive_bot.py "
                    f"--live-tracking-state {active_hotlane_live_tracking_state_path} "
                    f"--live-tracking-event-log {active_hotlane_live_tracking_event_log_path} "
                    f"--output {adaptive_bot_state_path} "
                    f"--paper-state {adaptive_bot_paper_state_path} "
                    f"--paper-event-log {adaptive_bot_paper_event_log_path} "
                    "--tracker-time-replay-paper-state "
                    "data/research/wallet_copy_adaptive_tracker_time_replay_paper_state.json "
                    f"--tracker-time-replay-paper-event-log {adaptive_tracker_time_replay_paper_event_log_path} "
                    "--single-wallet-exact-copy-paper-state "
                    "data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_state.json "
                    "--single-wallet-exact-copy-paper-event-log "
                    f"{adaptive_single_wallet_exact_copy_paper_event_log_path} "
                    "--max-event-log-rows 3000 --max-observation-age-s 30 --max-observed-event-age-s 10 "
                    "--max-signal-cluster-age-s 8"
                ),
            }
        )
    if checks["adaptive_single_wallet_exact_copy"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "adaptive single-wallet exact copy",
                "file": "src/wallet_copy/adaptive_bot.py",
                "function": "build_single_wallet_exact_copy_intents",
                "action": (
                    "when current-poll hot-lane evidence contains only one fresh profitable wallet, keep copying it "
                    "1:1 into a separate paper ledger with CLOB evidence instead of treating single-wallet evidence "
                    "as invisible"
                ),
                "verify": (
                    "jq '.single_wallet_exact_copy|{status,summary:{intents:.summary.intents,"
                    "filled:.summary.filled_orders,rejected:.summary.rejected_orders,wallet_count:.summary.wallet_count}}' "
                    f"{adaptive_bot_state_path}"
                ),
            }
        )
    if checks["active_hotlane_scope"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active wallet hot-lane",
                "file": "src/wallet_copy/hotlane.py",
                "function": "build_active_hotlane",
                "action": (
                    "select currently active BTC 5m wallets from live tracker JSONL, history, leaderboard, "
                    "and profit candidate evidence before adaptive bot measurement"
                ),
                "verify": (
                    "python3 scripts/select_wallet_copy_active_hotlane.py "
                    f"--registry configs/wallet_copy/wallets.json --live-tracking-event-log {args.live_tracking_event_log} "
                    f"--history-state {args.history_state} --leaderboard-state {args.leaderboard_state} "
                    f"--profit-state {args.profit_state} --output-registry {active_hotlane_registry_path} "
                    f"--output {active_hotlane_state_path} --max-wallets 4"
                ),
            }
        )
    if checks["direct_data_source_spot_check"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "direct data-source verification",
                "file": "scripts/audit_wallet_copy_learning_logs.py",
                "function": "_direct_data_source_spot_check",
                "action": (
                    "keep active hot-lane evidence non-green until the latest CLOB-backed copied BUY can be "
                    "re-confirmed against Data API /activity, Gamma /markets, and CLOB /book; if this repeats, "
                    "tighten hot-lane polling so the probe event is still on an active market"
                ),
                "verify": (
                    "python3 scripts/audit_wallet_copy_learning_logs.py --no-append-feedback-log --print-full | "
                    "python3 -c \"import json,sys; p=json.load(sys.stdin); "
                    "print(json.dumps(p['checks']['direct_data_source_spot_check'], indent=2))\""
                ),
            }
        )
    if checks["active_hotlane_tracking_evidence"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active wallet hot-lane tracker evidence",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": (
                    "run isolated active-hotlane tracker with per-poll runtime cap and require fresh source BUY/CLOB "
                    "copy-efficiency evidence before adaptive bot signals are trusted"
                ),
                "verify": (
                    "python3 scripts/run_wallet_live_tracker.py "
                    f"--registry {active_hotlane_registry_path} --state {active_hotlane_live_tracking_state_path} "
                    f"--event-log {active_hotlane_live_tracking_event_log_path} --limit 5 --pages 1 "
                    f"--tracker-time-replay-paper-event-log {active_hotlane_tracker_time_replay_paper_event_log_path} "
                    f"--all-order-tactic-replay-paper-event-log {active_hotlane_all_order_tactic_replay_paper_event_log_path} "
                    "--data-api-timeout-s 1.5 --max-poll-runtime-s 30 --iterations 1 --max-runtime-s 45 "
                    "--enable-clob-books --admission-mode --strict-mirror-coverage --no-use-profit-search-scope "
                    "--clob-timeout-s 1.0 --gamma-timeout-s 1.0"
                ),
            }
        )
    if (
        checks["active_hotlane_all_order_exact_copy"]["execution_tactic_plan"].get("recommended_tactic")
        and str(checks["active_hotlane_all_order_exact_copy"]["execution_tactic_plan"].get("recommended_tactic"))
        .startswith("paper_profile_replay:")
        and checks["active_hotlane_all_order_tactic_replay"]["status"] != "PASS"
    ):
        next_actions.append(
            {
                "area": "active wallet aggressive tactic replay proof",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker._all_order_aggressive_tactic_replay",
                "action": (
                    "convert the measured aggressive best-ask tactic into durable paper-only CopyIntent lifecycle "
                    "rows with event-level source_event_id, book_hash, strict_status, tactic_status, effective price, "
                    "cost delta, and zero fallback fills before using it as a candidate for PnL attribution"
                ),
                "verify": (
                    "jq '.checks.active_hotlane_all_order_tactic_replay|"
                    "{status,profile_id,replay_intents,filled_orders,rejected_orders,fallback_filled_orders,"
                    "incremental_source_event_ids_vs_strict,paper_event_log_lines}' "
                    "data/research/wallet_copy_learning_log_audit_state.json"
                ),
            }
        )
    if checks["active_hotlane_paper_copy_contract"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active hot-lane mechanical paper-copy contract",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": (
                    "preserve a strict CopyIntent lifecycle contract: every required copyable BUY in the active "
                    "hot-lane must become a CLOB-backed paper fill, with zero fallback, reject, or miss; empty "
                    "current polls stay ANALYZE but rolling evidence must remain PASS"
                ),
                "verify": (
                    "python3 scripts/audit_wallet_copy_learning_logs.py --no-append-feedback-log --print-full | "
                    "python3 -c \"import json,sys; p=json.load(sys.stdin); "
                    "print(json.dumps(p['checks']['active_hotlane_paper_copy_contract'], indent=2))\""
                ),
            }
        )
    if checks["hotlane_path_isolation"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active hot-lane path isolation",
                "file": "scripts/run_wallet_copy_autonomous_repair.py",
                "function": "build_repair_plan",
                "action": "keep active hot-lane tracker/paper outputs on dedicated paths so canonical evidence cannot be overwritten",
                "verify": "python3 -m pytest -q tests/test_wallet_copy_core.py -k active_hotlane",
            }
        )
    if checks["active_hotlane_guard_log_retention"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active hot-lane guard log retention",
                "file": "scripts/audit_wallet_copy_learning_logs.py",
                "function": "build_learning_log_audit",
                "action": (
                    "rotate or summarize wallet_copy_active_hotlane_guard.out while preserving structured JSONL "
                    "learning logs; do not call runtime green while the guard output grows past the retention cap"
                ),
                "verify": (
                    "jq '.checks.active_hotlane_guard_log_retention|{status,size_bytes,max_bytes,path}' "
                    "data/research/wallet_copy_learning_log_audit_state.json"
                ),
            }
        )
    if checks["active_hotlane_all_order_exact_copy"]["status"] != "PASS":
        next_actions.append(
            {
                "area": "active wallet all-order exact-copy proof",
                "file": "src/wallet_copy/live_tracker.py",
                "function": "LiveWalletTracker.poll_once",
                "action": (
                    "keep the separate all-order exact-copy paper ledger non-green until every observed BTC 5m "
                    "wallet BUY is converted to a paper CopyIntent and every lifecycle row is mirrored; fallback "
                    "fills are allowed only as paper research and must not count as live-money readiness; use "
                    "paper_tactic_profile_status_counts, execution_corrections, micro_batch_all_order_probe, "
                    "and execution_tactic_plan to decide whether aggressive best-ask limits, short micro-batching, "
                    "or skip/delay logic actually improves copyability"
                ),
                "verify": (
                    "python3 scripts/run_wallet_live_tracker.py "
                    f"--registry {active_hotlane_registry_path} --state {active_hotlane_live_tracking_state_path} "
                    f"--event-log {active_hotlane_live_tracking_event_log_path} --limit 5 --pages 1 "
                    f"--tracker-time-replay-paper-event-log {active_hotlane_tracker_time_replay_paper_event_log_path} "
                    "--data-api-timeout-s 1.5 --max-poll-runtime-s 30 --iterations 1 --max-runtime-s 45 "
                    "--enable-clob-books --admission-mode --strict-mirror-coverage --no-use-profit-search-scope "
                    "--clob-timeout-s 1.0 --gamma-timeout-s 1.0"
                ),
            }
        )

    source_counters = {
        "leaderboard_candidate_wallets": len(leaderboard.get("candidate_wallets") or []),
        "leaderboard_rows": len(leaderboard.get("leaderboard_rows") or []),
        "history_wallets": len(history_wallets),
        "history_events": len(history_events),
        "history_copy_intents": len(history_intents),
        "history_event_log_lines": jsonl_counts["history_event_log"],
        "paper_orders": len(paper_orders),
        "paper_event_log_lines": jsonl_counts["paper_event_log"],
        "live_tracker_time_replay_paper_event_log_lines": jsonl_counts[
            "live_tracker_time_replay_paper_event_log"
        ],
        "research_train_rows": len(research.get("train_rows") or []),
        "research_cross_wallet_windows": len(research.get("cross_wallet_windows") or []),
        "ml_dataset_rows": jsonl_counts["ml_dataset"],
        "live_tracking_event_log_lines": jsonl_counts["live_tracking_event_log"],
        "active_hotlane_live_tracking_event_log_lines": jsonl_counts["active_hotlane_live_tracking_event_log"],
        "active_hotlane_paper_event_log_lines": jsonl_counts["active_hotlane_paper_event_log"],
        "active_forward_probe_live_tracking_event_log_lines": jsonl_counts[
            "active_forward_probe_live_tracking_event_log"
        ],
        "active_forward_probe_paper_event_log_lines": jsonl_counts["active_forward_probe_paper_event_log"],
        "active_forward_probe_ranked_candidates": len(active_forward_probe_candidates),
        "active_forward_probe_required_buy_copy_events": active_forward_probe_required_buys,
        "active_forward_probe_clob_filled_buy_copy_events": active_forward_probe_clob_filled,
        "active_forward_probe_fallback_filled_buy_copy_events": active_forward_probe_fallback_filled,
        "active_forward_probe_rejected_buy_copy_events": active_forward_probe_rejected,
        "active_forward_probe_missed_buy_copy_events": active_forward_probe_missed,
        "active_hotlane_tracker_time_replay_paper_event_log_lines": jsonl_counts[
            "active_hotlane_tracker_time_replay_paper_event_log"
        ],
        "active_hotlane_all_order_tactic_replay_paper_event_log_lines": jsonl_counts[
            "active_hotlane_all_order_tactic_replay_paper_event_log"
        ],
        "active_hotlane_all_order_tactic_replay_durable_filled_orders": (
            active_hotlane_all_order_tactic_replay_paper_filled
        ),
        "active_hotlane_all_order_tactic_replay_durable_rejected_orders": (
            active_hotlane_all_order_tactic_replay_paper_rejected
        ),
        "active_hotlane_all_order_tactic_replay_durable_fallback_filled_orders": (
            active_hotlane_all_order_tactic_replay_paper_fallback
        ),
        "active_hotlane_tracker_source_buy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("source_buy_events") or 0
        ),
        "active_hotlane_tracker_required_buy_copy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("required_buy_copy_events") or 0
        ),
        "active_hotlane_tracker_clob_filled_buy_copy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("clob_filled_buy_copy_events") or 0
        ),
        "active_hotlane_tracker_fallback_filled_buy_copy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("fallback_filled_buy_copy_events") or 0
        ),
        "active_hotlane_tracker_rejected_buy_copy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("rejected_buy_copy_events") or 0
        ),
        "active_hotlane_tracker_missed_buy_copy_events": int(
            active_hotlane_tracker_copy_eff_summary.get("missed_buy_copy_events") or 0
        ),
        "active_hotlane_all_order_source_events": active_hotlane_all_order_source_events,
        "active_hotlane_all_order_buy_source_events": active_hotlane_all_order_buy_source_events,
        "active_hotlane_all_order_buy_intents": active_hotlane_all_order_buy_intents,
        "active_hotlane_all_order_clob_filled_buy_copy_events": active_hotlane_all_order_clob_filled,
        "active_hotlane_all_order_fallback_filled_buy_copy_events": active_hotlane_all_order_fallback_filled,
        "active_hotlane_all_order_rejected_buy_copy_events": active_hotlane_all_order_rejected,
        "active_hotlane_all_order_coverage_violations": active_hotlane_all_order_violations,
        "active_hotlane_all_order_missed_lifecycle_events": active_hotlane_all_order_missed_lifecycle,
        "direct_source_spot_check_pass": int(direct_source_spot_check.get("status") == "PASS"),
        "direct_source_data_api_http_200": int(
            _get_nested(direct_source_spot_check, "data_api_activity", "http_status") == 200
        ),
        "direct_source_gamma_http_200": int(
            _get_nested(direct_source_spot_check, "gamma_market", "http_status") == 200
        ),
        "direct_source_clob_http_200": int(
            _get_nested(direct_source_spot_check, "clob_book", "http_status") == 200
        ),
        "tracker_required_buy_copy_events": required_buys,
        "tracker_copyability_filtered_buy_events": copyability_filtered,
        "tracker_clob_filled_buy_copy_events": clob_filled,
        "tracker_fallback_filled_buy_copy_events": fallback_filled,
        "active_hotlane_selected_wallets": int(active_hotlane_summary.get("selected_wallets") or 0),
        "active_hotlane_scored_wallets": int(active_hotlane_summary.get("scored_wallets") or 0),
        "adaptive_bot_moves_seen": adaptive_moves_seen,
        "adaptive_bot_eligible_moves": adaptive_eligible_moves,
        "adaptive_bot_pass_signals": adaptive_pass_signals,
        "adaptive_bot_tracker_time_pass_signals": adaptive_tracker_time_pass_signals,
        "adaptive_bot_runtime_inventory_research_candidates": adaptive_runtime_inventory_research_candidates,
        "adaptive_bot_tracker_time_inventory_research_candidates": adaptive_tracker_time_inventory_research_candidates,
        "adaptive_bot_tracker_time_replay_intents": adaptive_tracker_time_replay_intents,
        "adaptive_bot_tracker_time_replay_filled_orders": adaptive_tracker_time_replay_filled_orders,
        "adaptive_bot_tracker_time_replay_event_log_lines": jsonl_counts[
            "adaptive_tracker_time_replay_paper_event_log"
        ],
        "adaptive_bot_single_wallet_exact_copy_intents": adaptive_single_wallet_intents,
        "adaptive_bot_single_wallet_exact_copy_filled_orders": adaptive_single_wallet_filled_orders,
        "adaptive_bot_single_wallet_exact_copy_event_log_lines": jsonl_counts[
            "adaptive_single_wallet_exact_copy_paper_event_log"
        ],
        "adaptive_bot_source_selected_wallet_overlap": adaptive_selected_wallet_overlap,
        "adaptive_bot_intents": adaptive_intents,
        "adaptive_bot_filled_orders": adaptive_filled_orders,
        "adaptive_bot_paper_event_log_lines": jsonl_counts["adaptive_bot_paper_event_log"],
    }
    feedback = _feedback_loop(
        learning_status=learning_status,
        checks=checks,
        next_actions=next_actions,
        source_counters=source_counters,
        feedback_log=str(_arg(args, "feedback_log", "data/research/wallet_copy_development_feedback_log.jsonl")),
    )
    if feedback.get("status") == "BUG_SUSPECT":
        learning_status = "BUG_SUSPECT"
    elif feedback.get("green_by_removal_guard", {}).get("status") in {"WATCH", "FAIL"} and learning_status == "GREEN":
        learning_status = "WATCH"

    return {
        "schema_version": 1,
        "kind": "wallet_copy_learning_log_audit_state",
        "generated_at": utc_now_iso(),
        "mission_contract": mission,
        "paper_only": True,
        "live_orders_allowed": False,
        "learning_status": learning_status,
        "summary": {
            "checks": len(checks),
            "pass": pass_count,
            "watch": watch_count,
            "fail": fail_count,
            "missing_paths": missing_paths,
        },
        "paths": paths,
        "jsonl_counts": jsonl_counts,
        "source_counters": source_counters,
        "checks": checks,
        "feedback_loop": feedback,
        "next_actions": next_actions,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leaderboard-state", default="data/research/wallet_copy_leaderboard_crypto_state.json")
    parser.add_argument("--history-state", default="data/research/wallet_copy_history_state.json")
    parser.add_argument("--history-event-log", default="data/research/wallet_copy_events.jsonl")
    parser.add_argument("--pipeline-resume-state", default="data/research/wallet_copy_pipeline_resume_state.json")
    parser.add_argument("--paper-state", default="data/research/wallet_copy_paper_state.json")
    parser.add_argument("--paper-event-log", default="data/research/wallet_copy_paper_events.jsonl")
    parser.add_argument(
        "--live-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_live_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument("--research-state", default="data/research/wallet_copy_research_state.json")
    parser.add_argument("--ml-dataset", default="data/research/wallet_copy_ml_dataset.jsonl")
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--live-tracking-state", default="data/research/wallet_copy_live_tracking_state.json")
    parser.add_argument("--live-tracking-event-log", default="data/research/wallet_copy_live_tracking_events.jsonl")
    parser.add_argument(
        "--candidate-forward-live-tracker-state",
        default="data/research/wallet_copy_candidate_forward_live_tracking_state.json",
    )
    parser.add_argument(
        "--candidate-forward-live-tracker-event-log",
        default="data/research/wallet_copy_candidate_forward_live_tracking_events.jsonl",
    )
    parser.add_argument(
        "--candidate-forward-paper-state",
        default="data/research/wallet_copy_candidate_forward_paper_state.json",
    )
    parser.add_argument(
        "--candidate-forward-paper-event-log",
        default="data/research/wallet_copy_candidate_forward_paper_events.jsonl",
    )
    parser.add_argument("--active-hotlane-state", default="data/research/wallet_copy_active_hotlane_state.json")
    parser.add_argument("--active-hotlane-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument(
        "--active-hotlane-live-tracking-state",
        default="data/research/wallet_copy_active_hotlane_live_tracking_state.json",
    )
    parser.add_argument(
        "--active-hotlane-live-tracking-event-log",
        default="data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
    )
    parser.add_argument("--active-hotlane-paper-state", default="data/research/wallet_copy_active_hotlane_paper_state.json")
    parser.add_argument(
        "--active-hotlane-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-forward-probe-profit-state",
        default="data/research/wallet_copy_active_forward_probe_profit_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-live-tracking-state",
        default="data/research/wallet_copy_active_forward_probe_live_tracking_state.json",
    )
    parser.add_argument(
        "--active-forward-probe-live-tracking-event-log",
        default="data/research/wallet_copy_active_forward_probe_live_tracking_events.jsonl",
    )
    parser.add_argument(
        "--active-forward-probe-paper-event-log",
        default="data/research/wallet_copy_active_forward_probe_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_active_hotlane_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--active-hotlane-all-order-exact-copy-paper-state",
        default="data/research/wallet_copy_active_hotlane_paper_state_all_order_exact_copy.json",
    )
    parser.add_argument("--active-hotlane-tick-state", default="data/research/wallet_copy_hotlane_tick_state.json")
    parser.add_argument("--active-hotlane-guard-log", default="data/research/wallet_copy_active_hotlane_guard.out")
    parser.add_argument("--max-active-hotlane-guard-log-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--adaptive-bot-state", default="data/research/wallet_copy_adaptive_bot_state.json")
    parser.add_argument("--adaptive-bot-paper-state", default="data/research/wallet_copy_adaptive_bot_paper_state.json")
    parser.add_argument("--adaptive-bot-paper-event-log", default="data/research/wallet_copy_adaptive_bot_paper_events.jsonl")
    parser.add_argument(
        "--adaptive-single-wallet-exact-copy-paper-event-log",
        default="data/research/wallet_copy_adaptive_single_wallet_exact_copy_paper_events.jsonl",
    )
    parser.add_argument(
        "--adaptive-tracker-time-replay-paper-event-log",
        default="data/research/wallet_copy_adaptive_tracker_time_replay_paper_events.jsonl",
    )
    parser.add_argument("--operator-onboarding-state", default="data/research/operator_wallet_onboarding_state.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/wallet_copy_learning_log_audit_state.json")
    parser.add_argument("--feedback-log", default="data/research/wallet_copy_development_feedback_log.jsonl")
    parser.add_argument("--append-feedback-log", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-learning-latency-s", type=float, default=10.0)
    parser.add_argument("--direct-source-spot-check", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--direct-source-timeout-s", type=float, default=3.0)
    parser.add_argument("--direct-source-retries", type=int, default=2)
    parser.add_argument("--print-full", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_learning_log_audit(args)
    if args.append_feedback_log:
        feedback_entry = {
            "generated_at": payload.get("generated_at"),
            "learning_status": payload.get("learning_status"),
            "summary": payload.get("summary"),
            "source_counters": payload.get("source_counters"),
            "jsonl_counts": payload.get("jsonl_counts"),
            "non_green_checks": payload.get("feedback_loop", {}).get("non_green_checks"),
            "repeated_blockers": payload.get("feedback_loop", {}).get("repeated_blockers"),
            "green_by_removal_guard": payload.get("feedback_loop", {}).get("green_by_removal_guard"),
            "next_actions": payload.get("next_actions"),
            "key_checks": {
                "profit_admission": payload.get("checks", {}).get("profit_admission"),
                "live_tracker_copy_efficiency": payload.get("checks", {}).get("live_tracker_copy_efficiency"),
                "live_admission_truth": payload.get("checks", {}).get("live_admission_truth"),
                "active_hotlane_scope": payload.get("checks", {}).get("active_hotlane_scope"),
                "active_hotlane_tracking_evidence": payload.get("checks", {}).get("active_hotlane_tracking_evidence"),
                "active_hotlane_paper_copy_contract": payload.get("checks", {}).get(
                    "active_hotlane_paper_copy_contract"
                ),
                "direct_data_source_spot_check": payload.get("checks", {}).get("direct_data_source_spot_check"),
                "adaptive_wallet_derived_bot": payload.get("checks", {}).get("adaptive_wallet_derived_bot"),
            },
        }
        append_jsonl(args.feedback_log, feedback_entry)
        payload["feedback_loop"]["appended_feedback_log"] = args.feedback_log
    atomic_write_json(args.output, payload)
    if args.print_full:
        printed = payload
    else:
        printed = {
            "output": args.output,
            "learning_status": payload.get("learning_status"),
            "summary": payload.get("summary"),
            "jsonl_counts": payload.get("jsonl_counts"),
            "copy_efficiency": payload.get("checks", {}).get("live_tracker_copy_efficiency"),
            "live_admission_truth": payload.get("checks", {}).get("live_admission_truth"),
            "active_hotlane_tracking_evidence": payload.get("checks", {}).get("active_hotlane_tracking_evidence"),
            "active_hotlane_paper_copy_contract": payload.get("checks", {}).get(
                "active_hotlane_paper_copy_contract"
            ),
            "direct_data_source_spot_check": payload.get("checks", {}).get("direct_data_source_spot_check"),
            "feedback_loop": payload.get("feedback_loop"),
            "next_actions": payload.get("next_actions"),
        }
    print(json.dumps(printed, indent=2, sort_keys=True, default=str))
    return 0 if payload.get("learning_status") in {"GREEN", "WATCH"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
