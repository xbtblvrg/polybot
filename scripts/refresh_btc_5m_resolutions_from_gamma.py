#!/usr/bin/env python3
"""Build canonical BTC 5m resolution rows from Polymarket Gamma events.

The Binance-derived resolution patch is useful for research, but it must not
prove live readiness. This script fetches resolved Polymarket event state and
emits non-research-only rows only when Gamma reports a closed, resolved market
with an unambiguous winning outcome price.
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import sys
import tempfile
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.refresh_btc_5m_resolutions_from_history import (  # noqa: E402
    collect_btc_5m_windows,
    load_existing_rows,
    load_history_events,
    merge_key,
    slug_start,
)
from src.wallet_copy.models import num  # noqa: E402
from src.wallet_copy.http_client import PolymarketHttpClient, PolymarketRouteError  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
DEFAULT_FREEZE_SIDECAR = (
    "data/research/copy_freeze_near_bar_allpass_dryrun_sidecar_latest.json"
)
DEFAULT_FINGERPRINT_EVIDENCE = (
    "data/research/wide_policy_fingerprint_evidence_latest.json"
)
DEFAULT_FREEZE_PROFIT_STATE = "data/research/wide_exact_policy_paper_state.json"
CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS = (
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", action="append", default=[])
    parser.add_argument("--history-state", action="append", default=[])
    parser.add_argument("--profit-state", action="append", default=[])
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--freeze-sidecar", default=DEFAULT_FREEZE_SIDECAR)
    parser.add_argument(
        "--fingerprint-evidence",
        default=DEFAULT_FINGERPRINT_EVIDENCE,
    )
    parser.add_argument("--existing", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--summary-output", default=None)
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument(
        "--merge-existing",
        dest="merge_existing",
        action="store_true",
        default=True,
        help="Preserve the existing canonical history (default and safe for bounded refreshes).",
    )
    write_mode.add_argument(
        "--replace-existing",
        dest="merge_existing",
        action="store_false",
        help="Explicitly replace the output with only this refresh's rows.",
    )
    parser.add_argument("--max-windows", type=int, default=250)
    parser.add_argument(
        "--max-wall-runtime-s",
        type=float,
        default=0.0,
        help="Stop cleanly before the caller timeout and persist partial coverage instead of being killed.",
    )
    parser.add_argument("--timeout-s", type=float, default=8.0)
    parser.add_argument("--sleep-s", type=float, default=0.05)
    parser.add_argument("--user-agent", default="Mozilla/5.0")
    parser.add_argument(
        "--allow-source-base-overrides",
        action="store_true",
        help="Allow configured proxy/base URL overrides. Default keeps canonical Gamma refresh on direct source routes.",
    )
    args = parser.parse_args()
    if not args.ledger:
        args.ledger = ["data/research/wallet_copy_live_execution_state.json"]
    return args


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iso_from_ts(ts: int | float) -> str:
    return dt.datetime.fromtimestamp(float(ts), dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_gamma_dt(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _winner_from_prices(outcomes: list[Any], prices: list[Any]) -> str:
    if not outcomes or not prices or len(outcomes) != len(prices):
        return ""
    parsed_prices: list[float] = []
    for price in prices:
        try:
            parsed_prices.append(float(price))
        except (TypeError, ValueError):
            return ""
    max_price = max(parsed_prices)
    winners = [idx for idx, price in enumerate(parsed_prices) if price == max_price]
    if len(winners) != 1 or max_price < 0.99:
        return ""
    winner = str(outcomes[winners[0]] or "").strip()
    return winner if winner.lower() in {"up", "down"} else ""


def gamma_market_to_resolution(market: dict[str, Any], *, slug_hint: str = "") -> dict[str, Any] | None:
    """Convert a resolved Gamma market object into our resolution row schema."""

    if not isinstance(market, dict):
        return None
    slug = str(market.get("slug") or slug_hint or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    start = slug_start(slug)
    if start is None:
        return None
    closed = _boolish(market.get("closed"))
    end_dt = _parse_gamma_dt(market.get("endDate"))
    lifecycle_ended = bool(end_dt is not None and end_dt <= dt.datetime.now(dt.UTC))
    if not closed and not lifecycle_ended:
        return None
    resolution_status = str(market.get("umaResolutionStatus") or "").lower()
    if closed and resolution_status and resolution_status != "resolved":
        return None

    outcomes = _json_list(market.get("outcomes"))
    prices = _json_list(market.get("outcomePrices"))
    tokens = [str(item) for item in _json_list(market.get("clobTokenIds"))]
    winner = _winner_from_prices(outcomes, prices)
    if not winner:
        return None

    token_by_outcome = {
        str(outcome).strip().lower(): tokens[idx]
        for idx, outcome in enumerate(outcomes)
        if idx < len(tokens) and str(outcome).strip()
    }
    direction = winner.upper()
    source = "polymarket_gamma_resolved_outcome" if closed else "polymarket_gamma_ended_outcome_price"
    return {
        "asset": "BTC",
        "computed_at_iso": utc_now_iso(),
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "direction": direction,
        "expiry_iso": iso_from_ts(start + 300),
        "expiry_unix_ts": start + 300,
        "market_slug": slug,
        "no_token": token_by_outcome.get("down", ""),
        "question": str(market.get("question") or ""),
        "research_only": False,
        "resolution_precision": source,
        "source": source,
        "gamma_closed": closed,
        "gamma_lifecycle_ended": lifecycle_ended,
        "uma_resolution_status": resolution_status or None,
        "window_start_iso": iso_from_ts(start),
        "window_start_unix_ts": start,
        "window_type": "5m",
        "yes_token": token_by_outcome.get("up", ""),
    }


def _fetch_gamma_event(
    slug: str,
    *,
    timeout_s: float,
    user_agent: str,
    client: PolymarketHttpClient | None = None,
) -> list[dict[str, Any]]:
    source_client = client or PolymarketHttpClient(timeout_s=timeout_s, retries=1, user_agent=user_agent)
    payload, _route_report = source_client.get_json(
        GAMMA_EVENTS_URL,
        params={"slug": slug},
        headers={
            "Accept": "application/json",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
            "User-Agent": user_agent,
        },
        request_role="gamma_resolution_refresh",
        timeout_s=timeout_s,
    )
    return payload if isinstance(payload, list) else []


def fetch_gamma_resolution(
    slug: str,
    *,
    timeout_s: float,
    user_agent: str,
    client: PolymarketHttpClient | None = None,
) -> dict[str, Any] | None:
    for event in _fetch_gamma_event(slug, timeout_s=timeout_s, user_agent=user_agent, client=client):
        if not isinstance(event, dict):
            continue
        for market in event.get("markets") or []:
            row = gamma_market_to_resolution(market, slug_hint=slug)
            if row is not None and row.get("market_slug") == slug:
                return row
    return None


def _existing_research_slugs(path: str | None) -> list[str]:
    slugs: list[str] = []
    for row in load_existing_rows(path):
        slug = str(row.get("market_slug") or "")
        if slug.startswith("btc-updown-5m-") and row.get("research_only") is True:
            slugs.append(slug)
    return slugs


def _profit_state_slugs(paths: Iterable[str]) -> list[str]:
    slugs: list[str] = []
    for path in paths:
        payload = load_json(path, default={})
        candidates: list[Any] = []
        # Exact-policy paper states persist their newest unresolved evidence
        # directly under orders[].  Treat those windows as first-class
        # resolution priorities so a paper lane does not wait behind the
        # historical backlog before its F1 evidence can settle.
        for row in payload.get("orders") or []:
            if not isinstance(row, dict) or row.get("resolved") is True:
                continue
            slug = str(row.get("market_slug") or "")
            if slug.startswith("btc-updown-5m-"):
                slugs.append(slug)
        for key in (
            "best_candidate",
            "forward_candidate",
            "runtime_admission_candidate",
            "best_runtime_candidate",
            "forward_runtime_candidate",
        ):
            if isinstance(payload.get(key), dict):
                candidates.append(payload[key])
        for key in (
            "ranked_candidates",
            "pass_candidates",
            "forward_tracking_queue",
            "forward_queue_runtime_candidates",
        ):
            for row in payload.get(key) or []:
                if isinstance(row, dict):
                    candidates.append(row)
        for candidate in candidates:
            resolution = (
                candidate.get("resolution_evidence_summary")
                if isinstance(candidate.get("resolution_evidence_summary"), dict)
                else {}
            )
            for key in (
                "newer_than_resolution_index_window_sample",
                "matured_unresolved_window_sample",
                "unresolved_window_sample",
            ):
                for slug in resolution.get(key) or []:
                    if str(slug).startswith("btc-updown-5m-"):
                        slugs.append(str(slug))
            summary = candidate.get("summary") if isinstance(candidate.get("summary"), dict) else {}
            window_metrics = summary.get("window_metrics") if isinstance(summary.get("window_metrics"), dict) else {}
            for item in window_metrics.get("orders_per_window_top") or []:
                if isinstance(item, dict):
                    slug = str(item.get("window") or "")
                    if slug.startswith("btc-updown-5m-"):
                        slugs.append(slug)
    return slugs


def _freeze_primary_profit_slugs(
    *,
    sidecar_path: str,
    evidence_path: str,
) -> tuple[list[str], dict[str, Any]]:
    """Return fingerprint-strict unresolved windows for the frozen primary."""

    sidecar = load_json(sidecar_path, default={})
    primary = sidecar.get("primary") if isinstance(sidecar, dict) else {}
    wallet = str((primary or {}).get("wallet") or "").lower()
    fingerprint = str((primary or {}).get("wide_policy_fingerprint") or "")
    evidence = load_json(evidence_path, default={})
    matched_cell: dict[str, Any] = {}
    for cell in evidence.get("cells") or [] if isinstance(evidence, dict) else []:
        if not isinstance(cell, dict):
            continue
        identity = cell.get("identity") if isinstance(cell.get("identity"), dict) else {}
        if (
            str(identity.get("wallet") or "").lower() == wallet
            and str(cell.get("wide_policy_fingerprint") or "") == fingerprint
        ):
            matched_cell = cell
            break
    resolution = (
        matched_cell.get("resolution_evidence_summary")
        if isinstance(matched_cell.get("resolution_evidence_summary"), dict)
        else {}
    )
    slugs = [
        str(slug)
        for slug in resolution.get("matured_unresolved_windows") or []
        if str(slug).startswith("btc-updown-5m-")
    ]
    return slugs, {
        "wallet": wallet or None,
        "wide_policy_fingerprint": fingerprint or None,
        "matched": bool(matched_cell),
        "matured_unresolved_window_count": len(slugs),
    }


def _live_ledger_filled_market_slugs(paths: Iterable[str]) -> list[str]:
    slugs: list[str] = []
    for path in paths:
        payload = load_json(path, default={})
        if not isinstance(payload, dict):
            continue
        for order in payload.get("orders") or []:
            if not isinstance(order, dict):
                continue
            status = str(order.get("final_status") or order.get("status") or "").upper()
            if status != "FILLED":
                continue
            slug = str(order.get("market_slug") or "")
            if slug.startswith("btc-updown-5m-"):
                slugs.append(slug)
    return slugs


def _ordered_unique_slugs(slugs: Iterable[str]) -> list[str]:
    keyed: dict[str, int] = {}
    for slug in slugs:
        if not str(slug).startswith("btc-updown-5m-"):
            continue
        keyed.setdefault(str(slug), slug_start(str(slug)) or 0)
    return [slug for slug, _ in sorted(keyed.items(), key=lambda item: item[1], reverse=True)]


def _canonical_gamma_source_route_guard(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, str]]:
    prior_values: dict[str, str] = {}
    if bool(getattr(args, "allow_source_base_overrides", False)):
        return {
            "enabled": False,
            "allow_source_base_overrides": True,
            "role": "canonical_gamma_resolution_refresh_source_route_guard_disabled_by_operator_flag",
        }, prior_values
    for env_var in CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS:
        if env_var in os.environ:
            prior_values[env_var] = os.environ.pop(env_var)
    return {
        "enabled": True,
        "allow_source_base_overrides": False,
        "cleared_env_vars": sorted(prior_values),
        "role": "canonical_gamma_resolution_refresh_direct_source_route_guard",
    }, prior_values


def _restore_source_route_env(prior_values: dict[str, str]) -> None:
    for env_var, value in prior_values.items():
        os.environ[env_var] = value


def _merge_rows(existing_path: str | None, generated_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = {merge_key(row): row for row in load_existing_rows(existing_path)}
    for row in generated_rows:
        merged[merge_key(row)] = row
    return sorted(
        merged.values(),
        key=lambda row: (int(num(row.get("expiry_unix_ts"), 0)), str(row.get("condition_id") or "")),
    )


def _atomic_write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    """Replace a JSONL file without exposing a truncated intermediate file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True, default=str))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _locked_merge_and_write_rows(
    *,
    existing_path: str | None,
    output_path: Path,
    generated_rows: list[dict[str, Any]],
    merge_existing: bool,
) -> list[dict[str, Any]]:
    """Serialize merge-at-commit so concurrent refreshes cannot lose rows."""

    lock_path = output_path.with_name(f".{output_path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            # Re-read only after acquiring the commit lock. A refresh may have
            # completed while this process was fetching Gamma outcomes.
            rows = _merge_rows(existing_path, generated_rows) if merge_existing else list(generated_rows)
            _atomic_write_rows(output_path, rows)
            return rows
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def main() -> int:
    args = parse_args()
    source_route_guard, prior_source_route_env = _canonical_gamma_source_route_guard(args)
    started = time.perf_counter()
    background_slugs: list[str] = []
    ledger_slugs = _live_ledger_filled_market_slugs(args.ledger)
    for history_state in args.history_state:
        background_slugs.extend(
            (row.get("market_slug") or "") for row in collect_btc_5m_windows(load_history_events([history_state])).values()
        )
    background_slugs.extend(_existing_research_slugs(args.existing))
    manual_priority = _ordered_unique_slugs(args.market_slug)
    freeze_path_enabled = any(
        Path(path).resolve() == (ROOT / DEFAULT_FREEZE_PROFIT_STATE).resolve()
        for path in args.profit_state
    )
    if freeze_path_enabled:
        freeze_slugs, freeze_primary_priority = _freeze_primary_profit_slugs(
            sidecar_path=args.freeze_sidecar,
            evidence_path=args.fingerprint_evidence,
        )
    else:
        freeze_slugs, freeze_primary_priority = [], {
            "wallet": None,
            "wide_policy_fingerprint": None,
            "matched": False,
            "matured_unresolved_window_count": 0,
            "enabled": False,
        }
    freeze_priority = [
        slug
        for slug in _ordered_unique_slugs(freeze_slugs)
        if slug not in set(manual_priority)
    ]
    explicit_head = [*manual_priority, *freeze_priority]
    ledger_priority = [
        slug
        for slug in _ordered_unique_slugs(ledger_slugs)
        if slug not in set(explicit_head)
    ]
    explicit_priority = [*explicit_head, *ledger_priority]
    profit_priority = [
        slug for slug in _ordered_unique_slugs(_profit_state_slugs(args.profit_state)) if slug not in set(explicit_priority)
    ]
    priority = [*explicit_priority, *profit_priority]
    priority_set = set(priority)
    background = [slug for slug in _ordered_unique_slugs(background_slugs) if slug not in priority_set]
    selected_slugs = [*priority, *background]
    if args.max_windows > 0:
        selected_slugs = selected_slugs[: args.max_windows]

    rows: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    skipped_due_budget: list[str] = []
    try:
        client = PolymarketHttpClient(timeout_s=float(args.timeout_s), retries=1, user_agent=str(args.user_agent))
        for idx, slug in enumerate(selected_slugs, start=1):
            if float(args.max_wall_runtime_s or 0.0) > 0:
                elapsed_s = time.perf_counter() - started
                remaining_s = float(args.max_wall_runtime_s) - elapsed_s
                if remaining_s <= max(float(args.timeout_s), 1.0):
                    skipped_due_budget = selected_slugs[idx - 1 :]
                    break
            try:
                row = fetch_gamma_resolution(
                    slug,
                    timeout_s=float(args.timeout_s),
                    user_agent=str(args.user_agent),
                    client=client,
                )
            except PolymarketRouteError as exc:
                route_report = exc.route_report if isinstance(exc.route_report, dict) else {}
                failed.append(
                    {
                        "slug": slug,
                        "error": f"{type(exc).__name__}: {exc}",
                        "route_class": route_report.get("route_class"),
                        "route_status": route_report.get("status"),
                        "source_base_override_configured": route_report.get("source_base_override_configured"),
                        "source_proxy_configured": route_report.get("source_proxy_configured"),
                        "required_operator_inputs": route_report.get("required_operator_inputs"),
                    }
                )
                continue
            except Exception as exc:  # noqa: BLE001 - persisted diagnostics are more useful than crashing early.
                failed.append({"slug": slug, "error": f"{type(exc).__name__}: {exc}"})
                continue
            if row is None:
                failed.append({"slug": slug, "error": "not_closed_resolved_or_unambiguous"})
            else:
                rows.append(row)
            if args.sleep_s > 0 and idx < len(selected_slugs):
                time.sleep(float(args.sleep_s))
    finally:
        _restore_source_route_env(prior_source_route_env)

    output = Path(args.output)
    output_rows = _locked_merge_and_write_rows(
        existing_path=args.existing,
        output_path=output,
        generated_rows=rows,
        merge_existing=bool(args.merge_existing),
    )

    summary = {
        "canonical_source": "polymarket_gamma_resolved_outcome",
        "existing": args.existing,
        "failed": failed[:100],
        "failed_count": len(failed),
        "fetched_canonical_rows": len(rows),
        "freeze_primary_priority": freeze_primary_priority,
        "freeze_priority_sample": freeze_priority[:25],
        "freeze_priority_windows": len(freeze_priority),
        "history_states": args.history_state,
        "ledger_priority_windows": len(_ordered_unique_slugs(ledger_slugs)),
        "ledger_states": args.ledger,
        "max_wall_runtime_s": float(args.max_wall_runtime_s or 0.0),
        "merge_existing": bool(args.merge_existing),
        "output": str(output),
        "output_rows": len(output_rows),
        "partial_refresh": bool(skipped_due_budget),
        "priority_window_count": len(priority),
        "priority_window_sample": priority[:25],
        "profit_priority_windows": len(profit_priority),
        "profit_priority_sample": profit_priority[:25],
        "profit_states": args.profit_state,
        "requested_windows": len(selected_slugs),
        "research_only": False,
        "skipped_due_budget_count": len(skipped_due_budget),
        "skipped_due_budget_sample": skipped_due_budget[:25],
        "source_route_guard": source_route_guard,
        "status": "PARTIAL_BUDGET_EXHAUSTED" if skipped_due_budget else ("PASS" if rows else "ANALYZE"),
    }
    summary_output = Path(args.summary_output) if args.summary_output else output.with_suffix(".gamma_summary.json")
    atomic_write_json(summary_output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 2 if skipped_due_budget else (0 if rows else 2)


if __name__ == "__main__":
    raise SystemExit(main())
