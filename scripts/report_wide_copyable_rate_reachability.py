#!/usr/bin/env python3
"""Measure whether the WIDE copyable-rate gate is denominator-reachable.

This is a read-only diagnostic.  It derives the frozen-policy-addressable
denominator from the prospective writer's F1-F4 terminal ledger and compares
it with the denominator currently consumed by candidate standings.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


COPYABLE = "COPYABLE_EXACT_POLICY_PAPER_FILL"
OUT_OF_SLICE = "REFUSED_ALPHA_PROFILE_FILTER"
METADATA_MISSING = "REFUSED_METADATA_MISSING"
STALE_RECEIPT = "REFUSED_STALE_RECEIPT_TO_FETCH"
PAPER_PREFETCH_DELAY = "REFUSED_PAPER_PREFETCH_DELAY_GT_5S"
INSUFFICIENT_DEPTH = "REFUSED_INSUFFICIENT_DEPTH_WITHIN_SLIPPAGE_CAP"
RESIDUAL_KEYS = (
    "out_of_selected_slice",
    "metadata_missing",
    "slippage_cap",
    "no_ask_liquidity",
    "stale_receipt",
)


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _terminal(row: dict[str, Any]) -> str:
    detail = row.get("f1_f4_terminal")
    return str((detail if isinstance(detail, dict) else {}).get("terminal") or "")


def _residual_bucket(terminal: str) -> str | None:
    if terminal == COPYABLE:
        return None
    if terminal == OUT_OF_SLICE:
        return "out_of_selected_slice"
    if terminal == METADATA_MISSING:
        return "metadata_missing"
    if terminal in {STALE_RECEIPT, PAPER_PREFETCH_DELAY}:
        return "stale_receipt"
    if terminal in {"REFUSED_PRICE_ABOVE_SLIPPAGE_CAP", INSUFFICIENT_DEPTH}:
        return "slippage_cap"
    if terminal == "REFUSED_NO_ASK_LIQUIDITY":
        return "no_ask_liquidity"
    raise ValueError(f"unknown terminal taxonomy: {terminal or 'MISSING_TERMINAL'}")


def _rate(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 6) if denominator else None


def _numeric_summary(values: list[float]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {"measured_rows": 0, "min": None, "median": None, "p90": None, "max": None}
    return {
        "measured_rows": len(ordered),
        "min": round(ordered[0], 6),
        "median": round(ordered[len(ordered) // 2], 6),
        "p90": round(ordered[int(0.90 * (len(ordered) - 1))], 6),
        "max": round(ordered[-1], 6),
    }


def _source_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("transaction_hash") or "").lower(),
        str(row.get("source_event_id") or row.get("log_index") or ""),
        _wallet(row.get("wallet") or row.get("selected_wallet")),
    )


def _observation(
    terminal: dict[str, Any],
    source: dict[str, Any],
    token_metadata: dict[str, Any],
) -> dict[str, Any]:
    token_id = str(terminal.get("token_id") or (source.get("decoded") or {}).get("asset") or "")
    meta = token_metadata.get(token_id) if isinstance(token_metadata.get(token_id), dict) else {}
    missing_fields = [
        field for field in ("market_slug", "condition_id", "outcome") if not meta.get(field)
    ]
    observed_at_s = float(source.get("received_at_s") or source.get("captured_at_s") or 0)
    event_ts = float(source.get("event_ts") or source.get("block_ts") or 0)
    market_slug = str(meta.get("market_slug") or "")
    expected_slug = (
        f"btc-updown-5m-{int(event_ts // 300 * 300)}" if event_ts > 0 else ""
    )
    expected_tokens = {
        str(candidate_token)
        for candidate_token, candidate_meta in token_metadata.items()
        if isinstance(candidate_meta, dict)
        and str(candidate_meta.get("market_slug") or "") == expected_slug
    }
    if market_slug.startswith("btc-updown-5m-"):
        btc5m_membership = "CONFIRMED_BTC5M"
    elif expected_tokens and token_id not in expected_tokens:
        btc5m_membership = "DEFINITELY_NOT_TIMESTAMP_BTC5M"
    else:
        btc5m_membership = "UNKNOWN"
    market_open: bool | None = None
    try:
        market_start = int(market_slug.rsplit("-", 1)[1])
        market_open = bool(market_start <= observed_at_s < market_start + 300)
    except (ValueError, IndexError):
        pass
    api_latency = source.get("api_latency_s")
    return {
        "attempt_id": terminal.get("attempt_id"),
        "wallet": _wallet(terminal.get("wallet")),
        "terminal": _terminal(terminal),
        "token_id": token_id or None,
        "market_slug": market_slug or None,
        "market_open_at_observation": market_open,
        "event_ts": event_ts or None,
        "observed_at_s": observed_at_s or None,
        "api_latency_s": float(api_latency) if api_latency is not None else None,
        "api_latency_status": (
            "MEASURED" if api_latency is not None else "NOT_APPLICABLE_POLYGON_WS"
        ),
        "ws_receive_lag_signed_s": source.get("ws_receive_lag_signed_s"),
        "receipt_to_fetch_ms": terminal.get("receipt_to_fetch_ms"),
        "book_fetch_ms": terminal.get("book_fetch_ms"),
        "upstream_to_fanout_ms": terminal.get("upstream_to_fanout_ms"),
        "fanout_to_fetch_ms": terminal.get("fanout_to_fetch_ms"),
        "missing_metadata_fields": missing_fields,
        "metadata_token_status": (
            "TOKEN_ABSENT_FROM_CACHE"
            if token_id not in token_metadata
            else "PARTIAL_METADATA"
            if missing_fields
            else "COMPLETE"
        ),
        "btc5m_membership": btc5m_membership,
        "source_event_found": bool(source),
    }


def _observation_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "market_open_at_observation": dict(
            Counter(
                "true" if row.get("market_open_at_observation") is True
                else "false" if row.get("market_open_at_observation") is False
                else "unknown"
                for row in rows
            )
        ),
        "api_latency_s": {
            "measured_rows": sum(row.get("api_latency_s") is not None for row in rows),
            "max": max(
                (float(row["api_latency_s"]) for row in rows if row.get("api_latency_s") is not None),
                default=None,
            ),
            "not_applicable_polygon_ws_rows": sum(
                row.get("api_latency_status") == "NOT_APPLICABLE_POLYGON_WS"
                for row in rows
            ),
        },
        "receipt_to_fetch_ms": _numeric_summary(
            [float(row["receipt_to_fetch_ms"]) for row in rows if row.get("receipt_to_fetch_ms") is not None]
        ),
        "book_fetch_ms": _numeric_summary(
            [float(row["book_fetch_ms"]) for row in rows if row.get("book_fetch_ms") is not None]
        ),
        "upstream_to_fanout_ms": _numeric_summary(
            [float(row["upstream_to_fanout_ms"]) for row in rows if row.get("upstream_to_fanout_ms") is not None]
        ),
        "fanout_to_fetch_ms": _numeric_summary(
            [float(row["fanout_to_fetch_ms"]) for row in rows if row.get("fanout_to_fetch_ms") is not None]
        ),
        "missing_metadata_fields": dict(
            Counter(field for row in rows for field in row.get("missing_metadata_fields") or [])
        ),
        "metadata_token_status": dict(Counter(row.get("metadata_token_status") for row in rows)),
        "btc5m_membership": dict(Counter(row.get("btc5m_membership") for row in rows)),
        "source_event_found": sum(row.get("source_event_found") is True for row in rows),
    }


def build_report(
    measurement: dict[str, Any],
    standings: dict[str, Any],
    *,
    source_events: list[dict[str, Any]] | None = None,
    token_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the diagnostic without mutating either source document."""

    manifest = measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    identities = (
        manifest.get("wallet_policy_identities")
        if isinstance(manifest.get("wallet_policy_identities"), dict)
        else {}
    )
    measured = (
        measurement.get("wallets")
        if isinstance(measurement.get("wallets"), dict)
        else {}
    )
    standings_rows = {
        _wallet(row.get("wallet")): row
        for row in standings.get("standings") or []
        if isinstance(row, dict) and _wallet(row.get("wallet"))
    }
    terminal_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in measurement.get("attempt_terminals") or []:
        if not isinstance(row, dict):
            continue
        wallet = _wallet(row.get("wallet"))
        if wallet:
            terminal_rows[wallet].append(row)
    source_index = {
        _source_key(row): row
        for row in source_events or []
        if isinstance(row, dict) and _source_key(row)[0]
    }
    metadata = token_metadata if isinstance(token_metadata, dict) else {}
    infrastructure_observations: list[dict[str, Any]] = []

    wallets: list[dict[str, Any]] = []
    for wallet in sorted(set(identities) | set(measured) | set(standings_rows)):
        identity = identities.get(wallet) if isinstance(identities.get(wallet), dict) else {}
        paper = measured.get(wallet) if isinstance(measured.get(wallet), dict) else {}
        standing = standings_rows.get(wallet) or {}
        terminals = terminal_rows.get(wallet, [])
        terminal_taxonomy = Counter(_terminal(row) or "MISSING_TERMINAL" for row in terminals)
        residual = Counter({key: 0 for key in RESIDUAL_KEYS})
        copyable = 0
        for row in terminals:
            terminal = _terminal(row)
            if terminal in {METADATA_MISSING, STALE_RECEIPT, PAPER_PREFETCH_DELAY}:
                infrastructure_observations.append(
                    _observation(row, source_index.get(_source_key(row), {}), metadata)
                )
            if terminal == COPYABLE:
                copyable += 1
            else:
                residual[_residual_bucket(terminal) or "slippage_cap"] += 1

        raw_input_rows = len(terminals)
        residual_total = sum(residual.values())
        policy_addressable_attempts = (
            copyable
            + residual["slippage_cap"]
            + residual["no_ask_liquidity"]
            + residual["stale_receipt"]
        )
        as_built_attempts = int(
            standing.get("attempted_buy_events")
            or paper.get("attempted_exact_policy_buys")
            or 0
        )
        as_built_copyable = int(
            standing.get("copyable_buy_events")
            or paper.get("copyable_exact_policy_buys")
            or 0
        )
        checks = {
            "raw_input_reconciles": raw_input_rows == copyable + residual_total,
            "copyable_matches_as_built_numerator": copyable == as_built_copyable,
            "frozen_policy_identity_present": bool(identity),
        }
        wallets.append(
            {
                "wallet": wallet,
                "wide_policy_fingerprint": identity.get("wide_policy_fingerprint"),
                "raw_input_rows": raw_input_rows,
                "copyable_buy_events": copyable,
                "as_built": {
                    "denominator_name": "attempted_buy_events",
                    "attempted_buy_events": as_built_attempts,
                    "copyable_rate_pct": _rate(as_built_copyable, as_built_attempts),
                    "source": "wide_candidate_standings",
                },
                "policy_addressable": {
                    "denominator_name": "f2_pass_frozen_move_slice_attempts",
                    "attempted_buy_events": policy_addressable_attempts,
                    "copyable_rate_pct": _rate(copyable, policy_addressable_attempts),
                    "basis": (
                        "F2_alpha_profile=PASS; the prospective writer computes this "
                        "only when move_slice_key is in the wallet's frozen policy"
                    ),
                },
                "denominator_comparison": {
                    "equal": policy_addressable_attempts == as_built_attempts,
                    "as_built_minus_policy_addressable": (
                        as_built_attempts - policy_addressable_attempts
                    ),
                },
                "excluded_attempt_taxonomy": dict(residual),
                "terminal_taxonomy": dict(sorted(terminal_taxonomy.items())),
                "checks": checks,
                "copyable_rate_gte_70": bool(
                    policy_addressable_attempts
                    and copyable / policy_addressable_attempts >= 0.70
                ),
            }
        )

    winners = [row["wallet"] for row in wallets if row["copyable_rate_gte_70"]]
    metric_defect_wallets = [
        row["wallet"]
        for row in wallets
        if row["copyable_rate_gte_70"]
        and (row["as_built"]["copyable_rate_pct"] or 0.0) < 70.0
    ]
    all_reconciled = all(
        row["checks"]["raw_input_reconciles"]
        and row["checks"]["copyable_matches_as_built_numerator"]
        for row in wallets
    )
    missing_identity_wallets = [
        row["wallet"]
        for row in wallets
        if row["checks"]["frozen_policy_identity_present"] is not True
        and row["raw_input_rows"] > 0
    ]
    infrastructure_by_class = {
        terminal: _observation_summary(
            [row for row in infrastructure_observations if row.get("terminal") == terminal]
        )
        for terminal in (METADATA_MISSING, STALE_RECEIPT, PAPER_PREFETCH_DELAY)
    }
    infrastructure_by_wallet = {
        wallet: {
            terminal: _observation_summary(
                [
                    row
                    for row in infrastructure_observations
                    if row.get("wallet") == wallet and row.get("terminal") == terminal
                ]
            )
            for terminal in (METADATA_MISSING, STALE_RECEIPT, PAPER_PREFETCH_DELAY)
        }
        for wallet in sorted({str(row.get("wallet") or "") for row in infrastructure_observations})
        if wallet
    }
    if metric_defect_wallets:
        branch = "OPEN_DEFECT_P_METRIC_MISDENOMINATED"
    elif not winners:
        branch = "RETIRE_WIDE_EXACT_POLICY_AS_NEAR_TERM_MONEY_ROUTE"
    else:
        branch = "GATE_REACHABLE_WITH_CURRENT_DENOMINATOR"
    terminal_reconciliation = (
        measurement.get("terminal_reconciliation")
        if isinstance(measurement.get("terminal_reconciliation"), dict)
        else {}
    )
    standings_summary = (
        standings.get("summary") if isinstance(standings.get("summary"), dict) else {}
    )
    return {
        "schema_version": 1,
        "kind": "wide_copyable_rate_reachability_diagnostic",
        "flow_stage": "LEARN/PROMOTE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "admission_authority": False,
        "measurement_only": True,
        "threshold_pct_unchanged": 70.0,
        "manifest_id": manifest.get("manifest_id"),
        "cohort_id": (measurement.get("cohort") or {}).get("cohort_id"),
        "wallets": wallets,
        "infrastructure_refusal_decomposition": {
            "source_generation": (measurement.get("cohort") or {}).get("run_id"),
            "by_refusal_class": infrastructure_by_class,
            "by_wallet": infrastructure_by_wallet,
            "row_sample": infrastructure_observations[:100],
        },
        "summary": {
            "measured_wallets": len(wallets),
            "policy_addressable_rate_gte_70_wallets": winners,
            "policy_addressable_rate_gte_70_count": len(winners),
            "metric_misdenomination_wallets": metric_defect_wallets,
            "all_wallet_reconciliations_pass": all_reconciled,
            "frozen_policy_identity_missing_wallets": missing_identity_wallets,
            "frozen_policy_identity_missing_count": len(missing_identity_wallets),
            "input_rows": int(terminal_reconciliation.get("input_rows") or 0),
            "terminal_rows": int(terminal_reconciliation.get("terminal_rows") or 0),
            "input_equals_terminal": terminal_reconciliation.get("input_equals_terminal") is True,
            "raw_wallet_terminal_rows": sum(row["raw_input_rows"] for row in wallets),
            "dual_gate_winners_before": int(standings_summary.get("dual_gate_winners") or 0),
            "dual_gate_winners_after": int(standings_summary.get("dual_gate_winners") or 0),
            "winner_wallets_before": standings.get("winner_wallets") or [],
            "winner_wallets_after": standings.get("winner_wallets") or [],
            "decision_branch": branch,
        },
        "decision_rule": {
            "metric_defect": (
                "open defect_P only if policy-addressable copyable rate reaches "
                ">=70% while the all-attempts rate provably cannot"
            ),
            "retire_route": (
                "if policy-addressable copyable rate is <70% for every wallet, "
                "retire WIDE exact-policy promotion as a near-term money route"
            ),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--measurement",
        default="data/research/wide_exact_policy_paper_state.json",
    )
    parser.add_argument(
        "--token-metadata",
        default="data/research/wide_token_metadata_cache.json",
    )
    parser.add_argument("--source-jsonl", default="")
    parser.add_argument(
        "--standings",
        default="data/research/wide_candidate_standings_latest.json",
    )
    parser.add_argument(
        "--output",
        default="data/research/wide_copyable_rate_reachability_latest.json",
    )
    args = parser.parse_args()
    measurement = load_json(args.measurement, default={})
    run_id = str((measurement.get("cohort") or {}).get("run_id") or "")
    source_path = Path(
        args.source_jsonl
        or f"data/research/polygon_orderfilled_ws_capture_alpha_decay_{run_id}.jsonl"
    )
    terminal_keys = {
        _source_key(row)
        for row in measurement.get("attempt_terminals") or []
        if isinstance(row, dict)
        and _terminal(row) in {METADATA_MISSING, STALE_RECEIPT, PAPER_PREFETCH_DELAY}
    }
    source_events = []
    if source_path.exists() and terminal_keys:
        with source_path.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict) and _source_key(row) in terminal_keys:
                    source_events.append(row)
    report = build_report(
        measurement,
        load_json(args.standings, default={}),
        source_events=source_events,
        token_metadata=load_json(args.token_metadata, default={}),
    )
    if not report["summary"]["all_wallet_reconciliations_pass"]:
        raise SystemExit("wallet terminal reconciliation failed")
    if not (
        report["summary"]["input_equals_terminal"]
        and report["summary"]["raw_wallet_terminal_rows"]
        == report["summary"]["terminal_rows"]
    ):
        raise SystemExit("input_rows != terminal_rows")
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
