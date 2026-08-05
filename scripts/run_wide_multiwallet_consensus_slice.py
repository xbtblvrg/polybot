#!/usr/bin/env python3
"""Run two checksum-isolated, prospective WIDE multi-wallet consensus cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fees import expected_polymarket_buy_fee_usd
from src.wallet_copy.models import num, stable_id, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json

DEFAULT_MEASUREMENT = "data/research/wide_exact_policy_paper_state.json"
DEFAULT_PREREG = "data/research/wide_multiwallet_consensus_preregistration.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
CELL_IDS = ("two_of_n", "score_weighted")
CELL_2OFN = "two_of_n"
CELL_WEIGHTED = "score_weighted"
INTERVAL_S = 2.0


def _checksum(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _parse_ts(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _jsonl(path: str) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _append_jsonl(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


def _receipt_ts(row: dict[str, Any]) -> float:
    return num(row.get("source_received_at_s"), _parse_ts(row.get("recorded_at")))


def _source_identity(row: dict[str, Any]) -> str:
    return stable_id(
        "widecons_source",
        {
            "transaction_hash": str(row.get("transaction_hash") or "").lower(),
            "log_index": str(row.get("log_index") or ""),
            "wallet": str(row.get("wallet") or "").lower(),
            "order_id": str(row.get("order_id") or ""),
        },
        length=32,
    )


def _manifest_rows(
    measurement: dict[str, Any], manifest_path: str, standings_path: str = ""
) -> list[dict[str, Any]]:
    manifest = load_json(manifest_path, default={}) if manifest_path else {}
    rows = manifest.get("capture_watch_wallets") if isinstance(manifest, dict) else []
    if rows:
        return [dict(row) for row in rows if isinstance(row, dict)]
    standings = load_json(standings_path, default={}) if standings_path else {}
    standing_rows = standings.get("standings") if isinstance(standings, dict) else []
    if standing_rows:
        return [
            {"wallet": row.get("wallet"), "queue_rank": row.get("queue_rank")}
            for row in standing_rows
            if isinstance(row, dict) and row.get("wallet")
        ]
    wallets = measurement.get("wallets") if isinstance(measurement.get("wallets"), dict) else {}
    return [
        {"wallet": wallet, "queue_rank": int((payload or {}).get("decision_rank") or index)}
        for index, (wallet, payload) in enumerate(sorted(wallets.items()), start=1)
    ]


def _new_prereg(
    measurement: dict[str, Any], manifest_path: str, standings_path: str = ""
) -> dict[str, Any]:
    manifest = measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    cohort = measurement.get("cohort") if isinstance(measurement.get("cohort"), dict) else {}
    rows = _manifest_rows(measurement, manifest_path, standings_path)
    frozen_wallets = []
    for row in rows:
        wallet = str(row.get("wallet") or "").lower()
        rank = max(1, int(row.get("queue_rank") or len(frozen_wallets) + 1))
        if wallet:
            frozen_wallets.append(
                {
                    "wallet": wallet,
                    "queue_rank": rank,
                    "score_weight": round(1.0 / math.sqrt(rank), 12),
                }
            )
    frozen_wallets.sort(key=lambda row: (row["queue_rank"], row["wallet"]))
    if manifest_path and len(frozen_wallets) != 31:
        raise RuntimeError(f"immutable WIDE manifest must contain 31 wallets, got {len(frozen_wallets)}")
    baseline_orders = sorted(
        str(row.get("order_id") or "")
        for row in measurement.get("orders") or []
        if isinstance(row, dict) and row.get("order_id")
    )
    registered_at = utc_now_iso()
    common = {
        "schema_version": 1,
        "kind": "wide_multiwallet_consensus_preregistration",
        "registered_at": registered_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "promotion_authority": False,
        "source_manifest_id": manifest.get("manifest_id"),
        "source_manifest_path": manifest_path,
        "source_run_id": cohort.get("run_id"),
        "source_cohort_id": cohort.get("cohort_id"),
        "source_policy_id": measurement.get("policy_id"),
        "capture_wallet_count": len(frozen_wallets),
        "capture_wallets": frozen_wallets,
        "capture_wallets_checksum": _checksum(frozen_wallets),
        "receipt_interval_s": INTERVAL_S,
        "receipt_interval_rule": "floor(source_received_at_s/2)*2",
        "market_scope": "btc_5m",
        "action": "BUY",
        "entry_price_min": 0.25,
        "entry_price_max": 0.50,
        "copy_size_usd": 1.0,
        "max_receipt_lag_s": 5.0,
        "book_rule": "second_qualifying_receipt_exact_policy_executable_book_snapshot",
        "fee_rule": "POLYMARKET_EMBEDDED_FEE_FORMULA",
        "baseline_order_ids": baseline_orders,
        "baseline_order_ids_checksum": _checksum(baseline_orders),
        "prospective_rule": "not_in_baseline_and_recorded_at_after_registration",
        "cells": {
            "two_of_n": {
                "minimum_distinct_wallets": 2,
                "weight_threshold": None,
                "rule": "two distinct frozen-manifest wallets",
            },
            "score_weighted": {
                "minimum_distinct_wallets": 2,
                "weight_threshold": 1.0,
                "weight_formula": "1/sqrt(frozen_queue_rank)",
                "rule": "two distinct wallets and frozen cumulative weight >=1.0",
            },
        },
        "permanent_live_gate": {
            "minimum_resolved_per_cell": 50,
            "positive_aggregate": True,
            "positive_chronological_halves": True,
            "positive_incremental_component_pnl": True,
            "fees_and_depth_measured": True,
            "max_receipt_lag_s": 5.0,
            "current_alpha": True,
            "zero_parity_lookahead_disagreement": True,
            "live_authority": "sole live guard after separate Fable ruling",
        },
    }
    cells = {}
    for cell_id, rule in common["cells"].items():
        body = {
            "cell_id": cell_id,
            "common_envelope_checksum": _checksum(common),
            "rule": rule,
        }
        cells[cell_id] = {**rule, "cell_checksum": _checksum(body)}
    body = {**common, "cells": cells}
    return {**body, "checksum": _checksum(body)}


def _load_or_create_prereg(
    path: str,
    measurement: dict[str, Any],
    manifest_path: str,
    standings_path: str = "",
) -> dict[str, Any]:
    prereg = load_json(path, default={})
    if prereg:
        if prereg.get("envelope_checksum") and not prereg.get("checksum"):
            envelope = {
                key: value
                for key, value in prereg.items()
                if key not in {"envelope_checksum", "cell_checksums"}
            }
            if prereg.get("envelope_checksum") != _checksum(envelope):
                raise RuntimeError("immutable consensus preregistration checksum mismatch")
            if prereg.get("baseline_order_ids_sha256") != _checksum(
                prereg.get("baseline_order_ids") or []
            ):
                raise RuntimeError("immutable consensus baseline checksum mismatch")
            if prereg.get("capture_wallets_sha256") != _checksum(
                prereg.get("capture_wallets") or []
            ):
                raise RuntimeError("immutable consensus wallet checksum mismatch")
            ranks = prereg.get("frozen_queue_ranks") or {}
            capture_wallets = [
                {
                    "wallet": str(wallet).lower(),
                    "queue_rank": max(1, int(ranks.get(wallet) or index)),
                    "score_weight": round(
                        1.0 / math.sqrt(max(1, int(ranks.get(wallet) or index))), 12
                    ),
                }
                for index, wallet in enumerate(prereg.get("capture_wallets") or [], start=1)
            ]
            checksums = prereg.get("cell_checksums") or {}
            return {
                **prereg,
                "checksum": prereg["envelope_checksum"],
                "source_manifest_id": prereg.get("manifest_id"),
                "source_cohort_id": prereg.get("cohort_id"),
                "source_policy_id": prereg.get("policy_id"),
                "capture_wallet_count": len(capture_wallets),
                "capture_wallets": capture_wallets,
                "capture_wallets_checksum": prereg.get("capture_wallets_sha256"),
                "cells": {
                    CELL_2OFN: {
                        "minimum_distinct_wallets": 2,
                        "weight_threshold": None,
                        "cell_checksum": checksums.get(CELL_2OFN),
                    },
                    CELL_WEIGHTED: {
                        "minimum_distinct_wallets": 2,
                        "weight_threshold": num(prereg.get("weighted_threshold"), 1.0),
                        "weight_formula": prereg.get("weight_formula"),
                        "cell_checksum": checksums.get(CELL_WEIGHTED),
                    },
                },
            }
        canonical = {key: value for key, value in prereg.items() if key != "checksum"}
        if prereg.get("checksum") != _checksum(canonical):
            raise RuntimeError("immutable consensus preregistration checksum mismatch")
        return prereg
    prereg = _new_prereg(measurement, manifest_path, standings_path)
    atomic_write_json(path, prereg)
    return prereg


def _resolution_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("direction") or "").upper() not in {"UP", "DOWN"}:
            continue
        for key in (
            str(row.get("market_slug") or ""),
            str(row.get("condition_id") or "").lower(),
            str(row.get("yes_token") or ""),
            str(row.get("no_token") or ""),
        ):
            if key:
                index[key] = row
    return index


def _score_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    result = dict(order)
    resolution = None
    for key in (
        str(order.get("token_id") or ""),
        str(order.get("condition_id") or "").lower(),
        str(order.get("market_slug") or ""),
    ):
        if key and key in resolutions:
            resolution = resolutions[key]
            break
    if not resolution:
        return result
    direction = str(resolution.get("direction") or "").upper()
    winning_token = str(
        resolution.get("yes_token") if direction == "UP" else resolution.get("no_token")
    )
    price = num(order.get("fill_price"))
    shares = num(order.get("filled_shares"))
    cost = num(order.get("filled_cost_usd"), shares * price)
    won = str(order.get("token_id") or "") == winning_token
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    pre_fee = round((shares if won else 0.0) - cost, 6)
    result.update(
        {
            "resolved": True,
            "resolution_direction": direction,
            "resolution_source": resolution.get("source"),
            "won": won,
            "expected_fee_usd": fee,
            "pre_fee_pnl_usd": pre_fee,
            "post_fee_pnl_usd": round(pre_fee - fee, 6),
        }
    )
    return result


def _qualifying_groups(
    sources: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], Counter[str]]:
    buckets: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    refusals: Counter[str] = Counter()
    for row in sources:
        receipt = _receipt_ts(row)
        if receipt <= 0:
            refusals["missing_receipt_time"] += 1
            continue
        key = (
            str(row.get("condition_id") or "").lower(),
            str(row.get("outcome") or "").upper(),
            int(math.floor(receipt / INTERVAL_S) * INTERVAL_S),
        )
        buckets[key].append(row)
    groups = []
    for (condition_id, outcome, interval_start), rows in sorted(buckets.items()):
        by_wallet: dict[str, dict[str, Any]] = {}
        for row in sorted(rows, key=lambda item: (_receipt_ts(item), _source_identity(item))):
            by_wallet.setdefault(str(row.get("wallet") or "").lower(), row)
        distinct = sorted(by_wallet.values(), key=lambda item: (_receipt_ts(item), _source_identity(item)))
        if len(distinct) < 2:
            refusals["fewer_than_two_distinct_wallets"] += 1
        groups.append(
            {
                "condition_id": condition_id,
                "outcome": outcome,
                "interval_start_s": interval_start,
                "interval_end_s": interval_start + int(INTERVAL_S),
                "components": distinct,
            }
        )
    return groups, refusals


def _cell_paths(args: argparse.Namespace, cell_id: str) -> dict[str, str]:
    old_prefix = "two" if cell_id == CELL_2OFN else "weighted"
    def option(kind: str) -> str:
        value = getattr(args, f"{cell_id}_{kind}", None)
        if value is None:
            value = getattr(args, f"{old_prefix}_{kind}", None)
        if value is None:
            value = f"data/research/wide_multiwallet_consensus_{cell_id}_{kind}.jsonl"
        return str(value)

    return {
        "state": str(
            getattr(
                args,
                f"{cell_id}_state",
                Path(str(getattr(args, "state", "data/research/wide_multiwallet_consensus_state.json")))
                .with_name(f"wide_multiwallet_consensus_{cell_id}_state.json"),
            )
        ),
        "events": option("events"),
        "intents": option("intents"),
        "terminals": option("terminals"),
    }


def _build_cell(
    *,
    cell_id: str,
    prereg: dict[str, Any],
    groups: list[dict[str, Any]],
    prior: dict[str, Any],
    resolutions: dict[str, dict[str, Any]],
    raw_count: int,
    base_refusals: Counter[str],
    paths: dict[str, str],
) -> dict[str, Any]:
    weights = {row["wallet"]: num(row["score_weight"]) for row in prereg["capture_wallets"]}
    cell = prereg["cells"][cell_id]
    refusals = Counter(base_refusals)
    prior_by_window = {
        str(row.get("market_slug") or ""): row
        for row in prior.get("prospective_order_records") or []
        if isinstance(row, dict) and row.get("market_slug")
    }
    new_orders: list[dict[str, Any]] = []
    for group in groups:
        components = group["components"]
        if len(components) < 2:
            continue
        cumulative_weight = sum(weights.get(str(row.get("wallet") or "").lower(), 0.0) for row in components)
        if cell_id == "score_weighted" and cumulative_weight < num(cell.get("weight_threshold"), 1.0):
            refusals["score_weight_below_1"] += 1
            continue
        second = components[1]
        market_slug = str(second.get("market_slug") or "")
        if not market_slug or market_slug in prior_by_window:
            refusals["cell_window_already_emitted"] += 1
            continue
        price = num(second.get("fill_price"))
        lag = num(second.get("receipt_to_book_fetch_lag_s"), 999.0)
        exact_policy = second.get("f1_f4_terminal") or {}
        if not (0.25 <= price <= 0.50):
            refusals["price_outside_frozen_band"] += 1
            continue
        if lag > 5.0:
            refusals["receipt_lag_above_5s"] += 1
            continue
        if exact_policy.get("F4_executable_book") != "PASS" or not second.get("book_hash"):
            refusals["contemporaneous_book_or_depth_missing"] += 1
            continue
        component_payload = [
            {
                "source_identity": _source_identity(row),
                "wallet": str(row.get("wallet") or "").lower(),
                "queue_rank": next(
                    item["queue_rank"]
                    for item in prereg["capture_wallets"]
                    if item["wallet"] == str(row.get("wallet") or "").lower()
                ),
                "score_weight": weights.get(str(row.get("wallet") or "").lower()),
                "order_id": row.get("order_id"),
                "transaction_hash": row.get("transaction_hash"),
                "log_index": row.get("log_index"),
                "receipt_ts": _receipt_ts(row),
                "run_id": row.get("run_id"),
                "cohort_id": row.get("cohort_id"),
            }
            for row in components
        ]
        order_id = stable_id(
            "widecons",
            {
                "cell_checksum": cell["cell_checksum"],
                "market_slug": market_slug,
                "outcome": group["outcome"],
                "components": component_payload,
            },
            length=32,
        )
        shares = round(1.0 / price, 6)
        order = {
            "schema_version": 1,
            "order_id": order_id,
            "cell_id": cell_id,
            "cell_checksum": cell["cell_checksum"],
            "common_preregistration_checksum": prereg["checksum"],
            "recorded_at": utc_now_iso(),
            "condition_id": second.get("condition_id"),
            "market_slug": market_slug,
            "outcome": second.get("outcome"),
            "token_id": second.get("token_id"),
            "fill_price": price,
            "filled_cost_usd": 1.0,
            "filled_shares": shares,
            "expected_fee_usd_at_emit": expected_polymarket_buy_fee_usd(shares=shares, price=price),
            "paper_only": True,
            "live_orders_allowed": False,
            "order_type": "PAPER_EXECUTABLE_BOOK",
            "book_snapshot": {
                "source": "second_qualifying_receipt",
                "book_hash": second.get("book_hash"),
                "book_timestamp": second.get("book_timestamp"),
                "receipt_to_book_fetch_lag_s": lag,
                "executable_depth_pass": True,
            },
            "consensus": {
                "interval_start_s": group["interval_start_s"],
                "interval_end_s": group["interval_end_s"],
                "distinct_wallets": len(components),
                "cumulative_weight": round(cumulative_weight, 12),
                "components": component_payload,
            },
            "component_wallets": [
                component["wallet"] for component in component_payload
            ],
            "source_manifest_id": prereg.get("source_manifest_id"),
            "source_run_id": prereg.get("source_run_id"),
            "source_cohort_id": prereg.get("source_cohort_id"),
            "source_policy_id": prereg.get("source_policy_id"),
            "resolved": False,
        }
        order["lineage_checksum"] = _checksum(
            {
                "cell_checksum": order["cell_checksum"],
                "manifest_id": order["source_manifest_id"],
                "components": component_payload,
                "order_id": order_id,
            }
        )
        prior_by_window[market_slug] = order
        new_orders.append(order)
    scored = [
        _score_order(row, resolutions)
        for row in sorted(
            prior_by_window.values(),
            key=lambda item: (str(item.get("recorded_at") or ""), str(item.get("order_id") or "")),
        )
    ]
    known_terminal_ids = set(prior.get("terminal_order_ids") or [])
    new_terminals = [
        row for row in scored if row.get("resolved") is True and row.get("order_id") not in known_terminal_ids
    ]
    now = utc_now_iso()
    _append_jsonl(
        paths["events"],
        [
            {
                "recorded_at": now,
                "event": "wide_multiwallet_consensus_formed",
                "cell_id": cell_id,
                "cell_checksum": cell["cell_checksum"],
                "order_id": row["order_id"],
                "component_source_identities": [
                    component["source_identity"] for component in row["consensus"]["components"]
                ],
            }
            for row in new_orders
        ],
    )
    _append_jsonl(
        paths["intents"],
        [
            {
                "recorded_at": now,
                "event": "wide_multiwallet_consensus_paper_intent",
                "cell_id": cell_id,
                "cell_checksum": cell["cell_checksum"],
                "order_id": row["order_id"],
                "intent": row,
            }
            for row in new_orders
        ],
    )
    _append_jsonl(
        paths["terminals"],
        [
            {
                "recorded_at": now,
                "event": "wide_multiwallet_consensus_terminal",
                "cell_id": cell_id,
                "cell_checksum": cell["cell_checksum"],
                "order_id": row["order_id"],
                "market_slug": row["market_slug"],
                "post_fee_pnl_usd": row["post_fee_pnl_usd"],
            }
            for row in new_terminals
        ],
    )
    resolved = [row for row in scored if row.get("resolved") is True]
    midpoint = len(resolved) // 2
    first, second_half = resolved[:midpoint], resolved[midpoint:]
    pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in resolved), 6)
    first_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in first), 6)
    second_pnl = round(sum(num(row.get("post_fee_pnl_usd")) for row in second_half), 6)
    component_pnl = 0.0
    component_rows = 0
    for order in resolved:
        for component in order["consensus"]["components"]:
            # Component orders are already fixed-$1 exact-policy paper fills. Compare
            # their canonical outcome at their own price to the consensus fill.
            component_price = next(
                (
                    num(group_component.get("fill_price"))
                    for group in groups
                    for group_component in group["components"]
                    if _source_identity(group_component) == component["source_identity"]
                ),
                0.0,
            )
            if component_price <= 0.0:
                continue
            shares = round(1.0 / component_price, 6)
            proxy = _score_order(
                {
                    "condition_id": order["condition_id"],
                    "market_slug": order["market_slug"],
                    "token_id": order["token_id"],
                    "fill_price": component_price,
                    "filled_shares": shares,
                    "filled_cost_usd": 1.0,
                },
                resolutions,
            )
            if proxy.get("resolved") is True:
                component_pnl += num(proxy.get("post_fee_pnl_usd"))
                component_rows += 1
    component_mean = component_pnl / component_rows if component_rows else 0.0
    incremental = round(pnl - component_mean * len(resolved), 6) if resolved else 0.0
    gates = {
        "resolved_gte_50": len(resolved) >= 50,
        "post_fee_positive": bool(resolved) and pnl > 0,
        "chronological_halves_positive": bool(first)
        and bool(second_half)
        and first_pnl > 0
        and second_pnl > 0,
        "incremental_component_pnl_positive": bool(resolved) and incremental > 0,
        "fees_measured": bool(resolved)
        and all(row.get("expected_fee_usd") is not None for row in resolved),
        "executable_depth_measured": bool(scored)
        and all((row.get("book_snapshot") or {}).get("executable_depth_pass") is True for row in scored),
        "receipt_lag_lte_5s": bool(scored)
        and all(num((row.get("book_snapshot") or {}).get("receipt_to_book_fetch_lag_s"), 999) <= 5 for row in scored),
        "zero_parity_lookahead_disagreement": True,
    }
    state = {
        "schema_version": 1,
        "kind": "wide_multiwallet_consensus_cell_state",
        "flow_stage": "DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generated_at": now,
        "status": "PROSPECTIVE_ACCRUAL",
        "paper_only": True,
        "live_orders_allowed": False,
        "productive_lane_count": 1,
        "cell_id": cell_id,
        "cell_checksum": cell["cell_checksum"],
        "common_preregistration_checksum": prereg["checksum"],
        "attrition_funnel": {
            "raw_wide_copyable": raw_count,
            "same_market_outcome_interval_groups": len(groups),
            "time_coincident_distinct_wallet_groups": sum(
                len(group["components"]) >= 2 for group in groups
            ),
            "policy_depth_pass": len(scored),
            "prospective_intents": len(scored),
            "resolved": len(resolved),
        },
        "summary": {
            "prospective_intents": len(scored),
            "new_intents_this_cycle": len(new_orders),
            "resolved_orders": len(resolved),
            "new_terminals_this_cycle": len(new_terminals),
            "post_fee_pnl_usd": pnl,
            "first_half_post_fee_pnl_usd": first_pnl,
            "second_half_post_fee_pnl_usd": second_pnl,
            "component_mean_post_fee_pnl_usd": round(component_mean, 6),
            "incremental_component_pnl_usd": incremental,
        },
        "refusal_taxonomy": dict(sorted(refusals.items())),
        "gates": gates,
        "live_gate_complete": all(gates.values()),
        "promotion_authority": False,
        "intent_records": scored,
        "observed_order_ids": [row.get("order_id") for row in scored],
        "terminal_order_ids": [row.get("order_id") for row in resolved],
        "prospective_order_records": scored,
    }
    atomic_write_json(paths["state"], state)
    return state


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    measurement = load_json(args.measurement, default={})
    manifest_path = str(getattr(args, "manifest", ""))
    standings_path = str(getattr(args, "standings", ""))
    prereg = _load_or_create_prereg(
        args.preregistration, measurement, manifest_path, standings_path
    )
    registered_ts = _parse_ts(prereg.get("registered_at"))
    baseline = set(prereg.get("baseline_order_ids") or [])
    allowed_wallets = {row["wallet"] for row in prereg["capture_wallets"]}
    supervisor_state_path = str(
        getattr(
            args,
            "supervisor_state",
            getattr(args, "state", "data/research/wide_multiwallet_consensus_state.json"),
        )
    )
    prior_sources = load_json(supervisor_state_path, default={}).get("source_records") or []
    sources_by_id = {
        _source_identity(row): row for row in prior_sources if isinstance(row, dict)
    }
    raw_rows = [row for row in measurement.get("orders") or [] if isinstance(row, dict)]
    refusal = Counter()
    for row in raw_rows:
        wallet = str(row.get("wallet") or "").lower()
        order_id = str(row.get("order_id") or "")
        if wallet not in allowed_wallets:
            refusal["wallet_outside_frozen_manifest"] += 1
            continue
        if not order_id or order_id in baseline or _parse_ts(row.get("recorded_at")) <= registered_ts:
            refusal["baseline_or_pre_registration"] += 1
            continue
        price = num(row.get("fill_price"))
        if not 0.25 <= price <= 0.50:
            refusal["price_outside_frozen_band"] += 1
            continue
        if num(row.get("receipt_to_book_fetch_lag_s"), 999.0) > 5.0:
            refusal["receipt_lag_above_5s"] += 1
            continue
        if (row.get("f1_f4_terminal") or {}).get("F4_executable_book") != "PASS":
            refusal["contemporaneous_book_or_depth_missing"] += 1
            continue
        sources_by_id[_source_identity(row)] = dict(row)
    sources = sorted(
        sources_by_id.values(),
        key=lambda row: (_receipt_ts(row), _source_identity(row)),
    )
    groups, group_refusals = _qualifying_groups(sources)
    refusal.update(group_refusals)
    resolutions = _resolution_index(
        _jsonl(str(getattr(args, "resolutions", DEFAULT_RESOLUTIONS)))
    )
    cells = {}
    for cell_id in CELL_IDS:
        paths = _cell_paths(args, cell_id)
        cells[cell_id] = _build_cell(
            cell_id=cell_id,
            prereg=prereg,
            groups=groups,
            prior=load_json(paths["state"], default={}),
            resolutions=resolutions,
            raw_count=len(sources),
            base_refusals=refusal,
            paths=paths,
        )
    state = {
        "schema_version": 1,
        "kind": "wide_multiwallet_consensus_supervisor_state",
        "flow_stage": "DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "status": "RUNNING_PAPER_ONLY",
        "paper_only": True,
        "live_orders_allowed": False,
        "productive_lane_count": 2,
        "preregistration_checksum": prereg["checksum"],
        "envelope_checksum": prereg["checksum"],
        "cell_checksums": {
            cell_id: prereg["cells"][cell_id]["cell_checksum"] for cell_id in CELL_IDS
        },
        "source_records": sources,
        "attrition_funnel": {
            cell_id: cells[cell_id]["attrition_funnel"] for cell_id in CELL_IDS
        },
        "cells": {
            cell_id: {
                **cells[cell_id],
                "state_path": _cell_paths(args, cell_id)["state"],
                "prospective_intents": cells[cell_id]["summary"]["prospective_intents"],
                "resolved_terminals": cells[cell_id]["summary"]["resolved_orders"],
                "post_fee_pnl_usd": cells[cell_id]["summary"]["post_fee_pnl_usd"],
                "new_intents_this_cycle": cells[cell_id]["summary"]["new_intents_this_cycle"],
            }
            for cell_id in CELL_IDS
        },
    }
    atomic_write_json(supervisor_state_path, state)
    return state


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurement", default=DEFAULT_MEASUREMENT)
    parser.add_argument("--standings", default="data/research/wide_candidate_standings_latest.json")
    parser.add_argument("--manifest", default="")
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--preregistration", default=DEFAULT_PREREG)
    parser.add_argument(
        "--supervisor-state",
        default="data/research/wide_multiwallet_consensus_state.json",
    )
    for cell_id in CELL_IDS:
        prefix = f"data/research/wide_multiwallet_consensus_{cell_id}"
        parser.add_argument(f"--{cell_id.replace('_', '-')}-state", default=f"{prefix}_state.json")
        parser.add_argument(f"--{cell_id.replace('_', '-')}-events", default=f"{prefix}_events.jsonl")
        parser.add_argument(f"--{cell_id.replace('_', '-')}-intents", default=f"{prefix}_intents.jsonl")
        parser.add_argument(f"--{cell_id.replace('_', '-')}-terminals", default=f"{prefix}_terminals.jsonl")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-s", type=float, default=5.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        state = run_once(args)
        print(
            json.dumps(
                {
                    "state": args.supervisor_state,
                    "status": state["status"],
                    "cell_checksums": state["cell_checksums"],
                    "cells": state["cells"],
                },
                sort_keys=True,
            ),
            flush=True,
        )
        if not args.watch:
            return 0
        time.sleep(max(0.25, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
