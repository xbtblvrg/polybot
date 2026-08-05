#!/usr/bin/env python3
"""Compare frozen move-slice selection with an unsliced fixed-policy rescore."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import resolution_index  # noqa: E402
from src.wallet_copy.alpha_decay import btc_5m_move_slice_for_values  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

TARGET_WALLETS = (
    "0x0484e64092ba4108c2786b61e6fc052d3bf41b1a",
    "0x951bd740ef681d05891ca35440232488271d433e",
    "0x31c290a2772e1e3143bcb6debbdbbf08ac081d13",
    "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
)
MAX_ORDER_USD = 1.0
MIN_ORDER_USD = 1.0
WALLET_FRACTION = 0.1
FEE_MODEL_ID = "polymarket_embedded_buy_fee_v1"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_ALPHA = "data/research/alpha_decay_report.json"
DEFAULT_OUTPUT = "data/research/positive_wallet_slice_selector_falsifier_latest.json"


def _jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _manifest_slice_keys(manifest: dict[str, Any]) -> dict[str, list[str]]:
    rows = (
        manifest.get("capture_watch_wallets")
        if isinstance(manifest.get("capture_watch_wallets"), list)
        else manifest.get("admitted_wallets")
    )
    return {
        str(row.get("wallet") or "").lower(): sorted(
            {str(key) for key in row.get("move_slice_keys") or [] if str(key)}
        )
        for row in rows or []
        if isinstance(row, dict) and row.get("wallet")
    }


def _alpha_slice_keys(alpha: dict[str, Any]) -> dict[str, list[str]]:
    profiles = (
        (alpha.get("execution_profiles") or {}).get("profiles_by_wallet") or {}
    )
    result: dict[str, list[str]] = {}
    for wallet, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        result[str(wallet).lower()] = sorted(
            {
                str(row.get("move_slice_key"))
                for row in profile.get("move_slices") or []
                if isinstance(row, dict)
                and str(row.get("move_slice_key") or "")
                and num(row.get("mean_edge")) > 0
                and num(row.get("median_edge")) > 0
                and num(row.get("copyable_rate_pct")) >= 70.0
            }
        )
    return result


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        rows,
        key=lambda row: (
            num(row.get("event_ts")),
            str(row.get("event_id") or ""),
        ),
    )
    midpoint = (len(ordered) + 1) // 2
    first = ordered[:midpoint]
    second = ordered[midpoint:]
    cost = sum(num(row.get("cost_usd")) for row in ordered)
    pnl = sum(num(row.get("post_fee_pnl_usd")) for row in ordered)
    return {
        "resolved": len(ordered),
        "cost_usd": round(cost, 6),
        "post_fee_pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        "first_half_post_fee_pnl_usd": (
            round(sum(num(row.get("post_fee_pnl_usd")) for row in first), 6)
            if first
            else None
        ),
        "second_half_post_fee_pnl_usd": (
            round(sum(num(row.get("post_fee_pnl_usd")) for row in second), 6)
            if second
            else None
        ),
        "both_halves_positive": bool(
            first
            and second
            and sum(num(row.get("post_fee_pnl_usd")) for row in first) > 0
            and sum(num(row.get("post_fee_pnl_usd")) for row in second) > 0
        ),
    }


def _score_wallet_events(
    events: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
    *,
    wallet: str,
) -> tuple[list[dict[str, Any]], int]:
    index = resolution_index(resolutions)
    seen: set[str] = set()
    scored: list[dict[str, Any]] = []
    unresolved = 0
    for event in events:
        if not isinstance(event, dict):
            continue
        if str(event.get("source_wallet") or "").lower() != wallet:
            continue
        slug = str(event.get("market_slug") or "")
        if (
            str(event.get("action") or "").upper() != "BUY"
            or str(event.get("asset") or "").upper() != "BTC"
            or str(event.get("duration") or "").lower() != "5m"
            or not slug.startswith("btc-updown-5m-")
        ):
            continue
        identity = "|".join(
            (
                str(event.get("transaction_hash") or "").lower(),
                str(event.get("token_id") or ""),
                str(event.get("event_id") or ""),
            )
        )
        if identity in seen:
            continue
        seen.add(identity)
        price = num(event.get("price"))
        if not 0 < price < 1:
            continue
        resolution = None
        for key in (
            str(event.get("token_id") or ""),
            str(event.get("condition_id") or "").lower(),
            slug,
        ):
            if key and key in index:
                resolution = index[key]
                break
        if not resolution:
            unresolved += 1
            continue
        cost = MAX_ORDER_USD
        shares = cost / price
        outcome = str(event.get("outcome") or "").upper()
        direction = str(resolution.get("direction") or "").upper()
        won = outcome == direction
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        pnl = (shares if won else 0.0) - cost - fee
        scored.append(
            {
                "event_id": event.get("event_id"),
                "event_ts": event.get("event_ts"),
                "market_slug": slug,
                "move_slice_key": btc_5m_move_slice_for_values(
                    market_slug=slug,
                    event_ts=num(event.get("event_ts")),
                    price=price,
                )["move_slice_key"],
                "fill_price": price,
                "cost_usd": cost,
                "expected_fee_usd": fee,
                "post_fee_pnl_usd": round(pnl, 6),
            }
        )
    return scored, unresolved


def build_report(
    *,
    events: list[dict[str, Any]],
    resolutions: list[dict[str, Any]],
    manifest: dict[str, Any],
    alpha: dict[str, Any],
    wallets: tuple[str, ...] = TARGET_WALLETS,
) -> dict[str, Any]:
    manifest_keys = _manifest_slice_keys(manifest)
    alpha_keys = _alpha_slice_keys(alpha)
    rows: list[dict[str, Any]] = []
    for wallet in wallets:
        scored, unresolved = _score_wallet_events(
            events, resolutions, wallet=wallet
        )
        slice_keys = manifest_keys.get(wallet)
        key_source = "latest_wide_manifest"
        if slice_keys is None:
            slice_keys = alpha_keys.get(wallet, [])
            key_source = "latest_alpha_positive_70pct_move_slices"
        allowed = set(slice_keys)
        sliced = [
            row for row in scored if str(row.get("move_slice_key") or "") in allowed
        ]
        rows.append(
            {
                "wallet": wallet,
                "slice_key_source": key_source,
                "move_slice_keys": slice_keys,
                "resolved_universe": len(scored),
                "unresolved_events": unresolved,
                "unsliced": _summary(scored),
                "sliced": _summary(sliced),
            }
        )
    sign_inversions = [
        row["wallet"]
        for row in rows
        if num(row["unsliced"].get("post_fee_pnl_usd")) >= 0
        and row["sliced"].get("resolved", 0) > 0
        and num(row["sliced"].get("post_fee_pnl_usd")) < 0
    ]
    all_unsliced_negative = bool(rows) and all(
        row["unsliced"].get("resolved", 0) > 0
        and num(row["unsliced"].get("post_fee_pnl_usd")) < 0
        for row in rows
    )
    verdict = (
        "SLICE_SELECTOR_SIGN_INVERTER"
        if len(sign_inversions) >= 2
        else "POSITIVE_LABELS_NOT_REPRODUCIBLE"
        if all_unsliced_negative
        else "MIXED_NO_PREREGISTERED_THRESHOLD"
    )
    return {
        "schema_version": 1,
        "kind": "positive_wallet_slice_selector_falsifier",
        "flow_stage": "LEARN/PROMOTE/OBSERVE",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "policy": {
            "wallet_fraction": WALLET_FRACTION,
            "max_order_usd": MAX_ORDER_USD,
            "min_order_usd": MIN_ORDER_USD,
            "fee_model_id": FEE_MODEL_ID,
            "fill_assumption": "source_trade_price for both sliced and unsliced arms",
            "resolved_window": "identical canonical resolution index for both arms",
        },
        "rows": rows,
        "sign_inversion_wallets": sign_inversions,
        "sign_inversion_count": len(sign_inversions),
        "all_unsliced_negative": all_unsliced_negative,
        "verdict": verdict,
        "preregistered_rule": (
            "selector is sign-inverter iff unsliced>=0 and sliced<0 for >=2/4; "
            "labels are not reproducible iff unsliced<0 for all 4"
        ),
        "authority": "measurement only; no live gate, roster, cap, or submitter mutation",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--manifest", default="")
    parser.add_argument("--alpha", default=DEFAULT_ALPHA)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    manifest_path = (
        Path(args.manifest)
        if args.manifest
        else sorted(ROOT.glob("data/research/wide_exact_policy_manifest_wide_*.json"))[-1]
    )
    history = load_json(args.history, default={})
    report = build_report(
        events=[
            row for row in history.get("events") or [] if isinstance(row, dict)
        ],
        resolutions=_jsonl(Path(args.resolutions)),
        manifest=load_json(manifest_path, default={}),
        alpha=load_json(args.alpha, default={}),
    )
    report["sources"] = {
        "history": args.history,
        "resolutions": args.resolutions,
        "manifest": str(manifest_path),
        "alpha": args.alpha,
    }
    atomic_write_json(args.output, report)
    print(
        json.dumps(
            {
                "output": args.output,
                "verdict": report["verdict"],
                "sign_inversion_wallets": report["sign_inversion_wallets"],
                "rows": [
                    {
                        "wallet": row["wallet"],
                        "unsliced": row["unsliced"],
                        "sliced": row["sliced"],
                    }
                    for row in report["rows"]
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
