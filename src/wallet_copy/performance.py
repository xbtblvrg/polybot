"""Paper performance scoring and live-admission evidence for wallet-copy."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from src.wallet_copy.models import num, utc_now_iso


@dataclass(frozen=True)
class AdmissionConfig:
    min_resolved_orders: int = 30
    min_roi_pct: float = 1.0
    min_wr_pct: float = 70.0
    max_unresolved_ratio: float = 0.5
    max_avg_latency_s: float = 30.0
    require_paper_only: bool = True

    def asdict(self) -> dict[str, Any]:
        return asdict(self)


def _slug_start_ts(slug: str) -> int | None:
    match = re.search(r"(\d{9,})$", str(slug or ""))
    return int(match.group(1)) if match else None


def _duration_seconds(slug: str) -> int:
    text = str(slug or "").lower()
    if "15m" in text:
        return 900
    if "1h" in text:
        return 3600
    return 300


def _outcome_from_direction(direction: str) -> str:
    value = str(direction or "").strip().upper()
    if value.startswith("UP"):
        return "Up"
    if value.startswith("DOWN"):
        return "Down"
    return ""


def load_resolutions(path: str | Path) -> dict[str, dict[str, Any]]:
    """Load BTC resolution rows by several match keys.

    The historical files may carry Gamma numeric condition ids while live paper
    orders use CLOB condition hashes, so slug/expiry and token ids are also
    indexed.
    """

    p = Path(path)
    if not p.exists():
        return {}
    lowered_name = p.name.lower()
    if lowered_name.endswith("_summary.json") or lowered_name.endswith(".gamma_summary.json"):
        raise ValueError(f"resolution source must be JSONL rows, not summary JSON: {p}")
    rows: dict[str, dict[str, Any]] = {}

    def _resolution_priority(row: dict[str, Any]) -> tuple[int, int, int]:
        source = str(row.get("source") or row.get("resolution_precision") or "").lower()
        canonical = 1 if not bool(row.get("research_only")) else 0
        gamma = 1 if "polymarket_gamma" in source else 0
        tokenized = 1 if str(row.get("yes_token") or row.get("no_token") or "") else 0
        return (canonical, gamma, tokenized)

    def _put(key: str, row: dict[str, Any]) -> None:
        if not key:
            return
        existing = rows.get(key)
        if existing is None or _resolution_priority(row) >= _resolution_priority(existing):
            rows[key] = row

    nonblank_lines = 0
    parsed_rows = 0
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        nonblank_lines += 1
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        parsed_rows += 1
        keys = {
            str(row.get("condition_id") or ""),
            str(row.get("yes_token") or ""),
            str(row.get("no_token") or ""),
        }
        expiry = int(num(row.get("expiry_unix_ts"), 0))
        if expiry > 0:
            keys.add(f"expiry:{expiry}")
            window_type = str(row.get("window_type") or "").lower()
            if window_type in {"5m", "5min", "5_minute"}:
                keys.add(f"slug_start:{expiry - 300}")
            elif window_type in {"15m", "15min", "15_minute"}:
                keys.add(f"slug_start:{expiry - 900}")
            else:
                keys.add(f"slug_start:{expiry - 300}")
                keys.add(f"slug_start:{expiry - 900}")
        for key in keys:
            _put(key, row)
    if nonblank_lines and parsed_rows / nonblank_lines < 0.5:
        raise ValueError(
            f"resolution source parsed only {parsed_rows}/{nonblank_lines} nonblank JSONL rows: {p}"
        )
    return rows


def _resolution_for_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    token_id = str(order.get("token_id") or source_intent.get("token_id") or "")
    keys = [
        str(order.get("condition_id") or ""),
        token_id,
    ]
    start_ts = _slug_start_ts(str(order.get("market_slug") or source_intent.get("market_slug") or ""))
    if start_ts is not None:
        keys.append(f"slug_start:{start_ts}")
    else:
        expiry = num(order.get("expiry_unix_ts") or source_intent.get("expiry_unix_ts"), 0)
        if expiry > 0:
            keys.append(f"expiry:{int(expiry)}")
    for key in keys:
        row = resolutions.get(key)
        if row:
            return row
    return None


def score_order(order: dict[str, Any], resolutions: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if str(order.get("final_status") or order.get("status") or "").upper() != "FILLED":
        return {
            "order_id": order.get("order_id"),
            "intent_id": order.get("intent_id"),
            "source_wallet": str(order.get("source_wallet") or "").lower(),
            "wallet_name": order.get("wallet_name"),
            "condition_id": order.get("condition_id"),
            "market_slug": order.get("market_slug"),
            "outcome": str(order.get("outcome") or ""),
            "cost_usd": 0.0,
            "shares": 0.0,
            "resolved": False,
            "winner": "",
            "win": None,
            "payout_usd": 0.0,
            "pnl_usd": 0.0,
            "roi_pct": 0.0,
            "final_status": order.get("final_status") or order.get("status"),
            "skip_reason": "paper_order_not_filled",
        }
    resolution = _resolution_for_order(order, resolutions)
    # Live maker lifecycle rows predate the canonical ``filled_*`` aliases.
    # Treat the exchange-response fields as equal authorities so a fully
    # matched exact-five order cannot be scored as a zero-share total loss.
    cost = max(
        num(order.get("filled_size_usd")),
        num(order.get("response_filled_size_usd")),
    )
    shares = max(
        num(order.get("filled_shares")),
        num(order.get("fill_size_shares")),
        num(order.get("response_fill_size_shares")),
    )
    outcome = str(order.get("outcome") or "")
    if not resolution:
        return {
            "order_id": order.get("order_id"),
            "intent_id": order.get("intent_id"),
            "source_wallet": str(order.get("source_wallet") or "").lower(),
            "wallet_name": order.get("wallet_name"),
            "condition_id": order.get("condition_id"),
            "market_slug": order.get("market_slug"),
            "outcome": outcome,
            "cost_usd": round(cost, 6),
            "shares": round(shares, 6),
            "resolved": False,
            "winner": "",
            "win": None,
            "payout_usd": 0.0,
            "pnl_usd": 0.0,
            "roi_pct": 0.0,
        }
    winner = _outcome_from_direction(str(resolution.get("direction") or ""))
    win = outcome.lower() == winner.lower()
    payout = shares if win else 0.0
    pnl = payout - cost
    return {
        "order_id": order.get("order_id"),
        "intent_id": order.get("intent_id"),
        "source_wallet": str(order.get("source_wallet") or "").lower(),
        "wallet_name": order.get("wallet_name"),
        "condition_id": order.get("condition_id"),
        "market_slug": order.get("market_slug"),
        "outcome": outcome,
        "cost_usd": round(cost, 6),
        "shares": round(shares, 6),
        "resolved": True,
        "winner": winner,
        "win": bool(win),
        "payout_usd": round(payout, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "resolution": {
            "direction": resolution.get("direction"),
            "expiry_unix_ts": resolution.get("expiry_unix_ts"),
            "delta_pct": resolution.get("delta_pct"),
            "source": resolution.get("source"),
            "research_only": bool(resolution.get("research_only")),
            "window_type": resolution.get("window_type"),
        },
    }


def summarize_scores(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved")]
    wins = [row for row in resolved if row.get("win")]
    cost = sum(num(row.get("cost_usd")) for row in resolved)
    pnl = sum(num(row.get("pnl_usd")) for row in resolved)
    unresolved = len(rows) - len(resolved)
    return {
        "orders": len(rows),
        "resolved_orders": len(resolved),
        "unresolved_orders": unresolved,
        "unresolved_ratio": round(unresolved / len(rows), 6) if rows else 0.0,
        "wins": len(wins),
        "losses": len(resolved) - len(wins),
        "wr_pct": round(len(wins) / len(resolved) * 100.0, 6) if resolved else 0.0,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(pnl / cost * 100.0, 6) if cost > 0 else 0.0,
    }


def summarize_lifecycle_realized_pnl(lifecycle_events: list[dict[str, Any]]) -> dict[str, Any]:
    realized_rows: list[dict[str, Any]] = []
    for event in lifecycle_events:
        if not isinstance(event, dict):
            continue
        reduction = event.get("position_reduction")
        if not isinstance(reduction, dict):
            continue
        if "realized_pnl_usd" not in reduction:
            continue
        realized_rows.append(
            {
                "wallet_lifecycle_id": event.get("wallet_lifecycle_id"),
                "source_wallet": str(event.get("source_wallet") or "").lower(),
                "wallet_action": event.get("wallet_action"),
                "condition_id": event.get("condition_id"),
                "outcome": event.get("outcome"),
                "proceeds_usd": round(num(reduction.get("proceeds_usd")), 6),
                "cost_removed_usd": round(num(reduction.get("cost_removed_usd")), 6),
                "realized_pnl_usd": round(num(reduction.get("realized_pnl_usd")), 6),
            }
        )
    proceeds = sum(num(row.get("proceeds_usd")) for row in realized_rows)
    cost_removed = sum(num(row.get("cost_removed_usd")) for row in realized_rows)
    pnl = sum(num(row.get("realized_pnl_usd")) for row in realized_rows)
    return {
        "realized_events": len(realized_rows),
        "proceeds_usd": round(proceeds, 6),
        "cost_removed_usd": round(cost_removed, 6),
        "realized_pnl_usd": round(pnl, 6),
        "realized_roi_pct": round(pnl / cost_removed * 100.0, 6) if cost_removed > 0 else 0.0,
        "rows": realized_rows,
    }


def wallet_scorecards(orders: list[dict[str, Any]], resolutions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    scored = [score_order(order, resolutions) for order in orders]
    by_wallet: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latencies: dict[str, list[float]] = defaultdict(list)
    for order, score in zip(orders, scored):
        wallet = str(score.get("source_wallet") or "").lower()
        by_wallet[wallet].append(score)
        source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
        latency = source_intent.get("api_latency_s")
        if latency is not None:
            latencies[wallet].append(num(latency))
    cards: list[dict[str, Any]] = []
    for wallet, rows in by_wallet.items():
        summary = summarize_scores(rows)
        latency_values = latencies.get(wallet) or []
        cards.append(
            {
                "source_wallet": wallet,
                "wallet_name": rows[0].get("wallet_name") if rows else "",
                **summary,
                "avg_api_latency_s": round(sum(latency_values) / len(latency_values), 6) if latency_values else None,
                "max_api_latency_s": round(max(latency_values), 6) if latency_values else None,
            }
        )
    return sorted(cards, key=lambda row: (-num(row.get("resolved_orders")), -num(row.get("roi_pct")), row.get("source_wallet") or ""))


def admission_report(
    *,
    paper_state: dict[str, Any],
    scorecards: list[dict[str, Any]],
    config: AdmissionConfig | None = None,
) -> dict[str, Any]:
    cfg = config or AdmissionConfig()
    blockers: list[str] = []
    if cfg.require_paper_only and paper_state.get("paper_only") is not True:
        blockers.append("paper_state_not_paper_only")
    if paper_state.get("live_orders_allowed") is not False:
        blockers.append("paper_state_live_orders_allowed")
    admitted_wallets: list[dict[str, Any]] = []
    wallet_reports: list[dict[str, Any]] = []
    for card in scorecards:
        wallet_blockers: list[str] = []
        if int(card.get("resolved_orders") or 0) < int(cfg.min_resolved_orders):
            wallet_blockers.append("insufficient_resolved_orders")
        if num(card.get("roi_pct")) < float(cfg.min_roi_pct):
            wallet_blockers.append("roi_below_minimum")
        if num(card.get("wr_pct")) < float(cfg.min_wr_pct):
            wallet_blockers.append("wr_below_minimum")
        if num(card.get("unresolved_ratio")) > float(cfg.max_unresolved_ratio):
            wallet_blockers.append("too_many_unresolved_orders")
        avg_latency = card.get("avg_api_latency_s")
        if avg_latency is not None and num(avg_latency) > float(cfg.max_avg_latency_s):
            wallet_blockers.append("latency_above_minimum")
        report = {
            **card,
            "status": "PASS" if not wallet_blockers else "BLOCKED",
            "blockers": wallet_blockers,
        }
        wallet_reports.append(report)
        if not wallet_blockers:
            admitted_wallets.append(report)
    if not admitted_wallets:
        blockers.append("no_wallet_passes_admission")
    return {
        "schema_version": 1,
        "kind": "wallet_copy_admission_report",
        "generated_at": utc_now_iso(),
        "status": "PASS" if not blockers else "BLOCKED",
        "blockers": blockers,
        "config": cfg.asdict(),
        "paper_only": True,
        "live_orders_allowed": False,
        "wallets": wallet_reports,
        "admitted_wallets": admitted_wallets,
    }


def score_paper_state(
    paper_state: dict[str, Any],
    *,
    resolutions_path: str | Path = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl",
    admission_config: AdmissionConfig | None = None,
) -> dict[str, Any]:
    orders = [row for row in paper_state.get("orders") or [] if isinstance(row, dict)]
    lifecycle_events = [row for row in paper_state.get("lifecycle_events") or [] if isinstance(row, dict)]
    resolutions = load_resolutions(resolutions_path)
    scored = [score_order(order, resolutions) for order in orders]
    cards = wallet_scorecards(orders, resolutions)
    return {
        "schema_version": 1,
        "kind": "wallet_copy_performance_report",
        "generated_at": utc_now_iso(),
        "resolution_path": str(resolutions_path),
        "resolution_rows_indexed": len(resolutions),
        "summary": summarize_scores(scored),
        "lifecycle_realized": summarize_lifecycle_realized_pnl(lifecycle_events),
        "wallet_scorecards": cards,
        "admission": admission_report(
            paper_state=paper_state,
            scorecards=cards,
            config=admission_config,
        ),
        "scored_orders": scored,
    }
