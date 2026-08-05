#!/usr/bin/env python3
"""Resolve accrued ETH-5m paper intents and update the preregistered gate."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.refresh_btc_5m_resolutions_from_gamma import _fetch_gamma_event, _json_list, _winner_from_prices  # noqa: E402
from src.wallet_copy.fees import POLYMARKET_EMBEDDED_FEE_RATE, expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_STATE = "data/research/eth5m_replication_scout_paper_state.json"
DEFAULT_EVENTS = "data/research/eth5m_replication_scout_paper_events.jsonl"
DEFAULT_RESOLUTIONS = "data/research/eth5m_replication_scout_resolutions.jsonl"


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, UTC).isoformat().replace("+00:00", "Z")


def _rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _slug_start(slug: str) -> int | None:
    try:
        return int(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return None


def _gamma_winner(slug: str, fetch: Callable[[str], list[dict[str, Any]]]) -> str | None:
    for event in fetch(slug):
        for market in event.get("markets") or []:
            if not isinstance(market, dict) or str(market.get("slug") or slug) != slug:
                continue
            winner = _winner_from_prices(_json_list(market.get("outcomes")), _json_list(market.get("outcomePrices")))
            if winner:
                return winner
    return None


def resolve_once(
    state_path: Path,
    events_path: Path,
    resolutions_path: Path,
    *,
    now: float | None = None,
    fetch: Callable[[str], list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    now = time.time() if now is None else now
    state = load_json(state_path, default={}) or {}
    intents = _rows(events_path)
    existing = {str(row.get("market_slug")): str(row.get("winner")) for row in _rows(resolutions_path)}
    fetch = fetch or (lambda slug: _fetch_gamma_event(slug, timeout_s=8.0, user_agent="Mozilla/5.0"))
    newly_resolved: list[dict[str, Any]] = []
    for slug in sorted({str(row.get("market_slug") or "") for row in intents}):
        start = _slug_start(slug)
        if not slug or slug in existing or start is None or start + 300 > now:
            continue
        winner = _gamma_winner(slug, fetch)
        if winner:
            existing[slug] = winner
            newly_resolved.append({"market_slug": slug, "winner": winner, "resolved_at": _iso(now), "source": "polymarket_gamma"})
    if newly_resolved:
        resolutions_path.parent.mkdir(parents=True, exist_ok=True)
        with resolutions_path.open("a", encoding="utf-8") as handle:
            for row in newly_resolved:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")

    resolved = 0
    pnl = 0.0
    resolved_windows: set[str] = set()
    for intent in intents:
        slug = str(intent.get("market_slug") or "")
        winner = existing.get(slug)
        if not winner:
            continue
        price = float(intent.get("source_price") or 0.0)
        notional = float(intent.get("paper_notional_usd") or 0.0)
        shares = notional / price if price > 0 else 0.0
        gross = shares * (1.0 - price) if str(intent.get("outcome") or "").lower() == winner.lower() else -notional
        pnl += gross - expected_polymarket_buy_fee_usd(shares=shares, price=price)
        resolved += 1
        resolved_windows.add(slug)

    first = state.get("first_observed_at_s")
    age_s = max(0.0, now - float(first)) if first is not None else 0.0
    gate = {
        "min_resolved_intents": 30,
        "min_distinct_windows": 12,
        "min_accrual_age_s": 21600,
        "post_fee_pnl_gt_zero": True,
    }
    gate_pass = resolved >= 30 and len(resolved_windows) >= 12 and age_s >= 21600 and pnl > 0
    state.update({
        "generated_at": _iso(now),
        "resolved_intents": resolved,
        "resolved_windows": len(resolved_windows),
        "post_fee_pnl_usd": round(pnl, 6),
        "fee_model": {"rate": POLYMARKET_EMBEDDED_FEE_RATE, "basis": "embedded buy fee"},
        "accrual_age_s": round(age_s, 6),
        "resolution_gate": gate,
        "promotion_gate_pass": gate_pass,
        "status": "PROMOTION_EVIDENCE_READY" if gate_pass else "ACCRUING",
        "live_orders_allowed": False,
    })
    atomic_write_json(state_path, state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--events", default=DEFAULT_EVENTS)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    args = parser.parse_args()
    state = resolve_once(Path(args.state), Path(args.events), Path(args.resolutions))
    print(json.dumps({key: state.get(key) for key in ("status", "resolved_intents", "post_fee_pnl_usd")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
