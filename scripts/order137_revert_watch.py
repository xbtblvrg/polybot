#!/usr/bin/env python3
"""Zero-AI mechanical revert watcher for the ORDER137 pinned-seat min-share cap.

Fable DIRECTION 2026-08-01T16:2xZ: ORDER137 ships with a pre-registered
revert — if the first N accepted orders placed under the widened cap net
<= -$2.00 resolved, the widened cap is withdrawn and never re-arms on its
own.

The sole submitter (the live guard) holds its code in memory and is not
restarted for this.  The revert is therefore a *data* switch: this script
runs as a fresh process each heartbeat, so its own logic can be changed at
any time with no restart, and it writes
``data/research/order137_min_share_cap_state.json``, which
``src.trade_executor._order137_revert_active`` reads per decision.

Writes nothing outside its own state/journal files.  Idempotent and
terminal: once ``revert_active`` is true it stays true.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402

DEFAULT_STATE = ROOT / "data/research/order137_min_share_cap_state.json"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_EVENT_LOG = ROOT / "data/research/order137_min_share_cap_events.jsonl"
DEFAULT_LIVE_CHANGE_JOURNAL = ROOT / "data/research/live_change_journal.jsonl"

ORDER137_PINNED_WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
ORDER137_FIRST_N_ACCEPTED = 6
ORDER137_REVERT_NET_USD = -2.0
DIRECTION_ID = "2026-08-01-fable-order137-first6-mechanical-revert"


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _order_source_wallet(order: dict[str, Any]) -> str:
    for candidate in (
        order.get("source_wallet"),
        order.get("wallet"),
        order.get("wallet_name"),
    ):
        text = str(candidate or "").strip().lower()
        if text.startswith("0x"):
            return text
    return ""


def _is_accepted(order: dict[str, Any]) -> bool:
    """Accepted == the venue took the order; local refusals are not attempts."""
    status = str(order.get("final_status") or order.get("status") or "").upper()
    if status in {"REJECTED", "REFUSED", "SKIPPED", "BLOCKED", ""}:
        return False
    if bool(order.get("paper_only")):
        return False
    return bool(str(order.get("order_id") or "").strip())


def _accepted_rows(
    *,
    orders: list[Any],
    resolutions: dict[str, Any],
    source_wallet: str,
    since: dt.datetime,
    first_n: int,
) -> list[dict[str, Any]]:
    candidates: list[tuple[dt.datetime, dict[str, Any]]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if _order_source_wallet(order) != source_wallet:
            continue
        if not _is_accepted(order):
            continue
        submitted_at = _parse_iso(
            order.get("submitted_at") or order.get("created_at") or order.get("updated_at")
        )
        if submitted_at is None or submitted_at < since:
            continue
        candidates.append((submitted_at, order))
    candidates.sort(key=lambda pair: (pair[0], str(pair[1].get("order_id") or "")))
    rows: list[dict[str, Any]] = []
    for submitted_at, order in candidates[: max(1, int(first_n))]:
        event = score_order(order, resolutions)
        resolved = bool(event.get("resolved"))
        cost = _float(event.get("cost_usd"))
        shares = _float(event.get("shares"))
        if cost <= 0:
            cost = _float(order.get("requested_size_usd") or order.get("size_usd"))
        if shares <= 0:
            shares = _float(order.get("requested_shares") or order.get("shares"))
        pnl = _float(event.get("pnl_usd"))
        if _float(event.get("cost_usd")) <= 0 and cost > 0 and resolved:
            pnl = (shares if bool(event.get("win")) else 0.0) - cost
        rows.append(
            {
                "submitted_at": submitted_at.isoformat().replace("+00:00", "Z"),
                "order_id": order.get("order_id"),
                "market_slug": order.get("market_slug"),
                "limit_price": _float(order.get("limit_price")),
                "resolved": resolved,
                "pnl_usd": round(float(pnl), 6) if resolved else 0.0,
                "cost_usd": round(float(cost), 6),
            }
        )
    return rows


def evaluate(
    *,
    state_path: Path = DEFAULT_STATE,
    ledger_path: Path = DEFAULT_LEDGER,
    resolutions_path: Path = DEFAULT_RESOLUTIONS,
    event_log: Path = DEFAULT_EVENT_LOG,
    live_change_journal: Path = DEFAULT_LIVE_CHANGE_JOURNAL,
    source_wallet: str = ORDER137_PINNED_WALLET,
    first_n: int = ORDER137_FIRST_N_ACCEPTED,
    net_threshold_usd: float = ORDER137_REVERT_NET_USD,
    execute: bool = False,
    now: str | None = None,
) -> dict[str, Any]:
    generated_at = now or _utc_now()
    state = _load_json(state_path, default={})
    if not isinstance(state, dict):
        state = {}
    base = {
        "flow_stage": "LIVE/DEFEND",
        "checked_at": generated_at,
        "direction_id": DIRECTION_ID,
        "source_wallet": source_wallet,
        "rule": (
            f"first {first_n} accepted orders since activation net "
            f"<= ${net_threshold_usd:.2f} resolved -> withdraw ORDER137 cap"
        ),
        "first_n_accepted": first_n,
        "net_threshold_usd": net_threshold_usd,
    }

    if state.get("revert_active") is True:
        return {
            **base,
            "revert_active": True,
            "status": "ALREADY_REVERTED",
            "reverted_at": state.get("reverted_at"),
        }
    if state.get("terminal") is True and state.get("verdict") == "ORDER137_KEPT":
        return {
            **base,
            "revert_active": False,
            "status": "ALREADY_KEPT",
            "verdict": "ORDER137_KEPT",
            "terminal": True,
            "completed_at": state.get("completed_at"),
            "attribution": state.get("attribution"),
        }

    activated_at = _parse_iso(state.get("activated_at"))
    if activated_at is None:
        return {
            **base,
            "revert_active": False,
            "status": "NOT_ARMED_NO_ACTIVATION_TIMESTAMP",
            "next_action": (
                "write activated_at (guard generation-reload time) into "
                f"{state_path.name} when the ORDER137 branch goes live"
            ),
        }

    ledger = _load_json(ledger_path, default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    if not isinstance(orders, list):
        orders = []
    resolutions = load_resolutions(str(resolutions_path))
    rows = _accepted_rows(
        orders=orders,
        resolutions=resolutions,
        source_wallet=source_wallet,
        since=activated_at,
        first_n=first_n,
    )
    resolved_rows = [row for row in rows if row.get("resolved")]
    net_usd = round(sum(_float(row.get("pnl_usd")) for row in resolved_rows), 6)
    attribution = {
        "accepted_orders": len(rows),
        "resolved_orders": len(resolved_rows),
        "net_resolved_pnl_usd": net_usd,
        "rows": rows,
    }
    payload = {
        **base,
        "activated_at": state.get("activated_at"),
        "revert_active": False,
        "attribution": attribution,
    }
    # Fire on the resolved subset: a defense may fire early, never late.
    if net_usd > net_threshold_usd + 1e-9:
        if len(rows) >= first_n and len(resolved_rows) >= first_n:
            kept = {
                **payload,
                "status": "ORDER137_KEPT",
                "verdict": "ORDER137_KEPT",
                "completed_at": generated_at,
                "terminal": True,
                "re_arm_rule": "terminal first-6 verdict; do not re-arm or clear",
            }
            if not execute:
                return {**kept, "status": "WOULD_KEEP", "executed": False}
            _write_json(state_path, {**state, **kept})
            _append_jsonl(event_log, kept)
            _append_jsonl(
                live_change_journal,
                {
                    "at": generated_at,
                    "kind": "order137_min_share_cap_keep",
                    "direction_id": DIRECTION_ID,
                    "source_wallet": source_wallet,
                    "net_resolved_pnl_usd": net_usd,
                    "accepted_orders": len(rows),
                    "resolved_orders": len(resolved_rows),
                },
            )
            return {**kept, "executed": True}
        return {
            **payload,
            "status": "WATCH" if len(rows) < first_n else "WATCH_WINDOW_FULL",
        }

    final = {
        **payload,
        "revert_active": True,
        "status": "ORDER137_REVERTED",
        "verdict": "ORDER137_REVERTED",
        "reverted_at": generated_at,
        "trigger_reason": "first_n_accepted_net_resolved_pnl_lte_threshold",
        "requires_fable_ping": True,
        "terminal": True,
        "re_arm_rule": "no automatic re-arm; only a Fable DIRECTION may clear this",
    }
    if not execute:
        return {**final, "status": "WOULD_REVERT", "revert_active": False, "executed": False}
    _write_json(state_path, {**state, **final})
    _append_jsonl(event_log, final)
    _append_jsonl(
        live_change_journal,
        {
            "at": generated_at,
            "kind": "order137_min_share_cap_revert",
            "direction_id": DIRECTION_ID,
            "source_wallet": source_wallet,
            "net_resolved_pnl_usd": net_usd,
            "accepted_orders": len(rows),
            "resolved_orders": len(resolved_rows),
        },
    )
    return {**final, "executed": True}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument("--live-change-journal", default=str(DEFAULT_LIVE_CHANGE_JOURNAL))
    parser.add_argument("--source-wallet", default=ORDER137_PINNED_WALLET)
    parser.add_argument("--first-n", type=int, default=ORDER137_FIRST_N_ACCEPTED)
    parser.add_argument("--net-threshold-usd", type=float, default=ORDER137_REVERT_NET_USD)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    result = evaluate(
        state_path=Path(args.state),
        ledger_path=Path(args.ledger),
        resolutions_path=Path(args.resolutions),
        event_log=Path(args.event_log),
        live_change_journal=Path(args.live_change_journal),
        source_wallet=str(args.source_wallet).strip().lower(),
        first_n=int(args.first_n),
        net_threshold_usd=float(args.net_threshold_usd),
        execute=bool(args.execute),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
