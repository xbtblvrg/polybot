#!/usr/bin/env python3
"""Build the BTC5M top-wallet live-paper fleet and window matrix seed.

Flow stage: LEARN/OBSERVE. This is a paper-only launch packet: it selects
the top N wallets from the full-universe copyability leaderboard and builds
queryable per-window wallet action rows from the existing history feed. It
does not submit orders and does not mutate the live guard.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_LEADERBOARD = "data/research/wallet_copy_full_universe_copyability_latest.json"
DEFAULT_HISTORY = "data/research/wallet_copy_history_state.json"
DEFAULT_OUTPUT = "data/research/btc5m_live_paper_fleet_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leaderboard", default=DEFAULT_LEADERBOARD)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--resolutions", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--matrix-max-rows", type=int, default=5000)
    return parser.parse_args()


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        raw = float(value)
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _window_start(row: dict[str, Any]) -> int:
    for key in ("window_start_s", "window_start", "market_start"):
        parsed = int(_parse_ts(row.get(key)) or 0)
        if parsed > 0:
            return parsed
    slug = str(row.get("market_slug") or "")
    match = re.search(r"btc-updown-5m-(\d{10})", slug)
    if match:
        return int(match.group(1))
    ts = _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp"))
    return int(ts // 300 * 300) if ts > 0 else 0


def _is_btc5m(row: dict[str, Any]) -> bool:
    slug = str(row.get("market_slug") or row.get("slug") or "").lower()
    if re.search(r"btc-updown-5m-\d{10}", slug):
        return True
    title = " ".join(str(row.get(key) or "") for key in ("title", "question", "series")).lower()
    return "btc" in title and ("5m" in title or "5 minute" in title or "5-minute" in title)


def _stake(row: dict[str, Any]) -> float:
    usdc = _float(row.get("usdc_size"), 0.0)
    if usdc > 0:
        return usdc
    price = _float(row.get("price"), 0.0)
    size = _float(row.get("size"), 0.0)
    return price * size if price > 0 and size > 0 else 0.0


def _outcome(row: dict[str, Any]) -> str:
    text = str(row.get("outcome") or row.get("side") or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _resolutions_path(root: Path, explicit: str) -> Path:
    if explicit:
        path = Path(explicit)
        return path if path.is_absolute() else root / path
    candidates = [path for path in (root / "data" / "research").glob("btc_resolutions_*.jsonl") if path.is_file()]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else root / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def _load_resolutions(path: Path) -> dict[str, str]:
    winners: dict[str, str] = {}
    if not path.exists():
        return winners
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            raw = str(row.get("direction") or row.get("winner") or row.get("resolved_outcome") or "").upper()
            if raw in {"UP", "YES"}:
                winner = "UP"
            elif raw in {"DOWN", "NO"}:
                winner = "DOWN"
            else:
                continue
            for key in (row.get("market_slug"), row.get("condition_id")):
                text = str(key or "")
                if text:
                    winners[text] = winner
    return winners


def _select_fleet(leaderboard: dict[str, Any], top_n: int) -> list[dict[str, Any]]:
    rows = leaderboard.get("leaderboard") if isinstance(leaderboard.get("leaderboard"), list) else []
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("registry_enabled") is False:
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if not wallet:
            continue
        replay = row.get("copy_replay") if isinstance(row.get("copy_replay"), dict) else {}
        source = row.get("source_history") if isinstance(row.get("source_history"), dict) else {}
        follow = row.get("followability") if isinstance(row.get("followability"), dict) else {}
        selected.append(
            {
                "wallet": wallet,
                "fleet_rank": 0,
                "paper_lane_id": f"paper_copy_1to1_top200_{wallet[-8:]}",
                "status": "LIVE_PAPER_TRACKING",
                "copyability_score": _float(row.get("copyability_score"), 0.0),
                "admission_status": row.get("admission_status") or "",
                "queue_eligible": bool(row.get("queue_eligible")),
                "paper_pnl_usd": replay.get("paper_pnl_usd"),
                "copyable_buy_events": int(replay.get("copyable_buy_events") or 0),
                "candidate_clob_backed_orders": int(replay.get("candidate_clob_backed_orders") or 0),
                "resolved_orders": int(replay.get("resolved_orders") or 0),
                "source_history_pnl_usd": source.get("pnl_usd"),
                "source_history_roi_pct": source.get("roi_pct"),
                "followability_score": follow.get("score"),
                "graduation_gate": "standard_copyintent_paper_to_live_gate",
            }
        )
        if len(selected) >= max(1, int(top_n)):
            break
    for idx, row in enumerate(selected, start=1):
        row["fleet_rank"] = idx
    return selected


def _matrix_rows(history: dict[str, Any], fleet_wallets: set[str], winners: dict[str, str], limit: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    grouped: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    skipped = {"non_btc5m": 0, "non_fleet_wallet": 0, "missing_wallet": 0, "bad_window": 0}
    events = history.get("events") if isinstance(history.get("events"), list) else []
    for row in events:
        if not isinstance(row, dict):
            continue
        if not _is_btc5m(row):
            skipped["non_btc5m"] += 1
            continue
        wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        if not wallet:
            skipped["missing_wallet"] += 1
            continue
        if wallet not in fleet_wallets:
            skipped["non_fleet_wallet"] += 1
            continue
        window_start = _window_start(row)
        if window_start <= 0:
            skipped["bad_window"] += 1
            continue
        market_slug = str(row.get("market_slug") or f"btc-updown-5m-{window_start}")
        condition_id = str(row.get("condition_id") or "")
        key = (window_start, market_slug, condition_id, wallet)
        item = grouped.setdefault(
            key,
            {
                "window_start_s": window_start,
                "market_slug": market_slug,
                "condition_id": condition_id,
                "wallet": wallet,
                "actions": 0,
                "buy_actions": 0,
                "up_buy_usd": 0.0,
                "down_buy_usd": 0.0,
                "total_buy_usd": 0.0,
                "first_event_ts": None,
                "last_event_ts": None,
                "winner": winners.get(market_slug) or winners.get(condition_id) or "",
            },
        )
        ts = _parse_ts(row.get("event_ts") or row.get("observed_ts") or row.get("timestamp"))
        stake = _stake(row)
        side = _outcome(row)
        item["actions"] += 1
        if str(row.get("action") or "").upper() == "BUY":
            item["buy_actions"] += 1
            item["total_buy_usd"] += stake
            if side == "UP":
                item["up_buy_usd"] += stake
            elif side == "DOWN":
                item["down_buy_usd"] += stake
        if ts > 0:
            item["first_event_ts"] = ts if item["first_event_ts"] is None else min(item["first_event_ts"], ts)
            item["last_event_ts"] = ts if item["last_event_ts"] is None else max(item["last_event_ts"], ts)
    rows = sorted(grouped.values(), key=lambda item: (-int(item["window_start_s"]), item["wallet"], item["market_slug"]))
    if limit > 0:
        rows = rows[:limit]
    for item in rows:
        first_ts = _float(item.get("first_event_ts"), 0.0)
        item["first_offset_s"] = round(first_ts - int(item["window_start_s"]), 6) if first_ts else None
        item["dominant_side"] = "UP" if item["up_buy_usd"] >= item["down_buy_usd"] else "DOWN"
        item["total_buy_usd"] = round(float(item["total_buy_usd"]), 6)
        item["up_buy_usd"] = round(float(item["up_buy_usd"]), 6)
        item["down_buy_usd"] = round(float(item["down_buy_usd"]), 6)
    return rows, skipped


def build_report(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    leaderboard = load_json(root / args.leaderboard, default={})
    history = load_json(root / args.history, default={})
    resolutions = _resolutions_path(root, args.resolutions)
    winners = _load_resolutions(resolutions)
    fleet = _select_fleet(leaderboard, args.top_n)
    matrix, skipped = _matrix_rows(history, {row["wallet"] for row in fleet}, winners, int(args.matrix_max_rows))
    windows = {int(row["window_start_s"]) for row in matrix}
    active_wallets = {row["wallet"] for row in matrix}
    rows_by_wallet: dict[str, int] = defaultdict(int)
    windows_by_wallet: dict[str, set[int]] = defaultdict(set)
    actions_by_wallet: dict[str, int] = defaultdict(int)
    for row in matrix:
        wallet = str(row.get("wallet") or "")
        if not wallet:
            continue
        rows_by_wallet[wallet] += 1
        windows_by_wallet[wallet].add(int(row.get("window_start_s") or 0))
        actions_by_wallet[wallet] += int(row.get("actions") or 0)
    required_head = min(50, len(fleet))
    for row in fleet:
        wallet = str(row.get("wallet") or "")
        row_count = int(rows_by_wallet.get(wallet, 0))
        window_count = len(windows_by_wallet.get(wallet, set()))
        row["matrix_rows"] = row_count
        row["matrix_windows"] = window_count
        row["matrix_actions"] = int(actions_by_wallet.get(wallet, 0))
        row["matrix_coverage"] = "WINDOW_ROWS" if row_count > 0 else "NONE"
        row["matrix_coverage_scope"] = "top50_required" if int(row["fleet_rank"]) <= required_head else "beyond_required_head"
        row["holdout_window_evidence"] = row_count > 0
    top50 = fleet[:required_head]
    top50_with_rows = sum(1 for row in top50 if int(row.get("matrix_rows") or 0) > 0)
    top50_rows = sum(int(row.get("matrix_rows") or 0) for row in top50)
    return {
        "schema_version": 1,
        "kind": "btc5m_live_paper_fleet",
        "flow_stage": "LEARN/OBSERVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": _utc_now_iso(),
        "inputs": {
            "leaderboard": args.leaderboard,
            "history": args.history,
            "resolutions": str(resolutions.relative_to(root)) if resolutions.is_relative_to(root) else str(resolutions),
        },
        "summary": {
            "fleet_size": len(fleet),
            "matrix_rows": len(matrix),
            "matrix_windows": len(windows),
            "fleet_wallets_with_matrix_rows": len(active_wallets),
            "ready_queue_wallets": sum(1 for row in fleet if row["admission_status"] == "READY_QUEUE"),
            "positive_copy_pnl_wallets": sum(1 for row in fleet if _float(row.get("paper_pnl_usd"), 0.0) > 0.0),
            "top_wallet": fleet[0]["wallet"] if fleet else "",
            "top_score": fleet[0]["copyability_score"] if fleet else None,
            "history_skipped": skipped,
            "top50_matrix_coverage": {
                "wallets": required_head,
                "with_rows": top50_with_rows,
                "none": required_head - top50_with_rows,
                "rows": top50_rows,
                "coverage_pct": round(100.0 * top50_with_rows / required_head, 6) if required_head else 0.0,
            },
            "coverage_defect": top50_with_rows < required_head,
            "coverage_defect_next_action": (
                "fetch or merge BTC5M window history for top50 rows with matrix_coverage=NONE before funding them"
                if top50_with_rows < required_head
                else ""
            ),
        },
        "fleet": fleet,
        "window_matrix": matrix,
        "next_action": "keep live-paper fleet running against fresh history feed; morning report ranks fleet plus mechanisms",
    }


def main() -> int:
    args = parse_args()
    report = build_report(ROOT, args)
    output = Path(args.output)
    if not output.is_absolute():
        output = ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, report)
    print(json.dumps(report["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
