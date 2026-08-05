#!/usr/bin/env python3
"""Build the paper-only active-set expansion shortlist from existing evidence.

Flow stage: PROMOTE/ROTATE/LEARN. This script reads local registry, alpha
profile, guard, and polygon capture evidence. It never fetches network data
and never touches the live guard.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backtest_whale_consensus_v1 import load_token_map  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.alpha_freshness import require_fresh_alpha_report  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_ALPHA = "data/research/alpha_decay_report.json"
DEFAULT_REGISTRY = "configs/wallet_copy/wallets.json"
DEFAULT_GUARD = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/active_set_expansion_shortlist.json"

PNL_RE = re.compile(r"\b(WEEK|MONTH) rank #(?P<rank>\d+) pnl=(?P<pnl>-?\d+(?:\.\d+)?) vol=(?P<vol>-?\d+(?:\.\d+)?)")


@dataclass(frozen=True)
class AlphaReportSelection:
    report: dict[str, Any]
    selected_path: str
    requested_path: str
    fallback_used: bool
    reason: str
    candidates_checked: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alpha-decay-report", default=DEFAULT_ALPHA)
    parser.add_argument("--wallet-registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD)
    parser.add_argument("--polygon-jsonl", default="", help="Default: alpha_decay_report.polygon_jsonl")
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--max-polygon-rows", type=int, default=0)
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _registry_wallets(registry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = registry.get("wallets") if isinstance(registry.get("wallets"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("address"))
        if wallet:
            out[wallet] = row
    return out


def _active_set_wallets(guard_state: dict[str, Any]) -> set[str]:
    active_set = guard_state.get("active_set") if isinstance(guard_state.get("active_set"), dict) else {}
    wallets: set[str] = set()
    for member in active_set.get("members") or []:
        if isinstance(member, dict):
            wallet = _norm_wallet(member.get("source_wallet"))
            if wallet:
                wallets.add(wallet)
    return wallets


def _active_traded_windows(guard_state: dict[str, Any]) -> set[str]:
    participation = guard_state.get("window_participation") if isinstance(guard_state.get("window_participation"), dict) else {}
    traded: set[str] = set()
    for row in participation.get("window_rollups") or []:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("market_slug") or "")
        if slug and (int(row.get("our_submits") or 0) > 0 or int(row.get("our_fills") or 0) > 0):
            traded.add(slug)
    return traded


def _alpha_execution(alpha_report: dict[str, Any]) -> dict[str, Any]:
    execution = alpha_report.get("execution_profiles") if isinstance(alpha_report.get("execution_profiles"), dict) else {}
    return execution


def _alpha_eligible_profile_count(alpha_report: dict[str, Any]) -> int:
    execution = _alpha_execution(alpha_report)
    explicit = execution.get("eligible_profile_count")
    if explicit not in (None, ""):
        return int(num(explicit))
    profiles = execution.get("profiles_by_wallet") if isinstance(execution.get("profiles_by_wallet"), dict) else {}
    return sum(1 for profile in profiles.values() if isinstance(profile, dict) and profile.get("eligible"))


def _alpha_status(alpha_report: dict[str, Any]) -> str:
    execution = _alpha_execution(alpha_report)
    return str(execution.get("alpha_decay_status") or alpha_report.get("alpha_decay_status") or alpha_report.get("status") or "")


def _alpha_report_fallback_candidates(requested: Path) -> list[Path]:
    search_dir = requested.parent if requested.parent != Path("") else Path(DEFAULT_ALPHA).parent
    patterns = ("alpha_decay_report*.json",)
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(search_dir.glob(pattern))
    return sorted({path for path in candidates if path.is_file()})


def load_alpha_report_with_fallback(path: str) -> AlphaReportSelection:
    requested = Path(path)
    requested_payload = load_json(str(requested), default={})
    requested_report = requested_payload if isinstance(requested_payload, dict) else {}
    requested_count = _alpha_eligible_profile_count(requested_report)
    requested_status = _alpha_status(requested_report)
    if requested_count > 0:
        return AlphaReportSelection(
            report=requested_report,
            selected_path=str(requested),
            requested_path=str(requested),
            fallback_used=False,
            reason="requested_alpha_report_has_eligible_profiles",
            candidates_checked=1,
        )

    best_path = requested
    best_report: dict[str, Any] = {}
    best_key: tuple[int, int, float, str] | None = None
    checked = 0
    for candidate in _alpha_report_fallback_candidates(requested):
        payload = load_json(str(candidate), default={})
        report = payload if isinstance(payload, dict) else {}
        eligible_count = _alpha_eligible_profile_count(report)
        status = _alpha_status(report).upper()
        checked += 1
        if eligible_count <= 0:
            continue
        pass_bonus = 1 if status == "PASS" else 0
        mtime = candidate.stat().st_mtime
        key = (pass_bonus, eligible_count, mtime, str(candidate))
        if best_key is None or key > best_key:
            best_key = key
            best_path = candidate
            best_report = report

    if best_report:
        reason_status = requested_status or "missing_status"
        return AlphaReportSelection(
            report=best_report,
            selected_path=str(best_path),
            requested_path=str(requested),
            fallback_used=True,
            reason=f"requested_alpha_report_empty_or_invalid:{reason_status}",
            candidates_checked=checked,
        )
    return AlphaReportSelection(
        report=requested_report,
        selected_path=str(requested),
        requested_path=str(requested),
        fallback_used=False,
        reason="no_fallback_alpha_report_with_eligible_profiles",
        candidates_checked=checked,
    )


def _leaderboard_pnl(row: dict[str, Any]) -> dict[str, Any]:
    structured_keys = (
        "weekly_pnl",
        "week_pnl",
        "leaderboard_week_pnl",
        "monthly_pnl",
        "month_pnl",
        "leaderboard_month_pnl",
        "pnl",
        "leaderboard_pnl",
    )
    for key in structured_keys:
        if key in row and row.get(key) not in (None, ""):
            return {"resolved_pnl": num(row.get(key)), "source": key, "period": "structured", "rank": None, "volume": None}

    matches = [match.groupdict() | {"period": match.group(1)} for match in PNL_RE.finditer(str(row.get("notes") or ""))]
    week = [match for match in matches if match["period"] == "WEEK"]
    chosen = week[-1] if week else (matches[-1] if matches else None)
    if not chosen:
        return {"resolved_pnl": 0.0, "source": "missing", "period": "missing", "rank": None, "volume": None}
    return {
        "resolved_pnl": round(num(chosen.get("pnl")), 6),
        "source": "registry_notes",
        "period": chosen["period"].lower(),
        "rank": int(num(chosen.get("rank"))),
        "volume": round(num(chosen.get("vol")), 6),
    }


def _alpha_fallback_score(profile: dict[str, Any]) -> float:
    stats = profile.get("edge_stats") if isinstance(profile.get("edge_stats"), dict) else {}
    return round(num(stats.get("mean")) * int(profile.get("fill_sample") or 0), 9)


def eligible_profile_rows(alpha_report: dict[str, Any], registry: dict[str, Any], guard_state: dict[str, Any]) -> list[dict[str, Any]]:
    execution = alpha_report.get("execution_profiles") if isinstance(alpha_report.get("execution_profiles"), dict) else {}
    profiles = execution.get("profiles_by_wallet") if isinstance(execution.get("profiles_by_wallet"), dict) else {}
    registry_by_wallet = _registry_wallets(registry)
    active_wallets = _active_set_wallets(guard_state)
    rows: list[dict[str, Any]] = []
    for wallet, profile in profiles.items():
        wallet = _norm_wallet(wallet)
        if not wallet or not isinstance(profile, dict) or not profile.get("eligible"):
            continue
        if wallet in active_wallets:
            continue
        registry_row = registry_by_wallet.get(wallet)
        if not registry_row or registry_row.get("enabled") is False:
            continue
        pnl = _leaderboard_pnl(registry_row)
        if pnl["source"] == "missing":
            pnl = {
                **pnl,
                "resolved_pnl": _alpha_fallback_score(profile),
                "source": "alpha_edge_score_fallback",
                "period": "alpha_edge",
            }
        rows.append(
            {
                "wallet": wallet,
                "name": registry_row.get("name") or "",
                "resolved_pnl": pnl["resolved_pnl"],
                "resolved_pnl_source": pnl["source"],
                "resolved_pnl_period": pnl["period"],
                "leaderboard_rank": pnl["rank"],
                "leaderboard_volume": pnl["volume"],
                "eligible_move_slices": int(profile.get("eligible_move_slice_count") or 0),
                "copyable_rate_pct": profile.get("copyable_rate_pct"),
                "fill_sample": int(profile.get("fill_sample") or 0),
                "mean_edge": profile.get("mean_edge"),
                "median_edge": profile.get("median_edge"),
                "eligible_profile": {
                    "status": profile.get("status"),
                    "best_eligible_move_slice": profile.get("best_eligible_move_slice") or {},
                    "eligible_move_slice_count": int(profile.get("eligible_move_slice_count") or 0),
                    "copyable_rate_pct": profile.get("copyable_rate_pct"),
                    "fill_sample": int(profile.get("fill_sample") or 0),
                    "mean_edge": profile.get("mean_edge"),
                    "median_edge": profile.get("median_edge"),
                },
            }
        )
    return rows


def scan_candidate_fills(
    *,
    polygon_jsonl: str,
    candidate_wallets: set[str],
    active_traded_windows: set[str],
    resolutions: str,
    max_rows: int = 0,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    token_map = load_token_map(Path(resolutions))
    fill_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "fills": 0,
            "complementary_fills": 0,
            "windows": set(),
            "complementary_windows": set(),
        }
    )
    diagnostics: Counter[str] = Counter()
    path = Path(polygon_jsonl)
    if not path.exists():
        diagnostics["missing_polygon_jsonl"] += 1
        return {}, dict(diagnostics)
    with path.open(errors="ignore") as handle:
        for raw in handle:
            if max_rows and diagnostics["rows_seen"] >= int(max_rows):
                break
            diagnostics["rows_seen"] += 1
            if "polygon_orderfilled_log" not in raw:
                diagnostics["non_orderfilled_line"] += 1
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                diagnostics["bad_json"] += 1
                continue
            decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
            asset = str(decoded.get("asset") or "")
            meta = token_map.get(asset)
            if meta is None:
                diagnostics["asset_not_btc5m_resolved"] += 1
                continue
            wallets = {_norm_wallet(row.get("selected_wallet"))}
            wallets.update(_norm_wallet(item) for item in (row.get("registry_wallets") or []))
            wallets.discard("")
            matched = wallets & candidate_wallets
            if not matched:
                diagnostics["non_candidate_wallet"] += 1
                continue
            for wallet in matched:
                stats = fill_stats[wallet]
                stats["fills"] += 1
                stats["windows"].add(meta.market_slug)
                if meta.market_slug not in active_traded_windows:
                    stats["complementary_fills"] += 1
                    stats["complementary_windows"].add(meta.market_slug)
                diagnostics["candidate_fills"] += 1
    out: dict[str, dict[str, Any]] = {}
    for wallet, stats in fill_stats.items():
        fills = int(stats["fills"])
        complementary = int(stats["complementary_fills"])
        windows = stats["windows"]
        comp_windows = stats["complementary_windows"]
        out[wallet] = {
            "fills": fills,
            "complementary_fills": complementary,
            "complementary_window_pct": round(100.0 * complementary / fills, 6) if fills else 0.0,
            "recent_fill_windows": len(windows),
            "complementary_windows": len(comp_windows),
        }
    return out, dict(sorted(diagnostics.items()))


def build_shortlist(
    *,
    alpha_report: dict[str, Any],
    registry: dict[str, Any],
    guard_state: dict[str, Any],
    polygon_jsonl: str,
    resolutions: str,
    top_n: int,
    max_polygon_rows: int = 0,
) -> dict[str, Any]:
    profile_rows = eligible_profile_rows(alpha_report, registry, guard_state)
    active_wallets = _active_set_wallets(guard_state)
    active_windows = _active_traded_windows(guard_state)
    fill_stats, scan_diagnostics = scan_candidate_fills(
        polygon_jsonl=polygon_jsonl,
        candidate_wallets={row["wallet"] for row in profile_rows},
        active_traded_windows=active_windows,
        resolutions=resolutions,
        max_rows=max_polygon_rows,
    )
    scored_rows: list[dict[str, Any]] = []
    for row in profile_rows:
        stats = fill_stats.get(
            row["wallet"],
            {
                "fills": 0,
                "complementary_fills": 0,
                "complementary_window_pct": 0.0,
                "recent_fill_windows": 0,
                "complementary_windows": 0,
            },
        )
        scored_rows.append({**row, **stats})
    fill_backed_rows = [row for row in scored_rows if int(row.get("fills") or 0) > 0]
    pnl_only_rows = [row for row in scored_rows if int(row.get("fills") or 0) <= 0]
    fill_backed_rows.sort(
        key=lambda row: (
            -int(row.get("complementary_fills") or 0),
            -num(row.get("resolved_pnl")),
            -int(row.get("fills") or 0),
            -int(row.get("eligible_move_slices") or 0),
            str(row.get("wallet") or ""),
        )
    )
    pnl_only_rows.sort(
        key=lambda row: (
            -num(row.get("resolved_pnl")),
            -int(row.get("eligible_move_slices") or 0),
            str(row.get("wallet") or ""),
        )
    )
    top = fill_backed_rows[: max(5, int(top_n))]
    return {
        "schema_version": 1,
        "kind": "active_set_expansion_shortlist",
        "flow_stage": "PROMOTE/ROTATE/LEARN",
        "paper_only": True,
        "live_orders_allowed": False,
        "generated_at": utc_now_iso(),
        "pool": {
            "eligible_profiles": int((alpha_report.get("execution_profiles") or {}).get("eligible_profile_count") or 0),
            "after_active_set_exclusion": len(profile_rows),
        },
        "inputs": {
            "alpha_decay_report": DEFAULT_ALPHA,
            "wallet_registry": DEFAULT_REGISTRY,
            "guard_state": DEFAULT_GUARD,
            "polygon_jsonl": polygon_jsonl,
            "resolutions": resolutions,
            "max_polygon_rows": int(max_polygon_rows),
        },
        "ranking": {
            "primary": "complementary_fills_desc",
            "tie_breakers": ["resolved_pnl_desc", "eligible_move_slices_desc", "fills_desc"],
            "complementary_definition": "candidate BTC-5m fills in windows where the current active set had zero submits/fills",
            "pnl_only_no_lane_evidence_excluded_from_top": True,
        },
        "summary": {
            "eligible_profiles": int((alpha_report.get("execution_profiles") or {}).get("eligible_profile_count") or 0),
            "pool_after_active_set_exclusion": len(profile_rows),
            "active_set_wallets": sorted(active_wallets),
            "active_traded_windows": len(active_windows),
            "denominator_limited": len(active_windows) < 20,
            "candidates_with_recent_fills": sum(1 for row in scored_rows if int(row.get("fills") or 0) > 0),
            "pnl_only_no_lane_evidence_count": len(pnl_only_rows),
            "top_count": len(top),
            "polygon_scan": scan_diagnostics,
        },
        "top": top,
        "top_candidates": top,
        "pnl_only_no_lane_evidence": pnl_only_rows,
    }


def main() -> int:
    args = parse_args()
    alpha_selection = load_alpha_report_with_fallback(args.alpha_decay_report)
    alpha_report = alpha_selection.report
    require_fresh_alpha_report(alpha_report, path=alpha_selection.selected_path)
    registry = load_json(args.wallet_registry, default={})
    registry = registry if isinstance(registry, dict) else {}
    guard_state = load_json(args.guard_state, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    polygon_jsonl = args.polygon_jsonl or str(alpha_report.get("polygon_jsonl") or "")
    if not polygon_jsonl:
        raise SystemExit("polygon_jsonl missing: pass --polygon-jsonl or provide alpha_decay_report.polygon_jsonl")
    report = build_shortlist(
        alpha_report=alpha_report,
        registry=registry,
        guard_state=guard_state,
        polygon_jsonl=polygon_jsonl,
        resolutions=args.resolutions,
        top_n=int(args.top_n),
        max_polygon_rows=int(args.max_polygon_rows),
    )
    report["inputs"].update(
        {
            "alpha_decay_report": alpha_selection.selected_path,
            "requested_alpha_decay_report": alpha_selection.requested_path,
            "wallet_registry": args.wallet_registry,
            "guard_state": args.guard_state,
        }
    )
    report["summary"]["alpha_report_selection"] = {
        "fallback_used": alpha_selection.fallback_used,
        "reason": alpha_selection.reason,
        "selected_path": alpha_selection.selected_path,
        "requested_path": alpha_selection.requested_path,
        "candidates_checked": alpha_selection.candidates_checked,
    }
    atomic_write_json(args.output, report)
    summary = report["summary"]
    best = report["top_candidates"][0] if report["top_candidates"] else {}
    print(
        "active_set_expansion_shortlist",
        f"top_count={summary['top_count']}",
        f"pool={summary['pool_after_active_set_exclusion']}",
        f"candidate_fills={summary['polygon_scan'].get('candidate_fills', 0)}",
        f"best={best.get('wallet', '')}",
        f"best_resolved_pnl={best.get('resolved_pnl', 0.0)}",
        f"output={args.output}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
