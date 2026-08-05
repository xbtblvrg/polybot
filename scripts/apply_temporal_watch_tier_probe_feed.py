#!/usr/bin/env python3
"""Apply temporal-profitability probe candidates to the watch-tier poller config.

This is measure-only configuration: it changes which wallets the separate
watch-tier poller observes, not live membership or CopyIntent eligibility.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_TEMPORAL_FEED = "data/research/wallet_temporal_profitability_latest.json"
DEFAULT_WATCH_CONFIG = "configs/wallet_copy/watch_tier_wallets.json"
DEFAULT_OUTPUT = "data/research/temporal_watch_tier_probe_apply_latest.json"
DEFAULT_SKIP_WALLETS = (
    "0xe6db20932faf0f9780acf75d95c74c9984407dac,"
    "0xac0586732786905d285959613f1813bc89246729"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--temporal-feed", default=DEFAULT_TEMPORAL_FEED)
    parser.add_argument("--watch-config", default=DEFAULT_WATCH_CONFIG)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--skip-wallets", default=DEFAULT_SKIP_WALLETS)
    parser.add_argument("--write-config", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _skip_set(raw: str) -> set[str]:
    return {_wallet(item) for item in str(raw or "").replace(" ", "").split(",") if _wallet(item)}


def _sources(*rows: Any) -> list[str]:
    out: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for source in row.get("sources") or []:
            source = str(source or "")
            if source and source not in out:
                out.append(source)
    if "temporal_profitability_dead_band_feed" not in out:
        out.append("temporal_profitability_dead_band_feed")
    return out


def _temporal_wallet_row(candidate: dict[str, Any], *, existing: dict[str, Any] | None = None) -> dict[str, Any]:
    existing = existing if isinstance(existing, dict) else {}
    wallet = _wallet(candidate.get("wallet"))
    dead = candidate.get("dead_band_18_22_utc") if isinstance(candidate.get("dead_band_18_22_utc"), dict) else {}
    fit = candidate.get("fit") if isinstance(candidate.get("fit"), dict) else {}
    staleness = fit.get("staleness") if isinstance(fit.get("staleness"), dict) else {}
    copyability = candidate.get("copyability") if isinstance(candidate.get("copyability"), dict) else {}
    return {
        **existing,
        "source_wallet": wallet,
        "selection_reason": "temporal_dead_band_probe_feed",
        "temporal_rank_score": candidate.get("rank_score"),
        "temporal_classification": candidate.get("classification"),
        "dead_band_roi_pct": dead.get("roi_pct"),
        "dead_band_resolved_trades": dead.get("resolved_trades"),
        "latest_dead_band_event_age_h": fit.get("latest_dead_band_event_age_h"),
        "staleness": staleness,
        "probe_feed_inclusion_reason": fit.get("inclusion_reason"),
        "copyability_score": copyability.get("copyability_score"),
        "paper_pnl_usd": copyability.get("paper_pnl_usd", existing.get("paper_pnl_usd")),
        "paper_only": True,
        "live_orders_allowed": False,
        "sources": _sources(existing, {"sources": ["temporal_profitability_dead_band_feed"]}),
    }


def build_payload(
    *,
    temporal: dict[str, Any],
    config: dict[str, Any],
    skip_wallets: set[str],
    watch_config_path: str = DEFAULT_WATCH_CONFIG,
) -> tuple[dict[str, Any], dict[str, Any]]:
    generated_at = utc_now_iso()
    feed = temporal.get("watch_tier_probe_feed") if isinstance(temporal.get("watch_tier_probe_feed"), dict) else {}
    candidates = feed.get("candidates") if isinstance(feed.get("candidates"), list) else []
    existing_rows = [row for row in config.get("wallets") or [] if isinstance(row, dict)]
    existing_by_wallet = {_wallet(row.get("source_wallet") or row.get("wallet")): row for row in existing_rows}
    before_wallets = [wallet for wallet in existing_by_wallet if wallet]
    selected_rows: list[dict[str, Any]] = []
    added_wallets: list[str] = []
    already_present_wallets: list[str] = []
    skipped: list[dict[str, Any]] = []
    skipped_seen: set[str] = set()
    seen: set[str] = set()

    for candidate in candidates:
        wallet = _wallet(candidate.get("wallet"))
        if not wallet:
            continue
        if wallet in skip_wallets:
            if wallet not in skipped_seen:
                skipped.append({"wallet": wallet, "reason": "already_under_live_or_routing_observation"})
                skipped_seen.add(wallet)
            continue
        existing = existing_by_wallet.get(wallet)
        selected_rows.append(_temporal_wallet_row(candidate, existing=existing))
        seen.add(wallet)
        if existing:
            already_present_wallets.append(wallet)
        else:
            added_wallets.append(wallet)

    retained_rows: list[dict[str, Any]] = []
    for row in existing_rows:
        wallet = _wallet(row.get("source_wallet") or row.get("wallet"))
        if not wallet:
            continue
        if wallet in skip_wallets:
            if wallet not in skipped_seen:
                skipped.append({"wallet": wallet, "reason": "already_under_live_or_routing_observation"})
                skipped_seen.add(wallet)
            continue
        if wallet in seen:
            continue
        retained = dict(row)
        retained["paper_only"] = True
        retained["live_orders_allowed"] = False
        retained_rows.append(retained)
        seen.add(wallet)

    wallets = selected_rows + retained_rows
    criteria = config.get("selection_criteria") if isinstance(config.get("selection_criteria"), dict) else {}
    criteria = {
        **criteria,
        "source_pool": "data/research/wallet_temporal_profitability_latest.json.watch_tier_probe_feed",
        "temporal_feed_count": len(candidates),
        "temporal_feed_applied_count": len(selected_rows),
        "skip_wallets": sorted(skip_wallets),
        "skipped_reason": "already under live/routing observation per Fable 2026-07-10T19:49Z",
        "cap": max(int(criteria.get("cap") or 0), len(wallets)),
        "ranking": "temporal dead-band feed first after staleness/ROI hygiene, then existing watch-tier rows",
    }
    updated_config = {
        **config,
        "schema_version": 2,
        "kind": "wallet_copy_watch_tier_wallets",
        "flow_stage": "DISCOVER/LEARN",
        "generated_at": generated_at,
        "pinned_by": "codex_temporal_dead_band_probe_20260710T2005Z",
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "contract": "Measure-only watch-tier Data API polling into separate files; no CopyIntent path, no live membership change, no order submission.",
        "ranking_artifact": "data/research/wallet_temporal_profitability_latest.json",
        "selection_criteria": criteria,
        "wallets": wallets,
    }
    status = {
        "schema_version": 1,
        "kind": "temporal_watch_tier_probe_apply",
        "flow_stage": "DISCOVER/LEARN",
        "generated_at": generated_at,
        "status": "APPLIED",
        "paper_only": True,
        "live_orders_allowed": False,
        "measure_only": True,
        "summary": {
            "feed_candidates": len(candidates),
            "configured_wallets_before": len(before_wallets),
            "configured_wallets_after": len(wallets),
            "temporal_applied": len(selected_rows),
            "added_wallets": added_wallets,
            "already_present_wallets": already_present_wallets,
            "skipped_wallets": skipped,
            "single_submitter_invariant": "watch-tier poller writes separate measurement files only; live guard remains sole submitter",
        },
        "watch_config": str(watch_config_path),
    }
    return updated_config, status


def main() -> int:
    args = parse_args()
    temporal = load_json(args.temporal_feed, default={})
    config = load_json(args.watch_config, default={})
    updated_config, status = build_payload(
        temporal=temporal if isinstance(temporal, dict) else {},
        config=config if isinstance(config, dict) else {},
        skip_wallets=_skip_set(args.skip_wallets),
        watch_config_path=args.watch_config,
    )
    if bool(args.write_config):
        atomic_write_json(args.watch_config, updated_config)
    atomic_write_json(args.output, status)
    print(json.dumps(status["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
