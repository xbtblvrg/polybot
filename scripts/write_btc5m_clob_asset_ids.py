#!/usr/bin/env python3
"""Continuously write current BTC 5m CLOB token ids for book capture."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.capture_clob_book_snapshots import _asset_ids_from_polygon_rows, _iter_recent_jsonl  # noqa: E402
from scripts.refresh_btc_5m_resolutions_from_gamma import (  # noqa: E402
    CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS,
    _fetch_gamma_event,
    _json_list,
)
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/research/alpha_decay_target_asset_ids.json")
    parser.add_argument("--metadata-cache", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--windows-behind", type=int, default=0)
    parser.add_argument("--windows-ahead", type=int, default=1)
    parser.add_argument("--interval-s", type=float, default=30.0)
    parser.add_argument("--iterations", type=int, default=1, help="0 means run until interrupted.")
    parser.add_argument("--timeout-s", type=float, default=4.0)
    parser.add_argument("--user-agent", default="wallet-copy-btc5m-asset-sidecar/1.0")
    parser.add_argument("--polygon-jsonl", default="")
    parser.add_argument("--polygon-scan-limit", type=int, default=50_000)
    parser.add_argument("--polygon-max-age-s", type=float, default=600.0)
    parser.add_argument("--max-polygon-assets", type=int, default=25)
    return parser.parse_args()


def _window_start(ts: float | None = None) -> int:
    now = time.time() if ts is None else float(ts)
    return int((now - 300) // 300) * 300


def _target_slugs(*, now_ts: float | None = None, windows_behind: int = 0, windows_ahead: int = 1) -> list[str]:
    start = _window_start(now_ts)
    return [
        f"btc-updown-5m-{start + (offset * 300)}"
        for offset in range(-max(0, int(windows_behind)), max(0, int(windows_ahead)) + 1)
    ]


def _market_tokens(event: dict[str, Any], slug: str) -> list[str]:
    tokens: list[str] = []
    for market in event.get("markets") or []:
        if not isinstance(market, dict):
            continue
        if str(market.get("slug") or "") != slug:
            continue
        for token_id in _json_list(market.get("clobTokenIds")):
            text = str(token_id or "").strip()
            if text:
                tokens.append(text)
    return tokens


def collect_asset_ids(slugs: list[str], *, timeout_s: float, user_agent: str) -> dict[str, Any]:
    prior_values: dict[str, str] = {}
    for env_var in CANONICAL_GAMMA_SOURCE_ROUTE_ENV_VARS:
        if env_var in os.environ:
            prior_values[env_var] = os.environ.pop(env_var)
    asset_ids: list[str] = []
    by_slug: dict[str, list[str]] = {}
    errors: dict[str, str] = {}
    token_metadata: dict[str, dict[str, str]] = {}
    seen: set[str] = set()
    try:
        for slug in slugs:
            try:
                events = _fetch_gamma_event(slug, timeout_s=timeout_s, user_agent=user_agent)
            except Exception as exc:  # noqa: BLE001 - sidecar must keep the capture alive.
                errors[slug] = f"{type(exc).__name__}:{exc}"
                continue
            tokens: list[str] = []
            for event in events:
                if isinstance(event, dict):
                    tokens.extend(_market_tokens(event, slug))
                    for market in event.get("markets") or []:
                        if not isinstance(market, dict) or str(market.get("slug") or "") != slug:
                            continue
                        outcomes = [str(value) for value in _json_list(market.get("outcomes"))]
                        token_ids = [str(value) for value in _json_list(market.get("clobTokenIds"))]
                        for index, token_id in enumerate(token_ids):
                            token_metadata[token_id] = {
                                "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
                                "market_slug": slug,
                                "outcome": outcomes[index] if index < len(outcomes) else "",
                            }
            by_slug[slug] = tokens
            for token in tokens:
                if token not in seen:
                    asset_ids.append(token)
                    seen.add(token)
    finally:
        for env_var, value in prior_values.items():
            os.environ[env_var] = value
    return {
        "asset_ids": asset_ids,
        "by_slug": by_slug,
        "cleared_env_vars": sorted(prior_values),
        "errors": errors,
        "token_metadata": token_metadata,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "gamma_current_next_btc5m_clob_token_ids",
        "slugs": slugs,
    }


def _polygon_asset_ids(path: str, *, scan_limit: int, max_age_s: float, max_assets: int) -> list[str]:
    if not str(path or "").strip():
        return []
    rows = _iter_recent_jsonl(str(path), int(scan_limit))
    return _asset_ids_from_polygon_rows(
        rows,
        max_assets=int(max_assets),
        max_age_s=float(max_age_s),
        registry_only=False,
        sources=("polygon_ws", "polygon_http_getLogs"),
    )


def _merge_asset_ids(primary: list[str], extra: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for token in [*primary, *extra]:
        text = str(token or "").strip()
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return out


def main() -> int:
    args = parse_args()
    remaining = int(args.iterations)
    while remaining != 0:
        payload = collect_asset_ids(
            _target_slugs(windows_behind=args.windows_behind, windows_ahead=args.windows_ahead),
            timeout_s=float(args.timeout_s),
            user_agent=str(args.user_agent),
        )
        polygon_asset_ids = _polygon_asset_ids(
            args.polygon_jsonl,
            scan_limit=int(args.polygon_scan_limit),
            max_age_s=float(args.polygon_max_age_s),
            max_assets=int(args.max_polygon_assets),
        )
        payload["polygon_asset_ids"] = polygon_asset_ids
        payload["asset_ids"] = _merge_asset_ids(payload["asset_ids"], polygon_asset_ids)
        payload["source"] = "gamma_current_next_btc5m_clob_token_ids_plus_polygon_seed_fills"
        atomic_write_json(args.output, payload)
        cache = load_json(args.metadata_cache, default={})
        cache = cache if isinstance(cache, dict) else {}
        cache.update(payload["token_metadata"])
        atomic_write_json(args.metadata_cache, cache)
        print(json.dumps({"output": args.output, "asset_ids": len(payload["asset_ids"]), "errors": len(payload["errors"])}, sort_keys=True))
        if remaining > 0:
            remaining -= 1
        if remaining != 0:
            time.sleep(max(1.0, float(args.interval_s)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
