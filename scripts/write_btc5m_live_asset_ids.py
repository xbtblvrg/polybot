#!/usr/bin/env python3
"""Compatibility wrapper for the BTC 5m CLOB asset-id sidecar."""

from __future__ import annotations

from argparse import Namespace
from typing import Any

from scripts.write_btc5m_clob_asset_ids import _target_slugs, collect_asset_ids, main


def _market_token_rows(
    *,
    now_s: float,
    lookahead_windows: int,
    gamma_base_url: str = "",
    timeout_s: float = 4.0,
) -> list[dict[str, Any]]:
    _ = gamma_base_url
    slugs = _target_slugs(now_ts=now_s, windows_ahead=max(0, int(lookahead_windows) - 1))
    payload = collect_asset_ids(slugs, timeout_s=float(timeout_s), user_agent="wallet-copy-btc5m-asset-sidecar/1.0")
    return [
        {
            "market_slug": slug,
            "token_ids": list(payload.get("by_slug", {}).get(slug, [])),
            "window_start_s": int(slug.rsplit("-", 1)[-1]),
        }
        for slug in slugs
    ]


def build_payload(args: Namespace, *, now_s: float) -> dict[str, Any]:
    asset_ids: list[str] = []
    seen: set[str] = set()
    rows = _market_token_rows(
        now_s=now_s,
        lookahead_windows=int(getattr(args, "lookahead_windows", 2)),
        gamma_base_url=str(getattr(args, "gamma_base_url", "")),
        timeout_s=float(getattr(args, "timeout_s", 4.0)),
    )
    for row in rows:
        for token_id in row.get("token_ids") or []:
            text = str(token_id or "").strip()
            if text and text not in seen:
                asset_ids.append(text)
                seen.add(text)
    return {
        "asset_count": len(asset_ids),
        "asset_ids": asset_ids,
        "live_orders_allowed": False,
        "market_rows": rows,
        "paper_only": True,
        "source": "gamma_current_next_btc5m_clob_token_ids",
    }


if __name__ == "__main__":
    raise SystemExit(main())
