#!/usr/bin/env python3
"""Register or update a wallet-copy target."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import WalletSpec
from src.wallet_copy.registry import DEFAULT_REGISTRY_PATH, upsert_wallet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet", required=True, help="Polymarket wallet address")
    parser.add_argument("--name", default="", help="Stable local wallet label")
    parser.add_argument("--registry", default=DEFAULT_REGISTRY_PATH)
    parser.add_argument("--data-api", default="https://data-api.polymarket.com")
    parser.add_argument("--market-filter", default="btc_5m")
    parser.add_argument("--asset", action="append", default=["BTC"])
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--notes", default="")
    parser.add_argument("--disabled", action="store_true")
    return parser.parse_args()


def _default_name(wallet: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "", wallet.lower())
    return f"wallet_{cleaned[:10]}"


def main() -> int:
    args = parse_args()
    wallet = args.wallet.strip().lower()
    if not re.fullmatch(r"0x[a-f0-9]{40}", wallet):
        raise SystemExit("wallet must be a 0x-prefixed EVM address")
    spec = WalletSpec(
        name=args.name or _default_name(wallet),
        address=wallet,
        enabled=not args.disabled,
        data_api=args.data_api,
        market_filter=args.market_filter,
        asset_allowlist=tuple(str(asset).upper() for asset in args.asset if asset),
        tags=tuple(str(tag) for tag in args.tag if tag),
        notes=args.notes,
    )
    payload = upsert_wallet(spec, path=args.registry)
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
