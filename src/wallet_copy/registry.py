"""Wallet registry helpers for copy-trading onboarding."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.wallet_copy.models import WalletSpec, utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json
from src.wallet_copy.strategy import wallet_spec_from_mapping


DEFAULT_REGISTRY_PATH = "configs/wallet_copy/wallets.json"


def load_wallet_registry(path: str | Path = DEFAULT_REGISTRY_PATH) -> list[WalletSpec]:
    payload = load_json(path, default={})
    rows = payload.get("wallets") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    specs: list[WalletSpec] = []
    for row in rows:
        if isinstance(row, dict):
            spec = wallet_spec_from_mapping(row)
            if spec.address:
                specs.append(spec)
    return specs


def save_wallet_registry(specs: list[WalletSpec], path: str | Path = DEFAULT_REGISTRY_PATH) -> dict[str, Any]:
    unique: dict[str, WalletSpec] = {}
    for spec in specs:
        unique[spec.normalized_address()] = spec
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_registry",
        "generated_at": utc_now_iso(),
        "wallets": [spec.asdict() for spec in sorted(unique.values(), key=lambda item: item.name)],
    }
    atomic_write_json(path, payload)
    return payload


def upsert_wallet(
    spec: WalletSpec,
    *,
    path: str | Path = DEFAULT_REGISTRY_PATH,
) -> dict[str, Any]:
    specs = load_wallet_registry(path)
    by_address = {item.normalized_address(): item for item in specs}
    by_address[spec.normalized_address()] = spec
    return save_wallet_registry(list(by_address.values()), path)
