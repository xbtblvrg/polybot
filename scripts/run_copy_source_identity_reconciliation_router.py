#!/usr/bin/env python3
"""Paper-first reconciliation of wallet source rows to Polygon OrderFilled logs."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Iterable


DEFAULT_HOT_HISTORY = "data/research/wallet_copy_live_guard_hot_history_state.json"
DEFAULT_GUARD_STATE = "data/research/wallet_copy_live_guard_state.json"
DEFAULT_DEADMAN_STATE = "data/research/order_flow_deadman_state.json"
DEFAULT_POLYGON_ACCUMULATOR = (
    "data/research/active_member_orderfilled_hot_source_shadow_accumulator.json"
)
DEFAULT_OUTPUT = "data/research/copy_source_identity_reconciliation_router_latest.json"
FROZEN_COHORT_CUTOFF = "2026-07-24T21:28:13Z"
FROZEN_COHORT_ROWS = 110
LOOKBACK_S = 1800.0
FROZEN_ENABLED_WALLETS = {
    "0x2d7c9298b64713de86402bd8a41695e31865a945",
    "0x32de91fa203321fa7735e7854f2b1c844e71ce9d",
    "0x4d8bc628487bbc9931b4d039e6a7529b8ae1a00d",
    "0x86de0516011e2ad7ce3abdc13577de8608db55c3",
    "0xa3e0985f2d0b3209a52f171660287863690d095d",
    "0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0",
    "0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1",
}


def _load_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(f"{target.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.replace(target)


def _parse_iso(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _utc_iso(timestamp: float | None = None) -> str:
    value = datetime.now(timezone.utc) if timestamp is None else datetime.fromtimestamp(timestamp, timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _market_start(market_slug: Any) -> int | None:
    try:
        return int(str(market_slug or "").rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None


def _source_identity(row: dict[str, Any]) -> tuple[str, str]:
    return _wallet(row.get("source_wallet")), str(
        row.get("event_id") or row.get("source_fingerprint") or ""
    )


def _is_btc5m_source(row: dict[str, Any]) -> bool:
    return str(row.get("market_slug") or row.get("market") or "").startswith(
        "btc-updown-5m-"
    )


def source_cohort(
    events: Iterable[dict[str, Any]],
    *,
    enabled_wallets: set[str],
    start_s: float,
    end_s: float,
    limit_latest: int | None = None,
) -> list[dict[str, Any]]:
    """Return stable, wallet/event-deduped BUY rows in a closed interval."""
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in events:
        if (
            not isinstance(row, dict)
            or str(row.get("action") or "").upper() != "BUY"
            or not _is_btc5m_source(row)
        ):
            continue
        identity = _source_identity(row)
        try:
            observed_s = float(row.get("observed_ts") or row.get("event_ts"))
        except (TypeError, ValueError):
            continue
        if (
            identity[0] not in enabled_wallets
            or not identity[1]
            or observed_s < start_s
            or observed_s > end_s
        ):
            continue
        previous = unique.get(identity)
        if previous is None or observed_s < float(
            previous.get("observed_ts") or previous.get("event_ts") or observed_s
        ):
            unique[identity] = row
    rows = sorted(
        unique.values(),
        key=lambda row: (
            float(row.get("observed_ts") or row.get("event_ts") or 0),
            *_source_identity(row),
        ),
    )
    return rows[-limit_latest:] if limit_latest is not None else rows


def excluded_non_btc_count(
    events: Iterable[dict[str, Any]],
    *,
    enabled_wallets: set[str],
    start_s: float,
    end_s: float,
) -> int:
    count = 0
    for row in events:
        if not isinstance(row, dict) or str(row.get("action") or "").upper() != "BUY":
            continue
        try:
            observed_s = float(row.get("observed_ts") or row.get("event_ts"))
        except (TypeError, ValueError):
            continue
        if (
            _wallet(row.get("source_wallet")) in enabled_wallets
            and start_s <= observed_s <= end_s
            and not _is_btc5m_source(row)
        ):
            count += 1
    return count


def polygon_identity_index(
    rows: Iterable[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    """Index unique Polygon logs by transaction hash."""
    by_transaction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[str] = set()
    duplicate_rows = 0
    for row in rows:
        if not isinstance(row, dict) or row.get("event") != "polygon_orderfilled_log":
            continue
        transaction_hash = str(row.get("transaction_hash") or "").strip().lower()
        log_index = row.get("log_index")
        if not transaction_hash or not isinstance(log_index, int):
            continue
        identity = f"{transaction_hash}|{log_index}"
        if identity in seen:
            duplicate_rows += 1
            continue
        seen.add(identity)
        by_transaction[transaction_hash].append(row)
    for transaction_rows in by_transaction.values():
        transaction_rows.sort(key=lambda row: int(row["log_index"]))
    return dict(by_transaction), duplicate_rows


def _polygon_asset(row: dict[str, Any]) -> str:
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return str(decoded.get("asset") or row.get("asset") or "")


def _polygon_participants(row: dict[str, Any]) -> set[str]:
    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    return {
        candidate
        for candidate in (
            _wallet(row.get("selected_wallet")),
            _wallet(row.get("maker") or decoded.get("maker")),
            _wallet(row.get("taker") or decoded.get("taker")),
        )
        if candidate
    }


def _polygon_receipt_s(row: dict[str, Any]) -> float | None:
    for key in ("sidecar_appended_at_s", "received_at_s", "captured_at_s", "event_ts"):
        try:
            return float(row.get(key))
        except (TypeError, ValueError):
            continue
    return None


def _was_fast_path_woken(source: dict[str, Any]) -> bool:
    raw = source.get("raw") if isinstance(source.get("raw"), dict) else {}
    sources = {
        str(raw.get("_walletCopySource") or ""),
        str(raw.get("detection_source") or ""),
        str(source.get("source") or ""),
    }
    sources.update(str(value) for value in raw.get("observation_sources") or [])
    return any("polygon_orderfilled" in value for value in sources)


def reconcile_cohort(
    source_rows: Iterable[dict[str, Any]],
    *,
    polygon_by_transaction: dict[str, list[dict[str, Any]]],
    token_metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    terminals: list[dict[str, Any]] = []
    terminal_counts: Counter[str] = Counter()
    wallet_counts: dict[str, Counter[str]] = defaultdict(Counter)
    parity_violations = 0
    duplicate_routes = 0
    source_replays_deduped = 0
    routed_identities: set[str] = set()
    proxy_alias_targets: dict[str, set[str]] = defaultdict(set)

    for source in source_rows:
        wallet = _wallet(source.get("source_wallet"))
        transaction_hash = str(
            source.get("transaction_hash")
            or (source.get("raw") or {}).get("transactionHash")
            or ""
        ).strip().lower()
        source_token = str(source.get("token_id") or (source.get("raw") or {}).get("asset") or "")
        candidates = list(polygon_by_transaction.get(transaction_hash) or [])
        exact = [row for row in candidates if source_token and _polygon_asset(row) == source_token]
        match = (exact or candidates or [None])[0]
        terminal = "missing_sidecar"
        polygon_identity = ""
        receipt_s: float | None = None
        parity_ok: bool | None = None
        participants: set[str] = set()
        route_eligible = False
        replay_deduped = False

        if isinstance(match, dict):
            polygon_identity = f"{transaction_hash}|{int(match['log_index'])}"
            receipt_s = _polygon_receipt_s(match)
            participants = _polygon_participants(match)
            metadata = token_metadata.get(_polygon_asset(match))
            metadata = metadata if isinstance(metadata, dict) else {}
            market_slug = str(source.get("market_slug") or source.get("market") or "")
            outcome = str(source.get("outcome") or source.get("side") or "").upper()
            parity_ok = bool(
                exact
                and str(metadata.get("market_slug") or "") == market_slug
                and str(metadata.get("outcome") or "").upper() == outcome
            )
            if not parity_ok:
                parity_violations += 1
            market_start = _market_start(market_slug)
            if market_start is not None and receipt_s is not None and receipt_s >= market_start + 300:
                terminal = "received_after_market_close"
            elif market_start is not None and receipt_s is not None:
                receipt_window = int(receipt_s // 300) * 300
                current_or_next = market_start in {receipt_window, receipt_window + 300}
                if current_or_next and not _was_fast_path_woken(source):
                    terminal = "current/+300_not_woken"
                elif wallet in participants:
                    terminal = "matched_enabled_proxy"
                else:
                    terminal = "matched_unmapped_proxy"
            elif wallet in participants:
                terminal = "matched_enabled_proxy"
            else:
                terminal = "matched_unmapped_proxy"

            if polygon_identity in routed_identities:
                source_replays_deduped += 1
                replay_deduped = True
            else:
                routed_identities.add(polygon_identity)
                route_eligible = terminal in {
                    "matched_enabled_proxy",
                    "current/+300_not_woken",
                }
            if parity_ok and route_eligible and wallet:
                for participant in participants:
                    proxy_alias_targets[participant].add(wallet)

        terminal_counts[terminal] += 1
        wallet_counts[wallet][terminal] += 1
        terminals.append(
            {
                "source_wallet": wallet,
                "event_id": str(source.get("event_id") or ""),
                "transaction_hash": transaction_hash,
                "polygon_identity": polygon_identity or None,
                "market_slug": str(source.get("market_slug") or source.get("market") or ""),
                "outcome": str(source.get("outcome") or source.get("side") or "").upper(),
                "observed_ts": source.get("observed_ts") or source.get("event_ts"),
                "polygon_receipt_s": receipt_s,
                "identity_market_outcome_parity": parity_ok,
                "proxy_participants": sorted(participants),
                "terminal": terminal,
                "route_eligible": route_eligible,
                "replay_deduped": replay_deduped,
            }
        )

    input_rows = len(terminals)
    terminal_rows = sum(terminal_counts.values())
    dominant = terminal_counts.most_common(1)[0][0] if terminal_counts else None
    actionable = Counter(
        {
            stage: count
            for stage, count in terminal_counts.items()
            if stage != "matched_enabled_proxy"
        }
    )
    return {
        "input_rows": input_rows,
        "terminal_rows": terminal_rows,
        "input_equals_terminal_rows": input_rows == terminal_rows,
        "terminal_counts": dict(sorted(terminal_counts.items())),
        "per_wallet_counts": {
            wallet: dict(sorted(counts.items())) for wallet, counts in sorted(wallet_counts.items())
        },
        "dominant_stage": dominant,
        "dominant_actionable_stage": (
            actionable.most_common(1)[0][0] if actionable else None
        ),
        "identity_market_outcome_parity_violations": parity_violations,
        "duplicate_routes": duplicate_routes,
        "source_identity_replays_deduped": source_replays_deduped,
        "proxy_source_aliases": {
            proxy: next(iter(wallets))
            for proxy, wallets in sorted(proxy_alias_targets.items())
            if len(wallets) == 1
        },
        "ambiguous_proxy_aliases": {
            proxy: sorted(wallets)
            for proxy, wallets in sorted(proxy_alias_targets.items())
            if len(wallets) > 1
        },
        "parity_violation_samples": [
            row for row in terminals if row.get("identity_market_outcome_parity") is False
        ][:20],
        "representative_rows": terminals[:40],
    }


def build_report(
    *,
    hot_history: dict[str, Any],
    guard_state: dict[str, Any],
    deadman_state: dict[str, Any],
    polygon_accumulator: dict[str, Any],
) -> dict[str, Any]:
    runtime = (
        guard_state.get("active_set_runtime")
        if isinstance(guard_state.get("active_set_runtime"), dict)
        else {}
    )
    enabled_wallets = {
        wallet
        for member in runtime.get("members") or []
        if isinstance(member, dict) and member.get("enabled") is not False
        for wallet in [_wallet(member.get("source_wallet") or member.get("wallet"))]
        if wallet
    }
    events = hot_history.get("events") if isinstance(hot_history.get("events"), list) else []
    polygon_rows = (
        polygon_accumulator.get("rows")
        if isinstance(polygon_accumulator.get("rows"), list)
        else []
    )
    token_metadata = (
        polygon_accumulator.get("token_metadata_cache")
        if isinstance(polygon_accumulator.get("token_metadata_cache"), dict)
        else {}
    )
    polygon_index, replay_deduped = polygon_identity_index(polygon_rows)

    frozen_end = _parse_iso(FROZEN_COHORT_CUTOFF)
    frozen = source_cohort(
        events,
        enabled_wallets=FROZEN_ENABLED_WALLETS,
        start_s=frozen_end - LOOKBACK_S,
        end_s=frozen_end,
        limit_latest=FROZEN_COHORT_ROWS,
    )
    method_acceptance = (
        (deadman_state.get("policy_choke") or {}).get("method_acceptance")
        if isinstance(deadman_state.get("policy_choke"), dict)
        else {}
    )
    current_end_iso = str((method_acceptance or {}).get("lookback_end") or _utc_iso())
    current_end = _parse_iso(current_end_iso)
    current = source_cohort(
        events,
        enabled_wallets=enabled_wallets,
        start_s=current_end - LOOKBACK_S,
        end_s=current_end,
    )
    frozen_non_btc_excluded = excluded_non_btc_count(
        events,
        enabled_wallets=FROZEN_ENABLED_WALLETS,
        start_s=frozen_end - LOOKBACK_S,
        end_s=frozen_end,
    )
    current_non_btc_excluded = excluded_non_btc_count(
        events,
        enabled_wallets=enabled_wallets,
        start_s=current_end - LOOKBACK_S,
        end_s=current_end,
    )
    frozen_report = reconcile_cohort(
        frozen,
        polygon_by_transaction=polygon_index,
        token_metadata=token_metadata,
    )
    current_report = reconcile_cohort(
        current,
        polygon_by_transaction=polygon_index,
        token_metadata=token_metadata,
    )
    frozen_gate = bool(
        frozen_report["input_rows"] == FROZEN_COHORT_ROWS
        and frozen_report["input_equals_terminal_rows"]
        and frozen_report["identity_market_outcome_parity_violations"] == 0
        and frozen_report["duplicate_routes"] == 0
    )
    current_gate = bool(
        current_report["input_equals_terminal_rows"]
        and current_report["identity_market_outcome_parity_violations"] == 0
        and current_report["duplicate_routes"] == 0
    )
    return {
        "schema_version": 1,
        "kind": "copy_source_identity_reconciliation_router",
        "flow_stage": "LIVE/OBSERVE/LEARN",
        "generated_at": _utc_iso(),
        "paper_only": True,
        "identity_rule": "transaction_hash|log_index",
        "enabled_wallets": sorted(enabled_wallets),
        "frozen_enabled_wallets": sorted(FROZEN_ENABLED_WALLETS),
        "polygon_unique_transactions": len(polygon_index),
        "polygon_replay_rows_deduped": replay_deduped,
        "frozen_cohort": {
            "cutoff": FROZEN_COHORT_CUTOFF,
            "selection": "latest 110 unique enabled-wallet BUY rows in the preceding 30 minutes",
            "excluded_non_btc_rows": frozen_non_btc_excluded,
            **frozen_report,
        },
        "current_cohort": {
            "lookback_end": current_end_iso,
            "lookback_s": LOOKBACK_S,
            "excluded_non_btc_rows": current_non_btc_excluded,
            **current_report,
        },
        "gates": {
            "frozen_110_of_110": frozen_gate,
            "current_full_reconciliation": current_gate,
            "zero_parity_violations": (
                frozen_report["identity_market_outcome_parity_violations"] == 0
                and current_report["identity_market_outcome_parity_violations"] == 0
            ),
            "zero_duplicate_routes": (
                frozen_report["duplicate_routes"] == 0
                and current_report["duplicate_routes"] == 0
            ),
            "live_route_allowed": False,
            "live_route_gate": "requires one fresh paper bridge survivor and Fable promotion",
        },
        "submitter_invariant": "paper classification only; the live guard remains the sole submitter",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hot-history", default=DEFAULT_HOT_HISTORY)
    parser.add_argument("--guard-state", default=DEFAULT_GUARD_STATE)
    parser.add_argument("--deadman-state", default=DEFAULT_DEADMAN_STATE)
    parser.add_argument("--polygon-accumulator", default=DEFAULT_POLYGON_ACCUMULATOR)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(
        hot_history=_load_json(args.hot_history),
        guard_state=_load_json(args.guard_state),
        deadman_state=_load_json(args.deadman_state),
        polygon_accumulator=_load_json(args.polygon_accumulator),
    )
    _atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
