#!/usr/bin/env python3
"""Zero-AI ORDER138 watcher for the staged wallet-fraction 0.20 lift.

ORDER138 is a launch-argument change.  The watcher arms from the sole resident
guard argv when it sees ``--wallet-fraction 0.20``; the 2026-08-02T00:00Z floor
is recorded as authorization evidence, not as a safety gate.  If an early
restart ever makes 0.20 resident, the revert protection still follows it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import plistlib
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402

DEFAULT_STATE = ROOT / "data/research/order138_wallet_fraction_state.json"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_EVENT_LOG = ROOT / "data/research/order138_wallet_fraction_events.jsonl"
DEFAULT_LIVE_CHANGE_JOURNAL = ROOT / "data/research/live_change_journal.jsonl"
DEFAULT_LAUNCHER = ROOT / "scripts/start_live_guard.sh"
DEFAULT_LAUNCHD_PLIST = ROOT / "launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist"
DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"

ORDER138_PINNED_WALLET = "0xbf337426aa856996b8bb79b238345dd1a0276bf7"
ORDER138_FIRST_N_ACCEPTED = 8
ORDER138_REVERT_NET_USD = -4.0
ORDER138_STAGED_WALLET_FRACTION = 0.20
ORDER138_REVERT_WALLET_FRACTION = 0.10
DIRECTION_ID = "2026-08-01-fable-order138-wallet-fraction-0.20"
ACTIVATION_NOT_BEFORE = dt.datetime(2026, 8, 2, tzinfo=dt.timezone.utc)
STAGED_ARG = "--wallet-fraction 0.20"
REVERT_ARG = "--wallet-fraction 0.10"
UNCHANGED_ARGS = (
    "--max-order-usd 8.0",
    "--drip-min-tranche-usd 1.0",
    "--drip-max-tranche-usd 2.5",
    "--per-window-fill-cap 1",
    "--max-intents 6",
)


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


def _restore_launcher(path: Path) -> dict[str, Any]:
    """Atomically restore only wallet_fraction while pinning every loss limit."""
    text = path.read_text(encoding="utf-8")
    for required in UNCHANGED_ARGS:
        if text.count(required) != 1:
            raise RuntimeError(f"launcher invariant missing or duplicated: {required}")
    staged_count = text.count(STAGED_ARG)
    reverted_count = text.count(REVERT_ARG)
    if staged_count == 0 and reverted_count == 1:
        return {"status": "ALREADY_RESTORED", "changed": False}
    if staged_count != 1 or reverted_count != 0:
        raise RuntimeError(
            "refusing ambiguous launcher rewrite: "
            f"staged_count={staged_count}, reverted_count={reverted_count}"
        )
    tmp = path.with_suffix(path.suffix + ".order138.tmp")
    tmp.write_text(text.replace(STAGED_ARG, REVERT_ARG, 1), encoding="utf-8")
    os.chmod(tmp, path.stat().st_mode)
    os.replace(tmp, path)
    return {"status": "RESTORED", "changed": True}


def _restore_launchd_plist(path: Path) -> dict[str, Any]:
    """Restore the canonical loaded launchd argv, preserving every other arg."""
    payload = plistlib.loads(path.read_bytes())
    args = payload.get("ProgramArguments") if isinstance(payload, dict) else None
    if not isinstance(args, list):
        raise RuntimeError("launchd ProgramArguments missing")
    indexes = [index for index, value in enumerate(args) if value == "--wallet-fraction"]
    if len(indexes) != 1 or indexes[0] + 1 >= len(args):
        raise RuntimeError("launchd wallet-fraction argument missing or ambiguous")
    value_index = indexes[0] + 1
    current = str(args[value_index])
    if current == f"{ORDER138_REVERT_WALLET_FRACTION:.2f}":
        return {"status": "ALREADY_RESTORED", "changed": False}
    if current != f"{ORDER138_STAGED_WALLET_FRACTION:.2f}":
        raise RuntimeError(f"refusing unexpected launchd wallet fraction: {current}")
    args[value_index] = f"{ORDER138_REVERT_WALLET_FRACTION:.2f}"
    tmp = path.with_suffix(path.suffix + ".order138.tmp")
    tmp.write_bytes(plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=False))
    os.chmod(tmp, path.stat().st_mode)
    os.replace(tmp, path)
    return {"status": "RESTORED", "changed": True}


def _resident_wallet_fraction(guard_state_path: Path) -> dict[str, Any]:
    """Read the sole resident guard argv; ambiguity is deliberately fail-open."""
    guard = _load_json(guard_state_path, {})
    identity = guard.get("guard_code_identity") if isinstance(guard, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    try:
        state_pid = int(identity.get("pid") or guard.get("pid") or 0)
    except (TypeError, ValueError):
        state_pid = 0
    pgrep = subprocess.run(
        ["pgrep", "-f", "scripts/run_wallet_copy_live_guard.py"],
        capture_output=True,
        text=True,
        check=False,
    )
    pids = [int(row) for row in pgrep.stdout.splitlines() if row.strip().isdigit()]
    if len(pids) != 1 or state_pid <= 0 or pids[0] != state_pid:
        return {"status": "AMBIGUOUS_FAIL_OPEN", "pids": pids, "state_pid": state_pid}
    command = subprocess.run(
        ["ps", "-p", str(state_pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    fractions: list[float] = []
    tokens = command.split()
    for index, token in enumerate(tokens[:-1]):
        if token == "--wallet-fraction":
            try:
                fractions.append(float(tokens[index + 1]))
            except ValueError:
                pass
    if len(fractions) != 1:
        return {"status": "AMBIGUOUS_FAIL_OPEN", "pids": pids, "state_pid": state_pid}
    return {"status": "PASS", "pid": state_pid, "wallet_fraction": fractions[0]}


def _resident_activation(guard_state_path: Path) -> dict[str, Any] | None:
    """Auto-arm from the sole PASS guard resident on 0.20.

    The not-before clock is an authorization record, not a safety predicate:
    protection must follow resident sizing if an unexpected restart makes 0.20
    live before the planned activation time.
    """
    guard = _load_json(guard_state_path, {})
    identity = guard.get("guard_code_identity") if isinstance(guard, dict) else {}
    identity = identity if isinstance(identity, dict) else {}
    started_at = _parse_iso(identity.get("started_at_utc"))
    try:
        pid = int(identity.get("pid") or guard.get("pid") or 0)
    except (TypeError, ValueError):
        return None
    if identity.get("status") != "PASS" or started_at is None or pid <= 0:
        return None
    pgrep = subprocess.run(
        ["pgrep", "-f", "scripts/run_wallet_copy_live_guard.py"],
        capture_output=True,
        text=True,
        check=False,
    )
    pids = [int(row) for row in pgrep.stdout.splitlines() if row.strip().isdigit()]
    if pids != [pid]:
        return None
    command = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    if command.count(STAGED_ARG) != 1 or REVERT_ARG in command:
        return None
    authorized_activation = started_at >= ACTIVATION_NOT_BEFORE
    return {
        "activated_at": started_at.isoformat().replace("+00:00", "Z"),
        "authorized_activation": authorized_activation,
        "pid": pid,
        "guard_code_identity": "PASS",
        "resident_wallet_fraction": ORDER138_STAGED_WALLET_FRACTION,
    }


def _order_source_wallet(order: dict[str, Any]) -> str:
    for candidate in (order.get("source_wallet"), order.get("wallet"), order.get("wallet_name")):
        text = str(candidate or "").strip().lower()
        if text.startswith("0x"):
            return text
    return ""


def _is_accepted(order: dict[str, Any]) -> bool:
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
    since: dt.datetime,
    first_n: int,
) -> list[dict[str, Any]]:
    candidates: list[tuple[dt.datetime, dict[str, Any]]] = []
    for order in orders:
        if not isinstance(order, dict):
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
            cost = _float(
                order.get("response_filled_size_usd")
                or order.get("requested_size_usd")
                or order.get("size_usd")
            )
        if shares <= 0:
            shares = _float(
                order.get("response_fill_size_shares")
                or order.get("requested_shares")
                or order.get("shares")
            )
        pnl = _float(event.get("pnl_usd"))
        if _float(event.get("cost_usd")) <= 0 and cost > 0 and resolved:
            pnl = (shares if bool(event.get("win")) else 0.0) - cost
        rows.append(
            {
                "submitted_at": submitted_at.isoformat().replace("+00:00", "Z"),
                "order_id": order.get("order_id"),
                "source_wallet": _order_source_wallet(order),
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
    launcher_path: Path = DEFAULT_LAUNCHER,
    launchd_plist_path: Path = DEFAULT_LAUNCHD_PLIST,
    guard_state_path: Path = DEFAULT_GUARD_STATE,
    source_wallet: str = ORDER138_PINNED_WALLET,
    first_n: int = ORDER138_FIRST_N_ACCEPTED,
    net_threshold_usd: float = ORDER138_REVERT_NET_USD,
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
        "pinned_wallet": source_wallet,
        "source_wallet": source_wallet,
        "staged_wallet_fraction": ORDER138_STAGED_WALLET_FRACTION,
        "revert_wallet_fraction": ORDER138_REVERT_WALLET_FRACTION,
        "first_n_accepted": first_n,
        "net_threshold_usd": net_threshold_usd,
        "rule": (
            f"first {first_n} resident-guard accepted orders since activation net "
            f"<= ${net_threshold_usd:.2f} resolved -> withdraw ORDER138"
        ),
        "attribution_scope": "resident_guard_activation_window_all_source_wallets",
    }

    pending = state.get("revert_pending") if isinstance(state.get("revert_pending"), dict) else {}
    if pending:
        resident = _resident_wallet_fraction(guard_state_path)
        prior_pid = int(pending.get("resident_pid_before") or 0)
        if resident.get("status") != "PASS" or int(resident.get("pid") or 0) == prior_pid:
            return {
                **base,
                "status": "ORDER138_REVERT_PENDING_RELOAD",
                "revert_active": False,
                "executed": False,
                "revert_pending": pending,
                "resident_assertion": resident,
            }
        if abs(_float(resident.get("wallet_fraction")) - ORDER138_REVERT_WALLET_FRACTION) > 1e-9:
            failed = {
                **state,
                "status": "ORDER138_REVERT_DID_NOT_LAND",
                "revert_active": False,
                "executed": False,
                "requires_fable_ping": True,
                "resident_assertion": resident,
                "checked_at": generated_at,
            }
            _write_json(state_path, failed)
            _append_jsonl(event_log, failed)
            return {**base, **failed}
        landed = {
            **state,
            "status": "ORDER138_REVERTED",
            "verdict": "ORDER138_REVERTED",
            "revert_active": True,
            "executed": True,
            "reverted_at": generated_at,
            "resident_assertion": resident,
            "resident_reload_required": False,
        }
        landed.pop("revert_pending", None)
        _write_json(state_path, landed)
        _append_jsonl(event_log, landed)
        return {**base, **landed}

    if state.get("revert_active") is True:
        return {
            **base,
            "revert_active": True,
            "status": "ALREADY_REVERTED",
            "reverted_at": state.get("reverted_at"),
            "restore_wallet_fraction": ORDER138_REVERT_WALLET_FRACTION,
        }

    activated_at = _parse_iso(state.get("activated_at"))
    if activated_at is None and execute:
        activation_evidence = _resident_activation(guard_state_path)
        if activation_evidence is not None:
            state = {
                **state,
                "activated_at": activation_evidence["activated_at"],
                "activation_evidence": activation_evidence,
                "authorized_activation": bool(activation_evidence.get("authorized_activation")),
                "status": (
                    "ACTIVE_WATCH"
                    if bool(activation_evidence.get("authorized_activation"))
                    else "ARMED_EARLY_UNAUTHORIZED_ACTIVATION"
                ),
            }
            _write_json(state_path, state)
            activated_at = _parse_iso(state.get("activated_at"))
    if activated_at is None:
        return {
            **base,
            "revert_active": False,
            "status": "STAGED_NOT_ACTIVE",
            "next_action": (
                "after the authorized reload the watcher auto-arms from the sole "
                "PASS guard's post-midnight 0.20 argv; do not hand-edit state"
            ),
        }

    if execute:
        resident_observation = _resident_activation(guard_state_path)
        if resident_observation is not None:
            prior_observation = (
                state.get("resident_observation")
                if isinstance(state.get("resident_observation"), dict)
                else {}
            )
            if prior_observation.get("pid") != resident_observation.get("pid"):
                state = {
                    **state,
                    "resident_observation": {
                        **resident_observation,
                        "observed_at": generated_at,
                        "activation_clock_preserved_at": state.get("activated_at"),
                    },
                }
                _write_json(state_path, state)

    ledger = _load_json(ledger_path, default={})
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    if not isinstance(orders, list):
        orders = []
    rows = _accepted_rows(
        orders=orders,
        resolutions=load_resolutions(str(resolutions_path)),
        since=activated_at,
        first_n=first_n,
    )
    resolved_rows = [row for row in rows if row.get("resolved")]
    net_usd = round(sum(_float(row.get("pnl_usd")) for row in resolved_rows), 6)
    payload = {
        **base,
        "activated_at": state.get("activated_at"),
        "activation_evidence": state.get("activation_evidence"),
        "authorized_activation": state.get("authorized_activation"),
        "revert_active": False,
        "attribution": {
            "accepted_orders": len(rows),
            "resolved_orders": len(resolved_rows),
            "net_resolved_pnl_usd": net_usd,
            "rows": rows,
        },
    }
    if net_usd > net_threshold_usd + 1e-9:
        return {**payload, "status": "WATCH" if len(rows) < first_n else "WATCH_WINDOW_FULL"}

    final = {
        **payload,
        "revert_active": True,
        "status": "ORDER138_REVERTED",
        "verdict": "ORDER138_REVERTED",
        "reverted_at": generated_at,
        "restore_wallet_fraction": ORDER138_REVERT_WALLET_FRACTION,
        "trigger_reason": "first_n_accepted_net_resolved_pnl_lte_threshold",
        "requires_fable_ping": True,
        "terminal": True,
        "re_arm_rule": "no automatic re-arm; only a Fable DIRECTION may clear this",
    }
    if not execute:
        return {**final, "status": "WOULD_REVERT", "revert_active": False, "executed": False}
    resident_before = _resident_wallet_fraction(guard_state_path)
    launcher_restore = _restore_launcher(launcher_path)
    launchd_restore = _restore_launchd_plist(launchd_plist_path)
    final = {
        **final,
        "status": "ORDER138_REVERT_PENDING_RELOAD",
        "verdict": "ORDER138_REVERT_PENDING_RELOAD",
        "revert_active": False,
        "executed": False,
        "terminal": False,
        "canonical_launcher_restore": launcher_restore,
        "canonical_launchd_restore": launchd_restore,
        "revert_pending": {
            "requested_at": generated_at,
            "resident_pid_before": resident_before.get("pid"),
        },
        "resident_reload_required": True,
        "resident_reload_command": (
            "python3 scripts/brainless_live_guard_restart.py --execute "
            "--allow-generation-reload --generation-reload-reason "
            f"{DIRECTION_ID}-mechanical-revert"
        ),
    }
    _write_json(state_path, {**state, **final})
    _append_jsonl(event_log, final)
    _append_jsonl(
        live_change_journal,
        {
            "at": generated_at,
            "kind": "order138_wallet_fraction_revert",
            "direction_id": DIRECTION_ID,
            "source_wallet": source_wallet,
            "net_resolved_pnl_usd": net_usd,
            "accepted_orders": len(rows),
            "resolved_orders": len(resolved_rows),
            "restore_wallet_fraction": ORDER138_REVERT_WALLET_FRACTION,
            "canonical_launcher_restore": launcher_restore,
            "resident_reload_required": True,
        },
    )
    return final


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--event-log", default=str(DEFAULT_EVENT_LOG))
    parser.add_argument("--live-change-journal", default=str(DEFAULT_LIVE_CHANGE_JOURNAL))
    parser.add_argument("--launcher", default=str(DEFAULT_LAUNCHER))
    parser.add_argument("--launchd-plist", default=str(DEFAULT_LAUNCHD_PLIST))
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--source-wallet", default=ORDER138_PINNED_WALLET)
    parser.add_argument("--first-n", type=int, default=ORDER138_FIRST_N_ACCEPTED)
    parser.add_argument("--net-threshold-usd", type=float, default=ORDER138_REVERT_NET_USD)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    result = evaluate(
        state_path=Path(args.state),
        ledger_path=Path(args.ledger),
        resolutions_path=Path(args.resolutions),
        event_log=Path(args.event_log),
        live_change_journal=Path(args.live_change_journal),
        launcher_path=Path(args.launcher),
        launchd_plist_path=Path(args.launchd_plist),
        guard_state_path=Path(args.guard_state),
        source_wallet=str(args.source_wallet).strip().lower(),
        first_n=int(args.first_n),
        net_threshold_usd=float(args.net_threshold_usd),
        execute=bool(args.execute),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
