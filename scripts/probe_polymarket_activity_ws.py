#!/usr/bin/env python3
"""Probe Polymarket's live activity websocket as read-only wallet-copy evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.registry import load_wallet_registry
from src.wallet_copy.realtime_feed import parse_rtds_trade_frame
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL
from src.wallet_copy.store import append_jsonl, atomic_write_json, load_json
from scripts.operator_notification_discipline import record_operator_event


DEFAULT_WSS_URL = "wss://ws-live-data.polymarket.com"
DEFAULT_OUTPUT = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_REPORT = "data/research/detection_latency_report.json"
DEFAULT_CIRCUIT_STATE = "data/research/rtds_feed_circuit_breaker_state.json"
DEFAULT_CIRCUIT_EVENTS = "data/research/rtds_feed_circuit_breaker_events.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wss-url", default=DEFAULT_WSS_URL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--report", default=DEFAULT_REPORT)
    parser.add_argument("--duration-s", type=float, default=300.0)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--registry", action="append", default=["configs/wallet_copy/wallets.json"])
    parser.add_argument("--active-registry", default="data/research/wallet_copy_active_hotlane_registry.json")
    parser.add_argument("--send-subscribe", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--stale-feed-s", type=float, default=60.0)
    parser.add_argument("--circuit-state", default=DEFAULT_CIRCUIT_STATE)
    parser.add_argument("--circuit-events", default=DEFAULT_CIRCUIT_EVENTS)
    parser.add_argument("--crash-loop-window-s", type=float, default=300.0)
    parser.add_argument("--crash-loop-threshold", type=int, default=3)
    return parser.parse_args()


def _notify_operator(
    message: str,
    *,
    incident_class: str,
    self_healed: bool = False,
    state_dir: Path | None = None,
) -> dict[str, Any]:
    state_dir = state_dir or ROOT / "data/research"
    return record_operator_event(
        incident_class=incident_class,
        message=message,
        title="Polymarket RTDS",
        actionable=False,
        self_healed=self_healed,
        state_path=state_dir / "operator_notification_discipline_state.json",
        events_path=state_dir / "operator_notification_discipline_events.jsonl",
    )


def _record_circuit_event(
    *,
    state_path: str,
    events_path: str,
    status: str,
    reason: str,
    crash_loop_window_s: float,
    crash_loop_threshold: int,
) -> dict[str, Any]:
    now = time.time()
    state = load_json(state_path, default={})
    if not isinstance(state, dict):
        state = {}
    starts = [
        float(value)
        for value in state.get("recent_process_starts_s") or []
        if now - float(value) <= max(1.0, crash_loop_window_s)
    ]
    if status == "STARTING":
        starts.append(now)
    crash_loop = status == "STARTING" and len(starts) >= max(2, int(crash_loop_threshold))
    effective_status = "INCIDENT_RTDS_FEED_CRASH_LOOP" if crash_loop else status
    event = {
        "generated_at": utc_now_iso(),
        "generated_at_s": now,
        "status": effective_status,
        "reason": reason,
        "pid": os.getpid(),
        "recent_process_starts": len(starts),
        "crash_loop_window_s": crash_loop_window_s,
        "crash_loop_threshold": crash_loop_threshold,
        "next_action": "launchd KeepAlive restarts the sole writer; heartbeat names route failure if flow does not resume",
    }
    previous_status = str(state.get("status") or "")
    process_started_at_s = now if status == "STARTING" else state.get("current_process_started_at_s")
    updated = {
        **event,
        "recent_process_starts_s": starts,
        "current_process_started_at_s": process_started_at_s,
    }
    atomic_write_json(state_path, updated)
    append_jsonl(events_path, event)
    if effective_status.startswith("INCIDENT_") and effective_status != previous_status:
        _notify_operator(
            f"{effective_status}: {reason}",
            incident_class=effective_status,
            state_dir=Path(state_path).parent,
        )
    elif effective_status == "OK" and previous_status.startswith("INCIDENT_"):
        _notify_operator(
            f"{previous_status} self-healed: websocket frames resumed",
            incident_class=previous_status,
            self_healed=True,
            state_dir=Path(state_path).parent,
        )
    return updated


def _is_recv_timeout(exc: BaseException) -> bool:
    return isinstance(exc, TimeoutError) or type(exc).__name__ == "WebSocketTimeoutException"


def _registry_wallets(args: argparse.Namespace) -> set[str]:
    paths = [*(args.registry or [])]
    if args.active_registry:
        paths.append(args.active_registry)
    wallets: set[str] = set()
    for path in paths:
        try:
            wallets.update(spec.normalized_address() for spec in load_wallet_registry(path) if spec.enabled)
        except Exception:
            continue
    return wallets


def _subscribe_payloads() -> list[dict[str, Any]]:
    return [
        {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "activity", "type": "trades", "filters": ""},
                {"topic": "activity", "type": "orders_matched", "filters": ""},
            ],
        },
        {
            "action": "subscribe",
            "subscriptions": [
                {"topic": "activity", "type": "trades"},
                {"topic": "activity", "type": "orders_matched"},
            ],
        },
        {"action": "subscribe", "channel": "activity"},
    ]


def _append_report(path: str, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    report = load_json(path, default={})
    if not isinstance(report, dict):
        report = {}
    sources = report.get("sources") if isinstance(report.get("sources"), dict) else {}
    prior = sources.get("rtds") if isinstance(sources.get("rtds"), dict) else {}
    existing_rows = prior.get("rows") if isinstance(prior.get("rows"), list) else []
    sources["rtds"] = {
        **prior,
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "meta": meta,
        "rows": (existing_rows + rows)[-500:],
    }
    report["sources"] = sources
    report["updated_at"] = utc_now_iso()
    atomic_write_json(path, report)


def main() -> int:
    args = parse_args()
    import websocket

    circuit = _record_circuit_event(
        state_path=args.circuit_state,
        events_path=args.circuit_events,
        status="STARTING",
        reason="feed writer process started",
        crash_loop_window_s=float(args.crash_loop_window_s),
        crash_loop_threshold=int(args.crash_loop_threshold),
    )
    registry_wallets = _registry_wallets(args)
    started_at_s = time.time()
    deadline = started_at_s + float(args.duration_s) if float(args.duration_s) > 0 else None
    rows: deque[dict[str, Any]] = deque(maxlen=500)
    errors: deque[dict[str, Any]] = deque(maxlen=500)
    frames = 0
    normalized_events = 0
    registry_events = 0
    first_frame_s: float | None = None
    last_frame_s: float | None = None
    max_frame_gap_s = 0.0

    ws = None
    connection_started_s: float | None = None
    try:
        while deadline is None or time.time() < deadline:
            if ws is None:
                try:
                    ws = websocket.create_connection(
                        args.wss_url,
                        timeout=float(args.timeout_s),
                        header=[
                            "User-Agent: wallet-copy-polymarket-activity-probe/1.0",
                            "Origin: https://polymarket.com",
                        ],
                    )
                    connection_started_s = time.time()
                    append_jsonl(
                        args.output,
                        {
                            "event": "rtds_connection_open",
                            "captured_at_s": connection_started_s,
                            "captured_at_iso": utc_now_iso(),
                            "wss_url": args.wss_url,
                            "paper_only": True,
                            "live_orders_allowed": False,
                        },
                    )
                    ws.settimeout(min(5.0, max(1.0, float(args.timeout_s))))
                    if args.send_subscribe:
                        for payload in _subscribe_payloads():
                            try:
                                ws.send(json.dumps(payload))
                                append_jsonl(
                                    args.output,
                                    {
                                        "event": "rtds_subscribe_sent",
                                        "captured_at_s": time.time(),
                                        "captured_at_iso": utc_now_iso(),
                                        "payload": payload,
                                    },
                                )
                            except Exception as exc:  # noqa: BLE001
                                errors.append(
                                    {
                                        "stage": "subscribe",
                                        "error_type": type(exc).__name__,
                                        "error": str(exc),
                                        "payload": payload,
                                    }
                                )
                except Exception as exc:  # noqa: BLE001 - probe evidence.
                    errors.append({"stage": "connect", "error_type": type(exc).__name__, "error": str(exc)})
                    circuit = _record_circuit_event(
                        state_path=args.circuit_state,
                        events_path=args.circuit_events,
                        status="INCIDENT_RTDS_FEED_CONNECT_FAILED",
                        reason=f"{type(exc).__name__}: {exc}",
                        crash_loop_window_s=float(args.crash_loop_window_s),
                        crash_loop_threshold=int(args.crash_loop_threshold),
                    )
                    ws = None
                    time.sleep(min(5.0, max(0.1, float(args.timeout_s))))
                    continue

            try:
                raw = ws.recv()
            except Exception as exc:  # noqa: BLE001
                now_s = time.time()
                connection_activity_s = max(
                    value
                    for value in (last_frame_s, connection_started_s, started_at_s)
                    if value is not None
                )
                if _is_recv_timeout(exc) and now_s - connection_activity_s < max(
                    1.0, float(args.stale_feed_s)
                ):
                    continue
                stage = "stale_feed" if _is_recv_timeout(exc) else "recv"
                reason = (
                    f"no websocket frame for {now_s - connection_activity_s:.3f}s"
                    if stage == "stale_feed"
                    else f"{type(exc).__name__}: {exc}"
                )
                errors.append(
                    {
                        "stage": stage,
                        "error_type": "StaleFeed" if stage == "stale_feed" else type(exc).__name__,
                        "error": reason,
                    }
                )
                circuit = _record_circuit_event(
                    state_path=args.circuit_state,
                    events_path=args.circuit_events,
                    status=(
                        "INCIDENT_RTDS_FEED_STALE"
                        if stage == "stale_feed"
                        else "INCIDENT_RTDS_FEED_RECV_FAILED"
                    ),
                    reason=reason,
                    crash_loop_window_s=float(args.crash_loop_window_s),
                    crash_loop_threshold=int(args.crash_loop_threshold),
                )
                try:
                    ws.close()
                except Exception:
                    pass
                ws = None
                connection_started_s = None
                continue

            captured_at_s = time.time()
            if str(circuit.get("status") or "").startswith("INCIDENT_") or frames == 0:
                circuit = _record_circuit_event(
                    state_path=args.circuit_state,
                    events_path=args.circuit_events,
                    status="OK",
                    reason="websocket frames flowing",
                    crash_loop_window_s=float(args.crash_loop_window_s),
                    crash_loop_threshold=int(args.crash_loop_threshold),
                )
            frames += 1
            if first_frame_s is None:
                first_frame_s = captured_at_s
            if last_frame_s is not None:
                max_frame_gap_s = max(max_frame_gap_s, captured_at_s - last_frame_s)
            last_frame_s = captured_at_s
            append_jsonl(args.output, {"event": "rtds_raw_frame", "captured_at_s": captured_at_s, "captured_at_iso": utc_now_iso(), "raw": raw})
            try:
                events = parse_rtds_trade_frame(raw, received_at_s=captured_at_s)
            except Exception as exc:  # noqa: BLE001
                errors.append({"stage": "normalize", "error_type": type(exc).__name__, "error": str(exc)})
                continue
            for event in events:
                row = event.asdict()
                row["event"] = "rtds_trade_event"
                row["is_registry_wallet"] = event.source_wallet in registry_wallets
                normalized_events += 1
                if row["is_registry_wallet"]:
                    registry_events += 1
                rows.append(row)
                append_jsonl(args.output, row)
    finally:
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        append_jsonl(
            args.output,
            {
                "event": "rtds_connection_close",
                "captured_at_s": time.time(),
                "captured_at_iso": utc_now_iso(),
                "frames": frames,
                "normalized_events": normalized_events,
                "registry_events": registry_events,
                "first_frame_s": first_frame_s,
                "last_frame_s": last_frame_s,
                "max_frame_gap_s": max_frame_gap_s,
            },
        )

    meta = {
        "wss_url": args.wss_url,
        "registry_wallets_loaded": len(registry_wallets),
        "frames": frames,
        "normalized_events": normalized_events,
        "registry_events": registry_events,
        "first_frame_s": first_frame_s,
        "last_frame_s": last_frame_s,
        "max_frame_gap_s": max_frame_gap_s,
        "errors": errors,
    }
    meta["errors"] = list(errors)
    _append_report(args.report, list(rows), meta)
    summary = {
        "event": "rtds_probe_summary",
        "captured_at_s": time.time(),
        "captured_at_iso": utc_now_iso(),
        **meta,
        "output": args.output,
        "report": args.report,
        "paper_only": True,
        "live_orders_allowed": False,
    }
    append_jsonl(args.output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if str(circuit.get("status") or "").startswith("INCIDENT_"):
        return 75
    return 0 if frames else 3


if __name__ == "__main__":
    raise SystemExit(main())
