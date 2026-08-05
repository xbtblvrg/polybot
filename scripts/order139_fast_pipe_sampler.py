#!/usr/bin/env python3
"""Read-only ORDER139 fast-pipe lag and latched freshness sampler."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import statistics
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CAPTURE = ROOT / "data/research/polymarket_activity_ws_capture_vpn_burnin_20260703T180934Z.jsonl"
DEFAULT_GUARD = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/order139_fast_pipe_samples.jsonl"
DEFAULT_STATE = ROOT / "data/research/order139_fast_pipe_sampler_state.json"
DEFAULT_SERIES = ROOT / "data/research/weekend_seat_freshness_series.jsonl"
SELECTED_WALLET = "0x2d7c9298b64713de86402bd8a41695e31865a945"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _append(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True) + "\n")


def capture_lags(path: Path, *, wallet: str, target_n: int, scan_bytes: int) -> list[dict[str, Any]]:
    size = path.stat().st_size
    start = max(0, size - max(1, scan_bytes))
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    with path.open("rb") as handle:
        handle.seek(start)
        if start:
            handle.readline()
        for raw_line in handle:
            try:
                row = json.loads(raw_line)
            except (ValueError, UnicodeDecodeError):
                continue
            if str(row.get("source_wallet") or "").lower() != wallet.lower():
                continue
            if str(row.get("side") or "").upper() != "BUY":
                continue
            event_id = str(row.get("event_id") or "")
            if not event_id or event_id in seen:
                continue
            try:
                event_ts = float(row["event_ts"])
                received_at_s = float(row["received_at_s"])
            except (KeyError, TypeError, ValueError):
                continue
            seen.add(event_id)
            rows.append(
                {
                    "event_id": event_id,
                    "event_ts": event_ts,
                    "received_at_s": received_at_s,
                    "receive_lag_s": round(received_at_s - event_ts, 6),
                    "source_wallet": wallet.lower(),
                    "transaction_hash": row.get("transaction_hash"),
                }
            )
    return rows[-max(1, target_n) :]


def guard_sample(guard: dict[str, Any], prior: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    discriminator = guard.get("freshness_discriminator")
    if not isinstance(discriminator, dict):
        poller = guard.get("active_set_dataapi_poller")
        discriminator = poller.get("freshness_acceptance_discriminator", {}) if isinstance(poller, dict) else {}
    funnel = guard.get("drought_funnel") if isinstance(guard.get("drought_funnel"), dict) else {}
    identity = guard.get("guard_code_identity") if isinstance(guard.get("guard_code_identity"), dict) else {}
    fast_ts = discriminator.get("fast_pipe_latest_observed_ts")
    previous_fast_ts = prior.get("fast_pipe_latest_observed_ts")
    advanced = fast_ts is not None and previous_fast_ts is not None and float(fast_ts) > float(previous_fast_ts)
    fresh_rows = int(discriminator.get("policy_compatible_fresh_buy_rows_le_30s") or 0)
    firing = bool(advanced and fresh_rows == 0)
    generated_at = str(guard.get("generated_at") or "")
    prior_generated_at = str(prior.get("guard_generated_at") or "")
    cycle_period_s = None
    if generated_at and prior_generated_at and generated_at != prior_generated_at:
        current = dt.datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        previous = dt.datetime.fromisoformat(prior_generated_at.replace("Z", "+00:00"))
        cycle_period_s = round((current - previous).total_seconds(), 6)
    cumulative = int(prior.get("cumulative_transport_firings") or 0) + int(firing)
    observed_at = dt.datetime.now(dt.timezone.utc).isoformat()
    sample = {
        "observed_at": observed_at,
        "guard_generated_at": generated_at,
        "guard_cycle_period_s": cycle_period_s,
        "guard_pid": identity.get("pid") or guard.get("pid"),
        "selected_wallet": discriminator.get("selected_wallet"),
        "fast_pipe_latest_observed_ts": fast_ts,
        "fast_pipe_advanced_level": advanced,
        "fresh_rows_le_30s": fresh_rows,
        "transport_firing": firing,
        "cumulative_transport_firings": cumulative,
        "freshest_buy_lag_s": discriminator.get("freshest_buy_lag_s"),
        "discriminator_status": discriminator.get("status"),
        "base_intents": funnel.get("base_intents"),
        "fresh_candidate_intents": funnel.get("fresh_candidate_intents"),
        "orders_submitted": funnel.get("orders_submitted"),
    }
    state = {
        "fast_pipe_latest_observed_ts": fast_ts,
        "guard_generated_at": generated_at,
        "cumulative_transport_firings": cumulative,
    }
    return sample, state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", default=str(DEFAULT_CAPTURE))
    parser.add_argument("--guard", default=str(DEFAULT_GUARD))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--state", default=str(DEFAULT_STATE))
    parser.add_argument("--series", default=str(DEFAULT_SERIES))
    parser.add_argument("--wallet", default=SELECTED_WALLET)
    parser.add_argument("--target-n", type=int, default=10)
    parser.add_argument("--scan-bytes", type=int, default=256 * 1024 * 1024)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--interval-s", type=float, default=5.0)
    args = parser.parse_args()

    lags = capture_lags(Path(args.capture), wallet=args.wallet, target_n=args.target_n, scan_bytes=args.scan_bytes)
    lag_values = [float(row["receive_lag_s"]) for row in lags]
    summary = {
        "flow_stage": "LIVE/DEFEND/MEASURE",
        "kind": "ORDER139_FAST_PIPE_RECEIVE_LAG",
        "n": len(lags),
        "target_n": args.target_n,
        "decision_ready": len(lags) >= args.target_n,
        "min_receive_lag_s": min(lag_values) if lag_values else None,
        "median_receive_lag_s": statistics.median(lag_values) if lag_values else None,
        "max_receive_lag_s": max(lag_values) if lag_values else None,
        "decision": "D1_FAST_PIPE_BAR_SATISFIABLE" if len(lags) >= args.target_n and min(lag_values) <= 30 else ("D2_FAST_PIPE_BAR_UNREACHABLE" if len(lags) >= args.target_n else "COLLECTING"),
        "events": lags,
    }
    _append(Path(args.output), summary)

    state = _read_json(Path(args.state))
    samples: list[dict[str, Any]] = []
    for index in range(max(1, args.samples)):
        sample, state = guard_sample(_read_json(Path(args.guard)), state)
        _append(Path(args.output), {"flow_stage": "LIVE/DEFEND/MEASURE", "kind": "ORDER139_GUARD_SAMPLE", **sample})
        _append(
            Path(args.series),
            {
                "observed_at": sample["observed_at"],
                "selected_wallet": sample["selected_wallet"],
                "freshest_buy_lag_s": sample["freshest_buy_lag_s"],
                "policy_compatible_fresh_buy_rows_le_30s": sample["fresh_rows_le_30s"],
                "fast_pipe_latest_observed_ts": sample["fast_pipe_latest_observed_ts"],
                "fast_pipe_latest_observed_ts_advanced": sample["fast_pipe_advanced_level"],
                "source_had_new_buy_since_last_poll": None,
                "discriminator_status": sample["discriminator_status"],
                "base_intents": sample["base_intents"],
                "fresh_candidate_intents": sample["fresh_candidate_intents"],
                "orders_submitted": sample["orders_submitted"],
                "guard_pid": sample["guard_pid"],
                "guard_cycle_period_s": sample["guard_cycle_period_s"],
                "cumulative_transport_firings": sample["cumulative_transport_firings"],
            },
        )
        samples.append(sample)
        Path(args.state).write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        if index + 1 < max(1, args.samples):
            time.sleep(min(10.0, max(0.0, args.interval_s)))
    print(json.dumps({"receive_lag": summary, "last_guard_sample": samples[-1]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
