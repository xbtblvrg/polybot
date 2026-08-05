#!/usr/bin/env python3
"""Continuous wallet-market mining cadence.

Flow stage: DISCOVER/LEARN/PROMOTE. This is paper/research only. It turns the
market-cohort mine from a one-shot replay into a bounded, brainless cadence:
intake, replay, liveness, packet rebuild, and a null-cycle answer when no
better live-ready supply appears.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_STATE = "data/research/wallet_market_mining_cadence_state.json"
DEFAULT_SOURCE_ACTIVE_BATCH = "data/research/wallet_market_mining_source_active_liveness_batch_latest.json"
DEFAULT_SOURCE_ACTIVE_COHORT = "data/research/wallet_market_mining_source_active_liveness_cohort_latest.json"
DEFAULT_SOURCE_ACTIVE_ORDERING = "data/research/wallet_market_mining_source_active_liveness_replay_ordering_latest.json"
DEFAULT_ADMISSION_PACKETS = "data/research/wallet_market_mining_cohort_alive_admission_packets_latest.json"


@dataclass(frozen=True)
class StepSpec:
    name: str
    command: list[str]
    interval_min: float
    timeout_s: float


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _parse_active_hours(spec: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for raw_part in str(spec or "").split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" in part:
            raw_start, raw_end = part.split("-", 1)
        else:
            raw_start, raw_end = part, part
        try:
            start = int(raw_start)
            end = int(raw_end)
        except ValueError:
            continue
        if 0 <= start <= 23 and 0 <= end <= 24:
            ranges.append((start, end))
    return ranges or [(6, 23)]


def _hour_in_ranges(hour: int, ranges: list[tuple[int, int]]) -> bool:
    for start, end in ranges:
        if start == end:
            if hour == start:
                return True
        elif start < end:
            if start <= hour < end:
                return True
        elif hour >= start or hour < end:
            return True
    return False


def _last_success(prior: dict[str, Any], step_name: str) -> datetime | None:
    steps = prior.get("steps") if isinstance(prior.get("steps"), dict) else {}
    step = steps.get(step_name) if isinstance(steps.get(step_name), dict) else {}
    return _parse_iso(step.get("last_success_at"))


def _is_due(
    prior: dict[str, Any],
    step_name: str,
    *,
    now: datetime,
    interval_min: float,
    force: bool,
) -> tuple[bool, float | None]:
    if force:
        return True, None
    last = _last_success(prior, step_name)
    if last is None:
        return True, None
    age_min = max(0.0, (now - last).total_seconds() / 60.0)
    return age_min >= float(interval_min), round(age_min, 3)


def _script(root: Path, name: str) -> str:
    return str(root / "scripts" / name)


def _current_scope(prior: dict[str, Any], args: argparse.Namespace) -> dict[str, int]:
    next_scope = prior.get("next_scope") if isinstance(prior.get("next_scope"), dict) else {}
    return {
        "intake_max_pages": max(
            int(args.intake_max_pages),
            min(int(args.intake_max_pages_max), _as_int(next_scope.get("intake_max_pages"), int(args.intake_max_pages))),
        ),
        "replay_wallet_limit": max(
            int(args.replay_wallet_limit),
            min(
                int(args.replay_wallet_limit_max),
                _as_int(next_scope.get("replay_wallet_limit"), int(args.replay_wallet_limit)),
            ),
        ),
        "external_liveness_ranked_limit": max(
            int(args.external_liveness_ranked_limit),
            min(
                int(args.external_liveness_ranked_limit_max),
                _as_int(next_scope.get("external_liveness_ranked_limit"), int(args.external_liveness_ranked_limit)),
            ),
        ),
    }


def _widen_scope(scope: dict[str, int], args: argparse.Namespace) -> dict[str, int]:
    return {
        "intake_max_pages": min(
            int(args.intake_max_pages_max),
            int(scope.get("intake_max_pages") or int(args.intake_max_pages)) + int(args.scope_widen_intake_pages),
        ),
        "replay_wallet_limit": min(
            int(args.replay_wallet_limit_max),
            int(scope.get("replay_wallet_limit") or int(args.replay_wallet_limit)) + int(args.scope_widen_wallets),
        ),
        "external_liveness_ranked_limit": min(
            int(args.external_liveness_ranked_limit_max),
            int(scope.get("external_liveness_ranked_limit") or int(args.external_liveness_ranked_limit))
            + int(args.scope_widen_wallets),
        ),
    }


def _build_steps(root: Path, args: argparse.Namespace, *, scope: dict[str, int], intake_interval_min: float) -> list[StepSpec]:
    python = sys.executable
    batch_id = datetime.now(timezone.utc).strftime("mining_%Y%m%dT%H%MZ")
    return [
        StepSpec(
            name="intake",
            interval_min=float(intake_interval_min),
            timeout_s=float(args.intake_timeout_s),
            command=[
                python,
                _script(root, "build_wallet_market_scan_intake.py"),
                "--include-leaderboard",
                "--max-pages",
                str(scope["intake_max_pages"]),
                "--max-wall-runtime-s",
                str(args.intake_max_wall_runtime_s),
                "--leaderboard-pages",
                str(args.intake_leaderboard_pages),
            ],
        ),
        StepSpec(
            name="replay",
            interval_min=float(args.replay_interval_min),
            timeout_s=float(args.replay_timeout_s),
            command=[
                python,
                _script(root, "build_wallet_market_cohort_replay.py"),
                "--wallet-limit",
                str(scope["replay_wallet_limit"]),
                "--max-wall-runtime-s",
                str(args.replay_max_wall_runtime_s),
            ],
        ),
        StepSpec(
            name="external_liveness",
            interval_min=float(args.external_liveness_interval_min),
            timeout_s=float(args.external_liveness_timeout_s),
            command=[
                python,
                _script(root, "probe_queue_remote_dataapi_fresh_flow.py"),
                "--clearance-limit",
                "0",
                "--ranked-limit",
                str(scope["external_liveness_ranked_limit"]),
                "--cohort-replay",
                "data/research/wallet_market_cohort_replay_latest.json",
                "--cohort-limit",
                str(scope["external_liveness_ranked_limit"]),
                "--no-default-include-wallet",
                "--pages",
                "1",
                "--limit",
                "50",
                "--timeout-s",
                str(args.external_liveness_request_timeout_s),
            ],
        ),
        StepSpec(
            name="source_active_liveness",
            interval_min=float(args.source_active_interval_min),
            timeout_s=float(args.source_active_timeout_s),
            command=[
                python,
                _script(root, "probe_cohort_source_active_liveness.py"),
                "--wallet-limit",
                str(args.source_active_wallet_limit),
                "--batch-id",
                batch_id,
                "--latest-name",
                Path(DEFAULT_SOURCE_ACTIVE_BATCH).name,
                "--ordering-latest-name",
                Path(DEFAULT_SOURCE_ACTIVE_ORDERING).name,
                "--tail-bytes",
                str(args.source_active_tail_bytes),
            ],
        ),
        StepSpec(
            name="admission_packets",
            interval_min=float(args.packet_interval_min),
            timeout_s=float(args.packet_timeout_s),
            command=[
                python,
                _script(root, "report_cohort_alive_admission_packets.py"),
                "--require-source-active-policy",
                "--source-active-cohort",
                DEFAULT_SOURCE_ACTIVE_COHORT,
                "--output",
                DEFAULT_ADMISSION_PACKETS,
                "--packet-limit",
                str(args.packet_limit),
            ],
        ),
    ]


def _run_step(root: Path, spec: StepSpec, *, skip_execution: bool) -> dict[str, Any]:
    out_path = root / "data" / "research" / f"wallet_market_mining_{spec.name}.out"
    err_path = root / "data" / "research" / f"wallet_market_mining_{spec.name}.err"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if skip_execution:
        out_path.write_text("", encoding="utf-8")
        err_path.write_text("skip_execution=true\n", encoding="utf-8")
        return {
            "name": spec.name,
            "status": "DRY_RUN",
            "returncode": None,
            "command": spec.command,
            "out_path": str(out_path.relative_to(root)),
            "err_path": str(err_path.relative_to(root)),
        }
    try:
        proc = subprocess.run(
            spec.command,
            cwd=root,
            text=True,
            capture_output=True,
            timeout=float(spec.timeout_s),
        )
        out_path.write_text(proc.stdout, encoding="utf-8")
        err_path.write_text(proc.stderr, encoding="utf-8")
        status = "PASS" if proc.returncode == 0 else "ERROR"
        return {
            "name": spec.name,
            "status": status,
            "returncode": proc.returncode,
            "command": spec.command,
            "out_path": str(out_path.relative_to(root)),
            "err_path": str(err_path.relative_to(root)),
            "stdout_tail": _tail(proc.stdout),
            "stderr_tail": _tail(proc.stderr),
        }
    except subprocess.TimeoutExpired as exc:
        out_path.write_text(exc.stdout or "", encoding="utf-8")
        err_path.write_text((exc.stderr or "") + "\ntimeout_expired=true\n", encoding="utf-8")
        return {
            "name": spec.name,
            "status": "TIMEOUT",
            "returncode": None,
            "command": spec.command,
            "out_path": str(out_path.relative_to(root)),
            "err_path": str(err_path.relative_to(root)),
            "timeout_s": spec.timeout_s,
        }


def _copy_source_active_alias(root: Path) -> dict[str, Any]:
    src = root / DEFAULT_SOURCE_ACTIVE_BATCH
    dst = root / DEFAULT_SOURCE_ACTIVE_COHORT
    if not src.exists():
        return {"status": "MISSING_BATCH", "source": str(src.relative_to(root)), "alias": str(dst.relative_to(root))}
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    return {"status": "PASS", "source": str(src.relative_to(root)), "alias": str(dst.relative_to(root))}


def _observed_counts(root: Path) -> dict[str, Any]:
    intake_payload = _load_json(root / "data/research/wallet_market_scan_ranked.json", {})
    replay_payload = _load_json(root / "data/research/wallet_market_cohort_replay_latest.json", {})
    packet_payload = _load_json(root / DEFAULT_ADMISSION_PACKETS, {})
    external = _load_json(root / "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json", {})
    source_active = _load_json(root / DEFAULT_SOURCE_ACTIVE_COHORT, {})
    intake_summary = intake_payload.get("summary") if isinstance(intake_payload.get("summary"), dict) else {}
    replay_summary = replay_payload.get("summary") if isinstance(replay_payload.get("summary"), dict) else {}
    packet_summary = packet_payload.get("summary") if isinstance(packet_payload.get("summary"), dict) else {}
    external_summary = external.get("summary") if isinstance(external.get("summary"), dict) else {}
    source_active_reports = (
        source_active.get("reports") if isinstance(source_active.get("reports"), list) else []
    )
    return {
        "intake_generated_at": intake_payload.get("generated_at"),
        "intake_active_wallets": _as_int(intake_summary.get("active_wallets")),
        "intake_new_active_wallets": _as_int(intake_summary.get("new_active_wallets")),
        "intake_wallets_ranked": _as_int(intake_summary.get("wallets_ranked")),
        "replay_generated_at": replay_payload.get("generated_at"),
        "cohort_size": _as_int(replay_summary.get("cohort_size")),
        "batch_wallets": _as_int(replay_summary.get("batch_wallets")),
        "shadow_positive": _as_int(replay_summary.get("cohort_shadow_positive")),
        "live_ready_picks": _as_int(replay_summary.get("live_ready_picks")),
        "top_live_ready_wallet": str(replay_summary.get("top_live_ready_wallet") or ""),
        "packet_generated_at": packet_payload.get("generated_at"),
        "packet_count": _as_int(packet_summary.get("packet_count")),
        "four_way_admission_ready": _as_int(packet_summary.get("four_way_admission_ready")),
        "top_packet_wallet": str(packet_summary.get("top_wallet") or ""),
        "external_liveness_generated_at": external.get("generated_at"),
        "external_liveness_wallets": _as_int(external_summary.get("cumulative_wallets") or external_summary.get("wallets")),
        "external_liveness_pass": _as_int(
            external_summary.get("cumulative_pass_admission_threshold")
            or external_summary.get("pass_admission_threshold")
        ),
        "source_active_generated_at": source_active.get("generated_at"),
        "source_active_wallets": _as_int(source_active.get("wallet_count"), len(source_active_reports)),
        "source_active_policy_pass": _as_int(source_active.get("policy_eligible_pass")),
    }


def _improvement(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    if _as_int(current.get("live_ready_picks")) > _as_int(previous.get("live_ready_picks")):
        reasons.append("live_ready_picks_increased")
    if _as_int(current.get("shadow_positive")) > _as_int(previous.get("shadow_positive")):
        reasons.append("shadow_positive_increased")
    if _as_int(current.get("packet_count")) > _as_int(previous.get("packet_count")):
        reasons.append("admission_packet_count_increased")
    if current.get("top_live_ready_wallet") and current.get("top_live_ready_wallet") != previous.get("top_live_ready_wallet"):
        reasons.append("top_live_ready_wallet_changed")
    if current.get("top_packet_wallet") and current.get("top_packet_wallet") != previous.get("top_packet_wallet"):
        reasons.append("top_packet_wallet_changed")
    return {"improved": bool(reasons), "reasons": reasons}


def build_cadence(root: Path, args: argparse.Namespace, *, now: datetime | None = None) -> dict[str, Any]:
    now_dt = now or _utc_now()
    state_path = root / args.state
    prior = _load_json(state_path, {})
    prior = prior if isinstance(prior, dict) else {}
    observed_before = prior.get("observed") if isinstance(prior.get("observed"), dict) else {}
    active_ranges = _parse_active_hours(args.active_hours_utc)
    active_hours_now = _hour_in_ranges(now_dt.hour, active_ranges)
    intake_interval = float(args.intake_active_interval_min if active_hours_now else args.intake_quiet_interval_min)
    scope = _current_scope(prior, args)
    steps = _build_steps(root, args, scope=scope, intake_interval_min=intake_interval)
    step_status: dict[str, dict[str, Any]] = {}
    due_steps: list[str] = []
    ran_steps: list[str] = []
    errors: list[dict[str, Any]] = []

    if bool(args.resource_paused):
        observed = _observed_counts(root)
        payload = {
            "schema_version": 1,
            "kind": "wallet_market_mining_cadence",
            "flow_stage": "DISCOVER/LEARN/PROMOTE",
            "status": "RESOURCE_PAUSED",
            "generated_at": _iso(now_dt),
            "paper_only": True,
            "live_orders_allowed": False,
            "live_path_mutated": False,
            "launchd": {
                "service": "com.belavarga.polymarket.brainless-ops",
                "start_interval_s": 600,
                "cadence_owner": "scripts/brainless_ops.sh",
            },
            "observed": observed,
            "next_scope": scope,
            "null_cycle": {
                "status": "NOT_A_MINING_CYCLE_RESOURCE_PAUSED",
                "answer": "resource pressure preserved live/guard floors; mining cadence will resume on the next allowed brainless pulse",
            },
            "next_action": "rerun cadence when memory/resource floor allows background discovery",
        }
        atomic_write_json(state_path, payload)
        return payload

    for spec in steps:
        due, age_min = _is_due(prior, spec.name, now=now_dt, interval_min=spec.interval_min, force=bool(args.force))
        step_status[spec.name] = {
            "name": spec.name,
            "due": due,
            "age_min": age_min,
            "interval_min": spec.interval_min,
            "last_success_at": (_last_success(prior, spec.name) or None).isoformat().replace("+00:00", "Z")
            if _last_success(prior, spec.name)
            else None,
            "status": "NOT_DUE",
        }
        if not due:
            continue
        due_steps.append(spec.name)
        result = _run_step(root, spec, skip_execution=bool(args.skip_execution))
        step_status[spec.name].update(result)
        ran_steps.append(spec.name)
        if result.get("status") == "PASS":
            step_status[spec.name]["last_success_at"] = _iso(now_dt)
            if spec.name == "source_active_liveness":
                step_status[spec.name]["cohort_alias"] = _copy_source_active_alias(root)
        elif result.get("status") == "DRY_RUN":
            pass
        else:
            errors.append({"step": spec.name, "status": result.get("status"), "returncode": result.get("returncode")})

    observed = _observed_counts(root)
    delta = {
        key: _as_int(observed.get(key)) - _as_int(observed_before.get(key))
        for key in (
            "intake_active_wallets",
            "intake_new_active_wallets",
            "cohort_size",
            "batch_wallets",
            "shadow_positive",
            "live_ready_picks",
            "packet_count",
            "four_way_admission_ready",
            "external_liveness_pass",
            "source_active_policy_pass",
        )
    }
    improvement = _improvement(observed_before, observed)
    ran_real_steps = [name for name in ran_steps if step_status.get(name, {}).get("status") != "DRY_RUN"]
    next_scope = dict(scope)
    if args.skip_execution:
        status = "DRY_RUN_PLANNED"
        null_cycle_status = "DRY_RUN_NO_MINING_EXECUTED"
        answer = f"would run {due_steps}; no mining execution requested"
    elif errors:
        status = "DEGRADED_ERRORS"
        null_cycle_status = "ERRORS_BEFORE_NULL_CYCLE"
        answer = f"mining cycle attempted {ran_steps}; errors={errors}"
    elif not due_steps:
        status = "NO_STEPS_DUE"
        null_cycle_status = "NOT_DUE"
        answer = "no cadence step due on this 10-minute brainless pulse"
    elif improvement["improved"]:
        status = "IMPROVEMENT_FOUND"
        null_cycle_status = "NOT_NULL_IMPROVEMENT_FOUND"
        answer = f"mining cycle found improvement: {','.join(improvement['reasons'])}"
    else:
        status = "NULL_CYCLE_SHIPPED_SCOPE_WIDENED"
        null_cycle_status = "SHIPPED"
        next_scope = _widen_scope(scope, args)
        answer = (
            f"scanned/replayed batch_wallets={observed.get('batch_wallets')} "
            f"cohort={observed.get('cohort_size')} live_ready={observed.get('live_ready_picks')} "
            f"packets={observed.get('packet_count')}; none beat incumbents; next scope widens to {next_scope}"
        )
    payload = {
        "schema_version": 1,
        "kind": "wallet_market_mining_cadence",
        "flow_stage": "DISCOVER/LEARN/PROMOTE",
        "status": status,
        "generated_at": _iso(now_dt),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "launchd": {
            "service": "com.belavarga.polymarket.brainless-ops",
            "start_interval_s": 600,
            "cadence_owner": "scripts/brainless_ops.sh",
        },
        "cadence_policy": {
            "intake_interval_min": intake_interval,
            "intake_active_hours_utc": args.active_hours_utc,
            "active_hours_now": active_hours_now,
            "replay_interval_min": float(args.replay_interval_min),
            "external_liveness_interval_min": float(args.external_liveness_interval_min),
            "source_active_interval_min": float(args.source_active_interval_min),
            "packet_interval_min": float(args.packet_interval_min),
            "null_cycle_rule": "every executed mining cycle ships an answer; no-improvement cycles widen the next scope",
        },
        "scope_used": scope,
        "next_scope": next_scope,
        "due_steps": due_steps,
        "ran_steps": ran_steps,
        "ran_real_steps": ran_real_steps,
        "steps": step_status,
        "errors": errors,
        "observed": observed,
        "delta_vs_prior_observed": delta,
        "improvement": improvement,
        "null_cycle": {
            "status": null_cycle_status,
            "answer": answer,
            "scope_widened": next_scope != scope,
            "none_beat_incumbents": bool(due_steps and not errors and not improvement["improved"] and not args.skip_execution),
        },
        "commitment_rows": [
            {
                "id": "continuous_mining_intake_loop",
                "flow_stage": "DISCOVER/LEARN",
                "status": "WIRED",
                "evidence": "scripts/run_wallet_market_mining_cadence.py:intake + scripts/brainless_ops.sh:market_mining_cadence",
            },
            {
                "id": "continuous_mining_replay_loop",
                "flow_stage": "DISCOVER/LEARN/PROMOTE",
                "status": "WIRED",
                "evidence": "scripts/run_wallet_market_mining_cadence.py:replay",
            },
            {
                "id": "continuous_mining_liveness_loop",
                "flow_stage": "PROMOTE/DEFEND",
                "status": "WIRED",
                "evidence": "scripts/run_wallet_market_mining_cadence.py:external_liveness+source_active_liveness",
            },
            {
                "id": "continuous_mining_null_cycle_rule",
                "flow_stage": "DISCOVER/LEARN",
                "status": "WIRED",
                "evidence": "data/research/wallet_market_mining_cadence_state.json:null_cycle",
            },
        ],
        "artifacts": {
            "intake": "data/research/wallet_market_scan_ranked.json",
            "cohort_replay": "data/research/wallet_market_cohort_replay_latest.json",
            "external_liveness": "data/research/queue_remote_dataapi_fresh_flow_probe_latest.json",
            "source_active_cohort": DEFAULT_SOURCE_ACTIVE_COHORT,
            "source_active_batch": DEFAULT_SOURCE_ACTIVE_BATCH,
            "source_active_replay_ordering": DEFAULT_SOURCE_ACTIVE_ORDERING,
            "admission_packets": DEFAULT_ADMISSION_PACKETS,
        },
        "next_action": (
            "inspect failed step stderr and rerun cadence"
            if errors
            else "continue brainless mining cadence; apply next_scope on the next null cycle"
        ),
    }
    atomic_write_json(state_path, payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-execution", action="store_true")
    parser.add_argument("--resource-paused", action="store_true")
    parser.add_argument("--active-hours-utc", default="6-23")
    parser.add_argument("--intake-active-interval-min", type=float, default=30.0)
    parser.add_argument("--intake-quiet-interval-min", type=float, default=60.0)
    parser.add_argument("--intake-max-pages", type=int, default=20)
    parser.add_argument("--intake-max-pages-max", type=int, default=120)
    parser.add_argument("--intake-max-wall-runtime-s", type=float, default=150.0)
    parser.add_argument("--intake-timeout-s", type=float, default=210.0)
    parser.add_argument("--intake-leaderboard-pages", type=int, default=2)
    parser.add_argument("--replay-interval-min", type=float, default=30.0)
    parser.add_argument("--replay-wallet-limit", type=int, default=10)
    parser.add_argument("--replay-wallet-limit-max", type=int, default=80)
    parser.add_argument("--replay-max-wall-runtime-s", type=float, default=210.0)
    parser.add_argument("--replay-timeout-s", type=float, default=270.0)
    parser.add_argument("--external-liveness-interval-min", type=float, default=10.0)
    parser.add_argument("--external-liveness-ranked-limit", type=int, default=25)
    parser.add_argument("--external-liveness-ranked-limit-max", type=int, default=150)
    parser.add_argument("--external-liveness-pages", type=int, default=2)
    parser.add_argument("--external-liveness-request-timeout-s", type=float, default=8.0)
    parser.add_argument("--external-liveness-timeout-s", type=float, default=240.0)
    parser.add_argument("--source-active-interval-min", type=float, default=360.0)
    parser.add_argument("--source-active-wallet-limit", type=int, default=100)
    parser.add_argument("--source-active-tail-bytes", type=int, default=384_000_000)
    parser.add_argument("--source-active-timeout-s", type=float, default=240.0)
    parser.add_argument("--packet-interval-min", type=float, default=30.0)
    parser.add_argument("--packet-limit", type=int, default=25)
    parser.add_argument("--packet-timeout-s", type=float, default=90.0)
    parser.add_argument("--scope-widen-wallets", type=int, default=5)
    parser.add_argument("--scope-widen-intake-pages", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    payload = build_cadence(root, args)
    print(
        json.dumps(
            {
                "status": payload.get("status"),
                "due_steps": payload.get("due_steps"),
                "ran_steps": payload.get("ran_steps"),
                "observed": payload.get("observed"),
                "null_cycle": payload.get("null_cycle"),
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("status") not in {"DEGRADED_ERRORS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
