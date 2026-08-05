#!/usr/bin/env python3
"""Build a report-only compute and lane utilization meter.

Flow stage: SELF-DEV/LEARN. This is a reading aid for the standing
capacity-efficiency order; it never mutates live guard, roster, policy, or
eligibility state.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402


DEFAULT_STRATEGY_MAP = "data/research/strategy_map_latest.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/resource_utilization_latest.json"
DEFAULT_MARKDOWN_OUTPUT = "data/research/resource_utilization_latest.md"
DEFAULT_LANE_MEMORY_BUDGET_GB = 1.5
DEFAULT_MIN_IDLE_CPU_HEADROOM_PCT = 25.0
DEFAULT_MIN_IDLE_MEMORY_HEADROOM_PCT = 20.0
NORTHSTAR_DAY_PNL_USD = 100.0


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _load_json(path: str | Path, default: Any) -> Any:
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _cpu_snapshot() -> dict[str, Any]:
    cpu_count = max(1, int(os.cpu_count() or 1))
    try:
        load1, load5, load15 = os.getloadavg()
    except OSError:
        return {"status": "UNKNOWN", "cpu_count": cpu_count}
    used_pct = min(100.0, max(0.0, float(load1) / float(cpu_count) * 100.0))
    return {
        "status": "OK",
        "cpu_count": cpu_count,
        "load1": round(float(load1), 6),
        "load5": round(float(load5), 6),
        "load15": round(float(load15), 6),
        "used_pct": round(used_pct, 6),
        "headroom_pct": round(max(0.0, 100.0 - used_pct), 6),
    }


def _parse_meminfo(text: str) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, rest = line.split(":", 1)
        parts = rest.strip().split()
        if not parts:
            continue
        try:
            values[key] = int(parts[0]) * 1024
        except ValueError:
            continue
    return values


def _linux_memory_snapshot() -> dict[str, Any]:
    path = Path("/proc/meminfo")
    if not path.exists():
        return {"status": "UNAVAILABLE"}
    values = _parse_meminfo(path.read_text(encoding="utf-8", errors="ignore"))
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", values.get("MemFree", 0))
    if total <= 0:
        return {"status": "UNAVAILABLE"}
    return _memory_payload(total_bytes=total, available_bytes=available, source="proc_meminfo")


def _darwin_memory_snapshot() -> dict[str, Any]:
    try:
        total = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
        vm_stat = subprocess.check_output(["vm_stat"], text=True)
    except Exception:
        return {"status": "UNAVAILABLE"}
    page_size = 4096
    pages: dict[str, int] = {}
    for line in vm_stat.splitlines():
        if "page size of" in line:
            try:
                page_size = int(line.split("page size of", 1)[1].split("bytes", 1)[0].strip())
            except (IndexError, ValueError):
                pass
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        try:
            pages[key.strip()] = int(value.strip().rstrip("."))
        except ValueError:
            continue
    available_pages = (
        pages.get("Pages free", 0)
        + pages.get("Pages inactive", 0)
        + pages.get("Pages speculative", 0)
    )
    return _memory_payload(total_bytes=total, available_bytes=available_pages * page_size, source="vm_stat")


def _memory_payload(*, total_bytes: int, available_bytes: int, source: str) -> dict[str, Any]:
    total = max(0, int(total_bytes))
    available = max(0, min(int(available_bytes), total))
    used = max(0, total - available)
    headroom_pct = (available / total * 100.0) if total > 0 else 0.0
    return {
        "status": "OK" if total > 0 else "UNAVAILABLE",
        "source": source,
        "total_gb": round(total / 1_073_741_824.0, 6),
        "available_gb": round(available / 1_073_741_824.0, 6),
        "used_gb": round(used / 1_073_741_824.0, 6),
        "headroom_pct": round(headroom_pct, 6),
    }


def _memory_snapshot() -> dict[str, Any]:
    if sys.platform == "darwin":
        payload = _darwin_memory_snapshot()
        if payload.get("status") == "OK":
            return payload
    return _linux_memory_snapshot()


def _launchd_pid(label: str) -> int | None:
    if not label or sys.platform != "darwin":
        return None
    try:
        output = subprocess.check_output(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return None
    for line in output.splitlines():
        if line.strip().startswith("pid ="):
            try:
                return int(line.split("=", 1)[1].strip())
            except ValueError:
                return None
    return None


def _process_usage(pid: int) -> dict[str, float] | None:
    try:
        raw = subprocess.check_output(
            ["ps", "-o", "%cpu=,rss=", "-p", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        cpu_text, rss_text = raw.split()[:2]
        return {"cpu_pct": float(cpu_text), "rss_mb": float(rss_text) / 1024.0}
    except Exception:
        return None


def _parse_ts(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _artifact_generated_at(path: Path) -> datetime | None:
    payload = _load_json(path, {})
    candidates: list[datetime] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key).lower() in {
                    "generated_at",
                    "updated_at",
                    "recorded_at",
                    "as_of",
                    "timestamp",
                }:
                    parsed = _parse_ts(child)
                    if parsed is not None:
                        candidates.append(parsed)
                elif isinstance(child, (dict, list)):
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return max(candidates) if candidates else None


def _strategy_lane_summary(
    strategy_map: dict[str, Any],
    *,
    now: datetime | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=UTC)
    root = root or ROOT
    rows = strategy_map.get("rows") if isinstance(strategy_map.get("rows"), list) else []
    active_statuses = {"LIVE", "PAPER", "GATED"}
    active_rows = [
        row for row in rows if isinstance(row, dict) and str(row.get("status") or "").upper() in active_statuses
    ]
    by_status: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "UNKNOWN").upper()
        by_status[status] = by_status.get(status, 0) + 1
    bound: dict[str, dict[str, Any]] = {}
    stale_bound_rows = 0
    unwired_rows = 0
    for row in active_rows:
        binding = row.get("runner_binding")
        binding = binding if isinstance(binding, dict) else {}
        label = str(binding.get("label") or "")
        if not label:
            unwired_rows += 1
            continue
        evidence_artifact = str(
            binding.get("evidence_artifact")
            or binding.get("productive_cell_artifact")
            or ""
        )
        artifact_path = Path(evidence_artifact)
        artifact_path = artifact_path if artifact_path.is_absolute() else root / artifact_path
        generated = _artifact_generated_at(artifact_path) if evidence_artifact else None
        slo_s = max(1.0, _num(binding.get("freshness_slo_s"), 300.0))
        fresh = generated is not None and (now - generated).total_seconds() <= slo_s
        if not fresh:
            stale_bound_rows += 1
            continue
        if label in bound:
            continue
        pid = _launchd_pid(label)
        usage = _process_usage(pid) if pid else None
        if pid and usage:
            lane_count = 1
            cell_artifact = str(binding.get("productive_cell_artifact") or "")
            cell_field = str(binding.get("productive_cell_field") or "")
            if cell_artifact and cell_field:
                cell_state = _load_json(cell_artifact, {})
                lane_count = max(1, int(_num(cell_state.get(cell_field), 1.0)))
            bound[label] = {
                "pid": pid,
                "productive_lane_count": lane_count,
                "evidence_artifact": evidence_artifact,
                "evidence_generated_at": generated.isoformat()
                if generated is not None
                else None,
                "freshness_slo_s": slo_s,
                **usage,
            }
    return {
        "strategy_rows": len(rows),
        "registry_active_or_gated_count": len(active_rows),
        "registry_status_occupancy_pct": round(
            len(active_rows) / float(len(rows)) * 100.0, 6
        )
        if rows
        else 0.0,
        "productive_lane_count": sum(
            int(row.get("productive_lane_count") or 1) for row in bound.values()
        ),
        "productive_services": bound,
        "unwired_active_or_gated_rows": unwired_rows,
        "stale_bound_rows": stale_bound_rows,
        "status_counts": dict(sorted(by_status.items())),
        "active_statuses": sorted(active_statuses),
    }


def _scorecard_goal(scorecard: dict[str, Any]) -> dict[str, Any]:
    today = scorecard.get("today") if isinstance(scorecard.get("today"), dict) else {}
    total = today.get("total") if isinstance(today.get("total"), dict) else {}
    basis = scorecard.get("day_pnl_basis") if isinstance(scorecard.get("day_pnl_basis"), dict) else {}
    volume = scorecard.get("volume_kpi") if isinstance(scorecard.get("volume_kpi"), dict) else {}
    canonical = volume.get("canonical_daily") if isinstance(volume.get("canonical_daily"), dict) else {}
    pnl = _num(basis.get("day_pnl_response_basis"), _num(total.get("pnl_usd"), 0.0))
    windows_filled = int(_num(canonical.get("windows_filled"), 0.0))
    denominator = int(_num(canonical.get("denominator_windows"), 288.0)) or 288
    return {
        "day_pnl_usd": round(pnl, 6),
        "target_day_pnl_usd": NORTHSTAR_DAY_PNL_USD,
        "windows_filled": windows_filled,
        "denominator_windows": denominator,
        "goal_unmet": bool(pnl < NORTHSTAR_DAY_PNL_USD or windows_filled < denominator),
    }


def build_meter(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    strategy_path = Path(args.strategy_map)
    strategy_path = strategy_path if strategy_path.is_absolute() else root / strategy_path
    scorecard_path = Path(args.scorecard)
    scorecard_path = scorecard_path if scorecard_path.is_absolute() else root / scorecard_path
    strategy_map = _load_json(strategy_path, {})
    strategy_map = strategy_map if isinstance(strategy_map, dict) else {}
    scorecard = load_fresh_scorecard(scorecard_path)
    output_path = Path(args.output)
    output_path = output_path if output_path.is_absolute() else root / output_path
    cpu = _cpu_snapshot()
    memory = _memory_snapshot()
    lanes = _strategy_lane_summary(strategy_map, root=root)
    goal = _scorecard_goal(scorecard)

    lane_budget = max(0.1, float(args.lane_memory_budget_gb))
    available_gb = _num(memory.get("available_gb"), 0.0)
    productive_lanes = int(lanes.get("productive_lane_count") or 0)
    services = lanes.get("productive_services")
    services = services if isinstance(services, dict) else {}
    total_rss_mb = sum(_num(row.get("rss_mb")) for row in services.values())
    total_process_cpu_pct = sum(_num(row.get("cpu_pct")) for row in services.values())
    avg_rss_gb = total_rss_mb / productive_lanes / 1024.0 if productive_lanes else None
    avg_process_cpu_pct = total_process_cpu_pct / productive_lanes if productive_lanes else None
    additional_lanes_by_memory = (
        int(available_gb // avg_rss_gb)
        if memory.get("status") == "OK" and avg_rss_gb and avg_rss_gb > 0
        else None
    )
    cpu_headroom = _num(cpu.get("headroom_pct"), -1.0)
    memory_headroom = _num(memory.get("headroom_pct"), -1.0)
    additional_lanes_by_cpu = (
        int(cpu_headroom // avg_process_cpu_pct)
        if cpu.get("status") == "OK" and avg_process_cpu_pct and avg_process_cpu_pct > 0
        else None
    )
    additions = [
        value for value in (additional_lanes_by_memory, additional_lanes_by_cpu) if value is not None
    ]
    additional_capacity = min(additions) if additions else None
    measured_max_lanes = (
        productive_lanes + additional_capacity if additional_capacity is not None else None
    )
    idle_lane_capacity = (
        max(0, int(measured_max_lanes) - productive_lanes)
        if measured_max_lanes is not None
        else None
    )
    utilization_pct = (
        round(productive_lanes / float(measured_max_lanes) * 100.0, 6)
        if measured_max_lanes and measured_max_lanes > 0
        else None
    )
    has_idle_capacity = (
        goal.get("goal_unmet") is True
        and cpu.get("status") == "OK"
        and memory.get("status") == "OK"
        and productive_lanes > 0
        and (idle_lane_capacity or 0) > 0
    )
    if cpu.get("status") != "OK" or memory.get("status") != "OK":
        verdict = "UNKNOWN_CAPACITY"
    elif productive_lanes == 0:
        verdict = "INSUFFICIENT_PRODUCTIVE_BINDINGS"
    elif has_idle_capacity:
        verdict = "IDLE_CAPACITY"
    else:
        verdict = "CAPACITY_CONSTRAINED_OR_GOAL_MET"
    defect = {
        "open": verdict in {"IDLE_CAPACITY", "INSUFFICIENT_PRODUCTIVE_BINDINGS"},
        "id": (
            "idle_capacity_while_goal_unmet"
            if verdict == "IDLE_CAPACITY"
            else "no_process_backed_productive_lane_evidence"
            if verdict == "INSUFFICIENT_PRODUCTIVE_BINDINGS"
            else ""
        ),
        "next": (
            "convert idle capacity into paper lanes, broader polling, or faster evidence refresh; no live gate change without Fable"
            if verdict == "IDLE_CAPACITY"
            else "wire running services and fresh artifacts before making a productive-utilization claim"
            if verdict == "INSUFFICIENT_PRODUCTIVE_BINDINGS"
            else ""
        ),
    }

    return {
        "kind": "utilization_meter",
        "flow_stage": "SELF-DEV/LEARN",
        "generated_at": _utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "live_path_mutated": False,
        "authority": "REPORT_ONLY_NO_LIVE_ELIGIBILITY_CHANGE",
        "strategy_map": str(strategy_path.relative_to(root)) if strategy_path.is_relative_to(root) else str(strategy_path),
        "scorecard": str(scorecard_path.relative_to(root)) if scorecard_path.is_relative_to(root) else str(scorecard_path),
        "cpu": cpu,
        "memory": memory,
        "memory_pressure": {
            "headroom_to_pause_pct": memory_headroom,
            "source": memory.get("source"),
        },
        "lanes": {
            **lanes,
            "lane_memory_budget_gb": lane_budget,
            "measured_total_process_rss_mb": round(total_rss_mb, 6),
            "measured_total_process_cpu_pct": round(total_process_cpu_pct, 6),
            "measured_average_lane_rss_gb": round(avg_rss_gb, 6) if avg_rss_gb else None,
            "measured_average_lane_cpu_pct": round(avg_process_cpu_pct, 6)
            if avg_process_cpu_pct
            else None,
            "additional_lanes_by_memory": additional_lanes_by_memory,
            "additional_lanes_by_cpu": additional_lanes_by_cpu,
            "measured_max_lane_count": measured_max_lanes,
        },
        "registry_active_or_gated_lane_count": lanes["registry_active_or_gated_count"],
        "registry_status_occupancy_pct": lanes["registry_status_occupancy_pct"],
        "productive_lane_count": productive_lanes,
        "measured_max_lane_count": measured_max_lanes,
        "idle_lane_capacity": idle_lane_capacity,
        "productive_utilization_pct": utilization_pct,
        "utilization_pct": utilization_pct,
        "goal": goal,
        "thresholds": {
            "min_idle_cpu_headroom_pct": float(args.min_idle_cpu_headroom_pct),
            "min_idle_memory_headroom_pct": float(args.min_idle_memory_headroom_pct),
        },
        "verdict": verdict,
        "defect": defect,
        "next_action": (
            "convert idle capacity into paper lanes, broader polling, or faster evidence refresh; no live gate change without Fable"
            if verdict == "IDLE_CAPACITY"
            else "wire running services and fresh artifacts before making a productive-utilization claim"
            if verdict == "INSUFFICIENT_PRODUCTIVE_BINDINGS"
            else "continue utilization watch"
        ),
    }


def render_markdown(payload: dict[str, Any]) -> str:
    defect = payload.get("defect") if isinstance(payload.get("defect"), dict) else {}
    cpu = payload.get("cpu") if isinstance(payload.get("cpu"), dict) else {}
    memory = payload.get("memory") if isinstance(payload.get("memory"), dict) else {}
    goal = payload.get("goal") if isinstance(payload.get("goal"), dict) else {}
    return "\n".join(
        [
            "# Utilization Meter",
            f"generated_at: {payload.get('generated_at')}",
            f"verdict: {payload.get('verdict')}",
            f"registry: active_or_gated={payload.get('registry_active_or_gated_lane_count')} "
            f"occupancy_pct={payload.get('registry_status_occupancy_pct')}",
            f"productive_lanes: active={payload.get('productive_lane_count')} "
            f"max={payload.get('measured_max_lane_count')} "
            f"idle={payload.get('idle_lane_capacity')} "
            f"utilization_pct={payload.get('productive_utilization_pct')}",
            f"cpu_headroom_pct: {cpu.get('headroom_pct')}",
            f"memory_headroom_pct: {memory.get('headroom_pct')}",
            f"goal_unmet: {goal.get('goal_unmet')}",
            f"defect_open: {defect.get('open')}",
            f"next_action: {payload.get('next_action')}",
            "",
        ]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy-map", default=DEFAULT_STRATEGY_MAP)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown-output", default=DEFAULT_MARKDOWN_OUTPUT)
    parser.add_argument("--lane-memory-budget-gb", type=float, default=DEFAULT_LANE_MEMORY_BUDGET_GB)
    parser.add_argument("--min-idle-cpu-headroom-pct", type=float, default=DEFAULT_MIN_IDLE_CPU_HEADROOM_PCT)
    parser.add_argument("--min-idle-memory-headroom-pct", type=float, default=DEFAULT_MIN_IDLE_MEMORY_HEADROOM_PCT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_meter(ROOT, args)
    output = Path(args.output)
    output = output if output.is_absolute() else ROOT / output
    markdown_output = Path(args.markdown_output)
    markdown_output = markdown_output if markdown_output.is_absolute() else ROOT / markdown_output
    atomic_write_json(output, payload)
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(render_markdown(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(output),
                "markdown": str(markdown_output),
                "verdict": payload.get("verdict"),
                "registry_active_or_gated_lane_count": payload.get("registry_active_or_gated_lane_count"),
                "productive_lane_count": payload.get("productive_lane_count"),
                "measured_max_lane_count": payload.get("measured_max_lane_count"),
                "idle_lane_capacity": payload.get("idle_lane_capacity"),
                "defect": payload.get("defect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
