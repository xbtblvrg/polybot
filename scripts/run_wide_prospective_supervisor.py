#!/usr/bin/env python3
"""Continuously roll WIDE captures and score each from the prior frozen cut."""

from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import json
import os
import select
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.store import atomic_write_json, load_json

WIDE_CAPTURE_CAP_BYTES = 6 * 1024**3
WIDE_CAPTURE_KEEP_TAIL_BYTES = 2 * 1024**3
WIDE_SCORER_CYCLE_HISTORY_LIMIT = 12
RESEARCH_CAPTURE_INVENTORY = (
    ROOT / "data" / "research" / "research_capture_rotation_inventory.json"
)
ACTIVE_MANIFEST_POINTER = (
    ROOT / "data" / "research" / "wide_exact_policy_manifest_active.json"
)


@dataclass(frozen=True)
class ForwardLaneSpec:
    run_id: str
    wallet: str
    fingerprint: str
    policy_family: str
    manifest: str
    state: str
    ledger: str
    evidence: str
    atomic_output: str
    lane_output: str
    source_history: str
    registration_authority: str = ""
    observation_window_s: int = 86_400


BAC25_FORWARD_LANE = ForwardLaneSpec(
    run_id="bac25_forward_only",
    wallet="0x82c857cb4d18e919c1b7d3c6865be4debe50da77",
    fingerprint="bac25beda563430ef4f482544eccda250f4e3f7eb3bed91362fb70093dbc1fce",
    policy_family="fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
    manifest="data/research/wide_exact_policy_manifest_bac25_forward_only.json",
    state="data/research/bac25_forward_only_measurement_state.json",
    ledger="data/research/bac25_forward_only_orders.jsonl",
    evidence="data/research/bac25_forward_only_evidence_latest.json",
    atomic_output="data/research/bac25_forward_only_atomic_move_slice_rescore_latest.json",
    lane_output="data/research/bac25_forward_only_lane_latest.json",
    source_history="data/research/bac25_forward_only_no_retrospective_source.json",
)
WALLET_951B_FORWARD_LANE = ForwardLaneSpec(
    run_id="951b_forward_only",
    wallet="0x951bd740ef681d05891ca35440232488271d433e",
    fingerprint="2d8af91d223d7cc91753bf4b9ad0a03dcaeeebbb3903becbfc2382ae0d34f212",
    policy_family="fast_wf_0.10_cap_4_951b_two_slice_forward",
    manifest="data/research/wide_exact_policy_manifest_951b_forward_only.json",
    state="data/research/951b_forward_only_measurement_state.json",
    ledger="data/research/951b_forward_only_orders.jsonl",
    evidence="data/research/951b_forward_only_evidence_latest.json",
    atomic_output="data/research/951b_forward_only_atomic_move_slice_rescore_latest.json",
    lane_output="data/research/951b_forward_only_lane_latest.json",
    source_history="data/research/951b_forward_only_no_retrospective_source.json",
)


def _path(kind: str, run_id: str, suffix: str) -> str:
    return f"data/research/{kind}_{run_id}{suffix}"


def _retained_scorer_cycles(cycles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return cycles[-WIDE_SCORER_CYCLE_HISTORY_LIMIT:]


def publish_active_manifest_pointer(
    manifest_path: str,
    *,
    pointer_path: str | Path = ACTIVE_MANIFEST_POINTER,
) -> dict[str, Any]:
    """Publish the exact freshness-gated manifest consumed by the paper lane."""
    manifest = load_json(manifest_path, default={})
    if not isinstance(manifest, dict) or not manifest.get("manifest_id"):
        raise RuntimeError(f"ACTIVE_MANIFEST_INVALID path={manifest_path}")
    pointer = {
        "schema_version": 1,
        "kind": "wide_exact_policy_active_manifest_pointer",
        "paper_only": True,
        "live_orders_allowed": False,
        "manifest_path": manifest_path,
        "manifest_id": manifest.get("manifest_id"),
        "source_alpha_report": manifest.get("source_alpha_report"),
        "source_alpha_status": manifest.get("source_alpha_status"),
        "source_alpha_age_h": manifest.get("source_alpha_age_h"),
        "published_at_s": time.time(),
    }
    atomic_write_json(pointer_path, pointer)
    return pointer


def record_missing_produced_seed(
    *,
    supervisor_state_path: str | Path,
    supervisor: dict[str, Any],
    seed_run: str,
    score_run: str,
    completed: list[dict[str, Any]],
) -> bool:
    """Refuse seed advancement when a completed capture emitted no alpha file."""
    missing_path = _path("alpha_decay_report", score_run, ".json")
    if Path(missing_path).exists():
        return False
    atomic_write_json(
        supervisor_state_path,
        {
            **supervisor,
            "schema_version": 1,
            "kind": "wide_prospective_supervisor_state",
            "paper_only": True,
            "live_orders_allowed": False,
            "status": "SEED_ALPHA_NOT_PRODUCED",
            "seed_run_id": seed_run,
            "managed_run_id": score_run,
            "missing_seed_alpha_path": missing_path,
            "completed_runs": completed,
            "updated_at_s": time.time(),
        },
    )
    return True


def resolve_seed_alpha(
    seed_run: str,
    *,
    active_manifest_pointer: str | Path = ACTIVE_MANIFEST_POINTER,
) -> str:
    """Resolve a canonical seed or the exact alpha bound to an adopted active cut."""
    canonical = _path("alpha_decay_report", seed_run, ".json")
    if Path(canonical).exists():
        return canonical
    pointer = load_json(active_manifest_pointer, default={})
    manifest_path = str(pointer.get("manifest_path") or "") if isinstance(pointer, dict) else ""
    manifest = load_json(manifest_path, default={}) if manifest_path else {}
    if (
        isinstance(manifest, dict)
        and str(manifest.get("score_run_id") or "") == seed_run
        and str(manifest.get("manifest_id") or "") == str(pointer.get("manifest_id") or "")
        and manifest.get("source_alpha_report")
    ):
        return str(manifest["source_alpha_report"])
    return canonical


def ensure_capture_inventory(
    capture_path: str,
    *,
    inventory_path: str | Path = RESEARCH_CAPTURE_INVENTORY,
) -> dict[str, Any]:
    """Enroll each rolling WIDE capture before its writer can cross 5 GiB."""
    inventory = load_json(inventory_path, default={})
    entries = (
        inventory.get("entries")
        if isinstance(inventory.get("entries"), list)
        else []
    )
    if any(
        isinstance(entry, dict) and str(entry.get("path") or "") == capture_path
        for entry in entries
    ):
        return {"status": "ALREADY_ENROLLED", "path": capture_path}
    entries.append(
        {
            "path": capture_path,
            "cap_bytes": WIDE_CAPTURE_CAP_BYTES,
            "keep_tail_bytes": WIDE_CAPTURE_KEEP_TAIL_BYTES,
            "max_consumer_tail_bytes": WIDE_CAPTURE_KEEP_TAIL_BYTES,
            "rotation_action": "copytruncate_line_aligned_tail",
            "readers": [
                "scripts/run_wide_prospective_supervisor.py direct-event reconcile",
                "scripts/capture_clob_book_snapshots.py --polygon-jsonl tail scanner",
                "scripts/report_alpha_decay.py current rolling WIDE capture",
            ],
        }
    )
    inventory.update(
        {
            "flow_stage": "LIVE/DEFEND/SELF-DEV",
            "generated_at": datetime.now(timezone.utc).isoformat().replace(
                "+00:00", "Z"
            ),
            "kind": "research_capture_rotation_inventory",
            "rule": (
                "Every data/research file above 5GiB must be listed here or "
                "the research disk deadman incidents."
            ),
            "entries": entries,
        }
    )
    atomic_write_json(inventory_path, inventory)
    return {"status": "ENROLLED", "path": capture_path}


def _new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("wide_%Y%m%dT%H%M%SZ")


def _run(cmd: list[str], timeout_s: float = 180.0) -> dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "cmd": cmd,
            "returncode": None,
            "ok": False,
            "timed_out": True,
            "duration_s": round(time.time() - started, 3),
            "stdout_tail": "",
            "stderr_tail": f"TimeoutExpired after {timeout_s}s",
        }
    return {
        "cmd": cmd,
        "returncode": proc.returncode,
        "ok": proc.returncode in (0, 2),
        "duration_s": round(time.time() - started, 3),
        "stdout_tail": proc.stdout[-2000:],
        "stderr_tail": proc.stderr[-2000:],
    }


def _run_policy_fingerprint_evidence(
    cmd: list[str],
    *,
    runner: Callable[[list[str], float], dict[str, Any]],
) -> dict[str, Any]:
    """Keep a slow evidence refresh from terminating the resident supervisor."""

    started = time.time()
    try:
        return runner(cmd, 60.0)
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "returncode": None,
            "ok": False,
            "status": "POLICY_FINGERPRINT_EVIDENCE_TIMEOUT",
            "duration_s": round(time.time() - started, 3),
            "timeout_s": 60.0,
            "stdout_tail": str(exc.stdout or "")[-2000:],
            "stderr_tail": str(exc.stderr or "")[-2000:],
        }


def _refresh_order128_packet(
    score_run_id: str,
    runner: Callable[[list[str], float], dict[str, Any]] = _run,
) -> dict[str, Any]:
    """Couple ORDER128 packet generation to the manifest's deadman cut."""

    return runner(
        [
            sys.executable,
            "scripts/report_order128_fastest_lawful_path.py",
            "--score-run-id",
            score_run_id,
        ],
        60.0,
    )


def _require_order128_refresh_success(result: dict[str, Any]) -> None:
    """Refuse to build a manifest unless the packet reporter completed cleanly."""

    # _run treats 2 as ok for repo-wide refusal-producing commands. At this
    # boundary, 2 means the packet was not rewritten and must fail closed.
    if not result.get("ok") or result.get("returncode") != 0:
        raise RuntimeError(result)


def _run_reconcile_with_direct_file(
    cmd: list[str],
    direct_event: dict[str, Any] | list[dict[str, Any]] | None,
    *,
    runner: Callable[[list[str], float], dict[str, Any]],
    timeout_s: float,
) -> dict[str, Any]:
    """Keep arbitrarily rich direct batches off argv and delete after consumption."""
    if direct_event is None:
        return runner(cmd, timeout_s)
    payload_dir = ROOT / "data" / "research" / "wide_direct_event_batches"
    payload_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="direct_event_",
        suffix=".json",
        dir=payload_dir,
        delete=False,
    ) as handle:
        json.dump(direct_event, handle, sort_keys=True, separators=(",", ":"))
        payload_path = Path(handle.name)
    try:
        return runner([*cmd, "--direct-event-file", str(payload_path)], timeout_s)
    finally:
        payload_path.unlink(missing_ok=True)


def score_once(
    *,
    run_id: str,
    seed_alpha: str,
    manifest: str,
    polygon_jsonl: str,
    state_path: str,
    ledger_path: str,
    standings_path: str,
    resolution_path: str,
    refresh_resolutions: bool,
    direct_event: dict[str, Any] | list[dict[str, Any]] | None = None,
    runner: Callable[[list[str], float], dict[str, Any]] = _run,
) -> list[dict[str, Any]]:
    """Run the ordered resolution -> reconcile -> standings paper chain."""

    results: list[dict[str, Any]] = []
    standings_snapshot_path = str(
        Path(standings_path).with_name(f"wide_candidate_standings_{run_id}.json")
    )
    if refresh_resolutions:
        results.append(
            runner(
                [
                    sys.executable,
                    "scripts/refresh_btc_5m_resolutions_from_gamma.py",
                    "--ledger",
                    "data/research/wallet_copy_live_execution_state.json",
                    "--existing",
                    resolution_path,
                    "--output",
                    resolution_path,
                    "--summary-output",
                    "data/research/btc_resolutions_wide_supervisor_summary.json",
                    "--merge-existing",
                    "--max-windows",
                    "250",
                    "--max-wall-runtime-s",
                    "60",
                    "--timeout-s",
                    "8",
                ],
                120.0,
            )
        )
    reconcile_cmd = [
                sys.executable,
                "scripts/reconcile_wide_exact_policy_paper.py",
                "--run-id",
                run_id,
                "--manifest",
                manifest,
                "--polygon-jsonl",
                polygon_jsonl,
                "--alpha-report",
                seed_alpha,
                "--token-metadata-cache",
                "data/research/wide_token_metadata_cache.json",
                "--resolutions",
                resolution_path,
                "--state",
                state_path,
                "--ledger",
                ledger_path,
                "--f3-instrumentation-jsonl",
                "data/research/wide_f3_instrumentation_events.jsonl",
                "--f3-instrumentation-run-prefix",
                "wide_",
            ]
    results.append(
        _run_reconcile_with_direct_file(
            reconcile_cmd,
            direct_event,
            runner=runner,
            timeout_s=180.0,
        )
    )
    standings_build_result = runner(
        [
            sys.executable,
            "scripts/build_wide_candidate_standings.py",
            "--alpha",
            seed_alpha,
            "--measurement",
            state_path,
            "--manifest",
            manifest,
            "--output",
            standings_path,
        ],
        60.0,
    )
    results.append(standings_build_result)
    if not standings_build_result.get("ok"):
        results.append(
            {
                "ok": False,
                "returncode": 1,
                "status": "STANDINGS_SNAPSHOT_SKIPPED_STALE_SOURCE",
                "source": standings_path,
                "output": standings_snapshot_path,
            }
        )
    else:
        try:
            shutil.copyfile(standings_path, standings_snapshot_path)
            results.append(
                {
                    "ok": True,
                    "returncode": 0,
                    "status": "STANDINGS_SNAPSHOT_COPIED_FROM_FRESH_BUILD",
                    "source": standings_path,
                    "output": standings_snapshot_path,
                }
            )
        except OSError as exc:
            results.append(
                {
                    "ok": False,
                    "returncode": 1,
                    "status": "STANDINGS_SNAPSHOT_COPY_FAILED",
                    "source": standings_path,
                    "output": standings_snapshot_path,
                    "error": str(exc),
                }
            )
    results.append(
        runner(
            [
                sys.executable,
                "scripts/report_wide_f3_batch_interval_attribution.py",
                "--instrumentation-events",
                "data/research/wide_f3_instrumentation_events.jsonl",
            ],
            30.0,
        )
    )
    results.append(
        _run_policy_fingerprint_evidence(
            [
                sys.executable,
                "scripts/build_wide_policy_fingerprint_evidence.py",
                "--ledger",
                ledger_path,
                "--output",
                "data/research/wide_policy_fingerprint_evidence_latest.json",
                "--atomic-output",
                "data/research/order134_c_atomic_move_slice_rescore_latest.json",
                "--sweep-output",
                "data/research/order134_d_venue_min_order_sweep_latest.json",
            ],
            runner=runner,
        )
    )
    results.append(
        runner(
            [
                sys.executable,
                "scripts/build_frozen_fingerprint_f2_prewarm_shadow.py",
                "--auto-retarget",
            ],
            30.0,
        )
    )
    sidecar = runner(
        [
            sys.executable,
            "scripts/build_copy_freeze_near_bar_allpass_dryrun_sidecar.py",
        ],
        30.0,
    )
    results.append(sidecar)
    try:
        sidecar_summary = json.loads(
            str(sidecar.get("stdout_tail") or "").strip().splitlines()[-1]
        )
    except (IndexError, json.JSONDecodeError):
        sidecar_summary = {}
    if sidecar.get("ok") and sidecar_summary.get("all_pass") is True:
        # The deadman remains the sole admission actuator. Running it in the
        # same score cycle removes heartbeat latency without bypassing F1-F4.
        results.append(
            runner(
                [sys.executable, "scripts/order_flow_deadman.py"],
                300.0,
            )
        )
    return results


def _matured_unresolved_slugs(
    state: dict[str, Any],
    *,
    now_s: float,
) -> list[str]:
    slugs: set[str] = set()
    for row in state.get("orders") or []:
        if not isinstance(row, dict) or row.get("resolved") is True:
            continue
        slug = str(row.get("market_slug") or "")
        if not slug.startswith("btc-updown-5m-"):
            continue
        try:
            start_s = int(slug.rsplit("-", 1)[-1])
        except ValueError:
            continue
        if start_s + 300 <= int(now_s):
            slugs.add(slug)
    return sorted(slugs)


def score_forward_lane(
    *,
    spec: ForwardLaneSpec,
    seed_alpha: str,
    polygon_jsonl: str,
    resolution_path: str,
    direct_event: dict[str, Any] | list[dict[str, Any]] | None,
    runner: Callable[[list[str], float], dict[str, Any]] = _run,
) -> list[dict[str, Any]]:
    """Score one exact fingerprint without importing retrospective rows."""

    manifest = spec.manifest
    state = spec.state
    ledger = spec.ledger
    evidence = spec.evidence
    results: list[dict[str, Any]] = []
    manifest_payload = load_json(manifest, default={})
    terminal = (
        manifest_payload.get("terminal_outcome_on_deadline")
        if isinstance(manifest_payload, dict)
        else {}
    )
    if isinstance(terminal, dict) and terminal.get("stop_writer") is True:
        return [
            {
                "cmd": ["forward_lane_terminal_stop_writer", spec.run_id],
                "returncode": 0,
                "ok": True,
                "status": "FORWARD_LANE_TERMINAL_STOP_WRITER_SKIPPED",
                "run_id": spec.run_id,
                "manifest": manifest,
                "terminal_status": terminal.get("status"),
                "stop_writer": True,
            }
        ]
    prior_state = load_json(state, default={})
    matured_unresolved_slugs = _matured_unresolved_slugs(
        prior_state if isinstance(prior_state, dict) else {},
        now_s=time.time(),
    )
    if matured_unresolved_slugs:
        refresh_cmd = [
            sys.executable,
            "scripts/refresh_btc_5m_resolutions_from_gamma.py",
            "--existing",
            resolution_path,
            "--output",
            resolution_path,
            "--max-windows",
            str(len(matured_unresolved_slugs)),
        ]
        for slug in matured_unresolved_slugs:
            refresh_cmd.extend(["--market-slug", slug])
        results.append(runner(refresh_cmd, 120.0))
    results.append(
        runner(
            [
                sys.executable,
                "scripts/build_bac25_forward_only_lane.py",
                "--wallet",
                spec.wallet,
                "--fingerprint",
                spec.fingerprint,
                "--policy-family",
                spec.policy_family,
                "--score-run-id",
                spec.run_id,
                "--lane-kind",
                f"{spec.run_id}_paper_lane",
                "--observation-window-s",
                str(spec.observation_window_s),
                "--manifest",
                manifest,
                "--forward-evidence",
                evidence,
                "--orders",
                ledger,
                "--output",
                spec.lane_output,
            ],
            30.0,
        )
    )
    reconcile_cmd = [
        sys.executable,
        "scripts/reconcile_wide_exact_policy_paper.py",
        "--run-id",
        spec.run_id,
        "--manifest",
        manifest,
        "--polygon-jsonl",
        polygon_jsonl,
        "--alpha-report",
        seed_alpha,
        "--token-metadata-cache",
        "data/research/wide_token_metadata_cache.json",
        "--resolutions",
        resolution_path,
        "--state",
        state,
        "--ledger",
        ledger,
    ]
    results.append(
        _run_reconcile_with_direct_file(
            reconcile_cmd,
            direct_event,
            runner=runner,
            timeout_s=180.0,
        )
    )
    results.append(
        _run_policy_fingerprint_evidence(
            [
                sys.executable,
                "scripts/build_wide_policy_fingerprint_evidence.py",
                "--ledger",
                ledger,
                "--manifest-glob",
                manifest,
                "--source-history-acquisition",
                spec.source_history,
                "--output",
                evidence,
                "--atomic-output",
                spec.atomic_output,
                "--no-sweep-output",
            ],
            runner=runner,
        )
    )
    results.append(
        runner(
            [
                sys.executable,
                "scripts/build_bac25_forward_only_lane.py",
                "--wallet",
                spec.wallet,
                "--fingerprint",
                spec.fingerprint,
                "--policy-family",
                spec.policy_family,
                "--score-run-id",
                spec.run_id,
                "--lane-kind",
                f"{spec.run_id}_paper_lane",
                "--observation-window-s",
                str(spec.observation_window_s),
                "--manifest",
                manifest,
                "--forward-evidence",
                evidence,
                "--orders",
                ledger,
                "--output",
                spec.lane_output,
            ],
            30.0,
        )
    )
    lane = load_json(
        spec.lane_output,
        default={},
    )
    if isinstance(lane, dict) and lane.get("admission_eligible") is True:
        # The deadman remains the sole admission actuator. Trigger it in the
        # same paper cycle that the preregistered forward gate crosses.
        results.append(
            runner(
                [sys.executable, "scripts/order_flow_deadman.py"],
                300.0,
            )
        )
    return results


def score_bac25_forward_lane(
    *,
    seed_alpha: str,
    polygon_jsonl: str,
    resolution_path: str,
    direct_event: dict[str, Any] | list[dict[str, Any]] | None,
    runner: Callable[[list[str], float], dict[str, Any]] = _run,
) -> list[dict[str, Any]]:
    return score_forward_lane(
        spec=BAC25_FORWARD_LANE,
        seed_alpha=seed_alpha,
        polygon_jsonl=polygon_jsonl,
        resolution_path=resolution_path,
        direct_event=direct_event,
        runner=runner,
    )


def _capture_cmd(
    run_id: str,
    duration_s: float,
    roster: str,
    *,
    fanout_socket: str = "",
) -> list[str]:
    command = [
        sys.executable,
        "scripts/run_alpha_decay_simultaneous_capture.py",
        "--run-id",
        run_id,
        "--duration-s",
        str(duration_s),
        "--active-registry",
        roster,
        "--polygon-disable-default-registry",
        "--polygon-registry-only",
        "--http-backfill-fallback",
        "--disable-source-base-overrides",
        "--profile-min-fills",
        "20",
        "--alpha-report",
        _path("alpha_decay_report", run_id, ".json"),
        "--event-log",
        _path("alpha_decay_simultaneous_capture_events", run_id, ".jsonl"),
        "--detection-report",
        _path("detection_latency_report", run_id, ".json"),
        "--asset-ids-output",
        _path("alpha_decay_target_asset_ids", run_id, ".json"),
    ]
    if fanout_socket:
        command.extend(["--orderfilled-fanout-socket", fanout_socket])
    return command


def _drain_fanout(sock: socket.socket) -> list[dict[str, Any]]:
    events: dict[tuple[str, str], dict[str, Any]] = {}
    while True:
        try:
            row = json.loads(sock.recv(65535))
            fanout_received_monotonic_s = time.monotonic()
        except BlockingIOError:
            break
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue
        row = {
            **row,
            "fanout_received_monotonic_s": fanout_received_monotonic_s,
        }
        identity = (
            str(row.get("transaction_hash") or row.get("transactionHash") or "").lower(),
            str(row.get("log_index") or row.get("event_id") or ""),
        )
        if identity[0] and identity[1]:
            events.setdefault(identity, row)
    return list(events.values())


def _attach_direct_book_prefetch(
    events: list[dict[str, Any]],
    *,
    clob: CLOBMarketClient,
) -> list[dict[str, Any]]:
    """Fetch each unique event token's book at the datagram receive boundary."""

    fetch_cycle_id = uuid.uuid4().hex
    token_ids = sorted(
        {
            str((row.get("decoded") or {}).get("asset") or "")
            for row in events
            if isinstance(row.get("decoded"), dict)
            and str((row.get("decoded") or {}).get("asset") or "")
        }
    )

    dispatched = time.monotonic()
    token_fanout_received = {
        token_id: min(
            float(row.get("fanout_received_monotonic_s") or dispatched)
            for row in events
            if str((row.get("decoded") or {}).get("asset") or "") == token_id
        )
        for token_id in token_ids
    }

    def fetch_one(token_id: str) -> tuple[str, dict[str, Any]]:
        started = time.monotonic()
        try:
            book = clob.get_book(token_id)
            finished = time.monotonic()
            route = (
                book.get("__walletCopyClobRouteReport")
                if isinstance(book, dict)
                and isinstance(book.get("__walletCopyClobRouteReport"), dict)
                else {}
            )
            total_ms = max(0.0, finished - started) * 1000.0
            network_ms = min(total_ms, float(route.get("elapsed_ms_total") or 0.0))
            return token_id, {
                "book": book,
                "fetch_started_monotonic_s": started,
                "fetch_cycle_id": fetch_cycle_id,
                "fetch_provenance": "capture_prefetched",
                "book_fetch_ms": round(total_ms, 3),
                "prefetch_queue_wait_ms": round(
                    max(0.0, started - token_fanout_received[token_id]) * 1000.0,
                    3,
                ),
                "prefetch_worker_queue_ms": round(
                    max(0.0, started - dispatched) * 1000.0,
                    3,
                ),
                "prefetch_network_ms": round(network_ms, 3),
                "prefetch_parse_ms": round(max(0.0, total_ms - network_ms), 3),
                "error": None,
            }
        except Exception as exc:
            finished = time.monotonic()
            return token_id, {
                "book": None,
                "fetch_started_monotonic_s": started,
                "fetch_cycle_id": fetch_cycle_id,
                "fetch_provenance": "capture_prefetched",
                "book_fetch_ms": round((finished - started) * 1000.0, 3),
                "prefetch_queue_wait_ms": round(
                    max(0.0, started - token_fanout_received[token_id]) * 1000.0,
                    3,
                ),
                "prefetch_worker_queue_ms": round(
                    max(0.0, started - dispatched) * 1000.0,
                    3,
                ),
                "prefetch_network_ms": None,
                "prefetch_parse_ms": None,
                "error": f"{type(exc).__name__}: {exc}",
            }

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(8, len(token_ids)))
    ) as pool:
        prefetched = dict(pool.map(fetch_one, token_ids))
    return [
        {
            **row,
            "_direct_book_prefetch": prefetched.get(
                str((row.get("decoded") or {}).get("asset") or ""),
                {},
            ),
        }
        for row in events
    ]


class _DirectFanoutReceiver:
    """Continuously receive and prefetch while the scorer subprocess blocks."""

    def __init__(self, sock: socket.socket, *, clob: CLOBMarketClient):
        self.sock = sock
        self.clob = clob
        self._pending: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="wide-orderfilled-direct-receiver",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=3.0)

    def drain(self, *, max_rows: int = 256) -> list[dict[str, Any]]:
        with self._lock:
            selected_keys = list(self._pending)[: max(1, int(max_rows))]
            rows = [self._pending.pop(key) for key in selected_keys]
        return rows

    def _run(self) -> None:
        while not self._stop.is_set():
            ready, _, _ = select.select([self.sock], [], [], 0.25)
            if not ready:
                continue
            events = _drain_fanout(self.sock)
            if not events:
                continue
            rows = _attach_direct_book_prefetch(events, clob=self.clob)
            with self._lock:
                for row in rows:
                    identity = (
                        str(
                            row.get("transaction_hash")
                            or row.get("transactionHash")
                            or ""
                        ).lower(),
                        str(row.get("log_index") or row.get("event_id") or ""),
                    )
                    if identity[0] and identity[1]:
                        self._pending.setdefault(identity, row)


def recover_completed_boundary(
    *,
    supervisor: dict[str, Any],
    adopt_run_id: str,
    duration_s: float,
) -> tuple[str, list[dict[str, Any]], dict[str, Any] | None]:
    """Recover a final managed run when the supervisor exited at its timebox seam."""

    completed = [row for row in (supervisor.get("completed_runs") or []) if isinstance(row, dict)]
    managed_run = str(supervisor.get("managed_run_id") or "")
    if not managed_run or managed_run == adopt_run_id:
        return adopt_run_id, completed, None
    if any(str(row.get("run_id") or "") == managed_run for row in completed):
        return managed_run, completed, None

    final_alpha = _path("alpha_decay_report", managed_run, ".json")
    capture_state_path = _path("alpha_decay_simultaneous_capture_state", managed_run, ".json")
    alpha = load_json(final_alpha, default={})
    capture_state = load_json(capture_state_path, default={})
    if not isinstance(alpha, dict) or not isinstance(capture_state, dict):
        return adopt_run_id, completed, None
    commands = capture_state.get("commands") if isinstance(capture_state.get("commands"), list) else []
    by_name = {
        str(row.get("name") or ""): row
        for row in commands
        if isinstance(row, dict) and row.get("name")
    }
    polygon = by_name.get("polygon_fills", {})
    clob = by_name.get("clob_books", {})
    report = by_name.get("alpha_decay_report", {})
    known_timebox_seam = (
        str(capture_state.get("run_id") or "") == managed_run
        and int(polygon.get("returncode") or 0) == -15
        and float(polygon.get("duration_s") or 0.0) >= max(1.0, float(duration_s))
        and bool(clob.get("ok"))
        and bool(report.get("ok"))
        and str(alpha.get("status") or "") == "PASS_CURRENT_SOURCE"
        and str((alpha.get("execution_profiles") or {}).get("status") or "") == "PASS"
    )
    if not known_timebox_seam:
        return adopt_run_id, completed, None
    recovered = {
        "run_id": managed_run,
        "final_alpha": final_alpha,
        "recovered_at_s": time.time(),
        "recovery_reason": "completed_timebox_polygon_sigterm_misclassified",
    }
    completed.append(recovered)
    return managed_run, completed, recovered


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adopt-run-id", default="")
    parser.add_argument("--duration-s", type=float, default=1800.0)
    parser.add_argument("--score-interval-s", type=float, default=30.0)
    parser.add_argument("--resolution-interval-s", type=float, default=300.0)
    parser.add_argument("--roster", default="data/research/wide_alpha_capture_roster_latest.json")
    parser.add_argument("--queue", default="data/research/wallet_copy_full_universe_copyability_latest.json")
    parser.add_argument("--degrade", default="data/research/wallet_copy_active_set_auto_degrade_state.json")
    parser.add_argument("--state", default="data/research/wide_exact_policy_paper_state.json")
    parser.add_argument("--ledger", default="data/research/wide_exact_policy_paper_orders.jsonl")
    parser.add_argument("--standings", default="data/research/wide_candidate_standings_latest.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--supervisor-state", default="data/research/wide_prospective_supervisor_state.json")
    parser.add_argument(
        "--active-manifest-pointer",
        default="data/research/wide_exact_policy_manifest_active.json",
    )
    parser.add_argument(
        "--lock-file",
        default="data/research/wide_prospective_supervisor.lock",
    )
    parser.add_argument("--max-managed-runs", type=int, default=0)
    return parser.parse_args()


def acquire_supervisor_lock(path: str | Path):
    """Hold an advisory singleton lock for the lifetime of the supervisor."""
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise RuntimeError("WIDE_PROSPECTIVE_SUPERVISOR_LOCK_HELD")
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def active_manifest_run_id(pointer_path: str | Path) -> str:
    pointer = load_json(pointer_path, default={})
    manifest_path = str(pointer.get("manifest_path") or "") if isinstance(pointer, dict) else ""
    manifest = load_json(manifest_path, default={}) if manifest_path else {}
    return str(manifest.get("score_run_id") or "") if isinstance(manifest, dict) else ""


def run_supervisor(args: argparse.Namespace) -> int:
    os.chdir(ROOT)
    supervisor_lock = acquire_supervisor_lock(args.lock_file)
    supervisor = load_json(args.supervisor_state, default={})
    supervisor = supervisor if isinstance(supervisor, dict) else {}
    adopted_run_id = active_manifest_run_id(args.active_manifest_pointer) or str(
        args.adopt_run_id or ""
    )
    if not adopted_run_id:
        raise RuntimeError("ACTIVE_MANIFEST_RUN_ID_MISSING")
    seed_run, completed, recovered = recover_completed_boundary(
        supervisor=supervisor,
        adopt_run_id=adopted_run_id,
        duration_s=float(args.duration_s),
    )
    if recovered is not None:
        atomic_write_json(
            args.supervisor_state,
            {
                **supervisor,
                "status": "RECOVERED_COMPLETED_BOUNDARY",
                "seed_run_id": seed_run,
                "completed_runs": completed,
                "boundary_recovery": recovered,
                "updated_at_s": time.time(),
            },
        )
    managed = 0
    while args.max_managed_runs <= 0 or managed < args.max_managed_runs:
        seed_alpha = resolve_seed_alpha(
            seed_run,
            active_manifest_pointer=args.active_manifest_pointer,
        )
        while not Path(seed_alpha).exists():
            atomic_write_json(
                args.supervisor_state,
                {
                    "schema_version": 1,
                    "kind": "wide_prospective_supervisor_state",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "status": "WAITING_FOR_ADOPTED_FINAL",
                    "adopted_run_id": seed_run,
                    "completed_runs": completed,
                    "updated_at_s": time.time(),
                },
            )
            time.sleep(min(10.0, max(0.25, args.score_interval_s)))
            seed_alpha = resolve_seed_alpha(
                seed_run,
                active_manifest_pointer=args.active_manifest_pointer,
            )
        score_run = _new_run_id()
        while score_run == seed_run:
            time.sleep(1.0)
            score_run = _new_run_id()
        manifest = _path("wide_exact_policy_manifest", score_run, ".json")
        order128_result = _refresh_order128_packet(score_run)
        _require_order128_refresh_success(order128_result)
        manifest_result = _run(
            [
                sys.executable,
                "scripts/build_wide_exact_policy_manifest.py",
                "--alpha-report",
                seed_alpha,
                "--score-run-id",
                score_run,
                "--queue",
                args.queue,
                "--roster",
                args.roster,
                "--degrade",
                args.degrade,
                "--output",
                manifest,
            ],
            60.0,
        )
        if not manifest_result["ok"]:
            raise RuntimeError(manifest_result)
        active_manifest_pointer = publish_active_manifest_pointer(
            manifest,
            pointer_path=args.active_manifest_pointer,
        )
        polygon = _path("polygon_orderfilled_ws_capture_alpha_decay", score_run, ".jsonl")
        ensure_capture_inventory(polygon)
        fanout_path = str(ROOT / "data" / "research" / f"wide_orderfilled_{score_run}.sock")
        try:
            Path(fanout_path).unlink()
        except FileNotFoundError:
            pass
        fanout = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        fanout.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
        fanout.bind(fanout_path)
        fanout.setblocking(False)
        direct_receiver = _DirectFanoutReceiver(
            fanout,
            clob=CLOBMarketClient(timeout_s=1.5, retries=1),
        )
        direct_receiver.start()
        capture = subprocess.Popen(
            _capture_cmd(
                score_run,
                args.duration_s,
                args.roster,
                fanout_socket=fanout_path,
            ),
            cwd=ROOT,
        )
        cycles: list[dict[str, Any]] = []
        last_resolution_s = time.time()
        while capture.poll() is None:
            time.sleep(max(0.05, min(float(args.score_interval_s), 0.25)))
            direct_events = direct_receiver.drain()
            direct_event = direct_events or None
            cycle_started = time.time()
            if direct_event is None and cycle_started - (
                cycles[-1]["started_at_s"] if cycles else 0.0
            ) < args.score_interval_s:
                continue
            refresh = (
                direct_event is None
                and cycle_started - last_resolution_s >= args.resolution_interval_s
            )
            results = score_once(
                run_id=score_run,
                seed_alpha=seed_alpha,
                manifest=manifest,
                polygon_jsonl=polygon,
                state_path=args.state,
                ledger_path=args.ledger,
                standings_path=args.standings,
                resolution_path=args.resolutions,
                refresh_resolutions=refresh,
                # An empty direct batch is intentional on timer-only cycles:
                # refresh resolutions/standings without rescanning the
                # cumulative sidecar and contaminating generation-local
                # tx/log terminal reconciliation.
                direct_event=direct_event if direct_event is not None else [],
            )
            for forward_lane in (BAC25_FORWARD_LANE, WALLET_951B_FORWARD_LANE):
                results.extend(
                    score_forward_lane(
                        spec=forward_lane,
                        seed_alpha=seed_alpha,
                        polygon_jsonl=polygon,
                        resolution_path=args.resolutions,
                        direct_event=direct_event if direct_event is not None else [],
                    )
                )
            if refresh:
                last_resolution_s = cycle_started
            cycles.append(
                {
                    "started_at_s": cycle_started,
                    "direct_event": direct_event is not None,
                    "polygon_bytes": Path(polygon).stat().st_size if Path(polygon).exists() else 0,
                    "state_updated_at": (load_json(args.state, default={}) or {}).get("updated_at"),
                    "standings_generated_at": (load_json(args.standings, default={}) or {}).get("generated_at"),
                    "results": results,
                }
            )
            atomic_write_json(
                args.supervisor_state,
                {
                    "schema_version": 1,
                    "kind": "wide_prospective_supervisor_state",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "status": "CAPTURE_AND_SCORER_RESIDENT",
                    "seed_run_id": seed_run,
                    "managed_run_id": score_run,
                    "capture_pid": capture.pid,
                    "event_handoff": "DIRECT_UNIX_DGRAM_PARSED_ORDERFILLED_V1",
                    "fanout_socket": fanout_path,
                    "direct_event_cycles": sum(
                        1 for row in cycles if row.get("direct_event")
                    ),
                    "manifest": manifest,
                    "active_manifest_pointer": args.active_manifest_pointer,
                    "manifest_id": active_manifest_pointer.get("manifest_id"),
                    "source_alpha_age_h": active_manifest_pointer.get("source_alpha_age_h"),
                    "completed_runs": completed,
                    "cycle_count": len(cycles),
                    "latest_cycles": _retained_scorer_cycles(cycles),
                    "updated_at_s": time.time(),
                },
            )
        capture.wait()
        direct_receiver.stop()
        fanout.close()
        try:
            Path(fanout_path).unlink()
        except FileNotFoundError:
            pass
        if capture.returncode != 0:
            raise RuntimeError(f"WIDE capture {score_run} exited {capture.returncode}")
        if record_missing_produced_seed(
            supervisor_state_path=args.supervisor_state,
            supervisor=supervisor,
            seed_run=seed_run,
            score_run=score_run,
            completed=completed,
        ):
            return 2
        completed.append({"run_id": score_run, "final_alpha": _path("alpha_decay_report", score_run, ".json")})
        seed_run = score_run
        managed += 1
    return 0


def main() -> int:
    args = parse_args()
    try:
        return run_supervisor(args)
    except Exception as exc:
        prior = load_json(args.supervisor_state, default={})
        prior = prior if isinstance(prior, dict) else {}
        atomic_write_json(
            args.supervisor_state,
            {
                **prior,
                "status": "PRODUCER_CRASH_EXIT",
                "crash_type": type(exc).__name__,
                "crash_message": str(exc),
                "crashed_at_s": time.time(),
                "active_manifest_run_id": active_manifest_run_id(
                    args.active_manifest_pointer
                ),
            },
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
