#!/usr/bin/env python3
"""Build the RTDS premerge old-vs-new output-equivalence artifact."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import time
from argparse import Namespace
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import merge_rtds_wallet_events as current_merge  # noqa: E402
from src.wallet_copy.research import unique_wallet_events  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_PRE_REPAIR_COMMIT = "0bda605"
DEFAULT_HISTORY = ROOT / "data/research/wallet_copy_history_state.json"
DEFAULT_RTDS = ROOT / DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_POLYGON = ROOT / "data/research/polygon_orderfilled_ws_shadow_resident.jsonl"
DEFAULT_LIVE_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/rtds_premerge_output_equivalence_latest.json"


def _utc_now_compact() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _tail_copy(src: Path, dst: Path, *, tail_bytes: int) -> dict[str, Any]:
    size = src.stat().st_size
    start = max(0, size - max(0, int(tail_bytes)))
    with src.open("rb") as source:
        source.seek(start)
        if start > 0:
            source.readline()
        actual_start = source.tell()
        with dst.open("wb") as out:
            shutil.copyfileobj(source, out)
    return {
        "source": str(src),
        "source_size_bytes": int(size),
        "start_offset": int(actual_start),
        "bytes": int(dst.stat().st_size),
        "sha256": _sha256_path(dst),
    }


def _load_old_module(tempdir: Path, *, commit: str) -> Any:
    source = subprocess.check_output(
        ["git", "show", f"{commit}:scripts/merge_rtds_wallet_events.py"],
        cwd=ROOT,
        text=True,
    )
    module_path = tempdir / "old_merge_rtds_wallet_events.py"
    module_path.write_text(source, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("old_merge_rtds_wallet_events", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not import pinned pre-repair merge module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wallets_from_state(path: Path) -> tuple[list[str], str]:
    state = _load_json(path)
    premerge = state.get("active_set_rtds_premerge") if isinstance(state.get("active_set_rtds_premerge"), dict) else {}
    rows = premerge.get("rows") if isinstance(premerge.get("rows"), list) else []
    wallets = [_norm_wallet(row.get("source_wallet")) for row in rows if isinstance(row, dict)]
    runtime = state.get("active_set_runtime") if isinstance(state.get("active_set_runtime"), dict) else {}
    runtime_wallets = runtime.get("wallets") if isinstance(runtime.get("wallets"), list) else []
    wallets.extend(_norm_wallet(wallet) for wallet in runtime_wallets)
    wallets = [wallet for wallet in dict.fromkeys(wallets) if wallet]
    selected = _norm_wallet(premerge.get("selected_wallet"))
    if not selected:
        selected = _norm_wallet(state.get("selected_wallet"))
    if selected:
        wallets = [selected, *[wallet for wallet in wallets if wallet != selected]]
    return wallets, selected


def _matching_tail_events(
    *,
    history: dict[str, Any],
    rtds_tail: Path,
    polygon_tail: Path,
    wallets: list[str],
    scan_limit: int,
) -> list[Any]:
    events_by_wallet: dict[str, list[Any]] = {wallet: [] for wallet in wallets}
    wallet_set = set(wallets)
    for line in rtds_tail.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("source_wallet") or current_merge._raw_dict(row).get("proxyWallet"))
        if wallet not in wallet_set:
            continue
        event = current_merge._wallet_event_from_rtds(
            row,
            source_wallet=wallet,
            wallet_name=f"live_primary_{wallet[-8:]}",
        )
        if event is not None:
            events_by_wallet[wallet].append(event)

    token_meta = current_merge._token_metadata_from_history(history)
    polygon_by_wallet, _profile = current_merge._iter_polygon_wallet_events(
        str(polygon_tail),
        wallets=wallets,
        wallet_names={wallet: f"live_primary_{wallet[-8:]}" for wallet in wallets},
        token_meta=token_meta,
        tail_bytes=polygon_tail.stat().st_size,
        max_events_per_wallet=int(scan_limit),
    )
    for wallet, events in polygon_by_wallet.items():
        events_by_wallet.setdefault(wallet, []).extend(events)

    all_events: list[Any] = []
    for wallet in wallets:
        all_events.extend(events_by_wallet.get(wallet, [])[-max(1, int(scan_limit)) :])
    return unique_wallet_events(all_events)


def _history_with_branch_gap(history: dict[str, Any], matching_events: list[Any], *, min_new_events: int) -> tuple[dict[str, Any], list[str]]:
    existing_rows = [row for row in history.get("events") or [] if isinstance(row, dict)]
    existing_ids = {str(row.get("event_id") or "") for row in existing_rows if str(row.get("event_id") or "")}
    removable_ids = [str(event.event_id) for event in matching_events if str(event.event_id) in existing_ids]
    removable_ids = list(dict.fromkeys(removable_ids))[: max(1, int(min_new_events))]
    if len(removable_ids) < max(1, int(min_new_events)):
        raise RuntimeError(
            f"not enough retained matching tail events to create branch exercise: "
            f"have={len(removable_ids)} need={min_new_events}"
        )
    removable = set(removable_ids)
    patched = dict(history)
    patched["events"] = [row for row in existing_rows if str(row.get("event_id") or "") not in removable]
    patched["generated_at"] = _utc_now_iso()
    return patched, removable_ids


def _merge_args(
    *,
    history_state: Path,
    history_index: Path,
    wallet_event_log: Path,
    rtds_tail: Path,
    polygon_tail: Path,
    wallets: list[str] | None = None,
    wallet: str = "",
    offset_state: Path | None = None,
    offset_states: dict[str, str] | None = None,
    watermark_state: Path,
    scan_limit: int,
    max_new_events: int,
    rtds_tail_bytes: int,
    polygon_tail_bytes: int,
) -> Namespace:
    values: dict[str, Any] = {
        "rtds_jsonl": str(rtds_tail),
        "history_state": str(history_state),
        "history_window_index": str(history_index),
        "wallet_event_log": str(wallet_event_log),
        "scan_limit": int(scan_limit),
        "tail_bytes": int(rtds_tail_bytes),
        "cold_tail_bytes": int(rtds_tail_bytes),
        "watermark_state": str(watermark_state),
        "polygon_jsonl": str(polygon_tail),
        "polygon_tail_bytes": int(polygon_tail_bytes),
        "max_new_events": int(max_new_events),
        "history_retain_events": 250_000,
        "history_retain_copy_intents": 250_000,
    }
    if wallets is not None:
        values["source_wallets"] = wallets
        values["wallet_names"] = {item: f"live_primary_{item[-8:]}" for item in wallets}
        values["offset_states"] = offset_states or {}
    else:
        values["source_wallet"] = wallet
        values["wallet_name"] = f"live_primary_{wallet[-8:]}"
        values["offset_state"] = str(offset_state or "")
    return Namespace(**values)


def _run_legacy_path(
    old_module: Any,
    *,
    tempdir: Path,
    base_history: Path,
    rtds_tail: Path,
    polygon_tail: Path,
    wallets: list[str],
    selected_wallet: str,
    scan_limit: int,
    max_new_events: int,
    rtds_tail_bytes: int,
    polygon_tail_bytes: int,
) -> tuple[Path, dict[str, Any], float]:
    history = tempdir / "old_history.json"
    index = tempdir / "old_index.json"
    events = tempdir / "old_wallet_events.jsonl"
    watermarks = tempdir / "old_watermarks.json"
    shutil.copy2(base_history, history)
    started = time.perf_counter()
    selected_summary: dict[str, Any] = {}
    if selected_wallet:
        selected_summary = old_module.run_merge(
            _merge_args(
                history_state=history,
                history_index=index,
                wallet_event_log=events,
                rtds_tail=rtds_tail,
                polygon_tail=polygon_tail,
                wallet=selected_wallet,
                offset_state=tempdir / f"old.offset.{selected_wallet[-8:]}.json",
                watermark_state=watermarks,
                scan_limit=scan_limit,
                max_new_events=max_new_events,
                rtds_tail_bytes=rtds_tail_bytes,
                polygon_tail_bytes=polygon_tail_bytes,
            )
        )
    remaining = [wallet for wallet in wallets if wallet != selected_wallet]
    batch_summary: dict[str, Any] = {}
    if remaining:
        batch_summary = old_module.run_multi_wallet_merge(
            _merge_args(
                history_state=history,
                history_index=index,
                wallet_event_log=events,
                rtds_tail=rtds_tail,
                polygon_tail=polygon_tail,
                wallets=remaining,
                offset_states={wallet: str(tempdir / f"old.offset.{wallet[-8:]}.json") for wallet in remaining},
                watermark_state=watermarks,
                scan_limit=scan_limit,
                max_new_events=max_new_events,
                rtds_tail_bytes=rtds_tail_bytes,
                polygon_tail_bytes=polygon_tail_bytes,
            )
        )
    elapsed = round(time.perf_counter() - started, 6)
    return history, {"selected": selected_summary, "batch": batch_summary}, elapsed


def _run_new_path(
    *,
    tempdir: Path,
    base_history: Path,
    rtds_tail: Path,
    polygon_tail: Path,
    wallets: list[str],
    scan_limit: int,
    max_new_events: int,
    rtds_tail_bytes: int,
    polygon_tail_bytes: int,
) -> tuple[Path, dict[str, Any], float]:
    history = tempdir / "new_history.json"
    index = tempdir / "new_index.json"
    events = tempdir / "new_wallet_events.jsonl"
    watermarks = tempdir / "new_watermarks.json"
    shutil.copy2(base_history, history)
    started = time.perf_counter()
    summary = current_merge.run_multi_wallet_merge(
        _merge_args(
            history_state=history,
            history_index=index,
            wallet_event_log=events,
            rtds_tail=rtds_tail,
            polygon_tail=polygon_tail,
            wallets=wallets,
            offset_states={wallet: str(tempdir / f"new.offset.{wallet[-8:]}.json") for wallet in wallets},
            watermark_state=watermarks,
            scan_limit=scan_limit,
            max_new_events=max_new_events,
            rtds_tail_bytes=rtds_tail_bytes,
            polygon_tail_bytes=polygon_tail_bytes,
        )
    )
    elapsed = round(time.perf_counter() - started, 6)
    return history, summary, elapsed


def _canonical_section(history_path: Path, section: str) -> tuple[int, str]:
    payload = _load_json(history_path)
    rows = payload.get(section) if isinstance(payload.get(section), list) else []
    if section == "events":
        rows = sorted(rows, key=lambda row: (str(row.get("event_id") or ""), json.dumps(row, sort_keys=True)))
    elif section == "copy_intents":
        rows = sorted(rows, key=lambda row: (str(row.get("intent_id") or ""), json.dumps(row, sort_keys=True)))
    elif section == "wallets":
        rows = sorted(rows, key=lambda row: (str(row.get("address") or ""), json.dumps(row, sort_keys=True)))
    else:
        rows = sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return len(rows), hashlib.sha256(encoded).hexdigest()


def _compare_histories(old_history: Path, new_history: Path) -> dict[str, Any]:
    compared = []
    mismatches = 0
    for section in ("events", "copy_intents", "wallets"):
        old_count, old_hash = _canonical_section(old_history, section)
        new_count, new_hash = _canonical_section(new_history, section)
        match = old_count == new_count and old_hash == new_hash
        mismatches += 0 if match else 1
        compared.append(
            {
                "section": section,
                "match": match,
                "old_count": old_count,
                "new_count": new_count,
                "old_sha256": old_hash,
                "new_sha256": new_hash,
            }
        )
    return {
        "field_for_field_basis": (
            "JSON canonical sort for events/copy_intents/wallets; audit-only "
            "generated_at/rtds_ingest/wallet_results excluded from equivalence decision"
        ),
        "sections_compared": compared,
        "matched_sections": len(compared) - mismatches,
        "mismatched_sections": mismatches,
    }


def _new_matching_total(summary: dict[str, Any]) -> int:
    total = 0
    if "new_matching_events" in summary:
        try:
            total += int(summary.get("new_matching_events") or 0)
        except (TypeError, ValueError):
            pass
    for key in ("selected", "batch"):
        row = summary.get(key)
        if isinstance(row, dict):
            total += _new_matching_total(row)
    return total


def _polygon_matching_total(summary: dict[str, Any]) -> int:
    if not isinstance(summary, dict):
        return 0
    if isinstance(summary.get("wallet_summaries"), dict):
        profile = summary.get("premerge_substage_profile") if isinstance(summary.get("premerge_substage_profile"), dict) else {}
        polygon = profile.get("polygon_ws_premerge_parse") if isinstance(profile.get("polygon_ws_premerge_parse"), dict) else {}
        try:
            return int(polygon.get("matching_events") or 0)
        except (TypeError, ValueError):
            return 0
    if isinstance(summary.get("polygon_ws_premerge"), dict):
        try:
            return int(summary["polygon_ws_premerge"].get("matching_events") or 0)
        except (TypeError, ValueError):
            return 0
    total = 0
    for value in summary.values():
        if isinstance(value, dict):
            total += _polygon_matching_total(value)
    return total


def _token_mapping_missing(summary: dict[str, Any]) -> int:
    if not isinstance(summary, dict):
        return 0
    total = 0
    profile = summary.get("premerge_substage_profile") if isinstance(summary.get("premerge_substage_profile"), dict) else {}
    polygon = profile.get("polygon_ws_premerge_parse") if isinstance(profile.get("polygon_ws_premerge_parse"), dict) else {}
    diagnostics = polygon.get("diagnostics") if isinstance(polygon.get("diagnostics"), dict) else {}
    try:
        total += int(diagnostics.get("token_mapping_missing") or 0)
    except (TypeError, ValueError):
        pass
    for value in summary.values():
        if isinstance(value, dict):
            total += _token_mapping_missing(value)
    return total


def _top_polygon_profile(summary: dict[str, Any]) -> dict[str, Any]:
    profile = summary.get("premerge_substage_profile") if isinstance(summary.get("premerge_substage_profile"), dict) else {}
    polygon = profile.get("polygon_ws_premerge_parse") if isinstance(profile.get("polygon_ws_premerge_parse"), dict) else {}
    return polygon


def _token_mapping_unresolved_final(summary: dict[str, Any]) -> int:
    polygon = _top_polygon_profile(summary)
    if "token_mapping_unresolved_final" in polygon:
        try:
            return int(polygon.get("token_mapping_unresolved_final") or 0)
        except (TypeError, ValueError):
            return 0
    diagnostics = polygon.get("diagnostics") if isinstance(polygon.get("diagnostics"), dict) else {}
    try:
        return int(diagnostics.get("token_mapping_missing") or 0)
    except (TypeError, ValueError):
        return 0


def _first_pass_token_mapping_missing(summary: dict[str, Any]) -> int:
    polygon = _top_polygon_profile(summary)
    try:
        return int(polygon.get("first_pass_token_mapping_missing") or 0)
    except (TypeError, ValueError):
        return 0


def build_artifact(args: argparse.Namespace) -> dict[str, Any]:
    baseline_artifact = _load_json(Path(args.baseline_artifact)) if str(getattr(args, "baseline_artifact", "") or "") else {}
    wallets, selected = _wallets_from_state(Path(args.live_guard_state))
    if baseline_artifact:
        baseline_wallets = [_norm_wallet(wallet) for wallet in baseline_artifact.get("wallets") or []]
        baseline_wallets = [wallet for wallet in baseline_wallets if wallet]
        if baseline_wallets:
            wallets = baseline_wallets
        selected = _norm_wallet(baseline_artifact.get("selected_wallet")) or selected
    if not wallets:
        raise RuntimeError("could not derive active-set wallets from live guard state")
    if not selected:
        selected = wallets[0]
    tempdir = Path(tempfile.mkdtemp(prefix="rtds_premerge_equiv_artifact."))
    history_path = Path(args.history_state)
    rtds_tail = tempdir / "rtds_tail.jsonl"
    polygon_tail = tempdir / "polygon_tail.jsonl"
    base_history = tempdir / "history_base.json"
    frozen_input_dir = Path(str(getattr(args, "frozen_input_dir", "") or ""))
    if frozen_input_dir:
        for name, target in (
            ("rtds_tail.jsonl", rtds_tail),
            ("polygon_tail.jsonl", polygon_tail),
            ("history_base.json", base_history),
        ):
            source = frozen_input_dir / name
            if not source.exists():
                raise RuntimeError(f"frozen input missing {name}: {source}")
            shutil.copy2(source, target)
        rtds_tail_meta = {
            "source": str(frozen_input_dir / "rtds_tail.jsonl"),
            "bytes": int(rtds_tail.stat().st_size),
            "sha256": _sha256_path(rtds_tail),
            "frozen_input_dir": str(frozen_input_dir),
        }
        polygon_tail_meta = {
            "source": str(frozen_input_dir / "polygon_tail.jsonl"),
            "bytes": int(polygon_tail.stat().st_size),
            "sha256": _sha256_path(polygon_tail),
            "frozen_input_dir": str(frozen_input_dir),
        }
        removed_ids = list(
            baseline_artifact.get("comparison", {}).get("removed_existing_event_ids_to_create_premerge_input") or []
        )
    else:
        history = _load_json(history_path)
        rtds_tail_meta = _tail_copy(Path(args.rtds_jsonl), rtds_tail, tail_bytes=int(args.rtds_tail_bytes))
        polygon_tail_meta = _tail_copy(Path(args.polygon_jsonl), polygon_tail, tail_bytes=int(args.polygon_tail_bytes))

        matching_events = _matching_tail_events(
            history=history,
            rtds_tail=rtds_tail,
            polygon_tail=polygon_tail,
            wallets=wallets,
            scan_limit=int(args.scan_limit),
        )
        base_payload, removed_ids = _history_with_branch_gap(
            history,
            matching_events,
            min_new_events=int(args.min_new_events),
        )
        atomic_write_json(base_history, base_payload)

    old_module = _load_old_module(tempdir, commit=str(args.pre_repair_commit))
    old_history, old_summary, old_duration = _run_legacy_path(
        old_module,
        tempdir=tempdir,
        base_history=base_history,
        rtds_tail=rtds_tail,
        polygon_tail=polygon_tail,
        wallets=wallets,
        selected_wallet=selected,
        scan_limit=int(args.scan_limit),
        max_new_events=int(args.max_new_events),
        rtds_tail_bytes=int(args.rtds_tail_bytes),
        polygon_tail_bytes=int(args.polygon_tail_bytes),
    )
    new_history, new_summary, new_duration = _run_new_path(
        tempdir=tempdir,
        base_history=base_history,
        rtds_tail=rtds_tail,
        polygon_tail=polygon_tail,
        wallets=wallets,
        scan_limit=int(args.scan_limit),
        max_new_events=int(args.max_new_events),
        rtds_tail_bytes=int(args.rtds_tail_bytes),
        polygon_tail_bytes=int(args.polygon_tail_bytes),
    )
    comparison = _compare_histories(old_history, new_history)
    old_polygon_matching_events = _polygon_matching_total(old_summary)
    new_polygon_matching_events = _polygon_matching_total(new_summary)
    new_token_mapping_missing = _token_mapping_missing(new_summary)
    new_token_mapping_unresolved_final = _token_mapping_unresolved_final(new_summary)
    comparison.update(
        {
            "old_new_matching_events_combined": _new_matching_total(old_summary),
            "new_new_matching_events": _new_matching_total(new_summary),
            "branch_exercised": _new_matching_total(new_summary) >= int(args.min_new_events),
            "removed_existing_event_ids_to_create_premerge_input": removed_ids,
            "old_polygon_matching_events": old_polygon_matching_events,
            "new_polygon_matching_events": new_polygon_matching_events,
            "polygon_matching_parity": old_polygon_matching_events == new_polygon_matching_events,
            "new_token_mapping_missing": new_token_mapping_missing,
            "new_first_pass_token_mapping_missing": _first_pass_token_mapping_missing(new_summary),
            "new_token_mapping_unresolved_final": new_token_mapping_unresolved_final,
            "token_mapping_missing_zero": new_token_mapping_missing == 0,
            "token_mapping_unresolved_final_zero": new_token_mapping_unresolved_final == 0,
            "new_duration_under_gate": new_duration <= float(args.max_new_duration_s),
            "new_duration_gate_s": float(args.max_new_duration_s),
        }
    )
    status = (
        "PASS"
        if comparison["mismatched_sections"] == 0
        and comparison["branch_exercised"]
        and comparison["token_mapping_unresolved_final_zero"]
        and comparison["new_duration_under_gate"]
        else "FAIL"
    )
    artifact = {
        "schema_version": 2,
        "status": status,
        "flow_stage": "LIVE/LEARN/SELF-DEV",
        "purpose": "same-input old-vs-new RTDS premerge output-equivalence gate with >=1 new matching event",
        "generated_at": _utc_now_iso(),
        "pre_repair_commit": str(args.pre_repair_commit),
        "current_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "legacy_path": "selected_wallet_run_merge_then_remaining_run_multi_wallet_merge",
        "new_path": "single_batch_run_active_set_rtds_premerge_selected_wallet_first",
        "selected_wallet": selected,
        "wallets": wallets,
        "input_snapshot": {
            "history": (
                (baseline_artifact.get("input_snapshot") or {}).get("history")
                if frozen_input_dir and baseline_artifact
                else {
                    "source": str(history_path),
                    "size_bytes": int(history_path.stat().st_size),
                    "sha256": _sha256_path(history_path),
                }
            ),
            "history_base": {
                "mode": "frozen_input_dir" if frozen_input_dir else "current_history_minus_recent_tail_events",
                "sha256": _sha256_path(base_history),
                "removed_event_count": len(removed_ids),
                "source": str(frozen_input_dir / "history_base.json") if frozen_input_dir else "",
            },
            "rtds_tail": rtds_tail_meta,
            "polygon_tail": polygon_tail_meta,
            "tempdir": str(tempdir),
        },
        "comparison": comparison,
        "timing": {
            "old_duration_s": old_duration,
            "new_duration_s": new_duration,
            "speedup_x": round(old_duration / new_duration, 6) if new_duration > 0 else None,
        },
        "old_result_summary": old_summary,
        "new_result_summary": new_summary,
        "submitter_invariant": "paper-only temp histories; no production live guard state or order path mutated",
    }
    return artifact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", default=str(DEFAULT_HISTORY))
    parser.add_argument("--rtds-jsonl", default=str(DEFAULT_RTDS))
    parser.add_argument("--polygon-jsonl", default=str(DEFAULT_POLYGON))
    parser.add_argument("--live-guard-state", default=str(DEFAULT_LIVE_GUARD_STATE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--baseline-artifact", default="")
    parser.add_argument("--pre-repair-commit", default=DEFAULT_PRE_REPAIR_COMMIT)
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--max-new-events", type=int, default=500)
    parser.add_argument("--min-new-events", type=int, default=1)
    parser.add_argument("--rtds-tail-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--polygon-tail-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--frozen-input-dir", default="")
    parser.add_argument("--max-new-duration-s", type=float, default=6.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact = build_artifact(args)
    output = Path(args.output)
    atomic_write_json(output, artifact)
    timestamped = output.with_name(f"{output.stem}_{_utc_now_compact()}{output.suffix}")
    atomic_write_json(timestamped, artifact)
    print(json.dumps({"status": artifact["status"], "output": str(output), "timestamped": str(timestamped)}, sort_keys=True))
    return 0 if artifact["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
