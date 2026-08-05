#!/usr/bin/env python3
"""Mechanical CODEX_STARVATION deadman.

Flow stage: SELF-DEV/LIVE. If Fable has a non-empty next-list, Codex has
made no serving commit for the configured window, and the heartbeat wrapper
still reports PASS, process-health is masking service starvation.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import re
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_HANDOFF = "docs/agents/HANDOFF.md"
DEFAULT_HEARTBEAT_STATE = "data/research/codex_heartbeat_log_rotation_state.json"
DEFAULT_STATE = "data/research/codex_starvation_deadman_state.json"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def latest_direction_block(handoff_text: str) -> tuple[str, str]:
    headings = list(re.finditer(r"(?m)^## .*(?:fable DIRECTION|DIRECTION).*$", handoff_text))
    if not headings:
        return "", ""
    match = headings[-1]
    next_match = re.search(r"(?m)^## ", handoff_text[match.end() :])
    end = match.end() + next_match.start() if next_match else len(handoff_text)
    heading = match.group(0)
    return heading, handoff_text[match.start() : end].strip()


def direction_heading_ts(heading: str) -> datetime | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}Z)", heading)
    if not match:
        return None
    return _parse_ts(match.group(1))


def direction_has_nonempty_next(block: str) -> bool:
    if not block:
        return False
    markers = (
        "RESUMPTION QUEUE",
        "CODEX BACKLOG",
        "QUEUE",
        "NEXT(",
        "Ordered next actions",
        "next-list",
        "next:",
    )
    if not any(marker.lower() in block.lower() for marker in markers):
        return False
    nonempty_lines = []
    for raw in block.splitlines():
        text = raw.strip()
        if not text or text.startswith("##"):
            continue
        lowered = text.lower()
        if "next: missing" in lowered or "queue unchanged" in lowered:
            continue
        if re.match(r"^[-*]\s+(?:next|queue|codex backlog|resumption queue|next\()", lowered):
            nonempty_lines.append(text)
        elif re.match(r"^\d+\.\s+", text):
            nonempty_lines.append(text)
        elif any(marker.lower() in lowered for marker in markers[:4]):
            nonempty_lines.append(text)
    return bool(nonempty_lines)


def last_codex_commit(repo: Path, *, subject_prefix: str = "codex:") -> dict[str, Any]:
    proc = subprocess.run(
        ["git", "log", "-200", "--format=%H%x00%ct%x00%s"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
        timeout=5.0,
    )
    if proc.returncode != 0:
        return {"status": "ERROR", "error": (proc.stderr or proc.stdout or "").strip()}
    prefix = subject_prefix.lower()
    # Newest-first scan: the first non-fable commit is the freshest serving
    # commit, whether or not it carries the codex subject prefix. An older
    # prefixed commit must not shadow newer unprefixed serving commits.
    for line in proc.stdout.splitlines():
        parts = line.split("\x00")
        if len(parts) != 3:
            continue
        commit, ts, subject = parts
        row = {"status": "PASS", "commit": commit, "commit_ts": float(ts), "subject": subject}
        lowered = subject.lower()
        if lowered.startswith(prefix):
            return row
        if not lowered.startswith("fable:"):
            return {**row, "status": "FALLBACK_NON_FABLE_PREFIX"}
    return {"status": "MISSING", "commit": "", "commit_ts": None, "subject": ""}


def heartbeat_pass_state(path: Path, *, now: datetime, max_age_s: float) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {"status": "MISSING", "pass": False}
    generated = _parse_ts(payload.get("generated_at") or payload.get("finished_at") or payload.get("checked_at"))
    age_s = None if generated is None else max(0.0, (now - generated).total_seconds())
    status = str(payload.get("status") or "")
    return {
        "status": status,
        "generated_at": generated.isoformat() if generated else None,
        "age_s": age_s,
        "pass": status == "PASS" and age_s is not None and age_s <= float(max_age_s),
    }


def evaluate(
    *,
    handoff_text: str,
    last_commit: dict[str, Any],
    heartbeat: dict[str, Any],
    now: datetime,
    threshold_s: float,
    max_direction_age_s: float = 2 * 60 * 60,
) -> dict[str, Any]:
    heading, block = latest_direction_block(handoff_text)
    has_next = direction_has_nonempty_next(block)
    heading_ts = direction_heading_ts(heading)
    heading_age_s = None if heading_ts is None else max(0.0, (now - heading_ts).total_seconds())
    commit_ts = _parse_ts(last_commit.get("commit_ts"))
    commit_age_s = None if commit_ts is None else max(0.0, (now - commit_ts).total_seconds())
    handoff_parse_failed = bool(
        not heading
        or heading_ts is None
        or heading_age_s is None
        or heading_age_s > float(max_direction_age_s)
    )
    direction_contract_breach = bool(not handoff_parse_failed and not has_next)
    starvation = bool(
        not handoff_parse_failed
        and has_next
        and heartbeat.get("pass")
        and (commit_age_s is None or commit_age_s > float(threshold_s))
    )
    if handoff_parse_failed:
        status = "HANDOFF_PARSE_FAILED"
    elif direction_contract_breach:
        status = "FABLE_DIRECTION_CONTRACT_BREACH"
    elif starvation:
        status = "CODEX_STARVATION"
    else:
        status = "OK"
    firing = status != "OK"
    return {
        "schema_version": 1,
        "kind": "codex_starvation_deadman",
        "flow_stage": "SELF-DEV/LIVE",
        "checked_at": now.isoformat(),
        "status": status,
        "latest_direction_heading": heading,
        "latest_direction_heading_ts": heading_ts.isoformat() if heading_ts else None,
        "latest_direction_heading_age_s": None if heading_age_s is None else round(heading_age_s, 6),
        "latest_direction_max_age_s": float(max_direction_age_s),
        "latest_direction_has_next": has_next,
        "handoff_parse_failed": handoff_parse_failed,
        "direction_contract_breach": direction_contract_breach,
        "starvation_firing": starvation,
        "last_codex_commit": last_commit,
        "last_codex_commit_age_s": None if commit_age_s is None else round(commit_age_s, 6),
        "threshold_s": float(threshold_s),
        "heartbeat": heartbeat,
        "fable_escalation_armed": firing,
        "paper_only": True,
        "live_orders_allowed": False,
        "next_action": (
            "repair HANDOFF parser/freshness; latest Fable DIRECTION heading must be readable and <=2h old"
            if handoff_parse_failed
            else "Fable DIRECTION must carry a non-empty next-list/queue; missing next is an armed contract breach"
            if direction_contract_breach
            else
            "Fable implements the latest non-empty next-list directly if the next pulse still sees starvation"
            if starvation
            else "continue deterministic service check"
        ),
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _append_notify_if_due(
    *,
    handoff_path: Path,
    state_path: Path,
    payload: dict[str, Any],
    now: datetime,
    realert_s: float,
) -> None:
    if payload.get("status") != "CODEX_STARVATION":
        return
    try:
        previous = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError):
        previous = {}
    last_alert = _parse_ts(previous.get("last_alert_at"))
    if last_alert is not None and (now - last_alert).total_seconds() < float(realert_s):
        payload["last_alert_at"] = previous.get("last_alert_at")
        return
    payload["last_alert_at"] = now.isoformat()
    minutes = int(float(payload.get("last_codex_commit_age_s") or 0.0) // 60)
    heading = str(payload.get("latest_direction_heading") or "latest DIRECTION")
    entry = (
        f"\n## {now.strftime('%Y-%m-%dT%H:%M:%SZ')} brainless NOTIFY — CODEX_STARVATION\n"
        f"- service_gap [SELF-DEV/LIVE]: latest Fable next-list non-empty; heartbeat PASS but "
        f"no Codex serving commit for {minutes} min (threshold {int(float(payload['threshold_s']) // 60)} min).\n"
        f"- evidence [SELF-DEV]: {heading}; last_codex_commit="
        f"{(payload.get('last_codex_commit') or {}).get('commit', '')[:12]} "
        f"subject={(payload.get('last_codex_commit') or {}).get('subject', '')!r}.\n"
        "- next [SELF-DEV/LIVE]: Fable direct-implementation escalation is armed if the next pulse still sees starvation.\n"
    )
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    with handoff_path.open("a", encoding="utf-8") as handle:
        handle.write(entry)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--handoff", default=DEFAULT_HANDOFF)
    parser.add_argument("--heartbeat-state", default=DEFAULT_HEARTBEAT_STATE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--threshold-s", type=float, default=45 * 60)
    parser.add_argument("--heartbeat-max-age-s", type=float, default=60 * 60)
    parser.add_argument("--realert-s", type=float, default=45 * 60)
    parser.add_argument("--codex-subject-prefix", default="codex:")
    parser.add_argument("--now", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    now = _parse_ts(args.now) if args.now else _utc_now()
    if now is None:
        now = _utc_now()
    handoff_path = root / args.handoff
    state_path = root / args.state
    try:
        handoff_text = handoff_path.read_text(encoding="utf-8")
    except OSError:
        handoff_text = ""
    payload = evaluate(
        handoff_text=handoff_text,
        last_commit=last_codex_commit(root, subject_prefix=str(args.codex_subject_prefix)),
        heartbeat=heartbeat_pass_state(root / args.heartbeat_state, now=now, max_age_s=float(args.heartbeat_max_age_s)),
        now=now,
        threshold_s=float(args.threshold_s),
    )
    _append_notify_if_due(
        handoff_path=handoff_path,
        state_path=state_path,
        payload=payload,
        now=now,
        realert_s=float(args.realert_s),
    )
    _write_json(state_path, payload)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
