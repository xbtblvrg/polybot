#!/usr/bin/env bash
# Called by Codex to consult the Fable co-operator.
# Usage: ./scripts/ask_fable.sh [optional specific question]
# Fable audits HANDOFF.md + recent commits, appends a DIRECTION entry,
# and the entry is echoed back so the caller sees it in its own output.
set -euo pipefail
cd "$(dirname "$0")/.."

ASK_FABLE_LOCK_DIR="${ASK_FABLE_LOCK_DIR:-data/research/ask_fable.lock.d}"
if mkdir "$ASK_FABLE_LOCK_DIR" 2>/dev/null; then
  printf '%s\n' "$$" >"$ASK_FABLE_LOCK_DIR/pid"
  trap 'rm -rf "$ASK_FABLE_LOCK_DIR"' EXIT INT TERM
else
  LOCK_PID="$(cat "$ASK_FABLE_LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "ask_fable already running under pid=$LOCK_PID; refusing concurrent brain call"
    exit 75
  fi
  rm -rf "$ASK_FABLE_LOCK_DIR"
  if mkdir "$ASK_FABLE_LOCK_DIR" 2>/dev/null; then
    printf '%s\n' "$$" >"$ASK_FABLE_LOCK_DIR/pid"
    trap 'rm -rf "$ASK_FABLE_LOCK_DIR"' EXIT INT TERM
  else
    echo "ask_fable lock busy; refusing concurrent brain call"
    exit 75
  fi
fi

QUESTION="${*:-}"
CLAUDE_TIMEOUT_S="${ASK_FABLE_CLAUDE_TIMEOUT_S:-2400}"  # operator 2026-07-05: primary brain gets 40min to think
GROK_TIMEOUT_S="${ASK_FABLE_GROK_TIMEOUT_S:-600}"
CODEX_TIMEOUT_S="${ASK_FABLE_CODEX_TIMEOUT_S:-900}"
ASK_FABLE_LOG_DIR="${ASK_FABLE_LOG_DIR:-data/research/ask_fable_provider_logs}"
BRAIN_OUTAGE_STATE="${BRAIN_OUTAGE_STATE:-data/research/brain_outage_state.json}"
AGY_QUOTA_STATE="${AGY_QUOTA_STATE:-data/research/agy_quota_state.json}"
BRAIN_CALL_SUCCEEDED=0
BRAIN_CALL_PROVIDER="none"

PROMPT="Act as co-operator per docs/agents/FABLE_OPERATOR.md. Audit
docs/agents/HANDOFF.md (newest entries) and recent commits, verify claimed
evidence, and resolve every open defect with concrete direction.
Append exactly one DIRECTION entry to docs/agents/HANDOFF.md.

Bounded-audit rule for this timeout-guarded CLI call: prefer concise shell
summaries over full file reads. Use tail/sed windows for HANDOFF, git log
--oneline/-n, git diff --stat/--name-only/--shortstat, and jq summaries for
JSON state. Do not read or print full data/research files, full registry
files, or raw diffs for configs/wallet_copy/wallets.json; that registry can
be thousands of lines and will starve the DIRECTION. If the specific question
contains an evidence packet, decide from that packet plus the bounded checks
needed to verify it. Writing the DIRECTION is the deliverable."

if [[ -n "$QUESTION" ]]; then
  PROMPT="$PROMPT

Specific question from codex, answer it inside the DIRECTION entry:
$QUESTION"
fi

# Portable timeout guard: a hung brain call must not stall the chain
# (macOS has no coreutils `timeout` by default).
run_with_timeout() {
  local secs="$1"; shift
  local out
  local label
  local stamp
  out="$(mktemp)"
  label="${RUN_WITH_TIMEOUT_LABEL:-}"
  if [[ -z "$label" ]]; then
    label="$(basename "${1:-provider}" | tr -c 'A-Za-z0-9_.-' '_')"
  else
    label="$(printf '%s' "$label" | tr -c 'A-Za-z0-9_.-' '_')"
  fi
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  "$@" >"$out" 2>&1 &
  local pid=$!
  ( sleep "$secs"; pkill -TERM -P "$pid" 2>/dev/null; kill -TERM "$pid" 2>/dev/null ) >/dev/null 2>&1 &
  local watchdog=$!
  local rc=0
  wait "$pid" 2>/dev/null || rc=$?
  { kill "$watchdog"; wait "$watchdog"; } 2>/dev/null
  cat "$out"
  mkdir -p "$ASK_FABLE_LOG_DIR"
  cp "$out" "$ASK_FABLE_LOG_DIR/${stamp}_${label}_${pid}_rc${rc}.log"
  rm -f "$out"
  return $rc
}

record_brain_outage_state() {
  local succeeded="$1"
  BRAIN_SUCCEEDED="$succeeded" BRAIN_OUTAGE_STATE="$BRAIN_OUTAGE_STATE" python3 - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path

path = Path(os.environ["BRAIN_OUTAGE_STATE"])
path.parent.mkdir(parents=True, exist_ok=True)
try:
    prior = json.loads(path.read_text())
except Exception:
    prior = {}
succeeded = os.environ.get("BRAIN_SUCCEEDED") == "1"
failures = 0 if succeeded else int(prior.get("consecutive_failures") or 0) + 1
payload = {
    "schema_version": 1,
    "kind": "brain_outage_state",
    "brain_outage": (not succeeded) and failures >= 3,
    "consecutive_failures": failures,
    "updated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    "source": "scripts/ask_fable.sh",
    "next_action": (
        "brainless_ops executes standing mechanical rules until the brain chain recovers"
        if (not succeeded) and failures >= 3
        else "continue normal brain consultation"
    ),
}
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
}

agy_quota_skip_until() {
  AGY_QUOTA_STATE="$AGY_QUOTA_STATE" python3 - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path

path = Path(os.environ["AGY_QUOTA_STATE"])
try:
    payload = json.loads(path.read_text())
except Exception:
    raise SystemExit(0)
until_raw = payload.get("degraded_until")
if not isinstance(until_raw, str) or not until_raw:
    raise SystemExit(0)
try:
    until = dt.datetime.fromisoformat(until_raw.replace("Z", "+00:00"))
except ValueError:
    raise SystemExit(0)
now = dt.datetime.now(dt.timezone.utc)
if until > now:
    print(until_raw)
PY
}

record_agy_quota_state() {
  local output_file="$1"
  AGY_QUOTA_STATE="$AGY_QUOTA_STATE" AGY_OUTPUT_FILE="$output_file" python3 - <<'PY'
import datetime as dt
import json
import os
import re
from pathlib import Path

out_path = Path(os.environ["AGY_OUTPUT_FILE"])
text = out_path.read_text(errors="ignore") if out_path.exists() else ""
lowered = text.lower()
if "quota" not in lowered:
    raise SystemExit(0)
if not any(marker in lowered for marker in ("quota reached", "quota exceeded", "individual quota")):
    raise SystemExit(0)
match = re.search(
    r"Resets in\s+(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?",
    text,
    re.I,
)
if not match:
    raise SystemExit(0)
days = int(match.group(1) or 0)
hours = int(match.group(2) or 0)
minutes = int(match.group(3) or 0)
seconds = int(match.group(4) or 0)
if days == hours == minutes == seconds == 0:
    raise SystemExit(0)
now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
degraded_until = now + dt.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
state_path = Path(os.environ["AGY_QUOTA_STATE"])
state_path.parent.mkdir(parents=True, exist_ok=True)
payload = {
    "schema_version": 1,
    "kind": "agy_quota_state",
    "status": "DEGRADED_QUOTA",
    "observed_at": now.isoformat().replace("+00:00", "Z"),
    "degraded_until": degraded_until.isoformat().replace("+00:00", "Z"),
    "raw_reset": match.group(0),
    "source": "scripts/ask_fable.sh",
    "next_action": "skip AGY fallback attempts until degraded_until, then run one tagged confirmatory smoke",
}
state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
}

# MONEY-TRUTH INJECTION (operator idea, 2026-07-07): every brain call
# opens with the machine-written cash truth — the brain cannot choose
# not to see it. Source: state_digest pnl line (brainless_ops-generated,
# never AI-written).
MONEY_LINE="$(grep -m1 '^pnl:' data/research/state_digest.md 2>/dev/null || echo 'pnl: DIGEST MISSING — treat as defect')"
MONEY_CUT="$(grep -m1 '^generated_at:' data/research/state_digest.md 2>/dev/null | sed 's/^generated_at:[[:space:]]*//' || true)"
MONEY_CUT="${MONEY_CUT:-MISSING}"
# FLOW TRUTH (operator, 2026-07-07): absence must be injected, not
# inferred — the brain cannot detect what has no line. Raw idle seconds
# + deadman verdict + window coverage, machine-computed every call.
if [ -f scripts/read_deadman_flow_banner.py ]; then
  FLOW_LINE="$(python3 scripts/read_deadman_flow_banner.py \
    --state data/research/order_flow_deadman_state.json \
    --stderr-log data/research/ask_fable_deadman_banner_stderr.log \
    --max-age-s "${ASK_FABLE_DEADMAN_MAX_AGE_S:-900}")"
else
  FLOW_LINE='{"status":"INCIDENT_ORDER_FLOW_DEAD_UNVERIFIED","fail_closed_reason":"banner_helper_missing","next_action":"restore scripts/read_deadman_flow_banner.py before rendering flow clear"}'
fi
FLOW_CUT="$(printf '%s' "$FLOW_LINE" | python3 -c 'import json,sys; p=json.load(sys.stdin); print(p.get("checked_at") or p.get("generated_at") or "MISSING")' 2>/dev/null || echo MISSING)"
VOLUME_LINE="$(grep -m1 '^volume:' data/research/state_digest.md 2>/dev/null || echo 'volume: DIGEST MISSING')"
PROMPT="MONEY TRUTH (machine-written, non-negotiable; cut=$MONEY_CUT): $MONEY_LINE | baseline: \$335 topup 2026-07-05 | goal: daily-profitable BTC-5m quant-bot, \$100-300/day.
FLOW TRUTH (machine-written; cut=$FLOW_CUT): $FLOW_LINE | $VOLUME_LINE — if status is INCIDENT_ORDER_FLOW_DEAD or money_anchored_status is FLOW_DEAD_MONEY_ANCHORED, restoring live order flow outranks every other subject in this prompt.
Everything below is secondary to closing these gaps.
$PROMPT"
PROMPT_FILE="$(mktemp)"
printf '%s\n' "$PROMPT" >"$PROMPT_FILE"

# Fable is only replaced when truly necessary (operator, 2026-07-04):
# session limit = hard signal, immediate fallback; any other failure or
# timeout gets ONE retry before the chain moves on.
FABLE_RC=1
FABLE_OUT=""
for attempt in 1 2; do
  FABLE_RC=0
  # Brain model pin (operator, 2026-07-28): fable-cli runs Opus 5.
  # Override with FABLE_MODEL=... if the operator switches again.
  FABLE_MODEL="${FABLE_MODEL:-claude-opus-5}"
  FABLE_OUT="$(run_with_timeout "$CLAUDE_TIMEOUT_S" bash -c 'claude -p --model "$1" --dangerously-skip-permissions --max-turns 40 < "$2"' _ "$FABLE_MODEL" "$PROMPT_FILE" 2>&1)" || FABLE_RC=$?
  echo "$FABLE_OUT"
  if [ $FABLE_RC -eq 0 ]; then
    BRAIN_CALL_PROVIDER="claude"
    break
  fi
  if printf '%s' "$FABLE_OUT" | grep -qi "session limit"; then
    echo "claude (fable) session limit — hard signal, no retry"
    break
  fi
  if [ $attempt -eq 1 ]; then
    echo "claude (fable) attempt 1 failed (rc=$FABLE_RC) — retrying once before any fallback"
    sleep 5
  else
    echo "claude (fable) failed twice (rc=$FABLE_RC) — only now treating as unavailable"
  fi
done

if [ $FABLE_RC -ne 0 ] || printf '%s' "$FABLE_OUT" | grep -qi "session limit"; then
  echo "claude (fable) unavailable — falling back to grok CLI as 1:1 substitute (OP decision 2026-07-04)"
  GROK_PROMPT="You are acting as Fable, the lead operator of this repo, as a
1:1 substitute while the Claude CLI is unavailable. Follow
docs/agents/FABLE_OPERATOR.md exactly — same role, same mandates, same
output format. $PROMPT"
  GROK_RC=1
  FALLBACK_PROVIDER="grok"
  if command -v grok >/dev/null 2>&1; then
    GROK_PROMPT_FILE="$(mktemp)"
    printf '%s\n' "$GROK_PROMPT" >"$GROK_PROMPT_FILE"
    { run_with_timeout "$GROK_TIMEOUT_S" grok --prompt-file "$GROK_PROMPT_FILE" 2>&1; } && {
        GROK_RC=0
        FALLBACK_PROVIDER="grok"
      }
    rm -f "$GROK_PROMPT_FILE"
  else
    echo "grok CLI not installed"
  fi

  if [ $GROK_RC -ne 0 ] && command -v agy >/dev/null 2>&1; then
    # ANTIGRAVITY substitute (operator, 2026-07-13): "agy" CLI wired as
    # tertiary brain — same FABLE_OPERATOR mandate, recursion-guarded.
    AGY_TIMEOUT_S="${AGY_TIMEOUT_S:-600}"
    AGY_PRINT_TIMEOUT="${AGY_PRINT_TIMEOUT:-10m0s}"
    AGY_SKIP_UNTIL="$(agy_quota_skip_until || true)"
    if [[ -n "$AGY_SKIP_UNTIL" ]]; then
      echo "agy skipped (quota until $AGY_SKIP_UNTIL)"
    else
      echo "grok unavailable — trying antigravity (agy) as tertiary Fable substitute (operator order 2026-07-13)"
      AGY_PROMPT="MODE: SUBSTITUTE-FABLE
AUTHORITY: full FABLE_OPERATOR mandate; SUBSTITUTE AUTHORITY applies; append a DIRECTION entry.
NO_SECRETS: do not request or print .env content, private keys, API keys, or credentials.

You are substituting as Fable per docs/agents/FABLE_OPERATOR.md (read it first, follow it literally). $PROMPT"
	      AGY_RC=1
	      AGY_OUTPUT_FILE="$(mktemp)"
	      AGY_PROMPT_FILE="$(mktemp)"
	      printf '%s\n' "$AGY_PROMPT" >"$AGY_PROMPT_FILE"
	      AGY_OUT="$(RUN_WITH_TIMEOUT_LABEL="agy_SUBSTITUTE-FABLE" run_with_timeout "$AGY_TIMEOUT_S" bash -c 'agy --add-dir "$1" --dangerously-skip-permissions --print-timeout "$2" --print < "$3"' _ "$PWD" "$AGY_PRINT_TIMEOUT" "$AGY_PROMPT_FILE" 2>&1)" && AGY_RC=0 || AGY_RC=$?
	      printf '%s\n' "$AGY_OUT"
	      printf '%s\n' "$AGY_OUT" >"$AGY_OUTPUT_FILE"
	      if [ $AGY_RC -ne 0 ]; then
	        AGY_OUT_2="$(RUN_WITH_TIMEOUT_LABEL="agy_SUBSTITUTE-FABLE" run_with_timeout "$AGY_TIMEOUT_S" bash -c 'agy --add-dir "$1" --dangerously-skip-permissions --print-timeout "$2" --prompt < "$3"' _ "$PWD" "$AGY_PRINT_TIMEOUT" "$AGY_PROMPT_FILE" 2>&1)" && AGY_RC=0 || AGY_RC=$?
	        printf '%s\n' "$AGY_OUT_2"
	        printf '%s\n' "$AGY_OUT_2" >>"$AGY_OUTPUT_FILE"
	      fi
      if [ $AGY_RC -ne 0 ]; then
        record_agy_quota_state "$AGY_OUTPUT_FILE" || true
      fi
	      rm -f "$AGY_OUTPUT_FILE" "$AGY_PROMPT_FILE"
      if [ $AGY_RC -eq 0 ]; then
        AGY_RC=0
        FALLBACK_PROVIDER="agy"
        GROK_RC=0
      fi
    fi
  fi
  if [ $GROK_RC -ne 0 ]; then
    echo "grok/agy unavailable — falling back to codex/gpt as quaternary Fable substitute (OP decision 2026-07-04)"
    if [ "${ASK_FABLE_DEPTH:-0}" != "0" ]; then
      echo "recursion guard: already inside a brain-substitute session; proceed per the latest fable DIRECTION's priority order literally"
    elif command -v codex >/dev/null 2>&1; then
      GPT_PROMPT="You are acting as Fable, the lead operator of this repo,
as the TERTIARY substitute brain while both Claude and Grok CLIs are
unavailable. Follow docs/agents/FABLE_OPERATOR.md exactly — same role,
mandates, and output format. Do NOT run scripts/ask_fable.sh in this
session (you ARE the brain right now). $PROMPT"
      CODEX_RC=1
      GPT_PROMPT_FILE="$(mktemp)"
      printf '%s\n' "$GPT_PROMPT" >"$GPT_PROMPT_FILE"
      ASK_FABLE_DEPTH=1 run_with_timeout "$CODEX_TIMEOUT_S" bash -c 'codex exec --dangerously-bypass-approvals-and-sandbox - < "$1"' _ "$GPT_PROMPT_FILE" 2>&1 && CODEX_RC=0 \
        || echo "codex/gpt fallback also failed; proceed per the latest fable DIRECTION's priority order literally"
      rm -f "$GPT_PROMPT_FILE"
      if [ $CODEX_RC -eq 0 ]; then
        BRAIN_CALL_SUCCEEDED=1
        BRAIN_CALL_PROVIDER="codex"
      fi
    else
      echo "codex CLI not found; proceed per the latest fable DIRECTION's priority order literally"
    fi
  else
    BRAIN_CALL_SUCCEEDED=1
    BRAIN_CALL_PROVIDER="$FALLBACK_PROVIDER"
  fi
else
  BRAIN_CALL_SUCCEEDED=1
  BRAIN_CALL_PROVIDER="claude"
fi

record_brain_outage_state "$BRAIN_CALL_SUCCEEDED"
rm -f "$PROMPT_FILE"
if [[ -s data/research/cli_versions_state.json ]]; then
  SMOKE_RC=1
  SMOKE_SANITY="FAIL"
  if [[ "$BRAIN_CALL_SUCCEEDED" == "1" ]]; then
    SMOKE_RC=0
    SMOKE_SANITY="PASS"
  fi
  python3 scripts/record_cli_versions.py smoke \
    --provider "$BRAIN_CALL_PROVIDER" \
    --rc "$SMOKE_RC" \
    --output-sanity "$SMOKE_SANITY" >/dev/null || true
fi

echo
echo "===== latest HANDOFF entries ====="
tail -n 60 docs/agents/HANDOFF.md
