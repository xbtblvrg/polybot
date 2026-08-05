#!/usr/bin/env bash
# Self-running development heartbeat: launchd runs this every 15 minutes so
# the Codex+Fable loop never depends on a human keeping an app open.
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:$HOME/bin:$PATH"
cd "$(dirname "$0")/.."

LOCKDIR=/tmp/polymarket_codex_heartbeat.lock
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  exit 0  # previous heartbeat still running
fi
SERVING_MUTEX_TOKEN=/tmp/polymarket_codex_serving_run.codex_heartbeat.token
SERVING_MUTEX_ACQUIRED=0
cleanup() {
  if [ "$SERVING_MUTEX_ACQUIRED" = "1" ]; then
    python3 scripts/codex_serving_mutex.py \
      --lock-dir /tmp/polymarket_codex_serving_run.lock \
      release --token-file "$SERVING_MUTEX_TOKEN" >>"${LOG:-/dev/null}" 2>&1 || true
  fi
  rmdir "$LOCKDIR" || true
}
trap cleanup EXIT

mkdir -p logs
LOG=logs/codex_heartbeat.log
python3 scripts/rotate_wallet_copy_runtime_logs.py \
  --log "$LOG" \
  --state data/research/codex_heartbeat_log_rotation_state.json \
  --event-log data/research/codex_heartbeat_log_rotation_events.jsonl \
  --max-bytes $((100 * 1024 * 1024)) \
  --keep-tail-bytes $((20 * 1024 * 1024)) >/dev/null 2>&1 || true
echo "=== heartbeat $(date -u +%Y-%m-%dT%H:%M:%SZ) ===" >>"$LOG"

if python3 scripts/codex_serving_mutex.py \
  --lock-dir /tmp/polymarket_codex_serving_run.lock \
  acquire --owner codex_heartbeat.sh --holder-pid "$$" --token-file "$SERVING_MUTEX_TOKEN" >>"$LOG" 2>&1; then
  SERVING_MUTEX_ACQUIRED=1
  export CODEX_SERVING_MUTEX_HELD=1
else
  echo "codex serving mutex held; skip-fast at $(date -u +%H:%M:%SZ)" >>"$LOG"
  exit 0
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "codex CLI not found in PATH" >>"$LOG"
  exit 1
fi

PROMPT_FILE=docs/agents/HEARTBEAT_PROMPT.md
if [ ! -s "$PROMPT_FILE" ]; then
  echo "missing heartbeat prompt: $PROMPT_FILE" >>"$LOG"
  exit 1
fi
PROMPT="Read docs/agents/HEARTBEAT_PROMPT.md in /Users/belavarga/claudecode/polymarket-agent and execute it literally as this run's instructions."

python3 scripts/record_codex_serving_heartbeat.py \
  --task codex_heartbeat_start \
  --note "codex exec start" >>"$LOG" 2>&1 || true

# Fixed-clock 0x82c8 seat adjudication. Exit 2 is the expected pre-deadline
# NOT_DUE result; the first 15-minute heartbeat at/after the timestamp executes
# the monotone PARK or all-pass handoff without consulting incident state.
python3 scripts/execute_82c8_terminal_decision.py --execute >>"$LOG" 2>&1
ACTUATOR_RC=$?
if [ "$ACTUATOR_RC" -ne 0 ] && [ "$ACTUATOR_RC" -ne 2 ]; then
  echo "82c8 terminal actuator failed at $(date -u +%H:%M:%SZ) rc=$ACTUATOR_RC" >>"$LOG"
fi

# The second 0x82c8 clock is judged by concentration robustness and the
# live cell's loss state, never by its raw paper PnL alone. Exit 2 is the
# expected pre-deadline result.
python3 scripts/report_two_arm_concentration_decomposition.py --execute >>"$LOG" 2>&1
ACTUATOR_RC=$?
if [ "$ACTUATOR_RC" -ne 0 ] && [ "$ACTUATOR_RC" -ne 2 ]; then
  echo "82c8 forward-seat actuator failed at $(date -u +%H:%M:%SZ) rc=$ACTUATOR_RC" >>"$LOG"
fi

(
  while true; do
    sleep 900
    python3 scripts/record_codex_serving_heartbeat.py \
      --task codex_heartbeat_long_task \
      --note "codex exec still running" >>"$LOG" 2>&1 || true
  done
) &
MARKER_PID=$!

codex exec --dangerously-bypass-approvals-and-sandbox \
  "$PROMPT" >>"$LOG" 2>&1
CODEX_RC=$?
kill "$MARKER_PID" >/dev/null 2>&1 || true
wait "$MARKER_PID" >/dev/null 2>&1 || true
if [ "$CODEX_RC" -ne 0 ]; then
  echo "codex exited non-zero at $(date -u +%H:%M:%SZ)" >>"$LOG"
fi

python3 scripts/codex_starvation_deadman.py >>"$LOG" 2>&1 \
  || echo "codex starvation deadman failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
