#!/usr/bin/env bash
# Proactive Fable steering pulse: the brain drives on its own schedule,
# independent of codex asks. Runs the canonical gateway with a steering
# mandate; the brain chain (claude -> grok -> agy -> gpt) and timeouts apply.
set -uo pipefail
cd "$(dirname "$0")/.."

export PATH="$HOME/.local/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

LOCKDIR="${FABLE_PULSE_LOCKDIR:-/tmp/polymarket_fable_pulse.lock}"
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$LOCKDIR"' EXIT

mkdir -p logs
PULSE_LOG="${FABLE_PULSE_LOG:-logs/fable_pulse.log}"
ASK_FABLE_LOCK_DIR="${ASK_FABLE_LOCK_DIR:-data/research/ask_fable.lock.d}"
PULSE_INTERVAL_S="${FABLE_PULSE_INTERVAL_S:-3600}"
PULSE_BUSY_RETRY_S="${FABLE_PULSE_BUSY_RETRY_S:-300}"

lock_mtime_s() {
  stat -f %m "$1" 2>/dev/null || stat -c %Y "$1" 2>/dev/null || echo 0
}

prepare_ask_fable_lock() {
  local lock_dir="$1"
  local pid_file="$lock_dir/pid"
  local pid=""
  local now_s
  local mtime_s
  local age_s
  if [[ ! -d "$lock_dir" ]]; then
    return 0
  fi
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  now_s="$(date -u +%s)"
  mtime_s="$(lock_mtime_s "$lock_dir")"
  age_s=$((now_s - mtime_s))
  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    echo "$(date -u +%FT%TZ) PULSE_STALE_LOCK_REMOVED pid=${pid:-missing} age_s=$age_s" >> "$PULSE_LOG"
    rm -rf "$lock_dir"
    return 0
  fi
  if (( age_s >= PULSE_INTERVAL_S )); then
    echo "$(date -u +%FT%TZ) PULSE_STALE_LOCK_TAKEOVER pid=$pid age_s=$age_s" >> "$PULSE_LOG"
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2
    rm -rf "$lock_dir"
    return 0
  fi
  echo "$(date -u +%FT%TZ) PULSE_BUSY_RETRY pid=$pid age_s=$age_s retry_s=$PULSE_BUSY_RETRY_S" >> "$PULSE_LOG"
  sleep "$PULSE_BUSY_RETRY_S"
  if [[ ! -d "$lock_dir" ]]; then
    return 0
  fi
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  now_s="$(date -u +%s)"
  mtime_s="$(lock_mtime_s "$lock_dir")"
  age_s=$((now_s - mtime_s))
  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    echo "$(date -u +%FT%TZ) PULSE_STALE_LOCK_REMOVED_AFTER_RETRY pid=${pid:-missing} age_s=$age_s" >> "$PULSE_LOG"
    rm -rf "$lock_dir"
    return 0
  fi
  if (( age_s >= PULSE_INTERVAL_S )); then
    echo "$(date -u +%FT%TZ) PULSE_STALE_LOCK_TAKEOVER_AFTER_RETRY pid=$pid age_s=$age_s" >> "$PULSE_LOG"
    kill -TERM "$pid" 2>/dev/null || true
    sleep 2
    rm -rf "$lock_dir"
    return 0
  fi
  echo "$(date -u +%FT%TZ) PULSE_DEFER_BUSY pid=$pid age_s=$age_s" >> "$PULSE_LOG"
  exit 0
}

prepare_ask_fable_lock "$ASK_FABLE_LOCK_DIR"
if [[ "${FABLE_PULSE_PREPARE_ONLY:-0}" == "1" ]]; then
  exit 0
fi
./scripts/ask_fable.sh "PROACTIVE STEERING PULSE (scheduled, hourly): you were
not asked by codex — you are driving. Audit the freshest state (ledger PnL
delta, volume KPI: windows traded vs total, active-set membership count,
coverage idle hours, pending gates/deadlines like the consensus 48h clock,
open defects). DECIDE everything decidable: fire overdue mechanical rules,
adjust the queue, order the next highest profit-per-hour work. If and only
if there is genuinely nothing to decide and no KPI breach, append NO entry
(write the single line PULSE_OK to $PULSE_LOG instead)." \
  >> "$PULSE_LOG" 2>&1 \
  || echo "$(date -u +%FT%TZ) pulse failed" >> "$PULSE_LOG"
