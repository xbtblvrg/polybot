#!/usr/bin/env bash
set -euo pipefail

LOCK_DIR="${WALLET_COPY_RESEARCH_DEVELOPMENT_LOCK:-/tmp/wallet_copy_research_development_serving.lock}"
ROOT="${POLYMARKET_AGENT_ROOT:-/Users/belavarga/claudecode/polymarket-agent}"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  if [[ -f "$LOCK_DIR/pid" ]]; then
    pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
      echo "research-development serving lock held by live pid $pid" >&2
      exit 75
    fi
    echo "defect | stale research-development serving lock pid=$pid | attempts: kill -0 failed, lock metadata read, reclaim requested | next=reclaim stale lock and continue under live holder pid" >&2
  else
    echo "defect | malformed research-development serving lock without pid | attempts: lock dir read, pid missing, reclaim requested | next=reclaim stale lock and continue under live holder pid" >&2
  fi
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi

cleanup() {
  rm -rf "$LOCK_DIR"
}
trap cleanup EXIT INT TERM

printf '%s\n' "$$" > "$LOCK_DIR/pid"
date -u +%Y-%m-%dT%H:%M:%SZ > "$LOCK_DIR/started_at"
git -C "$ROOT" log -1 --format=%H > "$LOCK_DIR/start_commit" 2>/dev/null || true

"$@"
