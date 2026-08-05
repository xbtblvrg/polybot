#!/usr/bin/env bash
set -euo pipefail

ROOT="${POLYMARKET_AGENT_ROOT:-/Users/belavarga/claudecode/polymarket-agent}"
HANDOFF="${HANDOFF_PATH:-$ROOT/docs/agents/HANDOFF.md}"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 '<expected heading prefix>' < status.md" >&2
  exit 2
fi

expected_heading="$1"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
cat > "$tmp"

if ! head -n 1 "$tmp" | grep -F -- "$expected_heading" >/dev/null; then
  echo "status stdin must start with expected heading: $expected_heading" >&2
  exit 2
fi

if [[ "$expected_heading" =~ replay\ advanced\ to\ ([0-9]+)/963 ]]; then
  requested_cursor="${BASH_REMATCH[1]}"
  state_path="$ROOT/data/research/wallet_market_cohort_replay_state.json"
  if [[ ! -f "$state_path" ]]; then
    echo "cohort cursor guard missing state file: $state_path" >&2
    exit 1
  fi
  current_cursor="$(
    python3 - "$state_path" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
print(len(payload.get("completed_wallets") or []))
PY
  )"
  if (( current_cursor < requested_cursor )); then
    echo "cohort cursor guard rejected stale STATUS: heading cursor $requested_cursor exceeds current state cursor $current_cursor" >&2
    exit 1
  fi
  max_prior_cursor="$(
    python3 - "$HANDOFF" <<'PY'
import re
import sys
pattern = re.compile(r"^## .*codex STATUS .*replay advanced to ([0-9]+)/963")
max_cursor = 0
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line in handle:
        match = pattern.search(line)
        if match:
            max_cursor = max(max_cursor, int(match.group(1)))
print(max_cursor)
PY
  )"
  if (( requested_cursor <= max_prior_cursor )); then
    echo "cohort cursor guard rejected stale/duplicate STATUS: heading cursor $requested_cursor is not above prior max $max_prior_cursor" >&2
    exit 1
  fi
fi

{
  printf '\n'
  cat "$tmp"
} >> "$HANDOFF"

last_heading="$(grep -n '^## ' "$HANDOFF" | tail -1 | cut -d: -f2-)"
if [[ "$last_heading" != "$expected_heading"* ]]; then
  echo "handoff append verification failed: newest heading is '$last_heading'" >&2
  exit 1
fi
