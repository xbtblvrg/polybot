#!/usr/bin/env bash
# Alternating Codex <-> Fable operator loop.
# Usage: ./scripts/agent_loop.sh [rounds]   (default 5)
set -euo pipefail
cd "$(dirname "$0")/.."

ROUNDS="${1:-5}"
CODEX_PROMPT="Read docs/agents/HEARTBEAT_PROMPT.md in /Users/belavarga/claudecode/polymarket-agent and execute it literally as this run's instructions."

for ((i = 1; i <= ROUNDS; i++)); do
  echo "=== round $i/$ROUNDS: codex ==="
  codex exec --dangerously-bypass-approvals-and-sandbox "$CODEX_PROMPT" || echo "codex exited non-zero, continuing"

  echo "=== round $i/$ROUNDS: fable operator ==="
  claude -p --model "${FABLE_MODEL:-claude-opus-5}" \
    "Act as co-operator per docs/agents/FABLE_OPERATOR.md. Audit
     docs/agents/HANDOFF.md and recent commits, verify evidence, resolve
     every open defect with concrete direction, append your DIRECTION entry
     to HANDOFF.md." \
    --dangerously-skip-permissions || echo "claude exited non-zero, continuing"
done

echo "Loop finished. Review docs/agents/HANDOFF.md and git log."
