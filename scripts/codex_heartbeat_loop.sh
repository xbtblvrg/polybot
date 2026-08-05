#!/usr/bin/env bash
# Fallback self-running loop when launchd install is not available:
# runs the codex heartbeat every 15 minutes forever. Safe to run alongside
# launchd later — codex_heartbeat.sh has a lock against overlap.
cd "$(dirname "$0")/.."
mkdir -p logs
echo "heartbeat loop started $(date -u +%Y-%m-%dT%H:%M:%SZ) pid $$" >> logs/codex_heartbeat.log
while true; do
  bash scripts/codex_heartbeat.sh
  sleep 900
done
