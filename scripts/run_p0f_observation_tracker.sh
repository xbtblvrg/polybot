#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

exec "$PYTHON_BIN" scripts/run_wallet_live_tracker.py \
  --registry data/research/wallet_copy_active_hotlane_registry.json \
  --wallet-address 0xa6896d11f76dfa2820662c1f441496f51553559b \
  --wallet-address 0x927f7694de44d19a72bce76254e628d1c141d215 \
  --wallet-address 0xa3e0985f2d0b3209a52f171660287863690d095d \
  --wallet-address 0x2d7c9298b64713de86402bd8a41695e31865a945 \
  --wallet-address 0xf418d3a1a941292f9c8707d62a14980c5beb95a3 \
  --wallet-address 0xc5391c6dfda1174e456b1bc7e05eb9d0179673d1 \
  --wallet-address 0xeebde7a0e019a63e6b476eb425505b7b3e6eba30 \
  --wallet-address 0x40138697bf1a0d655593f3be6237d60c1dc7ab35 \
  --wallet-address 0xc50d0f25cb8eafadcf7059b6a8f4e2542ab40de0 \
  --wallet-address 0x4d8bc628487bbc9931b4d039e6a7529b8ae1a00d \
  --wallet-address 0x86de0516011e2ad7ce3abdc13577de8608db55c3 \
  --wallet-address 0x80be724a3511017be131fdcd188296128fea7ee7 \
  --wallet-address 0xf54209347a46f0f19fbd011efe9d0589b9dd4791 \
  --state data/research/p0f_observation_live_tracking_state.json \
  --event-log data/research/p0f_observation_live_tracking_events.jsonl \
  --paper-state data/research/p0f_observation_paper_state.json \
  --paper-event-log data/research/p0f_observation_paper_events.jsonl \
  --tracker-time-replay-paper-state data/research/p0f_observation_tracker_time_replay_paper_state.json \
  --tracker-time-replay-paper-event-log data/research/p0f_observation_tracker_time_replay_paper_events.jsonl \
  --single-wallet-exact-copy-paper-state data/research/p0f_observation_single_wallet_paper_state.json \
  --single-wallet-exact-copy-paper-event-log data/research/p0f_observation_single_wallet_paper_events.jsonl \
  --iterations 2000 \
  --poll-interval-s 5 \
  --max-runtime-s 7500 \
  --max-poll-runtime-s 60 \
  --limit 100 \
  --pages 1 \
  --data-api-timeout-s 2 \
  --data-api-retries 1 \
  --parallel-wallet-fetches 4 \
  --enable-clob-books \
  --admission-mode \
  --strict-mirror-coverage \
  --no-use-profit-search-scope \
  --max-copyability-event-age-s 10 \
  --max-book-slippage-bps 150 \
  --min-copyability-clob-fill-ratio 0.999 \
  --wallet-fraction 0.10 \
  --max-order-usd 8.0 \
  --min-order-usd 1.0 \
  --market-ws-jsonl data/research/clob_market_ws_events.jsonl \
  --no-use-global-tracker-lock
