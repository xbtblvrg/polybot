#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KEY="${1:-}"
case "$KEY" in
  960bf404c1)
    WALLET="0x960bf404c1eca257411203164357a925e33fae8d"
    WALLET_NAME="leaderboard_crypto_960bf404c1"
    ;;
  04a162e06d)
    WALLET="0x04a162e06d1e82745a08b95e247bf3a965693527"
    WALLET_NAME="leaderboard_crypto_04a162e06d"
    ;;
  *)
    echo "usage: $0 {960bf404c1|04a162e06d}" >&2
    exit 2
    ;;
esac

PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

PREFIX="data/research/fast_track_replacement_${KEY}"

exec "$PYTHON_BIN" scripts/run_wallet_live_tracker.py \
  --registry configs/wallet_copy/wallets.json \
  --wallet-address "$WALLET" \
  --wallet-name "$WALLET_NAME" \
  --state "${PREFIX}_live_tracking_state.json" \
  --event-log "${PREFIX}_live_tracking_events.jsonl" \
  --paper-state "${PREFIX}_paper_state.json" \
  --paper-event-log "${PREFIX}_paper_events.jsonl" \
  --tracker-time-replay-paper-state "${PREFIX}_tracker_time_replay_paper_state.json" \
  --tracker-time-replay-paper-event-log "${PREFIX}_tracker_time_replay_paper_events.jsonl" \
  --single-wallet-exact-copy-paper-state "${PREFIX}_single_wallet_exact_copy_paper_state.json" \
  --single-wallet-exact-copy-paper-event-log "${PREFIX}_single_wallet_exact_copy_paper_events.jsonl" \
  --profit-policy-state "${PREFIX}_profit_policy_state.json" \
  --track-blocked-profit-policy \
  --profit-policy-candidate-only \
  --limit "${FAST_TRACK_LIMIT:-20}" \
  --pages "${FAST_TRACK_PAGES:-2}" \
  --data-api-timeout-s "${FAST_TRACK_DATA_API_TIMEOUT_S:-1.5}" \
  --data-api-retries "${FAST_TRACK_DATA_API_RETRIES:-1}" \
  --data-api-trade-query-keys "${FAST_TRACK_DATA_API_KEYS:-user,proxyWallet}" \
  --no-include-activity \
  --max-poll-runtime-s "${FAST_TRACK_MAX_POLL_RUNTIME_S:-30}" \
  --iterations "${FAST_TRACK_ITERATIONS:-2000}" \
  --poll-interval-s "${FAST_TRACK_POLL_INTERVAL_S:-5}" \
  --max-runtime-s "${FAST_TRACK_MAX_RUNTIME_S:-7500}" \
  --max-copyability-event-age-s "${FAST_TRACK_MAX_EVENT_AGE_S:-30}" \
  --wallet-fraction 0.10 \
  --max-order-usd 8.0 \
  --min-order-usd 1.0 \
  --enable-clob-books \
  --admission-mode \
  --strict-mirror-coverage \
  --no-use-profit-search-scope \
  --parallel-data-api-sources \
  --parallel-wallet-fetches 1 \
  --clob-timeout-s "${FAST_TRACK_CLOB_TIMEOUT_S:-1.0}" \
  --gamma-timeout-s "${FAST_TRACK_GAMMA_TIMEOUT_S:-1.0}" \
  --enable-onchain-receipts \
  --onchain-timeout-s "${FAST_TRACK_ONCHAIN_TIMEOUT_S:-1.0}" \
  --market-ws-jsonl data/research/clob_market_ws_events.jsonl \
  --max-book-slippage-bps 250 \
  --paper-retain-orders 5000 \
  --paper-retain-lifecycle-events 15000 \
  --paper-retain-dedupe-ids 250000 \
  --no-use-global-tracker-lock
