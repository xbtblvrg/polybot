#!/usr/bin/env bash
# CANONICAL live guard launcher — THE single documented start command.
# (fable 2026-07-10: the guard's start flags lived only in operational
# practice; a bare start would come up PAPER-mode = silent live halt.)
set -euo pipefail
cd "$(dirname "$0")/.."
# Env parity with the DEPLOYED launchd plist
# (~/Library/LaunchAgents/com.belavarga.polymarket.wallet-copy-live-guard.plist,
# mirrored at launchd/com.belavarga.polymarket.wallet-copy-live-guard.plist):
# starting the guard without these silently changes live chase/price-band
# behavior even with identical argv.
export PYTHONUNBUFFERED=1
export WALLET_COPY_CHASE_MAX_PRICE=1.0
export WALLET_COPY_MAX_BUY_PRICE=0
export WALLET_COPY_MAX_CHASE_TICKS=1
export WALLET_COPY_PRICE_BAND_DECISION_SINCE=2026-07-04T22:22:26Z
export WALLET_COPY_PRICE_BAND_DECISION_MAX_PRICE=0.32
export WALLET_COPY_PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S=60
exec /opt/homebrew/Cellar/python@3.14/3.14.2_1/Frameworks/Python.framework/Versions/3.14/Resources/Python.app/Contents/MacOS/Python \
  scripts/run_wallet_copy_live_guard.py \
  --operator-approval-id OP-LIVE-20260703-BELA \
  --execute-live \
  --explicit-live-operator-go \
  --live-orders-allowed \
  --rtds-tail-bytes 33554432 \
  --rtds-cold-tail-bytes 33554432 \
  --rtds-offset-state data/research/wallet_copy_history_state.json.rotation_d97.rtds_offset.json \
  --iterations 0 \
  --sleep-s 0.5 \
  --pipeline-timeout-s 60 \
  --live-timeout-s 60 \
  --max-event-age-s 30 \
  --live-build-max-observed-age-s 30 \
  --max-intents 6 \
  --min-live-order-usd 1.0 \
  --wallet-fraction 0.20 \
  --max-order-usd 8.0 \
  --alpha-decay-report data/research/alpha_decay_report.json \
  --enable-drift-buffer \
  --max-drift-buffer-price 0.05 \
  --enable-maker-fallback \
  --copy-model drip \
  --drip-min-tranche-usd 1.0 \
  --drip-max-tranche-usd 2.5 \
  --drip-max-tranches-per-window 12 \
  --per-window-fill-cap 1 \
  --inventory-late-window-stop-s 5 \
  --inventory-max-converge-orders-per-window 6 \
  --inventory-best-ask-timeout-s 1.0 \
  --inventory-future-window-lookahead-s 7200 \
  --price-band-decision-since 2026-07-04T22:22:26Z \
  --active-set-dataapi-poller-every-n-cycles 4 \
  --active-set-live-execution-probes-every-n-cycles 16 \
  --active-set-runtime-refresh-every-n-cycles 16 \
  --cap-step-revert-every-n-cycles 16 \
  --active-set-rtds-premerge-member-limit 1 \
  --post-live-reporting-every-n-cycles 8 \
  --shadow-lanes-every-n-cycles 16 \
  --self-feed-ledger-diff-every-n-cycles 16 \
  --no-active-set-dataapi-poller-disable-source-base-overrides \
  --cross-exchange-promoted-cell-state data/research/btc5m_promoted_cell_active_selector.json \
  --cross-exchange-promoted-cell-sources data/research/btc5m_cross_exchange_promoted_cell_latest.json,data/research/btc5m_multivenue_ttl_passive_residual_selector.json,data/research/btc5m_multivenue_ttl_maker_first_residual_selector.json \
  --no-cross-exchange-live-actuator \
  --no-e5-live-actuator
