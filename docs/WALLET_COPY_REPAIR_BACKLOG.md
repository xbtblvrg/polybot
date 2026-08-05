# Wallet Copy Repair Backlog

This backlog captures non-green wallet-copy repair items that must stay visible
until they are fixed with measured evidence. It is not a live-readiness waiver.

## 2026-06-27 Source Route And Current-Poll Proof

Status: CORRECTION

Current blocker:

- Polymarket Data API, Gamma, and CLOB direct routes reset locally across
  requests, curl, and httpx variants, while the internet control route passes.
- Active hot-lane rolling evidence can show CLOB-backed filled BUY copies, but
  candidate-forward current-poll proof is still empty or runtime-limited.
- The best multi-wallet inventory profit candidate is positive ROI but below
  70% WR/validation WR, below 100 resolved orders, one order per window, and
  fallback-only for fill evidence.

Required repairs:

1. Add explicit route provenance to every source request.
   Files/functions:
   - `src/wallet_copy/http_client.py::PolymarketHttpClient.request`
   - `scripts/probe_polymarket_source_routes.py`
   Metrics:
   - `route_report_id`
   - `request_fingerprint`
   - `request_role`
   - `attempt_count`
   - `reset_attempt_count`
   - `elapsed_ms_total`
   Verify:
   - `python3 scripts/probe_polymarket_source_routes.py --output data/research/wallet_copy_source_route_state.json --timeout-s 10 --print`
   Progress:
   - 2026-06-27T00:40Z heartbeat added route provenance fields to the shared
     client and standalone source-route probe, including request role,
     fingerprint, route report id, attempt count, reset count, elapsed total,
     and query param keys.
   - Verified with `python3 -m pytest -q` and `python3 -m compileall -q src scripts tests`.
   - Remaining blocker: source route still resets without a configured alternate
     proxy/base URL; provenance improves diagnosis but does not make it green.

2. Configure and measure an alternate Polymarket source route instead of
   treating reset endpoints as copy-ready.
   Inputs:
   - `POLYMARKET_SOURCE_PROXY_URL`
   - `POLYMARKET_HTTPS_PROXY`
   - `POLYMARKET_DATA_API_BASE_URL`
   - `POLYMARKET_GAMMA_API_BASE_URL`
   - `POLYMARKET_CLOB_API_BASE_URL`
   Verify:
   - `python3 scripts/probe_polymarket_source_routes.py --output data/research/wallet_copy_source_route_state.json --timeout-s 10 --print`

3. Split candidate-forward seed/history work from current-poll proof work.
   Files/functions:
   - `scripts/run_wallet_copy_autonomous_repair.py::_candidate_forward_tracker_command`
   Metrics:
   - `candidate_forward_poll_runtime_status`
   - `runtime_limited_before_wallet_fetch`
   - `runtime_limited_before_clob_book`
   - `candidate_forward_current_poll_invalidated_by_runtime_limit`
   Verify:
   - `python3 scripts/run_wallet_copy_autonomous_repair.py --command-timeout-s 900 --candidate-forward-probe-ranks 8 --candidate-forward-probe-iterations 5 --candidate-forward-probe-max-runtime-s 180 --candidate-forward-probe-max-poll-runtime-s 45 --hot-path-data-api-trade-query-keys user`

4. Attach CLOB route provenance to current-poll proof rows.
   Files/functions:
   - `src/wallet_copy/live_tracker.py::CLOBMarketClient.get_book`
   - `src/wallet_copy/live_tracker.py::_enrich_event`
   - `src/wallet_copy/copy_efficiency.py::score_copy_event`
   - `src/wallet_copy/profit_engine.py::_runtime_proof_rows_from_truth`
   Metrics:
   - `wallet_route_class`
   - `clob_route_class`
   - `route_report_id`
   - `book_hash`
   Verify:
   - `python3 -m pytest -q tests/test_wallet_copy_core.py -k "source_route or current_poll or runtime_proof"`
   Progress:
   - 2026-06-27T00:55Z heartbeat carried wallet/Data API route
     provenance into current-poll evidence, copy-efficiency score rows, and
     runtime proof index rows. It also made source-route truth visible in the
     profit-engine live-readiness certificate, so route reset cannot be hidden
     behind generic profit/copy blockers.
   - Verified with focused route/proof/live-readiness tests, full
     `python3 -m pytest -q`, and `python3 -m compileall -q src scripts tests`.
   - Remaining blocker: CLOB book fetch still needs first-class route-report
     capture because `CLOBMarketClient.get_book` is still a direct JSON fetch
     wrapper.
   - 2026-06-27T01:13Z heartbeat added first-class CLOB book route-report
     capture through `CLOBMarketClient.get_book`, propagated CLOB route
     provenance into `tracking_evidence.clob_book`, copy-efficiency score rows,
     and runtime proof index rows.
   - Verified with focused CLOB route/proof tests, full `python3 -m pytest -q`,
     and `python3 -m compileall -q src scripts tests`.
   - Remaining blocker: measured source route still reports Polymarket
     Data/Gamma/CLOB `DIRECT_RESET`; this requires an alternate route/proxy/base
     URL or relay, not a gate relaxation.

5. Make paper/live parity a proof-grade ledger field.
   Files/functions:
   - `src/wallet_copy/execution.py::build_copy_intent_parity_capsule`
   - `src/wallet_copy/paper.py::_apply_intent_to_state`
   - `src/wallet_copy/profit_engine.py::_live_readiness_certificate`
   Metrics/blockers:
   - `parity_digest`
   - `token_mapping_guard_status`
   - `current_poll_parity_capsule_missing`
   - `token_mapping_guard_missing`
   - `paper_order_lifecycle_missing`
   Verify:
   - `python3 -m pytest -q tests/test_wallet_copy_core.py -k "parity or token_mapping or live_readiness"`

Live-ready remains false until these repairs produce candidate-specific,
current-poll, CLOB-backed CopyIntent lifecycle evidence with profitable resolved
paper results and zero fallback, reject, miss, or parity defects.
