# Temporal evidence core stall — bounded cause report

Generated: 2026-07-30  
Flow stage: LEARN/PROMOTE  
Scope: cause and exact stalled write point only; paper-only; no rebuild, backfill, threshold change, or freeze re-derivation.

## Finding

The temporal evidence core stopped advancing because its research input path became orphaned, not because a merge crashed or refused an atomic write.

The temporal registry reads `data/research/wallet_copy_history_state.json` plus the files named by `data/research/temporal_supplemental_history_manifest.json`. The primary research history's last filesystem write is `2026-07-23T22:09:49Z`. The supplemental manifest's last successful batch is `20260724T193401Z`; it was atomically written at `2026-07-24T19:38:45.598436Z` with 268 files after the documented one-shot command `replay_source_active_policy_history.py --max-wallets 110` replayed 46 wallets without API errors.

The only code that appends this manifest is `_append_manifest()` in `scripts/replay_source_active_policy_history.py`, whose final mutation is `atomic_write_json(manifest_path, payload)` at line 198. No LaunchAgent, repository scheduler, config, or running process invokes that replay. Therefore there was no later manifest write attempt to crash or refuse: execution stopped upstream of the writer because the producer is manual and was not scheduled again.

Meanwhile the resident live guard is healthy and consuming new RTDS data, but commit `b554bd83` (`Use hot history for live guard runtime`, 2026-07-15) made the default runtime path `data/research/wallet_copy_live_guard_hot_history_state.json`. `_with_live_guard_runtime_history()` replaces `args.history_state` with that hot path before the merge. The hot file was generated at `2026-07-30T10:49:56.799783Z` and contains the retained 8,192 events, proving ongoing ingestion. The merge writer at `scripts/merge_rtds_wallet_events.py:1535` therefore writes the hot state, not the primary research state consumed by the temporal registry.

## Exact stalled write point

The last successful supplemental write is `scripts/replay_source_active_policy_history.py:198`, batch `20260724T193401Z`, manifest timestamp `2026-07-24T19:38:45.598436Z`. Advancement stalls before the next call to `_append_manifest()` because no recurring producer exists. In the live path, routing changes at `scripts/run_wallet_copy_live_guard.py:15276` via `_with_live_guard_runtime_history()`; subsequent `merge_rtds_wallet_events.py:1535` writes the hot state instead of `wallet_copy_history_state.json`.

## Cause classification

`ORPHANED_RESEARCH_MERGE_PATH`: deliberate hot-store routing preserved live runtime performance, while the temporal registry retained the base-plus-manifest research contract and the only manifest producer remained an unscheduled one-shot replay. No evidence of a crash, atomic-write refusal, or malformed manifest was found.
