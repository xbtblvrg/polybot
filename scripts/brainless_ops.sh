#!/usr/bin/env bash
set -euo pipefail

ROOT="${POLYMARKET_AGENT_ROOT:-/Users/belavarga/claudecode/polymarket-agent}"
cd "$ROOT"

PY="${POLYMARKET_AGENT_PYTHON:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PY" ]]; then
  PY="${PYTHON:-python3}"
fi
LOCK_DIR="${BRAINLESS_LOCK_DIR:-data/research/brainless_ops.lock}"
STATE="${BRAINLESS_STATE:-data/research/brainless_ops_state.json}"
OUTAGE_STATE="${BRAIN_OUTAGE_STATE:-data/research/brain_outage_state.json}"
STATUS_JSON="${BRAINLESS_STATUS_JSON:-data/research/brainless_ops_latest.json}"
HANDOFF="${BRAINLESS_HANDOFF:-docs/agents/HANDOFF.md}"
ATOMIC_HANDOFF_SCRIPT="${BRAINLESS_ATOMIC_HANDOFF_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/append_handoff_and_commit.py}"
HANDOFF_PENDING_RECOVERY="${BRAINLESS_HANDOFF_PENDING_RECOVERY:-data/research/brainless_ops_handoff_pending.md}"
COMMITMENTS_STATUS_JSON="${BRAINLESS_COMMITMENTS_STATUS_JSON:-data/research/brainless_ops_commitments_overdue.json}"
MEMORY_PRESSURE_MAX_PCT="${BRAINLESS_MEMORY_PRESSURE_MAX_PCT:-70}"
STEP_VMEM_LIMIT_KB="${BRAINLESS_STEP_VMEM_LIMIT_KB:-24000000}"
DATA_LAYER_ENABLED="${BRAINLESS_DATA_LAYER_ENABLED:-1}"
DATA_LAYER_MAX_BYTES="${BRAINLESS_DATA_LAYER_MAX_BYTES:-268435456}"
DATA_LAYER_MAX_FILES="${BRAINLESS_DATA_LAYER_MAX_FILES:-8}"
DR_REMOTE="${BRAINLESS_DR_REMOTE:-dr}"
DR_SNAPSHOT_BRANCH="${BRAINLESS_DR_SNAPSHOT_BRANCH:-dr-main}"
DR_EVERY_N="${BRAINLESS_DR_EVERY_N:-6}"

mkdir -p data/research "$(dirname "$HANDOFF")"

lock_reclaimed=""
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  lock_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || true)"
  if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
    echo "{\"status\":\"ALREADY_RUNNING\",\"holder_pid\":${lock_pid}}"
    exit 0
  fi
  # Holder pid dead or absent: a hard-killed run never fired its EXIT trap,
  # so an orphaned lock must not silence this lane forever.
  rm -rf "$LOCK_DIR"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo '{"status":"ALREADY_RUNNING"}'
    exit 0
  fi
  lock_reclaimed="1"
fi
echo "$$" > "$LOCK_DIR/pid"
trap 'rm -rf "$LOCK_DIR" 2>/dev/null || true' EXIT
if [[ -n "$lock_reclaimed" ]]; then
  echo '{"status":"STALE_LOCK_RECLAIMED"}'
fi

started_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
tmp="$(mktemp)"
step_durations_tsv="$(mktemp)"
handoff_pending="$(mktemp)"
if [[ -s "$HANDOFF_PENDING_RECOVERY" ]]; then
  cp "$HANDOFF_PENDING_RECOVERY" "$handoff_pending"
fi
status="OK"
failed_steps=()
rotation_action=""
queue_ready="0"
queue_depth="0"
scorecard_summary=""
memory_free_pct=""
memory_pressure_pct=""
memory_action="ALLOW_BACKGROUND_REFRESH"
data_layer_status="SKIPPED_NOT_RUN"
own_positions_status="SKIPPED_NOT_RUN"
own_redeemer_status="SKIPPED_NOT_RUN"
wallet_outflow_status="SKIPPED_NOT_RUN"
codex_starvation_status="SKIPPED_NOT_RUN"
runtime_speed_status="SKIPPED_NOT_RUN"
research_disk_deadman_status="SKIPPED_NOT_RUN"
temporal_profitability_status="SKIPPED_NOT_RUN"
winner_variation_status="SKIPPED_NOT_RUN"
cli_versions_status="SKIPPED_NOT_RUN"
utilization_meter_status="SKIPPED_NOT_RUN"
market_mining_status="SKIPPED_NOT_RUN"
factory_funnel_status="SKIPPED_NOT_RUN"
live_guard_restart_status="SKIPPED_NOT_RUN"
commitments_overdue_status="SKIPPED_NOT_RUN"
commitments_operator_notify="false"
commitments_overdue_count="0"
commitments_oldest_id=""
dr_preflight_status="SKIPPED_NOT_DUE"

memory_free_pct="$(memory_pressure -Q 2>/dev/null | awk -F': ' '/free percentage/ {gsub(/%/, "", $2); print int($2); exit}' || true)"
if [[ -n "$memory_free_pct" ]]; then
  memory_pressure_pct="$((100 - memory_free_pct))"
  if (( memory_pressure_pct > MEMORY_PRESSURE_MAX_PCT )); then
    memory_action="PAUSE_LOW_PRIORITY_BACKGROUND_JOBS"
    status="DEGRADED"
  fi
else
  memory_action="UNKNOWN_ALLOW_BACKGROUND_REFRESH"
fi

run_step() {
  local name="$1"
  shift
  local step_started_s
  local step_finished_s
  local step_duration_s
  local step_rc
  step_started_s="$("$PY" -c 'import time; print(f"{time.time():.6f}")')"
  if (
    ulimit -v "$STEP_VMEM_LIMIT_KB" 2>/dev/null || true
    "$@"
  ) >"data/research/brainless_ops_${name}.out" 2>"data/research/brainless_ops_${name}.err"; then
    step_rc=0
  else
    step_rc=$?
  fi
  step_finished_s="$("$PY" -c 'import time; print(f"{time.time():.6f}")')"
  step_duration_s="$("$PY" - "$step_started_s" "$step_finished_s" <<'PY'
import sys
start = float(sys.argv[1])
finish = float(sys.argv[2])
print(f"{max(0.0, finish - start):.6f}")
PY
)"
  printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$step_duration_s" "$step_rc" "$step_started_s" "$step_finished_s" >> "$step_durations_tsv"
  if (( step_rc != 0 )); then
    status="DEGRADED"
    failed_steps+=("$name")
  fi
}

if [[ "${BRAINLESS_SYNTHETIC_ROTATION_TEST:-0}" == "1" ]]; then
  cat > data/research/brainless_ops_scorecard.out <<'EOF'
day_utc=2099-01-01 window=2099-01-01T00:00:00Z..2099-01-02T00:00:00Z
total orders=3 fills=2 resolved=2 rejects=1 pnl=-16.000000
EOF
  cat > data/research/wallet_copy_promotion_rotation_full_pool_state.json <<'EOF'
{"decision":{"action":"RETAIN_NO_ELIGIBLE_CANDIDATE"}}
EOF
  cat > data/research/wallet_copy_full_pool_member_queue.json <<'EOF'
{"summary":{"queue_depth":1,"ready_for_live":0}}
EOF
else
  # Deterministic evidence refresh. No AI calls, no live-path edits.
  overlay_refresh_day="$(date -u +%F)"
  overlay_refresh_marker="data/research/self_feed_overlay_refresh_${overlay_refresh_day}.done"
  if [[ ! -s "$overlay_refresh_marker" ]]; then
    run_step self_feed_overlay_refresh "$PY" scripts/refresh_self_feed_overlay.py
    overlay_refresh_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj = json.loads(Path("data/research/self_feed_overlay_refresh_latest.json").read_text())
except Exception:
    obj = {}
print(obj.get("status") or "")
PY
)"
    if [[ "$overlay_refresh_status" == "PASS" ]]; then
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" > "$overlay_refresh_marker"
    fi
  fi
  run_step scorecard "$PY" scripts/daily_scorecard.py --day "$(date -u +%F)" --format text
  run_step strategy_map "$PY" scripts/build_strategy_map.py
  run_step utilization_meter "$PY" scripts/build_utilization_meter.py
  utilization_meter_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj = json.loads(Path("data/research/resource_utilization_latest.json").read_text())
except Exception:
    obj = {}
print(obj.get("verdict") or obj.get("status") or "")
PY
)"
  if [[ "$memory_action" == "PAUSE_LOW_PRIORITY_BACKGROUND_JOBS" ]]; then
    {
      echo "memory pressure ${memory_pressure_pct}% > ${MEMORY_PRESSURE_MAX_PCT}%; skipped promotion/member_queue/member_factory refresh"
      echo "guard/feed processes are not touched by brainless_ops pressure control"
    } > data/research/brainless_ops_memory_pressure_pause.out
    if [[ -f scripts/run_wallet_market_mining_cadence.py ]]; then
      run_step market_mining_cadence "$PY" scripts/run_wallet_market_mining_cadence.py --resource-paused
    else
      echo "scripts/run_wallet_market_mining_cadence.py missing; skipped resource-paused mining state" \
        > data/research/brainless_ops_market_mining_cadence.out
    fi
    if [[ -f scripts/build_factory_funnel.py ]]; then
      run_step factory_funnel "$PY" scripts/build_factory_funnel.py
    fi
  else
    # Continuous wallet-market mine (Fable 2026-07-14T17:31Z): bounded
    # intake/replay/liveness/packet cadence; research-only, no live mutation.
    run_step market_mining_cadence "$PY" scripts/run_wallet_market_mining_cadence.py
    # Factory funnel (operator 2026-07-14T17:38Z): one causal ladder from
    # market population to profitable sustained output; research-only.
    run_step factory_funnel "$PY" scripts/build_factory_funnel.py
    run_step promotion "$PY" scripts/select_wallet_copy_promotion_rotation.py \
      --paper-lane-state data/research/wallet_copy_full_pool_broad_paper_lane_state.json \
      --discover-candidates data/research/wallet_copy_discover_live_band_candidates_full_pool_replay.json \
      --output data/research/wallet_copy_promotion_rotation_full_pool_state.json
    run_step member_queue "$PY" scripts/build_full_pool_member_queue.py --limit 0
    run_step member_dow_profiles "$PY" scripts/report_member_dow_profiles.py
    run_step temporal_profitability "$PY" scripts/build_wallet_temporal_profitability_registry.py
    run_step winner_variation_siblings "$PY" scripts/build_wallet_copy_winner_variation_siblings.py
    run_step member_factory "$PY" scripts/report_member_factory_kpi.py \
      --state data/research/member_factory_kpi_state.json
    if [[ "$DATA_LAYER_ENABLED" == "1" ]]; then
      run_step data_layer "$PY" scripts/build_wallet_copy_data_layer.py \
        --max-bytes "$DATA_LAYER_MAX_BYTES" \
        --max-files "$DATA_LAYER_MAX_FILES"
    else
      echo "BRAINLESS_DATA_LAYER_ENABLED=$DATA_LAYER_ENABLED; skipped DATA LAYER v1 ingest" \
        > data/research/brainless_ops_data_layer.out
    fi
  fi
  run_step experiment_preregistration "$PY" scripts/preregister_experiment.py --audit-only
  run_step entry_price_band_gate_counterfactual "$PY" scripts/report_entry_price_band_gate_counterfactual.py
  run_step f418_post_band_gate_residual_loss_causal "$PY" scripts/report_f418_post_band_gate_residual_loss_causal.py
  run_step profit_latency_suppression_counterfactual "$PY" scripts/report_profit_latency_suppression_counterfactual.py
  run_step market_buy_precision_counterfactual "$PY" scripts/report_market_buy_precision_counterfactual.py
  run_step f418_acceptance_funnel "$PY" scripts/report_f418_acceptance_funnel.py
  run_step f418_green_day_conversion_shadow "$PY" scripts/report_f418_green_day_conversion_shadow.py
  # ORDER137 mechanical revert (Fable, 2026-08-01): zero-AI pre-registered
  # withdrawal of the pinned-seat min-share cap if the first 6 accepted
  # orders under it net <= -$2.00 resolved.  Runs as a fresh process so the
  # revert needs no guard restart; the executor reads its state file.
  run_step order137_revert_watch "$PY" scripts/order137_revert_watch.py --execute
  # ORDER138 mechanical revert (Fable, 2026-08-01): staged and inert until
  # its activation timestamp is recorded after the authorized 00:00Z reload.
  # If the first 8 accepted orders net <= -$4.00 resolved, write the terminal
  # withdrawal state; a guarded generation reload is required to converge any
  # launch-arg restore in the resident process.
  run_step order138_revert_watch "$PY" scripts/order138_revert_watch.py --execute
  # Order-flow dead-man (operator, 2026-07-07): zero-AI drought alarm;
  # fires on can_trade=True with no accepted order for 30 min.
  run_step order_flow_deadman "$PY" scripts/order_flow_deadman.py --handoff "$handoff_pending"
  # Research disk deadman (operator/Fable SOS 2026-07-16): zero-AI
  # free-space and uncapped-capture guard; discovers >5GiB research files
  # missing from the rotation inventory instead of trusting enumeration.
  if [[ -f scripts/research_disk_deadman.py ]]; then
    run_step research_disk_enforce "$PY" scripts/research_disk_deadman.py enforce
    run_step research_disk_deadman "$PY" scripts/research_disk_deadman.py audit --write-handoff-on-incident --handoff "$handoff_pending"
  else
    echo "scripts/research_disk_deadman.py missing; skipped research disk deadman" \
      > data/research/brainless_ops_research_disk_deadman.out
  fi
  # RED-ACTS-NOW law (Fable/operator 2026-07-13): if order-flow is red
  # and the running guard is behind the disk generation, or the guard is
  # unresponsive, restart the canonical live guard without a brain call.
  run_step live_guard_auto_restart "$PY" scripts/brainless_live_guard_restart.py \
    --execute \
    --max-guard-state-age-s 300
  # Per-file resident-vs-disk guard identity decomposition (Fable D22-3):
  # reporting only and outside the live generation/restart decision path.
  run_step guard_generation_delta "$PY" scripts/report_guard_generation_delta.py
  # e6db price-reject counterfactual (Fable 2026-07-13T15:46Z): collect
  # every >0.50<=0.70 rejected event and mechanically arm a $2 canary only
  # if n>=12 resolved hypothetical fills are net-positive.
  run_step e6db_price_reject_counterfactual "$PY" scripts/report_e6db_price_reject_counterfactual.py --apply-canary
  # CLOB book route health check for inventory best-ask skips; measurement
  # only, no trading gates loosened.
  run_step inventory_best_ask_route_probe "$PY" scripts/probe_inventory_best_ask_route.py
  # Wallet-outflow deadman (Fable L1 2026-07-11): any own-wallet pUSD
  # outflow not matched to our orders/redemptions within one cycle is P0.
  run_step wallet_outflow_deadman "$PY" scripts/wallet_outflow_deadman.py \
    --transfer-source blockscout \
    --timeout-s 30 \
    --handoff "$handoff_pending" \
    --write-handoff-on-incident
  # POST-BOOT RECOVERY AUDIT (fable 2026-07-10): run once per boot —
  # detects a new kern.boottime and fires the recovery audit so every
  # reboot self-documents its recovery state in HANDOFF.
  boot_ts="$(sysctl -n kern.boottime 2>/dev/null | sed -n 's/.*sec = \([0-9]*\).*/\1/p')"
  if [[ -n "$boot_ts" ]]; then
    boot_marker="data/research/post_boot_audit_boot_ts.txt"
    last_boot="$(cat "$boot_marker" 2>/dev/null || echo "")"
    if [[ "$boot_ts" != "$last_boot" ]]; then
      run_step post_boot_recovery "$PY" scripts/post_boot_recovery_audit.py
      echo "$boot_ts" > "$boot_marker"
    fi
  fi
  # CODEX_STARVATION deadman (Fable 2026-07-09T06:55Z): heartbeat PASS
  # without serving the latest next-list is a service incident.
  run_step codex_starvation "$PY" scripts/codex_starvation_deadman.py --handoff "$handoff_pending"
  # CLI version watcher (Fable 2026-07-11, AGY extended 2026-07-13):
  # updates are safe, but any brain CLI version change makes the next
  # ask_fable call an explicit smoke test.
  run_step cli_versions "$PY" scripts/record_cli_versions.py --handoff "$handoff_pending" record
  # Own-position redemption visibility/deadman (Fable P0 2026-07-08):
  # refreshes redeemable inventory and executes relayer redemption when
  # Data API confirms standard CTF redeemable positions.
  run_step own_positions "$PY" scripts/report_own_positions.py --handoff "$handoff_pending" --write-handoff-on-incident
  run_step own_redeemer "$PY" scripts/run_own_position_redeemer.py --execute --wait --ledger-estimate-fallback
  # Speed invariant (Fable 2026-07-10T19:30Z): wide-lane report only.
  run_step runtime_speed "$PY" scripts/report_runtime_speed_baseline.py --pin-if-missing
  # Off-machine DR snapshot (Fable/operator 2026-07-13): every 6th
  # brainless cycle by default, push a slim orphan snapshot branch; never
  # pushes raw local history or secrets.
  dr_cadence_file="data/research/dr_snapshot_push_cadence_state.json"
  dr_cadence_line="$("$PY" - <<PY
import json
from pathlib import Path
path = Path("$dr_cadence_file")
try:
    obj = json.loads(path.read_text())
except Exception:
    obj = {}
cycle = int(obj.get("cycle_count") or 0) + 1
every = max(1, int("$DR_EVERY_N" or 6))
due = cycle % every == 0
path.write_text(json.dumps({"cycle_count": cycle, "every_n": every, "due": due, "updated_at": "$started_iso"}, indent=2, sort_keys=True))
print(cycle, "1" if due else "0")
PY
)"
  read -r dr_cycle dr_due <<< "$dr_cadence_line"
  if [[ "$dr_due" == "1" ]]; then
    run_step dr_preflight "$PY" scripts/dr_preflight.py \
      --remote "$DR_REMOTE" \
      --push-snapshot \
      --snapshot-branch "$DR_SNAPSHOT_BRANCH"
    dr_preflight_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/wallet_copy_dr_preflight_latest.json").read_text())
except Exception:
    obj={}
summary=obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
push=obj.get("snapshot_push") if isinstance(obj.get("snapshot_push"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"remote={summary.get('remote_requested')} "
    f"branch={summary.get('snapshot_branch')} "
    f"commit={push.get('commit_sha')} "
    f"tracked_secrets={len(summary.get('tracked_secret_paths') or [])} "
    f"snapshot_bytes={summary.get('snapshot_total_bytes')}"
)
PY
)"
    "$PY" - <<PY
import json
from pathlib import Path
handoff = Path("$handoff_pending")
state_path = Path("data/research/dr_snapshot_push_failure_state.json")
preflight_path = Path("data/research/wallet_copy_dr_preflight_latest.json")
try:
    preflight = json.loads(preflight_path.read_text())
except Exception:
    preflight = {}
try:
    state = json.loads(state_path.read_text())
except Exception:
    state = {}
status = str(preflight.get("status") or "UNKNOWN")
ok = status == "SNAPSHOT_PUSHED"
count = 0 if ok else int(state.get("consecutive_failures") or 0) + 1
push = preflight.get("snapshot_push") if isinstance(preflight.get("snapshot_push"), dict) else {}
summary = preflight.get("summary") if isinstance(preflight.get("summary"), dict) else {}
state = {
    "updated_at": "$started_iso",
    "status": status,
    "consecutive_failures": count,
    "operator_notify": count >= 3,
    "remote": summary.get("remote_requested"),
    "snapshot_branch": summary.get("snapshot_branch"),
    "commit_sha": push.get("commit_sha"),
}
state_path.write_text(json.dumps(state, indent=2, sort_keys=True))
if not ok:
    with handoff.open("a", encoding="utf-8") as handle:
        handle.write(
            f"\\n## $started_iso brainless STATUS [SELF-DEV/DR]\\n"
            f"- defect | [SELF-DEV/DR] hourly DR snapshot push failed | attempts: status={status}, consecutive_failures={count}, remote={summary.get('remote_requested')}, branch={summary.get('snapshot_branch')} | next=inspect data/research/wallet_copy_dr_preflight_latest.json and rerun scripts/dr_preflight.py --remote {summary.get('remote_requested') or 'dr'} --push-snapshot --snapshot-branch {summary.get('snapshot_branch') or 'dr-main'}\\n"
        )
        if count >= 3:
            handle.write("- operator_notify [SELF-DEV/DR]: DR snapshot push failed 3 consecutive brainless cycles.\\n")
PY
  fi
  # Keep E1's ledger watermark on the same hourly cut as the live ledger.
  # The digest previously loaded the newest day-named artifact even when it
  # was hours behind, silently pricing downstream supply work on a stale book.
  e1_refresh_day="$(date -u +%F)"
  run_step e1_framework_audit_inputs "$PY" \
    scripts/report_e1_framework_audit_inputs.py \
    --day "$e1_refresh_day" \
    --reject-since "${e1_refresh_day}T00:00:00Z" \
    --scorecard data/research/wallet_copy_daily_scorecard_current.json
  # Refresh the human-readable scorecard immediately before reconciliation.
  # The earlier scorecard drives the rest of the cut, but live fills can land
  # while the intervening evidence steps run.  Keeping this refresh adjacent
  # to state_digest gives the text and fill bases the same latest ledger
  # watermark instead of manufacturing STALE_TEXT_BASIS during active trading.
  run_step scorecard_basis_refresh "$PY" scripts/daily_scorecard.py \
    --day "$(date -u +%F)" --format text
  if [[ -s data/research/brainless_ops_scorecard_basis_refresh.out ]]; then
    cp data/research/brainless_ops_scorecard_basis_refresh.out \
      data/research/brainless_ops_scorecard.out
  fi
  run_step state_digest "$PY" scripts/update_state_digest.py \
    --output data/research/state_digest.md \
    --json-output data/research/state_digest.json
  # ORDER153: publish and assert the same-cut since-topup bank identity on
  # every hourly evidence cut. The reporter writes its red artefact before a
  # non-zero exit, so run_step records the failure without suppressing digest.
  run_step fee_realization_bank_reconciliation "$PY" \
    scripts/report_fee_realization_bank_reconciliation.py
  run_step fee_bank_digest_refresh "$PY" scripts/update_state_digest.py \
    --output data/research/state_digest.md \
    --json-output data/research/state_digest.json
fi

if [[ -s data/research/wallet_copy_promotion_rotation_full_pool_state.json ]]; then
  rotation_action="$("$PY" - <<'PY'
import json
from pathlib import Path
obj=json.loads(Path("data/research/wallet_copy_promotion_rotation_full_pool_state.json").read_text())
print((obj.get("decision") or {}).get("action") or "")
PY
)"
fi
if [[ -s data/research/wallet_copy_full_pool_member_queue.json ]]; then
  read -r queue_depth queue_ready < <("$PY" - <<'PY'
import json
from pathlib import Path
obj=json.loads(Path("data/research/wallet_copy_full_pool_member_queue.json").read_text())
s=obj.get("summary") or {}
print(int(s.get("queue_depth") or 0), int(s.get("ready_for_live") or 0))
PY
)
fi
if [[ -s data/research/brainless_ops_scorecard.out ]]; then
  scorecard_summary="$(head -n 2 data/research/brainless_ops_scorecard.out | tr '\n' ' ' | sed 's/"/\\"/g')"
fi
if [[ -s data/research/runtime_speed_baseline_latest.json ]]; then
  runtime_speed_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/runtime_speed_baseline_latest.json").read_text())
except Exception:
    obj={}
comparison=obj.get("comparison") if isinstance(obj.get("comparison"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"regressions={comparison.get('regression_count')} "
    f"baseline={obj.get('baseline_created_at')}"
)
PY
)"
fi
if [[ -s data/research/research_disk_deadman_state.json ]]; then
  research_disk_deadman_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/research_disk_deadman_state.json").read_text())
except Exception:
    obj={}
disk=obj.get("disk") if isinstance(obj.get("disk"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"incident={bool(obj.get('incident'))} "
    f"free_gib={disk.get('free_gib')} "
    f"uninventoried={len(obj.get('uninventoried_large_files') or [])} "
    f"inventory={obj.get('inventory_count')} "
    f"memory_swap={(obj.get('memory_swap') or {}).get('status')} "
    f"swapfiles={((obj.get('memory_swap') or {}).get('swapfiles') or {}).get('count')}"
)
PY
)"
elif [[ -s data/research/brainless_ops_research_disk_deadman.err ]]; then
  research_disk_deadman_status="ERROR_SEE_brainless_ops_research_disk_deadman.err"
fi
if [[ -s data/research/wallet_temporal_profitability_latest.json ]]; then
  temporal_profitability_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/wallet_temporal_profitability_latest.json").read_text())
except Exception:
    obj={}
summary=obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
dow=obj.get("dow_weight_verification") if isinstance(obj.get("dow_weight_verification"), dict) else {}
print(
    f"status={dow.get('status') or 'UNKNOWN'} "
    f"wallets={summary.get('wallets_total')} "
    f"history={summary.get('wallets_with_resolved_btc5m_history')} "
    f"feed={summary.get('watch_tier_feed_count')}"
)
PY
)"
fi
if [[ -s data/research/wallet_copy_winner_variation_siblings_latest.json ]]; then
  winner_variation_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/wallet_copy_winner_variation_siblings_latest.json").read_text())
except Exception:
    obj={}
summary=obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"lanes={summary.get('lane_count')} "
    f"twin={summary.get('paper_twin_lane_id')} "
    f"best={summary.get('best_sibling_lane_id')} "
    f"diff={summary.get('best_sibling_roi_diff_pp')} "
    f"n={summary.get('best_sibling_resolved_fills')}"
)
PY
)"
fi
if [[ -s data/research/data_layer_v1_manifest.json ]]; then
  data_layer_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/data_layer_v1_manifest.json").read_text())
except Exception:
    obj={}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"rows={int(obj.get('rows_converted') or 0)} "
    f"files={int(obj.get('files_converted') or 0)}/{int(obj.get('files_considered') or 0)} "
    f"bytes={int(obj.get('bytes_read') or 0)} "
    f"duckdb_rows={int((obj.get('duckdb') or {}).get('rows') or 0)}"
)
PY
)"
elif [[ -s data/research/brainless_ops_data_layer.err ]]; then
  data_layer_status="ERROR_SEE_brainless_ops_data_layer.err"
fi
if [[ -s data/research/own_positions_latest.json || -s data/research/own_position_deadman_state.json ]]; then
  own_positions_status="$("$PY" - <<'PY'
import json
from pathlib import Path
report={}
deadman={}
try:
    report=json.loads(Path("data/research/own_positions_latest.json").read_text())
except Exception:
    pass
try:
    deadman=json.loads(Path("data/research/own_position_deadman_state.json").read_text())
except Exception:
    pass
summary=report.get("summary") if isinstance(report.get("summary"), dict) else {}
print(
    f"status={report.get('status') or 'UNKNOWN'} "
    f"locked={summary.get('redeemable_locked_usd')} "
    f"source={summary.get('locked_value_source')} "
    f"deadman={deadman.get('status') or 'UNKNOWN'}"
)
PY
)"
fi
if [[ -s data/research/own_redeemer_state.json ]]; then
  own_redeemer_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/own_redeemer_state.json").read_text())
except Exception:
    obj={}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"candidates={int(obj.get('candidate_count') or 0)} "
    f"executed={bool(obj.get('executed'))}"
)
PY
)"
fi
if [[ -s data/research/wallet_outflow_deadman_state.json ]]; then
  wallet_outflow_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/wallet_outflow_deadman_state.json").read_text())
except Exception:
    obj={}
summary=obj.get("summary") if isinstance(obj.get("summary"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"incident={bool(obj.get('incident'))} "
    f"outflows={summary.get('outflow_rows')} "
    f"unmatched={summary.get('unmatched_outflows')} "
    f"incident_rows={summary.get('incident_outflows')} "
    f"usd={summary.get('unmatched_outflow_usd')}"
)
PY
)"
fi
if [[ -s data/research/codex_starvation_deadman_state.json ]]; then
  codex_starvation_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/codex_starvation_deadman_state.json").read_text())
except Exception:
    obj={}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"age_s={obj.get('last_codex_commit_age_s')} "
    f"armed={bool(obj.get('fable_escalation_armed'))}"
)
PY
)"
fi
if [[ -s data/research/cli_versions_state.json ]]; then
  cli_versions_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/cli_versions_state.json").read_text())
except Exception:
    obj={}
pending=obj.get("pending_smoke_test") if isinstance(obj.get("pending_smoke_test"), dict) else {}
versions=obj.get("versions") if isinstance(obj.get("versions"), dict) else {}
freshness=obj.get("freshness") if isinstance(obj.get("freshness"), dict) else {}
stale_tools=obj.get("stale_tools") if isinstance(obj.get("stale_tools"), list) else []
parts=[]
for tool in ("claude", "codex", "grok", "agy"):
    row=versions.get(tool) if isinstance(versions.get(tool), dict) else {}
    fresh=freshness.get(tool) if isinstance(freshness.get(tool), dict) else {}
    latest=fresh.get("latest") if isinstance(fresh.get("latest"), dict) else {}
    parts.append(
        f"{tool}={row.get('version') or 'UNAVAILABLE'}"
        f"/latest={latest.get('latest_version') or 'UNKNOWN'}"
        f"/stale={fresh.get('stale')}"
    )
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"pending={bool(pending)} "
    f"stale={stale_tools} "
    + " ".join(parts)
)
PY
)"
fi
if [[ -s data/research/resource_utilization_latest.json ]]; then
  utilization_meter_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/resource_utilization_latest.json").read_text())
except Exception:
    obj={}
print(
    f"verdict={obj.get('verdict') or 'UNKNOWN'} "
    f"active={obj.get('active_or_gated_lane_count')} "
    f"max={obj.get('memory_capped_max_lane_count')} "
    f"idle={obj.get('idle_lane_capacity')} "
    f"utilization_pct={obj.get('utilization_pct')}"
)
PY
)"
fi
if [[ -s data/research/wallet_market_mining_cadence_state.json ]]; then
  market_mining_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/wallet_market_mining_cadence_state.json").read_text())
except Exception:
    obj={}
observed=obj.get("observed") if isinstance(obj.get("observed"), dict) else {}
null_cycle=obj.get("null_cycle") if isinstance(obj.get("null_cycle"), dict) else {}
print(
    f"status={obj.get('status') or 'UNKNOWN'} "
    f"ran={obj.get('ran_steps') or []} "
    f"live_ready={observed.get('live_ready_picks')} "
    f"packets={observed.get('packet_count')} "
    f"null={null_cycle.get('status')} "
    f"widened={null_cycle.get('scope_widened')}"
)
PY
)"
elif [[ -s data/research/brainless_ops_market_mining_cadence.err ]]; then
  market_mining_status="ERROR_SEE_brainless_ops_market_mining_cadence.err"
fi
if [[ -s data/research/factory_funnel_latest.json ]]; then
  factory_funnel_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/factory_funnel_latest.json").read_text())
except Exception:
    obj={}
enemy=obj.get("enemy_line") if isinstance(obj.get("enemy_line"), dict) else {}
counts=obj.get("counts") if isinstance(obj.get("counts"), dict) else {}
print(
    f"status={enemy.get('status') or 'UNKNOWN'} "
    f"first={enemy.get('link_id')} "
    f"live_ready={counts.get('live_ready')} "
    f"admitted={counts.get('admitted')} "
    f"armed={counts.get('armed_runtime_loaded')} "
    f"filled={counts.get('filled_live_orders_today')}"
)
PY
)"
elif [[ -s data/research/brainless_ops_factory_funnel.err ]]; then
  factory_funnel_status="ERROR_SEE_brainless_ops_factory_funnel.err"
fi
if [[ -s data/research/brainless_live_guard_restart_state.json ]]; then
  live_guard_restart_status="$("$PY" - <<'PY'
import json
from pathlib import Path
try:
    obj=json.loads(Path("data/research/brainless_live_guard_restart_state.json").read_text())
except Exception:
    obj={}
latest=obj.get("latest_decision") if isinstance(obj.get("latest_decision"), dict) else obj
storm=latest.get("storm_guard") if isinstance(latest.get("storm_guard"), dict) else {}
print(
    f"status={latest.get('status') or 'UNKNOWN'} "
    f"reason={latest.get('reason')} "
    f"restart_allowed={bool(latest.get('restart_allowed'))} "
    f"today={storm.get('restarts_today')}/{storm.get('max_restarts_per_day')}"
)
PY
)"
fi
if [[ -s data/research/state_digest.json ]]; then
  IFS=$'\t' read -r commitments_overdue_status commitments_operator_notify commitments_overdue_count commitments_oldest_id < <(
    "$PY" - <<PY
import json
from pathlib import Path
digest_path = Path("data/research/state_digest.json")
out_path = Path("$COMMITMENTS_STATUS_JSON")
try:
    digest = json.loads(digest_path.read_text())
except Exception:
    digest = {}
summary = digest.get("commitments_overdue") if isinstance(digest.get("commitments_overdue"), dict) else {}
try:
    overdue = int(summary.get("overdue") or 0)
except Exception:
    overdue = 0
status = "OVERDUE_NOTIFY" if overdue > 0 else "OK"
payload = {
    "status": status,
    "digest_path": str(digest_path),
    "commitments_path": summary.get("path") or "data/research/commitments.jsonl",
    "overdue": overdue,
    "oldest_id": summary.get("oldest_id"),
    "oldest_due_ts": summary.get("oldest_due_ts"),
    "late": int(summary.get("late") or 0),
    "due_today": int(summary.get("due_today") or 0),
    "active": int(summary.get("active") or 0),
    "evidence_unmarked": int(summary.get("evidence_unmarked") or 0),
    "overdue_with_evidence": int(summary.get("overdue_with_evidence") or 0),
    "operator_notify": overdue > 0,
    "flow_stage": "SELF-DEV/DEFEND",
    "next_action": (
        "serve or close the oldest overdue commitment evidence, then rerun brainless_ops until overdue=0"
        if overdue > 0
        else "continue commitment-deadman read each brainless pulse"
    ),
}
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(f"{status}\t{str(overdue > 0).lower()}\t{overdue}\t{payload.get('oldest_id') or ''}")
PY
  )
else
  "$PY" - <<PY
import json
from pathlib import Path
payload = {
    "status": "NO_DIGEST_JSON",
    "digest_path": "data/research/state_digest.json",
    "commitments_path": "data/research/commitments.jsonl",
    "overdue": 0,
    "oldest_id": None,
    "operator_notify": False,
    "flow_stage": "SELF-DEV/DEFEND",
    "next_action": "restore state_digest.json generation so commitment deadman can read overdue rows",
}
path = Path("$COMMITMENTS_STATUS_JSON")
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
fi
if [[ "$commitments_operator_notify" == "true" ]]; then
  status="DEGRADED"
  {
    echo
    echo "## ${started_iso} brainless NOTIFY - COMMITMENT_DEADMAN"
    echo "- defect | [SELF-DEV/DEFEND] commitments_overdue=${commitments_overdue_count} oldest=${commitments_oldest_id:-unknown} | attempts: state_digest commitments_overdue read, brainless_ops_latest operator_notify=true, HANDOFF notify emitted | next=serve or close oldest commitment evidence; continue pulse enforcement until overdue=0."
  } >> "$handoff_pending"
fi

brain_outage="false"
if [[ "${BRAINLESS_FORCE_BRAIN_OUTAGE:-0}" == "1" ]]; then
  cat > "$OUTAGE_STATE" <<'EOF'
{"brain_outage": true, "consecutive_failures": 3, "source": "synthetic_test"}
EOF
fi
if [[ -s "$OUTAGE_STATE" ]]; then
  brain_outage="$("$PY" - <<'PY'
import json
import os
from pathlib import Path
try:
    path = os.environ.get("BRAIN_OUTAGE_STATE", "data/research/brain_outage_state.json")
    obj=json.loads(Path(path).read_text())
except Exception:
    obj={}
print("true" if obj.get("brain_outage") else "false")
PY
)"
fi

if ((${#failed_steps[@]})); then
  failed_steps_json="$("$PY" -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "${failed_steps[@]}")"
else
  failed_steps_json="[]"
fi

"$PY" - <<PY > "$tmp"
import json
brain_outage = "$brain_outage" == "true"
step_durations = {}
step_results = {}
try:
    with open("$step_durations_tsv") as handle:
        for line in handle:
            name, duration_s, rc, started_s, finished_s = line.rstrip("\n").split("\t")
            duration = round(float(duration_s), 6)
            step_durations[name] = duration
            step_results[name] = {
                "duration_s": duration,
                "rc": int(rc),
                "started_s": round(float(started_s), 6),
                "finished_s": round(float(finished_s), 6),
            }
except Exception:
    step_durations = {}
    step_results = {}
slowest_step = None
if step_durations:
    slowest_name = max(step_durations, key=step_durations.get)
    slowest_step = {"name": slowest_name, "duration_s": step_durations[slowest_name]}
try:
    commitments_overdue_payload = json.load(open("$COMMITMENTS_STATUS_JSON"))
except Exception:
    commitments_overdue_payload = {
        "status": "$commitments_overdue_status",
        "overdue": int("$commitments_overdue_count" or 0),
        "oldest_id": "$commitments_oldest_id" or None,
        "operator_notify": "$commitments_operator_notify" == "true",
        "flow_stage": "SELF-DEV/DEFEND",
    }
payload = {
    "schema_version": 1,
    "kind": "brainless_ops_state",
    "started_at": "$started_iso",
    "finished_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "status": "$status",
    "failed_steps": $failed_steps_json,
    "step_durations": step_durations,
    "step_results": step_results,
    "slowest_step": slowest_step,
    "brain_outage": brain_outage,
    "paper_only": False,
    "live_orders_allowed": False,
    "live_path_mutated": False,
    "rotation_action": "$rotation_action",
    "queue_depth": int("$queue_depth" or 0),
    "queue_ready_for_live": int("$queue_ready" or 0),
    "scorecard_summary": "$scorecard_summary",
    "memory_pressure": {
        "free_pct": None if "$memory_free_pct" == "" else int("$memory_free_pct"),
        "pressure_pct": None if "$memory_pressure_pct" == "" else int("$memory_pressure_pct"),
        "threshold_pct": int("$MEMORY_PRESSURE_MAX_PCT"),
        "action": "$memory_action",
        "step_vmem_limit_kb": int("$STEP_VMEM_LIMIT_KB"),
    },
    "state_digest": {
        "path": "data/research/state_digest.md",
        "json_path": "data/research/state_digest.json",
        "status": "SHADOW_VALIDATE_READ_WITH_FULL_CONTEXT",
    },
    "runtime_speed_baseline": {
        "path": "data/research/runtime_speed_baseline_latest.json",
        "pinned_path": "data/research/runtime_speed_baseline_pinned.json",
        "status": "$runtime_speed_status",
    },
    "research_disk_deadman": {
        "path": "data/research/research_disk_deadman_state.json",
        "inventory": "data/research/research_capture_rotation_inventory.json",
        "status": "$research_disk_deadman_status",
        "law": "incident when free<150GiB, any data/research file >5GiB is absent from rotation inventory, swapfiles>20 sustained, or memory pressure is critical",
    },
    "utilization_meter": {
        "path": "data/research/resource_utilization_latest.json",
        "markdown": "data/research/resource_utilization_latest.md",
        "status": "$utilization_meter_status",
    },
    "data_layer_v1": {
        "enabled": "$DATA_LAYER_ENABLED" == "1",
        "manifest": "data/research/data_layer_v1_manifest.json",
        "status": "$data_layer_status",
        "max_bytes_per_run": int("$DATA_LAYER_MAX_BYTES"),
        "max_files_per_run": int("$DATA_LAYER_MAX_FILES"),
        "raw_jsonl_remains_source_of_record": True,
    },
    "market_mining_cadence": {
        "path": "data/research/wallet_market_mining_cadence_state.json",
        "status": "$market_mining_status",
        "law": "continuous intake/replay/liveness/packet mining cadence with null-cycle widening; research-only",
    },
    "factory_funnel": {
        "path": "data/research/factory_funnel_latest.json",
        "status": "$factory_funnel_status",
        "law": "ordered causal ladder from market population to profitable sustained output; names first broken conversion",
    },
    "member_factory_kpi": {
        "path": "data/research/member_factory_kpi_state.json",
        "status": "DETERMINISTIC_PROMOTE_SUPPLY_WATCHDOG",
    },
    "temporal_profitability": {
        "path": "data/research/wallet_temporal_profitability_latest.json",
        "status": "$temporal_profitability_status",
    },
    "winner_variation_siblings": {
        "path": "data/research/wallet_copy_winner_variation_siblings_latest.json",
        "status": "$winner_variation_status",
    },
    "own_positions": {
        "path": "data/research/own_positions_latest.json",
        "deadman_path": "data/research/own_position_deadman_state.json",
        "status": "$own_positions_status",
    },
    "own_redeemer": {
        "state": "data/research/own_redeemer_state.json",
        "events": "data/research/own_redeem_events.jsonl",
        "status": "$own_redeemer_status",
    },
    "wallet_outflow_deadman": {
        "state": "data/research/wallet_outflow_deadman_state.json",
        "status": "$wallet_outflow_status",
    },
    "codex_starvation_deadman": {
        "path": "data/research/codex_starvation_deadman_state.json",
        "status": "$codex_starvation_status",
    },
    "cli_versions": {
        "path": "data/research/cli_versions_state.json",
        "status": "$cli_versions_status",
    },
    "live_guard_auto_restart": {
        "path": "data/research/brainless_live_guard_restart_state.json",
        "status": "$live_guard_restart_status",
        "law": "red order-flow generation mismatch or guard unresponsive triggers canonical restart with cooldown and daily cap",
    },
    "commitments_overdue": commitments_overdue_payload,
    "dr_preflight": {
        "path": "data/research/wallet_copy_dr_preflight_latest.json",
        "cadence_state": "data/research/dr_snapshot_push_cadence_state.json",
        "failure_state": "data/research/dr_snapshot_push_failure_state.json",
        "remote": "$DR_REMOTE",
        "snapshot_branch": "$DR_SNAPSHOT_BRANCH",
        "every_n_cycles": int("$DR_EVERY_N"),
        "status": "$dr_preflight_status",
    },
    "forbidden_actions_enforced": [
        "no_ai_calls",
        "no_new_members_outside_precomputed_queue",
        "no_band_changes",
        "no_sizing_changes",
        "no_method_promotion",
        "no_live_path_code_changes",
    ],
    "next_action": (
        "mechanical rotation has no ready queue member; keep live lane and continue evidence refresh"
        if "$rotation_action" == "RETAIN_NO_ELIGIBLE_CANDIDATE"
        else "continue deterministic monitoring"
    ),
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY
mv "$tmp" "$STATUS_JSON"
cp "$STATUS_JSON" "$STATE"
rm -f "$tmp" "$step_durations_tsv" 2>/dev/null || true

if [[ "$brain_outage" == "true" ]]; then
  {
    echo
    echo "## ${started_iso} brainless STATUS (LIVE/SELF-DEV)"
    echo "- brain=none; status=${status}; rotation_action=${rotation_action}; queue_ready=${queue_ready}/${queue_depth}."
    echo "- next: $("$PY" -c 'import json;print(json.load(open("data/research/brainless_ops_latest.json"))["next_action"])')"
  } >> "$handoff_pending"
fi

if [[ -s "$handoff_pending" ]]; then
  if ! "$PY" "$ATOMIC_HANDOFF_SCRIPT" \
    --handoff "$HANDOFF" \
    --pending "$handoff_pending" \
    --message "ops: record brainless heartbeat ${started_iso}" \
    > data/research/brainless_ops_handoff_commit.out; then
    failed_steps+=("handoff_atomic_commit")
    status="DEGRADED"
    mv "$handoff_pending" "$HANDOFF_PENDING_RECOVERY"
    "$PY" - <<PY
import json
from pathlib import Path
for raw in ("$STATUS_JSON", "$STATE"):
    path = Path(raw)
    try:
        payload = json.loads(path.read_text())
    except Exception:
        continue
    failed = [str(item) for item in payload.get("failed_steps") or []]
    if "handoff_atomic_commit" not in failed:
        failed.append("handoff_atomic_commit")
    payload["failed_steps"] = failed
    payload["status"] = "DEGRADED"
    payload["handoff_atomic_commit"] = {
        "status": "PENDING_RECOVERY",
        "path": "$HANDOFF_PENDING_RECOVERY",
        "next_action": "rerun the atomic HANDOFF commit before adding a new brainless status",
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
  else
    rm -f "$HANDOFF_PENDING_RECOVERY" "$handoff_pending"
  fi
else
  rm -f "$handoff_pending"
fi

cat "$STATUS_JSON"
