#!/usr/bin/env bash
# Measurement-only refreshers that must run independently of long Codex work.
set -uo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:$HOME/bin:$PATH"
cd "$(dirname "$0")/.."

LOCKDIR=/tmp/polymarket_codex_refresh_cadence.lock
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$LOCKDIR" 2>/dev/null || true' EXIT

mkdir -p logs data/research
LOG=logs/codex_refresh_cadence.log
STATE=data/research/codex_refresh_cadence_state.json
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "=== refresh cadence $STARTED_AT ===" >>"$LOG"

python3 - "$STATE" "$STARTED_AT" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
started_at = sys.argv[2]
try:
    prior = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    prior = {}
previous = prior.get("last_cycle_start_at")
period_s = None
if previous:
    try:
        current_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        previous_dt = datetime.fromisoformat(str(previous).replace("Z", "+00:00"))
        period_s = max(0.0, (current_dt - previous_dt).total_seconds())
    except ValueError:
        period_s = None
payload = {
    "schema_version": 1,
    "kind": "codex_refresh_cadence",
    "producer_path": "scripts/codex_refresh_cadence.sh",
    "declared_cycle_period_s": 900,
    "previous_cycle_start_at": previous,
    "last_cycle_start_at": started_at,
    "cycle_period_s_observed": period_s,
    "cycle_period_status": (
        "UNKNOWN_FIRST_CYCLE" if period_s is None else
        "PASS_WITHIN_2X_DECLARED" if period_s <= 1800 else
        "BREACH_OVER_2X_DECLARED"
    ),
    "measurement_only": True,
    "live_mutation": False,
}
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.replace(tmp, path)
PY

python3 scripts/run_btc5m_structural_scalp_paper_lane.py >>"$LOG" 2>&1 \
  || echo "structural scalp lane runner failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
python3 scripts/accumulate_wallet_copy_hot_history.py \
  --supplemental-output data/research/wallet_copy_hot_history_accumulator_supplemental.json \
  --supplemental-manifest data/research/temporal_supplemental_history_manifest.json >>"$LOG" 2>&1 \
  || echo "wallet hot-history accumulator failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
python3 scripts/report_pipeline_slo.py --refresh-binding >>"$LOG" 2>&1 \
  || echo "pipeline SLO reporter failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
python3 scripts/accumulate_orderfilled_01a_supply_daily.py >>"$LOG" 2>&1 \
  || echo "01a daily supply accumulator failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
python3 scripts/report_01a_supply_admission_gate.py >>"$LOG" 2>&1 \
  || echo "01a supply admission gate failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
python3 scripts/replay_source_active_policy_history.py \
  --recurring-cohort data/research/wide_direct_admissible_frontier_latest.json \
  --max-wallets 2 --lookback-days 14 --max-events-per-wallet 2000 \
  --history-limit 500 --timeout-s 10 --sleep-s 0.05 >>"$LOG" 2>&1 \
  || echo "bounded WIDE temporal replay failed at $(date -u +%H:%M:%SZ)" >>"$LOG"

UTC_HOUR=$(date -u +%H)
UTC_MIN_NUM=$((10#$(date -u +%M)))
if [ "$UTC_HOUR" = "00" ] && [ "$UTC_MIN_NUM" -ge 5 ] && [ "$UTC_MIN_NUM" -le 20 ]; then
  python3 scripts/report_daily_scorecard.py --format json >>"$LOG" 2>&1 \
    || echo "daily scorecard failed at $(date -u +%H:%M:%SZ)" >>"$LOG"
fi
