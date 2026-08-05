# DR Restore Runbook

Purpose: restore the repo brain from the one-way `dr/dr-main` backup after
local machine loss. The DR remote is backup-only; do not pull it into the
normal local workflow.

## One Command

```bash
set -euo pipefail
RESTORE_DIR="${1:-$HOME/polymarket-agent-restore}"
git clone --branch dr-main --single-branch git@github.com:xbtblvrg/polybot.git "$RESTORE_DIR"
cd "$RESTORE_DIR"
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -r requirements.txt
gzip -cd configs/wallet_copy/wallets.json.gz > configs/wallet_copy/wallets.json
.venv/bin/python -m compileall -q src scripts tests
.venv/bin/python -m pytest -q \
  tests/test_data_layer_and_dr.py::test_dr_size_gate_flags_oversized_files \
  tests/test_data_layer_and_dr.py::test_dr_snapshot_push_uses_temp_index_and_snapshot_ref \
  tests/test_state_digest.py::test_state_digest_renders_latest_operator_order
```

## Drill Transcript

Run on 2026-07-13 from `dr-main`:

```text
RESTORE_TMP=/tmp/polybot-dr-restore-Q0Ujg0
STEP clone
Cloning into '/tmp/polybot-dr-restore-Q0Ujg0/repo'...
HEAD=22a49d0c026a1eb711db7de6f6c9faad09b87e1d
FILE_COUNT=450
STEP gunzip registry
configs/wallet_copy/wallets.json     106M
configs/wallet_copy/wallets.json.gz   12M
STEP compileall
compileall_rc=0
STEP focused_pytest_retry_snapshot_existing
3 passed in 1.57s
pytest_retry_rc=0
```

The first smoke attempt targeted a newer local test absent from snapshot
`22a49d0c`; the retry used tests present in that snapshot and passed. Use
snapshot-present tests for resurrection smoke, then run the full suite after
normal development resumes.

## Manual Resurrection Steps

1. Recreate `.env` from `.env.example` and local secure records. Never store
   keys under innocuous filenames; the DR gate is filename-based plus size
   gates.
2. Accept `data/` as local runtime state. The DR snapshot carries compact key
   state and compressed registry, not bulky replay/history artifacts. Regenerate
   derived `data/research/*` by running the normal scripts as needed.
3. Restore the registry with:

```bash
gzip -cd configs/wallet_copy/wallets.json.gz > configs/wallet_copy/wallets.json
```

4. Validate code before any launchd load:

```bash
.venv/bin/python -m compileall -q src scripts tests
.venv/bin/python -m pytest -q tests/test_data_layer_and_dr.py tests/test_brainless_ops.py
```

5. Load services in this order after `.env` and dependencies are present:
   source/ingest services, deadmen, brainless ops, then live guard. Do not start
   guard before keys, source routes, and deadmen are known-good.
6. Start live guard only through the canonical launcher and confirm one submitter:

```bash
./scripts/start_live_guard.sh
pgrep -fl 'scripts/run_wallet_copy_live_guard.py'
POLYMARKET_DEADMAN_NOTIFY=0 .venv/bin/python scripts/order_flow_deadman.py
```

7. Recreate off-machine backup on the new host:

```bash
git remote add dr git@github.com:xbtblvrg/polybot.git
.venv/bin/python scripts/dr_preflight.py --remote dr --push-snapshot --snapshot-branch dr-main
```

After this, DR returns to zero-AI brainless cadence. Normal work stays local;
the remote is read only for resurrection.
