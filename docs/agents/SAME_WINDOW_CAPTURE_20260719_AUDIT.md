# Same-Window Capture Completion Audit — 2026-07-19

Flow stages: DISCOVER / LEARN / OBSERVE / PROMOTE. Run ID:
`20260719T162300Z`. This is a paper-only evidence run; it submitted no live
orders and did not change the live candidate or policy.

## Verdict

PASS. Run 1 completed its 7,500 second collection window and all four
preregistered consumer gates passed. Counts below are bounded by capture time
`2026-07-19T18:26:30.357766Z`; Polygon connection-error rows are excluded.
The launchd-created run 2 append is excluded from the audit.

## Required Completion Fields

| Field | Evidence | Verdict |
|---|---|---|
| (a) lifecycle | Run 1 started `16:21:30.344516Z`; Data API completed `18:26:30.357766Z`; alpha finalized `18:26:43.638732Z`; supervisor/alpha/Data API return codes `0/0/0`; lock released; launchd label and all capture PIDs absent | PASS |
| (b) duration/windows | Actual Data API duration `7500.013150s`; three-way interval spans 26 BTC-5m windows | PASS (`>=7200s`, `>=24`) |
| (c) overlap | Polygon `event_ts` `16:20:31Z..18:25:31Z` (bounded by `captured_at_s`); CLOB capture `16:22:09.675Z..18:26:01.204Z`; Data API observation `16:21:31.672Z..18:26:29.087Z`; intersection `7401.324798s` | PASS |
| (d) volume | Polygon: 498,638 OrderFilled rows, 11/19 registry wallets matched, 8,917 assets; CLOB: 16,278 best-bid/ask snapshots, 328 assets; Data API: 8,184 scanned and retained rows, 9 wallets, 1,152 assets | PASS |
| (e) consumer gates | Alpha overlap `7430.324798s`, book coverage `6096/6149`; top10 `source.rows_scanned=8184`; whale consensus `17` windows / `4293` events | PASS 4/4 |
| (f) live non-interference | Sole guard PID remained `38019`; PnL basis `MATCH`; adjusted missed/consecutive `2/0`, incident false | PASS |

The boundary-safe stream audit excluded 23,161 Polygon OrderFilled rows, 557
CLOB snapshots, and 26 Data API rows captured after the run-1 boundary. It is
reproducible with:

```bash
python3 scripts/report_same_window_capture_completion.py \
  --run-dir data/research/same_window_capture/20260719T162300Z \
  --boundary-iso 2026-07-19T18:26:30.357766Z
```

## Defect Closure

`capture-launchd-keepalive-relaunch-loop` is closed. Bare `KeepAlive=true`
restarted the cleanly completed supervisor into the same run directory at
`18:26:49Z`. Fable preserved the authoritative run-1 reports under
`run1_final/`, booted out the service, verified zero capture PIDs, removed the
orphaned lock, and confirmed that no persistent LaunchAgent plist remained.
Future captures use `scripts/launch_same_window_research_capture.py`, which
refuses non-empty run IDs, uses `KeepAlive={SuccessfulExit:false}`, and sets
`SSL_CERT_FILE` to certifi's CA bundle.
