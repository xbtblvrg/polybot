# Same-Window Capture Findings — 2026-07-19

Flow stages: LEARN / PROMOTE. Source of truth is run 1 under
`data/research/same_window_capture/20260719T162300Z/run1_final/`.

## Post-Fee Alpha

Expected embedded BUY fee uses the canonical formula
`0.069997697 * shares * price * (1-price)`. For each BUY fill, the analysis
subtracts fee per share from observed price edge and keeps book observations
with lag at most five seconds. Positive means the equal-fill mean remained
positive after the expected fee; it is evidence, not a live-admission ruling.

| Wallet | Horizon | Timely BUY fills | Gross edge/share | Fee/share | Net edge/share | Net-positive fills |
|---|---:|---:|---:|---:|---:|---:|
| `0xa6896d11f76dfa2820662c1f441496f51553559b` | 5s | 116 | +0.017319 | 0.013270 | **+0.004049** | 56.9% |
| `0xa6896d11f76dfa2820662c1f441496f51553559b` | 30s | 121 | +0.034718 | 0.013837 | **+0.020881** | 56.2% |
| `0x8bc176d95c3312d8264ba26c5e26ee43a5c1b473` | 30s | 88 | +0.022727 | 0.017405 | **+0.005322** | 37.5% |

No wallet has positive mean post-fee alpha at the 1s or 2s horizon. In
particular, the five gross-edge profiles marked eligible by the alpha report
are not fee-aware: `0x8bc...b473` is the closest 2s case, but changes from
`+0.015625` gross to `-0.001761` net per share across 56 timely BUY fills.
The other four gross-eligible 2s profiles are more clearly negative after fee.

## Consumer Interpretation

- Alpha capture quality is now adequate: `6096/6149` fills have book coverage,
  with `7401s` of strict three-source overlap in the boundary-safe audit.
- Top10 ingestion is repaired (`8184` Data API rows), but this run produced no
  new copyable top10 BUY sample. Source recovery is proven; strategy edge is
  not.
- Whale consensus produced 17 windows and 4293 events. Its best `060-120s`
  bucket earned `+$3.250506` on only four $1 trades. This is a toy sample and
  cannot justify promotion or sizing.

## Decision

No promotion, rotation, threshold change, or sizing change follows from this
run. The fresh capture closes the stale-data defect and identifies two
post-fee-positive longer-horizon wallet signals for further preregistered
paper evidence. The current live lane remains untouched.
