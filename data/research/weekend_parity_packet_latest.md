# Weekend Parity Packet

- experiment_id: `campaign-weekend-parity-20260711`
- generated_at: `2026-07-25T01:07:49.111622+00:00`
- verdict: `WEEKEND_PARITY_FAIL_FOR_LIVE_WALLET_COPY`
- weekend_pnl_usd: `-42.793216`
- weekend_roi_pct: `-24.384078`
- current_roster_postures: `{'TRADE_FLOOR_SIZE': 1, 'BENCH': 4}`

## Current Roster Weekend Plan

- rule: `reduced-but-positive posture is the goal; weekday volume parity is not expected and must not be forced`
- day_probe_trigger_usd: `-8.0`
- 0xc50d...0de0: posture `TRADE_FLOOR_SIZE`, fills `4`, pnl `0.495299`, weekend_status `HAS_WEEKEND_SAMPLE`, first_slice_loss `-16.0`
- 0x141d...0f59: posture `BENCH`, fills `0`, pnl `0.0`, weekend_status `HAS_WEEKEND_SAMPLE`, first_slice_loss `-16.0`
- 0xa689...559b: posture `BENCH`, fills `0`, pnl `0.0`, weekend_status `HAS_WEEKEND_SAMPLE`, first_slice_loss `-16.0`
- 0x32de...ce9d: posture `BENCH`, fills `0`, pnl `0.0`, weekend_status `HAS_WEEKEND_SAMPLE`, first_slice_loss `-16.0`
- 0x3048...7537: posture `BENCH`, fills `0`, pnl `0.0`, weekend_status `HAS_WEEKEND_SAMPLE`, first_slice_loss `-16.0`

## Legacy Weekend Evidence

## Q1 Volume

- answer: `YES_BUT_REDUCED`
- weekend windows/fills/orders: `61` / `74` / `104`
- ratios vs weekday avg windows/fills/orders: `0.677778` / `0.561457` / `0.508309`

## Q2 Per-Wallet Edge

- answer: `NEGATIVE_ALL_LIVE_MEMBERS`
- 0x4d8b...a00d: fills `1`, pnl `-2.375245`, roi `-100.0`, edge `negative`
- 0x5e4a...be6b: fills `16`, pnl `-17.577379`, roi `-46.999645`, edge `negative`
- 0xac05...6729: fills `31`, pnl `-12.648688`, roi `-17.214696`, edge `negative`
- 0xd97a...c068: fills `3`, pnl `-2.450001`, roi `-32.885915`, edge `negative`
- 0xe6db...7dac: fills `23`, pnl `-7.741903`, roi `-14.12853`, edge `negative`

## Q3 Weekend Specialists

- answer: `UNPROVEN_CONTINUE_STAKEOUT`
- stakeout_status: `POLL_WAKE_UP_SEEN`
- fresh_alerts: `0`

## Q4 Maker/E7

- answer: `MAKER_FIRST_STRONGEST_PATH_COUNTER_TRUST_DEPENDENT_E11_NEGATIVE`
- maker_first pnl/roi/fills: `24.704575` / `6.953249` / `244`
- maker_first trusted gate fills/status: `24665` / `PASS`
- maker_first trusted source: `data/research/maker_first_btc5m_book_aware_state.json` / `e5_maker_first_btc5m_v1`
- e11 pnl/roi/fills: `-38.216649` / `-11.545815` / `331`

## Named Path

- Keep RULED-FLAT-WEEKEND until 2026-07-13T00:00Z.
- Continue weekend specialist stakeout; admit only after copyability gates pass.
- Treat maker-first as the strongest named weekend path, paper-only until the trusted counter and promotion gates pass.
- Use Fable's day<=-15 degrade threshold for the next live day to prevent repeat drawdown.
