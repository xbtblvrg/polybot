from scripts.report_coverage_gap_signal_supply_check import build_signal_supply_report


def _coverage_gap(start: int) -> dict:
    return {
        "kind": "coverage_gap_diagnosis",
        "generated_at": "2026-07-10T07:15:20Z",
        "window": {"start_ts": start, "end_ts": start + 1800, "windows_total": 6},
        "rows": [
            {
                "market_slug": f"btc-updown-5m-{start}",
                "window_start_s": start,
                "reason_class": "no-eligible-signal",
                "observed_guard_rollup": False,
            },
            {
                "market_slug": f"btc-updown-5m-{start + 300}",
                "window_start_s": start + 300,
                "reason_class": "no-eligible-signal",
                "observed_guard_rollup": False,
            },
            {
                "market_slug": f"btc-updown-5m-{start + 600}",
                "window_start_s": start + 600,
                "reason_class": "no-eligible-signal",
                "observed_guard_rollup": False,
            },
            {
                "market_slug": f"btc-updown-5m-{start + 900}",
                "window_start_s": start + 900,
                "reason_class": "no-eligible-signal",
                "observed_guard_rollup": True,
            },
            {
                "market_slug": f"btc-updown-5m-{start + 1200}",
                "window_start_s": start + 1200,
                "reason_class": "price/eligibility-filter",
                "observed_guard_rollup": True,
            },
        ],
    }


def _guard_state() -> dict:
    return {
        "active_set": {
            "members": [
                {
                    "source_wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
                    "candidate_id": "disabled",
                    "policy_id": "p",
                },
                {
                    "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "candidate_id": "a",
                    "policy_id": "p",
                },
                {
                    "source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "candidate_id": "b",
                    "policy_id": "p",
                },
            ]
        },
        "active_set_runtime": {
            "members": [
                {
                    "source_wallet": "0xcccccccccccccccccccccccccccccccccccccccc",
                    "candidate_id": "disabled",
                    "policy_id": "p",
                    "enabled": False,
                },
                {
                    "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "candidate_id": "a",
                    "policy_id": "p",
                    "enabled": True,
                },
                {
                    "source_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "candidate_id": "b",
                    "policy_id": "p",
                    "enabled": True,
                },
            ]
        }
    }


def test_signal_supply_check_splits_idle_from_traded_unobserved() -> None:
    start = 1_800_000_000
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    report = build_signal_supply_report(
        coverage_gap=_coverage_gap(start),
        guard_state=_guard_state(),
        wallet_trades={
            wallet_a: [
                {
                    "slug": f"btc-updown-5m-{start + 300}",
                    "proxyWallet": wallet_a,
                    "transactionHash": "0x1",
                    "side": "BUY",
                    "timestamp": "2026-07-10T07:05:00Z",
                },
                {
                    "slug": f"eth-updown-5m-{start + 600}",
                    "proxyWallet": wallet_a,
                    "transactionHash": "0x2",
                    "side": "BUY",
                    "timestamp": "2026-07-10T07:10:00Z",
                },
            ],
            wallet_b: [],
        },
        fetch_meta={wallet_a: {"pages": 1}, wallet_b: {"pages": 1}},
        history_state={
            "events": [
                {
                    "source_wallet": wallet_a,
                    "market_slug": f"btc-updown-5m-{start + 300}",
                    "source": "rtds_activity",
                    "action": "BUY",
                    "event_ts": start + 310,
                    "observed_ts": start + 311,
                    "transaction_hash": "0x1",
                }
            ]
        },
    )

    assert report["summary"]["unobserved_no_signal_windows"] == 3
    assert report["summary"]["sources_idle_windows"] == 2
    assert report["summary"]["sources_traded_but_unobserved_windows"] == 1
    assert report["summary"]["unknown_fetch_incomplete_windows"] == 0
    assert report["summary"]["dominant_class"] == "sources_idle"
    assert report["summary"]["wallet_hit_counts"] == {wallet_a: 1}
    assert report["summary"]["traded_unobserved_with_local_history_events"] == 1
    assert report["summary"]["traded_unobserved_without_local_history_events"] == 0
    assert report["summary"]["root_cause"] == "participation_rollup_retention_gap_not_source_ingest"

    by_start = {row["window_start_s"]: row for row in report["rows"]}
    assert by_start[start]["classification"] == "sources_idle"
    assert by_start[start + 300]["classification"] == "sources_traded_but_unobserved"
    assert by_start[start + 300]["active_wallets_with_remote_trades"] == [wallet_a]
    assert by_start[start + 300]["local_history_event_count"] == 1
    assert by_start[start + 600]["classification"] == "sources_idle"

    wallet_rows = {row["wallet"]: row for row in report["active_wallets"]}
    assert wallet_rows[wallet_a]["remote_trade_windows_inside_unobserved"] == 1
    assert wallet_rows[wallet_b]["remote_trade_windows_inside_unobserved"] == 0


def test_signal_supply_check_marks_idle_unknown_when_fetch_incomplete() -> None:
    start = 1_800_000_000
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    report = build_signal_supply_report(
        coverage_gap=_coverage_gap(start),
        guard_state=_guard_state(),
        wallet_trades={wallet_a: [], wallet_b: []},
        fetch_meta={wallet_a: {"truncated": True}, wallet_b: {"pages": 1}},
    )

    assert report["summary"]["fetch_complete"] is False
    assert report["summary"]["incomplete_wallets"] == [wallet_a]
    assert report["summary"]["sources_idle_windows"] == 0
    assert report["summary"]["unknown_fetch_incomplete_windows"] == 3
    assert {row["classification"] for row in report["rows"]} == {"unknown_fetch_incomplete"}


def test_signal_supply_check_reports_resolved_when_no_unobserved_no_signal_windows() -> None:
    start = 1_800_000_000

    report = build_signal_supply_report(
        coverage_gap={
            "kind": "coverage_gap_diagnosis",
            "generated_at": "2026-07-10T07:15:20Z",
            "window": {"start_ts": start, "end_ts": start + 600, "windows_total": 2},
            "rows": [
                {
                    "market_slug": f"btc-updown-5m-{start}",
                    "window_start_s": start,
                    "reason_class": "selector-abstain",
                    "observed_guard_rollup": False,
                    "history_derived_signal": True,
                }
            ],
        },
        guard_state=_guard_state(),
        wallet_trades={},
        fetch_meta={},
        history_state={},
    )

    assert report["summary"]["unobserved_no_signal_windows"] == 0
    assert report["summary"]["dominant_class"] == "none"
    assert report["summary"]["root_cause"] == "no_unobserved_no_signal_windows_after_history_derived_coverage"
    assert report["rows"] == []
