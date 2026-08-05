import json

import pytest

from scripts import build_wide_policy_fingerprint_evidence as subject
from scripts.reconcile_wide_exact_policy_paper import wide_policy_identity


def _manifest(run_id, wallet, slices):
    return {
        "score_run_id": run_id,
        "manifest_id": f"manifest-{run_id}",
        "capture_watch_wallets": [{"wallet": wallet, "move_slice_keys": slices}],
    }


def _fill(order_id, run_id, wallet, slice_key):
    return {
        "event": "wide_exact_policy_paper_order_filled",
        "event_id": f"fill-{order_id}",
        "order_id": order_id,
        "run_id": run_id,
        "wallet": wallet,
        "policy_id": subject.POLICY_ID,
        "filled_cost_usd": 1.0,
        "source_event_ts": float(order_id),
        "transaction_hash": f"tx-{order_id}",
        "log_index": order_id,
        "token_id": "token",
        "fill_price": 0.5,
        "market_slug": f"btc-updown-5m-{order_id}",
        "alpha_move_slice": {"move_slice_key": slice_key},
    }


def _resolve(order_id, pnl=1.0):
    return {
        "event": "wide_exact_policy_paper_order_resolved",
        "event_id": f"resolve-{order_id}",
        "order_id": order_id,
        "resolved": True,
        "post_fee_pnl_usd": pnl,
    }


def test_fingerprint_is_order_independent_and_wallet_scoped():
    wallet = "0x" + "1" * 40
    assert wide_policy_identity(wallet=wallet, move_slice_keys=["b", "a"])[
        "wide_policy_fingerprint"
    ] == wide_policy_identity(wallet=wallet, move_slice_keys=["a", "b"])[
        "wide_policy_fingerprint"
    ]
    assert wide_policy_identity(wallet=wallet, move_slice_keys=["a"])[
        "wide_policy_fingerprint"
    ] != wide_policy_identity(wallet=wallet, move_slice_keys=["b"])[
        "wide_policy_fingerprint"
    ]


def test_f1_summary_publishes_concentration_admissibility():
    rows = [
        {
            "resolved": True,
            "source_event_ts": index,
            "order_id": str(index),
            "filled_cost_usd": 1.0,
            "fill_price": 0.5,
            "market_slug": f"btc-updown-5m-{1000 + index}",
            "post_fee_pnl_usd": pnl,
        }
        for index, pnl in enumerate((2.0, 1.0, -0.5))
    ]

    summary = subject._summarize(rows)

    assert summary["top_1_market_share_pct"] == 80.0
    assert summary["pnl_excluding_top_1_market"] == 0.5
    assert summary["concentration_admissible"] is False


def test_f1_summary_uses_same_market_domain_for_concentration():
    summary = subject._summarize(
        [
            {
                "resolved": True,
                "source_event_ts": 1,
                "order_id": "1",
                "filled_cost_usd": 1.0,
                "fill_price": 0.5,
                "market_slug": "not-btc5m",
                "post_fee_pnl_usd": 1.0,
            }
        ]
    )

    assert summary["concentration_market_domain"] == "all_rows"
    assert summary["concentration_excluded_market_domain_rows"] == 0
    assert summary["concentration_row_domain_match"] is True
    assert summary["concentration_admissible"] is False
    assert "concentration_row_domain_match" not in summary["concentration_deficits"]


def test_f1_summary_derives_btc5m_market_identity_from_event_timestamp():
    rows = [
        {
            "resolved": True,
            "source_event_ts": 1001,
            "order_id": "1",
            "filled_cost_usd": 1.0,
            "fill_price": 0.5,
            "post_fee_pnl_usd": 1.0,
            "alpha_move_slice": {"market_type": "btc_5m"},
        }
    ]

    summary = subject._summarize(rows)

    assert summary["top_1_market"] == "btc-updown-5m-900"
    assert summary["concentration_missing_market_identity_rows"] == 0
    assert "concentration_market_identity_complete" not in summary["concentration_deficits"]


def test_best_by_wallet_rejects_old_size_ranked_concentrated_cell(tmp_path):
    wallet = "0x" + "1" * 40
    manifests = []
    ledger = []
    for run, slice_key, pnls in (
        ("r1", "a", (4.0, -1.0, -1.0, -1.0)),
        ("r2", "b", (0.6, 0.6, 0.6)),
    ):
        path = tmp_path / f"{run}.json"
        path.write_text(json.dumps(_manifest(run, wallet, [slice_key])))
        manifests.append(path)
        for index, pnl in enumerate(pnls):
            order_id = f"{1 if run == 'r1' else 2}{index}"
            ledger.extend(
                [
                    _fill(order_id, run, wallet, slice_key),
                    _resolve(order_id, pnl),
                ]
            )

    packet = subject.build_evidence(ledger_rows=ledger, manifests=manifests)
    selected = packet["best_by_wallet"][wallet]
    rejected = packet["best_by_wallet_rejected_for_concentration"][wallet]

    assert selected["identity"]["move_slice_keys"] == ["b"]
    assert rejected["rejected_resolved"] == 4
    assert rejected["rejected_top_1_market_share_pct"] == 400.0


def test_single_cell_walk_forward_requires_clean_second_half(tmp_path):
    wallet = "0x" + "1" * 40
    manifest = tmp_path / "r1.json"
    manifest.write_text(json.dumps(_manifest("r1", wallet, ["a"])))
    ledger = []
    for index in range(400):
        order_id = str(index)
        ledger.extend(
            [_fill(order_id, "r1", wallet, "a"), _resolve(order_id, 0.1)]
        )

    packet = subject.build_evidence(ledger_rows=ledger, manifests=[manifest])
    summary = packet["cells"][0]["fixed_policy_full_stream_rescore"]

    assert summary["first_half"]["resolved"] == 200
    assert summary["second_half"]["resolved"] == 200
    assert summary["selected_by_first_half_rule"] is True
    assert summary["f1_walk_forward_admissible"] is True
    assert summary["selection_count_adjustment"]["cells_tested"] >= 1
    assert summary["selection_count_adjustment"]["h1_used_for_selection_only"] is True


def test_walk_forward_admissibility_is_computed_for_non_selected_cells(tmp_path):
    wallet = "0x" + "2" * 40
    manifests = []
    ledger = []
    for run, slice_key, pnl in (("r1", "a", 0.1), ("r2", "b", 0.2)):
        manifest = tmp_path / f"{run}.json"
        manifest.write_text(json.dumps(_manifest(run, wallet, [slice_key])))
        manifests.append(manifest)
        for index in range(400):
            order_id = f"{1 if run == 'r1' else 2}{index:03d}"
            ledger.extend(
                [_fill(order_id, run, wallet, slice_key), _resolve(order_id, pnl)]
            )
    packet = subject.build_evidence(ledger_rows=ledger, manifests=manifests)
    by_wallet = {}
    for cell in packet["cells"]:
        by_wallet.setdefault(cell["identity"]["wallet"], []).append(cell)
    wallet_cells = next(rows for rows in by_wallet.values() if len(rows) > 1)
    non_selected = next(
        cell for cell in wallet_cells
        if not cell["fixed_policy_full_stream_rescore"]["selected_by_first_half_rule"]
    )
    summary = non_selected["fixed_policy_full_stream_rescore"]
    first = summary["first_half"]
    second = summary["second_half"]
    assert first["pnl_excluding_top_1_market"] == 19.9
    assert second["post_fee_pnl_usd"] == 20.0
    assert second["pnl_excluding_top_1_market"] == 19.9
    assert first["f1_pass"] is True
    assert second["f1_pass"] is True
    assert summary["f1_walk_forward_admissible"] is True
    assert summary["half_pnl_excluding_top_1_market"] == {
        "first_half": first["pnl_excluding_top_1_market"],
        "second_half": second["pnl_excluding_top_1_market"],
    }
    assert summary["selection_count_adjustment"]["cells_tested"] == len(wallet_cells)


def test_bac25_walk_forward_regression_is_frozen_to_published_evidence():
    first_half = {
        "pnl_excluding_top_1_market": 38.7936,
        "f1_pass": True,
    }
    second_half = {
        "post_fee_pnl_usd": 67.807526,
        "pnl_excluding_top_1_market": 31.822262,
        "f1_pass": True,
    }

    assert subject.walk_forward_admissible(first_half, second_half) is True


def test_walk_forward_refuses_positive_half_driven_by_one_market(tmp_path):
    wallet = "0x" + "3" * 40
    manifest = tmp_path / "r1.json"
    manifest.write_text(json.dumps(_manifest("r1", wallet, ["a"])))
    ledger = []
    for index in range(400):
        pnl = 0.1 if index < 200 else (20.0 if index == 200 else -0.05)
        order_id = str(index)
        ledger.extend(
            [_fill(order_id, "r1", wallet, "a"), _resolve(order_id, pnl)]
        )

    packet = subject.build_evidence(ledger_rows=ledger, manifests=[manifest])
    summary = packet["cells"][0]["venue_executable_full_stream_rescore"]

    assert summary["second_half"]["post_fee_pnl_usd"] > 0
    assert summary["second_half"]["pnl_excluding_top_1_market"] <= 0
    assert summary["f1_walk_forward_admissible"] is False


def test_fixed_policy_rescore_filters_to_one_slice_set(tmp_path):
    wallet = "0x" + "1" * 40
    manifests = []
    for run, slices in (("r1", ["a"]), ("r2", ["b"])):
        path = tmp_path / f"{run}.json"
        path.write_text(json.dumps(_manifest(run, wallet, slices)))
        manifests.append(path)
    ledger = []
    for i, (run, slice_key) in enumerate((("r1", "a"), ("r2", "b")), start=1):
        ledger.extend([_fill(str(i), run, wallet, slice_key), _resolve(str(i))])
    packet = subject.build_evidence(ledger_rows=ledger, manifests=manifests)
    assert packet["fingerprint_cell_count"] == 2
    assert all(
        cell["fixed_policy_full_stream_rescore"]["resolved"] == 1
        for cell in packet["cells"]
    )
    assert {
        tuple(cell["move_slice_venue_executable_full_stream_rescore"])
        for cell in packet["cells"]
    } == {("a",), ("b",)}
    assert len(packet["manifest_wallet_fingerprints"]) == 2


def test_atomic_move_slice_rescore_partitions_overlapping_fingerprint_sets(tmp_path):
    wallet = "0x" + "7" * 40
    manifests = []
    ledger = []
    for run, slices in (("r1", ["a", "b"]), ("r2", ["a"])):
        path = tmp_path / f"{run}.json"
        path.write_text(json.dumps(_manifest(run, wallet, slices)))
        manifests.append(path)
    for index in range(800):
        run = "r1" if index < 400 else "r2"
        slice_key = "a" if index % 2 == 0 else "b"
        order_id = str(index)
        ledger.extend(
            [_fill(order_id, run, wallet, slice_key), _resolve(order_id, 0.1)]
        )

    packet = subject.build_evidence(ledger_rows=ledger, manifests=manifests)
    atomic = packet["atomic_move_slice_rescore"]
    wallet_rows = [row for row in atomic["rows"] if row["wallet"] == wallet]

    assert {row["move_slice_key"] for row in wallet_rows} == {"a", "b"}
    assert sum(row["source_row_count"] for row in wallet_rows) == 800
    assert atomic["unique_resolved_count"] == 800
    assert atomic["assigned_resolved_count"] == 800
    assert atomic["resolved_partition_reconciles"] is True
    assert atomic["duplicate_assignment_count"] == 0
    assert atomic["generated_at"]
    assert atomic["scope"] == {"status": "IN_PROCESS_UNSPECIFIED"}
    assert atomic["wallet_scope"] == [wallet]
    assert atomic["by_wallet"][wallet]["unique_resolved_count"] == 800
    assert atomic["by_wallet"][wallet]["max_fixed_policy_min_half_resolved"] == 200
    assert atomic["venue_walk_forward_admissible_count"] == 2
    assert all(
        row["venue_executable_rescore"]["venue_order_type"] == "taker"
        for row in wallet_rows
    )
    assert all(row["identity_min_order_usd_values"] == [1.0] for row in wallet_rows)
    assert all(row["paper_only"] is True for row in wallet_rows)
    assert all(row["promotion_authority"] is False for row in wallet_rows)


def test_incompatible_run_filters_do_not_pool_observed_evidence(tmp_path):
    wallet = "0x" + "1" * 40
    manifests = []
    ledger = []
    for i in range(200):
        run = "r1" if i < 100 else "r2"
        slice_key = "a" if run == "r1" else "b"
        path = tmp_path / f"{run}.json"
        if not path.exists():
            path.write_text(json.dumps(_manifest(run, wallet, [slice_key])))
            manifests.append(path)
        ledger.extend([_fill(str(i), run, wallet, slice_key), _resolve(str(i))])
    packet = subject.build_evidence(ledger_rows=ledger, manifests=manifests)
    assert all(
        cell["observed_same_fingerprint"]["resolved"] == 100
        for cell in packet["cells"]
    )
    assert all(
        cell["observed_same_fingerprint"]["f1_pass"] is False
        for cell in packet["cells"]
    )


def test_venue_f1_refuses_price_selected_subset_below_reachability_floor():
    rows = [
        {
            "resolved": True,
            "fill_price": 0.49 if index < 3 else 0.51,
            "filled_cost_usd": 1.0,
            "post_fee_pnl_usd": 0.1,
            "market_slug": f"btc-updown-5m-{1000 + index}",
        }
        for index in range(10)
    ]

    summary = subject._venue_executable_summary(rows, min_order_usd=1.0)

    assert summary["venue_reachable_share_pct"] == 30.0
    assert summary["f1_venue_reachable_admissible"] is False
    assert summary["f1_pass"] is False
    assert "venue_reachable_share_gte_40pct" in summary["f1_deficits"]


def test_taker_summary_does_not_publish_maker_share_minimum():
    summary = subject._venue_executable_summary(
        [{"resolved": True, "fill_price": 0.9}],
        min_order_usd=1.0,
        order_type="taker",
    )

    assert summary["venue_order_type"] == "taker"
    assert summary["venue_minimum_shares"] is None
    assert summary["venue_nominal_min_order_usd"] == 1.0
    assert summary["venue_minimum_max_price"] == 1.0
    assert summary["f1_venue_reachable_admissible"] is True


def test_venue_summary_publishes_reason_and_entry_band_agreement():
    rows = [
        {
            "resolved": True,
            "fill_price": 0.4,
            "alpha_move_slice": {"entry_price_band": "0.25-0.50"},
        },
        {
            "resolved": True,
            "fill_price": 0.6,
            "alpha_move_slice": {"entry_price_band": "0.25-0.50"},
        },
        {"resolved": True},
    ]
    summary = subject._venue_executable_summary(rows, min_order_usd=1.0)

    assert summary["venue_discard_reason_counts_resolved"] == {
        "executable": 1,
        "price_above_venue_minimum_max_price": 1,
        "price_field_absent": 1,
    }
    assert summary["venue_discard_price_band_counts_resolved"] == {
        "0.50-0.60": 1,
        "unknown": 1,
    }
    assert summary["entry_price_band_agreement"] == {
        "agree": 1,
        "disagree": 1,
        "band_unknown": 1,
    }


@pytest.mark.parametrize(
    ("rows", "min_order_usd", "verdict"),
    [
        ([{"resolved": True}, {"resolved": True, "fill_price": 0.4}], 1.0, "COVERAGE_DEFECT"),
        ([{"resolved": True, "fill_price": 0.8}, {"resolved": True, "fill_price": 0.4}], 1.0, "STRUCTURAL_REACHABILITY"),
        ([{"resolved": True, "fill_price": 0.4}], 0.0, "MIXED_NO_ACTION"),
    ],
)
def test_order134_b_precommitted_verdict_branches(rows, min_order_usd, verdict):
    unique = {"0xwallet": {str(index): row for index, row in enumerate(rows)}}
    result = subject._order134_b_venue_discard_decomposition(
        unique, min_order_usd=min_order_usd
    )
    assert result["verdict"] == verdict
    assert set(result["reason_counts"]) == {
        "executable",
        "price_field_absent",
        "price_nonpositive",
        "min_order_usd_nonpositive",
        "price_above_venue_minimum_max_price",
    }
    assert sum(result["entry_price_band_agreement"].values()) == result["resolved_total"]
    assert result["paper_only"] is True
    assert result["promotion_authority"] is False


def test_order134_d_reports_marginal_rescaled_edge_and_refuses_rate():
    wallet = "0x" + "8" * 40
    rows = {
        str(index): {
            "wallet": wallet,
            "resolved": True,
            "fill_price": 0.55,
            "filled_shares": 5.0,
            "filled_cost_usd": 2.75,
            "post_fee_pnl_usd": 0.1,
            "source_event_ts": index,
            "order_id": str(index),
            "market_slug": f"btc-updown-5m-{1000 + 300 * index}",
            "alpha_move_slice": {
                "move_slice_key": "120-180|0.50-0.75",
                "entry_price_band": "0.50-0.75",
            },
        }
        for index in range(400)
    }
    fingerprint = "f" * 64
    result = subject._order134_d_venue_min_order_sweep(
        unique_by_wallet={wallet: rows},
        fingerprints={
            fingerprint: {
                "wallet": wallet,
                "move_slice_keys": ["120-180|0.50-0.75"],
            }
        },
    )
    by_nominal = {point["min_order_usd"]: point for point in result["points"]}

    assert by_nominal[2.5]["verdict"] == "CEILING_DUPLICATE"
    first_band = by_nominal[3.0]
    assert first_band["marginal"]["resolved"] == 400
    assert first_band["marginal"]["post_fee_pnl_usd"] > first_band["marginal"]["raw_post_fee_pnl_usd"]
    assert first_band["marginal_edge_pass"] is True
    assert first_band["marginal_sample_pass"] is True
    assert first_band["marginal"]["f1_pass_cells"] == 1
    assert first_band["marginal"]["walk_forward_pass_cells"] == 1
    assert first_band["verdict"] == "REFUSE_RATE_UNAFFORDABLE"
    assert first_band["refusal_deficits"] == ["target_rate_unaffordable"]
    assert first_band["loss_bound"]["hard_bound_pass"] is True
    assert first_band["loss_bound"]["target_rate_affordable"] is False
    assert by_nominal[5.0]["venue_gate_is_vacuous_at_ceiling"] is True
    assert result["overall_verdict"] == "NO_SIZE_UP_PROPOSAL"
    assert result["paper_only"] is True
    assert result["promotion_authority"] is False
