from scripts import run_btc5m_cross_exchange_comparator_matrix as matrix


def test_matrix_has_eight_isolated_paper_cells_and_distinct_preregistrations():
    rows = [
        matrix._preregistration(offset, mode, f"model-{offset}")
        for offset in matrix.OFFSETS
        for mode in ("taker", "passive")
    ]
    assert len(rows) == 8
    assert len({row["checksum"] for row in rows}) == 8
    assert all(row["paper_only"] is True for row in rows)
    assert all(row["live_orders_allowed"] is False for row in rows)


def test_nondefault_offsets_have_distinct_model_experiment_ids():
    from scripts import run_btc5m_cross_exchange_probability_edge_paper_lane as lane

    assert lane._experiment_id(30) == lane.EXPERIMENT_ID
    assert len({lane._experiment_id(offset) for offset in matrix.OFFSETS}) == 4
