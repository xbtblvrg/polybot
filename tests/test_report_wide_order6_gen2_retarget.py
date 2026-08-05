from scripts.report_wide_order6_gen2_retarget import evaluate


def _row(fp: str, resolved: int, plausible: bool, first: float = 1, second: float = 1):
    return {
        "wide_policy_fingerprint": fp,
        "resolved": resolved,
        "residual_to_200": 200 - resolved,
        "venue_reachable_share_pct": 60,
        "first_half": {"post_fee_pnl_usd": first},
        "second_half": {"post_fee_pnl_usd": second},
        "residual_zero_clearance_projection": {"plausibly_clears_at_residual_zero": plausible},
    }


def test_retargets_on_near_zero_velocity_to_healthy_runner() -> None:
    report = evaluate(
        {"focus": _row("old", 50, False)},
        {"wallet": "0xabc", "focus": _row("old", 51, False), "runner_up_for_conditional_fable_disposition_only": _row("new", 25, True)},
    )
    assert report["retarget_applied"] is True
    assert report["to_fingerprint"] == "new"
    assert report["bars_mutated"] is False


def test_holds_when_velocity_is_material() -> None:
    report = evaluate(
        {"focus": _row("old", 50, False)},
        {"focus": _row("old", 65, False), "runner_up_for_conditional_fable_disposition_only": _row("new", 25, True)},
    )
    assert report["decision"] == "HOLD_FOCUS_CONTINUE_GEN3"
