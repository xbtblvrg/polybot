from scripts.report_wide_order7_diagnosis import (
    build_metadata_diagnosis,
    build_reanchor,
    roster_jaccard,
    roster_summary,
    terminal_summary,
)


def _packet(alpha: int, metadata: int, copyable: int, unknown: int) -> dict:
    total = alpha + metadata + copyable
    return {
        "wallets": [
            {
                "wallet": "0x951bd740ef681d05891ca35440232488271d433e",
                "wide_policy_fingerprint": "5d113e3b966ec2645cafa46d78c95354b5727d842122659a28a04a2f90f51934",
                "raw_input_rows": total,
                "copyable_buy_events": copyable,
                "terminal_taxonomy": {
                    "REFUSED_ALPHA_PROFILE_FILTER": alpha,
                    "REFUSED_METADATA_MISSING": metadata,
                    "COPYABLE_EXACT_POLICY_PAPER_FILL": copyable,
                },
            }
        ],
        "infrastructure_refusal_decomposition": {
            "by_refusal_class": {
                "REFUSED_METADATA_MISSING": {
                    "rows": metadata,
                    "btc5m_membership": {"UNKNOWN": unknown},
                    "metadata_token_status": {"TOKEN_ABSENT_FROM_CACHE": metadata},
                }
            }
        },
    }


def _manifest(wallets: list[str]) -> dict:
    return {
        "capture_watch_wallets": [
            {"wallet": wallet, "wide_policy_fingerprint": "fp", "move_slice_keys": ["slice"]}
            for wallet in wallets
        ],
        "source_identity": {"eligible_profile_count": 2},
    }


def test_terminal_and_roster_summaries() -> None:
    assert terminal_summary(_packet(7, 2, 1, 2))["alpha_share_pct"] == 70.0
    left, right = roster_summary(_manifest(["0xa", "0xb"])), roster_summary(_manifest(["0xa", "0xc"]))
    assert left["frozen_policy_identity_coverage_pct"] == 100.0
    assert roster_jaccard(left, right) == 0.333333


def test_stable_scorer_and_roster_select_mix_shift() -> None:
    code = {
        "scorer": {"blob_sha": "same"},
        "supervisor": {"blob_sha": "same"},
    }
    report = build_reanchor(
        baseline_manifest=_manifest(["0xa"]),
        clean_manifest=_manifest(["0xa"]),
        gen1_packet=_packet(300, 600, 100, 600),
        gen2_packet=_packet(700, 200, 100, 100),
        baseline_mix={"event_hour_utc_histogram": {"18": 10}},
        clean_mix={"event_hour_utc_histogram": {"21": 10}},
        baseline_code=code,
        clean_code=code,
    )
    assert report["verdict"] == "MIX_SHIFT_NONSTATIONARY"
    assert report["control_chart"]["recenter_applied"] is True
    assert report["control_chart"]["new_centerline_alpha_share_pct"] == 50.0
    assert report["control_chart"]["noise_method"].startswith("within-generation")
    assert report["control_chart"]["points_outside_band"] == ["clean_gen1", "clean_gen2"]
    assert report["control_chart"]["gate_status"] == "INFORMATIVE_ONLY"


def test_metadata_unknown_shrink_and_handoff(tmp_path) -> None:
    cache = tmp_path / "cache.json"
    cache.write_text("{}")
    report = build_metadata_diagnosis(
        gen1_packet=_packet(3, 6, 1, 5),
        gen2_packet=_packet(7, 2, 1, 1),
        cache_path=cache,
    )
    assert report["unknown_shrink"]["rows"] == 4
    assert report["capture_handoff"]["token_id_omitted_before_scorer"] is False
    assert report["writer_change_authorized"] is False
