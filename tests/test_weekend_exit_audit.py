import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "report_weekend_exit_audit.py"
    spec = importlib.util.spec_from_file_location("report_weekend_exit_audit", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_weekend_exit_audit_flags_enabled_member_without_ruling() -> None:
    mod = _load_module()
    guard_state = {
        "generated_at": "2026-07-13T09:00:00Z",
        "active_set": {
            "members": [
                {
                    "candidate_id": "active_a",
                    "source_wallet": "0xactive",
                }
            ]
        },
        "active_set_runtime": {
            "temporal_slice_exclusion": {
                "as_of": "2026-07-13T09:00:01Z",
                "flow_stage": "LIVE/LEARN/ROTATE",
                "excluded_members": [
                    {
                        "candidate_id": "excluded_b",
                        "source_wallet": "0xexcluded",
                        "classification": "FADING",
                        "reason": "temporal_slice_weekday_proven_negative",
                        "matched_slice": {
                            "slice": "weekday",
                            "label": "PROVEN-NEGATIVE",
                            "resolved_trades": 7,
                            "roi_pct": -1.2,
                            "pnl_usd": -3.4,
                        },
                    }
                ],
            }
        },
    }
    auto_state = {
        "updated_at": "2026-07-13T09:00:02Z",
        "latest_weekend_roster_alignment": {
            "direction_id": "2026-07-11T08:52Z-fable-weekend-roster-alignment",
            "applied_at": "2026-07-11T08:57:36Z",
            "actions": [
                {
                    "candidate_id": "active_a",
                    "source_wallet": "0xactive",
                    "action": "WEEKEND_BENCHED",
                }
            ],
        },
        "latest_5e4a_t2_defend_demotion": {
            "candidate_id": "demoted_c",
            "source_wallet": "0xdemoted",
            "direction_id": "2026-07-11T08:21Z-fable-t2-defend-cap1-demotion",
            "demoted_at": "2026-07-11T08:27:30Z",
        },
        "latest_ac05_e4_suppression": {
            "candidate_id": "suppressed_d",
            "source_wallet": "0xsuppressed",
            "direction_id": "2026-07-11T14:13Z-fable-e4-ratified-a95b-stands",
            "applied_at": "2026-07-11T14:20:00Z",
        },
        "members": [
            {"candidate_id": "active_a", "source_wallet": "0xactive", "enabled": True},
            {"candidate_id": "excluded_b", "source_wallet": "0xexcluded", "enabled": True},
            {"candidate_id": "unknown_e", "source_wallet": "0xunknown", "enabled": True},
        ],
    }

    report = mod.build_report(guard_state, auto_state)
    rows = {(row["source_wallet"], row["candidate_id"]): row for row in report["rows"]}

    assert rows[("0xactive", "active_a")]["action"] == "WEEKEND_BENCHED_AUTO_RETURNED_ACTIVE"
    assert rows[("0xexcluded", "excluded_b")]["direction_id"] == mod.TEMPORAL_LIVE_EFFECT_DIRECTION_ID
    assert rows[("0xunknown", "unknown_e")]["action"] == "RE_EVALUATE_REQUIRED"
    assert report["summary"]["re_evaluate_required_count"] == 1
