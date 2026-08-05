import json
from datetime import UTC, datetime, timedelta

import pytest

from scripts import build_wide_exact_policy_manifest as manifest_builder
from scripts import order_flow_deadman
from scripts import reconcile_wide_exact_policy_paper as reconciler


def test_manifest_builder_refuses_stale_alpha_source(tmp_path):
    path = tmp_path / "alpha.json"
    path.write_text(
        json.dumps(
            {"updated_at": (datetime.now(tz=UTC) - timedelta(hours=43)).isoformat()}
        )
    )

    with pytest.raises(ValueError, match="STALE_ALPHA_REPORT_REFUSED"):
        manifest_builder.load_fresh_alpha_source(str(path))


def test_deadman_snapshot_surfaces_stale_manifest_alpha_binding(tmp_path):
    now = datetime.now(tz=UTC)
    path = tmp_path / "alpha.json"
    path.write_text(
        json.dumps({"updated_at": (now - timedelta(hours=43)).isoformat()})
    )
    wide = {
        "updated_at": now.isoformat(),
        "policy_id": "paper",
        "manifest": {
            "manifest_id": "manifest",
            "source_alpha_report": str(path),
        },
        "cohort": {"cohort_id": "cohort", "run_id": "run"},
        "terminal_reconciliation": {
            "input_rows": 0,
            "terminal_rows": 0,
            "input_equals_terminal": True,
            "direct_event_handoff": True,
        },
    }

    snapshot = order_flow_deadman._wide_direct_generation_snapshot(wide, now=now)

    assert snapshot["alpha_binding"]["status"] == "STALE_ALPHA_REPORT_REFUSED"
    assert snapshot["alpha_binding"]["source_alpha_age_h"] == 43.0
    assert snapshot["alpha_binding"]["max_age_h"] == 24.0


def test_reconciler_resolves_fresh_named_manifest_pointer(tmp_path):
    now = datetime.now(tz=UTC)
    alpha = tmp_path / "alpha.json"
    manifest = tmp_path / "manifest.json"
    pointer = tmp_path / "active.json"
    alpha.write_text(json.dumps({"updated_at": now.isoformat()}))
    manifest.write_text(
        json.dumps(
            {
                "manifest_id": "manifest-fresh",
                "source_alpha_report": str(alpha),
            }
        )
    )
    pointer.write_text(
        json.dumps(
            {
                "manifest_path": str(manifest),
                "manifest_id": "manifest-fresh",
            }
        )
    )

    assert reconciler.resolve_manifest_pointer(str(pointer)) == str(manifest)


def test_reconciler_refuses_pointer_manifest_id_mismatch(tmp_path):
    manifest = tmp_path / "manifest.json"
    pointer = tmp_path / "active.json"
    manifest.write_text(json.dumps({"manifest_id": "actual"}))
    pointer.write_text(
        json.dumps({"manifest_path": str(manifest), "manifest_id": "wrong"})
    )

    with pytest.raises(ValueError, match="MANIFEST_POINTER_ID_MISMATCH_REFUSED"):
        reconciler.resolve_manifest_pointer(str(pointer))
