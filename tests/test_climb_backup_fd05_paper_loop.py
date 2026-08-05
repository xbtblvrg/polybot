import json

import pytest

from scripts.run_climb_backup_fd05_paper_loop import (
    isolated_write_path,
    validate_manifest,
)


def test_isolated_write_path_rejects_primary_outputs():
    with pytest.raises(ValueError, match="non-isolated"):
        isolated_write_path("data/research/wide_exact_policy_paper_state.json")


def test_validate_manifest_requires_paper_only_no_promotion(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "paper_only": True,
                "live_orders_allowed": False,
                "capture_watch_wallets": [
                    {
                        "paper_measurement_only": True,
                        "promotion_authority": False,
                    }
                ],
            }
        )
    )
    validate_manifest(path)

    payload = json.loads(path.read_text())
    payload["capture_watch_wallets"][0]["promotion_authority"] = True
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="promotion_authority"):
        validate_manifest(path)
