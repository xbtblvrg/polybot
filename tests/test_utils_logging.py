from src import utils


def test_pytest_logging_is_separated_from_production_guard_log(monkeypatch) -> None:
    monkeypatch.setattr(utils.sys, "argv", ["python", "-m", "pytest"])
    assert utils._effective_log_path("logs/wallet_copy.log") == "logs/wallet_copy_test.log"
    assert utils._effective_log_path("logs/custom.log") == "logs/custom.log"
