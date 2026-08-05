from __future__ import annotations

from scripts.launch_same_window_research_capture import build_launchd_payload


def test_launchd_payload_retries_failure_but_not_success() -> None:
    payload = build_launchd_payload(
        run_id="20260719T162300Z",
        duration_s=7500.0,
        python="/usr/bin/python3",
        output_dir="data/research/same_window_capture",
    )

    assert payload["KeepAlive"] == {"SuccessfulExit": False}
    assert payload["RunAtLoad"] is True


def test_launchd_payload_injects_certifi_ca_and_exact_run_id() -> None:
    payload = build_launchd_payload(
        run_id="20260719T162300Z",
        duration_s=7500.0,
        python="/usr/bin/python3",
        output_dir="data/research/same_window_capture",
    )

    assert payload["EnvironmentVariables"]["SSL_CERT_FILE"].endswith("cacert.pem")
    argv = payload["ProgramArguments"]
    assert argv[argv.index("--run-id") + 1] == "20260719T162300Z"
