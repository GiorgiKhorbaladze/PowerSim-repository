"""Release-only 8760-hour Georgia planning demonstration acceptance.

This test is intentionally separate from the fast pull-request suite.  It
executes the current typed application project through the same queued executor,
canonical QA, publication gate and persisted result envelope used by the UI.
The project is sanitized and aggregated; it is not private operational data.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from powersim.application import ApplicationService
from powersim.contracts import ProjectContract
from powersim.execution import RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager


pytestmark = pytest.mark.skipif(
    os.environ.get("POWERSIM_ANNUAL_ACCEPTANCE") != "1",
    reason="set POWERSIM_ANNUAL_ACCEPTANCE=1 for the release 8760-hour gate",
)

ROOT = Path(__file__).resolve().parents[1]


def test_georgia_2026_annual_application_run_is_publishable(tmp_path):
    project = ProjectContract.model_validate_json(
        (ROOT / "samples/projects/georgia_2026_baseline.json").read_text(encoding="utf-8")
    )
    assert project.time.periods == 8760
    assert project.workflow == "deterministic_uc"

    manager = RunManager(tmp_path / "workspace")
    manager.save_project(project)
    service = ApplicationService(manager, RegisteredLocalWorkflowExecutor())
    run = service.create_run(project.id)
    launched = service.launch(run["id"])
    assert launched["accepted"] and launched["queued"]

    deadline = time.monotonic() + 600
    while service.run_status(run["id"])["status"] not in {"completed", "failed"}:
        assert time.monotonic() < deadline, "8760-hour release acceptance timed out"
        time.sleep(0.25)

    status = service.run_status(run["id"])
    assert status["status"] == "completed"
    envelope = service.result(run["id"])
    assert envelope["qa"]["status"] == "pass"
    assert envelope["validity"] == "valid"
    assert envelope["results"]["publication"]["publishable"] is True
    assert manager.verify_run(run["id"])["valid"] is True

    diagnostics = envelope["solver"]
    evidence = {
        "periods": project.time.periods,
        "run_id": run["id"],
        "snapshot_fingerprint": envelope["snapshot_fingerprint"],
        "backend": diagnostics["backend"],
        "termination_condition": diagnostics["termination_condition"],
        "actual_mip_gap": diagnostics.get("actual_mip_gap"),
        "best_bound": diagnostics.get("best_bound"),
        "qa": envelope["qa"]["status"],
        "validity": envelope["validity"],
        "publication": envelope["results"]["publication"],
    }
    print("ANNUAL_RELEASE_ACCEPTANCE=" + json.dumps(evidence, sort_keys=True))
