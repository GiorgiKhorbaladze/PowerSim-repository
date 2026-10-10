"""Publishable application release smoke.

Unlike the retained legacy structural smoke, this test traverses the current
application -> queued executor -> canonical QA -> publication path.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from powersim.application import ApplicationService
from powersim.contracts import ProjectContract
from powersim.execution import RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager


ROOT = Path(__file__).resolve().parents[1]


def test_compact_application_release_smoke_is_publishable(tmp_path):
    project=ProjectContract.model_validate_json((ROOT/"samples/application_compact_demo.json").read_text(encoding="utf-8"))
    manager=RunManager(tmp_path/"workspace")
    manager.save_project(project)
    service=ApplicationService(manager, RegisteredLocalWorkflowExecutor())
    run=service.create_run(project.id)
    launched=service.launch(run["id"])
    assert launched["accepted"] and launched["queued"]
    deadline=time.monotonic()+30
    while service.run_status(run["id"])["status"] not in {"completed","failed"}:
        assert time.monotonic()<deadline
        time.sleep(0.02)
    envelope=service.result(run["id"])
    assert service.run_status(run["id"])["status"] == "completed"
    assert envelope["qa"]["status"] == "pass"
    assert envelope["validity"] == "valid"
    assert manager.verify_run(run["id"])["valid"]
