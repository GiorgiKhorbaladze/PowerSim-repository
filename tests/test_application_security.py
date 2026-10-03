"""Application workflow registry, async lifecycle, routes and path safety."""
from __future__ import annotations

from datetime import datetime
import time

import pytest

from powersim.application import ApplicationService, build_fastapi_app
from powersim.contracts import (ProjectContract, ProjectVersionContract, QAReport,
                                ResultEnvelope, SolverDiagnostics, TimeContract,
                                UnitSystem)
from powersim.execution import LocalWorkflowExecutor, resolved_to_workflow_input
from powersim.platform import RunManager


def _project(identifier="project", workflow="deterministic_uc"):
    return ProjectContract(id=identifier, workflow=workflow, version=ProjectVersionContract(revision=1),
        units=UnitSystem(currency="USD"), time=TimeContract(timezone="UTC", study_year=2026,
        resolution_minutes=60, interval_duration_hours=1, start=datetime(2026,1,1), periods=1,
        calendar_policy="explicit_periods"))


@pytest.mark.parametrize("identifier", ["", "..", "../escape", "a/b", "a\\b", "/absolute", "a\x00b", "a" * 129])
def test_public_identifiers_reject_path_traversal(tmp_path, identifier):
    manager=RunManager(tmp_path)
    with pytest.raises(Exception):
        manager.save_project(_project(identifier))
    with pytest.raises(ValueError):
        manager.get_run(identifier)


def test_typed_workflow_is_authoritative_over_legacy_payload():
    resolved=_project(workflow="chronological_adequacy").model_copy(
        update={"legacy_payload":{"_powersim_workflow":"security_scuc"}}
    )
    from powersim.contracts import resolve_scenario
    projected=resolved_to_workflow_input(resolve_scenario(resolved))
    assert projected["_powersim_workflow"]=="chronological_adequacy"


def test_async_launch_returns_queued_then_persists_completion(tmp_path):
    manager=RunManager(tmp_path)
    def runner(_, run_id, fingerprint):
        time.sleep(.05)
        diagnostics=SolverDiagnostics(backend="test", termination_condition="optimal", normalized_status="optimal",
            has_incumbent=True, incumbent_objective=1, result_validity="valid", qa_status="pass")
        return ResultEnvelope(run_id=run_id, snapshot_fingerprint=fingerprint, created_at=datetime.now(),
            validity="valid", solver=diagnostics, qa=QAReport(status="pass"), results={"ok":True})
    service=ApplicationService(manager, LocalWorkflowExecutor(runner))
    service.save_project(_project())
    run=service.create_run("project")
    launched=service.launch(run["id"])
    assert launched["accepted"] and launched["queued"] is True and launched["run"]["status"]=="queued"
    deadline=time.monotonic()+5
    while service.run_status(run["id"])["status"] not in {"completed","failed"} and time.monotonic()<deadline:
        time.sleep(.01)
    assert service.run_status(run["id"])["status"]=="completed"


def test_compare_route_is_registered_before_dynamic_run_route(tmp_path):
    app=build_fastapi_app(ApplicationService(RunManager(tmp_path)))
    paths=[route.path for route in app.routes]
    assert paths.index("/runs/compare") < paths.index("/runs/{run_id}")
