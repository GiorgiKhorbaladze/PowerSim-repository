"""Production executor regression coverage."""
from datetime import datetime
import time

from powersim.application import ApplicationService
from powersim.contracts import (AssetContract, ProfileContract, ProjectContract,
                                ProjectVersionContract, TimeContract, UnitSystem)
from powersim.execution import RegisteredLocalWorkflowExecutor, run_deterministic_workflow
from powersim.platform import RunManager
from tests.test_stage4c_mixed_fleet import _input


def test_real_deterministic_executor_uses_gated_solver_output():
    result = run_deterministic_workflow(_input(resolution_min=60, rolling=False), "run-1", "snapshot-1")
    assert result.run_id == "run-1"
    assert result.snapshot_fingerprint == "snapshot-1"
    assert result.qa.status.value == "pass"
    assert result.validity.value == "valid"
    assert result.results["publication"]["publishable"] is True


def test_registered_executor_runs_immutable_snapshot_end_to_end(tmp_path):
    """The production adapter must detach, not mutate, its resolved snapshot."""
    raw = _input(resolution_min=60, rolling=False)
    project = ProjectContract(
        id="registered-executor", version=ProjectVersionContract(revision=1),
        units=UnitSystem(currency="USD"),
        time=TimeContract(timezone="UTC", study_year=2026, resolution_minutes=60,
            interval_duration_hours=1, start=datetime(2026, 1, 1), periods=4,
            calendar_policy="explicit_periods"),
        assets=[AssetContract(id=a["id"], kind=a["type"], bus=a.get("bus"),
            capacity_min_mw=a.get("pmin"), capacity_max_mw=a.get("pmax", a.get("power_mw")),
            legacy_extensions=a) for a in raw["assets"]],
        profiles=[ProfileContract(id=k, unit="MW" if k == "demand" else "availability_factor", values=v)
                  for k, v in raw["profiles"].items()],
        reserve_products=raw["reserve_products"], solver_settings=raw["solver_settings"],
        legacy_payload=raw,
    )
    manager = RunManager(tmp_path)
    manager.save_project(project)
    service = ApplicationService(manager, RegisteredLocalWorkflowExecutor())
    run = service.create_run(project.id)
    assert service.launch(run["id"])["queued"] is True
    deadline = time.monotonic() + 10
    while service.run_status(run["id"])["status"] not in {"completed", "failed"}:
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert service.run_status(run["id"])["status"] == "completed"
    envelope = service.result(run["id"])
    assert envelope["qa"]["status"] == "pass"
    assert envelope["validity"] == "valid"
    assert manager.verify_run(run["id"])["valid"]
