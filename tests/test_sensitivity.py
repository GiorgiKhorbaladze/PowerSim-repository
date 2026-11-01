"""Scenario 'scale' overlays, sensitivity runs and the scenario overview."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from powersim.contracts import ProjectContract, ScenarioOverlayError, resolve_scenario
from powersim.validation.readiness import diagnose_project

ROOT = Path(__file__).resolve().parents[1]


def _compact(scale=None) -> dict:
    project = json.loads((ROOT / "samples/application_compact_demo.json").read_text(encoding="utf-8"))
    if scale is not None:
        project["scenarios"] = [{"id": "s", "overlay": {"scale": scale}}]
    return project


def test_scale_multiplies_profiles_and_asset_parameters():
    resolved = resolve_scenario(ProjectContract.model_validate(_compact(
        {"profiles": {"demand": 1.5}, "assets": {"thermal": {"capacity_max_mw": 0.5, "vom": 2}}})), "s")
    assert list(resolved.profiles[0].values) == [30, 30, 30, 30]
    thermal = resolved.assets[0]
    assert thermal.capacity_max_mw == 15 and thermal.legacy_extensions["pmax"] == 15
    assert thermal.legacy_extensions["vom"] == 20
    assert dict(resolved.metadata)["scenario_scale"]["profiles"] == {"demand": 1.5}


@pytest.mark.parametrize("scale,fragment", [
    ({"profiles": {"nope": 1.1}}, "unknown profile"),
    ({"profiles": {"demand": -1}}, ">= 0"),
    ({"assets": {"thermal": {"name": 2}}}, "cannot be scaled"),
    ({"assets": {"ghost": {"vom": 2}}}, "unknown asset"),
    ({"assets": {"import": {"heat_rate": 2}}}, "has no heat_rate"),
    ({"other": {}}, "only contain"),
])
def test_invalid_scales_fail_closed(scale, fragment):
    project = ProjectContract.model_validate(_compact(scale))
    with pytest.raises(ScenarioOverlayError, match=fragment):
        resolve_scenario(project, "s")
    diagnostics = diagnose_project(project.model_dump(mode="json"))
    assert not diagnostics["contract_valid"]
    assert any(i["code"] == "invalid_scenario_scale" for i in diagnostics["issues"])


def test_sensitivity_runs_and_overview(tmp_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from powersim.application import ApplicationService, build_fastapi_app
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager
    client = TestClient(build_fastapi_app(ApplicationService(RunManager(tmp_path), RegisteredLocalWorkflowExecutor(),
                                                             asynchronous=False)))
    client.post("/samples/application_compact_demo/load")
    base = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{base['id']}/launch")
    created = client.post("/projects/application-compact-demo/sensitivity",
                          json={"target": {"type": "profile", "id": "demand"}, "factors": [0.5, 1.5]}).json()
    assert created["scenarios"] == ["sens-demand-050", "sens-demand-150"] and created["revision"] == 2
    assert len(created["runs"]) == 2
    overview = {row["scenario_id"]: row for row in client.get("/projects/application-compact-demo/overview").json()}
    # demand 80 MWh at thermal 10 USD/MWh (import 100 USD/MWh is never needed)
    assert overview[None]["metrics"]["total_energy_mwh"] == pytest.approx(80)
    assert overview["sens-demand-050"]["metrics"]["total_energy_mwh"] == pytest.approx(40)
    assert overview["sens-demand-050"]["metrics"]["total_objective_cost_usd"] == pytest.approx(400)
    assert overview["sens-demand-150"]["metrics"]["total_energy_mwh"] == pytest.approx(120)
    assert overview["sens-demand-150"]["metrics"]["total_objective_cost_usd"] == pytest.approx(1200)
    project = client.get("/projects/application-compact-demo").json()["project"]
    assert project["metadata"]["sensitivity_groups"]["demand"]["factors"] == [0.5, 1.5]
    # asset parameter sensitivity on top of an existing scenario
    asset = client.post("/projects/application-compact-demo/sensitivity",
                        json={"target": {"type": "asset", "id": "thermal", "field": "vom"}, "factors": [3],
                              "base_scenario": "sens-demand-150", "launch": False}).json()
    assert asset["scenarios"] == ["sens-thermal-vom-300-sens-demand-150"] and asset["runs"] == []
    scenario = next(s for s in client.get("/projects/application-compact-demo").json()["project"]["scenarios"]
                    if s["id"] == asset["scenarios"][0])
    assert scenario["overlay"]["scale"] == {"assets": {"thermal": {"vom": 3}}}  # base scale is not stacked silently
    for body in ({"target": {"type": "profile", "id": "nope"}, "factors": [1.1]},
                 {"target": {"type": "asset", "id": "thermal", "field": "name"}, "factors": [1.1]},
                 {"target": {"type": "profile", "id": "demand"}, "factors": []},
                 {"target": {"type": "profile", "id": "demand"}, "factors": [11]}):
        assert client.post("/projects/application-compact-demo/sensitivity", json=body).status_code == 422
