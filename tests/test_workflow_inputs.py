"""Workflow-specific inputs: pre-run validation and runs built from form-shaped inputs."""
from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from powersim.contracts import ProjectContract, resolve_scenario
from powersim.execution import resolved_to_workflow_input, run_registered_workflow
from powersim.validation.readiness import diagnose_project

ROOT = Path(__file__).resolve().parents[1]
BASE = json.loads((ROOT / "samples/application_compact_demo.json").read_text(encoding="utf-8"))


def _project(workflow, **legacy):
    project = copy.deepcopy(BASE)
    project["workflow"] = workflow
    project["legacy_payload"].update(legacy)
    return project


def _errors(project):
    return [(i["path"], i["message"]) for i in diagnose_project(project)["issues"] if i["severity"] == "error"]


def _run(project):
    envelope = run_registered_workflow(resolved_to_workflow_input(resolve_scenario(ProjectContract.model_validate(project))),
                                       "run", "sha256:x")
    return envelope.qa.status.value, envelope.validity.value


@pytest.mark.parametrize("workflow,path", [
    ("stochastic_uc", "legacy_payload.stochastic_scenarios"),
    ("security_scuc", "legacy_payload.contingencies"),
    ("chronological_adequacy", "legacy_payload.chronological_adequacy"),
    ("scoped_expansion", "legacy_payload.expansion"),
])
def test_missing_workflow_inputs_block_runs_before_solving(workflow, path):
    errors = _errors(_project(workflow))
    assert errors and errors[0][0] == path
    assert diagnose_project(_project(workflow))["contract_valid"]  # still saveable as a draft


def test_stochastic_inputs_from_the_form():
    project = _project("stochastic_uc", stochastic_scenarios=[
        {"id": "low", "probability": 0.5, "profile_overrides": {"demand": [18] * 4}, "demand_profile_source": "x"},
        {"id": "high", "probability": 0.6, "profile_overrides": {}}])
    assert any("sum to 1.0" in m for _, m in _errors(project))
    project["legacy_payload"]["stochastic_scenarios"][1]["probability"] = 0.5
    assert ("solver_settings.component_engine", "validated stochastic UC requires solver_settings.component_engine='shared'") \
        in _errors({**project, "solver_settings": {"component_engine": "legacy"}})
    project["solver_settings"] = {"component_engine": "shared"}
    assert _errors(project) == []
    assert _run(project) == ("pass", "valid")


def test_security_inputs_from_the_form():
    project = _project("security_scuc", contingencies=[{"id": "c1", "kind": "unit_outage", "elements": ["thermal"]}])
    assert _errors(project) == [("reserve_products", "validated corrective SCUC requires explicit reserve products")]
    project["reserve_products"] = [{"id": "up", "direction": "up", "requirement": 5, "eligible_units": ["import"]}]
    project["solver_settings"] = {"component_engine": "shared"}
    assert _errors(project) == []
    assert _run(project) == ("pass", "valid")
    project["legacy_payload"]["contingencies"][0]["elements"] = ["ghost"]
    assert any("unknown asset ghost" in m for _, m in _errors(project))


def test_adequacy_and_expansion_inputs_from_the_form():
    adequacy = _project("chronological_adequacy", chronological_adequacy={"samples": 0, "seed": 1})
    assert ("legacy_payload.chronological_adequacy.samples", "must be a positive integer") in _errors(adequacy)
    adequacy["legacy_payload"]["chronological_adequacy"]["samples"] = 50
    for asset in adequacy["assets"]:
        asset["legacy_extensions"]["for_rate"] = 0.05
    assert _errors(adequacy) == [] and _run(adequacy) == ("pass", "valid")
    candidate = {"id": "cand-1", "capex_per_mw": 1e6, "opex_per_mw_yr": 2e4, "max_build_mw": 100, "life_yrs": 25,
                 "capacity_credit": 0.15, "capacity_factor": 0.4}
    expansion = _project("scoped_expansion", expansion={"mode": "scoped_screening", "years": 1, "discount_rate": 0.08,
                                                        "reserve_margin": 0.15, "candidates": [dict(candidate, capacity_factor=2)]})
    assert any(p.endswith("capacity_factor") for p, _ in _errors(expansion))
    expansion["legacy_payload"]["expansion"]["candidates"] = [candidate]
    assert _errors(expansion) == [] and _run(expansion) == ("pass", "valid")
