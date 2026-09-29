"""Construction-level tests for the shared stochastic extensive-form seam."""
from __future__ import annotations
import importlib.util
from pathlib import Path
import pytest
from powersim.contracts import SolverDiagnostics

ROOT=Path(__file__).parents[1]
SPEC=importlib.util.spec_from_file_location("ef",ROOT/"solver"/"powersim_stochastic_extensive_shared.py")
ef=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(ef)


def _input():
    return {"assets":[{"id":"g","type":"thermal","committable":True,"pmin":0,"pmax":20,"mc":10,"vom":0,"ramp_up":20,"ramp_down":20}],
     "profiles":{"demand":[5,10]},"study_horizon":{"horizon_hours":2},"reserve_products":[],
     "solver_settings":{"solver":"highs","component_engine":"shared"},
     "stochastic_scenarios":[{"id":"a","probability":.5},{"id":"b","probability":.5}]}


def test_extensive_form_builds_one_model_with_physical_scenario_blocks_and_nonanticipativity():
    model=ef.build_extensive_form(_input())
    assert len(model.scenario)==2
    assert len(model.non_anticipativity)==6  # u/y/z × 2 periods for Scenario B
    assert not model.scenario[0].OBJ.active and model.OBJ.active


def test_extensive_form_solves_once_and_exposes_shared_first_stage():
    out=ef.solve_extensive_form(_input())
    assert out["has_incumbent"] is True
    assert out["workflow"]=="stochastic_uc"
    assert out["solver_diagnostics"]["normalized_status"] in {"optimal", "feasible"}
    assert out["publication"]["publishable"] is True
    assert len(out["scenarios"]) == 2
    assert out["qa"]["status"] == "pass"


def test_stochastic_rolling_is_fail_closed():
    inp=_input(); inp["solver_settings"].update({"rolling_window_h":1,"rolling_step_h":1})
    with pytest.raises(ValueError,match="rolling horizon is unsupported"): ef.build_extensive_form(inp)


def test_corrupted_extracted_first_stage_fails_aggregate_qa_and_publication():
    out=ef.solve_extensive_form(_input())
    corrupted=out["scenarios"]
    corrupted[1]["first_stage"][0]["commitment"] = 1 - corrupted[1]["first_stage"][0]["commitment"]
    diagnostics=SolverDiagnostics.model_validate(out["solver_diagnostics"])
    report=ef._aggregate_qa(corrupted, diagnostics, out["aggregate"]["expected_objective_usd"])
    decision=ef.evaluate_publication(diagnostics, report, extraction_completed=True, required_values_finite=True,
        required_check_ids=("stochastic.non_anticipativity",))
    assert next(c for c in report.checks if c.check_id=="stochastic.non_anticipativity").status.value=="fail"
    assert report.status.value=="fail" and decision.publishable is False


def test_corrupted_scenario_cost_stream_fails_expected_objective_publication_gate():
    out=ef.solve_extensive_form(_input())
    corrupted=out["scenarios"]
    corrupted[1]["objective_usd"] += 1.0
    diagnostics=SolverDiagnostics.model_validate(out["solver_diagnostics"])
    report=ef._aggregate_qa(corrupted, diagnostics, out["aggregate"]["expected_objective_usd"])
    decision=ef.evaluate_publication(diagnostics, report, extraction_completed=True, required_values_finite=True,
        required_check_ids=("stochastic.expected_objective",))
    assert next(c for c in report.checks if c.check_id=="stochastic.expected_objective").status.value=="fail"
    assert decision.publishable is False
