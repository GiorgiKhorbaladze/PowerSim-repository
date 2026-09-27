import importlib.util
from pathlib import Path
import pytest
from dataclasses import replace
from powersim.components import BuildContext, DemandResponseComponent
from powersim.qa import check_component_results

spec=importlib.util.spec_from_file_location("solver",Path(__file__).parents[2]/"solver/powersim_solver.py")
solver=importlib.util.module_from_spec(spec); spec.loader.exec_module(solver)
def inp(shared): return {"assets":[{"id":"g","type":"thermal","pmax":10,"mc":100},{"id":"d","type":"dr","pmax_curtail":5,"price_per_mwh":1,"hours_per_year_max":8760}],"profiles":{"demand":[10,10]},"study_horizon":{"horizon_hours":2},"reserve_products":[],"solver_settings":{"solver":"highs","component_engine":"shared" if shared else "legacy"}}
def run(shared):
    i=inp(shared); return solver.solve_all(i,solver.build_asset_map(i),i["profiles"],{})
def test_shared_dr_matches_legacy_and_extracts_results():
    old,_,oo=run(False); new,_,no=run(True)
    assert no==pytest.approx(oo,abs=.01)
    assert [x["dr"] for x in new]==[x["dr"] for x in old]
    rows=[x for x in new.component_results if x.component_kind=="dr"]
    assert len(rows)==2 and all(x.curtailed_mwh is not None for x in rows)
    checks=check_component_results(rows,1,resolved_input=inp(True))
    assert all(check.status.value=="pass" for check in checks)
    bad=list(rows); bad[0]=replace(bad[0], injection_mw=bad[0].injection_mw+1)
    assert next(check for check in check_component_results(bad,1,resolved_input=inp(True)) if check.check_id=="component.demand_response_bounds").status.value=="fail"

def test_dr_end_to_end_qa_failure_blocks_publication():
    i=inp(True); assets=solver.build_asset_map(i); hourly,elapsed,obj=solver.solve_all(i,assets,i["profiles"],{})
    result=solver.build_result_store(hourly,assets,i,elapsed,obj)
    assert result["qa"]["status"]=="pass"
    assert result["publication"]["publishable"] is True
    component=list(hourly.component_results)
    dr_index=next(index for index,item in enumerate(component) if item.component_kind=="dr")
    component[dr_index]=replace(component[dr_index], injection_mw=99)
    corrupted=solver.SolvedRows(hourly,solver_diagnostics=hourly.solver_diagnostics,window_diagnostics=hourly.window_diagnostics,component_results=component,extraction_completed=True)
    invalid=solver.build_result_store(corrupted,assets,i,elapsed,obj)
    assert invalid["qa"]["status"]=="fail"
    assert invalid["diagnostics"]["result_validity"]=="invalid"
    assert invalid["publication"]["publishable"] is False

def test_dr_validation_is_structured_and_legacy_missing_profile_is_explicit_warning():
    context=BuildContext(builder=object(),periods=(1,2),duration_hours=1,profiles={},capabilities=frozenset({"deterministic"}))
    issues=DemandResponseComponent({"id":"d","pmax_curtail":"bad","price_per_mwh":float("nan"),"hours_per_year_max":-1,"availability_profile":"missing"}).validate(context)
    assert {issue.code for issue in issues} >= {"invalid_demand_response_parameter","demand_response_availability_profile_compatibility_fallback"}

def _budget_input(*, resolution_min=60, horizon_hours=48, window_hours=24):
    periods=int(horizon_hours*60/resolution_min)
    return {"resolution_min":resolution_min,"assets":[{"id":"g","type":"thermal","committable":False,"pmax":10,"mc":100},{"id":"d","type":"dr","pmax_curtail":10,"price_per_mwh":1,"hours_per_year_max":8}],"profiles":{"demand":[10.0]*periods},"study_horizon":{"horizon_hours":horizon_hours},"reserve_products":[],"solver_settings":{"solver":"highs","component_engine":"shared","rolling_window_h":window_hours,"rolling_step_h":window_hours}}

@pytest.mark.parametrize("resolution_min",[60,15])
def test_dr_single_window_budget_is_prorated_and_qa_matches_solver(resolution_min):
    i=_budget_input(resolution_min=resolution_min,horizon_hours=4,window_hours=24); assets=solver.build_asset_map(i)
    rows,elapsed,obj=solver.solve_all(i,assets,i["profiles"],{})
    result=solver.build_result_store(rows,assets,i,elapsed,obj)
    used=sum(x.curtailed_mwh for x in rows.component_results if x.component_kind=="dr")
    assert used == pytest.approx(10*8*(4/8760),abs=1e-5)
    assert result["qa"]["status"]=="pass"

@pytest.mark.parametrize("resolution_min",[60,15])
def test_dr_rolling_budget_uses_full_carried_callout_budget_and_qa_matches_solver(resolution_min):
    i=_budget_input(resolution_min=resolution_min,horizon_hours=48,window_hours=24); assets=solver.build_asset_map(i)
    rows,elapsed,obj=solver.solve_all(i,assets,i["profiles"],{})
    result=solver.build_result_store(rows,assets,i,elapsed,obj)
    used=sum(x.curtailed_mwh for x in rows.component_results if x.component_kind=="dr")
    assert used == pytest.approx(10*8,abs=1e-5)
    assert len([x for x in rows.component_results if x.component_kind=="dr"]) == int(48*60/resolution_min)
    assert result["qa"]["status"]=="pass"
