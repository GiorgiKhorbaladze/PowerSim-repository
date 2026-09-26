import importlib.util
from pathlib import Path
import pytest
from dataclasses import replace
from powersim.qa import check_component_results, run_qa

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
    report=run_qa(i,result,persisted=True,component_results=component,component_duration_hours=1)
    assert report.status.value=="fail"
