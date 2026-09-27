import importlib.util
from pathlib import Path
import pytest
from dataclasses import replace
from powersim.components import BuildContext, PumpedHydroComponent
from powersim.qa import check_component_results

SPEC = importlib.util.spec_from_file_location("legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py")
legacy = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(legacy)

def _input(shared):
    return {"assets":[
        {"id":"g","type":"thermal","committable":False,"pmin":0,"pmax":200,"mc":100,"vom":0},
        {"id":"ph","type":"pumped_hydro","committable":False,"pmax":30,"pump_mw":30,"energy_mwh":100,"soc_init":.5,"soc_min":.1,"soc_max":.9,"efficiency_pump":.9,"efficiency_gen":.9,"vom":0},
    ],"profiles":{"demand":[0,30,60]},"study_horizon":{"horizon_hours":3},"reserve_products":[],"solver_settings":{"solver":"highs","component_engine":"shared" if shared else "legacy"}}

def _solve(shared):
    inp=_input(shared); return legacy.solve_all(inp, legacy.build_asset_map(inp), inp["profiles"], {})

def test_shared_pumped_hydro_matches_legacy():
    old,_,old_obj=_solve(False); new,_,new_obj=_solve(True)
    assert new_obj == pytest.approx(old_obj, abs=.01)
    assert [r["pumped_hydro"] for r in new] == [r["pumped_hydro"] for r in old]
    items=[x for x in new.component_results if x.component_kind=="pumped_hydro"]
    assert len(items)==3 and all(x.state_of_charge_mwh is not None for x in items)
    assert all(x.status.value == "pass" for x in check_component_results(items, 1, resolved_input=_input(True)))

def test_pumped_hydro_validation_and_independent_soc_reconstruction():
    context=BuildContext(builder=object(),periods=(1,),duration_hours=1,capabilities=frozenset({"deterministic"}))
    issues=PumpedHydroComponent({"id":"ph","pmax":-1,"pump_mw":"bad","energy_mwh":1,"soc_init":2,"soc_min":.8,"soc_max":.2,"efficiency_pump":1.1,"efficiency_gen":0,"vom":-1}).validate(context)
    assert any(issue.code=="invalid_pumped_hydro_parameter" for issue in issues)
    rows,_,_=_solve(True); items=[x for x in rows.component_results if x.component_kind=="pumped_hydro"]
    corrupt=list(items); corrupt[0]=replace(corrupt[0],generation_high_mw=(corrupt[0].generation_high_mw or 0)+1)
    checks=check_component_results(corrupt,1,resolved_input=_input(True))
    assert next(x for x in checks if x.check_id=="component.pumped_hydro_segments").status.value=="fail"

@pytest.mark.parametrize("resolution_min",[60,15])
def test_pumped_hydro_vom_canonical_term_matches_legacy_objective_units(resolution_min):
    i=_input(True); i["resolution_min"]=resolution_min
    factor=60//resolution_min; i["profiles"]["demand"]=[value for value in i["profiles"]["demand"] for _ in range(factor)]
    i["assets"][1]["vom"]=7
    rows,_,obj=legacy.solve_all(i,legacy.build_asset_map(i),i["profiles"],{})
    items=[x for x in rows.component_results if x.component_kind=="pumped_hydro"]
    dt=resolution_min/60
    assert sum(x.cost_usd or 0 for x in items) == pytest.approx(7*sum(x.injection_mw for x in items)*dt)
    old={**i,"solver_settings":{**i["solver_settings"],"component_engine":"legacy"}}
    _,_,old_obj=legacy.solve_all(old,legacy.build_asset_map(old),old["profiles"],{})
    assert obj == pytest.approx(old_obj,abs=.01)
