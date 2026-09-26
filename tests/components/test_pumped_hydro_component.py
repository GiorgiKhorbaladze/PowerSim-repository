import importlib.util
from pathlib import Path
import pytest
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
