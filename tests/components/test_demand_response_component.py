import importlib.util
from pathlib import Path
import pytest

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
