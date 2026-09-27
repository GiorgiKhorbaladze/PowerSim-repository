"""Stage 4B DC-network units, extraction and publication-gate tests."""
from __future__ import annotations
from copy import deepcopy
import importlib.util
from pathlib import Path
import pytest

SPEC=importlib.util.spec_from_file_location("solver",Path(__file__).parents[2]/"solver"/"powersim_solver.py")
solver=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(solver)

def _input(*, resolution_min=60, x_pu=None, capacity=20):
    periods=60//resolution_min
    line={"id":"ab","from_bus":"a","to_bus":"b","capacity_mw":capacity}
    if x_pu is None: line["susceptance_mw_per_rad"]=1000
    else: line.update({"x_pu":x_pu,"base_mva":100})
    return {"resolution_min":resolution_min,"assets":[{"id":"g","type":"thermal","committable":False,"pmin":0,"pmax":100,"mc":1,"vom":0,"bus":"a"}],"profiles":{"demand":[10.0]*periods},"study_horizon":{"horizon_hours":1},"reserve_products":[],"buses":[{"id":"a","is_slack":True},{"id":"b"}],"lines":[line],"load_share_by_bus":{"a":0,"b":1},"solver_settings":{"solver":"highs"}}

def _solve(inp):
    assets=solver.build_asset_map(inp); rows,elapsed,obj=solver.solve_all(inp,assets,inp["profiles"],{})
    return rows,assets,solver.build_result_store(rows,assets,inp,elapsed,obj)

@pytest.mark.parametrize("resolution_min",[60,15])
def test_dc_network_mw_per_rad_and_publication(resolution_min):
    rows,_,result=_solve(_input(resolution_min=resolution_min,x_pu=.1))
    assert result["qa"]["status"]=="pass" and result["publication"]["publishable"] is True
    assert rows[0]["line_flow"]["ab"] == pytest.approx(10)
    assert rows[0]["bus_angle_rad"]["a"] == pytest.approx(0)

def test_dc_network_rejects_missing_bus_and_base_mva():
    bad=_input(x_pu=.1); del bad["lines"][0]["base_mva"]
    with pytest.raises(ValueError,match="base_mva"): _solve(bad)
    bad=_input(); del bad["assets"][0]["bus"]
    with pytest.raises(ValueError,match="every asset"): _solve(bad)

def test_dc_network_corruption_blocks_publication():
    inp=_input(); rows,assets,valid=_solve(inp)
    assert valid["publication"]["publishable"] is True
    corrupt=deepcopy(list(rows)); corrupt[0]["line_flow"]["ab"]=999
    badrows=solver.SolvedRows(corrupt,solver_diagnostics=rows.solver_diagnostics,window_diagnostics=rows.window_diagnostics,component_results=rows.component_results,reserve_results=rows.reserve_results,extraction_completed=True)
    invalid=solver.build_result_store(badrows,assets,inp,0,0)
    assert invalid["qa"]["status"]=="fail" and invalid["publication"]["publishable"] is False
