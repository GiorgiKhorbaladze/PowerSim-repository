"""Canonical gas accounting and publication-gate tests."""
from __future__ import annotations
from dataclasses import replace
import importlib.util
from pathlib import Path
import pytest

SPEC=importlib.util.spec_from_file_location("solver",Path(__file__).parents[2]/"solver"/"powersim_solver.py")
solver=importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(solver)

def _input(*, resolution_min=60, horizon_hours=4, window_hours=None):
    periods=horizon_hours*60//resolution_min
    return {"resolution_min":resolution_min,"assets":[
        {"id":"gas","type":"thermal","committable":False,"pmin":0,"pmax":100,"heat_rate":7,"fuel_type":"gas","fuel_price":1,"vom":0,"ramp_up":9999,"ramp_down":9999},
        {"id":"other","type":"thermal","committable":False,"pmin":0,"pmax":100,"heat_rate":7,"fuel_type":"coal","fuel_price":100,"vom":0,"ramp_up":9999,"ramp_down":9999}],
        "profiles":{"demand":[100.0]*periods},"study_horizon":{"horizon_hours":horizon_hours},"reserve_products":[],
        "gas_constraints":{"mode":"annual+monthly","applies_to":["gas"],"annual":{"cap":100},"monthly":{"Jan":100}},
        "solver_settings":{"solver":"highs","component_engine":"shared","rolling_window_h":window_hours or horizon_hours,"rolling_step_h":window_hours or horizon_hours}}

@pytest.mark.parametrize("resolution_min",[60,15])
def test_gas_canonical_conversion_and_publication(resolution_min):
    inp=_input(resolution_min=resolution_min); assets=solver.build_asset_map(inp)
    rows,elapsed,objective=solver.solve_all(inp,assets,inp["profiles"],solver.build_gas_limits(inp,0,inp["study_horizon"]["horizon_hours"]))
    result=solver.build_result_store(rows,assets,inp,elapsed,objective)
    records=[item for item in rows.component_results if item.asset_id=="gas"]
    assert sum(item.gas_consumption_mm3 or 0 for item in records)==pytest.approx(100*4*7/35000)
    assert result["qa"]["status"]=="pass" and result["publication"]["publishable"] is True
    corrupt=list(rows.component_results); idx=next(i for i,x in enumerate(corrupt) if x.asset_id=="gas")
    corrupt[idx]=replace(corrupt[idx],gas_consumption_mm3=999)
    invalid=solver.build_result_store(solver.SolvedRows(rows,solver_diagnostics=rows.solver_diagnostics,window_diagnostics=rows.window_diagnostics,component_results=corrupt,extraction_completed=True),assets,inp,elapsed,objective)
    assert invalid["qa"]["status"]=="fail" and invalid["publication"]["publishable"] is False

@pytest.mark.parametrize("resolution_min",[60,15])
def test_gas_rolling_budget_is_independently_checked(resolution_min):
    inp=_input(resolution_min=resolution_min,horizon_hours=48,window_hours=24)
    inp["profiles"]["demand"]=[100.0]*(48*60//resolution_min)
    inp["gas_constraints"]["annual"]["cap"]=0.48
    assets=solver.build_asset_map(inp)
    rows,elapsed,objective=solver.solve_all(inp,assets,inp["profiles"],solver.build_gas_limits(inp,0,48))
    result=solver.build_result_store(rows,assets,inp,elapsed,objective)
    used=sum(item.gas_consumption_mm3 or 0 for item in rows.component_results if item.asset_id=="gas")
    assert used <= .48+1e-6 and result["qa"]["status"]=="pass"


@pytest.mark.parametrize("resolution_min", [60, 15])
def test_gas_monthly_cap_uses_study_start_offset(resolution_min):
    inp = _input(resolution_min=resolution_min)
    inp["study_horizon"]["start_hour"] = 744  # First hour of February.
    inp["gas_constraints"]["annual"]["cap"] = 100
    inp["gas_constraints"]["monthly"] = {"Feb": 0.08}
    assets = solver.build_asset_map(inp)
    rows, elapsed, objective = solver.solve_all(
        inp,
        assets,
        inp["profiles"],
        solver.build_gas_limits(inp, 744, 4),
    )
    result = solver.build_result_store(rows, assets, inp, elapsed, objective)
    used = sum(
        item.gas_consumption_mm3 or 0
        for item in rows.component_results
        if item.asset_id == "gas"
    )
    assert used == pytest.approx(0.08 * 4 / (28 * 24))
    assert result["qa"]["status"] == "pass"
    assert result["publication"]["publishable"] is True


@pytest.mark.parametrize("resolution_min", [60, 15])
def test_gas_rolling_monthly_budget_carries_full_month_cap(resolution_min):
    inp = _input(resolution_min=resolution_min, horizon_hours=48, window_hours=24)
    inp["study_horizon"]["start_hour"] = 744
    inp["gas_constraints"]["annual"]["cap"] = 100
    inp["gas_constraints"]["monthly"] = {"Feb": 0.08}
    assets = solver.build_asset_map(inp)
    rows, elapsed, objective = solver.solve_all(
        inp,
        assets,
        inp["profiles"],
        solver.build_gas_limits(inp, 744, 48),
    )
    result = solver.build_result_store(rows, assets, inp, elapsed, objective)
    used = sum(
        item.gas_consumption_mm3 or 0
        for item in rows.component_results
        if item.asset_id == "gas"
    )
    assert used == pytest.approx(0.08)
    assert result["qa"]["status"] == "pass"
    assert result["publication"]["publishable"] is True
