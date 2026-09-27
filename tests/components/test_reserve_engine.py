"""Stage 4A canonical reserve extraction, stacking and publication tests."""
from __future__ import annotations

from dataclasses import replace
import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("solver", Path(__file__).parents[2] / "solver" / "powersim_solver.py")
solver = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(solver)


def _input(*, resolution_min=60, products=None, demand=5.0, horizon_hours=2, window_hours=None):
    periods = horizon_hours * 60 // resolution_min
    return {
        "resolution_min": resolution_min,
        "assets": [{"id": "g", "type": "thermal", "committable": False, "pmin": 0, "pmax": 10,
                    "mc": 1, "vom": 0, "ramp_up": 999, "ramp_down": 999}],
        "profiles": {"demand": [demand] * periods, "req": [2.0, 4.0] * (periods // 2)},
        "study_horizon": {"horizon_hours": horizon_hours},
        "reserve_products": products or [],
        "solver_settings": {"solver": "highs", "rolling_window_h": window_hours or horizon_hours,
                            "rolling_step_h": window_hours or horizon_hours},
    }


def _solve(inp):
    assets = solver.build_asset_map(inp)
    rows, elapsed, objective = solver.solve_all(inp, assets, inp["profiles"], {})
    return rows, solver.build_result_store(rows, assets, inp, elapsed, objective)


def test_requirement_profile_and_symmetric_canonical_records_publish():
    inp = _input(products=[{"id": "F", "direction": "symmetric", "requirement_profile": "req",
                            "shortfall_penalty": 500, "eligible_units": ["g"]}])
    rows, result = _solve(inp)
    systems = [r for r in rows.reserve_results if r.provider_id is None]
    assert [r.requirement_mw for r in systems] == [2.0, 4.0]
    assert result["qa"]["status"] == "pass"
    assert result["publication"]["publishable"] is True


def test_multiple_products_cannot_double_count_thermal_headroom():
    inp = _input(products=[
        {"id": "R1", "direction": "up", "requirement": 5, "eligible_units": ["g"]},
        {"id": "R2", "direction": "up", "requirement": 5, "eligible_units": ["g"]},
    ])
    rows, result = _solve(inp)
    first = [r for r in rows.reserve_results if r.provider_id == "g" and r.period == 0]
    assert sum(r.provided_mw for r in first) <= 5.000001
    assert result["qa"]["status"] == "pass"


@pytest.mark.parametrize("resolution_min", [60, 15])
def test_reserve_rolling_and_corruption_fail_publication(resolution_min):
    inp = _input(resolution_min=resolution_min, horizon_hours=4, window_hours=2,
                 products=[{"id": "R", "direction": "up", "requirement": 2,
                            "shortfall_penalty": 321, "eligible_units": ["g"]}])
    rows, valid = _solve(inp)
    assert valid["qa"]["status"] == "pass" and valid["publication"]["publishable"] is True
    records = list(rows.reserve_results)
    index = next(i for i, r in enumerate(records) if r.provider_id == "g")
    records[index] = replace(records[index], provided_mw=999, effective_provided_mw=999)
    invalid_rows = solver.SolvedRows(rows, solver_diagnostics=rows.solver_diagnostics,
        window_diagnostics=rows.window_diagnostics, component_results=rows.component_results,
        reserve_results=records, extraction_completed=True)
    assets = solver.build_asset_map(inp)
    invalid = solver.build_result_store(invalid_rows, assets, inp, 0.0, 0.0)
    assert invalid["qa"]["status"] == "fail"
    assert invalid["diagnostics"]["result_validity"] == "invalid"
    assert invalid["publication"]["publishable"] is False
