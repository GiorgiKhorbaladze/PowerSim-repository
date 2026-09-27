"""Parity and independent-QA coverage for shared reservoir hydro core."""
from __future__ import annotations

from dataclasses import replace
import importlib.util
from pathlib import Path

import pytest

from powersim.components import BuildContext, ReservoirHydroComponent
from powersim.qa import check_component_results

SPEC = importlib.util.spec_from_file_location("legacy_solver", Path(__file__).parents[2] / "solver" / "powersim_solver.py")
solver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(solver)


def _input(shared: bool, resolution_min: int = 60, horizon_hours: int = 4):
    periods = horizon_hours * 60 // resolution_min
    inflow = [0.25] * periods
    demand = [100.0] * periods
    return {
        "resolution_min": resolution_min,
        "assets": [
            {"id": "h", "type": "hydro_reg", "committable": False, "pmin": 0, "pmax": 100,
             "ramp_up": 100, "ramp_down": 100, "vom": 0, "inflow_profile": "inflow",
             "hydro": {"efficiency": 100, "reservoir_min": 0, "reservoir_max": 20,
                       "reservoir_init": 10, "reservoir_end_min": 4,
                       "min_release_mm3h": 0.1, "spill_cost_usd_per_mm3": 3}},
            {"id": "backup", "type": "thermal", "committable": False, "pmin": 0, "pmax": 200,
             "mc": 100, "vom": 0, "ramp_up": 200, "ramp_down": 200},
        ],
        "profiles": {"demand": demand, "inflow": inflow},
        "study_horizon": {"horizon_hours": horizon_hours},
        "reserve_products": [],
        "solver_settings": {"solver": "highs", "component_engine": "shared" if shared else "legacy",
                            "rolling_window_h": horizon_hours, "rolling_step_h": horizon_hours},
    }


def _solve(shared: bool, resolution_min: int = 60):
    inp = _input(shared, resolution_min)
    assets = solver.build_asset_map(inp)
    return inp, assets, solver.solve_all(inp, assets, inp["profiles"], {})


@pytest.mark.parametrize("resolution_min", [60, 15])
def test_shared_reservoir_matches_legacy_and_reconstructs_water_balance(resolution_min):
    old_input, old_assets, (old_rows, _, old_objective) = _solve(False, resolution_min)
    new_input, new_assets, (new_rows, _, new_objective) = _solve(True, resolution_min)
    assert new_objective == pytest.approx(old_objective, abs=.01)
    assert [row["hydro"] for row in new_rows] == [row["hydro"] for row in old_rows]
    records = [item for item in new_rows.component_results if item.component_kind == "hydro_reg"]
    assert len(records) == len(new_rows)
    checks = check_component_results(records, resolution_min / 60, resolved_input=new_input)
    assert all(item.status.value == "pass" for item in checks)


def test_reservoir_validation_is_structured():
    context = BuildContext(builder=object(), periods=(1,), duration_hours=1, capabilities=frozenset({"deterministic"}))
    issues = ReservoirHydroComponent({"id": "h", "hydro": {"efficiency": 0, "reservoir_min": 9, "reservoir_init": 3, "reservoir_max": 2}}).validate(context)
    assert {item.code for item in issues} >= {"invalid_reservoir_parameter", "invalid_reservoir_range"}


def test_corrupt_reservoir_canonical_result_blocks_publication():
    inp, assets, (rows, elapsed, objective) = _solve(True)
    valid = solver.build_result_store(rows, assets, inp, elapsed, objective)
    assert valid["qa"]["status"] == "pass"
    assert valid["publication"]["publishable"] is True
    component = list(rows.component_results)
    index = next(i for i, item in enumerate(component) if item.component_kind == "hydro_reg")
    component[index] = replace(component[index], state_of_water_mm3=(component[index].state_of_water_mm3 or 0) + 2)
    corrupted = solver.SolvedRows(rows, solver_diagnostics=rows.solver_diagnostics,
        window_diagnostics=rows.window_diagnostics, component_results=component,
        extraction_completed=True)
    invalid = solver.build_result_store(corrupted, assets, inp, elapsed, objective)
    assert invalid["qa"]["status"] == "fail"
    assert invalid["diagnostics"]["result_validity"] == "invalid"
    assert invalid["publication"]["publishable"] is False


def test_reservoir_rolling_boundary_is_extracted_and_independently_checked():
    inp = _input(True, 60, horizon_hours=48)
    inp["solver_settings"].update({"rolling_window_h": 24, "rolling_step_h": 24})
    inp["assets"][0]["hydro"]["reservoir_end_min"] = 0
    inp["profiles"] = {name: values * 12 for name, values in inp["profiles"].items()}
    assets = solver.build_asset_map(inp)
    rows, _, _ = solver.solve_all(inp, assets, inp["profiles"], {})
    records = [item for item in rows.component_results if item.component_kind == "hydro_reg"]
    assert len(records) == 48
    assert records[24].previous_state_of_water_mm3 == pytest.approx(records[23].state_of_water_mm3)
    checks = check_component_results(records, 1, resolved_input=inp)
    assert next(item for item in checks if item.check_id == "component.reservoir_water_balance").status.value == "pass"


def test_reservoir_month_target_head_curve_and_spill_remain_in_parity():
    inp = _input(True, 60, horizon_hours=800)
    hydro = inp["assets"][0]["hydro"]
    hydro.update({"reservoir_init": 18, "reservoir_max": 20, "reservoir_end_min": 0,
                  "head_efficiency_curve": [[0, 50], [20, 100]],
                  "storage_targets": {"month_end": [12] + [None] * 11,
                                      "penalty_usd_per_mm3": 10000},
                  "spill_cost_usd_per_mm3": 100})
    inp["profiles"] = {"demand": [100.0] * 800, "inflow": [0.25] * 800}
    old = {**inp, "solver_settings": {**inp["solver_settings"], "component_engine": "legacy"}}
    old_assets, new_assets = solver.build_asset_map(old), solver.build_asset_map(inp)
    old_rows, _, old_objective = solver.solve_all(old, old_assets, old["profiles"], {})
    new_rows, _, new_objective = solver.solve_all(inp, new_assets, inp["profiles"], {})
    assert new_objective == pytest.approx(old_objective, abs=.01)
    assert [row["hydro"] for row in new_rows] == [row["hydro"] for row in old_rows]
    records = [item for item in new_rows.component_results if item.component_kind == "hydro_reg"]
    assert all(item.water_efficiency_mwh_per_mm3 == pytest.approx(95) for item in records)


def test_cascade_asset_remains_legacy_until_cascade_coupler_migration():
    asset = _input(True)["assets"][0]
    asset["hydro"]["cascade_upstream"] = "upstream"
    assert ReservoirHydroComponent.supports_shared(asset) is False


def test_legacy_soft_end_penalty_is_excluded_from_validated_shared_core():
    asset = _input(True)["assets"][0]
    asset["hydro"].update({"target_end_level_frac": .8, "end_level_penalty": 20})
    assert ReservoirHydroComponent.supports_shared(asset) is False
