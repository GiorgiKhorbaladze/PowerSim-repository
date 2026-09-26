import importlib.util
from dataclasses import replace
from pathlib import Path

import pytest

from powersim.qa import check_component_results


SPEC = importlib.util.spec_from_file_location(
    "legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py"
)
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


def _input(shared: bool):
    return {
        "assets": [{
            "id": "t", "type": "thermal", "committable": True,
            "pmin": 0, "pmax": 100, "mc": 20, "vom": 1,
            "ramp_up": 40, "ramp_down": 40, "min_up": 2, "min_down": 2,
            "startup_cost": 50, "no_load_cost": 5,
        }],
        "profiles": {"demand": [0, 40, 80, 20]},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4},
        "reserve_products": [],
        "solver_settings": {"solver": "highs", "time_limit_s": 30,
                            "component_engine": "shared" if shared else "legacy"},
    }


def _solve(shared: bool):
    inp = _input(shared)
    assets = legacy.build_asset_map(inp)
    return legacy.solve_all(inp, assets, inp["profiles"], {})


def test_shared_thermal_uc_matches_legacy_objective_and_dispatch():
    old_rows, _, old_objective = _solve(False)
    new_rows, _, new_objective = _solve(True)
    assert new_objective == pytest.approx(old_objective, abs=0.01)
    assert [row["dispatch"] for row in new_rows] == [row["dispatch"] for row in old_rows]
    assert [row["commitment"] for row in new_rows] == [row["commitment"] for row in old_rows]


def test_shared_thermal_results_are_canonical_and_independently_checked():
    rows, _, _ = _solve(True)
    thermal = [item for item in rows.component_results if item.component_kind == "thermal"]
    assert len(thermal) == 4
    checks = check_component_results(thermal, 1.0, resolved_input=_input(True))
    assert checks
    assert all(check.status.value == "pass" for check in checks)
    corrupt = list(thermal)
    corrupt[2] = replace(corrupt[2], injection_mw=500)
    checks = check_component_results(corrupt, 1.0, resolved_input=_input(True))
    assert next(check for check in checks if check.check_id == "component.thermal_bounds").status.value == "fail"


def test_thermal_ramp_is_scaled_by_period_duration():
    rows, _, _ = _solve(True)
    thermal = [item for item in rows.component_results if item.component_kind == "thermal"]
    corrupt = list(thermal)
    corrupt[1] = replace(corrupt[1], injection_mw=float(corrupt[0].injection_mw) + 21)
    checks = check_component_results(corrupt, 0.25, resolved_input=_input(True))
    assert next(check for check in checks if check.check_id == "component.thermal_ramp_up").status.value == "fail"


def test_thermal_canonical_results_reconstruct_start_no_load_and_co2_costs():
    inp = _input(True)
    asset = inp["assets"][0]
    asset.update({
        "startup_cost_hot": 10,
        "startup_cost_cold": 20,
        "hot_start_threshold_h": 3,
        "co2_factor_t_per_mwh": 0.5,
    })
    inp["co2_price_usd_per_t"] = 30
    assets = legacy.build_asset_map(inp)
    rows, _, _ = legacy.solve_all(inp, assets, inp["profiles"], {})
    thermal = [item for item in rows.component_results if item.component_kind == "thermal"]
    assert all(
        item.cost_usd == pytest.approx(
            (item.variable_cost_usd or 0) + (item.startup_cost_usd or 0)
            + (item.no_load_cost_usd or 0) + (item.co2_cost_usd or 0)
        )
        for item in thermal
    )
    assert sum(item.co2_t or 0 for item in thermal) == pytest.approx(60.0)
    assert sum(item.co2_cost_usd or 0 for item in thermal) == pytest.approx(1800.0)
    assert any((item.startup_cost_usd or 0) > 0 for item in thermal)
    assert all(item.no_load_cost_usd is not None for item in thermal)
