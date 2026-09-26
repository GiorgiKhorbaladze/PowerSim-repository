import importlib.util
from pathlib import Path

import pytest
from dataclasses import replace

from powersim.qa import check_component_results


SPEC = importlib.util.spec_from_file_location("legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py")
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


def _input(shared: bool):
    return {
        "assets": [
            {"id": "g", "type": "thermal", "committable": False, "pmin": 0, "pmax": 100, "mc": 100, "vom": 0},
            {"id": "b", "type": "bess", "committable": False, "power_mw": 30, "energy_mwh": 60,
             "soc_init": 0.5, "soc_min": 0.1, "soc_max": 0.9, "eta_charge": 0.9, "eta_discharge": 0.9,
             "vom_discharge": 2, "cycle_cost_per_mwh": 1, "ramp_up_mw_min": 1, "ramp_down_mw_min": 1},
        ],
        "profiles": {"demand": [0, 0, 30, 60]},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4},
        "reserve_products": [],
        "solver_settings": {"solver": "highs", "time_limit_s": 30, "component_engine": "shared" if shared else "legacy"},
    }


def _solve(shared: bool):
    inp = _input(shared)
    return legacy.solve_all(inp, legacy.build_asset_map(inp), inp["profiles"], {})


def test_shared_bess_matches_legacy_dispatch_and_objective():
    old_rows, _, old_objective = _solve(False)
    new_rows, _, new_objective = _solve(True)
    assert new_objective == pytest.approx(old_objective, abs=0.01)
    assert [row["bess"] for row in new_rows] == [row["bess"] for row in old_rows]


def test_shared_bess_extracts_canonical_state_and_flows():
    rows, _, _ = _solve(True)
    results = [item for item in rows.component_results if item.component_kind == "bess"]
    assert len(results) == 4
    assert all(item.state_of_charge_mwh is not None for item in results)
    assert all(item.injection_mw >= 0 and item.withdrawal_mw >= 0 for item in results)
    assert all(not (item.injection_mw > 1e-7 and item.withdrawal_mw > 1e-7) for item in results)
    checks = check_component_results(results, 1.0, resolved_input=_input(True))
    assert all(check.status.value == "pass" for check in checks)
    corrupt = list(results)
    corrupt[0] = replace(corrupt[0], injection_mw=99)
    checks = check_component_results(corrupt, 1.0, resolved_input=_input(True))
    assert next(check for check in checks if check.check_id == "component.bess_bounds").status.value == "fail"
