"""Stage 4C deterministic, shared mixed-fleet acceptance coverage."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("solver", Path(__file__).parents[1] / "solver" / "powersim_solver.py")
solver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(solver)


def _input(*, resolution_min: int, rolling: bool) -> dict:
    periods = 4 * 60 // resolution_min
    settings = {"solver": "highs", "component_engine": "shared", "time_limit_s": 30,
                "rolling_window_h": 2 if rolling else 4, "rolling_step_h": 2 if rolling else 4}
    return {
        "resolution_min": resolution_min,
        "assets": [
            {"id": "thermal", "type": "thermal", "committable": False, "pmin": 0, "pmax": 80,
             "heat_rate": 7, "fuel_type": "gas", "fuel_price": 5, "vom": 1,
             "ramp_up": 1000, "ramp_down": 1000, "bus": "a"},
            {"id": "reservoir", "type": "hydro_reg", "committable": False, "pmin": 0, "pmax": 8,
             "vom": 0, "inflow_profile": "inflow", "bus": "a",
             "hydro": {"efficiency": 100, "reservoir_min": 0, "reservoir_max": 20,
                       "reservoir_init": 10, "reservoir_end_min": 0, "spill_cost_usd_per_mm3": 1}},
            {"id": "ror", "type": "hydro_ror", "committable": False, "pmin": 0, "pmax": 4,
             "availability_profile": "ror", "mc": 1, "vom": 0, "bus": "a"},
            {"id": "wind", "type": "wind", "committable": False, "pmax_installed": 5,
             "availability_profile": "wind", "mc": 0, "vom": 0, "bus": "a"},
            {"id": "solar", "type": "solar", "committable": False, "pmax_installed": 5,
             "availability_profile": "solar", "mc": 0, "vom": 0, "bus": "a"},
            {"id": "bess", "type": "bess", "committable": False, "power_mw": 4, "energy_mwh": 8,
             "soc_init": .5, "soc_min": .1, "soc_max": .9, "eta_charge": .9, "eta_discharge": .9,
             "vom_discharge": .1, "cycle_cost_per_mwh": .1, "bus": "b"},
            {"id": "ph", "type": "pumped_hydro", "committable": False, "pmax": 4, "pump_mw": 4,
             "energy_mwh": 10, "soc_init": .5, "soc_min": .1, "soc_max": .9,
             "efficiency_pump": .9, "efficiency_gen": .9, "vom": .1, "bus": "b"},
            {"id": "dr", "type": "dr", "pmax_curtail": 2, "price_per_mwh": 100,
             "hours_per_year_max": 100, "bus": "b"},
            {"id": "import", "type": "import", "committable": False, "pmax": 30, "mc": 50,
             "vom": 0, "bus": "a"},
        ],
        "profiles": {"demand": [20.0] * periods, "inflow": [.05] * periods,
                     "ror": [.5] * periods, "wind": [.4] * periods, "solar": [.2] * periods},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4},
        "gas_constraints": {"mode": "annual+monthly", "applies_to": ["thermal"],
                            "annual": {"cap": 100}, "monthly": {"Jan": 100}},
        "reserve_products": [{"id": "up", "direction": "up", "requirement": 1,
                              "eligible_units": ["thermal", "bess"], "shortfall_penalty": 500}],
        "buses": [{"id": "a", "is_slack": True}, {"id": "b"}],
        "lines": [{"id": "ab", "from_bus": "a", "to_bus": "b",
                   "susceptance_mw_per_rad": 1000, "capacity_mw": 100}],
        "load_share_by_bus": {"a": .5, "b": .5},
        "solver_settings": settings,
    }


@pytest.mark.parametrize("resolution_min,rolling", [(60, False), (15, False), (60, True)])
def test_shared_mixed_fleet_is_independently_validated_and_publishable(resolution_min, rolling):
    inp = _input(resolution_min=resolution_min, rolling=rolling)
    assets = solver.build_asset_map(inp)
    rows, elapsed, objective = solver.solve_all(inp, assets, inp["profiles"], {})
    result = solver.build_result_store(rows, assets, inp, elapsed, objective)
    assert rows.solver_diagnostics.has_incumbent
    assert result["qa"]["status"] == "pass"
    assert result["publication"]["publishable"] is True
    objective_check = next(c for c in result["qa"]["checks"] if c["check_id"] == "objective_reconstruction")
    assert objective_check["status"] == "pass"
    assert objective_check["tolerance"] < 0.01  # normal numerical tolerance, not legacy 0.5%
    assert result["diagnostics"]["objective_breakdown"]["validated_full_precision"] is True
    assert len(rows.component_results) >= len(rows) * 5
    assert all(row["unserved_mwh"] == pytest.approx(0) for row in rows)

