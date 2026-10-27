"""Variable cost is counted exactly once - hand-checked 1-hour fixture.

Regression for a release-blocking defect: ``build_asset_map`` used ``vom`` as
the fallback fuel marginal cost (``_dispMC``) and every objective then added
``vom`` again, so imports and zero-heat-rate units paid VOM twice, reported
an implied fuel cost, and could lose merit order to costlier units.

Fixture: one hour, 100 MW demand, all units non-committable, CO2 price 0.

======  ==========================================  ========  ==========
unit    inputs                                      $/MWh     capacity
======  ==========================================  ========  ==========
zhr     heat_rate 0, fuel_price 5, vom 30           0 + 30    40 MW
imp     import, vom (import price) 45               0 + 45    50 MW
gas     heat_rate 10 GJ/MWh, fuel_price 5 $/MMBtu,  47.39 + 2 60 MW
        vom 2  (10 x 5 x 0.9478 = 47.39)
======  ==========================================  ========  ==========

Merit order 30 < 45 < 49.39, so dispatch is zhr 40, imp 50, gas 10 MW and

    objective = 40 x 30 + 50 x 45 + 10 x 49.39 = 1200 + 2250 + 493.9 = 3943.9 USD

with gas marginal at 49.39 $/MWh.  With the defect (zhr 60, imp 90 $/MWh) the
optimum was gas 60 + zhr 40 + imp 0 = 5363.4 USD instead.
"""
from __future__ import annotations

import copy

import pytest

from powersim.contracts import ProjectContract, resolve_scenario
from powersim.execution import resolved_to_workflow_input, run_registered_workflow
from solver.powersim_solver import build_asset_map, compute_marginal_prices, variable_cost_per_mwh

GAS_FUEL_MC = 10 * 5 * 0.9478  # 47.39 $/MWh
EXPECTED_DISPATCH = {"zhr": 40.0, "imp": 50.0, "gas": 10.0}
EXPECTED_OBJECTIVE = 40 * 30 + 50 * 45 + 10 * (GAS_FUEL_MC + 2)  # 3943.9


def _project() -> ProjectContract:
    def thermal(asset_id, pmax, heat_rate, fuel_price, vom):
        return {"id": asset_id, "kind": "thermal", "capacity_min_mw": 0, "capacity_max_mw": pmax,
                "legacy_extensions": {"committable": False, "heat_rate": heat_rate, "fuel_price": fuel_price,
                                      "vom": vom, "ramp_up": pmax, "ramp_down": pmax}}
    return ProjectContract.model_validate({
        "id": "variable-cost-fixture", "version": {"revision": 1}, "workflow": "deterministic_uc",
        "units": {"currency": "USD"},
        "time": {"timezone": "UTC", "study_year": 2026, "resolution_minutes": 60, "interval_duration_hours": 1,
                 "start": "2026-01-01T00:00:00Z", "periods": 1, "calendar_policy": "explicit_periods"},
        "assets": [thermal("gas", 60, 10, 5, 2), thermal("zhr", 40, 0, 5, 30),
                   {"id": "imp", "kind": "import", "capacity_max_mw": 50, "legacy_extensions": {"vom": 45}}],
        "profiles": [{"id": "demand", "unit": "MW", "values": [100]}],
        "solver_settings": {"solver": "highs", "time_limit_s": 30},
        "legacy_payload": {"resolution_min": 60, "study_horizon": {"start_hour": 0, "horizon_hours": 1}},
    })


@pytest.fixture(scope="module")
def workflow():
    return resolved_to_workflow_input(resolve_scenario(_project()))


@pytest.fixture(scope="module")
def solved(workflow):
    envelope = run_registered_workflow(copy.deepcopy(workflow), "fixture-run", "sha256:fixture")
    return envelope


def test_marginal_cost_components(workflow):
    assets = build_asset_map(workflow)
    # 1. import: no implied fuel cost; total variable cost is the import price once
    assert assets["imp"]["_dispMC"] == 0
    assert variable_cost_per_mwh(assets["imp"]) == 45
    # 2. thermal: fuel (heat rate x fuel price) + VOM, once
    assert assets["gas"]["_dispMC"] == pytest.approx(GAS_FUEL_MC)
    assert variable_cost_per_mwh(assets["gas"]) == pytest.approx(GAS_FUEL_MC + 2)
    # 3. zero heat rate: never an implied fuel cost, even with a fuel price
    assert assets["zhr"]["_dispMC"] == 0
    assert variable_cost_per_mwh(assets["zhr"]) == 30


def test_shared_component_cost_terms_count_vom_once(workflow):
    import pyomo.environ as pyo
    from powersim.components import ExchangeComponent, ThermalComponent
    assets = build_asset_map(workflow)
    assert ExchangeComponent(assets["imp"]).marginal_cost() == 45
    model = pyo.ConcreteModel()
    model.p = pyo.Var(["gas", "zhr"], [1], initialize=1.0)
    gas = pyo.value(ThermalComponent(assets["gas"])._variable_cost_expression(model, 1, 1.0))
    zhr = pyo.value(ThermalComponent(assets["zhr"])._variable_cost_expression(model, 1, 1.0))
    assert gas == pytest.approx(GAS_FUEL_MC + 2)
    assert zhr == 30


def test_solved_dispatch_costs_and_objective_match_hand_calculation(solved):
    assert solved.qa.status.value == "pass" and solved.validity.value == "valid"
    results = solved.results
    hour = results.get("hourly_by_unit")
    by_unit = results["by_unit_summary"]
    dispatch = {gid: by_unit[gid]["energy_mwh"] for gid in EXPECTED_DISPATCH}
    assert dispatch == pytest.approx(EXPECTED_DISPATCH, abs=1e-6), (dispatch, hour)

    # 1. import cost = import price x energy, once; no fuel component
    assert by_unit["imp"]["fuel_cost"] == 0
    assert by_unit["imp"]["vom_cost"] == pytest.approx(50 * 45)
    assert by_unit["imp"]["gross_cost"] == pytest.approx(50 * 45)
    assert by_unit["imp"]["SRMC"] == 45
    # 2. thermal cost = fuel + VOM, once
    assert by_unit["gas"]["fuel_cost"] == pytest.approx(10 * GAS_FUEL_MC, abs=0.5)
    assert by_unit["gas"]["vom_cost"] == pytest.approx(10 * 2)
    assert by_unit["gas"]["gross_cost"] == pytest.approx(10 * (GAS_FUEL_MC + 2), abs=0.5)
    assert by_unit["gas"]["SRMC"] == pytest.approx(GAS_FUEL_MC + 2, abs=0.005)
    # 3. zero heat rate: no fuel cost at all
    assert by_unit["zhr"]["fuel_cost"] == 0
    assert by_unit["zhr"]["vom_cost"] == pytest.approx(40 * 30)
    assert by_unit["zhr"]["gross_cost"] == pytest.approx(40 * 30)

    # 4. objective = hand-calculated 3943.9 USD
    summary = results["system_summary"]
    assert summary["total_unserved_mwh"] == 0
    assert summary["total_objective_cost_usd"] == pytest.approx(EXPECTED_OBJECTIVE, abs=1.0)
    witness = next(c for c in solved.qa.checks if c.check_id == "objective_reconstruction").witness
    assert witness["reconstructed_objective_usd"] == pytest.approx(EXPECTED_OBJECTIVE, abs=1e-3)
    if solved.solver.incumbent_objective is not None:
        assert solved.solver.incumbent_objective == pytest.approx(EXPECTED_OBJECTIVE, abs=1e-3)


def test_marginal_price_includes_vom_once(workflow, solved):
    """The ED re-solve and the price fallback use the same total variable cost."""
    assets = build_asset_map(workflow)
    hourly = [{"load_mw": 100.0, "dispatch": {gid: v for gid, v in EXPECTED_DISPATCH.items()},
               "commitment": {}, "bess": {}, "dr": {}, "pumped_hydro": {}}]
    priced = compute_marginal_prices(hourly, assets, {"demand": [100.0]}, [], {})
    assert priced[0]["lambda_usd_mwh"] == pytest.approx(GAS_FUEL_MC + 2, abs=1e-3)
    # the in-solve system price agrees
    assert solved.results["hourly_system"][0]["lambda_usd_mwh"] == pytest.approx(GAS_FUEL_MC + 2, abs=1e-3)
