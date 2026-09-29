"""Stage 5A first gate: singleton stochastic UC must be deterministic parity."""
from __future__ import annotations
import importlib.util
from pathlib import Path
import pytest

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("solver", ROOT / "solver" / "powersim_solver.py")
solver = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(solver)
SHARED = importlib.util.spec_from_file_location("stoch_shared", ROOT / "solver" / "powersim_stochastic_shared.py")
stochastic = importlib.util.module_from_spec(SHARED); SHARED.loader.exec_module(stochastic)


def _input(resolution_min=60):
    periods = 2 * 60 // resolution_min
    return {"resolution_min": resolution_min,
      "assets":[
        {"id":"g","type":"thermal","committable":False,"pmin":0,"pmax":30,"mc":20,"vom":1,"ramp_up":100,"ramp_down":100,"bus":"a","heat_rate":7,"fuel_type":"gas","fuel_price":5},
        {"id":"w","type":"wind","committable":False,"pmax_installed":4,"availability_profile":"wind","bus":"a"},
        {"id":"b","type":"bess","power_mw":2,"energy_mwh":4,"soc_init":.5,"soc_min":0,"soc_max":1,"eta_charge":.9,"eta_discharge":.9,"bus":"b"},
        {"id":"i","type":"import","committable":False,"pmax":10,"mc":50,"bus":"b"}],
      "profiles":{"demand":[10.0]*periods,"wind":[.5]*periods},
      "study_horizon":{"horizon_hours":2}, "reserve_products":[{"id":"up","direction":"up","requirement":1,"eligible_units":["g","b"]}],
      "gas_constraints":{"mode":"annual","applies_to":["g"],"annual":{"cap":100}},
      "buses":[{"id":"a","is_slack":True},{"id":"b"}],"lines":[{"id":"ab","from_bus":"a","to_bus":"b","susceptance_mw_per_rad":1000,"capacity_mw":100}],"load_share_by_bus":{"a":.5,"b":.5},
      "solver_settings":{"solver":"highs","component_engine":"shared"},
      "stochastic_scenarios":[{"id":"base","probability":1.0}]}


@pytest.mark.parametrize("resolution_min", [60, 15])
def test_single_scenario_shared_stochastic_matches_deterministic_all_canonical_rows(resolution_min):
    inp=_input(resolution_min)
    assets=solver.build_asset_map(inp)
    gas=solver.build_gas_limits(inp, 0, 2)
    rows, elapsed, objective=solver.solve_all(inp, assets, inp["profiles"], gas)
    deterministic=solver.build_result_store(rows, assets, inp, elapsed, objective)
    out=stochastic.run_stochastic_shared(inp)
    scenario=out["scenarios"][0]["result"]
    assert out["aggregate"]["expected_objective_usd"] == pytest.approx(objective)
    assert scenario["hourly_system"] == deterministic["hourly_system"]
    assert scenario["qa"]["status"] == "pass" and out["publication"]["publishable"] is True


@pytest.mark.parametrize("scenarios,match", [
    ([{"id":"a","probability":-1}], "strictly positive"),
    ([{"id":"a","probability":float("nan")}], "strictly positive"),
    ([{"id":"a","probability":0}], "strictly positive"),
    ([{"id":"a","probability":.5},{"id":"a","probability":.5}], "duplicate"),
    ([{"id":"a","probability":.9}], "sum to 1"),
])
def test_shared_stochastic_probability_validation_fails_closed(scenarios, match):
    inp=_input(); inp["stochastic_scenarios"]=scenarios
    with pytest.raises(ValueError, match=match): stochastic.run_stochastic_shared(inp)


def test_two_scenario_identical_inputs_share_first_stage_and_publish():
    inp=_input(); inp["stochastic_scenarios"]=[{"id":"a","probability":.5},{"id":"b","probability":.5}]
    out=stochastic.run_stochastic_shared(inp)
    a,b=out["scenarios"]
    assert a["first_stage"] == b["first_stage"]
    assert a["result"]["hourly_system"] == b["result"]["hourly_system"]
    assert a["objective_usd"] == pytest.approx(b["objective_usd"])
    assert out["aggregate"]["expected_objective_usd"] == pytest.approx(a["objective_usd"])
    assert out["publication"]["publishable"] is True
    for scenario in (a,b):
        check_ids={check["check_id"] for check in scenario["result"]["qa"]["checks"]}
        assert {"component.thermal_gas_conversion", "reserve.product_requirement", "network.nodal_balance", "objective_reconstruction"} <= check_ids


def test_divergent_demand_profiles_keep_first_stage_nonanticipative_and_allow_recourse():
    inp=_input()
    inp["stochastic_scenarios"]=[
        {"id":"low","probability":.5,"profile_overrides":{"demand":[8.0,8.0]}},
        {"id":"high","probability":.5,"profile_overrides":{"demand":[14.0,14.0]}},
    ]
    out=stochastic.run_stochastic_shared(inp)
    low,high=out["scenarios"]
    assert low["first_stage"] == high["first_stage"]
    assert low["result"]["hourly_system"] != high["result"]["hourly_system"]
    assert out["qa"]["status"] == "pass"


def test_unknown_or_nonfinite_profile_override_fails_closed():
    inp=_input(); inp["stochastic_scenarios"]=[{"id":"a","probability":1.0,"profile_overrides":{"missing":[1,1]}}]
    with pytest.raises(ValueError, match="unknown profile"): stochastic.run_stochastic_shared(inp)
    inp=_input(); inp["stochastic_scenarios"]=[{"id":"a","probability":1.0,"profile_overrides":{"demand":[1,float("nan")]}}]
    with pytest.raises(ValueError, match="non-finite"): stochastic.run_stochastic_shared(inp)
