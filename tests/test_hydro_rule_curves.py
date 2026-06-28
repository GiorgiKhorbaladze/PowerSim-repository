from __future__ import annotations
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "schema", ROOT / "solver"):
    if str(p) not in sys.path: sys.path.insert(0, str(p))
from powersim_solver import build_asset_map, slice_profiles, build_gas_limits, solve_all, HOURS_PER_YEAR


def _inp(horizon=4, start=0):
    return {
        "metadata":{"model_version":"PowerSim v4.0","schema_version":"1.5","timezone":"Asia/Tbilisi","study_year":2026},
        "study_horizon":{"start_hour":start,"horizon_hours":horizon,"mode":"full"},
        "profiles":{"demand":[100.0]*HOURS_PER_YEAR},
        "assets":[], "reserve_products":[], "gas_constraints":{"mode":"none"},
        "solver_settings":{"rolling_window_h":168,"rolling_step_h":24,"unserved_penalty":5000,"time_limit_s":60},
        "scenario_metadata":{"id":"A_mean","label":"hydro-test","probability":1.0},
    }

def _run(inp):
    assets=build_asset_map(inp); profiles,H=slice_profiles(inp); gas=build_gas_limits(inp, inp["study_horizon"].get("start_hour",0), H)
    hourly,_,obj=solve_all(inp,assets,profiles,gas); return hourly,assets,obj

def _thermal(mc=50, pmax=200):
    return {"id":"thermal","type":"thermal","committable":False,"pmin":0,"pmax":pmax,"heat_rate":0,"mc":mc,"vom":0,"ramp_up":999,"ramp_down":999}

def _hydro(**h):
    hydro={"reservoir_init":10,"reservoir_min":0,"reservoir_max":10,"reservoir_end_min":0,"efficiency":100,"water_value":0,"inflow":0}
    hydro.update(h)
    return {"id":"hydro","type":"hydro_reg","committable":False,"pmin":0,"pmax":100,"ramp_up":999,"ramp_down":999,"hydro":hydro}

def test_monthly_min_rule_curve_soft_violation_keeps_feasible():
    inp=_inp(horizon=2)
    inp["assets"]=[_hydro(reservoir_init=1, rule_curve={"monthly_min_frac":{"Jan":0.8},"min_violation_penalty_usd_per_mm3":10}), _thermal()]
    hourly,assets,obj=_run(inp)
    assert obj >= 0
    assert sum(h["hydro"]["hydro"]["rule_curve_min_violation_mm3"] for h in hourly) > 0

def test_monthly_target_rule_curve_shortfall_and_penalty():
    inp=_inp(horizon=2)
    inp["assets"]=[_hydro(reservoir_init=1, rule_curve={"monthly_target_frac":{"Jan":0.9},"target_penalty_usd_per_mm3":25}), _thermal()]
    hourly,assets,obj=_run(inp)
    assert sum(h["hydro"]["hydro"]["rule_curve_target_shortfall_mm3"] for h in hourly) > 0
    assert obj > 0

def test_monthly_water_value_reduces_dispatch_when_high():
    high=_inp(horizon=1); low=_inp(horizon=1)
    high["assets"]=[_hydro(water_value_profile={"Jan":100}), _thermal(mc=20)]
    low["assets"]=[_hydro(water_value_profile={"Jan":0}), _thermal(mc=20)]
    h1,_,_=_run(high); h2,_,_=_run(low)
    assert h1[0]["dispatch"]["hydro"] < h2[0]["dispatch"]["hydro"]

def test_minimum_release_enforced():
    inp=_inp(horizon=3)
    inp["profiles"]["demand"]=[0.0]*HOURS_PER_YEAR
    inp["assets"]=[_hydro(reservoir_init=10, min_release_mm3_per_h=1.0, pmax=0)]
    hourly,_,_=_run(inp)
    for h in hourly:
        row=h["hydro"]["hydro"]
        assert row["release_mm3h"] + row["spill_mm3h"] >= 0.999


def test_head_efficiency_curve_reports_two_bin_mode():
    inp=_inp(horizon=1)
    inp["assets"]=[_hydro(head_efficiency_curve=[{"storage_frac":0.2,"efficiency":80},{"storage_frac":1.0,"efficiency":120}]), _thermal()]
    hourly,_,_=_run(inp)
    assert hourly[0]["hydro"]["hydro"]["head_efficiency_mode"] == "two_bin"
