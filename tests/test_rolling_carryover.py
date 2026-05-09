"""
PowerSim — rolling-horizon carry-over correctness tests
========================================================

Each test builds a small synthetic two-window problem so we can prove
that the cross-window invariants hold:

  1. **Gas annual budget** — Σ_window gas_used ≤ annual_cap.
  2. **Gas monthly budget** — Σ_window gas_used_in_month_X ≤ cap_X.
  3. **DR annual hours** — Σ_window DR_hours_called ≤ hours_per_year_max.
  4. **UC min_up / min_down** — periods_on / periods_off propagate; a
     unit started near the end of window 1 stays on for the remainder
     of its min_up time in window 2 (and symmetric for shutdown).

The tests invoke `solver.powersim_solver.solve_all` directly with
synthetic, schema-light inputs.  No CSV/XLSX project_data is needed.

Run:  python tests/test_rolling_carryover.py     (exit 0 = pass)
or:   pytest tests/test_rolling_carryover.py -v  (with pytest)
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "schema", ROOT / "solver"):
    if str(p) not in sys.path: sys.path.insert(0, str(p))

from powersim_solver import (
    build_asset_map, slice_profiles, build_gas_limits, solve_all,
    solve_window, HOURS_PER_YEAR,
)


# ──────────────────────────────────────────────────────────────────────
# Synthetic problem factories
# ──────────────────────────────────────────────────────────────────────
def _base_input(*, horizon_h: int = 48, window_h: int = 24, step_h: int = 24,
                start_hour: int = 0) -> dict:
    """Minimal but schema-compatible PowerSim input."""
    demand = [200.0] * HOURS_PER_YEAR
    return {
        "metadata": {"model_version":"PowerSim v4.0", "schema_version":"1.4",
                     "timezone":"Asia/Tbilisi", "study_year":2026},
        "study_horizon": {"start_hour":start_hour, "horizon_hours":horizon_h, "mode":"rolling"},
        "assets": [],
        "profiles": {"demand": demand},
        "gas_constraints": {"mode":"none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap":0.005, "time_limit_s":120,
            "rolling_window_h": window_h, "rolling_step_h": step_h,
            "unserved_penalty":3000, "curtailment_penalty":0, "threads":0,
        },
        "scenario_metadata": {"id":"A_mean","label":"test","probability":1.0},
    }


def _drive(inp: dict) -> tuple[list, dict]:
    """Run solve_all and return (committed_hourly, asset_map)."""
    assets = build_asset_map(inp)
    profiles, _H = slice_profiles(inp)
    sh = inp["study_horizon"]
    gas_lim = build_gas_limits(inp, sh.get("start_hour",0), _H)
    hourly, _swall, _obj = solve_all(inp, assets, profiles, gas_lim)
    return hourly, assets


# ──────────────────────────────────────────────────────────────────────
# 1. GAS ANNUAL CAP — total Σ across windows must respect annual cap
# ──────────────────────────────────────────────────────────────────────
def test_gas_annual_cap_carry_over():
    inp = _base_input(horizon_h=48, window_h=24, step_h=24)
    inp["assets"] = [
        # One thermal big enough to cover demand for the whole 48h, but
        # gas-cap-bound to a budget that only allows ~24h of full output.
        {"id":"th_gas", "type":"thermal", "committable":True,
         "pmin":50, "pmax":300, "heat_rate":7.0, "fuel_type":"gas",
         "fuel_price":7.0, "vom":0, "startup_cost":0, "no_load_cost":0,
         "ramp_up":300, "ramp_down":300, "min_up":1, "min_down":1,
         "initial_status":1, "initial_power":200, "for_rate":0.0},
    ]
    # gas_rate = heat_rate / 35000 = 7.0/35000 = 2e-4 Mm³/MWh
    # 48h × 200 MW × 2e-4 = 1.92 Mm³ would be unconstrained dispatch.
    # Cap at 1.0 Mm³ so the second window has to be tighter than the first.
    inp["gas_constraints"] = {
        "mode":"annual", "unit":"Mm3",
        "annual": {"cap": 1.0},
        "applies_to": ["th_gas"],
    }
    # Allow shortage rather than infeasibility.
    hourly, _ = _drive(inp)
    total_gas = sum(h["gas_mm3h"] for h in hourly) * 1.0   # dt=1h
    print(f"  test_gas_annual_cap_carry_over:  Σ gas = {total_gas:.4f} Mm³  cap=1.0")
    assert total_gas <= 1.0 + 1e-6, (
        f"FAIL: 48h Σ gas {total_gas:.4f} > annual cap 1.0 — carry-over leaked")
    # Without carry-over the old behaviour allowed each window its own
    # daily_limit×H_hours allowance; assert we used a meaningful share.
    assert total_gas >= 0.1, f"sanity: gas barely used ({total_gas})"
    print("    ✓ annual cap respected across two windows")


# ──────────────────────────────────────────────────────────────────────
# 2. GAS MONTHLY CAP — Σ in month X across windows ≤ cap_X
# ──────────────────────────────────────────────────────────────────────
def test_gas_monthly_cap_carry_over():
    # Two 24h windows starting at hour 0 → both fall inside January.
    # Set Jan cap to 0.5 Mm³ — both windows must collectively respect it.
    inp = _base_input(horizon_h=48, window_h=24, step_h=24, start_hour=0)
    inp["assets"] = [
        {"id":"th_gas", "type":"thermal", "committable":True,
         "pmin":50, "pmax":300, "heat_rate":7.0, "fuel_type":"gas",
         "fuel_price":7.0, "vom":0, "startup_cost":0, "no_load_cost":0,
         "ramp_up":300, "ramp_down":300, "min_up":1, "min_down":1,
         "initial_status":1, "initial_power":200, "for_rate":0.0},
    ]
    inp["gas_constraints"] = {
        "mode":"monthly", "unit":"Mm3",
        "monthly": {"Jan": 0.5},
        "applies_to": ["th_gas"],
    }
    hourly, _ = _drive(inp)
    # All 48h are in January (start_hour=0).
    jan_gas = sum(h["gas_mm3h"] for h in hourly) * 1.0
    print(f"  test_gas_monthly_cap_carry_over: Σ Jan gas = {jan_gas:.4f} Mm³  cap=0.5")
    assert jan_gas <= 0.5 + 1e-6, (
        f"FAIL: monthly cap 0.5 exceeded — got {jan_gas:.4f}")
    print("    ✓ monthly cap respected across two windows")


# ──────────────────────────────────────────────────────────────────────
# 3. DR ANNUAL HOURS — Σ DR call-out hours across windows ≤ cap
# ──────────────────────────────────────────────────────────────────────
def test_dr_annual_hours_carry_over():
    # Tight gas + cheap DR: solver should call DR; cap DR hours to a
    # number lower than what unconstrained dispatch would consume.
    inp = _base_input(horizon_h=48, window_h=24, step_h=24)
    inp["assets"] = [
        # Thermal: very expensive marginal cost so DR is preferred.
        {"id":"th_gas", "type":"thermal", "committable":True,
         "pmin":50, "pmax":300, "heat_rate":15.0, "fuel_type":"gas",
         "fuel_price":15.0, "vom":0, "startup_cost":0, "no_load_cost":0,
         "ramp_up":300, "ramp_down":300, "min_up":1, "min_down":1,
         "initial_status":1, "initial_power":50, "for_rate":0.0},
        # DR: cheap per-MWh + very limited annual hours.
        {"id":"dr_load", "type":"dr", "committable":False,
         "pmax_curtail":200, "price_per_mwh":40,
         "hours_per_year_max": 8},   # only 8 call-out hours / year
    ]
    hourly, _ = _drive(inp)
    pmax = 200.0
    dr_used_mwh = sum((h.get("dr") or {}).get("dr_load", 0) for h in hourly) * 1.0
    dr_used_h   = dr_used_mwh / pmax    # MWh / MW = hours-equivalent
    print(f"  test_dr_annual_hours_carry_over: DR used = {dr_used_h:.3f} h  cap=8h")
    assert dr_used_h <= 8.0 + 1e-3, (
        f"FAIL: DR hours cap 8 exceeded across two windows — got {dr_used_h:.3f}")
    print("    ✓ DR annual hours respected across two windows")


# ──────────────────────────────────────────────────────────────────────
# 4. UC min_up / min_down BOUNDARY — periods_on / periods_off propagate
# ──────────────────────────────────────────────────────────────────────
def _minimal_uc_assets(min_up: int = 4, min_down: int = 4) -> dict:
    """One peaker + one cheap baseload, both committable, min_up/down set.
    Peaker has pmin=0 so being "on" doesn't force minimum output (avoids
    coupling the test to demand-MW gymnastics)."""
    return {
        "metadata": {"model_version":"PowerSim v4.0", "schema_version":"1.4",
                     "timezone":"Asia/Tbilisi", "study_year":2026},
        "study_horizon": {"start_hour":0, "horizon_hours":24, "mode":"full"},
        "assets":[
            {"id":"baseload", "type":"thermal", "committable":True,
             "pmin":0, "pmax":300, "heat_rate":6.0, "fuel_type":"gas",
             "fuel_price":5.0, "vom":1, "startup_cost":0, "no_load_cost":0,
             "ramp_up":300, "ramp_down":300, "min_up":1, "min_down":1,
             "initial_status":1, "initial_power":100, "for_rate":0.0},
            {"id":"peaker",   "type":"thermal", "committable":True,
             "pmin":0, "pmax":150, "heat_rate":12.0, "fuel_type":"gas",
             "fuel_price":12.0, "vom":3, "startup_cost":2000, "no_load_cost":500,
             "ramp_up":150, "ramp_down":150, "min_up":min_up, "min_down":min_down,
             "initial_status":0, "initial_power":0, "for_rate":0.0},
        ],
        "profiles":{"demand":[20.0]*HOURS_PER_YEAR},   # demand low → peaker prefers OFF
        "gas_constraints":{"mode":"none"},
        "reserve_products":[],
        "solver_settings":{"mip_gap":0.001, "time_limit_s":60,
                           "rolling_window_h":24, "rolling_step_h":24,
                           "unserved_penalty":3000, "curtailment_penalty":0,
                           "threads":0},
        "scenario_metadata":{"id":"A_mean","label":"test","probability":1.0},
    }


def test_min_up_boundary():
    """
    Direct boundary test: hand-craft init_state that says peaker just
    started 1 period before this window started (periods_on=1) with
    min_up=4.  Run a single window with very low demand: optimal would
    be to turn the peaker off immediately, but the boundary fix must
    keep it ON for 3 more periods (=min_up-periods_on).
    """
    inp = _minimal_uc_assets(min_up=4, min_down=1)
    assets = build_asset_map(inp)
    profiles, H = slice_profiles(inp)
    gas_lim = build_gas_limits(inp, 0, H)

    init_state = {
        "baseload": {"u":1, "p":20.0, "periods_on":24, "periods_off":0},
        "peaker":   {"u":1, "p":50.0, "periods_on":1,  "periods_off":0},
    }
    hourly, fin, _swall, _obj = solve_window(
        assets, profiles["demand"][:24], profiles, [], gas_lim,
        init_state=init_state, solver_cfg=inp["solver_settings"],
        offset_h=24, dt=1.0, commit_periods=24)
    com = [h["commitment"].get("peaker", 0) for h in hourly]
    print(f"  test_min_up_boundary: peaker u in window 2 (first 6h): {com[:6]}")
    # Boundary fix must force u=1 for periods 1, 2, 3 (3 more periods to
    # reach min_up=4 since periods_on=1 was carried in).
    assert com[0] == 1, "u[1] should be on (boundary force; min_up not met)"
    assert com[1] == 1, "u[2] should be on (boundary force; min_up not met)"
    assert com[2] == 1, "u[3] should be on (boundary force; min_up not met)"
    # By period 4 the unit is free to shut down (low demand, expensive
    # peaker → optimum is off ASAP).
    assert com[3] == 0, (
        f"u[4] should be allowed off after min_up satisfied; "
        f"got commit sequence {com[:5]}")
    print("    ✓ min_up=4 boundary respected (3 forced periods, then free)")
    # And fin_state must report the final state truthfully.
    assert fin["peaker"]["u"] == 0, fin["peaker"]
    assert fin["peaker"]["periods_off"] >= 1, fin["peaker"]


def test_min_down_boundary():
    """Symmetric direct test: peaker shutdown carried in (periods_off=1)
    with min_down=4 must stay OFF for 3 more periods even when window-2
    demand spikes to make starting it economically attractive."""
    inp = _minimal_uc_assets(min_up=1, min_down=4)
    # Make demand high so peaker would want to start ASAP if allowed.
    inp["profiles"]["demand"] = [400.0]*HOURS_PER_YEAR
    assets = build_asset_map(inp)
    profiles, H = slice_profiles(inp)
    gas_lim = build_gas_limits(inp, 0, H)
    init_state = {
        "baseload": {"u":1, "p":300.0, "periods_on":24, "periods_off":0},
        "peaker":   {"u":0, "p":0.0,    "periods_on":0,  "periods_off":1},
    }
    hourly, fin, _, _ = solve_window(
        assets, profiles["demand"][:24], profiles, [], gas_lim,
        init_state=init_state, solver_cfg=inp["solver_settings"],
        offset_h=24, dt=1.0, commit_periods=24)
    com = [h["commitment"].get("peaker", 0) for h in hourly]
    print(f"  test_min_down_boundary: peaker u in window 2 (first 6h): {com[:6]}")
    # Boundary fix must force u=0 for periods 1, 2, 3 (3 more periods of
    # forced off).
    for t_idx in (0, 1, 2):
        assert com[t_idx] == 0, (
            f"u[{t_idx+1}] should be off (boundary; min_down not met); "
            f"got {com[:5]}")
    # By period 4 the unit may turn on.
    assert com[3] == 1, (
        f"u[4] should be allowed on after min_down satisfied; got {com[:5]}")
    print("    ✓ min_down=4 boundary respected (3 forced-off periods)")


# ──────────────────────────────────────────────────────────────────────
# CLI / pytest entry
# ──────────────────────────────────────────────────────────────────────
TESTS = [
    test_gas_annual_cap_carry_over,
    test_gas_monthly_cap_carry_over,
    test_dr_annual_hours_carry_over,
    test_min_up_boundary,
    test_min_down_boundary,
]


def main() -> int:
    print("═" * 72)
    print("  Rolling-horizon carry-over deterministic tests")
    print("═" * 72)
    fails = []
    for t in TESTS:
        try:
            t()
        except AssertionError as e:
            print(f"  ✗ {t.__name__}: {e}")
            fails.append(t.__name__)
        except Exception as e:                       # pragma: no cover
            print(f"  ✗ {t.__name__}: {type(e).__name__}: {e}")
            fails.append(t.__name__)
    print("═" * 72)
    if fails:
        print(f"  FAIL: {len(fails)} of {len(TESTS)}")
        for n in fails: print(f"   ✗ {n}")
        return 1
    print(f"  PASS: {len(TESTS)}/{len(TESTS)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
