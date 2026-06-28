#!/usr/bin/env python3
"""
PowerSim — deterministic tests for Thermal Stage 3 (PLEXOS parity).

Covers three optional fields added to ``thermal.*`` (all default-safe):

  A. co2_factor_t_per_mwh + co2_price_usd_per_t
       Per-MWh CO₂ emissions and an objective-cost charge at the
       system-wide carbon price. Output: per-unit ``total_co2_t`` /
       ``co2_cost_usd`` plus system totals.

  B. startup_cost_hot / startup_cost_cold + hot_start_threshold_h
       Two-bucket startup pricing. ``y_hot[g,t] ≤ y[g,t]`` and
       ``y_hot ≤ Σ_{k=1..hot_h} u[g,t-k]`` ⇒ hot allowed only when the
       unit was on within the threshold; cold otherwise. Init-state
       ``periods_on`` covers rolling-horizon boundaries.

  C. temp_derating_curve + temp_profile_key
       Piecewise-linear capacity multiplier vs ambient °C. Folded into
       ``get_pmax_t`` alongside maintenance derating, so it applies
       uniformly through the LP upper bound on ``m.p[g,t]``.

  D. backward compatibility — none of these set ⇒ identical behavior.

Exits 0 on success. Wired into CI alongside the other stage tests.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOLVER = ROOT / "solver" / "powersim_solver.py"
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

EPS = 1e-2


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _thermal(aid="T1", pmin=0, pmax=100, mc=0,
             heat_rate=10, fuel_price=0,
             startup_cost=0, no_load_cost=0,
             min_up=1, min_down=1,
             committable=True,
             extra=None):
    a = {
        "id": aid, "name": aid, "type": "thermal",
        "committable": committable,
        "pmin": pmin, "pmax": pmax,
        "heat_rate": heat_rate, "fuel_type": "gas", "fuel_price": fuel_price,
        "ramp_up": pmax, "ramp_down": pmax,
        "min_up": min_up, "min_down": min_down,
        "vom": mc,
        "startup_cost": startup_cost,
        "no_load_cost": no_load_cost,
    }
    if extra:
        a.update(extra)
    return a


def _input(horizon_h, demand, assets, *, profiles=None, co2_price=None):
    p = {"demand": (demand + [0.0] * (8760 - len(demand)))[:8760]}
    if profiles:
        p.update(profiles)
    inp = {
        "metadata": {"schema_version": "1.5", "model_version": "thermal-stage3-test"},
        "study_horizon": {"start_hour": 0, "horizon_hours": horizon_h, "mode": "full"},
        "resolution_min": 60,
        "profiles": p,
        "assets": assets,
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": horizon_h, "rolling_step_h": horizon_h,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }
    if co2_price is not None:
        inp["co2_price_usd_per_t"] = co2_price
    return inp


def _run(inp, wd):
    in_p = wd / "in.json"; out_p = wd / "out.json"; xl_p = wd / "out.xlsx"
    in_p.write_text(json.dumps(inp), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(SOLVER),
         "--input", str(in_p), "--output", str(out_p), "--excel", str(xl_p)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-2000:])
        sys.stderr.write(proc.stderr[-2000:])
        raise RuntimeError(f"solver rc={proc.returncode}")
    return json.loads(out_p.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────────
# A. CO₂ emissions + price
# ─────────────────────────────────────────────────────────────────────
def check_co2_emissions_and_cost(wd: Path):
    # Single thermal at zero fuel cost — only CO₂ + (no) vom drive cost.
    asset = _thermal(pmin=0, pmax=100, heat_rate=10, fuel_price=0,
                     extra={"co2_factor_t_per_mwh": 0.5})
    inp = _input(2, [100.0, 100.0], [asset], co2_price=50.0)
    res = _run(inp, wd)
    bu = res["by_unit_summary"]["T1"]
    sm = res["system_summary"]
    # Energy 200 MWh × 0.5 t/MWh = 100 t CO₂; cost = 100 × $50 = $5,000.
    _assert(abs(float(bu["total_co2_t"]) - 100.0) <= EPS,
            f"CO2: per-unit total {bu['total_co2_t']} ≠ 100 t")
    _assert(abs(float(bu["co2_cost_usd"]) - 5000.0) <= 1.0,
            f"CO2: per-unit cost {bu['co2_cost_usd']} ≠ 5000")
    _assert(abs(float(sm["total_co2_t"]) - 100.0) <= EPS,
            f"CO2: system total {sm['total_co2_t']} ≠ 100 t")
    _assert(abs(float(sm["total_co2_cost_usd"]) - 5000.0) <= 1.0,
            f"CO2: system cost {sm['total_co2_cost_usd']} ≠ 5000")
    _assert(float(sm["co2_price_usd_per_t"]) == 50.0,
            "CO2: system summary echoes price")
    # No price ⇒ no cost, totals still emitted.
    inp2 = _input(2, [100.0, 100.0], [asset])    # no co2_price
    res2 = _run(inp2, wd)
    _assert(float(res2["system_summary"]["total_co2_t"]) == 100.0,
            "CO2: totals always emitted regardless of price")
    _assert(float(res2["system_summary"]["total_co2_cost_usd"]) == 0.0,
            "CO2: zero price ⇒ zero cost")


# ─────────────────────────────────────────────────────────────────────
# B. Multi-stage startup (hot vs cold)
# ─────────────────────────────────────────────────────────────────────
def _short_off_obj(wd):
    # pmin=50 forces the unit to shut down when demand=0 (infeasible to
    # stay committed since p ≥ pmin·u). Off-period is ONE hour → within
    # the 3-hour hot threshold ⇒ hot-start eligible.
    a = _thermal(pmin=50, pmax=100, heat_rate=10, fuel_price=0,
                 no_load_cost=0,
                 extra={"startup_cost_hot": 100.0,
                        "startup_cost_cold": 10000.0,
                        "hot_start_threshold_h": 3})
    inp = _input(5, [80, 80, 0, 80, 80], [a])
    return _run(inp, wd)


def _long_off_obj(wd):
    # Same plant, but off-period extends to 6 hours ⇒ cold start mandatory.
    a = _thermal(pmin=50, pmax=100, heat_rate=10, fuel_price=0,
                 no_load_cost=0,
                 extra={"startup_cost_hot": 100.0,
                        "startup_cost_cold": 10000.0,
                        "hot_start_threshold_h": 3})
    inp = _input(10, [80, 80, 0, 0, 0, 0, 0, 0, 80, 80], [a])
    return _run(inp, wd)


def check_multi_stage_startup(wd: Path):
    res_short = _short_off_obj(wd)
    res_long  = _long_off_obj(wd)
    # Both runs serve identical energy (320 MWh) with the same fuel
    # cost ($0 here). The only obj diff is startup cost:
    #   short-off: 1 cold (h1) + 1 hot (h4)  = 10000 + 100  = 10,100
    #   long-off : 1 cold (h1) + 1 cold (h9) = 10000 + 10000= 20,000
    # → diff = 9,900.
    sc_short = sum(float(h["startup"].get("T1", 0))
                   for h in res_short["hourly_by_unit"]["T1"]
                   if False) if False else None
    # Use total_startup_cost from system summary (reliable cross-version key).
    sus_s = float(res_short["system_summary"]["total_startup_cost"])
    sus_l = float(res_long["system_summary"]["total_startup_cost"])
    _assert(abs(sus_s - 10100.0) <= 5.0,
            f"multi-stage: short-off startup cost {sus_s} ≠ 10,100 "
            f"(expected 1 cold + 1 hot)")
    _assert(abs(sus_l - 20000.0) <= 5.0,
            f"multi-stage: long-off startup cost {sus_l} ≠ 20,000 "
            f"(expected 2 cold)")
    _assert((sus_l - sus_s) >= 9000.0,
            f"multi-stage: cost increase from short→long start "
            f"({sus_l - sus_s}) below hot/cold delta")


# ─────────────────────────────────────────────────────────────────────
# C. Ambient temperature derating
# ─────────────────────────────────────────────────────────────────────
def check_temp_derating(wd: Path):
    # Curve: at 20°C → 1.0×pmax; at 40°C → 0.7×pmax. Temperature profile
    # 20,20,40,40,20,20. With pmax=100 the cap is 100,100,70,70,100,100.
    a = _thermal(pmin=0, pmax=100, heat_rate=10, fuel_price=0,
                 extra={"temp_derating_curve": [[20, 1.0], [40, 0.7]],
                        "temp_profile_key": "amb_temp"})
    inp = _input(6, [80.0] * 6, [a],
                 profiles={"amb_temp": ([20, 20, 40, 40, 20, 20]
                                        + [20.0] * 8760)[:8760]})
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["T1"]
    sysrows = res["hourly_system"]
    expected = [80, 80, 70, 70, 80, 80]
    for t, (r, h) in enumerate(zip(rows, sysrows)):
        disp = float(r["dispatch_mw"])
        unserved = float(h.get("unserved_mwh", 0))
        if t in (2, 3):
            _assert(abs(disp - 70.0) <= EPS,
                    f"temp derating: t={t} dispatch {disp} ≠ 70 (cap)")
            _assert(abs(unserved - 10.0) <= EPS,
                    f"temp derating: t={t} unserved {unserved} ≠ 10 MWh")
        else:
            _assert(abs(disp - 80.0) <= EPS,
                    f"temp derating: t={t} dispatch {disp} ≠ 80 (cool hour)")


# ─────────────────────────────────────────────────────────────────────
# D. Backward compatibility
# ─────────────────────────────────────────────────────────────────────
def check_backward_compat(wd: Path):
    a = _thermal(pmin=0, pmax=100, heat_rate=10, fuel_price=5,
                 startup_cost=500, no_load_cost=10)
    inp = _input(3, [50, 50, 50], [a])
    res = _run(inp, wd)
    bu = res["by_unit_summary"]["T1"]
    sm = res["system_summary"]
    # No co2_factor ⇒ totals = 0 (still emitted for schema stability).
    _assert(bu.get("total_co2_t", 0) == 0,
            f"back-compat: zero co2_factor ⇒ total_co2_t=0; got {bu.get('total_co2_t')}")
    _assert(sm.get("total_co2_t", 0) == 0,
            "back-compat: system total_co2_t=0 with no factors")
    _assert(sm.get("total_co2_cost_usd", 0) == 0,
            "back-compat: system co2 cost=0 with no price")


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP thermal Stage 3 test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("co2_emissions_and_cost",          check_co2_emissions_and_cost),
        ("multi_stage_startup_hot_cold",    check_multi_stage_startup),
        ("ambient_temp_derating",           check_temp_derating),
        ("backward_compat_no_new_fields",   check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — Thermal Stage 3 test")
    print("═" * 60)
    failed = 0
    with tempfile.TemporaryDirectory() as td:
        wd = Path(td)
        for name, fn in checks:
            try:
                fn(wd)
                print(f"  ✓ {name}")
            except Exception as e:
                failed += 1
                print(f"  ✗ {name}\n     {type(e).__name__}: {e}")
    if failed:
        print(f"\n❌ {failed}/{len(checks)} thermal Stage 3 checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} thermal Stage 3 checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
