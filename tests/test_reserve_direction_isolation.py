#!/usr/bin/env python3
"""
PowerSim — deterministic regression test for the reserve-direction bug.

Auditor finding (verified in solver/powersim_solver.py:1560+):

    A reserve product with ``direction == "down"`` declares
    ``m.res_up[rid, g, t]`` for every eligible ``g, t`` (the variable
    is shared across all products) but adds no headroom constraint
    for it, and the eligible-unit ``.fix(0)`` block only zeroes the
    variable for INELIGIBLE units. That leaves ``m.res_up`` unbounded
    for eligible units under a down-only product — the solver just
    happens to leave it at zero because the objective doesn't reward
    a non-zero value, but nothing guarantees it and the output layer
    silently propagates whatever value the solver returns.

    Symmetric problem for an up-only product with ``m.res_down``.

The fix makes the "opposite" direction ``.fix(0)`` for eligible units
of a single-direction product, mirroring what BESS eligibility already
does. This test proves the invariant.

Test scenarios
--------------
A. Down-only reserve product: verify every ``reserve_up`` entry in
   ``hourly_by_unit`` is exactly 0 for every hour and every eligible
   unit. Baseline pre-fix could silently non-zero this.
B. Up-only reserve product: verify every ``reserve_down`` entry is
   exactly 0 for every hour and every eligible unit.
C. Symmetric reserve product: both directions may be non-zero
   (regression guard — the fix must not break this).

Exits 0 on success.
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


def _missing_deps() -> list[str]:
    return [m for m in ("pyomo", "highspy", "numpy", "pandas")
            if importlib.util.find_spec(m) is None]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _base_input(reserve_products):
    return {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 6, "mode": "full"},
        "resolution_min": 60,
        "profiles": {"demand": [80.0] * 8760},
        "assets": [
            {
                "id": "T1", "name": "Th 1", "type": "thermal",
                "committable": True,
                "pmin": 20, "pmax": 100,
                "heat_rate": 7, "fuel_type": "gas", "fuel_price": 5,
                "ramp_up": 100, "ramp_down": 100,
                "min_up": 1, "min_down": 1, "vom": 0,
                "startup_cost": 0, "no_load_cost": 0,
            },
        ],
        "gas_constraints": {"mode": "none"},
        "reserve_products": reserve_products,
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": 6, "rolling_step_h": 6,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }


def _run(inp, wd):
    ip = wd / "i.json"; op = wd / "o.json"; xp = wd / "x.xlsx"
    ip.write_text(json.dumps(inp), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(SOLVER),
         "--input", str(ip), "--output", str(op), "--excel", str(xp)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-1500:])
        sys.stderr.write(proc.stderr[-1500:])
        raise RuntimeError(f"solver rc={proc.returncode}")
    return json.loads(op.read_text(encoding="utf-8"))


def _reserve_up_for(rows, rid):
    """Return a flat list of every reserve_up value seen for `rid` in
    the hourly_by_unit rows for the unit."""
    out = []
    for r in rows:
        ru = r.get("reserve_up") or {}
        v = ru.get(rid, None)
        if v is not None:
            out.append(float(v))
    return out


def _reserve_down_for(rows, rid):
    out = []
    for r in rows:
        rd = r.get("reserve_down") or {}
        v = rd.get(rid, None)
        if v is not None:
            out.append(float(v))
    return out


def check_down_only_zeroes_res_up(wd):
    inp = _base_input([{
        "id": "aFRR_dn", "name": "aFRR Down",
        "direction": "down", "requirement": 5,
        "shortfall_penalty": 500,
        "eligible_units": ["T1"],
        "derating_factors": {},
    }])
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["T1"]
    ups = _reserve_up_for(rows, "aFRR_dn")
    _assert(ups, "hourly_by_unit reserve_up dict missing aFRR_dn key")
    _assert(all(abs(v) <= 1e-6 for v in ups),
            f"down-only aFRR_dn produced non-zero reserve_up: {ups[:6]}")


def check_up_only_zeroes_res_down(wd):
    inp = _base_input([{
        "id": "aFRR_up", "name": "aFRR Up",
        "direction": "up", "requirement": 5,
        "shortfall_penalty": 500,
        "eligible_units": ["T1"],
        "derating_factors": {},
    }])
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["T1"]
    dns = _reserve_down_for(rows, "aFRR_up")
    _assert(dns, "hourly_by_unit reserve_down dict missing aFRR_up key")
    _assert(all(abs(v) <= 1e-6 for v in dns),
            f"up-only aFRR_up produced non-zero reserve_down: {dns[:6]}")


def check_symmetric_still_allows_both(wd):
    # This is the regression guard: after fixing single-direction leaks,
    # a symmetric product must still be able to move both variables.
    inp = _base_input([{
        "id": "FCR", "name": "FCR",
        "direction": "symmetric", "requirement": 5,
        "shortfall_penalty": 500,
        "eligible_units": ["T1"],
        "derating_factors": {},
    }])
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["T1"]
    ups = _reserve_up_for(rows, "FCR")
    dns = _reserve_down_for(rows, "FCR")
    # Not necessarily strictly > 0, but at least the keys must be present
    # and the sum must equal the requirement per period (5 MW) since T1
    # is the only eligible unit and shortfall costs more than participating.
    _assert(len(ups) == len(rows) == 6,
            f"symmetric FCR: reserve_up rows count {len(ups)} != 6")
    _assert(len(dns) == len(rows) == 6,
            f"symmetric FCR: reserve_down rows count {len(dns)} != 6")
    _assert(all(u + 1e-6 >= 5 for u in ups),
            f"symmetric FCR: at least one hour under-supplied up: {ups}")
    _assert(all(d + 1e-6 >= 5 for d in dns),
            f"symmetric FCR: at least one hour under-supplied dn: {dns}")


def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP reserve-direction test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("down_only_zeroes_res_up",         check_down_only_zeroes_res_up),
        ("up_only_zeroes_res_down",         check_up_only_zeroes_res_down),
        ("symmetric_still_moves_both",      check_symmetric_still_allows_both),
    ]
    print("═" * 60)
    print("  PowerSim — reserve direction isolation test")
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
        print(f"\n❌ {failed}/{len(checks)} reserve-direction checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} reserve-direction checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
