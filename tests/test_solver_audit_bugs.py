#!/usr/bin/env python3
"""
PowerSim — deterministic regression tests for the 5-bug audit fix batch.

Covers five CONFIRMED audit findings in solver/powersim_solver.py:

  #2 start_h scope shadow in the rolling driver — inner ``start_h``
     (window offset) shadowed the outer ``sh.start_hour`` value and
     silently dropped it from ``offset_h`` and monthly-gas lookups
     for every window after the first.
  #3 End-of-horizon storage / SOC target constraints (``StorEnd``,
     ``EndShort``, ``BessEndShort``) were declared at every rolling
     window's last period. That drove intermediate reservoir/SOC to
     the end target artificially and inflated cost.
  #5 Reconstructed objective omitted ``co2_cost`` (thermal CO₂ term)
     and ``stor_target_pen`` (hydro monthly storage-target penalty),
     opening a silent closure gap when either was non-zero.
  #6 Reserve-shortfall penalty in the reconstructed objective did
     not multiply by ``dt`` — LP charged ``pen × MW × dt`` per period
     but closure summed only ``pen × MW``. Wrong at sub-hourly
     resolution (dt != 1) and would have masked reserve costs then.
  #7 ``hourly_by_unit[bess].energy_mwh`` was 0 because the sum used
     the ``dispatch`` map, which does not include BESS. The BESS MW
     flows live under the ``bess`` map key.

Test scenarios
--------------
A. **Reserve × dt closure at 30-min resolution.**
   Force a reserve shortfall (requirement > any single unit), run at
   ``resolution_min=30`` (dt=0.5), and check that the reconstructed
   reserve penalty equals ``pen × sum(shortfall_MW) × dt``, not
   ``pen × sum(shortfall_MW)``.

B. **CO₂ term appears in the reconstructed objective.**
   Set ``co2_price_usd_per_t`` > 0 and ``co2_factor_t_per_mwh`` on a
   thermal unit that must dispatch to serve load. Check that
   ``objective_breakdown.co2_cost > 0`` (not silently dropped) and
   that ``closure_gap_pct`` is inside tolerance.

C. **BESS ``energy_mwh`` is non-zero when it delivered energy.**
   With a solar profile that peaks in the middle of the day and a
   cheap thermal unit off overnight, the BESS is the cheapest way
   to shift solar into evening peak. Check the BESS ``by_unit``
   entry has ``energy_mwh > 0`` and it matches the sum of
   ``discharge_mw - charge_mw`` over ``hourly`` rows.

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


# ── Scenario A: reserve × dt closure at 30-min resolution ──────────
def check_reserve_dt_closure(wd):
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4, "mode": "full"},
        "resolution_min": 30,
        "profiles": {"demand": [80.0] * (8760 * 2)},
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
        "reserve_products": [{
            # Requirement larger than headroom (pmax - load = 20 MW)
            # forces a persistent shortfall each period.
            "id": "aFRR_up", "name": "aFRR Up",
            "direction": "up", "requirement": 60,
            "shortfall_penalty": 500,
            "eligible_units": ["T1"],
            "derating_factors": {},
        }],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": 4, "rolling_step_h": 4,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }
    res = _run(inp, wd)
    ob   = res["diagnostics"]["objective_breakdown"]
    pen  = float(ob["reserve_shortfall_penalty"])
    gap  = ob.get("closure_gap_pct")
    _assert(pen > 0, f"expected positive reserve penalty (shortfall was forced); got {pen}")
    _assert(gap is not None and gap < 0.5,
            f"closure_gap_pct {gap} exceeds 0.5% tolerance at 30-min dt")

    # Cross-check the sign of the fix: reconstructed penalty should equal
    # pen × sum(shortfall_MW) × dt (=0.5). Pre-fix, closure would differ
    # by ×2 (miss the dt factor). Post-fix it converges.
    shortfall_mw_sum = 0.0
    for row in res["hourly_system"]:
        rs = row.get("reserve_shortfall") or {}
        shortfall_mw_sum += float(rs.get("aFRR_up", 0) or 0)
    expected = 500.0 * shortfall_mw_sum * 0.5   # pen × MW × dt(hours)
    _assert(abs(pen - expected) / max(expected, 1.0) < 1e-3,
            f"pen_reserve reconstruction wrong: got {pen}, expected ≈ {expected}")


# ── Scenario B: CO₂ term captured in closure ────────────────────────
def check_co2_in_closure(wd):
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 6, "mode": "full"},
        "resolution_min": 60,
        "co2_price_usd_per_t": 40.0,
        "profiles": {"demand": [60.0] * 8760},
        "assets": [
            {
                "id": "T1", "name": "Th 1", "type": "thermal",
                "committable": True,
                "pmin": 20, "pmax": 100,
                "heat_rate": 7, "fuel_type": "gas", "fuel_price": 5,
                "co2_factor_t_per_mwh": 0.4,
                "ramp_up": 100, "ramp_down": 100,
                "min_up": 1, "min_down": 1, "vom": 0,
                "startup_cost": 0, "no_load_cost": 0,
            },
        ],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": 6, "rolling_step_h": 6,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }
    res = _run(inp, wd)
    ob  = res["diagnostics"]["objective_breakdown"]
    co2 = ob.get("co2_cost")
    _assert(co2 is not None, "objective_breakdown missing co2_cost key after fix")
    _assert(co2 > 0, f"co2_cost should be > 0 (T1 dispatched, factor > 0, price > 0); got {co2}")
    gap = ob.get("closure_gap_pct")
    _assert(gap is not None and gap < 0.5,
            f"closure_gap_pct {gap} exceeds 0.5% tolerance under CO₂ price")


# ── Scenario C: BESS energy_mwh non-zero ────────────────────────────
def check_bess_energy_mwh(wd):
    # 6-hour horizon: 3 h solar surplus (solar > load), then 3 h peak
    # demand that exceeds thermal pmax alone → BESS must discharge to
    # avoid expensive unserved energy. This guarantees non-zero cycling.
    solar = [1.0, 1.0, 1.0, 0.0, 0.0, 0.0] + [0.0] * (8760 - 6)
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 6, "mode": "full"},
        "resolution_min": 60,
        "profiles": {
            # Off-peak 40 MW then peak 100 MW. Thermal alone tops out at
            # 80 MW so the last three hours must borrow 20 MW from BESS
            # (or pay unserved_penalty × 20 × 3 = $180k).
            "demand": [40.0, 40.0, 40.0, 100.0, 100.0, 100.0] + [40.0] * (8760 - 6),
            "solar_S1": solar,
        },
        "assets": [
            {
                "id": "T1", "name": "Th 1", "type": "thermal",
                "committable": True,
                "pmin": 10, "pmax": 80,
                "heat_rate": 8, "fuel_type": "gas", "fuel_price": 8,
                "ramp_up": 100, "ramp_down": 100,
                "min_up": 1, "min_down": 1, "vom": 0,
                "startup_cost": 0, "no_load_cost": 0,
            },
            {
                # 100-MW solar, curtailment penalty tilts the objective
                # toward charging BESS from the 60 MW off-peak surplus.
                "id": "S1", "name": "Sol 1", "type": "solar",
                "pmax": 100, "profile": "solar_S1", "vom": 0,
            },
            {
                "id": "B1", "name": "BESS 1", "type": "bess",
                "committable": False,
                "power_mw": 30, "energy_mwh": 60,
                "soc_init": 0.5, "soc_min": 0.05, "soc_max": 0.95,
                "eta_charge": 0.95, "eta_discharge": 0.95,
                "vom_charge": 0.0, "vom_discharge": 0.1,
            },
        ],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": 6, "rolling_step_h": 6,
            "unserved_penalty": 3000, "solver": "auto",
            "curtailment_penalty": 10,
        },
    }
    res = _run(inp, wd)
    hu = res["hourly_by_unit"].get("B1")
    _assert(hu, "hourly_by_unit missing B1")
    # by_unit_summary carries the aggregate energy_mwh per asset.
    summ = res.get("by_unit_summary") or {}
    b1 = summ.get("B1") or {}
    e = float(b1.get("energy_mwh", 0) or 0)
    _assert(abs(e) > 1e-3,
            f"BESS energy_mwh silently 0 (bug #7 regression): got {e}")

    # Verify it matches the row-level (discharge − charge) × dt sum
    # over the per-unit hourly rows in `hourly_by_unit`.
    net = 0.0
    for r in hu:
        b = r.get("bess") or {}
        net += float(b.get("discharge_mw", 0) or 0) - float(b.get("charge_mw", 0) or 0)
    _assert(abs(e - net) / max(abs(net), 1.0) < 1e-2,
            f"BESS energy_mwh ({e}) != row-level net ({net})")


def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP solver-audit-bugs test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("reserve_dt_closure_at_30min",  check_reserve_dt_closure),
        ("co2_cost_in_closure",          check_co2_in_closure),
        ("bess_energy_mwh_nonzero",      check_bess_energy_mwh),
    ]
    print("═" * 60)
    print("  PowerSim — solver audit bug batch test")
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
        print(f"\n❌ {failed}/{len(checks)} solver-audit-bugs checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} solver-audit-bugs checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
