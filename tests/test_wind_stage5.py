#!/usr/bin/env python3
"""
PowerSim — deterministic tests for Wind Stage 5 (PLEXOS parity).

Three optional wind-only fields, layered on top of the generic Solar
Stage 4 inverter/degradation/temperature stack. All additive and
default-safe — absence ⇒ unchanged behavior.

  A. wake_loss_frac           — constant farm-level loss multiplier
  B. monthly_availability_factor — 12-entry seasonal mask (icing,
                                   planned maintenance patterns)
  C. air_density_correction   — physical density-vs-temperature
                                correction (∝ 1/T_kelvin) using the
                                existing temperature profile
  D. backward compatibility   — pre-Stage-5 inputs unchanged.

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

EPS = 0.10   # MW — tolerant of LP slack on rounded outputs


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _wind(aid="W1", pmax_installed=100, profile_key="wind_cf", extra=None):
    a = {
        "id": aid, "name": aid, "type": "wind",
        "committable": False,
        "pmax_installed": pmax_installed,
        "availability_profile": profile_key,
        "vom": 0,
    }
    if extra:
        a.update(extra)
    return a


def _backup(pmax=500, mc_high=False):
    return {
        "id": "BACK", "name": "Backup", "type": "thermal", "committable": False,
        "pmin": 0, "pmax": pmax,
        "heat_rate": 10, "fuel_type": "gas",
        "fuel_price": 200 if mc_high else 5,
        "ramp_up": pmax, "ramp_down": pmax, "min_up": 1, "min_down": 1, "vom": 0,
    }


def _input(horizon_h, demand, assets, *, profiles=None, study_year=2030,
           start_hour=0):
    p = {"demand": (demand + [0.0] * (8760 - len(demand)))[:8760]}
    if profiles:
        for k, v in profiles.items():
            p[k] = (v + [0.0] * (8760 - len(v)))[:8760]
    return {
        "metadata": {"schema_version": "1.5", "model_version": "wind-stage5-test",
                     "study_year": study_year},
        "study_horizon": {"start_hour": start_hour, "horizon_hours": horizon_h,
                          "mode": "full"},
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


def _disp(res, gid):
    return [float(r["dispatch_mw"]) for r in res["hourly_by_unit"][gid]]


# ─────────────────────────────────────────────────────────────────────
# A. Wake losses
# ─────────────────────────────────────────────────────────────────────
def check_wake_loss(wd: Path):
    # wake_loss_frac=0.10 ⇒ dispatch ≤ 100 × 1.0 × (1−0.10) = 90 MW.
    a = _wind(extra={"wake_loss_frac": 0.10})
    inp = _input(3, [200.0] * 3, [a, _backup()],
                 profiles={"wind_cf": [1.0] * 3})
    res = _run(inp, wd)
    for t, d in enumerate(_disp(res, "W1")):
        _assert(abs(d - 90.0) <= EPS,
                f"wake_loss: t={t} dispatch {d:.3f} ≠ 90.0")


# ─────────────────────────────────────────────────────────────────────
# B. Monthly availability mask
# ─────────────────────────────────────────────────────────────────────
def check_monthly_availability(wd: Path):
    # Window starts at hour 0 (January). Months 0,2 = 1.0; month 1 = 0.7
    # — but we only span the first 6 hours so all of them land in January.
    # Set Jan = 0.7 ⇒ dispatch = pmax × 1.0 × 0.7 = 70.
    months = [0.7] + [1.0] * 11
    a = _wind(extra={"monthly_availability_factor": months})
    inp = _input(6, [200.0] * 6, [a, _backup()],
                 profiles={"wind_cf": [1.0] * 6})
    res = _run(inp, wd)
    for t, d in enumerate(_disp(res, "W1")):
        _assert(abs(d - 70.0) <= EPS,
                f"monthly_avail (Jan=0.7): t={t} dispatch {d:.3f} ≠ 70.0")

    # Different window: start_hour = 1500 (well into March — Jan ends
    # at 744, Feb at 1416; 1500 is hour 84 of March). Set Jan=0.5,
    # Feb=0.5, March=1.0. Expected dispatch = 100 (no derate in March).
    months2 = [0.5, 0.5] + [1.0] * 10
    a2 = _wind(extra={"monthly_availability_factor": months2})
    demand_full = [0.0] * 8760
    cf_full     = [0.0] * 8760
    for i in range(1500, 1506):
        demand_full[i] = 200.0
        cf_full[i]     = 1.0
    inp2 = _input(6, [200.0] * 6, [a2, _backup()],
                  profiles={"wind_cf": cf_full},
                  start_hour=1500)
    # Override demand: tests/_input fills outside [0..N] with zero, but
    # for start_hour > 0 we need demand at the offset.
    inp2["profiles"]["demand"] = demand_full
    res2 = _run(inp2, wd)
    for t, d in enumerate(_disp(res2, "W1")):
        _assert(abs(d - 100.0) <= EPS,
                f"monthly_avail (March=1.0): t={t} dispatch {d:.3f} ≠ 100.0")


# ─────────────────────────────────────────────────────────────────────
# C. Air density correction
# ─────────────────────────────────────────────────────────────────────
def check_air_density_correction(wd: Path):
    # ref_temp = 15 °C ⇒ ref_K = 288.15.
    # At 5 °C  (278.15 K): factor = 288.15/278.15 = 1.0360 → dispatch ≈ 103.60
    # At 25 °C (298.15 K): factor = 288.15/298.15 = 0.9665 → dispatch ≈  96.65
    # We need pmax_installed large enough that the boosted output isn't
    # capped at pmax. With pmax_installed=200 and cf=0.5, base = 100 MW,
    # then × factor.
    a = _wind(pmax_installed=200,
              extra={"air_density_correction": True,
                     "density_ref_temp_c": 15.0,
                     "temp_profile_key": "amb_temp"})
    inp = _input(4, [300.0] * 4, [a, _backup()],
                 profiles={"wind_cf": [0.5] * 4,
                           "amb_temp": [5, 5, 25, 25]})
    res = _run(inp, wd)
    d = _disp(res, "W1")
    expect = [100 * 288.15 / 278.15, 100 * 288.15 / 278.15,
              100 * 288.15 / 298.15, 100 * 288.15 / 298.15]
    for t, (a_d, e) in enumerate(zip(d, expect)):
        _assert(abs(a_d - e) <= EPS,
                f"density: t={t} dispatch {a_d:.3f} ≠ {e:.3f}")


# ─────────────────────────────────────────────────────────────────────
# D. Backward compatibility
# ─────────────────────────────────────────────────────────────────────
def check_backward_compat(wd: Path):
    a = _wind()        # no new fields
    inp = _input(4, [200.0] * 4, [a, _backup()],
                 profiles={"wind_cf": [0.3, 0.5, 0.7, 1.0]})
    res = _run(inp, wd)
    d = _disp(res, "W1")
    expect = [30.0, 50.0, 70.0, 100.0]
    for t, (a_d, e) in enumerate(zip(d, expect)):
        _assert(abs(a_d - e) <= EPS,
                f"back-compat: t={t} dispatch {a_d:.3f} ≠ {e}")


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP wind Stage 5 test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("wake_loss_constant",              check_wake_loss),
        ("monthly_availability_mask",       check_monthly_availability),
        ("air_density_correction",          check_air_density_correction),
        ("backward_compat_no_new_fields",   check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — Wind Stage 5 test")
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
        print(f"\n❌ {failed}/{len(checks)} wind Stage 5 checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} wind Stage 5 checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
