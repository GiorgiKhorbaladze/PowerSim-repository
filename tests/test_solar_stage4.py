#!/usr/bin/env python3
"""
PowerSim — deterministic tests for Solar / VRE Stage 4 (PLEXOS parity).

Three optional fields added to ``solar``/``wind`` assets (all additive,
default-safe — absence ⇒ unchanged behavior):

  A. dc_ac_ratio + inverter_efficiency
       Profile is treated as DC capacity factor; the LP upper bound
       becomes ``pmax_installed × min(cf × dc_ac_ratio, 1.0)
                                 × inverter_efficiency``. Captures
       modern PV plants that overbuild DC vs the AC inverter rating
       (typical 1.2–1.4) to clip midday peaks and flatten the profile.
  B. degradation_rate_per_year + commissioning_year
       Pre-computed in ``build_asset_map`` as
       ``(1 - rate)^(metadata.study_year − commissioning_year)`` and
       applied to the LP upper bound. Multi-year studies see the
       progressive capacity loss without changing the input profile.
  C. temp_derating_curve + temp_profile_key
       Schema validation now allows the derating mechanism on VRE
       units; the solver already applies ``_temp_factor`` uniformly
       via ``get_pmax_t`` (added in Thermal Stage 3).

  D. backward compatibility — none of these set ⇒ outputs unchanged.

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

EPS = 5e-2


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _solar(aid="S1", pmax_installed=100, profile_key="solar_cf", extra=None):
    a = {
        "id": aid, "name": aid, "type": "solar",
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


def _input(horizon_h, demand, assets, *, profiles=None, study_year=2030):
    p = {"demand": (demand + [0.0] * (8760 - len(demand)))[:8760]}
    if profiles:
        for k, v in profiles.items():
            p[k] = (v + [0.0] * (8760 - len(v)))[:8760]
    return {
        "metadata": {"schema_version": "1.5", "model_version": "solar-stage4-test",
                     "study_year": study_year},
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


def _dispatch(res, gid):
    return [float(r["dispatch_mw"]) for r in res["hourly_by_unit"][gid]]


# ─────────────────────────────────────────────────────────────────────
# A. DC/AC ratio + inverter efficiency
# ─────────────────────────────────────────────────────────────────────
def check_dc_ac_ratio_clipping(wd: Path):
    # Profile [0.4, 0.6, 0.8, 1.0] × dc_ac_ratio 1.5 → [0.60, 0.90, 1.20, 1.50]
    # then clipped at 1.0 → [0.60, 0.90, 1.00, 1.00]. inverter_eff=1.0 here.
    # With pmax=100: available is [60, 90, 100, 100]. High demand makes the
    # solar dispatch at exactly the cap.
    a = _solar(extra={"dc_ac_ratio": 1.5, "inverter_efficiency": 1.0})
    inp = _input(4, [200.0] * 4, [a, _backup()],
                 profiles={"solar_cf": [0.4, 0.6, 0.8, 1.0]})
    res = _run(inp, wd)
    disp = _dispatch(res, "S1")
    expect = [60.0, 90.0, 100.0, 100.0]
    for t, (d, e) in enumerate(zip(disp, expect)):
        _assert(abs(d - e) <= EPS,
                f"dc_ac_clip: t={t} dispatch {d:.2f} ≠ {e}")


def check_inverter_efficiency(wd: Path):
    # inverter_efficiency=0.95, no DC/AC overbuild. Profile = 1.0 → 0.95 AC.
    a = _solar(extra={"inverter_efficiency": 0.95})
    inp = _input(2, [200.0] * 2, [a, _backup()],
                 profiles={"solar_cf": [1.0, 1.0]})
    res = _run(inp, wd)
    disp = _dispatch(res, "S1")
    for t, d in enumerate(disp):
        _assert(abs(d - 95.0) <= EPS,
                f"inv_eta: t={t} dispatch {d:.2f} ≠ 95")


# ─────────────────────────────────────────────────────────────────────
# B. Multi-year degradation
# ─────────────────────────────────────────────────────────────────────
def check_degradation_rate(wd: Path):
    # Commissioned 2030, rate 0.005/y. Same plant simulated in 2030 vs 2036.
    # Expected factor: 2030 → 1.0;  2036 → (1-0.005)^6 ≈ 0.9703.
    a_2030 = _solar(extra={"degradation_rate_per_year": 0.005,
                           "commissioning_year": 2030})
    a_2036 = _solar(extra={"degradation_rate_per_year": 0.005,
                           "commissioning_year": 2030})
    profile = [1.0] * 4
    inp_30 = _input(4, [200.0] * 4, [a_2030, _backup()],
                    profiles={"solar_cf": profile}, study_year=2030)
    inp_36 = _input(4, [200.0] * 4, [a_2036, _backup()],
                    profiles={"solar_cf": profile}, study_year=2036)
    d30 = _dispatch(_run(inp_30, wd), "S1")
    d36 = _dispatch(_run(inp_36, wd), "S1")
    factor_expected = 0.995 ** 6     # ≈ 0.97029
    for t in range(4):
        _assert(abs(d30[t] - 100.0) <= EPS,
                f"degradation: 2030 dispatch {d30[t]:.2f} ≠ 100")
        _assert(abs(d36[t] - 100.0 * factor_expected) <= EPS,
                f"degradation: 2036 dispatch {d36[t]:.3f} ≠ {100*factor_expected:.3f}")


# ─────────────────────────────────────────────────────────────────────
# C. Temperature derating on solar (mechanism shared with thermal)
# ─────────────────────────────────────────────────────────────────────
def check_temp_derating_on_solar(wd: Path):
    # Curve [[25,1.0],[45,0.9]] linearly interpolated at 35°C → 0.95.
    # Temp profile [25, 25, 35, 35, 25, 25]. With profile cf=1.0 and
    # pmax=100: dispatch ≤ [100, 100, 95, 95, 100, 100].
    a = _solar(extra={"temp_derating_curve": [[25, 1.0], [45, 0.9]],
                      "temp_profile_key": "amb_temp"})
    inp = _input(6, [200.0] * 6, [a, _backup()],
                 profiles={"solar_cf": [1.0] * 6,
                           "amb_temp": [25, 25, 35, 35, 25, 25]})
    res = _run(inp, wd)
    disp = _dispatch(res, "S1")
    expect = [100.0, 100.0, 95.0, 95.0, 100.0, 100.0]
    for t, (d, e) in enumerate(zip(disp, expect)):
        _assert(abs(d - e) <= EPS,
                f"temp_solar: t={t} dispatch {d:.2f} ≠ {e}")


# ─────────────────────────────────────────────────────────────────────
# D. Backward compatibility
# ─────────────────────────────────────────────────────────────────────
def check_backward_compat(wd: Path):
    a = _solar()       # no new fields
    inp = _input(3, [80.0] * 3, [a, _backup()],
                 profiles={"solar_cf": [0.5, 0.7, 0.9]})
    res = _run(inp, wd)
    disp = _dispatch(res, "S1")
    expect = [50.0, 70.0, 80.0]   # unchanged from pre-Stage-4 behavior
    for t, (d, e) in enumerate(zip(disp, expect)):
        _assert(abs(d - e) <= EPS,
                f"back-compat: t={t} dispatch {d:.2f} ≠ {e}")


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP solar Stage 4 test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("dc_ac_ratio_clipping",           check_dc_ac_ratio_clipping),
        ("inverter_efficiency",            check_inverter_efficiency),
        ("multi_year_degradation",         check_degradation_rate),
        ("temp_derating_on_solar",         check_temp_derating_on_solar),
        ("backward_compat_no_new_fields",  check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — Solar / VRE Stage 4 test")
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
        print(f"\n❌ {failed}/{len(checks)} solar Stage 4 checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} solar Stage 4 checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
