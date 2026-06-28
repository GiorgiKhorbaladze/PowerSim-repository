#!/usr/bin/env python3
"""
PowerSim — deterministic tests for Hydro Stage 2.

Covers three optional fields added to ``hydro.*``:

  A. min_release_mm3h       — mandatory environmental / sanitary flow
                              (release + spill ≥ min every period)
  B. storage_targets        — monthly-end reservoir level soft-penalty
                              that forces the LP to keep water on hand
  C. head_efficiency_curve  — turbine MWh/Mm³ as a function of starting
                              storage (window-level scalar). Same demand,
                              higher head ⇒ less water per MWh.
  D. backward compatibility — none of these set ⇒ identical behavior.

Exits 0 on success. Wired into CI alongside Stage 1.
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


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def _backup(pmax=500, mc_high=False):
    return {
        "id": "BACK", "name": "Backup", "type": "thermal", "committable": False,
        "pmin": 0, "pmax": pmax,
        "heat_rate": 10, "fuel_type": "gas",
        "fuel_price": 200 if mc_high else 5,
        "ramp_up": pmax, "ramp_down": pmax, "min_up": 1, "min_down": 1, "vom": 0,
    }


def _hydro(aid="H1", pmax=100, hydro_extra=None,
           inflow_profile="hydro_inflow",
           reservoir_init=500, reservoir_max=1000):
    h = {
        "efficiency": 350,
        "reservoir_min": 0,
        "reservoir_max": reservoir_max,
        "reservoir_init": reservoir_init,
        "reservoir_end_min": 0,
    }
    if hydro_extra:
        h.update(hydro_extra)
    return {
        "id": aid, "name": aid, "type": "hydro_reg", "committable": False,
        "pmin": 0, "pmax": pmax,
        "ramp_up": pmax, "ramp_down": pmax, "min_up": 1, "min_down": 1, "vom": 0,
        "inflow_profile": inflow_profile,
        "hydro": h,
    }


def _input(horizon_h, demand, profiles, assets):
    return {
        "metadata": {"schema_version": "1.5", "model_version": "hydro-stage2-test"},
        "study_horizon": {"start_hour": 0, "horizon_hours": horizon_h, "mode": "full"},
        "resolution_min": 60,
        "profiles": {
            "demand": (demand + [0.0] * (8760 - len(demand)))[:8760],
            **profiles,
        },
        "profile_bundle": {"hydro_inflow_unit": "Mm3_per_h"},
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


# ─────────────────────────────────────────────────────────────────────
# A. Minimum environmental release
# ─────────────────────────────────────────────────────────────────────
def check_min_release(wd: Path):
    # Demand 0, no backup needed. Solver has no incentive to release.
    # Without min_release, both release & spill would stay 0.
    # With min_release_mm3h=0.10 the constraint forces release+spill≥0.10.
    inp = _input(
        horizon_h=6,
        demand=[0.0] * 6,
        profiles={"hydro_inflow": [0.05] * 8760},
        assets=[_hydro(reservoir_init=500, reservoir_max=1000,
                       hydro_extra={"min_release_mm3h": 0.10})],
    )
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["H1"]
    for t, r in enumerate(rows):
        rel = float(r["hydro"]["release_mm3h"])
        spl = float(r["hydro"]["spill_mm3h"])
        _assert(rel + spl >= 0.10 - 1e-4,
                f"min_release: t={t} release+spill={rel+spl:.4f} < 0.10")


# ─────────────────────────────────────────────────────────────────────
# B. Storage target trajectory (single month boundary inside horizon)
# ─────────────────────────────────────────────────────────────────────
def _stor_at(res, gid, t_global):
    """Return storage_mm3 at global hour `t_global` (0-indexed in rows)."""
    return float(res["hourly_by_unit"][gid][t_global]["hydro"]["storage_mm3"])


def _energy(res, gid):
    return float(res["by_unit_summary"][gid]["energy_mwh"])


def check_storage_target(wd: Path):
    # Horizon 800h spans Jan 31 23:00 boundary (hour 744 = month-end of January).
    # Force the LP to drain the reservoir without a target by setting strong
    # demand + expensive backup. With a target, the LP should keep storage
    # high at the month boundary.
    base_assets = lambda extra=None: [
        _hydro(reservoir_init=800, reservoir_max=1000,
               hydro_extra=extra),
        _backup(pmax=500, mc_high=True),       # backup MC ≈ $190/MWh
    ]
    base = _input(
        horizon_h=800,
        demand=[50.0] * 800,
        profiles={"hydro_inflow": [0.02] * 8760},   # very low inflow
        assets=base_assets(),
    )
    res_no_tgt = _run(base, wd)
    stor_at_744_no = _stor_at(res_no_tgt, "H1", 743)   # row 743 = period 744

    # Now add a target for Jan (month_end[0]) of 700 Mm³ with stiff penalty.
    months = [None] * 12
    months[0] = 700.0
    inp2 = _input(
        horizon_h=800,
        demand=[50.0] * 800,
        profiles={"hydro_inflow": [0.02] * 8760},
        assets=base_assets({"storage_targets": {
            "month_end": months, "penalty_usd_per_mm3": 50_000.0}}),
    )
    res_tgt = _run(inp2, wd)
    stor_at_744_tgt = _stor_at(res_tgt, "H1", 743)

    _assert(stor_at_744_no < stor_at_744_tgt - 5.0,
            f"storage target: without target stor@744={stor_at_744_no:.1f}, "
            f"with target stor@744={stor_at_744_tgt:.1f} — target had no effect")
    _assert(stor_at_744_tgt >= 700.0 - 5.0,
            f"storage target: stor@744={stor_at_744_tgt:.1f} < target 700")


# ─────────────────────────────────────────────────────────────────────
# C. Head-dependent efficiency
# ─────────────────────────────────────────────────────────────────────
def check_head_efficiency(wd: Path):
    # Same plant, same demand. Run 1 starts low storage (low head ⇒ low eff).
    # Run 2 starts high storage (high head ⇒ high eff). The total water
    # released for the same MWh generated must differ proportionally.
    curve = [[100, 200], [1000, 400]]   # Mm³ → MWh/Mm³
    common = {"head_efficiency_curve": curve, "reservoir_end_min": 0}
    assets_low = [
        _hydro(reservoir_init=100, reservoir_max=1000, hydro_extra=common),
        _backup(),
    ]
    assets_high = [
        _hydro(reservoir_init=1000, reservoir_max=1000, hydro_extra=common),
        _backup(),
    ]
    inp_low = _input(
        horizon_h=24, demand=[50.0] * 24,
        profiles={"hydro_inflow": [0.0] * 8760},
        assets=assets_low,
    )
    inp_high = _input(
        horizon_h=24, demand=[50.0] * 24,
        profiles={"hydro_inflow": [0.0] * 8760},
        assets=assets_high,
    )
    rl = _run(inp_low, wd)
    rh = _run(inp_high, wd)
    rel_low  = float(rl["by_unit_summary"]["H1"]["total_hydro_release_mm3"])
    rel_high = float(rh["by_unit_summary"]["H1"]["total_hydro_release_mm3"])
    en_low   = _energy(rl, "H1")
    en_high  = _energy(rh, "H1")
    # Both should generate the same energy (demand-driven). Low-head plant
    # needs about 2× the water for the same MWh.
    _assert(abs(en_low - en_high) < 1.0,
            f"head_eff: energy differs ({en_low} vs {en_high})")
    _assert(rel_low > 1.5 * rel_high,
            f"head_eff: low-head release {rel_low:.3f} not >> high-head {rel_high:.3f}")
    # Sanity — ratio close to 400/200 = 2.0
    ratio = rel_low / max(rel_high, 1e-6)
    _assert(1.7 <= ratio <= 2.3,
            f"head_eff: release ratio {ratio:.3f} ≠ ~2.0")


# ─────────────────────────────────────────────────────────────────────
# D. Backward compatibility
# ─────────────────────────────────────────────────────────────────────
def check_backward_compat(wd: Path):
    inp = _input(
        horizon_h=6, demand=[20.0] * 6,
        profiles={"hydro_inflow": [0.10] * 8760},
        assets=[_hydro(reservoir_init=500, reservoir_max=1000), _backup()],
    )
    res = _run(inp, wd)
    # No new fields ⇒ output schema unchanged, run feasible, no stor target
    # shortfall artefacts in diagnostics block.
    _assert("total_spill_mm3" in res["by_unit_summary"]["H1"],
            "backward compat: total_spill_mm3 still emitted (Stage 1)")
    _assert(res["diagnostics"].get("hydro_inflow_conversion_applied") is False,
            "backward compat: Mm3_per_h declared ⇒ no conversion applied")


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP hydro Stage 2 test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("min_release_environmental_flow",      check_min_release),
        ("storage_target_month_end",            check_storage_target),
        ("head_dependent_efficiency",           check_head_efficiency),
        ("backward_compat_no_new_fields",       check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — Hydro Stage 2 test")
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
        print(f"\n❌ {failed}/{len(checks)} hydro Stage 2 checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} hydro Stage 2 checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
