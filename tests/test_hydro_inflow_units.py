#!/usr/bin/env python3
"""
PowerSim — deterministic tests for Hydro Stage 1.

Covers:
  A. m³/s → Mm³/h unit normalization (helper + solver round-trip).
  B. spill_cost_usd_per_mm3 reduces optimal spill.
  C. cascade_flow_mode='release_plus_spill' delivers upstream bypass
     to the downstream reservoir; default 'turbined_only' does not.
  D. Backward compatibility: no new hydro fields ⇒ unchanged behavior.

Exits 0 on success. Suitable for CI alongside the other deterministic
hydro / BESS / maintenance regression tests.
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


# ─────────────────────────────────────────────────────────────────────
# Inputs
# ─────────────────────────────────────────────────────────────────────
def _backup(pmax=500, mc_high=False) -> dict:
    """Backup thermal so the solver can always meet demand."""
    return {
        "id": "BACK", "name": "Backup", "type": "thermal", "committable": False,
        "pmin": 0, "pmax": pmax,
        "heat_rate": 10, "fuel_type": "gas",
        "fuel_price": 50 if mc_high else 5,
        "ramp_up": pmax, "ramp_down": pmax, "min_up": 1, "min_down": 1, "vom": 0,
    }


def _hydro(aid="H1", pmax=100, eff=300,
           reservoir_min=0, reservoir_max=200, reservoir_init=100,
           inflow_profile="hydro_inflow",
           hydro_extra=None) -> dict:
    h = {
        "efficiency": eff,
        "reservoir_min": reservoir_min,
        "reservoir_max": reservoir_max,
        "reservoir_init": reservoir_init,
        "reservoir_end_min": 0,
    }
    if hydro_extra:
        h.update(hydro_extra)
    return {
        "id": aid, "name": aid, "type": "hydro_reg", "committable": False,
        "pmin": 0, "pmax": pmax,
        "ramp_up": pmax, "ramp_down": pmax,
        "min_up": 1, "min_down": 1, "vom": 0,
        "inflow_profile": inflow_profile,
        "hydro": h,
    }


def _input(horizon_h, demand, profiles, assets, *,
           hydro_inflow_unit="raw") -> dict:
    return {
        "metadata": {"schema_version": "1.5", "model_version": "hydro-stage1-test"},
        "study_horizon": {"start_hour": 0, "horizon_hours": horizon_h, "mode": "full"},
        "resolution_min": 60,
        "profiles": {
            "demand": (demand + [0.0] * (8760 - len(demand)))[:8760],
            **profiles,
        },
        "profile_bundle": {"hydro_inflow_unit": hydro_inflow_unit},
        "assets": assets,
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": horizon_h, "rolling_step_h": horizon_h,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }


def _run(inp: dict, wd: Path) -> dict:
    inp_path = wd / "in.json"
    out_path = wd / "out.json"
    xls_path = wd / "out.xlsx"
    inp_path.write_text(json.dumps(inp), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(SOLVER),
         "--input", str(inp_path), "--output", str(out_path),
         "--excel", str(xls_path)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-2000:])
        sys.stderr.write(proc.stderr[-2000:])
        raise RuntimeError(f"solver rc={proc.returncode}")
    return json.loads(out_path.read_text(encoding="utf-8"))


# ─────────────────────────────────────────────────────────────────────
# A. m³/s conversion
# ─────────────────────────────────────────────────────────────────────
def check_m3_per_s_helper():
    """Pure helper — 100 m³/s = 0.36 Mm³/h."""
    from powersim_dataio import normalize_hydro_inflow_rate
    out = normalize_hydro_inflow_rate(100, "m3_per_s")
    _assert(abs(out - 0.36) < 1e-9, f"helper: 100 m³/s → {out} (expected 0.36)")
    _assert(normalize_hydro_inflow_rate(42, "Mm3_per_h") == 42, "Mm3_per_h is unchanged")
    _assert(normalize_hydro_inflow_rate(42, "raw") == 42, "raw is unchanged")
    # normalized w/ inflow_scale_mm3h
    asset = {"hydro": {"inflow_scale_mm3h": 0.5}}
    out2 = normalize_hydro_inflow_rate(2.0, "normalized", asset=asset)
    _assert(abs(out2 - 1.0) < 1e-9, f"normalized·scale: 2·0.5={out2}")
    # normalized w/ annual_inflow_mm3
    asset2 = {"hydro": {"annual_inflow_mm3": 8760.0}}
    out3 = normalize_hydro_inflow_rate(1.0, "normalized", asset=asset2)
    _assert(abs(out3 - 1.0) < 1e-9, f"normalized·annual: 1·(8760/8760)={out3}")


def check_m3_per_s_solver_roundtrip(wd: Path):
    """Full solver run: 100 m³/s inflow → output inflow_mm3h = 0.36."""
    inp = _input(
        horizon_h=4,
        demand=[10.0] * 4,
        profiles={"hydro_inflow": [100.0] * 8760},
        assets=[
            _hydro(reservoir_init=50, reservoir_max=200, pmax=100),
            _backup(),
        ],
        hydro_inflow_unit="m3_per_s",
    )
    res = _run(inp, wd)
    rows = res["hourly_by_unit"]["H1"]
    for t, r in enumerate(rows):
        infl = float(r["hydro"]["inflow_mm3h"])
        _assert(abs(infl - 0.36) < 5e-3,
                f"solver: t={t} inflow_mm3h {infl} ≠ 0.36 (m³/s normalization)")
    diag = res["diagnostics"]
    _assert(diag.get("hydro_inflow_unit_used") == "m3_per_s",
            f"diag.hydro_inflow_unit_used={diag.get('hydro_inflow_unit_used')}")
    _assert(diag.get("hydro_inflow_conversion_applied") is True,
            "diag.hydro_inflow_conversion_applied should be True")
    _assert(diag.get("hydro_raw_unit_warning") is False,
            "diag.hydro_raw_unit_warning should be False for m³/s")


# ─────────────────────────────────────────────────────────────────────
# B. Spill cost
# ─────────────────────────────────────────────────────────────────────
def _spill_case(wd, spill_cost):
    # Tiny reservoir, big inflow ⇒ spill is unavoidable if not turbined.
    # With backup MC > water_value/$0, turbining is cheaper than backup.
    # With spill_cost > 0, turbining is even MORE attractive vs spilling.
    assets = [
        _hydro(
            pmax=5, eff=200,                  # very small turbine: pmax=5 MW
            reservoir_min=0, reservoir_max=10, reservoir_init=10,
            hydro_extra={"spill_cost_usd_per_mm3": spill_cost,
                         "reservoir_end_min": 0},
        ),
        _backup(pmax=500, mc_high=True),     # backup ~$474/MWh
    ]
    inp = _input(
        horizon_h=4,
        demand=[100.0] * 4,                  # demand far exceeds hydro capacity
        profiles={"hydro_inflow": [2.0] * 8760},  # large inflow Mm³/h
        assets=assets,
    )
    res = _run(inp, wd)
    return float(res["by_unit_summary"]["H1"]["total_spill_mm3"])


def check_spill_cost_effect(wd: Path):
    spill_free = _spill_case(wd, spill_cost=0)
    spill_pen  = _spill_case(wd, spill_cost=10_000)
    _assert(spill_pen <= spill_free + 1e-6,
            f"spill cost: penalized run spilled {spill_pen} > free run {spill_free}")
    # Spill should genuinely happen in the no-cost case (reservoir tiny + huge inflow).
    _assert(spill_free > 0.1,
            f"spill cost: free-run spill {spill_free} ≈ 0 — test setup not exercising spill")


# ─────────────────────────────────────────────────────────────────────
# C. Cascade flow mode
# ─────────────────────────────────────────────────────────────────────
def _cascade_case(wd, downstream_mode):
    # Upstream: tiny turbine (5 MW), big inflow → must spill.
    # Downstream: no own inflow, sees only upstream water via cascade.
    upstream = _hydro(
        aid="UP", pmax=5, eff=200,
        reservoir_min=0, reservoir_max=10, reservoir_init=5,
        inflow_profile="hydro_inflow",
        hydro_extra={"reservoir_end_min": 0},
    )
    downstream = _hydro(
        aid="DN", pmax=100, eff=200,
        reservoir_min=0, reservoir_max=100, reservoir_init=10,
        inflow_profile="zero_inflow",
        hydro_extra={
            "cascade_upstream": "UP",
            "cascade_travel_delay_h": 0,
            "cascade_gain": 1.0,
            "cascade_flow_mode": downstream_mode,
            "reservoir_end_min": 0,
        },
    )
    inp = _input(
        horizon_h=4,
        demand=[100.0] * 4,
        profiles={
            "hydro_inflow": [2.0] * 8760,
            "zero_inflow":  [0.0] * 8760,
        },
        assets=[upstream, downstream, _backup(pmax=500, mc_high=True)],
    )
    res = _run(inp, wd)
    bu = res["by_unit_summary"]
    dn_energy = float(bu["DN"]["energy_mwh"])
    last_stor = float(res["hourly_by_unit"]["DN"][-1]["hydro"]["storage_mm3"])
    return dn_energy, last_stor


def check_cascade_modes(wd: Path):
    e_turb, s_turb = _cascade_case(wd, "turbined_only")
    e_both, s_both = _cascade_case(wd, "release_plus_spill")
    # release_plus_spill must deliver ≥ as much downstream water → ≥ energy or storage.
    _assert(e_both + s_both >= e_turb + s_turb - 1e-3,
            f"cascade mode: release_plus_spill (energy={e_both}, stor={s_both}) "
            f"should be ≥ turbined_only (energy={e_turb}, stor={s_turb})")
    # And strictly greater when upstream actually spills.
    _assert((e_both + s_both) > (e_turb + s_turb) + 0.05,
            f"cascade mode: release_plus_spill expected to add upstream spill, "
            f"but totals are turbined_only=(E={e_turb}, S={s_turb}) "
            f"vs release_plus_spill=(E={e_both}, S={s_both})")


# ─────────────────────────────────────────────────────────────────────
# D. Backward compatibility
# ─────────────────────────────────────────────────────────────────────
def check_backward_compat(wd: Path):
    # No spill_cost, no cascade_flow_mode, default raw unit.
    inp = _input(
        horizon_h=4,
        demand=[10.0] * 4,
        profiles={"hydro_inflow": [0.1] * 8760},
        assets=[_hydro(reservoir_init=50, reservoir_max=200, pmax=20), _backup()],
    )
    res = _run(inp, wd)
    diag = res["diagnostics"]
    _assert(diag.get("hydro_raw_unit_warning") is True,
            "raw unit should set raw_unit_warning=True")
    _assert(diag.get("hydro_inflow_conversion_applied") is False,
            "raw unit should NOT report conversion_applied=True")
    # Per-unit hydro totals should be present.
    bu = res["by_unit_summary"]["H1"]
    _assert("total_spill_mm3" in bu and "total_hydro_release_mm3" in bu,
            "hydro_reg should always have spill/release totals")


# ─────────────────────────────────────────────────────────────────────
# Driver
# ─────────────────────────────────────────────────────────────────────
def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP hydro Stage 1 test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("m3_per_s_helper",                       lambda _: check_m3_per_s_helper()),
        ("m3_per_s_solver_roundtrip",             check_m3_per_s_solver_roundtrip),
        ("spill_cost_reduces_spill",              check_spill_cost_effect),
        ("cascade_flow_mode_release_plus_spill",  check_cascade_modes),
        ("backward_compat_no_new_fields",         check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — Hydro Stage 1 test")
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
        print(f"\n❌ {failed}/{len(checks)} hydro Stage 1 checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} hydro Stage 1 checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
