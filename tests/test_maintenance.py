#!/usr/bin/env python3
"""
PowerSim — deterministic maintenance-calendar enforcement test.

Proves that a top-level ``maintenance`` event (and the inline per-asset
``maint_windows`` form) derates an asset's available capacity over its
[start_h, end_h) window inside the solver.

Scenario: single non-committable thermal unit, pmax=100 MW, flat demand
80 MW. A maintenance window caps the unit to 40 MW for hours 6..11.

Expected:
  • hours  0..5  and 12..23 → dispatch == 80 MW (full demand served)
  • hours  6..11           → dispatch == 40 MW (capped; 40 MW unserved)

Backward-compat: an input without any maintenance key dispatches 80 MW for
the whole horizon (the stub used to always return factor 1.0).

Exits 0 on success, non-zero on any failure. Suitable for CI.
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

EPS = 1e-3


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _base_input() -> dict:
    return {
        "metadata": {"schema_version": "1.5", "model_version": "maint-test"},
        "study_horizon": {"start_hour": 0, "horizon_hours": 24, "mode": "full"},
        "resolution_min": 60,
        "profiles": {"demand": [80.0] * 8760},
        "assets": [
            {
                "id": "T1", "name": "Thermal 1", "type": "thermal",
                "committable": False,
                "pmin": 0, "pmax": 100,
                "heat_rate": 7.0, "fuel_type": "gas", "fuel_price": 5.0,
                "ramp_up": 100, "ramp_down": 100, "min_up": 1, "min_down": 1,
                "vom": 1,
            }
        ],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.01, "time_limit_s": 60,
            "rolling_window_h": 24, "rolling_step_h": 24,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }


def _run_solver(inp: dict, workdir: Path) -> dict:
    in_path = workdir / "in.json"
    out_path = workdir / "out.json"
    xls_path = workdir / "out.xlsx"
    in_path.write_text(json.dumps(inp), encoding="utf-8")
    proc = subprocess.run(
        [sys.executable, str(SOLVER),
         "--input", str(in_path), "--output", str(out_path),
         "--excel", str(xls_path)],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-2000:])
        sys.stderr.write(proc.stderr[-2000:])
        raise RuntimeError(f"solver exited rc={proc.returncode}")
    return json.loads(out_path.read_text(encoding="utf-8"))


def _dispatch_by_hour(res: dict, gid: str) -> dict[int, float]:
    rows = res["hourly_by_unit"][gid]
    return {int(r["t"]): float(r["dispatch_mw"]) for r in rows}


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def check_capacity_cap(workdir: Path) -> None:
    inp = _base_input()
    inp["maintenance"] = [{
        "asset_id": "T1", "start_h": 6, "end_h": 12,
        "event_type": "partial_derating", "available_capacity_mw": 40,
    }]
    disp = _dispatch_by_hour(_run_solver(inp, workdir), "T1")
    for h in range(24):
        expect = 40.0 if 6 <= h < 12 else 80.0
        _assert(abs(disp[h] - expect) <= EPS,
                f"capacity-cap: hour {h} dispatch {disp[h]:.3f} ≠ {expect}")


def check_availability_factor(workdir: Path) -> None:
    inp = _base_input()
    inp["maintenance"] = [{
        "asset_id": "T1", "start_h": 6, "end_h": 12,
        "event_type": "partial_derating", "availability_factor": 0.5,
    }]
    disp = _dispatch_by_hour(_run_solver(inp, workdir), "T1")
    for h in range(24):
        expect = 50.0 if 6 <= h < 12 else 80.0   # 0.5 × pmax(100)
        _assert(abs(disp[h] - expect) <= EPS,
                f"factor: hour {h} dispatch {disp[h]:.3f} ≠ {expect}")


def check_full_outage(workdir: Path) -> None:
    inp = _base_input()
    # neither factor nor capacity ⇒ full outage
    inp["maintenance"] = [{"asset_id": "T1", "start_h": 6, "end_h": 12,
                           "event_type": "forced_outage"}]
    disp = _dispatch_by_hour(_run_solver(inp, workdir), "T1")
    for h in range(6, 12):
        _assert(disp[h] <= EPS, f"full-outage: hour {h} dispatch {disp[h]:.3f} ≠ 0")
    _assert(abs(disp[0] - 80.0) <= EPS, "full-outage: hour 0 should be 80")


def check_inline_maint_windows(workdir: Path) -> None:
    inp = _base_input()
    inp["assets"][0]["maint_windows"] = [
        {"start_h": 6, "end_h": 12, "available_capacity_mw": 40}
    ]
    disp = _dispatch_by_hour(_run_solver(inp, workdir), "T1")
    for h in range(6, 12):
        _assert(abs(disp[h] - 40.0) <= EPS,
                f"inline-window: hour {h} dispatch {disp[h]:.3f} ≠ 40")


def check_backward_compat(workdir: Path) -> None:
    inp = _base_input()  # no maintenance at all
    disp = _dispatch_by_hour(_run_solver(inp, workdir), "T1")
    for h in range(24):
        _assert(abs(disp[h] - 80.0) <= EPS,
                f"backcompat: hour {h} dispatch {disp[h]:.3f} ≠ 80")


def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP maintenance test — missing deps: " + ", ".join(missing))
        return 0

    checks = [
        ("capacity_cap_40mw",        check_capacity_cap),
        ("availability_factor_0.5",  check_availability_factor),
        ("full_outage",              check_full_outage),
        ("inline_maint_windows",     check_inline_maint_windows),
        ("backward_compat_nomaint",  check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — maintenance enforcement test")
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
        print(f"\n❌ {failed}/{len(checks)} maintenance checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} maintenance checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
