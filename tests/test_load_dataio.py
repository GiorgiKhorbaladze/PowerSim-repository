#!/usr/bin/env python3
"""
PowerSim — deterministic tests for the GSE hourly-load loader.

Covers:
  A. the committed snapshot loads and has all 5 years × 8760 h,
  B. each year's total energy is within 0.1% of the target GWh label,
  C. per-year peaks are monotonically increasing 2026 → 2030 (demand
     growth trajectory in the P50 central scenario),
  D. profiles slot straight into a solver input as ``profiles.demand``
     and pass the schema validator.

Exits 0 on success.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from load_dataio import (
    load_snapshot, load_snapshot_year, snapshot_years,
)


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def check_snapshot_shape():
    snap = load_snapshot()
    _assert(snap["meta"]["unit"] == "MW", "meta.unit must be 'MW'")
    years = snapshot_years()
    _assert(years == [2026, 2027, 2028, 2029, 2030],
            f"unexpected years: {years}")
    for y in years:
        arr = load_snapshot_year(y)
        _assert(len(arr) == 8760,
                f"{y}: {len(arr)} hours ≠ 8760")
        _assert(all(v >= 0 for v in arr), f"{y}: negative demand values")


def check_target_gwh_matches_sum():
    snap = load_snapshot()
    for ys, arr in snap["load_mw_by_year"].items():
        target = snap["meta"]["target_gwh_per_year"].get(ys)
        if target is None:
            continue
        total_gwh = sum(arr) / 1000.0
        rel = abs(total_gwh - target) / target
        _assert(rel < 1e-3,
                f"{ys}: Σ={total_gwh:.1f} GWh differs from target {target} GWh "
                f"by {rel*100:.3f}%")


def check_peak_growth():
    snap = load_snapshot()
    peaks = [snap["meta"]["peak_mw_per_year"][str(y)]
             for y in (2026, 2027, 2028, 2029, 2030)]
    for a, b in zip(peaks, peaks[1:]):
        _assert(b >= a, f"peak decreased: {a} MW → {b} MW")
    # sanity: growth roughly matches Target_GWh growth (~15%).
    ratio = peaks[-1] / peaks[0]
    _assert(1.10 <= ratio <= 1.25,
            f"2030/2026 peak ratio {ratio:.3f} outside expected [1.10, 1.25]")


def check_schema_validation():
    from powersim_schema import validate_input
    demand = load_snapshot_year(2030)
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 24, "mode": "full"},
        "resolution_min": 60,
        "profiles": {"demand": demand},
        "assets": [{
            "id": "T1", "name": "t", "type": "thermal", "committable": False,
            "pmin": 0, "pmax": 5000,
            "heat_rate": 7, "fuel_type": "gas", "fuel_price": 5,
            "ramp_up": 5000, "ramp_down": 5000,
            "min_up": 1, "min_down": 1, "vom": 0,
        }],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {"solver": "auto"},
    }
    ok, errs, _warn = validate_input(inp)
    _assert(ok, f"validate_input failed: {errs}")


def main() -> int:
    checks = [
        ("snapshot_shape_and_5_years",  check_snapshot_shape),
        ("target_gwh_matches_sum",      check_target_gwh_matches_sum),
        ("annual_peak_grows_2026_2030", check_peak_growth),
        ("plugs_into_schema_validator", check_schema_validation),
    ]
    print("═" * 60)
    print("  PowerSim — GSE hourly-load loader test")
    print("═" * 60)
    failed = 0
    for name, fn in checks:
        try:
            fn()
            print(f"  ✓ {name}")
        except Exception as e:
            failed += 1
            print(f"  ✗ {name}\n     {type(e).__name__}: {e}")
    if failed:
        print(f"\n❌ {failed}/{len(checks)} GSE-load checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} GSE-load checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
