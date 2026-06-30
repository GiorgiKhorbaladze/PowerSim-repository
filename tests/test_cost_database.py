#!/usr/bin/env python3
"""
PowerSim — tests for the PyPSA-Eur cost-database snapshot + helper API.

Validates:
  • the committed snapshot is loadable and well-formed,
  • lookups for the expected canonical technologies + parameters work,
  • the annuity / annualized-CAPEX helpers match a hand-computed result,
  • missing-key paths raise a helpful CostDatabaseError.

Exits 0 on success. Wired into CI alongside the other deterministic tests.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "solver"))

from cost_database import (
    CostDatabaseError, annualized_capex_usd_per_mw_yr, annuity,
    available_technologies, available_years, get, load,
)


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


def check_snapshot_loads():
    snap = load()
    meta = snap.get("meta", {})
    _assert(meta.get("version") == "1.0", "meta.version missing")
    _assert(meta.get("license", "").startswith("CC-BY"),
            f"unexpected license: {meta.get('license')!r}")
    years = available_years()
    _assert(2030 in years, "snapshot missing year 2030")
    _assert(len(years) >= 3, f"expected ≥3 snapshot years; got {years}")


def check_core_technologies_present():
    techs_2030 = set(available_technologies(2030))
    # The bare minimum a PowerSim user is likely to look up.
    must = {"CCGT", "OCGT", "onwind", "solar-utility",
            "battery storage", "battery inverter"}
    missing = must - techs_2030
    _assert(not missing, f"snapshot missing core techs for 2030: {missing}")


def check_canonical_lookups():
    # CCGT 2030: efficiency ~0.57, lifetime ~25 years.
    eff = get("CCGT", 2030, "efficiency")
    life = get("CCGT", 2030, "lifetime")
    _assert(0.4 <= eff <= 0.7, f"CCGT efficiency {eff} outside [0.4, 0.7]")
    _assert(15 <= life <= 40, f"CCGT lifetime {life} outside [15, 40]")
    # OCGT investment should be cheaper per kW than CCGT.
    inv_ccgt = get("CCGT", 2030, "investment")
    inv_ocgt = get("OCGT", 2030, "investment")
    _assert(inv_ocgt < inv_ccgt,
            f"OCGT inv {inv_ocgt} ≥ CCGT inv {inv_ccgt} — implausible")


def check_annuity_math():
    # Closed-form CRF check: $1000 over 10 years at 7% → ≈ 142.38 /yr.
    a = annuity(1000.0, 10.0, discount_rate=0.07)
    _assert(math.isclose(a, 142.3775, rel_tol=1e-4),
            f"annuity(1000, 10, 0.07) = {a:.4f} ≠ 142.3775")
    # Zero rate falls back to straight-line depreciation.
    _assert(math.isclose(annuity(1000.0, 10.0, 0.0), 100.0),
            "zero-rate annuity should equal investment/lifetime")
    _assert(annuity(0, 10) == 0.0 and annuity(1000, 0) == 0.0,
            "zero investment / lifetime should give 0")


def check_annualized_capex_helper():
    # Compare against manual computation for solar-utility 2030.
    inv_eur_per_kw = get("solar-utility", 2030, "investment")
    life            = get("solar-utility", 2030, "lifetime")
    inv_usd_per_mw  = inv_eur_per_kw * 1000.0 * 1.08
    expected        = annuity(inv_usd_per_mw, life, 0.07)
    got = annualized_capex_usd_per_mw_yr("solar-utility", 2030,
                                         eur_to_usd=1.08, discount_rate=0.07)
    _assert(math.isclose(got, expected, rel_tol=1e-9),
            f"annualized helper {got} ≠ expected {expected}")


def check_missing_key_errors():
    raised = False
    try:
        get("nonexistent-tech", 2030)
    except CostDatabaseError as e:
        raised = True
        _assert("nonexistent-tech" in str(e), "error message lacks tech name")
    _assert(raised, "missing tech should raise CostDatabaseError")
    raised = False
    try:
        get("CCGT", 1900)
    except CostDatabaseError:
        raised = True
    _assert(raised, "missing year should raise CostDatabaseError")
    raised = False
    try:
        get("CCGT", 2030, "fake-parameter")
    except CostDatabaseError:
        raised = True
    _assert(raised, "missing parameter should raise CostDatabaseError")


def main() -> int:
    checks = [
        ("snapshot_loads_and_metadata",   check_snapshot_loads),
        ("core_technologies_present",     check_core_technologies_present),
        ("canonical_lookups_in_range",    check_canonical_lookups),
        ("annuity_closed_form",           check_annuity_math),
        ("annualized_capex_helper",       check_annualized_capex_helper),
        ("missing_key_errors_are_clear",  check_missing_key_errors),
    ]
    print("═" * 60)
    print("  PowerSim — Cost database test")
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
        print(f"\n❌ {failed}/{len(checks)} cost-database checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} cost-database checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
