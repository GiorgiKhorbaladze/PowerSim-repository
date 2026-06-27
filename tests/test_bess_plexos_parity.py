#!/usr/bin/env python3
"""
PowerSim — deterministic tests for v1.5 BESS PLEXOS-parity additions.

Proves each new optional field actually changes solver behavior:

  • self_discharge_pct_per_h   — SOC decays per hour when idle.
  • aux_mw                     — parasitic draw subtracted from SOC.
  • charge_power_mw /
    discharge_power_mw         — asymmetric inverter sizing capped.
  • c_rate_max                 — P/E ratio tightens ch/dis power.
  • ramp_up_mw_min /
    ramp_down_mw_min           — inverter MW-per-period delta cap.
  • soc_end_target +
    soc_end_penalty_usd_mwh    — soft end-of-horizon SOC target.
  • bess_equivalent_cycles     — throughput / (2·energy_mwh).
  • bess_cycle/calendar_used_pct — aging metrics in by_unit_summary.

Each scenario uses a minimal one-BESS + one-backup-thermal system so the
expected post-dispatch state is hand-verifiable. Exits 0 on success.
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

EPS = 5e-2   # MW/MWh — HiGHS LP slack tolerance


def _missing_deps() -> list[str]:
    required = ["pyomo", "highspy", "numpy", "pandas"]
    return [m for m in required if importlib.util.find_spec(m) is None]


def _bess(**over) -> dict:
    a = {
        "id": "B1", "name": "BESS 1", "type": "bess",
        "committable": False,
        "power_mw": 100, "energy_mwh": 100,
        "soc_init": 1.0, "soc_min": 0.0, "soc_max": 1.0,
        "eta_charge": 1.0, "eta_discharge": 1.0,
        "vom_discharge": 0,
    }
    a.update(over)
    return a


def _thermal_backup() -> dict:
    # High-MC backup so the solver prefers BESS discharge first.
    return {
        "id": "BACK", "name": "Backup", "type": "thermal", "committable": False,
        "pmin": 0, "pmax": 500,
        "heat_rate": 10, "fuel_type": "gas", "fuel_price": 50,
        "ramp_up": 500, "ramp_down": 500, "min_up": 1, "min_down": 1, "vom": 0,
    }


def _base_input(demand, horizon_h, bess_override) -> dict:
    return {
        "metadata": {"schema_version": "1.5", "model_version": "bess-parity-test"},
        "study_horizon": {"start_hour": 0, "horizon_hours": horizon_h, "mode": "full"},
        "resolution_min": 60,
        "profiles": {"demand": demand + [0.0] * (8760 - len(demand))},
        "assets": [_bess(**bess_override), _thermal_backup()],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": horizon_h, "rolling_step_h": horizon_h,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }


def _run(inp: dict, workdir: Path) -> dict:
    inp_path = workdir / "in.json"
    out_path = workdir / "out.json"
    xls_path = workdir / "out.xlsx"
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


def _hbu(res, gid):
    return res["hourly_by_unit"][gid]


def _assert(cond, msg):
    if not cond:
        raise AssertionError(msg)


# ── Individual checks ─────────────────────────────────────────────────

def check_self_discharge(wd: Path) -> None:
    # Demand=0 ⇒ ch=dis=0; SOC decays purely by self-discharge.
    # sd = 10%/h, soc_init=1.0, energy=100 → SOC = 100·0.9^t.
    inp = _base_input([0.0] * 6, 6, {"self_discharge_pct_per_h": 10.0})
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    expected = [100.0 * (0.9 ** (t + 1)) for t in range(6)]
    for t, r in enumerate(rows):
        got = float(r["bess"]["soc_mwh"])
        _assert(abs(got - expected[t]) <= EPS,
                f"self_discharge: t={t} soc {got:.3f} ≠ {expected[t]:.3f}")


def check_aux_load(wd: Path) -> None:
    # Demand=0, no self-discharge, aux=2 MW ⇒ SOC drops 2 MWh per hour.
    inp = _base_input([0.0] * 5, 5, {"aux_mw": 2.0})
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    for t, r in enumerate(rows):
        expect = 100.0 - 2.0 * (t + 1)
        got = float(r["bess"]["soc_mwh"])
        _assert(abs(got - expect) <= EPS,
                f"aux_load: t={t} soc {got:.2f} ≠ {expect}")


def check_asymmetric_power(wd: Path) -> None:
    # power_mw=100 baseline, but charge_power_mw=20, discharge_power_mw=80.
    # Demand spikes high so BESS discharges at its cap.
    inp = _base_input([200.0] * 3, 3, {
        "charge_power_mw": 20, "discharge_power_mw": 80,
    })
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    for t, r in enumerate(rows):
        dis = float(r["bess"]["discharge_mw"])
        ch  = float(r["bess"]["charge_mw"])
        _assert(dis <= 80 + EPS, f"asym: t={t} dis {dis} > 80 cap")
        _assert(ch  <= 20 + EPS, f"asym: t={t} ch  {ch} > 20 cap")


def check_c_rate(wd: Path) -> None:
    # power_mw=100 but c_rate_max=0.5 with energy=100 → effective cap=50 MW.
    inp = _base_input([200.0] * 2, 2, {"c_rate_max": 0.5})
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    for t, r in enumerate(rows):
        dis = float(r["bess"]["discharge_mw"])
        _assert(dis <= 50 + EPS, f"c_rate: t={t} dis {dis} > 50 cap")


def check_ramp_rate(wd: Path) -> None:
    # power_mw=100, ramp_down_mw_min=0.5 → 30 MW/h delta cap.
    # Demand: [100, 0, 0, 0]. Hour 0: dis=100; hour 1: must be ≥ 70 (ramp).
    inp = _base_input([100, 0, 0, 0], 4, {"ramp_down_mw_min": 0.5})
    # Need enough thermal to absorb? No — backup just supplies. With demand=0
    # the BESS doesn't have to discharge but solver minimizes cost; with
    # zero MC backup is free, so BESS may just hold. Force discharge by
    # giving backup a high MC (already done) AND tighten: use a strict
    # ramp_down only on BESS dispatch when demand drops.
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    dis0 = float(rows[0]["bess"]["discharge_mw"])
    dis1 = float(rows[1]["bess"]["discharge_mw"])
    if dis0 > 30:    # only meaningful when hour-0 dispatch is non-trivial
        _assert(dis0 - dis1 <= 30 + EPS,
                f"ramp_dn: dis dropped {dis0}→{dis1} (>{30} MW/h cap)")


def check_end_soc_target(wd: Path) -> None:
    # Target 50% SOC at horizon end with a $1000/MWh penalty.
    # Demand large enough that without the target, BESS would empty.
    # With target, BESS retains ≥ 50 MWh.
    inp = _base_input([200.0] * 3, 3, {
        "soc_end_target": 0.5,
        "soc_end_penalty_usd_mwh": 1000.0,
    })
    res = _run(inp, wd)
    rows = _hbu(res, "B1")
    final_soc = float(rows[-1]["bess"]["soc_mwh"])
    _assert(final_soc >= 50.0 - EPS,
            f"end_soc_target: final SOC {final_soc:.2f} < 50 target")


def check_cycle_counter_output(wd: Path) -> None:
    # Demand profile that forces ~50 MWh round-trip in 4 hours:
    #   demand = [0, 0, 50, 50] → BESS could charge then discharge (but
    # demand of 50 in last hours could be served by backup). Simpler: use
    # a price differential by giving backup high MC AND letting BESS
    # arbitrage via end-target requiring 100 MWh return.  Easiest deterministic:
    # set soc_init=0 then check 100 MWh end-target charges 100 MWh.
    inp = _base_input([0.0] * 6, 6, {
        "soc_init": 0.0, "soc_end_target": 1.0,
        "soc_end_penalty_usd_mwh": 5000.0,
    })
    res = _run(inp, wd)
    bu = res["by_unit_summary"]["B1"]
    _assert("bess_equivalent_cycles" in bu, "missing bess_equivalent_cycles")
    _assert("bess_throughput_mwh" in bu, "missing bess_throughput_mwh")
    # Throughput should equal the charge required (~100 MWh).
    tput = float(bu["bess_throughput_mwh"])
    _assert(tput >= 99.0, f"cycle: throughput {tput} < 99")
    cycles = float(bu["bess_equivalent_cycles"])
    _assert(abs(cycles - tput / 200.0) <= 1e-3,
            f"cycle: cycles {cycles} ≠ throughput/(2·energy) = {tput/200}")


def check_aging_metrics_output(wd: Path) -> None:
    # Calendar life used: 6h / (10·8760) * 100 ≈ 0.00685%.
    # Cycle life used: 0 / 1000 = 0% (no dispatch).
    inp = _base_input([0.0] * 6, 6, {
        "lifetime_full_cycles": 1000,
        "calendar_life_years": 10,
    })
    res = _run(inp, wd)
    bu = res["by_unit_summary"]["B1"]
    cal = bu.get("bess_calendar_used_pct")
    _assert(cal is not None, "missing bess_calendar_used_pct")
    expect_cal = 6.0 / (10.0 * 8760.0) * 100
    _assert(abs(cal - expect_cal) <= 1e-3,
            f"aging: calendar used {cal} ≠ {expect_cal:.5f}")


def check_backward_compat(wd: Path) -> None:
    # No new fields ⇒ unchanged classic BESS behavior; output omits aging fields.
    inp = _base_input([0.0] * 3, 3, {})
    res = _run(inp, wd)
    bu = res["by_unit_summary"]["B1"]
    _assert("bess_throughput_mwh" in bu, "throughput always emitted")
    _assert(bu.get("bess_cycle_life_used_pct") is None,
            "cycle life pct should be None when life not set")
    _assert(bu.get("bess_calendar_used_pct") is None,
            "calendar pct should be None when life not set")


def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP BESS parity test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("self_discharge_decay",     check_self_discharge),
        ("aux_load_drain",           check_aux_load),
        ("asymmetric_charge_dis",    check_asymmetric_power),
        ("c_rate_max_cap",           check_c_rate),
        ("ramp_rate_delta_cap",      check_ramp_rate),
        ("soc_end_target_soft_pen",  check_end_soc_target),
        ("cycle_counter_output",     check_cycle_counter_output),
        ("aging_metrics_output",     check_aging_metrics_output),
        ("backward_compat",          check_backward_compat),
    ]
    print("═" * 60)
    print("  PowerSim — BESS PLEXOS-parity test")
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
        print(f"\n❌ {failed}/{len(checks)} BESS parity checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} BESS parity checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
