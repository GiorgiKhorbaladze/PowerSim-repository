#!/usr/bin/env python3
"""
PowerSim — deterministic regression tests for the audit follow-up batch.

Covers four findings surfaced after PR #57:

  B2 (MiniMax audit)  — ``backend/powersim_ai_tools.summarize_results``
      looked up ``total_cost`` / ``avg_lambda`` / ``gas`` etc., but the
      solver emits ``total_cost_usd``, ``avg_lambda_usd_mwh``,
      ``total_gas_mm3``. The summarizer silently returned ``None`` for
      real solver outputs. Fixed by preferring the *_usd suffixed keys
      and normalising ``by_unit_summary`` from dict → list-of-dicts.

  #4 (self audit)     — ``resolve_marginal_prices`` (ED resolve) used
      ``get_pmax_t`` for every asset in ``m.G``. BESS / DR / pumped-hydro
      have no ``pmax`` field, so their upper bound was 0 — the LP had
      to serve their MIP-committed net MW from other assets or via
      unserved, yielding a wrong marginal price. Fixed by dropping
      those types from ``m.G`` and injecting their MIP MW into the
      balance RHS.

  #8 (self audit)     — For a thermal asset that only defines
      ``heat_rate_curve`` (no scalar ``heat_rate``), ``_dispMC``
      collapsed to ``vom`` and ``by_unit.fuel_cost = energy × vom``
      lost the piecewise fuel signal (often orders of magnitude too
      small). Fixed by interpolating each period's dispatch along
      the vertex costs of the curve.

Scenarios
---------
A. **summarize_results reads real solver keys.** Feed the summariser
   a stand-in ``results_json`` matching the solver's actual schema and
   assert ``total_cost``, ``avg_lambda``, ``gas``, ``unserved``, and
   ``curtailment`` come back with the correct numeric values.

B. **ED resolve marginal price stays sane with BESS on the grid.**
   Run a 6-hour horizon with a peak that requires BESS discharge to
   avoid unserved, invoke ``--ed-resolve``, and check the returned
   ``lambda_usd_mwh`` is bounded by the fleet's marginal cost (not
   the unserved_penalty).

C. **HRC-only asset fuel_cost is non-trivial.** Define a thermal
   with only ``heat_rate_curve`` (no scalar ``heat_rate``), force
   full dispatch, and check that ``by_unit_summary[g].fuel_cost``
   is close to ``curve_hr × p × fp × 0.9478 × dt × H`` rather than
   the ~0 that ``energy × vom`` would give.

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


def _run(inp, wd, extra_args=None):
    ip = wd / "i.json"; op = wd / "o.json"; xp = wd / "x.xlsx"
    ip.write_text(json.dumps(inp), encoding="utf-8")
    cmd = [sys.executable, str(SOLVER),
           "--input", str(ip), "--output", str(op), "--excel", str(xp)]
    if extra_args:
        cmd += list(extra_args)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.stdout.write(proc.stdout[-1500:])
        sys.stderr.write(proc.stderr[-1500:])
        raise RuntimeError(f"solver rc={proc.returncode}")
    return json.loads(op.read_text(encoding="utf-8"))


# ── Scenario A: AI summarizer reads real solver keys ────────────────
def check_ai_summarize_uses_real_keys(_wd):
    sys.path.insert(0, str(ROOT))
    try:
        from backend.powersim_ai_tools import summarize_results
    finally:
        sys.path.pop(0)

    # A minimal stand-in mirroring what the actual solver emits.
    results = {
        "system_summary": {
            "total_cost_usd":            123_456.0,
            "total_objective_cost_usd":  120_000.0,
            "avg_lambda_usd_mwh":        42.5,
            "total_gas_mm3":             1.75,
            "total_unserved_mwh":        0.0,
            "total_curtailed_mwh":       12.0,
        },
        "hourly_system": [{"lambda_usd_mwh": 40.0}, {"lambda_usd_mwh": 45.0}],
        "by_unit_summary": {
            "T1": {"type": "thermal", "energy_mwh": 200.0, "gross_cost": 4000.0},
            "B1": {"type": "bess",    "energy_mwh":  20.0, "gross_cost":   80.0},
        },
    }
    out = summarize_results(results)
    _assert(out["total_cost"] == 123_456.0,
            f"summarize_results total_cost wrong: {out['total_cost']}")
    _assert(out["total_objective_cost"] == 120_000.0,
            f"summarize_results total_objective_cost wrong: {out['total_objective_cost']}")
    # lambdas come from the hourly array when non-empty.
    _assert(abs(out["avg_lambda"] - 42.5) < 1e-6,
            f"summarize_results avg_lambda wrong: {out['avg_lambda']}")
    _assert(out["gas"] == 1.75,
            f"summarize_results gas wrong: {out['gas']}")
    _assert(out["unserved"] == 0.0,
            f"summarize_results unserved wrong: {out['unserved']}")
    _assert(out["curtailment"] == 12.0,
            f"summarize_results curtailment wrong: {out['curtailment']}")
    # top_gen should sort by energy_mwh — T1 first (200 > 20).
    _assert(out["top_units_by_generation"][0].get("id") == "T1",
            f"summarize_results top_gen ordering wrong: {out['top_units_by_generation']}")
    # bess filter should surface the BESS.
    _assert(len(out["bess_metrics"]) == 1 and out["bess_metrics"][0]["id"] == "B1",
            f"summarize_results bess_metrics wrong: {out['bess_metrics']}")


# ── Scenario B: ED resolve λ stays sane with BESS active ─────────────
def check_ed_resolve_with_bess(wd):
    solar = [1.0, 1.0, 1.0, 0.0, 0.0, 0.0] + [0.0] * (8760 - 6)
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 6, "mode": "full"},
        "resolution_min": 60,
        "profiles": {
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
    res = _run(inp, wd, extra_args=["--ed-resolve"])
    # Every lambda should be <= thermal MC (~$8 × 8 × 0.9478 ≈ $60/MWh)
    # + some slack, and CERTAINLY less than the unserved penalty (3000).
    lambdas = [float(h.get("lambda_usd_mwh", 0) or 0) for h in res["hourly_system"]]
    _assert(lambdas, "hourly_system rows missing lambdas")
    hi = max(lambdas)
    _assert(hi < 500,
            f"ED resolve returned λ {hi} — suggests balance blew up "
            f"(BESS treated as free gen). All λ: {lambdas}")
    # Must also observe `lp_dual` source on at least one hour.
    sources = {h.get("lambda_source") for h in res["hourly_system"]}
    _assert("lp_dual" in sources,
            f"ED resolve did not tag any hour with lp_dual: {sources}")


# ── Scenario C: HRC-only asset fuel_cost is > 0 and matches curve ────
def check_hrc_only_fuel_cost(wd):
    # 4-hour horizon, thermal driven at ~pmax. No scalar heat_rate — only
    # the piecewise curve. Pre-fix, `_dispMC` would collapse to `vom=0`
    # and fuel_cost per by_unit would be 0.
    inp = {
        "metadata": {"schema_version": "1.5", "study_year": 2030},
        "study_horizon": {"start_hour": 0, "horizon_hours": 4, "mode": "full"},
        "resolution_min": 60,
        "profiles": {"demand": [95.0] * 8760},
        "assets": [
            {
                "id": "T1", "name": "Th HRC", "type": "thermal",
                "committable": True,
                "pmin": 20, "pmax": 100,
                # No scalar heat_rate. Convex curve: 12 GJ/MWh at 20 MW,
                # 9 at 60 MW, 8 at 100 MW.
                "heat_rate_curve": [[20.0, 12.0], [60.0, 9.0], [100.0, 8.0]],
                "fuel_type": "gas", "fuel_price": 6.0,
                "ramp_up": 100, "ramp_down": 100,
                "min_up": 1, "min_down": 1, "vom": 0,
                "startup_cost": 0, "no_load_cost": 0,
            },
        ],
        "gas_constraints": {"mode": "none"},
        "reserve_products": [],
        "solver_settings": {
            "mip_gap": 0.0001, "time_limit_s": 60,
            "rolling_window_h": 4, "rolling_step_h": 4,
            "unserved_penalty": 3000, "solver": "auto",
        },
    }
    res = _run(inp, wd)
    bu = res["by_unit_summary"].get("T1") or {}
    fc = float(bu.get("fuel_cost", 0) or 0)
    _assert(fc > 100,
            f"HRC-only fuel_cost silently ~zero (bug #8 regression): {fc}")
    # Rough sanity check: dispatch ≈ 95 MW × 4 h = 380 MWh; at 95 MW the
    # interpolated heat rate lies between 8 and 9 GJ/MWh; fuel_cost ≈
    # 380 × 8.5 × 6 × 0.9478 ≈ $18k. Allow a wide band.
    _assert(5000 < fc < 40000,
            f"HRC-only fuel_cost out of expected band ($5k–$40k): {fc}")


def main() -> int:
    missing = _missing_deps()
    if missing:
        print("⚠ SKIP audit-followup test — missing deps: " + ", ".join(missing))
        return 0
    checks = [
        ("ai_summarize_uses_real_keys",  check_ai_summarize_uses_real_keys),
        ("ed_resolve_with_bess",         check_ed_resolve_with_bess),
        ("hrc_only_fuel_cost_nonzero",   check_hrc_only_fuel_cost),
    ]
    print("═" * 60)
    print("  PowerSim — audit follow-up batch test")
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
        print(f"\n❌ {failed}/{len(checks)} audit-followup checks FAILED")
        return 1
    print(f"\n✅ all {len(checks)} audit-followup checks PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
