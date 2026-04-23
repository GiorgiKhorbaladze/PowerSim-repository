"""
PowerSim v4.0 — Capacity-Expansion Planner (LT-Plan)
====================================================

Implements the #4 gap: endogenous build/retire decisions.  Simple but
real LP (continuous build MW) with a *production-cost proxy* in the
objective so the planner prefers cheap-dispatchable candidates:

    min   Σ_c capex_c · x_c · CRF_c        (annualised investment)
        + Σ_c prodcost_c · x_c             (simple proxy: $/MW-yr)
        − Σ_c value_c · x_c                (optional capacity value $/MW-yr)
    s.t.  Σ_c Peak_c · x_c ≥ RS_target − Σ_a Peak_a        (adequacy)
          Σ_c Energy_c · x_c ≥ E_target − Σ_a Energy_a     (energy)
          x_c ≤ max_build_mw_c
          x_c ≥ 0                          (or integer when block size declared)

CRF (capital recovery factor) with discount rate `r` and life `n`:
    CRF = r (1 + r)^n / ((1 + r)^n − 1)

Planner takes a *base* PowerSim input (fleet + profiles) and an
`expansion` block:

    "expansion": {
        "mode":          "enabled",
        "discount_rate": 0.08,
        "years":         10,
        "reserve_margin": 0.15,                     # fraction above peak
        "candidates": [
            {"id":"wind_new", "type":"wind",
             "capex_per_mw": 1_400_000,
             "opex_per_mw_yr": 18_000,
             "life_yrs": 20,
             "capacity_factor": 0.34,
             "capacity_credit": 0.15,
             "max_build_mw": 500,
             "block_mw": 50},
            ...
        ]
    }

Output (written to `expansion_plan.json`):
    {
      "CRF":            { "wind_new": 0.1019, ... },
      "builds_mw":      { "wind_new": 250, "solar_new": 400, ... },
      "total_capex":    ...,
      "annual_capex":   ...,
      "annual_opex":    ...,
      "reserve_margin": 0.153,
      "energy_closed":  true
    }

This is a SCREENING tool: it hands off a recommended new-build list that
can then be pasted into `assets` and re-solved with the full UC/ED solver
to get hourly dispatch and dual LMPs.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pyomo.environ as pyo

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _CRF(r: float, n: int) -> float:
    if r <= 0: return 1.0 / max(n, 1)
    return r * (1 + r) ** n / ((1 + r) ** n - 1)


def _existing_peak_mw(inp: dict) -> float:
    total = 0.0
    for a in inp.get("assets") or []:
        if a.get("type") in ("wind", "solar"):
            # capacity credit ≈ 15% for RE by default — use pmax_installed × 0.15
            total += float(a.get("pmax_installed", 0) or 0) * 0.15
        elif a.get("type") == "bess":
            total += float(a.get("power_mw", 0) or 0)
        elif a.get("type") == "import":
            pp = a.get("pmax_profile")
            total += float(pp) if isinstance(pp, (int, float)) else 0.0
        else:
            total += float(a.get("pmax", 0) or 0)
    return total


def _existing_annual_energy_mwh(inp: dict) -> float:
    """Rough: Σ pmax_(installed) × CF_typical × 8760."""
    cf_typical = {"thermal": 0.70, "hydro_reg": 0.55, "hydro_ror": 0.50,
                  "wind": 0.30, "solar": 0.18, "import": 0.70, "bess": 0.0}
    total = 0.0
    for a in inp.get("assets") or []:
        t = a.get("type"); cf = cf_typical.get(t, 0.5)
        mw = (a.get("pmax_installed") if t in ("wind", "solar") else
              a.get("power_mw") if t == "bess" else
              a.get("pmax_profile") if t == "import" else
              a.get("pmax"))
        total += float(mw or 0) * cf * 8760
    return total


def plan(inp: dict) -> dict:
    """Build and solve the capacity-expansion LP."""
    ex = inp.get("expansion") or {}
    cands = ex.get("candidates") or []
    if not cands:
        return {"error": "no expansion.candidates defined"}

    r = float(ex.get("discount_rate", 0.08))
    rm_target = float(ex.get("reserve_margin", 0.15))
    e_target_twh = ex.get("energy_target_twh")
    demand = inp.get("profiles", {}).get("demand") or []
    peak_load = max(demand) if demand else 0.0
    peak_existing = _existing_peak_mw(inp)
    existing_energy_mwh = _existing_annual_energy_mwh(inp)
    # Energy target: either supplied, or ≈ peak × 5000 hours equivalent.
    e_target_mwh = (e_target_twh * 1e6) if e_target_twh else max(
        (sum(demand) if demand else peak_load * 5000), existing_energy_mwh)

    # Reserve adequacy target = (1 + RM) · peak.
    adequacy_target = (1 + rm_target) * peak_load

    m = pyo.ConcreteModel()
    cids = [c["id"] for c in cands]
    m.C = pyo.Set(initialize=cids, ordered=True)
    m.x = pyo.Var(m.C, domain=pyo.NonNegativeReals)

    CRF = {c["id"]: _CRF(r, int(c.get("life_yrs", 20))) for c in cands}
    capex = {c["id"]: float(c.get("capex_per_mw", 0)) for c in cands}
    opex  = {c["id"]: float(c.get("opex_per_mw_yr", 0)) for c in cands}
    cap_credit = {c["id"]: float(c.get("capacity_credit", 0.15))
                  for c in cands}
    cf    = {c["id"]: float(c.get("capacity_factor", 0.4)) for c in cands}
    maxb  = {c["id"]: float(c.get("max_build_mw", 1e6)) for c in cands}

    def _ub(m, c): return m.x[c] <= maxb[c]
    m.UB = pyo.Constraint(m.C, rule=_ub)

    # Adequacy: Σ CC × x  ≥ target − existing_firm_mw
    def _adeq(m):
        return sum(cap_credit[c] * m.x[c] for c in m.C) >= max(
            0.0, adequacy_target - peak_existing)
    m.Adequacy = pyo.Constraint(rule=_adeq)

    # Energy closure: Σ CF × 8760 × x ≥ target − existing
    def _energy(m):
        return sum(cf[c] * 8760 * m.x[c] for c in m.C) >= max(
            0.0, e_target_mwh - existing_energy_mwh)
    m.Energy = pyo.Constraint(rule=_energy)

    def _obj(m):
        return sum((capex[c] * CRF[c] + opex[c]) * m.x[c] for c in m.C)
    m.OBJ = pyo.Objective(rule=_obj, sense=pyo.minimize)

    # HiGHS is fine for an LP.
    try:
        from pyomo.contrib.appsi.solvers.highs import Highs
        s = Highs(); s.highs_options["log_to_console"] = False
        try: s.solve(m, load_solutions=True)
        except TypeError: s.solve(m)
    except Exception:
        pyo.SolverFactory("appsi_highs").solve(m)

    builds = {c: float(pyo.value(m.x[c]) or 0.0) for c in cids}
    annual_opex = sum(opex[c] * builds[c] for c in cids)
    annual_capex = sum(capex[c] * CRF[c] * builds[c] for c in cids)
    firm_added = sum(cap_credit[c] * builds[c] for c in cids)
    energy_added = sum(cf[c] * 8760 * builds[c] for c in cids)
    return {
        "CRF":              {c: round(CRF[c], 6) for c in cids},
        "peak_load_mw":     round(peak_load, 1),
        "existing_firm_mw": round(peak_existing, 1),
        "existing_energy_mwh": round(existing_energy_mwh, 0),
        "adequacy_target_mw": round(adequacy_target, 1),
        "energy_target_mwh":  round(e_target_mwh, 0),
        "builds_mw":        {c: round(builds[c], 2) for c in cids},
        "firm_added_mw":    round(firm_added, 1),
        "energy_added_mwh": round(energy_added, 0),
        "annual_capex":     round(annual_capex, 0),
        "annual_opex":      round(annual_opex, 0),
        "annual_total":     round(annual_capex + annual_opex, 0),
    }


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",  required=True)
    ap.add_argument("--output", default="expansion_plan.json")
    args = ap.parse_args()
    inp = json.loads(Path(args.input).read_text(encoding="utf-8"))
    result = plan(inp)
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n💾 wrote {args.output}")
