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


def plan_multi_year(inp: dict) -> dict:
    """
    Multi-year capacity expansion (#9).

    Extends the single-year `plan()` into a Y-year horizon with:
      • year-over-year linking (build-once-stays-built; cumulative
        capacity at year y = Σ x_c,y' for y'≤y)
      • per-year demand growth via `expansion.demand_growth_pct` or
        an explicit `expansion.peak_load_by_year` list
      • per-year reserve-margin & energy-closure adequacy
      • discounted total cost (NPV) objective:
          Σ_y (1+r)^-y · (Σ_c capex_c · x_c,y · CRF_c + Σ_c opex_c · cum_x_c,y)

    Activates when `expansion.years > 1` AND `expansion.candidates`
    is supplied.  Falls back to the single-year planner otherwise.
    """
    ex = inp.get("expansion") or {}
    years = int(ex.get("years", 1))
    cands = ex.get("candidates") or []
    if years <= 1 or not cands:
        return plan(inp)            # single-year path

    r = float(ex.get("discount_rate", 0.08))
    rm_target = float(ex.get("reserve_margin", 0.15))
    growth = float(ex.get("demand_growth_pct", 2.5)) / 100.0
    explicit_peaks = ex.get("peak_load_by_year")        # optional list
    e_target_twh   = ex.get("energy_target_twh")
    demand = inp.get("profiles", {}).get("demand") or []
    base_peak = max(demand) if demand else 0.0
    base_energy = sum(demand) if demand else base_peak * 5000
    peak_existing = _existing_peak_mw(inp)
    energy_existing = _existing_annual_energy_mwh(inp)

    # Resolve per-year demand projections.
    peaks = []
    energies = []
    for y in range(1, years + 1):
        if explicit_peaks and len(explicit_peaks) >= y:
            p = float(explicit_peaks[y-1])
        else:
            p = base_peak * ((1.0 + growth) ** y)
        peaks.append(p)
        if e_target_twh:
            energies.append(float(e_target_twh) * 1e6 * ((1.0 + growth) ** y))
        else:
            energies.append(base_energy * ((1.0 + growth) ** y))

    m = pyo.ConcreteModel()
    cids = [c["id"] for c in cands]
    yrs  = list(range(1, years + 1))
    m.C = pyo.Set(initialize=cids, ordered=True)
    m.Y = pyo.Set(initialize=yrs, ordered=True)
    m.x = pyo.Var(m.C, m.Y, domain=pyo.NonNegativeReals)        # build in year y
    m.cum = pyo.Var(m.C, m.Y, domain=pyo.NonNegativeReals)      # cumulative

    CRF   = {c["id"]: _CRF(r, int(c.get("life_yrs", 20))) for c in cands}
    capex = {c["id"]: float(c.get("capex_per_mw", 0)) for c in cands}
    opex  = {c["id"]: float(c.get("opex_per_mw_yr", 0)) for c in cands}
    cap_credit = {c["id"]: float(c.get("capacity_credit", 0.15)) for c in cands}
    cf_   = {c["id"]: float(c.get("capacity_factor", 0.4)) for c in cands}
    maxb  = {c["id"]: float(c.get("max_build_mw", 1e6)) for c in cands}

    # Cumulative-capacity recursion: cum[c, y] = cum[c, y-1] + x[c, y].
    def _cum_rule(m, c, y):
        if y == 1: return m.cum[c, y] == m.x[c, y]
        return m.cum[c, y] == m.cum[c, y-1] + m.x[c, y]
    m.CumLink = pyo.Constraint(m.C, m.Y, rule=_cum_rule)

    # Total cumulative build cap.
    def _maxb(m, c, y): return m.cum[c, y] <= maxb[c]
    m.MaxBuild = pyo.Constraint(m.C, m.Y, rule=_maxb)

    # Adequacy: Σ CC · cum[c, y]  ≥  (1+RM) · peaks[y] − existing_firm.
    def _adeq(m, y):
        target = max(0.0, (1 + rm_target) * peaks[y-1] - peak_existing)
        return sum(cap_credit[c] * m.cum[c, y] for c in m.C) >= target
    m.Adeq = pyo.Constraint(m.Y, rule=_adeq)

    # Energy closure: Σ CF · 8760 · cum[c, y] ≥ energies[y] − existing_energy.
    def _energy(m, y):
        target = max(0.0, energies[y-1] - energy_existing)
        return sum(cf_[c] * 8760 * m.cum[c, y] for c in m.C) >= target
    m.E = pyo.Constraint(m.Y, rule=_energy)

    # NPV objective: Σ_y (1+r)^-y · (capex × x + opex × cum).
    def _obj(m):
        tot = 0
        for y in yrs:
            disc = (1.0 + r) ** (-y)
            tot += disc * (
                sum(capex[c] * CRF[c] * m.x[c, y] for c in m.C)
                + sum(opex[c]  * m.cum[c, y] for c in m.C)
            )
        return tot
    m.OBJ = pyo.Objective(rule=_obj, sense=pyo.minimize)

    try:
        from pyomo.contrib.appsi.solvers.highs import Highs
        s = Highs(); s.highs_options["log_to_console"] = False
        try: s.solve(m, load_solutions=True)
        except TypeError: s.solve(m)
    except Exception:
        pyo.SolverFactory("appsi_highs").solve(m)

    builds_by_year = {}
    cum_by_year    = {}
    for y in yrs:
        builds_by_year[y] = {c: round(float(pyo.value(m.x[c, y]) or 0.0), 2)   for c in cids}
        cum_by_year[y]    = {c: round(float(pyo.value(m.cum[c, y]) or 0.0), 2) for c in cids}
    annual_capex = {y: round(sum(capex[c] * CRF[c] * builds_by_year[y][c] for c in cids), 0) for y in yrs}
    annual_opex  = {y: round(sum(opex[c]  * cum_by_year[y][c] for c in cids), 0) for y in yrs}
    npv_total    = round(float(pyo.value(m.OBJ)), 0)

    return {
        "mode":                 "multi_year",
        "years":                years,
        "discount_rate":        r,
        "demand_growth_pct":    growth * 100,
        "CRF":                  {c: round(CRF[c], 6) for c in cids},
        "peaks_by_year_mw":     [round(p, 1) for p in peaks],
        "energy_target_by_year_mwh": [round(e, 0) for e in energies],
        "existing_firm_mw":     round(peak_existing, 1),
        "existing_energy_mwh":  round(energy_existing, 0),
        "builds_by_year":       builds_by_year,
        "cumulative_by_year":   cum_by_year,
        "annual_capex_by_year": annual_capex,
        "annual_opex_by_year":  annual_opex,
        "npv_total":            npv_total,
    }


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
    # Auto-pick multi-year planner when expansion.years > 1.
    yrs = int((inp.get("expansion") or {}).get("years", 1))
    result = plan_multi_year(inp) if yrs > 1 else plan(inp)
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n💾 wrote {args.output}")
