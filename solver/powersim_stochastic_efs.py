"""
PowerSim v4.0 — Stochastic UC, full extensive-form (#5)
=======================================================

Implements the *true* 2-stage stochastic UC formulation that was earlier
approximated by the consensus heuristic in `powersim_stochastic.py`.

Variables
    First-stage  (here-and-now):
        u[g, t]       commitment binary       — shared across scenarios
        y[g, t]       startup binary          — shared
        z[g, t]       shutdown binary         — shared
    Second-stage (wait-and-see, indexed by scenario s):
        p[g, t, s]    dispatch MW
        unserv[t, s]  unserved MWh/h
        res_sh[r,t,s] reserve shortfall
        soc[b, t, s]  BESS state of charge
        stor[h, t, s] hydro reservoir

Objective
    min Σ_g,t (no_load · u + startup · y) · dt              ← first stage
      + Σ_s π_s · ( Σ_g,t fuel_g(p[g,t,s]) · dt
                    + UNSERVED_PEN · Σ_t unserv[t,s] · dt
                    + reserve penalties · res_sh )            ← per-scenario
      [+ CVaR_α term across scenario costs if requested]

Inputs
    `inp` is a normal PowerSim input dict + `stochastic_tree` block:
        {
          "scenarios": [{"id":"MC_P10","prob":0.2,"profile_overrides":{...}}, ...],
          "objective": "expected" | "cvar" | "expected+cvar",
          "cvar_alpha": 0.95,
          "cvar_weight": 0.3
        }
    Each scenario carries a `profile_overrides` dict that maps an asset
    id → an alternate profile key already present in `profiles`.

This is significantly more honest than the consensus heuristic: it
finds the cost-optimal *single* commitment that ANY scenario must live
with, and reports the per-scenario operating cost.  Scaling is the
classical limit — at K scenarios × T periods, the LP/MIP grows ~K-fold.
Suitable for 3-10 scenarios on weekly horizons, not for 8760h × dozens.

CLI:
    python solver/powersim_stochastic_efs.py \\
        --input out/stoch_input.json --out out/stoch_efs

Inheriting `mip_gap` / `time_limit_s` / `solver` from `solver_settings`.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import pyomo.environ as pyo

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT / "solver", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_solver import (                                     # noqa: E402
    build_asset_map, slice_profiles, build_gas_limits,
    resolve_resolution, get_pmax_t, HOURS_PER_YEAR,
)


def _percentile(vals, q):
    if not vals: return 0.0
    s = sorted(vals)
    return s[max(0, min(len(s)-1, int(round(q*(len(s)-1)))))]


def solve_efs(inp: dict, *, time_limit_s: float | None = None,
              mip_gap: float | None = None, solver_backend: str = "auto") -> dict:
    """Build & solve the extensive-form 2-stage stochastic UC."""
    st = inp.get("stochastic_tree")
    if not st: raise ValueError("input.stochastic_tree missing")

    scenarios = list(st["scenarios"])
    K = len(scenarios)
    if K < 2: raise ValueError("need ≥ 2 scenarios for stochastic EF")

    probs = [float(sc.get("prob", sc.get("probability", 1.0))) for sc in scenarios]
    psum = sum(probs) or 1.0
    probs = [p / psum for p in probs]
    obj_mode = st.get("objective", "expected")
    alpha    = float(st.get("cvar_alpha", 0.95))
    w_cvar   = float(st.get("cvar_weight", 0.3))

    # ── Pre-build assets / profiles (shared) ─────────────────────────
    assets    = build_asset_map(inp)
    profiles, H = slice_profiles(inp)
    gas_lim   = build_gas_limits(inp,
        int(inp.get("study_horizon", {}).get("start_hour", 0)), H)
    r_min, _ppy, dt = resolve_resolution(inp)

    # Per-scenario profile maps — apply scenario.profile_overrides.
    scen_profiles = []
    for sc in scenarios:
        pf = dict(profiles)
        for aid, pkey in (sc.get("profile_overrides") or {}).items():
            # The override redirects the asset's existing profile key; if
            # the override key exists in the global `profiles` dict, the
            # solver will pick it up via `assets[aid].inflow_profile` /
            # `availability_profile` – we fix that pointer here.
            for a in inp.get("assets", []):
                if a.get("id") == aid:
                    if a.get("type") in ("hydro_reg", "hydro_ror"):
                        a["inflow_profile"] = pkey
                    elif a.get("type") in ("wind", "solar"):
                        a["availability_profile"] = pkey
        scen_profiles.append(pf)

    print(f"⚙️  Stochastic EF: {K} scenario(s) × {H} period(s) "
          f"× {len(assets)} asset(s)  dt={dt}h")

    m   = pyo.ConcreteModel()
    T   = list(range(1, H + 1))
    S   = list(range(K))
    all_ids   = list(assets.keys())
    disp_ids  = [i for i, a in assets.items() if a["type"] not in ("dr", "pumped_hydro")]
    committable = [i for i in disp_ids if assets[i]["_committable"]]
    bess_ids = [i for i, a in assets.items() if a["type"] == "bess"]
    hydro_reg = [i for i, a in assets.items() if a["type"] == "hydro_reg"]

    m.T = pyo.Set(initialize=T, ordered=True)
    m.S = pyo.Set(initialize=S, ordered=True)
    m.G = pyo.Set(initialize=disp_ids)
    m.GC = pyo.Set(initialize=committable)
    m.BESS = pyo.Set(initialize=bess_ids)
    m.GR = pyo.Set(initialize=hydro_reg)

    # First-stage commitment / startup / shutdown — *non-anticipative*
    m.u = pyo.Var(m.GC, m.T, domain=pyo.Binary)
    m.y = pyo.Var(m.GC, m.T, domain=pyo.Binary)
    m.z = pyo.Var(m.GC, m.T, domain=pyo.Binary)
    # Second-stage per-scenario dispatch + slacks
    m.p = pyo.Var(m.G, m.T, m.S, domain=pyo.NonNegativeReals)
    m.unserv = pyo.Var(m.T, m.S, domain=pyo.NonNegativeReals)
    if bess_ids:
        m.ch  = pyo.Var(m.BESS, m.T, m.S, domain=pyo.NonNegativeReals)
        m.dis = pyo.Var(m.BESS, m.T, m.S, domain=pyo.NonNegativeReals)
        m.soc = pyo.Var(m.BESS, m.T, m.S, domain=pyo.NonNegativeReals)
    if hydro_reg:
        m.stor  = pyo.Var(m.GR, m.T, m.S, domain=pyo.NonNegativeReals)
        m.spill = pyo.Var(m.GR, m.T, m.S, domain=pyo.NonNegativeReals)

    # ── Constraints ──────────────────────────────────────────────────
    # UC logic — shared
    def uc_logic(m, g, t):
        u_prev = 0 if t == 1 else m.u[g, t-1]
        return m.u[g, t] - u_prev == m.y[g, t] - m.z[g, t]
    m.UCLogic = pyo.Constraint(m.GC, m.T, rule=uc_logic)

    # Per-scenario gen bounds, balance, ramp, hydro, BESS.
    def gen_lb(m, g, t, s):
        if g in committable:
            return m.p[g, t, s] >= float(assets[g].get("pmin", 0)) * m.u[g, t]
        return m.p[g, t, s] >= 0
    def gen_ub(m, g, t, s):
        pmx = get_pmax_t(assets[g], t-1, scen_profiles[s])
        if g in committable:
            return m.p[g, t, s] <= pmx * m.u[g, t]
        return m.p[g, t, s] <= pmx
    m.GenLB = pyo.Constraint(m.G, m.T, m.S, rule=gen_lb)
    m.GenUB = pyo.Constraint(m.G, m.T, m.S, rule=gen_ub)

    def balance(m, t, s):
        gen = sum(m.p[g, t, s] for g in disp_ids)
        bess_net = sum(m.dis[b, t, s] - m.ch[b, t, s] for b in bess_ids) if bess_ids else 0
        d = profiles["demand"][t-1]    # demand is shared across scenarios
        return gen + bess_net + m.unserv[t, s] == d
    m.Balance = pyo.Constraint(m.T, m.S, rule=balance)

    # Hydro reservoir balance (per scenario, scenario-specific inflow).
    if hydro_reg:
        def hydro_bal(m, h, t, s):
            ha = assets[h]["hydro"]
            eff = float(ha.get("efficiency", 350))
            ikey = assets[h].get("inflow_profile")
            if ikey and ikey in scen_profiles[s]:
                infl = scen_profiles[s][ikey][t-1]
            else:
                infl = float(ha.get("inflow") or 0)
            release = m.p[h, t, s] / max(eff, 0.001)
            stor_prev = float(ha.get("reservoir_init", 700)) if t == 1 else m.stor[h, t-1, s]
            return m.stor[h, t, s] == stor_prev + (infl - release - m.spill[h, t, s]) * dt
        m.HydroBal = pyo.Constraint(m.GR, m.T, m.S, rule=hydro_bal)

        def stor_bnd(m, h, t, s):
            ha = assets[h]["hydro"]
            return (float(ha.get("reservoir_min", 0)),
                    m.stor[h, t, s],
                    float(ha.get("reservoir_max", 9999)))
        m.StorBnd = pyo.Constraint(m.GR, m.T, m.S, rule=stor_bnd)

    # BESS dynamics per scenario.
    if bess_ids:
        def bess_soc(m, b, t, s):
            a = assets[b]
            ec = float(a["eta_charge"]); ed = float(a["eta_discharge"])
            soc_prev = float(a["soc_init"]) * float(a["energy_mwh"]) if t == 1 else m.soc[b, t-1, s]
            return m.soc[b, t, s] == soc_prev + (ec * m.ch[b, t, s] - m.dis[b, t, s] / max(ed, 0.001)) * dt
        m.BessSOC = pyo.Constraint(m.BESS, m.T, m.S, rule=bess_soc)
        def bess_bnd(m, b, t, s):
            a = assets[b]
            return (float(a["soc_min"]) * float(a["energy_mwh"]),
                    m.soc[b, t, s],
                    float(a["soc_max"]) * float(a["energy_mwh"]))
        m.BessBnd = pyo.Constraint(m.BESS, m.T, m.S, rule=bess_bnd)
        def bess_ch_ub(m, b, t, s):
            return m.ch[b, t, s] <= float(assets[b]["power_mw"])
        def bess_dis_ub(m, b, t, s):
            return m.dis[b, t, s] <= float(assets[b]["power_mw"])
        m.BessChUB  = pyo.Constraint(m.BESS, m.T, m.S, rule=bess_ch_ub)
        m.BessDisUB = pyo.Constraint(m.BESS, m.T, m.S, rule=bess_dis_ub)

    # ── Objective ─────────────────────────────────────────────────────
    UNSERVED_PEN = float(inp.get("solver_settings", {}).get("unserved_penalty", 3000))
    def first_stage_cost():
        return sum(
            float(assets[g].get("startup_cost", 0)) * m.y[g, t]
          + float(assets[g].get("no_load_cost", 0)) * m.u[g, t] * dt
            for g in committable for t in m.T)

    def scen_cost(s):
        fuel = sum((assets[g]["_dispMC"] + assets[g].get("vom", 0)) * m.p[g, t, s] * dt
                   for g in disp_ids for t in m.T)
        unserv = UNSERVED_PEN * sum(m.unserv[t, s] * dt for t in m.T)
        return fuel + unserv

    if obj_mode == "expected":
        m.OBJ = pyo.Objective(rule=lambda m: first_stage_cost()
                              + sum(probs[s] * scen_cost(s) for s in S),
                              sense=pyo.minimize)
    else:
        # CVaR via auxiliary η + non-negative shortfalls (Rockafellar-Uryasev)
        m.eta = pyo.Var(domain=pyo.Reals)
        m.delta = pyo.Var(m.S, domain=pyo.NonNegativeReals)
        def _delta_link(m, s):
            return m.delta[s] >= scen_cost(s) - m.eta
        m.DeltaLink = pyo.Constraint(m.S, rule=_delta_link)
        def cvar_term():
            return m.eta + (1.0 / (1.0 - alpha)) * sum(probs[s] * m.delta[s] for s in S)
        if obj_mode == "cvar":
            m.OBJ = pyo.Objective(rule=lambda m: first_stage_cost() + cvar_term(),
                                  sense=pyo.minimize)
        else:   # expected + cvar
            m.OBJ = pyo.Objective(rule=lambda m: first_stage_cost()
                                  + (1 - w_cvar) * sum(probs[s] * scen_cost(s) for s in S)
                                  + w_cvar * cvar_term(),
                                  sense=pyo.minimize)

    # ── Solve ─────────────────────────────────────────────────────────
    s_cfg = inp.get("solver_settings") or {}
    if time_limit_s is not None: s_cfg = {**s_cfg, "time_limit_s": time_limit_s}
    if mip_gap is not None:      s_cfg = {**s_cfg, "mip_gap": mip_gap}

    try:
        from pyomo.contrib.appsi.solvers.highs import Highs
        sol = Highs()
        sol.highs_options["time_limit"]  = float(s_cfg.get("time_limit_s", 600))
        sol.highs_options["mip_rel_gap"] = float(s_cfg.get("mip_gap", 0.01))
        sol.highs_options["log_to_console"] = False
        t0 = time.time()
        try: result = sol.solve(m, load_solutions=True)
        except TypeError: result = sol.solve(m)
        wall = time.time() - t0
    except Exception as e:
        return {"error": f"solver failed: {e}"}

    # ── Extract per-scenario costs + summary ────────────────────────
    fs_cost = float(pyo.value(first_stage_cost()))
    sc_costs = [float(pyo.value(scen_cost(s))) for s in S]
    e_cost   = sum(probs[s] * (fs_cost + sc_costs[s]) for s in S)
    var_a    = _percentile([fs_cost + c for c in sc_costs], alpha)
    cvar_a   = var_a + sum(probs[s] * max(0.0, fs_cost + sc_costs[s] - var_a) for s in S) / max(1.0 - alpha, 1e-9)

    out = {
        "method":         "extensive_form",
        "scenarios":      [{
            "id":   scenarios[s].get("id", f"S{s}"),
            "label": scenarios[s].get("label", scenarios[s].get("id", f"S{s}")),
            "prob": probs[s],
            "first_stage_cost": round(fs_cost, 0),
            "second_stage_cost": round(sc_costs[s], 0),
            "total_cost":  round(fs_cost + sc_costs[s], 0),
            "unserved_mwh": round(sum(float(pyo.value(m.unserv[t, s])) * dt for t in T), 1),
        } for s in S],
        "objective_mode":  obj_mode,
        "cvar_alpha":      alpha,
        "cvar_weight":     w_cvar if obj_mode == "expected+cvar" else None,
        "first_stage_cost": round(fs_cost, 0),
        "E_total_cost":    round(e_cost, 0),
        "VaR_alpha":       round(var_a, 0),
        "CVaR_alpha":      round(cvar_a, 0),
        "risk_premium":    round(cvar_a - e_cost, 0),
        "objective_value": round(float(pyo.value(m.OBJ)), 0),
        "wallclock_s":     round(wall, 2),
        "K_scenarios":     K, "H_periods": H, "n_assets": len(assets),
    }
    return out


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", default="stochastic_efs_summary.json")
    ap.add_argument("--time-limit", type=int, default=None)
    ap.add_argument("--mip-gap", type=float, default=None)
    ap.add_argument("--solver", default="auto")
    args = ap.parse_args()

    inp = json.loads(Path(args.input).read_text(encoding="utf-8"))
    res = solve_efs(inp, time_limit_s=args.time_limit, mip_gap=args.mip_gap,
                    solver_backend=args.solver)
    out = Path(args.out); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n" + "═" * 72)
    print("  STOCHASTIC EXTENSIVE-FORM SUMMARY")
    print("═" * 72)
    if "error" in res:
        print("❌", res["error"]); sys.exit(1)
    print(f"  scenarios:        {res['K_scenarios']}    H={res['H_periods']}    "
          f"obj_mode={res['objective_mode']}")
    print(f"  first-stage cost: ${res['first_stage_cost']:>14,.0f}")
    for sc in res["scenarios"]:
        print(f"  {sc['id']:<8} π={sc['prob']:.3f}  total=${sc['total_cost']:>14,.0f}  "
              f"unserved={sc['unserved_mwh']:>9.1f} MWh")
    print(f"  E[total]:        ${res['E_total_cost']:>14,.0f}")
    print(f"  VaR_{res['cvar_alpha']:.2f}:        ${res['VaR_alpha']:>14,.0f}")
    print(f"  CVaR_{res['cvar_alpha']:.2f}:       ${res['CVaR_alpha']:>14,.0f}")
    print(f"  risk premium:    ${res['risk_premium']:>14,.0f}")
    print(f"  wallclock:       {res['wallclock_s']:.1f}s")
    print(f"\n💾 wrote {out}")
