"""
PowerSim v4.0 — Python Solver
==============================
Reads:  powersim_input.json   (from HTML Export)
Writes: powersim_results.json + powersim_results.xlsx

Architecture:
  - 1h resolution, 8760h/year (non-leap)
  - Pyomo + HiGHS (free) — Gurobi drop-in available
  - Rolling horizon for large horizons (>168h)
  - Full asset support: thermal, hydro_reg, hydro_ror,
    wind, solar, import, BESS
  - User-defined reserve products
  - Gas constraints: annual / monthly / annual+monthly
  - Marginal price via fixed-commitment ED resolve

Google Colab usage:
  !pip install pyomo highspy pandas openpyxl xlsxwriter -q
  # Upload powersim_input.json
  !python powersim_solver_v2.py

Local usage:
  python powersim_solver_v2.py [--input path] [--output path]
"""

# ── 0. Install if in Colab ────────────────────────────────────────────
import subprocess, sys
def _colab_install():
    try:
        import google.colab  # noqa
        subprocess.run([sys.executable,"-m","pip","install",
            "pyomo","highspy","pandas","openpyxl","xlsxwriter","-q"],
            capture_output=True)
        print("✅ packages installed")
    except ImportError:
        pass
_colab_install()

# ── 1. Imports ────────────────────────────────────────────────────────
import json, time, math, warnings, argparse, os
from datetime import datetime, timedelta
from collections import defaultdict

import numpy as np
import pandas as pd
import pyomo.environ as pyo
warnings.filterwarnings("ignore")

# ── Schema coupling (Stage 1 patch) ───────────────────────────────────
# The schema module owns the version constant and the output validator.
# We import it defensively so the solver still runs if the schema is in
# an adjacent path rather than installed as a package.
try:
    from powersim_schema import (                                   # type: ignore
        SCHEMA_VERSION as _SCHEMA_VER, MODEL_VERSION as _MODEL_VER,
        validate_output,
    )
except ImportError:
    _here = os.path.dirname(os.path.abspath(__file__))
    for _p in (_here, os.path.join(_here, "..", "schema"),
               os.path.join(_here, "schema")):
        _abs = os.path.abspath(_p)
        if _abs not in sys.path:
            sys.path.insert(0, _abs)
    try:
        from powersim_schema import (                               # type: ignore
            SCHEMA_VERSION as _SCHEMA_VER, MODEL_VERSION as _MODEL_VER,
            validate_output,
        )
    except ImportError:
        # Last-resort fallback — keep solver runnable even if schema missing.
        _SCHEMA_VER = "1.1"
        _MODEL_VER  = "PowerSim v4.0"
        def validate_output(_out):                                  # type: ignore
            return True, [], ["schema module unavailable; validation skipped"]

SCHEMA_VERSION = _SCHEMA_VER
MODEL_VERSION  = _MODEL_VER
RESOLUTION_H   = 1          # FIXED — never change
HOURS_PER_YEAR = 8760
SOLVER_VERSION = "powersim_solver 1.1.0"   # Stage-1-patched


# ══════════════════════════════════════════════════════════════════════
# 2. INPUT LOADING & PREPROCESSING
# ══════════════════════════════════════════════════════════════════════

def load_input(path: str = "powersim_input.json") -> dict:
    """Load and parse input JSON."""
    if not os.path.exists(path):
        print(f"⚠️  {path} not found — using built-in GSE 2026 demo data")
        return _demo_input()
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"✅ Loaded: {path}")
    _print_input_summary(data)
    return data


def _print_input_summary(inp: dict):
    assets = inp.get("assets", [])
    by_type = defaultdict(int)
    for a in assets:
        by_type[a.get("type","?")] += 1
    sh = inp.get("study_horizon", {})
    print(f"   Assets: {len(assets)} ({dict(by_type)})")
    print(f"   Horizon: {sh.get('start_hour',0)}h + {sh.get('horizon_hours',8760)}h")
    print(f"   Scenario: {inp.get('scenario_metadata',{}).get('id','—')}")


def slice_profiles(inp: dict) -> dict:
    """
    Extract the study slice from 8760-length profiles.
    Returns: {profile_key: [float × horizon_h]}
    """
    sh      = inp.get("study_horizon", {})
    start   = int(sh.get("start_hour", 0))
    horizon = int(sh.get("horizon_hours", HOURS_PER_YEAR))
    horizon = min(horizon, HOURS_PER_YEAR - start)

    profiles_full = inp.get("profiles", {})
    sliced = {}
    for key, arr in profiles_full.items():
        if isinstance(arr, list) and len(arr) >= start + horizon:
            sliced[key] = arr[start:start + horizon]
        elif isinstance(arr, list):
            # tile if needed
            extended = arr * math.ceil((start + horizon) / max(len(arr), 1))
            sliced[key] = extended[start:start + horizon]
        else:
            sliced[key] = [float(arr)] * horizon  # scalar expansion
    return sliced, horizon


def build_asset_map(inp: dict) -> dict:
    """Return {asset_id: asset_dict} with derived fields."""
    amap = {}
    for a in inp.get("assets", []):
        a2 = dict(a)
        # Effective MC
        hr = float(a.get("heat_rate", 0))
        fp = float(a.get("fuel_price", 0))
        mc = float(a.get("mc", a.get("vom", 0)))  # fallback
        if hr > 0 and fp > 0:
            mc = hr * fp * 0.9478   # GJ/MWh × $/MMBtu → $/MWh

        # ── D1 patch: hydro opportunity cost ─────────────────────────
        # Prior behaviour: hydro_reg / hydro_ror saw mc == 0 (no heat_rate,
        # no vom), so they dominated the merit order trivially.  We now
        # FLOOR hydro marginal cost with water_value — treating stored
        # water as a priced resource.  This is what the water_value field
        # was meant for; it was previously defined in schema but never
        # consumed by the solver.
        if a.get("type") in ("hydro_reg", "hydro_ror"):
            wv = float((a.get("hydro") or {}).get("water_value", 0) or 0)
            if wv > mc:
                mc = wv

        a2["_dispMC"] = mc
        # Committable flag
        a2["_committable"] = bool(a.get("committable", a.get("type") in ("thermal","hydro_reg")))
        # Gas usage rate [Mm³/MWh]
        a2["_gas_rate"] = hr / 35_000.0 if hr > 0 and a.get("fuel_type","gas")=="gas" else 0.0
        amap[a2["id"]] = a2
    return amap


def get_pmax_t(asset: dict, t_local: int, profiles: dict) -> float:
    """Effective Pmax at hour t (0-indexed within horizon)."""
    atype = asset.get("type")
    if atype in ("wind", "solar"):
        prof_key = asset.get("availability_profile")
        cf = profiles.get(prof_key, [1.0] * (t_local + 1))[t_local] if prof_key else 1.0
        return float(asset.get("pmax_installed", asset.get("pmax", 0))) * max(0.0, min(1.0, cf))
    elif atype == "hydro_ror":
        prof_key = asset.get("availability_profile")
        cf = profiles.get(prof_key, [asset.get("cf", 0.65)] * (t_local + 1))[t_local] if prof_key else asset.get("cf", 0.65)
        return float(asset.get("pmax", 0)) * max(0.0, min(1.0, cf))
    elif atype == "import":
        prof_key = asset.get("pmax_profile")
        if prof_key and isinstance(profiles.get(prof_key), list):
            return float(profiles[prof_key][t_local])
        return float(asset.get("pmax_profile", asset.get("pmax", 0)))
    # Maintenance factor
    pmax = float(asset.get("pmax", 0))
    mf = _maint_factor(asset, t_local)
    return pmax * mf


def _maint_factor(asset: dict, t_local: int) -> float:
    """Return availability factor [0,1] based on maintenance windows."""
    windows = asset.get("maint_windows", [])
    if not windows:
        return 1.0
    # t_local → calendar date requires knowing start date
    # For now return 1.0 if no window data; full implementation in Phase 2
    return 1.0


# ══════════════════════════════════════════════════════════════════════
# 3. GAS CONSTRAINT BUILDER
# ══════════════════════════════════════════════════════════════════════

def build_gas_limits(inp: dict, start_h: int, horizon_h: int) -> dict:
    """
    Returns per-hour gas limits for each thermal unit.
    Structure: {
        'mode': str,
        'annual_limit': float or None,
        'monthly_limits': {month_idx: float} or None,
        'applies_to': [str],
        'daily_limit': float  # derived for rolling horizon use
    }
    """
    gc = inp.get("gas_constraints", {})
    mode = gc.get("mode", "none")
    applies = gc.get("applies_to", [])

    annual = gc.get("annual", {}).get("cap") if "annual" in gc else None
    monthly_raw = gc.get("monthly", {})
    # Accept any of: "Jan".."Dec", "jan".."dec", int 1..12, str "1".."12"
    _MONTH_NAMES = ["jan","feb","mar","apr","may","jun",
                    "jul","aug","sep","oct","nov","dec"]
    monthly = None
    if monthly_raw:
        monthly = {}
        for k, v in monthly_raw.items():
            mo = None
            if isinstance(k, int) and 1 <= k <= 12:
                mo = k
            elif isinstance(k, str):
                kk = k.strip().lower()
                if kk.isdigit() and 1 <= int(kk) <= 12:
                    mo = int(kk)
                elif kk in _MONTH_NAMES:
                    mo = _MONTH_NAMES.index(kk) + 1
            if mo is not None:
                monthly[mo] = float(v)
        # Fill missing months with 0 (treated as "no limit set" downstream
        # only if the dict is empty; here we keep partial dicts as-given
        # and the constraint loop only constrains months actually present)

    # Daily limit derived from annual cap
    daily = None
    if annual and mode in ("annual", "annual+monthly"):
        daily = annual / 365.0

    return {
        "mode":          mode,
        "annual_limit":  annual,
        "monthly_limits": monthly,
        "applies_to":    applies,
        "daily_limit":   daily
    }


# ══════════════════════════════════════════════════════════════════════
# 4. PYOMO WINDOW SOLVER
# ══════════════════════════════════════════════════════════════════════

def solve_window(
    assets:       dict,          # {id: asset_dict}
    demand_w:     list,          # [float × H]
    profiles_w:   dict,          # sliced profiles for this window
    reserve_prods: list,         # reserve product defs
    gas_limits:   dict,          # gas constraint config
    init_state:   dict,          # carry-over from previous window
    solver_cfg:   dict,          # solver settings
    offset_h:     int = 0        # global hour offset for calendar
) -> tuple[list, dict, float]:
    """
    Solve one rolling window.
    Returns: (hourly_results, final_state, solve_time_s)
    """
    H   = len(demand_w)
    T   = list(range(1, H + 1))    # 1-indexed periods
    m   = pyo.ConcreteModel()

    # Asset lists by type
    all_ids    = list(assets.keys())
    thermal    = [i for i,a in assets.items() if a["type"]=="thermal"]
    hydro_reg  = [i for i,a in assets.items() if a["type"]=="hydro_reg"]
    hydro_ror  = [i for i,a in assets.items() if a["type"]=="hydro_ror"]
    wind_solar = [i for i,a in assets.items() if a["type"] in ("wind","solar")]
    imports    = [i for i,a in assets.items() if a["type"]=="import"]
    bess_ids   = [i for i,a in assets.items() if a["type"]=="bess"]
    committable = [i for i in all_ids if assets[i]["_committable"]]
    non_commit = [i for i in all_ids if not assets[i]["_committable"]]
    gas_units  = [i for i in thermal
                  if i in gas_limits.get("applies_to", []) and assets[i]["_gas_rate"] > 0]

    m.T    = pyo.Set(initialize=T, ordered=True)
    m.G    = pyo.Set(initialize=all_ids)
    m.GC   = pyo.Set(initialize=committable)     # has binary vars
    m.GR   = pyo.Set(initialize=hydro_reg)
    m.BESS = pyo.Set(initialize=bess_ids)

    # ── Decision Variables ─────────────────────────────────────────────
    m.p  = pyo.Var(m.G,  m.T, domain=pyo.NonNegativeReals)  # dispatch MW
    m.u  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # commitment
    m.y  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # startup
    m.z  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # shutdown

    # Unserved energy and reserve shortfall slacks
    m.unserv = pyo.Var(m.T, domain=pyo.NonNegativeReals)

    # Reserve slack per product per hour
    res_ids = [rp["id"] for rp in reserve_prods]
    m.RIDS = pyo.Set(initialize=res_ids)
    m.res_sh = pyo.Var(m.RIDS, m.T, domain=pyo.NonNegativeReals)

    # Reserve allocation per eligible unit per product per hour
    res_elig = {}  # {res_id: [eligible_asset_ids]}
    for rp in reserve_prods:
        res_elig[rp["id"]] = [u for u in rp.get("eligible_units",[]) if u in assets]
    m.res_up   = pyo.Var(m.RIDS, m.G, m.T, domain=pyo.NonNegativeReals)
    m.res_down = pyo.Var(m.RIDS, m.G, m.T, domain=pyo.NonNegativeReals)

    # Hydro reservoir storage and spill
    if hydro_reg:
        m.stor  = pyo.Var(m.GR, m.T, domain=pyo.NonNegativeReals)
        m.spill = pyo.Var(m.GR, m.T, domain=pyo.NonNegativeReals)

    # BESS charge/discharge/SOC
    if bess_ids:
        m.ch   = pyo.Var(m.BESS, m.T, domain=pyo.NonNegativeReals)  # charge MW
        m.dis  = pyo.Var(m.BESS, m.T, domain=pyo.NonNegativeReals)  # discharge MW
        m.soc  = pyo.Var(m.BESS, m.T, domain=pyo.NonNegativeReals)  # stored MWh
        m.xch  = pyo.Var(m.BESS, m.T, domain=pyo.Binary)            # 1=charging

    # ── Objective ─────────────────────────────────────────────────────
    UNSERVED_PEN = float(solver_cfg.get("unserved_penalty", 3000))
    CURT_PEN     = float(solver_cfg.get("curtailment_penalty", 0))

    res_penalties = {rp["id"]: float(rp.get("shortfall_penalty",500))
                     for rp in reserve_prods}

    def obj_rule(m):
        # Generation costs
        fuel  = sum((assets[g]["_dispMC"] + assets[g].get("vom",0)) * m.p[g,t]
                    for g in all_ids for t in m.T)
        start = sum(float(assets[g].get("startup_cost",0)) * m.y[g,t]
                    for g in committable for t in m.T)
        noload= sum(float(assets[g].get("no_load_cost",0)) * m.u[g,t]
                    for g in committable for t in m.T)
        # BESS degradation via vom_discharge
        bess_cost = sum(
            float(assets[b].get("vom_discharge",0)) * m.dis[b,t]
            for b in bess_ids for t in m.T
        ) if bess_ids else 0
        # Penalties
        unserved_pen = UNSERVED_PEN * sum(m.unserv[t] for t in m.T)
        res_pen = sum(
            res_penalties[rid] * m.res_sh[rid,t]
            for rid in res_ids for t in m.T
        )
        # Stage 2: strategic end-of-horizon penalty for reservoir hydro.
        # Activates only if at least one plant declared target + penalty.
        end_level_pen = 0
        if hasattr(m, "end_short"):
            end_level_pen = sum(
                float(assets[h]["hydro"]["end_level_penalty"]) * m.end_short[h]
                for h in m.GR_strat
            )
        return fuel + start + noload + bess_cost + unserved_pen + res_pen + end_level_pen
    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    # ── Energy Balance ─────────────────────────────────────────────────
    # Σ p[g,t] + unserv - BESS_net = demand[t]
    def balance(m, t):
        gen  = sum(m.p[g,t] for g in all_ids)
        bess_net = sum(m.dis[b,t] - m.ch[b,t] for b in bess_ids) if bess_ids else 0
        return gen + bess_net + m.unserv[t] == demand_w[t-1]
    m.Balance = pyo.Constraint(m.T, rule=balance)

    # ── Generation bounds ──────────────────────────────────────────────
    def gen_lb(m, g, t):
        if g in committable:
            return m.p[g,t] >= float(assets[g].get("pmin",0)) * m.u[g,t]
        return m.p[g,t] >= 0
    def gen_ub(m, g, t):
        pmx = get_pmax_t(assets[g], t-1, profiles_w)
        if g in committable:
            return m.p[g,t] <= pmx * m.u[g,t]
        return m.p[g,t] <= pmx
    m.GenLB = pyo.Constraint(m.G, m.T, rule=gen_lb)
    m.GenUB = pyo.Constraint(m.G, m.T, rule=gen_ub)

    # ── UC logic: u[t] - u[t-1] = y[t] - z[t] ────────────────────────
    def uc_logic(m, g, t):
        u_prev = init_state.get(g, {}).get("u", 0) if t == 1 else m.u[g, t-1]
        return m.u[g,t] - u_prev == m.y[g,t] - m.z[g,t]
    m.UCLogic = pyo.Constraint(m.GC, m.T, rule=uc_logic)

    def yz_ub(m, g, t): return m.y[g,t] + m.z[g,t] <= 1
    m.YZUB = pyo.Constraint(m.GC, m.T, rule=yz_ub)

    # ── Minimum Up Time ────────────────────────────────────────────────
    def min_up(m, g, t):
        mut = int(assets[g].get("min_up", 0))
        if mut < 2: return pyo.Constraint.Skip
        end = min(t + mut - 1, T[-1])
        return sum(m.y[g,tau] for tau in range(t, end+1)) <= m.u[g,t]
    m.MinUp = pyo.Constraint(m.GC, m.T, rule=min_up)

    # ── Minimum Down Time ──────────────────────────────────────────────
    def min_dn(m, g, t):
        mdt = int(assets[g].get("min_down", 0))
        if mdt < 2: return pyo.Constraint.Skip
        end = min(t + mdt - 1, T[-1])
        return sum(m.z[g,tau] for tau in range(t, end+1)) <= 1 - m.u[g,t]
    m.MinDn = pyo.Constraint(m.GC, m.T, rule=min_dn)

    # ── Ramp constraints ───────────────────────────────────────────────
    def ramp_up_c(m, g, t):
        if t == 1: return pyo.Constraint.Skip
        ru = float(assets[g].get("ramp_up", 9999))
        if ru >= 9999: return pyo.Constraint.Skip
        return m.p[g,t] - m.p[g,t-1] <= ru
    def ramp_dn_c(m, g, t):
        if t == 1: return pyo.Constraint.Skip
        rd = float(assets[g].get("ramp_down", 9999))
        if rd >= 9999: return pyo.Constraint.Skip
        return m.p[g,t-1] - m.p[g,t] <= rd
    m.RampUp = pyo.Constraint(m.G, m.T, rule=ramp_up_c)
    m.RampDn = pyo.Constraint(m.G, m.T, rule=ramp_dn_c)

    # ── Hydro Reservoir Balance ────────────────────────────────────────
    # stor[h,t] = stor[h,t-1] + inflow_own[t] + cascade_in[t] - release[t] - spill[t]
    # release[t] = p[h,t] / efficiency  (mode 2: release-to-energy)
    #
    # Stage 2 cascade model:
    #   cascade_in[t] = cascade_gain × release_upstream[t - travel_delay_h]
    #   (for t ≤ travel_delay_h the upstream contribution is 0 — bootstrap)
    #
    # Note — we explicitly exclude upstream *spill* from downstream inflow.
    # Spill is water released bypassing turbines (overflow); it physically
    # does reach the downstream reservoir, but counting it here would re-
    # introduce the double-counting we just fixed (spill = water the
    # upstream plant didn't turbine; if we let the downstream plant
    # turbine it, we've just recovered the upstream plant's waste).
    # Conservative choice: downstream sees only turbined water.
    if hydro_reg:
        def hydro_bal(m, h, t):
            ha   = assets[h]["hydro"]
            eff  = float(ha.get("efficiency", 350))   # MWh/Mm³
            infl_key = assets[h].get("inflow_profile")
            # Stage 2: no fallback inflow. If inflow_profile is null AND
            # hydro.inflow is not explicitly set, inflow is 0. Reservoirs
            # with no inflow source rely solely on initial storage + cascade.
            if infl_key and infl_key in profiles_w:
                infl = profiles_w[infl_key][t-1]
            else:
                raw_inflow = ha.get("inflow")
                infl = float(raw_inflow) if raw_inflow is not None else 0.0
            release = m.p[h,t] / max(eff, 0.001)

            # ── Cascade with travel delay ──
            up_id = ha.get("cascade_upstream")
            cascade_in = 0
            if up_id and up_id in assets:
                delay    = int(ha.get("cascade_travel_delay_h", 0))
                gain     = float(ha.get("cascade_gain", 1.0))
                t_upstream = t - delay
                if t_upstream >= 1:
                    up_eff = float(assets[up_id].get("hydro",{}).get("efficiency",350))
                    # Only turbined water (release = p/eff), not spill
                    cascade_in = gain * (m.p[up_id, t_upstream] / max(up_eff, 0.001))
                # else: pre-horizon upstream water not modeled — bootstrap=0

            infl_total = infl + cascade_in
            if t == 1:
                stor_prev = init_state.get(h, {}).get("stor", float(ha.get("reservoir_init",700)))
            else:
                stor_prev = m.stor[h,t-1]
            return m.stor[h,t] == stor_prev + infl_total - release - m.spill[h,t]
        m.HydroBal = pyo.Constraint(m.GR, m.T, rule=hydro_bal)

        def stor_lb(m, h, t):
            return m.stor[h,t] >= float(assets[h]["hydro"].get("reservoir_min",0))
        def stor_ub(m, h, t):
            return m.stor[h,t] <= float(assets[h]["hydro"].get("reservoir_max",9999))
        def stor_end(m, h):
            return m.stor[h,T[-1]] >= float(assets[h]["hydro"].get("reservoir_end_min",
                                              assets[h]["hydro"].get("reservoir_min",0)))
        m.StorLB  = pyo.Constraint(m.GR, m.T, rule=stor_lb)
        m.StorUB  = pyo.Constraint(m.GR, m.T, rule=stor_ub)
        m.StorEnd = pyo.Constraint(m.GR, rule=stor_end)

        # ── Stage 2: strategic end-of-horizon soft penalty ──────────
        # Purpose: discourage myopic depletion over the horizon when the
        # plant has seasonal value. If end storage falls below a target
        # fraction of reservoir_max, a penalty of $/Mm³ is added to the
        # objective per unit of shortfall.
        #
        # Formulation: end_shortfall[h] ≥ target - stor[h, T_last], ≥ 0
        #              objective += end_level_penalty × end_shortfall[h]
        _strategic_plants = [
            h for h in m.GR
            if (assets[h]["hydro"].get("end_level_penalty") or 0) > 0
            and (assets[h]["hydro"].get("target_end_level_frac") or 0) > 0
        ]
        if _strategic_plants:
            m.GR_strat = pyo.Set(initialize=_strategic_plants)
            m.end_short = pyo.Var(m.GR_strat, domain=pyo.NonNegativeReals)

            def end_short_c(m, h):
                ha     = assets[h]["hydro"]
                target = float(ha["target_end_level_frac"]) * float(ha.get("reservoir_max", 0))
                return m.end_short[h] >= target - m.stor[h, T[-1]]
            m.EndShort = pyo.Constraint(m.GR_strat, rule=end_short_c)

    # ── BESS Constraints ───────────────────────────────────────────────
    if bess_ids:
        def bess_soc(m, b, t):
            a  = assets[b]
            ec = float(a["eta_charge"])
            ed = float(a["eta_discharge"])
            if t == 1:
                soc_prev = init_state.get(b, {}).get("soc",
                           float(a["soc_init"]) * float(a["energy_mwh"]))
            else:
                soc_prev = m.soc[b,t-1]
            return m.soc[b,t] == soc_prev + ec * m.ch[b,t] - m.dis[b,t] / max(ed,0.001)
        def bess_soc_lb(m, b, t):
            return m.soc[b,t] >= float(assets[b]["soc_min"]) * float(assets[b]["energy_mwh"])
        def bess_soc_ub(m, b, t):
            return m.soc[b,t] <= float(assets[b]["soc_max"]) * float(assets[b]["energy_mwh"])
        def bess_ch_ub(m, b, t):
            return m.ch[b,t] <= float(assets[b]["power_mw"]) * m.xch[b,t]
        def bess_dis_ub(m, b, t):
            return m.dis[b,t] <= float(assets[b]["power_mw"]) * (1 - m.xch[b,t])
        m.BessSOC   = pyo.Constraint(m.BESS, m.T, rule=bess_soc)
        m.BessSOCLB = pyo.Constraint(m.BESS, m.T, rule=bess_soc_lb)
        m.BessSOCUB = pyo.Constraint(m.BESS, m.T, rule=bess_soc_ub)
        m.BessCHUB  = pyo.Constraint(m.BESS, m.T, rule=bess_ch_ub)
        m.BessDISUB = pyo.Constraint(m.BESS, m.T, rule=bess_dis_ub)

    # ── Reserve constraints ────────────────────────────────────────────
    for rp in reserve_prods:
        rid  = rp["id"]
        req  = float(rp["requirement"]) if isinstance(rp["requirement"],(int,float)) else 0
        elig = res_elig[rid]
        dirn = rp.get("direction","up")
        derating = rp.get("derating_factors", {})

        def res_supply(m, t, _rid=rid, _elig=elig, _req=req, _dirn=dirn, _der=derating):
            sup = sum(m.res_up[_rid,g,t] * _der.get(g,1.0) for g in _elig) \
                  if _dirn in ("up","symmetric") else \
                  sum(m.res_down[_rid,g,t] * _der.get(g,1.0) for g in _elig)
            return sup + m.res_sh[_rid,t] >= _req
        m.add_component(f"ResSup_{rid}", pyo.Constraint(m.T, rule=res_supply))

        # Headroom/footroom: p + res_up ≤ pmax·u ; p - res_down ≥ pmin·u
        for g in elig:
            if dirn in ("up","symmetric"):
                def res_head(m, t, _g=g, _rid=rid):
                    pmx = get_pmax_t(assets[_g], t-1, profiles_w)
                    u_t = m.u[_g,t] if _g in committable else 1
                    return m.p[_g,t] + m.res_up[_rid,_g,t] <= pmx * u_t
                m.add_component(f"ResHead_{rid}_{g}", pyo.Constraint(m.T, rule=res_head))
            if dirn in ("down","symmetric"):
                def res_foot(m, t, _g=g, _rid=rid):
                    pmin = float(assets[_g].get("pmin",0))
                    u_t  = m.u[_g,t] if _g in committable else 1
                    return m.p[_g,t] - m.res_down[_rid,_g,t] >= pmin * u_t
                m.add_component(f"ResFoot_{rid}_{g}", pyo.Constraint(m.T, rule=res_foot))

        # Ineligible units: zero reserve
        for g in all_ids:
            if g not in elig:
                for t in T:
                    m.res_up[rid,g,t].fix(0)
                    m.res_down[rid,g,t].fix(0)

    # ── Gas constraints ────────────────────────────────────────────────
    gas_mode = gas_limits.get("mode","none")
    if gas_mode != "none" and gas_units:
        def gas_total(m):
            return sum(assets[g]["_gas_rate"] * m.p[g,t]
                       for g in gas_units for t in m.T) <= \
                   gas_limits.get("daily_limit", 9999) * H
        if gas_mode in ("annual","annual+monthly") and gas_limits.get("daily_limit"):
            m.GasTotal = pyo.Constraint(rule=gas_total)
        # ── Stage 2: real per-calendar-month gas cap ───────────────────
        # Map each window hour t (1..H) to its calendar month using the
        # global hour offset.  For each month that the window TOUCHES,
        # emit one constraint:
        #   Σ_{t in month_m ∩ window} gas_rate × p[g,t]  ≤  cap_m × frac
        # where `frac` = (hours in window ∩ month) / (total hours in month).
        # This pro-rates correctly when the horizon is shorter than a month
        # (e.g. 168h Jan window gets cap_Jan × 168/744).
        monthly = gas_limits.get("monthly_limits")
        if gas_mode in ("monthly","annual+monthly") and monthly:
            HOURS_PER_MONTH = (
                31*24, 28*24, 31*24, 30*24, 31*24, 30*24,   # Jan..Jun
                31*24, 31*24, 30*24, 31*24, 30*24, 31*24,   # Jul..Dec
            )
            # Bucket window hours by calendar month
            def _hour_to_month(global_h: int) -> int:
                """Return 1..12 for a global hour 0..8759 (non-leap)."""
                acc = 0
                for i, hpm in enumerate(HOURS_PER_MONTH, start=1):
                    if global_h < acc + hpm:
                        return i
                    acc += hpm
                return 12
            month_to_periods: dict[int, list[int]] = {}
            for t in T:
                gh = (offset_h + (t - 1)) % 8760
                mo = _hour_to_month(gh)
                month_to_periods.setdefault(mo, []).append(t)
            # Build constraints — only for months that have a cap declared
            constrained = [mo for mo in sorted(month_to_periods.keys()) if mo in monthly]
            if constrained:
                m.GasMonthlyIdx = pyo.Set(initialize=constrained, ordered=True)
                def _gas_month_rule(m, mo):
                    periods = month_to_periods[mo]
                    cap_full = float(monthly[mo])
                    # Pro-rate: fraction of the month covered by this window
                    frac = len(periods) / float(HOURS_PER_MONTH[mo - 1])
                    cap_window = cap_full * frac
                    return sum(assets[g]["_gas_rate"] * m.p[g, t]
                               for g in gas_units for t in periods) <= cap_window
                m.GasMonthly = pyo.Constraint(m.GasMonthlyIdx, rule=_gas_month_rule)

    # ── Solve ──────────────────────────────────────────────────────────
    m.dual = pyo.Suffix(direction=pyo.Suffix.IMPORT)
    try:
        # Pyomo ≤6.9 ships HiGHS; Pyomo ≥6.10 renamed the class to Highs.
        try:
            from pyomo.contrib.appsi.solvers.highs import HiGHS as _HighsCls
        except ImportError:
            from pyomo.contrib.appsi.solvers.highs import Highs as _HighsCls
        solver = _HighsCls()
        solver.highs_options["time_limit"]    = float(solver_cfg.get("time_limit_s",300))
        solver.highs_options["mip_rel_gap"]   = float(solver_cfg.get("mip_gap",0.005))
        solver.highs_options["log_to_console"]= False
    except Exception:
        solver = pyo.SolverFactory("appsi_highs")

    t0     = time.time()
    # Pyomo 6.10 dropped the load_solutions kwarg from appsi.Highs.solve().
    # Try the old signature first; fall back silently for newer versions.
    try:
        result = solver.solve(m, load_solutions=True)
    except TypeError:
        result = solver.solve(m)
    dt     = time.time() - t0

    def pv(var, *keys):
        try: v = pyo.value(var[keys]); return float(v) if v else 0.0
        except: return 0.0

    # ── Extract hourly results ─────────────────────────────────────────
    hourly_w = []
    for t in T:
        disp = {g: round(pv(m.p, g, t), 3) for g in all_ids}
        comm = {g: round(pv(m.u, g, t))    for g in committable}
        start_h = {g: round(pv(m.y, g, t)) for g in committable}
        shut_h  = {g: round(pv(m.z, g, t)) for g in committable}

        # Gas
        hgas = sum(assets[g]["_gas_rate"] * disp[g] for g in gas_units)

        # Curtailment (RE potential - actual)
        curt = 0.0
        for g in wind_solar:
            pot = get_pmax_t(assets[g], t-1, profiles_w)
            curt += max(0.0, pot - disp[g])

        # Lambda (VOLL-aware, from ED resolve in Phase 2 — here use dual)
        try:    lam = abs(float(m.dual[m.Balance[t]]))
        except: lam = 0.0
        if lam < 1e-6:
            # VOLL-aware fallback: check unserved → marginal committed
            unserv = pv(m.unserv, t)
            if unserv > 1e-3:
                lam = float(solver_cfg.get("unserved_penalty", 3000))
            else:
                mcs = [assets[g]["_dispMC"] for g in all_ids if disp.get(g,0) > 0.5]
                lam = max(mcs) if mcs else 0.0

        # Reserves
        res_up_h   = {rid: {g: pv(m.res_up,  rid, g, t) for g in res_elig[rid]}
                      for rid in res_ids}
        res_down_h = {rid: {g: pv(m.res_down, rid, g, t) for g in res_elig[rid]}
                      for rid in res_ids}
        res_sh_h   = {rid: pv(m.res_sh, rid, t) for rid in res_ids}

        # BESS
        bess_h = {}
        if bess_ids:
            for b in bess_ids:
                cap = float(assets[b]["energy_mwh"])
                s   = pv(m.soc, b, t)
                bess_h[b] = {
                    "charge_mw":    pv(m.ch,  b, t),
                    "discharge_mw": pv(m.dis, b, t),
                    "soc_mwh":      round(s, 2),
                    "soc_frac":     round(s / cap, 3) if cap > 0 else 0
                }

        # Hydro
        hydro_h = {}
        if hydro_reg:
            for h_id in hydro_reg:
                ha  = assets[h_id]["hydro"]
                eff = float(ha.get("efficiency",350))
                rel = disp[h_id] / max(eff, 0.001)
                infl_key = assets[h_id].get("inflow_profile")
                infl = profiles_w.get(infl_key, [float(ha.get("inflow",0.05))]*(t))[t-1] if infl_key else float(ha.get("inflow",0.05))
                hydro_h[h_id] = {
                    "storage_mm3":  round(pv(m.stor, h_id, t), 3),
                    "release_mm3h": round(rel, 5),
                    "inflow_mm3h":  round(infl, 5),
                    "spill_mm3h":   round(pv(m.spill, h_id, t), 5)
                }

        gen_total = sum(disp.values()) + (
            sum(bess_h[b]["discharge_mw"] - bess_h[b]["charge_mw"] for b in bess_ids)
            if bess_ids else 0
        )

        hourly_w.append({
            "t":               t - 1 + offset_h,
            "load_mw":         demand_w[t-1],
            "generation_mw":   round(gen_total, 2),
            "lambda_usd_mwh":  round(lam, 3),
            "lambda_source":   "dual_or_fallback",
            "unserved_mwh":    round(pv(m.unserv, t), 3),
            "curtailed_mwh":   round(curt, 2),
            "gas_mm3h":        round(hgas, 6),
            "dispatch":        disp,
            "commitment":      comm,
            "startup":         start_h,
            "shutdown":        shut_h,
            "reserve_up":      res_up_h,
            "reserve_down":    res_down_h,
            "reserve_shortfall": res_sh_h,
            "bess":            bess_h,
            "hydro":           hydro_h
        })

    # ── Carry final state ──────────────────────────────────────────────
    last_t = T[-1]
    fin_state = {}
    for g in committable:
        fin_state[g] = {"u": round(pv(m.u, g, last_t)),
                        "p": pv(m.p,  g, last_t)}
    for h_id in hydro_reg:
        fin_state.setdefault(h_id, {})["stor"] = pv(m.stor, h_id, last_t)
    for b in bess_ids:
        fin_state.setdefault(b, {})["soc"] = pv(m.soc, b, last_t)

    tc = str(getattr(result, "solver", {}).termination_condition
             if hasattr(result,"solver") else "—")
    print(f"   ✅ {tc} | {dt:.1f}s | H={H}")

    # ── Return Pyomo objective for real closure reconciliation ────────
    # (Stage-1 patch: closure check was self-referential; now compare
    #  reconstructed cost against the solver's own objective value.)
    try:
        obj_val = float(pyo.value(m.OBJ))
    except Exception:
        obj_val = float("nan")
    return hourly_w, fin_state, dt, obj_val


# ══════════════════════════════════════════════════════════════════════
# 5. ROLLING HORIZON DRIVER
# ══════════════════════════════════════════════════════════════════════

def solve_all(inp: dict, assets: dict, profiles: dict, gas_limits: dict) -> tuple[list, float, float]:
    """
    Run UC/ED over the full study horizon using rolling windows if needed.
    Returns: (all_hourly, total_solve_time, total_objective)
    Stage-1 patch: also returns summed Pyomo objective for closure check.
    """
    sh      = inp.get("study_horizon", {})
    H_total = int(sh.get("horizon_hours", len(profiles.get("demand",[]))))
    demand  = profiles["demand"]
    reserve_prods = inp.get("reserve_products", [])
    solver_cfg    = inp.get("solver_settings", {})

    window_h = int(solver_cfg.get("rolling_window_h", 168))
    step_h   = int(solver_cfg.get("rolling_step_h",   24))

    use_rolling = H_total > window_h

    if not use_rolling:
        print(f"⚙️  Full-horizon solve: {len(assets)} assets × {H_total}h")
        hourly, _, dt, obj_val = solve_window(
            assets, demand[:H_total], profiles, reserve_prods, gas_limits,
            init_state={}, solver_cfg=solver_cfg, offset_h=0
        )
        return hourly, dt, obj_val

    # Rolling
    n_windows = math.ceil((H_total - window_h) / step_h) + 1
    print(f"⚙️  Rolling Horizon: {H_total}h ÷ {window_h}h window × {step_h}h step = {n_windows} windows")

    all_hourly, state, total_dt, obj_accum = [], {}, 0.0, 0.0
    committed = 0

    for w in range(n_windows):
        start = w * step_h
        end   = min(start + window_h, H_total)
        if start >= H_total: break

        demand_w   = demand[start:end]
        profiles_w = {k: v[start:end] if isinstance(v, list) else v
                      for k, v in profiles.items()}

        hourly_w, state, dt, obj_w = solve_window(
            assets, demand_w, profiles_w, reserve_prods, gas_limits,
            init_state=state, solver_cfg=solver_cfg, offset_h=start
        )
        total_dt += dt
        # Commit only first step_h hours — objective is scaled by the committed
        # fraction so rolling windows contribute proportionally to closure check.
        commit_n    = min(step_h, end - start)
        all_hourly.extend(hourly_w[:commit_n])
        committed  += commit_n
        if obj_w == obj_w and len(hourly_w) > 0:          # NaN check
            obj_accum += obj_w * (commit_n / len(hourly_w))
        pct = committed / H_total * 100
        print(f"   window {w+1:3d}/{n_windows}: h{start:5d}-{start+commit_n-1:5d} "
              f"| {dt:5.1f}s | {pct:5.1f}% done")

    print(f"\n   ✅ Total: {committed}h, {total_dt:.0f}s ({total_dt/60:.1f} min)")
    return all_hourly, total_dt, obj_accum


# ══════════════════════════════════════════════════════════════════════
# 6. MARGINAL PRICE (Fixed-Commitment ED Resolve)
# ══════════════════════════════════════════════════════════════════════

def compute_marginal_prices(hourly: list, assets: dict, profiles: dict,
                             reserve_prods: list, solver_cfg: dict) -> list:
    """
    Post-process: fix commitment from UC solve, re-run LP ED to get
    clean dual-based marginal prices (lambda).
    This is the correct method for MIP → LP duality extraction.
    """
    print("⚡ ED resolve for marginal prices (LP, fixed commitment)...")
    demand = [h["load_mw"] for h in hourly]
    H = len(demand)
    T = list(range(1, H + 1))
    m = pyo.ConcreteModel()
    m.T = pyo.Set(initialize=T, ordered=True)
    m.G = pyo.Set(initialize=list(assets.keys()))

    # Continuous only — no binary (LP)
    m.p = pyo.Var(m.G, m.T, domain=pyo.NonNegativeReals)
    m.unserv = pyo.Var(m.T, domain=pyo.NonNegativeReals)

    # Fix upper bound based on UC commitment
    for g in assets:
        for t in T:
            h_dict  = hourly[t-1]
            comm    = h_dict.get("commitment", {}).get(g, 1)  # default 1 for non-committable
            pmx     = get_pmax_t(assets[g], t-1, profiles)
            pmin    = float(assets[g].get("pmin",0)) * comm
            m.p[g,t].setub(pmx * comm)
            m.p[g,t].setlb(pmin)

    UNSERVED_PEN = float(solver_cfg.get("unserved_penalty", 3000))
    def obj(m): return sum(assets[g]["_dispMC"] * m.p[g,t] for g in m.G for t in m.T) + \
                        UNSERVED_PEN * sum(m.unserv[t] for t in m.T)
    m.OBJ = pyo.Objective(rule=obj, sense=pyo.minimize)

    def balance(m, t):
        return sum(m.p[g,t] for g in m.G) + m.unserv[t] == demand[t-1]
    m.Balance = pyo.Constraint(m.T, rule=balance)

    m.dual = pyo.Suffix(direction=pyo.Suffix.IMPORT)
    try:
        from pyomo.contrib.appsi.solvers.highs import HiGHS
        solver = HiGHS()
        solver.highs_options["log_to_console"] = False
    except:
        solver = pyo.SolverFactory("appsi_highs")
    solver.solve(m, load_solutions=True)

    # Extract duals
    for t in T:
        try:    lam = abs(float(m.dual[m.Balance[t]]))
        except: lam = 0.0
        hourly[t-1]["lambda_usd_mwh"]  = round(lam, 3)
        hourly[t-1]["lambda_source"]   = "lp_dual"
    print("   ✅ Marginal prices computed (LP dual)")
    return hourly


# ══════════════════════════════════════════════════════════════════════
# 7. RESULT STORE BUILDER
# ══════════════════════════════════════════════════════════════════════

def build_result_store(hourly: list, assets: dict, inp: dict, solve_time: float,
                       obj_total: float | None = None) -> dict:
    """
    Build the standardized output dict for HTML import.

    Stage-1 patches applied:
      • monthly aggregation no longer uses the dead `mo_energy = annual/H × len(idxs)`
        placeholder — it now carries real per-month per-unit energy/cost rollups
      • closure check compares reconstructed cost against the real Pyomo
        objective (passed in as `obj_total`), not a self-referential expression
      • diagnostics block carries hydro end-storage violations, gas-binding
        flag, and solver/loader versions
      • data_source_fingerprint echoed into metadata from input.profile_bundle
      • result is validated against OUTPUT_SCHEMA before return (warnings only)
    """
    from calendar import month_abbr
    sh       = inp.get("study_horizon", {})
    sc_meta  = inp.get("scenario_metadata", {})
    s_cfg    = inp.get("solver_settings", {})
    pbundle  = inp.get("profile_bundle", {})

    H        = len(hourly)
    res_ids  = [rp["id"] for rp in inp.get("reserve_products",[])]
    gas_cfg  = inp.get("gas_constraints", {}) or {}
    gas_units= [a["id"] for a in inp.get("assets",[])
                if a.get("type")=="thermal" and a["id"] in
                   gas_cfg.get("applies_to", [])]

    # ── By-unit summary ────────────────────────────────────────────────
    by_unit = {}
    for gid, a in assets.items():
        energy    = sum(h["dispatch"].get(gid,0) for h in hourly)
        oper_h    = sum(1 for h in hourly if h["commitment"].get(gid,0) > 0.5 or
                        (not a["_committable"] and h["dispatch"].get(gid,0) > 0.1))
        starts    = sum(1 for i,h in enumerate(hourly)
                        if h["startup"].get(gid,0) > 0.5)
        fuel_cost = energy * a["_dispMC"]
        sc_cost   = starts * float(a.get("startup_cost",0))
        nl_cost   = oper_h * float(a.get("no_load_cost",0))
        vom_cost  = energy * float(a.get("vom",0))
        gross     = fuel_cost + sc_cost + nl_cost + vom_cost
        gas_mm3   = energy * a["_gas_rate"]
        # Capacity-factor reference: pmax_installed for RE, pmax for thermal/hydro,
        # power_mw for BESS, scalar pmax_profile for imports (otherwise observed peak).
        if a.get("type") in ("wind", "solar"):
            pmax_inst = float(a.get("pmax_installed", 0) or 0)
        elif a.get("type") == "bess":
            pmax_inst = float(a.get("power_mw", 0) or 0)
        elif a.get("type") == "import":
            pp = a.get("pmax_profile")
            pmax_inst = float(pp) if isinstance(pp, (int, float)) else max(
                (h["dispatch"].get(gid, 0) for h in hourly), default=0.0)
        else:
            pmax_inst = float(a.get("pmax", 0) or 0)
        curt      = sum(h["curtailed_mwh"] for h in hourly
                        if a.get("type") in ("wind","solar"))  # simplified

        by_unit[gid] = {
            "name":          a.get("name", gid),
            "type":          a.get("type"),
            "energy_mwh":    round(energy, 1),
            "capacity_factor": round(energy / max(pmax_inst * H, 1) * 100, 2),
            "oper_hours":    oper_h,
            "starts":        starts,
            "fuel_cost":     round(fuel_cost, 0),
            "startup_cost":  round(sc_cost, 0),
            "no_load_cost":  round(nl_cost, 0),
            "vom_cost":      round(vom_cost, 0),
            "gross_cost":    round(gross, 0),
            "avg_cost_mwh":  round(gross / max(energy,1), 3),
            "gas_mm3":       round(gas_mm3, 4),
            "SRMC":          round(a["_dispMC"], 2),
            "curtailed_mwh": round(curt, 1)
        }

    # ── System summary ─────────────────────────────────────────────────
    total_cost    = sum(bu["gross_cost"]   for bu in by_unit.values())
    total_energy  = sum(bu["energy_mwh"]   for bu in by_unit.values())
    total_gas     = sum(bu["gas_mm3"]      for bu in by_unit.values())
    total_unserv  = sum(h["unserved_mwh"]  for h in hourly)
    total_curt    = sum(h["curtailed_mwh"] for h in hourly)
    avg_lam       = sum(h["lambda_usd_mwh"] for h in hourly) / H if H else 0
    peak_load     = max((h["load_mw"] for h in hourly), default=0)
    res_shortfall = {rid: sum(h["reserve_shortfall"].get(rid,0) for h in hourly) for rid in res_ids}

    # ── Gas binding check (Stage-1 patch: real binding detection) ──────
    gas_used_h        = [h["gas_mm3h"] for h in hourly]
    hours_gas_used    = sum(1 for v in gas_used_h if v > 1e-6)

    gas_mode    = gas_cfg.get("mode", "none")
    annual_cap  = (gas_cfg.get("annual") or {}).get("cap")
    gas_binding = False
    gas_util_pct = None
    if gas_mode != "none" and annual_cap and annual_cap > 0:
        # Scale annual cap to horizon; binding = utilization > 99%.
        window_cap = float(annual_cap) * (H / HOURS_PER_YEAR)
        used = sum(gas_used_h)
        gas_util_pct = round(used / max(window_cap, 1e-9) * 100, 2) if window_cap else None
        gas_binding  = (gas_util_pct is not None) and (gas_util_pct >= 99.0)
    # Per-hour limited count stays a proxy — refining requires per-hour duals
    # which HiGHS doesn't surface cleanly for MIP.  Deferred to Stage 2.
    hours_gas_limited = H if gas_binding else 0

    # ── Monthly aggregation (Stage-1 patch: real per-month rollups) ────
    study_year = inp.get("metadata",{}).get("study_year", 2026)
    start_h    = int(sh.get("start_hour", 0))
    month_map  = _build_month_map(study_year, start_h, H)

    monthly = []
    for mo in range(1, 13):
        idxs = [i for i, m in enumerate(month_map) if m == mo]
        if not idxs:
            continue
        # Real per-unit monthly breakdown — no placeholder spread.
        per_unit_mo = {}
        for gid, a in assets.items():
            m_energy = sum(hourly[i]["dispatch"].get(gid, 0) for i in idxs)
            m_starts = sum(1 for i in idxs if hourly[i]["startup"].get(gid, 0) > 0.5)
            m_oper   = sum(1 for i in idxs
                           if hourly[i]["commitment"].get(gid, 0) > 0.5
                           or (not a["_committable"] and hourly[i]["dispatch"].get(gid,0) > 0.1))
            m_fuel   = m_energy * a["_dispMC"]
            m_vom    = m_energy * float(a.get("vom", 0))
            m_sc     = m_starts * float(a.get("startup_cost", 0))
            m_nl     = m_oper   * float(a.get("no_load_cost", 0))
            m_gross  = m_fuel + m_vom + m_sc + m_nl
            per_unit_mo[gid] = {
                "energy_mwh":  round(m_energy, 1),
                "starts":      m_starts,
                "oper_hours":  m_oper,
                "fuel_cost":   round(m_fuel, 0),
                "gross_cost":  round(m_gross, 0),
                "gas_mm3":     round(m_energy * a["_gas_rate"], 4),
            }
        m_cost = sum(pu["gross_cost"] for pu in per_unit_mo.values())
        monthly.append({
            "month":          mo,
            "label":          month_abbr[mo],
            "hours":          len(idxs),
            "total_energy_mwh": round(sum(hourly[i]["generation_mw"] for i in idxs), 0),
            "total_cost_usd": round(m_cost, 0),
            "avg_lambda":     round(sum(hourly[i]["lambda_usd_mwh"] for i in idxs)/max(len(idxs),1), 2),
            "gas_mm3":        round(sum(hourly[i]["gas_mm3h"] for i in idxs), 3),
            "curtailed_mwh":  round(sum(hourly[i]["curtailed_mwh"] for i in idxs), 1),
            "unserved_mwh":   round(sum(hourly[i]["unserved_mwh"] for i in idxs), 1),
            "peak_load_mw":   round(max(hourly[i]["load_mw"] for i in idxs), 0),
            "by_unit":        per_unit_mo,
        })

    # ── Hydro end-storage warnings (Stage-1 patch: real detection) ─────
    hydro_end_warnings = []
    if hourly:
        last = hourly[-1]
        for gid, a in assets.items():
            if a.get("type") != "hydro_reg":
                continue
            end_min = float(a.get("hydro", {}).get("reservoir_end_min", 0))
            stor    = float(last.get("hydro", {}).get(gid, {}).get("storage_mm3", 0))
            if end_min > 0 and stor < end_min - 1e-3:
                hydro_end_warnings.append({
                    "asset":   gid,
                    "end_min": end_min,
                    "end_actual": round(stor, 3),
                    "shortfall":  round(end_min - stor, 3),
                })

    # ── Closure reconciliation (Stage-1 patch: real gap) ───────────────
    # reconstructed_cost: sum of the objective terms from hourly results.
    unserv_pen = float(s_cfg.get("unserved_penalty", 3000))
    res_pen_by = {rp["id"]: float(rp.get("shortfall_penalty", 500))
                  for rp in inp.get("reserve_products", [])}
    pen_unserved = unserv_pen * total_unserv
    pen_reserve  = sum(res_pen_by.get(rid, 0) * res_shortfall.get(rid, 0)
                       for rid in res_ids)
    reconstructed = total_cost + pen_unserved + pen_reserve

    if obj_total is None or obj_total != obj_total:      # NaN / not supplied
        closure_gap  = None
        closure_ok   = None
        closure_note = "objective unavailable; closure skipped"
    else:
        denom       = max(abs(obj_total), 1.0)
        closure_gap = abs(obj_total - reconstructed) / denom
        closure_ok  = closure_gap < 5e-3                  # 0.5% tolerance
        closure_note = (f"obj={obj_total:.2f} reconstructed={reconstructed:.2f} "
                        f"gap={closure_gap*100:.4f}%")

    # ── Provenance: echo profile_bundle into metadata ──────────────────
    fingerprint = {
        "profile_bundle":    pbundle,
        "input_file_hashes": (pbundle.get("file_hashes") or {}) if pbundle else {},
        "loader_version":    (pbundle.get("generated_by") if pbundle else None),
        "solver_version":    SOLVER_VERSION,
    }

    result = {
        "metadata": {
            "model_version":  MODEL_VERSION,
            "schema_version": SCHEMA_VERSION,
            "scenario":       sc_meta.get("id","A_mean"),
            "solved_at":      datetime.now().isoformat(),
            "study_start":    inp.get("time_index",[""])[sh.get("start_hour",0)] if inp.get("time_index") else "",
            "horizon_hours":  H,
            "resolution_h":   RESOLUTION_H,
            "closure_ok":     closure_ok,
            "closure_gap":    None if closure_gap is None else round(closure_gap, 6),
            "closure_note":   closure_note,
            "data_source_fingerprint": fingerprint,
        },
        "diagnostics": {
            "solver_status":           "solved",
            "solver_version":          SOLVER_VERSION,
            "solve_time_s":            round(solve_time, 2),
            "mip_gap_pct":             float(s_cfg.get("mip_gap",0.005)) * 100,
            "infeasible_flag":         total_unserv > 0.1,
            "unserved_hours":          sum(1 for h in hourly if h["unserved_mwh"] > 0.1),
            "reserve_shortfall_hours": {rid: sum(1 for h in hourly if h["reserve_shortfall"].get(rid,0) > 0.1) for rid in res_ids},
            "gas_cap_binding":         gas_binding,
            "gas_utilization_pct":     gas_util_pct,
            "hours_gas_used":          hours_gas_used,
            "hydro_end_storage_warnings": hydro_end_warnings,
            "rolling_boundary_warnings":  [],
            "n_assets":                len(assets),
            "n_reserves":              len(res_ids),
            "output_schema_warnings":  [],    # filled in after validation below
        },
        "system_summary": {
            "total_cost_usd":      round(total_cost, 0),
            "total_energy_mwh":    round(total_energy, 0),
            "avg_cost_usd_mwh":    round(total_cost / max(total_energy,1), 3),
            "avg_lambda_usd_mwh":  round(avg_lam, 3),
            "peak_load_mw":        round(peak_load, 1),
            "total_gas_mm3":       round(total_gas, 4),
            "total_unserved_mwh":  round(total_unserv, 2),
            "total_curtailed_mwh": round(total_curt, 2),
            "total_fuel_cost":     round(sum(bu["fuel_cost"] for bu in by_unit.values()), 0),
            "total_startup_cost":  round(sum(bu["startup_cost"] for bu in by_unit.values()), 0),
            "hours_gas_used":      hours_gas_used,
            "reserve_shortfall_mwh": res_shortfall
        },
        "hourly_system": [
            {
                "t":             h["t"],
                "load_mw":       h["load_mw"],
                "generation_mw": h["generation_mw"],
                "lambda_usd_mwh": h["lambda_usd_mwh"],
                "lambda_source": h["lambda_source"],
                "unserved_mwh":  h["unserved_mwh"],
                "curtailed_mwh": h["curtailed_mwh"],
                "gas_mm3h":      h["gas_mm3h"],
                "reserve_shortfall": h["reserve_shortfall"]
            }
            for h in hourly
        ],
        "hourly_by_unit": {
            gid: [
                {
                    "t":             h["t"],
                    "dispatch_mw":   h["dispatch"].get(gid, 0),
                    "commitment":    h["commitment"].get(gid, 1 if not assets[gid]["_committable"] else 0),
                    "startup":       h["startup"].get(gid, 0),
                    "shutdown":      h["shutdown"].get(gid, 0),
                    "reserve_up":    {rid: h["reserve_up"].get(rid,{}).get(gid,0) for rid in res_ids},
                    "reserve_down":  {rid: h["reserve_down"].get(rid,{}).get(gid,0) for rid in res_ids},
                    "gas_mm3h":      round(h["dispatch"].get(gid,0) * assets[gid]["_gas_rate"], 6),
                    "hydro":         h["hydro"].get(gid,{}),
                    "bess":          h["bess"].get(gid,{})
                }
                for h in hourly
            ]
            for gid in assets
        },
        "by_unit_summary": by_unit,
        "monthly_summary": monthly,
        "stochastic_summary": None
    }

    # ── Output-side validation hook (Stage-1 patch) ────────────────────
    try:
        ok, errs, warns = validate_output(result)
        if errs:
            print(f"⚠️  Output validation produced {len(errs)} error(s):")
            for e in errs[:5]: print(f"     - {e}")
        if warns:
            print(f"ℹ️  Output validation produced {len(warns)} warning(s):")
            for w in warns[:5]: print(f"     - {w}")
        result["diagnostics"]["output_schema_ok"]       = ok
        result["diagnostics"]["output_schema_errors"]   = errs
        result["diagnostics"]["output_schema_warnings"] = warns
    except Exception as e:                                   # pragma: no cover
        print(f"⚠️  validate_output raised: {e}")

    return result


def _build_month_map(year: int, start_h: int, horizon_h: int) -> list:
    """Return list of month numbers for each hour in the horizon."""
    base = datetime(year, 1, 1) + timedelta(hours=start_h)
    return [(base + timedelta(hours=i)).month for i in range(horizon_h)]


# ══════════════════════════════════════════════════════════════════════
# 8. EXCEL EXPORT (PLEXOS-style)
# ══════════════════════════════════════════════════════════════════════

def export_excel(results: dict, filename: str = "powersim_results.xlsx"):
    """
    Export to Excel with sheets:
      Summary | Hourly_System | Hourly_By_Unit | Commitment
      Reserves | Hydro | Fuel_Gas | Curtailment | Diagnostics
      Monthly_System | Monthly_By_Unit | Monthly_Gas
    """
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        wb  = writer.book
        hdr = wb.add_format({"bold":True, "bg_color":"#0e2040",
                              "font_color":"#e8f0fa", "border":1})
        num = wb.add_format({"num_format":"#,##0.00"})

        def sheet(name, df):
            df.to_excel(writer, sheet_name=name, index=False)
            ws = writer.sheets[name]
            ws.set_row(0, 18, hdr)
            ws.set_column(0, len(df.columns)-1, 13)

        # Summary
        sm = results["system_summary"]
        meta = results["metadata"]
        diag = results["diagnostics"]
        sum_rows = [
            ["Scenario",      meta.get("scenario"),           "—"],
            ["Horizon Hours", meta.get("horizon_hours"),      "h"],
            ["Solved At",     meta.get("solved_at",""),        "—"],
            ["Total Cost",    sm["total_cost_usd"],            "$"],
            ["Total Energy",  sm["total_energy_mwh"],          "MWh"],
            ["Avg Cost",      sm["avg_cost_usd_mwh"],          "$/MWh"],
            ["Avg Lambda",    sm["avg_lambda_usd_mwh"],        "$/MWh"],
            ["Peak Load",     sm["peak_load_mw"],              "MW"],
            ["Total Gas",     sm["total_gas_mm3"],             "Mm³"],
            ["Unserved",      sm["total_unserved_mwh"],        "MWh"],
            ["Curtailed",     sm["total_curtailed_mwh"],       "MWh"],
            ["Solve Time",    diag["solve_time_s"],            "s"],
            ["Closure OK",    str(meta.get("closure_ok","")), "bool"],
        ]
        sheet("Summary", pd.DataFrame(sum_rows, columns=["Metric","Value","Unit"]))

        # Hourly System
        sheet("Hourly_System", pd.DataFrame(results["hourly_system"]))

        # Hourly By Unit (wide)
        # NOTE: column names use the asset ID (stable, unique) rather than
        # a truncation of the display name.  The display-name approach
        # collided on duplicates like ვარციხეჰესი 1/2/3/4 after 10-char
        # truncation, causing a pandas MergeError.  Asset IDs are already
        # unique and ASCII-safe by construction (see asset mapper).
        hbu = results["hourly_by_unit"]
        all_h = results["hourly_system"]
        if hbu and all_h:
            base = pd.DataFrame(all_h)[["t","load_mw","lambda_usd_mwh","unserved_mwh"]]
            for gid, rows in hbu.items():
                df_g  = pd.DataFrame(rows)[["t","dispatch_mw","commitment"]]
                df_g  = df_g.rename(columns={"dispatch_mw":f"{gid}_MW",
                                             "commitment":  f"{gid}_ON"})
                base  = base.merge(df_g, on="t", how="left")
            sheet("Hourly_By_Unit", base)

        # Commitment sheet — also uses asset IDs for the same reason.
        if hbu:
            comm_df = pd.DataFrame({"t": [h["t"] for h in all_h]})
            for gid, rows in hbu.items():
                comm_df[gid] = [r["commitment"] for r in rows]
            sheet("Commitment", comm_df)

        # By Unit Summary
        bu_rows = [[gid]+list(d.values()) for gid,d in results["by_unit_summary"].items()]
        if bu_rows:
            cols = ["id"] + list(results["by_unit_summary"][list(results["by_unit_summary"].keys())[0]].keys())
            sheet("By_Unit_Summary", pd.DataFrame(bu_rows, columns=cols))

        # Monthly
        if results["monthly_summary"]:
            sheet("Monthly_System", pd.DataFrame(results["monthly_summary"]))

        # Diagnostics
        diag_rows = [[k, str(v)] for k,v in diag.items()]
        sheet("Diagnostics", pd.DataFrame(diag_rows, columns=["Key","Value"]))

        # Gas
        gas_rows = []
        for h in all_h:
            gas_rows.append({"t": h["t"], "gas_mm3h": h["gas_mm3h"]})
        sheet("Fuel_Gas", pd.DataFrame(gas_rows))

        # Curtailment
        curt_rows = [{"t":h["t"],"curtailed_mwh":h["curtailed_mwh"]} for h in all_h]
        sheet("Curtailment", pd.DataFrame(curt_rows))

    print(f"   ✅ Excel: {filename} ({len(results['by_unit_summary'])} assets, "
          f"{results['metadata']['horizon_hours']}h)")
    return filename


# ══════════════════════════════════════════════════════════════════════
# 9. DEMO INPUT (fallback when no JSON provided)
# ══════════════════════════════════════════════════════════════════════

def _demo_input() -> dict:
    """Minimal GSE 2026 demo: 24h × 4 assets."""
    import random; random.seed(42)
    demand_24 = [420,400,385,375,370,388,435,490,
                 540,575,600,615,608,590,575,582,
                 610,645,658,638,605,560,510,465]
    wind_cf   = [.36,.39,.42,.44,.41,.36,.28,.22,.19,.17,.14,.11,
                 .10,.12,.16,.21,.27,.34,.40,.44,.46,.43,.41,.38]
    inflow    = [0.08]*24
    return {
        "metadata":   {"model_version": MODEL_VERSION, "schema_version": SCHEMA_VERSION,
                       "timezone": "Asia/Tbilisi", "study_year": 2026},
        "time_index": None,
        "study_horizon": {"start_hour": 0, "horizon_hours": 24, "mode": "auto"},
        "assets": [
            {"id":"enguri","name":"ენგური","type":"hydro_reg","committable":True,
             "pmin":195,"pmax":1300,"ramp_up":200,"ramp_down":200,
             "min_up":0,"min_down":0,"startup_cost":0,"no_load_cost":0,"vom":0,
             "hydro":{"reservoir_init":700,"reservoir_min":100,"reservoir_max":1100,
                      "reservoir_end_min":500,"efficiency":350,"spill_cost":0,"water_value":18,
                      "cascade_upstream":None,"travel_delay_h":0,"conversion_mode":2},
             "inflow_profile":"enguri_inflow"},
            {"id":"gardabani_1","name":"გარდაბანი TPP-1","type":"thermal","committable":True,
             "pmin":92,"pmax":231.2,"heat_rate":6.8,"fuel_type":"gas","fuel_price":7.0,
             "vom":2,"startup_cost":12000,"no_load_cost":600,
             "ramp_up":50,"ramp_down":50,"min_up":3,"min_down":2},
            {"id":"kartli_wind","name":"ქართლის ქარი","type":"wind","committable":False,
             "pmax_installed":20.7,"vom":0,"curtailment_cost":0,
             "availability_profile":"wind_cf"},
            {"id":"import_tr","name":"TR იმპ.","type":"import","committable":False,
             "vom":55,"pmax_profile":700.0}
        ],
        "profiles": {
            "demand":        demand_24,
            "enguri_inflow": inflow,
            "wind_cf":       wind_cf,
        },
        "gas_constraints": {
            "mode": "annual",
            "unit": "Mm3",
            "annual": {"cap": 1200.0},
            "applies_to": ["gardabani_1"]
        },
        "reserve_products": [
            {"id":"FCR","name":"FCR","direction":"symmetric","requirement":60.0,
             "shortfall_penalty":500,"eligible_units":["enguri","gardabani_1"],
             "response_time_label":"30s","derating_factors":{}}
        ],
        "solver_settings": {
            "mip_gap":0.005,"time_limit_s":120,
            "rolling_window_h":168,"rolling_step_h":24,
            "unserved_penalty":3000,"curtailment_penalty":0
        },
        "scenario_metadata": {"id":"A_mean","label":"Base","probability":1.0}
    }


# ══════════════════════════════════════════════════════════════════════
# 10. STOCHASTIC WRAPPER
# ══════════════════════════════════════════════════════════════════════

# Stage-1 fallback — used only when input does not declare stochastic_scenarios.
# Kept here as a default so existing workflows don't break, but the source of
# truth is inp["stochastic_scenarios"] (list of {id,label,prob}).
_DEFAULT_STOCH_SCENARIOS = [
    {"id":"MC_P10","label":"P10 (wet)","prob":0.20},
    {"id":"A_mean","label":"Base",      "prob":0.60},
    {"id":"MC_P90","label":"P90 (dry)", "prob":0.20},
]
CVaR_ALPHA = 0.95


def _resolve_stoch_scenarios(inp: dict) -> list:
    """
    Read stochastic scenarios from input if present; otherwise fall back
    to the default P10/Base/P90 triplet. Normalises shape and probabilities.
    """
    scs = inp.get("stochastic_scenarios")
    if not scs:
        # Also accept the plural-less spelling some callers may use.
        scs = inp.get("stochastic_scenario_set") or _DEFAULT_STOCH_SCENARIOS

    out, total = [], 0.0
    for sc in scs:
        sid   = sc.get("id")
        if not sid:
            continue
        label = sc.get("label", sid)
        prob  = float(sc.get("prob", sc.get("probability", 0.0)))
        out.append({"id": sid, "label": label, "prob": prob})
        total += prob
    if not out:
        return list(_DEFAULT_STOCH_SCENARIOS)
    # Renormalise if probabilities don't sum to ~1.
    if total > 0 and abs(total - 1.0) > 1e-6:
        for s in out:
            s["prob"] = s["prob"] / total
    return out


def run_stochastic(inp: dict) -> dict:
    """Run UC for each scenario and compute E[Cost], CVaR."""
    scenarios = _resolve_stoch_scenarios(inp)
    print(f"🎲 Stochastic run with {len(scenarios)} scenario(s) "
          f"(source: {'input' if inp.get('stochastic_scenarios') else 'default'})")
    results = {}
    for sc in scenarios:
        print(f"\n── Scenario {sc['label']} (π={sc['prob']:.3f}) ──")
        inp_sc = dict(inp)
        inp_sc["scenario_metadata"] = {"id":sc["id"],"label":sc["label"],"probability":sc["prob"]}
        assets   = build_asset_map(inp_sc)
        profiles, H = slice_profiles(inp_sc)
        gas_lim  = build_gas_limits(inp_sc, int(inp_sc.get("study_horizon",{}).get("start_hour",0)), H)
        hourly, dt, _obj = solve_all(inp_sc, assets, profiles, gas_lim)
        results[sc["id"]] = {
            "prob": sc["prob"], "label": sc["label"],
            "total_cost": sum(assets[g]["_dispMC"]*h["dispatch"].get(g,0)
                              for h in hourly for g in assets),
            "avg_lambda": sum(h["lambda_usd_mwh"] for h in hourly) / max(len(hourly),1),
            "total_unserved": sum(h["unserved_mwh"] for h in hourly)
        }
    # Aggregate
    active = [s for s in scenarios if s["id"] in results]
    exp_cost = sum(s["prob"]*results[s["id"]]["total_cost"] for s in active)
    costs_s  = sorted([{"cost":results[s["id"]]["total_cost"],"prob":s["prob"],"label":s["label"]}
                        for s in active], key=lambda x: -x["cost"])
    tail = 1 - CVaR_ALPHA; cum = 0; cvar_n = 0; cvar_d = 0
    for c in costs_s:
        if cum >= tail: break
        contrib = min(c["prob"], tail-cum)
        cvar_n += c["cost"]*contrib; cvar_d += contrib; cum += c["prob"]
    cvar = cvar_n/cvar_d if cvar_d > 0 else (costs_s[0]["cost"] if costs_s else 0)
    return {
        "expected_cost": round(exp_cost, 0),
        "cvar95":        round(cvar, 0),
        "risk_premium":  round(cvar - exp_cost, 0),
        "scenarios":     [{"id":s["id"],"label":s["label"],"prob":s["prob"],
                           **results[s["id"]]} for s in active]
    }


# ══════════════════════════════════════════════════════════════════════
# 11. MAIN
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PowerSim v4.0 Solver")
    parser.add_argument("--input",      default="powersim_input.json")
    parser.add_argument("--output",     default="powersim_results.json")
    parser.add_argument("--excel",      default="powersim_results.xlsx")
    parser.add_argument("--stochastic", action="store_true")
    parser.add_argument("--ed-resolve", action="store_true",
                        help="Run LP ED resolve for accurate marginal prices")
    args = parser.parse_args()

    print("=" * 60)
    print("⚡ PowerSim v4.0 — MIP UC/ED Solver")
    print("   Resolution: 1h | GSE ER&A")
    print("=" * 60)

    # Load
    inp      = load_input(args.input)
    assets   = build_asset_map(inp)
    profiles, H = slice_profiles(inp)
    gas_lim  = build_gas_limits(inp, int(inp.get("study_horizon",{}).get("start_hour",0)), H)

    print(f"\n   Assets: {len(assets)} | Horizon: {H}h | "
          f"Gas mode: {gas_lim['mode']}")

    # Solve
    t0                        = time.time()
    hourly, dt_mip, obj_total = solve_all(inp, assets, profiles, gas_lim)
    total_time                = time.time() - t0

    # Optional: LP ED resolve for accurate lambdas
    reserve_prods = inp.get("reserve_products", [])
    s_cfg         = inp.get("solver_settings", {})
    if args.ed_resolve:
        hourly = compute_marginal_prices(hourly, assets, profiles, reserve_prods, s_cfg)

    # Build results  (obj_total passed in so closure check compares against
    # the real Pyomo objective, not a self-referential expression)
    results = build_result_store(hourly, assets, inp, total_time, obj_total=obj_total)

    # Print summary
    sm = results["system_summary"]
    print(f"\n{'='*60}")
    print(f"✅ Solved!")
    print(f"   Total Cost:  ${sm['total_cost_usd']:>14,.0f}")
    print(f"   Total Energy:{sm['total_energy_mwh']:>14,.0f} MWh")
    print(f"   Avg λ:       ${sm['avg_lambda_usd_mwh']:>10.3f}/MWh")
    print(f"   Peak Load:   {sm['peak_load_mw']:>14.0f} MW")
    print(f"   Gas:         {sm['total_gas_mm3']:>14.3f} Mm³")
    print(f"   Unserved:    {sm['total_unserved_mwh']:>14.1f} MWh")
    print(f"   Solve time:  {total_time:>14.1f}s")
    print()
    bu = results["by_unit_summary"]
    for gid, bg in bu.items():
        print(f"   {bg['name']:24s} {bg['energy_mwh']:8,.0f}MWh  "
              f"CF:{bg['capacity_factor']:5.1f}%  "
              f"${bg['gross_cost']:>12,.0f}")
    print(f"{'='*60}")

    # Stochastic
    if args.stochastic:
        print("\n🎲 Stochastic UC — P10/Base/P90...")
        stoch = run_stochastic(inp)
        results["stochastic_summary"] = stoch
        print(f"   E[Cost]:    ${stoch['expected_cost']:>12,.0f}")
        print(f"   CVaR₉₅:     ${stoch['cvar95']:>12,.0f}")
        print(f"   Risk Prem.: ${stoch['risk_premium']:>12,.0f}")

    # Save
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON:  {args.output}")
    export_excel(results, args.excel)

    # Colab download
    try:
        from google.colab import files
        files.download(args.output)
        files.download(args.excel)
        print("✅ Files downloaded!")
    except ImportError:
        print(f"✅ Files saved locally: {args.output}, {args.excel}")
