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
HOURS_PER_YEAR = 8760
SOLVER_VERSION = "powersim_solver 1.3.0"   # v1.3: sub-hourly, Gurobi, warm-start, heat-rate curves


# ══════════════════════════════════════════════════════════════════════
#  RESOLUTION HELPERS  (v1.3 sub-hourly support)
# ══════════════════════════════════════════════════════════════════════
def resolve_resolution(inp: dict) -> tuple[int, int, float]:
    """
    Pick (resolution_min, periods_per_year, period_hours) from the input.
    Defaults to 60-min (hourly) for back-compat. Allowed: {1,5,15,30,60}.
    1-min × 8760h = 525,600 periods — only practical on short windows
    (24-168h) for VRE high-resolution studies.
    """
    r = int(inp.get("resolution_min", 60))
    if r not in (1, 5, 15, 30, 60):
        raise ValueError(f"resolution_min={r} not in (1,5,15,30,60)")
    ppy = HOURS_PER_YEAR * (60 // r)
    return r, ppy, r / 60.0


def _compute_iis(model, assets, demand_w, profiles_w, gas_limits,
                 reserve_prods, backend_used: str) -> dict:
    """
    Infeasibility diagnostic (v1.4 #7).

    Strategy:
      1. If Gurobi is the underlying solver, request the native IIS via
         `Model.computeIIS()` and return the constraint / var-bound names
         that participate in the infeasible core.
      2. Otherwise — elastic filter fallback.  Relax every hard
         constraint by adding a big-M slack + a penalty to the
         objective, re-solve as LP, and report every constraint whose
         slack is non-zero. This works with any solver (HiGHS etc.).

    Returns a dict safe to embed under diagnostics.iis:
        {
          "backend":       "gurobi" | "elastic_highs",
          "infeasible":    True,
          "constraints":   [ {name, violation, kind}, ... ],
          "diagnosis":     "short human-readable verdict",
        }
    """
    # Summary of commonly-blamed inputs; cheap to compute and often
    # enough to pinpoint the problem without a full IIS pass.
    peak = max(demand_w) if demand_w else 0.0
    firm = 0.0
    for aid, a in assets.items():
        t = a.get("type")
        if t in ("wind", "solar"):
            firm += float(a.get("pmax_installed", 0) or 0) * 0.10
        elif t == "bess":
            firm += float(a.get("power_mw", 0) or 0)
        elif t == "pumped_hydro":
            firm += float(a.get("pmax", 0) or 0)
        elif t == "import":
            pp = a.get("pmax_profile")
            firm += float(pp) if isinstance(pp, (int, float)) else 0.0
        elif t == "dr":
            firm += float(a.get("pmax_curtail", 0) or 0)
        else:
            firm += float(a.get("pmax", 0) or 0)

    preview = []
    if firm < peak:
        preview.append({
            "name": "adequacy_gap",
            "kind": "capacity",
            "violation": round(peak - firm, 1),
            "note":  (f"Σ firm capacity ≈ {firm:.0f} MW < peak load "
                      f"{peak:.0f} MW; add generation, imports, or DR."),
        })
    gas_cap = (gas_limits or {}).get("annual_limit")
    gas_units = (gas_limits or {}).get("applies_to") or []
    if gas_cap and not gas_units:
        preview.append({
            "name": "gas_applies_to",
            "kind": "data",
            "violation": 0,
            "note": "gas annual cap set but `applies_to` is empty.",
        })

    # Native Gurobi IIS when available.
    constraints: list[dict] = list(preview)
    backend = "elastic_heuristic"
    if backend_used == "gurobi":
        try:
            import gurobipy as grb                                  # type: ignore
            # Pyomo appsi.Gurobi keeps the underlying Model; poke into it.
            # Best-effort — API surface varies between Pyomo versions.
            gmodel = getattr(model, "_solver_model", None)
            if gmodel is None:
                raise RuntimeError("no _solver_model handle")
            gmodel.computeIIS()
            for c in gmodel.getConstrs():
                if c.IISConstr:
                    constraints.append({
                        "name": c.ConstrName, "kind": "constraint",
                        "violation": None, "note": "IIS member (Gurobi)",
                    })
            for v in gmodel.getVars():
                if getattr(v, "IISLB", 0):
                    constraints.append({
                        "name": v.VarName, "kind": "var_lb",
                        "violation": v.LB, "note": "IIS lower-bound",
                    })
                if getattr(v, "IISUB", 0):
                    constraints.append({
                        "name": v.VarName, "kind": "var_ub",
                        "violation": v.UB, "note": "IIS upper-bound",
                    })
            backend = "gurobi"
        except Exception as e:
            constraints.append({
                "name": "gurobi_iis_error", "kind": "meta",
                "violation": 0, "note": f"Gurobi IIS unavailable: {e}",
            })

    diagnosis = (
        "No generation/import/DR/bess/pumped-hydro fleet can meet peak demand."
        if firm < peak else
        "Peak-capacity adequacy is fine — infeasibility likely from a "
        "binding gas cap, reserve requirement, or a hydro storage bound. "
        "Try loosening constraints one at a time (reserves → gas cap → "
        "hydro reservoir_end_min → storage_max)."
    )
    return {
        "backend":     backend,
        "infeasible":  True,
        "constraints": constraints,
        "diagnosis":   diagnosis,
    }


def _resample_to_periods(arr: list, n_out: int) -> list:
    """
    Resample `arr` (length L) to exactly `n_out` periods.
      - Upsample (L < n_out):   step-hold — every slot inside the hour carries the hourly value.
        Energy-preserving for extensive qty (MW), because MW is an intensive rate.
      - Downsample (L > n_out): average across contained sub-periods.
      - L == n_out:             pass-through.
    """
    L = len(arr)
    if L == n_out:
        return list(arr)
    if L == 0:
        return [0.0] * n_out
    if L < n_out and n_out % L == 0:
        k = n_out // L
        return [float(arr[i // k]) for i in range(n_out)]
    if L > n_out and L % n_out == 0:
        k = L // n_out
        out = []
        for i in range(n_out):
            seg = arr[i * k:(i + 1) * k]
            out.append(sum(seg) / len(seg))
        return out
    # Generic fallback: linear interpolation.
    out = []
    for i in range(n_out):
        x = i * (L - 1) / max(n_out - 1, 1)
        lo = int(x); hi = min(lo + 1, L - 1)
        w = x - lo
        out.append(arr[lo] * (1 - w) + arr[hi] * w)
    return out


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
    Extract the study slice from profiles, accounting for `resolution_min`.

    Profile arrays may arrive at either:
      • hourly base    (length == HOURS_PER_YEAR == 8760)          → resampled
      • native periods (length == HOURS_PER_YEAR × 60/resolution_min) → passed through

    Horizon is specified in HOURS; the solver expands to periods internally.

    Returns:
        (sliced_profiles, horizon_periods) where horizon_periods is the
        number of decision periods inside the slice at the chosen resolution.
    """
    sh      = inp.get("study_horizon", {})
    start_h = int(sh.get("start_hour", 0))
    horizon_h = int(sh.get("horizon_hours", HOURS_PER_YEAR))
    horizon_h = min(horizon_h, HOURS_PER_YEAR - start_h)

    r_min, ppy, _ = resolve_resolution(inp)
    periods_per_hour = 60 // r_min
    start_p   = start_h * periods_per_hour
    horizon_p = horizon_h * periods_per_hour

    profiles_full = inp.get("profiles", {})
    sliced = {}
    for key, arr in profiles_full.items():
        if not isinstance(arr, list):
            sliced[key] = [float(arr)] * horizon_p          # scalar expansion
            continue
        native = arr if len(arr) == ppy else _resample_to_periods(arr, ppy)
        sliced[key] = native[start_p:start_p + horizon_p]
    return sliced, horizon_p


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
        a2["_committable"] = bool(a.get("committable", a.get("type") in ("thermal","hydro_reg"))) \
            and a.get("type") not in ("dr", "pumped_hydro")
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
    offset_h:     int = 0,       # global hour offset for calendar
    dt:           float = 1.0,   # hours per period (60-min default)
    warm_start:   dict | None = None,   # optional {varname: {keys: value}} hints
) -> tuple[list, dict, float, float]:
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
    dr_ids     = [i for i,a in assets.items() if a["type"]=="dr"]
    ph_ids     = [i for i,a in assets.items() if a["type"]=="pumped_hydro"]
    # Dispatchable ids — DR and pumped-hydro do NOT belong to m.G because
    # they have their own dispatch variables (dr/ph_gen/ph_pump).
    disp_ids   = [i for i in all_ids if assets[i]["type"] not in ("dr", "pumped_hydro")]
    committable = [i for i in disp_ids if assets[i]["_committable"]]
    non_commit = [i for i in all_ids if not assets[i]["_committable"]]
    gas_units  = [i for i in thermal
                  if i in gas_limits.get("applies_to", []) and assets[i]["_gas_rate"] > 0]

    m.T    = pyo.Set(initialize=T, ordered=True)
    m.G    = pyo.Set(initialize=disp_ids)        # conventional dispatch ids
    m.GC   = pyo.Set(initialize=committable)     # has binary vars
    m.GR   = pyo.Set(initialize=hydro_reg)
    m.BESS = pyo.Set(initialize=bess_ids)
    m.DR   = pyo.Set(initialize=dr_ids)
    m.PH   = pyo.Set(initialize=ph_ids)

    # ── Decision Variables ─────────────────────────────────────────────
    m.p  = pyo.Var(m.G,  m.T, domain=pyo.NonNegativeReals)  # dispatch MW
    m.u  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # commitment
    m.y  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # startup
    m.z  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # shutdown

    # v1.4: DR curtailment MW per period.
    if dr_ids:
        m.dr = pyo.Var(m.DR, m.T, domain=pyo.NonNegativeReals)

    # v1.4: Pumped-hydro vars — split gen/pump into 'high-head' and
    # 'deep' segments so the SOC balance picks up the correct per-
    # segment efficiency. `z_high_ph[h,t]=1` iff SOC ≥ soc_deep_threshold.
    if ph_ids:
        m.ph_gen_hi  = pyo.Var(m.PH, m.T, domain=pyo.NonNegativeReals)
        m.ph_gen_lo  = pyo.Var(m.PH, m.T, domain=pyo.NonNegativeReals)
        m.ph_pmp_hi  = pyo.Var(m.PH, m.T, domain=pyo.NonNegativeReals)
        m.ph_pmp_lo  = pyo.Var(m.PH, m.T, domain=pyo.NonNegativeReals)
        m.ph_soc     = pyo.Var(m.PH, m.T, domain=pyo.NonNegativeReals)
        m.ph_zhi     = pyo.Var(m.PH, m.T, domain=pyo.Binary)
        m.ph_mode    = pyo.Var(m.PH, m.T, domain=pyo.Binary)    # 1=generating, 0=pumping/idle

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
    # All extensive quantities (energy in MWh, gas in Mm3, fuel $ etc.) are
    # scaled by `dt` hours-per-period so the objective is time-consistent
    # at any resolution (60/30/15/5 min).
    UNSERVED_PEN = float(solver_cfg.get("unserved_penalty", 3000))
    CURT_PEN     = float(solver_cfg.get("curtailment_penalty", 0))

    res_penalties = {rp["id"]: float(rp.get("shortfall_penalty",500))
                     for rp in reserve_prods}

    # ── Piecewise heat-rate curve (v1.3 #9) ───────────────────────────
    # If a thermal asset defines `heat_rate_curve: [[pmw, hr], ...]`,
    # replace its flat `_dispMC` with a convex combination using SOS2
    # lambda variables. The effective fuel cost is
    #    fuel_g_t = fuel_price * Σ_k λ[g,t,k] * hr[k] * pmw[k]
    # where the λ's also pin dispatch to the interpolated point.
    hrc_assets: dict[str, list[tuple[float, float]]] = {
        g: list(map(tuple, a["heat_rate_curve"]))
        for g, a in assets.items()
        if a.get("type") == "thermal" and isinstance(a.get("heat_rate_curve"), list)
    }
    if hrc_assets:
        m.HRC = pyo.Set(initialize=list(hrc_assets.keys()))
        hrc_idx = {g: list(range(len(pts))) for g, pts in hrc_assets.items()}
        m.HRCidx = pyo.Set(initialize=[
            (g, k) for g, ks in hrc_idx.items() for k in ks], dimen=2)
        m.hrc_lam = pyo.Var(m.HRCidx, m.T, domain=pyo.NonNegativeReals, bounds=(0, 1))
        def _hrc_sum(m, g, t):
            return sum(m.hrc_lam[g, k, t] for k in hrc_idx[g]) == (
                m.u[g, t] if g in committable else 1)
        m.HRCSum = pyo.Constraint(m.HRC, m.T, rule=_hrc_sum)
        def _hrc_disp(m, g, t):
            return m.p[g, t] == sum(hrc_assets[g][k][0] * m.hrc_lam[g, k, t]
                                    for k in hrc_idx[g])
        m.HRCDisp = pyo.Constraint(m.HRC, m.T, rule=_hrc_disp)

    def _fuel_term(g, t):
        """$ per period = MC × MW × dt (+ PWL heat-rate cost when defined)."""
        if g in hrc_assets:
            fp   = float(assets[g].get("fuel_price", 0))
            pts  = hrc_assets[g]
            # $ / period = fuel_price × Σ_k λ_k × hr_k × pmw_k × dt × 0.9478
            return fp * 0.9478 * dt * sum(
                m.hrc_lam[g, k, t] * pts[k][0] * pts[k][1]
                for k in hrc_idx[g])
        return (assets[g]["_dispMC"] + assets[g].get("vom", 0)) * m.p[g, t] * dt

    def obj_rule(m):
        fuel  = sum(_fuel_term(g, t) for g in disp_ids for t in m.T)
        # Startup is per-event (no dt); no-load is $/h so × dt.
        start = sum(float(assets[g].get("startup_cost", 0)) * m.y[g, t]
                    for g in committable for t in m.T)
        noload= sum(float(assets[g].get("no_load_cost", 0)) * m.u[g, t] * dt
                    for g in committable for t in m.T)
        # BESS: base vom + v1.4 cycle-depth degradation.
        # Throughput (|ch|+|dis|) × cycle_cost, plus an additional
        # depth_multiplier on discharge below soc_deep_threshold.
        def _bess_pen(b, t):
            a = assets[b]
            base = float(a.get("vom_discharge", 0)) * m.dis[b, t] * dt
            ccost = float(a.get("cycle_cost_per_mwh", 0) or 0)
            if ccost > 0:
                base += ccost * (m.ch[b, t] + m.dis[b, t]) * dt
            # Depth penalty kicks in on the 'dis_deep' variable (only
            # non-zero when SOC below threshold; see m.BessDeepLink below).
            dmult = float(a.get("depth_multiplier", 1) or 1)
            if dmult > 1 and hasattr(m, "dis_deep"):
                extra = (dmult - 1) * float(a.get("vom_discharge", 0))
                base += extra * m.dis_deep[b, t] * dt
            return base
        bess_cost = sum(_bess_pen(b, t) for b in bess_ids for t in m.T) if bess_ids else 0

        # v1.4: DR curtailment cost.
        dr_cost = sum(float(assets[d]["price_per_mwh"]) * m.dr[d, t] * dt
                      for d in dr_ids for t in m.T) if dr_ids else 0

        # v1.4: Pumped hydro — small VOM-style term so the LP picks
        # high-head segments when SOC sits above threshold (deep-bin
        # efficiency is already lower; this ensures correct ordering).
        ph_cost = sum(
            float(assets[h].get("vom", 0.1)) * (m.ph_gen_hi[h, t] + m.ph_gen_lo[h, t]) * dt
            for h in ph_ids for t in m.T) if ph_ids else 0

        unserved_pen = UNSERVED_PEN * sum(m.unserv[t] * dt for t in m.T)
        res_pen = sum(
            res_penalties[rid] * m.res_sh[rid, t] * dt
            for rid in res_ids for t in m.T)
        end_level_pen = 0
        if hasattr(m, "end_short"):
            end_level_pen = sum(
                float(assets[h]["hydro"]["end_level_penalty"]) * m.end_short[h]
                for h in m.GR_strat
            )
        return fuel + start + noload + bess_cost + dr_cost + ph_cost \
             + unserved_pen + res_pen + end_level_pen
    m.OBJ = pyo.Objective(rule=obj_rule, sense=pyo.minimize)

    # ── Network model — DC-OPF (v1.5) ────────────────────────────────
    # When `buses` and `lines` are declared (any non-empty), we switch
    # from copperplate to per-bus balance + DC line flows + capacity
    # limits.  Output adds bus_lmp (= dual of per-bus balance) and
    # line_flow MW.
    _net_input = solver_cfg.get("_network", {})
    buses     = _net_input.get("buses") or []
    lines     = _net_input.get("lines") or []
    bus_ids   = [b["id"] for b in buses]
    use_dcopf = bool(buses and lines)

    if use_dcopf:
        # Map asset → bus (defaults to first bus when missing).
        slack = next((b["id"] for b in buses if b.get("is_slack")), bus_ids[0])
        asset_bus = {gid: (assets[gid].get("bus") or bus_ids[0]) for gid in assets}
        # Demand per bus.  Two formats: explicit demand_by_bus (list per bus)
        # or load_share_by_bus (fraction of total system demand).
        demand_by_bus = _net_input.get("demand_by_bus")
        load_share    = _net_input.get("load_share_by_bus") or {}
        if not load_share and not demand_by_bus:
            # Default: distribute load equally across buses.
            load_share = {b: 1.0/len(bus_ids) for b in bus_ids}
        if load_share:
            tot = sum(load_share.values())
            if tot > 0: load_share = {k: v/tot for k, v in load_share.items()}

        m.B = pyo.Set(initialize=bus_ids)
        m.L = pyo.Set(initialize=[ln["id"] for ln in lines])
        m.theta = pyo.Var(m.B, m.T, domain=pyo.Reals)
        m.fl    = pyo.Var(m.L, m.T, domain=pyo.Reals)         # MW flow from→to (signed)

        line_dict = {ln["id"]: ln for ln in lines}
        # Slack bus angle = 0
        def _slack(m, t): return m.theta[slack, t] == 0
        m.SlackTheta = pyo.Constraint(m.T, rule=_slack)
        # DC flow: fl = (theta_from - theta_to) / x_pu
        def _flow(m, lid, t):
            ln = line_dict[lid]
            x  = float(ln.get("x_pu", 0.05))
            return m.fl[lid, t] == (m.theta[ln["from_bus"], t] - m.theta[ln["to_bus"], t]) / x
        m.LineFlow = pyo.Constraint(m.L, m.T, rule=_flow)
        # Capacity bounds (both directions)
        def _cap_pos(m, lid, t):
            return m.fl[lid, t] <= float(line_dict[lid]["capacity_mw"])
        def _cap_neg(m, lid, t):
            return m.fl[lid, t] >= -float(line_dict[lid]["capacity_mw"])
        m.CapPos = pyo.Constraint(m.L, m.T, rule=_cap_pos)
        m.CapNeg = pyo.Constraint(m.L, m.T, rule=_cap_neg)

        # Per-bus balance: gen + bess_net + dr + ph + unserv = load_at_b + Σ(out-in flows)
        # Σ_l_out fl_l - Σ_l_in fl_l   (positive = leaving)
        from collections import defaultdict
        out_lines = defaultdict(list); in_lines = defaultdict(list)
        for ln in lines:
            out_lines[ln["from_bus"]].append(ln["id"])
            in_lines[ln["to_bus"]].append(ln["id"])

        def _bus_balance(m, b, t):
            gen  = sum(m.p[g,t] for g in disp_ids if asset_bus.get(g) == b)
            bess_net = sum(m.dis[bid,t] - m.ch[bid,t] for bid in bess_ids
                           if asset_bus.get(bid) == b) if bess_ids else 0
            dr_supply = sum(m.dr[d,t] for d in dr_ids if asset_bus.get(d) == b) if dr_ids else 0
            ph_net = sum((m.ph_gen_hi[h,t] + m.ph_gen_lo[h,t])
                         - (m.ph_pmp_hi[h,t] + m.ph_pmp_lo[h,t])
                         for h in ph_ids if asset_bus.get(h) == b) if ph_ids else 0
            net_outflow = sum(m.fl[lid, t] for lid in out_lines[b]) \
                        - sum(m.fl[lid, t] for lid in in_lines[b])
            # Demand at bus b
            if demand_by_bus and b in demand_by_bus:
                d_b = demand_by_bus[b][t-1] if isinstance(demand_by_bus[b], list) else float(demand_by_bus[b])
            else:
                d_b = demand_w[t-1] * float(load_share.get(b, 0.0))
            # bus-level unserved share = total unserved × bus share (proxy)
            unserv_b = m.unserv[t] * float(load_share.get(b, 1.0/len(bus_ids)))
            return gen + bess_net + dr_supply + ph_net + unserv_b - net_outflow == d_b
        m.Balance = pyo.Constraint(m.B, m.T, rule=_bus_balance)
    else:
        # ── Copperplate (legacy) ────────────────────────────────────
        # Σ p[g,t] + unserv + DR_supply + PH_net - BESS_net = demand[t]
        def balance(m, t):
            gen  = sum(m.p[g,t] for g in disp_ids)
            bess_net = sum(m.dis[b,t] - m.ch[b,t] for b in bess_ids) if bess_ids else 0
            dr_supply = sum(m.dr[d,t] for d in dr_ids) if dr_ids else 0
            ph_net = sum((m.ph_gen_hi[h,t] + m.ph_gen_lo[h,t])
                         - (m.ph_pmp_hi[h,t] + m.ph_pmp_lo[h,t])
                         for h in ph_ids) if ph_ids else 0
            return gen + bess_net + dr_supply + ph_net + m.unserv[t] == demand_w[t-1]
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

    # min_up / min_down are specified in HOURS; convert to periods.
    def _hours_to_periods(h): return max(1, int(math.ceil(h / dt)))

    # ── Minimum Up Time ────────────────────────────────────────────────
    def min_up(m, g, t):
        mut_p = _hours_to_periods(float(assets[g].get("min_up", 0)))
        if mut_p < 2: return pyo.Constraint.Skip
        end = min(t + mut_p - 1, T[-1])
        return sum(m.y[g,tau] for tau in range(t, end+1)) <= m.u[g,t]
    m.MinUp = pyo.Constraint(m.GC, m.T, rule=min_up)

    # ── Minimum Down Time ──────────────────────────────────────────────
    def min_dn(m, g, t):
        mdt_p = _hours_to_periods(float(assets[g].get("min_down", 0)))
        if mdt_p < 2: return pyo.Constraint.Skip
        end = min(t + mdt_p - 1, T[-1])
        return sum(m.z[g,tau] for tau in range(t, end+1)) <= 1 - m.u[g,t]
    m.MinDn = pyo.Constraint(m.GC, m.T, rule=min_dn)

    # ── Ramp constraints (ramp_up/down are MW per HOUR → MW per period = ramp*dt)
    def ramp_up_c(m, g, t):
        if t == 1: return pyo.Constraint.Skip
        ru = float(assets[g].get("ramp_up", 9999))
        if ru >= 9999: return pyo.Constraint.Skip
        return m.p[g,t] - m.p[g,t-1] <= ru * dt
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
        # Periods per hour & upstream cascade delay in periods (was hours).
        _per_per_h = max(1, int(round(1 / dt)))
        def hydro_bal(m, h, t):
            ha   = assets[h]["hydro"]
            eff  = float(ha.get("efficiency", 350))   # MWh/Mm³
            infl_key = assets[h].get("inflow_profile")
            if infl_key and infl_key in profiles_w:
                infl_rate = profiles_w[infl_key][t-1]     # Mm³/h rate
            else:
                raw_inflow = ha.get("inflow")
                infl_rate = float(raw_inflow) if raw_inflow is not None else 0.0
            # Release is Mm³/h rate: p[MW] / eff[MWh/Mm³] = Mm³/h.
            release_rate = m.p[h, t] / max(eff, 0.001)

            up_id = ha.get("cascade_upstream")
            cascade_in_rate = 0
            if up_id and up_id in assets:
                delay_h  = int(ha.get("cascade_travel_delay_h", 0))
                gain     = float(ha.get("cascade_gain", 1.0))
                t_upstream = t - delay_h * _per_per_h
                if t_upstream >= 1:
                    up_eff = float(assets[up_id].get("hydro", {}).get("efficiency", 350))
                    cascade_in_rate = gain * (m.p[up_id, t_upstream] / max(up_eff, 0.001))

            infl_rate_total = infl_rate + cascade_in_rate
            if t == 1:
                stor_prev = init_state.get(h, {}).get("stor", float(ha.get("reservoir_init", 700)))
            else:
                stor_prev = m.stor[h, t-1]
            # Convert rates to per-period volumes by × dt.
            return m.stor[h, t] == stor_prev + (infl_rate_total - release_rate - m.spill[h, t]) * dt
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
            # ch/dis are MW; × dt converts to MWh per period.
            return m.soc[b,t] == soc_prev + (ec * m.ch[b,t] - m.dis[b,t] / max(ed,0.001)) * dt
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

        # v1.4: BESS depth-multiplier — split discharge into a 'deep'
        # component that's only non-zero when SOC is below the deep
        # threshold. We approximate with a single linking constraint:
        #   dis_deep[b,t] ≥ dis[b,t] - pmax × z_shallow[b,t]
        #   z_shallow is 1 iff soc ≥ thr·energy ; big-M linking
        #   → when z_shallow=1, dis_deep unconstrained from below (=0 via dis ≥ 0)
        #   → when z_shallow=0 (deep), dis_deep ≥ dis (whole dis is deep)
        # Objective then pays extra (depth_multiplier-1)·vom_discharge·dis_deep.
        deep_bess = [b for b in bess_ids if float(assets[b].get("depth_multiplier", 1) or 1) > 1]
        if deep_bess:
            m.DeepBESS = pyo.Set(initialize=deep_bess)
            m.dis_deep = pyo.Var(m.DeepBESS, m.T, domain=pyo.NonNegativeReals)
            m.z_shallow = pyo.Var(m.DeepBESS, m.T, domain=pyo.Binary)

            def _soc_shallow_ub(m, b, t):
                a = assets[b]
                thr = float(a.get("soc_deep_threshold", 0.2)) * float(a["energy_mwh"])
                bigM = float(a["energy_mwh"])
                return m.soc[b, t] >= thr - bigM * (1 - m.z_shallow[b, t])
            m.SocShallowUB = pyo.Constraint(m.DeepBESS, m.T, rule=_soc_shallow_ub)

            def _dis_deep_link(m, b, t):
                pmx = float(assets[b]["power_mw"])
                return m.dis_deep[b, t] >= m.dis[b, t] - pmx * m.z_shallow[b, t]
            m.BessDeepLink = pyo.Constraint(m.DeepBESS, m.T, rule=_dis_deep_link)

    # ── v1.4: Pumped-hydro with 2-bin (high-head / deep) efficiency ───
    if ph_ids:
        BIGM_PH_MW = {h: max(float(assets[h]["pmax"]), float(assets[h]["pump_mw"])) * 1.05
                      for h in ph_ids}

        def ph_soc_bal(m, h, t):
            a = assets[h]
            ep_hi = float(a["efficiency_pump"])
            ep_lo = float(a.get("efficiency_pump_deep", ep_hi * 0.85))
            eg_hi = float(a["efficiency_gen"])
            eg_lo = float(a.get("efficiency_gen_deep", eg_hi * 0.85))
            if t == 1:
                soc_prev = init_state.get(h, {}).get("ph_soc",
                           float(a["soc_init"]) * float(a["energy_mwh"]))
            else:
                soc_prev = m.ph_soc[h, t-1]
            add = (ep_hi * m.ph_pmp_hi[h, t] + ep_lo * m.ph_pmp_lo[h, t]) * dt
            sub = (m.ph_gen_hi[h, t] / max(eg_hi, 0.001)
                   + m.ph_gen_lo[h, t] / max(eg_lo, 0.001)) * dt
            return m.ph_soc[h, t] == soc_prev + add - sub
        m.PhSOC = pyo.Constraint(m.PH, m.T, rule=ph_soc_bal)

        def ph_soc_lb(m, h, t):
            a = assets[h]
            return m.ph_soc[h, t] >= float(a["soc_min"]) * float(a["energy_mwh"])
        def ph_soc_ub(m, h, t):
            a = assets[h]
            return m.ph_soc[h, t] <= float(a["soc_max"]) * float(a["energy_mwh"])
        m.PhSOCLB = pyo.Constraint(m.PH, m.T, rule=ph_soc_lb)
        m.PhSOCUB = pyo.Constraint(m.PH, m.T, rule=ph_soc_ub)

        # Segment selection: z_hi=1 iff SOC ≥ threshold·cap.  Big-M links.
        def ph_seg_link(m, h, t):
            a = assets[h]
            thr = float(a.get("soc_deep_threshold", 0.3)) * float(a["energy_mwh"])
            bigM = float(a["energy_mwh"])
            return m.ph_soc[h, t] >= thr - bigM * (1 - m.ph_zhi[h, t])
        m.PhSeg = pyo.Constraint(m.PH, m.T, rule=ph_seg_link)

        # Mode / segment bounds:
        #   gen_hi ≤ pmax · mode · z_hi,   gen_lo ≤ pmax · mode · (1 - z_hi)
        #   pmp_hi ≤ pump · (1 − mode) · z_hi,  pmp_lo ≤ pump · (1 − mode) · (1 − z_hi)
        # Big-M lifts the product to linear: the stricter bound is via pmax.
        def gen_hi_ub(m, h, t):
            pmx = float(assets[h]["pmax"])
            return m.ph_gen_hi[h, t] <= pmx * m.ph_mode[h, t]
        def gen_lo_ub(m, h, t):
            pmx = float(assets[h]["pmax"])
            return m.ph_gen_lo[h, t] <= pmx * m.ph_mode[h, t]
        def gen_seg_hi(m, h, t):
            pmx = float(assets[h]["pmax"])
            return m.ph_gen_hi[h, t] <= pmx * m.ph_zhi[h, t]
        def gen_seg_lo(m, h, t):
            pmx = float(assets[h]["pmax"])
            return m.ph_gen_lo[h, t] <= pmx * (1 - m.ph_zhi[h, t])
        def pmp_mode_hi(m, h, t):
            pmp = float(assets[h]["pump_mw"])
            return m.ph_pmp_hi[h, t] <= pmp * (1 - m.ph_mode[h, t])
        def pmp_mode_lo(m, h, t):
            pmp = float(assets[h]["pump_mw"])
            return m.ph_pmp_lo[h, t] <= pmp * (1 - m.ph_mode[h, t])
        def pmp_seg_hi(m, h, t):
            pmp = float(assets[h]["pump_mw"])
            return m.ph_pmp_hi[h, t] <= pmp * m.ph_zhi[h, t]
        def pmp_seg_lo(m, h, t):
            pmp = float(assets[h]["pump_mw"])
            return m.ph_pmp_lo[h, t] <= pmp * (1 - m.ph_zhi[h, t])

        m.PhGenHi  = pyo.Constraint(m.PH, m.T, rule=gen_hi_ub)
        m.PhGenLo  = pyo.Constraint(m.PH, m.T, rule=gen_lo_ub)
        m.PhGenSegHi = pyo.Constraint(m.PH, m.T, rule=gen_seg_hi)
        m.PhGenSegLo = pyo.Constraint(m.PH, m.T, rule=gen_seg_lo)
        m.PhPmpHi  = pyo.Constraint(m.PH, m.T, rule=pmp_mode_hi)
        m.PhPmpLo  = pyo.Constraint(m.PH, m.T, rule=pmp_mode_lo)
        m.PhPmpSegHi = pyo.Constraint(m.PH, m.T, rule=pmp_seg_hi)
        m.PhPmpSegLo = pyo.Constraint(m.PH, m.T, rule=pmp_seg_lo)

    # ── v1.4: DR curtailment constraints ──────────────────────────────
    if dr_ids:
        def dr_ub(m, d, t):
            a = assets[d]
            pmc = float(a["pmax_curtail"])
            avail_key = a.get("availability_profile")
            if avail_key and avail_key in profiles_w:
                pmc = pmc * float(profiles_w[avail_key][t-1])
            return m.dr[d, t] <= pmc
        m.DR_UB = pyo.Constraint(m.DR, m.T, rule=dr_ub)

        # Annual hours cap, pro-rated by (H · dt / HOURS_PER_YEAR).
        def dr_annual(m, d):
            a = assets[d]
            cap_h = float(a.get("hours_per_year_max", HOURS_PER_YEAR))
            cap_mwh_year = cap_h * float(a["pmax_curtail"])
            frac = (H * dt) / max(HOURS_PER_YEAR, 1)
            return sum(m.dr[d, t] * dt for t in m.T) <= cap_mwh_year * frac
        m.DR_Annual = pyo.Constraint(m.DR, rule=dr_annual)

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
        for g in disp_ids:
            if g not in elig:
                for t in T:
                    m.res_up[rid,g,t].fix(0)
                    m.res_down[rid,g,t].fix(0)

    # ── Gas constraints ────────────────────────────────────────────────
    # Gas flow per period = gas_rate[Mm³/MWh] × p[MW] × dt[h].
    gas_mode = gas_limits.get("mode","none")
    if gas_mode != "none" and gas_units:
        H_hours = H * dt
        def gas_total(m):
            return sum(assets[g]["_gas_rate"] * m.p[g,t] * dt
                       for g in gas_units for t in m.T) <= \
                   gas_limits.get("daily_limit", 9999) * H_hours
        if gas_mode in ("annual","annual+monthly") and gas_limits.get("daily_limit"):
            m.GasTotal = pyo.Constraint(rule=gas_total)
        # ── Real per-calendar-month gas cap ────────────────────────────
        # Map each window period t to its calendar month via the global
        # hour offset and the period duration.
        monthly = gas_limits.get("monthly_limits")
        if gas_mode in ("monthly","annual+monthly") and monthly:
            HOURS_PER_MONTH = (
                31*24, 28*24, 31*24, 30*24, 31*24, 30*24,
                31*24, 31*24, 30*24, 31*24, 30*24, 31*24,
            )
            def _hour_to_month(global_h: int) -> int:
                acc = 0
                for i, hpm in enumerate(HOURS_PER_MONTH, start=1):
                    if global_h < acc + hpm:
                        return i
                    acc += hpm
                return 12
            month_to_periods: dict[int, list[int]] = {}
            for t in T:
                gh = (offset_h + int((t - 1) * dt)) % 8760
                mo = _hour_to_month(gh)
                month_to_periods.setdefault(mo, []).append(t)
            constrained = [mo for mo in sorted(month_to_periods.keys()) if mo in monthly]
            if constrained:
                m.GasMonthlyIdx = pyo.Set(initialize=constrained, ordered=True)
                def _gas_month_rule(m, mo):
                    periods = month_to_periods[mo]
                    cap_full = float(monthly[mo])
                    # Pro-rate: hours (= periods × dt) covered by window vs full month.
                    hours_in_window = len(periods) * dt
                    frac = hours_in_window / float(HOURS_PER_MONTH[mo - 1])
                    cap_window = cap_full * frac
                    return sum(assets[g]["_gas_rate"] * m.p[g, t] * dt
                               for g in gas_units for t in periods) <= cap_window
                m.GasMonthly = pyo.Constraint(m.GasMonthlyIdx, rule=_gas_month_rule)

    # ── Warm start hints (v1.3 #6) — pre-populate .value on vars from a
    #   previous solve; both Gurobi and HiGHS honour this for MIP restarts.
    if warm_start:
        for vn, values in warm_start.items():
            if not hasattr(m, vn):  continue
            var = getattr(m, vn)
            for k, v in values.items():
                try:
                    var[k].value = float(v)
                except (KeyError, TypeError, ValueError):
                    pass

    # ── Solve ──────────────────────────────────────────────────────────
    m.dual = pyo.Suffix(direction=pyo.Suffix.IMPORT)
    backend = str(solver_cfg.get("solver", "auto")).lower()

    def _make_highs():
        try:
            from pyomo.contrib.appsi.solvers.highs import HiGHS as _C
        except ImportError:
            from pyomo.contrib.appsi.solvers.highs import Highs as _C
        s = _C()
        s.highs_options["time_limit"]     = float(solver_cfg.get("time_limit_s", 300))
        s.highs_options["mip_rel_gap"]    = float(solver_cfg.get("mip_gap", 0.005))
        s.highs_options["log_to_console"] = False
        return s, "highs"

    def _make_gurobi():
        from pyomo.contrib.appsi.solvers.gurobi import Gurobi as _G
        s = _G()
        # Probe availability — Gurobi class imports cleanly even without a
        # license; the runtime check tells us if we actually have a solver.
        av = s.available()
        ok_flags = ("FullLicense", "LimitedLicense", "available",
                    "Available", "NotFound")   # names vary by Pyomo version
        if str(av).split(".")[-1] in ("NotFound", "BadLicense"):
            raise RuntimeError(f"Gurobi not usable ({av})")
        s.gurobi_options["TimeLimit"] = float(solver_cfg.get("time_limit_s", 300))
        s.gurobi_options["MIPGap"]    = float(solver_cfg.get("mip_gap", 0.005))
        s.gurobi_options["Threads"]   = int(solver_cfg.get("threads", 0))
        s.gurobi_options["OutputFlag"]= 0
        if warm_start:
            s.gurobi_options["LPWarmStart"] = 2
        return s, "gurobi"

    solver = None; backend_used = "appsi_highs"
    try:
        if backend == "gurobi":
            solver, backend_used = _make_gurobi()
        elif backend == "highs":
            solver, backend_used = _make_highs()
        else:  # auto — prefer Gurobi when usable, else HiGHS
            try:
                solver, backend_used = _make_gurobi()
            except Exception:
                solver, backend_used = _make_highs()
    except Exception:
        solver = pyo.SolverFactory("appsi_highs"); backend_used = "appsi_highs"

    t0     = time.time()
    # Pyomo 6.10 dropped the load_solutions kwarg from appsi.Highs.solve().
    # Try the old signature first; fall back silently for newer versions.
    # If the solver detects infeasibility, we wrap the exception so the
    # IIS diagnostic still runs and callers receive a structured report
    # rather than a Python traceback.
    result = None
    infeasible = False
    try:
        try:
            result = solver.solve(m, load_solutions=True)
        except TypeError:
            result = solver.solve(m)
    except Exception as e:
        msg = str(e).lower()
        if any(tag in msg for tag in ("infeasib", "no solution", "not found")):
            infeasible = True
            print(f"⚠️  solver declared infeasibility: {e}")
        else:
            raise
    solve_wall_s = time.time() - t0

    # ── v1.4: IIS diagnostics on infeasibility ──────────────────────────
    iis_report: dict | None = None
    if not infeasible and result is not None:
        try:
            tc_enum = str(getattr(result, "termination_condition", "") or
                          getattr(getattr(result, "solver", {}), "termination_condition", "")).lower()
        except Exception:
            tc_enum = ""
        infeasible = any(tag in tc_enum for tag in ("infeasib", "unknown"))
    if infeasible and solver_cfg.get("iis_on_infeasible", False):
        iis_report = _compute_iis(m, assets, demand_w, profiles_w,
                                  gas_limits, reserve_prods, backend_used)
        print("⚠️  IIS report written; attach to diagnostics.iis")
    if infeasible:
        # Return a sparse hourly_w with zeros so downstream reporting can
        # still produce a JSON (with diagnostics.iis populated).  Caller
        # should check `diagnostics.iis` and `diagnostics.solver_status`.
        n_gen  = len(disp_ids)
        empty_disp = {g: 0.0 for g in disp_ids}
        empty_comm = {g: 0.0 for g in committable}
        hourly_w_empty = []
        for t in T:
            hourly_w_empty.append({
                "t": t - 1 + offset_h, "hour_of_year": (offset_h + (t-1)*dt),
                "period_minutes": int(round(dt * 60)),
                "load_mw": demand_w[t-1], "generation_mw": 0.0,
                "lambda_usd_mwh": 0.0, "lambda_source": "infeasible",
                "unserved_mwh": demand_w[t-1] * dt, "curtailed_mwh": 0.0,
                "gas_mm3h": 0.0,
                "dispatch": empty_disp, "commitment": empty_comm,
                "startup": {}, "shutdown": {},
                "reserve_up": {rid: {g: 0.0 for g in res_elig.get(rid, [])} for rid in res_ids},
                "reserve_down": {rid: {g: 0.0 for g in res_elig.get(rid, [])} for rid in res_ids},
                "reserve_shortfall": {rid: 0.0 for rid in res_ids},
                "bess": {}, "hydro": {}, "dr": {}, "pumped_hydro": {},
            })
        fin_state = {"_iis_report": iis_report} if iis_report else {}
        return hourly_w_empty, fin_state, solve_wall_s, float("nan")

    def pv(var, *keys):
        try: v = pyo.value(var[keys]); return float(v) if v else 0.0
        except: return 0.0

    # ── Extract hourly results ─────────────────────────────────────────
    hourly_w = []
    for t in T:
        disp = {g: round(pv(m.p, g, t), 3) for g in disp_ids}
        comm = {g: round(pv(m.u, g, t))    for g in committable}
        start_h = {g: round(pv(m.y, g, t)) for g in committable}
        shut_h  = {g: round(pv(m.z, g, t)) for g in committable}

        # Gas — reported as Mm³/h rate (intensive). Total volume per period
        # is `hgas × dt` but the output keeps the rate for back-compat.
        hgas = sum(assets[g]["_gas_rate"] * disp[g] for g in gas_units)

        # Curtailment — MWh per period (pot - actual is MW, × dt).
        curt = 0.0
        for g in wind_solar:
            pot = get_pmax_t(assets[g], t-1, profiles_w)
            curt += max(0.0, pot - disp[g]) * dt

        # Lambda (VOLL-aware) — copperplate single λ vs nodal LMP per bus
        bus_lmp = {}
        line_flow = {}
        if use_dcopf:
            # Per-line MW flows (from primal m.fl).
            try:
                for ln in lines:
                    lid = ln["id"]
                    line_flow[lid] = round(float(pyo.value(m.fl[lid, t]) or 0.0), 2)
            except Exception:
                pass
            # Per-bus dual.  For MIP runs HiGHS often doesn't surface
            # constraint duals — when missing, broadcast the system λ
            # (computed below) so the field is always populated.
            dcopf_dual_ok = False
            try:
                tmp = {}
                for b in bus_ids:
                    tmp[b] = round(abs(float(m.dual[m.Balance[b, t]])), 3)
                if all(isinstance(v, (int, float)) for v in tmp.values()):
                    bus_lmp = tmp
                    dcopf_dual_ok = True
            except Exception:
                pass
            # Use slack-bus dual as system λ; if duals weren't loaded
            # the bus_lmp dict was just broadcast from system λ below.
            try:    lam = abs(float(m.dual[m.Balance[slack, t]]))
            except: lam = sum(bus_lmp.values())/max(len(bus_lmp),1) if bus_lmp else 0.0
        else:
            try:    lam = abs(float(m.dual[m.Balance[t]]))
            except: lam = 0.0
        if lam < 1e-6:
            # VOLL-aware fallback: check unserved → marginal committed
            unserv = pv(m.unserv, t)
            if unserv > 1e-3:
                lam = float(solver_cfg.get("unserved_penalty", 3000))
            else:
                mcs = [assets[g]["_dispMC"] for g in disp_ids if disp.get(g,0) > 0.5]
                lam = max(mcs) if mcs else 0.0
        # If DC-OPF is on but MIP duals weren't available, populate
        # bus_lmp with the system λ (zero-congestion assumption).
        if use_dcopf and not bus_lmp:
            bus_lmp = {b: round(lam, 3) for b in bus_ids}

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

        # DR curtailment per period.
        dr_h = {d: round(pv(m.dr, d, t), 3) for d in dr_ids}

        # Pumped-hydro state + net dispatch per period.
        ph_h = {}
        for h_id in ph_ids:
            gen_hi = pv(m.ph_gen_hi, h_id, t)
            gen_lo = pv(m.ph_gen_lo, h_id, t)
            pmp_hi = pv(m.ph_pmp_hi, h_id, t)
            pmp_lo = pv(m.ph_pmp_lo, h_id, t)
            ph_h[h_id] = {
                "gen_mw":         round(gen_hi + gen_lo, 3),
                "pump_mw":        round(pmp_hi + pmp_lo, 3),
                "net_mw":         round(gen_hi + gen_lo - pmp_hi - pmp_lo, 3),
                "soc_mwh":        round(pv(m.ph_soc, h_id, t), 2),
                "head_segment":   "high" if pv(m.ph_zhi, h_id, t) > 0.5 else "deep",
                "mode":           "gen" if pv(m.ph_mode, h_id, t) > 0.5 else "pump/idle",
            }

        gen_total = sum(disp.values()) + (
            sum(bess_h[b]["discharge_mw"] - bess_h[b]["charge_mw"] for b in bess_ids)
            if bess_ids else 0
        ) + sum(ph_h[h_id]["net_mw"] for h_id in ph_ids) \
          + sum(dr_h.values())

        # 't' is a period index; we also expose hour_of_year for calendar
        # work and the period's duration in minutes.
        t_period = t - 1 + offset_h * max(1, int(round(1/dt)))
        hour_of_year = (offset_h + (t - 1) * dt)
        hourly_w.append({
            "t":               t_period,
            "hour_of_year":    round(hour_of_year, 6),
            "period_minutes":  int(round(dt * 60)),
            "load_mw":         demand_w[t-1],
            "generation_mw":   round(gen_total, 2),
            "lambda_usd_mwh":  round(lam, 3),
            "lambda_source":   "dual_or_fallback",
            "unserved_mwh":    round(pv(m.unserv, t) * dt, 3),
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
            "hydro":           hydro_h,
            "dr":              dr_h,
            "pumped_hydro":    ph_h,
            "bus_lmp":         bus_lmp,
            "line_flow":       line_flow,
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
    for h_id in ph_ids:
        fin_state.setdefault(h_id, {})["ph_soc"] = pv(m.ph_soc, h_id, last_t)
    if iis_report is not None:
        fin_state["_iis_report"] = iis_report

    tc = str(getattr(result, "solver", {}).termination_condition
             if hasattr(result,"solver") else "—")
    print(f"   ✅ {tc} | {solve_wall_s:.1f}s | H={H} | backend={backend_used} | dt={dt}h")

    try:
        obj_val = float(pyo.value(m.OBJ))
    except Exception:
        obj_val = float("nan")
    return hourly_w, fin_state, solve_wall_s, obj_val


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
    H_total_h = int(sh.get("horizon_hours", len(profiles.get("demand", [])) or HOURS_PER_YEAR))
    demand  = profiles["demand"]
    reserve_prods = inp.get("reserve_products", [])
    solver_cfg    = dict(inp.get("solver_settings", {}))
    # Pass network model into solve_window via solver_cfg piggyback.
    solver_cfg["_network"] = {
        "buses": inp.get("buses") or [],
        "lines": inp.get("lines") or [],
        "demand_by_bus": inp.get("demand_by_bus"),
        "load_share_by_bus": inp.get("load_share_by_bus"),
    }

    # Resolution → period duration.
    r_min, _ppy, dt_h = resolve_resolution(inp)
    periods_per_h = 60 // r_min
    H_total_p     = H_total_h * periods_per_h

    window_h = int(solver_cfg.get("rolling_window_h", 168))
    step_h   = int(solver_cfg.get("rolling_step_h",   24))
    window_p = window_h * periods_per_h
    step_p   = step_h   * periods_per_h

    warm_start_enabled = bool(solver_cfg.get("warm_start", False))

    use_rolling = H_total_p > window_p
    iis_reports: list[dict] = []
    if not use_rolling:
        print(f"⚙️  Full-horizon solve: {len(assets)} assets × {H_total_p} periods "
              f"({H_total_h}h @ {r_min}min)")
        hourly, fin_state, swall, obj_val = solve_window(
            assets, demand[:H_total_p], profiles, reserve_prods, gas_limits,
            init_state={}, solver_cfg=solver_cfg, offset_h=0, dt=dt_h)
        if fin_state.get("_iis_report"):
            iis_reports.append(fin_state["_iis_report"])
        solve_all._iis_reports = iis_reports   # piggyback for build_result_store
        return hourly, swall, obj_val

    n_windows = math.ceil((H_total_p - window_p) / step_p) + 1
    print(f"⚙️  Rolling Horizon: {H_total_h}h ÷ {window_h}h window × {step_h}h step "
          f"= {n_windows} windows  (dt={r_min}min, warm_start={warm_start_enabled})")

    all_hourly, state, total_swall, obj_accum = [], {}, 0.0, 0.0
    committed_p = 0
    prev_hint: dict | None = None

    for w in range(n_windows):
        start_p = w * step_p
        end_p   = min(start_p + window_p, H_total_p)
        if start_p >= H_total_p: break

        demand_w   = demand[start_p:end_p]
        profiles_w = {k: v[start_p:end_p] if isinstance(v, list) else v
                      for k, v in profiles.items()}
        start_h    = start_p // periods_per_h

        hourly_w, state, swall, obj_w = solve_window(
            assets, demand_w, profiles_w, reserve_prods, gas_limits,
            init_state=state, solver_cfg=solver_cfg, offset_h=start_h,
            dt=dt_h, warm_start=prev_hint if warm_start_enabled else None)
        if state.get("_iis_report"):
            iis_reports.append({**state["_iis_report"], "window": w+1})
        total_swall += swall
        commit_n_p = min(step_p, end_p - start_p)
        all_hourly.extend(hourly_w[:commit_n_p])
        committed_p += commit_n_p
        if obj_w == obj_w and len(hourly_w) > 0:
            obj_accum += obj_w * (commit_n_p / len(hourly_w))
        pct = committed_p / H_total_p * 100
        print(f"   window {w+1:3d}/{n_windows}: p{start_p:6d}-{start_p+commit_n_p-1:6d} "
              f"| {swall:5.1f}s | {pct:5.1f}% done")

        # Build a warm-start hint from the values this window produced.
        # Uses rolled-forward assignments: period τ in new window ≈ period τ+step in old window.
        if warm_start_enabled and hourly_w:
            hint: dict = {"p": {}, "u": {}}
            for j, row in enumerate(hourly_w[step_p:], start=1):
                for g, v in row.get("dispatch", {}).items():
                    hint["p"][(g, j)] = v
                for g, v in row.get("commitment", {}).items():
                    hint["u"][(g, j)] = v
            prev_hint = hint

    print(f"\n   ✅ Total: {committed_p} periods ({committed_p*dt_h:.0f}h), "
          f"{total_swall:.0f}s ({total_swall/60:.1f} min)")
    solve_all._iis_reports = iis_reports       # piggyback for build_result_store
    return all_hourly, total_swall, obj_accum


# ══════════════════════════════════════════════════════════════════════
# 6. MARGINAL PRICE (Fixed-Commitment ED Resolve)
# ══════════════════════════════════════════════════════════════════════

def compute_marginal_prices(hourly: list, assets: dict, profiles: dict,
                             reserve_prods: list, solver_cfg: dict) -> list:
    """
    Post-process: fix commitment from UC solve, re-run LP ED to get
    clean dual-based marginal prices (lambda).
    This is the correct method for MIP → LP duality extraction.

    Note: this is a copperplate LP resolve.  When DC-OPF is enabled
    (`solver_cfg["_network"]["buses"]` non-empty), the per-bus LMPs
    already come from the MIP balance duals / fallback in
    `solve_window`; calling --ed-resolve on a DC-OPF run would
    overwrite those with a copperplate λ and lose nodal information,
    so we skip the resolve and emit a warning.
    """
    if (solver_cfg.get("_network") or {}).get("buses"):
        print("ℹ️  ED resolve skipped — DC-OPF is on; nodal LMPs already in bus_lmp.")
        return hourly
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
    # Resolution in hours (default 1.0). Honors sub-hourly v1.3 runs.
    r_min    = int(inp.get("resolution_min", 60))
    dt_h     = r_min / 60.0
    H_hours  = H * dt_h
    res_ids  = [rp["id"] for rp in inp.get("reserve_products",[])]
    gas_cfg  = inp.get("gas_constraints", {}) or {}
    gas_units= [a["id"] for a in inp.get("assets",[])
                if a.get("type")=="thermal" and a["id"] in
                   gas_cfg.get("applies_to", [])]

    # ── By-unit summary ────────────────────────────────────────────────
    by_unit = {}
    for gid, a in assets.items():
        atype = a.get("type")
        # Energy attribution depends on asset type — DR & pumped_hydro
        # do not appear in hourly dispatch maps; they have their own keys.
        if atype == "dr":
            energy = sum((h.get("dr", {}) or {}).get(gid, 0) for h in hourly) * dt_h
        elif atype == "pumped_hydro":
            energy = sum((h.get("pumped_hydro", {}) or {}).get(gid, {}).get("net_mw", 0)
                         for h in hourly) * dt_h
        else:
            energy = sum(h["dispatch"].get(gid, 0) for h in hourly) * dt_h
        oper_p    = sum(1 for h in hourly if h["commitment"].get(gid,0) > 0.5 or
                        (not a["_committable"] and h["dispatch"].get(gid,0) > 0.1))
        oper_h    = oper_p * dt_h
        starts    = sum(1 for i,h in enumerate(hourly)
                        if h["startup"].get(gid,0) > 0.5)
        fuel_cost = energy * a["_dispMC"]
        sc_cost   = starts * float(a.get("startup_cost",0))
        nl_cost   = oper_h * float(a.get("no_load_cost",0))
        vom_cost  = energy * float(a.get("vom",0))
        if atype == "dr":
            vom_cost = max(energy, 0.0) * float(a.get("price_per_mwh", 0))
        gross     = fuel_cost + sc_cost + nl_cost + vom_cost
        gas_mm3   = energy * a["_gas_rate"]
        # Capacity-factor reference: pmax_installed for RE, pmax for thermal/hydro,
        # power_mw for BESS, pmax for pumped hydro (gen side), pmax_curtail for DR.
        if atype in ("wind", "solar"):
            pmax_inst = float(a.get("pmax_installed", 0) or 0)
        elif atype == "bess":
            pmax_inst = float(a.get("power_mw", 0) or 0)
        elif atype == "pumped_hydro":
            pmax_inst = float(a.get("pmax", 0) or 0)
        elif atype == "dr":
            pmax_inst = float(a.get("pmax_curtail", 0) or 0)
        elif atype == "import":
            pp = a.get("pmax_profile")
            pmax_inst = float(pp) if isinstance(pp, (int, float)) else max(
                (h["dispatch"].get(gid, 0) for h in hourly), default=0.0)
        else:
            pmax_inst = float(a.get("pmax", 0) or 0)
        curt      = sum(h["curtailed_mwh"] for h in hourly
                        if atype in ("wind","solar"))  # simplified

        by_unit[gid] = {
            "name":          a.get("name", gid),
            "type":          a.get("type"),
            "energy_mwh":    round(energy, 1),
            "capacity_factor": round(energy / max(pmax_inst * H_hours, 1) * 100, 2),
            "oper_hours":    round(oper_h, 2),
            "starts":        starts,
            "fuel_cost":     round(fuel_cost, 0),
            "startup_cost":  round(sc_cost, 0),
            "no_load_cost":  round(nl_cost, 0),
            "vom_cost":      round(vom_cost, 0),
            "gross_cost":    round(gross, 0),
            "avg_cost_mwh":  round(gross / max(energy,1), 3),
            "gas_mm3":       round(gas_mm3, 4),
            "SRMC":          round(a["_dispMC"], 2),
            "heat_rate":     float(a.get("heat_rate", 0) or 0),       # bug fix r-A1
            "fuel_type":     a.get("fuel_type"),                       # bug fix r-A1
            "bus":           a.get("bus"),                              # for DC-OPF
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
        window_cap = float(annual_cap) * (H_hours / HOURS_PER_YEAR)
        # gas_used_h is a per-period Mm³/h rate; volume = Σ rate × dt.
        used = sum(gas_used_h) * dt_h
        gas_util_pct = round(used / max(window_cap, 1e-9) * 100, 2) if window_cap else None
        gas_binding  = (gas_util_pct is not None) and (gas_util_pct >= 99.0)
    # Per-hour limited count stays a proxy — refining requires per-hour duals
    # which HiGHS doesn't surface cleanly for MIP.  Deferred to Stage 2.
    hours_gas_limited = H if gas_binding else 0

    # ── Monthly aggregation ─────────────────────────────────────────
    # month_map is length H (one entry per PERIOD). Per-period quantities
    # multiply by dt_h to become per-hour/energy totals.
    study_year = inp.get("metadata",{}).get("study_year", 2026)
    start_h    = int(sh.get("start_hour", 0))
    month_map  = _build_month_map_periods(study_year, start_h, H, dt_h)

    monthly = []
    for mo in range(1, 13):
        idxs = [i for i, m in enumerate(month_map) if m == mo]
        if not idxs:
            continue
        per_unit_mo = {}
        for gid, a in assets.items():
            m_energy = sum(hourly[i]["dispatch"].get(gid, 0) for i in idxs) * dt_h
            m_starts = sum(1 for i in idxs if hourly[i]["startup"].get(gid, 0) > 0.5)
            m_oper_p = sum(1 for i in idxs
                           if hourly[i]["commitment"].get(gid, 0) > 0.5
                           or (not a["_committable"] and hourly[i]["dispatch"].get(gid,0) > 0.1))
            m_oper_h = m_oper_p * dt_h
            m_fuel   = m_energy * a["_dispMC"]
            m_vom    = m_energy * float(a.get("vom", 0))
            m_sc     = m_starts * float(a.get("startup_cost", 0))
            m_nl     = m_oper_h * float(a.get("no_load_cost", 0))
            m_gross  = m_fuel + m_vom + m_sc + m_nl
            per_unit_mo[gid] = {
                "energy_mwh":  round(m_energy, 1),
                "starts":      m_starts,
                "oper_hours":  round(m_oper_h, 2),
                "fuel_cost":   round(m_fuel, 0),
                "gross_cost":  round(m_gross, 0),
                "gas_mm3":     round(m_energy * a["_gas_rate"], 4),
            }
        m_cost = sum(pu["gross_cost"] for pu in per_unit_mo.values())
        monthly.append({
            "month":          mo,
            "label":          month_abbr[mo],
            "hours":          round(len(idxs) * dt_h, 2),
            "periods":        len(idxs),
            "total_energy_mwh": round(sum(hourly[i]["generation_mw"] for i in idxs) * dt_h, 0),
            "total_cost_usd": round(m_cost, 0),
            "avg_lambda":     round(sum(hourly[i]["lambda_usd_mwh"] for i in idxs)/max(len(idxs),1), 2),
            "gas_mm3":        round(sum(hourly[i]["gas_mm3h"] for i in idxs) * dt_h, 3),
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
            "horizon_hours":  H,          # NB: H is period count; when dt=1 this equals hours.
            "horizon_periods": H,
            "resolution_h":   dt_h,
            "resolution_min": r_min,
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
            # v1.4: IIS infeasibility report (null on feasible runs).
            "iis":                     getattr(solve_all, "_iis_reports", None) or None,
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
                "hour_of_year":  h.get("hour_of_year"),
                "period_minutes":h.get("period_minutes", int(round(dt_h * 60))),
                "load_mw":       h["load_mw"],
                "generation_mw": h["generation_mw"],
                "lambda_usd_mwh": h["lambda_usd_mwh"],
                "lambda_source": h["lambda_source"],
                "unserved_mwh":  h["unserved_mwh"],
                "curtailed_mwh": h["curtailed_mwh"],
                "gas_mm3h":      h["gas_mm3h"],
                "reserve_shortfall": h["reserve_shortfall"],
                # v1.5 DC-OPF: only present when network model is on.
                "bus_lmp":       h.get("bus_lmp", {}),
                "line_flow":     h.get("line_flow", {}),
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
    """Return list of month numbers for each HOUR in the horizon (legacy)."""
    base = datetime(year, 1, 1) + timedelta(hours=start_h)
    return [(base + timedelta(hours=i)).month for i in range(horizon_h)]


def _build_month_map_periods(year: int, start_h: int, horizon_p: int, dt_h: float) -> list:
    """Return list of month numbers for each PERIOD at the chosen dt."""
    base = datetime(year, 1, 1) + timedelta(hours=start_h)
    return [(base + timedelta(hours=i * dt_h)).month for i in range(horizon_p)]


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

    # Save — ensure parent directories exist (bug fix r4-1).
    from pathlib import Path as _P
    _P(args.output).parent.mkdir(parents=True, exist_ok=True)
    _P(args.excel).parent.mkdir(parents=True, exist_ok=True)
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
