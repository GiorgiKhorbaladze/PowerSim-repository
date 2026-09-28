Warning: truncated output (original token count: 47702)
Total output lines: 3695

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


class SolvedRows(list):
    """List-compatible extraction payload carrying this solve's explicit context."""

    def __init__(self, rows=(), *, solver_diagnostics=None, window_diagnostics=None,
                 iis_reports=None, component_results=None, reserve_results=None,
                 extraction_completed=True):
        super().__init__(rows)
        self.solver_diagnostics = solver_diagnostics
        self.window_diagnostics = list(window_diagnostics or [])
        self.iis_reports = list(iis_reports or [])
        self.component_results = list(component_results or [])
        self.reserve_results = list(reserve_results or [])
        self.extraction_completed = extraction_completed

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
SOLVER_VERSION = "powersim_solver 1.6.0"   # v1.6: solver hardening, storage reserves, closure, stochastic summary


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


def reserve_requirement_at(product: dict, period_index: int, profiles: dict,
                           offset_h: float, dt: float) -> float:
    """Resolve a product requirement without silently converting a profile to 0.

    ``period_index`` is one-based within a solve window.  The solver passes
    window-sliced profiles, matching the existing availability semantics.
    """
    profile_key = product.get("requirement_profile")
    if profile_key is None:
        value = product.get("requirement", 0.0)
    else:
        values = profiles.get(profile_key)
        index = period_index - 1
        if not isinstance(values, list) or index < 0 or index >= len(values):
            raise ValueError(
                f"reserve product {product.get('id', '?')!r} requirement_profile "
                f"{profile_key!r} must cover every solved period"
            )
        value = values[index]
    try:
        requirement = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"reserve product {product.get('id', '?')!r} requirement must be numeric") from exc
    if not math.isfinite(requirement) or requirement < 0:
        raise ValueError(f"reserve product {product.get('id', '?')!r} requirement must be finite and non-negative")
    return requirement


def normalize_dc_network(inp: dict, assets: dict) -> dict:
    """Return the sole MW/radian DC-network representation used by the solver.

    Network mode is intentionally opt-in and fail-closed.  Copperplate runs
    remain available when neither buses nor branches are declared.
    """
    buses = list(inp.get("buses") or [])
    lines = list(inp.get("lines") or [])
    if not buses and not lines:
        return {"buses": [], "lines": [], "demand_by_bus": None, "load_share_by_bus": {}}
    if not buses or not lines:
        raise ValueError("DC network mode requires explicit non-empty buses and lines")
    bus_ids = [str(b.get("id", "")) for b in buses]
    if any(not b for b in bus_ids) or len(set(bus_ids)) != len(bus_ids):
        raise ValueError("DC network buses require unique non-empty ids")
    slack = [b for b in buses if b.get("is_slack")]
    if len(slack) != 1:
        raise ValueError("DC network mode requires exactly one reference/slack bus")
    missing_assets = [gid for gid, asset in assets.items() if asset.get("bus") not in set(bus_ids)]
    if missing_assets:
        raise ValueError("DC network mode requires every asset to declare a valid bus: " + ", ".join(sorted(missing_assets)))
    demand_by_bus = inp.get("demand_by_bus")
    load_share = dict(inp.get("load_share_by_bus") or {})
    if not demand_by_bus and not load_share:
        raise ValueError("DC network mode requires demand_by_bus or load_share_by_bus; equal-share default is not permitted")
    if any(str(bus) not in set(bus_ids) for bus in (demand_by_bus or load_share)):
        raise ValueError("DC network demand mapping references an unknown bus")
    normal_lines = []
    for line in lines:
        lid = str(line.get("id", "")); fb = str(line.get("from_bus", "")); tb = str(line.get("to_bus", ""))
        if not lid or fb not in bus_ids or tb not in bus_ids or fb == tb:
            raise ValueError(f"DC branch {lid or '<unnamed>'!r} has invalid endpoints")
        b = line.get("susceptance_mw_per_rad")
        x = line.get("x_pu", line.get("reactance_pu"))
        if b is not None and x is not None:
            raise ValueError(f"DC branch {lid!r} must declare either susceptance_mw_per_rad or x_pu + base_mva")
        if b is None:
            base = line.get("base_mva", inp.get("base_mva"))
            if x is None or base is None:
                raise ValueError(f"DC branch {lid!r} x_pu requires explicit base_mva")
            try: b = float(base) / float(x)
            except (TypeError, ValueError, ZeroDivisionError) as exc: raise ValueError(f"DC branch {lid!r} has invalid x_pu/base_mva") from exc
        try:
            b = float(b); limit = float(line.get("normal_limit_mw", line.get("capacity_mw")))
        except (TypeError, ValueError) as exc: raise ValueError(f"DC branch {lid!r} requires numeric susceptance and MW limit") from exc
        if not math.isfinite(b) or b == 0 or not math.isfinite(limit) or limit <= 0:
            raise ValueError(f"DC branch {lid!r} requires nonzero finite susceptance and positive MW limit")
        normal_lines.append({"id": lid, "from_bus": fb, "to_bus": tb, "susceptance_mw_per_rad": b, "capacity_mw": limit})
    return {"buses": buses, "lines": normal_lines, "demand_by_bus": demand_by_bus, "load_share_by_bus": load_share}


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
        data = _demo_input()
    else:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"✅ Loaded: {path}")
        _print_input_summary(data)
    # Single conversion layer — the LP balance always operates on Mm³/h.
    normalize_hydro_inflow_profiles(data)
    return data


def hydro_efficiency_at(ha: dict, stor_level: float) -> float:
    """Return effective turbine efficiency (MWh/Mm³) at a given storage level.

    If ``hydro.head_efficiency_curve`` is set — a list of ``[stor_mm3,
    eff_mwh_per_mm3]`` breakpoints (any order; sorted internally) — the
    return is the piecewise-linear interpolation at ``stor_level``,
    clamped to the curve endpoints outside its domain. When the curve is
    absent, falls back to ``hydro.efficiency`` (default 350).
    """
    curve = ha.get("head_efficiency_curve")
    if not curve:
        return float(ha.get("efficiency", 350))
    pts = sorted(curve, key=lambda p: float(p[0]))
    s = float(stor_level)
    if s <= float(pts[0][0]):
        return float(pts[0][1])
    if s >= float(pts[-1][0]):
        return float(pts[-1][1])
    for i in range(1, len(pts)):
        s0, e0 = float(pts[i - 1][0]), float(pts[i - 1][1])
        s1, e1 = float(pts[i][0]),     float(pts[i][1])
        if s <= s1:
            frac = (s - s0) / max(s1 - s0, 1e-9)
            return e0 + frac * (e1 - e0)
    return float(ha.get("efficiency", 350))


# Hour-of-year at the end of each calendar month (non-leap; 8760h total).
_MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088,
                    5832, 6552, 7296, 8016, 8760)


def normalize_hydro_inflow_profiles(inp: dict) -> dict:
    """Mutate ``inp`` so every hydro_reg ``inflow_profile`` is in Mm³/h.

    Reads ``profile_bundle.hydro_inflow_unit`` (defaults to ``"raw"`` for
    backward compatibility). Profiles already referenced by hydro_reg
    assets are converted via :func:`normalize_hydro_inflow_rate`, and a
    diagnostic block is attached at ``inp["_hydro_inflow_diagnostic"]``
    so the results layer can emit ``hydro_inflow_unit_used``,
    ``hydro_inflow_conversion_applied`` and ``hydro_raw_unit_warning``.
    Idempotent: if the unit is already ``"Mm3_per_h"`` (or a previous
    normalization already converted in place), no values change.
    """
    from powersim_dataio import normalize_hydro_inflow_rate
    pb = (inp.get("profile_bundle") or {})
    unit = pb.get("hydro_inflow_unit", "raw")
    profiles = inp.get("profiles") or {}
    converted_keys: list[str] = []
    missing_scale: list[str] = []
    skipped_unconverted = (unit in ("Mm3_per_h", "raw"))
    asset_by_key = {}
    for a in inp.get("assets", []) or []:
        if a.get("type") != "hydro_reg":
            continue
        key = a.get("inflow_profile")
        if key and key in profiles and key not in asset_by_key:
            asset_by_key[key] = a
    if not skipped_unconverted:
        for key, asset in asset_by_key.items():
            arr = profiles.get(key)
            if not isinstance(arr, list):
                continue
            ha = (asset.get("hydro") or {})
            if unit == "normalized" and not (
                    ha.get("inflow_scale_mm3h") or ha.get("annual_inflow_mm3")):
                missing_scale.append(key)
                continue
            profiles[key] = [
                normalize_hydro_inflow_rate(v, unit, asset=asset, profile_key=key)
                for v in arr
            ]
            converted_keys.append(key)
    inp["_hydro_inflow_diagnostic"] = {
        "unit_declared":          unit,
        "conversion_applied":     bool(converted_keys),
        "raw_unit_warning":       unit == "raw",
        "normalized_profile_keys": converted_keys,
        "missing_scale_for":      missing_scale,
    }
    if missing_scale:
        print(f"⚠️  hydro_inflow_unit='normalized' but {len(missing_scale)} "
              f"profile(s) lack annual_inflow_mm3/inflow_scale_mm3h — left as-is: "
              f"{missing_scale}")
    if unit == "raw":
        print("⚠️  profile_bundle.hydro_inflow_unit='raw' — treating as Mm³/h "
              "(legacy assumption; declare the true source unit to remove this).")
    return inp


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
    # Top-level maintenance events → per-asset maintenance windows.
    maint_by_asset = {}
    for ev in inp.get("maintenance", []) or []:
        aid = ev.get("asset_id")
        if aid is None:
            continue
        maint_by_asset.setdefault(aid, []).append(ev)

    # v1.5 Solar Stage 4 — multi-year VRE degradation precompute. Compounds
    # ``(1 - rate)^(study_year - commissioning_year)`` once at load time and
    # stores as ``_degradation_factor`` so ``get_pmax_t`` stays O(1).
    study_year = int((inp.get("metadata") or {}).get("study_year") or 0)

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
        # v1.5 Solar Stage 4 — VRE annual degradation factor (default 1.0).
        a2["_degradation_factor"] = 1.0
        if a.get("type") in ("wind", "solar"):
            rate = float(a.get("degradation_rate_per_year", 0) or 0)
            if rate > 0:
                comm_year = int(a.get("commissioning_year", study_year) or study_year)
                years = max(0, study_year - comm_year) if study_year else 0
                a2["_degradation_factor"] = max(0.0, (1.0 - rate) ** years)
        # Maintenance windows: per-asset field + top-level maintenance events.
        mw = list(a.get("maint_windows", []) or [])
        mw.extend(maint_by_asset.get(a2["id"], []))
        if mw:
            a2["maint_windows"] = mw
        amap[a2["id"]] = a2
    return amap


def get_pmax_t(asset: dict, t_local: int, profiles: dict,
               offset_h: int = 0, dt: float = 1.0) -> float:
    """Effective Pmax at period t_local (0-indexed within the window).

    `offset_h` + `dt` map the window-local period to the global hour-of-study
    so maintenance windows (defined in global hours) can be enforced for every
    asset type — not only thermal/hydro_reg.
    """
    atype = asset.get("type")
    if atype in ("wind", "solar"):
        prof_key = asset.get("availability_profile")
        cf = profiles.get(prof_key, [1.0] * (t_local + 1))[t_local] if prof_key else 1.0
        # v1.5 Solar Stage 4 — DC/AC ratio with inverter clipping at 1.0 AC,
        # inverter efficiency, and pre-computed annual degradation factor.
        cf = max(0.0, float(cf))
        dc_ac    = float(asset.get("dc_ac_ratio", 1.0) or 1.0)
        inv_eta  = float(asset.get("inverter_efficiency", 1.0) or 1.0)
        cf_clipped = min(1.0, cf * dc_ac) * inv_eta
        deg = float(asset.get("_degradation_factor", 1.0) or 1.0)
        base = float(asset.get("pmax_installed", asset.get("pmax", 0))) * cf_clipped * deg
        # v1.5 Wind Stage 5 — wind-only multipliers: wake losses, monthly
        # availability mask, and air-density (∝ 1/T_kelvin) correction.
        if atype == "wind":
            base *= _wind_extras_factor(asset, profiles, t_local, offset_h, dt)
    elif atype == "hydro_ror":
        prof_key = asset.get("availability_profile")
        cf = profiles.get(prof_key, [asset.get("cf", 0.65)] * (t_local + 1))[t_local] if prof_key else asset.get("cf", 0.65)
        base = float(asset.get("pmax", 0)) * max(0.0, min(1.0, cf))
    elif atype == "import":
        prof_key = asset.get("pmax_profile")
        if prof_key and isinstance(profiles.get(prof_key), list):
            base = float(profiles[prof_key][t_local])
        else:
            base = float(asset.get("pmax_profile", asset.get("pmax", 0)))
    else:
        base = float(asset.get("pmax", 0))
    # Maintenance derating applies uniformly to all asset types.
    global_h = int(offset_h + round(t_local * dt))
    # v1.5 Thermal Stage 3 — optional ambient-temperature derating curve.
    return base * _maint_factor(asset, global_h) * _temp_factor(asset, profiles, t_local)


def _temp_factor(asset: dict, profiles: dict, t_local: int) -> float:
    """Capacity multiplier from ``asset.temp_derating_curve`` evaluated at
    ``profiles[asset.temp_profile_key][t_local]`` (ambient °C). Default 1.0
    when the curve or profile is absent. Piecewise-linear, clamped to the
    curve endpoints outside its domain.
    """
    curve = asset.get("temp_derating_curve")
    key   = asset.get("temp_profile_key")
    if not curve or not key:
        return 1.0
    series = profiles.get(key)
    if not isinstance(series, list) or t_local >= len(series):
        return 1.0
    temp = float(series[t_local])
    pts = sorted(curve, key=lambda p: float(p[0]))
    if temp <= float(pts[0][0]):
        return max(0.0, float(pts[0][1]))
    if temp >= float(pts[-1][0]):
        return max(0.0, float(pts[-1][1]))
    for i in range(1, len(pts)):
        t0, f0 = float(pts[i-1][0]), float(pts[i-1][1])
        t1, f1 = float(pts[i][0]),   float(pts[i][1])
        if temp <= t1:
            frac = (temp - t0) / max(t1 - t0, 1e-9)
            return max(0.0, f0 + frac * (f1 - f0))
    return 1.0


def _wind_extras_factor(asset: dict, profiles: dict, t_local: int,
                        offset_h: int = 0, dt: float = 1.0) -> float:
    """Wind-only post-multipliers applied on top of the generic
    wind/solar capacity factor inside :func:`get_pmax_t`.

    Composes three multiplicative effects:

    * **Wake loss** — constant farm-level loss factor
      ``(1 − asset.wake_loss_frac)``. Default 0.
    * **Monthly availability mask** — uses ``_MONTH_END_HOURS`` to map
      the global hour at this period to a calendar month and multiplies
      by ``asset.monthly_availability_factor[month]`` when supplied.
      Captures icing, winter degradation, monthly-binned planned outages.
    * **Air-density correction** — wind power ∝ density ∝ 1/T_kelvin
      at constant pressure. When ``air_density_correction = true`` and
      ``temp_profile_key`` is set, the factor is
      ``(T_ref_K) / (273.15 + amb_temp[t])``. ``density_ref_temp_c``
      defaults to 15 °C (288.15 K, IEC standard).

    Returns 1.0 when no wind-specific field is set (full back-compat).
    """
    factor = 1.0
    wl = float(asset.get("wake_loss_frac", 0) or 0)
    if wl > 0:
        factor *= max(0.0, 1.0 - wl)
    mam = asset.get("monthly_availability_factor")
    if isinstance(mam, list) and len(mam) == 12:
        global_h = int(offset_h + round(t_local * dt))
        m_idx = 0
        for j, hh in enumerate(_MONTH_END_HOURS):
            if global_h < hh:
                m_idx = j
                break
        v = mam[m_idx]
        if v is not None:
            factor *= max(0.0, float(v))
    if asset.get("air_density_correction"):
        key = asset.get("temp_profile_key")
        series = profiles.get(key) if key else None
        if isinstance(series, list) and t_local < len(series):
            ref_c = float(asset.get("density_ref_temp_c", 15.0) or 15.0)
            amb_c = float(series[t_local])
            ref_k = 273.15 + ref_c
            amb_k = 273.15 + amb_c
            if amb_k > 1e-3:
                factor *= ref_k / amb_k
    return factor


def _maint_factor(asset: dict, global_h: int) -> float:
    """Availability factor in [0,1] at global hour `global_h`.

    Reads per-asset ``maint_windows`` (populated from the top-level
    ``maintenance`` list by :func:`build_asset_map`). Each window:
      {start_h|start, end_h|end, availability_factor? , available_capacity_mw?}
    A window with neither factor nor capacity is treated as a full outage.
    Overlapping windows compose multiplicatively-min (the most restrictive
    factor wins).
    """
    windows = asset.get("maint_windows", [])
    if not windows:
        return 1.0
    pmax = float(asset.get("pmax",
                 asset.get("pmax_installed",
                 asset.get("power_mw", 0))) or 0)
    factor = 1.0
    for w in windows:
        s = w.get("start_h", w.get("start"))
        e = w.get("end_h", w.get("end"))
        try:
            s = float(s); e = float(e)
        except (TypeError, ValueError):
            continue
        if not (s <= global_h < e):
            continue
        if w.get("availability_factor") is not None:
            wf = float(w["availability_factor"])
        elif w.get("available_capacity_mw") is not None and pmax > 0:
            wf = float(w["available_capacity_mw"]) / pmax
        else:
            wf = 0.0
        factor = min(factor, max(0.0, min(1.0, wf)))
    return factor


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
    commit_periods: int | None = None,  # rolling-horizon: periods committed
                                        # to all_hourly from this window;
                                        # fin_state is computed at this
                                        # boundary so cumulative budgets and
                                        # min-up/down state propagate
                                        # correctly to the next window.
) -> tuple[list, dict, float, float]:
    """
    Solve one rolling window.
    Returns: (hourly_results, final_state, solve_time_s)
    """
    H   = len(demand_w)
    T   = list(range(1, H + 1))    # 1-indexed periods
    m   = pyo.ConcreteModel()

    # Stage 3A rollback seam. The default remains the validated legacy path;
    # shared is explicit and deterministic (never selected by environment).
    component_engine = str(solver_cfg.get("component_engine", "legacy")).lower()
    if component_engine not in {"legacy", "shared"}:
        raise ValueError("solver_settings.component_engine must be 'legacy' or 'shared'")

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
    disp_ids   = [i for i in all_ids if assets[i]["type"] not in ("dr", "pumped_hydro", "bess")]
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

    # v1.5: BESS end-SOC target var must exist BEFORE the objective rule
    # runs (Pyomo evaluates the rule once at component construction).
    _bess_end_target = [b for b in bess_ids
                        if assets[b].get("soc_end_target") is not None
                        and float(assets[b].get("soc_end_penalty_usd_mwh", 0) or 0) > 0]
    if _bess_end_target:
        m.BessEndTgt = pyo.Set(initialize=_bess_end_target)
        m.bess_end_short = pyo.Var(m.BessEndTgt, domain=pyo.NonNegativeReals)

    # v1.5 Hydro Stage 2 — monthly reservoir storage targets.
    # Targets are global-hour points; only month-ends falling inside the
    # current window are enforceable. The shortfall var must exist before
    # the objective so the rule sees it.
    _window_end_h = offset_h + H * dt
    _stor_target_specs = []   # list of (asset_id, month_idx, period_t, target_mm3, penalty)
    for _h_id in hydro_reg:
        _st = (assets[_h_id].get("hydro") or {}).get("storage_targets")
        if not _st:
            continue
        _targets = _st.get("month_end") or []
        _penalty = float(_st.get("penalty_usd_per_mm3", 50) or 0)
        if _penalty <= 0 or len(_targets) != 12:
            continue
        for _m_idx, _target in enumerate(_targets):
            if _target is None:
                continue
            _global_h = _MONTH_END_HOURS[_m_idx]
            if _global_h <= offset_h or _global_h > _window_end_h:
                continue
            _t_at = int(round((_global_h - offset_h) / dt))
            if 1 <= _t_at <= H:
                _stor_target_specs.append(
                    (_h_id, _m_idx, _t_at, float(_target), _penalty))
    if _stor_target_specs:
        m.StorTgt = pyo.Set(initialize=[(s[0], s[1]) for s in _stor_target_specs],
                            dimen=2)
        m.stor_target_short = pyo.Var(m.StorTgt, domain=pyo.NonNegativeReals)
        m._stor_target_period   = {(s[0], s[1]): s[2] for s in _stor_target_specs}
        m._stor_target_target   = {(s[0], s[1]): s[3] for s in _stor_target_specs}
        m._stor_target_penalty  = {(s[0], s[1]): s[4] for s in _stor_target_specs}

    # ── Decision Variables ─────────────────────────────────────────────
    m.p  = pyo.Var(m.G,  m.T, domain=pyo.NonNegativeReals)  # dispatch MW
    m.u  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # commitment
    m.y  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # startup
    m.z  = pyo.Var(m.GC, m.T, domain=pyo.Binary)            # shutdown

    # v1.5 Thermal Stage 3 — multi-stage startup (hot vs cold). A unit
    # qualifies for hot-start cost iff it was committed within the prior
    # ``hot_start_threshold_h`` periods. Otherwise the cold cost applies.
    _ms_start = [g for g in committable
                 if assets[g].get("startup_cost_hot") is not None
                 and assets[g].get("startup_cost_cold") is not None]
    if _ms_start:
        m.MSStart = pyo.Set(initialize=_ms_start)
        m.y_hot = pyo.Var(m.MSStart, m.T, domain=pyo.Binary)
        m._ms_hot_threshold = {
            g: max(1, int(assets[g].get("hot_start_threshold_h", 3) or 3))
            for g in _ms_start
        }

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

    # Reserve allocation per eligible provider per product per hour.
    # Conventional dispatch assets keep the legacy variables. BESS has
    # separate variables because it is bounded by inverter headroom and SOC.
    res_elig = {}       # {res_id: [all eligible asset ids present in model]}
    res_elig_g = {}     # conventional dispatch providers
    res_elig_bess = {}  # BESS providers
    reserve_eligible_filtered = []
    supported_reserve_types = {"thermal", "hydro_reg", "hydro_ror", "import", "bess"}
    _disp_set = set(disp_ids)
    _bess_set = set(bess_ids)
    for rp in reserve_prods:
        rid = rp["id"]
        listed = [u for u in rp.get("eligible_units", []) if u in assets]
        res_elig_g[rid] = [u for u in listed if u in _disp_set]
        res_elig_bess[rid] = [u for u in listed if u in _bess_set]
        res_elig[rid] = res_elig_g[rid] + res_elig_bess[rid]
        for u in listed:
            typ = assets[u].get("type")
            if typ not in supported_reserve_types:
                reserve_eligible_filtered.append({
                    "product": rid, "asset": u, "type": typ,
                    "reason": "reserve provider type not implemented in Stage 1"
                })
    m.res_up   = pyo.Var(m.RIDS, m.G, m.T, domain=pyo.NonNegativeReals)
    m.res_down = pyo.Var(m.RIDS, m.G, m.T, domain=pyo.NonNegativeReals)
    if bess_ids:
        m.bess_res_up   = pyo.Var(m.RIDS, m.BESS, m.T, domain=pyo.NonNegativeReals)
        m.bess_res_down = pyo.Var(m.RIDS, m.BESS, m.T, domain=pyo.NonNegativeReals)

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

    # CO₂ price for this window (0 disables the term cleanly).
    inp_co2_price = float(solver_cfg.get("_co2_price_usd_per_t", 0) or 0)

    def obj_rule(m):
        fuel  = sum(_fuel_term(g, t) for g in disp_ids for t in m.T)
        # Startup is per-event (no dt); no-load is $/h so × dt.
        # Multi-stage: hot for y_hot, cold for the residual (y - y_hot).
        # Plain start cost still applies to assets without multi-stage fields.
        def _startup_cost_g(g, t):
            if g in (_ms_start if _ms_start else []):
                hot  = float(assets[g]["startup_cost_hot"])
                cold = float(assets[g]["startup_cost_cold"])
                return hot * m.y_hot[g, t] + cold * (m.y[g, t] - m.y_hot[g, t])
            return float(assets[g].get("startup_cost", 0)) * m.y[g, t]
        start = sum(_startup_cost_g(g, t) for g in committable for t in m.T)
        noload= sum(float(assets[g].get("no_load_cost", 0)) * m.u[g, t] * dt
                    for g in committable for t in m.T)
        # v1.5 Thermal Stage 3 — CO₂ cost on dispatched MWh per thermal unit.
        cp = float(inp_co2_price)
        co2_cost = 0
        if cp > 0:
            co2_cost = sum(
                float(assets[g].get("co2_factor_t_per_mwh", 0) or 0)
                * m.p[g, t] * dt * cp
                for g in thermal for t in m.T
            )
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
        # Hydro spill cost — encourages turbining over bypass when storage
        # head-room permits. Defaults to 0 → backward compatible.
        spill_pen = 0
        if hydro_reg:
            spill_pen = sum(
                float((assets[h]["hydro"] or {}).get(
                    "spill_cost_usd_per_mm3",
                    (assets[h]["hydro"] or {}).get("spill_cost", 0)) or 0)
                * m.spill[h, t] * dt
                for h in hydro_reg for t in m.T
            )
        bess_end_pen = 0
        if hasattr(m, "bess_end_short"):
            bess_end_pen = sum(
                float(assets[b].get("soc_end_penalty_usd_mwh", 0) or 0) * m.bess_end_short[b]
                for b in m.BessEndTgt
            )
        stor_target_pen = 0
        if hasattr(m, "stor_target_short"):
            stor_target_pen = sum(
                m._stor_target_penalty[(h, mo)] * m.stor_target_short[h, mo]
                for (h, mo) in m._stor_target_penalty
            )
        return fuel + start + noload + bess_cost + dr_cost + ph_cost \
             + unserved_pen + res_pen + end_level_pen + bess_end_pen \
             + spill_pen + stor_target_pen + co2_cost
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
        # Inputs are normalized and fully mapped by ``normalize_dc_network``.
        slack = next(b["id"] for b in buses if b.get("is_slack"))
        asset_bus = {gid: assets[gid]["bus"] for gid in assets}
        # Demand per bus.  Two formats: explicit demand_by_bus (list per bus)
        # or load_share_by_bus (fraction of total system demand).
        demand_by_bus = _net_input.get("demand_by_bus")
        load_share    = _net_input.get("load_share_by_bus") or {}
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
        # DC flow in declared MW/radian units: f = B × (theta_from-theta_to).
        def _flow(m, lid, t):
            ln = line_dict[lid]
            return m.fl[lid, t] == float(ln["susceptance_mw_per_rad"]) * (m.theta[ln["from_bus"], t] - m.theta[ln["to_bus"], t])
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
    shared_session = None
    shared_asset_ids = frozenset()
    if component_engine == "shared":
        from powersim.workflows.deterministic import SharedComponentSession, build_stage3a_context
        # Stateful components need the same terminal-window marker used by
        # the legacy assembler.  It is data, not a constraint inspection.
        m._powersim_is_last_window = bool(solver_cfg.get("_is_last_window", True))
        shared_context = build_stage3a_context(m, T, dt, profiles_w, assets,
                                               offset_hours=offset_h,
                                               initial_state=init_state,
                                               availability_resolver=lambda asset, index: get_pmax_t(
                                                   asset, index, profiles_w, offset_h, dt),
                                               co2_price_usd_per_t=float(
                                                   solver_cfg.get("_co2_price_usd_per_t", 0) or 0))
        shared_session = SharedComponentSession(shared_context)
        shared_asset_ids = shared_session.asset_ids
    shared_bess_ids = set(shared_asset_ids).intersection(bess_ids)
    shared_ph_ids = set(shared_asset_ids).intersection(ph_ids)
    shared_dr_ids = set(shared_asset_ids).intersection(dr_ids)
    shared_reservoir_ids = set(shared_asset_ids).intersection(hydro_reg)

    def gen_lb(m, g, t):
        if g in shared_asset_ids:
            return pyo.Constraint.Skip
        if g in committable:
            return m.p[g,t] >= float(assets[g].get("pmin",0)) * m.u[g,t]
        return m.p[g,t] >= 0
    def gen_ub(m, g, t):
        if g in shared_asset_ids:
            return pyo.Constraint.Skip
        pmx = get_pmax_t(assets[g], t-1, profiles_w, offset_h, dt)
        if g in committable:
            return m.p[g,t] <= pmx * m.u[g,t]
        return m.p[g,t] <= pmx
    m.GenLB = pyo.Constraint(m.G, m.T, rule=gen_lb)
    m.GenUB = pyo.Constraint(m.G, m.T, rule=gen_ub)
    if shared_session is not None:
        # The component implementation is the sole upper-bound owner for
        # migrated assets; all other assets remain on the legacy path.
        shared_session.build()

    # ── UC logic: u[t] - u[t-1] = y[t] - z[t] ────────────────────────
    def uc_logic(m, g, t):
        if g in shared_asset_ids:
            return pyo.Constraint.Skip
        u_prev = init_state.get(g, {}).get("u", 0) if t == 1 else m.u[g, t-1]
        return m.u[g,t] - u_prev == m.y[g,t] - m.z[g,t]
    m.UCLogic = pyo.Constraint(m.GC, m.T, rule=uc_logic)

    def yz_ub(m, g, t):
        if g in shared_asset_ids:
            return pyo.Constraint.Skip
        return m.y[g,t] + m.z[g,t] <= 1
    m.YZUB = pyo.Constraint(m.GC, m.T, rule=yz_ub)

    # v1.5 Thermal Stage 3 — multi-stage startup linking constraints.
    # y_hot[g,t] ≤ y[g,t] and y_hot[g,t] ≤ Σ_{k=1..hot_h} u[g, t-k].
    # The history window must look BACK; for periods where t-k < 1 we
    # fold in init_state's prior on-state (1 if periods_on > k-1 else 0),
    # so rolling-horizon boundaries don't gratuitously force cold-starts.
    if _ms_start:
        def _u_hist(m, g, t, k):
            tk = t - k
            if tk >= 1:
                return m.u[g, tk]
            # tk <= 0 ⇒ look in init_state: was the unit on at boundary?
            prev_on = int((init_state.get(g, {}) or {}).get("periods_on", 0)…17702 tokens truncated…iagnostic(diagnostics, number: int, start_period: int, end_period: int) -> dict:
    return {"window": number, "start_period": start_period, "end_period": end_period,
            "diagnostics": diagnostics.model_dump(mode="json")}


def _aggregate_solver_diagnostics(windows: list[dict], _objective: float):
    """Aggregate deterministically; any failed window dominates later success."""
    from powersim.contracts import QAStatus, ResultValidity, SolverDiagnostics, SolverStatus
    if not windows:
        return SolverDiagnostics(backend="unknown", termination_condition="no windows solved",
            normalized_status=SolverStatus.SOLVER_ERROR, has_incumbent=False,
            result_validity=ResultValidity.INVALID, qa_status=QAStatus.NOT_RUN)
    diagnostics = [w["diagnostics"] for w in windows]
    severity = {"solver_error": 7, "numerical_error": 6, "infeasible": 5, "unbounded": 4,
                "time_limit": 3, "feasible": 2, "optimal": 1}
    worst_record = max(windows, key=lambda w: severity[w["diagnostics"]["normalized_status"]])
    worst = worst_record["diagnostics"]
    all_incumbents = all(d["has_incumbent"] for d in diagnostics)
    failure = worst["normalized_status"] in {"solver_error", "numerical_error", "infeasible", "unbounded"}
    has_incumbent = all_incumbents and not failure
    return SolverDiagnostics(backend=worst["backend"], backend_version=worst.get("backend_version"),
        requested_backend=worst.get("requested_backend"), termination_condition=(
            f"rolling aggregate: window {worst_record['window']} "
            f"{worst['termination_condition']}"), normalized_status=worst["normalized_status"],
        # Overlapping rolling-window objectives and bounds are not one global
        # primal/dual pair. Preserve them per-window, never synthesize a gap.
        incumbent_objective=None, best_bound=None, actual_mip_gap=None,
        requested_mip_gap=worst.get("requested_mip_gap"),
        runtime_s=sum(d.get("runtime_s") or 0 for d in diagnostics) or None,
        orchestration_runtime_s=sum(d.get("orchestration_runtime_s") or 0 for d in diagnostics),
        has_incumbent=has_incumbent, result_validity=ResultValidity.INVALID, qa_status=QAStatus.NOT_RUN,
        effective_options=worst.get("effective_options") or {},
        solve_started_at=diagnostics[0].get("solve_started_at"), solve_finished_at=diagnostics[-1].get("solve_finished_at"),
        metadata={"window_count": len(windows), "failed_window": worst_record["window"] if failure else None,
                  "aggregate_gap_available":False,
                  "aggregate_gap_unavailable_reason":
                      "overlapping rolling windows do not define a single global primal/bound pair"})


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
    # Storage / DR / pumped-hydro MW are not on the `dispatch` map and
    # don't use `pmax`. Rather than trying to redispatch them as free
    # generators (which drops their contribution to the balance and
    # yields a wrong marginal price), snapshot their committed MIP MW
    # per period and inject them into the RHS of the balance constraint.
    demand = [h["load_mw"] for h in hourly]
    H = len(demand)
    T = list(range(1, H + 1))
    disp_assets = {gid: a for gid, a in assets.items()
                   if a.get("type") not in ("bess", "dr", "pumped_hydro")}
    fixed_net_mw = [0.0] * H
    for t_i, h in enumerate(hourly):
        bess = h.get("bess") or {}
        for gid in bess:
            b = bess.get(gid) or {}
            fixed_net_mw[t_i] += float(b.get("discharge_mw", 0) or 0) - float(b.get("charge_mw", 0) or 0)
        dr = h.get("dr") or {}
        for gid in dr:
            fixed_net_mw[t_i] += float(dr.get(gid, 0) or 0)
        ph = h.get("pumped_hydro") or {}
        for gid in ph:
            v = ph.get(gid) or {}
            fixed_net_mw[t_i] += float(v.get("net_mw", 0) or 0)
    m = pyo.ConcreteModel()
    m.T = pyo.Set(initialize=T, ordered=True)
    m.G = pyo.Set(initialize=list(disp_assets.keys()))

    # Continuous only — no binary (LP)
    m.p = pyo.Var(m.G, m.T, domain=pyo.NonNegativeReals)
    m.unserv = pyo.Var(m.T, domain=pyo.NonNegativeReals)

    # Fix upper bound based on UC commitment
    for g in disp_assets:
        for t in T:
            h_dict  = hourly[t-1]
            comm    = h_dict.get("commitment", {}).get(g, 1)  # default 1 for non-committable
            pmx     = get_pmax_t(disp_assets[g], t-1, profiles)
            pmin    = float(disp_assets[g].get("pmin",0)) * comm
            m.p[g,t].setub(pmx * comm)
            m.p[g,t].setlb(pmin)

    UNSERVED_PEN = float(solver_cfg.get("unserved_penalty", 3000))
    def obj(m): return sum(disp_assets[g]["_dispMC"] * m.p[g,t] for g in m.G for t in m.T) + \
                        UNSERVED_PEN * sum(m.unserv[t] for t in m.T)
    m.OBJ = pyo.Objective(rule=obj, sense=pyo.minimize)

    def balance(m, t):
        return sum(m.p[g,t] for g in m.G) + m.unserv[t] == demand[t-1] - fixed_net_mw[t-1]
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
        elif atype == "bess":
            # BESS is not in `dispatch` — its per-period MW flows live in
            # the `bess` map. Net delivered energy = (discharge − charge) × dt.
            energy = sum(
                float((h.get("bess", {}) or {}).get(gid, {}).get("discharge_mw", 0) or 0)
                - float((h.get("bess", {}) or {}).get(gid, {}).get("charge_mw", 0) or 0)
                for h in hourly
            ) * dt_h
        else:
            energy = sum(h["dispatch"].get(gid, 0) for h in hourly) * dt_h
        oper_p    = sum(1 for h in hourly if h["commitment"].get(gid,0) > 0.5 or
                        (not a["_committable"] and h["dispatch"].get(gid,0) > 0.1))
        oper_h    = oper_p * dt_h
        starts    = sum(1 for i,h in enumerate(hourly)
                        if h["startup"].get(gid,0) > 0.5)
        # Per-period fuel cost. For assets with a piecewise heat_rate_curve,
        # `_dispMC` collapses to `vom` (no scalar heat_rate) and would
        # under-report fuel cost by orders of magnitude. Reconstruct the
        # curve-consistent cost by interpolating each period's dispatch
        # `p_t` along the vertex costs `hr_k × p_k × fp × 0.9478`.
        hrc = a.get("heat_rate_curve") if atype == "thermal" else None
        if isinstance(hrc, list) and len(hrc) >= 2:
            fp    = float(a.get("fuel_price", 0) or 0)
            pts   = sorted((float(pmw), float(hr)) for pmw, hr in hrc)
            xs    = [pt[0] for pt in pts]
            costs = [pt[1] * pt[0] * fp * 0.9478 for pt in pts]  # $ / h at vertex
            fuel_cost = 0.0
            for h in hourly:
                p_t = float(h["dispatch"].get(gid, 0) or 0)
                if p_t <= xs[0]:
                    cost_rate = costs[0]
                elif p_t >= xs[-1]:
                    cost_rate = costs[-1]
                else:
                    for k in range(len(xs) - 1):
                        if xs[k] <= p_t <= xs[k+1]:
                            frac = (p_t - xs[k]) / (xs[k+1] - xs[k])
                            cost_rate = costs[k] + frac * (costs[k+1] - costs[k])
                            break
                fuel_cost += cost_rate * dt_h
        else:
            fuel_cost = energy * a["_dispMC"]
        # v1.5 Thermal Stage 3 — multi-stage startup cost reconstruction.
        if a.get("startup_cost_hot") is not None and a.get("startup_cost_cold") is not None:
            hot_n  = sum(1 for h in hourly if (h.get("startup_hot") or {}).get(gid, 0) > 0.5)
            cold_n = starts - hot_n
            sc_cost = (hot_n * float(a["startup_cost_hot"])
                       + cold_n * float(a["startup_cost_cold"]))
        else:
            sc_cost = starts * float(a.get("startup_cost", 0))
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
        # v1.5: BESS aging metrics — cycle counter + calendar life used.
        if atype == "bess":
            cap = float(a.get("energy_mwh", 0) or 0)
            ch_total  = sum((h.get("bess", {}).get(gid, {}) or {}).get("charge_mw", 0)
                            * dt_h for h in hourly)
            dis_total = sum((h.get("bess", {}).get(gid, {}) or {}).get("discharge_mw", 0)
                            * dt_h for h in hourly)
            throughput = ch_total + dis_total
            equiv_cycles = throughput / (2.0 * cap) if cap > 0 else 0.0
            life_cycles = a.get("lifetime_full_cycles")
            life_years  = a.get("calendar_life_years")
            study_yrs   = H_hours / 8760.0
            by_unit[gid].update({
                "bess_throughput_mwh":      round(throughput, 1),
                "bess_equivalent_cycles":   round(equiv_cycles, 4),
                "bess_cycle_life_used_pct": round(equiv_cycles / float(life_cycles) * 100, 3)
                    if life_cycles and float(life_cycles) > 0 else None,
                "bess_calendar_used_pct":   round(study_yrs / float(life_years) * 100, 3)
                    if life_years and float(life_years) > 0 else None,
            })
        # v1.5 Hydro Stage 1 — per-reservoir spill and turbined-release totals.
        if atype == "hydro_reg":
            spill_total   = sum((h.get("hydro", {}).get(gid, {}) or {}).get("spill_mm3h", 0)
                                * dt_h for h in hourly)
            release_total = sum((h.get("hydro", {}).get(gid, {}) or {}).get("release_mm3h", 0)
                                * dt_h for h in hourly)
            by_unit[gid].update({
                "total_spill_mm3":         round(spill_total, 4),
                "total_hydro_release_mm3": round(release_total, 4),
            })
        # v1.5 Thermal Stage 3 — CO₂ emissions + cost per unit.
        if atype == "thermal":
            co2_factor = float(a.get("co2_factor_t_per_mwh", 0) or 0)
            co2_price  = float(inp.get("co2_price_usd_per_t", 0) or 0)
            total_co2  = energy * co2_factor
            by_unit[gid].update({
                "co2_factor_t_per_mwh": co2_factor,
                "total_co2_t":          round(total_co2, 2),
                "co2_cost_usd":         round(total_co2 * co2_price, 0),
            })

    # ── System summary ─────────────────────────────────────────────────
    total_cost    = sum(bu["gross_cost"]   for bu in by_unit.values())
    total_energy  = sum(bu["energy_mwh"]   for bu in by_unit.values())
    total_gas     = sum(bu["gas_mm3"]      for bu in by_unit.values())
    total_unserv  = sum(h["unserved_mwh"]  for h in hourly)
    total_curt    = sum(h["curtailed_mwh"] for h in hourly)
    avg_lam       = sum(h["lambda_usd_mwh"] for h in hourly) / H if H else 0
    peak_load     = max((h["load_mw"] for h in hourly), default=0)
    res_shortfall = {rid: sum(h["reserve_shortfall"].get(rid,0) for h in hourly) for rid in res_ids}
    reserve_supply_by_type = {}
    for h in hourly:
        for rid in res_ids:
            for gid, mw in (h.get("reserve_up", {}).get(rid, {}) or {}).items():
                typ = assets.get(gid, {}).get("type", "unknown")
                reserve_supply_by_type.setdefault(typ, 0.0)
                reserve_supply_by_type[typ] += float(mw or 0) * dt_h
            for gid, mw in (h.get("reserve_down", {}).get(rid, {}) or {}).items():
                typ = assets.get(gid, {}).get("type", "unknown")
                reserve_supply_by_type.setdefault(typ, 0.0)
                reserve_supply_by_type[typ] += float(mw or 0) * dt_h
    reserve_supply_by_type = {k: round(v, 6) for k, v in reserve_supply_by_type.items()}

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

    # ── Closure reconciliation (v1.6: full structured objective) ───────
    unserv_pen = float(s_cfg.get("unserved_penalty", 3000))
    res_pen_by = {rp["id"]: float(rp.get("shortfall_penalty", 500))
                  for rp in inp.get("reserve_products", [])}
    fuel_cost = sum(bu["fuel_cost"] for bu in by_unit.values())
    startup_cost = sum(bu["startup_cost"] for bu in by_unit.values())
    no_load_cost = sum(bu["no_load_cost"] for bu in by_unit.values())
    vom_cost = sum(bu["vom_cost"] for bu in by_unit.values()
                   if bu.get("type") not in ("bess", "dr", "pumped_hydro"))
    bess_degradation_cost = 0.0
    bess_end_soc_penalty = 0.0
    dr_cost = sum(bu["vom_cost"] for bu in by_unit.values() if bu.get("type") == "dr")
    pumped_hydro_cost = sum(bu["vom_cost"] for bu in by_unit.values() if bu.get("type") == "pumped_hydro")
    hydro_end_level_penalty = 0.0
    hydro_spill_penalty = 0.0
    for gid, a in assets.items():
        if a.get("type") == "bess":
            for h in hourly:
                bh = (h.get("bess", {}) or {}).get(gid, {}) or {}
                ch = float(bh.get("charge_mw", 0) or 0); dis = float(bh.get("discharge_mw", 0) or 0)
                bess_degradation_cost += (float(a.get("vom_discharge", 0) or 0) * dis
                    + float(a.get("cycle_cost_per_mwh", 0) or 0) * (ch + dis)) * dt_h
            if a.get("soc_end_target") is not None and hourly:
                end_soc = float((hourly[-1].get("bess", {}) or {}).get(gid, {}).get("soc_mwh", 0) or 0)
                target = float(a.get("soc_end_target") or 0) * float(a.get("energy_mwh", 0) or 0)
                bess_end_soc_penalty += max(0.0, target - end_soc) * float(a.get("soc_end_penalty_usd_mwh", 0) or 0)
        if a.get("type") == "hydro_reg" and hourly:
            ha = a.get("hydro", {}) or {}
            if float(ha.get("end_level_penalty", 0) or 0) > 0 and float(ha.get("target_end_level_frac", 0) or 0) > 0:
                end_stor = float((hourly[-1].get("hydro", {}) or {}).get(gid, {}).get("storage_mm3", 0) or 0)
                target = float(ha.get("target_end_level_frac", 0)) * float(ha.get("reservoir_max", 0) or 0)
                hydro_end_level_penalty += max(0.0, target - end_stor) * float(ha.get("end_level_penalty", 0) or 0)
            spill_cost = float(ha.get("spill_cost_usd_per_mm3", ha.get("spill_cost", 0)) or 0)
            hydro_spill_penalty += spill_cost * sum(float((h.get("hydro", {}) or {}).get(gid, {}).get("spill_mm3h", 0) or 0) * dt_h for h in hourly)
    pen_unserved = unserv_pen * total_unserv
    # LP objective charges `res_penalty × m.res_sh × dt` per period, so
    # the reconstructed penalty must also multiply the summed MW shortfall
    # by dt_h to yield MWh at sub-hourly resolutions.
    pen_reserve  = sum(res_pen_by.get(rid, 0) * res_shortfall.get(rid, 0) * dt_h
                       for rid in res_ids)
    # LP objective includes a CO₂ term `co2_factor × p × dt × co2_price`
    # for every thermal unit; per-unit outputs already carry
    # `co2_cost_usd`, so summing them closes that term into the objective.
    co2_cost_total = sum(float(bu.get("co2_cost_usd", 0) or 0)
                         for bu in by_unit.values())
    # LP objective includes a storage-target shortfall penalty for hydro
    # plants with a monthly `storage_targets.month_end` schedule. Sum
    # actual shortfall at each target month-end against its target.
    _MONTH_END_HOURS_LOCAL = (744, 1416, 2160, 2880, 3624, 4344,
                              5088, 5832, 6552, 7296, 8016, 8760)
    stor_target_pen = 0.0
    if hourly:
        # index rows by their (rounded) hour_of_year for O(1) month-end lookup
        row_by_hoy = {}
        for row in hourly:
            key = int(round(float(row.get("hour_of_year", 0))))
            row_by_hoy.setdefault(key, row)
        for gid, a in assets.items():
            if a.get("type") != "hydro_reg":
                continue
            st = ((a.get("hydro") or {}).get("storage_targets") or {})
            targets = st.get("month_end") or []
            penalty = float(st.get("penalty_usd_per_mm3", 50) or 0)
            if penalty <= 0 or len(targets) != 12:
                continue
            for m_idx, tgt in enumerate(targets):
                if tgt is None:
                    continue
                key = _MONTH_END_HOURS_LOCAL[m_idx]
                row = row_by_hoy.get(key)
                if row is None:
                    continue
                stor_now = float((row.get("hydro") or {}).get(gid, {}).get("storage_mm3", 0) or 0)
                short = max(0.0, float(tgt) - stor_now)
                if short > 0:
                    stor_target_pen += penalty * short
    reconstructed = (fuel_cost + startup_cost + no_load_cost + vom_cost + bess_degradation_cost
        + bess_end_soc_penalty + dr_cost + pumped_hydro_cost + pen_unserved + pen_reserve
        + hydro_end_level_penalty + hydro_spill_penalty
        + co2_cost_total + stor_target_pen)

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

    objective_breakdown = {
        "fuel_cost": round(fuel_cost, 6), "startup_cost": round(startup_cost, 6),
        "no_load_cost": round(no_load_cost, 6), "vom_cost": round(vom_cost, 6),
        "bess_degradation_cost": round(bess_degradation_cost, 6),
        "bess_end_soc_penalty": round(bess_end_soc_penalty, 6),
        "dr_cost": round(dr_cost, 6), "pumped_hydro_cost": round(pumped_hydro_cost, 6),
        "unserved_penalty": round(pen_unserved, 6),
        "reserve_shortfall_penalty": round(pen_reserve, 6),
        "hydro_end_level_penalty": round(hydro_end_level_penalty, 6),
        "hydro_spill_penalty": round(hydro_spill_penalty, 6),
        "co2_cost": round(co2_cost_total, 6),
        "storage_target_penalty": round(stor_target_pen, 6),
        "total_reconstructed": round(reconstructed, 6),
        "pyomo_objective": None if obj_total is None or obj_total != obj_total else round(obj_total, 6),
        "closure_gap_pct": None if closure_gap is None else round(closure_gap * 100, 6),
    }

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
            "solver_status":           "pending_qa",
            "solver_diagnostics":      None,
            "solver_version":          SOLVER_VERSION,
            "solve_time_s":            round(solve_time, 2),
            "mip_gap_pct":             None,
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
            "iis":                     getattr(hourly, "iis_reports", None) or None,
            # v1.5 Hydro Stage 1 — inflow unit normalization provenance.
            "hydro_inflow_unit_used":         (inp.get("_hydro_inflow_diagnostic") or {}).get("unit_declared"),
            "hydro_inflow_conversion_applied": bool((inp.get("_hydro_inflow_diagnostic") or {}).get("conversion_applied")),
            "hydro_raw_unit_warning":         bool((inp.get("_hydro_inflow_diagnostic") or {}).get("raw_unit_warning")),
            "reserve_eligible_filtered": hourly[0].get("reserve_eligible_filtered", []) if hourly else [],
            "reserve_provider_types": sorted({assets[gid].get("type") for h in hourly for rid in res_ids for gid in (h.get("reserve_up", {}).get(rid, {}) | h.get("reserve_down", {}).get(rid, {}))}),
            "reserve_supply_by_type": reserve_supply_by_type,
            "reserve_shortfall_by_product": res_shortfall,
            "dc_network": normalize_dc_network(inp, assets),
            "objective_breakdown": objective_breakdown,
        },
        "system_summary": {
            "total_cost_usd":      round(total_cost, 0),
            "total_objective_cost_usd": round(reconstructed, 0),
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
            "reserve_shortfall_mwh": res_shortfall,
            # v1.5 Thermal Stage 3 — system-wide CO₂ totals.
            "total_co2_t":         round(sum(bu.get("total_co2_t", 0) for bu in by_unit.values()), 2),
            "co2_price_usd_per_t": float(inp.get("co2_price_usd_per_t", 0) or 0),
            "total_co2_cost_usd":  round(sum(bu.get("co2_cost_usd", 0) for bu in by_unit.values()), 0),
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
                "bus_angle_rad": h.get("bus_angle_rad", {}),
                "bus_injection_mw": h.get("bus_injection_mw", {}),
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

    # Stage 2 canonical publication path. Legacy output is rounded during
    # extraction, so reconciliation intentionally uses persisted tolerances.
    from powersim.contracts import QAStatus, ResultValidity, SolverDiagnostics, SolverStatus
    from powersim.qa import run_qa
    from powersim.results import evaluate_publication, finalized_diagnostics
    solver_diagnostics = getattr(hourly, "solver_diagnostics", None)
    if solver_diagnostics is None:
        solver_diagnostics = SolverDiagnostics(backend="unknown", termination_condition="diagnostics unavailable",
            normalized_status=SolverStatus.SOLVER_ERROR, has_incumbent=False,
            result_validity=ResultValidity.INVALID, qa_status=QAStatus.NOT_RUN,
            metadata={"unavailability_reason":"caller did not provide solve execution context"})
    component_results = list(getattr(hourly, "component_results", None) or [])
    reserve_results = list(getattr(hourly, "reserve_results", None) or [])
    if component_results:
        from dataclasses import asdict
        result["component_results"] = [asdict(item) for item in component_results]
    else:
        result["component_results"] = []
    if reserve_results:
        from dataclasses import asdict
        result["reserve_results"] = [asdict(item) for item in reserve_results]
    else:
        result["reserve_results"] = []

    qa_report = run_qa(
        inp,
        result,
        persisted=True,
        component_results=component_results,
        component_duration_hours=dt_h if (component_results or reserve_results) else None,
        reserve_results=reserve_results,
    )
    finite_check = next((check for check in qa_report.checks if check.check_id == "finite_values"), None)
    required_finite = finite_check is not None and finite_check.status == QAStatus.PASS
    decision = evaluate_publication(solver_diagnostics, qa_report,
        extraction_completed=bool(getattr(hourly, "extraction_completed", False)),
        required_values_finite=required_finite,
        # A deterministic result is not publishable unless its incumbent
        # objective has been independently reconciled from canonical output.
        required_check_ids=("objective_reconstruction",))
    solver_diagnostics = finalized_diagnostics(solver_diagnostics, qa_report.status, decision.publishable)
    result["qa"] = qa_report.model_dump(mode="json")
    result["publication"] = {"publishable":decision.publishable, "reasons":list(decision.reasons)}
    result["diagnostics"].update({
        "solver_status":solver_diagnostics.normalized_status.value,
        "solver_diagnostics":solver_diagnostics.model_dump(mode="json"),
        "window_diagnostics":getattr(hourly, "window_diagnostics", []),
        "mip_gap_pct":solver_diagnostics.actual_mip_gap * 100 if solver_diagnostics.actual_mip_gap is not None else None,
        "result_validity":solver_diagnostics.result_validity.value,
        "qa_status":solver_diagnostics.qa_status.value,
        "qa_tolerance_mode":"persisted",
    })

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

        # By Unit Summary — union of all keys across assets (type-specific
        # fields like bess_throughput_mwh appear only on BESS rows).
        bu = results["by_unit_summary"]
        if bu:
            all_keys = []
            seen = set()
            for d in bu.values():
                for k in d.keys():
                    if k not in seen:
                        all_keys.append(k); seen.add(k)
            cols = ["id"] + all_keys
            bu_rows = [[gid] + [d.get(k) for k in all_keys] for gid, d in bu.items()]
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

        sc_norm = dict(sc)
        sc_norm.update({"id": sid, "label": label, "prob": prob})
        out.append(sc_norm)
        total += prob
    if not out:
        return list(_DEFAULT_STOCH_SCENARIOS)
    # Renormalise if probabilities don't sum to ~1.
    if total > 0 and abs(total - 1.0) > 1e-6:
        for s in out:
            s["prob"] = s["prob"] / total
    return out


def _apply_stochastic_profile_overrides(inp: dict, sc: dict) -> tuple[dict, list[str]]:
    import copy
    inp_sc = copy.deepcopy(inp)
    warnings = []
    overrides = sc.get("profile_overrides") or sc.get("profiles") or {}
    if overrides:
        inp_sc.setdefault("profiles", {})
        for key, val in overrides.items():
            inp_sc["profiles"][key] = val
    else:
        switched = False
        sid = sc.get("id")
        for key, val in list((inp_sc.get("profiles") or {}).items()):
            if isinstance(val, dict) and sid in val:
                inp_sc["profiles"][key] = val[sid]
                switched = True
        if not switched:
            warnings.append("stochastic_profiles_not_switched")
    inp_sc["scenario_metadata"] = {"id": sc["id"], "label": sc["label"], "probability": sc["prob"]}
    return inp_sc, warnings


def _weighted_percentile(items: list[tuple[float, float]], q: float) -> float | None:
    if not items:
        return None
    total = sum(w for _, w in items)
    target = q * total
    acc = 0.0
    for val, wt in sorted(items):
        acc += wt
        if acc >= target:
            return val
    return sorted(items)[-1][0]


def run_stochastic(inp: dict) -> dict:
    """Run full solves for each stochastic scenario and summarize objective costs."""
    scenarios = _resolve_stoch_scenarios(inp)
    print(f"🎲 Stochastic run with {len(scenarios)} scenario(s) "
          f"(source: {'input' if inp.get('stochastic_scenarios') else 'default'})")
    rows = []
    diagnostics = {"warnings": []}
    for sc in scenarios:
        print(f"\n── Scenario {sc['label']} (π={sc['prob']:.3f}) ──")
        inp_sc, warns = _apply_stochastic_profile_overrides(inp, sc)
        diagnostics["warnings"].extend(warns)
        assets = build_asset_map(inp_sc)
        profiles, H = slice_profiles(inp_sc)
        gas_lim = build_gas_limits(inp_sc, int(inp_sc.get("study_horizon",{}).get("start_hour",0)), H)
        hourly, solve_time, obj = solve_all(inp_sc, assets, profiles, gas_lim)
        result = build_result_store(hourly, assets, inp_sc, solve_time, obj)
        sm = result.get("system_summary", {})
        diag = result.get("diagnostics", {})
        ob = diag.get("objective_breakdown", {})
        rows.append({
            "id": sc["id"], "label": sc["label"], "probability": sc["prob"], "prob": sc["prob"],
            "total_objective_cost_usd": sm.get("total_objective_cost_usd", ob.get("total_reconstructed", obj)),
            "total_cost_usd": sm.get("total_cost_usd"),
            "avg_lambda_usd_mwh": sm.get("avg_lambda_usd_mwh"),
            "total_unserved_mwh": sm.get("total_unserved_mwh", 0),
            "total_curtailed_mwh": sm.get("total_curtailed_mwh", 0),
            "total_gas_mm3": sm.get("total_gas_mm3", 0),
            "reserve_shortfall": sm.get("reserve_shortfall_mwh", {}),
            "solve_status": diag.get("solver_status", "solver_error"),
            "closure_ok": result.get("metadata", {}).get("closure_ok"),
        })
    exp_obj = sum(float(r["probability"]) * float(r.get("total_objective_cost_usd") or 0) for r in rows)
    exp_unserved = sum(float(r["probability"]) * float(r.get("total_unserved_mwh") or 0) for r in rows)
    exp_curt = sum(float(r["probability"]) * float(r.get("total_curtailed_mwh") or 0) for r in rows)
    exp_gas = sum(float(r["probability"]) * float(r.get("total_gas_mm3") or 0) for r in rows)
    weighted_costs = [(float(r.get("total_objective_cost_usd") or 0), float(r["probability"])) for r in rows]
    costs_desc = sorted(weighted_costs, key=lambda x: -x[0])
    tail = 1 - CVaR_ALPHA; cum = cvar_n = cvar_d = 0.0
    for cost, prob in costs_desc:
        if cum >= tail: break
        take = min(prob, tail - cum)
        cvar_n += cost * take; cvar_d += take; cum += take
    cvar = cvar_n / cvar_d if cvar_d > 0 else (costs_desc[0][0] if costs_desc else 0)
    return {
        "expected_objective_cost": round(exp_obj, 0), "expected_cost": round(exp_obj, 0),
        "expected_unserved_mwh": round(exp_unserved, 3),
        "expected_curtailed_mwh": round(exp_curt, 3), "expected_gas_mm3": round(exp_gas, 6),
        "p10_cost": None if len(rows) < 2 else round(_weighted_percentile(weighted_costs, 0.10), 0),
        "p50_cost": None if len(rows) < 2 else round(_weighted_percentile(weighted_costs, 0.50), 0),
        "p90_cost": None if len(rows) < 2 else round(_weighted_percentile(weighted_costs, 0.90), 0),
        "cvar95": round(cvar, 0), "risk_premium": round(cvar - exp_obj, 0),
        "scenarios": rows, "diagnostics": diagnostics,
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
    print("✅ Solved and publishable!" if results.get("publication", {}).get("publishable")
          else "❌ Run is invalid and not publishable; inspect diagnostics.")
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
