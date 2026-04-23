"""
PowerSim v4.0 — Input/Output JSON Schema
=========================================
Schema version: 1.2
Timezone:       Asia/Tbilisi
Resolution:     hourly, 8760h (non-leap year)

Changes vs 1.1 (Stage 2):
  + hydro.cascade_upstream          — upstream asset id feeding downstream inflow
  + hydro.cascade_travel_delay_h    — integer hours of travel delay (0..168)
  + hydro.cascade_gain              — 0..1 gain on upstream release (default 1.0)
  + hydro.target_end_level_frac     — strategic end-level (0..1 of reservoir_max)
  + hydro.end_level_penalty         — $/Mm³ penalty below target (soft constraint)
  + gas_constraints.monthly         — proper per-month caps {month_name: Mm³}
  + validate_input() gains cascade, strategic-reservoir, monthly-gas checks

Changes vs 1.0 (Stage 1):
  + Top-level `profile_bundle`    — scenario id + unit metadata + file hashes
  + Top-level `hydro_zone_map`    — asset_id → hydrological zone binding
  + Top-level `re_site_map`       — asset_id → renewable site binding
  + Top-level `demand_spec`       — "shape_times_annual" vs "absolute"
  + OUTPUT_SCHEMA.metadata.data_source_fingerprint
  + New `validate_output()` function
  + `validate_input()` now returns (ok, errors, warnings) — 3-tuple
  + schema_version "1.0" accepted with warning during Stage 1 transition

Usage:
    from powersim_schema import (
        validate_input, validate_output, generate_time_index,
        SCHEMA_VERSION, HYDRO_ZONES, RE_SITES, SCENARIOS,
        HYDRO_INFLOW_UNITS, DEMAND_MODES,
    )
"""

from __future__ import annotations

import unicodedata
from datetime import datetime, timedelta
from typing import Any


# ──────────────────────────────────────────────────────────────────────
# VERSION + FIXED CONSTANTS
# ──────────────────────────────────────────────────────────────────────
SCHEMA_VERSION          = "1.3"
ACCEPTED_SCHEMA_PRIOR   = {"1.0", "1.1", "1.2"}  # accept legacy configs with warning
MODEL_VERSION           = "PowerSim v4.0"
TIMEZONE                = "Asia/Tbilisi"
HOURS_PER_YEAR          = 8760

# Allowed sub-hourly resolutions (minutes per period).
# The default remains 60 for full backward-compatibility with v1.0..v1.2.
ALLOWED_RESOLUTIONS_MIN: tuple[int, ...] = (60, 30, 15, 5)

# Back-ends the solver can drive. 'auto' → HiGHS if Gurobi isn't importable.
ALLOWED_SOLVER_BACKENDS: tuple[str, ...] = ("auto", "highs", "gurobi", "cplex")

# Allowed identifiers for `profile_bundle.scenario_id`.
SCENARIOS: tuple[str, ...] = ("A_mean", "MC_P10", "MC_P50", "MC_P90")

# 16 Georgian hydrological zones. NFC-normalized so equality checks are
# deterministic across file sources that may use NFD composition.
HYDRO_ZONES: tuple[str, ...] = tuple(unicodedata.normalize("NFC", z) for z in (
    "Z01_კოდორი",          "Z02_რიონი-ონი",       "Z03_რიონი-ალპანა",
    "Z04_ტეხური",          "Z05_ყვირილა",         "Z06_სუფსა",
    "Z07_აჭარისწყალი",     "Z08_ლიახვი",          "Z09_ფარავანი",
    "Z10_თუშეთის ალაზანი", "Z11_მთიულეთის არაგვი","Z12_ალაზანი-ბირკიანი",
    "Z13_მაშავერა",        "Z14_ალაზანი-შაქრიანი","Z15_ჭოროხი",
    "Z16_სამგორი",
))

# 10 renewable sites with ready 8760h scenario CSVs.
RE_SITES: tuple[str, ...] = (
    "Tbilisi", "Dedoplistskaro", "Gori", "Kutaisi", "Mta_Sabueti",
    "Ninotsminda", "Rustavi", "Sagarejo", "Telavi", "Zugdidi",
)

# Allowed values for `profile_bundle.hydro_inflow_unit`.
# Stage 1 rule: only "raw" is used until the physical unit is verified.
HYDRO_INFLOW_UNITS: tuple[str, ...] = ("raw", "Mm3_per_h", "m3_per_s", "normalized")

# Allowed values for `demand_spec.mode`.
DEMAND_MODES: tuple[str, ...] = ("absolute", "shape_times_annual")

# Asset type taxonomy, used by validators.
ASSET_TYPES: tuple[str, ...] = (
    "thermal", "hydro_reg", "hydro_ror", "wind", "solar", "import", "bess",
)


# ──────────────────────────────────────────────────────────────────────
# TIME INDEX
# ──────────────────────────────────────────────────────────────────────
def generate_time_index(year: int = 2026) -> list[str]:
    """
    Produce 8760 naive hourly timestamps "YYYY-MM-DD HH:MM" for a non-leap
    year (implicit Asia/Tbilisi). Raises ValueError on a leap year —
    PowerSim v4.0 is non-leap only by contract.
    """
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        raise ValueError(f"PowerSim v4.0 is non-leap only; got year={year}")
    start = datetime(year, 1, 1, 0, 0)
    return [(start + timedelta(hours=h)).strftime("%Y-%m-%d %H:%M")
            for h in range(HOURS_PER_YEAR)]


# ──────────────────────────────────────────────────────────────────────
# INPUT SCHEMA (documentation by example — not used for validation itself)
# ──────────────────────────────────────────────────────────────────────
INPUT_SCHEMA: dict[str, Any] = {
    "metadata": {
        "model_version":  MODEL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "timezone":       TIMEZONE,
        "exported_at":    "ISO8601 timestamp",
        "study_year":     2026,
        "description":    "optional string",
    },

    "time_index": ["2026-01-01 00:00", "..."],  # 8760 strings

    "study_horizon": {
        "start_hour":    0,
        "horizon_hours": 8760,
        "mode":          "auto",          # "auto" | "full" | "rolling"
    },

    # Assets are as in v1.0 — type-specific fields per ASSET_TYPES.
    # When `hydro_zone_map` / `re_site_map` are present, the dataio loader
    # overwrites `inflow_profile` / `availability_profile` on matching assets.
    "assets": [
        {"id": "enguri",      "type": "hydro_reg", "pmin": 195, "pmax": 1300},
        {"id": "gardabani_1", "type": "thermal",   "pmin": 92,  "pmax": 231.2},
    ],

    "profiles": {
        "demand":                 [],    # MW per hour, length 8760
        "demand_shape":           [],    # dimensionless, Σ ≈ 1 (optional)
        "_hydro_enguri":          [],    # derived by dataio
        "_solar_Tbilisi":         [],    # derived by dataio
        "_wind_Dedoplistskaro":   [],    # derived by dataio
    },

    # ── v1.1 NEW blocks ──────────────────────────────────────────────
    "profile_bundle": {
        "scenario_id":       "A_mean",
        "hydro_source":      "2026_A_historical_mean",
        "renewables_source": "A_mean",
        "demand_source":     "CharYear_x_SCurve",      # or "PLEXOS_sc2_2026_absolute"
        "hydro_inflow_unit": "raw",                    # one of HYDRO_INFLOW_UNITS
        "generated_by":      "powersim_dataio 1.0",
        "generated_at":      "ISO8601",
        "file_hashes": {
            "hydro_csv":         "sha256:…",
            "demand_shape_xlsx": "sha256:…",
            "renewables_csvs":   {"wind_Dedoplistskaro": "sha256:…"},
        },
    },
    "hydro_zone_map": {
        "enguri":      {"zone": "Z01_კოდორი", "share": 1.0, "scaling_mw": None},
        "hydro_small": {"zone": "Z08_ლიახვი", "share": 0.3, "scaling_mw": None},
    },
    "re_site_map": {
        "wind_dedoplistskaro": {"source": "wind",  "site": "Dedoplistskaro"},
        "solar_tbilisi":       {"source": "solar", "site": "Tbilisi"},
    },
    "demand_spec": {
        "mode":                 "shape_times_annual",
        "shape_profile_key":    "demand_shape",
        "annual_twh":           15.621,
        "absolute_profile_key": None,
    },

    # ── Unchanged from v1.0 ──────────────────────────────────────────
    "reserve_products": [
        {"id": "FCR", "direction": "symmetric", "requirement": 30.0,
         "shortfall_penalty": 500.0,
         "eligible_units": ["enguri", "gardabani_1"]},
    ],
    "gas_constraints": {
        "mode": "annual", "unit": "Mm3",
        "annual": {"cap": 1200.0},
        "applies_to": ["gardabani_1"],
    },
    "solver_settings": {
        "mip_gap": 0.005, "time_limit_s": 300,
        "rolling_window_h": 168, "rolling_step_h": 24,
        "unserved_penalty": 3000.0, "curtailment_penalty": 0.0,
        "threads": 0,
    },
    "scenario_metadata": {"id": "A_mean", "label": "Base", "probability": 1.0},
}


# ──────────────────────────────────────────────────────────────────────
# OUTPUT SCHEMA (documentation by example)
# ──────────────────────────────────────────────────────────────────────
OUTPUT_SCHEMA: dict[str, Any] = {
    "metadata": {
        "model_version":  MODEL_VERSION,
        "schema_version": SCHEMA_VERSION,
        "scenario":       "A_mean",
        "solved_at":      "ISO8601",
        "study_start":    "2026-01-01 00:00",
        "horizon_hours":  168,
        "resolution_h":   1,
        # v1.1 NEW: provenance
        "data_source_fingerprint": {
            "profile_bundle":    {"...": "..."},
            "input_file_hashes": {"hydro_csv": "sha256:…"},
            "loader_version":    "powersim_dataio 1.0",
        },
    },
    "diagnostics":        {"solver_status": "Optimal"},
    "system_summary":     {"total_cost_usd": 0.0},
    "hourly_system":      [{"t": 0, "load_mw": 420.0}],
    "hourly_by_unit":     {"enguri": [{"t": 0, "dispatch_mw": 400.0}]},
    "by_unit_summary":    {"enguri": {"energy_mwh": 0.0}},
    "monthly_summary":    [],
    "stochastic_summary": None,
}


# ──────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────────────
def _nfc(s: Any) -> Any:
    """NFC-normalize a string; passthrough for non-strings."""
    return unicodedata.normalize("NFC", s) if isinstance(s, str) else s


# ──────────────────────────────────────────────────────────────────────
# INPUT VALIDATION
# ──────────────────────────────────────────────────────────────────────
def validate_input(inp: dict) -> tuple[bool, list[str], list[str]]:
    """
    Validate a PowerSim input dict against schema v1.1.

    Returns:
        (ok, errors, warnings).  ok == (len(errors) == 0).
        Warnings never fail the gate.

    Accepts schema_version "1.0" with a warning (Stage 1 transition only);
    rejects any other value that is not "1.1".
    """
    errors:   list[str] = []
    warnings: list[str] = []

    # ── metadata + schema_version ───────────────────────────────────
    meta = inp.get("metadata") or {}
    sv = meta.get("schema_version")
    if sv == SCHEMA_VERSION:
        pass
    elif sv in ACCEPTED_SCHEMA_PRIOR:
        warnings.append(f"schema_version '{sv}' accepted in Stage 1 "
                        "(will be rejected in Stage 2)")
    else:
        errors.append(f"schema_version '{sv}' unsupported "
                      f"(expected '{SCHEMA_VERSION}')")

    # ── time_index ──────────────────────────────────────────────────
    ti = inp.get("time_index") or []
    if ti and len(ti) != HOURS_PER_YEAR:
        errors.append(f"time_index length {len(ti)} ≠ {HOURS_PER_YEAR}")

    # ── v1.3: resolution_min — sub-hourly support ───────────────────
    res_min = inp.get("resolution_min", 60)
    if res_min not in ALLOWED_RESOLUTIONS_MIN:
        errors.append(
            f"resolution_min '{res_min}' not in {ALLOWED_RESOLUTIONS_MIN}")
    periods_per_year = HOURS_PER_YEAR * (60 // res_min) if res_min in ALLOWED_RESOLUTIONS_MIN else HOURS_PER_YEAR

    # ── profiles ────────────────────────────────────────────────────
    # In v1.3 profile arrays must be HOURS_PER_YEAR (hourly base) OR
    # periods_per_year (matching resolution_min). We accept both — the
    # solver resamples at load time.
    profiles = inp.get("profiles") or {}
    accepted_lengths = {HOURS_PER_YEAR, periods_per_year}
    demand = profiles.get("demand") or []
    if not demand:
        errors.append("profiles.demand is required")
    else:
        if len(demand) not in accepted_lengths:
            errors.append(
                f"profiles.demand length {len(demand)} "
                f"not in {sorted(accepted_lengths)} "
                f"(hourly base or resolution_min={res_min}min)")
        if any((v is None) or (v != v) for v in demand):
            errors.append("profiles.demand contains NaN/None")
        if any(isinstance(v, (int, float)) and v < 0 for v in demand):
            errors.append("profiles.demand contains negative values")

    for key, arr in profiles.items():
        if key == "demand" or not isinstance(arr, list):
            continue
        if len(arr) not in accepted_lengths:
            errors.append(
                f"profile '{key}' length {len(arr)} "
                f"not in {sorted(accepted_lengths)}")
        if any((v is None) or (v != v) for v in arr):
            errors.append(f"profile '{key}' contains NaN/None")

    # ── assets ──────────────────────────────────────────────────────
    asset_ids: set[str] = set()
    for a in inp.get("assets") or []:
        aid = a.get("id")
        if not aid:
            errors.append(f"asset missing id: name={a.get('name', '?')}")
            continue
        if aid in asset_ids:
            errors.append(f"duplicate asset id: {aid}")
        asset_ids.add(aid)

        atype = a.get("type")
        if atype not in ASSET_TYPES:
            errors.append(f"asset '{aid}': unknown type '{atype}'")
            continue
        if atype == "thermal":
            for f in ("pmin", "pmax", "heat_rate", "ramp_up", "ramp_down",
                      "min_up", "min_down"):
                if f not in a:
                    errors.append(f"thermal '{aid}' missing field '{f}'")
            if "pmin" in a and "pmax" in a and a["pmin"] > a["pmax"]:
                errors.append(f"thermal '{aid}': pmin > pmax")
        elif atype in ("wind", "solar"):
            if "pmax_installed" not in a:
                errors.append(f"{atype} '{aid}' missing pmax_installed")
        elif atype == "bess":
            for f in ("power_mw", "energy_mwh", "soc_init", "soc_min",
                      "soc_max", "eta_charge", "eta_discharge"):
                if f not in a:
                    errors.append(f"bess '{aid}' missing field '{f}'")

    # ── v1.1: profile_bundle ────────────────────────────────────────
    pb = inp.get("profile_bundle")
    if pb is not None:
        if not isinstance(pb, dict):
            errors.append("profile_bundle must be a dict")
        else:
            unit = pb.get("hydro_inflow_unit", "raw")
            if unit not in HYDRO_INFLOW_UNITS:
                errors.append(
                    f"profile_bundle.hydro_inflow_unit '{unit}' "
                    f"not in {HYDRO_INFLOW_UNITS}")
            sc = pb.get("scenario_id")
            if sc is not None and sc not in SCENARIOS:
                warnings.append(
                    f"profile_bundle.scenario_id '{sc}' not in {SCENARIOS}")

    # ── v1.1: hydro_zone_map ────────────────────────────────────────
    hzm = inp.get("hydro_zone_map")
    if hzm is not None:
        if not isinstance(hzm, dict):
            errors.append("hydro_zone_map must be a dict")
        else:
            for aid, entry in hzm.items():
                if aid not in asset_ids:
                    errors.append(f"hydro_zone_map: unknown asset '{aid}'")
                if not isinstance(entry, dict):
                    errors.append(f"hydro_zone_map['{aid}'] must be a dict")
                    continue
                z = _nfc(entry.get("zone", ""))
                if z not in HYDRO_ZONES:
                    errors.append(
                        f"hydro_zone_map['{aid}'].zone '{z}' "
                        f"not a known hydro zone")
                share = entry.get("share", 1.0)
                if not (isinstance(share, (int, float)) and 0 < share <= 1):
                    errors.append(
                        f"hydro_zone_map['{aid}'].share must be in (0, 1]")

    # ── v1.1: re_site_map ───────────────────────────────────────────
    rsm = inp.get("re_site_map")
    if rsm is not None:
        if not isinstance(rsm, dict):
            errors.append("re_site_map must be a dict")
        else:
            for aid, entry in rsm.items():
                if aid not in asset_ids:
                    errors.append(f"re_site_map: unknown asset '{aid}'")
                if not isinstance(entry, dict):
                    errors.append(f"re_site_map['{aid}'] must be a dict")
                    continue
                if entry.get("source") not in ("wind", "solar"):
                    errors.append(
                        f"re_site_map['{aid}'].source must be 'wind' or 'solar'")
                if entry.get("site") not in RE_SITES:
                    errors.append(
                        f"re_site_map['{aid}'].site '{entry.get('site')}' "
                        f"not in RE_SITES")

    # ── v1.1: demand_spec ───────────────────────────────────────────
    ds = inp.get("demand_spec")
    if ds is not None:
        if not isinstance(ds, dict):
            errors.append("demand_spec must be a dict")
        else:
            mode = ds.get("mode")
            if mode not in DEMAND_MODES:
                errors.append(f"demand_spec.mode '{mode}' not in {DEMAND_MODES}")
            elif mode == "shape_times_annual":
                spk = ds.get("shape_profile_key")
                if not spk or spk not in profiles:
                    errors.append(
                        f"demand_spec.shape_profile_key '{spk}' not in profiles")
                atw = ds.get("annual_twh")
                if not (isinstance(atw, (int, float)) and atw > 0):
                    errors.append("demand_spec.annual_twh must be > 0")

    # ── reserves ────────────────────────────────────────────────────
    for rp in inp.get("reserve_products") or []:
        rid = rp.get("id", "?")
        for uid in rp.get("eligible_units", []):
            if uid not in asset_ids:
                errors.append(f"reserve '{rid}': unit '{uid}' not in assets")
        if rp.get("direction") not in ("up", "down", "symmetric"):
            errors.append(
                f"reserve '{rid}': bad direction '{rp.get('direction')}'")

    # ── Stage 2: hydro cascade + strategic reservoir fields ─────────
    # These are additive; absence is accepted. We only validate that
    # cascade references point to existing hydro assets and that
    # scalar ranges are sane.
    for a in inp.get("assets") or []:
        aid = a.get("id")
        if a.get("type") not in ("hydro_reg", "hydro_ror"):
            continue
        h = a.get("hydro") or {}
        # Cascade validation
        up = h.get("cascade_upstream")
        if up is not None:
            if up not in asset_ids:
                errors.append(
                    f"hydro '{aid}': cascade_upstream '{up}' not in assets")
            elif up == aid:
                errors.append(f"hydro '{aid}': cascade_upstream cannot be self")
        delay = h.get("cascade_travel_delay_h", 0)
        if not (isinstance(delay, int) and 0 <= delay <= 168):
            errors.append(
                f"hydro '{aid}': cascade_travel_delay_h must be int in [0, 168]")
        gain = h.get("cascade_gain", 1.0)
        if not (isinstance(gain, (int, float)) and 0 <= gain <= 1.1):
            warnings.append(
                f"hydro '{aid}': cascade_gain {gain!r} outside typical (0, 1.0]")
        # Strategic end-of-horizon reservoir fields
        tel = h.get("target_end_level_frac")
        if tel is not None and not (isinstance(tel, (int, float)) and 0 <= tel <= 1):
            errors.append(
                f"hydro '{aid}': target_end_level_frac must be in [0, 1]")
        elp = h.get("end_level_penalty")
        if elp is not None and not (isinstance(elp, (int, float)) and elp >= 0):
            errors.append(
                f"hydro '{aid}': end_level_penalty must be >= 0")

    # ── Stage 2: monthly gas_constraints validation ─────────────────
    gc = inp.get("gas_constraints") or {}
    if gc:
        mode = gc.get("mode", "none")
        if mode not in ("none", "annual", "monthly", "annual+monthly"):
            errors.append(
                f"gas_constraints.mode '{mode}' must be one of "
                f"'none'/'annual'/'monthly'/'annual+monthly'")
        if mode in ("monthly", "annual+monthly"):
            monthly = gc.get("monthly", {})
            if not isinstance(monthly, dict) or not monthly:
                errors.append(
                    "gas_constraints.monthly must be a non-empty dict "
                    "when mode includes 'monthly'")
            else:
                # Accept month names OR month-number strings "1".."12"
                _MONTH_NAMES = (
                    "Jan","Feb","Mar","Apr","May","Jun",
                    "Jul","Aug","Sep","Oct","Nov","Dec",
                )
                for k, v in monthly.items():
                    key_ok = (k in _MONTH_NAMES) or (
                        isinstance(k, str) and k.isdigit() and 1 <= int(k) <= 12
                    ) or (isinstance(k, int) and 1 <= k <= 12)
                    if not key_ok:
                        errors.append(
                            f"gas_constraints.monthly key '{k}' must be a "
                            f"month name {_MONTH_NAMES} or number 1..12")
                    if not (isinstance(v, (int, float)) and v >= 0):
                        errors.append(
                            f"gas_constraints.monthly['{k}'] cap must be >= 0")

    # ── study_horizon ───────────────────────────────────────────────
    sh = inp.get("study_horizon") or {}
    start = int(sh.get("start_hour", 0))
    hlen  = int(sh.get("horizon_hours", HOURS_PER_YEAR))
    if start < 0 or hlen < 1 or start + hlen > HOURS_PER_YEAR:
        errors.append(
            f"study_horizon out of range: "
            f"start={start}, horizon={hlen}, cap={HOURS_PER_YEAR}")

    # ── v1.3: piecewise heat_rate_curve on thermal ───────────────────
    for a in inp.get("assets") or []:
        if a.get("type") != "thermal":
            continue
        hrc = a.get("heat_rate_curve")
        if hrc is None:
            continue
        if not isinstance(hrc, list) or len(hrc) < 2:
            errors.append(f"thermal '{a.get('id')}': heat_rate_curve must be a list of "
                          "≥2 [pmw, heat_rate] pairs when provided")
            continue
        prev_p = -1.0
        for pt in hrc:
            if not (isinstance(pt, (list, tuple)) and len(pt) == 2):
                errors.append(f"thermal '{a.get('id')}': heat_rate_curve entry {pt!r} must be [pmw, hr]")
                break
            p, hr = pt
            if not (isinstance(p, (int, float)) and isinstance(hr, (int, float))):
                errors.append(f"thermal '{a.get('id')}': heat_rate_curve non-numeric in {pt!r}")
                break
            if p <= prev_p:
                errors.append(f"thermal '{a.get('id')}': heat_rate_curve breakpoints must strictly increase in pmw")
                break
            prev_p = p

    # ── v1.3: solver backend selector ─────────────────────────────
    scfg = inp.get("solver_settings") or {}
    bke = scfg.get("solver", "auto")
    if bke not in ALLOWED_SOLVER_BACKENDS:
        errors.append(f"solver_settings.solver '{bke}' not in {ALLOWED_SOLVER_BACKENDS}")
    ws = scfg.get("warm_start", False)
    if not isinstance(ws, bool):
        errors.append("solver_settings.warm_start must be boolean")

    # ── v1.3: stochastic_tree (2-stage UC) ────────────────────────
    st = inp.get("stochastic_tree")
    if st is not None:
        if not isinstance(st, dict):
            errors.append("stochastic_tree must be a dict")
        else:
            scs = st.get("scenarios") or []
            if not isinstance(scs, list) or len(scs) < 2:
                errors.append("stochastic_tree.scenarios must be a list of ≥2 scenarios")
            else:
                total = 0.0
                seen = set()
                for sc in scs:
                    sid = sc.get("id")
                    if not sid or sid in seen:
                        errors.append(f"stochastic_tree: duplicate or missing scenario id '{sid}'")
                        continue
                    seen.add(sid)
                    p = sc.get("prob", sc.get("probability"))
                    if not (isinstance(p, (int, float)) and 0 < p <= 1):
                        errors.append(f"stochastic_tree scenario '{sid}': prob must be (0, 1]")
                    else:
                        total += p
                if abs(total - 1.0) > 1e-4:
                    warnings.append(
                        f"stochastic_tree: scenario probabilities sum to {total:.4f} "
                        "(solver will renormalize)")
            obj = st.get("objective", "expected")
            if obj not in ("expected", "cvar", "expected+cvar"):
                errors.append(f"stochastic_tree.objective '{obj}' must be expected|cvar|expected+cvar")
            alpha = st.get("cvar_alpha", 0.95)
            if not (isinstance(alpha, (int, float)) and 0 < alpha < 1):
                errors.append("stochastic_tree.cvar_alpha must be in (0, 1)")

    # ── v1.3: capacity expansion (LT-Plan) ────────────────────────
    ex = inp.get("expansion")
    if ex is not None:
        if not isinstance(ex, dict):
            errors.append("expansion must be a dict")
        else:
            mode = ex.get("mode", "fixed")
            if mode not in ("fixed", "enabled"):
                errors.append(f"expansion.mode '{mode}' must be 'fixed' or 'enabled'")
            disc = ex.get("discount_rate", 0.08)
            if not (isinstance(disc, (int, float)) and 0 <= disc < 0.5):
                errors.append("expansion.discount_rate must be in [0, 0.5)")
            years = ex.get("years", 1)
            if not (isinstance(years, int) and 1 <= years <= 30):
                errors.append("expansion.years must be integer in [1, 30]")
            cands = ex.get("candidates") or []
            if not isinstance(cands, list):
                errors.append("expansion.candidates must be a list")
            else:
                for c in cands:
                    for f in ("id", "type", "capex_per_mw", "max_build_mw"):
                        if f not in c:
                            errors.append(f"expansion candidate missing field '{f}'")

    # ── v1.3: kpi_templates ──────────────────────────────────────
    kps = inp.get("kpi_templates")
    if kps is not None:
        if not isinstance(kps, list):
            errors.append("kpi_templates must be a list")
        else:
            for k in kps:
                if not (isinstance(k, dict) and k.get("id") and k.get("formula")):
                    errors.append("each kpi_template needs id + formula")

    ok = len(errors) == 0
    return ok, errors, warnings


# ──────────────────────────────────────────────────────────────────────
# OUTPUT VALIDATION
# ──────────────────────────────────────────────────────────────────────
_REQUIRED_OUTPUT_KEYS = (
    "metadata", "diagnostics", "system_summary",
    "hourly_system", "hourly_by_unit", "by_unit_summary",
)


def validate_output(out: dict) -> tuple[bool, list[str], list[str]]:
    """
    Structural validator for solver output JSON against schema v1.1.
    Does NOT re-check cost arithmetic — only shape and internal consistency.

    Returns:
        (ok, errors, warnings).
    """
    errors:   list[str] = []
    warnings: list[str] = []

    if not isinstance(out, dict):
        return False, ["output is not a dict"], []

    for k in _REQUIRED_OUTPUT_KEYS:
        if k not in out:
            errors.append(f"missing top-level key '{k}'")

    meta = out.get("metadata") or {}
    sv = meta.get("schema_version")
    if sv == SCHEMA_VERSION:
        pass
    elif sv in ACCEPTED_SCHEMA_PRIOR:
        warnings.append(f"output schema_version '{sv}' accepted "
                        "(v1.0 transition)")
    else:
        errors.append(f"output schema_version '{sv}' unsupported")

    # Horizon vs hourly_system length coherence.
    H  = meta.get("horizon_hours")
    hs = out.get("hourly_system")
    if H is not None and isinstance(hs, list) and len(hs) != H:
        errors.append(
            f"hourly_system length {len(hs)} ≠ metadata.horizon_hours {H}")

    # Per-unit series length + summary presence.
    hbu = out.get("hourly_by_unit") or {}
    bus = out.get("by_unit_summary") or {}
    for gid in hbu:
        if gid not in bus:
            warnings.append(
                f"asset '{gid}' in hourly_by_unit but not in by_unit_summary")
        rows = hbu.get(gid)
        if isinstance(rows, list) and H is not None and len(rows) != H:
            errors.append(f"hourly_by_unit['{gid}'] length {len(rows)} ≠ {H}")

    if "data_source_fingerprint" in meta and \
            not isinstance(meta["data_source_fingerprint"], dict):
        errors.append("metadata.data_source_fingerprint must be a dict")

    ok = len(errors) == 0
    return ok, errors, warnings


# ──────────────────────────────────────────────────────────────────────
# CLI SELF-TEST
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"PowerSim Schema v{SCHEMA_VERSION} ({MODEL_VERSION})")
    print(f"  Timezone:     {TIMEZONE}")
    print(f"  Hours/year:   {HOURS_PER_YEAR}")
    print(f"  Hydro zones:  {len(HYDRO_ZONES)}")
    print(f"  RE sites:     {len(RE_SITES)}")
    print(f"  Scenarios:    {SCENARIOS}")
    ti = generate_time_index()
    print(f"  Time index:   {ti[0]}  →  {ti[-1]}  ({len(ti)}h)")

    # Minimal Stage-1 input smoke
    ex = {
        "metadata": {"schema_version": SCHEMA_VERSION, "study_year": 2026},
        "time_index": generate_time_index(),
        "assets": [
            {"id": "gardabani_1", "type": "thermal",
             "pmin": 92, "pmax": 231.2, "heat_rate": 6.8,
             "ramp_up": 50, "ramp_down": 50, "min_up": 3, "min_down": 2},
        ],
        "profiles": {"demand": [500.0] * HOURS_PER_YEAR},
        "study_horizon": {"start_hour": 0, "horizon_hours": 168, "mode": "full"},
    }
    ok, errs, warns = validate_input(ex)
    print(f"\nSelf-test validate_input: ok={ok} errors={len(errs)} warnings={len(warns)}")
    for e in errs:  print("  ERROR:", e)
    for w in warns: print("  WARN: ", w)
