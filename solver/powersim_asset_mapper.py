"""
PowerSim v4.0 — Asset Mapping Layer
====================================
Data-driven transformation from the GSE installed-capacity workbook
"დადგმული სიმძლავრე - 2026.xlsx" → list of assets compatible with
powersim_schema v1.1.

This module is the single place that understands the workbook layout.
It is pure data-shaping: no solver calls, no HTML coupling, no schema
modifications.  The solver stays untouched; the schema stays untouched.
Only the input-preparation layer gains a new entry point.

Typical usage (library):
    from powersim_asset_mapper import build_assets_from_capacity_excel
    assets, hzm, rsm, diag = build_assets_from_capacity_excel(
        "/path/to/დადგმული_სიმძლავრე__2026.xlsx")

Typical usage (CLI preview):
    python solver/powersim_asset_mapper.py \\
        --excel /path/to/დადგმული_სიმძლავრე__2026.xlsx \\
        --preview 10
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import pandas as pd


# ── Schema coupling (defensive, like the loader) ─────────────────────
try:
    from powersim_schema import (                               # type: ignore
        HYDRO_ZONES, RE_SITES,
    )
except ImportError:
    _here = Path(__file__).resolve().parent
    for _p in (_here.parent / "schema", _here):
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
    try:
        from powersim_schema import HYDRO_ZONES, RE_SITES       # type: ignore
    except ImportError:
        # Keep the module importable even without schema — minimal fallbacks.
        HYDRO_ZONES = ()
        RE_SITES    = ("Tbilisi", "Dedoplistskaro", "Gori", "Kutaisi",
                       "Mta_Sabueti", "Ninotsminda", "Rustavi", "Sagarejo",
                       "Telavi", "Zugdidi")


MAPPER_VERSION = "powersim_asset_mapper 1.0.0"


# ══════════════════════════════════════════════════════════════════════
# Constants — category mapping, placeholder costs, geographic matching
# ══════════════════════════════════════════════════════════════════════
# Excel category string → (schema type, extra defaults dict).
# Every plant's `type` is decided by its category cell (column 5).
# Edit this mapping to re-classify — it is the only authoritative table.
_CATEGORY_TO_TYPE: dict[str, tuple[str, dict]] = {
    "Hydro - Reservoir": ("hydro_reg", {"_subclass": "reservoir"}),
    "Hydro - Seasonal":  ("hydro_reg", {"_subclass": "seasonal"}),
    "Hydro - Small":     ("hydro_ror", {"_subclass": "small_assumed_ror"}),
    "Solar":             ("solar",     {}),
    "Thermal":           ("thermal",   {"fuel_type": "gas"}),
    "Wind":              ("wind",      {}),
}


# Placeholder cost / dynamic parameters by schema type.  These are NOT
# calibrated — they are structured defaults so the solver has values to
# work with.  Every field here is flagged in NEEDS_CALIBRATION below.
_THERMAL_DEFAULTS_GAS = {
    "heat_rate":     7.2,      # MMBtu/MWh  — typical CCGT placeholder
    "fuel_price":    7.0,      # $/MMBtu
    "vom":           2.0,      # $/MWh
    "startup_cost":  10000.0,  # $/start
    "no_load_cost":  500.0,    # $/h
    "ramp_up":       60.0,     # MW/h
    "ramp_down":     60.0,
    "min_up":        3,        # hours
    "min_down":      2,
    "for_rate":      0.08,     # forced-outage rate
}
_THERMAL_DEFAULTS_COAL = {
    "heat_rate":     10.5,
    "fuel_price":    4.0,
    "vom":           3.5,
    "startup_cost":  40000.0,
    "no_load_cost":  1200.0,
    "ramp_up":       20.0,
    "ramp_down":     20.0,
    "min_up":        6,
    "min_down":      6,
    "for_rate":      0.12,
}

# Hydro reservoir defaults — only used as fallbacks, per-plant values
# should come from a calibrated override table (see D2 CSV template).
# The numbers below are per-unit-of-installed-capacity rules of thumb,
# then multiplied by pmax.
#
# D2 revision: previous rule (10× / 20× pmax) was far too generous and
# let the solver treat hydro as essentially unlimited over any horizon.
# New default: reservoir_init = 2× pmax, reservoir_max = 4× pmax.
# For Enguri (pmax=1300 MW) this gives init=2600 Mm³ — still not the
# real ~1100, but only ~2× off instead of ~12× off.  Real numbers come
# from the per-plant override CSV.
_HYDRO_RESERVOIR_DEFAULTS = {
    "efficiency":    350.0,    # MWh / Mm³ (placeholder)
    "water_value":   15.0,     # $/MWh opportunity cost
    "spill_cost":    0.0,
    "travel_delay_h":0,        # cascade behaviour deferred to Stage 2
    "conversion_mode": 2,
    "ramp_fraction": 0.30,     # 30% of pmax per hour (generous hydro)
    "pmin_fraction": 0.15,     # 15% of pmax minimum stable load
    # D2: shrunk multipliers
    "init_x_pmax":   2.0,      # reservoir_init = 2 × pmax (was 10×)
    "max_x_pmax":    4.0,      # reservoir_max  = 4 × pmax (was 20×)
    "end_min_frac":  0.5,      # end_min = 50% of init
}
_HYDRO_ROR_DEFAULTS = {
    "efficiency":    400.0,
    "water_value":   10.0,
    "spill_cost":    0.0,
    "ramp_fraction": 0.50,
    "pmin_fraction": 0.0,      # small run-of-river: no min stable load
}

# Site matching — fuzzy Georgian → English site lookup.  Keyword hits
# in the plant name pick a PowerSim RE_SITES entry.  Plants that don't
# match any keyword are still emitted, but with `availability_profile:
# null` and a warning — the caller can override.
_RE_SITE_KEYWORDS: dict[str, list[str]] = {
    # keyword (lowercase, unaccented for matching)     → site name
    "ქართლი":        ["Gori"],
    "ქართლის":       ["Gori"],
    "ზემო ვინდი":   ["Dedoplistskaro"],
    "ზემო":          ["Dedoplistskaro"],
    "გამარჯვება":    ["Telavi"],
    "გამარჯვების":   ["Telavi"],
    "ჯუგაანი":       ["Telavi"],
    "ჯუგაანის":      ["Telavi"],
    "ხირსი":         ["Telavi"],
    "ხირსის":        ["Telavi"],
    "ქისტაური":      ["Telavi"],
    "ქისტაურის":     ["Telavi"],
    "კასპი":         ["Gori"],
    "კასპის":        ["Gori"],
    "ქედა":          ["Kutaisi"],
    "ქედის":         ["Kutaisi"],
    "ნინოწმინდა":    ["Ninotsminda"],
    "რუსთავ":        ["Rustavi"],
    "საგარეჯ":       ["Sagarejo"],
    "ზუგდიდ":        ["Zugdidi"],
    "თელავ":         ["Telavi"],
    "თბილის":        ["Tbilisi"],
    "მთა საბუეტი":  ["Mta_Sabueti"],
    "საბუეტი":      ["Mta_Sabueti"],
}


# ══════════════════════════════════════════════════════════════════════
# DIAGNOSTIC CONTAINER
# ══════════════════════════════════════════════════════════════════════
@dataclass
class MappingDiagnostics:
    """Structured record of what the mapper did and what's uncertain."""
    n_rows_seen:           int             = 0
    n_assets_emitted:      int             = 0
    n_skipped:             int             = 0
    section_counts:        dict[str, int]  = field(default_factory=dict)
    section_totals_mw:     dict[str, float]= field(default_factory=dict)
    duplicates_renamed:    list[tuple]     = field(default_factory=list)
    warnings:              list[str]       = field(default_factory=list)
    needs_calibration:     list[str]       = field(default_factory=list)
    unmatched_re_sites:    list[str]       = field(default_factory=list)
    matched_re_sites:      list[tuple]     = field(default_factory=list)
    special_cases:         list[str]       = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "mapper_version":     MAPPER_VERSION,
            "n_rows_seen":        self.n_rows_seen,
            "n_assets_emitted":   self.n_assets_emitted,
            "n_skipped":          self.n_skipped,
            "section_counts":     self.section_counts,
            "section_totals_mw":  self.section_totals_mw,
            "duplicates_renamed": self.duplicates_renamed,
            "warnings":           self.warnings,
            "needs_calibration":  self.needs_calibration,
            "unmatched_re_sites": self.unmatched_re_sites,
            "matched_re_sites":   self.matched_re_sites,
            "special_cases":      self.special_cases,
        }


# ══════════════════════════════════════════════════════════════════════
# NAME NORMALIZATION (Georgian → ASCII-friendly id)
# ══════════════════════════════════════════════════════════════════════
# Manual transliteration table — Georgian Mkhedruli to ASCII.
# Chosen to match the conventional Latin transliteration used in GSE
# documentation (Enguri, Zhinvali, etc.) rather than strict ISO 9984.
_KA_TO_LATIN: dict[str, str] = {
    "ა":"a","ბ":"b","გ":"g","დ":"d","ე":"e","ვ":"v","ზ":"z",
    "თ":"t","ი":"i","კ":"k","ლ":"l","მ":"m","ნ":"n","ო":"o",
    "პ":"p","ჟ":"zh","რ":"r","ს":"s","ტ":"t","უ":"u","ფ":"f",
    "ქ":"k","ღ":"gh","ყ":"q","შ":"sh","ჩ":"ch","ც":"ts","ძ":"dz",
    "წ":"ts","ჭ":"ch","ხ":"kh","ჯ":"j","ჰ":"h",
}


def _transliterate_ka(s: str) -> str:
    """Georgian Mkhedruli → Latin; non-Georgian chars pass through."""
    return "".join(_KA_TO_LATIN.get(ch, ch) for ch in s)


def _normalize_id(name: str) -> str:
    """
    Produce a stable, ASCII-only asset id from a Georgian plant name.

    Steps: NFC-normalize → transliterate → lowercase → replace any run
    of non-[a-z0-9] with underscore → strip leading/trailing underscores.
    Collapses `ჰესი` (HPP) and `სადგური` (station) suffixes down to `hesi`
    / `sadguri` but leaves them present so distinct plants remain distinct.
    """
    s = unicodedata.normalize("NFC", name or "").strip()
    s = _transliterate_ka(s)
    s = s.lower()
    # Collapse non-alphanumeric runs to a single underscore.
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s or "unnamed"


# ══════════════════════════════════════════════════════════════════════
# AGGREGATE-STRING PARSER
# ══════════════════════════════════════════════════════════════════════
_AGG_COUNT_X_SIZE = re.compile(r"^(\d+)\s*[xхХ×]\s*([\d.,]+)$")
_AGG_SIZE_X_COUNT = re.compile(r"^([\d.,]+)\s*[xхХ×]\s*(\d+)$")


def _parse_number(tok: str) -> float | None:
    tok = tok.strip().rstrip(";").replace(",", ".")
    if not tok:
        return None
    try:
        return float(tok)
    except ValueError:
        return None


def parse_aggregate(agg_str: object, pmax_mw: float) -> dict:
    """
    Parse the 'აგრეგატების რაოდენობა' column into {n_units, unit_size_mw}.

    Handles real shapes observed in the workbook:
        "5x260"           → 5 × 260
        "3x37,6;"         → 3 × 37.6    (Georgian decimal + trailing ;)
        "2x10.6+2x9.6"    → 4 units, variable sizes (best-effort)
        "12.7+11+12+11"   → 4 units listed explicitly
        "1x2.175"         → 1 × 2.175
        "1"               → 1 unit at pmax
        "x"               → unknown (solar) — fall back to 1 × pmax
        None / empty      → fall back to 1 × pmax
    """
    if agg_str is None or (isinstance(agg_str, float) and pd.isna(agg_str)):
        return {"n_units": 1, "unit_size_mw": float(pmax_mw), "source": "fallback_null"}
    s = str(agg_str).strip()
    if not s or s.lower() == "x":
        return {"n_units": 1, "unit_size_mw": float(pmax_mw), "source": "fallback_unknown"}

    # Pattern 1: "count × size"
    m = _AGG_COUNT_X_SIZE.match(s.rstrip(";").strip())
    if m:
        n = int(m.group(1))
        size = _parse_number(m.group(2)) or 0
        return {"n_units": n, "unit_size_mw": size, "source": "count_x_size"}

    # Pattern 2: "size × count" (rare — seen once as "10.4+8")
    m = _AGG_SIZE_X_COUNT.match(s.rstrip(";").strip())
    if m:
        size = _parse_number(m.group(1)) or 0
        n = int(m.group(2))
        return {"n_units": n, "unit_size_mw": size, "source": "size_x_count"}

    # Pattern 3: sum of terms "a+b+c" or mixed "2x3+1x4"
    if "+" in s:
        terms = [t.strip() for t in s.rstrip(";").split("+")]
        total_n = 0
        for t in terms:
            m = _AGG_COUNT_X_SIZE.match(t)
            if m:
                total_n += int(m.group(1))
            else:
                num = _parse_number(t)
                if num is not None:
                    total_n += 1
        if total_n > 0:
            return {"n_units": total_n,
                    "unit_size_mw": float(pmax_mw) / total_n,
                    "source": "sum_of_terms"}

    # Pattern 4: a bare integer like "1" → single unit at pmax
    if s.isdigit():
        n = int(s)
        return {"n_units": n,
                "unit_size_mw": float(pmax_mw) / max(n, 1),
                "source": "count_only"}

    return {"n_units": 1, "unit_size_mw": float(pmax_mw), "source": "unparsed"}


# ══════════════════════════════════════════════════════════════════════
# RE SITE FUZZY MATCH
# ══════════════════════════════════════════════════════════════════════
def _match_re_site(plant_name_ka: str) -> str | None:
    """
    Return a site name from RE_SITES if any keyword hits, else None.
    Matching is case-insensitive and does substring checks on the
    NFC-normalized Georgian name.
    """
    name = unicodedata.normalize("NFC", (plant_name_ka or "")).lower()
    for kw, candidates in _RE_SITE_KEYWORDS.items():
        if kw.lower() in name:
            for c in candidates:
                if c in RE_SITES:
                    return c
    return None


# ══════════════════════════════════════════════════════════════════════
# D2: RESERVOIR OVERRIDE TABLE
# ══════════════════════════════════════════════════════════════════════
# CSV schema (see the bundled template for an example):
#   asset_id, reservoir_init, reservoir_min, reservoir_max,
#   reservoir_end_min, efficiency, water_value
#
# Any column may be left blank — the default is kept for blank cells.
# Only `asset_id` is mandatory.  Unknown asset_ids are ignored with a
# warning; this makes the file safe to share across fleet revisions.
_OVERRIDE_NUMERIC_FIELDS = (
    "reservoir_init", "reservoir_min", "reservoir_max",
    "reservoir_end_min", "efficiency", "water_value",
)


def load_hydro_overrides(
    path: str | Path | None,
) -> dict[str, dict]:
    """
    Read a reservoir override CSV and return {asset_id: {field: value}}.
    Blank / NaN cells are omitted from the per-asset dict so the default
    flows through.  Returns {} on None input or missing file.
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise DataIOError_like(f"reservoir override CSV not found: {p}")
    df = pd.read_csv(p, comment="#")
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    if "asset_id" not in df.columns:
        raise DataIOError_like(
            f"{p.name}: missing required 'asset_id' column; "
            f"got {list(df.columns)}")
    out: dict[str, dict] = {}
    for _, row in df.iterrows():
        aid = str(row.get("asset_id") or "").strip()
        if not aid:
            continue
        entry: dict = {}
        for f in _OVERRIDE_NUMERIC_FIELDS:
            if f in df.columns:
                v = row.get(f)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    try:
                        entry[f] = float(v)
                    except (TypeError, ValueError):
                        pass
        if entry:
            out[aid] = entry
    return out


# Lightweight exception type used by the override loader; mirrors the
# DataIOError shape without importing the dataio module (keeping this
# file self-contained).
class DataIOError_like(Exception):
    pass


# ══════════════════════════════════════════════════════════════════════
# ROW CLASSIFICATION (data vs header vs total)
# ══════════════════════════════════════════════════════════════════════
_SECTION_HEADER_MARKERS = {"Column1"}
_TOTAL_ROW_MARKERS_COL0 = {"I","II","III","IV","V","VI","VII","VIII"}


def _classify_row(row: pd.Series) -> str:
    """
    Returns one of: 'data', 'section_header', 'section_total',
    'title', 'header', 'empty', 'unknown'.
    """
    v0 = row.iloc[0]
    v1 = row.iloc[1] if len(row) > 1 else None

    # Empty row
    if all((pd.isna(v) or str(v).strip() == "") for v in row.tolist()):
        return "empty"

    v0s = str(v0).strip() if pd.notna(v0) else ""
    v1s = str(v1).strip() if pd.notna(v1) else ""

    # "Column1 | Column2 | ..." repeated section-header row
    if v0s in _SECTION_HEADER_MARKERS:
        return "section_header"

    # Title row: first cell has long Georgian title
    if v0s.startswith("საქართველოს"):
        return "title"

    # Main header row: contains '#' and 'დასახელება'
    if v0s == "#" or v1s == "დასახელება":
        return "header"

    # Section total: Roman numeral in col 0  OR empty col 0 + 'ჯამი' in col 1
    if v0s in _TOTAL_ROW_MARKERS_COL0:
        return "section_total"
    if v0s == "" and ("ჯამი" in v1s or "სისტემა" in v1s or "სულ" in v1s):
        return "section_total"

    # Data row: col 0 should be a positive integer (row index within section)
    try:
        int(float(v0s))
        return "data"
    except (ValueError, TypeError):
        return "unknown"


# ══════════════════════════════════════════════════════════════════════
# PLANT → ASSET BUILDER
# ══════════════════════════════════════════════════════════════════════
def _is_coal(plant_name_ka: str) -> bool:
    """Detect the Tkibuli coal station case (name contains ნახშირ)."""
    return "ნახშირ" in (plant_name_ka or "")


def _build_hydro_reservoir_fields(pmax: float, subclass: str,
                                  override: dict | None = None) -> dict:
    """
    Reservoir-hydro defaults with per-plant override support (D2).

    Any numeric field supplied in `override` takes precedence over the
    pmax-scaled default.  Remaining fields follow the default rule.
    """
    d = _HYDRO_RESERVOIR_DEFAULTS
    override = override or {}

    # Default-rule values (pmax-scaled)
    init_default = d["init_x_pmax"] * pmax
    max_default  = d["max_x_pmax"]  * pmax
    end_default  = d["end_min_frac"] * init_default

    fields = {
        "reservoir_init":    override.get("reservoir_init",    init_default),
        "reservoir_min":     override.get("reservoir_min",     5.0),
        "reservoir_max":     override.get("reservoir_max",     max_default),
        "reservoir_end_min": override.get("reservoir_end_min", end_default),
        "efficiency":        override.get("efficiency",        d["efficiency"]),
        "spill_cost":        d["spill_cost"],
        "water_value":       override.get("water_value",       d["water_value"]),
        "cascade_upstream":  None,
        "travel_delay_h":    d["travel_delay_h"],
        "conversion_mode":   d["conversion_mode"],
        "_subclass":         subclass,
        "_override_applied": bool(override),
    }
    return fields


def _build_hydro_ror_fields(pmax: float) -> dict:
    d = _HYDRO_ROR_DEFAULTS
    return {
        "reservoir_init":    0.0,
        "reservoir_min":     0.0,
        "reservoir_max":     0.0,
        "reservoir_end_min": 0.0,
        "efficiency":        d["efficiency"],
        "spill_cost":        d["spill_cost"],
        "water_value":       d["water_value"],
        "cascade_upstream":  None,
        "travel_delay_h":    0,
        "conversion_mode":   2,
        "_subclass":         "run_of_river",
    }


def _row_to_asset(
    idx: int,
    row: pd.Series,
    section: str,
    diag: MappingDiagnostics,
    used_ids: dict[str, int],
    hydro_overrides: dict[str, dict] | None = None,
) -> dict | None:
    """Convert one data row into an asset dict.  Returns None on failure."""
    plant_name_ka = str(row.iloc[1] or "").strip()
    try:
        pmax = float(row.iloc[2])
    except (TypeError, ValueError):
        diag.warnings.append(f"row {idx}: pmax not parseable ({row.iloc[2]!r}) — skipped")
        diag.n_skipped += 1
        return None
    if pmax <= 0:
        diag.warnings.append(f"row {idx} '{plant_name_ka}': non-positive pmax {pmax} — skipped")
        diag.n_skipped += 1
        return None

    agg_raw     = row.iloc[3] if len(row) > 3 else None
    commiss_yr  = row.iloc[4] if len(row) > 4 else None
    # NaN-safe category read.  pd.isna handles empty cells; str(NaN) would
    # otherwise produce the literal "nan" and defeat the section fallback.
    cat_cell    = row.iloc[5] if len(row) > 5 else None
    category_xl = "" if (cat_cell is None or pd.isna(cat_cell)) else str(cat_cell).strip()

    # Category fallback: use the last section-header category if the cell
    # is empty (Mtkvarihesi row 39 is the real example).
    if not category_xl:
        category_xl = section or ""
        if category_xl:
            diag.warnings.append(
                f"row {idx} '{plant_name_ka}': empty category cell "
                f"→ inferred '{category_xl}' from section context")

    if category_xl not in _CATEGORY_TO_TYPE:
        diag.warnings.append(
            f"row {idx} '{plant_name_ka}': unknown category '{category_xl}' — skipped")
        diag.n_skipped += 1
        return None

    ptype, extras = _CATEGORY_TO_TYPE[category_xl]
    agg = parse_aggregate(agg_raw, pmax)

    # Stable, deterministic id.  Collisions are numbered _2, _3, …
    base_id = _normalize_id(plant_name_ka)
    if base_id in used_ids:
        used_ids[base_id] += 1
        new_id = f"{base_id}_{used_ids[base_id]}"
        diag.duplicates_renamed.append((base_id, new_id, plant_name_ka))
        asset_id = new_id
    else:
        used_ids[base_id] = 1
        asset_id = base_id

    # Base asset dict
    asset: dict = {
        "id":             asset_id,
        "name":           plant_name_ka,
        "type":           ptype,
        "_excel_category":category_xl,
        "_commiss_year":  (int(commiss_yr) if pd.notna(commiss_yr)
                           and str(commiss_yr).strip().isdigit() else None),
        "_n_units":       agg["n_units"],
        "_unit_size_mw":  round(agg["unit_size_mw"], 4),
        "pmax":           float(pmax),
    }

    # Type-specific finishing
    if ptype == "thermal":
        is_coal = _is_coal(plant_name_ka)
        defaults = _THERMAL_DEFAULTS_COAL if is_coal else _THERMAL_DEFAULTS_GAS
        asset.update({
            "committable":   True,
            "pmin":          round(0.4 * pmax, 2),       # 40% of pmax
            "fuel_type":     "coal" if is_coal else "gas",
            "heat_rate":     defaults["heat_rate"],
            "fuel_price":    defaults["fuel_price"],
            "vom":           defaults["vom"],
            "startup_cost":  defaults["startup_cost"],
            "no_load_cost":  defaults["no_load_cost"],
            "ramp_up":       defaults["ramp_up"],
            "ramp_down":     defaults["ramp_down"],
            "min_up":        defaults["min_up"],
            "min_down":      defaults["min_down"],
            "initial_status":0,
            "initial_power": 0.0,
            "for_rate":      defaults["for_rate"],
            "maint_windows": [],
        })
        if is_coal:
            diag.special_cases.append(
                f"{asset_id}: coal detected in name ('{plant_name_ka}') "
                f"→ fuel_type=coal, heat_rate={defaults['heat_rate']}")

    elif ptype == "hydro_reg":
        override = (hydro_overrides or {}).get(asset_id)
        hd = _build_hydro_reservoir_fields(
            pmax, extras.get("_subclass","reservoir"), override=override)
        if override:
            diag.special_cases.append(
                f"{asset_id}: reservoir override applied "
                f"({list(override.keys())})")
        pmin = _HYDRO_RESERVOIR_DEFAULTS["pmin_fraction"] * pmax
        ramp = _HYDRO_RESERVOIR_DEFAULTS["ramp_fraction"] * pmax
        # `inflow_driven` classification:
        #   Section I  (subclass='reservoir') → False  (handle separately later)
        #   Section II (subclass='seasonal')  → True   (treat as inflow-driven for now)
        # This is the single source of truth consumed by _build_hydro_zone_map
        # to decide whether an asset participates in zone mapping.
        subclass = extras.get("_subclass", "reservoir")
        inflow_driven = (subclass != "reservoir")
        asset.update({
            "committable":   True,
            "pmin":          round(pmin, 2),
            "ramp_up":       round(ramp, 2),
            "ramp_down":     round(ramp, 2),
            "min_up":        0,
            "min_down":      0,
            "startup_cost":  0.0,
            "no_load_cost":  0.0,
            "vom":           0.0,
            "initial_status":1,
            "initial_power": round(0.4 * pmax, 2),
            "for_rate":      0.05,
            "hydro":         hd,
            "inflow_profile":None,       # filled in by dataio when hydro_zone_map present
            "inflow_driven": inflow_driven,
        })

    elif ptype == "hydro_ror":
        hd = _build_hydro_ror_fields(pmax)
        ramp = _HYDRO_ROR_DEFAULTS["ramp_fraction"] * pmax
        asset.update({
            "committable":   False,
            "pmin":          0.0,
            "ramp_up":       round(ramp, 2),
            "ramp_down":     round(ramp, 2),
            "min_up":        0,
            "min_down":      0,
            "startup_cost":  0.0,
            "no_load_cost":  0.0,
            "vom":           0.0,
            "initial_status":1,
            "initial_power": round(0.3 * pmax, 2),
            "for_rate":      0.06,
            "hydro":         hd,
            "inflow_profile":None,
            "inflow_driven": True,       # all hydro_ror are inflow-driven by definition
        })

    elif ptype in ("wind", "solar"):
        asset.update({
            "committable":         False,
            "pmax_installed":      float(pmax),
            "vom":                 0.0,
            "curtailment_cost":    0.0,
            "availability_profile":None,
        })
        asset.pop("pmax", None)

    diag.n_assets_emitted += 1
    diag.section_counts[category_xl] = diag.section_counts.get(category_xl, 0) + 1
    diag.section_totals_mw[category_xl] = round(
        diag.section_totals_mw.get(category_xl, 0.0) + pmax, 3)
    return asset


# ══════════════════════════════════════════════════════════════════════
# HYDRO ZONE MAP / RE SITE MAP BUILDERS
# ══════════════════════════════════════════════════════════════════════
# ──────────────────────────────────────────────────────────────────────
# MANUAL HYDRO ZONE MAPPING — canonical plant → zone assignment
# (supplied and confirmed by the project owner)
# Takes precedence over keyword heuristics.  Keyword logic only runs for
# plants NOT present in this table.
#
# NOTE: some plants legitimately appear in multiple zones (e.g. Zones 3/5
# share 8 plants; Zones 5/6 share 9 plants) because their watershed
# contribution can be aggregated into different hydrological zone
# profiles.  The v1.1 schema stores a single zone per asset, so we
# currently resolve ambiguity by first-zone-wins with a diagnostic
# warning — the user can override via `build_assets_from_capacity_excel(
# manual_zone_overrides=...)` if a different resolution is desired.
# ──────────────────────────────────────────────────────────────────────
_MANUAL_HYDRO_ZONES: dict[str, list[str]] = {
    # Zone id → list of plant names AS WRITTEN BY THE PROJECT OWNER.
    # Names undergo NFC + whitespace/hyphen normalization at match time.
    "Z01_კოდორი": [
        "ოლდ ენერჯი ჰესი",
    ],
    "Z02_რიონი-ონი": [
        "ჩორდულა ჰესი", "მესტიაჭალა 2 ჰესი", "მესტიაჭალა 1 ჰესი",
        "ლახამი 1 ჰესი", "ლახამი 2 ჰესი", "ნაკრა ჰესი",
        "იფარი ჰესი", "ხელრა ჰესი", "ხეორი ჰესი",
        "ბერალი ჰესი", "ნაცეშარი ჰესი",
    ],
    "Z03_რიონი-ალპანა": [
        "სქურჰესი", "ჯონოული-1 ჰესი", "კასლეთი 2 ჰესი",
        "გუმათჰესი", "რიონჰესი", "რაჭაჰესი",
        "რიცეულაჰესი", "ხობი 2 ჰესი",
    ],
    "Z04_ტეხური": [
        "აბჰესი", "ვარციხეჰესი", "ჩხორჰესი", "სულორჰესი",
    ],
    "Z05_ყვირილა": [
        # duplicated with Zone 3 — first 8 plants (by design)
        "სქურჰესი", "ჯონოული-1 ჰესი", "კასლეთი 2 ჰესი",
        "გუმათჰესი", "რიონჰესი", "რაჭაჰესი",
        "რიცეულაჰესი", "ხობი 2 ჰესი",
        # plus 9 unique-to-Zone-5-or-6:
        "აჭიჰესი", "საშუალა ჰესი", "საშუალაჰესი 1", "საშუალაჰესი 2",
        "კინტრიში ჰესი", "ბახვი 3 ჰესი", "კინკიშაჰესი",
        "ნაბეღლავიჰესი", "ბჟუჟაჰესი",
    ],
    "Z06_სუფსა": [
        # duplicated with Zone 5 — these 9 plants
        "აჭიჰესი", "საშუალა ჰესი", "საშუალაჰესი 1", "საშუალაჰესი 2",
        "კინტრიში ჰესი", "ბახვი 3 ჰესი", "კინკიშაჰესი",
        "ნაბეღლავიჰესი", "ბჟუჟაჰესი",
    ],
    "Z07_აჭარისწყალი": [
        "სანალიაჰესი", "სხალთა ჰესი", "სკურდიდიჰესი",
        "აწჰესი", "მაჭახელაჰესი", "შევაბურიჰესი",
    ],
    "Z08_ლიახვი": [
        "შაქშაქეთი ჰესი", "ოკამიჰესი", "იგოეთჰესი", "ტირიფონჰესი",
    ],
    "Z09_ფარავანი": [
        "ფარავანი ჰესი", "დვირულა ჰესი", "ახალქალაქი-1", "ახალქალაქი 2",
        "ჩითახევჰესი", "ენერგეტიკი", "კახარეთჰესი", "ტოლოში ჰესი",
    ],
    "Z10_თუშეთის ალაზანი": [
        "დარიალი ჰესი", "ლარსიჰესი", "ყაზბეგიჰესი",
    ],
    "Z11_მთიულეთის არაგვი": [
        "მისაქციელჰესი", "არაგვიჰესი", "არაგვი 2 ჰესი",
        "როშკა-3 ჰესი", "როშკა-2 ჰესი", "როშკა-1 ჰესი",
        "კორშა ჰესი", "როშკა ჰესი", "კორშა-1 ჰესი",
        "ფშაველი ჰესი", "საგურამო ჰესი", "ბოდორნა ჰესი", "ფშაველაჰესი",
    ],
    "Z12_ალაზანი-ბირკიანი": [
        "ინწობაჰესი", "ხადორჰესი", "შილდაჰესი-1", "შილდაჰესი",
        "ახმეტაჰესი", "ხადორი-2 ჰესი", "ლოპოტა ჰესი",
    ],
    "Z13_მაშავერა": [
        "დაშბაშჰესი", "ჭაპალა ჰესი", "ხრამი ჰესი", "თეთრიხევჰესი",
        "დებედა ჰესი", "ორო ჰესი", "ორთაჭალაჰესი", "ზაჰესი",
        "საცხენჰესი", "მარტყოფჰესი", "სიონჰესი", "კაზრეთიჰესი",
        "მაშავერაჰესი", "დმანისი ჰესი", "ალგეთიჰესი", "ძამა ჰესი",
        "დაღეთი ჰესი",
    ],
    "Z14_ალაზანი-შაქრიანი": [
        "ავანი ჰესი", "კაბალჰესი", "ალაზანჰესი-2", "ალაზანჰესი",
        "ჩალაჰესი",
    ],
    "Z15_ჭოროხი": [
        "ხელვაჩაური-1 ჰესი", "კირნათი ჰესი",
    ],
    # Z16_სამგორი intentionally empty — no plants assigned in the manual list.
}


def _normalize_plant_name(name: str) -> str:
    """
    Produce a stable match-key from a Georgian plant name.

    This is the 'basic' canonical form: NFC → lowercase → keep only
    Georgian letters (0x10A0–0x10FF) and digits → concatenate.
    Use :func:`_collect_manual_name_aliases` for full matching which
    additionally handles parentheticals, trailing digits, ჰესი-suffix
    position, and Georgian nominative "-ი" variants.
    """
    if not name:
        return ""
    s = unicodedata.normalize("NFC", name).strip().lower()
    out = []
    for ch in s:
        cp = ord(ch)
        if (0x10A0 <= cp <= 0x10FF) or ch.isdigit():
            out.append(ch)
    return "".join(out)


# Georgian vowels — stripped from word-stems to fold nominative "-ი",
# dative "-ა", etc., so that "ხრამი" ↔ "ხრამ" and "კინტრიშა" ↔ "კინტრიშ".
_GEORGIAN_VOWELS = "აეიოუ"
_HESI_SUFFIX = "ჰესი"
_PARENS_RE = __import__("re").compile(r"\(([^)]+)\)")


def _collect_manual_name_aliases(name: str) -> set[str]:
    """
    Produce all match keys that should hit the same plant.

    Robust against the real variations observed in the data:

    1. **Parentheticals** — "ოლდ ენერჯიჰესი (სოხუმჰესი)" is treated as
       two candidate names: "ოლდ ენერჯიჰესი" and "სოხუმჰესი".  Either
       form may match a manual entry.

    2. **Trailing plant-numbering** — "ვარციხეჰესი 1", "ვარციხეჰესი 2",
       "ვარციხეჰესი 3", "ვარციხეჰესი 4" all collapse to the same zone
       as manual "ვარციხეჰესი" because we also emit a digit-free alias.

    3. **ჰესი-suffix position ambiguity** — "ხობიჰესი 2" vs
       "ხობი 2 ჰესი" normalize differently character-by-character, but
       after digit-removal they both reduce to "ხობიჰესი", which then
       strips to "ხობი".

    4. **Nominative "-ი" / case-ending vowels** — Georgian nouns
       routinely carry a final vowel (nominative "-ი", etc.) that is
       absent in stem-compounded forms.  We strip trailing vowels from
       the post-ჰესი stem, so "ხრამი" and "ხრამ" both produce the
       alias "ხრამ".

    Examples that normalize to at least one shared key:
      'ხრამჰესი 1'     and 'ხრამი ჰესი'     → share "ხრამ"
      'ფარავანჰესი'    and 'ფარავანი ჰესი'   → share "ფარავან"
      'ხობიჰესი 2'     and 'ხობი 2 ჰესი'     → share "ხობი"
      'მისაქციელი'     and 'მისაქციელჰესი'    → share "მისაქციელ"
      'ვარციხეჰესი 3'  and 'ვარციხეჰესი'      → share "ვარციხე"
    """
    if not name:
        return set()
    s = unicodedata.normalize("NFC", name).strip().lower()

    # Separate parenthetical content.  The main part is the text with
    # parens replaced by a space; the content inside parens is treated
    # as an alternate-name candidate.
    paren_parts = _PARENS_RE.findall(s)
    main_part   = _PARENS_RE.sub(" ", s)

    all_candidates = [main_part] + paren_parts
    keys: set[str] = set()

    for piece in all_candidates:
        # Clean: keep only Georgian letters + digits
        cleaned = "".join(
            ch for ch in piece
            if (0x10A0 <= ord(ch) <= 0x10FF) or ch.isdigit()
        )
        if not cleaned:
            continue

        # Two parallel pipelines: (a) digits preserved, (b) all digits stripped.
        variants: set[str] = {cleaned}
        no_digits = "".join(ch for ch in cleaned if not ch.isdigit())
        if no_digits and no_digits != cleaned:
            variants.add(no_digits)

        # For each variant, also emit ჰესი-stripped and vowel-stripped forms.
        derived: set[str] = set()
        for v in variants:
            derived.add(v)
            nh = v[:-len(_HESI_SUFFIX)] if v.endswith(_HESI_SUFFIX) else v
            if nh and nh != v:
                derived.add(nh)
            # Strip trailing vowels from the post-ჰესი stem
            stem = nh
            while stem and stem[-1] in _GEORGIAN_VOWELS:
                stem = stem[:-1]
            if stem and stem != nh:
                derived.add(stem)

        # Filter: keep only reasonably-long keys to avoid false matches
        # (e.g. a 1–2 character stem could hit anything).
        for k in derived:
            if len(k) >= 3:
                keys.add(k)

    return keys


def _build_manual_zone_matches(
    assets: list[dict],
    manual_zones: dict[str, list[str]] | None = None,
) -> tuple[dict[str, list[str]], dict]:
    """
    Match each hydro asset in the fleet against the manual zone mapping.

    Returns:
      (match_dict, report)
        match_dict: {asset_id: [list of zones it could belong to]}
                    (multi-element when plant is in >1 zone — ambiguous)
        report: structured coverage report
                {
                  'total_hydro':        int,
                  'matched':            int,
                  'unmatched':          [asset_id, ...],
                  'ambiguous':          {asset_id: [zones]},
                  'manual_plants_total':           int,
                  'manual_plants_matched':         int,
                  'manual_plants_unmatched_in_fleet': [plant_name, ...],
                }
    """
    manual = manual_zones if manual_zones is not None else _MANUAL_HYDRO_ZONES

    # Build two parallel structures:
    #   key_to_zones[match_key]  = [zone_id, ...]       (unique zones)
    #   key_to_plants[match_key] = {plant_name, ...}    (original manual names)
    key_to_zones: dict[str, list[str]] = {}
    key_to_plants: dict[str, set[str]] = {}
    all_manual_plants: set[str] = set()          # canonical plant names seen in manual list

    for zone_id, plants in manual.items():
        for plant_name in plants:
            all_manual_plants.add(plant_name)
            for k in _collect_manual_name_aliases(plant_name):
                zs = key_to_zones.setdefault(k, [])
                if zone_id not in zs:
                    zs.append(zone_id)
                key_to_plants.setdefault(k, set()).add(plant_name)

    # Match fleet hydro assets
    hydro_assets = [a for a in assets if a["type"] in ("hydro_reg", "hydro_ror")]
    match_dict: dict[str, list[str]] = {}
    matched_manual_plants: set[str] = set()

    for a in hydro_assets:
        fleet_keys = _collect_manual_name_aliases(a["name"])
        # Collect ALL zones hit by ANY of the fleet's alias keys.
        hit_zones: list[str] = []
        hit_plants: set[str] = set()
        for fk in fleet_keys:
            if fk in key_to_zones:
                for z in key_to_zones[fk]:
                    if z not in hit_zones:
                        hit_zones.append(z)
                hit_plants.update(key_to_plants.get(fk, ()))
        if hit_zones:
            match_dict[a["id"]] = hit_zones
            matched_manual_plants.update(hit_plants)

    unmatched_ids = [a["id"] for a in hydro_assets if a["id"] not in match_dict]
    ambiguous = {aid: zones for aid, zones in match_dict.items() if len(zones) > 1}

    manual_unmatched = sorted(all_manual_plants - matched_manual_plants)

    report = {
        "total_hydro":                      len(hydro_assets),
        "matched":                          len(match_dict),
        "unmatched":                        unmatched_ids,
        "ambiguous":                        ambiguous,
        "manual_plants_total":              len(all_manual_plants),
        "manual_plants_matched":            len(matched_manual_plants),
        "manual_plants_unmatched_in_fleet": manual_unmatched,
    }
    return match_dict, report


def _build_hydro_zone_map(
    assets: list[dict],
    diag: MappingDiagnostics,
    manual_zones: dict[str, list[str]] | None = None,
    manual_zone_overrides: dict[str, str] | None = None,
) -> dict:
    """
    Hydro zone map builder — manual-first, keyword-fallback.

    Resolution order for each asset:
      1. `manual_zone_overrides[asset_id]` (hard override, top priority)
      2. Manual zone list match (primary source of truth)
      3. Keyword heuristic (fallback only if steps 1 & 2 yield nothing)

    Ambiguous assets (matched to multiple manual zones) default to the
    FIRST zone listed, with a warning in diag so the user can refine
    via `manual_zone_overrides`.
    """
    manual_zone_overrides = manual_zone_overrides or {}

    # Manual-match first (primary source of truth)
    manual_matches, coverage = _build_manual_zone_matches(assets, manual_zones)
    diag.special_cases.append(
        f"Manual hydro zone mapping: matched {coverage['matched']} / "
        f"{coverage['total_hydro']} hydro assets  "
        f"(ambiguous={len(coverage['ambiguous'])}  "
        f"manual-plants-unmatched-in-fleet={len(coverage['manual_plants_unmatched_in_fleet'])}"
        f"/{coverage['manual_plants_total']})"
    )

    # Keyword fallback table — only used for plants NOT in the manual list
    # AND that ARE inflow-driven.  Section I reservoirs (Enguri, Vardnili,
    # Zhinvali, Khrami 1/2, Shaori, Dzevrula) are intentionally EXCLUDED
    # from this table because they are handled separately as storage
    # reservoirs, not as inflow-driven units.
    _KW_TO_ZONE = {
        "ლაჯანურ":   "Z03_რიონი-ალპანა",    # Lajanuri (Section II) — not in manual list
        "შუახევ":    "Z07_აჭარისწყალი",     # Shuakhevi (Section II) — not in manual list
        "მტკვარ":    "Z02_რიონი-ონი",       # Mtkvarihesi (Section II) — crude fallback
    }

    # ── First pass: resolve each plant's zone ───────────────────────
    # Output records collected here with placeholder share=None.
    # Pmax-weighted shares are applied in the second pass below
    # (Layer 1 of hydro inflow allocation correction).
    out: dict[str, dict] = {}
    excluded_by_design: list[str] = []          # inflow_driven=False assets (Section I)
    pmax_by_id: dict[str, float] = {}           # cached for second pass
    for a in assets:
        if a["type"] not in ("hydro_reg", "hydro_ror"):
            continue
        aid  = a["id"]
        name = a["name"]
        pmax_by_id[aid] = float(a.get("pmax", 0) or 0)

        # ──  Inflow-driven gate ─────────────────────────────────────
        # Only inflow-driven hydro participates in zone mapping.
        # Section I reservoirs (inflow_driven=False) are tracked
        # separately as "excluded by design" and kept out of hzm.
        # Their manual-list match (if any) is still reported so you
        # can see the classification didn't accidentally hide a plant.
        if not a.get("inflow_driven", True):
            excluded_by_design.append(aid)
            if aid in manual_matches:
                diag.special_cases.append(
                    f"{aid} ('{name}') — reservoir (inflow_driven=False); "
                    f"skipped from hydro_zone_map (matched manual zones "
                    f"{manual_matches[aid]} would have applied if enabled)"
                )
            else:
                diag.special_cases.append(
                    f"{aid} ('{name}') — reservoir (inflow_driven=False); "
                    f"skipped from hydro_zone_map (no manual match either)"
                )
            continue

        chosen_zone: str | None = None
        source = ""

        # (1) Hard override
        if aid in manual_zone_overrides:
            chosen_zone = manual_zone_overrides[aid]
            source = "override"

        # (2) Manual list match
        elif aid in manual_matches:
            zones = manual_matches[aid]
            chosen_zone = zones[0]                      # first-zone-wins
            if len(zones) > 1:
                source = f"manual-ambiguous(first of {len(zones)})"
                diag.warnings.append(
                    f"hydro_zone_map: '{aid}' ('{name}') matched by manual list to "
                    f"{len(zones)} zones {zones} — using first ({chosen_zone}); "
                    f"to override, pass manual_zone_overrides={{'{aid}': 'Z??_...'}}"
                )
            else:
                source = "manual"

        # (3) Keyword fallback
        else:
            for kw, zone in _KW_TO_ZONE.items():
                if kw in name and zone in HYDRO_ZONES:
                    chosen_zone = zone
                    source = f"keyword({kw})"
                    break

        if chosen_zone and chosen_zone in HYDRO_ZONES:
            # Share will be filled in by the second pass.
            out[aid] = {"zone": chosen_zone, "share": None, "scaling_mw": None,
                        "_source": source}
        else:
            diag.warnings.append(
                f"hydro_zone_map: '{aid}' ('{name}') — no zone match "
                f"(not in manual list, no keyword hit); "
                f"inflow_profile will remain null")

    # ── Second pass (Layer 1): pmax-weighted shares per zone ────────
    # share_i = pmax_i / Σ(pmax for all plants assigned to the same zone)
    # Guarantees Σ(share) = 1.0 per zone, eliminating water double-counting.
    # Edge case: all-zero pmax in a zone → fall back to equal-split so the
    # plants don't silently disappear from the model.
    zone_total_pmax: dict[str, float] = {}
    for aid, entry in out.items():
        zone = entry["zone"]
        zone_total_pmax[zone] = zone_total_pmax.get(zone, 0.0) + pmax_by_id.get(aid, 0.0)

    zone_member_count: dict[str, int] = {}
    for entry in out.values():
        zone_member_count[entry["zone"]] = zone_member_count.get(entry["zone"], 0) + 1

    zero_pmax_zones: list[str] = []
    for aid, entry in out.items():
        zone = entry["zone"]
        zsum = zone_total_pmax.get(zone, 0.0)
        if zsum > 0:
            entry["share"] = round(pmax_by_id.get(aid, 0.0) / zsum, 8)
        else:
            # Degenerate: zone has assets but all pmax==0; equal-split fallback.
            n = zone_member_count.get(zone, 1)
            entry["share"] = round(1.0 / n, 8)
            if zone not in zero_pmax_zones:
                zero_pmax_zones.append(zone)

    # Verify Σ(share) ≈ 1.0 per zone, log any deviation > 1e-6
    share_sums: dict[str, float] = {}
    for entry in out.values():
        share_sums[entry["zone"]] = share_sums.get(entry["zone"], 0.0) + entry["share"]
    for zone, s in share_sums.items():
        if abs(s - 1.0) > 1e-6:
            diag.warnings.append(
                f"hydro_zone_map: zone '{zone}' Σ(share) = {s:.10f}, "
                f"expected 1.0 (Δ = {s - 1.0:+.2e})"
            )

    if zero_pmax_zones:
        diag.warnings.append(
            f"hydro_zone_map: {len(zero_pmax_zones)} zones had all-zero pmax assets "
            f"and used equal-split fallback: {zero_pmax_zones}"
        )

    # Record allocation diagnostics
    diag.special_cases.append(
        f"Layer 1 (pmax-weighted share): applied to {len(out)} mapped assets across "
        f"{len(zone_total_pmax)} zones; Σ(share) per zone validated to 1.0 ± 1e-6"
    )

    # Extend the coverage report with the classification split.
    # `matched` in the raw report still counts ALL name-matches in the
    # manual list (regardless of inflow_driven).  We add two new fields:
    #   - excluded_by_design: Section I reservoirs skipped from hzm
    #   - inflow_driven_total / mapped / unmatched: counts restricted
    #     to the inflow-driven subset (the "real" hydro_zone_map domain)
    inflow_driven_assets = [
        a for a in assets
        if a["type"] in ("hydro_reg", "hydro_ror") and a.get("inflow_driven", True)
    ]
    inflow_driven_ids = {a["id"] for a in inflow_driven_assets}
    coverage["excluded_by_design"] = excluded_by_design
    coverage["inflow_driven_total"]     = len(inflow_driven_assets)
    coverage["inflow_driven_mapped"]    = sum(1 for aid in out if aid in inflow_driven_ids)
    coverage["inflow_driven_unmapped"]  = [aid for aid in inflow_driven_ids if aid not in out]

    # Carry the coverage report into diagnostics for downstream inspection
    diag.manual_zone_coverage = coverage                 # type: ignore[attr-defined]
    return out


def _build_re_site_map(assets: list[dict], diag: MappingDiagnostics) -> dict:
    out: dict[str, dict] = {}
    for a in assets:
        if a["type"] not in ("wind", "solar"):
            continue
        site = _match_re_site(a["name"])
        if site:
            out[a["id"]] = {"source": a["type"], "site": site}
            diag.matched_re_sites.append((a["id"], a["name"], site))
        else:
            diag.unmatched_re_sites.append(a["id"])
            diag.warnings.append(
                f"re_site_map: '{a['id']}' ('{a['name']}') — no site match; "
                f"availability_profile will remain null")
    return out


# ══════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY
# ══════════════════════════════════════════════════════════════════════
def build_assets_from_capacity_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    hydro_overrides: dict[str, dict] | None = None,
    hydro_overrides_csv: str | Path | None = None,
    manual_zones: dict[str, list[str]] | None = None,
    manual_zone_overrides: dict[str, str] | None = None,
) -> tuple[list[dict], dict, dict, MappingDiagnostics]:
    """
    Transform the GSE installed-capacity workbook into a PowerSim asset
    list.  Returns (assets, hydro_zone_map, re_site_map, diagnostics).

    The returned shapes are directly compatible with
    powersim_dataio.build_input_from_project(..., assets=..., hydro_zone_map=...,
    re_site_map=...).

    D2 — reservoir overrides:
        If `hydro_overrides_csv` is given, it is read with
        :func:`load_hydro_overrides` and merged with `hydro_overrides`.
        Per-asset entries replace default (pmax-scaled) reservoir values
        field-by-field.  Any override applied is logged in diagnostics.

    Manual hydro zone mapping:
        `manual_zones` supplies a {zone_id: [plant_name, ...]} dict that
        overrides the built-in `_MANUAL_HYDRO_ZONES` table.  Pass None
        to use the built-in (recommended — it already encodes the project
        owner's confirmed assignments).

        `manual_zone_overrides` supplies a hard {asset_id: zone_id} dict
        for plants that the manual list matches ambiguously (to multiple
        zones) — overrides first-zone-wins.
    """
    diag = MappingDiagnostics()

    # Resolve reservoir overrides: explicit dict (if any) + CSV (if any)
    overrides: dict[str, dict] = {}
    if hydro_overrides_csv:
        try:
            overrides.update(load_hydro_overrides(hydro_overrides_csv))
        except Exception as e:
            diag.warnings.append(f"override CSV read failed: {e}")
    if hydro_overrides:
        for aid, entry in hydro_overrides.items():
            overrides.setdefault(aid, {}).update(entry)

    # Read with header=None so we can walk raw rows and classify structurally.
    df = pd.read_excel(path, sheet_name=sheet_name, header=None)
    diag.n_rows_seen = len(df)

    assets: list[dict] = []
    used_ids: dict[str, int] = {}
    current_section_category: str = ""

    for idx, row in df.iterrows():
        kind = _classify_row(row)

        if kind == "data":
            cat_cell = row.iloc[5] if len(row) > 5 else None
            if cat_cell is not None and not pd.isna(cat_cell):
                current_section_category = str(cat_cell).strip()
            a = _row_to_asset(idx, row, current_section_category,
                              diag, used_ids, hydro_overrides=overrides)
            if a is not None:
                assets.append(a)
            continue
        continue

    # Flag reservoir-override asset_ids that were never matched
    matched_ids = {a["id"] for a in assets}
    for aid in overrides:
        if aid not in matched_ids:
            diag.warnings.append(
                f"reservoir override asset_id '{aid}' not present in fleet — ignored")

    # Flag manual_zone_override asset_ids that don't match the fleet either
    for aid in (manual_zone_overrides or {}):
        if aid not in matched_ids:
            diag.warnings.append(
                f"manual_zone_overrides asset_id '{aid}' not present in fleet — ignored")

    # Build companion mappings (manual-first for hydro zones)
    hzm = _build_hydro_zone_map(assets, diag,
                                manual_zones=manual_zones,
                                manual_zone_overrides=manual_zone_overrides)
    rsm = _build_re_site_map(assets, diag)

    # ── Stage 2: Section-I reservoir cascade + strategic end-level ───
    # All Section-I reservoirs (inflow_driven=False, _subclass=='reservoir')
    # receive strategic end-of-horizon defaults so the solver no longer
    # myopically depletes them across a weekly window.
    #
    # Specific cascade topology (river physics):
    #   • Enguri  → Vardnili  (Enguri river, ~2h travel, 3% transit losses)
    #   • Khrami 1 → Khrami 2 (Khrami river, ~1h travel, 5% transit losses)
    _CASCADE_LINKS = {
        "vardnilhesi": {
            "cascade_upstream":       "engurhesi",
            "cascade_travel_delay_h": 2,
            "cascade_gain":           0.97,
        },
        "khramhesi_2": {
            "cascade_upstream":       "khramhesi_1",
            "cascade_travel_delay_h": 1,
            "cascade_gain":           0.95,
        },
    }
    _STRATEGIC_DEFAULTS = {
        "target_end_level_frac": 0.85,
        "end_level_penalty":     20.0,
    }
    _section1_count = 0
    _cascade_count  = 0
    asset_ids = {a["id"] for a in assets}
    for a in assets:
        if a["type"] != "hydro_reg":
            continue
        h = a.get("hydro") or {}
        if h.get("_subclass") != "reservoir":
            continue
        # Strategic defaults on every Section-I reservoir
        h["target_end_level_frac"] = _STRATEGIC_DEFAULTS["target_end_level_frac"]
        h["end_level_penalty"]     = _STRATEGIC_DEFAULTS["end_level_penalty"]
        _section1_count += 1
        # Cascade overlay if applicable
        link = _CASCADE_LINKS.get(a["id"])
        if link and link["cascade_upstream"] in asset_ids:
            h["cascade_upstream"]       = link["cascade_upstream"]
            h["cascade_travel_delay_h"] = link["cascade_travel_delay_h"]
            h["cascade_gain"]           = link["cascade_gain"]
            _cascade_count += 1
            diag.special_cases.append(
                f"Stage 2 cascade: {a['id']} ← {link['cascade_upstream']} "
                f"(delay={link['cascade_travel_delay_h']}h, "
                f"gain={link['cascade_gain']})"
            )
        a["hydro"] = h
    diag.special_cases.append(
        f"Stage 2: applied strategic end-level defaults to {_section1_count} "
        f"Section-I reservoirs; {_cascade_count} cascade links wired"
    )

    # Calibration flags — structured, not prose
    diag.needs_calibration.extend([
        "thermal.heat_rate       — placeholder values only (7.2 for gas, 10.5 for coal)",
        "thermal.fuel_price      — placeholder 7.0 $/MMBtu gas, 4.0 coal",
        "thermal.startup_cost    — placeholder 10000 gas, 40000 coal",
        "thermal.no_load_cost    — placeholder 500 gas, 1200 coal",
        "thermal.ramp_up/down    — placeholder 60 MW/h gas, 20 coal",
        "thermal.min_up/min_down — placeholder 3/2 gas, 6/6 coal",
        "thermal.for_rate        — placeholder 0.08 gas, 0.12 coal",
        "thermal.pmin            — coarse 40% of pmax rule",
        "hydro_reg.reservoir_*   — coarse 2×pmax / 4×pmax rule (D2 shrunk from 10×/20×); "
        "prefer per-plant override CSV",
        "hydro_reg.efficiency    — placeholder 350 MWh/Mm³",
        "hydro_reg.water_value   — placeholder 15 $/MWh (reservoir) / 10 (ror)",
        "hydro cascade topology  — all cascade_upstream set to null; travel_delay_h=0",
        "hydro_ror vs hydro_reg  — 'Hydro - Small' assumed RoR; some may actually regulate",
        "wind/solar vom, curtailment_cost — all zero by default",
    ])

    return assets, hzm, rsm, diag


# ══════════════════════════════════════════════════════════════════════
# CLI PREVIEW
# ══════════════════════════════════════════════════════════════════════
def _format_asset_preview(a: dict) -> str:
    """Compact one-line-per-field preview of an asset."""
    cat = a.get("_excel_category","?")
    pm  = a.get("pmax", a.get("pmax_installed","-"))
    n   = a.get("_n_units","?")
    us  = a.get("_unit_size_mw","?")
    return (f"  {a['id']:<30} | {a['type']:<10} | {pm:>8} MW | "
            f"{cat:<17} | {n} × {us} MW | \"{a['name']}\"")


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="PowerSim v4.0 asset mapper — installed-capacity xlsx → asset list")
    ap.add_argument("--excel", required=True,
                    help="Path to the installed-capacity workbook.")
    ap.add_argument("--sheet", default="0",
                    help="Sheet name or 0-based index (default: 0).")
    ap.add_argument("--preview", type=int, default=10,
                    help="Number of assets to preview (default: 10; 0 = all).")
    ap.add_argument("--hydro-overrides", default=None,
                    help="Optional reservoir-override CSV "
                         "(columns: asset_id, reservoir_init, reservoir_min, "
                         "reservoir_max, reservoir_end_min, efficiency, water_value).")
    ap.add_argument("--out", default=None,
                    help="Optional: write {assets, hydro_zone_map, re_site_map, diagnostics} to JSON.")
    args = ap.parse_args(argv)

    sheet: str | int
    try:
        sheet = int(args.sheet)
    except ValueError:
        sheet = args.sheet

    assets, hzm, rsm, diag = build_assets_from_capacity_excel(
        args.excel, sheet_name=sheet,
        hydro_overrides_csv=args.hydro_overrides)

    print("─" * 72)
    print(f"PowerSim asset mapper — {MAPPER_VERSION}")
    print(f"  source:  {args.excel}")
    print(f"  sheet:   {sheet}")
    print(f"  rows seen: {diag.n_rows_seen}   emitted: {diag.n_assets_emitted}"
          f"   skipped: {diag.n_skipped}")
    print("─" * 72)

    print("\nSection counts and MW totals:")
    for cat, n in diag.section_counts.items():
        mw = diag.section_totals_mw.get(cat, 0.0)
        print(f"  {cat:<22} {n:>3} plants   Σ = {mw:>9.2f} MW")
    print(f"  {'TOTAL':<22} {diag.n_assets_emitted:>3} plants   "
          f"Σ = {sum(diag.section_totals_mw.values()):>9.2f} MW")

    print(f"\nPreview — first {args.preview or 'all'} assets:")
    for a in (assets if args.preview == 0 else assets[:args.preview]):
        print(_format_asset_preview(a))

    print(f"\nhydro_zone_map entries seeded: {len(hzm)} / "
          f"{sum(1 for a in assets if a['type'].startswith('hydro'))} hydro assets")
    if hzm:
        for aid, entry in list(hzm.items())[:5]:
            print(f"  {aid:<30} → zone={entry['zone']}  share={entry['share']}")
        if len(hzm) > 5:
            print(f"  … ({len(hzm)-5} more)")

    print(f"\nre_site_map entries seeded: {len(rsm)}")
    for aid, entry in rsm.items():
        print(f"  {aid:<30} → {entry['source']:<5} @ {entry['site']}")

    if diag.duplicates_renamed:
        print(f"\nDuplicate ids auto-renamed: {len(diag.duplicates_renamed)}")
        for base, new, nm in diag.duplicates_renamed[:5]:
            print(f"  '{nm}': {base} → {new}")

    if diag.unmatched_re_sites:
        print(f"\nRE assets without site match: {len(diag.unmatched_re_sites)}")
        for aid in diag.unmatched_re_sites:
            print(f"  {aid}")

    if diag.special_cases:
        print("\nSpecial cases:")
        for s in diag.special_cases:
            print(f"  • {s}")

    print(f"\nFields that need calibration before Stage 2:")
    for f in diag.needs_calibration:
        print(f"  • {f}")

    if diag.warnings:
        print(f"\nWarnings ({len(diag.warnings)}):")
        for w in diag.warnings[:15]:
            print(f"  ⚠ {w}")
        if len(diag.warnings) > 15:
            print(f"  … and {len(diag.warnings)-15} more")

    if args.out:
        Path(args.out).write_text(
            json.dumps({
                "assets":            assets,
                "hydro_zone_map":    hzm,
                "re_site_map":       rsm,
                "diagnostics":       diag.to_dict(),
            }, ensure_ascii=False, indent=2),
            encoding="utf-8")
        print(f"\n💾 wrote {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
