"""
PowerSim v4.0 — Project Data Loader
====================================
Sole responsibility: assemble a schema-v1.1-compliant input dict from the
raw GSE project files (hydro CSVs, renewables CSVs, CharYear/PLEXOS xlsx).

No Pyomo. No optimization. No HTML coupling.

Input files this module knows about:
  - 2026_A_historical_mean.csv        (hydro, A_mean)
  - 2026_B_montecarlo_P10/P50/P90.csv (hydro, MC scenarios)
  - Solar_{Site}_2026_{Scenario}.csv  (;-sep, comma-decimal, BOM)
  - Wind_{Site}_2026_{Scenario}.csv
  - GSE_CharYear_Normalized_1.xlsx    (demand shape, Σ=1)
  - GSE_PLEXOS_sc2_2026_1.xlsx        (absolute MW, optional Stage 2 path)

Hydro inflow unit handling:
  Values are passed through as-is. The physical unit is declared in
  profile_bundle.hydro_inflow_unit (default "raw"). No conversion is
  performed here. Once the unit is verified, the solver will honor the
  declared unit — this module needs no code changes to flip it.

Usage:
  # Library:
  from powersim_dataio import build_input_from_project
  inp = build_input_from_project(project_dir, assets=..., ...)

  # CLI:
  python solver/powersim_dataio.py \\
      --project-dir /path/to/project_data \\
      --config     tests/stage1_smoke_fleet.json \\
      --out        powersim_input.json
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

# Import the schema — support layouts:
#   powersim_v4/{schema,solver}/*.py  (sibling package dirs)
#   flat directory (everything next to each other)
try:
    from schema.powersim_schema import (
        DEMAND_MODES, HOURS_PER_YEAR, HYDRO_INFLOW_UNITS, HYDRO_ZONES,
        MODEL_VERSION, RE_SITES, SCENARIOS, SCHEMA_VERSION, TIMEZONE,
        generate_time_index, validate_input,
    )
except ImportError:
    # Fallback: same directory as this file, or sys.path already set up.
    _here = Path(__file__).resolve().parent
    sys.path.insert(0, str(_here.parent / "schema"))
    sys.path.insert(0, str(_here))
    from powersim_schema import (                              # type: ignore
        DEMAND_MODES, HOURS_PER_YEAR, HYDRO_INFLOW_UNITS, HYDRO_ZONES,
        MODEL_VERSION, RE_SITES, SCENARIOS, SCHEMA_VERSION, TIMEZONE,
        generate_time_index, validate_input,
    )


LOADER_VERSION = "powersim_dataio 1.0.1"


# ══════════════════════════════════════════════════════════════════════
# ERRORS
# ══════════════════════════════════════════════════════════════════════
class DataIOError(Exception):
    """Base class for dataio errors."""


class FileFormatError(DataIOError):
    """CSV/XLSX could not be parsed even with robust heuristics."""


class DatetimeMismatchError(DataIOError):
    """Profile DateTime column does not cover a full 8760h non-leap year."""


class ProfileLengthError(DataIOError):
    """Profile row count != HOURS_PER_YEAR."""


# ══════════════════════════════════════════════════════════════════════
# ROBUST CSV READER
# ══════════════════════════════════════════════════════════════════════
_DELIMITER_CANDIDATES = (";", ",", "\t")


def _strip_bom(text: str) -> str:
    """Remove a single leading UTF-8 BOM if present."""
    return text.lstrip("\ufeff")


def _sniff_delimiter(text: str) -> str:
    """
    Choose the most plausible delimiter by counting occurrences in the first
    five non-empty lines.  Ties broken toward ';' (the locale default for
    the project's renewable CSVs).
    """
    lines = [ln for ln in text.splitlines() if ln.strip()][:5]
    if not lines:
        raise FileFormatError("empty file")
    counts = {d: sum(ln.count(d) for ln in lines) for d in _DELIMITER_CANDIDATES}
    best_delim, best_count = max(
        counts.items(),
        key=lambda kv: (kv[1], 1 if kv[0] == ";" else 0),
    )
    if best_count == 0:
        raise FileFormatError(
            f"no known delimiter found; tried {_DELIMITER_CANDIDATES}")
    return best_delim


def _sniff_decimal(df_probe: pd.DataFrame) -> str:
    """
    After a first parse with decimal='.', scan string-typed columns for the
    pattern r'^-?\\d+,\\d+$' — the signature of Georgian-locale decimals.
    Returns ',' if comma-decimal is detected, else '.'.

    Accepts both ``object`` and pandas 2.x ``StringDtype`` — ``dtype=str`` in
    recent pandas returns the new string dtype, not object.
    """
    for col in df_probe.columns:
        s = df_probe[col]
        if not (pd.api.types.is_object_dtype(s)
                or pd.api.types.is_string_dtype(s)):
            continue
        sample = s.dropna().astype(str).head(50)
        if sample.empty:
            continue
        hits = sample.str.match(r"^-?\d+,\d+$").sum()
        if hits >= max(3, len(sample) // 2):
            return ","
    return "."


def read_csv_robust(path: str | Path) -> pd.DataFrame:
    """
    Read a CSV regardless of delimiter (;/,/tab), decimal (./,) or BOM.

    The first column is expected to carry the DateTime label; its name is
    preserved as-is.  No temporal validation here — see
    :func:`validate_datetime_index`.
    """
    path = Path(path)
    raw = path.read_bytes().decode("utf-8", errors="replace")
    text = _strip_bom(raw)

    delim = _sniff_delimiter(text)
    df_probe = pd.read_csv(io.StringIO(text), sep=delim, decimal=".",
                           dtype=str, engine="python")
    dec = _sniff_decimal(df_probe)
    df = pd.read_csv(io.StringIO(text), sep=delim, decimal=dec, engine="python")

    # NFC-normalize column names so equality checks against HYDRO_ZONES
    # are deterministic across different Unicode compositions.
    df.columns = [unicodedata.normalize("NFC", str(c)).strip()
                  for c in df.columns]
    return df


# ══════════════════════════════════════════════════════════════════════
# DATETIME VALIDATION
# ══════════════════════════════════════════════════════════════════════
_DATETIME_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$")


def validate_datetime_index(
    df: pd.DataFrame,
    year: int = 2026,
    datetime_col: str | None = None,
) -> tuple[bool, list[str]]:
    """
    Verify the DataFrame's DateTime column covers exactly
    ``year-01-01 00:00`` → ``year-12-31 23:00`` hourly, with 8760 rows,
    no gaps, no duplicates, monotonically increasing.

    If the data year differs from `year` but is itself a valid non-leap
    8760 hourly sequence, it is accepted — hour-of-year mapping handles
    the re-stamping later (CharYear workbook is a good example).

    Returns (ok, errors).  Does not raise.
    """
    errors: list[str] = []

    if datetime_col is None:
        # Prefer a column that *looks* like a DateTime column.
        for c in df.columns:
            if c.strip().lower().startswith(("datetime", "date_time", "date/time")):
                datetime_col = c
                break
        if datetime_col is None:
            datetime_col = df.columns[0]

    if datetime_col not in df.columns:
        return False, [f"DateTime column '{datetime_col}' not found"]

    if len(df) != HOURS_PER_YEAR:
        errors.append(f"row count {len(df)} ≠ {HOURS_PER_YEAR}")

    s = df[datetime_col].astype(str).str.strip()
    bad_fmt = ~s.str.match(_DATETIME_PAT)
    if bad_fmt.any():
        errors.append(f"{int(bad_fmt.sum())} DateTime cells with unexpected format")

    try:
        parsed = pd.to_datetime(s, errors="coerce")
    except Exception as e:                                      # pragma: no cover
        return False, [f"DateTime parse failed: {e}"] + errors

    if parsed.isna().any():
        errors.append(f"{int(parsed.isna().sum())} unparseable DateTime cells")
        return False, errors

    if not parsed.is_monotonic_increasing:
        errors.append("DateTime is not monotonically increasing")
    if parsed.duplicated().any():
        errors.append(f"{int(parsed.duplicated().sum())} duplicate DateTime rows")
    # Strictly hourly step
    diffs = parsed.diff().dropna().unique()
    if len(diffs) == 1 and diffs[0] != pd.Timedelta(hours=1):
        errors.append(f"DateTime step is {diffs[0]}, expected 1 hour")
    elif len(diffs) > 1:
        errors.append(f"DateTime step not uniform: {len(diffs)} distinct deltas")

    return len(errors) == 0, errors


# ══════════════════════════════════════════════════════════════════════
# FINGERPRINTING
# ══════════════════════════════════════════════════════════════════════
def compute_fingerprint(path: str | Path, *, length: int = 16) -> str:
    """SHA-256 of file bytes, hex-truncated to `length` chars. Returns 'sha256:<hex>'."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()[:length]}"


# ══════════════════════════════════════════════════════════════════════
# PATH RESOLUTION — unified, diagnostic-friendly
# ══════════════════════════════════════════════════════════════════════
# Candidate subdirectories, probed in order. Covers common project
# layouts: flat, `renewables/`, `re/`, `data/*`, `profiles/*`, `hydro/`,
# `demand/`, `plexos/`. Extending this tuple is the single place to add
# new layouts — the three loaders share it.
_SEARCH_SUBDIRS: tuple[str, ...] = (
    "",                             # flat project dir
    "renewables", "re", "RE",
    "hydro", "inflow", "inflows",
    "demand", "plexos",
    "data", "data/renewables", "data/re", "data/hydro", "data/demand",
    "profiles", "profiles/renewables", "profiles/hydro", "profiles/demand",
    "inputs", "inputs/renewables", "inputs/hydro", "inputs/demand",
)


def _site_name_variants(site: str) -> tuple[str, ...]:
    """
    Produce canonical filesystem name variants for a site.

    Schema always carries the underscore-canonical form (e.g. "Mta_Sabueti").
    The filesystem may use either form — some of the uploaded files use
    "Mta_Sabueti" while the attachment UI may render them with a literal
    space. Both must resolve.

    Returns a tuple preserving order: canonical first, then alternates.
    """
    canonical = site
    variants  = [canonical]
    if "_" in canonical:
        spaced = canonical.replace("_", " ")
        if spaced not in variants:
            variants.append(spaced)
    elif " " in canonical:
        under = canonical.replace(" ", "_")
        if under not in variants:
            variants.append(under)
    return tuple(variants)


def _find_in_layout(
    project_dir: Path,
    basenames: Iterable[str],
    *,
    label: str,
    recursive_fallback: bool = True,
) -> Path:
    """
    Locate the first file whose basename matches any of `basenames`,
    probing subdirectories in `_SEARCH_SUBDIRS` order.

    If none match after the structured search and ``recursive_fallback`` is
    True, a single ``rglob`` pass walks the whole tree as a last resort.
    On total failure, raises :class:`DataIOError` with a diagnostic listing
    every path that was checked.

    Args:
        project_dir:          project root to search under
        basenames:            candidate filenames (e.g., "Wind_Mta_Sabueti_2026_A_mean.csv",
                              "Wind_Mta Sabueti_2026_A_mean.csv")
        label:                short human string for the error message
                              (e.g., "renewables file", "hydro file")
        recursive_fallback:   if True, rglob the whole project_dir as a last step

    Returns:
        The resolved :class:`Path` of the first matching file found.
    """
    basenames = list(basenames)
    tried: list[str] = []

    # Structured probe: every (subdir, basename) combination.
    for sub in _SEARCH_SUBDIRS:
        for name in basenames:
            candidate = project_dir / sub / name
            tried.append(str(candidate))
            if candidate.is_file():
                return candidate

    # Last-resort recursive scan — cheap when nothing is found but the
    # structured probe may miss deeply-nested or renamed parent dirs.
    if recursive_fallback:
        target_set = set(basenames)
        try:
            for hit in project_dir.rglob("*"):
                if hit.is_file() and hit.name in target_set:
                    return hit
        except (OSError, PermissionError) as e:              # pragma: no cover
            tried.append(f"<rglob failed: {e}>")

    # Build a concise, actionable error message.
    lines = [
        f"{label} not found under {project_dir!s}.",
        f"  wanted any of: {basenames}",
        f"  probed {len(tried)} path(s):",
    ]
    # Cap the listed paths to keep the error readable.
    SHOW_MAX = 12
    for p in tried[:SHOW_MAX]:
        lines.append(f"    - {p}")
    if len(tried) > SHOW_MAX:
        lines.append(f"    … and {len(tried) - SHOW_MAX} more")
    lines.append(
        "  hint: put files in the project root or in any of: "
        + ", ".join(repr(s) for s in _SEARCH_SUBDIRS if s)
    )
    raise DataIOError("\n".join(lines))


# ══════════════════════════════════════════════════════════════════════
# HYDRO LOADERS
# ══════════════════════════════════════════════════════════════════════
# Scenario → filename mapping for zonal inflow CSVs.
# Kept as an explicit table so a filename change stays local to this dict.
_HYDRO_SCENARIO_FILE: dict[str, str] = {
    "A_mean":  "2026_A_historical_mean.csv",
    "MC_P10":  "2026_B_montecarlo_P10.csv",
    "MC_P50":  "2026_B_montecarlo_P50.csv",
    "MC_P90":  "2026_B_montecarlo_P90.csv",
}


def _resolve_hydro_path(project_dir: Path, scenario: str) -> Path:
    if scenario not in _HYDRO_SCENARIO_FILE:
        raise DataIOError(
            f"unknown hydro scenario '{scenario}'; "
            f"expected one of {list(_HYDRO_SCENARIO_FILE)}")
    return _find_in_layout(
        project_dir,
        basenames=[_HYDRO_SCENARIO_FILE[scenario]],
        label=f"hydro file for scenario '{scenario}'",
    )


def load_hydro_scenario(
    project_dir: str | Path,
    scenario: str,
) -> tuple[dict[str, list[float]], Path]:
    """
    Load the zonal inflow CSV for a given scenario.

    Returns:
        ({zone_name: [8760 floats]}, file_path_used)

    Values are RAW — no unit conversion performed.
    """
    project_dir = Path(project_dir)
    path = _resolve_hydro_path(project_dir, scenario)

    df = read_csv_robust(path)
    ok, errs = validate_datetime_index(df)
    if not ok:
        raise DatetimeMismatchError(f"{path.name}: DateTime issues → {errs}")

    missing = [z for z in HYDRO_ZONES if z not in df.columns]
    if missing:
        raise FileFormatError(
            f"{path.name}: missing hydro zone columns (first few): {missing[:3]}")

    data: dict[str, list[float]] = {}
    for z in HYDRO_ZONES:
        vals = df[z].astype(float).tolist()
        if len(vals) != HOURS_PER_YEAR:
            raise ProfileLengthError(f"{path.name}:{z} length {len(vals)}")
        if any(v != v for v in vals):
            raise FileFormatError(f"{path.name}:{z} contains NaN")
        data[z] = vals
    return data, path


def build_asset_inflow_profiles(
    hydro_by_zone: dict[str, list[float]],
    hydro_zone_map: dict[str, dict],
) -> dict[str, list[float]]:
    """
    For each (asset_id → {zone, share}) entry, produce a derived profile
    under the key ``_hydro_{asset_id}`` equal to zone_series * share.
    """
    out: dict[str, list[float]] = {}
    for aid, entry in hydro_zone_map.items():
        zone = unicodedata.normalize("NFC", entry["zone"])
        share = float(entry.get("share", 1.0))
        if zone not in hydro_by_zone:
            raise DataIOError(
                f"hydro_zone_map['{aid}']: zone '{zone}' not loaded")
        out[f"_hydro_{aid}"] = [v * share for v in hydro_by_zone[zone]]
    return out


# ══════════════════════════════════════════════════════════════════════
# RENEWABLE (WIND / SOLAR) LOADERS
# ══════════════════════════════════════════════════════════════════════
def _resolve_re_path(
    project_dir: Path, source: str, site: str, scenario: str,
) -> Path:
    """
    Resolve a renewables file, tolerating:
      • layout variation   — flat, renewables/, re/, data/renewables/, …
      • source-case        — Wind / WIND / wind  (filesystem is case-sensitive
                             on Linux, so we generate explicit variants)
      • site-name form     — Mta_Sabueti  and  Mta Sabueti
    """
    src_variants  = (source.capitalize(), source.upper(), source.lower())
    site_variants = _site_name_variants(site)

    basenames: list[str] = []
    for src in src_variants:
        for s in site_variants:
            name = f"{src}_{s}_2026_{scenario}.csv"
            if name not in basenames:
                basenames.append(name)

    return _find_in_layout(
        project_dir,
        basenames=basenames,
        label=f"renewables file (source={source}, site={site}, scenario={scenario})",
    )


def load_renewable_site(
    project_dir: str | Path,
    source: str,
    site: str,
    scenario: str,
) -> tuple[list[float], Path]:
    """
    Load one renewable-site capacity-factor profile (8760 floats in [0, 1]).
    Handles ;-delimited, comma-decimal, BOM-prefixed CSVs.
    """
    if source not in ("wind", "solar"):
        raise DataIOError(f"source must be 'wind' or 'solar' (got {source!r})")
    if site not in RE_SITES:
        raise DataIOError(f"site '{site}' not in RE_SITES")
    if scenario not in SCENARIOS:
        raise DataIOError(f"scenario '{scenario}' not in SCENARIOS")

    project_dir = Path(project_dir)
    path = _resolve_re_path(project_dir, source, site, scenario)
    df = read_csv_robust(path)

    ok, errs = validate_datetime_index(df)
    if not ok:
        raise DatetimeMismatchError(f"{path.name}: {errs}")

    # The CF column is whichever non-DateTime column exists first.
    cf_candidates = [c for c in df.columns if c != df.columns[0]]
    if not cf_candidates:
        raise FileFormatError(f"{path.name}: no CF column found")
    cf_col = cf_candidates[0]
    vals = df[cf_col].astype(float).tolist()

    if len(vals) != HOURS_PER_YEAR:
        raise ProfileLengthError(f"{path.name} length {len(vals)}")

    # Clamp to [0, 1]; CFs outside that band are physically meaningless.
    vals = [max(0.0, min(1.0, v)) for v in vals]
    return vals, path


def build_re_site_profiles(
    project_dir: str | Path,
    re_site_map: dict[str, dict],
    scenario: str,
) -> tuple[dict[str, list[float]], dict[str, str]]:
    """
    For each (asset_id → {source, site}) entry, load the CF profile and
    return derived keys ``_{source}_{site}`` plus a fingerprint map.
    """
    profiles: dict[str, list[float]] = {}
    hashes:   dict[str, str] = {}
    for aid, entry in re_site_map.items():
        src, site = entry["source"], entry["site"]
        vals, path = load_renewable_site(project_dir, src, site, scenario)
        profiles[f"_{src}_{site}"] = vals
        hashes[f"{src}_{site}"] = compute_fingerprint(path)
    return profiles, hashes


# ══════════════════════════════════════════════════════════════════════
# DEMAND LOADERS
# ══════════════════════════════════════════════════════════════════════
_CHAR_YEAR_FILENAME = "GSE_CharYear_Normalized_1.xlsx"
_PLEXOS_FILENAME    = "GSE_PLEXOS_sc2_2026_1.xlsx"


def _resolve_xlsx(project_dir: Path, basename: str) -> Path:
    return _find_in_layout(
        project_dir,
        basenames=[basename],
        label=f"workbook '{basename}'",
    )


def load_demand_shape_xlsx(project_dir: str | Path) -> tuple[list[float], Path]:
    """
    Load the 8760-length normalized demand shape (Σ ≈ 1) from
    GSE_CharYear_Normalized.  The workbook uses a 2023 DateTime template;
    we ignore the year and map by hour-of-year index 0..8759.
    """
    project_dir = Path(project_dir)
    path = _resolve_xlsx(project_dir, _CHAR_YEAR_FILENAME)
    df = pd.read_excel(path, sheet_name="CharYear_Normalized_8760")
    if len(df) != HOURS_PER_YEAR:
        raise ProfileLengthError(
            f"{path.name}: shape has {len(df)} rows, expected {HOURS_PER_YEAR}")
    if "Normalized_Profile" not in df.columns:
        raise FileFormatError(
            f"{path.name}: missing 'Normalized_Profile' column")
    return df["Normalized_Profile"].astype(float).tolist(), path


def load_demand_absolute_plexos(
    project_dir: str | Path,
) -> tuple[list[float], Path]:
    """
    Load the 8760 absolute MW demand series from GSE_PLEXOS_sc2_2026.
    """
    project_dir = Path(project_dir)
    path = _resolve_xlsx(project_dir, _PLEXOS_FILENAME)
    df = pd.read_excel(path, sheet_name="PLEXOS_sc2_2026")
    if len(df) != HOURS_PER_YEAR:
        raise ProfileLengthError(
            f"{path.name}: has {len(df)} rows, expected {HOURS_PER_YEAR}")
    if "MW_sc2_2026" not in df.columns:
        raise FileFormatError(
            f"{path.name}: missing 'MW_sc2_2026' column")
    return df["MW_sc2_2026"].astype(float).tolist(), path


def build_demand_profile(
    demand_spec: dict,
    shape_profile: list[float] | None,
    absolute_profile: list[float] | None,
) -> list[float]:
    """
    Materialize `profiles.demand` (MW per hour) from a demand_spec.

    Mode 'shape_times_annual':
        demand[h] = shape[h] * annual_twh * 1e6           [MW]
        Σshape ≈ 1  ⇒  Σ(demand * 1h) ≈ annual_twh * 1e6 MWh = annual_twh TWh.

    Mode 'absolute':
        demand[h] = absolute_profile[h]                   [MW, pass-through]
    """
    mode = demand_spec.get("mode")
    if mode == "absolute":
        if absolute_profile is None:
            raise DataIOError(
                "demand_spec.mode='absolute' but absolute_profile is None")
        return list(absolute_profile)
    if mode == "shape_times_annual":
        if shape_profile is None:
            raise DataIOError(
                "demand_spec.mode='shape_times_annual' but shape_profile is None")
        atw = float(demand_spec["annual_twh"])
        scale = atw * 1e6                                  # TWh → MWh/h = MW
        return [v * scale for v in shape_profile]
    raise DataIOError(f"unknown demand_spec.mode '{mode}'")


# ══════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════
def _default_solver_settings() -> dict:
    return {
        "mip_gap":             0.01,
        "time_limit_s":        300,
        "rolling_window_h":    168,
        "rolling_step_h":      24,
        "unserved_penalty":    3000.0,
        "curtailment_penalty": 0.0,
        "threads":             0,
    }


def build_input_from_project(
    project_dir: str | Path,
    *,
    assets: list[dict],
    hydro_zone_map: dict[str, dict],
    re_site_map: dict[str, dict],
    reserve_products: list[dict],
    gas_constraints: dict,
    study_horizon: dict,
    scenario: str = "A_mean",
    annual_twh: float = 15.621,
    demand_mode: str = "shape_times_annual",
    solver_settings: dict | None = None,
    hydro_inflow_unit: str = "raw",
    scenario_metadata: dict | None = None,
    study_year: int = 2026,
) -> dict:
    """
    Assemble a schema v1.1 input dict from the raw project files.

    Steps:
      1. Load hydro CSV for `scenario`.
      2. Load every renewable site referenced in `re_site_map` × `scenario`.
      3. Load demand shape (xlsx) or absolute (PLEXOS) per `demand_mode`.
      4. Materialize derived profile keys (_hydro_*, _wind_*, _solar_*).
      5. Rewrite each asset's inflow_profile / availability_profile to point
         at the derived keys (only for assets present in the maps).
      6. Assemble profile_bundle with SHA-256 file fingerprints.
      7. Validate against schema v1.1.  Raise on any error.

    Returns:
        dict serializable to ``powersim_input.json``.
    """
    project_dir = Path(project_dir)
    if not project_dir.is_dir():
        raise DataIOError(f"project_dir not found: {project_dir}")
    if scenario not in SCENARIOS:
        raise DataIOError(f"scenario '{scenario}' not in {SCENARIOS}")
    if hydro_inflow_unit not in HYDRO_INFLOW_UNITS:
        raise DataIOError(
            f"hydro_inflow_unit '{hydro_inflow_unit}' not in {HYDRO_INFLOW_UNITS}")
    if demand_mode not in DEMAND_MODES:
        raise DataIOError(f"demand_mode '{demand_mode}' not in {DEMAND_MODES}")

    file_hashes: dict[str, object] = {}

    # 1) hydro
    hydro_by_zone, hydro_path = load_hydro_scenario(project_dir, scenario)
    file_hashes["hydro_csv"] = compute_fingerprint(hydro_path)
    hydro_profiles = build_asset_inflow_profiles(hydro_by_zone, hydro_zone_map)

    # 2) renewables
    re_profiles, re_hashes = build_re_site_profiles(
        project_dir, re_site_map, scenario)
    file_hashes["renewables_csvs"] = re_hashes

    # 3) demand
    shape_vals: list[float] | None = None
    abs_vals:   list[float] | None = None
    if demand_mode == "shape_times_annual":
        shape_vals, shape_path = load_demand_shape_xlsx(project_dir)
        file_hashes["demand_shape_xlsx"] = compute_fingerprint(shape_path)
        demand_source_label = "CharYear_x_SCurve"
    else:                                                   # "absolute"
        abs_vals, abs_path = load_demand_absolute_plexos(project_dir)
        file_hashes["demand_absolute_xlsx"] = compute_fingerprint(abs_path)
        demand_source_label = "PLEXOS_sc2_2026_absolute"

    demand_spec = {
        "mode":                 demand_mode,
        "shape_profile_key":    "demand_shape"    if shape_vals is not None else None,
        "annual_twh":           annual_twh        if shape_vals is not None else None,
        "absolute_profile_key": "demand_absolute" if abs_vals   is not None else None,
    }
    demand_mw = build_demand_profile(demand_spec, shape_vals, abs_vals)

    # 4) combined profiles dict (derived keys are underscore-prefixed)
    profiles: dict[str, list[float]] = {"demand": demand_mw}
    profiles.update(hydro_profiles)
    profiles.update(re_profiles)
    if shape_vals is not None:
        profiles["demand_shape"] = shape_vals
    if abs_vals is not None:
        profiles["demand_absolute"] = abs_vals

    # 5) rewrite asset pointers so the solver consumes derived keys
    assets_out: list[dict] = []
    for a in assets:
        a2 = dict(a)
        aid = a2.get("id")
        if aid in hydro_zone_map and a2.get("type") in ("hydro_reg", "hydro_ror"):
            a2["inflow_profile"] = f"_hydro_{aid}"
        if aid in re_site_map and a2.get("type") in ("wind", "solar"):
            ent = re_site_map[aid]
            a2["availability_profile"] = f"_{ent['source']}_{ent['site']}"
        assets_out.append(a2)

    # 6) profile_bundle
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    profile_bundle = {
        "scenario_id":       scenario,
        "hydro_source":      _HYDRO_SCENARIO_FILE[scenario].replace(".csv", ""),
        "renewables_source": scenario,
        "demand_source":     demand_source_label,
        "hydro_inflow_unit": hydro_inflow_unit,
        "generated_by":      LOADER_VERSION,
        "generated_at":      now_iso,
        "file_hashes":       file_hashes,
    }

    # 7) assemble
    inp = {
        "metadata": {
            "model_version":  MODEL_VERSION,
            "schema_version": SCHEMA_VERSION,
            "timezone":       TIMEZONE,
            "exported_at":    now_iso,
            "study_year":     study_year,
            "description":    f"Built by {LOADER_VERSION} — scenario={scenario}",
        },
        "time_index":        generate_time_index(study_year),
        "study_horizon":     study_horizon,
        "assets":            assets_out,
        "profiles":          profiles,
        "profile_bundle":    profile_bundle,
        "hydro_zone_map":    hydro_zone_map,
        "re_site_map":       re_site_map,
        "demand_spec":       demand_spec,
        "reserve_products":  reserve_products,
        "gas_constraints":   gas_constraints,
        "solver_settings":   solver_settings or _default_solver_settings(),
        "scenario_metadata": scenario_metadata or {
            "id": scenario, "label": scenario, "probability": 1.0,
        },
    }

    # Validate generated input — must pass before returning.
    ok, errs, warns = validate_input(inp)
    if not ok:
        raise DataIOError(
            "Generated input failed schema v1.1 validation:\n  - "
            + "\n  - ".join(errs))
    if warns:
        # Keep warnings visible but do not bake them into the schema output.
        inp.setdefault("_loader_meta", {})["warnings"] = warns
    return inp


# ══════════════════════════════════════════════════════════════════════
# SUMMARY HELPER
# ══════════════════════════════════════════════════════════════════════
def summary_report(inp: dict) -> str:
    """Short, human-readable summary of what was loaded."""
    sb = inp["profile_bundle"]
    sh = inp["study_horizon"]
    ds = inp["demand_spec"]
    demand = inp["profiles"]["demand"]

    asset_types: dict[str, int] = {}
    for a in inp["assets"]:
        asset_types[a["type"]] = asset_types.get(a["type"], 0) + 1
    hydro_keys = [k for k in inp["profiles"] if k.startswith("_hydro_")]
    re_keys    = [k for k in inp["profiles"] if k.startswith(("_solar_", "_wind_"))]

    total_hydro_raw = sum(sum(inp["profiles"][k]) for k in hydro_keys)
    demand_twh = sum(demand) / 1e6

    lines = [
        f"PowerSim input built by {LOADER_VERSION}",
        f"  Scenario:         {sb['scenario_id']}",
        f"  Hydro source:     {sb['hydro_source']}",
        f"  Demand source:    {sb['demand_source']}   (mode={ds['mode']})",
        f"  Hydro unit:       {sb['hydro_inflow_unit']}  (not yet verified)",
        f"  Horizon:          start={sh['start_hour']}h  length={sh['horizon_hours']}h",
        f"  Assets:           {len(inp['assets'])}   {asset_types}",
        f"  Profile keys:     {len(inp['profiles'])} "
        f"(hydro={len(hydro_keys)}, re={len(re_keys)})",
        f"  Annual demand:    {demand_twh:>7.3f} TWh",
        f"  Σ hydro (raw):    {total_hydro_raw:>10.1f}  (unit unchecked)",
    ]
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════
# D3 — HYDRO INFLOW DIAGNOSTIC
# ══════════════════════════════════════════════════════════════════════
def hydro_inflow_diagnostic(
    project_dir: str | Path,
    scenarios: Iterable[str] = SCENARIOS,
    hydro_zone_map: dict[str, dict] | None = None,
) -> dict:
    """
    Report annual inflow totals per zone (and per mapped asset) under
    each scenario, WITHOUT converting units.  The goal is to give the
    operator enough information to externally verify what the 'raw'
    numbers physically represent (Mm³/h vs m³/s vs normalized).

    Returns a dict:
      {
        "unit_declared": "raw",     # from caller's metadata only
        "hours_per_year": 8760,
        "by_scenario": {
           "A_mean": {
              "by_zone": {zone_name: {"annual_sum": float,
                                      "hourly_mean": float,
                                      "hourly_min":  float,
                                      "hourly_max":  float,
                                      "file_path":   str}},
              "by_asset": {asset_id: {"zone": ..., "share": ...,
                                      "annual_sum_shared": float}},
              "system_total":     float,
           }, ...
        },
        "notes": [...],
      }
    """
    project_dir = Path(project_dir)
    out: dict = {
        "unit_declared": "raw",
        "hours_per_year": HOURS_PER_YEAR,
        "by_scenario":    {},
        "notes": [
            "Inflow values are passed through unchanged from the source CSV.",
            "This diagnostic is unit-agnostic; values are summed as-is.",
            "If 'raw' ≈ Mm³/h, then annual_sum has units of Mm³/year.",
            "If 'raw' ≈ m³/s, convert:  Mm³/year = annual_sum × 3600 / 1e6.",
            "If 'raw' ≈ normalized,     no physical meaning; pair with a per-zone scaler.",
            "Cross-check: divide annual_sum by a known historical GWh figure "
            "for the zone to estimate MWh/Mm³ efficiency (typical ≈ 200–600).",
        ],
    }

    for scen in scenarios:
        try:
            by_zone, path = load_hydro_scenario(project_dir, scen)
        except DataIOError as e:
            out["by_scenario"][scen] = {"error": str(e)}
            continue

        zone_stats = {}
        for zname, arr in by_zone.items():
            n = len(arr)
            s = sum(arr)
            zone_stats[zname] = {
                "annual_sum":    round(s, 4),
                "hourly_mean":   round(s / max(n, 1), 6),
                "hourly_min":    round(min(arr), 6) if arr else 0.0,
                "hourly_max":    round(max(arr), 6) if arr else 0.0,
                "n_hours":       n,
            }

        asset_stats = {}
        if hydro_zone_map:
            for aid, entry in hydro_zone_map.items():
                z = unicodedata.normalize("NFC", entry.get("zone", ""))
                share = float(entry.get("share", 1.0))
                if z in by_zone:
                    asset_stats[aid] = {
                        "zone":              z,
                        "share":             share,
                        "annual_sum_shared": round(sum(by_zone[z]) * share, 4),
                    }

        out["by_scenario"][scen] = {
            "file_path":    str(path),
            "by_zone":      zone_stats,
            "by_asset":     asset_stats,
            "system_total": round(sum(stats["annual_sum"] for stats in zone_stats.values()), 4),
        }

    return out


def print_inflow_diagnostic(diag: dict) -> None:
    """Human-readable formatter for the hydro inflow diagnostic."""
    print("─" * 78)
    print(f"Hydro inflow diagnostic  —  declared unit: '{diag['unit_declared']}' (unverified)")
    print(f"Hours per year: {diag['hours_per_year']}")
    print("─" * 78)

    # By-scenario, by-zone totals
    scenarios = list(diag["by_scenario"].keys())
    first_scen = scenarios[0] if scenarios else None
    if first_scen is None or "error" in diag["by_scenario"][first_scen]:
        print("No valid scenarios loaded.")
        return

    zones = list(diag["by_scenario"][first_scen]["by_zone"].keys())
    # Header row
    print(f"\nAnnual inflow totals by zone (raw units, unconverted):")
    header = f"  {'zone':<32} | " + " | ".join(f"{s:>10}" for s in scenarios)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for z in zones:
        vals = []
        for s in scenarios:
            byz = diag["by_scenario"][s].get("by_zone", {})
            if z in byz:
                vals.append(f"{byz[z]['annual_sum']:>10.2f}")
            else:
                vals.append(f"{'?':>10}")
        print(f"  {z[:32]:<32} | " + " | ".join(vals))
    sys_line = ["SYSTEM TOTAL"]
    totals = []
    for s in scenarios:
        totals.append(f"{diag['by_scenario'][s].get('system_total', 0):>10.2f}")
    print(f"  {'SYSTEM TOTAL':<32} | " + " | ".join(totals))

    # By-asset rollup (if hydro_zone_map was supplied)
    any_asset = any(diag["by_scenario"][s].get("by_asset") for s in scenarios)
    if any_asset:
        print(f"\nAnnual inflow by mapped asset (share-applied, raw units):")
        asset_ids = sorted(set().union(*[
            set(diag["by_scenario"][s].get("by_asset", {}).keys()) for s in scenarios
        ]))
        for aid in asset_ids:
            first = next((diag["by_scenario"][s]["by_asset"][aid]
                          for s in scenarios
                          if aid in diag["by_scenario"][s].get("by_asset", {})), None)
            if not first: continue
            z = first["zone"]; sh = first["share"]
            print(f"  {aid:<30} (zone={z[:24]}, share={sh})")
            for s in scenarios:
                byz = diag["by_scenario"][s].get("by_asset", {})
                if aid in byz:
                    print(f"    {s:<10}  annual_sum_shared = {byz[aid]['annual_sum_shared']:>10.2f}")

    print(f"\nNotes:")
    for n in diag["notes"]:
        print(f"  • {n}")


# ══════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════
def _load_config(path: str | Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="powersim_dataio",
        description="Build PowerSim v4.0 input JSON from project data files.",
    )
    ap.add_argument("--project-dir", required=True,
                    help="Directory containing hydro/renewable/xlsx files.")
    ap.add_argument("--config", default=None,
                    help="Fleet + mapping config JSON (e.g. stage1_smoke_fleet.json). "
                         "Required unless --inflow-diag is used.")
    ap.add_argument("--out", default="powersim_input.json",
                    help="Output JSON path (default: powersim_input.json).")
    ap.add_argument("--inflow-diag", action="store_true",
                    help="Run hydro inflow diagnostic and exit; no input JSON written.")
    ap.add_argument("--inflow-diag-json", default=None,
                    help="Write the inflow diagnostic as JSON to this path.")
    # Optional overrides
    ap.add_argument("--scenario",          default=None, choices=SCENARIOS)
    ap.add_argument("--annual-twh",        type=float, default=None)
    ap.add_argument("--demand-mode",       default=None, choices=DEMAND_MODES)
    ap.add_argument("--horizon-hours",     type=int,  default=None)
    ap.add_argument("--start-hour",        type=int,  default=None)
    ap.add_argument("--hydro-inflow-unit", default=None, choices=HYDRO_INFLOW_UNITS)
    args = ap.parse_args(argv)

    # ── D3 mode: inflow diagnostic only ─────────────────────────────
    if args.inflow_diag:
        hzm = {}
        if args.config:
            cfg = _load_config(args.config)
            hzm = cfg.get("hydro_zone_map", {})
        scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
        diag = hydro_inflow_diagnostic(args.project_dir,
                                        scenarios=scenarios,
                                        hydro_zone_map=hzm)
        print_inflow_diagnostic(diag)
        if args.inflow_diag_json:
            Path(args.inflow_diag_json).write_text(
                json.dumps(diag, ensure_ascii=False, indent=2),
                encoding="utf-8")
            print(f"\n💾 wrote {args.inflow_diag_json}")
        return 0

    # ── normal mode: build input JSON ───────────────────────────────
    if not args.config:
        ap.error("--config is required unless --inflow-diag is used")

    cfg = _load_config(args.config)

    scenario    = args.scenario    or cfg.get("scenario", "A_mean")
    demand_mode = args.demand_mode or cfg.get("demand_mode", "shape_times_annual")
    annual_twh  = args.annual_twh if args.annual_twh is not None \
                   else cfg.get("annual_twh", 15.621)
    hiu         = args.hydro_inflow_unit or cfg.get("hydro_inflow_unit", "raw")

    sh = dict(cfg.get("study_horizon",
                       {"start_hour": 0, "horizon_hours": 8760, "mode": "full"}))
    if args.horizon_hours is not None: sh["horizon_hours"] = args.horizon_hours
    if args.start_hour    is not None: sh["start_hour"]    = args.start_hour

    print(f"[dataio] scenario={scenario}  demand_mode={demand_mode}  "
          f"annual_twh={annual_twh}  horizon={sh.get('horizon_hours')}h  "
          f"hydro_unit={hiu}")

    inp = build_input_from_project(
        args.project_dir,
        assets            = cfg["assets"],
        hydro_zone_map    = cfg.get("hydro_zone_map", {}),
        re_site_map       = cfg.get("re_site_map", {}),
        reserve_products  = cfg.get("reserve_products", []),
        gas_constraints   = cfg.get("gas_constraints", {}),
        study_horizon     = sh,
        scenario          = scenario,
        annual_twh        = annual_twh,
        demand_mode       = demand_mode,
        solver_settings   = cfg.get("solver_settings"),
        hydro_inflow_unit = hiu,
        scenario_metadata = cfg.get("scenario_metadata"),
        study_year        = cfg.get("study_year", 2026),
    )

    out_path = Path(args.out)
    # Compact JSON — profiles are big; pretty-print bloats the file.
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(inp, f, ensure_ascii=False, separators=(",", ":"))

    print(summary_report(inp))
    print(f"\n[dataio] wrote {out_path}  "
          f"({out_path.stat().st_size / 1024:.1f} KiB)")

    if "_loader_meta" in inp and inp["_loader_meta"].get("warnings"):
        print("\n[dataio] warnings:")
        for w in inp["_loader_meta"]["warnings"]:
            print(f"  ⚠ {w}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
