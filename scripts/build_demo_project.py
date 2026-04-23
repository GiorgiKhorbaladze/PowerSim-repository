"""
PowerSim v4.0 — Demo Project Data Generator
===========================================

Synthesizes a self-contained ``project_data/`` directory with realistic but
SYNTHETIC profiles so the full HTML→JSON→solver→JSON pipeline can be smoke
tested without the proprietary GSE source files.

Outputs (8760h / non-leap year):
    project_data/2026_A_historical_mean.csv   (16 hydro zones)
    project_data/2026_B_montecarlo_P10.csv
    project_data/2026_B_montecarlo_P50.csv
    project_data/2026_B_montecarlo_P90.csv
    project_data/Solar_<Site>_2026_<Scenario>.csv  (10 sites × 4 scenarios)
    project_data/Wind_<Site>_2026_<Scenario>.csv
    project_data/GSE_CharYear_Normalized_1.xlsx   (demand shape, Σ≈1)

Once built, the smoke pipeline can be exercised:

    python tests/smoke_168h.py \\
        --project-dir project_data \\
        --config      tests/stage1_smoke_fleet.json \\
        --keep-outputs out/

This is INTENDED for evaluation and CI only — replace project_data/ with
the real GSE files for production runs.
"""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# Resolve schema regardless of where the script is launched from.
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
for p in (ROOT / "schema", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from powersim_schema import HYDRO_ZONES, RE_SITES, generate_time_index, HOURS_PER_YEAR


def hourly_index(year: int = 2026) -> list[str]:
    return generate_time_index(year)


def synth_demand_shape(seed: int = 42) -> np.ndarray:
    """Realistic 8760h normalized demand shape (Σ≈1) with diurnal + seasonal."""
    rng = np.random.default_rng(seed)
    h = np.arange(HOURS_PER_YEAR)
    hour_of_day = h % 24
    day_of_year = h // 24
    diurnal  = 0.85 + 0.18 * np.sin((hour_of_day - 7) / 24 * 2 * math.pi)
    diurnal += 0.10 * np.sin((hour_of_day - 18) / 24 * 2 * math.pi) ** 2
    seasonal = 1.00 + 0.20 * np.cos((day_of_year - 15) / 365 * 2 * math.pi)  # winter peak
    weekend  = np.where((day_of_year % 7) >= 5, 0.92, 1.0)
    noise    = 1 + 0.04 * rng.standard_normal(HOURS_PER_YEAR)
    raw = diurnal * seasonal * weekend * noise
    raw = np.clip(raw, 0.40, None)
    return raw / raw.sum()


def synth_solar_cf(seed: int) -> np.ndarray:
    """Solar capacity factor [0,1], 8760h. Diurnal + seasonal."""
    rng = np.random.default_rng(seed)
    h = np.arange(HOURS_PER_YEAR)
    hour_of_day = h % 24
    day_of_year = h // 24
    daylight = np.maximum(0.0, np.sin((hour_of_day - 6) / 12 * math.pi))
    summer   = 0.6 + 0.4 * np.sin((day_of_year - 80) / 365 * 2 * math.pi)
    cf = daylight ** 1.4 * summer * 0.95
    cf = cf * (0.7 + 0.6 * rng.uniform(0, 1, HOURS_PER_YEAR) ** 2)
    return np.clip(cf, 0.0, 1.0)


def synth_wind_cf(seed: int) -> np.ndarray:
    """Wind CF [0,1], 8760h. Persistent stochastic + seasonal."""
    rng = np.random.default_rng(seed)
    h = np.arange(HOURS_PER_YEAR)
    day_of_year = h // 24
    seasonal = 0.32 + 0.12 * np.cos((day_of_year - 30) / 365 * 2 * math.pi)
    # AR(1) noise to model persistence
    noise = np.zeros(HOURS_PER_YEAR)
    e = rng.standard_normal(HOURS_PER_YEAR)
    for t in range(1, HOURS_PER_YEAR):
        noise[t] = 0.85 * noise[t - 1] + 0.30 * e[t]
    cf = seasonal + 0.18 * noise
    return np.clip(cf, 0.0, 0.95)


def synth_zone_inflow(zone_idx: int, scenario: str) -> np.ndarray:
    """Synthetic hourly inflow per zone (Mm³/h-equivalent 'raw' units).

    Realistic shape: spring snow-melt peak, late-summer trough, low winter
    baseflow. Scenario shifts amplitude (P10 wet > P50 ≈ A_mean > P90 dry).
    """
    rng = np.random.default_rng(1000 + zone_idx + hash(scenario) % 997)
    h = np.arange(HOURS_PER_YEAR)
    day = h / 24.0
    snowmelt = np.exp(-((day - 130) / 35) ** 2) * 1.6
    summer   = np.exp(-((day -  60) / 50) ** 2) * 0.6
    winter   = 0.35 + 0.10 * np.cos((day - 15) / 365 * 2 * math.pi)
    base     = winter + snowmelt + summer
    # Per-zone amplitude
    amp = (0.06 + 0.04 * (zone_idx % 4)) * (1 + 0.05 * (zone_idx // 4))
    scen_factor = {"A_mean": 1.00, "MC_P50": 1.00,
                   "MC_P10": 1.30, "MC_P90": 0.65}.get(scenario, 1.0)
    noise = 1 + 0.10 * rng.standard_normal(HOURS_PER_YEAR)
    return np.clip(base * amp * scen_factor * noise, 0.0, None)


def write_hydro_csv(out_path: Path, scenario: str, year: int) -> None:
    cols = ["DateTime"] + list(HYDRO_ZONES)
    df = pd.DataFrame({"DateTime": hourly_index(year)})
    for i, z in enumerate(HYDRO_ZONES):
        df[z] = synth_zone_inflow(i, scenario).round(6)
    df = df[cols]
    df.to_csv(out_path, index=False, sep=";", decimal=",", encoding="utf-8")


def write_renewable_csv(out_path: Path, source: str, site: str,
                        scenario: str, year: int) -> None:
    rng_seed = abs(hash((source, site, scenario))) % 10_000
    if source == "solar":
        cf = synth_solar_cf(rng_seed)
    else:
        cf = synth_wind_cf(rng_seed)
    df = pd.DataFrame({
        "DateTime": hourly_index(year),
        f"{site}_CF": cf.round(5),
    })
    df.to_csv(out_path, index=False, sep=";", decimal=",", encoding="utf-8")


def write_charyear_xlsx(out_path: Path, year: int) -> None:
    shape = synth_demand_shape().round(8)
    df = pd.DataFrame({
        "DateTime": hourly_index(year),
        "Normalized_Profile": shape,
    })
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="CharYear_Normalized_8760", index=False)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="project_data",
                    help="Target directory for generated CSV/XLSX files.")
    ap.add_argument("--year", type=int, default=2026,
                    help="Study year — must be non-leap (8760h).")
    ap.add_argument("--scenarios", nargs="+",
                    default=["A_mean", "MC_P10", "MC_P50", "MC_P90"])
    args = ap.parse_args(argv)

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"PowerSim v4.0 — synthesizing demo project_data → {out_dir}")
    print(f"  Year:       {args.year}")
    print(f"  Hydro:      {len(HYDRO_ZONES)} zones × {len(args.scenarios)} scenario(s)")
    print(f"  Renewables: {len(RE_SITES)} sites × 2 sources × {len(args.scenarios)} scenario(s)")

    # Hydro
    scenario_to_filename = {
        "A_mean":  "2026_A_historical_mean.csv",
        "MC_P10":  "2026_B_montecarlo_P10.csv",
        "MC_P50":  "2026_B_montecarlo_P50.csv",
        "MC_P90":  "2026_B_montecarlo_P90.csv",
    }
    for scen in args.scenarios:
        if scen not in scenario_to_filename:
            print(f"  ! skipping unknown scenario {scen}")
            continue
        fp = out_dir / scenario_to_filename[scen]
        write_hydro_csv(fp, scen, args.year)
        print(f"  ✓ hydro     {fp.name}")

    # Renewables (10 sites × wind/solar × N scenarios)
    for scen in args.scenarios:
        for site in RE_SITES:
            for source in ("solar", "wind"):
                src_label = source.capitalize()
                fp = out_dir / f"{src_label}_{site}_2026_{scen}.csv"
                write_renewable_csv(fp, source, site, scen, args.year)
        print(f"  ✓ renewables ({scen}): 20 files")

    # Demand shape
    fp = out_dir / "GSE_CharYear_Normalized_1.xlsx"
    write_charyear_xlsx(fp, args.year)
    print(f"  ✓ demand    {fp.name}")

    print(f"\n✅ Demo project_data written to {out_dir}")
    print(f"   To smoke-test the full pipeline:")
    print(f"     python tests/smoke_168h.py --project-dir {out_dir} \\")
    print(f"         --config tests/stage1_smoke_fleet.json --keep-outputs out/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
