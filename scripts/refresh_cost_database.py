#!/usr/bin/env python3
"""Rebuild data/cost_database_pypsa_eur.json from the upstream
PyPSA technology-data CSVs.

Curates the subset of technologies relevant to PowerSim (Georgia 2026+
planning use case) and the subset of parameters the solver / sizing
tools actually consume. The resulting JSON is ~120 KiB — small enough
to commit and update annually.

Usage:
    python scripts/refresh_cost_database.py
    python scripts/refresh_cost_database.py --years 2025 2030 2035
    python scripts/refresh_cost_database.py --out data/cost_database_pypsa_eur.json
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from pathlib import Path

DEFAULT_YEARS = [2025, 2030, 2035, 2040]
BASE_URL = "https://raw.githubusercontent.com/PyPSA/technology-data/master/outputs/costs_{year}.csv"

# Curated subset relevant to PowerSim / Georgia planning.
TECHNOLOGIES = [
    # Thermal
    "CCGT", "OCGT", "coal", "nuclear",
    "biomass", "biomass CHP", "biogas", "geothermal",
    # VRE
    "onwind", "offwind", "offwind-float",
    "solar", "solar-utility",
    "solar-utility single-axis tracking", "solar-rooftop",
    # Storage
    "battery storage", "battery inverter",
    "home battery storage", "home battery inverter",
    # Hydro
    "Pumped-Storage-Hydro-bicharger", "Pumped-Storage-Hydro-store",
    "hydro", "ror",
    # Hydrogen
    "Alkaline electrolyzer large size",
    "Alkaline electrolyzer medium size",
    "PEM electrolyzer small size",
    "electrolysis", "fuel cell",
    "hydrogen storage tank type 1 including compressor",
    "hydrogen storage tank type 1",
    "hydrogen storage underground", "hydrogen storage cavern",
    "SMR", "SMR CC",
    # Transmission
    "HVDC inverter pair", "HVDC overhead", "HVDC submarine", "HVDC underground",
    "HVAC overhead", "HVAC submarine", "HVAC underground",
    # Sector-coupling reference
    "central air-sourced heat pump",
]

PARAMETERS = {
    "investment", "FOM", "VOM",
    "efficiency", "efficiency-heat",
    "lifetime", "fuel", "CO2 intensity",
    "discount rate", "electricity-input",
}


def fetch_csv(year: int) -> list[dict]:
    url = BASE_URL.format(year=year)
    with urllib.request.urlopen(url, timeout=30) as r:
        text = r.read().decode("utf-8")
    return list(csv.DictReader(text.splitlines()))


def build(years: list[int]) -> dict:
    snapshot = {
        "meta": {
            "version": "1.0",
            "source": "PyPSA technology-data (https://github.com/PyPSA/technology-data)",
            "license": "CC-BY 4.0 (data) / GPL-3.0 (scripts)",
            "snapshot_years": years,
            "currency": "EUR (mixed reference years — see per-row source)",
            "note": "Curated subset for PowerSim. Refresh via scripts/refresh_cost_database.py.",
        },
        "costs_by_year": {},
    }
    for y in years:
        print(f"  fetching {y} …", file=sys.stderr)
        rows = fetch_csv(y)
        yd = {}
        for t in TECHNOLOGIES:
            params = {}
            for r in rows:
                if r["technology"] == t and r["parameter"] in PARAMETERS:
                    try:
                        v = float(r["value"])
                    except (TypeError, ValueError):
                        continue
                    params[r["parameter"]] = {
                        "value": v, "unit": r.get("unit") or "",
                        "source": (r.get("source") or "")[:120],
                    }
            if params:
                yd[t] = params
        snapshot["costs_by_year"][str(y)] = yd
        print(f"    {y}: {len(yd)} technologies", file=sys.stderr)
    return snapshot


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild PyPSA-Eur cost database snapshot")
    ap.add_argument("--years", type=int, nargs="*", default=DEFAULT_YEARS)
    ap.add_argument("--out", default="data/cost_database_pypsa_eur.json")
    args = ap.parse_args()

    snap = build(args.years)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(snap, indent=2, ensure_ascii=False))
    size = out.stat().st_size / 1024
    print(f"\nWrote {out} — {size:.1f} KiB", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
