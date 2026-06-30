#!/usr/bin/env python3
"""CLI for the PyPSA-Eur cost-database snapshot.

Examples
--------
    # List available years and ~40 curated technologies
    python scripts/cost_lookup.py --list

    # Show every parameter PyPSA records for a tech in 2030
    python scripts/cost_lookup.py --tech CCGT --year 2030

    # Compare investment / FOM / lifetime across 4 snapshot years
    python scripts/cost_lookup.py --tech "solar-utility" --param investment

    # Annualized USD/MW/yr CAPEX for a BESS sizing input
    python scripts/cost_lookup.py --tech "battery storage" --year 2030 --annualize
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "solver"))

from cost_database import (
    available_years, available_technologies, get,
    annualized_capex_usd_per_mw_yr, load,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="PyPSA-Eur cost-database lookup")
    ap.add_argument("--list", action="store_true",
                    help="list available years and technologies")
    ap.add_argument("--tech", help="technology name (exact match)")
    ap.add_argument("--year", type=int, help="snapshot year (omit ⇒ all years)")
    ap.add_argument("--param", help="single parameter name (omit ⇒ all)")
    ap.add_argument("--annualize", action="store_true",
                    help="print annualized CAPEX in USD/MW/yr")
    ap.add_argument("--eur-to-usd", type=float, default=1.08)
    ap.add_argument("--discount-rate", type=float, default=0.07)
    args = ap.parse_args()

    if args.list or not args.tech:
        years = available_years()
        print(f"Snapshot years: {years}")
        for y in years:
            techs = available_technologies(y)
            print(f"\n  {y} ({len(techs)} technologies):")
            for t in techs:
                print(f"    {t}")
        return 0

    years = [args.year] if args.year else available_years()

    if args.annualize:
        if not args.year:
            for y in years:
                try:
                    v = annualized_capex_usd_per_mw_yr(
                        args.tech, y,
                        eur_to_usd=args.eur_to_usd,
                        discount_rate=args.discount_rate,
                    )
                    print(f"  {y}: {v:>12,.0f} USD/MW/yr")
                except Exception as e:
                    print(f"  {y}: skip ({e})")
        else:
            v = annualized_capex_usd_per_mw_yr(
                args.tech, args.year,
                eur_to_usd=args.eur_to_usd,
                discount_rate=args.discount_rate,
            )
            print(f"{args.tech} @ {args.year}: {v:,.0f} USD/MW/yr "
                  f"(FX {args.eur_to_usd}, r={args.discount_rate})")
        return 0

    for y in years:
        try:
            params = get(args.tech, y)
        except Exception as e:
            print(f"\n[{y}] {e}")
            continue
        print(f"\n[{y}] {args.tech}")
        items = params.items() if not args.param else [
            (args.param, params.get(args.param))]
        for p, info in items:
            if info is None:
                print(f"  {p:30s} (not set)")
            else:
                v = float(info["value"])
                u = info.get("unit", "")
                s = info.get("source", "")
                print(f"  {p:30s} = {v:>14,.4f} {u:<18s} {s[:60]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
