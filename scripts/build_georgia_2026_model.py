"""Build the Georgia 2026/2027 unit-level dispatch models from committed inputs.

Usage::

    python scripts/build_georgia_2026_model.py --year 2026 --out samples/projects/georgia_2026_balance.json
    python scripts/build_georgia_2026_model.py --year 2027 --out samples/projects/georgia_2027_balance.json

The 2027 model is the 2026 model with the GSE P50 2027 load and the plants
listed in ``ADDITIONS[2027]`` (Ruisi wind, 206 MW).

Inputs (all committed):

* ``data/gse_load_2026_2030.json`` - GSE P50 hourly load 2026 (15,621 GWh).
* ``samples/demo_asset_registry_2026.json`` - 2026 installed capacities.
* ``data/actuals/georgia_balance_2024.json`` / ``..._2025.json`` - monthly
  balances supplied by the user (parsed by ``scripts/parse_georgia_balance.py``).
* ``data/cost_database_pypsa_eur.json`` - generic thermal efficiencies / VOM.
* ``data/renewables/Wind_Gori_2026_A_mean.csv`` / ``Solar_Gori_2026_A_mean.csv``
  - hourly shapes for wind and solar.

Modelling choices (documented in ``docs/GEORGIA_2026_MODEL.md``):

* Hydrology = mean of 2024 and 2025 monthly energy per plant (user choice).
  Regulating HPPs: exact monthly energy budget, dispatchable within the month
  up to installed capacity. Seasonal HPPs and the small-HPP aggregate: flat
  within each month at the monthly mean, spill allowed.
* Net interconnector flow = 2025 monthly net balance (user choice), flat
  within the month: net import as a fixed import, net export added to load.
* Thermal units are dispatched by the optimiser on illustrative costs; the
  cost inputs are placeholders that require review (user choice).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOURS = 8760
MONTH_HOURS = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
MONTH_END = [sum(MONTH_HOURS[: i + 1]) for i in range(12)]
MMBTU_PER_MWH = 3.412142  # 1 MWh = 3.412142 MMBtu

# PDF row name -> registry asset ids (capacity is the sum of the registry units)
REGULATING = {
    "ენგურჰესი": ["hydro_res_006"], "ვარდნილჰესი": ["hydro_res_005"], "ხრამჰესი 1": ["hydro_res_001"],
    "ხრამჰესი 2": ["hydro_res_004"], "შაორჰესი": ["hydro_res_002"], "ძევრულაჰესი": ["hydro_res_003"],
    "ჟინვალჰესი": ["hydro_res_007"],
}
SEASONAL = {
    "ვარციხეჰესი": ["hydro_seasonal_009", "hydro_seasonal_010", "hydro_seasonal_011", "hydro_seasonal_012"],
    "გუმათჰესი": ["hydro_seasonal_006", "hydro_seasonal_007"], "რიონჰესი": ["hydro_seasonal_002"],
    "ლაჯანურჰესი": ["hydro_seasonal_008"], "ორთაჭალაჰესი": ["hydro_seasonal_005"], "ზაჰესი": ["hydro_seasonal_001"],
    "ჩითახევჰესი": ["hydro_seasonal_004"], "ხადორჰესი": ["hydro_seasonal_013"], "აწჰესი": ["hydro_seasonal_003"],
    "ლარსიჰესი": ["hydro_seasonal_014"], "ფარავანჰესი": ["hydro_seasonal_015"], "დარიალიჰესი": ["hydro_seasonal_016"],
    "ხელვაჩაურიჰესი 1": ["hydro_seasonal_017"], "შუახევიჰესი": ["hydro_seasonal_018"],
    "კირნათიჰესი": ["hydro_seasonal_020"], "ოლდ ენერჯიჰესი": ["hydro_seasonal_019"],
    "მესტიაჭალაჰესი 2": ["hydro_seasonal_021"], "მესტიაჭალაჰესი 1": ["hydro_seasonal_022"],
    "ხობი-2 ჰესი": ["hydro_seasonal_023"], "მტკვარიჰესი": ["hydro_seasonal_024"],
}
# Plants without a registry entry are folded into the small-HPP aggregate.
FOLD_INTO_SMALL = ["ჭიორა ჰესი"]
# Thermal: registry ids, PyPSA technology used for efficiency/VOM, fuel
THERMAL = {
    "tpp-gardabani-1": (["thermal_006"], "CCGT", "gas", "გარდაბნის თბოსადგური"),
    "tpp-gardabani-2": (["thermal_007"], "CCGT", "gas", "გარდაბნის თბოსადგური 2"),
    "tpp-mtkvari-energy": (["thermal_003"], "coal", "gas", "მტკვარი ენერჯი"),
    "tpp-tbilsresi": (["thermal_001", "thermal_002"], "coal", "gas", "თბილსრესი"),
    "tpp-g-power": (["thermal_004"], "OCGT", "gas", "ჯიფაუერი"),
    "tpp-tkibuli": (["thermal_005"], "coal", "coal", "ტყიბულის თბოსადგური"),
}
# Illustrative fuel prices already used by the repository's GSE 2026 demo input
# (samples/sample_input_gse_2026_168h.json). NOT a forecast; review required.
FUEL_PRICE_USD_PER_MMBTU = {"gas": 5.5, "coal": 4.0}
VOLL_USD_PER_MWH = 3000.0
# Plants added after the 2026 registry, per study year (user-stated).
ADDITIONS = {
    2027: [{"id": "wind-ruisi", "name": "რუისის ქარის ელექტროსადგური", "capacity_mw": 206.0,
            "note": "user-stated 2027 addition, 206 MW at Ruisi (Kareli, Shida Kartli). ASSUMPTIONS: in operation "
                    "all year; uses the Kartli wind capacity factor (nearest measured wind farm, Gori)"}],
}
YEARS = (2026, 2027)


def _sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _month_of(hour: int) -> int:
    return next(i for i, end in enumerate(MONTH_END) if hour < end)


def _balance(year: int) -> dict[str, list[float]]:
    document = json.loads((ROOT / f"data/actuals/georgia_balance_{year}.json").read_text(encoding="utf-8"))
    return {row["key"]: row["monthly_gwh"] for row in document["rows"]}


def _csv_profile(path: Path) -> list[float]:
    values = []
    for line in path.read_text(encoding="utf-8-sig").splitlines()[1:]:
        values.append(float(line.split(";")[1].replace(",", ".")))
    if len(values) != HOURS:
        raise ValueError(f"{path} has {len(values)} hours")
    return values


def mean_hydrology(balances: dict[int, dict[str, list[float]]], name: str,
                   years: tuple[int, ...] | None = None) -> list[float]:
    """Mean monthly GWh over the years in which the plant produced at all.

    ``years`` restricts the mean to those balance years (a single-year
    hydrology); a plant without generation in them (commissioned later)
    falls back to every year with data.
    """
    def produced(selection):
        return [balances[y][name] for y in selection if name in balances[y] and sum(balances[y][name]) > 0]
    series = produced(years or tuple(balances)) or produced(tuple(balances))
    if not series:
        raise ValueError(f"no generation data for {name}")
    return [sum(values) / len(series) for values in zip(*series)]


def build_project(year: int = 2026, hydrology_years: tuple[int, ...] = (2024, 2025)) -> dict:
    """``hydrology_years`` selects the balance years averaged for hydro output
    (default: mean of 2024 and 2025)."""
    if year not in YEARS:
        raise ValueError(f"supported study years: {YEARS}")
    if not hydrology_years or any(y not in (2024, 2025) for y in hydrology_years):
        raise ValueError("hydrology_years must be a non-empty subset of (2024, 2025)")
    load_path = ROOT / "data/gse_load_2026_2030.json"
    registry_path = ROOT / "samples/demo_asset_registry_2026.json"
    cost_path = ROOT / "data/cost_database_pypsa_eur.json"
    wind_path = ROOT / "data/renewables/Wind_Gori_2026_A_mean.csv"
    solar_path = ROOT / "data/renewables/Solar_Gori_2026_A_mean.csv"
    actual_paths = {y: ROOT / f"data/actuals/georgia_balance_{y}.json" for y in (2024, 2025)}

    registry = {a["asset_id"]: a for a in json.loads(registry_path.read_text(encoding="utf-8"))["assets"]}
    costs = json.loads(cost_path.read_text(encoding="utf-8"))["costs_by_year"]["2025"]
    balances = {y: _balance(y) for y in actual_paths}
    actual_sources = ", ".join(f"{_rel(p)} ({_sha(p)[:19]}...)" for p in actual_paths.values())

    def capacity(ids: list[str]) -> float:
        return round(sum(registry[i]["installed_capacity_mw"] for i in ids), 6)

    def provenance(source: str, version: str, **details) -> dict:
        return {"source": source, "source_version": version,
                "transformation": "scripts/build_georgia_2026_model.py", "details": details}

    hydro_prov = lambda name, ids, note: provenance(  # noqa: E731
        f"monthly balances 2024-2025 ({actual_sources}); capacity {_rel(registry_path)}",
        _sha(actual_paths[2025]), balance_row=name, registry_ids=ids, note=note)

    assets: list[dict] = []
    profiles: list[dict] = []

    # --- load and net interconnector flow -------------------------------------------
    load = json.loads(load_path.read_text(encoding="utf-8"))["load_mw_by_year"][str(year)]
    net_flow = balances[2025]["გადადინება (სალდო) მ/შ"]
    import_mw = [0.0] * HOURS
    demand = list(load)
    for hour in range(HOURS):
        month = _month_of(hour)
        mw = net_flow[month] * 1000 / MONTH_HOURS[month]
        if mw >= 0:
            import_mw[hour] = round(mw, 6)
        else:
            demand[hour] = round(demand[hour] - mw, 6)
    profiles.append({"id": "demand", "unit": "MW", "values": demand, "provenance": provenance(
        f"{_rel(load_path)} (GSE P50 {year}) + 2025 monthly net export", _sha(load_path),
        note=f"GSE P50 hourly load {year} plus the 2025 net export of each net-export month, flat within the month")})
    profiles.append({"id": "net-import-mw", "unit": "MW", "values": import_mw, "provenance": provenance(
        _rel(actual_paths[2025]), _sha(actual_paths[2025]), balance_row="გადადინება (სალდო) მ/შ",
        note="2025 monthly net import, flat within the month; 0 in net-export months")})
    assets.append({"id": "net-import", "kind": "import", "capacity_min_mw": 0,
                   "capacity_max_mw": round(max(import_mw) + 1, 3),
                   "legacy_extensions": {"name": "Net import (2025 monthly, fixed)", "vom": 0,
                                         "pmax_profile": "net-import-mw", "pmin_profile": "net-import-mw"},
                   "provenance": profiles[-1]["provenance"]})
    profiles.append({"id": "flat", "unit": "per-unit", "values": [1.0] * HOURS,
                     "provenance": provenance("constant", "1", note="always available")})

    # --- regulating HPPs: exact monthly energy -----------------------------------------
    for name, ids in REGULATING.items():
        energy = mean_hydrology(balances, name, hydrology_years)
        cap = capacity(ids)
        for month, gwh in enumerate(energy):
            if gwh * 1000 > cap * MONTH_HOURS[month] + 1e-6:
                raise ValueError(f"{name}: month {month + 1} energy exceeds capacity")
        assets.append({"id": f"reg-{ids[0].replace('_', '-')}", "kind": "hydro_ror", "capacity_min_mw": 0,
                       "capacity_max_mw": cap, "profile_references": ["flat"],
                       "legacy_extensions": {"name": f"{name} (regulating)", "committable": False, "vom": 0,
                                             "monthly_energy_mwh": [round(g * 1000, 3) for g in energy],
                                             "monthly_energy_mode": "exact"},
                       "provenance": hydro_prov(name, ids, "mean 2024-2025 monthly energy as an exact budget")})

    # --- seasonal HPPs and small-HPP aggregate: flat monthly availability ----------------
    def flat_monthly(asset_id: str, label: str, energy: list[float], cap: float, ids: list[str], note: str) -> None:
        cf = []
        for hour in range(HOURS):
            month = _month_of(hour)
            cf.append(round(min(1.0, energy[month] * 1000 / MONTH_HOURS[month] / cap), 9))
        profiles.append({"id": f"cf-{asset_id}", "unit": "per-unit", "values": cf,
                         "provenance": hydro_prov(label, ids, note)})
        assets.append({"id": asset_id, "kind": "hydro_ror", "capacity_min_mw": 0, "capacity_max_mw": cap,
                       "profile_references": [f"cf-{asset_id}"],
                       "legacy_extensions": {"name": label, "committable": False, "vom": 0},
                       "provenance": hydro_prov(label, ids, note)})

    for name, ids in SEASONAL.items():
        flat_monthly(f"ror-{ids[0].replace('_', '-')}", name, mean_hydrology(balances, name, hydrology_years), capacity(ids), ids,
                     "mean monthly energy of the years with generation, flat within the month, spill allowed")
    small = mean_hydrology(balances, "მცირე ჰესები", hydrology_years)
    for name in FOLD_INTO_SMALL:
        small = [a + b for a, b in zip(small, mean_hydrology(balances, name, hydrology_years))]
    small_ids = [i for i, a in registry.items() if a["source_category"] == "Hydro - Small"]
    flat_monthly("ror-small-hpp", "მცირე ჰესები (aggregate)", small, capacity(small_ids), small_ids,
                 "small-HPP row (mean 2024-2025) plus plants missing from the registry: " + ", ".join(FOLD_INTO_SMALL))

    # --- wind and solar -------------------------------------------------------------------
    wind_shape = _csv_profile(wind_path)
    kartli = mean_hydrology(balances, "ქართლის ქარის ელ.სადგური")
    kartli_cap = capacity(["wind_001"])
    wind_cf = []
    for hour in range(HOURS):
        month = _month_of(hour)
        start = MONTH_END[month] - MONTH_HOURS[month]
        shape_sum = math.fsum(wind_shape[start:MONTH_END[month]])
        scale = kartli[month] * 1000 / kartli_cap / shape_sum if shape_sum > 0 else 0.0
        wind_cf.append(round(min(1.0, wind_shape[hour] * scale), 9))
    profiles.append({"id": "cf-wind-kartli", "unit": "per-unit", "values": wind_cf, "provenance": provenance(
        f"{_rel(wind_path)} shape scaled to Kartli wind monthly energy 2024-2025", _sha(wind_path),
        balance_row="ქართლის ქარის ელ.სადგური",
        note="hourly shape of the repository's Gori 2026 wind profile, scaled per month to the mean Kartli output")})
    for wind_id, note in (("wind_001", "Kartli wind, monthly energy = mean 2024-2025"),
                          ("wind_002", "ASSUMPTION: new in 2026, no data - uses the Kartli capacity factor")):
        assets.append({"id": f"wind-{wind_id.replace('_', '-')}", "kind": "wind", "capacity_min_mw": 0,
                       "capacity_max_mw": capacity([wind_id]), "profile_references": ["cf-wind-kartli"],
                       "legacy_extensions": {"name": registry[wind_id]["name_ge"], "committable": False, "vom": 0},
                       "provenance": provenance(_rel(registry_path), _sha(registry_path), registry_ids=[wind_id],
                                                note=note, **({"balance_row": "ქართლის ქარის ელ.სადგური"}
                                                              if wind_id == "wind_001" else {}))})
    solar_ids = [i for i, a in registry.items() if a["source_category"] == "Solar"]
    profiles.append({"id": "cf-solar-gori", "unit": "per-unit",
                     "values": [round(min(1.0, max(0.0, v)), 9) for v in _csv_profile(solar_path)],
                     "provenance": provenance(_rel(solar_path), _sha(solar_path),
                                              note="repository 2026 Gori solar profile (2025 solar output is not a full year)")})
    assets.append({"id": "solar-aggregate", "kind": "solar", "capacity_min_mw": 0,
                   "capacity_max_mw": capacity(solar_ids), "profile_references": ["cf-solar-gori"],
                   "legacy_extensions": {"name": "მზის სადგურები (aggregate)", "committable": False, "vom": 0},
                   "provenance": provenance(_rel(registry_path), _sha(registry_path), registry_ids=solar_ids,
                                            note="six 2025 solar plants aggregated")})

    for extra in ADDITIONS.get(year, []):
        assets.append({"id": extra["id"], "kind": "wind", "capacity_min_mw": 0, "capacity_max_mw": extra["capacity_mw"],
                       "profile_references": ["cf-wind-kartli"],
                       "legacy_extensions": {"name": extra["name"], "committable": False, "vom": 0},
                       "provenance": provenance("user statement (study-year addition)", f"additions-{year}",
                                                note=extra["note"])})

    # --- thermal units -------------------------------------------------------------------
    for asset_id, (ids, technology, fuel, label) in THERMAL.items():
        efficiency = costs[technology]["efficiency"]["value"]
        vom = costs[technology]["VOM"]["value"]
        heat_rate = round(MMBTU_PER_MWH / efficiency, 4)
        cap = capacity(ids)
        assets.append({"id": asset_id, "kind": "thermal", "capacity_min_mw": 0, "capacity_max_mw": cap,
                       "legacy_extensions": {"name": label, "type": "thermal", "pmin": 0, "pmax": cap,
                                             "committable": False, "fuel_type": fuel, "heat_rate": heat_rate,
                                             "fuel_price": FUEL_PRICE_USD_PER_MMBTU[fuel], "vom": round(vom, 4),
                                             "ramp_up": cap, "ramp_down": cap,
                                             "review_required": True},
                       "provenance": provenance(
                           f"capacity {_rel(registry_path)}; efficiency/VOM {_rel(cost_path)} ({technology})",
                           _sha(registry_path), registry_ids=ids, balance_row=label,
                           note="REVIEW REQUIRED: generic PyPSA efficiency/VOM (EUR values used as USD), "
                                "illustrative fuel price from the GSE 2026 demo input; full availability, no outages")})

    return {
        "id": f"georgia-{year}-balance",
        "version": {"revision": 1},
        "workflow": "deterministic_uc",
        "metadata": {
            "name": f"Georgia {year} (unit-level, 2024-2025 balance hydrology)",
            "classification": "planning study model; thermal costs are illustrative placeholders that require review",
            "mapping_document": "docs/GEORGIA_2026_MODEL.md",
            "inputs": [_rel(load_path), _rel(registry_path), *(_rel(p) for p in actual_paths.values()),
                       _rel(cost_path), _rel(wind_path), _rel(solar_path)],
            "limitations": [
                "Copper-plate (single node): no transmission limits inside Georgia or on interconnectors.",
                "Hydrology is the mean of 2024 and 2025; regulating HPPs have exact monthly energy without a reservoir model.",
                "Net interconnector flow is fixed to 2025 monthly values; the optimiser cannot import more.",
                "Thermal efficiency, VOM and fuel prices are generic/illustrative; units are always available.",
                "Not an official GSE forecast or operational dispatch; not a policy-grade adequacy result.",
            ],
        },
        "units": {"currency": "USD"},
        "time": {"timezone": "Asia/Tbilisi", "study_year": year, "resolution_minutes": 60,
                 "interval_duration_hours": 1, "start": f"{year}-01-01T00:00:00+04:00", "periods": HOURS,
                 "calendar_policy": "non_leap"},
        "assets": assets,
        "profiles": profiles,
        "solver_settings": {"solver": "highs", "rolling_window_h": HOURS, "rolling_step_h": HOURS,
                            "time_limit_s": 900, "mip_gap": 0.001, "unserved_penalty": VOLL_USD_PER_MWH},
        "scenarios": [],
        "legacy_payload": {"resolution_min": 60, "study_horizon": {"start_hour": 0, "horizon_hours": HOURS}},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--year", type=int, default=2026, choices=YEARS)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    out = args.out or ROOT / f"samples/projects/georgia_{args.year}_balance.json"
    project = build_project(args.year)
    out.write_text(json.dumps(project, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"{len(project['assets'])} assets, {len(project['profiles'])} profiles -> {out}")


if __name__ == "__main__":
    main()
