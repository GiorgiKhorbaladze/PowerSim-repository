"""Georgia 2027-2032 outlook: dispatch, reserve, adequacy and RICE / CCGT-3 timing.

Usage::

    python scripts/georgia_outlook.py --out data/outlook/georgia_outlook_2027_2032.json

Builds one PowerSim project per year / scenario / sensitivity from the 2026
unit-level model (``scripts/build_georgia_2026_model.py``), solves each with
the PowerSim application executor (8,760-hour HiGHS LP, canonical QA and
publication gate), and adds two analyses the generic workflows cannot do
for a hydro-dominated, energy-limited system:

* **Adequacy (LOLE / EENS)** - sequential Monte Carlo with two-state Markov
  thermal outages, monthly energy-limited regulating hydro and a daily-cycling
  battery (``adequacy``). PowerSim's generic chronological-adequacy workflow
  treats hydro as always available at full capacity, which would hide the
  winter energy constraint, so it is not used here.
* **Reserve** - requirement = largest single unit + P99 of the hourly change in
  wind + solar output; available upward headroom per hour from the dispatch.

The expansion logic (``plan_expansion``) adds firm capacity year by year when
LOLE exceeds the target or the reserve requirement cannot be met, and picks the
cheaper of "RICE blocks only" and "CCGT-3 + RICE blocks" by annualised capital
+ fixed O&M + dispatch cost. Every assumption is listed in ``ASSUMPTIONS`` and
in ``docs/GEORGIA_OUTLOOK_2027_2032.md``.
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import math
import tempfile
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
YEARS = (2027, 2028, 2029, 2030, 2031, 2032)
SCENARIOS = ("A", "B")
HOURS = 8760
MONTH_HOURS = [744, 672, 744, 720, 744, 720, 744, 744, 720, 744, 720, 744]
MONTH_END = [sum(MONTH_HOURS[: i + 1]) for i in range(12)]
MONTH_OF = np.repeat(np.arange(12), MONTH_HOURS)
MMBTU_PER_MWH = 3.412142

GSE_YEARS = (2026, 2027, 2028, 2029, 2030)
BESS = {"from_year": 2029, "power_mw": 200.0, "energy_mwh": 200.0, "eta": 0.92, "soc_min": 0.1}
RETIRED_FROM_2027 = ["tpp-tbilsresi"]  # Tbilsresi III + IV (thermal_002 + thermal_001), 272 MW
NOT_OPERATING = ["tpp-tkibuli"]  # no output in the 2024 and 2025 balances
EARLIEST_YEAR = {"rice": 2028, "ccgt3": 2029}  # build lead times from 2026
LOLE_TARGET_H = 3.0
STRESS_HYDROLOGY = "2024"  # lowest winter regulating-hydro energy of the two balance years
DISCOUNT_RATE = 0.08
CANDIDATES = {
    # PyPSA technology-data 2030 CCGT (data/cost_database_pypsa_eur.json); EUR used as USD
    "ccgt3": {"id": "new-ccgt-3", "name": "CCGT-3 (candidate)", "block_mw": 250.0, "efficiency": 0.58,
              "capex_usd_per_kw": 1108.7166, "fom_usd_per_kw_yr": 1108.7166 * 0.033494, "vom": 5.6104,
              "life_yrs": 25, "for": 0.04, "mttr_h": 40.0},
    # ASSUMPTION (not in the repository's cost database): gas-engine plant, review required
    "rice": {"id": "new-rice", "name": "RICE gas engines (candidate)", "block_mw": 20.0, "efficiency": 0.46,
             "capex_usd_per_kw": 850.0, "fom_usd_per_kw_yr": 15.0, "vom": 7.0, "life_yrs": 25,
             "for": 0.02, "mttr_h": 24.0},
}
# ASSUMPTION: generic forced-outage rates / mean time to repair, review required
OUTAGES = {"tpp-gardabani-1": (0.04, 40.0), "tpp-gardabani-2": (0.04, 40.0), "tpp-mtkvari-energy": (0.10, 72.0),
           "tpp-tbilsresi": (0.10, 72.0), "tpp-g-power": (0.05, 30.0), "tpp-tkibuli": (0.15, 72.0)}
LARGEST_HYDRO_UNIT_MW = 260.0  # Enguri unit (5 x 260 MW, registry units_text)
ASSUMPTIONS = [
    "Load: GSE P50 hourly projection 2027-2030; 2031-2032 = 2030 profile x 3.69 %/yr (mean GSE growth 2026-2030).",
    "Net interconnector flow fixed to the 2025 monthly balance (import as supply, export as load) in dispatch; "
    "adequacy uses domestic load only (exports are interruptible) and the fixed import.",
    "Tbilsresi III + IV (272 MW) retired from 2027; Tkibuli (13 MW, no output in 2024-2025) excluded; other units stay.",
    "Lead times: RICE available from 2028, CCGT-3 from 2029 at the earliest.",
    "Battery 200 MW / 200 MWh from 2029 (enters at the end of 2028), 92 % per-way efficiency, 10 % minimum SoC.",
    "Ruisi wind 206 MW from 2027 with the Kartli capacity factor (43 %).",
    "Scenario A: no other new plants. Scenario B: linear build-out to the PLEXOS 2030 wind (1,909 MW) and solar "
    "(1,496 MW) fleet and its additional run-of-river energy (~2,013 GWh) by 2030, constant after 2030.",
    "Hydrology: mean of 2024 and 2025 (base); 2024 and 2025 alone as sensitivities. 2025 has less hydro energy in the year, "
    "2024 less regulating-hydro energy in winter (995 vs 1,213 GWh in Jan/Feb/Nov/Dec), so 2024 is the adequacy stress year.",
    "Leap years 2028 and 2032 are modelled with 8,760 hours (29 February omitted, as in the GSE projections).",
    "Thermal efficiency/VOM: PyPSA generic; gas 5.5 USD/MMBtu, coal 4.0 USD/MMBtu (illustrative, review required).",
    "Candidates: CCGT-3 250 MW (PyPSA 2030 CCGT costs) and RICE in 20 MW blocks (assumed 850 USD/kW, 46 %); "
    "8 % discount rate, 25-year life.",
    "Forced outages: generic two-state Markov rates (CCGT 4 %, old steam 10 %, OCGT 5 %, coal 15 %, RICE 2 %); "
    "no planned maintenance; hydro units not outaged (energy is their binding limit).",
    "Adequacy target LOLE <= 3 h/year; hydrology sampled 50/50 from 2024 and 2025.",
    "Reserve requirement = largest unit + P99 of the unpredictable hourly wind/solar change (Gori profiles, "
    "single-site, conservative; PLEXOS ramps are implausible); headroom = unused thermal "
    "and regulating-hydro capacity in the dispatch (battery not counted).",
    "Single node: no transmission limits, no N-1 network security, no AC power flow.",
]


def _module(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BUILDER = _module("build_georgia_2026_model")
_LOAD = json.loads((ROOT / "data/gse_load_2026_2030.json").read_text(encoding="utf-8"))
GROWTH = (_LOAD["meta"]["target_gwh_per_year"]["2030"] / _LOAD["meta"]["target_gwh_per_year"]["2026"]) ** 0.25 - 1


def domestic_load(year: int) -> np.ndarray:
    if year in GSE_YEARS:
        return np.asarray(_LOAD["load_mw_by_year"][str(year)], dtype=float)
    return np.asarray(_LOAD["load_mw_by_year"]["2030"], dtype=float) * (1 + GROWTH) ** (year - 2030)


def net_export_mw() -> np.ndarray:
    """2025 monthly net export (positive MW), flat within the month, 0 in net-import months."""
    net = BUILDER._balance(2025)["გადადინება (სალდო) მ/შ"]
    return np.asarray([max(0.0, -net[m]) * 1000 / MONTH_HOURS[m] for m in MONTH_OF], dtype=float)


def buildout_factor(year: int, scenario: str) -> float:
    return 0.0 if scenario == "A" else min(1.0, (year - 2026) / 4)


def _thermal_asset(candidate: dict, capacity: float) -> dict:
    heat_rate = round(MMBTU_PER_MWH / candidate["efficiency"], 4)
    return {"id": candidate["id"], "kind": "thermal", "capacity_min_mw": 0, "capacity_max_mw": capacity,
            "legacy_extensions": {"name": candidate["name"], "type": "thermal", "pmin": 0, "pmax": capacity,
                                  "committable": False, "fuel_type": "gas", "heat_rate": heat_rate,
                                  "fuel_price": BUILDER.FUEL_PRICE_USD_PER_MMBTU["gas"], "vom": candidate["vom"],
                                  "ramp_up": capacity, "ramp_down": capacity, "review_required": True},
            "provenance": {"source": "scripts/georgia_outlook.py CANDIDATES", "source_version": "outlook-v1",
                           "transformation": "scripts/georgia_outlook.py",
                           "details": {"note": "expansion candidate; costs are assumptions that require review"}}}


def _plexos() -> dict:
    return json.loads((ROOT / "samples/projects/georgia_2030_plexos.json").read_text(encoding="utf-8"))


def build_case(year: int, scenario: str = "A", hydrology: str = "mean", demand_factor: float = 1.0,
               rice_mw: float = 0.0, ccgt3: bool = False, retire_tbilsresi: bool = True,
               vre: str = "average") -> dict:
    """``vre='calm-low'`` swaps the scenario-B PLEXOS wind/solar profiles for PLEXOS's own calm-wind
    and low-solar profiles (the PLEXOS 'stress' weather)."""
    if year not in YEARS or scenario not in SCENARIOS:
        raise ValueError(f"year must be in {YEARS} and scenario in {SCENARIOS}")
    years = {"mean": (2024, 2025), "2024": (2024,), "2025": (2025,)}[hydrology]
    project = BUILDER.build_project(2027, hydrology_years=years)  # 2026 fleet + Ruisi
    profiles = {p["id"]: p for p in project["profiles"]}
    demand = domestic_load(year) * demand_factor + net_export_mw()
    profiles["demand"]["values"] = [round(float(v), 6) for v in demand]
    profiles["demand"]["provenance"]["details"]["note"] = (
        f"domestic load {year} (GSE P50{' extrapolated' if year not in GSE_YEARS else ''}) x {demand_factor} "
        "plus the 2025 net export of each net-export month")
    assets = [a for a in project["assets"] if a["id"] not in NOT_OPERATING
              and not (retire_tbilsresi and a["id"] in RETIRED_FROM_2027)]
    if year >= BESS["from_year"]:
        assets.append({"id": "bess-200", "kind": "bess", "capacity_min_mw": 0, "capacity_max_mw": BESS["power_mw"],
                       "legacy_extensions": {"name": "Battery 200 MW / 200 MWh", "power_mw": BESS["power_mw"],
                                             "energy_mwh": BESS["energy_mwh"], "eta_charge": BESS["eta"],
                                             "eta_discharge": BESS["eta"], "soc_init": 0.5,
                                             "soc_min": BESS["soc_min"], "soc_max": 1.0, "vom_discharge": 0.5},
                       "provenance": {"source": "user statement", "source_version": "outlook-v1",
                                      "transformation": "scripts/georgia_outlook.py",
                                      "details": {"note": "200 MW / 200 MWh, enters at the end of 2028"}}})
    factor = buildout_factor(year, scenario)
    if factor > 0:
        plexos = _plexos()
        pprof = {p["id"]: p for p in plexos["profiles"]}
        existing = {"wind": sum(a["capacity_max_mw"] for a in assets if a["kind"] == "wind"),
                    "solar": sum(a["capacity_max_mw"] for a in assets if a["kind"] == "solar")}
        for kind in ("wind", "solar"):
            fleet = [a for a in plexos["assets"] if a["kind"] == kind]
            total = sum(a["capacity_max_mw"] for a in fleet)
            scale = factor * max(0.0, total - existing[kind]) / total
            for asset in fleet:
                ref = asset["profile_references"][0]
                if vre == "calm-low":
                    ref = ref.replace("-average", "-calm" if kind == "wind" else "-low")
                if ref not in profiles:
                    profiles[ref] = copy.deepcopy(pprof[ref])
                    profiles[ref]["values"] = [min(1.0, max(0.0, v)) for v in profiles[ref]["values"]]
                assets.append({**copy.deepcopy(asset), "id": f"new-{asset['id']}", "profile_references": [ref],
                               "capacity_max_mw": round(asset["capacity_max_mw"] * scale, 4),
                               "provenance": {**asset["provenance"], "details": {
                                   **asset["provenance"]["details"],
                                   "note": f"scenario B build-out {factor:.2f} of the PLEXOS 2030 {kind} additions"}}})
        energy = [e * factor for e in _additional_ror_mwh(plexos, project)]
        cap = 330.0
        cf = [round(min(1.0, energy[m] / MONTH_HOURS[m] / cap), 9) for m in MONTH_OF]
        profiles["cf-new-ror-plexos"] = {"id": "cf-new-ror-plexos", "unit": "per-unit", "values": cf,
                                         "provenance": {"source": "PLEXOS 2030 run-of-river minus 2026 model",
                                                        "source_version": plexos["metadata"]["source_sha256"],
                                                        "transformation": "scripts/georgia_outlook.py",
                                                        "details": {"note": "monthly energy difference, flat in month"}}}
        assets.append({"id": "new-ror-plexos", "kind": "hydro_ror", "capacity_min_mw": 0, "capacity_max_mw": cap,
                       "profile_references": ["cf-new-ror-plexos"],
                       "legacy_extensions": {"name": "New run-of-river (PLEXOS 2030 additions)", "committable": False,
                                             "vom": 0},
                       "provenance": profiles["cf-new-ror-plexos"]["provenance"]})
    if ccgt3:
        assets.append(_thermal_asset(CANDIDATES["ccgt3"], CANDIDATES["ccgt3"]["block_mw"]))
    if rice_mw > 0:
        assets.append(_thermal_asset(CANDIDATES["rice"], float(rice_mw)))
    project["assets"] = assets
    project["profiles"] = list(profiles.values())
    tag = f"{year}-{scenario}-{hydrology}-d{round(demand_factor * 100)}"
    project["id"] = f"georgia-outlook-{tag}".lower()
    project["metadata"] = {**project["metadata"], "name": f"Georgia outlook {tag}",
                           "classification": "planning outlook; placeholder costs and outage rates require review",
                           "mapping_document": "docs/GEORGIA_OUTLOOK_2027_2032.md",
                           "outlook": {"year": year, "scenario": scenario, "hydrology": hydrology,
                                       "demand_factor": demand_factor, "rice_mw": rice_mw, "ccgt3": ccgt3,
                                       "retire_tbilsresi": retire_tbilsresi}}
    leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    # GSE hourly projections have 8,760 values in every year; leap years drop 29 February
    project["time"] = {**project["time"], "study_year": year, "start": f"{year}-01-01T00:00:00+04:00",
                       "calendar_policy": "explicit_periods" if leap else "non_leap"}
    return project


def _additional_ror_mwh(plexos: dict, project: dict) -> list[float]:
    """Monthly PLEXOS 2030 run-of-river energy minus the 2026 model's seasonal/small HPPs (>= 0)."""
    pprof = {p["id"]: p["values"] for p in plexos["profiles"]}
    prof = {p["id"]: p["values"] for p in project["profiles"]}
    plexos_h = np.zeros(HOURS)
    for asset in plexos["assets"]:
        if asset["kind"] == "hydro_ror" and asset["id"] != "regulating-hpp":
            plexos_h += np.clip(pprof[asset["profile_references"][0]], 0, 1) * asset["capacity_max_mw"]
    current_h = np.zeros(HOURS)
    for asset in project["assets"]:
        if asset["id"].startswith("ror-"):
            current_h += np.asarray(prof[asset["profile_references"][0]]) * asset["capacity_max_mw"]
    return [max(0.0, float(plexos_h[MONTH_OF == m].sum() - current_h[MONTH_OF == m].sum())) for m in range(12)]


# ---------------------------------------------------------------- adequacy ---------------------------------------
def components(project: dict) -> dict:
    """Adequacy inputs: domestic load, fixed supply, regulating hydro, thermal units, battery."""
    prof = {p["id"]: np.asarray(p["values"], dtype=float) for p in project["profiles"]}
    fixed = np.zeros(HOURS)
    reg_energy = np.zeros(12)
    reg_cap = 0.0
    units, bess = [], None
    for asset in project["assets"]:
        kind, aid = asset["kind"], asset["id"]
        ext = asset.get("legacy_extensions", {})
        if aid.startswith("reg-"):
            reg_energy += np.asarray(ext["monthly_energy_mwh"])
            reg_cap += asset["capacity_max_mw"]
        elif kind in ("hydro_ror", "wind", "solar"):
            fixed += prof[asset["profile_references"][0]] * asset["capacity_max_mw"]
        elif kind == "import":
            fixed += prof[ext["pmax_profile"]]
        elif kind == "thermal":
            rate, mttr = OUTAGES.get(aid) or next((c["for"], c["mttr_h"]) for c in CANDIDATES.values() if c["id"] == aid)
            units.append((aid, asset["capacity_max_mw"], rate, mttr))
        elif kind == "bess":
            bess = (ext["power_mw"], ext["energy_mwh"], ext["eta_discharge"] * ext["eta_charge"])
    load = prof["demand"] - net_export_mw()
    return {"load": load, "fixed": fixed, "reg_energy": reg_energy, "reg_cap": reg_cap, "units": units, "bess": bess}


def markov_availability(capacity: float, rate: float, mttr: float, samples: int, rng) -> np.ndarray:
    """Hourly available MW (samples x HOURS) of a two-state unit with forced-outage rate ``rate``."""
    if rate <= 0:
        return np.full((samples, HOURS), capacity)
    mu = 1.0 / mttr                       # repair probability per hour
    lam = rate * mu / (1.0 - rate)        # failure probability per hour (steady state = rate)
    up = rng.random(samples) >= rate
    out = np.empty((samples, HOURS))
    draws = rng.random((HOURS, samples))
    for t in range(HOURS):
        up = np.where(up, draws[t] >= lam, draws[t] < mu)
        out[:, t] = up
    return out * capacity


def hydro_dispatch_shortfall(residual: np.ndarray, energy: float, cap: float) -> tuple[np.ndarray, np.ndarray]:
    """Unserved MW per hour and a 'loss-of-load' flag after optimal use of energy-limited hydro.

    ``residual`` (samples x hours of one month) is load minus all other available supply.
    Hydro output per hour is at most ``cap`` and the residual; total at most ``energy``.
    Minimum EENS = sum(residual+) - min(energy, sum(min(cap, residual+))); LOLE is minimised by
    covering the smallest positive residuals that fit under ``cap`` first.
    """
    pos = np.clip(residual, 0.0, None)
    coverable = np.minimum(pos, cap)
    unserved = pos - coverable                      # hours above cap stay short by (residual - cap)
    lol = pos > cap + 1e-9
    total = coverable.sum(axis=1)
    short = total > energy + 1e-9                   # energy-limited samples
    for s in np.nonzero(short)[0]:
        need = coverable[s]
        order = np.argsort(np.where(need > 0, need, np.inf))
        cum = np.cumsum(need[order])
        covered = cum <= energy + 1e-9
        rest = energy - (cum[covered][-1] if covered.any() else 0.0)
        u = need.copy()
        u[order[covered]] = 0.0
        partial = order[~covered]
        partial = partial[need[partial] > 0]
        if partial.size:                            # spend the remainder on the largest uncovered hour
            target = partial[-1] if lol[s, partial[-1]] else partial[0]
            u[target] = max(0.0, u[target] - rest)
        unserved[s] += u
        lol[s] |= u > 1e-9
    return unserved, lol


def adequacy(projects: list[dict], samples: int = 200, seed: int = 20270101) -> dict:
    """LOLE (h/yr) and EENS (MWh/yr): samples split evenly over the given hydrology projects."""
    rng = np.random.default_rng(seed)
    per = [samples // len(projects) + (1 if i < samples % len(projects) else 0) for i in range(len(projects))]
    lole, eens, peak_short = [], [], []
    for project, count in zip(projects, per):
        comp = components(project)
        thermal = sum(markov_availability(c, r, m, count, rng) for _, c, r, m in comp["units"])
        residual = comp["load"][None, :] - comp["fixed"][None, :] - thermal
        unserved = np.zeros_like(residual)
        lol = np.zeros(residual.shape, dtype=bool)
        for month in range(12):
            cols = MONTH_OF == month
            u, flag = hydro_dispatch_shortfall(residual[:, cols], comp["reg_energy"][month], comp["reg_cap"])
            unserved[:, cols], lol[:, cols] = u, flag
        if comp["bess"]:
            power, energy, eta = comp["bess"]
            surplus = np.clip(-residual, 0.0, None)
            for day in range(HOURS // 24):
                cols = slice(day * 24, day * 24 + 24)
                stored = np.minimum(energy * (1 - BESS["soc_min"]), np.minimum(surplus[:, cols], power).sum(axis=1)) * eta
                block = np.minimum(unserved[:, cols], power)
                order = np.argsort(block, axis=1)
                sorted_block = np.take_along_axis(block, order, axis=1)
                cum = np.cumsum(sorted_block, axis=1)
                take = np.clip(np.minimum(cum, stored[:, None]) - (cum - sorted_block), 0.0, None)
                relief = np.zeros_like(block)
                np.put_along_axis(relief, order, take, axis=1)
                unserved[:, cols] -= relief
                lol[:, cols] &= unserved[:, cols] > 1e-9
        lole.extend(lol.sum(axis=1).tolist())
        eens.extend(unserved.sum(axis=1).tolist())
        peak_short.extend(unserved.max(axis=1).tolist())
    lole_a, eens_a = np.asarray(lole), np.asarray(eens)
    return {"LOLE_h": float(lole_a.mean()), "EENS_mwh": float(eens_a.mean()), "samples": int(len(lole_a)),
            "LOLE_p90_h": float(np.percentile(lole_a, 90)), "max_unserved_mw_p90": float(np.percentile(peak_short, 90)),
            "LOLE_stderr_h": float(lole_a.std(ddof=1) / math.sqrt(len(lole_a))) if len(lole_a) > 1 else 0.0}


# ---------------------------------------------------------------- dispatch + reserve -----------------------------
def _csv(name: str) -> np.ndarray:
    lines = (ROOT / "data/renewables" / name).read_text(encoding="utf-8-sig").splitlines()[1:]
    return np.asarray([float(line.split(";")[1].replace(",", ".")) for line in lines], dtype=float)


def _unit_balancing_errors() -> tuple[np.ndarray, np.ndarray]:
    """Per-MW hourly forecast-error proxies from the repository's Gori 2026 profiles.

    Wind: hourly change (persistence error). Solar: hourly change minus the mean change for
    the same hour of day and month (the sunrise/sunset ramp is predictable). The PLEXOS
    2030 profiles are not used here: their hourly changes (P99 68-90 % of capacity per
    region) are not physically plausible ramp statistics, although their energy is.
    """
    wind, solar = _csv("Wind_Gori_2026_A_mean.csv"), np.clip(_csv("Solar_Gori_2026_A_mean.csv"), 0, 1)
    d_solar = np.diff(solar, prepend=solar[0])
    hour = np.arange(HOURS) % 24
    expected = np.zeros(HOURS)
    for m in range(12):
        for h in range(24):
            mask = (MONTH_OF == m) & (hour == h)
            expected[mask] = d_solar[mask].mean()
    return np.diff(wind, prepend=wind[0]), d_solar - expected


WIND_ERROR, SOLAR_ERROR = _unit_balancing_errors()


def reserve_requirement(project: dict) -> dict:
    """Largest unit + P99 of the unpredictable hourly wind/solar change (single-site, conservative)."""
    capacity = {"wind": 0.0, "solar": 0.0}
    largest = LARGEST_HYDRO_UNIT_MW
    for asset in project["assets"]:
        if asset["kind"] in capacity:
            capacity[asset["kind"]] += asset["capacity_max_mw"]
        if asset["kind"] == "thermal":
            largest = max(largest, asset["capacity_max_mw"] if asset["id"] != "new-rice" else CANDIDATES["rice"]["block_mw"])
    error = WIND_ERROR * capacity["wind"] + SOLAR_ERROR * capacity["solar"]
    balancing = float(np.percentile(np.abs(error), 99))
    return {"largest_unit_mw": largest, "vre_balancing_mw": round(balancing, 1),
            "requirement_mw": round(largest + balancing, 1),
            "wind_mw": round(capacity["wind"], 1), "solar_mw": round(capacity["solar"], 1)}


def solve(project: dict) -> dict:
    from powersim.application import ApplicationService
    from powersim.contracts import ProjectContract
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager

    with tempfile.TemporaryDirectory() as workspace:
        manager = RunManager(Path(workspace))
        manager.save_project(ProjectContract.model_validate(project))
        service = ApplicationService(manager, RegisteredLocalWorkflowExecutor(), asynchronous=False)
        run = service.create_run(project["id"])
        started = time.monotonic()
        service.launch(run["id"])
        status = service.run_status(run["id"])
        if status["status"] != "completed":
            raise RuntimeError(f"{project['id']}: {status['events'][-1]}")
        envelope = service.result(run["id"])
    results = envelope["results"]
    assets = {a["id"]: a for a in project["assets"]}
    monthly, headroom = {}, np.zeros(HOURS)
    thermal_out = np.zeros(HOURS)
    for asset_id, rows in results["hourly_by_unit"].items():
        dispatch = np.asarray([r["dispatch_mw"] for r in rows], dtype=float)
        monthly[asset_id] = [round(float(dispatch[MONTH_OF == m].sum()) / 1000, 3) for m in range(12)]
        asset = assets[asset_id]
        if asset["kind"] == "thermal" or asset_id.startswith("reg-"):
            headroom += asset["capacity_max_mw"] - dispatch
        if asset["kind"] == "thermal":
            thermal_out += dispatch
    thermal_cap = sum(a["capacity_max_mw"] for a in project["assets"] if a["kind"] == "thermal")
    requirement = reserve_requirement(project)
    short = np.clip(requirement["requirement_mw"] - headroom, 0, None)
    summary = results["system_summary"]
    return {"project_id": project["id"], "qa": envelope["qa"]["status"],
            "publishable": results["publication"]["publishable"],
            "runtime_s": round(time.monotonic() - started, 1),
            "objective_usd": summary["total_objective_cost_usd"], "fuel_cost_usd": summary["total_fuel_cost"],
            "served_gwh": round(summary["total_energy_mwh"] / 1000, 2),
            "unserved_gwh": round(summary["total_unserved_mwh"] / 1000, 3),
            "curtailed_gwh": round(summary["total_curtailed_mwh"] / 1000, 3),
            "gas_mm3": summary.get("total_gas_mm3"), "peak_load_mw": summary["peak_load_mw"],
            "monthly_gwh": monthly,
            "hours_thermal_full": int((thermal_out >= 0.999 * thermal_cap).sum()),
            "reserve": {**requirement, "min_headroom_mw": round(float(headroom.min()), 1),
                        "hours_short": int((short > 1e-6).sum()), "max_short_mw": round(float(short.max()), 1),
                        "p99_short_mw": round(float(np.percentile(short, 99.9)), 1)}}


def annualised_cost(rice_mw: float, ccgt3: bool) -> float:
    total = 0.0
    for key, mw in (("rice", rice_mw), ("ccgt3", CANDIDATES["ccgt3"]["block_mw"] if ccgt3 else 0.0)):
        c = CANDIDATES[key]
        crf = DISCOUNT_RATE / (1 - (1 + DISCOUNT_RATE) ** -c["life_yrs"])
        total += mw * 1000 * (c["capex_usd_per_kw"] * crf + c["fom_usd_per_kw_yr"])
    return total


def _rice_for_target(year, scenario, ccgt3, rice_start, samples, log) -> tuple[float, dict]:
    block = CANDIDATES["rice"]["block_mw"]
    rice = rice_start
    hydro = [build_case(year, scenario, h, rice_mw=0, ccgt3=ccgt3) for h in ("2024", "2025")]

    def evaluate(mw):
        projects = []
        for p in hydro:
            q = copy.deepcopy(p)
            if mw > 0:
                q["assets"].append(_thermal_asset(CANDIDATES["rice"], mw))
            projects.append(q)
        return adequacy(projects, samples)

    result = evaluate(rice)
    while result["LOLE_h"] > LOLE_TARGET_H:
        step = max(block, block * math.ceil(result["max_unserved_mw_p90"] / block / 4))
        rice += step
        result = evaluate(rice)
        log(f"    {year}{scenario} ccgt3={ccgt3} rice={rice:.0f} LOLE={result['LOLE_h']:.2f}")
    # step back in single blocks while the target still holds
    while rice - block >= rice_start:
        trial = evaluate(rice - block)
        if trial["LOLE_h"] > LOLE_TARGET_H:
            break
        rice, result = rice - block, trial
    return rice, result


def plan_expansion(scenario: str, samples: int = 200, log=print) -> list[dict]:
    """Year-by-year cumulative builds (RICE MW, CCGT-3) meeting LOLE and reserve at least cost."""
    rice, ccgt3 = 0.0, False
    plan = []
    block = CANDIDATES["rice"]["block_mw"]
    for year in YEARS:
        options = []
        choices = sorted({ccgt3, ccgt3 or year >= EARLIEST_YEAR["ccgt3"]})
        rice_allowed = year >= EARLIEST_YEAR["rice"]
        for with_ccgt in choices:
            if rice_allowed:
                need, adq = _rice_for_target(year, scenario, with_ccgt, rice, samples, log)
            else:
                need = rice
                adq = adequacy([build_case(year, scenario, h, rice_mw=need, ccgt3=with_ccgt)
                                for h in ("2024", "2025")], samples)
            dispatch = solve(build_case(year, scenario, rice_mw=need, ccgt3=with_ccgt))
            extra = block * math.ceil(dispatch["reserve"]["max_short_mw"] / block) if rice_allowed else 0.0
            if extra > 0:  # reserve needs more firm capacity than adequacy
                need += extra
                dispatch = solve(build_case(year, scenario, rice_mw=need, ccgt3=with_ccgt))
                adq = adequacy([build_case(year, scenario, h, rice_mw=need, ccgt3=with_ccgt)
                                for h in ("2024", "2025")], samples)
            cost = annualised_cost(need, with_ccgt) + dispatch["objective_usd"]
            options.append({"ccgt3": with_ccgt, "rice_mw": need, "annual_cost_usd": cost,
                            "capital_fom_usd": annualised_cost(need, with_ccgt), "adequacy": adq,
                            "dispatch": dispatch, "meets_target": adq["LOLE_h"] <= LOLE_TARGET_H
                            and dispatch["reserve"]["hours_short"] == 0})
            log(f"  {year}{scenario} option ccgt3={with_ccgt} rice={need:.0f} cost={cost / 1e6:.1f} M "
                f"LOLE={adq['LOLE_h']:.2f}")
        best = min(options, key=lambda o: o["annual_cost_usd"])
        rice, ccgt3 = best["rice_mw"], best["ccgt3"]
        plan.append({"year": year, "scenario": scenario, "rice_mw": rice, "ccgt3": ccgt3,
                     "meets_target": best["meets_target"],
                     "options": [{k: v for k, v in o.items() if k != "dispatch"} | {
                         "objective_usd": o["dispatch"]["objective_usd"]} for o in options],
                     "adequacy": best["adequacy"], "dispatch": best["dispatch"]})
    return plan


def plan_pathway(scenario: str, ccgt_from: int | None, samples: int = 200, log=print) -> dict:
    """A fixed technology pathway: CCGT-3 from ``ccgt_from`` (None = never), RICE sized each year
    for LOLE and reserve. Returns the yearly builds and the NPV of annual costs (discounted to 2027).

    The year-by-year least-cost plan locks in earlier RICE before CCGT-3 is compared; comparing
    whole pathways removes that path dependence.
    """
    block = CANDIDATES["rice"]["block_mw"]
    rice, years, npv = 0.0, [], 0.0
    for year in YEARS:
        with_ccgt = ccgt_from is not None and year >= max(ccgt_from, EARLIEST_YEAR["ccgt3"])
        if year >= EARLIEST_YEAR["rice"]:
            rice, adq = _rice_for_target(year, scenario, with_ccgt, rice, samples, lambda *_: None)
        else:
            adq = adequacy([build_case(year, scenario, h, rice_mw=rice, ccgt3=with_ccgt) for h in ("2024", "2025")],
                           samples)
        dispatch = solve(build_case(year, scenario, rice_mw=rice, ccgt3=with_ccgt))
        if year >= EARLIEST_YEAR["rice"] and dispatch["reserve"]["max_short_mw"] > 0:
            rice += block * math.ceil(dispatch["reserve"]["max_short_mw"] / block)
            dispatch = solve(build_case(year, scenario, rice_mw=rice, ccgt3=with_ccgt))
        cost = annualised_cost(rice, with_ccgt) + dispatch["objective_usd"]
        npv += cost / (1 + DISCOUNT_RATE) ** (year - YEARS[0])
        years.append({"year": year, "rice_mw": rice, "ccgt3": with_ccgt, "annual_cost_usd": cost,
                      "LOLE_h": adq["LOLE_h"], "thermal_objective_usd": dispatch["objective_usd"]})
        log(f"  pathway {scenario} ccgt_from={ccgt_from} {year}: rice={rice:.0f} cost={cost / 1e6:.1f} M")
    return {"scenario": scenario, "ccgt_from": ccgt_from, "years": years, "npv_usd": npv}


def stress_adequacy(samples: int = 200, log=print) -> dict:
    """Hardest winter hydrology (2024: regulating HPPs had 995 GWh in Jan/Feb/Nov/Dec against
    1,213 GWh in 2025) for both scenarios, plus PLEXOS calm-wind / low-solar weather in B:
    LOLE without new capacity and the RICE needed for LOLE <= target (adequacy only)."""
    block = CANDIDATES["rice"]["block_mw"]
    out = {}
    for scenario in SCENARIOS:
        vre = "calm-low" if scenario == "B" else "average"
        rice = 0.0
        for year in YEARS:
            base = build_case(year, scenario, STRESS_HYDROLOGY, vre=vre)
            none = adequacy([base], samples)
            if year >= EARLIEST_YEAR["rice"]:
                def lole(mw):
                    q = copy.deepcopy(base)
                    if mw > 0:
                        q["assets"].append(_thermal_asset(CANDIDATES["rice"], mw))
                    return adequacy([q], samples)
                result = lole(rice)
                while result["LOLE_h"] > LOLE_TARGET_H:
                    rice += max(block, block * math.ceil(result["max_unserved_mw_p90"] / block / 4))
                    result = lole(rice)
                floor = rice
                while rice - block >= 0 and rice - block >= floor - 10 * block:
                    trial = lole(rice - block)
                    if trial["LOLE_h"] > LOLE_TARGET_H:
                        break
                    rice, result = rice - block, trial
            else:
                result = none
            out[f"{scenario}-{year}"] = {"weather": f"hydrology {STRESS_HYDROLOGY}" + (", calm wind, low solar" if vre != "average" else ""),
                                         "lole_no_build": none["LOLE_h"], "eens_no_build_mwh": none["EENS_mwh"],
                                         "rice_needed_mw": rice if year >= EARLIEST_YEAR["rice"] else None,
                                         "lole_with_rice": result["LOLE_h"]}
            log(f"  stress {scenario}{year}: no-build LOLE {none['LOLE_h']:.1f}, RICE {rice:.0f} MW")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=ROOT / "data/outlook/georgia_outlook_2027_2032.json")
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--stress-out", type=Path, help="write the dry/calm stress adequacy to this file and stop")
    parser.add_argument("--pathways-only", action="store_true",
                        help="add the CCGT-3 pathway comparison to an existing results file")
    args = parser.parse_args()
    if args.stress_out:
        result = stress_adequacy(args.samples, lambda m: print(m, flush=True))
        args.stress_out.write_text(json.dumps(result, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        return
    if args.pathways_only:
        started = time.monotonic()
        output = json.loads(args.out.read_text(encoding="utf-8"))
        output["pathways"] = []
        for scenario in SCENARIOS:
            for ccgt_from in (None, 2029, 2031):
                output["pathways"].append(plan_pathway(scenario, ccgt_from, args.samples,
                                                       lambda m: print(f"[{time.monotonic() - started:7.0f}s] {m}",
                                                                       flush=True)))
                args.out.write_text(json.dumps(output, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        return
    started = time.monotonic()
    log = lambda msg: print(f"[{time.monotonic() - started:7.0f}s] {msg}", flush=True)  # noqa: E731
    output = {"schema": "powersim.georgia_outlook.v1", "assumptions": ASSUMPTIONS, "lole_target_h": LOLE_TARGET_H,
              "candidates": CANDIDATES, "outage_rates": OUTAGES, "bess": BESS, "growth_2031_2032": GROWTH,
              "plans": {}, "sensitivities": {}, "no_retirement": {}, "no_expansion": {}}
    for scenario in SCENARIOS:
        log(f"scenario {scenario}: expansion plan")
        plan = plan_expansion(scenario, args.samples, log)
        output["plans"][scenario] = plan
        for step in plan:
            year, rice, ccgt3 = step["year"], step["rice_mw"], step["ccgt3"]
            sens = {}
            for label, kwargs in (("dry_2025", {"hydrology": "2025"}), ("wet_2024", {"hydrology": "2024"}),
                                  ("demand_+5", {"demand_factor": 1.05}), ("demand_-5", {"demand_factor": 0.95})):
                project = build_case(year, scenario, rice_mw=rice, ccgt3=ccgt3, **kwargs)
                sens[label] = solve(project)
                if label.startswith("demand"):
                    sens[label]["adequacy"] = adequacy([build_case(year, scenario, h, kwargs["demand_factor"],
                                                                   rice, ccgt3) for h in ("2024", "2025")],
                                                       args.samples)
                log(f"  {year}{scenario} {label}: thermal {sum(sum(v) for k, v in sens[label]['monthly_gwh'].items() if k.startswith(('tpp-', 'new-ccgt', 'new-rice'))):.0f} GWh")
            output["sensitivities"][f"{scenario}-{year}"] = sens
            output["no_expansion"][f"{scenario}-{year}"] = adequacy(
                [build_case(year, scenario, h) for h in ("2024", "2025")], args.samples)
            output["no_retirement"][f"{scenario}-{year}"] = adequacy(
                [build_case(year, scenario, h, retire_tbilsresi=False) for h in ("2024", "2025")], args.samples)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(output, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    output["runtime_s"] = round(time.monotonic() - started)
    args.out.write_text(json.dumps(output, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    log(f"done -> {args.out}")


if __name__ == "__main__":
    main()
