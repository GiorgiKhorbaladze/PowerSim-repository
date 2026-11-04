#!/usr/bin/env python3
"""Georgia 2027 ASSETRA demonstrator grounded in PowerSim inputs.

CAUTION: demand is PowerSim P50 2027; assets are SANITIZED 2026 demo registry;
hydrology, forced outages and imports are explicitly exploratory assumptions.
Neither official GSE adequacy nor 2027 commissioning plan.
"""
import argparse
import csv
import json
import math
import os
from pathlib import Path

import numpy as np
import xarray as xr

from assetra.system import EnergySystemBuilder
from assetra.units import DemandUnit, StaticUnit, StochasticUnit, HydroUnit, StorageUnit
from assetra.utils import get_hourly_time_series_xr
from assetra.simulation import ProbabilisticSimulation
from assetra.metrics import (ExpectedUnservedEnergy, LossOfLoadHours, LossOfLoadDays, LossOfLoadFrequency)

ROOT = Path(__file__).resolve().parents[1]
START, END = "2027-01-01 00:00:00", "2027-12-31 23:00:00"
N = 8760
MONTH_DAYS = np.array([31,28,31,30,31,30,31,31,30,31,30,31])
ROR_CF = np.array([.33,.36,.50,.68,.75,.69,.58,.49,.41,.34,.32,.32])
HYDRO_WEIGHTS = np.array([1.13,1.07,1.00,.87,.75,.72,.82,.89,.96,1.00,1.09,1.20])
HYDRO_2025_BENCHMARK_MWH = 10_986_600.0
MONTH = np.repeat(np.arange(1,13), MONTH_DAYS*24)
assert len(MONTH) == N


def hourly(values):
    x = np.asarray(values, dtype=float)
    if x.shape != (N,) or not np.all(np.isfinite(x)):
        raise ValueError("Hourly profile must contain 8760 finite values")
    return get_hourly_time_series_xr(x.tolist(), start_hour=START)


def load_cf(path):
    vals = []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for row in reader:
            vals.append(float(row["CF"].replace(",", ".")))
    vals = np.array(vals)
    if len(vals) != N or np.any((vals < 0) | (vals > 1)):
        raise ValueError(f"Invalid 2026 analogue renewable profile {path}: {len(vals)}")
    return vals


def load_inputs():
    src = json.loads((ROOT/"data/gse_load_2026_2030.json").read_text(encoding="utf8"))
    demand = np.array(src["load_mw_by_year"]["2027"], dtype=float)
    registry = json.loads((ROOT/"samples/demo_asset_registry_2026.json").read_text(encoding="utf8"))
    assets = registry["assets"]
    assert len(demand) == N and np.all(demand >= 0)
    assert abs(demand.sum()/1000-src["meta"]["target_gwh_per_year"]["2027"]) < 1.0
    assert abs(demand.max()-src["meta"]["peak_mw_per_year"]["2027"]) < 1.0
    assert len(assets) == registry["metadata"]["expected_asset_count"]
    return demand, assets


def category_sum(assets, category):
    return sum(float(a["installed_capacity_mw"]) for a in assets if a["source_category"] == category)


def build(assets, demand, *, imported=True, dry=False, thermal_add=0, bess=False):
    builder=EnergySystemBuilder()
    builder.add_unit(DemandUnit(id=builder.size, hourly_demand=hourly(demand)))
    ror_capacity = category_sum(assets, "Hydro - Seasonal") + category_sum(assets, "Hydro - Small")
    reservoir_capacity = category_sum(assets, "Hydro - Reservoir")
    ror_avail = ror_capacity * ROR_CF[MONTH-1] * (.8 if dry else 1.0)
    builder.add_unit(StaticUnit(id=builder.size, nameplate_capacity=ror_capacity, hourly_capacity=hourly(ror_avail)))

    # Calibrate to the 2025 annual hydropower benchmark only as a sensitivity
    # assumption (NOT 2027 monthly flow/dispatch). HydroUnit is monthly energy-limited.
    reservoir_annual = (HYDRO_2025_BENCHMARK_MWH - float(ror_avail.sum()))
    if reservoir_annual <= 0: raise ValueError("Hydro calibration negative reservoir energy")
    monthly_budget = HYDRO_WEIGHTS / HYDRO_WEIGHTS.sum() * reservoir_annual
    if dry: monthly_budget *= .8
    builder.add_unit(HydroUnit(id=builder.size, nameplate_capacity=reservoir_capacity,
        monthly_expected_generation=xr.DataArray(monthly_budget,dims=["month"],coords={"month":np.arange(1,13)})))

    # 2026 P50 resource factors used strictly as 2027 chronological analogues.
    renewable_root=ROOT/"data/renewables"
    wind_profiles = {
        "wind_001":load_cf(renewable_root/"Wind_Gori_2026_MC_P50.csv"),
        "wind_002":load_cf(renewable_root/"Wind_Dedoplistskaro_2026_MC_P50.csv"),
    }
    pv_profile=load_cf(renewable_root/"Solar_Tbilisi_2026_MC_P50.csv")
    for a in assets:
        kind=a["source_category"]
        if kind not in ("Thermal","Wind","Solar"): continue
        cap=float(a["installed_capacity_mw"])
        if kind=="Thermal":
            # Illustrative FOR, not calibrated GSE outage data.
            for_rate=.08 if a["commissioning_year"]>=2015 else .12
            cap_series=np.full(N,cap)
        elif kind=="Wind":
            for_rate=.02
            cap_series=cap*wind_profiles[a["asset_id"]]
        else:
            for_rate=.01
            cap_series=cap*pv_profile
        builder.add_unit(StochasticUnit(id=builder.size, nameplate_capacity=cap,
            hourly_capacity=hourly(cap_series),
            hourly_forced_outage_rate=hourly(np.full(N,for_rate))))

    if imported:
        # Illustrative import availability, NEVER interpreted as guaranteed NTC.
        import_profile=np.where(np.isin(MONTH,[11,12,1,2,3]),250.,100.)
        builder.add_unit(StochasticUnit(id=builder.size,nameplate_capacity=250.,
            hourly_capacity=hourly(import_profile),
            hourly_forced_outage_rate=hourly(np.full(N,.08))))

    if thermal_add:
        builder.add_unit(StochasticUnit(id=builder.size,nameplate_capacity=float(thermal_add),
            hourly_capacity=hourly(np.full(N,float(thermal_add))),
            hourly_forced_outage_rate=hourly(np.full(N,.05))))
    if bess:
        builder.add_unit(StorageUnit(id=builder.size, nameplate_capacity=200.,
            charge_rate=200., discharge_rate=200., charge_capacity=200.,
            roundtrip_efficiency=.88, initial_soc=.5))

    return builder.build(), {"ror_annual_gwh":round(float(ror_avail.sum())/1000,2),
       "reservoir_budget_annual_gwh":round(float(monthly_budget.sum())/1000,2),
       "reservoir_budget_monthly_gwh":[round(x/1000,2) for x in monthly_budget]}


def simulate(system, *, trials, seed):
    np.random.seed(seed)
    sim=ProbabilisticSimulation(START, END, trials)
    sim.assign_energy_system(system)
    sim.run()
    metrics={
        "EUE_MWh_per_year":float(ExpectedUnservedEnergy(sim).evaluate()),
        "LOLH_hours_per_year":float(LossOfLoadHours(sim).evaluate()),
        "LOLD_days_per_year":float(LossOfLoadDays(sim).evaluate()),
        "LOLF_events_per_year":float(LossOfLoadFrequency(sim).evaluate()),
    }
    net=sim.net_hourly_capacity_matrix.values
    shortage=np.maximum(-net,0)
    p_hour=np.mean(shortage>0,axis=0)
    monthly=[
        {"month":m,"EUE_MWh":round(float(shortage[:,MONTH==m].sum(axis=1).mean()),2),
        "LOLH_hours":round(float((shortage[:,MONTH==m]>0).sum(axis=1).mean()),3)}
        for m in range(1,13)
    ]
    return metrics, monthly, p_hour


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--trials",type=int,default=60)
    ap.add_argument("--seed",type=int,default=2027)
    ap.add_argument("--out",default="assetra_georgia_2027_results")
    args=ap.parse_args()
    if args.trials<1: ap.error("trials must be >0")
    demand,assets=load_inputs()
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    scen_defs={
      "S0_no_import":{"imported":False},
      "S1_import_illustrative":{"imported":True},
      "S3_dry_20pct":{"imported":True,"dry":True},
      "S5_plus_300MW_thermal":{"imported":True,"thermal_add":300},
      "S6_plus_200MW_200MWh_BESS":{"imported":True,"bess":True},
    }
    result={
      "status":"exploratory_not_official","study_year":2027,"engine":"assetra",
      "trials":args.trials,"seed":args.seed,
      "demand":{"energy_gwh":round(float(demand.sum())/1000,3),"peak_mw":round(float(demand.max()),2)},
      "source":{"demand":"PowerSim GSE_PLEXOS_sc2_2027 P50","fleet":"sanitized 2026 demo asset registry",
        "solar_wind":"2026 P50 analogue CFs","hydro":"illustrative monthly allocation calibrated to 2025 hydro annual output",
        "imports":"illustrative 250MW winter 100MW other seasons"},
      "installed_demo_capacity_mw":{k:round(category_sum(assets,k),3) for k in
         ("Hydro - Reservoir","Hydro - Seasonal","Hydro - Small","Thermal","Wind","Solar")},
      "scenarios":{}
    }
    print("DEMAND",result["demand"],flush=True)
    print("FLEET",result["installed_demo_capacity_mw"],flush=True)
    for name,config in scen_defs.items():
        sys,hydro=build(assets,demand,**config)
        metrics,monthly,p_hour=simulate(sys,trials=args.trials,seed=args.seed)
        result["scenarios"][name]={"metrics":metrics,"monthly":monthly,"hydro":hydro}
        with (out/(name+"_hourly_LOLP.csv")).open("w",newline="") as f:
            wr=csv.writer(f);wr.writerow(["hour_index","loss_probability"])
            wr.writerows((i,round(float(p),6)) for i,p in enumerate(p_hour))
        print("SCENARIO",name,"METRICS",json.dumps(metrics),flush=True)
    (out/"summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf8")
    with (out/"metrics.csv").open("w",newline="") as f:
        wr=csv.writer(f);wr.writerow(["scenario",*next(iter(result["scenarios"].values()))["metrics"].keys()])
        for name,row in result["scenarios"].items():wr.writerow([name,*row["metrics"].values()])
    print("RESULT_FILE",str((out/"summary.json").resolve()),flush=True)


if __name__=="__main__":
    main()
