#!/usr/bin/env python3
"""Exploratory ASSETRA ELCC for additions to the 2027 PowerSim demo fleet."""
import argparse
import json
from pathlib import Path
import numpy as np

from assetra.system import EnergySystemBuilder
from assetra.units import StochasticUnit, StorageUnit
from assetra.simulation import ProbabilisticSimulation
from assetra.metrics import LossOfLoadHours
from assetra.contribution import EffectiveLoadCarryingCapability

from assetra_georgia_2027 import START, END, N, build, hourly, load_inputs

def addition_thermal():
    b=EnergySystemBuilder()
    b.add_unit(StochasticUnit(
        id=0, nameplate_capacity=300.,
        hourly_capacity=hourly(np.full(N,300.)),
        hourly_forced_outage_rate=hourly(np.full(N,.05))))
    return b.build()

def addition_bess():
    b=EnergySystemBuilder()
    b.add_unit(StorageUnit(
        id=0,nameplate_capacity=200.,charge_rate=200.,discharge_rate=200.,
        charge_capacity=200.,roundtrip_efficiency=.88,initial_soc=.5))
    return b.build()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--trials",type=int,default=150)
    ap.add_argument("--seed",type=int,default=2027)
    ap.add_argument("--out",default="assetra_georgia_2027_results")
    args=ap.parse_args()
    if args.trials<1:ap.error("trials must be positive")
    demand,assets=load_inputs()
    system,_=build(assets,demand,imported=True)
    result={"status":"exploratory_demo_only", "baseline":"S1_import_illustrative",
            "trials":args.trials,"seed":args.seed,
            "criterion":"constant incremental load maintaining baseline ASSETRA LOLH (ELCC)"}
    for name,add in (("thermal_300MW",addition_thermal()),("bess_200MW_200MWh",addition_bess())):
        np.random.seed(args.seed)
        sim=ProbabilisticSimulation(START,END,args.trials)
        sim.assign_energy_system(system)
        calc=EffectiveLoadCarryingCapability(system,sim,LossOfLoadHours)
        value=calc.evaluate(add,additional_demand_resolution_pct=.02)
        result[name]={"elcc_mw":float(value),"installed_mw":add.system_capacity,
                      "capacity_credit_pct":float(value/add.system_capacity*100)}
        print("ELCC",name,json.dumps(result[name]),flush=True)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/"elcc.json").write_text(json.dumps(result,indent=2),encoding="utf8")

if __name__=="__main__":
    main()
