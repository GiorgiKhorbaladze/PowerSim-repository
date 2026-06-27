from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT/'schema', ROOT/'solver'): sys.path.insert(0,str(p))
from powersim_solver import build_asset_map, solve_all, build_result_store

def _run(inp):
    assets=build_asset_map(inp); profiles=inp['profiles']; hourly,st,obj=solve_all(inp,assets,profiles,{})
    return build_result_store(hourly,assets,inp,st,obj)

def test_closure_includes_reserve_shortfall_penalty():
    inp={'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':10,'mc':1,'vom':0}], 'profiles':{'demand':[0]}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[{'id':'R','direction':'up','requirement':5,'shortfall_penalty':123,'eligible_units':[]}], 'solver_settings':{'solver':'highs'}}
    r=_run(inp); ob=r['diagnostics']['objective_breakdown']
    assert ob['reserve_shortfall_penalty'] == 615
    assert ob['closure_gap_pct'] < 0.5

def test_closure_includes_unserved_penalty():
    inp={'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':1,'mc':1,'vom':0}], 'profiles':{'demand':[3]}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[], 'solver_settings':{'solver':'highs','unserved_penalty':777}}
    r=_run(inp); ob=r['diagnostics']['objective_breakdown']
    assert ob['unserved_penalty'] >= 1553
    assert ob['closure_gap_pct'] < 0.5

def test_closure_includes_bess_terms():
    inp={'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':10,'mc':100,'vom':0},{'id':'b','type':'bess','committable':False,'power_mw':5,'energy_mwh':10,'soc_init':1,'soc_min':0,'soc_max':1,'eta_charge':1,'eta_discharge':1,'vom_discharge':2,'cycle_cost_per_mwh':3,'soc_end_target':1,'soc_end_penalty_usd_mwh':10}], 'profiles':{'demand':[5]}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[], 'solver_settings':{'solver':'highs'}}
    r=_run(inp); ob=r['diagnostics']['objective_breakdown']
    assert ob['bess_degradation_cost'] > 0 and ob['bess_end_soc_penalty'] > 0
    assert ob['closure_gap_pct'] < 0.5

if __name__=='__main__':
    test_closure_includes_reserve_shortfall_penalty(); test_closure_includes_unserved_penalty(); test_closure_includes_bess_terms(); print('objective closure tests passed')
