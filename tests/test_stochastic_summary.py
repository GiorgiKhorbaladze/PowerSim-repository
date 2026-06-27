from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT/'schema', ROOT/'solver'): sys.path.insert(0,str(p))
from powersim_solver import run_stochastic

def _inp(with_overrides=True):
    sc=[{'id':'lo','prob':0.5,'profile_overrides':{'demand':[5]*8760}},{'id':'hi','prob':0.5,'profile_overrides':{'demand':[15]*8760}}] if with_overrides else [{'id':'lo','prob':0.5},{'id':'hi','prob':0.5}]
    return {'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':10,'mc':10,'vom':0}], 'profiles':{'demand':[5]*8760}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[], 'stochastic_scenarios':sc, 'solver_settings':{'solver':'highs','unserved_penalty':1000}}

def test_two_scenarios_different_costs_and_expected_cost():
    s=run_stochastic(_inp(True)); costs=[r['total_objective_cost_usd'] for r in s['scenarios']]
    assert costs[0] != costs[1]
    assert abs(s['expected_objective_cost'] - sum(costs)/2) <= 1

def test_cvar_and_summary_fields_present():
    s=run_stochastic(_inp(True))
    assert s['cvar95'] >= max(r['total_objective_cost_usd'] for r in s['scenarios']) - 1
    for key in ('expected_unserved_mwh','expected_curtailed_mwh','expected_gas_mm3'):
        assert key in s

def test_missing_profile_override_warns():
    s=run_stochastic(_inp(False))
    assert 'stochastic_profiles_not_switched' in s['diagnostics']['warnings']

if __name__=='__main__':
    test_two_scenarios_different_costs_and_expected_cost(); test_cvar_and_summary_fields_present(); test_missing_profile_override_warns(); print('stochastic summary tests passed')
