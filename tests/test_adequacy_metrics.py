from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
for p in (ROOT/'solver', ROOT):
    if str(p) not in sys.path: sys.path.insert(0,str(p))
from powersim_adequacy import run_adequacy, bess_firm_capacity

def base(demand, assets, adequacy=None):
    return {'profiles': {'demand': demand}, 'assets': assets, 'adequacy': adequacy or {'lole_target_h': 0, 'eens_target_mwh': 0}}

def test_no_shortfall():
    m=run_adequacy(base([80,90,100],[{'id':'t1','type':'thermal','pmax':125,'for_rate':0.0}]))
    assert m['LOLE_h_per_year']==0 and m['EENS_mwh']==0 and m['adequacy_pass'] is True

def test_shortfall():
    m=run_adequacy(base([80,120],[{'id':'t1','type':'thermal','pmax':100,'for_rate':0.0}]))
    assert m['LOLE_h_per_year']>0 and m['EENS_mwh']>0 and m['adequacy_pass'] is False

def test_bess_duration_limit():
    assert bess_firm_capacity({'type':'bess','power_mw':100,'energy_mwh':100},4)==25
    m=run_adequacy(base([120],[{'id':'t','type':'thermal','pmax':100},{'id':'b','type':'bess','power_mw':100,'energy_mwh':100}], {'required_storage_duration_h':4,'lole_target_h':0,'eens_target_mwh':0}))
    assert m['firm_capacity_mw']==125

def test_monte_carlo_reproducible():
    inp=base([95]*24,[{'id':'t1','type':'thermal','pmax':100,'for_rate':0.5,'outage_model':'bernoulli'}], {'mode':'monte_carlo','samples':50,'seed':7,'lole_target_h':999,'eens_target_mwh':999999})
    a=run_adequacy(inp); b=run_adequacy(inp); c=run_adequacy(inp, seed=8)
    assert a['LOLE_h_per_year']==b['LOLE_h_per_year'] and a['EENS_mwh']==b['EENS_mwh']
    assert (a['LOLE_h_per_year'], a['EENS_mwh']) != (c['LOLE_h_per_year'], c['EENS_mwh'])

if __name__ == '__main__':
    test_no_shortfall(); test_shortfall(); test_bess_duration_limit(); test_monte_carlo_reproducible(); print('adequacy metrics tests passed')
