from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT/'schema', ROOT/'solver'):
    sys.path.insert(0,str(p))
from powersim_solver import build_asset_map, solve_window, build_result_store

def _inp(soc=0.8, req=40, direction='up'):
    return {'assets':[
        {'id':'gen','type':'thermal','committable':False,'pmin':0,'pmax':100,'mc':50,'vom':0,'ramp_up':9999,'ramp_down':9999},
        {'id':'bat','type':'bess','committable':False,'power_mw':50,'energy_mwh':100,'soc_init':soc,'soc_min':0.1,'soc_max':0.9,'eta_charge':1,'eta_discharge':1,'vom_discharge':100},
        {'id':'dr1','type':'dr','committable':False,'pmax_curtail':25,'price_per_mwh':100,'hours_per_year_max':100},
    ],'profiles':{'demand':[10]},'study_horizon':{'horizon_hours':1},
    'reserve_products':[{'id':'R','direction':direction,'requirement':req,'shortfall_penalty':1000,'eligible_units':['bat','dr1']}],
    'solver_settings':{'solver':'highs','time_limit_s':30,'unserved_penalty':10000}}

def _solve(inp):
    assets=build_asset_map(inp); hourly,_,_,obj=solve_window(assets,[10],inp['profiles'],inp['reserve_products'],{}, {}, inp['solver_settings'])
    return assets,hourly,build_result_store(hourly,assets,inp,0,obj)

def test_bess_can_satisfy_reserve_up_without_dispatch():
    _,h,r=_solve(_inp())
    assert h[0]['reserve_up']['R']['bat'] >= 39.9
    assert h[0]['bess']['bat']['discharge_mw'] < 1e-6
    assert r['diagnostics']['reserve_supply_by_type']['bess'] >= 39.9

def test_bess_reserve_up_limited_by_soc():
    _,h,_=_solve(_inp(soc=0.2, req=40))
    assert h[0]['reserve_up']['R']['bat'] <= 10.01
    assert h[0]['reserve_shortfall']['R'] >= 29.9

def test_bess_reserve_down_limited_by_empty_soc_and_headroom():
    _,h,_=_solve(_inp(soc=0.85, req=20, direction='down'))
    assert h[0]['reserve_down']['R']['bat'] <= 5.01
    assert h[0]['reserve_shortfall']['R'] >= 14.9

def test_unsupported_eligible_asset_warned_not_silent():
    _,_,r=_solve(_inp())
    filt=r['diagnostics']['reserve_eligible_filtered']
    assert any(x['asset']=='dr1' and x['type']=='dr' for x in filt)

if __name__=='__main__':
    test_bess_can_satisfy_reserve_up_without_dispatch(); test_bess_reserve_up_limited_by_soc(); test_bess_reserve_down_limited_by_empty_soc_and_headroom(); test_unsupported_eligible_asset_warned_not_silent(); print('reserve storage tests passed')
