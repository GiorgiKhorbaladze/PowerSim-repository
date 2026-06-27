from __future__ import annotations
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT/'schema', ROOT/'solver'): sys.path.insert(0,str(p))
from powersim_solver import build_asset_map, solve_window

def _assets(ramp=10):
    return build_asset_map({'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':100,'mc':1,'vom':0,'ramp_up':ramp,'ramp_down':ramp}]})

def test_boundary_ramp_up_uses_previous_dispatch():
    h,_,_,_=solve_window(_assets(), [100], {'demand':[100]}, [], {}, {'g':{'p':20}}, {'solver':'highs','unserved_penalty':1000})
    assert h[0]['dispatch']['g'] <= 30.001
    assert h[0]['unserved_mwh'] >= 69.9

def test_boundary_ramp_down_uses_previous_dispatch():
    h,_,_,_=solve_window(_assets(), [40], {'demand':[40]}, [], {}, {'g':{'p':50}}, {'solver':'highs','unserved_penalty':1000})
    assert h[0]['dispatch']['g'] >= 39.999

def test_boundary_ramp_scales_with_subhourly_dt():
    h,_,_,_=solve_window(_assets(40), [100], {'demand':[100]}, [], {}, {'g':{'p':20}}, {'solver':'highs','unserved_penalty':1000}, dt=0.25)
    assert h[0]['dispatch']['g'] <= 30.001

if __name__=='__main__':
    test_boundary_ramp_up_uses_previous_dispatch(); test_boundary_ramp_down_uses_previous_dispatch(); test_boundary_ramp_scales_with_subhourly_dt(); print('rolling boundary ramp tests passed')
