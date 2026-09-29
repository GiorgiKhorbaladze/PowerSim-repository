from __future__ import annotations
import copy, sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
for p in (ROOT/'solver',ROOT):
    if str(p) not in sys.path: sys.path.insert(0,str(p))
from powersim_expansion_scoped import ExpansionValidationError, run_scoped_expansion

def sample(years=1):
    return {'resolution_min':60,'profiles':{'demand':[100]*24},'assets':[], 'expansion':{'mode':'scoped_screening','years':years,'discount_rate':.08,'reserve_margin':.1,'candidates':[{'id':'new','type':'thermal','capex_per_mw':1000,'opex_per_mw_yr':1,'life_yrs':20,'capacity_credit':1,'capacity_factor':1,'max_build_mw':200}]}}

def test_scoped_single_year_reconstructs_and_publishes_screening():
    out=run_scoped_expansion(sample())
    assert out['qa']['status']=='pass' and out['publication']=={'publishable':True,'classification':'screening','reasons':[]}

def test_scoped_multi_year_recursion_is_checked():
    out=run_scoped_expansion(sample(2))
    assert out['qa']['status']=='pass'

@pytest.mark.parametrize('path,value',[('resolution_min',15),('resolution_min','bad'),('block_mw',10)])
def test_unsupported_physics_fails_closed(path,value):
    inp=sample()
    if path=='block_mw': inp['expansion']['candidates'][0][path]=value
    else: inp[path]=value
    with pytest.raises(ExpansionValidationError): run_scoped_expansion(inp)

def test_corruption_blocks_publication():
    inp=sample(); out=run_scoped_expansion(inp); bad=copy.deepcopy(out); bad['plan']['objective_usd']=0
    from powersim_expansion_scoped import _qa
    qa=_qa(inp,bad)
    assert qa['status']=='fail'
