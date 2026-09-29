from __future__ import annotations
import copy
import sys
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[1]
for p in (ROOT/'solver', ROOT/'src', ROOT):
    if str(p) not in sys.path: sys.path.insert(0,str(p))
from powersim_adequacy_chronological import (AdequacyValidationError, run_chronological_adequacy,
                                              run_chronological_adequacy_qa,
                                              evaluate_chronological_adequacy_publication)


def case(resolution=60):
    n=4*(60//resolution)
    return {'resolution_min':resolution,'profiles':{'demand':[10]*n,'wind':[0,1,1,0]* (60//resolution)},
      'chronological_adequacy':{'samples':4,'seed':17},'assets':[
        {'id':'t','type':'thermal','pmax':6,'for_rate':0.0},
        {'id':'w','type':'wind','pmax':8,'availability_profile':'wind','for_rate':0.0},
        {'id':'b','type':'bess','power_mw':4,'energy_mwh':4,'soc_init':0,'soc_min':0,'soc_max':1,'eta_charge':1,'eta_discharge':1,'for_rate':0},
      ]}


def test_chronological_bess_charges_then_limits_later_shortfall():
    out=run_chronological_adequacy(case())
    rows=out['samples'][0]['periods']
    assert rows[1]['storage']['b']['charge_mw']==4
    assert rows[2]['storage']['b']['soc_end_mwh']==4
    assert rows[3]['storage']['b']['discharge_mw']==4
    assert out['metrics']['EENS_mwh']==4 and out['qa']['status']=='pass' and out['publication']['publishable']


def test_subhourly_dt_and_seed_reproducibility():
    a=run_chronological_adequacy(case(15)); b=run_chronological_adequacy(case(15))
    assert a['metrics']==b['metrics']
    # One loss-of-load interval is one 15-minute period, not one hour.
    assert a['metrics']['LOLE_h']==0.25


def test_outages_are_probabilistic_and_energy_limited():
    inp=case(); inp['assets'][0]['for_rate']=0.5
    a=run_chronological_adequacy(inp); b=run_chronological_adequacy(inp)
    assert a['metrics']==b['metrics'] and a['metrics']['EENS_mwh'] > 4


@pytest.mark.parametrize('mutate', [
    lambda r: r['samples'][0]['periods'][1]['storage']['b'].__setitem__('soc_end_mwh', 99),
    lambda r: r['samples'][0]['periods'][1]['available_supply_mw'].__setitem__('w', -1),
    lambda r: r['metrics'].__setitem__('EENS_mwh', 0),
])
def test_corrupted_canonical_output_fails_independent_qa(mutate):
    inp=case(); out=run_chronological_adequacy(inp); corrupt=copy.deepcopy(out); mutate(corrupt)
    qa=run_chronological_adequacy_qa(inp, corrupt)
    assert qa['status']=='fail'
    gate=evaluate_chronological_adequacy_publication(inp, corrupt)
    assert gate['result_validity']=='invalid' and gate['publication']['publishable'] is False


def test_input_validation_is_structured_and_fail_closed():
    inp=case(); inp['profiles']['wind']=[1]; inp['chronological_adequacy']['samples']=0
    with pytest.raises(AdequacyValidationError) as caught: run_chronological_adequacy(inp)
    assert all({'path','message'} <= issue.keys() for issue in caught.value.issues)
