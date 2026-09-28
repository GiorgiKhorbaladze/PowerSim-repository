from __future__ import annotations
import sys
from pathlib import Path
from copy import deepcopy
import math
ROOT=Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT/'schema', ROOT/'solver'): sys.path.insert(0,str(p))
from powersim_solver import build_asset_map, solve_all, build_result_store
from powersim.contracts import QAStatus, QACheckResult, ResultValidity, SolverDiagnostics, SolverStatus
from powersim.qa import ObjectiveReconstructionCheck

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

def test_objective_reconstruction_participates_in_publication_qa():
    inp={'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':10,'mc':3,'vom':0}], 'profiles':{'demand':[5]}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[], 'solver_settings':{'solver':'highs'}}
    r=_run(inp)
    check=next(c for c in r['qa']['checks'] if c['check_id']=='objective_reconstruction')
    assert check['status']=='pass'


def test_missing_objective_breakdown_blocks_production_publication(monkeypatch):
    """The production build_result_store path requires objective QA to pass."""
    inp={'assets':[{'id':'g','type':'thermal','committable':False,'pmin':0,'pmax':10,'mc':3,'vom':0}], 'profiles':{'demand':[5]}, 'study_horizon':{'horizon_hours':1}, 'reserve_products':[], 'solver_settings':{'solver':'highs'}}
    def unavailable(self, resolved_input, result, tolerance, *, persisted=False):
        return QACheckResult(check_id=self.check_id, status=QAStatus.NOT_RUN,
                             message='objective data deliberately unavailable')
    monkeypatch.setattr(ObjectiveReconstructionCheck, 'run', unavailable)
    r=_run(inp)
    assert r['publication']['publishable'] is False
    assert 'mandatory_qa_check_not_passed:objective_reconstruction' in r['publication']['reasons']
    assert r['diagnostics']['result_validity']=='invalid'


def test_incomplete_or_nonfinite_objective_reconstruction_fails_cleanly():
    from powersim.qa import run_qa
    from powersim.results import evaluate_publication
    base={'hourly':[{'t':0,'load_mw':1.0,'generation_mw':1.0,'unserved_mwh':0.0,'curtailed_mwh':0.0}],
          'diagnostics':{'objective_breakdown':{'pyomo_objective':1.0}}}
    report=run_qa({}, base)
    check=next(c for c in report.checks if c.check_id=='objective_reconstruction')
    assert check.status==QAStatus.FAIL and check.message=='objective reconstruction is incomplete'
    diag=SolverDiagnostics(backend='test', termination_condition='optimal',
                           normalized_status=SolverStatus.OPTIMAL, has_incumbent=True,
                           result_validity=ResultValidity.INVALID, qa_status=QAStatus.NOT_RUN)
    decision=evaluate_publication(diag, report, extraction_completed=True,
                                  required_values_finite=True,
                                  required_check_ids=('objective_reconstruction',))
    assert decision.publishable is False
    broken=deepcopy(base)
    broken['diagnostics']['objective_breakdown']['total_reconstructed']=math.nan
    report=run_qa({}, broken)
    check=next(c for c in report.checks if c.check_id=='objective_reconstruction')
    assert check.status==QAStatus.FAIL and check.message=='objective reconstruction is non-finite'
    broken['diagnostics']['objective_breakdown']['total_reconstructed']='not-a-number'
    report=run_qa({}, broken)
    check=next(c for c in report.checks if c.check_id=='objective_reconstruction')
    assert check.status==QAStatus.FAIL and check.message=='objective reconstruction is non-numeric'


def test_corrupted_objective_reconstruction_fails_closed():
    from powersim.qa import run_qa
    from powersim.results import evaluate_publication
    result={'hourly':[{'t':0,'load_mw':1.0,'generation_mw':1.0,'unserved_mwh':0.0,'curtailed_mwh':0.0}],
            'diagnostics':{'objective_breakdown':{'pyomo_objective':100.0,'total_reconstructed':200.0}}}
    report=run_qa({}, result)
    assert report.status==QAStatus.FAIL
    diag=SolverDiagnostics(backend='test', termination_condition='optimal',
                           normalized_status=SolverStatus.OPTIMAL, has_incumbent=True,
                           result_validity=ResultValidity.INVALID, qa_status=QAStatus.NOT_RUN)
    decision=evaluate_publication(diag, report, extraction_completed=True,
                                  required_values_finite=True,
                                  required_check_ids=('objective_reconstruction',))
    assert decision.publishable is False
    assert {'qa_failed', 'mandatory_qa_check_not_passed:objective_reconstruction'} <= set(decision.reasons)

if __name__=='__main__':
    test_closure_includes_reserve_shortfall_penalty(); test_closure_includes_unserved_penalty(); test_closure_includes_bess_terms(); test_objective_reconstruction_participates_in_publication_qa(); print('objective closure tests passed')
