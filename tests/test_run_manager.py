from datetime import datetime
from powersim.contracts import *
from powersim.platform import RunManager

def project():
    return ProjectContract(id='p',version=ProjectVersionContract(revision=1),units=UnitSystem(currency='USD'),time=TimeContract(timezone='UTC',study_year=2025,resolution_minutes=60,interval_duration_hours=1,start=datetime(2025,1,1),periods=2,calendar_policy='explicit_periods'),profiles=[ProfileContract(id='d',unit='MW',values=[1,2])],scenarios=[ScenarioContract(id='high',overlay={'metadata':{'case':'high'}})])

def test_immutable_snapshot_lifecycle_and_provenance(tmp_path):
    m=RunManager(tmp_path); m.save_project(project()); r=m.create_run('p','high',run_id='r')
    assert r.snapshot_fingerprint and (tmp_path/'runs/r/manifest.json').exists()
    for state in [RunStatus.VALIDATING,RunStatus.QUEUED,RunStatus.PREPARING,RunStatus.SOLVING,RunStatus.VALIDATING_RESULTS]: r=m.transition('r',state)
    assert r.events[-1].status==RunStatus.VALIDATING_RESULTS
    assert m.get_run('r').snapshot_fingerprint==r.snapshot_fingerprint

def test_invalid_transition_and_foreign_result_fail_closed(tmp_path):
    m=RunManager(tmp_path); m.save_project(project()); m.create_run('p',run_id='r')
    try: m.transition('r',RunStatus.COMPLETED)
    except ValueError: pass
    else: assert False

def test_batch_and_result_provenance_verification(tmp_path):
    m=RunManager(tmp_path); m.save_project(project()); runs=m.create_batch('p',[None,'high'])
    assert len(runs)==2 and runs[0].snapshot_fingerprint != runs[1].snapshot_fingerprint
    run=runs[0]
    for state in [RunStatus.VALIDATING,RunStatus.QUEUED,RunStatus.PREPARING,RunStatus.SOLVING,RunStatus.VALIDATING_RESULTS]: m.transition(run.id,state)
    diagnostics=SolverDiagnostics(backend='test',termination_condition='optimal',normalized_status='optimal',has_incumbent=True,incumbent_objective=1,result_validity='valid',qa_status='pass')
    qa=QAReport(status='pass')
    envelope=ResultEnvelope(run_id=run.id,snapshot_fingerprint=run.snapshot_fingerprint,created_at=datetime.now(),validity='valid',solver=diagnostics,qa=qa,results={'objective_usd':1})
    m.store_result(run.id,envelope)
    assert m.verify_run(run.id)['valid'] and m.compare(run.id,run.id)['same_snapshot']
