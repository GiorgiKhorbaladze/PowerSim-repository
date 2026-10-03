from datetime import datetime
import time
from powersim.application import ApplicationService
from powersim.platform import RunManager
from powersim.contracts import *
from powersim.execution import LocalWorkflowExecutor, resolved_to_workflow_input

def payload(): return ProjectContract(id='api',version=ProjectVersionContract(revision=1),units=UnitSystem(currency='USD'),time=TimeContract(timezone='UTC',study_year=2025,resolution_minutes=60,interval_duration_hours=1,start=datetime(2025,1,1),periods=1,calendar_policy='explicit_periods'),profiles=[ProfileContract(id='d',unit='MW',values=[1])]).model_dump(mode='json')

def test_application_service_is_solver_free_and_fails_closed(tmp_path):
    service=ApplicationService(RunManager(tmp_path)); saved=service.save_project(payload()); run=service.create_run(saved['project_id'])
    launched=service.launch(run['id'])
    assert not launched['accepted'] and launched['reason']=='no_backend_executor'
    assert service.run_status(run['id'])['status']=='failed'

def test_local_executor_persists_only_matching_gated_result(tmp_path):
    manager=RunManager(tmp_path)
    def runner(workflow, run_id, fingerprint):
        assert workflow['profiles']['d']==[1]
        diagnostics=SolverDiagnostics(backend='test',termination_condition='optimal',normalized_status='optimal',has_incumbent=True,incumbent_objective=1,best_bound=1,actual_mip_gap=0,result_validity='valid',qa_status='pass')
        qa=QAReport(status='pass')
        return ResultEnvelope(run_id=run_id,snapshot_fingerprint=fingerprint,created_at=datetime.now(),validity='valid',solver=diagnostics,qa=qa,results={'ok':True})
    service=ApplicationService(manager,LocalWorkflowExecutor(runner))
    run=service.create_run(service.save_project(payload())['project_id'])
    launched=service.launch(run['id'])
    assert launched['accepted'] and launched['run']['status']=='queued'
    deadline=time.monotonic()+5
    while service.run_status(run['id'])['status'] not in {'completed','failed'} and time.monotonic()<deadline:
        time.sleep(.01)
    assert service.run_status(run['id'])['status']=='completed'
    assert service.result(run['id'])['results']=={'ok':True}
    assert manager.verify_run(run['id'])['valid']
