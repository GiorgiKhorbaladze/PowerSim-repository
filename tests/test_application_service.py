from datetime import datetime
from powersim.application import ApplicationService
from powersim.platform import RunManager
from powersim.contracts import *

def payload(): return ProjectContract(id='api',version=ProjectVersionContract(revision=1),units=UnitSystem(currency='USD'),time=TimeContract(timezone='UTC',study_year=2025,resolution_minutes=60,interval_duration_hours=1,start=datetime(2025,1,1),periods=1,calendar_policy='explicit_periods'),profiles=[ProfileContract(id='d',unit='MW',values=[1])]).model_dump(mode='json')

def test_application_service_is_solver_free_and_fails_closed(tmp_path):
    service=ApplicationService(RunManager(tmp_path)); saved=service.save_project(payload()); run=service.create_run(saved['project_id'])
    launched=service.launch(run['id'])
    assert not launched['accepted'] and launched['reason']=='no_backend_executor'
    assert service.run_status(run['id'])['status']=='failed'
