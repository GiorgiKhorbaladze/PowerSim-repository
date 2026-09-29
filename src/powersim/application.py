"""Application service - a solver-free control layer over :class:`RunManager`."""
from __future__ import annotations
from typing import Any, Protocol
from powersim.contracts import ProjectContract, RunStatus
from powersim.platform import RunManager

class RunExecutor(Protocol):
    def submit(self, run_id: str, resolved_input_path: str) -> None: ...

class ApplicationService:
    def __init__(self, manager: RunManager, executor: RunExecutor|None=None): self.manager,self.executor=manager,executor
    def save_project(self, payload: dict[str,Any])->dict[str,Any]:
        project=ProjectContract.model_validate(payload); return {"project_id":project.id,"project_fingerprint":self.manager.save_project(project)}
    def create_run(self, project_id:str, scenario_id:str|None=None)->dict[str,Any]:
        run=self.manager.create_run(project_id,scenario_id); return run.model_dump(mode="json")
    def run_status(self, run_id:str)->dict[str,Any]: return self.manager.get_run(run_id).model_dump(mode="json")
    def launch(self, run_id:str)->dict[str,Any]:
        run=self.manager.get_run(run_id)
        for status in (RunStatus.VALIDATING,RunStatus.QUEUED,RunStatus.PREPARING):
            if run.status != status: run=self.manager.transition(run_id,status)
        if self.executor is None:
            run=self.manager.transition(run_id,RunStatus.FAILED,"no backend executor configured")
            return {"accepted":False,"reason":"no_backend_executor","run":run.model_dump(mode="json")}
        self.executor.submit(run_id,str(self.manager.root/"runs"/run_id/"resolved_input.json"))
        run=self.manager.transition(run_id,RunStatus.SOLVING,"submitted to backend executor")
        return {"accepted":True,"run":run.model_dump(mode="json")}
    def result(self,run_id:str)->dict[str,Any]:
        path=self.manager.root/"runs"/run_id/"result.json"
        if not path.exists(): raise FileNotFoundError("result is not available")
        import json
        return json.loads(path.read_text(encoding="utf-8"))
    def compare(self,left_run_id:str,right_run_id:str)->dict[str,Any]: return self.manager.compare(left_run_id,right_run_id)

def build_fastapi_app(service: ApplicationService):
    """Optional HTTP adapter. FastAPI is intentionally not a core solver dependency."""
    from fastapi import FastAPI, HTTPException
    app=FastAPI(title="PowerSim application API",version="1.0")
    @app.post('/projects')
    def save_project(payload:dict[str,Any]): return service.save_project(payload)
    @app.post('/projects/{project_id}/runs')
    def create_run(project_id:str,scenario_id:str|None=None): return service.create_run(project_id,scenario_id)
    @app.post('/runs/{run_id}/launch')
    def launch(run_id:str): return service.launch(run_id)
    @app.get('/runs/{run_id}')
    def status(run_id:str): return service.run_status(run_id)
    @app.get('/runs/{run_id}/result')
    def result(run_id:str):
        try: return service.result(run_id)
        except FileNotFoundError as exc: raise HTTPException(status_code=404,detail=str(exc))
    @app.get('/runs/compare')
    def compare(left_run_id:str,right_run_id:str): return service.compare(left_run_id,right_run_id)
    return app
