"""Application service - a solver-free control layer over :class:`RunManager`."""
from __future__ import annotations
from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock
from typing import Any, Protocol
from powersim.contracts import ProjectContract, ResultEnvelope, RunStatus
from powersim.platform import RunManager

class RunExecutor(Protocol):
    def execute(self, run_id: str, resolved_input_path: str) -> ResultEnvelope: ...

class ApplicationService:
    """Thread-safe application control layer with a bounded local work queue."""
    def __init__(self, manager: RunManager, executor: RunExecutor|None=None, *, asynchronous: bool=True):
        self.manager,self.executor=manager,executor
        self.asynchronous=asynchronous
        self._pool=ThreadPoolExecutor(max_workers=1, thread_name_prefix="powersim-run") if asynchronous else None
        self._futures: dict[str,Future[Any]]={}
        self._lock=RLock()
    def save_project(self, payload: dict[str,Any])->dict[str,Any]:
        project=ProjectContract.model_validate(payload); return {"project_id":project.id,"project_fingerprint":self.manager.save_project(project)}
    def project_list(self)->list[dict[str,Any]]: return self.manager.list_projects()
    def run_list(self, project_id:str|None=None)->list[dict[str,Any]]: return self.manager.list_runs(project_id)
    def create_run(self, project_id:str, scenario_id:str|None=None)->dict[str,Any]:
        run=self.manager.create_run(project_id,scenario_id); return run.model_dump(mode="json")
    def run_status(self, run_id:str)->dict[str,Any]: return self.manager.get_run(run_id).model_dump(mode="json")
    def _execute(self, run_id: str) -> dict[str, Any]:
        """Worker body. Every exception becomes a persisted failed run."""
        try:
            run=self.manager.transition(run_id,RunStatus.PREPARING,"local worker preparing immutable input")
            run=self.manager.transition(run_id,RunStatus.SOLVING,"submitted to backend executor")
            assert self.executor is not None
            result=self.executor.execute(run_id,str(self.manager._run_dir(run_id)/"resolved_input.json"))
            if result.run_id != run_id or result.snapshot_fingerprint != run.snapshot_fingerprint:
                raise ValueError("backend result does not match immutable run identity")
            self.manager.transition(run_id,RunStatus.VALIDATING_RESULTS,"backend result received")
            self.manager.transition(run_id,RunStatus.REPORTING,"persisting gated result")
            self.manager.store_result(run_id,result)
            run=self.manager.transition(run_id,RunStatus.COMPLETED,"result persisted after QA/publication gate")
            return {"accepted":True,"run":run.model_dump(mode="json"),"publishable":result.validity.value!="invalid"}
        except Exception as exc:
            current=self.manager.get_run(run_id)
            if current.status not in {RunStatus.COMPLETED,RunStatus.FAILED,RunStatus.CANCELLED}:
                failed=self.manager.transition(run_id,RunStatus.FAILED,"backend execution failed",{"error":str(exc)})
            else:
                failed=current
            return {"accepted":False,"reason":"backend_execution_failed","run":failed.model_dump(mode="json")}

    def launch(self, run_id:str)->dict[str,Any]:
        run=self.manager.get_run(run_id)
        for status in (RunStatus.VALIDATING,RunStatus.QUEUED):
            if run.status != status: run=self.manager.transition(run_id,status)
        if self.executor is None:
            run=self.manager.transition(run_id,RunStatus.FAILED,"no backend executor configured")
            return {"accepted":False,"reason":"no_backend_executor","run":run.model_dump(mode="json")}
        if not self.asynchronous:
            return self._execute(run_id)
        with self._lock:
            if run_id in self._futures and not self._futures[run_id].done():
                return {"accepted":True,"run":run.model_dump(mode="json"),"queued":True}
            assert self._pool is not None
            self._futures[run_id]=self._pool.submit(self._execute,run_id)
        return {"accepted":True,"run":run.model_dump(mode="json"),"queued":True}
    def result(self,run_id:str)->dict[str,Any]:
        path=self.manager._run_dir(run_id)/"result.json"
        if not path.exists(): raise FileNotFoundError("result is not available")
        import json
        return json.loads(path.read_text(encoding="utf-8"))
    def compare(self,left_run_id:str,right_run_id:str)->dict[str,Any]: return self.manager.compare(left_run_id,right_run_id)

def build_fastapi_app(service: ApplicationService):
    """Optional HTTP adapter. FastAPI is intentionally not a core solver dependency."""
    from fastapi import FastAPI, HTTPException
    app=FastAPI(title="PowerSim application API",version="1.0")
    def _raise_client_error(exc: Exception):
        raise HTTPException(status_code=422 if isinstance(exc, ValueError) else 404, detail=str(exc))

    @app.get('/projects')
    def project_list(): return service.project_list()
    @app.post('/projects')
    def save_project(payload:dict[str,Any]):
        try: return service.save_project(payload)
        except ValueError as exc: _raise_client_error(exc)
    @app.get('/projects/{project_id}/runs')
    def project_runs(project_id:str):
        try: return service.run_list(project_id)
        except ValueError as exc: _raise_client_error(exc)
    @app.post('/projects/{project_id}/runs')
    def create_run(project_id:str,scenario_id:str|None=None):
        try: return service.create_run(project_id,scenario_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/runs')
    def run_list(): return service.run_list()
    @app.post('/runs/{run_id}/launch')
    def launch(run_id:str):
        try: return service.launch(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/runs/compare')
    def compare(left_run_id:str,right_run_id:str):
        try: return service.compare(left_run_id,right_run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/runs/{run_id}')
    def status(run_id:str):
        try: return service.run_status(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/runs/{run_id}/result')
    def result(run_id:str):
        try: return service.result(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    return app
