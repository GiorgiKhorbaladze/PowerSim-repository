"""Filesystem-backed project, scenario, run and result management.

This is intentionally orchestration only. It resolves immutable contracts and
records provenance; all electrical equations remain in the backend workflows.
"""
from __future__ import annotations

import hashlib
import json
import re
from threading import RLock
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from typing import Any

from powersim.contracts import ProjectContract, ResultEnvelope, RunContract, RunEvent, RunStatus, resolve_scenario
from powersim.version import CONTRACT_VERSION, PRODUCT_VERSION, WORKFLOW_MODEL_VERSION


_ALLOWED = {
    RunStatus.DRAFT:{RunStatus.VALIDATING,RunStatus.CANCELLED}, RunStatus.VALIDATING:{RunStatus.QUEUED,RunStatus.FAILED,RunStatus.CANCELLED},
    RunStatus.QUEUED:{RunStatus.PREPARING,RunStatus.FAILED,RunStatus.CANCELLED}, RunStatus.PREPARING:{RunStatus.SOLVING,RunStatus.FAILED,RunStatus.CANCELLED},
    RunStatus.SOLVING:{RunStatus.VALIDATING_RESULTS,RunStatus.FAILED,RunStatus.CANCELLED}, RunStatus.VALIDATING_RESULTS:{RunStatus.REPORTING,RunStatus.FAILED},
    RunStatus.REPORTING:{RunStatus.COMPLETED,RunStatus.FAILED}, RunStatus.COMPLETED:set(), RunStatus.FAILED:set(), RunStatus.CANCELLED:set(),
}
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _identifier(value: str, label: str) -> str:
    """Return a filesystem-safe public identifier or reject it fail-closed."""
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value) or value in {".", ".."}:
        raise ValueError(f"invalid {label} identifier")
    return value

def _utc() -> datetime: return datetime.now(timezone.utc)
def _hash(value: Any) -> str:
    raw=json.dumps(value, sort_keys=True, separators=(",",":"), ensure_ascii=False).encode()
    return "sha256:"+hashlib.sha256(raw).hexdigest()

class RunManager:
    def __init__(self, root: str|Path):
        self.root=Path(root).resolve(); self.root.mkdir(parents=True,exist_ok=True)
        (self.root/"projects").mkdir(exist_ok=True); (self.root/"runs").mkdir(exist_ok=True)
        self._lock=RLock()
    def _project_path(self, project_id: str) -> Path:
        return self.root/"projects"/f"{_identifier(project_id, 'project')}.json"
    def _run_dir(self, run_id: str) -> Path:
        return self.root/"runs"/_identifier(run_id, 'run')
    def _write(self,path:Path,value:Any)->None:
        """Atomically replace JSON state so polling never reads a partial run."""
        with self._lock:
            temporary=path.with_suffix(path.suffix+".tmp")
            temporary.write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,indent=2,default=str),encoding="utf-8")
            temporary.replace(path)
    def save_project(self, project: ProjectContract) -> str:
        payload=project.model_dump(mode="json"); fingerprint=_hash(payload); self._write(self._project_path(project.id),payload); return fingerprint
    def load_project(self, project_id:str)->ProjectContract:
        return ProjectContract.model_validate_json(self._project_path(project_id).read_text(encoding="utf-8"))
    def create_run(self, project_id:str, scenario_id:str|None=None, *, run_id:str|None=None)->RunContract:
        project_id=_identifier(project_id, "project")
        if scenario_id is not None: _identifier(scenario_id, "scenario")
        resolved=resolve_scenario(self.load_project(project_id),scenario_id); rid=_identifier(run_id or str(uuid4()), "run")
        directory=self._run_dir(rid)
        if directory.exists(): raise ValueError(f"run already exists: {rid}")
        directory.mkdir(); self._write(directory/"resolved_input.json",resolved.model_dump(mode="json"))
        manifest={"run_id":rid,"project_id":project_id,"scenario_id":scenario_id,"snapshot_fingerprint":resolved.fingerprint(),"resolved_input_sha256":_hash(resolved.model_dump(mode="json")),"contract_version":CONTRACT_VERSION,"workflow_model_version":WORKFLOW_MODEL_VERSION,"software_version":PRODUCT_VERSION,"created_at":_utc().isoformat()}
        self._write(directory/"manifest.json",manifest)
        run=RunContract(id=rid,snapshot_fingerprint=resolved.fingerprint(),events=[RunEvent(sequence=0,timestamp=_utc(),status=RunStatus.DRAFT,message="immutable resolved input stored")])
        self._write(directory/"run.json",run.model_dump(mode="json")); return run
    def create_batch(self, project_id: str, scenario_ids: list[str|None]) -> list[RunContract]:
        """Create independent immutable snapshots, one for every requested scenario."""
        if not scenario_ids: raise ValueError("batch requires at least one scenario")
        runs=[self.create_run(project_id, scenario_id) for scenario_id in scenario_ids]
        batch={"batch_id":str(uuid4()),"project_id":project_id,"run_ids":[run.id for run in runs],"scenario_ids":scenario_ids,"created_at":_utc().isoformat()}
        self._write(self.root/"runs"/f"batch-{batch['batch_id']}.json",batch)
        return runs
    def get_run(self,run_id:str)->RunContract: return RunContract.model_validate_json((self._run_dir(run_id)/"run.json").read_text(encoding="utf-8"))
    def transition(self,run_id:str,status:RunStatus,message:str|None=None,details:dict[str,Any]|None=None)->RunContract:
        with self._lock:
            run=self.get_run(run_id)
            if status not in _ALLOWED[run.status]: raise ValueError(f"invalid run transition: {run.status.value} -> {status.value}")
            updated=run.model_copy(update={"status":status,"events":run.events+[RunEvent(sequence=len(run.events),timestamp=_utc(),status=status,message=message,details=details or {})]})
            self._write(self._run_dir(run_id)/"run.json",updated.model_dump(mode="json")); return updated
    def store_result(self,run_id:str,result:ResultEnvelope)->None:
        run=self.get_run(run_id)
        if result.run_id!=run_id or result.snapshot_fingerprint!=run.snapshot_fingerprint: raise ValueError("result does not belong to this immutable run snapshot")
        if run.status not in {RunStatus.VALIDATING_RESULTS,RunStatus.REPORTING}: raise ValueError("result may only be stored during result validation/reporting")
        payload=result.model_dump(mode="json"); directory=self._run_dir(run_id); self._write(directory/"result.json",payload)
        manifest=json.loads((directory/"manifest.json").read_text(encoding="utf-8")); manifest["result_sha256"]=_hash(payload); manifest["result_validity"]=result.validity.value; self._write(directory/"manifest.json",manifest)
    def verify_run(self, run_id: str) -> dict[str, Any]:
        """Verify persisted snapshot/result hashes without solving or trusting UI state."""
        directory=self._run_dir(run_id); manifest=json.loads((directory/"manifest.json").read_text(encoding="utf-8"))
        snapshot=json.loads((directory/"resolved_input.json").read_text(encoding="utf-8")); snapshot_ok=_hash(snapshot)==manifest.get("resolved_input_sha256")
        result_ok=None
        if (directory/"result.json").exists(): result_ok=_hash(json.loads((directory/"result.json").read_text(encoding="utf-8")))==manifest.get("result_sha256")
        return {"run_id":run_id,"snapshot_hash_valid":snapshot_ok,"result_hash_valid":result_ok,"valid":snapshot_ok and result_ok is not False}
    def compare(self,left:str,right:str)->dict[str,Any]:
        def result(rid): return json.loads((self._run_dir(rid)/"result.json").read_text(encoding="utf-8"))
        a,b=result(left),result(right)
        return {"left_run_id":left,"right_run_id":right,"same_snapshot":a["snapshot_fingerprint"]==b["snapshot_fingerprint"],"result_hashes":{"left":_hash(a),"right":_hash(b)},"validity":{"left":a["validity"],"right":b["validity"]}}
