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
_NONTERMINAL = {RunStatus.QUEUED, RunStatus.PREPARING, RunStatus.SOLVING, RunStatus.VALIDATING_RESULTS, RunStatus.REPORTING}
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

def _numeric(value: Any) -> float | int | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None

def _first_metric(value: Any, names: set[str]) -> float | int | None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in names and _numeric(item) is not None:
                return item
        for item in value.values():
            found = _first_metric(item, names)
            if found is not None:
                return found
    return None

def _metrics(value: Any, names: dict[str, set[str]]) -> dict[str, float | int]:
    """Extract only explicitly emitted canonical scalar metrics.

    This intentionally never derives a missing value from dispatch series: a
    comparison must distinguish unavailable information from calculated data.
    """
    return {label: metric for label, aliases in names.items()
            if (metric := _first_metric(value, aliases)) is not None}


class RunManager:
    def __init__(self, root: str|Path):
        self.root=Path(root).resolve(); self.root.mkdir(parents=True,exist_ok=True)
        (self.root/"projects").mkdir(exist_ok=True); (self.root/"runs").mkdir(exist_ok=True)
        self._lock=RLock()
        self.reconcile_interrupted_runs()
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
    def list_projects(self) -> list[dict[str, Any]]:
        """Return safe public project metadata, never workspace paths."""
        projects=[]
        for path in sorted((self.root/"projects").glob("*.json")):
            try:
                project=self.load_project(path.stem)
            except (OSError, ValueError):
                continue
            projects.append({"id":project.id, "name":project.metadata.get("name", project.id),
                             "workflow":project.workflow,
                             "scenarios":[{"id":scenario.id, "name":scenario.name or scenario.id}
                                          for scenario in project.scenarios]})
        return projects
    def create_run(self, project_id:str, scenario_id:str|None=None, *, run_id:str|None=None)->RunContract:
        project_id=_identifier(project_id, "project")
        if scenario_id is not None: _identifier(scenario_id, "scenario")
        resolved=resolve_scenario(self.load_project(project_id),scenario_id); rid=_identifier(run_id or str(uuid4()), "run")
        directory=self._run_dir(rid)
        if directory.exists(): raise ValueError(f"run already exists: {rid}")
        directory.mkdir(); self._write(directory/"resolved_input.json",resolved.model_dump(mode="json"))
        manifest={"run_id":rid,"project_id":project_id,"scenario_id":scenario_id,"snapshot_fingerprint":resolved.fingerprint(),"workflow":resolved.workflow,"resolved_input_sha256":_hash(resolved.model_dump(mode="json")),"contract_version":CONTRACT_VERSION,"workflow_model_version":WORKFLOW_MODEL_VERSION,"software_version":PRODUCT_VERSION,"created_at":_utc().isoformat()}
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
    def list_runs(self, project_id: str|None=None) -> list[dict[str, Any]]:
        """List persisted run metadata/status without exposing workspace paths."""
        if project_id is not None: _identifier(project_id, "project")
        listed=[]
        for directory in sorted((self.root/"runs").iterdir()):
            if not directory.is_dir() or not (directory/"run.json").is_file() or not _SAFE_IDENTIFIER.fullmatch(directory.name):
                continue
            try:
                manifest=json.loads((directory/"manifest.json").read_text(encoding="utf-8"))
                run=self.get_run(directory.name)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if project_id is not None and manifest.get("project_id") != project_id:
                continue
            listed.append({"id":run.id, "status":run.status.value, "project_id":manifest.get("project_id"),
                           "scenario_id":manifest.get("scenario_id"), "workflow":manifest.get("workflow"),
                           "created_at":manifest.get("created_at"),
                           "snapshot_fingerprint":run.snapshot_fingerprint,
                           "result_validity":manifest.get("result_validity")})
        return sorted(listed, key=lambda row: (row.get("created_at") or "", row["id"]), reverse=True)
    def reconcile_interrupted_runs(self) -> list[str]:
        """Fail nonterminal runs left by an earlier process instead of faking recovery."""
        interrupted=[]
        runs_dir=self.root/"runs"
        for directory in runs_dir.iterdir():
            if not directory.is_dir() or not (directory/"run.json").is_file() or not _SAFE_IDENTIFIER.fullmatch(directory.name):
                continue
            try:
                run=self.get_run(directory.name)
            except (OSError, ValueError):
                continue
            if run.status in _NONTERMINAL:
                self.transition(run.id, RunStatus.FAILED, "run interrupted before this PowerSim startup",
                                {"reason":"process_interrupted"})
                interrupted.append(run.id)
        return interrupted
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
        if (directory/"result.json").exists(): result_ok=_hash(json.loads((directory/"result.json").read_text(encoding="utf-8"))) == manifest.get("result_sha256")
        return {"run_id":run_id,"snapshot_hash_valid":snapshot_ok,"result_hash_valid":result_ok,"valid":snapshot_ok and result_ok is not False}
    def _comparison_summary(self, run_id: str, envelope: dict[str, Any]) -> dict[str, Any]:
        manifest=json.loads((self._run_dir(run_id)/"manifest.json").read_text(encoding="utf-8"))
        results=envelope.get("results", {})
        workflow=manifest.get("workflow") or results.get("workflow")
        common={"workflow":workflow, "scenario_id":manifest.get("scenario_id"),
                "qa_status":(envelope.get("qa") or {}).get("status"),
                "validity":envelope.get("validity"),
                "publication":results.get("publication")}
        if workflow == "chronological_adequacy":
            common["metrics"]=_metrics(results, {"lole": {"lole", "lole_hours"},
                                                  "lolp": {"lolp"},
                                                  "eens_mwh": {"eens_mwh", "eens"}})
        elif workflow == "scoped_expansion":
            common["classification"]="screening"
            common["metrics"]=_metrics(results, {"screening_objective_usd":{"screening_objective_usd","objective_usd"}})
            if isinstance(results.get("candidate_builds"), (dict, list)):
                common["candidate_builds"]=results["candidate_builds"]
        else:
            common["metrics"]=_metrics(results, {
                "objective_usd":{"objective_usd","total_objective_usd","system_cost_usd","total_cost_usd"},
                "demand_mwh":{"demand_mwh","total_demand_mwh","demand_energy_mwh"},
                "imports_mwh":{"imports_mwh","exchange_mwh","import_energy_mwh"},
                "renewable_curtailment_mwh":{"renewable_curtailment_mwh","vre_curtailment_mwh","curtailment_mwh"},
                "unserved_energy_mwh":{"unserved_energy_mwh","eens_mwh"},
                "bess_charge_mwh":{"bess_charge_mwh"},
                "bess_discharge_mwh":{"bess_discharge_mwh"},
                "storage_ending_mwh":{"storage_ending_mwh","ending_soc_mwh"},
                "reserve_requirement_mwh":{"reserve_requirement_mwh"},
                "reserve_provision_mwh":{"reserve_provision_mwh"},
                "reserve_shortfall_mwh":{"reserve_shortfall_mwh"},
                "gas_consumption_mm3":{"gas_consumption_mm3","gas_use_mm3"},
            })
        return {key:value for key,value in common.items() if value not in (None, {}, [])}
    def compare(self,left:str,right:str)->dict[str,Any]:
        if left == right: raise ValueError("Select two different runs to compare")
        def result(rid): return json.loads((self._run_dir(rid)/"result.json").read_text(encoding="utf-8"))
        a,b=result(left),result(right)
        left_summary=self._comparison_summary(left,a); right_summary=self._comparison_summary(right,b)
        compatible=left_summary.get("workflow") == right_summary.get("workflow")
        return {"left_run_id":left,"right_run_id":right,"same_snapshot":a["snapshot_fingerprint"]==b["snapshot_fingerprint"],
                "compatible":compatible,
                "compatibility_reason":None if compatible else "workflow-specific metrics are not directly comparable",
                "result_hashes":{"left":_hash(a),"right":_hash(b)},
                "validity":{"left":a["validity"],"right":b["validity"]},
                "summaries":{"left":left_summary,"right":right_summary}}
