"""Application service - a solver-free control layer over :class:`RunManager`."""
from __future__ import annotations
from concurrent.futures import Future, ThreadPoolExecutor
import csv
import importlib.util
import io
import json
from pathlib import Path
from threading import RLock
from typing import Any, Protocol, get_args
import zipfile
from powersim.contracts import ProjectContract, ResultEnvelope, RunStatus
from powersim.platform import RunManager
try:  # resolved by FastAPI from module globals (postponed annotations)
    from starlette.requests import Request
except ImportError:  # the HTTP adapter is optional for the core service
    Request = Any  # type: ignore[misc,assignment]

UNSUPPORTED_CAPABILITIES = (
    "AC power flow", "voltage and reactive-power studies", "EMT studies", "transient stability",
    "protection coordination", "commercial market settlement",
)
WORKFLOW_DESCRIPTIONS = {
    "deterministic_uc": ("Deterministic UC/ED", "Unit commitment and economic dispatch, rolling horizon and sub-hourly operation within the documented component scope."),
    "stochastic_uc": ("Stochastic UC", "Extensive-form stochastic unit commitment over weighted scenarios."),
    "security_scuc": ("N-1 security UC", "Deterministic N-1 security-constrained UC within non-islanding contingency scope."),
    "chronological_adequacy": ("Chronological adequacy", "Chronological probabilistic adequacy (LOLE/LOLP/EENS)."),
    "scoped_expansion": ("Capacity-expansion screening", "Scoped capacity-expansion screening; results are screening-grade, not investment decisions."),
}
_CANCELLABLE = {RunStatus.DRAFT, RunStatus.VALIDATING, RunStatus.QUEUED}


class ApiError(Exception):
    """A client-visible, structured application error."""
    def __init__(self, status: int, code: str, message: str, issues: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.status, self.code, self.message, self.issues = status, code, message, issues or []
    def detail(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "issues": self.issues}


class RunExecutor(Protocol):
    def execute(self, run_id: str, resolved_input_path: str) -> ResultEnvelope: ...


def publication_state(envelope: dict[str, Any]) -> dict[str, Any]:
    """Fail-closed publishability of a persisted result envelope."""
    results = envelope.get("results") if isinstance(envelope.get("results"), dict) else {}
    validity = envelope.get("validity")
    qa_status = (envelope.get("qa") or {}).get("status")
    reasons: list[str] = []
    if validity not in {"valid", "valid_with_warnings"}:
        reasons.append("result_invalid")
    if qa_status not in {"pass", "warn"}:
        reasons.append(f"qa_{qa_status or 'missing'}")
    publication = results.get("publication")
    if isinstance(publication, dict) and publication.get("publishable") is not True:
        reasons.extend(publication.get("reasons") or ["publication_gate_rejected"])
    return {"publishable": not reasons, "reasons": list(dict.fromkeys(reasons)),
            "qualified": validity == "valid_with_warnings" or qa_status == "warn"}


def _scalars(value: Any, prefix: str = "", depth: int = 0, limit: int = 80) -> dict[str, float | int | str | bool]:
    found: dict[str, Any] = {}
    if not isinstance(value, dict):
        return found
    for key, item in value.items():
        if len(found) >= limit:
            break
        name = f"{prefix}{key}"
        if isinstance(item, (bool, int, float)):
            found[name] = item
        elif isinstance(item, str) and len(item) <= 80 and depth > 0:
            found[name] = item
        elif isinstance(item, dict) and depth < 1 and key not in {"diagnostics", "qa", "metadata"}:
            found.update(_scalars(item, name + ".", depth + 1, limit - len(found)))
    return found


def _series(hourly: list[dict[str, Any]]) -> dict[str, Any]:
    """Downsample hourly system rows to at most ~400 points for display."""
    if not hourly:
        return {"resolution": None, "points": []}
    step = 1 if len(hourly) <= 400 else 24
    points = []
    for start in range(0, len(hourly), step):
        chunk = hourly[start:start + step]
        def mean(key: str) -> float | None:
            values = [float(r[key]) for r in chunk if isinstance(r.get(key), (int, float))]
            return round(sum(values) / len(values), 3) if values else None
        unserved = [float(r.get("unserved_mwh") or 0) for r in chunk if isinstance(r.get("unserved_mwh"), (int, float))]
        points.append({"index": start, "load_mw": mean("load_mw"), "generation_mw": mean("generation_mw"),
                       "price_usd_mwh": mean("lambda_usd_mwh"), "unserved_mwh": round(sum(unserved), 3)})
    return {"resolution": "hourly" if step == 1 else "daily_mean", "points": points}


def _result_view(results: dict[str, Any]) -> dict[str, Any]:
    if isinstance(results.get("system_summary"), dict):
        by_unit = []
        for unit_id, row in sorted((results.get("by_unit_summary") or {}).items()):
            if isinstance(row, dict):
                by_unit.append({"id": unit_id, **{k: row.get(k) for k in (
                    "name", "type", "energy_mwh", "capacity_factor", "gross_cost", "avg_cost_mwh", "starts",
                    "oper_hours", "curtailed_mwh")}})
        monthly = [{k: row.get(k) for k in ("label", "total_energy_mwh", "total_cost_usd", "peak_load_mw",
                                              "avg_lambda", "unserved_mwh", "curtailed_mwh")}
                   for row in results.get("monthly_summary") or [] if isinstance(row, dict)]
        hourly = results.get("hourly_system") if isinstance(results.get("hourly_system"), list) else []
        return {"kind": "dispatch", "system_summary": _scalars(results["system_summary"]), "by_unit": by_unit,
                "monthly": monthly, "series": _series(hourly)}
    return {"kind": "metrics", "metrics": _scalars({k: v for k, v in results.items()
                                                    if k not in {"publication", "solver_diagnostics"}})}


class ApplicationService:
    """Thread-safe application control layer with a bounded local work queue."""
    def __init__(self, manager: RunManager, executor: RunExecutor|None=None, *, asynchronous: bool=True,
                 reference_directory: str|Path|None=None):
        self.manager,self.executor=manager,executor
        self.asynchronous=asynchronous
        self._pool=ThreadPoolExecutor(max_workers=1, thread_name_prefix="powersim-run") if asynchronous else None
        self._futures: dict[str,Future[Any]]={}
        self._lock=RLock()
        self._reference_directory=reference_directory
        self._catalogue=None
        self._summaries: dict[str, dict[str, Any]]={}
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
        return json.loads(path.read_text(encoding="utf-8"))
    def compare(self,left_run_id:str,right_run_id:str)->dict[str,Any]: return self.manager.compare(left_run_id,right_run_id)

    # -- projects -----------------------------------------------------------
    def get_project(self, project_id: str) -> dict[str, Any]:
        try:
            project=self.manager.load_project(project_id)
        except FileNotFoundError:
            raise ApiError(404, "project_not_found", f"project {project_id!r} does not exist")
        payload=project.model_dump(mode="json")
        return {"project":payload,"fingerprint":project.fingerprint()}
    def validate_project(self, payload: Any) -> dict[str, Any]:
        from powersim.validation.readiness import diagnose_project
        diagnostics=diagnose_project(payload)
        diagnostics.pop("project")
        diagnostics["errors"]=sum(i["severity"]=="error" for i in diagnostics["issues"])
        diagnostics["warnings"]=sum(i["severity"]=="warning" for i in diagnostics["issues"])
        return diagnostics
    def save_validated_project(self, payload: Any) -> dict[str, Any]:
        """Save only a contract-valid project; report readiness separately."""
        from powersim.validation.readiness import diagnose_project
        diagnostics=diagnose_project(payload)
        project=diagnostics.pop("project")
        if not diagnostics["contract_valid"] or project is None:
            raise ApiError(422, "project_invalid", "the project document failed validation and was not saved",
                           diagnostics["issues"])
        fingerprint=self.manager.save_project(project)
        return {"project_id":project.id,"project_fingerprint":fingerprint,"revision":project.version.revision,
                "saved":True,"run_ready":diagnostics["run_ready"],"issues":diagnostics["issues"]}
    def create_validated_run(self, project_id: str, scenario_id: str|None=None) -> dict[str, Any]:
        """Create an immutable run snapshot only for a run-ready scenario."""
        from powersim.validation.readiness import scenario_readiness
        try:
            project=self.manager.load_project(project_id)
        except FileNotFoundError:
            raise ApiError(404, "project_not_found", f"project {project_id!r} has not been saved")
        try:
            errors=scenario_readiness(project, scenario_id)
        except ValueError as exc:
            raise ApiError(422, "scenario_invalid", str(exc))
        if errors:
            raise ApiError(422, "project_not_run_ready",
                           "the run was not created because the project has blocking validation errors", errors)
        return self.create_run(project_id, scenario_id)
    def export_project(self, project_id: str) -> bytes:
        return json.dumps(self.get_project(project_id)["project"], ensure_ascii=False, indent=2).encode("utf-8")

    # -- runs ---------------------------------------------------------------
    def cancel(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            run=self.manager.get_run(run_id)
            if run.status not in _CANCELLABLE:
                raise ApiError(409, "run_not_cancellable",
                               f"a {run.status.value} run cannot be cancelled; only draft or queued runs can")
            run=self.manager.transition(run_id, RunStatus.CANCELLED, "cancelled by user before execution")
        return run.model_dump(mode="json")
    def _manifest(self, run_id: str) -> dict[str, Any]:
        return json.loads((self.manager._run_dir(run_id)/"manifest.json").read_text(encoding="utf-8"))
    def run_summary(self, run_id: str) -> dict[str, Any]:
        """Status, failure cause, QA and - only if publishable - the result view."""
        run=self.manager.get_run(run_id)
        if run_id in self._summaries and run.status==RunStatus.COMPLETED:
            return self._summaries[run_id]
        manifest=self._manifest(run_id)
        summary: dict[str, Any]={"run_id":run_id,"status":run.status.value,"project_id":manifest.get("project_id"),
            "scenario_id":manifest.get("scenario_id"),"workflow":manifest.get("workflow"),
            "created_at":manifest.get("created_at"),"snapshot_fingerprint":run.snapshot_fingerprint,
            "events":[e.model_dump(mode="json") for e in run.events],"result_available":False,"failure":None}
        failed=[e for e in run.events if e.status in {RunStatus.FAILED, RunStatus.CANCELLED}]
        if failed:
            summary["failure"]={"status":failed[-1].status.value,"message":failed[-1].message,
                                "error":(failed[-1].details or {}).get("error") or (failed[-1].details or {}).get("reason")}
        path=self.manager._run_dir(run_id)/"result.json"
        if path.exists():
            envelope=json.loads(path.read_text(encoding="utf-8"))
            state=publication_state(envelope)
            qa=envelope.get("qa") or {}
            solver=envelope.get("solver") or {}
            summary.update({"result_available":True,"validity":envelope.get("validity"),
                "qa":{"status":qa.get("status"),"checks":[{k:c.get(k) for k in ("check_id","status","message","max_violation","tolerance")}
                                                         for c in qa.get("checks") or []]},
                "solver":{k:solver.get(k) for k in ("backend","backend_version","normalized_status","termination_condition",
                                                     "runtime_s","orchestration_runtime_s","actual_mip_gap","has_incumbent")},
                "publication":state,"verification":self.manager.verify_run(run_id)})
            if state["publishable"]:
                summary["view"]=_result_view(envelope.get("results") or {})
            else:
                summary["view"]=None
                summary["withheld_reason"]="results are withheld because the run is not publishable: "+", ".join(state["reasons"])
        if run.status==RunStatus.COMPLETED:
            self._summaries[run_id]=summary
        return summary
    def compare_gated(self, left_run_id: str, right_run_id: str) -> dict[str, Any]:
        if left_run_id==right_run_id:
            raise ApiError(422, "same_run", "select two different runs to compare")
        for rid in (left_run_id, right_run_id):
            if not (self.manager._run_dir(rid)/"result.json").exists():
                raise ApiError(409, "result_unavailable", f"run {rid} has no persisted result")
        comparison=self.compare(left_run_id, right_run_id)
        states={side:publication_state(self.result(rid)) for side,rid in (("left",left_run_id),("right",right_run_id))}
        comparison["publication"]=states
        if not all(state["publishable"] for state in states.values()):
            comparison["compatible"]=False
            comparison["compatibility_reason"]="metrics are withheld because at least one run is not publishable"
            for side in ("left","right"):
                comparison["summaries"][side].pop("metrics", None)
                comparison["summaries"][side].pop("candidate_builds", None)
        return comparison
    def export_run(self, run_id: str) -> bytes:
        """ZIP bundle: snapshot, manifest, events, verification and gated results."""
        directory=self.manager._run_dir(run_id)
        run=self.manager.get_run(run_id)
        buffer=io.BytesIO()
        summary=self.run_summary(run_id)
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as bundle:
            for name in ("manifest.json","run.json","resolved_input.json"):
                if (directory/name).exists():
                    bundle.write(directory/name, name)
            bundle.writestr("verification.json", json.dumps(self.manager.verify_run(run_id), indent=2))
            lines=["PowerSim run export", f"run_id: {run_id}", f"status: {run.status.value}"]
            if (directory/"result.json").exists():
                envelope=self.result(run_id)
                state=publication_state(envelope)
                if state["publishable"]:
                    bundle.write(directory/"result.json", "result.json")
                    view=summary.get("view") or {}
                    for name, rows in (("by_unit_summary.csv", view.get("by_unit")), ("monthly_summary.csv", view.get("monthly")),
                                       ("system_series.csv", (view.get("series") or {}).get("points"))):
                        if rows:
                            out=io.StringIO(); writer=csv.DictWriter(out, fieldnames=list(rows[0].keys()))
                            writer.writeheader(); writer.writerows(rows); bundle.writestr(name, out.getvalue())
                    lines.append("result: publishable; result.json is the complete gated envelope")
                else:
                    withheld={k:v for k,v in envelope.items() if k!="results"}
                    bundle.writestr("result_diagnostics.json", json.dumps(withheld, indent=2, default=str))
                    lines.append("result: NOT publishable ("+", ".join(state["reasons"])+"); numeric results withheld")
            else:
                lines.append("result: none (run did not complete)")
                if summary.get("failure"):
                    lines.append(f"failure: {summary['failure'].get('message')} {summary['failure'].get('error') or ''}")
            lines += ["", "Limits: this bundle contains planning-model outputs only. PowerSim does not perform "
                      + ", ".join(UNSUPPORTED_CAPABILITIES) + "."]
            bundle.writestr("README.txt", "\n".join(lines)+"\n")
        return buffer.getvalue()

    # -- samples and system ---------------------------------------------------
    def samples(self) -> list[dict[str, Any]]:
        from powersim.resources import SAMPLE_PROJECTS, resource_path
        listed=[]
        for key,item in SAMPLE_PROJECTS.items():
            path=resource_path(item["path"])
            project_id=None
            if path.is_file():
                try: project_id=json.loads(path.read_text(encoding="utf-8")).get("id")
                except (OSError, ValueError): project_id=None
            listed.append({"id":key,"name":item["name"],"description":item["description"],
                           "project_id":project_id,"available":project_id is not None})
        return listed
    def load_sample(self, sample_id: str, overwrite: bool=False) -> dict[str, Any]:
        from powersim.resources import SAMPLE_PROJECTS, resource_path
        if sample_id not in SAMPLE_PROJECTS:
            raise ApiError(404, "sample_not_found", f"unknown sample {sample_id!r}")
        path=resource_path(SAMPLE_PROJECTS[sample_id]["path"])
        if not path.is_file():
            raise ApiError(503, "sample_unavailable", f"sample file is not installed: {SAMPLE_PROJECTS[sample_id]['path']}")
        payload=json.loads(path.read_text(encoding="utf-8"))
        project_id=payload.get("id")
        if not overwrite and any(p["id"]==project_id for p in self.project_list()):
            raise ApiError(409, "project_exists", f"project {project_id!r} already exists in this workspace")
        return self.save_validated_project(payload)
    def system(self) -> dict[str, Any]:
        from powersim.version import CONTRACT_VERSION, PRODUCT_VERSION
        workflows=get_args(ProjectContract.model_fields["workflow"].annotation)
        runs=self.run_list()
        by_status: dict[str,int]={}
        for row in runs: by_status[row["status"]]=by_status.get(row["status"],0)+1
        def version(module: str) -> str|None:
            if importlib.util.find_spec(module) is None: return None
            try:
                from importlib.metadata import version as dist_version
                return dist_version(module)
            except Exception:
                return "installed"
        reference: dict[str, Any]
        try:
            catalogue=self.catalogue()
            reference={"available":True,"source_file":catalogue.manifest.get("source_file"),"sha256":catalogue.source_sha256}
        except (FileNotFoundError, ApiError) as exc:
            reference={"available":False,"reason":str(exc)}
        return {"product":"PowerSim","version":PRODUCT_VERSION,"contract_version":CONTRACT_VERSION,
                "workspace":{"name":self.manager.root.name,"projects":len(self.project_list()),"runs":len(runs),"runs_by_status":by_status},
                "executor":{"configured":self.executor is not None,"mode":"local queued worker" if self.asynchronous else "synchronous"},
                "solvers":{"highs":version("highspy"),"pyomo":version("pyomo")},
                "workflows":[{"id":w,"label":WORKFLOW_DESCRIPTIONS.get(w,(w,""))[0],
                              "description":WORKFLOW_DESCRIPTIONS.get(w,("",""))[1]} for w in workflows],
                "unsupported":list(UNSUPPORTED_CAPABILITIES),"reference_catalogue":reference}

    # -- bulk parameter workbook ---------------------------------------------
    def parameter_workbook(self, project_id: str) -> bytes:
        from powersim.parameter_workbook import build_template
        try:
            project=self.manager.load_project(project_id)
        except FileNotFoundError:
            raise ApiError(404, "project_not_found", f"project {project_id!r} has not been saved")
        return build_template(project)
    def parameter_workbook_upload(self, project_id: str, content: bytes, *, apply: bool=False,
                                  file_name: str|None=None, imported_by: str|None=None) -> dict[str, Any]:
        """Check (and optionally apply) an uploaded parameter workbook."""
        from powersim.parameter_workbook import WorkbookRejected, apply_workbook, public_report, read_workbook
        try:
            project=self.manager.load_project(project_id)
        except FileNotFoundError:
            raise ApiError(404, "project_not_found", f"project {project_id!r} has not been saved")
        if not content:
            raise ApiError(422, "empty_upload", "no workbook was uploaded")
        if not apply:
            return {"applied":False, **public_report(read_workbook(project, content))}
        try:
            updated, report=apply_workbook(project, content, file_name=file_name, imported_by=imported_by)
        except WorkbookRejected as exc:
            raise ApiError(422, "workbook_rejected", str(exc), exc.issues)
        saved=self.save_validated_project(updated.model_dump(mode="json"))
        return {"applied":True, **report, **saved}

    # -- reference catalogue --------------------------------------------------
    def catalogue(self):
        if self._catalogue is None:
            from powersim.reference import ReferenceCatalogue
            try:
                self._catalogue=ReferenceCatalogue(self._reference_directory)
            except FileNotFoundError as exc:
                raise ApiError(503, "reference_catalogue_unavailable", str(exc))
        return self._catalogue
    def reference_record(self, record_id: str) -> dict[str, Any]:
        catalogue=self.catalogue()
        try:
            table, record=catalogue.record(record_id)
        except KeyError:
            raise ApiError(404, "unknown_record", f"unknown reference record {record_id!r}")
        name=catalogue.record_name(record)
        matches=catalogue.matches(name) if name else []
        detail=None
        for match in matches:
            if match["table"]=="plant_detail_sheets" and match["relation"] in {"exact","variant"}:
                detail={"sheet":match["name"],"relation":match["relation"],"rows":catalogue.detail_sheets[match["name"]]["rows"]}
                break
        return {"table":table,"record":record,"issues":[i.as_dict() for i in catalogue.issues_for(record_id)],
                "matches":matches,"detail_sheet":detail}
    def reference_review(self, body: dict[str, Any]) -> dict[str, Any]:
        from powersim.reference.importer import review_import
        selections=body.get("selections")
        if not isinstance(selections, list) or not selections:
            raise ApiError(422, "no_selection", "stage at least one reference record")
        existing: set[str]=set()
        if body.get("project_id"):
            existing={a["id"] for a in self.get_project(str(body["project_id"]))["project"]["assets"]}
        return review_import(self.catalogue(), selections, existing_asset_ids=existing)
    def reference_import(self, project_id: str, body: dict[str, Any]) -> dict[str, Any]:
        from powersim.reference.importer import ImportRejected, apply_import
        try:
            project=self.manager.load_project(project_id)
        except FileNotFoundError:
            raise ApiError(404, "project_not_found", f"project {project_id!r} has not been saved")
        selections=body.get("selections")
        if not isinstance(selections, list):
            raise ApiError(422, "no_selection", "stage at least one reference record")
        try:
            updated, report=apply_import(project, self.catalogue(), selections,
                                         acknowledge_warnings=body.get("acknowledge_warnings") is True,
                                         reviewer=(str(body.get("reviewer")).strip() or None) if body.get("reviewer") else None)
        except ImportRejected as exc:
            raise ApiError(422, "import_rejected", str(exc), exc.issues)
        saved=self.save_validated_project(updated.model_dump(mode="json"))
        return {**saved, "import_id":report["import_id"], "asset_ids":report["asset_ids"], "imported_at":report["imported_at"]}


def build_fastapi_app(service: ApplicationService):
    """Optional HTTP adapter. FastAPI is intentionally not a core solver dependency."""
    from fastapi import Body, FastAPI, HTTPException
    from fastapi.responses import Response
    app=FastAPI(title="PowerSim application API",version="1.0")
    def _raise_client_error(exc: Exception):
        raise HTTPException(status_code=422 if isinstance(exc, ValueError) else 404, detail=str(exc))
    def _api(exc: ApiError):
        raise HTTPException(status_code=exc.status, detail=exc.detail())
    def _call(function, *args, **kwargs):
        try:
            return function(*args, **kwargs)
        except ApiError as exc:
            _api(exc)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail={"code":"not_found","message":str(exc),"issues":[]})
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code":"invalid_request","message":str(exc),"issues":[]})
    def _download(content: bytes, filename: str, media_type: str):
        return Response(content=content, media_type=media_type,
                        headers={"Content-Disposition":f'attachment; filename="{filename}"'})

    @app.get('/system')
    def system(): return service.system()
    @app.get('/samples')
    def samples(): return service.samples()
    @app.post('/samples/{sample_id}/load')
    def load_sample(sample_id:str, overwrite:bool=False): return _call(service.load_sample, sample_id, overwrite)
    @app.get('/projects')
    def project_list(): return service.project_list()
    @app.post('/projects')
    def save_project(payload:dict[str,Any]):
        try: return service.save_project(payload)
        except ValueError as exc: _raise_client_error(exc)
    @app.post('/projects/validate')
    def validate_project(payload:Any=Body(...)): return service.validate_project(payload)
    @app.post('/projects/save')
    def save_validated_project(payload:Any=Body(...)): return _call(service.save_validated_project, payload)
    @app.get('/projects/{project_id}')
    def get_project(project_id:str): return _call(service.get_project, project_id)
    @app.get('/projects/{project_id}/export')
    def export_project(project_id:str):
        return _download(_call(service.export_project, project_id), f"{project_id}.powersim.json", "application/json")
    @app.get('/projects/{project_id}/runs')
    def project_runs(project_id:str):
        try: return service.run_list(project_id)
        except ValueError as exc: _raise_client_error(exc)
    @app.post('/projects/{project_id}/runs')
    def create_run(project_id:str,scenario_id:str|None=None):
        return _call(service.create_validated_run, project_id, scenario_id)
    @app.get('/projects/{project_id}/parameter-workbook')
    def parameter_workbook(project_id:str):
        return _download(_call(service.parameter_workbook, project_id), f"{project_id}-parameters.xlsx",
                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    @app.post('/projects/{project_id}/parameter-workbook')
    async def parameter_workbook_upload(project_id:str, request:Request, apply:bool=False,
                                        file_name:str|None=None, imported_by:str|None=None):
        from powersim.parameter_workbook import MAX_WORKBOOK_BYTES
        content=await request.body()
        if len(content) > MAX_WORKBOOK_BYTES:
            raise HTTPException(status_code=413, detail={"code":"file_too_large","message":"the workbook is larger than 25 MB","issues":[]})
        return _call(service.parameter_workbook_upload, project_id, content, apply=apply,
                     file_name=file_name, imported_by=imported_by)
    @app.post('/projects/{project_id}/reference-import')
    def reference_import(project_id:str, body:dict[str,Any]): return _call(service.reference_import, project_id, body)
    @app.get('/runs')
    def run_list(): return service.run_list()
    @app.post('/runs/{run_id}/launch')
    def launch(run_id:str):
        try: return service.launch(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.post('/runs/{run_id}/cancel')
    def cancel(run_id:str): return _call(service.cancel, run_id)
    @app.get('/runs/compare')
    def compare(left_run_id:str,right_run_id:str):
        return _call(service.compare_gated, left_run_id, right_run_id)
    @app.get('/runs/{run_id}')
    def status(run_id:str):
        try: return service.run_status(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/runs/{run_id}/summary')
    def summary(run_id:str): return _call(service.run_summary, run_id)
    @app.get('/runs/{run_id}/export')
    def export_run(run_id:str):
        return _download(_call(service.export_run, run_id), f"powersim-run-{run_id}.zip", "application/zip")
    @app.get('/runs/{run_id}/result')
    def result(run_id:str):
        try: return service.result(run_id)
        except (ValueError, FileNotFoundError) as exc: _raise_client_error(exc)
    @app.get('/reference')
    def reference_summary(): return _call(lambda: service.catalogue().summary())
    @app.get('/reference/quality')
    def reference_quality(): return _call(lambda: service.catalogue().quality_report())
    @app.get('/reference/tables/{table}')
    def reference_table(table:str, q:str|None=None, offset:int=0, limit:int=500):
        return _call(lambda: service.catalogue().search(table, q, offset=max(0, offset), limit=max(1, min(limit, 1000))))
    @app.get('/reference/records/{record_id}')
    def reference_record(record_id:str): return _call(service.reference_record, record_id)
    @app.post('/reference/review')
    def reference_review(body:dict[str,Any]): return _call(service.reference_review, body)
    return app
