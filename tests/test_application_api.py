"""HTTP application API used by the browser UI: validation, gates and exports."""
from __future__ import annotations

import io
import json
import time
import zipfile
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient

from powersim.application import ApplicationService, build_fastapi_app, publication_state
from powersim.contracts import QACheckResult, QAReport, ResultEnvelope, SolverDiagnostics
from powersim.execution import LocalWorkflowExecutor, RegisteredLocalWorkflowExecutor, resolved_to_workflow_input
from powersim.contracts import ProjectContract, resolve_scenario
from powersim.platform import RunManager

ROOT = Path(__file__).resolve().parents[1]


def _compact() -> dict:
    return json.loads((ROOT / "samples/application_compact_demo.json").read_text(encoding="utf-8"))


def _client(tmp_path, executor=None, asynchronous=False):
    service = ApplicationService(RunManager(tmp_path / "ws"), executor, asynchronous=asynchronous)
    return TestClient(build_fastapi_app(service)), service


def _gated_runner(qa_status="pass", validity="valid", publishable=True):
    def runner(workflow, run_id, fingerprint):
        diagnostics = SolverDiagnostics(backend="test", termination_condition="optimal", normalized_status="optimal",
                                        has_incumbent=True, result_validity=validity, qa_status=qa_status)
        qa = QAReport(status=qa_status, checks=[QACheckResult(check_id="balance", status=qa_status, message="m")])
        return ResultEnvelope(run_id=run_id, snapshot_fingerprint=fingerprint, created_at=datetime.now(),
                              validity=validity, solver=diagnostics, qa=qa,
                              results={"publication": {"publishable": publishable, "reasons": [] if publishable else ["qa_failed"]},
                                       "system_summary": {"total_cost_usd": 10.0, "total_unserved_mwh": 0.0},
                                       "hourly_system": [{"load_mw": 1.0, "generation_mw": 1.0, "lambda_usd_mwh": 5.0}]})
    return LocalWorkflowExecutor(runner)


def test_validate_reports_path_severity_and_remediation(tmp_path):
    client, _ = _client(tmp_path)
    bad = _compact()
    bad["assets"][0]["profile_references"] = ["nope"]
    body = client.post("/projects/validate", json=bad).json()
    assert body["contract_valid"] is False and body["errors"] == 1
    issue = body["issues"][0]
    assert issue == {**issue, "code": "dangling_profile_reference", "severity": "error",
                     "path": "assets[0].profile_references"}
    assert issue["remediation"]
    assert client.post("/projects/validate", json=[1, 2]).json()["contract_valid"] is False


def test_save_rejects_invalid_projects_with_structured_issues(tmp_path):
    client, service = _client(tmp_path)
    bad = _compact()
    bad["time"]["periods"] = 0
    response = client.post("/projects/save", json=bad)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["code"] == "project_invalid" and detail["issues"][0]["path"] == "time.periods"
    assert service.project_list() == []
    saved = client.post("/projects/save", json=_compact()).json()
    assert saved["saved"] and saved["run_ready"] and saved["revision"] == 1
    assert client.get(f"/projects/{saved['project_id']}").json()["project"]["id"] == "application-compact-demo"


def test_run_creation_is_gated_on_run_readiness(tmp_path):
    client, _ = _client(tmp_path, _gated_runner())
    project = _compact()
    for asset in project["assets"]:
        asset["enabled"] = False
    assert client.post("/projects/save", json=project).json()["run_ready"] is False
    response = client.post("/projects/application-compact-demo/runs")
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "project_not_run_ready"
    assert response.json()["detail"]["issues"][0]["code"] == "no_enabled_assets"
    assert client.get("/runs").json() == []
    missing = client.post("/projects/unknown/runs")
    assert missing.status_code == 404
    bad_scenario = client.post("/projects/application-compact-demo/runs?scenario_id=nope")
    assert bad_scenario.status_code == 422


def test_disabled_assets_are_excluded_from_the_solver_input():
    project = ProjectContract.model_validate(_compact())
    payload = project.model_dump(mode="json")
    payload["assets"][1]["enabled"] = False
    workflow = resolved_to_workflow_input(resolve_scenario(ProjectContract.model_validate(payload)))
    assert [a["id"] for a in workflow["assets"]] == ["thermal"]


def test_publishable_run_summary_export_and_compare(tmp_path):
    client, _ = _client(tmp_path, _gated_runner())
    client.post("/projects/save", json=_compact())
    runs = []
    for _ in range(2):
        run = client.post("/projects/application-compact-demo/runs").json()
        assert client.post(f"/runs/{run['id']}/launch").json()["accepted"]
        runs.append(run["id"])
    summary = client.get(f"/runs/{runs[0]}/summary").json()
    assert summary["status"] == "completed" and summary["publication"]["publishable"]
    assert summary["view"]["kind"] == "dispatch" and summary["verification"]["valid"]
    listed = client.get("/projects/application-compact-demo/runs").json()
    assert {row["publishable"] for row in listed} == {True} and {row["qa_status"] for row in listed} == {"pass"}
    archive = zipfile.ZipFile(io.BytesIO(client.get(f"/runs/{runs[0]}/export").content))
    assert {"result.json", "manifest.json", "resolved_input.json", "verification.json", "README.txt",
            "system_series.csv"} <= set(archive.namelist())
    compare = client.get(f"/runs/compare?left_run_id={runs[0]}&right_run_id={runs[1]}").json()
    assert compare["compatible"] and compare["publication"]["left"]["publishable"]
    assert client.get(f"/runs/compare?left_run_id={runs[0]}&right_run_id={runs[0]}").status_code == 422
    exported = client.get("/projects/application-compact-demo/export")
    assert exported.headers["content-disposition"].startswith("attachment")
    assert json.loads(exported.content)["id"] == "application-compact-demo"


def test_non_publishable_results_are_withheld(tmp_path):
    client, _ = _client(tmp_path, _gated_runner(qa_status="fail", validity="invalid", publishable=False))
    client.post("/projects/save", json=_compact())
    run = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{run['id']}/launch")
    summary = client.get(f"/runs/{run['id']}/summary").json()
    assert summary["status"] == "completed"
    assert summary["publication"]["publishable"] is False and summary["view"] is None
    assert "qa_failed" in summary["withheld_reason"]
    names = set(zipfile.ZipFile(io.BytesIO(client.get(f"/runs/{run['id']}/export").content)).namelist())
    assert "result.json" not in names and "result_diagnostics.json" in names
    other = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{other['id']}/launch")
    compare = client.get(f"/runs/compare?left_run_id={run['id']}&right_run_id={other['id']}").json()
    assert compare["compatible"] is False and "metrics" not in compare["summaries"]["left"]


def test_failed_run_reports_its_cause(tmp_path):
    def broken(workflow, run_id, fingerprint):
        raise RuntimeError("solver input rejected: demo cause")
    client, _ = _client(tmp_path, LocalWorkflowExecutor(broken))
    client.post("/projects/save", json=_compact())
    run = client.post("/projects/application-compact-demo/runs").json()
    launched = client.post(f"/runs/{run['id']}/launch").json()
    assert launched["accepted"] is False
    summary = client.get(f"/runs/{run['id']}/summary").json()
    assert summary["status"] == "failed" and summary["failure"]["error"] == "solver input rejected: demo cause"
    assert summary["result_available"] is False


def test_cancel_only_before_execution(tmp_path):
    client, _ = _client(tmp_path, _gated_runner())
    client.post("/projects/save", json=_compact())
    run = client.post("/projects/application-compact-demo/runs").json()
    assert client.post(f"/runs/{run['id']}/cancel").json()["status"] == "cancelled"
    assert client.post(f"/runs/{run['id']}/cancel").status_code == 409
    done = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{done['id']}/launch")
    assert client.post(f"/runs/{done['id']}/cancel").status_code == 409


def test_samples_system_and_reference_routes(tmp_path):
    client, _ = _client(tmp_path, RegisteredLocalWorkflowExecutor())
    samples = {s["id"]: s for s in client.get("/samples").json()}
    assert samples["georgia_2026_baseline"]["project_id"] == "georgia-2026-baseline"
    assert client.post("/samples/application_compact_demo/load").status_code == 200
    assert client.post("/samples/application_compact_demo/load").status_code == 409
    assert client.post("/samples/application_compact_demo/load?overwrite=true").status_code == 200
    assert client.post("/samples/nope/load").status_code == 404
    system = client.get("/system").json()
    assert system["version"] and system["executor"]["configured"] and system["solvers"]["highs"]
    assert "AC power flow" in system["unsupported"]
    assert {w["id"] for w in system["workflows"]} == {"deterministic_uc", "stochastic_uc", "security_scuc",
                                                     "chronological_adequacy", "scoped_expansion"}
    assert client.get("/reference").json()["review_required"] is True
    assert client.get("/reference/tables/stations?q=enguri").json()["total"] >= 1
    assert client.get("/reference/tables/nope").status_code == 422
    assert client.get("/reference/records/stations:r140").json()["issues"]
    assert client.get("/reference/records/stations:r9999").status_code == 404
    assert client.get("/reference/quality").json()["errors"] > 0
    assert client.post("/reference/review", json={"selections": []}).status_code == 422
    rejected = client.post("/projects/application-compact-demo/reference-import",
                           json={"selections": [{"record_id": "stations:r140"}], "acknowledge_warnings": True})
    assert rejected.status_code == 422 and rejected.json()["detail"]["code"] == "import_rejected"
    imported = client.post("/projects/application-compact-demo/reference-import",
                           json={"selections": [{"record_id": "stations:r2"}], "acknowledge_warnings": True,
                                 "reviewer": "api-test"}).json()
    assert imported["revision"] == 2 and imported["run_ready"] is True and imported["asset_ids"] == ["ref-abhesi-hpp"]


def test_real_compact_solve_through_http(tmp_path):
    """The registered executor solves the compact case end to end."""
    client, service = _client(tmp_path, RegisteredLocalWorkflowExecutor(), asynchronous=True)
    client.post("/samples/application_compact_demo/load")
    run = client.post("/projects/application-compact-demo/runs").json()
    assert client.post(f"/runs/{run['id']}/launch").json()["queued"] is True
    deadline = time.monotonic() + 60
    while client.get(f"/runs/{run['id']}").json()["status"] not in {"completed", "failed"}:
        assert time.monotonic() < deadline
        time.sleep(0.05)
    summary = client.get(f"/runs/{run['id']}/summary").json()
    assert summary["status"] == "completed", summary["failure"]
    assert summary["qa"]["status"] == "pass" and summary["publication"]["publishable"]
    assert summary["view"]["system_summary"]["total_unserved_mwh"] == 0


def test_publication_state_is_fail_closed():
    assert publication_state({"validity": "valid", "qa": {"status": "pass"}, "results": {}})["publishable"]
    assert not publication_state({"validity": "valid", "qa": {"status": "pass"},
                                  "results": {"publication": {"publishable": False, "reasons": ["x"]}}})["publishable"]
    assert not publication_state({"validity": "invalid", "qa": {"status": "fail"}})["publishable"]
    assert not publication_state({})["publishable"]
    assert publication_state({"validity": "valid_with_warnings", "qa": {"status": "warn"}})["qualified"]
