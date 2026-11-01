"""Run reports: Excel workbook and printable HTML, publishable runs only."""
from __future__ import annotations

import io
from datetime import datetime

import openpyxl
import pytest

pytest.importorskip("httpx")
from fastapi.testclient import TestClient

from powersim.application import ApplicationService, build_fastapi_app
from powersim.contracts import QAReport, ResultEnvelope, SolverDiagnostics
from powersim.execution import LocalWorkflowExecutor, RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager


def _client(tmp_path, executor):
    return TestClient(build_fastapi_app(ApplicationService(RunManager(tmp_path), executor, asynchronous=False)))


def test_reports_for_a_real_publishable_run(tmp_path):
    client = _client(tmp_path, RegisteredLocalWorkflowExecutor())
    client.post("/samples/application_compact_demo/load")
    run = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{run['id']}/launch")
    workbook = openpyxl.load_workbook(io.BytesIO(client.get(f"/runs/{run['id']}/report.xlsx").content))
    assert workbook.sheetnames == ["Summary", "Monthly balance", "By resource", "Hourly system",
                                   "Hourly by resource", "QA checks"]
    summary = {row[0]: row[1] for row in workbook["Summary"].iter_rows(values_only=True) if row and row[0]}
    assert summary["QA"] == "pass" and summary["Energy served"] == 80
    assert [r[0] for r in workbook["Hourly system"].iter_rows(min_row=2, values_only=True)] == [0, 1, 2, 3]
    page = client.get(f"/runs/{run['id']}/report.html")
    assert page.status_code == 200
    assert page.headers["content-security-policy"].startswith("default-src 'none'")
    assert "<script" not in page.text and "PowerSim does not perform AC power flow" in page.text
    assert "Energy served" in page.text and "Unserved" in page.text


def test_reports_are_refused_for_non_publishable_runs(tmp_path):
    def runner(workflow, run_id, fingerprint):
        diagnostics = SolverDiagnostics(backend="t", termination_condition="x", normalized_status="optimal",
                                        has_incumbent=True, result_validity="invalid", qa_status="fail")
        return ResultEnvelope(run_id=run_id, snapshot_fingerprint=fingerprint, created_at=datetime.now(),
                              validity="invalid", solver=diagnostics, qa=QAReport(status="fail"), results={})
    client = _client(tmp_path, LocalWorkflowExecutor(runner))
    client.post("/samples/application_compact_demo/load")
    run = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{run['id']}/launch")
    for suffix in ("report.xlsx", "report.html"):
        response = client.get(f"/runs/{run['id']}/{suffix}")
        assert response.status_code == 409 and response.json()["detail"]["code"] == "run_not_publishable"


def test_html_report_escapes_project_text(tmp_path):
    client = _client(tmp_path, RegisteredLocalWorkflowExecutor())
    import json
    from pathlib import Path
    project = json.loads((Path(__file__).resolve().parents[1] / "samples/application_compact_demo.json").read_text())
    project["metadata"] = {"name": "<img src=x onerror=alert(1)>", "limitations": ["<b>bold</b>"]}
    client.post("/projects/save", json=project)
    run = client.post("/projects/application-compact-demo/runs").json()
    client.post(f"/runs/{run['id']}/launch")
    text = client.get(f"/runs/{run['id']}/report.html").text
    assert "<img src=x" not in text and "&lt;img src=x" in text and "<b>bold</b>" not in text
