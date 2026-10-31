"""Calibration of a run against measured data (template, metrics, gates)."""
from __future__ import annotations

import io
import json
from pathlib import Path

import openpyxl
import pytest

from powersim.calibration import CalibrationRejected, build_template, compare

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def envelope() -> dict:
    """A real compact solve: demand [20, 22, 25, 21] MW, thermal VOM 10 serves it all."""
    from powersim.contracts import ProjectContract, resolve_scenario
    from powersim.execution import resolved_to_workflow_input, run_registered_workflow
    project = json.loads((ROOT / "samples/application_compact_demo.json").read_text(encoding="utf-8"))
    project["profiles"][0]["values"] = [20, 22, 25, 21]
    project["legacy_payload"].pop("profiles", None)
    workflow = resolved_to_workflow_input(resolve_scenario(ProjectContract.model_validate(project)))
    result = run_registered_workflow(workflow, "cal-run", "sha256:x")
    assert result.qa.status.value == "pass"
    return json.loads(result.model_dump_json())


def _filled(envelope, hourly: dict[str, list] | None = None, monthly: dict[str, dict] | None = None) -> bytes:
    workbook = openpyxl.load_workbook(io.BytesIO(build_template(envelope)))
    for sheet_name, data in (("Hourly", hourly or {}), ("Monthly", monthly or {})):
        sheet = workbook[sheet_name]
        header = [c.value for c in sheet[1]]
        for column, values in data.items():
            if column not in header:
                sheet.cell(1, len(header) + 1, column)
                header.append(column)
            index = header.index(column) + 1
            if sheet_name == "Hourly":
                for row, value in enumerate(values, start=2):
                    sheet.cell(row, index, value)
            else:
                for row in range(2, 14):
                    if sheet.cell(row, 1).value in values:
                        sheet.cell(row, index, values[sheet.cell(row, 1).value])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_template_lists_periods_kinds_and_assets(envelope):
    workbook = openpyxl.load_workbook(io.BytesIO(build_template(envelope)))
    header = [c.value for c in workbook["Hourly"][1]]
    assert header[:3] == ["period", "month", "demand"]
    assert {"kind:thermal", "kind:import", "thermal", "import"} <= set(header)
    assert [r[0].value for r in workbook["Hourly"].iter_rows(min_row=2)] == [0, 1, 2, 3]
    assert [r[0].value for r in workbook["Monthly"].iter_rows(min_row=2)][:2] == ["Jan", "Feb"]


def test_hand_checked_metrics(envelope):
    """Measured = 1.1 x model -> bias = (1 - 1.1) / 1.1 = -9.0909 %, correlation 1."""
    model = [20, 22, 25, 21]
    report = compare(envelope, _filled(envelope, hourly={"demand": [v * 1.1 for v in model]}), file_name="m.xlsx")
    demand = report["hourly"]["demand"]
    assert demand["model_mwh"] == pytest.approx(88) and demand["actual_mwh"] == pytest.approx(96.8)
    assert demand["bias_pct"] == pytest.approx(-9.0909, abs=1e-3)
    assert demand["mae_mwh"] == pytest.approx(8.8 / 4) and demand["correlation"] == pytest.approx(1.0)
    assert demand["rmse_mwh"] == pytest.approx((sum((0.1 * v) ** 2 for v in model) / 4) ** 0.5, abs=1e-3)
    assert report["file_sha256"].startswith("sha256:") and report["compared_series"] == ["demand"]


def test_monthly_kind_comparison(envelope):
    # regression: asset id "thermal" equals its kind; the kind total must not be counted twice
    report = compare(envelope, _filled(envelope, monthly={"kind:thermal": {"Jan": 80}, "thermal": {"Jan": 80}}))
    assert report["monthly"]["thermal"]["model_mwh"] == pytest.approx(88)
    thermal = report["monthly"]["kind:thermal"]
    assert thermal["model_mwh"] == pytest.approx(88) and thermal["actual_mwh"] == 80
    assert thermal["months"] == [{"month": "Jan", "model_mwh": 88.0, "actual_mwh": 80.0, "bias_pct": 10.0}]


def test_rejections(envelope):
    with pytest.raises(CalibrationRejected, match="no measured values"):
        compare(envelope, build_template(envelope))
    with pytest.raises(CalibrationRejected) as rejected:
        compare(envelope, _filled(envelope, hourly={"demand": ["lots", 1, 1, 1]}))
    assert rejected.value.issues[0]["row"] == 2
    with pytest.raises(CalibrationRejected, match="not a readable"):
        compare(envelope, b"plain text")
    report = compare(envelope, _filled(envelope, hourly={"demand": [1, 1, 1, 1], "nuclear": [1, 1, 1, 1]}))
    assert any("nuclear" in i["message"] for i in report["issues"]) and report["warnings"] == 1


def test_calibration_http_routes_and_persistence(tmp_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    from powersim.application import ApplicationService, build_fastapi_app
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager
    service = ApplicationService(RunManager(tmp_path), RegisteredLocalWorkflowExecutor(), asynchronous=False)
    client = TestClient(build_fastapi_app(service))
    client.post("/samples/application_compact_demo/load")
    run = client.post("/projects/application-compact-demo/runs").json()
    assert client.get(f"/runs/{run['id']}/calibration-template").status_code == 409  # no result yet
    client.post(f"/runs/{run['id']}/launch")
    template = client.get(f"/runs/{run['id']}/calibration-template")
    assert template.status_code == 200
    envelope = client.get(f"/runs/{run['id']}/result").json()
    content = _filled(envelope, hourly={"demand": [21, 21, 21, 21]})
    report = client.post(f"/runs/{run['id']}/calibration?file_name=m.xlsx", content=content).json()
    assert report["hourly"]["demand"]["actual_mwh"] == 84
    assert [r["file_sha256"] for r in client.get(f"/runs/{run['id']}/calibration").json()] == [report["file_sha256"]]
    assert client.post(f"/runs/{run['id']}/calibration", content=b"").status_code == 422
