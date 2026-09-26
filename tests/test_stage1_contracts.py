from __future__ import annotations
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import pytest
from pydantic import ValidationError

from powersim import PRODUCT_VERSION
from powersim.contracts import *
from powersim.contracts.migrations import migrate_contract
from powersim.validation import adapt_legacy_input, validate_contract


def time_contract(periods=2):
    return TimeContract(timezone="UTC", study_year=2025, resolution_minutes=60,
        interval_duration_hours=1, start=datetime(2025,1,1), periods=periods,
        calendar_policy=CalendarPolicy.EXPLICIT_PERIODS)

def project(**changes):
    data = dict(id="p", version=ProjectVersionContract(revision=1), metadata={},
        units=UnitSystem(currency="USD"), time=time_contract(), profiles=[ProfileContract(id="demand", unit="MW", values=[1,2])])
    data.update(changes)
    return ProjectContract(**data)

def test_import_version_and_cli():
    assert PRODUCT_VERSION == "1.0.0-dev"
    out = subprocess.run([sys.executable, "-m", "powersim", "--version"], check=True, capture_output=True, text=True)
    assert out.stdout.strip() == "PowerSim 1.0.0-dev"

def test_roundtrip_schema_and_status_separation():
    p = project()
    assert ProjectContract.model_validate_json(p.model_dump_json()) == p
    for model in (ProjectContract, ResolvedInputContract, SolverDiagnostics, ResultEnvelope, QAReport, RunContract):
        assert model.model_json_schema()["type"] == "object"
    assert {e.value for e in SolverStatus} == {"optimal","feasible","time_limit","infeasible","unbounded","numerical_error","solver_error"}
    assert set(QAStatus) != set(RunStatus)

def test_diagnostics_null_and_consistency():
    d = SolverDiagnostics(backend="highs", termination_condition="time limit", normalized_status="time_limit",
        has_incumbent=False, result_validity="invalid", qa_status="not_run")
    assert d.incumbent_objective is d.best_bound is d.actual_mip_gap is None
    with pytest.raises(ValidationError):
        SolverDiagnostics(backend="x", termination_condition="infeasible", normalized_status="infeasible",
            has_incumbent=True, incumbent_objective=3, result_validity="invalid", qa_status="not_run")

def test_overlay_and_fingerprint_determinism():
    p = project(metadata={"b": 2, "nested": {"a": 1}}, scenarios=[ScenarioContract(id="s", overlay={"metadata":{"nested":{"z":3}}})])
    resolved = resolve_scenario(p, "s")
    assert resolved.metadata == {"b":2,"nested":{"a":1,"z":3}}
    assert resolved.fingerprint() == ResolvedInputContract.model_validate_json(resolved.model_dump_json()).fingerprint()
    assert resolved.canonical_json() == ResolvedInputContract.model_validate_json(resolved.model_dump_json()).canonical_json()

def test_network_and_reference_validation():
    prov = Provenance(source="test")
    p = project(assets=[AssetContract(id="a", kind="thermal", bus="missing", profile_references=["gone"]),
                                AssetContract(id="a", kind="wind")],
        network=NetworkContract(buses=[BusContract(id="b"),BusContract(id="b")],
            branches=[BranchContract(id="l", from_bus="b", to_bus="gone", susceptance_mw_per_rad=100,
                normal_limit_mw=20, provenance=prov)]))
    codes = {i.code for i in validate_contract(p)}
    assert {"duplicate_asset_id","duplicate_bus_id","dangling_profile_reference","dangling_bus_reference"} <= codes
    with pytest.raises(ValidationError):
        BranchContract(id="l", from_bus="a", to_bus="b", susceptance_mw_per_rad=0, normal_limit_mw=1, provenance=prov)

def test_time_calendar_validation():
    with pytest.raises(ValidationError): time_contract(0)
    with pytest.raises(ValidationError):
        TimeContract(timezone="UTC", study_year=2024, resolution_minutes=60, interval_duration_hours=1,
                     start=datetime(2024,1,1), periods=8784, calendar_policy="non_leap")

def test_legacy_sample_and_cli(tmp_path):
    path = Path("samples/sample_input_168h.json")
    result = adapt_legacy_input(json.loads(path.read_text()), preserve_legacy_validation=False)
    assert result.resolved_input is not None
    assert result.resolved_input.legacy_payload["metadata"]["model_version"] == "PowerSim v4.0"
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(path)], capture_output=True, text=True)
    payload = json.loads(proc.stdout)
    assert "issues" in payload and "valid" in payload
    current = project().model_dump(mode="json")
    input_path = tmp_path / "project.json"
    input_path.write_text(json.dumps(current))
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(input_path)], capture_output=True, text=True)
    assert proc.returncode == 0
    assert json.loads(proc.stdout)["valid"] is True

def test_unknown_version_fails_closed():
    data = json.loads(Path("samples/sample_input_168h.json").read_text())
    data["metadata"]["schema_version"] = "2.0"
    result = adapt_legacy_input(data, preserve_legacy_validation=False)
    assert result.resolved_input is None
    assert result.issues[0].code == "unsupported_contract_version"
    with pytest.raises(Exception): migrate_contract({}, "2.0")

def test_x_pu_requires_explicit_base():
    data = json.loads(Path("samples/sample_input_168h.json").read_text())
    data["buses"] = [{"id":"a"},{"id":"b"}]
    data["lines"] = [{"id":"l","from_bus":"a","to_bus":"b","x_pu":.1,"capacity_mw":100}]
    result = adapt_legacy_input(data, preserve_legacy_validation=False)
    assert any(i.code == "ambiguous_x_pu_base_mva" for i in result.issues)
