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
from powersim.version import CONTRACT_VERSION


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
    assert PRODUCT_VERSION == "1.0.0"
    out = subprocess.run([sys.executable, "-m", "powersim", "--version"], check=True, capture_output=True, text=True)
    assert out.stdout.strip() == "PowerSim 1.0.0"

def test_roundtrip_schema_and_status_separation():
    p = project()
    assert ProjectContract.model_validate_json(p.model_dump_json()) == p
    for model in (ProjectContract, ResolvedInputContract, SolverDiagnostics, ResultEnvelope, QAReport, RunContract):
        assert model.model_json_schema()["type"] == "object"
    assert {e.value for e in SolverStatus} == {"optimal","feasible","time_limit","infeasible","unbounded","numerical_error","solver_error","completed"}
    assert set(QAStatus) != set(RunStatus)

def test_single_authoritative_contract_version():
    models = (ProjectContract, ProjectVersionContract, ResolvedInputContract,
              QAReport, ResultEnvelope, RunContract)
    assert {model.model_fields["contract_version"].default for model in models} == {CONTRACT_VERSION}

def test_diagnostics_null_and_consistency():
    d = SolverDiagnostics(backend="highs", termination_condition="time limit", normalized_status="time_limit",
        has_incumbent=False, result_validity="invalid", qa_status="not_run")
    assert d.incumbent_objective is d.best_bound is d.actual_mip_gap is None
    with pytest.raises(ValidationError):
        SolverDiagnostics(backend="x", termination_condition="infeasible", normalized_status="infeasible",
            has_incumbent=True, incumbent_objective=3, result_validity="invalid", qa_status="not_run")

@pytest.mark.parametrize(("status", "incumbent", "validity", "accepted"), [
    ("optimal", True, "valid", True), ("optimal", False, "invalid", False),
    ("feasible", True, "valid_with_warnings", True), ("feasible", False, "invalid", False),
    ("time_limit", True, "valid_with_warnings", True), ("time_limit", False, "invalid", True),
    ("time_limit", False, "valid", False), ("infeasible", False, "invalid", True),
    ("infeasible", True, "invalid", False), ("unbounded", False, "invalid", True),
    ("numerical_error", False, "invalid", True), ("numerical_error", True, "valid", False),
    ("solver_error", False, "invalid", True), ("solver_error", True, "invalid", False),
    ("completed", False, "valid", True),
])
def test_complete_solver_status_matrix(status, incumbent, validity, accepted):
    kwargs = dict(backend="test", termination_condition=status, normalized_status=status,
                  has_incumbent=incumbent, incumbent_objective=1 if incumbent else None,
                  actual_mip_gap=0.01 if incumbent else None, result_validity=validity,
                  qa_status="pass" if validity == "valid" else "warn" if validity == "valid_with_warnings" else "not_run")
    if accepted:
        assert SolverDiagnostics(**kwargs).normalized_status.value == status
    else:
        with pytest.raises(ValidationError): SolverDiagnostics(**kwargs)

def test_overlay_and_fingerprint_determinism():
    p = project(metadata={"b": 2, "nested": {"a": 1}}, scenarios=[ScenarioContract(id="s", overlay={"metadata":{"nested":{"z":3}}})])
    resolved = resolve_scenario(p, "s")
    assert resolved.metadata == {"b":2,"nested":{"a":1,"z":3}}
    assert resolved.fingerprint() == ResolvedInputContract.model_validate_json(resolved.model_dump_json()).fingerprint()
    assert resolved.canonical_json() == ResolvedInputContract.model_validate_json(resolved.model_dump_json()).canonical_json()

def test_resolved_snapshot_is_deeply_immutable_and_detached():
    p = project(metadata={"nested":{"x":1}}, solver_settings={"options":{"gap":.1}},
        profiles=[ProfileContract(id="demand", unit="MW", values=[1,2])],
        network=NetworkContract(buses=[BusContract(id="b")]))
    resolved = resolve_scenario(p)
    fingerprint = resolved.fingerprint()
    with pytest.raises(TypeError): resolved.metadata["x"] = 1
    with pytest.raises(TypeError): resolved.metadata["nested"]["x"] = 2
    with pytest.raises(TypeError): resolved.solver_settings["options"]["gap"] = .2
    with pytest.raises((AttributeError, TypeError)): resolved.profiles[0].values.append(3)
    with pytest.raises((AttributeError, TypeError)): resolved.network.buses.append(BusContract(id="c"))
    p.metadata["nested"]["x"] = 99
    p.profiles.append(ProfileContract(id="other", unit="MW", values=[3]))
    assert resolved.metadata["nested"]["x"] == 1
    assert len(resolved.profiles) == 1
    assert resolved.fingerprint() == fingerprint

@pytest.mark.parametrize("field", ["id", "contract_version", "version", "workflow_model_version"])
def test_scenario_cannot_overlay_identity(field):
    p = project(scenarios=[ScenarioContract(id="bad", overlay={field: "changed"})])
    with pytest.raises(ScenarioOverlayError) as exc:
        resolve_scenario(p, "bad")
    assert exc.value.issue.code == "protected_scenario_overlay"

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
    with pytest.raises(ValidationError, match="divide"):
        TimeContract(timezone="UTC", study_year=2025, resolution_minutes=7, interval_duration_hours=7/60,
                     start=datetime(2025,1,1), periods=75086, calendar_policy="non_leap")

def test_native_unknown_asset_fields_fail_and_legacy_extensions_are_explicit():
    with pytest.raises(ValidationError):
        AssetContract(id="a", kind="thermal", typo_parameter=123)
    data = json.loads(Path("samples/sample_input_168h.json").read_text())
    result = adapt_legacy_input(data, preserve_legacy_validation=False)
    asset = result.resolved_input.assets[0]
    assert asset.legacy_extensions
    assert "name" in asset.legacy_extensions
    with pytest.raises(TypeError): asset.legacy_extensions["new"] = True

def test_legacy_sample_and_cli(tmp_path):
    path = Path("samples/sample_input_168h.json")
    result = adapt_legacy_input(json.loads(path.read_text()), preserve_legacy_validation=False)
    assert result.resolved_input is not None
    assert result.resolved_input.legacy_payload["metadata"]["model_version"] == "PowerSim v4.0"
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(path)], capture_output=True, text=True)
    payload = json.loads(proc.stdout)
    assert proc.returncode == 0
    assert payload["valid"] is True

    partial = json.loads(path.read_text())
    partial["time_index"] = partial["time_index"][:168]
    partial["profiles"] = {key: value[:168] if isinstance(value, list) else value
                           for key, value in partial["profiles"].items()}
    partial_path = tmp_path / "partial-legacy.json"
    partial_path.write_text(json.dumps(partial))
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(partial_path)], capture_output=True, text=True)
    payload = json.loads(proc.stdout)
    assert proc.returncode == 1 and payload["valid"] is False
    assert any(issue["code"] == "legacy_validation_error" for issue in payload["issues"])
    current = project().model_dump(mode="json")
    input_path = tmp_path / "project.json"
    input_path.write_text(json.dumps(current))
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(input_path)], capture_output=True, text=True)
    assert proc.returncode == 0
    assert json.loads(proc.stdout)["valid"] is True

    # A complete, supported legacy v1.5 input must validate successfully.
    legacy = {"metadata":{"schema_version":"1.5","study_year":2025,"timezone":"UTC"},
              "time_index":[f"2025-01-01 00:00"], "profiles":{"demand":[1.0]}, "assets":[]}
    # The legacy validator's supported contract is a non-leap full year.
    from schema.powersim_schema import generate_time_index
    legacy["time_index"] = generate_time_index(2025)
    legacy["profiles"]["demand"] = [1.0] * 8760
    legacy_path = tmp_path / "legacy.json"
    legacy_path.write_text(json.dumps(legacy))
    proc = subprocess.run([sys.executable, "-m", "powersim", "validate", str(legacy_path)], capture_output=True, text=True)
    assert proc.returncode == 0
    assert json.loads(proc.stdout)["valid"] is True

def diagnostics(validity="valid", qa="pass"):
    return SolverDiagnostics(backend="test", termination_condition="optimal", normalized_status="optimal",
        has_incumbent=True, incumbent_objective=1, actual_mip_gap=0,
        result_validity=validity, qa_status=qa)

def test_result_solver_qa_consistency():
    base = dict(run_id="r", snapshot_fingerprint="sha256:x", created_at=datetime.now(), results={})
    ResultEnvelope(**base, validity="valid", solver=diagnostics(), qa=QAReport(status="pass"))
    with pytest.raises(ValidationError, match="validity"):
        ResultEnvelope(**base, validity="invalid", solver=diagnostics(), qa=QAReport(status="pass"))
    with pytest.raises(ValidationError, match="qa.status"):
        ResultEnvelope(**base, validity="valid", solver=diagnostics(), qa=QAReport(status="fail"))
    with pytest.raises(ValidationError):
        ResultEnvelope(**base, validity="valid", solver=diagnostics(qa="not_run"), qa=QAReport(status="not_run"))
    ResultEnvelope(**base, validity="valid_with_warnings", solver=diagnostics("valid_with_warnings", "warn"),
                   qa=QAReport(status="warn"))

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
