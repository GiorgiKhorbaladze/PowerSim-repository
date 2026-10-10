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
    models = (ProjectContract, ResolvedInputContract, SolverDiagnostics, ResultEnvelope, QAReport, RunContract)
    for model in models:
        schema = model.model_json_schema()
        serialized = json.dumps(schema)
        assert CONTRACT_VERSION in serialized or model is not ProjectContract


def test_contract_validation_rejects_invalid_profile():
    p = project(profiles=[ProfileContract(id="demand", unit="MW", values=[1, float("nan")])])
    with pytest.raises(ValidationError):
        ProjectContract.model_validate(p.model_dump())


def test_migration_preserves_legacy_payload():
    raw = {
        "id": "legacy",
        "time": {
            "timezone": "UTC",
            "study_year": 2025,
            "resolution_minutes": 60,
            "interval_duration_hours": 1,
            "start": "2025-01-01T00:00:00",
            "periods": 2,
            "calendar_policy": "explicit_periods",
        },
        "profiles": [{"id": "demand", "unit": "MW", "values": [1, 2]}],
    }
    migrated = migrate_contract(raw)
    assert migrated.id == "legacy"
    assert migrated.legacy_payload == raw


def test_legacy_adapter_is_explicit():
    raw = {
        "time": {
            "timezone": "UTC",
            "study_year": 2025,
            "resolution_minutes": 60,
            "interval_duration_hours": 1,
            "start": "2025-01-01T00:00:00",
            "periods": 2,
            "calendar_policy": "explicit_periods",
        },
        "profiles": [{"id": "demand", "unit": "MW", "values": [1, 2]}],
    }
    adapted = adapt_legacy_input(raw)
    assert adapted.id == "legacy-adapted"
    report = validate_contract(adapted)
    assert report.valid
