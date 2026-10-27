"""Field-level reference import: mapping, overrides, provenance and gates."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from powersim.contracts import ProjectContract, resolve_scenario
from powersim.execution import resolved_to_workflow_input
from powersim.reference import ReferenceCatalogue
from powersim.reference.importer import ImportRejected, apply_import, review_import
from powersim.validation.readiness import diagnose_project

ROOT = Path(__file__).resolve().parents[1]
GARDABANI = "stations:r140"     # available 255 MW > installed 231.2 MW, thermal, no fuel
ABHESI = "stations:r2"          # clean run-of-river record
SECTION_HEADER = "stations:r134"
OK_OVERRIDES = {"capacity_max_mw": {"value": 231.2, "reason": "nameplate = unit sum"},
                "fuel_type": {"value": "natural gas", "reason": "CCGT"}}


@pytest.fixture(scope="module")
def catalogue():
    return ReferenceCatalogue()


def _project() -> ProjectContract:
    return ProjectContract.model_validate_json((ROOT / "samples/application_compact_demo.json").read_text(encoding="utf-8"))


def _review(catalogue, record_id, **selection):
    return review_import(catalogue, [{"record_id": record_id, **selection}])["reviews"][0]


def test_section_header_is_not_importable(catalogue):
    record = catalogue.record(SECTION_HEADER)[1]
    assert record["record_type"] == "section_header"
    review = _review(catalogue, SECTION_HEADER)
    assert review["blocking"]
    assert {"not_a_plant_record", "missing_installed_capacity"} <= {i["code"] for i in review["issues"]}


def test_conflict_and_missing_fuel_block_until_overridden(catalogue):
    review = _review(catalogue, GARDABANI)
    codes = {i["code"]: i["severity"] for i in review["issues"]}
    assert codes["available_exceeds_installed"] == "error"
    assert codes["missing_fuel"] == "error"
    assert review["blocking"] and review["proposed_asset"] is None
    capacity = next(f for f in review["fields"] if f["target"] == "capacity_max_mw")
    assert capacity["status"] == "conflict"
    assert capacity["source_ref"]["sheet"] == "Stations by Location" and capacity["source_ref"]["row"] == 140


def test_override_requires_reason(catalogue):
    review = _review(catalogue, GARDABANI, overrides={"capacity_max_mw": {"value": 231.2, "reason": " "}})
    assert "override_without_reason" in {i["code"] for i in review["issues"]}
    assert review["blocking"]


@pytest.mark.parametrize("field,value", [("capacity_max_mw", -1), ("kind", "nuclear"), ("id", "../x"), ("bus", "b1")])
def test_invalid_overrides_are_rejected(catalogue, field, value):
    review = _review(catalogue, ABHESI, overrides={field: {"value": value, "reason": "test"}})
    assert "invalid_override" in {i["code"] for i in review["issues"]}


def test_resolved_review_maps_fields_and_keeps_metadata(catalogue):
    review = _review(catalogue, GARDABANI, overrides=OK_OVERRIDES)
    assert not review["blocking"]
    severities = {i["code"]: i["severity"] for i in review["issues"]}
    assert severities["available_exceeds_installed"] == "resolved"
    fields = {f["target"]: f for f in review["fields"]}
    assert fields["capacity_max_mw"]["status"] == "override" and fields["capacity_max_mw"]["reason"] == "nameplate = unit sum"
    assert fields["kind"]["value"] == "thermal" and fields["kind"]["status"] == "derived"
    assert fields["capacity_min_mw"]["status"] == "assumed"
    assert fields["enabled"]["value"] is False
    # importer assumptions and unmapped solver inputs are explicit warnings
    warnings = [i for i in review["issues"] if i["severity"] == "warning"]
    assert any(i["code"] == "importer_assumption" for i in warnings)
    assert any(i["code"] == "unmapped_required_field" for i in warnings)
    # non-model source columns are attached as reference metadata
    metadata = review["reference_metadata"]
    assert metadata["available_capacity_mw"] == 255
    assert metadata["region"] == "Kvemo Kartli"
    assert metadata["parsed_generating_units"]["unit_count"] == 3
    assert review["proposed_asset"]["enabled"] is False


def test_apply_requires_acknowledged_warnings(catalogue):
    with pytest.raises(ImportRejected) as rejected:
        apply_import(_project(), catalogue, [{"record_id": ABHESI}], acknowledge_warnings=False)
    assert all(i["severity"] == "warning" for i in rejected.value.issues)


def test_apply_rejects_blocking_and_duplicate_selections(catalogue):
    with pytest.raises(ImportRejected):
        apply_import(_project(), catalogue, [{"record_id": GARDABANI}], acknowledge_warnings=True)
    with pytest.raises(ImportRejected) as rejected:
        apply_import(_project(), catalogue, [{"record_id": ABHESI}, {"record_id": ABHESI}], acknowledge_warnings=True)
    assert "duplicate_selection" in {i["code"] for i in rejected.value.issues}
    with pytest.raises(ImportRejected):
        apply_import(_project(), catalogue, [], acknowledge_warnings=True)


def test_asset_id_collision_is_blocking(catalogue):
    review = _review(catalogue, ABHESI, overrides={"id": {"value": "thermal", "reason": "collide"}})
    assert not review["blocking"]  # no project context
    report = review_import(catalogue, [{"record_id": ABHESI, "overrides": {"id": {"value": "thermal", "reason": "x"}}}],
                           existing_asset_ids={"thermal"})
    assert "asset_id_exists" in {i["code"] for i in report["reviews"][0]["issues"]}


def test_apply_records_provenance_and_new_revision(catalogue):
    project = _project()
    now = datetime(2026, 10, 27, tzinfo=timezone.utc)
    updated, report = apply_import(project, catalogue, [{"record_id": GARDABANI, "overrides": OK_OVERRIDES}],
                                   acknowledge_warnings=True, reviewer="tester", now=now)
    assert updated.version.revision == project.version.revision + 1
    assert updated.version.parent_fingerprint == project.fingerprint()
    asset = updated.assets[-1]
    assert asset.id == report["asset_ids"][0] == "ref-gardabani-tpp-1-ccpp-1"
    assert asset.enabled is False and asset.kind == "thermal" and asset.capacity_max_mw == 231.2
    assert asset.legacy_extensions["fuel_type"] == "natural gas"
    details = asset.provenance.details
    assert asset.provenance.source == "powersim.reference_catalogue"
    assert details["source_workbook"] == "Power Plant Parameters English.xlsx"
    assert details["source_sheet"] == "Stations by Location" and details["source_row"] == 140
    assert details["imported_at"] == now.isoformat() and details["imported_by"] == "tester"
    assert details["review_status"] == "requires_user_review"
    assert details["overrides"]["capacity_max_mw"]["reason"] == "nameplate = unit sum"
    assert "available_capacity_mw" in details["reference_metadata"]
    history = updated.metadata["reference_imports"][-1]
    assert history["records"] == [GARDABANI] and history["source_sha256"] == catalogue.source_sha256
    # reference metadata never reaches the solver input
    workflow = resolved_to_workflow_input(resolve_scenario(updated))
    assert all(a["id"] != asset.id for a in workflow["assets"])
    assert "reference_metadata" not in json.dumps(workflow)


def test_imported_asset_cannot_enter_a_run_unreviewed(catalogue):
    updated, _ = apply_import(_project(), catalogue, [{"record_id": GARDABANI, "overrides": OK_OVERRIDES}],
                              acknowledge_warnings=True)
    payload = updated.model_dump(mode="json")
    assert diagnose_project(payload)["run_ready"] is True  # disabled: excluded from runs
    payload["assets"][-1]["enabled"] = True
    diagnostics = diagnose_project(payload)
    errors = {i["code"] for i in diagnostics["issues"] if i["severity"] == "error"}
    assert diagnostics["contract_valid"] and not diagnostics["run_ready"]
    assert {"reference_asset_unreviewed", "missing_cost_basis"} <= errors
    payload["assets"][-1]["provenance"]["details"]["review_status"] = "user_reviewed"
    payload["assets"][-1]["legacy_extensions"]["vom"] = 40
    assert diagnose_project(payload)["run_ready"] is True


def test_reviewed_ror_asset_still_needs_a_profile(catalogue):
    updated, _ = apply_import(_project(), catalogue, [{"record_id": ABHESI}], acknowledge_warnings=True)
    payload = updated.model_dump(mode="json")
    payload["assets"][-1]["enabled"] = True
    payload["assets"][-1]["provenance"]["details"]["review_status"] = "user_reviewed"
    errors = {i["code"] for i in diagnose_project(payload)["issues"] if i["severity"] == "error"}
    assert errors == {"missing_availability_profile"}
