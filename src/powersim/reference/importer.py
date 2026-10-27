"""Field-level, auditable staging of reference records into a typed project.

The importer maps a *station* record of the reference catalogue to a typed
:class:`~powersim.contracts.AssetContract`.  It is fail-closed:

* any error-level issue (missing capacity, conflicting capacities, missing
  technology or fuel, invalid override, id collision) blocks the import until
  the user resolves it with an explicit override and reason;
* warnings (naming variants, duplicates, importer assumptions, fields the
  solver needs but the workbook does not provide) must be acknowledged;
* imported assets are always ``enabled=False`` with
  ``review_status="requires_user_review"`` so reference values never enter a
  study without a user decision (see ``powersim.validation.readiness``);
* source columns without a model field are kept as reference metadata in the
  asset provenance, which the solver adapter never reads.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from powersim.contracts import AssetContract, ProjectContract, Provenance

from .catalogue import (PROVENANCE_SOURCE, REMEDIATION, SOURCE_WORKBOOK, ReferenceCatalogue, parse_number,
                        parse_units)

IMPORTER_VERSION = "powersim-reference-import-1"
ALLOWED_KINDS = ("thermal", "hydro_ror", "hydro_reg", "solar", "wind")
OVERRIDABLE_FIELDS = ("id", "kind", "name", "capacity_max_mw", "capacity_min_mw", "fuel_type")
_CAPACITY_CODES = {"missing_installed_capacity", "capacity_conflict", "available_exceeds_installed",
                   "unit_capacity_mismatch"}
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

#: Inputs the solver needs for each component but the workbook cannot supply.
REQUIRED_STUDY_INPUTS = {
    "thermal": ["cost basis (heat_rate and fuel_price, or vom)"],
    "hydro_ror": ["availability profile (profile_references)"],
    "hydro_reg": ["inflow profile and reservoir model (legacy_extensions.hydro)"],
    "solar": ["availability profile (profile_references)"],
    "wind": ["availability profile (profile_references)"],
}


class ImportRejected(ValueError):
    """Raised when a staged import still has blocking issues."""

    def __init__(self, message: str, issues: list[dict[str, Any]]):
        super().__init__(message)
        self.issues = issues


def _issue(code: str, severity: str, message: str, *, field: str | None = None,
           context: dict[str, Any] | None = None, record_id: str | None = None) -> dict[str, Any]:
    return {"code": code, "severity": severity, "field": field, "message": message, "record_id": record_id,
            "remediation": REMEDIATION.get(code), "context": context or {}}


def _slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return ("ref-" + slug)[:120] or "ref-asset"


def infer_kind(regulation_type: Any, name: Any) -> str | None:
    regulation = str(regulation_type or "").strip().lower()
    label = str(name or "").lower()
    if regulation == "run-of-river":
        return "hydro_ror"
    if regulation in {"annual", "annual (cascade)", "daily", "seasonal", "inter-annual"}:
        return "hydro_reg"
    if regulation == "thermal":
        return "thermal"
    if regulation.startswith("renewable"):
        if "solar" in label:
            return "solar"
        if "wind" in label:
            return "wind"
    return None


def _validated_override(name: str, payload: Any) -> tuple[Any, str | None, dict[str, Any] | None]:
    """Return ``(value, reason, issue)`` for one user override."""
    if not isinstance(payload, dict):
        return None, None, _issue("invalid_override", "error", f"override for {name} must be an object "
                                  "with 'value' and 'reason'", field=name)
    reason = str(payload.get("reason") or "").strip()
    value = payload.get("value")
    if not reason:
        return None, None, _issue("override_without_reason", "error",
                                  f"override for {name} has no reason", field=name)
    if name not in OVERRIDABLE_FIELDS:
        return None, None, _issue("invalid_override", "error", f"{name} cannot be overridden", field=name)
    if name in {"capacity_max_mw", "capacity_min_mw"}:
        number, _ = parse_number(value)
        if number is None or number < 0:
            return None, None, _issue("invalid_override", "error",
                                      f"{name} override must be a non-negative number", field=name)
        return number, reason, None
    if name == "kind" and value not in ALLOWED_KINDS:
        return None, None, _issue("invalid_override", "error",
                                  f"kind must be one of {', '.join(ALLOWED_KINDS)}", field=name)
    if name == "id" and (not isinstance(value, str) or not _ID_RE.fullmatch(value)):
        return None, None, _issue("invalid_override", "error",
                                  "id must start with a letter or digit and use only letters, digits, '.', '_' or '-'",
                                  field=name)
    if name in {"name", "fuel_type"} and (not isinstance(value, str) or not value.strip()):
        return None, None, _issue("invalid_override", "error", f"{name} must be non-empty text", field=name)
    return (value.strip() if isinstance(value, str) else value), reason, None


def review_selection(catalogue: ReferenceCatalogue, selection: dict[str, Any], *,
                     existing_asset_ids: set[str] | frozenset[str] = frozenset()) -> dict[str, Any]:
    """Produce the field-level import review for one staged station record."""
    record_id = str(selection.get("record_id") or "")
    review: dict[str, Any] = {"record_id": record_id, "fields": [], "issues": [], "reference_metadata": {},
                              "proposed_asset": None, "blocking": True}
    try:
        table, record = catalogue.record(record_id)
    except KeyError:
        review["issues"].append(_issue("unknown_record", "error", f"unknown reference record {record_id!r}",
                                       record_id=record_id))
        return review
    values = record["values"]
    plant_name = values.get("plant_name")
    source = {"workbook": SOURCE_WORKBOOK, "sha256": catalogue.source_sha256, "sheet": record["source_sheet"],
              "row": record["source_row"], "record_id": record_id}
    review["source"] = source
    review["name"] = plant_name
    issues = review["issues"]
    if table != "stations":
        issues.append(_issue("not_a_plant_record", "error",
                             "only 'Stations by Location' plant records can be imported as assets; other tables "
                             "are corroborating reference data", record_id=record_id))
        return review
    if record.get("record_type") != "plant":
        issues.append(_issue("not_a_plant_record", "error",
                             f"'{plant_name}' is a {record['record_type'].replace('_', ' ')}, not a plant",
                             record_id=record_id))

    overrides: dict[str, dict[str, Any]] = {}
    for name, payload in sorted((selection.get("overrides") or {}).items()):
        value, reason, problem = _validated_override(name, payload)
        if problem:
            problem["record_id"] = record_id
            issues.append(problem)
        else:
            overrides[name] = {"value": value, "reason": reason}

    def cell(column: str) -> dict[str, Any]:
        return {**source, "column": column, "raw": values.get(column)}

    fields: list[dict[str, Any]] = []

    def put(target: str, value: Any, status: str, origin: str, *, source_ref: dict[str, Any] | None = None,
            note: str | None = None) -> Any:
        if target in overrides:
            fields.append({"target": target, "value": overrides[target]["value"], "status": "override",
                           "origin": "user_override", "reason": overrides[target]["reason"],
                           "proposed_value": value, "source_ref": source_ref, "note": note})
            return overrides[target]["value"]
        fields.append({"target": target, "value": value, "status": status, "origin": origin,
                       "source_ref": source_ref, "note": note})
        return value

    # name / kind / id
    name_value = put("name", plant_name, "mapped" if plant_name else "missing", "source",
                     source_ref=cell("plant_name"), note="stored as legacy_extensions.name")
    inferred = infer_kind(values.get("regulation_type"), plant_name)
    kind = put("kind", inferred, "derived" if inferred else "missing", "importer",
               source_ref=cell("regulation_type"),
               note=f"inferred from regulation type {values.get('regulation_type')!r}" if inferred else None)
    if kind is None:
        issues.append(_issue("missing_technology", "error",
                             f"'{plant_name}': regulation type {values.get('regulation_type')!r} does not identify "
                             "a PowerSim component", field="kind", record_id=record_id))
    elif "kind" not in overrides:
        issues.append(_issue("importer_assumption", "warning",
                             f"component kind '{kind}' was inferred from regulation type "
                             f"{values.get('regulation_type')!r}", field="kind", record_id=record_id))
    asset_id = put("id", _slug(str(name_value or record_id)), "derived", "importer",
                   note="derived from the plant name")

    # capacities
    installed, installed_note = parse_number(values.get("installed_capacity_mw"))
    capacity_max = put("capacity_max_mw", installed, "mapped" if installed is not None else "missing", "source",
                       source_ref=cell("installed_capacity_mw"),
                       note="installed (nameplate) capacity; available capacity kept as reference metadata")
    if installed_note:
        issues.append(_issue(installed_note, "warning", f"installed capacity {values.get('installed_capacity_mw')!r}",
                             field="capacity_max_mw", record_id=record_id))
    if "capacity_max_mw" not in overrides and installed is not None:
        issues.append(_issue("importer_assumption", "warning",
                             "capacity_max_mw uses installed capacity, not available capacity "
                             f"({values.get('available_capacity_mw')!r} MW)", field="capacity_max_mw",
                             record_id=record_id))
    capacity_min = put("capacity_min_mw", 0.0, "assumed", "importer",
                       note="no minimum stable output in the source")
    if "capacity_min_mw" not in overrides:
        issues.append(_issue("importer_assumption", "warning", "capacity_min_mw assumed to be 0 MW",
                             field="capacity_min_mw", record_id=record_id))
    if capacity_max is not None and capacity_min is not None and capacity_min > capacity_max:
        issues.append(_issue("invalid_override", "error", "capacity_min_mw cannot exceed capacity_max_mw",
                             field="capacity_min_mw", record_id=record_id))

    # fuel
    fuel = None
    if kind == "thermal":
        fuel = put("fuel_type", None, "missing", "source", note="the workbook has no fuel column")
        if fuel is None:
            issues.append(_issue("missing_fuel", "error", f"'{plant_name}': thermal plants require a fuel type",
                                 field="fuel_type", record_id=record_id))
    fields.append({"target": "enabled", "value": False, "status": "assumed", "origin": "importer",
                   "source_ref": None, "note": "reference assets are imported disabled until reviewed"})
    fields.append({"target": "bus", "value": None, "status": "not_in_source", "origin": "importer",
                   "source_ref": None, "note": "copper-plate; assign a bus manually if a network is modelled"})
    issues.append(_issue("importer_assumption", "warning",
                         "asset is imported disabled with review_status 'requires_user_review'", field="enabled",
                         record_id=record_id))
    if kind in REQUIRED_STUDY_INPUTS:
        missing_inputs = REQUIRED_STUDY_INPUTS[kind]
        fields.append({"target": "study_inputs", "value": None, "status": "unmapped_required", "origin": "importer",
                       "source_ref": None, "note": "; ".join(missing_inputs)})
        issues.append(_issue("unmapped_required_field", "warning",
                             f"not in the workbook and required before enabling: {', '.join(missing_inputs)}",
                             field="study_inputs", record_id=record_id, context={"missing": missing_inputs}))

    # catalogue-level evidence for this record
    capacity_overridden = "capacity_max_mw" in overrides
    for quality in catalogue.issues_for(record_id):
        entry = quality.as_dict()
        entry.pop("table", None)
        if entry["code"] == "not_a_plant_record":
            continue  # already reported as an error above
        if entry["code"] in _CAPACITY_CODES and capacity_overridden:
            entry["severity"] = "resolved"
            entry["resolution"] = overrides["capacity_max_mw"]["reason"]
        issues.append(entry)
    if not capacity_overridden and any(i["code"] in _CAPACITY_CODES and i["severity"] == "error" for i in issues):
        for entry in fields:
            if entry["target"] == "capacity_max_mw":
                entry["status"] = "conflict"
    units = parse_units(values.get("generating_units"))
    if units is None and kind in {"thermal", "hydro_ror", "hydro_reg"}:
        issues.append(_issue("missing_unit_configuration", "warning",
                             f"'{plant_name}': no generating-unit configuration in the source",
                             field="generating_units", record_id=record_id))
    evidence = catalogue.capacity_evidence(record_id)
    hydro_matches = [m for m in evidence["matches"] if m["table"] == "hydropower"]
    if len(hydro_matches) > 1:
        issues.append(_issue("duplicate_corroborating_records", "warning",
                             f"'{plant_name}' matches {len(hydro_matches)} hydropower-table records",
                             record_id=record_id, context={"matches": hydro_matches}))

    metadata = {key: value for key, value in values.items()
                if key not in {"plant_name", "installed_capacity_mw"} and value is not None}
    if units is not None:
        metadata["parsed_generating_units"] = {"raw": units.raw, "groups": [list(g) for g in units.groups],
                                               "unit_count": units.unit_count if units.groups else None,
                                               "unit_sum_mw": units.total_mw if units.groups else None,
                                               "notes": list(units.notes)}
    metadata["section"] = record.get("section")
    metadata["capacity_evidence"] = evidence["evidence"]
    metadata["matched_records"] = catalogue.matches(plant_name)
    review["reference_metadata"] = metadata
    review["fields"] = fields
    review["overrides"] = overrides

    if isinstance(asset_id, str) and asset_id in existing_asset_ids:
        issues.append(_issue("asset_id_exists", "error", f"asset id {asset_id!r} already exists in the project",
                             field="id", record_id=record_id))
    review["asset_id"] = asset_id
    review["kind"] = kind
    review["errors"] = sum(i["severity"] == "error" for i in issues)
    review["warnings"] = sum(i["severity"] == "warning" for i in issues)
    review["blocking"] = review["errors"] > 0
    if not review["blocking"]:
        legacy = {"name": name_value}
        if fuel is not None:
            legacy["fuel_type"] = fuel
        review["proposed_asset"] = {
            "id": asset_id, "kind": kind, "enabled": False, "capacity_min_mw": capacity_min,
            "capacity_max_mw": capacity_max, "legacy_extensions": legacy,
        }
    return review


def review_import(catalogue: ReferenceCatalogue, selections: list[dict[str, Any]], *,
                  existing_asset_ids: set[str] | frozenset[str] = frozenset()) -> dict[str, Any]:
    reviews, seen_records, seen_ids = [], set(), set()
    for selection in selections:
        review = review_selection(catalogue, selection, existing_asset_ids=existing_asset_ids)
        if review["record_id"] in seen_records:
            review["issues"].append(_issue("duplicate_selection", "error", "record staged more than once",
                                           record_id=review["record_id"]))
        seen_records.add(review["record_id"])
        asset_id = review.get("asset_id")
        if asset_id and asset_id in seen_ids:
            review["issues"].append(_issue("asset_id_exists", "error",
                                           f"asset id {asset_id!r} is used by another staged record",
                                           field="id", record_id=review["record_id"]))
        if asset_id:
            seen_ids.add(asset_id)
        review["errors"] = sum(i["severity"] == "error" for i in review["issues"])
        review["warnings"] = sum(i["severity"] == "warning" for i in review["issues"])
        review["blocking"] = review["errors"] > 0
        if review["blocking"]:
            review["proposed_asset"] = None
        reviews.append(review)
    return {"source_workbook": SOURCE_WORKBOOK, "source_sha256": catalogue.source_sha256,
            "importer_version": IMPORTER_VERSION, "reviews": reviews,
            "errors": sum(r["errors"] for r in reviews), "warnings": sum(r["warnings"] for r in reviews),
            "importable": bool(reviews) and not any(r["blocking"] for r in reviews)}


def apply_import(project: ProjectContract, catalogue: ReferenceCatalogue, selections: list[dict[str, Any]], *,
                 acknowledge_warnings: bool, reviewer: str | None = None,
                 now: datetime | None = None) -> tuple[ProjectContract, dict[str, Any]]:
    """Append reviewed reference assets to a new project revision, or raise."""
    if not selections:
        raise ImportRejected("no records were staged", [])
    existing = {asset.id for asset in project.assets}
    report = review_import(catalogue, selections, existing_asset_ids=existing)
    blocking = [i for r in report["reviews"] for i in r["issues"] if i["severity"] == "error"]
    if blocking:
        raise ImportRejected("the staged import has unresolved blocking issues", blocking)
    warnings = [i for r in report["reviews"] for i in r["issues"] if i["severity"] == "warning"]
    if warnings and not acknowledge_warnings:
        raise ImportRejected("review warnings must be acknowledged before import", warnings)
    timestamp = (now or datetime.now(timezone.utc)).isoformat()
    import_id = str(uuid.uuid4())
    payload = project.model_dump(mode="json")
    new_assets = []
    for review in report["reviews"]:
        proposed = review["proposed_asset"]
        details = {
            "import_id": import_id, "imported_at": timestamp, "importer_version": IMPORTER_VERSION,
            "source_workbook": SOURCE_WORKBOOK, "source_sha256": catalogue.source_sha256,
            "source_sheet": review["source"]["sheet"], "source_row": review["source"]["row"],
            "source_record_id": review["record_id"],
            "review_status": "requires_user_review",
            "field_origins": {f["target"]: {k: f.get(k) for k in ("status", "origin", "reason", "note")}
                              for f in review["fields"]},
            "overrides": review["overrides"],
            "acknowledged_warnings": sorted({i["code"] for i in review["issues"] if i["severity"] == "warning"}),
            "resolved_issues": [i for i in review["issues"] if i["severity"] == "resolved"],
            "reference_metadata": review["reference_metadata"],
        }
        if reviewer:
            details["imported_by"] = reviewer
        asset = AssetContract.model_validate({
            **proposed,
            "provenance": Provenance(source=PROVENANCE_SOURCE, source_version=catalogue.source_sha256,
                                     transformation=IMPORTER_VERSION, details=details).model_dump(mode="json"),
        })
        new_assets.append(asset.model_dump(mode="json"))
    payload["assets"] = list(payload["assets"]) + new_assets
    payload["version"] = {**payload["version"], "revision": int(payload["version"]["revision"]) + 1,
                          "parent_fingerprint": project.fingerprint()}
    metadata = dict(payload.get("metadata") or {})
    history = list(metadata.get("reference_imports") or [])
    history.append({"import_id": import_id, "imported_at": timestamp, "source_workbook": SOURCE_WORKBOOK,
                    "source_sha256": catalogue.source_sha256, "importer_version": IMPORTER_VERSION,
                    "records": [r["record_id"] for r in report["reviews"]],
                    "asset_ids": [a["id"] for a in new_assets],
                    "overrides": {r["record_id"]: r["overrides"] for r in report["reviews"] if r["overrides"]},
                    "acknowledged_warning_count": len(warnings), "imported_by": reviewer})
    metadata["reference_imports"] = history
    payload["metadata"] = metadata
    updated = ProjectContract.model_validate(payload)
    return updated, {"import_id": import_id, "imported_at": timestamp,
                     "asset_ids": [a["id"] for a in new_assets], "review": report}
