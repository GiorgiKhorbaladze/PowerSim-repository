"""Bulk asset-parameter editing through an Excel workbook.

The workbook is generated from the saved project (one row per asset plus the
project's profiles), filled in by the user with approved data, and uploaded
back.  Reading it is fail-closed and auditable:

* only existing assets can be edited (assets are added in the Assets page or
  through the reference import); identity columns cannot change;
* an empty cell means "leave unchanged" - nothing is cleared implicitly;
* every number must be finite and non-negative, kinds and booleans must be
  valid, profile references must resolve, and profile columns must have
  exactly ``time.periods`` or 8,760 values;
* marking a reference-catalogue asset reviewed requires a review note;
* the result is a change list (asset, field, old, new) that the user checks
  before it is applied as a new project revision with an import record.

Nothing here changes solver semantics: it only edits the typed project.
"""
from __future__ import annotations

import hashlib
import io
import math
from datetime import datetime, timezone
from typing import Any

from powersim.contracts import ProjectContract

ASSET_KINDS = ("thermal", "import", "hydro_ror", "hydro_reg", "pumped_hydro", "solar", "wind", "bess", "dr")
PROFILE_KINDS = ("wind", "solar", "hydro_ror")
REFERENCE_SOURCE = "powersim.reference_catalogue"
MAX_WORKBOOK_BYTES = 25 * 1024 * 1024

# (column header, description, target)
ASSET_COLUMNS = [
    ("asset_id", "Asset id (identity - do not change)", None),
    ("kind", "Component kind: " + ", ".join(ASSET_KINDS), "kind"),
    ("name", "Display name", "ext:name"),
    ("enabled", "TRUE to include the asset in runs, FALSE to exclude it", "enabled"),
    ("capacity_min_mw", "Minimum output, MW", "capacity_min_mw"),
    ("capacity_max_mw", "Maximum output / installed capacity, MW", "capacity_max_mw"),
    ("fuel_type", "Fuel type (thermal), for example natural gas, coal", "ext:fuel_type"),
    ("heat_rate_gj_per_mwh", "Heat rate, GJ/MWh (thermal)", "ext:heat_rate"),
    ("fuel_price_usd_per_mmbtu", "Fuel price, USD/MMBtu (thermal)", "ext:fuel_price"),
    ("vom_per_mwh", "Variable O&M or import price, currency/MWh", "ext:vom"),
    ("availability_profile", "Profile id for wind/solar/run-of-river availability (0-1 per period)",
     "profile_references"),
    ("inflow_profile", "Profile id for reservoir-hydro inflow (Mm3/h)", "ext:inflow_profile"),
    ("reviewed", "yes = parameters checked against approved sources (reference assets)", "review"),
    ("review_note", "What was checked, against which source (required with reviewed = yes)", "review_note"),
    ("source", "Read only: where the asset came from", None),
]
NUMERIC = {"capacity_min_mw", "capacity_max_mw", "heat_rate_gj_per_mwh", "fuel_price_usd_per_mmbtu", "vom_per_mwh"}
TRUE_WORDS = {"true", "yes", "y", "1", "კი", "დიახ"}
FALSE_WORDS = {"false", "no", "n", "0", "არა"}


class WorkbookRejected(ValueError):
    def __init__(self, message: str, issues: list[dict[str, Any]]):
        super().__init__(message)
        self.issues = issues


def _issue(code: str, severity: str, message: str, *, sheet: str | None = None, row: int | None = None,
           column: str | None = None) -> dict[str, Any]:
    return {"code": code, "severity": severity, "message": message, "sheet": sheet, "row": row,
            "column": column, "path": f"{sheet}!{column or ''}{row or ''}" if sheet else None}


def _source_label(asset: dict[str, Any]) -> str:
    provenance = asset.get("provenance") or {}
    if provenance.get("source") == REFERENCE_SOURCE:
        details = provenance.get("details") or {}
        status = "reviewed" if details.get("review_status") == "user_reviewed" else "REQUIRES REVIEW"
        return f"Reference catalogue {details.get('source_sheet')} row {details.get('source_row')} ({status})"
    return provenance.get("source") or "project"


def build_template(project: ProjectContract) -> bytes:
    """Excel template prefilled with the project's assets and profiles."""
    import openpyxl
    from openpyxl.styles import Font, PatternFill
    from openpyxl.worksheet.datavalidation import DataValidation

    payload = project.model_dump(mode="json")
    workbook = openpyxl.Workbook()
    info = workbook.active
    info.title = "Instructions"
    lines = [
        f"PowerSim asset parameter workbook - project '{project.id}' revision {project.version.revision}",
        "",
        "1. Edit the 'Assets' sheet: one row per existing asset. Do not change asset_id.",
        "2. An empty cell leaves the current value unchanged. Nothing is cleared automatically.",
        "3. Numbers must be non-negative. enabled and reviewed accept TRUE/FALSE or yes/no.",
        "4. Profiles: in the 'Profiles' sheet, add one column per profile; the header is the profile id.",
        f"   Each profile needs exactly {project.time.periods} values (time.periods) or 8760 hourly values.",
        "   A column with an existing profile id replaces that profile.",
        "5. Reference to a profile in availability_profile / inflow_profile by its id.",
        "6. Reference-catalogue assets can only be enabled for runs after reviewed = yes with a review note.",
        "7. Upload the file on the Assets page, check the change list, then apply it.",
        "",
        "Enter only approved study data. The reference catalogue is not approved study data.",
        "Reservoir hydro also needs a reservoir model (legacy_extensions.hydro) edited in the project document.",
    ]
    for line in lines:
        info.append([line])
    info.column_dimensions["A"].width = 110
    info["A1"].font = Font(bold=True, size=13)

    sheet = workbook.create_sheet("Assets")
    sheet.append([name for name, _, _ in ASSET_COLUMNS])
    sheet.append([description for _, description, _ in ASSET_COLUMNS])
    header_fill = PatternFill("solid", fgColor="DCE6F1")
    for cell in sheet[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
    for cell in sheet[2]:
        cell.font = Font(italic=True, color="666666")
    for asset in payload["assets"]:
        ext = asset.get("legacy_extensions") or {}
        details = (asset.get("provenance") or {}).get("details") or {}
        sheet.append([
            asset["id"], asset["kind"], ext.get("name"), bool(asset.get("enabled", True)),
            asset.get("capacity_min_mw"), asset.get("capacity_max_mw"), ext.get("fuel_type"),
            ext.get("heat_rate"), ext.get("fuel_price"), ext.get("vom"),
            (asset.get("profile_references") or [None])[0], ext.get("inflow_profile"),
            "yes" if details.get("review_status") == "user_reviewed" else None, details.get("review_note"),
            _source_label(asset),
        ])
    for index, (name, _, _) in enumerate(ASSET_COLUMNS, start=1):
        sheet.column_dimensions[openpyxl.utils.get_column_letter(index)].width = max(14, len(name) + 4)
    sheet.freeze_panes = "B3"
    last = max(3, sheet.max_row)
    kinds = DataValidation(type="list", formula1='"' + ",".join(ASSET_KINDS) + '"', allow_blank=True)
    booleans = DataValidation(type="list", formula1='"TRUE,FALSE"', allow_blank=True)
    review = DataValidation(type="list", formula1='"yes,no"', allow_blank=True)
    for validation, column in ((kinds, "B"), (booleans, "D"), (review, "M")):
        sheet.add_data_validation(validation)
        validation.add(f"{column}3:{column}{last}")

    profiles = workbook.create_sheet("Profiles")
    existing = payload.get("profiles") or []
    profiles.append(["period"] + [p["id"] for p in existing])
    for cell in profiles[1]:
        cell.font = Font(bold=True)
        cell.fill = header_fill
    length = max([project.time.periods] + [len(p["values"]) for p in existing])
    for index in range(length):
        profiles.append([index + 1] + [p["values"][index] if index < len(p["values"]) else None for p in existing])
    profiles.freeze_panes = "B2"

    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _number(raw: Any) -> tuple[float | None, str | None]:
    if isinstance(raw, bool):
        return None, "must be a number"
    if isinstance(raw, (int, float)):
        value = float(raw)
    else:
        text = str(raw).strip().replace(" ", "")
        if text.count(",") == 1 and "." not in text:
            text = text.replace(",", ".")
        try:
            value = float(text)
        except ValueError:
            return None, "must be a number"
    if not math.isfinite(value) or value < 0:
        return None, "must be a finite, non-negative number"
    return value, None


def _boolean(raw: Any) -> bool | None:
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in TRUE_WORDS:
        return True
    if text in FALSE_WORDS:
        return False
    return None


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def read_workbook(project: ProjectContract, content: bytes) -> dict[str, Any]:
    """Parse and check an uploaded workbook; return the proposed changes."""
    import openpyxl

    issues: list[dict[str, Any]] = []
    report: dict[str, Any] = {"file_sha256": "sha256:" + hashlib.sha256(content).hexdigest(), "changes": [],
                              "profiles": [], "issues": issues}
    if len(content) > MAX_WORKBOOK_BYTES:
        issues.append(_issue("file_too_large", "error", "the workbook is larger than 25 MB"))
        return _finish(report)
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:  # zip/xml errors from a non-xlsx upload
        issues.append(_issue("not_an_xlsx_workbook", "error", f"the file is not a readable .xlsx workbook ({exc.__class__.__name__})"))
        return _finish(report)
    if "Assets" not in workbook.sheetnames:
        issues.append(_issue("missing_sheet", "error", "the workbook has no 'Assets' sheet; download a new template"))
        return _finish(report)

    payload = project.model_dump(mode="json")
    assets = {asset["id"]: asset for asset in payload["assets"]}
    profile_ids = {p["id"] for p in payload.get("profiles") or []}

    # profiles first, so asset rows may reference new profiles
    new_profiles: dict[str, list[float]] = {}
    if "Profiles" in workbook.sheetnames:
        rows = list(workbook["Profiles"].iter_rows(values_only=True))
        header = list(rows[0]) if rows else []
        for column, profile_id in enumerate(header[1:], start=1):
            if _blank(profile_id):
                continue
            profile_id = str(profile_id).strip()
            values, bad = [], None
            for row_number, row in enumerate(rows[1:], start=2):
                cell = row[column] if column < len(row) else None
                if _blank(cell):
                    continue
                if isinstance(cell, bool) or not isinstance(cell, (int, float)) and _number(cell)[0] is None:
                    bad = row_number
                    break
                value = float(cell) if isinstance(cell, (int, float)) else _number(cell)[0]
                if not math.isfinite(value):
                    bad = row_number
                    break
                values.append(value)
            letter = openpyxl.utils.get_column_letter(column + 1)
            if bad is not None:
                issues.append(_issue("invalid_profile_value", "error", f"profile '{profile_id}' has a non-numeric value",
                                     sheet="Profiles", row=bad, column=letter))
                continue
            if len(values) not in {project.time.periods, 8760}:
                issues.append(_issue("profile_length_mismatch", "error",
                                     f"profile '{profile_id}' has {len(values)} values; expected {project.time.periods} "
                                     "(time.periods) or 8760", sheet="Profiles", row=1, column=letter))
                continue
            existing = next((p for p in payload.get("profiles") or [] if p["id"] == profile_id), None)
            if existing and [float(v) for v in existing["values"]] == values:
                continue
            new_profiles[profile_id] = values
            report["profiles"].append({"id": profile_id, "values": len(values),
                                       "action": "replaced" if existing else "added"})
    known_profiles = profile_ids | set(new_profiles)

    rows = list(workbook["Assets"].iter_rows(values_only=True))
    header = [str(h).strip() if h is not None else "" for h in (rows[0] if rows else [])]
    missing = [name for name, _, _ in ASSET_COLUMNS if name not in header]
    if "asset_id" in missing:
        issues.append(_issue("missing_column", "error", "the 'Assets' sheet has no asset_id column", sheet="Assets", row=1))
        return _finish(report)
    position = {name: header.index(name) for name, _, _ in ASSET_COLUMNS if name in header}
    seen: set[str] = set()
    updates: dict[str, dict[str, Any]] = {}
    for row_number, row in enumerate(rows[1:], start=2):
        def cell(name: str) -> Any:
            index = position.get(name)
            return row[index] if index is not None and index < len(row) else None
        asset_id = cell("asset_id")
        if _blank(asset_id):
            continue
        asset_id = str(asset_id).strip()
        if row_number == 2 and asset_id.startswith("Asset id"):
            continue  # description row
        if asset_id in seen:
            issues.append(_issue("duplicate_asset_row", "error", f"asset '{asset_id}' appears more than once",
                                 sheet="Assets", row=row_number, column="A"))
            continue
        seen.add(asset_id)
        if asset_id not in assets:
            issues.append(_issue("unknown_asset", "error",
                                 f"asset '{asset_id}' is not in the project; add new assets on the Assets page first",
                                 sheet="Assets", row=row_number, column="A"))
            continue
        asset = assets[asset_id]
        ext = asset.get("legacy_extensions") or {}
        proposed: dict[str, Any] = {}

        def col(name: str) -> str:
            return openpyxl.utils.get_column_letter(position[name] + 1)

        for name, _, target in ASSET_COLUMNS:
            if target is None or name not in position or name in {"reviewed", "review_note"}:
                continue
            raw = cell(name)
            if _blank(raw):
                continue
            if name in NUMERIC:
                value, problem = _number(raw)
                if problem:
                    issues.append(_issue("invalid_number", "error", f"{name} {problem} (got {raw!r})",
                                         sheet="Assets", row=row_number, column=col(name)))
                    continue
            elif name == "enabled":
                value = _boolean(raw)
                if value is None:
                    issues.append(_issue("invalid_boolean", "error", f"enabled must be TRUE or FALSE (got {raw!r})",
                                         sheet="Assets", row=row_number, column=col(name)))
                    continue
            elif name == "kind":
                value = str(raw).strip()
                if value not in ASSET_KINDS:
                    issues.append(_issue("invalid_kind", "error", f"kind must be one of {', '.join(ASSET_KINDS)}",
                                         sheet="Assets", row=row_number, column=col(name)))
                    continue
            else:
                value = str(raw).strip()
            if name in {"availability_profile", "inflow_profile"} and value not in known_profiles:
                issues.append(_issue("unknown_profile", "error",
                                     f"profile '{value}' is not in the project or the Profiles sheet",
                                     sheet="Assets", row=row_number, column=col(name)))
                continue
            if name == "availability_profile":
                current = (asset.get("profile_references") or [None])[0]
            elif target.startswith("ext:"):
                current = ext.get(target[4:])
            else:
                current = asset.get(target)
            if isinstance(current, (int, float)) and not isinstance(current, bool) and isinstance(value, float):
                same = math.isclose(float(current), value, rel_tol=0, abs_tol=1e-9)
            else:
                same = current == value
            if not same:
                proposed[name] = (current, value)

        kind = proposed.get("kind", (None, asset["kind"]))[1]
        if "availability_profile" in proposed and kind not in PROFILE_KINDS:
            issues.append(_issue("profile_not_applicable", "error",
                                 f"availability_profile applies to {', '.join(PROFILE_KINDS)}, not {kind}",
                                 sheet="Assets", row=row_number, column=col("availability_profile")))
            proposed.pop("availability_profile")
        low = proposed.get("capacity_min_mw", (None, asset.get("capacity_min_mw")))[1]
        high = proposed.get("capacity_max_mw", (None, asset.get("capacity_max_mw")))[1]
        if low is not None and high is not None and low > high:
            issues.append(_issue("capacity_order", "error", "capacity_min_mw cannot exceed capacity_max_mw",
                                 sheet="Assets", row=row_number, column=col("capacity_min_mw") if "capacity_min_mw" in position else None))

        provenance = asset.get("provenance") or {}
        is_reference = provenance.get("source") == REFERENCE_SOURCE
        details = provenance.get("details") or {}
        reviewed_raw, note = cell("reviewed"), cell("review_note")
        if not _blank(reviewed_raw):
            reviewed = _boolean(reviewed_raw)
            if reviewed is None:
                issues.append(_issue("invalid_boolean", "error", f"reviewed must be yes or no (got {reviewed_raw!r})",
                                     sheet="Assets", row=row_number, column=col("reviewed")))
            elif reviewed and details.get("review_status") != "user_reviewed":
                if not is_reference:
                    issues.append(_issue("review_not_applicable", "info",
                                         "reviewed applies to reference-catalogue assets only; ignored",
                                         sheet="Assets", row=row_number, column=col("reviewed")))
                elif _blank(note):
                    issues.append(_issue("review_note_required", "error",
                                         "reviewed = yes needs a review_note describing what was checked",
                                         sheet="Assets", row=row_number, column=col("review_note")))
                else:
                    proposed["reviewed"] = (details.get("review_status"), "user_reviewed")
                    proposed["review_note"] = (details.get("review_note"), str(note).strip())
            elif reviewed is False and details.get("review_status") == "user_reviewed":
                proposed["reviewed"] = ("user_reviewed", "requires_user_review")
        enabled_after = proposed.get("enabled", (None, asset.get("enabled", True)))[1]
        status_after = proposed.get("reviewed", (None, details.get("review_status")))[1]
        if is_reference and enabled_after and status_after != "user_reviewed":
            issues.append(_issue("reference_asset_unreviewed", "warning",
                                 "this reference asset will be enabled but is not reviewed; runs stay blocked until "
                                 "reviewed = yes with a review note", sheet="Assets", row=row_number))
        if proposed:
            updates[asset_id] = proposed
            for field, (old, new) in proposed.items():
                report["changes"].append({"asset_id": asset_id, "field": field, "old": old, "new": new,
                                          "row": row_number})
    referenced = {c["new"] for c in report["changes"] if c["field"] == "availability_profile"}
    for profile_id, values in new_profiles.items():
        if profile_id in referenced and any(v > 1 for v in values):
            issues.append(_issue("availability_above_one", "warning",
                                 f"availability profile '{profile_id}' has values above 1; the solver clamps "
                                 "availability to 0-1, so it must be a capacity factor, not MW", sheet="Profiles"))
    for name in missing:
        issues.append(_issue("missing_column", "info", f"column '{name}' is not in the sheet; it is left unchanged",
                             sheet="Assets", row=1))
    report["_updates"] = updates
    report["_profiles"] = new_profiles
    return _finish(report)


def _finish(report: dict[str, Any]) -> dict[str, Any]:
    issues = report["issues"]
    report["errors"] = sum(i["severity"] == "error" for i in issues)
    report["warnings"] = sum(i["severity"] == "warning" for i in issues)
    report["applicable"] = report["errors"] == 0 and bool(report["changes"] or report["profiles"])
    return report


def public_report(report: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in report.items() if not key.startswith("_")}


def apply_workbook(project: ProjectContract, content: bytes, *, file_name: str | None = None,
                   imported_by: str | None = None, now: datetime | None = None) -> tuple[ProjectContract, dict[str, Any]]:
    report = read_workbook(project, content)
    if report["errors"]:
        raise WorkbookRejected("the workbook has errors and was not applied",
                               [i for i in report["issues"] if i["severity"] == "error"])
    if not report["applicable"]:
        raise WorkbookRejected("the workbook contains no changes", [])
    timestamp = (now or datetime.now(timezone.utc)).isoformat()
    payload = project.model_dump(mode="json")
    profiles = {p["id"]: p for p in payload.get("profiles") or []}
    for profile_id, values in report["_profiles"].items():
        if profile_id in profiles:
            profiles[profile_id] = {**profiles[profile_id], "values": values}
        else:
            usage = {c["field"] for c in report["changes"] if c["new"] == profile_id}
            unit = "Mm3/h" if "inflow_profile" in usage else ("per-unit" if "availability_profile" in usage else "unspecified")
            profiles[profile_id] = {"id": profile_id, "unit": unit, "values": values,
                                    "provenance": {"source": "parameter_workbook", "source_version": report["file_sha256"],
                                                   "transformation": None, "details": {"imported_at": timestamp}}}
    payload["profiles"] = list(profiles.values())
    targets = {name: target for name, _, target in ASSET_COLUMNS}
    for asset in payload["assets"]:
        changes = report["_updates"].get(asset["id"])
        if not changes:
            continue
        ext = dict(asset.get("legacy_extensions") or {})
        provenance = asset.get("provenance")
        for field, (_, new) in changes.items():
            target = targets[field]
            if field == "availability_profile":
                asset["profile_references"] = [new] + [p for p in asset.get("profile_references") or [] if p != new]
            elif target.startswith("ext:"):
                ext[target[4:]] = new
            elif field in {"reviewed", "review_note"}:
                continue
            else:
                asset[target] = new
        # keep legacy compatibility keys consistent with the typed fields
        for key, typed in (("type", "kind"), ("pmin", "capacity_min_mw"), ("pmax", "capacity_max_mw")):
            if key in ext and asset.get(typed) is not None:
                ext[key] = asset[typed]
        asset["legacy_extensions"] = ext
        if provenance and provenance.get("source") == REFERENCE_SOURCE:
            details = dict(provenance.get("details") or {})
            origins = dict(details.get("field_origins") or {})
            for field in changes:
                if field not in {"reviewed", "review_note"}:
                    origins[field] = {"status": "override", "origin": "parameter_workbook",
                                      "reason": f"workbook {report['file_sha256'][:19]}", "note": timestamp}
            details["field_origins"] = origins
            if "reviewed" in changes:
                details["review_status"] = changes["reviewed"][1]
                if changes["reviewed"][1] == "user_reviewed":
                    details["reviewed_at"] = timestamp
                    details["review_note"] = changes["review_note"][1]
                    if imported_by:
                        details["reviewed_by"] = imported_by
            asset["provenance"] = {**provenance, "details": details}
    payload["version"] = {**payload["version"], "revision": int(payload["version"]["revision"]) + 1,
                          "parent_fingerprint": project.fingerprint()}
    metadata = dict(payload.get("metadata") or {})
    history = list(metadata.get("parameter_imports") or [])
    history.append({"imported_at": timestamp, "file_name": file_name, "file_sha256": report["file_sha256"],
                    "changes": [{k: c[k] for k in ("asset_id", "field", "old", "new")} for c in report["changes"]],
                    "profiles": report["profiles"], "imported_by": imported_by})
    metadata["parameter_imports"] = history
    payload["metadata"] = metadata
    updated = ProjectContract.model_validate(payload)
    return updated, public_report(report)
