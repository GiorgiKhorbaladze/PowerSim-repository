"""Project diagnostics for the application: contract, references and run readiness.

``validate_contract`` answers "is this a well-formed project?".  This module
adds the study-readiness rules the application enforces before a run snapshot
is created, and attaches remediation text for the UI.  Readiness rules never
modify the project; they only report.
"""
from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from powersim.contracts import ProjectContract, resolve_scenario
from powersim.contracts.errors import ScenarioOverlayError
from .contract_validation import pydantic_issues, validate_contract

REFERENCE_SOURCE = "powersim.reference_catalogue"
_PROFILE_KINDS = {"wind", "solar", "hydro_ror"}

REMEDIATION = {
    "invalid_contract_value": "Correct the value at this path; the project document does not match the typed contract.",
    "unsupported_contract_version": "Migrate the project to the current contract version.",
    "duplicate_asset_id": "Give every asset a unique id.",
    "duplicate_profile_id": "Give every profile a unique id.",
    "duplicate_bus_id": "Give every bus a unique id.",
    "duplicate_branch_id": "Give every branch a unique id.",
    "dangling_profile_reference": "Add the referenced profile or remove the reference.",
    "dangling_bus_reference": "Add the referenced bus or clear the asset/branch bus.",
    "no_enabled_assets": "Enable at least one asset so the study has supply.",
    "reference_asset_unreviewed": ("Reference-catalogue values must be reviewed by a user before they are used in a "
                                   "study. Open the asset, check its parameters and mark it reviewed, or disable it."),
    "missing_cost_basis": "Add heat_rate and fuel_price, or vom, to the asset's legacy_extensions.",
    "missing_availability_profile": "Attach an hourly availability profile in profile_references.",
    "solver_default_availability": ("Without an availability profile the solver uses its default availability "
                                    "(wind/solar 1.0, run-of-river 0.65). Attach a profile for a real study."),
    "missing_reservoir_model": ("Reservoir hydro needs an inflow profile and reservoir parameters in "
                                "legacy_extensions.hydro before it can be enabled."),
    "scenario_resolution_failed": "Correct the scenario overlay so it resolves against the project.",
    "protected_scenario_overlay": "Scenario overlays cannot change project identity or version lineage.",
    "reference_asset_disabled": "The asset is part of the project but excluded from runs until enabled.",
    "profile_length_mismatch": ("Give the profile exactly time.periods values, 8760 hourly values, or one full year "
                                "at the study resolution."),
}


def _entry(code: str, severity: str, path: str, message: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"code": code, "severity": severity, "path": path, "message": message,
            "remediation": REMEDIATION.get(code), "context": context or {}}


def _from_issue(issue, prefix: str = "") -> dict[str, Any]:
    data = issue.model_dump(mode="json")
    data["path"] = prefix + data["path"]
    data["remediation"] = REMEDIATION.get(data["code"])
    return data


def readiness_issues(value, prefix: str = "") -> list[dict[str, Any]]:
    """Study-readiness rules for a project or a resolved scenario snapshot."""
    issues: list[dict[str, Any]] = []
    enabled = [asset for asset in value.assets if asset.enabled]
    if not enabled:
        issues.append(_entry("no_enabled_assets", "error", prefix + "assets", "no asset is enabled"))
    if 60 % value.time.resolution_minutes == 0:
        accepted = {value.time.periods, 8760, 8760 * (60 // value.time.resolution_minutes)}
        for index, profile in enumerate(value.profiles):
            if len(profile.values) not in accepted:
                issues.append(_entry("profile_length_mismatch", "error", f"{prefix}profiles[{index}].values",
                                     f"profile {profile.id!r} has {len(profile.values)} values; expected "
                                     f"{value.time.periods} (time.periods) or a full year",
                                     {"profile_id": profile.id, "length": len(profile.values)}))
    for index, asset in enumerate(value.assets):
        path = f"{prefix}assets[{index}]"
        provenance = asset.provenance
        from_reference = provenance is not None and provenance.source == REFERENCE_SOURCE
        details = dict(provenance.details) if from_reference else {}
        extensions = dict(asset.legacy_extensions)
        if not asset.enabled:
            if from_reference:
                issues.append(_entry("reference_asset_disabled", "info", path,
                                     f"reference asset {asset.id!r} is disabled and excluded from runs",
                                     {"asset_id": asset.id}))
            continue
        if from_reference and details.get("review_status") != "user_reviewed":
            issues.append(_entry("reference_asset_unreviewed", "error", path + ".provenance.details.review_status",
                                 f"asset {asset.id!r} uses unreviewed reference-catalogue values",
                                 {"asset_id": asset.id, "review_status": details.get("review_status")}))
        if from_reference and asset.kind == "thermal":
            has_fuel_cost = float(extensions.get("heat_rate") or 0) > 0 and float(extensions.get("fuel_price") or 0) > 0
            if not has_fuel_cost and float(extensions.get("vom") or 0) <= 0 and "mc" not in extensions:
                issues.append(_entry("missing_cost_basis", "error", path + ".legacy_extensions",
                                     f"thermal asset {asset.id!r} has no cost basis; the solver would dispatch it "
                                     "at zero cost", {"asset_id": asset.id}))
        if asset.kind in _PROFILE_KINDS and not asset.profile_references and not extensions.get("availability_profile"):
            issues.append(_entry("missing_availability_profile" if from_reference else "solver_default_availability",
                                 "error" if from_reference else "warning", path + ".profile_references",
                                 f"{asset.kind} asset {asset.id!r} has no availability profile",
                                 {"asset_id": asset.id}))
        if from_reference and asset.kind == "hydro_reg":
            hydro = extensions.get("hydro")
            if not isinstance(hydro, dict) or not (extensions.get("inflow_profile") or hydro.get("inflow")):
                issues.append(_entry("missing_reservoir_model", "error", path + ".legacy_extensions.hydro",
                                     f"reservoir hydro asset {asset.id!r} has no inflow/reservoir model",
                                     {"asset_id": asset.id}))
    return issues


def diagnose_project(payload: Any) -> dict[str, Any]:
    """Return structured diagnostics for a raw project document.

    ``contract_valid`` gates saving; ``run_ready`` gates run creation.
    """
    if not isinstance(payload, dict):
        issue = _entry("invalid_contract_value", "error", "", "project document must be a JSON object")
        return {"contract_valid": False, "run_ready": False, "issues": [issue], "project": None}
    try:
        project = ProjectContract.model_validate(payload)
    except ValidationError as exc:
        return {"contract_valid": False, "run_ready": False,
                "issues": [_from_issue(i) for i in pydantic_issues(exc)], "project": None}
    issues = [_from_issue(i) for i in validate_contract(project)]
    contract_valid = not any(i["severity"] == "error" for i in issues)
    issues += readiness_issues(project)
    for scenario in project.scenarios:
        prefix = f"scenarios[{scenario.id}]."
        try:
            resolved = resolve_scenario(project, scenario.id)
        except ScenarioOverlayError as exc:
            issues.append(_from_issue(exc.issue, prefix))
            contract_valid = False
            continue
        except (ValidationError, ValueError) as exc:
            issues.append(_entry("scenario_resolution_failed", "error", prefix + "overlay", str(exc)[:500],
                                 {"scenario_id": scenario.id}))
            contract_valid = False
            continue
        issues += [_from_issue(i, prefix) for i in validate_contract(resolved)]
        base_codes = {(i["code"], i["path"]) for i in issues}
        for item in readiness_issues(resolved, prefix):
            if (item["code"], item["path"].removeprefix(prefix)) not in base_codes:
                issues.append(item)
    run_ready = not any(i["severity"] == "error" for i in issues)
    return {"contract_valid": contract_valid, "run_ready": run_ready, "issues": issues, "project": project}


def scenario_readiness(project: ProjectContract, scenario_id: str | None) -> list[dict[str, Any]]:
    """Errors that block a run of one scenario (or the base case)."""
    issues = [_from_issue(i) for i in validate_contract(project)]
    resolved = resolve_scenario(project, scenario_id)
    prefix = f"scenarios[{scenario_id}]." if scenario_id else ""
    if scenario_id:
        issues += [_from_issue(i, prefix) for i in validate_contract(resolved)]
    issues += readiness_issues(resolved, prefix)
    return [i for i in issues if i["severity"] == "error"]
