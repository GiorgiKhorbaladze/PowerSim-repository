"""Project, scenario overlay, asset, profile, and network contracts."""

from __future__ import annotations

import copy
from typing import Any, Literal

from pydantic import Field, model_validator

from powersim.version import CONTRACT_VERSION
from .common import ContractModel, FrozenDict, Provenance, TimeContract, UnitSystem
from .errors import IssueSeverity, ScenarioOverlayError, ValidationIssue


class AssetContract(ContractModel):
    id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    enabled: bool = True
    bus: str | None = None
    profile_references: tuple[str, ...] = ()
    capacity_min_mw: float | None = Field(default=None, ge=0)
    capacity_max_mw: float | None = Field(default=None, ge=0)
    provenance: Provenance | None = None
    legacy_extensions: FrozenDict = Field(default_factory=FrozenDict)

    @model_validator(mode="after")
    def bounds(self) -> "AssetContract":
        if self.capacity_min_mw is not None and self.capacity_max_mw is not None and self.capacity_min_mw > self.capacity_max_mw:
            raise ValueError("capacity_min_mw cannot exceed capacity_max_mw")
        return self


class ProfileContract(ContractModel):
    id: str
    unit: str
    values: tuple[float, ...]
    provenance: Provenance | None = None


class BusContract(ContractModel):
    id: str = Field(min_length=1)
    name: str | None = None
    is_slack: bool = False
    provenance: Provenance | None = None


class BranchContract(ContractModel):
    id: str = Field(min_length=1)
    from_bus: str
    to_bus: str
    susceptance_mw_per_rad: float
    normal_limit_mw: float = Field(gt=0)
    emergency_limit_mw: float | None = Field(default=None, gt=0)
    phase_shift_rad: float = 0.0
    provenance: Provenance

    @model_validator(mode="after")
    def branch_values(self) -> "BranchContract":
        if self.from_bus == self.to_bus:
            raise ValueError("branch endpoints must differ")
        if self.susceptance_mw_per_rad == 0:
            raise ValueError("susceptance_mw_per_rad cannot be zero")
        if self.emergency_limit_mw is not None and self.emergency_limit_mw < self.normal_limit_mw:
            raise ValueError("emergency_limit_mw cannot be below normal_limit_mw")
        return self


class NetworkContract(ContractModel):
    buses: tuple[BusContract, ...] = ()
    branches: tuple[BranchContract, ...] = ()


class ProjectVersionContract(ContractModel):
    contract_version: str = CONTRACT_VERSION
    revision: int = Field(ge=1)
    parent_fingerprint: str | None = None


class ScenarioContract(ContractModel):
    id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    name: str | None = None
    overlay: FrozenDict = Field(default_factory=FrozenDict)


class ProjectContract(ContractModel):
    contract_version: str = CONTRACT_VERSION
    id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    workflow: Literal["deterministic_uc", "stochastic_uc", "security_scuc", "chronological_adequacy", "scoped_expansion"] = "deterministic_uc"
    version: ProjectVersionContract
    metadata: dict[str, Any] = Field(default_factory=dict)
    units: UnitSystem
    time: TimeContract
    assets: list[AssetContract] = Field(default_factory=list)
    profiles: list[ProfileContract] = Field(default_factory=list)
    network: NetworkContract = Field(default_factory=NetworkContract)
    reserve_products: list[dict[str, Any]] = Field(default_factory=list)
    solver_settings: dict[str, Any] = Field(default_factory=dict)
    scenarios: list[ScenarioContract] = Field(default_factory=list)
    # Compatibility payload for the static editor. It is preserved for the
    # backend workflow but is never interpreted as a second solver model.
    legacy_payload: FrozenDict | None = None


def _deep_overlay(base: Any, overlay: Any) -> Any:
    """Deterministically merge mappings; lists/scalars are explicit replacements."""
    if isinstance(base, dict) and isinstance(overlay, dict):
        result = copy.deepcopy(base)
        for key in sorted(overlay):
            result[key] = _deep_overlay(result[key], overlay[key]) if key in result else copy.deepcopy(overlay[key])
        return result
    return copy.deepcopy(overlay)


_SCALABLE_ASSET_FIELDS = {"capacity_max_mw", "capacity_min_mw"}
_SCALABLE_EXTENSION_FIELDS = {"vom", "fuel_price", "heat_rate", "monthly_energy_mwh", "monthly_pmax_mw",
                              "monthly_pmin_mw", "startup_cost", "no_load_cost"}


def _scale_issue(scenario_id: str, message: str, **context: Any) -> ScenarioOverlayError:
    return ScenarioOverlayError(ValidationIssue(
        code="invalid_scenario_scale", severity=IssueSeverity.ERROR, path="scenarios.overlay.scale",
        message=message, context={"scenario_id": scenario_id, **context}))


def _factor(scenario_id: str, value: Any, where: str) -> float:
    import math
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise _scale_issue(scenario_id, f"{where}: scale factor must be a finite number >= 0", value=value)
    return float(value)


def _apply_scale(raw: dict[str, Any], scale: Any, scenario_id: str) -> dict[str, Any]:
    """Multiply selected profiles and asset parameters (sensitivity scenarios).

    ``{"profiles": {"demand": 1.1}, "assets": {"tpp-import": {"capacity_max_mw": 0.8, "vom": 1.5}}}``.
    Applied after the overlay merge.  Unknown ids or fields are errors, never ignored.
    """
    if not isinstance(scale, dict) or set(scale) - {"profiles", "assets"}:
        raise _scale_issue(scenario_id, "scale may only contain 'profiles' and 'assets' mappings")
    profiles = {p["id"]: p for p in raw.get("profiles") or []}
    for profile_id, value in (scale.get("profiles") or {}).items():
        factor = _factor(scenario_id, value, f"profile {profile_id}")
        if profile_id not in profiles:
            raise _scale_issue(scenario_id, f"unknown profile {profile_id!r}", profile_id=profile_id)
        profiles[profile_id]["values"] = [v * factor for v in profiles[profile_id]["values"]]
    assets = {a["id"]: a for a in raw.get("assets") or []}
    for asset_id, fields in (scale.get("assets") or {}).items():
        if asset_id not in assets or not isinstance(fields, dict):
            raise _scale_issue(scenario_id, f"unknown asset {asset_id!r}", asset_id=asset_id)
        asset = assets[asset_id]
        for field, value in fields.items():
            factor = _factor(scenario_id, value, f"asset {asset_id}.{field}")
            if field in _SCALABLE_ASSET_FIELDS:
                if asset.get(field) is not None:
                    asset[field] = asset[field] * factor
                ext = asset.get("legacy_extensions") or {}
                legacy = {"capacity_max_mw": "pmax", "capacity_min_mw": "pmin"}[field]
                if ext.get(legacy) is not None:
                    asset["legacy_extensions"] = {**ext, legacy: ext[legacy] * factor}
            elif field in _SCALABLE_EXTENSION_FIELDS:
                ext = dict(asset.get("legacy_extensions") or {})
                current = ext.get(field)
                if current is None:
                    raise _scale_issue(scenario_id, f"asset {asset_id} has no {field} to scale", asset_id=asset_id)
                ext[field] = ([None if v is None else v * factor for v in current] if isinstance(current, list)
                              else current * factor)
                asset["legacy_extensions"] = ext
            else:
                raise _scale_issue(scenario_id, f"field {field!r} cannot be scaled", field=field)
    metadata = dict(raw.get("metadata") or {})
    metadata["scenario_scale"] = copy.deepcopy(scale)
    raw["metadata"] = metadata
    return raw


def resolve_scenario(project: ProjectContract, scenario_id: str | None = None):
    from .input import ResolvedInputContract
    raw = project.model_dump(mode="json", exclude={"scenarios"})
    if scenario_id is not None:
        matches = [s for s in project.scenarios if s.id == scenario_id]
        if len(matches) != 1:
            raise ValueError(f"unknown or duplicate scenario: {scenario_id}")
        protected = sorted({"id", "contract_version", "version", "workflow_model_version"} & set(matches[0].overlay))
        if protected:
            raise ScenarioOverlayError(ValidationIssue(
                code="protected_scenario_overlay", severity=IssueSeverity.ERROR,
                path="scenarios.overlay", message="scenario overlay cannot modify project identity or version lineage",
                context={"fields": protected, "scenario_id": scenario_id},
            ))
        overlay = {key: value for key, value in matches[0].overlay.items() if key != "scale"}
        raw = _deep_overlay(raw, overlay)
        if "scale" in matches[0].overlay:
            raw = _apply_scale(raw, copy.deepcopy(dict(matches[0].overlay["scale"])), scenario_id)
    raw["project_id"] = raw.pop("id")
    raw["scenario_id"] = scenario_id
    raw.pop("version", None)
    return ResolvedInputContract.model_validate(raw)
