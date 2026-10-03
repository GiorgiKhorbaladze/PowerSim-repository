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
        raw = _deep_overlay(raw, matches[0].overlay)
    raw["project_id"] = raw.pop("id")
    raw["scenario_id"] = scenario_id
    raw.pop("version", None)
    return ResolvedInputContract.model_validate(raw)
