"""Loss-preserving adapter around the legacy ``schema/powersim_schema.py`` contract."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from powersim.contracts.common import CalendarPolicy, FlowUnitMetadata, Provenance, TimeContract, UnitSystem
from powersim.contracts.errors import IssueSeverity, ValidationIssue
from powersim.contracts.input import ResolvedInputContract
from powersim.contracts.project import AssetContract, BranchContract, BusContract, NetworkContract, ProfileContract
from powersim.validation.contract_validation import validate_contract


class LegacyAdaptation(BaseModel):
    resolved_input: ResolvedInputContract | None
    issues: list[ValidationIssue] = Field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.resolved_input is not None and not any(i.severity == IssueSeverity.ERROR for i in self.issues)


def _legacy_validation(data: dict[str, Any]) -> list[ValidationIssue]:
    try:
        from schema.powersim_schema import validate_input
    except ImportError:
        return [ValidationIssue(code="legacy_validator_unavailable", severity="warning", path="",
                                message="legacy validator is not installed; typed validation was still applied")]
    _, errors, warnings = validate_input(data)
    return [ValidationIssue(code="legacy_validation_error", severity="error", path="", message=m) for m in errors] + [
        ValidationIssue(code="legacy_validation_warning", severity="warning", path="", message=m) for m in warnings]


def _network(data: dict[str, Any], issues: list[ValidationIssue]) -> NetworkContract:
    buses = [BusContract(id=str(b.get("id")), name=b.get("name"), is_slack=bool(b.get("is_slack", False)),
                         provenance=Provenance(source="legacy")) for b in data.get("buses", []) if b.get("id")]
    branches = []
    for i, line in enumerate(data.get("lines", [])):
        raw_b = line.get("susceptance_mw_per_rad")
        x_pu = line.get("x_pu", line.get("reactance_pu"))
        base = line.get("base_mva", data.get("base_mva"))
        if raw_b is not None and x_pu is not None:
            issues.append(ValidationIssue(code="ambiguous_branch_impedance", severity="error", path=f"lines[{i}]",
                                          message="provide susceptance_mw_per_rad or x_pu, not both")); continue
        if raw_b is None:
            if x_pu is None or base is None or not isinstance(base, (int, float)) or base <= 0 or x_pu == 0:
                issues.append(ValidationIssue(code="ambiguous_x_pu_base_mva", severity="error", path=f"lines[{i}]",
                                              message="x_pu requires nonzero x_pu and explicit positive base_mva")); continue
            raw_b = base / x_pu
            transformation = "susceptance_mw_per_rad = base_mva / x_pu"
        else:
            transformation = "already normalized"
        try:
            branches.append(BranchContract(id=str(line.get("id", f"line-{i}")), from_bus=line.get("from_bus"),
                to_bus=line.get("to_bus"), susceptance_mw_per_rad=raw_b,
                normal_limit_mw=line.get("normal_limit_mw", line.get("capacity_mw")),
                emergency_limit_mw=line.get("emergency_limit_mw"), phase_shift_rad=line.get("phase_shift_rad", 0),
                provenance=Provenance(source="legacy", transformation=transformation,
                                      details={"x_pu": x_pu, "base_mva": base})))
        except Exception as exc:
            issues.append(ValidationIssue(code="invalid_branch", severity="error", path=f"lines[{i}]", message=str(exc)))
    return NetworkContract(buses=buses, branches=branches)


def adapt_legacy_input(data: dict[str, Any], *, preserve_legacy_validation: bool = True) -> LegacyAdaptation:
    issues = _legacy_validation(data) if preserve_legacy_validation else []
    meta = data.get("metadata") or {}
    schema_version = str(meta.get("schema_version", ""))
    if schema_version not in {"1.0", "1.1", "1.2", "1.3", "1.4", "1.5"}:
        issues.append(ValidationIssue(code="unsupported_contract_version", severity="error", path="metadata.schema_version",
                                      message=f"unsupported legacy schema version: {schema_version!r}"))
        return LegacyAdaptation(resolved_input=None, issues=issues)
    ti = data.get("time_index") or []
    resolution = data.get("resolution_min", 60)
    start_text = ti[0] if ti else f"{meta.get('study_year', 2025)}-01-01 00:00"
    try:
        start = datetime.fromisoformat(start_text)
        policy = CalendarPolicy.NON_LEAP if len(ti) == 365 * 24 * 60 // resolution else CalendarPolicy.EXPLICIT_PERIODS
        time = TimeContract(timezone=meta.get("timezone", "UTC"), study_year=meta.get("study_year", start.year),
                            resolution_minutes=resolution, interval_duration_hours=resolution / 60,
                            start=start, periods=len(ti) or len((data.get("profiles") or {}).get("demand", [])),
                            calendar_policy=policy)
        profiles = [ProfileContract(id=k, unit="MW" if k == "demand" else "legacy_unspecified", values=v,
                                    provenance=Provenance(source="legacy"))
                    for k, v in (data.get("profiles") or {}).items() if isinstance(v, list)]
        assets = []
        for raw in data.get("assets", []):
            refs = [raw[k] for k in ("profile", "availability_profile", "inflow_profile") if isinstance(raw.get(k), str)]
            excluded = {"id", "type", "bus", "pmin", "pmax", "profile", "availability_profile", "inflow_profile"}
            extras = {k: v for k, v in raw.items() if k not in excluded}
            assets.append(AssetContract(id=str(raw.get("id", "")), kind=raw.get("type", "legacy_unknown"), bus=raw.get("bus"),
                 profile_references=refs, capacity_min_mw=raw.get("pmin"), capacity_max_mw=raw.get("pmax"),
                 provenance=Provenance(source="legacy", source_version=schema_version), legacy_extensions=extras))
        resolved = ResolvedInputContract(project_id=str(meta.get("project_id", "legacy-import")), metadata=meta,
            units=UnitSystem(currency=str(meta.get("currency", "legacy_currency")),
                water=FlowUnitMetadata(volume_unit="Mm3", rate_unit=(data.get("profile_bundle") or {}).get("hydro_inflow_unit", "legacy_unspecified")),
                gas=FlowUnitMetadata(volume_unit="Mm3", rate_unit="Mm3/h")), time=time, assets=tuple(assets),
            profiles=tuple(profiles), network=_network(data, issues), reserve_products=tuple(data.get("reserve_products", [])),
            solver_settings=data.get("solver_settings", {}), legacy_payload=data)
        issues.extend(validate_contract(resolved))
    except Exception as exc:
        issues.append(ValidationIssue(code="legacy_adaptation_failed", severity="error", path="", message=str(exc)))
        return LegacyAdaptation(resolved_input=None, issues=issues)
    return LegacyAdaptation(resolved_input=resolved, issues=issues)
