"""Cross-reference validation that cannot be expressed by field types alone."""

from __future__ import annotations

from collections import Counter
from typing import Any

from pydantic import ValidationError

from powersim.contracts.errors import IssueSeverity, ValidationIssue
from powersim.contracts.input import ResolvedInputContract
from powersim.contracts.project import ProjectContract
from powersim.version import CONTRACT_VERSION


def _issue(code: str, path: str, message: str, *, context: dict[str, Any] | None = None) -> ValidationIssue:
    return ValidationIssue(code=code, severity=IssueSeverity.ERROR, path=path,
                           message=message, context=context or {})


def validate_contract(value: ProjectContract | ResolvedInputContract) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if value.contract_version != CONTRACT_VERSION:
        issues.append(_issue("unsupported_contract_version", "contract_version",
                             f"expected {CONTRACT_VERSION}, got {value.contract_version}"))
    for label, ids in (("asset", [a.id for a in value.assets]),
                       ("profile", [p.id for p in value.profiles]),
                       ("bus", [b.id for b in value.network.buses]),
                       ("branch", [b.id for b in value.network.branches])):
        for duplicate in sorted(k for k, n in Counter(ids).items() if n > 1):
            issues.append(_issue(f"duplicate_{label}_id", f"{label}s",
                                 f"duplicate {label} id: {duplicate}", context={"id": duplicate}))
    profile_ids = {p.id for p in value.profiles}
    bus_ids = {b.id for b in value.network.buses}
    for i, asset in enumerate(value.assets):
        for ref in asset.profile_references:
            if ref not in profile_ids:
                issues.append(_issue("dangling_profile_reference", f"assets[{i}].profile_references",
                                     f"profile {ref!r} does not exist", context={"asset_id": asset.id}))
        if asset.bus is not None and asset.bus not in bus_ids:
            issues.append(_issue("dangling_bus_reference", f"assets[{i}].bus",
                                 f"bus {asset.bus!r} does not exist", context={"asset_id": asset.id}))
    for i, branch in enumerate(value.network.branches):
        for field, bus in (("from_bus", branch.from_bus), ("to_bus", branch.to_bus)):
            if bus not in bus_ids:
                issues.append(_issue("dangling_bus_reference", f"network.branches[{i}].{field}",
                                     f"bus {bus!r} does not exist", context={"branch_id": branch.id}))
    return issues


def pydantic_issues(exc: ValidationError) -> list[ValidationIssue]:
    return [ValidationIssue(code="invalid_contract_value", severity=IssueSeverity.ERROR,
                            path=".".join(map(str, e["loc"])), message=e["msg"],
                            context={"type": e["type"]}) for e in exc.errors()]

