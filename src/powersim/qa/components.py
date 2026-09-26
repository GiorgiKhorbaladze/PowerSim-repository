"""Stage-3A checks adapted directly into the canonical Stage-2 QA contract."""
from __future__ import annotations

import math
from typing import Any

from powersim.components.results import ComponentResult
from powersim.contracts import QACheckResult, QAStatus
from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy


def _field(item: ComponentResult | dict[str, Any], name: str, default=None):
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def check_component_results(results: list[ComponentResult | dict[str, Any]], duration_hours: float,
                            tolerance: TolerancePolicy = DEFAULT_TOLERANCES) -> list[QACheckResult]:
    """Reconstruct component invariants without consulting model constraints."""
    failures: dict[str, list[dict[str, Any]]] = {
        "component_finite_nonnegative": [], "component_availability_bound": [],
        "component_resource_accounting": [], "component_curtailment_energy": [],
    }
    worst = {key: 0.0 for key in failures}
    counts = {key: 0 for key in failures}
    for item in results:
        witness = {"asset_id": _field(item, "asset_id"), "period": _field(item, "period")}
        available = _field(item, "available_mw")
        injection = _field(item, "injection_mw", 0.0)
        withdrawal = _field(item, "withdrawal_mw", 0.0)
        curtailed = _field(item, "curtailed_mw")
        curtailed_mwh = _field(item, "curtailed_mwh")
        values = (available, injection, withdrawal, curtailed, curtailed_mwh)
        counts["component_finite_nonnegative"] += 1
        if not all(value is None or (isinstance(value, (int, float)) and
                   math.isfinite(value) and value >= 0) for value in values):
            failures["component_finite_nonnegative"].append(witness)
        if available is None:
            continue
        scale = max(1.0, abs(available), abs(injection))
        limit = tolerance.limit(scale)
        violation = max(0.0, injection - available)
        counts["component_availability_bound"] += 1
        worst["component_availability_bound"] = max(worst["component_availability_bound"], violation)
        if violation > limit:
            failures["component_availability_bound"].append({**witness, "violation": violation})
        if _field(item, "component_kind") == "import" or curtailed is None:
            continue
        accounting = abs(available - injection - curtailed)
        energy = abs((curtailed_mwh or 0.0) - curtailed * duration_hours)
        counts["component_resource_accounting"] += 1
        counts["component_curtailment_energy"] += 1
        worst["component_resource_accounting"] = max(worst["component_resource_accounting"], accounting)
        worst["component_curtailment_energy"] = max(worst["component_curtailment_energy"], energy)
        if accounting > limit:
            failures["component_resource_accounting"].append({**witness, "violation": accounting})
        if energy > tolerance.limit(max(1.0, abs(curtailed_mwh or 0.0))):
            failures["component_curtailment_energy"].append({**witness, "violation": energy})
    return [QACheckResult(check_id=check_id,
             status=QAStatus.FAIL if failures[check_id] else QAStatus.PASS,
             message=(f"{len(failures[check_id])} component violation(s)" if failures[check_id]
                      else f"{counts[check_id]} component value(s) passed"),
             witness={"worst": failures[check_id][:10], "tolerance_mode": "internal_full_precision"},
             tolerance=tolerance.limit(1.0), max_violation=worst[check_id],
             checked_count=counts[check_id]) for check_id in failures]


class ComponentResultsCheck:
    """Registry adapter placing every component check in the final QAReport."""
    check_id = "stage3a_component_results"

    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy,
            *, persisted: bool = False) -> list[QACheckResult]:
        engine = str(((result.get("diagnostics") or {}).get("component_engine", "legacy")))
        if engine != "shared":
            return [QACheckResult(check_id=self.check_id, status=QAStatus.NOT_RUN,
                                  message="shared component engine was not selected")]
        records = result.get("component_results") or []
        if not records:
            return [QACheckResult(check_id=self.check_id, status=QAStatus.FAIL,
                                  message="shared solve produced no canonical component results")]
        minutes = ((result.get("hourly_system") or [{}])[0].get("period_minutes", 60))
        return check_component_results(records, float(minutes) / 60.0, tolerance)
