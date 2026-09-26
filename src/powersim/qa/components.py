"""Independent QA over canonical Stage-3A results (never live constraints)."""
from __future__ import annotations

import math
from dataclasses import dataclass

from powersim.components.results import ComponentResult
from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy


@dataclass(frozen=True)
class ComponentQACheck:
    check_id: str
    passed: bool
    violation: float = 0.0
    message: str = ""


def check_component_results(results: list[ComponentResult], duration_hours: float,
                            tolerance: TolerancePolicy | None = None) -> list[ComponentQACheck]:
    tolerance = tolerance or DEFAULT_TOLERANCES
    checks = []
    for result in results:
        values = (result.available_mw, result.injection_mw, result.withdrawal_mw,
                  result.curtailed_mw, result.curtailed_mwh)
        finite = all(value is None or math.isfinite(value) for value in values)
        nonnegative = all(value is None or value >= 0 for value in values)
        checks.append(ComponentQACheck("finite_nonnegative", finite and nonnegative,
                                       message=f"{result.asset_id}@{result.period}"))
        available = result.available_mw
        if available is None:
            continue
        bound_violation = max(0.0, result.injection_mw - available)
        threshold = tolerance.limit(max(1, abs(available), abs(result.injection_mw)))
        check_id = "import_bound" if result.component_kind == "import" else "availability_bound"
        checks.append(ComponentQACheck(check_id, bound_violation <= threshold, bound_violation))
        if result.component_kind != "import" and result.curtailed_mw is not None:
            accounting = abs(available - result.injection_mw - result.curtailed_mw)
            energy = abs((result.curtailed_mwh or 0) - result.curtailed_mw * duration_hours)
            checks.append(ComponentQACheck("resource_accounting", accounting <= threshold, accounting))
            energy_threshold = tolerance.limit(max(1, abs(result.curtailed_mwh or 0)))
            checks.append(ComponentQACheck("curtailment_energy", energy <= energy_threshold, energy))
    return checks
