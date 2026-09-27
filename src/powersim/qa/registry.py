from typing import Any, Iterable

from powersim.contracts import QAReport, QAStatus

from .basic import BasicBoundsCheck, ElectricityBalanceCheck, FiniteValuesCheck, ObjectiveReconstructionCheck
from .components import check_component_results
from .reserves import check_reserve_results
from .network import check_network_results
from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy

DEFAULT_CHECKS = (
    FiniteValuesCheck(),
    ElectricityBalanceCheck(),
    BasicBoundsCheck(),
    ObjectiveReconstructionCheck(),
)


def run_qa(
    resolved_input: Any,
    result: Any,
    checks: Iterable[Any] = DEFAULT_CHECKS,
    tolerance: TolerancePolicy = DEFAULT_TOLERANCES,
    *,
    persisted: bool = False,
    component_results=None,
    component_duration_hours: float | None = None,
    reserve_results=None,
) -> QAReport:
    results = [
        check.run(resolved_input, result, tolerance, persisted=persisted)
        for check in checks
    ]
    if component_results:
        if component_duration_hours is None or component_duration_hours <= 0:
            raise ValueError("component_duration_hours must be positive when component_results are supplied")
        results.extend(
            check_component_results(
                list(component_results),
                component_duration_hours,
                tolerance=tolerance,
                resolved_input=resolved_input,
            )
        )
    if reserve_results:
        if component_duration_hours is None or component_duration_hours <= 0:
            raise ValueError("component_duration_hours must be positive when reserve results are supplied")
        results.extend(check_reserve_results(list(reserve_results), component_duration_hours,
                                             resolved_input, tolerance))
    if isinstance(resolved_input, dict) and ((resolved_input.get("buses") or []) or (resolved_input.get("lines") or [])):
        results.extend(check_network_results(resolved_input, result, tolerance))

    executed = [check.status for check in results if check.status != QAStatus.NOT_RUN]
    if not executed:
        status = QAStatus.NOT_RUN
    elif QAStatus.FAIL in executed:
        status = QAStatus.FAIL
    elif QAStatus.WARN in executed:
        status = QAStatus.WARN
    else:
        status = QAStatus.PASS
    return QAReport(status=status, checks=results)
