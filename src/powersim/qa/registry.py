from typing import Any, Iterable
from powersim.contracts import QAReport, QAStatus
from .basic import BasicBoundsCheck, ElectricityBalanceCheck, FiniteValuesCheck, ObjectiveReconstructionCheck
from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy

DEFAULT_CHECKS = (FiniteValuesCheck(), ElectricityBalanceCheck(), BasicBoundsCheck(), ObjectiveReconstructionCheck())


def run_qa(resolved_input: Any, result: Any, checks: Iterable[Any] = DEFAULT_CHECKS,
           tolerance: TolerancePolicy = DEFAULT_TOLERANCES) -> QAReport:
    results=[check.run(resolved_input,result,tolerance) for check in checks]
    executed=[check.status for check in results if check.status != QAStatus.NOT_RUN]
    if not executed: status=QAStatus.NOT_RUN
    elif QAStatus.FAIL in executed: status=QAStatus.FAIL
    elif QAStatus.WARN in executed: status=QAStatus.WARN
    else: status=QAStatus.PASS
    return QAReport(status=status,checks=results)
