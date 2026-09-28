"""Fail-closed result publication decision."""

from dataclasses import dataclass
from typing import Any
from powersim.contracts import QAReport, QAStatus, SolverDiagnostics
from .diagnostics import USABLE_STATUSES


@dataclass(frozen=True)
class PublicationDecision:
    publishable: bool
    reasons: tuple[str, ...]


def evaluate_publication(diagnostics: SolverDiagnostics, qa: QAReport, *, extraction_completed: bool,
                         required_values_finite: bool,
                         required_check_ids: tuple[str, ...] = ()) -> PublicationDecision:
    reasons=[]
    if diagnostics.normalized_status not in USABLE_STATUSES: reasons.append("solver_status_not_usable")
    if not diagnostics.has_incumbent: reasons.append("no_incumbent")
    if not extraction_completed: reasons.append("extraction_incomplete")
    if not required_values_finite: reasons.append("non_finite_required_values")
    if qa.status == QAStatus.FAIL: reasons.append("qa_failed")
    if qa.status == QAStatus.NOT_RUN: reasons.append("mandatory_qa_not_run")
    check_statuses = {check.check_id: check.status for check in qa.checks}
    for check_id in required_check_ids:
        if check_statuses.get(check_id) != QAStatus.PASS:
            reasons.append(f"mandatory_qa_check_not_passed:{check_id}")
    return PublicationDecision(not reasons,tuple(reasons))
