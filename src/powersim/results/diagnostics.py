"""Consistency helpers for solver, extraction and QA observations."""

from powersim.contracts import QAStatus, ResultValidity, SolverDiagnostics, SolverStatus

USABLE_STATUSES = {SolverStatus.OPTIMAL, SolverStatus.FEASIBLE, SolverStatus.TIME_LIMIT}


def finalized_diagnostics(diagnostics: SolverDiagnostics, qa_status: QAStatus, publishable: bool) -> SolverDiagnostics:
    validity = (ResultValidity.VALID_WITH_WARNINGS if diagnostics.normalized_status != SolverStatus.OPTIMAL or qa_status == QAStatus.WARN
                else ResultValidity.VALID) if publishable else ResultValidity.INVALID
    return diagnostics.model_copy(update={"qa_status": qa_status, "result_validity": validity})
