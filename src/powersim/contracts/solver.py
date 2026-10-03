from enum import Enum
from typing import Any
from pydantic import Field, model_validator
from .common import ContractModel
from .qa import QAStatus


class SolverStatus(str, Enum):
    OPTIMAL = "optimal"
    FEASIBLE = "feasible"
    TIME_LIMIT = "time_limit"
    INFEASIBLE = "infeasible"
    UNBOUNDED = "unbounded"
    NUMERICAL_ERROR = "numerical_error"
    SOLVER_ERROR = "solver_error"
    COMPLETED = "completed"


class ResultValidity(str, Enum):
    VALID = "valid"
    VALID_WITH_WARNINGS = "valid_with_warnings"
    INVALID = "invalid"


class SolverRequest(ContractModel):
    backend: str
    requested_mip_gap: float | None = Field(default=None, ge=0)
    time_limit_s: float | None = Field(default=None, gt=0)
    options: dict[str, Any] = Field(default_factory=dict)


class SolverDiagnostics(ContractModel):
    backend: str
    backend_version: str | None = None
    termination_condition: str
    normalized_status: SolverStatus
    incumbent_objective: float | None = None
    best_bound: float | None = None
    actual_mip_gap: float | None = Field(default=None, ge=0)
    requested_mip_gap: float | None = Field(default=None, ge=0)
    runtime_s: float | None = Field(default=None, ge=0)
    orchestration_runtime_s: float | None = Field(default=None, ge=0)
    has_incumbent: bool
    result_validity: ResultValidity
    qa_status: QAStatus
    requested_backend: str | None = None
    effective_options: dict[str, Any] = Field(default_factory=dict)
    solve_started_at: str | None = None
    solve_finished_at: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def incumbent_consistency(self) -> "SolverDiagnostics":
        status = self.normalized_status
        if status in {SolverStatus.OPTIMAL, SolverStatus.FEASIBLE, SolverStatus.COMPLETED} and not self.has_incumbent:
            raise ValueError(f"{status.value} requires an incumbent")
        no_incumbent_statuses = {
            SolverStatus.INFEASIBLE, SolverStatus.UNBOUNDED,
            SolverStatus.NUMERICAL_ERROR, SolverStatus.SOLVER_ERROR,
        }
        if status in no_incumbent_statuses and self.has_incumbent:
            raise ValueError(f"{status.value} cannot declare an incumbent")
        # Completed adequacy/screening workflows do not have a MIP incumbent.
        # Their canonical QA/publication result is still represented truthfully.
        if not self.has_incumbent and status != SolverStatus.COMPLETED:
            if self.incumbent_objective is not None:
                raise ValueError("incumbent_objective requires has_incumbent=true")
            if self.actual_mip_gap is not None:
                raise ValueError("actual_mip_gap requires has_incumbent=true")
            if self.result_validity != ResultValidity.INVALID:
                raise ValueError("a result without an incumbent must be invalid")
        if status in no_incumbent_statuses and self.result_validity != ResultValidity.INVALID:
            raise ValueError(f"{status.value} requires invalid result validity")
        return self
