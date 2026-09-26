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

    @model_validator(mode="after")
    def incumbent_consistency(self) -> "SolverDiagnostics":
        if not self.has_incumbent and self.incumbent_objective is not None:
            raise ValueError("incumbent_objective requires has_incumbent=true")
        if self.normalized_status in {SolverStatus.INFEASIBLE, SolverStatus.UNBOUNDED, SolverStatus.SOLVER_ERROR} and self.has_incumbent:
            raise ValueError(f"{self.normalized_status.value} cannot declare an incumbent")
        return self

