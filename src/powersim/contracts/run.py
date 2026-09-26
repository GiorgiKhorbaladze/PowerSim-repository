from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import Field
from .common import ContractModel
from .solver import SolverDiagnostics
from powersim.version import CONTRACT_VERSION


class RunStatus(str, Enum):
    DRAFT="draft"; VALIDATING="validating"; QUEUED="queued"; PREPARING="preparing"
    SOLVING="solving"; VALIDATING_RESULTS="validating_results"; REPORTING="reporting"
    COMPLETED="completed"; FAILED="failed"; CANCELLED="cancelled"


class RunEvent(ContractModel):
    sequence: int = Field(ge=0)
    timestamp: datetime
    status: RunStatus
    message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class RunContract(ContractModel):
    contract_version: str = CONTRACT_VERSION
    id: str
    snapshot_fingerprint: str
    status: RunStatus = RunStatus.DRAFT
    solver_diagnostics: SolverDiagnostics | None = None
    events: list[RunEvent] = Field(default_factory=list)
