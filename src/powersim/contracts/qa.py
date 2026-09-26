from enum import Enum
from typing import Any
from pydantic import Field
from .common import ContractModel
from powersim.version import CONTRACT_VERSION


class QAStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    NOT_RUN = "not_run"


class QACheckResult(ContractModel):
    check_id: str
    status: QAStatus
    message: str
    witness: dict[str, Any] = Field(default_factory=dict)
    tolerance: float | None = Field(default=None, ge=0)
    max_violation: float | None = Field(default=None, ge=0)
    checked_count: int | None = Field(default=None, ge=0)


class QAReport(ContractModel):
    contract_version: str = CONTRACT_VERSION
    status: QAStatus
    checks: list[QACheckResult] = Field(default_factory=list)
