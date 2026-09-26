from enum import Enum
from typing import Any
from pydantic import Field
from .common import ContractModel


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


class QAReport(ContractModel):
    contract_version: str = "1.0.0"
    status: QAStatus
    checks: list[QACheckResult] = Field(default_factory=list)

