from enum import Enum
from typing import Any

from pydantic import Field

from .common import ContractModel


class IssueSeverity(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class ValidationIssue(ContractModel):
    code: str
    severity: IssueSeverity
    path: str
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class StructuredError(ContractModel):
    code: str
    message: str
    issues: list[ValidationIssue] = Field(default_factory=list)
    correlation_id: str | None = None


class ContractMigrationError(ValueError):
    """Raised when a contract cannot be migrated without ambiguity or loss."""

