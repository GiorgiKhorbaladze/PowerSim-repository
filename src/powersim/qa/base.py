"""Interfaces for QA that consumes data, not live model objects."""

from typing import Any, Protocol
from powersim.contracts import QACheckResult
from .tolerance import TolerancePolicy


class QACheck(Protocol):
    check_id: str
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy,
            *, persisted: bool = False) -> QACheckResult: ...
