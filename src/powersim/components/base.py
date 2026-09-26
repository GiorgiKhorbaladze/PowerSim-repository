"""Small shared component protocol; no workflow or solver global state."""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .context import BoundaryState, BuildContext, ValidationIssue
from .results import ComponentQAMetadata, ComponentResult, CostTerm


class UnsupportedComponentOperation(NotImplementedError):
    pass


@runtime_checkable
class Component(Protocol):
    kind: str
    contract_version: str
    supported_capabilities: frozenset[str]
    def validate(self, context: BuildContext) -> list[ValidationIssue]: ...
    def declare_parameters(self, context: BuildContext) -> None: ...
    def declare_variables(self, context: BuildContext) -> None: ...
    def add_constraints(self, context: BuildContext) -> None: ...
    def objective_terms(self, context: BuildContext) -> list[CostTerm]: ...
    def boundary_state(self, solution: Any, at: int) -> BoundaryState: ...
    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]: ...
    def qa_spec(self) -> ComponentQAMetadata: ...

