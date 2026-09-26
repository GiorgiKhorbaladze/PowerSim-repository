from datetime import datetime
from typing import Any
from pydantic import Field, model_validator
from powersim.version import CONTRACT_VERSION

from .common import ContractModel
from .qa import QAReport
from .solver import ResultValidity, SolverDiagnostics


class ArtifactReference(ContractModel):
    id: str
    media_type: str
    uri: str
    sha256: str | None = None


class ResultEnvelope(ContractModel):
    contract_version: str = CONTRACT_VERSION
    run_id: str
    snapshot_fingerprint: str
    created_at: datetime
    validity: ResultValidity
    solver: SolverDiagnostics
    qa: QAReport
    artifacts: list[ArtifactReference] = Field(default_factory=list)
    results: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def consistent_publication_status(self) -> "ResultEnvelope":
        if self.validity != self.solver.result_validity:
            raise ValueError("validity must equal solver.result_validity")
        if self.qa.status != self.solver.qa_status:
            raise ValueError("qa.status must equal solver.qa_status")
        if self.qa.status in {self.qa.status.FAIL, self.qa.status.NOT_RUN} and self.validity != ResultValidity.INVALID:
            raise ValueError("QA fail/not_run requires invalid result")
        if self.qa.status == self.qa.status.WARN and self.validity == ResultValidity.VALID:
            raise ValueError("QA warn cannot produce an unqualified valid result")
        return self
