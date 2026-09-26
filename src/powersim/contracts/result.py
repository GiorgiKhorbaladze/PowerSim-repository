from datetime import datetime
from typing import Any
from pydantic import Field
from .common import ContractModel
from .qa import QAReport
from .solver import ResultValidity, SolverDiagnostics


class ArtifactReference(ContractModel):
    id: str
    media_type: str
    uri: str
    sha256: str | None = None


class ResultEnvelope(ContractModel):
    contract_version: str = "1.0.0"
    run_id: str
    snapshot_fingerprint: str
    created_at: datetime
    validity: ResultValidity
    solver: SolverDiagnostics
    qa: QAReport
    artifacts: list[ArtifactReference] = Field(default_factory=list)
    results: dict[str, Any] = Field(default_factory=dict)

