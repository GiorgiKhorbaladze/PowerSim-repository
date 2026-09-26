from typing import Any

from pydantic import ConfigDict, Field

from powersim.version import CONTRACT_VERSION, WORKFLOW_MODEL_VERSION
from .common import ContractModel, TimeContract, UnitSystem
from .project import AssetContract, NetworkContract, ProfileContract


class ResolvedInputContract(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    contract_version: str = CONTRACT_VERSION
    workflow_model_version: str = WORKFLOW_MODEL_VERSION
    project_id: str
    scenario_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    units: UnitSystem
    time: TimeContract
    assets: tuple[AssetContract, ...] = ()
    profiles: tuple[ProfileContract, ...] = ()
    network: NetworkContract = Field(default_factory=NetworkContract)
    reserve_products: tuple[dict[str, Any], ...] = ()
    solver_settings: dict[str, Any] = Field(default_factory=dict)
    legacy_payload: dict[str, Any] | None = None

