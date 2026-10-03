from typing import Any, Literal

from pydantic import ConfigDict, Field

from powersim.version import CONTRACT_VERSION, WORKFLOW_MODEL_VERSION
from .common import ContractModel, FrozenDict, TimeContract, UnitSystem
from .project import AssetContract, NetworkContract, ProfileContract


class ResolvedInputContract(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    contract_version: str = CONTRACT_VERSION
    workflow_model_version: str = WORKFLOW_MODEL_VERSION
    project_id: str
    workflow: Literal["deterministic_uc", "stochastic_uc", "security_scuc", "chronological_adequacy", "scoped_expansion"] = "deterministic_uc"
    scenario_id: str | None = None
    metadata: FrozenDict = Field(default_factory=FrozenDict)
    units: UnitSystem
    time: TimeContract
    assets: tuple[AssetContract, ...] = ()
    profiles: tuple[ProfileContract, ...] = ()
    network: NetworkContract = Field(default_factory=NetworkContract)
    reserve_products: tuple[FrozenDict, ...] = ()
    solver_settings: FrozenDict = Field(default_factory=FrozenDict)
    legacy_payload: FrozenDict | None = None
