from .base import Component, UnsupportedComponentOperation
from .context import BoundaryState, BuildContext, ValidationIssue
from .exchange import ExchangeComponent
from .hydro_ror import RunOfRiverComponent
from .registry import ComponentRegistry
from .results import ComponentQAMetadata, ComponentResult, CostTerm
from .solar import SolarComponent
from .wind import WindComponent


def stage3a_registry() -> ComponentRegistry:
    registry = ComponentRegistry()
    registry.register("wind", WindComponent)
    registry.register("solar", SolarComponent)
    registry.register("hydro_ror", RunOfRiverComponent)
    registry.register("import", ExchangeComponent)
    return registry


__all__ = ["BoundaryState", "BuildContext", "Component", "ComponentQAMetadata",
           "ComponentRegistry", "ComponentResult", "CostTerm", "ValidationIssue",
           "WindComponent", "SolarComponent", "RunOfRiverComponent", "ExchangeComponent",
           "UnsupportedComponentOperation", "stage3a_registry"]
