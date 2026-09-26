from .base import Component, UnsupportedComponentOperation
from .context import BoundaryState, BuildContext, ValidationIssue
from .exchange import ExchangeComponent
from .hydro_ror import RunOfRiverComponent
from .registry import ComponentRegistry
from .results import ComponentQAMetadata, ComponentResult, CostTerm
from .solar import SolarComponent
from .wind import WindComponent
from .thermal import ThermalComponent


def stage3a_registry() -> ComponentRegistry:
    registry = ComponentRegistry()
    registry.register("wind", WindComponent)
    registry.register("solar", SolarComponent)
    registry.register("hydro_ror", RunOfRiverComponent)
    registry.register("import", ExchangeComponent)
    return registry


def stage3b_registry() -> ComponentRegistry:
    """Stage-3B registry: Stage-3A passive components plus thermal UC."""
    registry = stage3a_registry()
    registry.register("thermal", ThermalComponent)
    return registry


__all__ = ["BoundaryState", "BuildContext", "Component", "ComponentQAMetadata",
           "ComponentRegistry", "ComponentResult", "CostTerm", "ValidationIssue",
           "WindComponent", "SolarComponent", "RunOfRiverComponent", "ExchangeComponent",
           "ThermalComponent", "UnsupportedComponentOperation", "stage3a_registry", "stage3b_registry"]
