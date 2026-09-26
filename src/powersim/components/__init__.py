from .base import Component, UnsupportedComponentOperation
from .context import BoundaryState, BuildContext, ValidationIssue
from .exchange import ExchangeComponent
from .hydro_ror import RunOfRiverComponent
from .registry import ComponentRegistry
from .results import ComponentQAMetadata, ComponentResult, CostTerm
from .solar import SolarComponent
from .wind import WindComponent
from .thermal import ThermalComponent
from .bess import BESSComponent
from .pumped_hydro import PumpedHydroComponent
from .demand_response import DemandResponseComponent


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


def stage3c_bess_registry() -> ComponentRegistry:
    registry = stage3b_registry()
    registry.register("bess", BESSComponent)
    return registry


def stage3c_pumped_hydro_registry() -> ComponentRegistry:
    registry = stage3c_bess_registry()
    registry.register("pumped_hydro", PumpedHydroComponent)
    return registry

def stage3c_registry() -> ComponentRegistry:
    registry = stage3c_pumped_hydro_registry()
    registry.register("dr", DemandResponseComponent)
    return registry


__all__ = ["BoundaryState", "BuildContext", "Component", "ComponentQAMetadata",
           "ComponentRegistry", "ComponentResult", "CostTerm", "ValidationIssue",
           "WindComponent", "SolarComponent", "RunOfRiverComponent", "ExchangeComponent",
           "ThermalComponent", "BESSComponent", "PumpedHydroComponent", "DemandResponseComponent", "UnsupportedComponentOperation", "stage3a_registry", "stage3b_registry", "stage3c_registry", "stage3c_pumped_hydro_registry"]
