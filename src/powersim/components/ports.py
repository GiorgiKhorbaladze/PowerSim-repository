"""Typed physical ports. Power is period-average MW; energy is MWh."""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Port:
    asset_id: str
    values: Any
    unit: str


@dataclass(frozen=True)
class PowerInjectionPort(Port):
    """Non-negative supply into the electricity balance, in MW."""
    unit: str = "MW"


@dataclass(frozen=True)
class PowerWithdrawalPort(Port):
    """Non-negative consumption from the electricity balance, in MW."""
    unit: str = "MW"


@dataclass(frozen=True)
class AvailabilityPort(Port):
    unit: str = "MW"


@dataclass(frozen=True)
class CurtailmentPort(Port):
    unit: str = "MW"


@dataclass(frozen=True)
class CostPort(Port):
    unit: str = "USD"


@dataclass(frozen=True)
class BoundaryStatePort(Port):
    unit: str = "state"

