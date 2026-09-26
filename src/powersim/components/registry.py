"""Deterministic canonical component registry."""
from collections import OrderedDict
from typing import Any


class ComponentRegistry:
    def __init__(self) -> None:
        self._implementations: OrderedDict[str, type] = OrderedDict()

    def register(self, kind: str, implementation: type) -> None:
        if kind in self._implementations:
            raise ValueError(f"component kind already registered: {kind}")
        self._implementations[kind] = implementation

    def create(self, kind: str, asset: dict[str, Any]):
        try:
            implementation = self._implementations[kind]
        except KeyError as exc:
            raise KeyError(f"unknown component kind: {kind!r}") from exc
        return implementation(asset)

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(self._implementations)

