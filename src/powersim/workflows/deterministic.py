"""Thin Stage-3A seam used by the otherwise-legacy deterministic assembler."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from powersim.components import BuildContext, stage3a_registry

MIGRATED_KINDS = frozenset({"wind", "solar", "hydro_ror", "import"})


def build_stage3a_context(model: Any, periods, duration_hours: float, profiles,
                          assets, *, offset_hours=0.0, capabilities=None,
                          validation_mode="native") -> BuildContext:
    return BuildContext(builder=model, periods=tuple(periods), duration_hours=duration_hours,
                        profiles=profiles, assets=assets, offset_hours=offset_hours,
                        capabilities=frozenset(capabilities or {"deterministic",
                                                                "subhourly" if duration_hours < 1 else "deterministic"}),
                        validation_mode=validation_mode)


@dataclass
class SharedComponentSession:
    context: BuildContext

    def __post_init__(self):
        registry = stage3a_registry()
        by_kind = {kind: [] for kind in registry.kinds}
        for asset in self.context.assets.values():
            if asset.get("type") in MIGRATED_KINDS:
                by_kind[asset["type"]].append(asset)
        self.components = [registry.create(kind, asset)
                           for kind in registry.kinds
                           for asset in sorted(by_kind[kind], key=lambda item: str(item["id"]))]

    @property
    def asset_ids(self) -> frozenset[str]:
        return frozenset(component.asset_id for component in self.components)

    def validate(self) -> None:
        for component in self.components:
            component.validate(self.context)
        errors = [issue for issue in self.context.validation_issues if issue.severity == "error"]
        if errors:
            raise ValueError("shared component validation failed: " + "; ".join(
                f"{issue.asset_id}: {issue.message}" for issue in errors))

    def build(self) -> None:
        self.validate()
        for component in self.components:
            component.declare_parameters(self.context)
            component.declare_variables(self.context)
            component.add_constraints(self.context)
            component.objective_terms(self.context)

    def extract(self, solution=None):
        solution = solution or self.context.builder
        return [result for component in self.components
                for result in component.extract(solution, self.context)]
