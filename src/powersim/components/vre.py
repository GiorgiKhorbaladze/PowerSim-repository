"""Shared passive availability/dispatch physics matching the legacy solver."""
from __future__ import annotations

import math
import re
from hashlib import sha1
from typing import Any

from .base import UnsupportedComponentOperation
from powersim.contracts import IssueSeverity, ValidationIssue
from .context import BoundaryState, BuildContext
from .ports import AvailabilityPort, CostPort, CurtailmentPort, PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm

MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)


def maintenance_factor(asset: dict, hour: int) -> float:
    pmax = float(asset.get("pmax", asset.get("pmax_installed", 0)) or 0)
    factor = 1.0
    for window in asset.get("maint_windows", []) or []:
        try:
            start = float(window.get("start_h", window.get("start")))
            end = float(window.get("end_h", window.get("end")))
        except (TypeError, ValueError):
            continue
        if start <= hour < end:
            if window.get("availability_factor") is not None:
                current = float(window["availability_factor"])
            elif window.get("available_capacity_mw") is not None and pmax > 0:
                current = float(window["available_capacity_mw"]) / pmax
            else:
                current = 0.0
            factor = min(factor, max(0.0, min(1.0, current)))
    return factor


def temperature_factor(asset: dict, profiles: dict, index: int) -> float:
    curve, key = asset.get("temp_derating_curve"), asset.get("temp_profile_key")
    series = profiles.get(key) if key else None
    if not curve or not isinstance(series, list) or index >= len(series):
        return 1.0
    temp, points = float(series[index]), sorted(curve, key=lambda p: float(p[0]))
    if temp <= float(points[0][0]): return max(0.0, float(points[0][1]))
    if temp >= float(points[-1][0]): return max(0.0, float(points[-1][1]))
    for (t0, f0), (t1, f1) in zip(points, points[1:]):
        if temp <= float(t1):
            return max(0.0, float(f0) + (temp-float(t0)) / max(float(t1)-float(t0), 1e-9) * (float(f1)-float(f0)))
    return 1.0


class PassiveInjectionComponent:
    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})

    def __init__(self, asset: dict[str, Any]):
        self.asset = asset
        self.asset_id = str(asset["id"])

    def validate(self, context: BuildContext) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        def issue(code, field, message, *, warning=False, compatibility=False):
            issues.append(ValidationIssue(code=code,
                severity=IssueSeverity.WARNING if warning else IssueSeverity.ERROR,
                path=f"assets.{self.asset_id}.{field}", message=message,
                context={"compatibility": compatibility}))
        try:
            capacity = self.installed_capacity()
        except (TypeError, ValueError, OverflowError):
            capacity = float("nan")
        if not math.isfinite(capacity) or capacity < 0:
            issue("invalid_capacity", "pmax", "capacity must be numeric, finite, and non-negative")
        profile_field = self.profile_field_name()
        key = self.profile_key()
        if key is not None and not isinstance(key, str):
            issue("invalid_profile_reference", profile_field,
                  "profile reference must be a string")
            key = None
        if key and key not in context.profiles:
            legacy = context.validation_mode == "legacy" and self.kind in {"wind", "solar", "import"}
            issue("missing_profile", profile_field, f"profile {key!r} is missing",
                  warning=legacy, compatibility=legacy)
        if key in context.profiles:
            series = context.profiles[key]
            if not isinstance(series, list) or len(series) < len(context.periods):
                legacy_import = context.validation_mode == "legacy" and self.kind == "import"
                issue("invalid_profile", profile_field, "profile must be a list covering every period",
                      warning=legacy_import, compatibility=legacy_import)
            else:
                for index, value in enumerate(series[:len(context.periods)]):
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError, OverflowError):
                        legacy_import = context.validation_mode == "legacy" and self.kind == "import"
                        issue("non_numeric_profile", f"{profile_field}[{index}]", "profile value must be numeric",
                              warning=legacy_import, compatibility=legacy_import)
                        continue
                    if not math.isfinite(numeric):
                        legacy_import = context.validation_mode == "legacy" and self.kind == "import"
                        issue("nonfinite_profile", f"{profile_field}[{index}]", "profile value must be finite",
                              warning=legacy_import, compatibility=legacy_import)
                    elif numeric < 0:
                        compatible_cf = context.validation_mode == "legacy" and self.kind != "import"
                        issue("negative_profile", f"{profile_field}[{index}]",
                              "negative profile value is invalid" if not compatible_cf else
                              "negative profile value is clamped to zero for legacy compatibility",
                              warning=compatible_cf, compatibility=compatible_cf)
        context.validation_issues.extend(issues)
        return issues

    def installed_capacity(self) -> float:
        return float(self.asset.get("pmax", 0) or 0)

    def profile_key(self) -> str | None:
        return self.asset.get("availability_profile")

    def profile_field_name(self) -> str:
        return "availability_profile"

    def profile_value(self, context: BuildContext, index: int) -> float:
        key = self.profile_key()
        # Legacy VRE treated a dangling profile reference as full availability.
        if key and isinstance(context.profiles.get(key), list):
            return float(context.profiles[key][index])
        return 1.0

    def available_mw(self, context: BuildContext, index: int) -> float:
        base = self.installed_capacity() * max(0.0, min(1.0, self.profile_value(context, index)))
        hour = int(context.offset_hours + round(index * context.duration_hours))
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)

    def marginal_cost(self) -> float:
        return float(self.asset.get("_dispMC", self.asset.get("mc", self.asset.get("vom", 0))) or 0) + float(self.asset.get("vom", 0) or 0)

    def declare_parameters(self, context: BuildContext) -> None:
        context.require_capabilities(self.supported_capabilities, self.asset_id)

    def declare_variables(self, context: BuildContext) -> None:
        # Transitional assembler owns the common dispatch variable m.p.
        if not hasattr(context.builder, "p"):
            raise UnsupportedComponentOperation("builder must expose the shared dispatch variable 'p'")

    def add_constraints(self, context: BuildContext) -> None:
        import pyomo.environ as pyo
        safe_id = re.sub(r"[^A-Za-z0-9_]", "_", self.asset_id)
        suffix = sha1(self.asset_id.encode("utf-8")).hexdigest()[:8]
        name = f"SharedUB_{safe_id}_{suffix}"
        constraint = pyo.Constraint(context.periods, rule=lambda model, t:
            model.p[self.asset_id, t] <= self.available_mw(context, context.periods.index(t)))
        context.builder.add_component(name, constraint)
        availability = {t: self.available_mw(context, i) for i, t in enumerate(context.periods)}
        curtailment = {t: availability[t] - context.builder.p[self.asset_id, t]
                       for t in context.periods}
        context.power_ports.extend((PowerInjectionPort(self.asset_id, context.builder.p[self.asset_id, :]),
                                    AvailabilityPort(self.asset_id, availability),
                                    CurtailmentPort(self.asset_id, curtailment)))

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        terms = [CostTerm(self.asset_id, "variable_cost", expression=self.marginal_cost() *
                          context.builder.p[self.asset_id, t] * context.duration_hours)
                 for t in context.periods]
        context.cost_terms.extend(terms)
        return terms

    def boundary_state(self, solution: Any, at: int) -> BoundaryState:
        return BoundaryState(self.asset_id, {})

    def extract(self, solution: Any, context: BuildContext) -> list[ComponentResult]:
        import pyomo.environ as pyo
        results = []
        for index, period in enumerate(context.periods):
            available = self.available_mw(context, index)
            dispatched = float(pyo.value(solution.p[self.asset_id, period]) or 0)
            curtailed = max(0.0, available - dispatched)
            coordinate = int(round(context.offset_hours / context.duration_hours)) + period - 1
            results.append(ComponentResult(self.asset_id, self.kind, coordinate,
                available_mw=available, injection_mw=dispatched, curtailed_mw=curtailed,
                curtailed_mwh=curtailed * context.duration_hours,
                cost_usd=self.marginal_cost() * dispatched * context.duration_hours))
        return results

    def qa_spec(self) -> ComponentQAMetadata:
        return ComponentQAMetadata(self.kind, ("availability_bound", "resource_accounting", "curtailment_energy"))
