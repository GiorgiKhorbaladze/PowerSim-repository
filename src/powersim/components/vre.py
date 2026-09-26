"""Shared passive availability/dispatch physics matching the legacy solver."""
from __future__ import annotations

import math
import re
from hashlib import sha1
from typing import Any

from powersim.contracts import IssueSeverity

from .base import UnsupportedComponentOperation
from .context import BoundaryState, BuildContext, make_validation_issue
from .ports import AvailabilityPort, CurtailmentPort, PowerInjectionPort
from .results import ComponentQAMetadata, ComponentResult, CostTerm

MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)


def _finite_float(value: Any) -> float | None:
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def maintenance_factor(asset: dict, hour: int) -> float:
    """Legacy-compatible maintenance availability factor."""
    raw_capacity = asset.get("pmax", asset.get("pmax_installed", asset.get("power_mw", 0)))
    pmax = _finite_float(raw_capacity) or 0.0
    factor = 1.0
    for window in asset.get("maint_windows", []) or []:
        start = _finite_float(window.get("start_h", window.get("start")))
        end = _finite_float(window.get("end_h", window.get("end")))
        if start is None or end is None or not (start <= hour < end):
            continue
        if window.get("availability_factor") is not None:
            current = _finite_float(window["availability_factor"])
            current = 0.0 if current is None else current
        elif window.get("available_capacity_mw") is not None and pmax > 0:
            available = _finite_float(window["available_capacity_mw"])
            current = 0.0 if available is None else available / pmax
        else:
            current = 0.0
        factor = min(factor, max(0.0, min(1.0, current)))
    return factor


def temperature_factor(asset: dict, profiles: dict, index: int) -> float:
    """Legacy-compatible piecewise-linear ambient-temperature derating."""
    curve, key = asset.get("temp_derating_curve"), asset.get("temp_profile_key")
    series = profiles.get(key) if key else None
    if not curve or not isinstance(series, list) or index >= len(series):
        return 1.0
    temp = _finite_float(series[index])
    if temp is None:
        return 1.0
    try:
        points = sorted(curve, key=lambda p: float(p[0]))
        numeric = [(float(t), float(f)) for t, f in points]
    except (TypeError, ValueError):
        return 1.0
    if temp <= numeric[0][0]:
        return max(0.0, numeric[0][1])
    if temp >= numeric[-1][0]:
        return max(0.0, numeric[-1][1])
    for (t0, f0), (t1, f1) in zip(numeric, numeric[1:]):
        if temp <= t1:
            return max(0.0, f0 + (temp - t0) / max(t1 - t0, 1e-9) * (f1 - f0))
    return 1.0


class PassiveInjectionComponent:
    """Shared Stage-3A passive injection component base."""

    contract_version = "1.0"
    supported_capabilities = frozenset({"deterministic", "subhourly"})
    kind = "passive"

    def __init__(self, asset: dict[str, Any]):
        self.asset = asset
        self.asset_id = str(asset["id"])

    def _issue(
        self,
        code: str,
        message: str,
        *,
        field_name: str | None = None,
        severity: IssueSeverity = IssueSeverity.ERROR,
        compatibility: bool = False,
    ):
        return make_validation_issue(
            self.asset_id,
            code,
            message,
            field_name=field_name,
            severity=severity,
            compatibility=compatibility,
        )

    def validate(self, context: BuildContext) -> list:
        issues = []

        try:
            capacity = float(self.installed_capacity())
        except (TypeError, ValueError, OverflowError):
            capacity = None
        if capacity is None or not math.isfinite(capacity) or capacity < 0:
            issues.append(
                self._issue(
                    "invalid_capacity",
                    "capacity must be numeric, finite and non-negative",
                    field_name="pmax",
                )
            )

        key = self.profile_key()
        series = context.profiles.get(key) if key else None
        if key and series is None:
            if self.kind in {"wind", "solar", "hydro_ror"} and context.compatibility_mode:
                issues.append(
                    self._issue(
                        "legacy_missing_profile_fallback",
                        f"profile {key!r} is missing; legacy fallback semantics will be used",
                        field_name="availability_profile",
                        severity=IssueSeverity.WARNING,
                        compatibility=True,
                    )
                )
            else:
                issues.append(
                    self._issue(
                        "missing_profile",
                        f"profile {key!r} is missing",
                        field_name="pmax_profile" if self.kind == "import" else "availability_profile",
                    )
                )
        elif key:
            field_name = "pmax_profile" if self.kind == "import" else "availability_profile"
            if not isinstance(series, list) or len(series) < len(context.periods):
                issues.append(
                    self._issue(
                        "invalid_profile",
                        "profile must be a list covering every period",
                        field_name=field_name,
                    )
                )
            else:
                for index, value in enumerate(series[: len(context.periods)]):
                    numeric = _finite_float(value)
                    if numeric is None:
                        issues.append(
                            self._issue(
                                "nonfinite_profile",
                                f"profile contains a non-numeric or non-finite value at index {index}",
                                field_name=field_name,
                            )
                        )
                        break
                    if numeric < 0:
                        if self.kind == "import":
                            issues.append(
                                self._issue(
                                    "negative_import_capacity",
                                    f"import availability is negative at index {index}",
                                    field_name=field_name,
                                )
                            )
                            break
                        if context.compatibility_mode:
                            issues.append(
                                self._issue(
                                    "legacy_negative_profile",
                                    f"negative profile value at index {index} is clamped for legacy parity",
                                    field_name=field_name,
                                    severity=IssueSeverity.WARNING,
                                    compatibility=True,
                                )
                            )
                            break
                        issues.append(
                            self._issue(
                                "negative_profile",
                                f"profile contains a negative value at index {index}",
                                field_name=field_name,
                            )
                        )
                        break

        context.validation_issues.extend(issues)
        return issues

    def installed_capacity(self) -> float:
        return float(self.asset.get("pmax", 0) or 0)

    def profile_key(self) -> str | None:
        return self.asset.get("availability_profile")

    def default_profile_value(self) -> float:
        return 1.0

    def profile_value(self, context: BuildContext, index: int) -> float:
        key = self.profile_key()
        if not key:
            return self.default_profile_value()
        series = context.profiles.get(key)
        if isinstance(series, list) and index < len(series):
            return float(series[index])
        return self.default_profile_value()

    def available_mw(self, context: BuildContext, index: int) -> float:
        base = self.installed_capacity() * max(0.0, min(1.0, self.profile_value(context, index)))
        hour = int(context.offset_hours + round(index * context.duration_hours))
        return base * maintenance_factor(self.asset, hour) * temperature_factor(self.asset, context.profiles, index)

    def marginal_cost(self) -> float:
        # Preserve the legacy deterministic objective exactly, including the
        # historical _dispMC + vom convention.
        return float(
            self.asset.get("_dispMC", self.asset.get("mc", self.asset.get("vom", 0))) or 0
        ) + float(self.asset.get("vom", 0) or 0)

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
        period_index = {period: i for i, period in enumerate(context.periods)}
        constraint = pyo.Constraint(
            context.periods,
            rule=lambda model, t: model.p[self.asset_id, t]
            <= self.available_mw(context, period_index[t]),
        )
        context.builder.add_component(name, constraint)

        availability = {
            period: self.available_mw(context, index)
            for index, period in enumerate(context.periods)
        }
        curtailment = {
            period: availability[period] - context.builder.p[self.asset_id, period]
            for period in context.periods
        }
        context.power_ports.extend(
            (
                PowerInjectionPort(self.asset_id, context.builder.p[self.asset_id, :]),
                AvailabilityPort(self.asset_id, availability),
                CurtailmentPort(self.asset_id, curtailment),
            )
        )

    def objective_terms(self, context: BuildContext) -> list[CostTerm]:
        terms = [
            CostTerm(
                self.asset_id,
                "variable_cost",
                expression=self.marginal_cost()
                * context.builder.p[self.asset_id, period]
                * context.duration_hours,
            )
            for period in context.periods
        ]
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
            results.append(
                ComponentResult(
                    self.asset_id,
                    self.kind,
                    context.period_coordinate(index),
                    available_mw=available,
                    injection_mw=dispatched,
                    curtailed_mw=curtailed,
                    curtailed_mwh=curtailed * context.duration_hours,
                    cost_usd=self.marginal_cost()
                    * dispatched
                    * context.duration_hours,
                )
            )
        return results

    def qa_spec(self) -> ComponentQAMetadata:
        return ComponentQAMetadata(
            self.kind,
            ("availability_bound", "resource_accounting", "curtailment_energy"),
        )
