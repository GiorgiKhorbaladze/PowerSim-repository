"""Independent QA over canonical Stage-3A results (never live constraints)."""
from __future__ import annotations

import math
from collections import defaultdict

from powersim.components.results import ComponentResult
from powersim.contracts import QACheckResult, QAStatus

from .tolerance import DEFAULT_TOLERANCES, TolerancePolicy


def _record(
    buckets: dict,
    check_id: str,
    *,
    passed: bool,
    violation: float,
    witness: dict,
    tolerance: float | None = None,
) -> None:
    bucket = buckets.setdefault(
        check_id,
        {
            "passed": True,
            "max_violation": 0.0,
            "checked": 0,
            "witnesses": [],
            "tolerance": tolerance,
        },
    )
    bucket["passed"] = bucket["passed"] and passed
    bucket["checked"] += 1
    bucket["max_violation"] = max(bucket["max_violation"], max(0.0, violation))
    if tolerance is not None:
        bucket["tolerance"] = max(bucket["tolerance"] or 0.0, tolerance)
    if not passed and len(bucket["witnesses"]) < 10:
        bucket["witnesses"].append(witness)


def check_component_results(
    results: list[ComponentResult],
    duration_hours: float,
    tolerance: TolerancePolicy | None = None,
) -> list[QACheckResult]:
    """Return canonical QA checks for shared component results.

    The checks intentionally reconstruct physics from the extracted component
    results and do not inspect live Pyomo constraints.
    """
    tolerance = tolerance or DEFAULT_TOLERANCES
    buckets: dict[str, dict] = {}

    for result in results:
        values = {
            "available_mw": result.available_mw,
            "injection_mw": result.injection_mw,
            "withdrawal_mw": result.withdrawal_mw,
            "curtailed_mw": result.curtailed_mw,
            "curtailed_mwh": result.curtailed_mwh,
        }
        finite = all(value is None or math.isfinite(float(value)) for value in values.values())
        negatives = {
            key: value
            for key, value in values.items()
            if value is not None and math.isfinite(float(value)) and float(value) < 0
        }
        finite_nonnegative = finite and not negatives
        _record(
            buckets,
            "component.finite_nonnegative",
            passed=finite_nonnegative,
            violation=0.0 if finite_nonnegative else 1.0,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "component_kind": result.component_kind,
                "values": values,
            },
        )

        available = result.available_mw
        injection = result.injection_mw
        if available is None or not math.isfinite(float(available)) or not math.isfinite(float(injection)):
            continue

        scale = max(1.0, abs(float(available)), abs(float(injection)))
        threshold = tolerance.limit(scale)
        bound_violation = max(0.0, float(injection) - float(available))
        check_id = (
            "component.import_bound"
            if result.component_kind == "import"
            else "component.availability_bound"
        )
        _record(
            buckets,
            check_id,
            passed=bound_violation <= threshold,
            violation=bound_violation,
            tolerance=threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "available_mw": available,
                "injection_mw": injection,
            },
        )

        if result.component_kind == "import" or result.curtailed_mw is None:
            continue

        curtailed = float(result.curtailed_mw)
        accounting = abs(float(available) - float(injection) - curtailed)
        _record(
            buckets,
            "component.resource_accounting",
            passed=accounting <= threshold,
            violation=accounting,
            tolerance=threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "available_mw": available,
                "injection_mw": injection,
                "curtailed_mw": curtailed,
            },
        )

        expected_mwh = curtailed * duration_hours
        actual_mwh = float(result.curtailed_mwh or 0.0)
        energy_threshold = tolerance.limit(max(1.0, abs(expected_mwh), abs(actual_mwh)))
        energy_violation = abs(actual_mwh - expected_mwh)
        _record(
            buckets,
            "component.curtailment_energy",
            passed=energy_violation <= energy_threshold,
            violation=energy_violation,
            tolerance=energy_threshold,
            witness={
                "asset_id": result.asset_id,
                "period": result.period,
                "curtailed_mw": curtailed,
                "duration_hours": duration_hours,
                "curtailed_mwh": actual_mwh,
                "expected_mwh": expected_mwh,
            },
        )

    checks: list[QACheckResult] = []
    for check_id in sorted(buckets):
        bucket = buckets[check_id]
        status = QAStatus.PASS if bucket["passed"] else QAStatus.FAIL
        checks.append(
            QACheckResult(
                check_id=check_id,
                status=status,
                message=(
                    f"{bucket['checked']} shared-component observation(s) checked"
                    if bucket["passed"]
                    else f"{len(bucket['witnesses'])} witness(es) show a component invariant violation"
                ),
                witness={"worst": bucket["witnesses"]},
                tolerance=bucket["tolerance"],
                max_violation=bucket["max_violation"],
                checked_count=bucket["checked"],
            )
        )
    return checks
