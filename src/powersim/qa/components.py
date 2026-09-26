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
    resolved_input=None,
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

        # VRE curtailment is available generation that was not injected.
        # Demand response uses ``curtailed_*`` to mean deliberately reduced
        # demand, which is represented as a positive supply injection.  Its
        # accounting is checked separately below.
        if result.component_kind in {"import", "dr"} or result.curtailed_mw is None:
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

    # Thermal UC QA uses only resolved input and canonical extraction.  It
    # intentionally does not inspect the Pyomo model or its live constraints.
    raw_assets = (resolved_input or {}).get("assets", []) if isinstance(resolved_input, dict) else []
    asset_map = {str(a.get("id")): a for a in raw_assets if isinstance(a, dict) and a.get("id") is not None}
    thermal_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "thermal":
            thermal_by_asset[item.asset_id].append(item)
    for asset_id, observations in thermal_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        pmin, pmax = float(asset.get("pmin", 0) or 0), float(asset.get("pmax", 0) or 0)
        ramp_up, ramp_down = float(asset.get("ramp_up", 9999) or 9999), float(asset.get("ramp_down", 9999) or 9999)
        for item in observations:
            u, dispatch = float(item.commitment if item.commitment is not None else 1.0), float(item.injection_mw)
            lo, hi = pmin * u, pmax * u
            limit = tolerance.limit(max(1.0, abs(lo), abs(hi), abs(dispatch)))
            violation = max(0.0, lo - dispatch, dispatch - hi)
            _record(buckets, "component.thermal_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "dispatch_mw": dispatch, "commitment": u, "pmin_mw": lo, "pmax_mw": hi})
        for previous, current in zip(observations, observations[1:]):
            delta = float(current.injection_mw) - float(previous.injection_mw)
            for check_id, rate, violation in (
                ("component.thermal_ramp_up", ramp_up, max(0.0, delta - ramp_up * duration_hours)),
                ("component.thermal_ramp_down", ramp_down, max(0.0, -delta - ramp_down * duration_hours)),
            ):
                if rate < 9999:
                    limit = tolerance.limit(max(1.0, rate * duration_hours, abs(delta)))
                    _record(buckets, check_id, passed=violation <= limit, violation=violation, tolerance=limit,
                            witness={"asset_id": asset_id, "period": current.period, "delta_mw": delta, "limit_mw": rate * duration_hours})
            expected = float(current.commitment or 0) - float(previous.commitment or 0)
            actual = float(current.startup or 0) - float(current.shutdown or 0)
            violation, limit = abs(expected - actual), tolerance.limit(1.0)
            _record(buckets, "component.thermal_uc_transition", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": current.period, "commitment_delta": expected, "startup_minus_shutdown": actual})

    bess_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "bess":
            bess_by_asset[item.asset_id].append(item)
    for asset_id, observations in bess_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        energy = float(asset.get("energy_mwh", 0) or 0)
        lower = float(asset.get("soc_min", 0) or 0) * energy
        upper = float(asset.get("soc_max", 1) or 1) * energy
        charge_cap = float(asset.get("charge_power_mw", asset.get("power_mw", 0)) or 0)
        discharge_cap = float(asset.get("discharge_power_mw", asset.get("power_mw", 0)) or 0)
        for item in observations:
            soc = float(item.state_of_charge_mwh or 0)
            violation = max(0.0, lower - soc, soc - upper, -float(item.injection_mw), -float(item.withdrawal_mw), float(item.injection_mw) - discharge_cap, float(item.withdrawal_mw) - charge_cap)
            limit = tolerance.limit(max(1.0, energy, charge_cap, discharge_cap))
            _record(buckets, "component.bess_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "soc_mwh": soc, "charge_mw": item.withdrawal_mw, "discharge_mw": item.injection_mw})
            mode_violation = min(float(item.injection_mw), float(item.withdrawal_mw))
            _record(buckets, "component.bess_mode", passed=mode_violation <= limit, violation=mode_violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "charge_mw": item.withdrawal_mw, "discharge_mw": item.injection_mw})

    ph_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "pumped_hydro":
            ph_by_asset[item.asset_id].append(item)
    for asset_id, observations in ph_by_asset.items():
        asset = asset_map.get(asset_id, {})
        cap = float(asset.get("energy_mwh", 0) or 0)
        lower, upper = float(asset.get("soc_min", 0) or 0) * cap, float(asset.get("soc_max", 1) or 1) * cap
        gen_cap, pump_cap = float(asset.get("pmax", 0) or 0), float(asset.get("pump_mw", 0) or 0)
        for item in observations:
            soc = float(item.state_of_charge_mwh or 0)
            violation = max(0.0, lower-soc, soc-upper, float(item.injection_mw)-gen_cap, float(item.withdrawal_mw)-pump_cap, -float(item.injection_mw), -float(item.withdrawal_mw))
            limit = tolerance.limit(max(1.0, cap, gen_cap, pump_cap))
            _record(buckets, "component.pumped_hydro_bounds", passed=violation <= limit, violation=violation, tolerance=limit, witness={"asset_id":asset_id,"period":item.period,"soc_mwh":soc})
            violation = min(float(item.injection_mw), float(item.withdrawal_mw))
            _record(buckets, "component.pumped_hydro_mode", passed=violation <= limit, violation=violation, tolerance=limit, witness={"asset_id":asset_id,"period":item.period})

    dr_by_asset: dict[str, list[ComponentResult]] = defaultdict(list)
    for item in results:
        if item.component_kind == "dr":
            dr_by_asset[item.asset_id].append(item)
    for asset_id, observations in dr_by_asset.items():
        asset = asset_map.get(asset_id, {})
        observations.sort(key=lambda item: item.period)
        cap = float(asset.get("pmax_curtail", 0) or 0)
        total_energy = 0.0
        for item in observations:
            curtailed = float(item.curtailed_mw or 0)
            limit = tolerance.limit(max(1.0, cap, curtailed, float(item.injection_mw)))
            violation = max(0.0, curtailed - cap, abs(float(item.injection_mw) - curtailed))
            _record(buckets, "component.demand_response_bounds", passed=violation <= limit, violation=violation, tolerance=limit,
                    witness={"asset_id": asset_id, "period": item.period, "curtailed_mw": curtailed, "injection_mw": item.injection_mw, "available_mw": item.available_mw})
            total_energy += float(item.curtailed_mwh or 0)
            expected_cost = float(asset.get("price_per_mwh", 0) or 0) * float(item.curtailed_mwh or 0)
            cost_violation = abs(float(item.cost_usd or 0) - expected_cost)
            _record(buckets, "component.demand_response_cost", passed=cost_violation <= tolerance.limit(max(1.0, abs(expected_cost))), violation=cost_violation,
                    tolerance=tolerance.limit(max(1.0, abs(expected_cost))), witness={"asset_id": asset_id, "period": item.period, "cost_usd": item.cost_usd, "expected_cost_usd": expected_cost})
        hours = float(asset.get("hours_per_year_max", 8760) or 0)
        horizon_hours = len(observations) * duration_hours
        cap_mwh = cap * hours * min(1.0, horizon_hours / 8760.0)
        annual_violation = max(0.0, total_energy - cap_mwh)
        annual_limit = tolerance.limit(max(1.0, cap_mwh, total_energy))
        _record(buckets, "component.demand_response_energy_cap", passed=annual_violation <= annual_limit, violation=annual_violation, tolerance=annual_limit,
                witness={"asset_id": asset_id, "curtailed_mwh": total_energy, "cap_mwh": cap_mwh, "horizon_hours": horizon_hours})

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
