"""Independent reserve-product QA over canonical extracted allocations."""
from __future__ import annotations

import math
from collections import defaultdict

from powersim.components.results import ReserveResult
from powersim.contracts import QACheckResult, QAStatus

from .tolerance import TolerancePolicy


def _check(check_id: str, passed: bool, violation: float, witness: dict,
           tolerance: float, checked: int = 1) -> QACheckResult:
    return QACheckResult(
        check_id=check_id,
        status=QAStatus.PASS if passed else QAStatus.FAIL,
        message="reserve invariant holds" if passed else "reserve invariant violated",
        witness=witness,
        tolerance=tolerance,
        max_violation=violation,
        checked_count=checked,
    )


def check_reserve_results(results: list[ReserveResult], duration_hours: float,
                          resolved_input: dict, tolerance: TolerancePolicy) -> list[QACheckResult]:
    """Reconstruct product and provider feasibility without Pyomo constraints."""
    products = {str(p.get("id")): p for p in (resolved_input.get("reserve_products") or []) if isinstance(p, dict)}
    assets = {str(a.get("id")): a for a in (resolved_input.get("assets") or []) if isinstance(a, dict)}
    records = [r for r in results if r.product_id in products]
    if not records:
        return []
    out: list[QACheckResult] = []
    bad = [r for r in records if not all(math.isfinite(float(v)) for v in (r.provided_mw, r.effective_provided_mw, r.derating_factor)) or r.provided_mw < 0 or r.effective_provided_mw < 0]
    out.append(_check("reserve.finite_nonnegative", not bad, 0.0 if not bad else 1.0,
                      {"records": len(records), "bad": [r.product_id for r in bad[:10]]}, tolerance.limit(1.0), len(records)))

    provider_rows = [r for r in records if r.provider_id is not None]
    system_rows = [r for r in records if r.provider_id is None]
    eligibility_bad = []
    capability_bad = []
    stacking: dict[tuple[int, str, str], list[ReserveResult]] = defaultdict(list)
    for r in provider_rows:
        product = products[r.product_id]
        allowed = {str(x) for x in product.get("eligible_units", [])}
        if r.provider_id not in allowed or r.provider_id not in assets:
            eligibility_bad.append({"product": r.product_id, "provider": r.provider_id})
        cap = float(r.physical_capability_mw or 0.0)
        limit = tolerance.limit(max(1.0, cap, r.provided_mw))
        if r.provided_mw - cap > limit:
            capability_bad.append({"product": r.product_id, "provider": r.provider_id, "provided_mw": r.provided_mw, "capability_mw": cap})
        stacking[(r.period, r.provider_id, r.direction)].append(r)
    out.append(_check("reserve.eligibility", not eligibility_bad, 0.0 if not eligibility_bad else 1.0,
                      {"bad": eligibility_bad[:10]}, tolerance.limit(1.0), len(provider_rows)))
    out.append(_check("reserve.provider_capability", not capability_bad, 0.0 if not capability_bad else max(x["provided_mw"] - x["capability_mw"] for x in capability_bad),
                      {"bad": capability_bad[:10]}, tolerance.limit(1.0), len(provider_rows)))

    stacking_bad = []
    for key, rows in stacking.items():
        total = sum(float(r.provided_mw) for r in rows)
        # Every row holds the same physical cap for a direction/time.  Taking
        # the minimum is conservative if future provider-specific products add
        # tighter limits.
        cap = min(float(r.physical_capability_mw or 0.0) for r in rows)
        limit = tolerance.limit(max(1.0, total, cap))
        if total - cap > limit:
            stacking_bad.append({"period": key[0], "provider": key[1], "direction": key[2], "allocated_mw": total, "capability_mw": cap})
    out.append(_check("reserve.stacking", not stacking_bad, 0.0 if not stacking_bad else max(x["allocated_mw"] - x["capability_mw"] for x in stacking_bad),
                      {"bad": stacking_bad[:10]}, tolerance.limit(1.0), len(stacking)))

    providers_by_product_direction: dict[tuple[int, str, str], float] = defaultdict(float)
    for r in provider_rows:
        providers_by_product_direction[(r.period, r.product_id, r.direction)] += float(r.effective_provided_mw)
    coverage_bad = []
    requirement_bad = []
    penalty_bad = []
    for r in system_rows:
        product = products[r.product_id]
        profile_key = product.get("requirement_profile")
        if profile_key is None:
            expected_requirement = float(product.get("requirement", 0.0) or 0.0)
        else:
            values = (resolved_input.get("profiles") or {}).get(profile_key)
            start_h = float(((resolved_input.get("study_horizon") or {}).get("start_hour", 0) or 0))
            index = int(round(r.period - start_h / duration_hours))
            expected_requirement = float(values[index]) if isinstance(values, list) and 0 <= index < len(values) else float("nan")
        req_limit = tolerance.limit(max(1.0, abs(expected_requirement), abs(float(r.requirement_mw or 0.0))))
        if not math.isfinite(expected_requirement) or abs(float(r.requirement_mw or 0.0) - expected_requirement) > req_limit:
            requirement_bad.append({"product": r.product_id, "period": r.period, "reported_mw": r.requirement_mw, "expected_mw": expected_requirement})
        directions = ("up", "down") if r.direction == "symmetric" else (r.direction,)
        expected_penalty = float(r.shortfall_mw or 0.0) * float(product.get("shortfall_penalty", 500) or 0.0) * duration_hours
        if r.shortfall_penalty_usd is not None and abs(float(r.shortfall_penalty_usd) - expected_penalty) > tolerance.limit(max(1.0, expected_penalty, float(r.shortfall_penalty_usd))):
            penalty_bad.append({"product": r.product_id, "period": r.period, "reported": r.shortfall_penalty_usd, "expected": expected_penalty})
        for direction in directions:
            supplied = providers_by_product_direction[(r.period, r.product_id, direction)] + float(r.shortfall_mw or 0.0)
            required = float(r.requirement_mw or 0.0)
            limit = tolerance.limit(max(1.0, supplied, required))
            if required - supplied > limit:
                coverage_bad.append({"product": r.product_id, "period": r.period, "direction": direction, "provided_plus_shortfall_mw": supplied, "requirement_mw": required})
    out.append(_check("reserve.product_requirement", not coverage_bad, 0.0 if not coverage_bad else max(x["requirement_mw"] - x["provided_plus_shortfall_mw"] for x in coverage_bad),
                      {"bad": coverage_bad[:10]}, tolerance.limit(1.0), len(system_rows)))
    out.append(_check("reserve.requirement_profile", not requirement_bad, 0.0 if not requirement_bad else 1.0,
                      {"bad": requirement_bad[:10]}, tolerance.limit(1.0), len(system_rows)))
    out.append(_check("reserve.shortfall_penalty", not penalty_bad, 0.0 if not penalty_bad else 1.0,
                      {"bad": penalty_bad[:10]}, tolerance.limit(1.0), len(system_rows)))
    return out
