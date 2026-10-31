"""Independent check of monthly operating envelopes and energy budgets.

Re-derives, from the resolved workflow input and the canonical per-unit
dispatch only, that every asset declaring ``monthly_pmin_mw``,
``monthly_pmax_mw``, ``pmin_profile`` or ``monthly_energy_mwh`` respected
them.  It deliberately does not import the solver: the month calendar and
the bounds are recomputed here.
"""
from __future__ import annotations

from typing import Any

from powersim.contracts import QACheckResult, QAStatus
from .tolerance import TolerancePolicy

_MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)
_FIELDS = ("monthly_pmin_mw", "monthly_pmax_mw", "pmin_profile", "monthly_energy_mwh")


def _month(hour: float) -> int:
    h = int(hour) % 8760
    return next(index + 1 for index, end in enumerate(_MONTH_END_HOURS) if h < end)


def _series(raw: Any) -> list[float | None] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        values: list[float | None] = [None] * 12
        for key, value in raw.items():
            values[int(key) - 1] = None if value is None else float(value)
        return values
    return [None if value is None else float(value) for value in raw]


class OperatingEnvelopeCheck:
    check_id = "operating_envelope"

    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy, *, persisted: bool = False) -> QACheckResult:
        inp = resolved_input if isinstance(resolved_input, dict) else {}
        if hasattr(result, "model_dump"):
            result = result.model_dump(mode="python")
        assets = [a for a in inp.get("assets") or [] if isinstance(a, dict) and any(a.get(f) is not None for f in _FIELDS)]
        if not assets:
            return QACheckResult(check_id=self.check_id, status=QAStatus.NOT_RUN,
                                 message="no asset declares a monthly envelope or energy budget", witness={})
        by_unit = (result or {}).get("hourly_by_unit") if isinstance(result, dict) else None
        if not isinstance(by_unit, dict):
            return QACheckResult(check_id=self.check_id, status=QAStatus.FAIL,
                                 message="per-unit dispatch is unavailable for an asset with an operating envelope",
                                 witness={"assets": [a.get("id") for a in assets]})
        dt = float(inp.get("resolution_min", 60)) / 60.0
        start_h = float((inp.get("study_horizon") or {}).get("start_hour", 0))
        profiles = inp.get("profiles") or {}
        power_tol = (tolerance.persisted_absolute + tolerance.persisted_power_rounding) if persisted else tolerance.internal_absolute
        violations: list[dict[str, Any]] = []
        worst = 0.0
        checked = 0
        for asset in assets:
            aid = str(asset.get("id"))
            rows = by_unit.get(aid)
            if not isinstance(rows, list):
                violations.append({"asset": aid, "issue": "dispatch missing"})
                continue
            pmin = _series(asset.get("monthly_pmin_mw"))
            pmax = _series(asset.get("monthly_pmax_mw"))
            energy = _series(asset.get("monthly_energy_mwh"))
            floor_profile = profiles.get(asset["pmin_profile"]) if isinstance(asset.get("pmin_profile"), str) else None
            start_p = int(round(start_h / dt))
            month_energy: dict[int, float] = {}
            month_periods: dict[int, int] = {}
            for row in rows:
                # canonical rows carry the study period index (offset included)
                t = int(row.get("t", 0))
                month = _month(t * dt)
                p = float(row.get("dispatch_mw", 0.0) or 0.0)
                month_energy[month] = month_energy.get(month, 0.0) + p * dt
                month_periods[month] = month_periods.get(month, 0) + 1
                cap = pmax[month - 1] if pmax and pmax[month - 1] is not None else None
                floor = pmin[month - 1] if pmin and pmin[month - 1] is not None else 0.0
                if floor_profile is not None:
                    # full-year profiles are indexed by study period; shorter ones by horizon period
                    index = t if len(floor_profile) >= 8760 else t - start_p
                    if 0 <= index < len(floor_profile):
                        floor = max(floor, float(floor_profile[index]))
                if cap is not None:
                    floor = min(floor, cap)
                    checked += 1
                    if p - cap > power_tol:
                        violation = p - cap
                        worst = max(worst, violation)
                        violations.append({"asset": aid, "t": t, "issue": "above monthly_pmax_mw", "value": p, "limit": cap})
                if floor > 0:
                    checked += 1
                    if floor - p > power_tol and row.get("commitment", 1):
                        violation = floor - p
                        worst = max(worst, violation)
                        violations.append({"asset": aid, "t": t, "issue": "below forced minimum", "value": p, "limit": floor})
            if energy:
                mode = asset.get("monthly_energy_mode", "max")
                for month, total in sorted(month_energy.items()):
                    budget = energy[month - 1]
                    if budget is None:
                        continue
                    checked += 1
                    # each persisted power value may be rounded by up to power_tol
                    limit = tolerance.limit(budget, persisted=persisted) + power_tol * dt * month_periods[month]
                    excess = total - budget
                    if excess > limit or (mode == "exact" and -excess > limit):
                        worst = max(worst, abs(excess))
                        violations.append({"asset": aid, "month": month, "issue": f"monthly energy ({mode})",
                                           "value": round(total, 6), "limit": budget, "tolerance": limit})
        status = QAStatus.FAIL if violations else QAStatus.PASS
        message = (f"{len(violations)} operating-envelope violation(s)" if violations
                   else f"monthly envelopes and energy budgets hold for {len(assets)} asset(s)")
        return QACheckResult(check_id=self.check_id, status=status, message=message,
                             witness={"violations": violations[:20], "tolerance_mode": "persisted" if persisted else "internal"},
                             checked_count=checked, max_violation=worst)
