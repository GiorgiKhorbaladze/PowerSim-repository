"""Backend-neutral solver status and gap helpers."""

from __future__ import annotations

import math

from powersim.contracts import SolverStatus


def normalize_termination(raw: object) -> SolverStatus:
    """Map a Pyomo/backend termination value without overstating success."""
    text = str(raw or "").strip().lower().replace(" ", "_")
    if "time" in text and ("limit" in text or "max" in text):
        return SolverStatus.TIME_LIMIT
    if "infeasibleorunbounded" in text or "infeasible_or_unbounded" in text:
        return SolverStatus.SOLVER_ERROR
    if "infeasible" in text:
        return SolverStatus.INFEASIBLE
    if "unbounded" in text:
        return SolverStatus.UNBOUNDED
    if any(word in text for word in ("numerical", "error_evaluating", "invalid_problem")):
        return SolverStatus.NUMERICAL_ERROR
    if text in {"optimal", "globallyoptimal", "locallyoptimal"} or text.endswith(".optimal"):
        return SolverStatus.OPTIMAL
    if any(word in text for word in ("feasible", "maxtime", "maxiterations")):
        return SolverStatus.FEASIBLE
    return SolverStatus.SOLVER_ERROR


def actual_mip_gap(incumbent: float | None, bound: float | None, *, epsilon: float = 1e-10) -> float | None:
    """Return the relative primal/dual gap (valid for either objective sense)."""
    if incumbent is None or bound is None:
        return None
    if not math.isfinite(incumbent) or not math.isfinite(bound):
        return None
    return abs(incumbent - bound) / max(abs(incumbent), epsilon)
