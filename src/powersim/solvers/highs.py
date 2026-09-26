"""Pyomo APPSI adapter for HiGHS."""

from __future__ import annotations

import math
import time
from typing import Any

from powersim.contracts import QAStatus, SolverDiagnostics, SolverStatus
from .base import SolveOutcome, SolverUnavailableError, initial_validity, utc_now, validate_common_options
from .status import actual_mip_gap, normalize_termination


class HighsBackend:
    name = "highs"
    _supported = {"mip_gap", "time_limit_s", "threads", "log_to_console"}

    def available(self) -> bool:
        try:
            from pyomo.contrib.appsi.solvers.highs import Highs
            return bool(Highs().available())
        except Exception:
            return False

    def version(self) -> str | None:
        try:
            import highspy
            return highspy.Highs().version()
        except Exception:
            return None

    def normalize_options(self, options: dict[str, Any], *, compatibility: bool = False) -> dict[str, Any]:
        return validate_common_options(options, self._supported, compatibility=compatibility)

    def solve(self, model: Any, options: dict[str, Any] | None = None, *, requested_backend: str | None = None) -> SolveOutcome:
        if not self.available():
            raise SolverUnavailableError("HiGHS is not available")
        from pyomo.contrib.appsi.solvers.highs import Highs
        effective = self.normalize_options(options or {})
        solver = Highs()
        mapping = {"mip_gap": "mip_rel_gap", "time_limit_s": "time_limit", "threads": "threads", "log_to_console": "log_to_console"}
        for key, value in effective.items():
            solver.highs_options[mapping[key]] = value
        solver.config.load_solution = False
        started = utc_now(); tick = time.perf_counter()
        try:
            raw = solver.solve(model)
            elapsed = time.perf_counter() - tick
            termination = str(raw.termination_condition)
            status = normalize_termination(raw.termination_condition)
            incumbent = _finite(getattr(raw, "best_feasible_objective", None))
            bound = _finite(getattr(raw, "best_objective_bound", None))
            has_incumbent = incumbent is not None
            if status in {SolverStatus.OPTIMAL, SolverStatus.FEASIBLE} and not has_incumbent:
                metadata_status = status.value
                status = SolverStatus.SOLVER_ERROR
            else:
                metadata_status = None
            if has_incumbent:
                solver.load_vars()
            gap = actual_mip_gap(incumbent, bound) if has_incumbent else None
            backend_runtime = _finite(getattr(raw, "wallclock_time", None))
            metadata: dict[str, Any] = {"actual_gap_source": "incumbent_and_bound" if gap is not None else None}
            if metadata_status:
                metadata["status_downgrade_reason"] = f"{metadata_status} reported without a finite incumbent"
            if backend_runtime is None:
                metadata["runtime_unavailable_reason"] = "APPSI result did not expose backend wallclock_time"
        except Exception as exc:
            elapsed = time.perf_counter() - tick
            raw = None; termination = f"exception: {type(exc).__name__}: {exc}"
            status = normalize_termination(exc); incumbent = bound = gap = None; has_incumbent = False
            backend_runtime = None
            metadata = {"exception_type": type(exc).__name__, "runtime_unavailable_reason": "solve raised before backend runtime was reported"}
        diagnostics = SolverDiagnostics(
            backend=self.name, backend_version=self.version(), requested_backend=requested_backend or self.name,
            termination_condition=termination, normalized_status=status, incumbent_objective=incumbent,
            best_bound=bound, actual_mip_gap=gap, requested_mip_gap=effective.get("mip_gap"),
            runtime_s=backend_runtime, orchestration_runtime_s=elapsed, has_incumbent=has_incumbent,
            result_validity=initial_validity(status, has_incumbent), qa_status=QAStatus.NOT_RUN,
            effective_options=effective, solve_started_at=started, solve_finished_at=utc_now(), metadata=metadata)
        return SolveOutcome(diagnostics, raw)

    def cancel(self) -> bool:
        return False  # APPSI HiGHS exposes no safe cross-thread cancellation API.


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None
