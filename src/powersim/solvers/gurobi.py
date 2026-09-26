"""Pyomo APPSI adapter for Gurobi (conditionally available/licensed)."""

from __future__ import annotations

from typing import Any

from .highs import HighsBackend


class GurobiBackend(HighsBackend):
    name = "gurobi"
    _supported = {"mip_gap", "time_limit_s", "threads", "log_to_console"}

    def available(self) -> bool:
        try:
            from pyomo.contrib.appsi.solvers.gurobi import Gurobi
            return str(Gurobi().available()).split(".")[-1] not in {"NotFound", "BadLicense", "NotFound"}
        except Exception:
            return False

    def version(self) -> str | None:
        try:
            import gurobipy
            return ".".join(map(str, gurobipy.gurobi.version()))
        except Exception:
            return None

    def solve(self, model: Any, options: dict[str, Any] | None = None, *, requested_backend: str | None = None):
        # Kept independent of workflow code while sharing diagnostics semantics.
        from pyomo.contrib.appsi.solvers.gurobi import Gurobi
        from powersim.contracts import QAStatus, SolverDiagnostics
        from .base import SolveOutcome, SolverUnavailableError, initial_validity, utc_now
        from .status import actual_mip_gap, normalize_termination
        import math, time
        if not self.available(): raise SolverUnavailableError("Gurobi is unavailable or unlicensed")
        effective = self.normalize_options(options or {}); solver = Gurobi()
        mapping = {"mip_gap":"MIPGap", "time_limit_s":"TimeLimit", "threads":"Threads", "log_to_console":"OutputFlag"}
        for key, value in effective.items(): solver.gurobi_options[mapping[key]] = int(bool(value)) if key == "log_to_console" else value
        solver.config.load_solution = False; started = utc_now(); tick = time.perf_counter()
        try:
            raw = solver.solve(model); elapsed = time.perf_counter()-tick
            termination = str(raw.termination_condition); status = normalize_termination(raw.termination_condition)
            finite = lambda v: float(v) if v is not None and math.isfinite(float(v)) else None
            incumbent = finite(getattr(raw,"best_feasible_objective",None)); bound = finite(getattr(raw,"best_objective_bound",None))
            has_incumbent = incumbent is not None
            metadata_status = None
            if status.value in {"optimal","feasible"} and not has_incumbent:
                metadata_status=status.value; status=normalize_termination("solver error")
            if has_incumbent: solver.load_vars()
            backend_runtime = finite(getattr(raw,"wallclock_time",None))
            metadata = {"actual_gap_source":"incumbent_and_bound" if actual_mip_gap(incumbent,bound) is not None else None}
            if metadata_status: metadata["status_downgrade_reason"]=f"{metadata_status} reported without a finite incumbent"
            if backend_runtime is None: metadata["runtime_unavailable_reason"]="APPSI result did not expose backend wallclock_time"
        except Exception as exc:
            elapsed=time.perf_counter()-tick; raw=None; termination=f"exception: {type(exc).__name__}: {exc}"
            status=normalize_termination(exc); incumbent=bound=None; has_incumbent=False; backend_runtime=None
            metadata={"exception_type":type(exc).__name__,"runtime_unavailable_reason":"solve raised before backend runtime was reported"}
        diagnostics=SolverDiagnostics(backend=self.name,backend_version=self.version(),requested_backend=requested_backend or self.name,
            termination_condition=termination,normalized_status=status,incumbent_objective=incumbent,best_bound=bound,
            actual_mip_gap=actual_mip_gap(incumbent,bound),requested_mip_gap=effective.get("mip_gap"),runtime_s=backend_runtime,
            orchestration_runtime_s=elapsed,has_incumbent=has_incumbent,result_validity=initial_validity(status,has_incumbent),
            qa_status=QAStatus.NOT_RUN,effective_options=effective,solve_started_at=started,solve_finished_at=utc_now(),metadata=metadata)
        return SolveOutcome(diagnostics,raw)
