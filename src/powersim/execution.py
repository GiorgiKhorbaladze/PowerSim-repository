"""Application-layer execution and typed-to-workflow adaptation.

This module deliberately owns orchestration only.  It never creates Pyomo
constraints or substitutes a second electrical model for the validated solver.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from powersim.contracts import (QACheckResult, QAReport, QAStatus, ResolvedInputContract,
                                ResultEnvelope, ResultValidity, SolverDiagnostics, SolverStatus)


def resolved_to_workflow_input(resolved: ResolvedInputContract) -> dict[str, Any]:
    """Build the existing workflow input with typed fields taking precedence.

    ``legacy_payload`` is retained only for compatibility-only fields that
    do not yet have typed counterparts.  Typed assets, profiles, network,
    reserves and solver settings are always projected from the immutable
    resolved snapshot.
    """
    base = copy.deepcopy(dict(resolved.legacy_payload or {}))
    base.setdefault("metadata", {})
    base["metadata"].update(dict(resolved.metadata))
    base["metadata"]["project_id"] = resolved.project_id
    base["metadata"]["scenario_id"] = resolved.scenario_id
    # The typed selector, not legacy metadata, chooses the authoritative
    # workflow registry entry.
    base["_powersim_workflow"] = resolved.workflow
    base["metadata"]["study_year"] = resolved.time.study_year
    base["metadata"]["timezone"] = resolved.time.timezone
    base["resolution_min"] = resolved.time.resolution_minutes
    base["profiles"] = {p.id: list(p.values) for p in resolved.profiles}
    assets = []
    for asset in resolved.assets:
        raw = copy.deepcopy(dict(asset.legacy_extensions))
        raw.update({"id": asset.id, "type": asset.kind, "pmin": asset.capacity_min_mw,
                    "pmax": asset.capacity_max_mw, "bus": asset.bus})
        for key, value in (("availability_profile", asset.profile_references[0] if asset.profile_references else None),):
            if value is not None: raw[key] = value
        assets.append({k: v for k, v in raw.items() if v is not None})
    base["assets"] = assets
    base["reserve_products"] = [dict(x) for x in resolved.reserve_products]
    base["solver_settings"] = dict(resolved.solver_settings)
    if resolved.network.buses:
        base["buses"] = [b.model_dump(mode="json", exclude={"provenance"}) for b in resolved.network.buses]
    if resolved.network.branches:
        base["lines"] = [b.model_dump(mode="json", exclude={"provenance"}) for b in resolved.network.branches]
    return base


class LocalWorkflowExecutor:
    """Synchronous local executor for a configured validated workflow runner.

    A runner receives the canonical workflow input and returns a fully gated
    :class:`ResultEnvelope`.  The default is intentionally absent rather than
    pretending that an unconfigured environment solved a study.
    """
    def __init__(self, runner: Callable[[dict[str, Any], str, str], ResultEnvelope] | None = None):
        self.runner = runner

    def execute(self, run_id: str, resolved_input_path: str) -> ResultEnvelope:
        if self.runner is None:
            raise RuntimeError("no local workflow runner configured")
        resolved = ResolvedInputContract.model_validate_json(Path(resolved_input_path).read_text(encoding="utf-8"))
        return self.runner(resolved_to_workflow_input(resolved), run_id, resolved.fingerprint())


def run_deterministic_workflow(workflow: dict[str, Any], run_id: str, snapshot_fingerprint: str) -> ResultEnvelope:
    """Execute the established deterministic solver and preserve its gate.

    The solver implementation, canonical extraction and independent QA remain
    in ``solver.powersim_solver``.  This wrapper only adapts the accepted
    result into the platform envelope.
    """
    from solver.powersim_solver import build_asset_map, build_gas_limits, build_result_store, slice_profiles, solve_all

    assets = build_asset_map(workflow)
    profiles, horizon_periods = slice_profiles(workflow)
    start_hour = int((workflow.get("study_horizon") or {}).get("start_hour", 0))
    gas_limits = build_gas_limits(workflow, start_hour, horizon_periods)
    hourly, elapsed, objective = solve_all(workflow, assets, profiles, gas_limits)
    raw = build_result_store(hourly, assets, workflow, elapsed, obj_total=objective)
    diagnostics = SolverDiagnostics.model_validate(raw["diagnostics"]["solver_diagnostics"])
    qa = QAReport.model_validate(raw["qa"])
    return ResultEnvelope(run_id=run_id, snapshot_fingerprint=snapshot_fingerprint,
                          created_at=datetime.now(timezone.utc), validity=diagnostics.result_validity,
                          solver=diagnostics, qa=qa, results=raw)


def _native_workflow_envelope(native: dict[str, Any], workflow_id: str,
                              run_id: str, snapshot_fingerprint: str) -> ResultEnvelope:
    """Adapt an already-gated workflow result without inventing optimizer data."""
    raw_diagnostics = native.get("solver_diagnostics")
    if isinstance(raw_diagnostics, dict):
        diagnostics = SolverDiagnostics.model_validate(raw_diagnostics)
    else:
        qa_data = native.get("qa") or {}
        qa_status = QAStatus(str(qa_data.get("status", "fail")))
        validity = ResultValidity(str(native.get("result_validity", "invalid")))
        diagnostics = SolverDiagnostics(
            backend=workflow_id, termination_condition="workflow_completed",
            normalized_status=SolverStatus.COMPLETED, has_incumbent=False,
            result_validity=validity, qa_status=qa_status,
            metadata={"workflow": workflow_id, "optimizer_diagnostics": "not_applicable"},
        )
    raw_qa = native.get("qa") or {}
    if isinstance(raw_qa, dict) and "checks" in raw_qa:
        qa = QAReport.model_validate(raw_qa)
    else:
        status = QAStatus(str(raw_qa.get("status", "fail"))) if isinstance(raw_qa, dict) else QAStatus.FAIL
        qa = QAReport(status=status, checks=[QACheckResult(
            check_id=f"{workflow_id}.native_qa", status=status,
            message="native workflow canonical QA result", witness=dict(raw_qa) if isinstance(raw_qa, dict) else {},
        )])
    return ResultEnvelope(run_id=run_id, snapshot_fingerprint=snapshot_fingerprint,
                          created_at=datetime.now(timezone.utc), validity=diagnostics.result_validity,
                          solver=diagnostics, qa=qa,
                          results={"workflow_id": workflow_id, **native})


def run_registered_workflow(workflow: dict[str, Any], run_id: str,
                            snapshot_fingerprint: str) -> ResultEnvelope:
    """The single application registry for already validated workflow runners."""
    workflow_id = str(workflow.pop("_powersim_workflow", "deterministic_uc"))
    if workflow_id == "deterministic_uc":
        return run_deterministic_workflow(workflow, run_id, snapshot_fingerprint)
    if workflow_id == "stochastic_uc":
        from solver.powersim_stochastic_shared import run_stochastic_shared
        native = run_stochastic_shared(workflow)
    elif workflow_id == "security_scuc":
        from solver.powersim_security_shared import run_security_shared
        native = run_security_shared(workflow)
    elif workflow_id == "chronological_adequacy":
        from solver.powersim_adequacy_chronological import run_chronological_adequacy
        native = run_chronological_adequacy(workflow)
    elif workflow_id == "scoped_expansion":
        from solver.powersim_expansion_scoped import run_scoped_expansion
        native = run_scoped_expansion(workflow)
    else:
        raise ValueError(f"unsupported workflow: {workflow_id}")
    return _native_workflow_envelope(native, workflow_id, run_id, snapshot_fingerprint)


class DeterministicLocalSolverExecutor(LocalWorkflowExecutor):
    """Backward-compatible production executor for every registered workflow."""
    def __init__(self):
        def registered(workflow: dict[str, Any], run_id: str, fingerprint: str) -> ResultEnvelope:
            return run_registered_workflow(workflow, run_id, fingerprint)
        super().__init__(registered)
