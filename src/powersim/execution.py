"""Application-layer execution and typed-to-workflow adaptation.

This module deliberately owns orchestration only.  It never creates Pyomo
constraints or substitutes a second electrical model for the validated solver.
"""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Callable

from powersim.contracts import ResolvedInputContract, ResultEnvelope


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
