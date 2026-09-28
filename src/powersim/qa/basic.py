"""Stage-2 independent checks over canonical extracted values."""

from __future__ import annotations
import math
from typing import Any
from powersim.contracts import QACheckResult, QAStatus
from .tolerance import TolerancePolicy


def _rows(result: Any) -> list[dict[str, Any]]:
    if hasattr(result, "model_dump"): result = result.model_dump(mode="python")
    if not isinstance(result, dict): return []
    rows = result.get("hourly") or result.get("hourly_results") or result.get("hourly_system") or result.get("intervals") or []
    return list(rows) if isinstance(rows, (list, tuple)) else []


class FiniteValuesCheck:
    check_id = "finite_values"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy, *, persisted: bool = False) -> QACheckResult:
        bad: list[str] = []; checked = 0
        def visit(value: Any, path: str) -> None:
            nonlocal checked
            if isinstance(value, dict):
                for key, item in value.items(): visit(item, f"{path}.{key}")
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value): visit(item, f"{path}[{index}]")
            elif isinstance(value, float):
                checked += 1
                if not math.isfinite(value) and len(bad) < 10: bad.append(path)
            elif isinstance(value, int) and not isinstance(value, bool): checked += 1
        visit(result, "result")
        return QACheckResult(check_id=self.check_id, status=QAStatus.FAIL if bad else QAStatus.PASS,
            message=f"found {len(bad)} non-finite value(s)" if bad else f"all {checked} numeric values are finite",
            witness={"paths": bad, "tolerance_mode": "persisted" if persisted else "internal"}, checked_count=checked, max_violation=None)


class ElectricityBalanceCheck:
    check_id = "electricity_balance"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy, *, persisted: bool = False) -> QACheckResult:
        rows = _rows(result)
        if not rows:
            return QACheckResult(check_id=self.check_id, status=QAStatus.NOT_RUN,
                message="canonical interval results are unavailable", witness={})
        worst = 0.0; witness = {}; checked = 0
        for index, row in enumerate(rows):
            if not all(key in row for key in ("load_mw", "generation_mw")): continue
            duration_h = float(row.get("period_minutes", 60.0)) / 60.0
            unserved_mw = float(row.get("unserved_mwh", 0.0)) / duration_h
            lhs = float(row["generation_mw"]) + unserved_mw
            # Stage-2 compatibility convention: generation_mw is net supply;
            # storage charging/pumping is already subtracted by extraction.
            rhs = float(row["load_mw"])
            violation = abs(lhs-rhs); checked += 1
            if violation >= worst:
                worst=violation
                witness={"interval":row.get("t",index),"supply":lhs,"demand":rhs,"duration_h":duration_h}
        if not checked:
            return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,message="balance fields are unavailable",witness={})
        scale=max(abs(witness.get("supply",0)),abs(witness.get("demand",0)),1.0)
        limit=tolerance.balance_limit(scale, witness["duration_h"], persisted=persisted)
        witness["tolerance_mode"] = "persisted" if persisted else "internal"
        return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL if worst>limit else QAStatus.PASS,
            message=f"maximum balance residual is {worst:g} MW",witness=witness,tolerance=limit,max_violation=worst,checked_count=checked)


class BasicBoundsCheck:
    check_id = "basic_bounds"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy, *, persisted: bool = False) -> QACheckResult:
        rows=_rows(result)
        if not rows: return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,message="canonical interval results are unavailable")
        bad=[]; checked=0
        for i,row in enumerate(rows):
            for key in ("load_mw","generation_mw","unserved_mwh","curtailed_mwh"):
                if key in row:
                    checked+=1
                    if float(row[key]) < -tolerance.internal_absolute: bad.append({"interval":row.get("t",i),"field":key,"value":row[key]})
        return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL if bad else QAStatus.PASS,
            message=f"found {len(bad)} negative domain value(s)" if bad else "basic non-negative domains hold",
            witness={"worst":bad[:10], "tolerance_mode":"persisted" if persisted else "internal"},checked_count=checked)


class ObjectiveReconstructionCheck:
    check_id = "objective_reconstruction"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy, *, persisted: bool = False) -> QACheckResult:
        if hasattr(result, "model_dump"): result = result.model_dump(mode="python")
        breakdown = ((result or {}).get("diagnostics") or {}).get("objective_breakdown") if isinstance(result, dict) else None
        if not isinstance(breakdown, dict) or breakdown.get("pyomo_objective") is None:
            return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,
                message="solver objective or canonical reconstruction is unavailable", witness={})
        if "total_reconstructed" not in breakdown:
            return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                message="objective reconstruction is incomplete",
                witness={"missing_fields": ["total_reconstructed"]})
        if breakdown.get("validated_full_precision") is False:
            return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                message="objective reconstruction includes unsupported legacy terms",
                witness={"unsupported_legacy_terms": breakdown.get("unsupported_legacy_terms", [])})
        streams = ((result or {}).get("diagnostics") or {}).get("objective_cost_streams")
        if breakdown.get("validated_full_precision") is True:
            if not isinstance(streams, dict) or not isinstance(streams.get("__system__"), dict):
                return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                    message="full-precision objective cost stream is unavailable", witness={})
            assets = {
                str(asset.get("id")): asset
                for asset in ((resolved_input or {}).get("assets") or [])
                if isinstance(asset, dict) and asset.get("id") is not None
            }
            try:
                stream_total = 0.0
                for asset_id, asset in assets.items():
                    stream = streams.get(asset_id)
                    if not isinstance(stream, dict):
                        raise ValueError(f"missing stream for {asset_id}")
                    asset_type = asset.get("type")
                    if asset_type not in {"bess", "dr", "pumped_hydro"}:
                        stream_total += float(stream["fuel_cost_usd"])
                        stream_total += float(stream["startup_cost_usd"])
                        stream_total += float(stream["no_load_cost_usd"])
                    if asset_type not in {"bess", "dr", "pumped_hydro"} or asset_type == "dr":
                        stream_total += float(stream["variable_vom_cost_usd"])
                system_stream = streams["__system__"]
                for key in (
                    "bess_degradation_cost_usd", "bess_end_soc_penalty_usd",
                    "pumped_hydro_vom_cost_usd", "unserved_energy_penalty_usd",
                    "reserve_shortfall_penalty_usd", "hydro_spill_penalty_usd",
                    "co2_cost_usd", "storage_target_penalty_usd",
                ):
                    stream_total += float(system_stream[key])
            except (KeyError, TypeError, ValueError):
                return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                    message="full-precision objective cost stream is incomplete or non-numeric", witness={})
            if not math.isfinite(stream_total):
                return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                    message="full-precision objective cost stream is non-finite", witness={})
            declared = breakdown.get("total_reconstructed")
            try:
                declared_float = float(declared)
            except (TypeError, ValueError):
                declared_float = float("nan")
            stream_scale = max(1.0, abs(stream_total), abs(declared_float))
            stream_limit = tolerance.limit(stream_scale, persisted=persisted)
            if not math.isfinite(declared_float) or abs(stream_total - declared_float) > stream_limit:
                return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                    message="objective cost stream does not close to canonical reconstruction",
                    witness={"cost_stream_total_usd": stream_total, "declared_total_usd": declared},
                    tolerance=stream_limit, max_violation=abs(stream_total - declared_float))
        try:
            objective = float(breakdown["pyomo_objective"])
            reconstructed = float(breakdown["total_reconstructed"])
        except (TypeError, ValueError):
            return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                message="objective reconstruction is non-numeric", witness={"breakdown": breakdown})
        if not math.isfinite(objective) or not math.isfinite(reconstructed):
            return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL,
                message="objective reconstruction is non-finite", witness={"breakdown": breakdown})
        violation = abs(objective - reconstructed)
        scale = max(1.0, abs(objective), abs(reconstructed))
        # Validated deterministic runs reconstruct from the full-precision
        # canonical cost stream.  Older callers without that explicit marker
        # retain the documented compatibility tolerance, but cannot satisfy
        # the production publication requirement without a PASS result.
        limit = tolerance.limit(scale, persisted=persisted)
        if breakdown.get("validated_full_precision") is not True:
            limit = max(limit, 5e-3 * scale)
        return QACheckResult(check_id=self.check_id,status=QAStatus.PASS if violation <= limit else QAStatus.FAIL,
            message=f"objective reconstruction residual is {violation:g} USD",
            witness={"solver_objective_usd": objective, "reconstructed_objective_usd": reconstructed,
                     "cost_stream_total_usd": stream_total if breakdown.get("validated_full_precision") is True else None,
                     "tolerance_mode":"persisted" if persisted else "internal"},
            tolerance=limit,max_violation=violation,checked_count=1)
