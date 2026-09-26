"""Stage-2 independent checks over canonical extracted values."""

from __future__ import annotations
import math
from typing import Any
from powersim.contracts import QACheckResult, QAStatus
from .tolerance import TolerancePolicy


def _rows(result: Any) -> list[dict[str, Any]]:
    if hasattr(result, "model_dump"): result = result.model_dump(mode="python")
    if not isinstance(result, dict): return []
    rows = result.get("hourly") or result.get("hourly_results") or result.get("intervals") or []
    return list(rows) if isinstance(rows, (list, tuple)) else []


class FiniteValuesCheck:
    check_id = "finite_values"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy) -> QACheckResult:
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
            witness={"paths": bad}, checked_count=checked, max_violation=None)


class ElectricityBalanceCheck:
    check_id = "electricity_balance"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy) -> QACheckResult:
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
            rhs = float(row["load_mw"]) + float(row.get("storage_charge_mw", 0.0))
            violation = abs(lhs-rhs); checked += 1
            if violation >= worst: worst=violation; witness={"interval":row.get("t",index),"supply":lhs,"demand":rhs}
        if not checked:
            return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,message="balance fields are unavailable",witness={})
        scale=max(abs(witness.get("supply",0)),abs(witness.get("demand",0)),1.0); limit=tolerance.limit(scale)
        return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL if worst>limit else QAStatus.PASS,
            message=f"maximum balance residual is {worst:g} MW",witness=witness,tolerance=limit,max_violation=worst,checked_count=checked)


class BasicBoundsCheck:
    check_id = "basic_bounds"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy) -> QACheckResult:
        rows=_rows(result)
        if not rows: return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,message="canonical interval results are unavailable")
        bad=[]; checked=0
        for i,row in enumerate(rows):
            for key in ("load_mw","generation_mw","unserved_mwh","curtailed_mwh"):
                if key in row:
                    checked+=1
                    if float(row[key]) < -tolerance.internal_absolute: bad.append({"interval":row.get("t",i),"field":key,"value":row[key]})
        return QACheckResult(check_id=self.check_id,status=QAStatus.FAIL if bad else QAStatus.PASS,
            message=f"found {len(bad)} negative domain value(s)" if bad else "basic non-negative domains hold",witness={"worst":bad[:10]},checked_count=checked)


class ObjectiveReconstructionCheck:
    check_id = "objective_reconstruction"
    def run(self, resolved_input: Any, result: Any, tolerance: TolerancePolicy) -> QACheckResult:
        return QACheckResult(check_id=self.check_id,status=QAStatus.NOT_RUN,
            message="deferred until Stage 3 canonical component cost extraction",witness={})
