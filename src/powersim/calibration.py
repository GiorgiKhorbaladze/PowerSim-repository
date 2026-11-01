"""Compare a completed run with measured (actual) operating data.

The user downloads a workbook generated from the run, fills in measured
energy per hour and/or per month, and uploads it.  Columns are either an
asset id or an asset kind (``hydro_ror``, ``wind``, ``solar``, ``thermal``,
``import``, ...), which sums every asset of that kind, or ``demand``.

For each column the report gives model vs actual totals, bias, MAE, RMSE
and (hourly only) the Pearson correlation, plus a month-by-month table.
Only cells the user filled are compared; nothing is interpolated.  The
comparison only describes the fit: it never changes the model.
"""
from __future__ import annotations

import hashlib
import io
import math
from datetime import datetime, timezone
from typing import Any

MONTH_END_HOURS = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
MAX_BYTES = 25 * 1024 * 1024


class CalibrationRejected(ValueError):
    def __init__(self, message: str, issues: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.issues = issues or []


def _month(hour: float) -> int:
    h = int(hour) % 8760
    return next(index for index, end in enumerate(MONTH_END_HOURS) if h < end)


def model_series(envelope: dict[str, Any]) -> tuple[dict[str, dict[int, float]], dict[str, str], float]:
    """Model energy (MWh) per study period for every asset, kind and demand."""
    results = envelope.get("results") or {}
    by_unit = results.get("hourly_by_unit") or {}
    summary = results.get("by_unit_summary") or {}
    system = results.get("hourly_system") or []
    if not by_unit or not system:
        raise CalibrationRejected("this run has no hourly per-unit results to compare")
    dt = float(system[0].get("period_minutes", 60)) / 60.0
    series: dict[str, dict[int, float]] = {}
    kinds: dict[str, str] = {}
    for asset_id, rows in by_unit.items():
        kind = (summary.get(asset_id) or {}).get("type") or "unknown"
        kinds[asset_id] = kind
        values = {int(row["t"]): float(row.get("dispatch_mw", 0.0) or 0.0) * dt for row in rows}
        series[asset_id] = values
        # kinds are prefixed so an asset id equal to a kind name ("thermal") cannot collide
        bucket = series.setdefault(f"kind:{kind}", {})
        for t, value in values.items():
            bucket[t] = bucket.get(t, 0.0) + value
    series["demand"] = {int(row["t"]): float(row.get("load_mw", 0.0)) * dt for row in system}
    return series, kinds, dt


def build_template(envelope: dict[str, Any]) -> bytes:
    import openpyxl
    from openpyxl.styles import Font, PatternFill

    series, kinds, dt = model_series(envelope)
    periods = sorted(series["demand"])
    columns = ["demand"] + sorted(f"kind:{kind}" for kind in set(kinds.values())) + sorted(kinds)
    workbook = openpyxl.Workbook()
    info = workbook.active
    info.title = "Instructions"
    for line in (
        f"PowerSim calibration workbook - run {envelope.get('run_id')}",
        "",
        "Enter MEASURED energy in MWh. Fill either the Hourly sheet, the Monthly sheet, or both.",
        "Columns: 'demand', 'kind:<kind>' (sums all assets of that kind, e.g. kind:hydro_ror) or an asset id.",
        "Delete or leave empty any column you have no data for. Only filled cells are compared.",
        "Hourly rows are study periods; 'period' is the model's period index and must not be changed.",
        "The comparison describes the fit only; it does not change the model.",
    ):
        info.append([line])
    info.column_dimensions["A"].width = 100
    info["A1"].font = Font(bold=True, size=13)
    fill = PatternFill("solid", fgColor="DCE6F1")
    hourly = workbook.create_sheet("Hourly")
    hourly.append(["period", "month"] + columns)
    for t in periods:
        hourly.append([t, MONTHS[_month(t * dt)]])
    monthly = workbook.create_sheet("Monthly")
    monthly.append(["month"] + columns)
    for name in MONTHS:
        monthly.append([name])
    for sheet in (hourly, monthly):
        for cell in sheet[1]:
            cell.font = Font(bold=True)
            cell.fill = fill
        sheet.freeze_panes = "B2"
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip().replace(" ", "")
        if text.count(",") == 1 and "." not in text:
            text = text.replace(",", ".")
        number = float(text)
    if not math.isfinite(number):
        raise ValueError("not finite")
    return number


def _metrics(pairs: list[tuple[float, float]]) -> dict[str, Any]:
    n = len(pairs)
    model_total = sum(m for m, _ in pairs)
    actual_total = sum(a for _, a in pairs)
    errors = [m - a for m, a in pairs]
    out: dict[str, Any] = {
        "points": n, "model_mwh": round(model_total, 3), "actual_mwh": round(actual_total, 3),
        "difference_mwh": round(model_total - actual_total, 3),
        "bias_pct": round(100.0 * (model_total - actual_total) / actual_total, 3) if actual_total else None,
        "mae_mwh": round(sum(abs(e) for e in errors) / n, 3) if n else None,
        "rmse_mwh": round(math.sqrt(sum(e * e for e in errors) / n), 3) if n else None,
    }
    if n >= 3:
        mm = model_total / n
        ma = actual_total / n
        cov = sum((m - mm) * (a - ma) for m, a in pairs)
        sm = math.sqrt(sum((m - mm) ** 2 for m, _ in pairs))
        sa = math.sqrt(sum((a - ma) ** 2 for _, a in pairs))
        out["correlation"] = round(cov / (sm * sa), 4) if sm > 0 and sa > 0 else None
    return out


def compare(envelope: dict[str, Any], content: bytes, *, file_name: str | None = None) -> dict[str, Any]:
    import openpyxl

    if len(content) > MAX_BYTES:
        raise CalibrationRejected("the workbook is larger than 25 MB")
    try:
        workbook = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    except Exception as exc:
        raise CalibrationRejected(f"the file is not a readable .xlsx workbook ({exc.__class__.__name__})")
    series, kinds, dt = model_series(envelope)
    known = set(series)
    issues: list[dict[str, Any]] = []
    report: dict[str, Any] = {"run_id": envelope.get("run_id"), "file_name": file_name,
                              "file_sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
                              "compared_at": datetime.now(timezone.utc).isoformat(),
                              "hourly": {}, "monthly": {}, "issues": issues}

    if "Hourly" in workbook.sheetnames:
        rows = list(workbook["Hourly"].iter_rows(values_only=True))
        header = [str(h).strip() if h is not None else "" for h in (rows[0] if rows else [])]
        if "period" not in header:
            issues.append({"severity": "error", "sheet": "Hourly", "message": "the Hourly sheet has no 'period' column"})
        else:
            pcol = header.index("period")
            for column, name in enumerate(header):
                if name in {"", "period", "month"}:
                    continue
                if name not in known:
                    issues.append({"severity": "warning", "sheet": "Hourly",
                                   "message": f"column '{name}' is not an asset id, 'kind:<kind>' or 'demand'; ignored"})
                    continue
                pairs, monthly_pairs = [], {}
                for row_number, row in enumerate(rows[1:], start=2):
                    try:
                        actual = _number(row[column] if column < len(row) else None)
                        period = _number(row[pcol])
                    except ValueError:
                        issues.append({"severity": "error", "sheet": "Hourly", "row": row_number, "column": name,
                                       "message": "not a number"})
                        continue
                    if actual is None or period is None:
                        continue
                    t = int(period)
                    if t not in series[name]:
                        issues.append({"severity": "error", "sheet": "Hourly", "row": row_number,
                                       "message": f"period {t} is not in this run"})
                        continue
                    model = series[name][t]
                    pairs.append((model, actual))
                    month = _month(t * dt)
                    monthly_pairs.setdefault(month, []).append((model, actual))
                if pairs:
                    report["hourly"][name] = {**_metrics(pairs), "monthly": [
                        {"month": MONTHS[m], **_metrics(p)} for m, p in sorted(monthly_pairs.items())]}

    if "Monthly" in workbook.sheetnames:
        rows = list(workbook["Monthly"].iter_rows(values_only=True))
        header = [str(h).strip() if h is not None else "" for h in (rows[0] if rows else [])]
        model_monthly = {name: [0.0] * 12 for name in known}
        for name, values in series.items():
            for t, value in values.items():
                model_monthly[name][_month(t * dt)] += value
        for column, name in enumerate(header):
            if name in {"", "month"}:
                continue
            if name not in known:
                issues.append({"severity": "warning", "sheet": "Monthly",
                               "message": f"column '{name}' is not an asset id, 'kind:<kind>' or 'demand'; ignored"})
                continue
            table, pairs = [], []
            for row_number, row in enumerate(rows[1:], start=2):
                label = str(row[0]).strip()[:3].title() if row and row[0] is not None else ""
                if label not in MONTHS:
                    continue
                try:
                    actual = _number(row[column] if column < len(row) else None)
                except ValueError:
                    issues.append({"severity": "error", "sheet": "Monthly", "row": row_number, "column": name,
                                   "message": "not a number"})
                    continue
                if actual is None:
                    continue
                model = model_monthly[name][MONTHS.index(label)]
                pairs.append((model, actual))
                table.append({"month": label, "model_mwh": round(model, 3), "actual_mwh": round(actual, 3),
                              "bias_pct": round(100 * (model - actual) / actual, 3) if actual else None})
            if pairs:
                report["monthly"][name] = {**_metrics(pairs), "months": table}

    report["errors"] = sum(1 for i in issues if i["severity"] == "error")
    report["warnings"] = sum(1 for i in issues if i["severity"] == "warning")
    report["compared_series"] = sorted(set(report["hourly"]) | set(report["monthly"]))
    if report["errors"]:
        raise CalibrationRejected("the calibration workbook has errors", issues)
    if not report["compared_series"]:
        raise CalibrationRejected("no measured values were found; fill at least one column", issues)
    report["model_kinds"] = kinds
    return report
