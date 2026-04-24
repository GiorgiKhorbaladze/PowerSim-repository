#!/usr/bin/env python3
"""Post-process PowerSim results into decision-grade comparison tables."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean

HYDRO_TYPES = {"hydro_reg", "hydro_ror"}
THERMAL_TYPES = {"thermal"}


@dataclass
class ScenarioMetrics:
    scenario: str
    horizon_h: int
    hydro_mwh: float
    thermal_mwh: float
    hydro_share_pct: float
    thermal_share_pct: float
    avg_lambda: float
    peak_lambda: float
    gas_mm3: float


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _energy_breakdown(results: dict) -> tuple[float, float]:
    hydro = 0.0
    thermal = 0.0
    for unit in (results.get("by_unit_summary") or {}).values():
        t = unit.get("type")
        e = float(unit.get("energy_mwh", 0.0) or 0.0)
        if t in HYDRO_TYPES:
            hydro += e
        elif t in THERMAL_TYPES:
            thermal += e
    return hydro, thermal


def _shares(hydro_mwh: float, thermal_mwh: float) -> tuple[float, float]:
    tot = hydro_mwh + thermal_mwh
    if tot <= 0:
        return 0.0, 0.0
    return hydro_mwh / tot * 100.0, thermal_mwh / tot * 100.0


def scenario_metrics(results: dict) -> ScenarioMetrics:
    meta = results.get("metadata") or {}
    hourly = results.get("hourly_system") or []
    sm = results.get("system_summary") or {}
    hydro_mwh, thermal_mwh = _energy_breakdown(results)
    hydro_share, thermal_share = _shares(hydro_mwh, thermal_mwh)
    peak_lambda = max((float(h.get("lambda_usd_mwh", 0.0) or 0.0) for h in hourly), default=0.0)
    return ScenarioMetrics(
        scenario=str(meta.get("scenario", "unknown")),
        horizon_h=int(meta.get("horizon_hours", len(hourly) or 0) or 0),
        hydro_mwh=round(hydro_mwh, 1),
        thermal_mwh=round(thermal_mwh, 1),
        hydro_share_pct=round(hydro_share, 2),
        thermal_share_pct=round(thermal_share, 2),
        avg_lambda=round(float(sm.get("avg_lambda_usd_mwh", 0.0) or 0.0), 3),
        peak_lambda=round(peak_lambda, 3),
        gas_mm3=round(float(sm.get("total_gas_mm3", 0.0) or 0.0), 4),
    )


def reservoir_trace(results: dict, unit_id: str) -> list[dict]:
    rows = (results.get("hourly_by_unit") or {}).get(unit_id) or []
    out = []
    for r in rows:
        h = r.get("hydro") or {}
        out.append({
            "t": r.get("t"),
            "storage_mm3": h.get("storage_mm3"),
            "release_mm3h": h.get("release_mm3h"),
            "inflow_mm3h": h.get("inflow_mm3h"),
            "spill_mm3h": h.get("spill_mm3h"),
        })
    return out


def cascade_validation(results: dict, input_payload: dict, downstream: str) -> dict:
    assets = {a.get("id"): a for a in (input_payload.get("assets") or []) if isinstance(a, dict)}
    d_asset = assets.get(downstream, {})
    d_h = d_asset.get("hydro") or {}
    upstream = d_h.get("cascade_upstream")
    delay = int(d_h.get("cascade_travel_delay_h", 0) or 0)
    gain = float(d_h.get("cascade_gain", 1.0) or 1.0)
    if not upstream:
        return {"downstream": downstream, "enabled": False}

    up_rows = (results.get("hourly_by_unit") or {}).get(upstream) or []
    dn_rows = (results.get("hourly_by_unit") or {}).get(downstream) or []
    n = min(len(up_rows), len(dn_rows))

    expected = []
    implied = []
    for i in range(n):
        up_idx = i - delay
        up_rel = 0.0
        if up_idx >= 0:
            up_rel = float((((up_rows[up_idx] or {}).get("hydro") or {}).get("release_mm3h", 0.0) or 0.0))
        exp = gain * up_rel

        dn_h = (dn_rows[i] or {}).get("hydro") or {}
        stor = float(dn_h.get("storage_mm3", 0.0) or 0.0)
        rel = float(dn_h.get("release_mm3h", 0.0) or 0.0)
        spill = float(dn_h.get("spill_mm3h", 0.0) or 0.0)
        own = float(dn_h.get("inflow_mm3h", 0.0) or 0.0)
        if i == 0:
            prev_stor = stor
        else:
            prev_stor = float((((dn_rows[i - 1] or {}).get("hydro") or {}).get("storage_mm3", stor) or stor))
        imp = stor - prev_stor + rel + spill - own

        expected.append(exp)
        implied.append(imp)

    mae = mean(abs(a - b) for a, b in zip(expected, implied)) if expected else 0.0
    upstream_turbined = sum(
        float((((r or {}).get("hydro") or {}).get("release_mm3h", 0.0) or 0.0))
        for r in up_rows
    )
    upstream_spilled = sum(
        float((((r or {}).get("hydro") or {}).get("spill_mm3h", 0.0) or 0.0))
        for r in up_rows
    )
    return {
        "downstream": downstream,
        "upstream": upstream,
        "enabled": True,
        "delay_h": delay,
        "gain": gain,
        "hours": n,
        "expected_total_mm3": round(sum(expected), 5),
        "implied_total_mm3": round(sum(implied), 5),
        "mae_mm3h": round(mae, 6),
        "upstream_turbined_mm3": round(upstream_turbined, 5),
        "upstream_spilled_mm3": round(upstream_spilled, 5),
    }


def strategic_reservoir_check(results: dict, input_payload: dict, unit_id: str) -> dict:
    assets = {a.get("id"): a for a in (input_payload.get("assets") or []) if isinstance(a, dict)}
    a = assets.get(unit_id, {})
    h_cfg = a.get("hydro") or {}
    rows = (results.get("hourly_by_unit") or {}).get(unit_id) or []
    if not rows:
        return {"unit_id": unit_id, "available": False}
    end_storage = float((((rows[-1] or {}).get("hydro") or {}).get("storage_mm3", 0.0) or 0.0))
    reservoir_max = float(h_cfg.get("reservoir_max", 0.0) or 0.0)
    target_frac = float(h_cfg.get("target_end_level_frac", 0.0) or 0.0)
    target_storage = reservoir_max * target_frac if reservoir_max > 0 else 0.0
    shortfall = max(0.0, target_storage - end_storage)
    end_min = float(h_cfg.get("reservoir_end_min", 0.0) or 0.0)
    return {
        "unit_id": unit_id,
        "available": True,
        "end_storage_mm3": round(end_storage, 3),
        "target_storage_mm3": round(target_storage, 3),
        "target_shortfall_mm3": round(shortfall, 3),
        "end_min_mm3": round(end_min, 3),
        "target_penalty_usd_per_mm3": float(h_cfg.get("end_level_penalty", 0.0) or 0.0),
    }


def seasonal_behavior(results: dict) -> dict:
    hbu = results.get("hourly_by_unit") or {}
    bus = results.get("by_unit_summary") or {}
    meta = results.get("metadata") or {}
    start_dt_raw = meta.get("start_datetime")
    if not hbu or not bus or not start_dt_raw:
        return {"available": False}

    try:
        start_dt = datetime.fromisoformat(str(start_dt_raw).replace("Z", "+00:00"))
    except ValueError:
        return {"available": False}

    hydro_by_month = {m: 0.0 for m in range(1, 13)}
    thermal_by_month = {m: 0.0 for m in range(1, 13)}

    for gid, rows in hbu.items():
        unit_type = (bus.get(gid) or {}).get("type")
        if unit_type not in HYDRO_TYPES and unit_type not in THERMAL_TYPES:
            continue
        for r in rows:
            t = int(r.get("t", 1) or 1) - 1
            dt = start_dt + timedelta(hours=t)
            e = float(r.get("dispatch_mw", 0.0) or 0.0)
            if unit_type in HYDRO_TYPES:
                hydro_by_month[dt.month] += e
            elif unit_type in THERMAL_TYPES:
                thermal_by_month[dt.month] += e

    monthly = results.get("monthly_summary") or []
    lam_by_month = {int(m.get("month")): float(m.get("avg_lambda", 0.0) or 0.0)
                    for m in monthly if m.get("month")}

    def _share_for_months(months: set[int]) -> dict:
        hydro = sum(hydro_by_month[m] for m in months)
        thermal = sum(thermal_by_month[m] for m in months)
        total = hydro + thermal
        lam_vals = [lam_by_month[m] for m in months if m in lam_by_month]
        h_share = hydro / total * 100 if total > 0 else 0.0
        t_share = thermal / total * 100 if total > 0 else 0.0
        return {
            "hydro_share_pct": round(h_share, 2),
            "thermal_share_pct": round(t_share, 2),
            "avg_lambda": round(mean(lam_vals), 3) if lam_vals else 0.0,
        }

    return {
        "available": True,
        "winter": _share_for_months({12, 1, 2}),
        "summer": _share_for_months({6, 7, 8}),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Analyze PowerSim results JSON files")
    ap.add_argument("--results", nargs="+", required=True, help="One or more powersim_results.json paths")
    ap.add_argument("--inputs", nargs="*", default=[], help="Matching powersim_input.json paths (optional)")
    ap.add_argument("--out-dir", default="outputs/reports", help="Directory for summary outputs")
    args = ap.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    input_map = {}
    for p in args.inputs:
        j = _load_json(Path(p))
        sid = ((j.get("scenario_metadata") or {}).get("id") or j.get("scenario") or Path(p).stem)
        input_map[str(sid)] = j

    rows = []
    for p in args.results:
        r = _load_json(Path(p))
        m = scenario_metrics(r)
        rows.append(m)

        sid = m.scenario
        inp = input_map.get(sid)
        per_scen_dir = out_dir / sid
        per_scen_dir.mkdir(parents=True, exist_ok=True)

        for unit_id in ("enguri", "engurhesi", "vardnili", "vardnilhesi"):
            trace = reservoir_trace(r, unit_id)
            if trace:
                with (per_scen_dir / f"reservoir_{unit_id}.json").open("w", encoding="utf-8") as f:
                    json.dump(trace, f, ensure_ascii=False, indent=2)

        if inp:
            casc = cascade_validation(r, inp, downstream="vardnilhesi")
            with (per_scen_dir / "cascade_vardnilhesi.json").open("w", encoding="utf-8") as f:
                json.dump(casc, f, ensure_ascii=False, indent=2)
            strat = {
                "engurhesi": strategic_reservoir_check(r, inp, "engurhesi"),
                "vardnilhesi": strategic_reservoir_check(r, inp, "vardnilhesi"),
            }
            with (per_scen_dir / "strategic_reservoir.json").open("w", encoding="utf-8") as f:
                json.dump(strat, f, ensure_ascii=False, indent=2)

            seas = seasonal_behavior(r)
            with (per_scen_dir / "seasonal_behavior.json").open("w", encoding="utf-8") as f:
                json.dump(seas, f, ensure_ascii=False, indent=2)

    table_path = out_dir / "scenario_comparison.csv"
    with table_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Scenario", "Horizon_h", "Hydro_MWh", "Thermal_MWh", "Hydro_%", "Thermal_%", "Avg_lambda", "Peak_lambda", "Gas_Mm3"])
        for r in sorted(rows, key=lambda x: x.scenario):
            w.writerow([
                r.scenario, r.horizon_h, r.hydro_mwh, r.thermal_mwh,
                r.hydro_share_pct, r.thermal_share_pct, r.avg_lambda,
                r.peak_lambda, r.gas_mm3,
            ])
    print(f"[report] wrote {table_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
