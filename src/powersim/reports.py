"""Run reports: an Excel workbook and a printable HTML page.

Both are built from the persisted, publishable result envelope only; a
report for a non-publishable run is refused by the application service.
They state the model's provenance and limits on the first page.
"""
from __future__ import annotations

import html
import io
from typing import Any

KPI_LABELS = [
    ("total_energy_mwh", "Energy served", "MWh"), ("total_unserved_mwh", "Unserved energy", "MWh"),
    ("total_curtailed_mwh", "Wind/solar curtailment", "MWh"), ("peak_load_mw", "Peak demand", "MW"),
    ("total_objective_cost_usd", "Objective cost", "USD"), ("total_cost_usd", "System cost", "USD"),
    ("avg_lambda_usd_mwh", "Average system price", "USD/MWh"), ("total_gas_mm3", "Gas use", "Mm3"),
    ("total_co2_t", "CO2 emissions", "t"),
]
KIND_LABELS = {"hydro_ror": "Hydro", "hydro_reg": "Reservoir hydro", "wind": "Wind", "solar": "Solar",
               "thermal": "Thermal", "import": "TPP / import", "bess": "Battery (net)", "pumped_hydro": "Pumped hydro",
               "dr": "Demand response", "unserved": "Unserved"}


def _context(summary: dict[str, Any], envelope: dict[str, Any], project: dict[str, Any] | None,
             unsupported: list[str]) -> dict[str, Any]:
    view = summary.get("view") or {}
    meta = (project or {}).get("metadata") or {}
    return {
        "title": meta.get("name") or summary.get("project_id"),
        "rows": [
            ("Project", f"{meta.get('name') or ''} ({summary.get('project_id')})"),
            ("Scenario", summary.get("scenario_id") or "Base case"),
            ("Workflow", summary.get("workflow")),
            ("Run", summary.get("run_id")),
            ("Created", summary.get("created_at")),
            ("QA", (summary.get("qa") or {}).get("status")),
            ("Validity", summary.get("validity")),
            ("Publication", "publishable" + (" (with warnings)" if (summary.get("publication") or {}).get("qualified") else "")),
            ("Solver", f"{(summary.get('solver') or {}).get('backend')} {(summary.get('solver') or {}).get('backend_version') or ''}"),
            ("Termination", (summary.get("solver") or {}).get("termination_condition")),
            ("Snapshot fingerprint", summary.get("snapshot_fingerprint")),
            ("Persisted hashes verified", "yes" if (summary.get("verification") or {}).get("valid") else "no"),
            ("Classification", meta.get("classification") or ""),
        ],
        "kpis": [(label, unit, (view.get("system_summary") or {}).get(key)) for key, label, unit in KPI_LABELS
                 if (view.get("system_summary") or {}).get(key) is not None],
        "technology": view.get("technology") or {"kinds": [], "monthly": []},
        "by_unit": view.get("by_unit") or [],
        "qa": (summary.get("qa") or {}).get("checks") or [],
        "limitations": list(meta.get("limitations") or []),
        "unsupported": unsupported,
        "results": envelope.get("results") or {},
    }


def excel_report(summary: dict[str, Any], envelope: dict[str, Any], project: dict[str, Any] | None,
                 unsupported: list[str]) -> bytes:
    import openpyxl
    from openpyxl.styles import Font, PatternFill

    ctx = _context(summary, envelope, project, unsupported)
    bold, fill = Font(bold=True), PatternFill("solid", fgColor="DCE6F1")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws.append([f"PowerSim run report - {ctx['title']}"])
    ws["A1"].font = Font(bold=True, size=14)
    ws.append([])
    for label, value in ctx["rows"]:
        ws.append([label, value])
    ws.append([])
    ws.append(["Key results", "Value", "Unit"])
    for cell in ws[ws.max_row]:
        cell.font, cell.fill = bold, fill
    for label, unit, value in ctx["kpis"]:
        ws.append([label, value, unit])
    ws.append([])
    ws.append(["Limits"])
    ws[f"A{ws.max_row}"].font = bold
    for line in ctx["limitations"]:
        ws.append([line])
    ws.append(["PowerSim does not perform: " + ", ".join(ctx["unsupported"]) + "."])
    ws.column_dimensions["A"].width = 30
    ws.column_dimensions["B"].width = 70

    def table(name: str, header: list[str], rows: list[list[Any]]) -> None:
        sheet = wb.create_sheet(name)
        sheet.append(header)
        for cell in sheet[1]:
            cell.font, cell.fill = bold, fill
        for row in rows:
            sheet.append(row)
        sheet.freeze_panes = "A2"
        for index in range(1, len(header) + 1):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(index)].width = 16

    kinds = ctx["technology"]["kinds"]
    table("Monthly balance", ["Month"] + [f"{KIND_LABELS.get(k, k)} (MWh)" for k in kinds]
          + ["Unserved (MWh)", "Curtailed (MWh)", "Demand (MWh)"],
          [[m["label"]] + [m.get(k) for k in kinds] + [m.get("unserved_mwh"), m.get("curtailed_mwh"), m.get("demand_mwh")]
           for m in ctx["technology"]["monthly"]])
    table("By resource", ["Resource", "Name", "Type", "Energy (MWh)", "Capacity factor (%)", "Gross cost",
                          "Avg cost (/MWh)", "Starts"],
          [[u["id"], u.get("name"), u.get("type"), u.get("energy_mwh"), u.get("capacity_factor"), u.get("gross_cost"),
            u.get("avg_cost_mwh"), u.get("starts")] for u in ctx["by_unit"]])
    system = ctx["results"].get("hourly_system") or []
    table("Hourly system", ["Period", "Demand (MW)", "Generation (MW)", "Unserved (MWh)", "Curtailed (MWh)",
                            "Price (USD/MWh)"],
          [[r.get("t"), r.get("load_mw"), r.get("generation_mw"), r.get("unserved_mwh"), r.get("curtailed_mwh"),
            r.get("lambda_usd_mwh")] for r in system])
    by_unit = ctx["results"].get("hourly_by_unit") or {}
    unit_ids = sorted(by_unit)
    if unit_ids and len(unit_ids) <= 200:
        index = {uid: {int(r["t"]): r for r in by_unit[uid]} for uid in unit_ids}
        periods = [int(r.get("t")) for r in system]
        table("Hourly by resource", ["Period"] + unit_ids,
              [[t] + [(index[uid].get(t) or {}).get("dispatch_mw") for uid in unit_ids] for t in periods])
    table("QA checks", ["Check", "Status", "Message", "Max violation", "Tolerance"],
          [[c.get("check_id"), c.get("status"), c.get("message"), c.get("max_violation"), c.get("tolerance")]
           for c in ctx["qa"]])
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def _fmt(value: Any, digits: int = 0) -> str:
    if isinstance(value, bool) or value is None:
        return "-" if value is None else str(value)
    if isinstance(value, (int, float)):
        return f"{value:,.{digits}f}"
    return html.escape(str(value))


def html_report(summary: dict[str, Any], envelope: dict[str, Any], project: dict[str, Any] | None,
                unsupported: list[str]) -> str:
    ctx = _context(summary, envelope, project, unsupported)
    esc = html.escape
    kinds = ctx["technology"]["kinds"]
    monthly = ctx["technology"]["monthly"]
    colours = {"hydro_ror": "#2b6cb0", "hydro_reg": "#1a4f8a", "wind": "#2a9d8f", "solar": "#e9b949",
               "thermal": "#8d6e63", "import": "#9e9e9e", "bess": "#7b4fa1", "unserved": "#c62828"}
    # monthly stacked bar chart as static SVG
    bars = []
    top = max([1.0] + [sum(max(0.0, m.get(k) or 0) for k in kinds) + (m.get("unserved_mwh") or 0) for m in monthly]
              + [m.get("demand_mwh") or 0 for m in monthly])
    width, height, left, bottom = 760, 230, 50, 24
    band = (width - left - 10) / max(1, len(monthly))
    scale = lambda v: (height - bottom - 10) * v / top
    for i, m in enumerate(monthly):
        acc = 0.0
        for kind in kinds + ["unserved"]:
            value = max(0.0, (m.get("unserved_mwh") if kind == "unserved" else m.get(kind)) or 0)
            if value <= 0:
                continue
            y = height - bottom - scale(acc + value)
            bars.append(f'<rect x="{left + i * band + band * .15:.1f}" y="{y:.1f}" width="{band * .7:.1f}" '
                        f'height="{scale(value):.1f}" fill="{colours.get(kind, "#b0bec5")}"/>')
            acc += value
        demand_y = height - bottom - scale(m.get("demand_mwh") or 0)
        bars.append(f'<line x1="{left + i * band + band * .08:.1f}" x2="{left + (i + 1) * band - band * .08:.1f}" '
                    f'y1="{demand_y:.1f}" y2="{demand_y:.1f}" stroke="#111" stroke-width="2"/>')
        bars.append(f'<text x="{left + (i + .5) * band:.1f}" y="{height - 6}" font-size="11" text-anchor="middle">'
                    f'{esc(m["label"])}</text>')
    legend = " ".join(f'<span class="sw" style="background:{colours.get(k, "#b0bec5")}"></span>'
                      f'{esc(KIND_LABELS.get(k, k))}' for k in kinds + ["unserved"])
    rows_html = "".join(f"<tr><th>{esc(str(k))}</th><td>{esc(str(v or ''))}</td></tr>" for k, v in ctx["rows"])
    kpi_html = "".join(f"<tr><th>{esc(label)}</th><td class=n>{_fmt(value, 1)}</td><td>{esc(unit)}</td></tr>"
                       for label, unit, value in ctx["kpis"])
    month_head = "".join(f"<th>{esc(KIND_LABELS.get(k, k))}</th>" for k in kinds)
    month_rows = "".join("<tr><th>" + esc(m["label"]) + "</th>" + "".join(f"<td class=n>{_fmt((m.get(k) or 0) / 1000, 1)}</td>" for k in kinds)
                         + f"<td class=n>{_fmt((m.get('unserved_mwh') or 0) / 1000, 1)}</td>"
                         + f"<td class=n>{_fmt((m.get('curtailed_mwh') or 0) / 1000, 1)}</td>"
                         + f"<td class=n>{_fmt((m.get('demand_mwh') or 0) / 1000, 1)}</td></tr>" for m in monthly)
    unit_rows = "".join(f"<tr><td>{esc(str(u.get('name') or u['id']))}</td><td>{esc(str(u.get('type') or ''))}</td>"
                        f"<td class=n>{_fmt((u.get('energy_mwh') or 0) / 1000, 1)}</td><td class=n>{_fmt(u.get('capacity_factor'), 1)}</td>"
                        f"<td class=n>{_fmt(u.get('gross_cost'))}</td></tr>" for u in ctx["by_unit"])
    qa_rows = "".join(f"<tr><td>{esc(str(c.get('check_id')))}</td><td>{esc(str(c.get('status')))}</td>"
                      f"<td>{esc(str(c.get('message') or ''))}</td></tr>" for c in ctx["qa"])
    limits = "".join(f"<li>{esc(line)}</li>" for line in ctx["limitations"])
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><title>PowerSim report - {esc(str(ctx['title']))}</title>
<style>
body{{font-family:system-ui,"Segoe UI",Roboto,"Noto Sans Georgian",sans-serif;color:#17202b;margin:24px;font-size:13px}}
h1{{font-size:20px;margin:0 0 4px}} h2{{font-size:15px;margin:22px 0 6px;border-bottom:1px solid #ccd;padding-bottom:3px}}
table{{border-collapse:collapse;width:100%;margin:4px 0}} th,td{{border-bottom:1px solid #e3e6ea;padding:3px 6px;text-align:left;vertical-align:top}}
td.n{{text-align:right;font-variant-numeric:tabular-nums}} .muted{{color:#5a6573}} .sw{{display:inline-block;width:10px;height:10px;margin:0 4px 0 10px}}
.box{{border:1px solid #c9a227;background:#fdf6dc;padding:8px 10px;margin:10px 0}} @media print{{body{{margin:10mm}} h2{{break-after:avoid}} table{{break-inside:auto}}}}
</style></head><body>
<h1>PowerSim run report</h1><p class="muted">{esc(str(ctx['title']))} &middot; {esc(str(summary.get('scenario_id') or 'Base case'))} &middot; print this page to save it as PDF</p>
<div class="box"><strong>Use within the model's limits.</strong> Results come from the PowerSim planning model and its inputs; they are publishable because they passed canonical QA and the publication gate. PowerSim does not perform {esc(', '.join(ctx['unsupported']))}.</div>
<h2>Run</h2><table>{rows_html}</table>
<h2>Key results</h2><table>{kpi_html}</table>
<h2>Monthly energy balance (GWh)</h2><div>{legend}<span class="sw" style="background:#111;height:3px"></span>Demand</div>
<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="Monthly energy balance">{''.join(bars)}</svg>
<table><tr><th>Month</th>{month_head}<th>Unserved</th><th>Curtailed</th><th>Demand</th></tr>{month_rows}</table>
<h2>By resource</h2><table><tr><th>Resource</th><th>Type</th><th>Energy (GWh)</th><th>Capacity factor (%)</th><th>Gross cost</th></tr>{unit_rows}</table>
<h2>Quality assurance</h2><table><tr><th>Check</th><th>Status</th><th>Message</th></tr>{qa_rows}</table>
<h2>Model limits</h2><ul>{limits or '<li>See the PowerSim limitations document.</li>'}</ul>
</body></html>"""
