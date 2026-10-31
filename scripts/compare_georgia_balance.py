"""Compare a completed Georgia 2026 model run with a monthly balance (actual or plan).

Usage::

    python scripts/compare_georgia_balance.py --workspace ./powersim_workspace \
        --run <run-id> --balance data/actuals/georgia_balance_2026.json --out comparison.csv

The balance JSON is produced by ``scripts/parse_georgia_balance.py`` from a
balance PDF in the standard layout. Model assets are grouped onto balance
rows through the ``balance_row`` recorded in each asset's provenance; the
group rows (TPP total, regulating / seasonal HPPs, net flow, consumption)
use the documented aggregates. Only publishable runs are compared.
The comparison is read-only and does not change the model.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

MONTH_END = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)
GROUPS = {
    "თბოსადურები სულ": lambda a: a["kind"] == "thermal",
    "მარეგულირებელი ჰესები": lambda a: a["id"].startswith("reg-"),
    "სეზონური ჰესები": lambda a: a["id"].startswith("ror-"),
    "ჰიდროსადგურები სულ": lambda a: a["kind"] == "hydro_ror",
    "ქარის სადგურები სულ": lambda a: a["kind"] == "wind",
    "სულ მზის სადგურები": lambda a: a["kind"] == "solar",
}
CONSUMPTION = "მოხმარება (ჯამური დანაკარგებით)"
NET_FLOW = "გადადინება (სალდო) მ/შ"


def _monthly(values) -> list[float]:
    out, start = [], 0
    for end in MONTH_END:
        out.append(sum(values[start:end]) / 1000)
        start = end
    return out


def compare(project: dict, dispatch_mw: dict[str, list[float]], balance_rows: list[dict]) -> list[dict]:
    """Rows of {row, month (1-12 or 'year'), model_gwh, balance_gwh, diff_gwh, diff_pct}."""
    balance = {row["key"]: row["monthly_gwh"] for row in balance_rows}
    assets = {a["id"]: a for a in project["assets"]}
    monthly = {asset_id: _monthly(series) for asset_id, series in dispatch_mw.items()}
    model: dict[str, list[float]] = {}

    def add(key: str, values: list[float]) -> None:
        model[key] = [a + b for a, b in zip(model.get(key, [0.0] * 12), values)]

    for asset_id, values in monthly.items():
        asset = assets[asset_id]
        row = asset.get("provenance", {}).get("details", {}).get("balance_row")
        if row and asset["kind"] != "import":
            add(row, values)
        for group, member in GROUPS.items():
            if member(asset):
                add(group, values)
    imports = monthly.get("net-import", [0.0] * 12)
    demand = _monthly(next(p["values"] for p in project["profiles"] if p["id"] == "demand"))
    load = _monthly(json.loads((Path(__file__).resolve().parents[1] / "data/gse_load_2026_2030.json")
                               .read_text(encoding="utf-8"))["load_mw_by_year"][str(project["time"]["study_year"])])
    model[NET_FLOW] = [i - (d - l) for i, d, l in zip(imports, demand, load)]
    model[CONSUMPTION] = load
    if "ქარის სადგურები სულ" not in balance and "ქართლის ქარის ელ.სადგური" in balance:
        model.pop("ქარის სადგურები სულ")  # only the Kartli row exists in this balance
    rows = []
    for key, values in model.items():
        if key not in balance:
            continue
        for month, (m, b) in enumerate(zip(values + [sum(values)], balance[key] + [sum(balance[key])])):
            rows.append({"row": key, "month": month + 1 if month < 12 else "year", "model_gwh": round(m, 2),
                         "balance_gwh": round(b, 2), "diff_gwh": round(m - b, 2),
                         "diff_pct": round((m - b) / b * 100, 1) if abs(b) > 1e-9 else None})
    return rows


def main() -> None:
    from powersim.application import ApplicationService
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--balance", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    manager = RunManager(args.workspace)
    service = ApplicationService(manager, RegisteredLocalWorkflowExecutor(), asynchronous=False)
    envelope = service.result(args.run)
    if not envelope["results"]["publication"]["publishable"] or envelope["qa"]["status"] == "fail":
        raise SystemExit("only publishable runs that pass QA can be compared")
    # the run's immutable resolved input, not the (possibly edited) current project
    project = json.loads((manager._run_dir(args.run) / "resolved_input.json").read_text(encoding="utf-8"))
    dispatch = {uid: [r["dispatch_mw"] for r in rows] for uid, rows in envelope["results"]["hourly_by_unit"].items()}
    balance = json.loads(args.balance.read_text(encoding="utf-8"))["rows"]
    rows = compare(project, dispatch, balance)
    with args.out.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} rows -> {args.out}")


if __name__ == "__main__":
    main()
