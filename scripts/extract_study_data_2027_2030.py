from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INFO = ROOT / "Info.xlsx"
OUT = ROOT / "out" / "study_extract"
OUT.mkdir(parents=True, exist_ok=True)
YEARS = (2027, 2028, 2029, 2030)


def num(v: Any) -> float | None:
    if pd.isna(v):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def date_year(v: Any) -> int | None:
    if pd.isna(v) or v is None or str(v).strip() == "":
        return None
    if hasattr(v, "year"):
        return int(v.year)
    try:
        return int(pd.to_datetime(v).year)
    except Exception:
        return None


def rows(sheet: str) -> pd.DataFrame:
    df = pd.read_excel(INFO, sheet_name=sheet, dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def generator_catalog(sheet: str, tech: str) -> list[dict]:
    df = rows(sheet)
    out = []
    for child, g in df.groupby("Child Object", dropna=True, sort=False):
        props: dict[str, list[dict]] = defaultdict(list)
        for _, r in g.iterrows():
            props[str(r.get("Property", "")).strip()].append({
                "value": None if pd.isna(r.get("Value")) else str(r.get("Value")),
                "value_num": num(r.get("Value")),
                "data_file": None if pd.isna(r.get("Data File")) else str(r.get("Data File")),
                "units": None if pd.isna(r.get("Units")) else str(r.get("Units")),
                "date_from": None if pd.isna(r.get("Date From")) else str(r.get("Date From")),
                "date_from_year": date_year(r.get("Date From")),
                "date_to": None if pd.isna(r.get("Date To")) else str(r.get("Date To")),
                "timeslice": None if pd.isna(r.get("Timeslice")) else str(r.get("Timeslice")),
                "scenario": None if pd.isna(r.get("Scenario")) else str(r.get("Scenario")),
                "category": None if pd.isna(r.get("Category")) else str(r.get("Category")),
            })
        def first_num(p: str, default: float = 0.0) -> float:
            vals = [x["value_num"] for x in props.get(p, []) if x["value_num"] is not None]
            return vals[0] if vals else default
        maxcap = first_num("Max Capacity", first_num("Capacity", 0.0))
        units = first_num("Units", 0.0)
        build_rows = props.get("Max Units Built", []) + props.get("Min Units Built", []) + props.get("Project Start Date", [])
        # Capture dated build events. For Max Units Built, value is units commissioned/allowed at date.
        build_events = []
        for p in ("Max Units Built", "Min Units Built", "Project Start Date"):
            for x in props.get(p, []):
                build_events.append({"property": p, **x})
        out.append({
            "name": str(child), "tech": tech, "sheet": sheet,
            "max_capacity_mw": maxcap, "units": units,
            "base_installed_mw": maxcap * units,
            "build_events": build_events,
            "properties": props,
        })
    return out


def capacity_by_year(asset: dict, year: int, candidate: bool) -> float:
    cap = float(asset.get("max_capacity_mw") or 0.0)
    if cap <= 0:
        return 0.0
    if not candidate:
        return cap * float(asset.get("units") or 0.0)
    # PLEXOS candidate convention used in workbook: Max Units Built dated rows
    # encode candidate availability/build allowance. Treat the latest dated
    # Max Units Built <= study year as the commissioned/available unit count.
    max_events = [e for e in asset.get("build_events", [])
                  if e.get("property") == "Max Units Built"
                  and e.get("date_from_year") is not None
                  and e.get("date_from_year") <= year
                  and e.get("value_num") is not None]
    if max_events:
        latest_year = max(e["date_from_year"] for e in max_events)
        vals = [e["value_num"] for e in max_events if e["date_from_year"] == latest_year]
        return cap * max(vals or [0.0])
    # If no dated build row, explicit Units > 0 is treated as existing/committed.
    units = float(asset.get("units") or 0.0)
    return cap * units


def extract_reserve_constraints() -> dict:
    df = rows("Constraints")
    out: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    relevant = df[(df["Property"].astype(str).str.strip() == "RHS")]
    for _, r in relevant.iterrows():
        name = str(r.get("Child Object", "")).strip()
        cat = str(r.get("Category", "")).strip()
        joined = (name + " " + cat).lower()
        if not any(k in joined for k in ("reserve", "fcr", "afrr", "mfrr")):
            continue
        y = date_year(r.get("Date From"))
        sc = "" if pd.isna(r.get("Scenario")) else str(r.get("Scenario")).strip()
        v = num(r.get("Value"))
        if y in YEARS and v is not None:
            out[name][str(y)][sc or "default"] = v
    return {k: dict(v) for k, v in out.items()}


def extract_reserve_products() -> list[dict]:
    df = rows("Reserves")
    out = []
    for child, g in df.groupby("Child Object", dropna=True, sort=False):
        p = {}
        for _, r in g.iterrows():
            key = str(r.get("Property", "")).strip()
            p.setdefault(key, []).append({
                "value": None if pd.isna(r.get("Value")) else str(r.get("Value")),
                "value_num": num(r.get("Value")),
                "units": None if pd.isna(r.get("Units")) else str(r.get("Units")),
                "timeslice": None if pd.isna(r.get("Timeslice")) else str(r.get("Timeslice")),
                "scenario": None if pd.isna(r.get("Scenario")) else str(r.get("Scenario")),
            })
        out.append({"name": str(child), "properties": p})
    return out


def extract_sheet_verbatim(sheet: str) -> list[dict]:
    df = rows(sheet)
    recs = []
    for _, r in df.iterrows():
        rec = {}
        for c in df.columns:
            v = r.get(c)
            if pd.isna(v):
                rec[c] = None
            elif hasattr(v, "isoformat"):
                try: rec[c] = v.isoformat()
                except Exception: rec[c] = str(v)
            elif isinstance(v, (int, float, str, bool)):
                rec[c] = v
            else:
                rec[c] = str(v)
        recs.append(rec)
    return recs


def main() -> None:
    fleets = []
    specs = [
        ("Existing Wind", "wind", False),
        ("Existing HPP", "hydro", False),
        ("Existing ST", "thermal", False),
        ("Existing CCGT", "thermal", False),
        ("Existing GT", "thermal", False),
        ("Candidate HPP", "hydro", True),
        ("Candidate Wind Farm", "wind", True),
        ("Candidate CCGT", "thermal", True),
        ("Candidate PV", "solar", True),
    ]
    yearly = {str(y): defaultdict(float) for y in YEARS}
    for sheet, tech, cand in specs:
        cats = generator_catalog(sheet, tech)
        fleets.extend(cats)
        for a in cats:
            for y in YEARS:
                yearly[str(y)][tech] += capacity_by_year(a, y, cand)
    yearly = {y: {k: round(v, 3) for k, v in vals.items()} for y, vals in yearly.items()}

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "years": YEARS,
        "yearly_capacity_summary_mw": yearly,
        "generator_catalog": fleets,
        "reserve_constraints": extract_reserve_constraints(),
        "reserve_products": extract_reserve_products(),
        "batteries_rows": extract_sheet_verbatim("Batteries"),
        "storages_rows": extract_sheet_verbatim("Storages"),
        "constraints_rows": extract_sheet_verbatim("Constraints"),
    }
    path = OUT / "study_data_2027_2030.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"yearly_capacity_summary_mw": yearly,
                      "reserve_constraint_names": list(payload["reserve_constraints"]),
                      "n_generators": len(fleets)}, ensure_ascii=False, indent=2))
    print(path)


if __name__ == "__main__":
    main()
