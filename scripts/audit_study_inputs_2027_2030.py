from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out" / "study_audit"
OUT.mkdir(parents=True, exist_ok=True)


def norm(v):
    if pd.isna(v):
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    if isinstance(v, (int, float, str, bool)) or v is None:
        return v
    return str(v)


def inspect_xlsx(path: Path) -> dict:
    xls = pd.ExcelFile(path)
    out = {"file": str(path.relative_to(ROOT)), "sheets": {}}
    for s in xls.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=s, header=None)
        except Exception as e:
            out["sheets"][s] = {"error": str(e)}
            continue
        # preserve only non-empty leading region to keep artifact small
        nonempty_rows = df.dropna(how="all")
        sample = nonempty_rows.head(120).iloc[:, :40]
        rows = [[norm(x) for x in row] for row in sample.values.tolist()]
        hits = []
        for rix, row in enumerate(rows):
            txt = " | ".join("" if x is None else str(x) for x in row)
            if re.search(r"202[6-9]|2030|wind|solar|ror|hydro|bess|battery|ქარ|მზ|ჰეს|ბატარე", txt, re.I):
                hits.append({"row": rix, "text": txt[:2000]})
        out["sheets"][s] = {
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "sample_rows": rows,
            "keyword_hits": hits[:120],
        }
    return out


def inspect_zip(path: Path) -> dict:
    out = {"file": str(path.relative_to(ROOT)), "members": []}
    with zipfile.ZipFile(path) as zf:
        for n in zf.namelist():
            low = n.lower()
            if any(k in low for k in ["2027", "2028", "2029", "2030", "bess", "wind", "solar", "ror", "load", "hydro"]):
                out["members"].append(n)
    return out


def main() -> None:
    report = {}
    for fname in ["Info.xlsx"]:
        p = ROOT / fname
        if p.exists():
            report[fname] = inspect_xlsx(p)
    for fname in ["plexos model.zip", "PLEXOS_2026_Wind_Solar (1).zip", "PLEXOS_2026_profiles-or.zip", "PLEXOS_2026_zones_60files.zip"]:
        p = ROOT / fname
        if p.exists():
            report[fname] = inspect_zip(p)
    # inventory text/json files relevant to study
    inv = []
    for p in ROOT.rglob("*"):
        if p.is_file() and p.suffix.lower() in {".json", ".csv", ".py", ".md"}:
            rel = str(p.relative_to(ROOT))
            if any(k in rel.lower() for k in ["load", "renew", "bess", "adequacy", "asset", "profile"]):
                inv.append(rel)
    report["inventory"] = sorted(inv)
    out = OUT / "study_input_audit.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)


if __name__ == "__main__":
    main()
