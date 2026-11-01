"""Parse a monthly Georgian electricity balance PDF into structured JSON.

Usage::

    python scripts/parse_georgia_balance.py 2025_GEO.pdf --year 2025 \
        --out data/actuals/georgia_balance_2025.json

The balance PDFs ("საქართველოს ელექტროენერგიის ბალანსი", million kWh = GWh)
list one row per plant, plant group, interconnector and loss item with 12
monthly values and an annual total. ``pdftotext -layout`` (poppler) keeps
the columns aligned; blank months are empty columns, so every number is
assigned to the month whose column it ends closest to. Every parsed row is
checked against its own annual total; a mismatch above 0.05 GWh aborts.

Rows are keyed by section, because names such as "- მიღება" (receipt)
repeat under each interconnector.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

NUMBER = re.compile(r"-?\d{1,3}(?: \d{3})*,\d+|-?\d+,\d+")
REFERENCE_ROW = "ენერგოსისტემა"
TOLERANCE_GWH = 0.05


def _tokens(line: str) -> list[tuple[int, float]]:
    return [(m.end(), float(m.group().replace(" ", "").replace(",", "."))) for m in NUMBER.finditer(line)]


def parse_text(text: str) -> list[dict]:
    lines = text.splitlines()
    reference = next(line for line in lines if line.startswith(REFERENCE_ROW))
    ends = [end for end, _ in _tokens(reference)]
    if len(ends) != 13:
        raise ValueError(f"reference row must have 12 months and a total, found {len(ends)} numbers")
    rows: list[dict] = []
    section = None
    for line in lines:
        tokens = _tokens(line)
        if not tokens:
            continue
        name = line[: line.find(NUMBER.search(line).group())].strip()
        if not name:
            continue
        values: list[float | None] = [None] * 13
        for end, value in tokens:
            column = min(range(13), key=lambda k: abs(ends[k] - end))
            if values[column] is not None:
                raise ValueError(f"two numbers in one column: {line!r}")
            values[column] = value
        months = [v if v is not None else 0.0 for v in values[:12]]
        total = values[12] if values[12] is not None else 0.0
        if abs(sum(months) - total) > TOLERANCE_GWH:
            raise ValueError(f"row {name!r}: months sum {sum(months):.3f} != total {total}")
        if name.startswith("-"):
            key = f"{section} {name}"
        else:
            section = name
            key = name
        rows.append({"key": key, "name": name, "monthly_gwh": months, "total_gwh": total})
    return rows


def parse_pdf(path: Path, year: int) -> dict:
    text = subprocess.run(["pdftotext", "-layout", str(path), "-"], check=True,
                          capture_output=True, text=True).stdout
    if str(year) not in "\n".join(text.splitlines()[:3]):
        raise ValueError(f"the PDF title does not mention {year}")
    return {
        "schema": "powersim.actuals.georgia_balance.v1",
        "year": year,
        "unit": "GWh (million kWh)",
        "source_file": path.name,
        "source_sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        "transformation": "scripts/parse_georgia_balance.py (pdftotext -layout, column alignment, row totals checked)",
        "note": "Monthly electricity balance of Georgia supplied by the user. Generation rows are gross; "
                "'მოხმარება (ჯამური დანაკარგებით)' = generation + net flow (consumption including losses). "
                "'სეზონური ჰესები' includes the 'მცირე ჰესები' row.",
        "rows": parse_text(text),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    document = parse_pdf(args.pdf, args.year)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(document, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{len(document['rows'])} rows -> {args.out}")


if __name__ == "__main__":
    main()
