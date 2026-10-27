"""Read-only plant-parameter reference catalogue and its quality checks.

The catalogue is transcribed from ``Power Plant Parameters English.xlsx`` by
``scripts/build_reference_catalogue.py``.  It is *reference* data: nothing in
this module turns it into study input.  Interpretation is deliberately
conservative:

* numbers are parsed only when the text is unambiguous; every reinterpretation
  (comma decimal, numeric text) is reported as a parse note;
* names are matched across tables with an explicit relation
  (``exact``/``variant``/``possible``), never silently merged;
* conflicting values are reported, not reconciled.
"""
from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass, field as dataclass_field
from functools import cached_property
from pathlib import Path
from typing import Any, Iterable

from powersim.resources import REFERENCE_CATALOGUE, resource_path

#: Relative tolerance above which two capacity statements disagree.
CAPACITY_RELATIVE_TOLERANCE = 0.02
#: Absolute tolerance (MW) below which differences are rounding noise.
CAPACITY_ABSOLUTE_TOLERANCE_MW = 0.05
#: Ratio above which two name stems are reported as a possible match.
NAME_SIMILARITY_THRESHOLD = 0.85

SOURCE_WORKBOOK = "Power Plant Parameters English.xlsx"
PROVENANCE_SOURCE = "powersim.reference_catalogue"

REMEDIATION = {
    "missing_installed_capacity": "Supply a capacity override with a documented source, or do not import this record.",
    "not_a_plant_record": "Section headings and aggregate totals are not plants and cannot be imported.",
    "missing_plant_name": "Correct the source workbook; a record without a name cannot be identified.",
    "duplicate_name_in_table": "Confirm which source row is authoritative; values from every duplicate are compared.",
    "naming_inconsistency": "Confirm that the differently spelled records describe the same plant before relying on them.",
    "capacity_conflict": "Choose the authoritative capacity with an override and record the reason.",
    "available_exceeds_installed": "Available capacity cannot exceed installed capacity. Override capacity with a reason.",
    "unit_capacity_mismatch": "The generating-unit string does not add up to the stated capacity. Override capacity with a reason.",
    "unit_count_conflict": "Unit counts differ between source tables. The unit count is kept as reference metadata only.",
    "unparsed_unit_string": "The unit string could not be interpreted; it is kept verbatim as reference metadata.",
    "comma_decimal": "A comma was read as a decimal separator. Check the parsed value against the workbook.",
    "compound_value": "The cell holds several values; it is kept verbatim and not mapped to a model field.",
    "reservoir_volume_order": "Reservoir volumes are inconsistent (min/active/max). Correct them before using this reservoir.",
    "reservoir_level_order": "Reservoir minimum operating level must be below the maximum operating level.",
    "negative_value": "Physical quantities cannot be negative. Correct the source value.",
    "missing_technology": "Regulation type/technology is missing. Select the PowerSim component explicitly with a reason.",
    "missing_fuel": "Thermal plants require a fuel. Supply the fuel type as an override with its source.",
    "missing_unit_configuration": "The source has no generating-unit configuration; unit-level data is unavailable.",
    "importer_assumption": "Review the importer assumption; override the field if the study needs a different value.",
    "unmapped_required_field": "The asset is imported disabled. Supply the listed inputs before enabling it in a study.",
    "duplicate_corroborating_records": "Several records in another table match this plant; all of them were compared.",
    "override_without_reason": "Every manual override needs a non-empty reason for the audit trail.",
    "invalid_override": "The override value is not valid for this field.",
    "asset_id_exists": "Choose a different asset id (override 'id') or remove the existing asset first.",
    "duplicate_selection": "Each source record can be staged only once per import.",
    "unknown_record": "The record id does not exist in the reference catalogue.",
}


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------

_NUMBER_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")


def parse_number(raw: Any) -> tuple[float | None, str | None]:
    """Parse a cell into a float, returning ``(value, parse_note_code)``.

    Unambiguous numbers parse silently.  A comma decimal (``"425,6"``) parses
    with the ``comma_decimal`` note.  Text holding several values
    (``"292/195"``) is not parsed and returns ``compound_value``.
    """
    if raw is None or isinstance(raw, bool):
        return None, None
    if isinstance(raw, (int, float)):
        return float(raw), None
    text = str(raw).strip()
    if not text:
        return None, None
    if _NUMBER_RE.fullmatch(text):
        if "," in text:
            return float(text.replace(",", ".")), "comma_decimal"
        return float(text), None
    return None, "compound_value"


_UNIT_PART_RE = re.compile(r"^\s*(?:(\d+)\s*[xX×х]\s*)?(\d+(?:[.,]\d+)?)\s*$")


@dataclass(frozen=True)
class UnitConfiguration:
    raw: str
    groups: tuple[tuple[int, float], ...]
    notes: tuple[str, ...]

    @property
    def unit_count(self) -> int:
        return sum(count for count, _ in self.groups)

    @property
    def total_mw(self) -> float:
        return round(sum(count * size for count, size in self.groups), 6)


def parse_units(raw: Any) -> UnitConfiguration | None:
    """Parse ``"3x37,6"``, ``"2x10.6+2x9.6"`` or ``"1x0,888;2x0,56"``.

    Returns ``None`` when absent and a configuration without groups when the
    string cannot be interpreted (the raw text is always retained).
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    notes: list[str] = []
    groups: list[tuple[int, float]] = []
    for part in re.split(r"[;+]", text):
        match = _UNIT_PART_RE.fullmatch(part)
        if not match:
            return UnitConfiguration(text, (), ("unparsed_unit_string",))
        count = int(match.group(1) or 1)
        size_text = match.group(2)
        if "," in size_text:
            notes.append("comma_decimal")
        groups.append((count, float(size_text.replace(",", "."))))
    return UnitConfiguration(text, tuple(groups), tuple(sorted(set(notes))))


_GENERIC_TOKENS = {"hpp", "hppi", "pp", "tpp", "ccpp", "hydro", "power", "plant", "station", "solar", "wind",
                   "gas", "turbine", "unit", "coal"}
_ROMAN = {"i": "1", "ii": "2", "iii": "3", "iv": "4", "v": "5", "vi": "6", "vii": "7", "viii": "8", "ix": "9"}


@dataclass(frozen=True)
class NameKey:
    words: tuple[str, ...]
    numbers: tuple[str, ...]

    @property
    def stem(self) -> str:
        return " ".join(w[:-1] if len(w) > 4 and w.endswith("i") else w for w in self.words)

    @property
    def text(self) -> str:
        return " ".join(self.words + self.numbers)


def name_key(name: Any) -> NameKey | None:
    """Normalise a plant name for matching; generic suffixes are ignored."""
    if not isinstance(name, str) or not name.strip():
        return None
    text = name.lower()
    text = re.sub(r"(?<=[a-z])(?=\d)|(?<=\d)(?=[a-z])", " ", text)
    tokens = [t for t in re.split(r"[^a-z0-9]+", text) if t]
    words, numbers = [], []
    for token in tokens:
        if token.isdigit():
            numbers.append(str(int(token)))
        elif token in _ROMAN and words:
            numbers.append(_ROMAN[token])
        elif token not in _GENERIC_TOKENS:
            words.append(token)
    if not words and not numbers:
        return None
    return NameKey(tuple(words), tuple(numbers))


def name_relation(left: Any, right: Any) -> str | None:
    """Return ``exact``, ``variant``, ``possible`` or ``None``.

    ``variant`` means identical after stemming (``Rion``/``Rioni``);
    ``possible`` means similar stems, including spelling variants with a
    differing unit number (``Gumat HPPi``/``Gumati HPP-1``).  Only ``exact``
    is ever an identity; the other relations require user confirmation.
    """
    a, b = name_key(left), name_key(right)
    if a is None or b is None:
        return None
    if a.text == b.text:
        return "exact"
    if a.stem == b.stem and a.numbers == b.numbers:
        return "variant"
    if not a.words or not b.words:
        return None
    ratio = difflib.SequenceMatcher(None, a.stem, b.stem).ratio()
    if ratio >= NAME_SIMILARITY_THRESHOLD and (a.numbers == b.numbers or not a.numbers or not b.numbers):
        return "possible"
    return None


def capacities_conflict(a: float, b: float) -> bool:
    difference = abs(a - b)
    return difference > CAPACITY_ABSOLUTE_TOLERANCE_MW and difference > CAPACITY_RELATIVE_TOLERANCE * max(abs(a), abs(b))


# --------------------------------------------------------------------------
# Catalogue model
# --------------------------------------------------------------------------

@dataclass
class QualityIssue:
    code: str
    severity: str  # "error" | "warning" | "info"
    table: str
    record_id: str | None
    message: str
    field: str | None = None
    source_row: int | None = None
    context: dict[str, Any] = dataclass_field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "severity": self.severity, "table": self.table, "record_id": self.record_id,
                "field": self.field, "source_row": self.source_row, "message": self.message,
                "remediation": REMEDIATION.get(self.code), "context": self.context}


TABLE_FILES = {
    "stations": "stations_by_location.json",
    "reservoirs": "reservoirs.json",
    "hydropower": "hydropower_assets.json",
    "hpp_summary": "hpp_summary.json",
}


class ReferenceCatalogue:
    """In-memory, read-only view of the normalized workbook exports."""

    def __init__(self, directory: str | Path | None = None):
        self.directory = Path(directory) if directory is not None else resource_path(REFERENCE_CATALOGUE)
        if not (self.directory / "manifest.json").is_file():
            raise FileNotFoundError(f"reference catalogue manifest not found in {self.directory}")
        self.manifest = json.loads((self.directory / "manifest.json").read_text(encoding="utf-8"))
        self.tables: dict[str, dict[str, Any]] = {}
        for name, filename in TABLE_FILES.items():
            self.tables[name] = json.loads((self.directory / filename).read_text(encoding="utf-8"))
        self.detail_sheets: dict[str, Any] = json.loads(
            (self.directory / "plant_detail_sheets.json").read_text(encoding="utf-8"))["sheets"]
        self._records = {record["record_id"]: (table, record)
                         for table, payload in self.tables.items() for record in payload["records"]}

    # -- basic access ------------------------------------------------------
    @property
    def source_sha256(self) -> str:
        return str(self.manifest.get("sha256"))

    def records(self, table: str) -> list[dict[str, Any]]:
        if table not in self.tables:
            raise ValueError(f"unknown reference table: {table}")
        return self.tables[table]["records"]

    def record(self, record_id: str) -> tuple[str, dict[str, Any]]:
        if record_id not in self._records:
            raise KeyError(record_id)
        return self._records[record_id]

    def columns(self, table: str) -> list[dict[str, Any]]:
        payload = self.tables[table]
        if "columns" in payload:
            return payload["columns"]
        merged: dict[str, dict[str, Any]] = {}
        for section in payload["sections"].values():
            for column in section["columns"]:
                merged.setdefault(column["key"], column)
        return list(merged.values())

    def summary(self) -> dict[str, Any]:
        return {"source_file": self.manifest.get("source_file"), "sha256": self.source_sha256,
                "sheet_count": self.manifest.get("sheet_count"), "record_counts": self.manifest.get("record_counts"),
                "generator": self.manifest.get("generator"), "important_note": self.manifest.get("important_note"),
                "review_required": True,
                "tables": {name: {"source_sheet": payload.get("source_sheet"), "records": len(payload["records"]),
                                  "layout_notes": payload.get("layout_notes", [])}
                           for name, payload in self.tables.items()},
                "detail_sheets": len(self.detail_sheets)}

    @staticmethod
    def record_name(record: dict[str, Any]) -> str | None:
        values = record.get("values", {})
        return values.get("plant_name") or values.get("reservoir_name")

    # -- detail sheets -----------------------------------------------------
    def detail_facts(self, sheet_name: str) -> dict[str, Any]:
        """Extract the few unambiguous key/value facts of a technical sheet."""
        sheet = self.detail_sheets[sheet_name]
        facts: dict[str, Any] = {"sheet": sheet_name, "rows": len(sheet["rows"])}
        previous_label = None
        for row in sheet["rows"]:
            cells = row["cells"] + [None] * 3
            label, sub, value = cells[0], cells[1], cells[2]
            label = label if label is not None else previous_label
            previous_label = label
            key = f"{label or ''}|{sub or ''}".lower()
            if "generating units" in key and value is not None:
                facts["generating_units"] = {"raw": value, "source_row": row["source_row"]}
            elif key.startswith("capacity (mw)|total") and value is not None:
                facts["installed_capacity_mw"] = {"raw": value, "source_row": row["source_row"]}
            elif key.startswith("regulation type") and value is not None:
                facts["regulation_type"] = {"raw": value, "source_row": row["source_row"]}
            elif key.startswith("year commissioned") and value is not None:
                facts["year_commissioned"] = {"raw": value, "source_row": row["source_row"]}
        return facts

    # -- cross-table matching ---------------------------------------------
    def matches(self, name: Any, tables: Iterable[str] = ("hydropower", "hpp_summary")) -> list[dict[str, Any]]:
        found = []
        for table in tables:
            for record in self.records(table):
                relation = name_relation(name, self.record_name(record))
                if relation:
                    found.append({"table": table, "record_id": record["record_id"], "relation": relation,
                                  "name": self.record_name(record), "source_row": record["source_row"]})
        for sheet in self.detail_sheets:
            relation = name_relation(name, sheet)
            if relation:
                found.append({"table": "plant_detail_sheets", "record_id": f"sheet:{sheet}",
                              "relation": relation, "name": sheet, "source_row": None})
        return found

    # -- capacity evidence for a station -----------------------------------
    def capacity_evidence(self, record_id: str) -> dict[str, Any]:
        """Collect every capacity/unit statement for a station record."""
        table, record = self.record(record_id)
        values = record["values"]
        evidence: list[dict[str, Any]] = []
        unit_counts: list[dict[str, Any]] = []
        notes: list[dict[str, Any]] = []

        def add(kind: str, raw: Any, source: dict[str, Any], relation: str = "self") -> None:
            value, note = parse_number(raw)
            if note:
                notes.append({"code": note, "raw": raw, **source})
            if value is not None:
                evidence.append({"quantity": kind, "value": value, "raw": raw, "relation": relation, **source})

        def add_units(raw: Any, source: dict[str, Any], relation: str = "self") -> None:
            config = parse_units(raw)
            if config is None:
                return
            for note in config.notes:
                notes.append({"code": note, "raw": raw, **source})
            if config.groups:
                evidence.append({"quantity": "unit_sum_mw", "value": config.total_mw, "raw": raw,
                                 "relation": relation, **source})
                unit_counts.append({"value": config.unit_count, "raw": raw, "relation": relation, **source})

        own = {"table": table, "record_id": record_id, "source_sheet": record["source_sheet"],
               "source_row": record["source_row"]}
        add("installed_mw", values.get("installed_capacity_mw"), {**own, "column": "installed_capacity_mw"})
        add("available_mw", values.get("available_capacity_mw"), {**own, "column": "available_capacity_mw"})
        add_units(values.get("generating_units"), {**own, "column": "generating_units"})
        matches = [m for m in self.matches(values.get("plant_name")) if m["relation"] in {"exact", "variant"}]
        for match in matches:
            source = {"table": match["table"], "record_id": match["record_id"]}
            if match["table"] == "plant_detail_sheets":
                sheet = match["name"]
                facts = self.detail_facts(sheet)
                for key, column in (("installed_capacity_mw", "Capacity (MW) / Total"),):
                    if key in facts:
                        add("installed_mw", facts[key]["raw"], {**source, "source_sheet": sheet,
                            "source_row": facts[key]["source_row"], "column": column}, match["relation"])
                if "generating_units" in facts:
                    add_units(facts["generating_units"]["raw"], {**source, "source_sheet": sheet,
                              "source_row": facts["generating_units"]["source_row"],
                              "column": "Installed / Generating units"}, match["relation"])
                continue
            other_table, other = self.record(match["record_id"])
            src = {**source, "source_sheet": other["source_sheet"], "source_row": other["source_row"]}
            if other_table == "hpp_summary":
                add("installed_mw", other["values"].get("installed_capacity_mw"),
                    {**src, "column": "installed_capacity_mw"}, match["relation"])
                add_units(other["values"].get("generating_units"), {**src, "column": "generating_units"},
                          match["relation"])
            elif other_table == "hydropower":
                add("turbine_max_mw", other["values"].get("turbine_max_capacity_mw"),
                    {**src, "column": "turbine_max_capacity_mw"}, match["relation"])
        return {"evidence": evidence, "unit_counts": unit_counts, "parse_notes": notes, "matches": matches}

    # -- whole-catalogue quality report ------------------------------------
    @cached_property
    def quality_issues(self) -> list[QualityIssue]:
        issues: list[QualityIssue] = []
        issues += self._station_issues()
        issues += self._duplicate_issues()
        issues += self._naming_issues()
        issues += self._reservoir_issues()
        return issues

    def quality_report(self) -> dict[str, Any]:
        issues = [issue.as_dict() for issue in self.quality_issues]
        counts: dict[str, int] = {}
        for issue in issues:
            counts[issue["code"]] = counts.get(issue["code"], 0) + 1
        return {"source_sha256": self.source_sha256,
                "tolerances": {"capacity_relative": CAPACITY_RELATIVE_TOLERANCE,
                               "capacity_absolute_mw": CAPACITY_ABSOLUTE_TOLERANCE_MW,
                               "name_similarity": NAME_SIMILARITY_THRESHOLD},
                "counts": dict(sorted(counts.items())),
                "errors": sum(i["severity"] == "error" for i in issues),
                "warnings": sum(i["severity"] == "warning" for i in issues),
                "issues": issues}

    def issues_for(self, record_id: str) -> list[QualityIssue]:
        return [issue for issue in self.quality_issues if issue.record_id == record_id]

    def _station_issues(self) -> list[QualityIssue]:
        issues = []
        for record in self.records("stations"):
            rid, row, values = record["record_id"], record["source_row"], record["values"]
            name = values.get("plant_name")
            if record["record_type"] != "plant":
                issues.append(QualityIssue("not_a_plant_record", "info", "stations", rid,
                                           f"'{name}' is a {record['record_type'].replace('_', ' ')}, not a plant",
                                           source_row=row, context={"record_type": record["record_type"]}))
            if values.get("installed_capacity_mw") is None or values.get("available_capacity_mw") is None:
                missing = [k for k in ("installed_capacity_mw", "available_capacity_mw") if values.get(k) is None]
                issues.append(QualityIssue("missing_installed_capacity", "error", "stations", rid,
                                           f"'{name}' has no {' or '.join(missing)}", field=missing[0],
                                           source_row=row, context={"missing": missing}))
            if record["record_type"] != "plant":
                continue
            installed, _ = parse_number(values.get("installed_capacity_mw"))
            available, _ = parse_number(values.get("available_capacity_mw"))
            if installed is not None and available is not None and available > installed + CAPACITY_ABSOLUTE_TOLERANCE_MW:
                issues.append(QualityIssue("available_exceeds_installed", "error", "stations", rid,
                                           f"'{name}': available {available:g} MW exceeds installed {installed:g} MW",
                                           field="available_capacity_mw", source_row=row,
                                           context={"installed_mw": installed, "available_mw": available}))
            units = parse_units(values.get("generating_units"))
            if units is not None:
                for note in units.notes:
                    issues.append(QualityIssue(note, "warning" if note == "unparsed_unit_string" else "info",
                                               "stations", rid, f"'{name}': generating units '{units.raw}'",
                                               field="generating_units", source_row=row))
                if units.groups and installed is not None and capacities_conflict(units.total_mw, installed):
                    issues.append(QualityIssue("unit_capacity_mismatch", "error", "stations", rid,
                                               f"'{name}': units '{units.raw}' sum to {units.total_mw:g} MW but "
                                               f"installed capacity is {installed:g} MW",
                                               field="generating_units", source_row=row,
                                               context={"unit_sum_mw": units.total_mw, "installed_mw": installed}))
            evidence = self.capacity_evidence(rid)
            installed_values = [e for e in evidence["evidence"] if e["quantity"] == "installed_mw"]
            if installed is not None:
                for item in installed_values:
                    if item["table"] != "stations" and capacities_conflict(item["value"], installed):
                        issues.append(QualityIssue(
                            "capacity_conflict", "error", "stations", rid,
                            f"'{name}': installed {installed:g} MW here but {item['value']:g} MW in "
                            f"{item['source_sheet']} row {item['source_row']}",
                            field="installed_capacity_mw", source_row=row,
                            context={"station_mw": installed, "other_mw": item["value"],
                                     "other_sheet": item["source_sheet"], "other_row": item["source_row"]}))
            counts = {c["value"] for c in evidence["unit_counts"]}
            if len(counts) > 1:
                issues.append(QualityIssue("unit_count_conflict", "warning", "stations", rid,
                                           f"'{name}': unit counts differ across sources {sorted(counts)}",
                                           field="generating_units", source_row=row,
                                           context={"unit_counts": evidence["unit_counts"]}))
        return issues

    def _duplicate_issues(self) -> list[QualityIssue]:
        issues = []
        for table in ("stations", "hydropower", "hpp_summary", "reservoirs"):
            groups: dict[str, list[dict[str, Any]]] = {}
            for record in self.records(table):
                if record.get("record_type") not in {"plant", "reservoir"}:
                    continue
                key = name_key(self.record_name(record))
                if key is None:
                    issues.append(QualityIssue("missing_plant_name", "error", table, record["record_id"],
                                               f"{record['source_sheet']} row {record['source_row']} has no name",
                                               source_row=record["source_row"]))
                    continue
                groups.setdefault(key.text, []).append(record)
            for members in groups.values():
                if len(members) < 2:
                    continue
                rows = [m["source_row"] for m in members]
                for member in members:
                    issues.append(QualityIssue(
                        "duplicate_name_in_table", "warning", table, member["record_id"],
                        f"'{self.record_name(member)}' appears {len(members)} times in {member['source_sheet']} "
                        f"(rows {rows})", source_row=member["source_row"],
                        context={"rows": rows, "names": [self.record_name(m) for m in members],
                                 "sections": sorted({m.get('section') for m in members})}))
        return issues

    def _naming_issues(self) -> list[QualityIssue]:
        issues = []
        hydro = [r for r in self.records("hydropower") if self.record_name(r)]
        for station in self.records("stations"):
            if station["record_type"] != "plant":
                continue
            name = self.record_name(station)
            for record in hydro:
                relation = name_relation(name, self.record_name(record))
                if relation in {"variant", "possible"}:
                    issues.append(QualityIssue(
                        "naming_inconsistency", "warning", "stations", station["record_id"],
                        f"Station '{name}' and hydropower record '{self.record_name(record)}' "
                        f"(row {record['source_row']}) are spelled differently ({relation} match)",
                        source_row=station["source_row"],
                        context={"relation": relation, "hydropower_record_id": record["record_id"],
                                 "hydropower_name": self.record_name(record)}))
        return issues

    def _reservoir_issues(self) -> list[QualityIssue]:
        issues = []
        specs = [("reservoirs", "max_volume_mm3", "active_volume_mm3", "min_operating_volume_mm3"),
                 ("hydropower", "reservoir_max_volume_mm3", "reservoir_active_volume_mm3", None)]
        for table, max_key, active_key, min_key in specs:
            for record in self.records(table):
                values, rid, row = record["values"], record["record_id"], record["source_row"]
                name = self.record_name(record)
                if max_key not in values and active_key not in values:
                    continue
                parsed = {}
                for key in (max_key, active_key, min_key, "min_operating_level_masl", "max_operating_level_masl",
                            "dam_height_m", "area_full_storage_km2"):
                    if key and key in values:
                        value, note = parse_number(values.get(key))
                        parsed[key] = value
                        if note:
                            issues.append(QualityIssue(note, "info", table, rid,
                                                       f"'{name}': {key} = {values.get(key)!r}", field=key,
                                                       source_row=row))
                        if value is not None and value < 0:
                            issues.append(QualityIssue("negative_value", "error", table, rid,
                                                       f"'{name}': {key} is negative ({value:g})", field=key,
                                                       source_row=row))
                vmax, vact = parsed.get(max_key), parsed.get(active_key)
                vmin = parsed.get(min_key) if min_key else None
                problems = []
                if vmax is not None and vact is not None and vact > vmax + 1e-9:
                    problems.append(f"active volume {vact:g} exceeds max volume {vmax:g} Mm3")
                if vmax is not None and vmin is not None and vmin > vmax + 1e-9:
                    problems.append(f"min operating volume {vmin:g} exceeds max volume {vmax:g} Mm3")
                if vmax is not None and vmin is not None and vact is not None and vmin + vact > vmax * 1.001 + 1e-9:
                    problems.append(f"min operating {vmin:g} + active {vact:g} exceeds max volume {vmax:g} Mm3")
                if vmax is not None and vmax == 0:
                    problems.append("max volume is zero")
                for problem in problems:
                    issues.append(QualityIssue("reservoir_volume_order", "error", table, rid, f"'{name}': {problem}",
                                               field=max_key, source_row=row,
                                               context={"max": vmax, "active": vact, "min": vmin}))
                lmin, lmax = parsed.get("min_operating_level_masl"), parsed.get("max_operating_level_masl")
                if lmin is not None and lmax is not None and lmin >= lmax:
                    issues.append(QualityIssue("reservoir_level_order", "error", table, rid,
                                               f"'{name}': min operating level {lmin:g} is not below max {lmax:g} masl",
                                               field="min_operating_level_masl", source_row=row,
                                               context={"min_level": lmin, "max_level": lmax}))
        return issues

    # -- search -------------------------------------------------------------
    def search(self, table: str, query: str | None = None, *, limit: int = 500,
               offset: int = 0) -> dict[str, Any]:
        records = self.records(table)
        if query:
            needle = query.strip().lower()
            records = [r for r in records if needle in json.dumps(r["values"], ensure_ascii=False).lower()
                       or needle in (r.get("section") or "").lower()]
        counts: dict[str, dict[str, int]] = {}
        for issue in self.quality_issues:
            if issue.record_id:
                bucket = counts.setdefault(issue.record_id, {"error": 0, "warning": 0, "info": 0})
                bucket[issue.severity] += 1
        page = records[offset:offset + limit]
        rows = [{**record, "quality": counts.get(record["record_id"], {"error": 0, "warning": 0, "info": 0}),
                 "importable": table == "stations" and record.get("record_type") == "plant"}
                for record in page]
        return {"table": table, "total": len(records), "offset": offset, "limit": limit,
                "columns": self.columns(table), "records": rows}
