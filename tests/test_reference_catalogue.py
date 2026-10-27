"""Reference catalogue parsing and data-quality safeguards.

These tests pin the known source conditions of ``Power Plant Parameters
English.xlsx`` so that a regenerated catalogue cannot silently hide them.
"""
from __future__ import annotations

import pytest

from powersim.reference import ReferenceCatalogue, name_relation, parse_number, parse_units
from powersim.reference.catalogue import capacities_conflict


@pytest.fixture(scope="module")
def catalogue() -> ReferenceCatalogue:
    return ReferenceCatalogue()


@pytest.fixture(scope="module")
def report(catalogue):
    return catalogue.quality_report()


def _codes(report, code):
    return [issue for issue in report["issues"] if issue["code"] == code]


def test_manifest_and_record_counts(catalogue):
    summary = catalogue.summary()
    assert summary["sha256"] == "f8c8b90f61ccbfcda12ea7b4d353c96fa27aac3bc9e58a8bfb7ee56a845ff4ca"
    assert summary["review_required"] is True
    assert len(catalogue.records("stations")) == 139
    assert len(catalogue.records("reservoirs")) == 83
    assert len(catalogue.records("hpp_summary")) == 118
    assert len(catalogue.records("hydropower")) == 122
    assert len(catalogue.detail_sheets) == 134
    # Every record keeps its source sheet and row for provenance.
    for table in ("stations", "reservoirs", "hydropower", "hpp_summary"):
        for record in catalogue.records(table):
            assert record["source_sheet"] and record["source_row"] >= 2


def test_hpp_summary_keeps_first_plant_as_a_record(catalogue):
    """A one-header export turned Khrami HPP-1 into the column header."""
    first = catalogue.records("hpp_summary")[0]
    assert first["values"]["plant_name"] == "Khrami HPP-1"
    assert first["values"]["generating_units"] == "3x37,6"


def test_hydropower_sections_keep_their_own_columns(catalogue):
    sections = {r["section"] for r in catalogue.records("hydropower")}
    assert sections == {"Hydropower plants (RoR)", "Hydropower plants (Storage)"}
    enguri = next(r for r in catalogue.records("hydropower") if r["values"]["plant_name"] == "Enguri HPP")
    assert enguri["values"]["reservoir_max_volume_mm3"] == 1100
    assert enguri["values"]["turbine_max_capacity_mw"] == 1300


@pytest.mark.parametrize("raw,expected,note", [
    (112.8, 112.8, None), ("425,6", 425.6, "comma_decimal"), ("292/195", None, "compound_value"),
    (" 18 ", 18.0, None), (None, None, None), ("", None, None),
])
def test_parse_number(raw, expected, note):
    assert parse_number(raw) == (expected, note)


@pytest.mark.parametrize("raw,count,total,notes", [
    ("3x37,6", 3, 112.8, ("comma_decimal",)),
    ("2x10.6+2x9.6", 4, 40.4, ()),
    ("1x0,888;2x0,56", 3, 2.008, ("comma_decimal",)),
    ("2x39.144+37.28", 3, 115.568, ()),
    ("3×6.25", 3, 18.75, ()),
])
def test_parse_units(raw, count, total, notes):
    parsed = parse_units(raw)
    assert parsed.unit_count == count
    assert parsed.total_mw == pytest.approx(total)
    assert parsed.notes == notes


def test_unparseable_unit_string_is_kept_not_guessed():
    parsed = parse_units("(3X38+1X0,7)/(3X12)")
    assert parsed.groups == () and parsed.notes == ("unparsed_unit_string",)
    assert parsed.raw == "(3X38+1X0,7)/(3X12)"


@pytest.mark.parametrize("left,right,relation", [
    ("Zahesi", "Zahesi HPP", "exact"),
    ("Rion HPP", "Rioni HPP", "variant"),
    ("Gumat HPPi", "Gumati HPP-1", "possible"),
    ("Faravani HPP", "Paravani HPP", "possible"),
    ("Gumati HPP-1", "Gumati HPP-2", None),
    ("Abhesi HPP", "Zahesi", None),
])
def test_name_relations_are_explicit(left, right, relation):
    assert name_relation(left, right) == relation


def test_capacity_tolerance():
    assert not capacities_conflict(100.0, 101.0)
    assert capacities_conflict(100.0, 103.0)
    assert not capacities_conflict(0.30, 0.34)


def test_two_station_records_without_capacity_are_reported(report):
    missing = _codes(report, "missing_installed_capacity")
    assert {i["context"]["missing"][0] for i in missing} == {"installed_capacity_mw"}
    assert len(missing) == 2
    assert all(i["severity"] == "error" for i in missing)


def test_known_duplicate_hydropower_names_are_reported(report):
    duplicated = {name for issue in _codes(report, "duplicate_name_in_table") if issue["table"] == "hydropower"
                  for name in issue["context"]["names"]}
    for name in ("Dariali HPP", "Ortachala HPP", "Shuakhevi HPP", "Zahesi", "Gumat HPPi"):
        assert name in duplicated
    assert len({n for n in duplicated}) >= 20


def test_naming_variants_between_station_and_hydropower_tables(report):
    pairs = {(i["message"].split("'")[1], i["context"]["hydropower_name"]) for i in _codes(report, "naming_inconsistency")}
    assert ("Gumati HPP-1", "Gumat HPPi") in pairs
    assert ("Rioni HPP", "Rion HPP") in pairs


def test_conflicting_capacities_are_errors(report):
    exceed = {i["message"].split("'")[1] for i in _codes(report, "available_exceeds_installed")}
    assert {"Gardabani TPP 1 (CCPP 1)", "Shuakhevi HPP"} <= exceed
    mismatch = {i["message"].split("'")[1]: i for i in _codes(report, "unit_capacity_mismatch")}
    assert mismatch["Aragvi 2 HPP"]["context"] == {"unit_sum_mw": 95.0, "installed_mw": 1.95}
    assert _codes(report, "capacity_conflict"), "cross-table capacity conflicts must be reported"
    assert all(i["severity"] == "error" for i in _codes(report, "capacity_conflict"))


def test_unit_count_conflict_and_comma_decimals(report):
    assert _codes(report, "unit_count_conflict")
    assert _codes(report, "comma_decimal")


def test_reservoir_ordering_checks(catalogue, tmp_path):
    import json
    import shutil
    target = tmp_path / "catalogue"
    shutil.copytree(catalogue.directory, target)
    reservoirs = json.loads((target / "reservoirs.json").read_text(encoding="utf-8"))
    values = reservoirs["records"][0]["values"]
    values.update({"max_volume_mm3": 100, "active_volume_mm3": 150, "min_operating_volume_mm3": 10,
                   "min_operating_level_masl": 500, "max_operating_level_masl": 480})
    (target / "reservoirs.json").write_text(json.dumps(reservoirs), encoding="utf-8")
    report = ReferenceCatalogue(target).quality_report()
    rid = reservoirs["records"][0]["record_id"]
    codes = {i["code"] for i in report["issues"] if i["record_id"] == rid}
    assert {"reservoir_volume_order", "reservoir_level_order"} <= codes


def test_search_flags_importable_station_rows_only(catalogue):
    result = catalogue.search("stations", "thermal power plants")
    header = next(r for r in result["records"] if r["record_type"] == "section_header")
    assert header["importable"] is False
    plants = catalogue.search("stations", "Enguri")["records"]
    assert any(r["importable"] for r in plants)
    assert all(not r["importable"] for r in catalogue.search("reservoirs", "Enguri")["records"])
