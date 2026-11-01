"""The outlook report generator reads every number from the results JSON."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _module():
    spec = importlib.util.spec_from_file_location("georgia_outlook_report", ROOT / "scripts/georgia_outlook_report.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dispatch(thermal: float) -> dict:
    monthly = {"reg-hydro-res-006": [100.0] * 12, "ror-hydro-seasonal-009": [50.0] * 12, "wind-ruisi": [10.0] * 12,
               "new-pv-imereti": [5.0] * 12, "net-import": [20.0] * 12, "tpp-gardabani-1": [thermal / 12] * 12,
               "new-rice": [1.0] * 12, "bess-200": [0.5] * 12}
    return {"monthly_gwh": monthly, "gas_mm3": 300.0, "served_gwh": 2000.0, "unserved_gwh": 0.0, "curtailed_gwh": 1.0,
            "peak_load_mw": 2900.0, "hours_thermal_full": 12, "objective_usd": 1.0e8, "qa": "pass", "publishable": True,
            "reserve": {"largest_unit_mw": 300.0, "vre_balancing_mw": 100.0, "requirement_mw": 400.0,
                        "min_headroom_mw": 450.0, "hours_short": 0, "wind_mw": 245.4, "solar_mw": 10.8}}


def _results() -> dict:
    adequacy = {"LOLE_h": 2.5, "EENS_mwh": 900.0}
    data = {"assumptions": ["a", "b"], "lole_target_h": 3.0, "candidates": {}, "bess": {"power_mw": 200, "energy_mwh": 200,
                                                                                          "from_year": 2029},
            "plans": {}, "sensitivities": {}, "no_expansion": {}, "no_retirement": {}}
    for scenario, rice, ccgt in (("A", 240.0, True), ("B", 0.0, False)):
        option = {"ccgt3": ccgt, "rice_mw": rice, "annual_cost_usd": 1.5e8, "capital_fom_usd": 5.0e7,
                  "adequacy": adequacy, "meets_target": True}
        data["plans"][scenario] = [{"year": 2030, "rice_mw": rice, "ccgt3": ccgt, "meets_target": True,
                                    "options": [option], "adequacy": adequacy, "dispatch": _dispatch(1200.0)}]
        key = f"{scenario}-2030"
        data["sensitivities"][key] = {"dry_2025": _dispatch(1500.0), "demand_+5": {**_dispatch(1400.0),
                                                                                   "adequacy": {"LOLE_h": 4.0}}}
        data["no_expansion"][key] = {"LOLE_h": 120.0, "EENS_mwh": 50000.0}
        data["no_retirement"][key] = {"LOLE_h": 40.0, "EENS_mwh": 9000.0}
    return data


def test_summary_groups_energy_and_reads_every_metric():
    report = _module()
    summary = report.summarise(_results())
    a = next(r for r in summary["rows"] if r["scenario"] == "A")
    assert a["energy"]["reg_hydro"] == 1200.0 and a["energy"]["thermal_existing"] == pytest.approx(1200.0)
    assert a["energy"]["thermal_new"] == 12.0 and a["energy"]["solar"] == 60.0
    assert a["thermal_gwh"] == pytest.approx(1212.0)  # battery is not counted as generation
    assert (a["lole_no_build"], a["lole_keep_tbilsresi"], a["lole_plan"]) == (120.0, 40.0, 2.5)
    assert a["sensitivities"]["dry_2025"]["thermal_gwh"] == pytest.approx(1512.0)
    assert a["sensitivities"]["demand_+5"]["lole"] == 4.0
    text = " ".join(report.findings(summary))
    assert "CCGT-3 — 2030 წლიდან" in text and "RICE საჭიროა 2030 წლიდან" in text
    assert "RICE 2032 წლამდე არ არის საჭირო" in text


def test_html_and_xlsx_are_written(tmp_path):
    report = _module()
    summary = report.summarise(_results())
    html, xlsx = tmp_path / "r.html", tmp_path / "r.xlsx"
    report.write_html(summary, html, ROOT / "scripts/templates/georgia_outlook_report.html")
    report.write_xlsx(summary, xlsx)
    page = html.read_text(encoding="utf-8")
    assert "/*__DATA__*/null" not in page and "<title>საქართველოს ენერგოსისტემა 2027–2032</title>" in page
    embedded = json.loads(page.split("const DATA = ", 1)[1].split(";\nconst S", 1)[0])
    assert embedded["summary"]["rows"][0]["lole_no_build"] == 120.0
    from openpyxl import load_workbook
    book = load_workbook(xlsx)
    assert book.sheetnames[:2] == ["შეჯამება", "ენერგობალანსი"] and "დაშვებები" in book.sheetnames
    assert book["შეჯამება"].max_row == 3
