"""Georgia 2026 unit-level model built from the 2024-2025 monthly balances.

Fast checks (always): the balance parser, the committed balances reconcile
with themselves (row totals, generation + net flow = consumption, seasonal
row = plants + small HPPs), the committed sample is exactly what the
builder produces, and the mapping follows the documented choices.

Annual solve (release gate, ``POWERSIM_ANNUAL_ACCEPTANCE=1``): the 8,760-hour
solve is publishable, serves all load, delivers each regulating HPP's exact
monthly energy and keeps thermal output within installed capacity.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "samples/projects/georgia_2026_balance.json"
MONTH_END = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _balance(year: int) -> dict[str, dict]:
    document = json.loads((ROOT / f"data/actuals/georgia_balance_{year}.json").read_text(encoding="utf-8"))
    return {row["key"]: row for row in document["rows"]}


@pytest.fixture(scope="module")
def project() -> dict:
    return json.loads(SAMPLE.read_text(encoding="utf-8"))


HEADER = ("            name     jan     feb     mar     apr     may     jun     jul     aug     sep     oct     nov     dec      total\n")


def _line(name: str, cells: list[str]) -> str:
    # 13 right-aligned columns of width 10, as pdftotext -layout produces
    return f"{name:<16}" + "".join(f"{c:>10}" for c in cells) + "\n"


def test_parser_assigns_blank_months_by_column_and_checks_totals():
    parse_text = _script("parse_georgia_balance").parse_text
    full = ["1,00"] * 12 + ["12,00"]
    sparse = ["", "2,50"] + [""] * 9 + ["1 000,25", "1 002,75"]
    text = HEADER + _line("ენერგოსისტემა", full) + _line("plant", sparse) + \
        _line("flow", ["-1,00"] + ["0,00"] * 11 + ["-1,00"]) + _line("- მიღება", ["3,00"] + [""] * 11 + ["3,00"])
    rows = {r["key"]: r for r in parse_text(text)}
    assert rows["plant"]["monthly_gwh"] == [0, 2.5] + [0] * 9 + [1000.25]
    assert rows["flow"]["total_gwh"] == -1.0
    assert "flow - მიღება" in rows  # sub-rows are keyed by their section
    bad = HEADER + _line("ენერგოსისტემა", full) + _line("plant", ["1,00"] + [""] * 11 + ["2,00"])
    with pytest.raises(ValueError, match="months sum"):
        parse_text(bad)


@pytest.mark.parametrize("year", [2024, 2025])
def test_committed_balances_reconcile(year):
    rows = _balance(year)
    for row in rows.values():
        assert sum(row["monthly_gwh"]) == pytest.approx(row["total_gwh"], abs=0.05), row["key"]
    total = lambda key: rows[key]["total_gwh"]  # noqa: E731
    keys = list(rows)
    seasonal_plants = keys[keys.index("სეზონური ჰესები") + 1: keys.index("მცირე ჰესები")]
    assert sum(total(k) for k in seasonal_plants) + total("მცირე ჰესები") == pytest.approx(total("სეზონური ჰესები"), abs=0.1)
    assert total("მარეგულირებელი ჰესები") + total("სეზონური ჰესები") == pytest.approx(total("ჰიდროსადგურები სულ"), abs=0.1)
    assert total("ენერგოსისტემა") + total("გადადინება (სალდო) მ/შ") == \
        pytest.approx(total("მოხმარება (ჯამური დანაკარგებით)"), abs=0.1)


def test_committed_sample_is_reproducible(project):
    rebuilt = _script("build_georgia_2026_model").build_project()
    assert json.loads(json.dumps(rebuilt, ensure_ascii=False)) == project


def test_mapping_follows_the_documented_choices(project):
    from powersim.contracts import ProjectContract
    from powersim.validation.readiness import diagnose_project
    ProjectContract.model_validate(project)
    assert diagnose_project(project)["run_ready"]
    assets = {a["id"]: a for a in project["assets"]}
    profiles = {p["id"]: p["values"] for p in project["profiles"]}
    b24, b25 = _balance(2024), _balance(2025)

    enguri = assets["reg-hydro-res-006"]
    assert enguri["capacity_max_mw"] == 1300.0
    mean = [(a + b) / 2 * 1000 for a, b in zip(b24["ენგურჰესი"]["monthly_gwh"], b25["ენგურჰესი"]["monthly_gwh"])]
    assert enguri["legacy_extensions"]["monthly_energy_mwh"] == pytest.approx(mean, abs=1e-3)
    assert enguri["legacy_extensions"]["monthly_energy_mode"] == "exact"

    # Mtkvari HPP has no 2024 generation: only 2025 is used, flat within each month
    cf = profiles["cf-ror-hydro-seasonal-024"]
    cap = assets["ror-hydro-seasonal-024"]["capacity_max_mw"]
    assert cf[744] * cap * 672 == pytest.approx(b25["მტკვარიჰესი"]["monthly_gwh"][1] * 1000, rel=1e-6)
    assert cf[0] == 0.0
    # Vartsikhe 1-4 share one balance row and one asset
    assert assets["ror-hydro-seasonal-009"]["capacity_max_mw"] == 184.0

    net = b25["გადადინება (სალდო) მ/შ"]["monthly_gwh"]
    load = json.loads((ROOT / "data/gse_load_2026_2030.json").read_text(encoding="utf-8"))["load_mw_by_year"]["2026"]
    exports = -sum(v for v in net if v < 0)
    imports = sum(v for v in net if v > 0)
    assert sum(profiles["demand"]) / 1000 == pytest.approx(sum(load) / 1000 + exports, abs=0.01)
    assert sum(profiles["net-import-mw"]) / 1000 == pytest.approx(imports, abs=0.01)
    assert assets["net-import"]["legacy_extensions"]["pmin_profile"] == "net-import-mw"

    thermal = [a for a in project["assets"] if a["kind"] == "thermal"]
    assert sum(a["capacity_max_mw"] for a in thermal) == pytest.approx(1181.4)
    assert all(a["legacy_extensions"]["review_required"] for a in thermal)
    assert project["solver_settings"]["rolling_window_h"] == 8760


@pytest.mark.skipif(os.environ.get("POWERSIM_ANNUAL_ACCEPTANCE") != "1",
                    reason="set POWERSIM_ANNUAL_ACCEPTANCE=1 for the 8760-hour 2026 solve")
def test_full_year_2026_solve(tmp_path, project):
    from powersim.application import ApplicationService
    from powersim.contracts import ProjectContract
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager

    manager = RunManager(tmp_path / "workspace")
    manager.save_project(ProjectContract.model_validate(project))
    service = ApplicationService(manager, RegisteredLocalWorkflowExecutor(), asynchronous=False)
    run = service.create_run(project["id"])
    service.launch(run["id"])
    status = service.run_status(run["id"])
    assert status["status"] == "completed", status["events"][-1]
    envelope = service.result(run["id"])
    assert envelope["qa"]["status"] == "pass" and envelope["results"]["publication"]["publishable"]
    results = envelope["results"]
    summary = results["system_summary"]
    demand = sum(next(p["values"] for p in project["profiles"] if p["id"] == "demand"))
    assert summary["total_unserved_mwh"] == pytest.approx(0, abs=1.0)
    assert summary["total_energy_mwh"] == pytest.approx(demand, rel=1e-6)
    assets = {a["id"]: a for a in project["assets"]}
    thermal_total = 0.0
    for asset_id, rows in results["hourly_by_unit"].items():
        asset = assets[asset_id]
        if asset_id.startswith("reg-"):
            monthly = [0.0] * 12
            for row in rows:
                monthly[next(i for i, end in enumerate(MONTH_END) if row["t"] < end)] += row["dispatch_mw"]
            assert monthly == pytest.approx(asset["legacy_extensions"]["monthly_energy_mwh"], abs=1.0)
        if asset["kind"] == "thermal":
            assert max(row["dispatch_mw"] for row in rows) <= asset["capacity_max_mw"] + 1e-6
            thermal_total += sum(row["dispatch_mw"] for row in rows)
    print("GEORGIA_2026=" + json.dumps({"thermal_gwh": round(thermal_total / 1000, 1),
                                        "objective_usd": summary["total_objective_cost_usd"],
                                        "served_gwh": round(summary["total_energy_mwh"] / 1000, 1)}))


def test_balance_comparison_groups_assets_onto_balance_rows(project):
    compare = _script("compare_georgia_balance").compare
    profiles = {p["id"]: p["values"] for p in project["profiles"]}
    dispatch = {a["id"]: [0.0] * 8760 for a in project["assets"]}
    dispatch["net-import"] = profiles["net-import-mw"]
    dispatch["tpp-gardabani-1"] = [100.0] * 8760
    dispatch["tpp-tbilsresi"] = [10.0] * 8760
    dispatch["wind-wind-001"] = [5.0] * 8760
    dispatch["wind-wind-002"] = [7.0] * 8760
    rows = {(r["row"], r["month"]): r for r in compare(project, dispatch, list(_balance(2025).values()))}
    assert rows[("გარდაბნის თბოსადგური", 1)]["model_gwh"] == pytest.approx(74.4)
    assert rows[("თბოსადურები სულ", "year")]["model_gwh"] == pytest.approx(963.6)
    assert rows[("ქართლის ქარის ელ.სადგური", "year")]["model_gwh"] == pytest.approx(43.8)  # Zemo wind excluded
    # the fixed 2025 net flow reproduces the 2025 balance row exactly
    assert rows[("გადადინება (სალდო) მ/შ", "year")]["diff_gwh"] == pytest.approx(0, abs=0.02)
    assert rows[("მოხმარება (ჯამური დანაკარგებით)", "year")]["model_gwh"] == pytest.approx(15621.0, abs=0.01)
    assert rows[("ენგურჰესი", 3)]["balance_gwh"] == 94.25 and rows[("ენგურჰესი", 3)]["model_gwh"] == 0
