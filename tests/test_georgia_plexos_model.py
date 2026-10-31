"""Georgia 2030 project reconstructed from the PLEXOS model 'BESS Research 800-1400'.

Fast checks (always): the committed sample is exactly what the importer
produces from the archive, the mapping matches the PLEXOS objects, and the
fixed supply reconciles with PLEXOS's own generation total.

Benchmark (release gate, ``POWERSIM_ANNUAL_ACCEPTANCE=1``): the 8,760-hour
PowerSim solve is publishable, delivers the regulating-HPP monthly energy
PLEXOS delivered, and has no more unserved energy than PLEXOS's own
solution implies (PowerSim optimises the whole year at once, so it may only
do better under the same constraints).
"""
from __future__ import annotations

import importlib.util
import json
import os
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "plexos model.zip"
SAMPLE = ROOT / "samples/projects/georgia_2030_plexos.json"
BENCHMARK = ROOT / "data/benchmarks/plexos_bess_research_800_1400.json"

# PLEXOS object values, transcribed from BESS Research 800-1400.xml (Max Capacity, MW)
ROR_CAPACITY_MW = {"kodori": 21.39, "rioni-oni": 217.44, "rioni-alpana": 254.89, "tekhuri": 192.81, "kvirila": 49.67,
                   "supsa": 136.7, "adjaristskali": 54.23, "liakhvi": 165.87, "paravani": 221.01,
                   "tusheti-alazani": 139.28, "mtiuleti-aragvi": 34.21, "alazani-birkiani": 67.44,
                   "mashavera": 150.19, "alazani-shakriani": 27.42, "chorokhi": 100.1, "samgori": 39.41}
REG_ENERGY_GWH = [296.0002195, 275.9999706, 490.9995911, 429.0, 829.9997318, 890.0003386, 1049.999735,
                  766.0004557, 497.999967, 366.9997787, 318.9996773, 366.9997589]


def _importer():
    spec = importlib.util.spec_from_file_location("import_plexos_model", ROOT / "scripts/import_plexos_model.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def project() -> dict:
    return json.loads(SAMPLE.read_text(encoding="utf-8"))


@pytest.mark.skipif(not ARCHIVE.is_file(), reason="PLEXOS archive not in this checkout")
def test_committed_sample_is_reproducible_from_the_archive(project):
    rebuilt = _importer().build_project(ARCHIVE)
    assert json.loads(json.dumps(rebuilt)) == project
    reference = _importer().benchmark_reference(ARCHIVE, rebuilt)
    assert reference == json.loads(BENCHMARK.read_text(encoding="utf-8"))


def test_mapping_matches_the_plexos_objects(project):
    from powersim.contracts import ProjectContract
    from powersim.validation.readiness import diagnose_project
    assert diagnose_project(project)["run_ready"]
    ProjectContract.model_validate(project)
    assets = {a["id"]: a for a in project["assets"]}
    kinds = [a["kind"] for a in project["assets"]]
    assert kinds.count("hydro_ror") == 17 and kinds.count("wind") == 5 and kinds.count("solar") == 9
    for zone, capacity in ROR_CAPACITY_MW.items():
        assert assets[f"ror-{zone}"]["capacity_max_mw"] == capacity
    reg = assets["regulating-hpp"]["legacy_extensions"]
    assert reg["monthly_energy_mwh"] == pytest.approx([v * 1000 for v in REG_ENERGY_GWH], abs=1e-3)
    assert reg["monthly_energy_mode"] == "exact" and reg["monthly_pmin_mw"][3] == 370.1
    tpp = assets["tpp-import"]["legacy_extensions"]
    assert tpp["pmax_profile"] == tpp["pmin_profile"] == "tpp-import-mw" and tpp["vom"] == 3
    bess = assets["bess-ksani"]["legacy_extensions"]
    assert (bess["power_mw"], bess["energy_mwh"], bess["soc_init"]) == (800, 1400, 0.5)
    assert all(a["provenance"]["source_version"] == project["metadata"]["source_sha256"] for a in project["assets"])
    assert project["solver_settings"]["rolling_window_h"] == 8760  # monthly energy needs a full-horizon solve


def test_fixed_supply_reconciles_with_plexos_generation(project):
    """PLEXOS forces renewables to their rating, so its generation equals the
    available energy of the default profiles plus the fixed TPP and HPP energy."""
    benchmark = json.loads(BENCHMARK.read_text(encoding="utf-8"))
    profiles = {p["id"]: p["values"] for p in project["profiles"]}
    available = 0.0
    for asset in project["assets"]:
        if asset["kind"] in {"hydro_ror", "wind", "solar"} and asset["id"] != "regulating-hpp":
            cf = profiles[asset["profile_references"][0]]
            available += sum(min(1.0, max(0.0, v)) for v in cf) * asset["capacity_max_mw"]
    fixed = available + sum(profiles["tpp-import-mw"]) + sum(REG_ENERGY_GWH) * 1000
    assert fixed == pytest.approx(sum(benchmark["generation_mwh"].values()), abs=1.0)
    assert sum(profiles["demand"]) == pytest.approx(benchmark["demand_mwh"], abs=0.01)
    assert benchmark["regulating_hpp_monthly_mwh"] == pytest.approx([v * 1000 for v in REG_ENERGY_GWH], abs=0.01)


def test_scenarios_only_swap_renewable_profiles(project):
    base = {a["id"]: a for a in project["assets"]}
    for scenario in project["scenarios"]:
        swapped = {a["id"]: a for a in scenario["overlay"]["assets"]}
        assert swapped.keys() == base.keys()
        for asset_id, asset in swapped.items():
            assert {k: v for k, v in asset.items() if k != "profile_references"} == \
                   {k: v for k, v in base[asset_id].items() if k != "profile_references"}
    dry = {a["id"]: a for a in next(s for s in project["scenarios"] if s["id"] == "hydro-dry")["overlay"]["assets"]}
    assert dry["ror-kodori"]["profile_references"] == ["cf-ror-kodori-dry"]
    assert dry["wind-kolkheti"]["profile_references"] == base["wind-kolkheti"]["profile_references"]


@pytest.mark.skipif(os.environ.get("POWERSIM_ANNUAL_ACCEPTANCE") != "1",
                    reason="set POWERSIM_ANNUAL_ACCEPTANCE=1 for the 8760-hour PLEXOS benchmark")
def test_powersim_full_year_matches_or_improves_on_plexos(tmp_path, project):
    from powersim.application import ApplicationService
    from powersim.contracts import ProjectContract
    from powersim.execution import RegisteredLocalWorkflowExecutor
    from powersim.platform import RunManager

    benchmark = json.loads(BENCHMARK.read_text(encoding="utf-8"))
    manager = RunManager(tmp_path / "workspace")
    manager.save_project(ProjectContract.model_validate(project))
    service = ApplicationService(manager, RegisteredLocalWorkflowExecutor(), asynchronous=False)
    run = service.create_run(project["id"])
    started = time.monotonic()
    service.launch(run["id"])
    status = service.run_status(run["id"])
    assert status["status"] == "completed", status["events"][-1]
    envelope = service.result(run["id"])
    assert envelope["qa"]["status"] == "pass" and envelope["results"]["publication"]["publishable"]
    results = envelope["results"]
    rows = results["hourly_by_unit"]["regulating-hpp"]
    month_end = (744, 1416, 2160, 2880, 3624, 4344, 5088, 5832, 6552, 7296, 8016, 8760)
    monthly = [0.0] * 12
    for row in rows:
        monthly[next(i for i, end in enumerate(month_end) if row["t"] < end)] += row["dispatch_mw"]
    assert monthly == pytest.approx(benchmark["regulating_hpp_monthly_mwh"], abs=1.0)
    summary = results["system_summary"]
    served = summary["total_energy_mwh"]
    assert served + summary["total_unserved_mwh"] == pytest.approx(benchmark["demand_mwh"], rel=1e-6)
    # same constraints, whole-year optimisation: never more unserved energy than PLEXOS
    assert summary["total_unserved_mwh"] <= benchmark["implied_unserved_mwh"] + 1.0
    print("PLEXOS_BENCHMARK=" + json.dumps({
        "runtime_s": round(time.monotonic() - started, 1),
        "powersim_unserved_mwh": summary["total_unserved_mwh"],
        "plexos_implied_unserved_mwh": benchmark["implied_unserved_mwh"],
        "powersim_curtailed_wind_solar_mwh": summary["total_curtailed_mwh"],
        "plexos_implied_dump_mwh": benchmark["implied_dump_mwh"],
        "objective_usd": summary["total_objective_cost_usd"]}, sort_keys=True))
