"""Georgia 2027-2032 outlook: case construction, adequacy maths and reserve sizing.

The adequacy functions are checked against hand-calculated cases; the case
builder against the documented assumptions (battery from 2029, Tbilsresi III/IV
retired, Tkibuli excluded, scenario B reaching the PLEXOS 2030 fleet).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def go():
    spec = importlib.util.spec_from_file_location("georgia_outlook", ROOT / "scripts/georgia_outlook.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_hydro_shortfall_capacity_limited(go):
    unserved, lol = go.hydro_dispatch_shortfall(np.array([[100.0, 50.0, -10.0]]), energy=1000, cap=80)
    assert unserved.tolist() == [[20.0, 0.0, 0.0]] and lol.tolist() == [[True, False, False]]


def test_hydro_shortfall_energy_limited_minimises_eens_then_lole(go):
    # 35 MWh covers the 10 and 20 MW hours fully; the rest reduces the 30 MW hour
    unserved, lol = go.hydro_dispatch_shortfall(np.array([[10.0, 20.0, 30.0]]), energy=35, cap=100)
    assert unserved.tolist() == [[0.0, 0.0, 25.0]] and lol.sum() == 1
    # EENS = sum(residual+) - energy whenever energy binds: 220 - 50 = 170
    unserved, lol = go.hydro_dispatch_shortfall(np.array([[10.0, 90.0, 120.0]]), energy=50, cap=100)
    assert unserved.sum() == pytest.approx(170.0) and lol.sum() == 2


def test_markov_availability_matches_the_forced_outage_rate(go):
    available = go.markov_availability(100.0, 0.1, 50.0, 400, np.random.default_rng(1))
    assert available.mean() == pytest.approx(90.0, abs=1.0)
    assert set(np.unique(available)) <= {0.0, 100.0}


def test_case_builder_follows_the_assumptions(go):
    a2028 = go.build_case(2028, "A")
    a2029 = go.build_case(2029, "A")
    ids = {a["id"] for a in a2028["assets"]}
    assert "tpp-tbilsresi" not in ids and "tpp-tkibuli" not in ids and "wind-ruisi" in ids
    assert "bess-200" not in ids
    bess = next(a for a in a2029["assets"] if a["id"] == "bess-200")["legacy_extensions"]
    assert (bess["power_mw"], bess["energy_mwh"]) == (200.0, 200.0)
    kept = go.build_case(2027, "A", retire_tbilsresi=False)
    assert any(a["id"] == "tpp-tbilsresi" for a in kept["assets"])

    def capacity(project, kind):
        return sum(a["capacity_max_mw"] for a in project["assets"] if a["kind"] == kind)
    b2030, b2032, b2028 = go.build_case(2030, "B"), go.build_case(2032, "B"), go.build_case(2028, "B")
    assert capacity(b2030, "wind") == pytest.approx(1908.55, abs=0.1)
    assert capacity(b2030, "solar") == pytest.approx(1496.06, abs=0.1)
    assert capacity(b2032, "wind") == pytest.approx(capacity(b2030, "wind"))
    assert capacity(b2028, "wind") - capacity(a2028, "wind") == pytest.approx((1908.55 - 245.45) / 2, abs=0.1)

    rice = go.build_case(2030, "A", rice_mw=60, ccgt3=True)
    new = {a["id"]: a for a in rice["assets"] if a["id"].startswith("new-")}
    assert new["new-rice"]["capacity_max_mw"] == 60 and new["new-ccgt-3"]["capacity_max_mw"] == 250
    from powersim.validation.readiness import diagnose_project
    assert diagnose_project(rice)["run_ready"]


def test_load_growth_after_2030(go):
    assert go.domestic_load(2030).sum() / 1000 == pytest.approx(18060.0, abs=1.0)
    assert go.domestic_load(2031).sum() / go.domestic_load(2030).sum() == pytest.approx(1 + go.GROWTH)
    assert go.GROWTH == pytest.approx(0.0369, abs=0.0002)


def test_reserve_requirement_and_annualised_cost(go):
    req = go.reserve_requirement(go.build_case(2027, "A"))
    assert req["largest_unit_mw"] == 300.0  # Mtkvari IX
    assert req["requirement_mw"] == pytest.approx(req["largest_unit_mw"] + req["vre_balancing_mw"])
    assert go.reserve_requirement(go.build_case(2030, "B"))["vre_balancing_mw"] > req["vre_balancing_mw"]
    crf = 0.08 / (1 - 1.08 ** -25)
    assert go.annualised_cost(20, False) == pytest.approx(20_000 * (850 * crf + 15))
