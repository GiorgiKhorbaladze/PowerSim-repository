import importlib.util
from pathlib import Path

import pytest

from powersim.components import BuildContext, ComponentResult, stage3a_registry
from powersim.qa import check_component_results

SPEC = importlib.util.spec_from_file_location("legacy_solver", Path(__file__).parents[2] / "solver/powersim_solver.py")
legacy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(legacy)


@pytest.mark.parametrize("kind,asset,profiles", [
    ("wind", {"id":"w","type":"wind","pmax_installed":100,"availability_profile":"p","_degradation_factor":.9}, {"p":[.3]}),
    ("solar", {"id":"s","type":"solar","pmax_installed":80,"availability_profile":"p","_degradation_factor":1}, {"p":[.5]}),
    ("hydro_ror", {"id":"r","type":"hydro_ror","pmax":30,"availability_profile":"p"}, {"p":[.75]}),
    ("import", {"id":"i","type":"import","pmax_profile":"p"}, {"p":[22]}),
])
def test_shared_availability_matches_legacy(kind, asset, profiles):
    component = stage3a_registry().create(kind, asset)
    context = BuildContext(None, (1,), .25, profiles=profiles)
    assert component.available_mw(context, 0) == pytest.approx(
        legacy.get_pmax_t(asset, 0, profiles, 0, .25), abs=1e-10)


def test_canonical_result_qa_and_curtailment_energy():
    result = ComponentResult("w", "wind", 1, available_mw=10,
                             injection_mw=6, curtailed_mw=4, curtailed_mwh=1)
    checks = check_component_results([result], .25)
    assert checks and all(check.passed for check in checks)


def test_legacy_default_and_shared_mixed_fleet_objective_dispatch_parity():
    assets = {
        "thermal": {"id":"thermal","type":"thermal","pmax":100,"pmin":0,"_committable":False,"_dispMC":30,"_gas_rate":0},
        "w": {"id":"w","type":"wind","pmax_installed":10,"availability_profile":"wind","_committable":False,"_dispMC":0,"_gas_rate":0},
        "s": {"id":"s","type":"solar","pmax_installed":10,"availability_profile":"solar","_committable":False,"_dispMC":0,"_gas_rate":0},
        "r": {"id":"r","type":"hydro_ror","pmax":10,"availability_profile":"ror","_committable":False,"_dispMC":5,"_gas_rate":0},
        "i": {"id":"i","type":"import","pmax":10,"_committable":False,"_dispMC":20,"_gas_rate":0},
    }
    profiles = {"wind":[.5], "solar":[.5], "ror":[.5]}
    base = {"solver":"highs", "time_limit_s":30}
    old = legacy.solve_window(assets, [30], profiles, [], {}, {}, base)
    new = legacy.solve_window(assets, [30], profiles, [], {}, {}, {**base, "component_engine":"shared"})
    assert new[3] == pytest.approx(old[3], abs=.01)
    assert new[0][0]["dispatch"] == old[0][0]["dispatch"]

