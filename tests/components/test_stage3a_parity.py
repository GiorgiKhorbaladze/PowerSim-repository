import importlib.util
from pathlib import Path

import pytest
import pyomo.environ as pyo

from powersim.components import BuildContext, ComponentResult, stage3a_registry
from powersim.qa import check_component_results, run_qa
from powersim.results import evaluate_publication, finalized_diagnostics

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
    assert len(new[0].component_results) == 4
    assert {item.period for item in new[0].component_results} == {0}


@pytest.mark.parametrize("duration", [1.0, .25])
@pytest.mark.parametrize("kind,asset", [
    ("wind", {"id":"a","type":"wind","pmax_installed":10,"_dispMC":2,"vom":.5}),
    ("solar", {"id":"a","type":"solar","pmax_installed":10,"_dispMC":2,"vom":.5}),
    ("hydro_ror", {"id":"a","type":"hydro_ror","pmax":10,"_dispMC":2,"vom":.5}),
    ("import", {"id":"a","type":"import","pmax":10,"_dispMC":2,"vom":.5}),
])
def test_shared_cost_terms_match_legacy_objective_contribution(kind, asset, duration):
    model = pyo.ConcreteModel()
    model.T = pyo.Set(initialize=[1])
    model.G = pyo.Set(initialize=["a"])
    model.p = pyo.Var(model.G, model.T)
    model.p["a", 1].value = 4
    context = BuildContext(model, (1,), duration)
    terms = stage3a_registry().create(kind, asset).objective_terms(context)
    assert sum(term.value() for term in terms) == pytest.approx(
        (asset["_dispMC"] + asset["vom"]) * 4 * duration)


@pytest.mark.parametrize("kind,asset,profiles,dt,offset", [
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p"}, {"p":[0]}, 1, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p"}, {"p":[1]}, 1, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p"}, {"p":[.37]}, .25, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","dc_ac_ratio":1.3}, {"p":[1]}, 1, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","_degradation_factor":.8,"wake_loss_frac":.1}, {"p":[.5]}, 1, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","monthly_availability_factor":[.7]*12}, {"p":[1]}, 1, 800),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","air_density_correction":True,"temp_profile_key":"temp"}, {"p":[1],"temp":[30]}, .25, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","maint_windows":[{"start_h":0,"end_h":1,"availability_factor":.5}]}, {"p":[1]}, 1, 0),
    ("wind", {"id":"a","type":"wind","pmax_installed":100,"availability_profile":"p","temp_profile_key":"temp","temp_derating_curve":[[0,1],[40,.6]]}, {"p":[1],"temp":[20]}, 1, 0),
    ("solar", {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p","dc_ac_ratio":1.4,"_degradation_factor":.9}, {"p":[0]}, .25, 0),
    ("solar", {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p","dc_ac_ratio":1.4,"_degradation_factor":.9}, {"p":[.5]}, 1, 0),
    ("solar", {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p","dc_ac_ratio":1.4,"_degradation_factor":.9}, {"p":[1]}, 1, 0),
    ("solar", {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p","maint_windows":[{"start_h":0,"end_h":1,"availability_factor":.5}]}, {"p":[1]}, 1, 0),
    ("solar", {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p","temp_profile_key":"temp","temp_derating_curve":[[0,1],[40,.6]]}, {"p":[1],"temp":[20]}, .25, 0),
])
def test_targeted_vre_formula_parity(kind, asset, profiles, dt, offset):
    context = BuildContext(None, (1,), dt, profiles=profiles, offset_hours=offset)
    actual = stage3a_registry().create(kind, asset).available_mw(context, 0)
    assert actual == pytest.approx(legacy.get_pmax_t(asset, 0, profiles, offset, dt), abs=1e-10)


def test_degradation_maintenance_temperature_are_each_applied_once():
    asset = {"id":"a","type":"solar","pmax_installed":100,"availability_profile":"p",
             "_degradation_factor":.9,
             "maint_windows":[{"start_h":0,"end_h":1,"availability_factor":.5}],
             "temp_profile_key":"temp","temp_derating_curve":[[0,1],[40,.6]]}
    profiles = {"p":[1], "temp":[20]}
    actual = stage3a_registry().create("solar", asset).available_mw(
        BuildContext(None, (1,), 1, profiles=profiles), 0)
    assert actual == pytest.approx(36)  # 100 × .9 × .5 × .8, exactly once each
    assert actual == legacy.get_pmax_t(asset, 0, profiles, 0, 1)


def test_rolling_shared_extraction_publishes_committed_periods_only():
    raw_assets = [
        {"id":"t","type":"thermal","pmax":100,"mc":50,"committable":False},
        {"id":"w","type":"wind","pmax_installed":10,"availability_profile":"wind"},
    ]
    assets = legacy.build_asset_map({"metadata":{"study_year":2026}, "assets":raw_assets})
    inp = {"resolution_min":60, "study_horizon":{"start_hour":0,"horizon_hours":3},
           "solver_settings":{"solver":"highs","component_engine":"shared",
                              "rolling_window_h":2,"rolling_step_h":1},
           "assets":raw_assets, "reserve_products":[]}
    profiles = {"demand":[5,5,5], "wind":[.2,.3,.4]}
    rows, _, _ = legacy.solve_all(inp, assets, profiles, {})
    assert len(rows) == 3
    assert len(rows.component_results) == 3
    assert [item.period for item in rows.component_results] == [0, 1, 2]


def test_shared_component_qa_failure_blocks_stage2_publication_gate():
    from powersim.contracts import SolverDiagnostics
    diagnostics = SolverDiagnostics(backend="highs", termination_condition="optimal",
        normalized_status="optimal", has_incumbent=True, incumbent_objective=1,
        result_validity="invalid", qa_status="not_run")
    result = {"diagnostics":{"component_engine":"shared"},
              "hourly_system":[{"t":0,"period_minutes":60,"load_mw":5,
                                "generation_mw":5,"unserved_mwh":0,"curtailed_mwh":0}],
              "component_results":[{"asset_id":"w","component_kind":"wind","period":0,
                  "available_mw":5,"injection_mw":5,"withdrawal_mw":0,
                  "curtailed_mw":2,"curtailed_mwh":2,"cost_usd":0,"unit":"MW"}]}
    report = run_qa({}, result)
    assert report.status.value == "fail"
    assert next(check for check in report.checks
                if check.check_id == "component_resource_accounting").status.value == "fail"
    decision = evaluate_publication(diagnostics, report, extraction_completed=True,
                                    required_values_finite=True)
    assert not decision.publishable and "qa_failed" in decision.reasons
    finalized = finalized_diagnostics(diagnostics, report.status, decision.publishable)
    assert finalized.result_validity.value == "invalid"
