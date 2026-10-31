"""Monthly energy budgets, monthly capacity envelopes and fixed profiles.

Hand-checked 48-hour fixture spanning the January/February boundary
(study hours 720-767): 24 hours in January, 24 in February, constant demand
100 MW, so total demand is 4,800 MWh.

* ``hyd`` - hydro unit with a monthly energy budget of 1,200 MWh (January)
  and 600 MWh (February) and a forced minimum of 20 MW (January) and
  10 MW (February).
* ``imp`` - import at 50 USD/MWh, 200 MW.

When hydro is cheaper (VOM 0) it uses its full budget in both modes:
hydro 1,800 MWh, import 3,000 MWh, objective 3,000 x 50 = 150,000 USD.

When hydro is costlier (VOM 80):

* ``max``   - hydro runs only at its forced minimum:
  24 x 20 + 24 x 10 = 720 MWh; import 4,080 MWh;
  objective 720 x 80 + 4,080 x 50 = 57,600 + 204,000 = 261,600 USD.
* ``exact`` - hydro must deliver 1,800 MWh; import 3,000 MWh;
  objective 1,800 x 80 + 3,000 x 50 = 144,000 + 150,000 = 294,000 USD.
"""
from __future__ import annotations

import copy

import pytest

from powersim.contracts import ProjectContract, resolve_scenario
from powersim.execution import resolved_to_workflow_input, run_registered_workflow
from powersim.qa.envelope import OperatingEnvelopeCheck
from powersim.qa.tolerance import DEFAULT_TOLERANCES

JAN, FEB = 0, 1
JAN_HOURS, FEB_HOURS = range(720, 744), range(744, 768)  # canonical rows carry the study hour


def _monthly(jan, feb):
    values = [None] * 12
    values[JAN], values[FEB] = jan, feb
    return values


def _project(*, hydro_vom=0.0, mode="exact", rolling=False, extra_assets=(), profiles=(), hydro_extra=None):
    hydro = {"committable": False, "vom": hydro_vom, "availability_profile": "flat",
             "monthly_energy_mwh": _monthly(1200, 600), "monthly_energy_mode": mode,
             "monthly_pmin_mw": _monthly(20, 10)}
    hydro.update(hydro_extra or {})
    return ProjectContract.model_validate({
        "id": "envelope-fixture", "version": {"revision": 1}, "workflow": "deterministic_uc",
        "units": {"currency": "USD"},
        "time": {"timezone": "UTC", "study_year": 2026, "resolution_minutes": 60, "interval_duration_hours": 1,
                 "start": "2026-01-31T00:00:00Z", "periods": 48, "calendar_policy": "explicit_periods"},
        "assets": [
            {"id": "hyd", "kind": "hydro_ror", "capacity_min_mw": 0, "capacity_max_mw": 100,
             "profile_references": ["flat"], "legacy_extensions": hydro},
            {"id": "imp", "kind": "import", "capacity_max_mw": 200, "legacy_extensions": {"vom": 50}},
            *extra_assets,
        ],
        "profiles": [{"id": "demand", "unit": "MW", "values": [100] * 48},
                     {"id": "flat", "unit": "per-unit", "values": [1] * 48}, *profiles],
        "solver_settings": {"solver": "highs", "time_limit_s": 60, "rolling_window_h": 24 if rolling else 48,
                            "rolling_step_h": 24},
        "legacy_payload": {"resolution_min": 60, "study_horizon": {"start_hour": 720, "horizon_hours": 48}},
    })


def _solve(project):
    workflow = resolved_to_workflow_input(resolve_scenario(project))
    return run_registered_workflow(copy.deepcopy(workflow), "run", "sha256:x")


def _energy(envelope, asset_id, hours):
    rows = envelope.results["hourly_by_unit"][asset_id]
    return sum(row["dispatch_mw"] for row in rows if row["t"] in hours)


@pytest.mark.parametrize("mode", ["max", "exact"])
def test_cheap_hydro_uses_its_full_monthly_budget(mode):
    envelope = _solve(_project(hydro_vom=0, mode=mode))
    assert envelope.qa.status.value == "pass", envelope.qa.model_dump()
    assert _energy(envelope, "hyd", JAN_HOURS) == pytest.approx(1200, abs=0.05)
    assert _energy(envelope, "hyd", FEB_HOURS) == pytest.approx(600, abs=0.05)
    assert envelope.results["system_summary"]["total_objective_cost_usd"] == pytest.approx(150_000, abs=1)


def test_costly_hydro_with_max_budget_runs_only_at_its_forced_minimum():
    envelope = _solve(_project(hydro_vom=80, mode="max"))
    assert envelope.qa.status.value == "pass"
    rows = envelope.results["hourly_by_unit"]["hyd"]
    assert all(row["dispatch_mw"] == pytest.approx(20 if row["t"] < 744 else 10, abs=1e-6) for row in rows)
    assert envelope.results["system_summary"]["total_objective_cost_usd"] == pytest.approx(261_600, abs=1)


def test_costly_hydro_with_exact_budget_delivers_the_budget():
    envelope = _solve(_project(hydro_vom=80, mode="exact"))
    assert envelope.qa.status.value == "pass"
    assert _energy(envelope, "hyd", JAN_HOURS) == pytest.approx(1200, abs=0.05)
    assert _energy(envelope, "hyd", FEB_HOURS) == pytest.approx(600, abs=0.05)
    assert envelope.results["system_summary"]["total_objective_cost_usd"] == pytest.approx(294_000, abs=1)
    check = next(c for c in envelope.qa.checks if c.check_id == "operating_envelope")
    assert check.status.value == "pass" and check.checked_count >= 48 + 2


def test_monthly_pmax_caps_dispatch():
    envelope = _solve(_project(hydro_vom=0, mode="max", hydro_extra={"monthly_pmax_mw": _monthly(30, 100),
                                                                     "monthly_energy_mwh": None}))
    rows = envelope.results["hourly_by_unit"]["hyd"]
    assert max(r["dispatch_mw"] for r in rows if r["t"] < 744) == pytest.approx(30, abs=1e-6)
    assert min(r["dispatch_mw"] for r in rows if r["t"] >= 744) == pytest.approx(100, abs=1e-6)


def test_pmin_and_pmax_profiles_fix_an_hourly_output():
    """An expensive must-take import follows its hourly profile exactly."""
    fixed = [30 + (t % 5) for t in range(48)]
    project = _project(hydro_vom=0, mode="max", hydro_extra={"monthly_energy_mwh": None, "monthly_pmin_mw": None},
                       extra_assets=[{"id": "tpp", "kind": "import", "capacity_max_mw": 100,
                                      "legacy_extensions": {"vom": 500, "pmax_profile": "tpp_mw",
                                                            "pmin_profile": "tpp_mw"}}],
                       profiles=[{"id": "tpp_mw", "unit": "MW", "values": fixed}])
    envelope = _solve(project)
    assert envelope.qa.status.value == "pass"
    tpp = [r["dispatch_mw"] for r in envelope.results["hourly_by_unit"]["tpp"]]
    assert tpp == pytest.approx(fixed, abs=1e-6)


def test_monthly_energy_is_refused_in_a_rolling_solve():
    with pytest.raises(ValueError, match="full-horizon"):
        _solve(_project(rolling=True))


def test_malformed_monthly_series_is_rejected():
    with pytest.raises(ValueError, match="12 monthly values"):
        _solve(_project(hydro_extra={"monthly_pmin_mw": [1, 2, 3]}))
    with pytest.raises(ValueError, match="monthly_energy_mode"):
        _solve(_project(mode="roughly"))


def test_qa_detects_envelope_violations_independently():
    workflow = resolved_to_workflow_input(resolve_scenario(_project(hydro_vom=80, mode="exact")))
    good = [{"t": t, "dispatch_mw": 50.0 if t < 744 else 25.0, "commitment": 1} for t in range(720, 768)]
    check = OperatingEnvelopeCheck()
    assert check.run(workflow, {"hourly_by_unit": {"hyd": good}}, DEFAULT_TOLERANCES, persisted=True).status.value == "pass"
    short = [dict(row, dispatch_mw=row["dispatch_mw"] - 5) for row in good]  # Jan 1080 MWh, not 1200
    result = check.run(workflow, {"hourly_by_unit": {"hyd": short}}, DEFAULT_TOLERANCES, persisted=True)
    assert result.status.value == "fail"
    assert any(v["issue"] == "monthly energy (exact)" for v in result.witness["violations"])
    below = [dict(row, dispatch_mw=5.0) for row in good]
    result = check.run(workflow, {"hourly_by_unit": {"hyd": below}}, DEFAULT_TOLERANCES, persisted=True)
    assert any(v["issue"] == "below forced minimum" for v in result.witness["violations"])
    # regression: January rows must be judged against January's budget, not a later month's
    jan_only = [row for row in short if row["t"] < 744]
    result = check.run(workflow, {"hourly_by_unit": {"hyd": jan_only}}, DEFAULT_TOLERANCES, persisted=True)
    assert [v["month"] for v in result.witness["violations"] if "month" in v] == [1]
