from __future__ import annotations
import math
import pytest

from powersim.contracts import QAReport, QACheckResult, QAStatus, SolverDiagnostics
from powersim.qa import ElectricityBalanceCheck, FiniteValuesCheck, run_qa
from powersim.qa.tolerance import DEFAULT_TOLERANCES
from powersim.results import evaluate_publication, finalized_diagnostics
from powersim.solvers import SolverConfigurationError, SolverUnavailableError, actual_mip_gap, get_backend, normalize_termination, solve_model


def diagnostics(**changes):
    values=dict(backend="highs",backend_version="test",termination_condition="optimal",normalized_status="optimal",
        incumbent_objective=1,best_bound=1,actual_mip_gap=0,requested_mip_gap=.25,runtime_s=.1,
        orchestration_runtime_s=.1,has_incumbent=True,result_validity="invalid",qa_status="not_run")
    values.update(changes); return SolverDiagnostics(**values)


@pytest.mark.parametrize(("raw","expected"),[("optimal","optimal"),("feasible","feasible"),("maxTimeLimit","time_limit"),
    ("infeasible","infeasible"),("unbounded","unbounded"),("numerical difficulty","numerical_error"),("unknown","solver_error")])
def test_status_mapping(raw,expected): assert normalize_termination(raw).value == expected


@pytest.mark.parametrize(("incumbent","bound","expected"),[(100,90,.1),(-100,-90,.1),(0,0,0),(1e-12,0,.01),(None,0,None),(1,None,None)])
def test_actual_gap_for_objective_senses_and_missing_values(incumbent,bound,expected):
    value=actual_mip_gap(incumbent,bound)
    assert value == pytest.approx(expected) if expected is not None else value is None


def test_option_validation_and_explicit_unsupported_backend():
    backend=get_backend("highs")
    for options in ({"mip_gap":-1},{"time_limit_s":0},{"threads":-1},{"mip_gaap":.1}):
        with pytest.raises(SolverConfigurationError): backend.normalize_options(options)
    with pytest.raises(SolverUnavailableError,match="unsupported"): get_backend("cplex")


def test_auto_selection_is_available_and_records_requested_backend():
    backend=get_backend("auto")
    assert backend.name in {"gurobi","highs"} and backend.available()


def test_highs_optimal_infeasible_unbounded_and_provenance():
    pyo=pytest.importorskip("pyomo.environ")
    if not get_backend("highs").available(): pytest.skip("HiGHS unavailable")
    model=pyo.ConcreteModel(); model.x=pyo.Var(domain=pyo.NonNegativeReals); model.obj=pyo.Objective(expr=model.x); model.c=pyo.Constraint(expr=model.x>=2)
    solved=solve_model(model,"highs",{"requested_mip_gap":.25,"time_limit_s":10})
    d=solved.diagnostics
    assert d.normalized_status.value == "optimal" and d.has_incumbent and pyo.value(model.x)==pytest.approx(2)
    assert d.actual_mip_gap == pytest.approx(0) and d.actual_mip_gap != d.requested_mip_gap
    assert d.backend == "highs" and d.backend_version and d.requested_backend == "highs" and d.effective_options
    infeasible=pyo.ConcreteModel(); infeasible.x=pyo.Var(); infeasible.o=pyo.Objective(expr=infeasible.x)
    infeasible.a=pyo.Constraint(expr=infeasible.x>=1); infeasible.b=pyo.Constraint(expr=infeasible.x<=0)
    bad=solve_model(infeasible,"highs").diagnostics
    assert bad.normalized_status.value == "infeasible" and not bad.has_incumbent and bad.incumbent_objective is None
    unbounded=pyo.ConcreteModel(); unbounded.x=pyo.Var(); unbounded.o=pyo.Objective(expr=-unbounded.x)
    ub=solve_model(unbounded,"highs").diagnostics
    assert ub.normalized_status.value in {"unbounded","solver_error"} and not ub.has_incumbent


def test_deterministic_extraction_runs_qa_and_finalizes_publication():
    from solver.powersim_solver import SolvedRows, build_asset_map, build_result_store, solve_all
    inp={"assets":[{"id":"g","type":"thermal","committable":False,"pmin":0,"pmax":10,"mc":1,"vom":0}],
         "profiles":{"demand":[5.0]},"study_horizon":{"horizon_hours":1},"reserve_products":[],
         "solver_settings":{"solver":"highs"}}
    assets=build_asset_map(inp)
    rows,elapsed,objective=solve_all(inp,assets,inp["profiles"],{})
    result=build_result_store(rows,assets,inp,elapsed,objective)
    assert result["qa"]["status"] == "pass"
    assert result["publication"]["publishable"] is True
    assert result["diagnostics"]["result_validity"] == "valid"
    assert result["diagnostics"]["qa_status"] == "pass"
    failed=diagnostics(normalized_status="infeasible",termination_condition="infeasible",has_incumbent=False,
        incumbent_objective=None,best_bound=None,actual_mip_gap=None)
    invalid_rows=SolvedRows(rows,solver_diagnostics=failed,extraction_completed=False)
    invalid=build_result_store(invalid_rows,assets,inp,elapsed,float("nan"))
    assert invalid["publication"]["publishable"] is False
    assert invalid["diagnostics"]["result_validity"] == "invalid"


def test_gurobi_conditionally():
    pyo=pytest.importorskip("pyomo.environ"); backend=__import__("powersim.solvers.gurobi",fromlist=["GurobiBackend"]).GurobiBackend()
    if not backend.available(): pytest.skip("Gurobi unavailable or unlicensed")
    model=pyo.ConcreteModel(); model.x=pyo.Var(bounds=(1,None)); model.o=pyo.Objective(expr=model.x)
    assert backend.solve(model).diagnostics.normalized_status.value == "optimal"


def test_qa_and_publication_gate():
    good={"hourly":[{"t":0,"load_mw":10.0,"generation_mw":10.0,"unserved_mwh":0.0,"curtailed_mwh":0.0}]}
    report=run_qa({},good)
    assert report.status == QAStatus.PASS
    assert evaluate_publication(diagnostics(),report,extraction_completed=True,required_values_finite=True).publishable
    bad={"hourly":[{"load_mw":10.0,"generation_mw":8.0,"unserved_mwh":0.0,"x":math.nan}]}
    report=run_qa({},bad)
    assert report.status == QAStatus.FAIL
    decision=evaluate_publication(diagnostics(),report,extraction_completed=True,required_values_finite=False)
    assert not decision.publishable and {"qa_failed","non_finite_required_values"} <= set(decision.reasons)


def test_no_incumbent_and_not_run_cannot_publish():
    d=diagnostics(normalized_status="time_limit",termination_condition="time limit",has_incumbent=False,
        incumbent_objective=None,best_bound=None,actual_mip_gap=None)
    decision=evaluate_publication(d,QAReport(status="not_run"),extraction_completed=False,required_values_finite=False)
    assert not decision.publishable and "no_incumbent" in decision.reasons


def test_time_limit_incumbent_is_qualified_only_after_qa():
    d=diagnostics(normalized_status="time_limit",termination_condition="time limit")
    qa=QAReport(status="pass",checks=[QACheckResult(check_id="finite_values",status="pass",message="ok")])
    decision=evaluate_publication(d,qa,extraction_completed=True,required_values_finite=True)
    final=finalized_diagnostics(d,qa.status,decision.publishable)
    assert decision.publishable and final.result_validity.value == "valid_with_warnings"
    assert final.qa_status.value == "pass"


@pytest.mark.parametrize("components", [
    {"bess":{"charge_mw":2.0}}, {"bess":{"discharge_mw":2.0}},
    {"pumped_hydro":{"net_mw":-3.0}}, {"imports_mw":4.0,"dr_mw":1.0},
])
def test_balance_uses_net_supply_without_component_double_counting(components):
    row={"load_mw":10.0,"generation_mw":10.0,"unserved_mwh":0.0,**components}
    check=ElectricityBalanceCheck().run({}, {"hourly":[row]}, DEFAULT_TOLERANCES)
    assert check.status == QAStatus.PASS


def test_persisted_rounding_tolerance_is_explicit_and_prevents_false_failure():
    data={"hourly_system":[{"load_mw":10.0,"generation_mw":9.9995,"unserved_mwh":0.0}]}
    internal=ElectricityBalanceCheck().run({},data,DEFAULT_TOLERANCES)
    persisted=ElectricityBalanceCheck().run({},data,DEFAULT_TOLERANCES,persisted=True)
    assert internal.status == QAStatus.FAIL
    assert persisted.status == QAStatus.PASS
    assert persisted.witness["tolerance_mode"] == "persisted"


def test_gurobi_warm_start_option_reaches_backend():
    from powersim.solvers.gurobi import GurobiBackend
    class FakeSolver:
        gurobi_options={}
    solver=FakeSolver()
    GurobiBackend().configure_solver(solver,{"warm_start":True})
    assert solver.gurobi_options["LPWarmStart"] == 2


def test_rolling_aggregate_failure_cannot_be_hidden_by_later_success():
    from solver.powersim_solver import _aggregate_solver_diagnostics
    failed=diagnostics(normalized_status="infeasible",termination_condition="infeasible",has_incumbent=False,
        incumbent_objective=None,best_bound=None,actual_mip_gap=None)
    succeeded=diagnostics()
    windows=[{"window":1,"start_period":0,"end_period":2,"diagnostics":failed.model_dump(mode="json")},
             {"window":2,"start_period":1,"end_period":3,"diagnostics":succeeded.model_dump(mode="json")}]
    aggregate=_aggregate_solver_diagnostics(windows,1.0)
    assert aggregate.normalized_status.value == "infeasible"
    assert not aggregate.has_incumbent and aggregate.result_validity.value == "invalid"
    assert aggregate.metadata["failed_window"] == 1


def test_solved_rows_keep_run_diagnostics_isolated():
    from solver.powersim_solver import SolvedRows
    first=SolvedRows([],solver_diagnostics=diagnostics(incumbent_objective=1))
    second=SolvedRows([],solver_diagnostics=diagnostics(incumbent_objective=2))
    assert first.solver_diagnostics.incumbent_objective == 1
    assert second.solver_diagnostics.incumbent_objective == 2


def test_no_incumbent_window_aborts_without_committing_placeholders(monkeypatch):
    import solver.powersim_solver as legacy
    failed=diagnostics(normalized_status="solver_error",termination_condition="error",has_incumbent=False,
        incumbent_objective=None,best_bound=None,actual_mip_gap=None)
    calls=[]
    def fake_window(*args,**kwargs):
        calls.append(1)
        placeholders=[{"load_mw":1.0,"generation_mw":0.0}]
        return legacy.SolvedRows(placeholders,solver_diagnostics=failed,extraction_completed=False),{},.1,float("nan")
    monkeypatch.setattr(legacy,"solve_window",fake_window)
    inp={"study_horizon":{"horizon_hours":3},"solver_settings":{"rolling_window_h":2,"rolling_step_h":1},
         "reserve_products":[],"assets":[]}
    rows,_,objective=legacy.solve_all(inp,{}, {"demand":[1.0,1.0,1.0]}, {})
    assert len(calls)==1 and rows == [] and math.isnan(objective)
    assert not rows.extraction_completed and len(rows.window_diagnostics)==1
    assert rows.solver_diagnostics.normalized_status.value == "solver_error"
