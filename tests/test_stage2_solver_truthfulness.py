from __future__ import annotations
import math
import pytest

from powersim.contracts import QAReport, QACheckResult, QAStatus, SolverDiagnostics
from powersim.qa import ElectricityBalanceCheck, FiniteValuesCheck, run_qa
from powersim.results import evaluate_publication
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
