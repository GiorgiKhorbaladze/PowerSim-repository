"""Production executor regression coverage."""
from powersim.execution import run_deterministic_workflow
from tests.test_stage4c_mixed_fleet import _input


def test_real_deterministic_executor_uses_gated_solver_output():
    result = run_deterministic_workflow(_input(resolution_min=60, rolling=False), "run-1", "snapshot-1")
    assert result.run_id == "run-1"
    assert result.snapshot_fingerprint == "snapshot-1"
    assert result.qa.status.value == "pass"
    assert result.validity.value == "valid"
    assert result.results["publication"]["publishable"] is True
