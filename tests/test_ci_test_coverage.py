"""Prevent new pytest files from becoming invisible to release CI."""
from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ci_runs_the_complete_pytest_tree() -> None:
    workflow = (ROOT / ".github" / "workflows" / "python-checks.yml").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "python -m pytest -q tests" in workflow
    assert 'python_files = ["test_*.py"]' in pyproject


def test_browser_e2e_is_a_dedicated_required_job() -> None:
    workflow = (ROOT / ".github" / "workflows" / "python-checks.yml").read_text(encoding="utf-8")
    assert "browser-e2e:" in workflow
    assert "tests/test_browser_e2e.py" in workflow


def test_application_reference_and_ui_modules_are_selected_explicitly() -> None:
    workflow = (ROOT / ".github" / "workflows" / "python-checks.yml").read_text(encoding="utf-8")
    for module in ("test_application_api.py", "test_application_security.py", "test_run_manager.py",
                   "test_chronological_adequacy.py", "test_scoped_expansion.py", "test_server.py",
                   "test_ui_application_connector.py", "test_reference_catalogue.py", "test_reference_import.py",
                   "test_parameter_workbook.py", "test_variable_cost_single_count.py"):
        assert f"tests/{module}" in workflow, module
        assert (ROOT / "tests" / module).is_file(), module


def test_clean_install_runs_on_every_supported_python() -> None:
    workflow = (ROOT / ".github" / "workflows" / "python-checks.yml").read_text(encoding="utf-8")
    clean = workflow[workflow.index("  clean-install:"):workflow.index("  annual-release-acceptance:")]
    assert '["3.10", "3.11", "3.12"]' in clean and "tests/test_clean_install.py" in clean
