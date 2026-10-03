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
