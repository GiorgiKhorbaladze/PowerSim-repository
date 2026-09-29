"""Static UI/API contract checks.

The production UI is a static browser asset, so this test deliberately checks
the loaded connector rather than duplicating application or solver behaviour.
Browser-hosted E2E remains a separate release gate.
"""
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "html" / "PowerSim_v4.html"
CONNECTOR = ROOT / "html" / "powersim_ror_autobind_2026.js"


def test_ui_loads_solver_free_application_api_connector():
    html = HTML.read_text(encoding="utf-8")
    source = CONNECTOR.read_text(encoding="utf-8")

    assert '<script src="powersim_ror_autobind_2026.js"></script>' in html
    assert "window.PowerSimApplicationAPI" in source
    for operation in ("saveProject", "createRun", "launch", "status", "result", "compare"):
        assert operation in source
    assert "powersim_solver" not in source
    assert "pyomo" not in source.lower()


def test_ui_application_connector_is_valid_javascript():
    completed = subprocess.run(
        ["node", "--check", str(CONNECTOR)], capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
