"""Static checks of the PowerSim 1.0 browser application.

The UI is a thin client of the same-origin API. These checks pin that it is
valid JavaScript, talks only to ``/api``, contains no model logic, never
inserts untrusted HTML and exposes the controls the E2E journey relies on.
Browser-hosted E2E remains a separate release gate.
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

UI = Path(__file__).resolve().parents[1] / "src" / "powersim" / "ui" / "static"
SOURCE = (UI / "app.js").read_text(encoding="utf-8")
HTML = (UI / "index.html").read_text(encoding="utf-8")


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_ui_is_valid_javascript():
    completed = subprocess.run(["node", "--check", str(UI / "app.js")], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr


def test_ui_identity_and_no_legacy_assets():
    assert "<title>PowerSim 1.0</title>" in HTML
    assert 'src="/static/app.js"' in HTML
    assert "v4" not in HTML and "PowerSim_v4" not in SOURCE
    assert "<script>" not in HTML and " style=" not in HTML  # CSP: no inline script/style


def test_ui_uses_only_the_same_origin_api():
    assert {p.split("${")[0] for p in re.findall(r"fetch\(`([^`]*)`", SOURCE)} == {"/api", "/api/projects/"}
    external = SOURCE.replace("http://www.w3.org/2000/svg", "")
    assert "http://" not in external and "https://" not in external
    for path in ("/projects/validate", "/projects/save", "/reference/review", "/reference-import", "/summary",
                 "/launch", "/cancel", "/runs/compare", "/samples/", "/export", "/system"):
        assert path in SOURCE, path


def test_ui_contains_no_model_logic_or_html_injection():
    lowered = SOURCE.lower()
    for forbidden in ("powersim_solver", "solve_all", "build_asset_map", "innerhtml", "outerhtml", "insertadjacenthtml",
                      "document.write", "eval(", "new function("):
        assert forbidden not in lowered, forbidden


def test_ui_exposes_the_journey_controls():
    for control in ("project-status", "backend-status", "workspace-status", "validate-project", "save-project",
                    "export-project"):
        assert f'id="{control}"' in HTML, control
    for control in ("new-project-json", "create-project", "validate-new-project", "load-sample-", "project-workflow",
                    "assets-table", "asset-editor", "apply-asset", "mark-reviewed", "scenario-overlay", "add-scenario",
                    "validation-issues", "create-run", "create-run-reason", "runs-table", "run-detail",
                    "result-status", "result-qa", "result-publication", "results-withheld", "export-run",
                    "compare-left", "compare-right", "compare-runs", "reference-banner", "reference-search",
                    "stage-checkbox", "review-import", "rerun-review", "ack-warnings", "apply-import"):
        assert control in SOURCE, control
    for view in ("overview", "project", "assets", "scenarios", "validation", "runs", "results", "compare",
                 "reference", "about"):
        assert f'id="view-{view}"' in HTML and f'data-view="{view}"' in HTML


def test_ui_states_unsupported_capabilities_and_review_requirement():
    assert "requires user review" in SOURCE
    assert "Results withheld" in SOURCE
    assert "not official GSE operational data" in SOURCE
