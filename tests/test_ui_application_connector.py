"""Static UI/API contract checks.

The production UI is a static browser asset, so this test deliberately checks
the loaded connector rather than duplicating application or solver behaviour.
Browser-hosted E2E remains a separate release gate.
"""
from pathlib import Path
import subprocess
import json

from powersim.contracts import ProjectContract


ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "html" / "PowerSim_v4.html"
CONNECTOR = ROOT / "html" / "powersim_ror_autobind_2026.js"


def test_ui_loads_solver_free_application_api_connector():
    html = HTML.read_text(encoding="utf-8")
    source = CONNECTOR.read_text(encoding="utf-8")

    assert '<script src="powersim_ror_autobind_2026.js"></script>' in html
    assert "window.PowerSimApplicationAPI" in source
    for operation in ("saveProject", "createRun", "launch", "status", "result", "compare", "projectFromLegacyPayload", "submitCurrentUiProject"):
        assert operation in source
    assert "powersim_solver" not in source
    assert "pyomo" not in source.lower()


def test_ui_application_connector_is_valid_javascript():
    completed = subprocess.run(
        ["node", "--check", str(CONNECTOR)], capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr


def test_connector_adapts_static_editor_payload_to_typed_project_contract():
    script = """
global.window={}; global.localStorage={getItem:()=>null,setItem:()=>{}};
global.document={addEventListener:()=>{}};
require(process.argv[1]);
const project=window.PowerSimApplicationAPI.projectFromLegacyPayload({
  metadata:{study_year:2026,timezone:'Asia/Tbilisi'}, resolution_min:15,
  study_horizon:{horizon_hours:1}, profiles:{demand:[10,11,12,13]},
  assets:[{id:'t1',type:'thermal',pmin:2,pmax:20}], reserve_products:[]
},'ui-test');
console.log(JSON.stringify(project));
"""
    completed = subprocess.run(
        ["node", "-e", script, str(CONNECTOR)], capture_output=True, text=True, check=False
    )
    assert completed.returncode == 0, completed.stderr
    project = ProjectContract.model_validate(json.loads(completed.stdout))
    assert project.id == "ui-test"
    assert project.time.resolution_minutes == 15
    assert project.assets[0].capacity_max_mw == 20
