"""Artifact-only installation acceptance.

This is opt-in because it builds distributions and creates an isolated virtual
environment.  Release CI enables it; the normal matrix still collects it so
the test cannot become orphaned.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import urlopen

import pytest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    os.environ.get("POWERSIM_CLEAN_INSTALL") != "1",
    reason="release clean-install job only",
)


def _port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait(url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"installed powersim serve exited: {process.stderr.read()}")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise AssertionError("installed powersim serve did not become ready")


def test_built_artifacts_install_and_run_outside_checkout(tmp_path: Path) -> None:
    """Wheel/sdist, CLI, server assets and a real compact solve all work."""
    dist = tmp_path / "dist"
    subprocess.run([sys.executable, "-m", "build", "--outdir", str(dist)], cwd=ROOT, check=True)
    wheels = list(dist.glob("powersim-*.whl"))
    sdists = list(dist.glob("powersim-*.tar.gz"))
    assert len(wheels) == 1 and len(sdists) == 1

    venv = tmp_path / "installed"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    command = venv / ("Scripts/powersim.exe" if os.name == "nt" else "bin/powersim")
    subprocess.run([str(python), "-m", "pip", "install", str(wheels[0])], check=True)
    version = subprocess.run([str(command), "--version"], capture_output=True, text=True, check=True)
    assert "PowerSim" in version.stdout

    project = {
        "id": "clean-install-demo", "version": {"revision": 1}, "workflow": "deterministic_uc",
        "units": {"currency": "USD"},
        "time": {"timezone": "UTC", "study_year": 2026, "resolution_minutes": 60,
                 "interval_duration_hours": 1, "start": "2026-01-01T00:00:00Z",
                 "periods": 4, "calendar_policy": "explicit_periods"},
        "assets": [
            {"id": "thermal", "kind": "thermal", "capacity_min_mw": 0, "capacity_max_mw": 30,
             "legacy_extensions": {"id": "thermal", "type": "thermal", "pmin": 0, "pmax": 30,
                                   "committable": False, "heat_rate": 0, "fuel_price": 0, "vom": 10,
                                   "ramp_up": 30, "ramp_down": 30}},
            {"id": "import", "kind": "import", "capacity_max_mw": 30,
             "legacy_extensions": {"id": "import", "type": "import", "pmax": 30, "vom": 100}},
        ],
        "profiles": [{"id": "demand", "unit": "MW", "values": [20, 20, 20, 20]}],
        "legacy_payload": {"resolution_min": 60, "profiles": {"demand": [20, 20, 20, 20]},
            "study_horizon": {"start_hour": 0, "horizon_hours": 4},
            "assets": [{"id": "thermal", "type": "thermal", "pmin": 0, "pmax": 30,
                        "committable": False, "heat_rate": 0, "fuel_price": 0, "vom": 10,
                        "ramp_up": 30, "ramp_down": 30},
                       {"id": "import", "type": "import", "pmax": 30, "vom": 100}],
            "solver_settings": {"solver": "highs", "time_limit_s": 30}},
    }
    project_path = tmp_path / "project.json"
    project_path.write_text(json.dumps(project), encoding="utf-8")
    subprocess.run([str(command), "validate", str(project_path)], cwd=tmp_path, check=True)

    run_script = tmp_path / "run.py"
    run_script.write_text("""
import json, sys, time
from powersim.application import ApplicationService
from powersim.contracts import ProjectContract
from powersim.execution import RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager
project = ProjectContract.model_validate(json.load(open(sys.argv[1])))
manager = RunManager(sys.argv[2]); manager.save_project(project)
service = ApplicationService(manager, RegisteredLocalWorkflowExecutor())
run = service.create_run(project.id); assert service.launch(run['id'])['accepted']
deadline = time.monotonic() + 30
while service.run_status(run['id'])['status'] not in {'completed', 'failed'}:
    assert time.monotonic() < deadline; time.sleep(.02)
assert service.run_status(run['id'])['status'] == 'completed'
result = service.result(run['id']); assert result['qa']['status'] == 'pass' and result['validity'] == 'valid'
""", encoding="utf-8")
    subprocess.run([str(python), str(run_script), str(project_path), str(tmp_path / "workspace")], cwd=tmp_path, check=True)

    port = _port()
    server = subprocess.Popen([str(command), "serve", "--workspace", str(tmp_path / "serve-workspace"),
                               "--host", "127.0.0.1", "--port", str(port)], cwd=tmp_path,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        _wait(f"http://127.0.0.1:{port}/", server)
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=10)
