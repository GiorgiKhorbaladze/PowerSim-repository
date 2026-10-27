"""Same-origin application-server assembly and serve smoke coverage."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

import pytest

from powersim.server import _default_static_dir, create_application

ROOT = Path(__file__).resolve().parents[1]


def test_same_origin_server_assembles_ui_and_api(tmp_path):
    app = create_application(tmp_path)
    paths = {route.path for route in app.routes}
    assert "/" in paths and "/api" in paths and "/static" in paths
    assert (_default_static_dir() / "index.html").is_file()
    # the archived v4 prototype is not part of the served application
    assert "html" not in _default_static_dir().parts[-2:]


def test_root_is_the_powersim_1_0_application(tmp_path):
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    client = TestClient(create_application(tmp_path))
    root = client.get("/")
    assert root.status_code == 200
    assert "<title>PowerSim 1.0</title>" in root.text
    assert "PowerSim v4" not in root.text and "GSE ER&A" not in root.text
    assert "default-src 'self'" in root.headers["content-security-policy"]
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/static/app.css").status_code == 200
    assert client.get("/PowerSim_v4.html").status_code == 404
    assert client.get("/api/system").json()["product"] == "PowerSim"


def _port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_powersim_serve_smoke(tmp_path):
    """``powersim serve --workspace <tmp>`` serves the 1.0 UI at the root URL."""
    port = _port()
    process = subprocess.Popen([sys.executable, "-m", "powersim", "serve", "--workspace", str(tmp_path / "ws"),
                                "--host", "127.0.0.1", "--port", str(port)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=tmp_path,
                               env={**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT / "src"), str(ROOT)])})
    try:
        deadline = time.monotonic() + 30
        body = None
        while time.monotonic() < deadline and body is None:
            assert process.poll() is None, "powersim serve exited"
            try:
                with urlopen(f"http://127.0.0.1:{port}/", timeout=1) as response:
                    body = response.read().decode("utf-8")
            except OSError:
                time.sleep(0.2)
        assert body is not None, "powersim serve did not become ready"
        assert "<title>PowerSim 1.0</title>" in body and "v4" not in body
        with urlopen(f"http://127.0.0.1:{port}/api/system", timeout=5) as response:
            assert b'"executor":{"configured":true' in response.read()
        assert (tmp_path / "ws" / "projects").is_dir()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
