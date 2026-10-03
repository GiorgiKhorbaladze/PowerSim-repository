"""Browser-hosted acceptance of the same-origin PowerSim application.

The test deliberately clicks the visible backend demonstration control.  It
therefore covers browser -> API -> RunManager -> local solver -> canonical QA
-> persisted envelope -> browser without using the page's JavaScript API as a
test shortcut.  It is opt-in locally because Chromium is intentionally not a
runtime dependency of PowerSim; release CI enables it.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("POWERSIM_BROWSER_E2E") != "1",
    reason="set POWERSIM_BROWSER_E2E=1 after installing Playwright Chromium",
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("powersim serve exited before the browser could connect")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise AssertionError("powersim serve did not become ready within 30 seconds")


def test_visible_browser_run_round_trip(tmp_path: Path) -> None:
    """A real Chromium browser runs the compact UI study and sees its result."""
    from playwright.sync_api import sync_playwright

    port = _free_port()
    workspace = tmp_path / "workspace"
    process = subprocess.Popen(
        [sys.executable, "-m", "powersim", "serve", "--workspace", str(workspace),
         "--host", "127.0.0.1", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    url = f"http://127.0.0.1:{port}/"
    try:
        _wait_for_server(url, process)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page()
            console_errors: list[str] = []
            failed_requests: list[str] = []
            launch_statuses: list[int] = []
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("requestfailed", lambda request: failed_requests.append(request.url))
            page.on("response", lambda response: launch_statuses.append(response.status)
                    if response.url.endswith("/launch") else None)
            page.goto(url, wait_until="networkidle")
            demo = page.locator("#powersim-backend-demo")
            demo.wait_for(state="attached")
            demo.click(force=True)
            page.locator("#powersim-backend-status").wait_for(state="visible")
            page.locator("#powersim-backend-status").wait_for(
                state="visible", timeout=30_000
            )
            page.wait_for_function(
                "(() => { const text=document.getElementById('powersim-backend-status')?.textContent || ''; return text.includes('QA:') || text.startsWith('შეცდომა:'); })()",
                timeout=30_000,
            )
            visible_status = page.locator("#powersim-backend-status").inner_text()
            assert "QA: pass" in visible_status, {
                "visible_status": visible_status,
                "console_errors": console_errors,
                "failed_requests": failed_requests,
            }
            assert page.locator("#pane-results").evaluate("element => element.classList.contains('active')")
            assert 200 in launch_statuses
            assert not console_errors, console_errors
            assert not failed_requests, failed_requests
            browser.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
