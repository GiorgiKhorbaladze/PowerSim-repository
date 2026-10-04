"""Browser-hosted acceptance of the same-origin queued PowerSim application.

The test clicks visible controls only. It covers browser to API to RunManager to
the local solver to canonical QA to persisted envelopes and back to the UI.
Playwright is opt-in locally; release CI enables it.
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


def _wait_for_valid_result(page) -> str:
    page.wait_for_function(
        "(() => { const text=document.getElementById('powersim-backend-status')?.textContent || ''; return text.includes('QA:') || text.startsWith('შეცდომა:'); })()",
        timeout=30_000,
    )
    visible_status = page.locator("#powersim-backend-status").inner_text()
    assert "QA: pass" in visible_status, visible_status
    return visible_status


def test_visible_browser_run_round_trip_and_compare(tmp_path: Path) -> None:
    """The browser performs two real runs, compares them and displays failure honestly."""
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
            comparison_requests: list[int] = []
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("requestfailed", lambda request: failed_requests.append(request.url))
            page.on("response", lambda response: launch_statuses.append(response.status)
                    if response.url.endswith("/launch") else None)
            page.on("response", lambda response: comparison_requests.append(response.status)
                    if "/runs/compare?" in response.url else None)
            page.goto(url, wait_until="networkidle")
            assert page.locator("#powersim-workflow-select").is_visible()
            assert page.locator("#powersim-scenario-select").is_visible()
            demo = page.locator("#powersim-backend-demo")
            demo.wait_for(state="attached")
            demo.click(force=True)
            _wait_for_valid_result(page)
            page.locator("button[data-tab='workflow']").click()
            demo.click(force=True)
            _wait_for_valid_result(page)
            page.locator("button[data-tab='workflow']").click()

            history = page.locator("#powersim-run-history").inner_text()
            assert history.count("deterministic_uc") == 2
            left = page.locator("#powersim-compare-left")
            right = page.locator("#powersim-compare-right")
            assert left.locator("option").count() == 2
            assert right.locator("option").count() == 2
            assert left.input_value() != right.input_value()
            page.locator("#powersim-compare-runs").click()
            page.wait_for_function(
                "document.getElementById('powersim-compare-result')?.textContent.includes('Compare completed')",
                timeout=30_000,
            )
            assert "Compare completed" in page.locator("#powersim-compare-result").inner_text()

            # A deliberate invalid UI action must be visible as an error, not as a result.
            right.select_option(left.input_value())
            assert right.input_value() == left.input_value()
            page.wait_for_function(
                "document.getElementById('powersim-compare-result')?.textContent.includes('Compare failed: Select two different runs to compare')",
                timeout=10_000,
            )
            page.locator("#powersim-compare-runs").click()
            page.wait_for_timeout(250)
            assert comparison_requests == [200]
            assert "Compare failed: Select two different runs to compare" in page.locator("#powersim-compare-result").inner_text()
            assert page.locator("#pane-results").evaluate("element => element.classList.contains('active')")
            assert launch_statuses.count(200) >= 2
            unexpected_console_errors = [message for message in console_errors if "/api/ai/health" not in message]
            unexpected_failed_requests = [request for request in failed_requests if "/api/ai/health" not in request]
            assert not unexpected_console_errors, unexpected_console_errors
            assert not unexpected_failed_requests, unexpected_failed_requests
            browser.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
