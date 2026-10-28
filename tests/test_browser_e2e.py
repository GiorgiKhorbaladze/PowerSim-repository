"""Browser acceptance of the PowerSim 1.0 application served by ``powersim serve``.

The test drives visible controls only (no direct API calls) through the full
user journey:

 1. root page is the PowerSim 1.0 application, not the archived v4 prototype
 2. supported workflows and unsupported capabilities are visible
 3. a typed project document with an invalid value shows structured diagnostics
 4. creating that invalid project is refused with the diagnostics
 5. the corrected document is created, saved and run-ready
 6. the sanitized Georgia demonstration loads (8,760 periods)
 7. an asset edit makes the project a draft and blocks run creation with a reason
 8. an invalid document edit is rejected on save; diagnostics show path,
    severity and remediation
 9. the corrected project saves successfully
10. a scenario is added and saved
11. a base-case run is launched and its lifecycle is observed to completion
12. QA, publication state and results are shown; the run bundle downloads
13. a second (scenario) run completes and the two runs are compared
14. reference data: a conflicting station is staged, blocked, resolved with
    audited overrides, acknowledged and imported as a disabled asset
15. enabling the unreviewed reference asset blocks runs with a visible reason;
    the Excel parameter workbook supplies its costs and review, making it run-ready;
    a run that the backend rejects is shown as failed with its cause; runs
    persist across a page reload; no console errors or failed requests occur

Playwright is opt-in locally (``POWERSIM_BROWSER_E2E=1``); CI enables it.
Set ``POWERSIM_CHROMIUM_EXECUTABLE`` to use a preinstalled Chromium.
"""
from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from urllib.request import urlopen

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("POWERSIM_BROWSER_E2E") != "1",
    reason="set POWERSIM_BROWSER_E2E=1 after installing Playwright Chromium",
)

RUN_TIMEOUT_MS = 300_000


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_server(url: str, process: subprocess.Popen) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("powersim serve exited before the browser could connect")
        try:
            with urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise AssertionError("powersim serve did not become ready within 60 seconds")


def _invalid_template() -> dict:
    return {
        "id": "e2e-compact", "version": {"revision": 1}, "workflow": "deterministic_uc",
        "metadata": {"name": "E2E compact"}, "units": {"currency": "USD"},
        "time": {"timezone": "UTC", "study_year": 2026, "resolution_minutes": 60, "interval_duration_hours": 1,
                 "start": "2026-01-01T00:00:00Z", "periods": 4, "calendar_policy": "explicit_periods"},
        "assets": [
            {"id": "thermal", "kind": "thermal", "capacity_min_mw": 50, "capacity_max_mw": 30,
             "legacy_extensions": {"committable": False, "heat_rate": 0, "fuel_price": 0, "vom": 10,
                                   "ramp_up": 30, "ramp_down": 30}},
            {"id": "import", "kind": "import", "capacity_max_mw": 30, "legacy_extensions": {"vom": 100}},
        ],
        "profiles": [{"id": "demand", "unit": "MW", "values": [20, 22, 25, 21]}],
        "legacy_payload": {"resolution_min": 60, "study_horizon": {"start_hour": 0, "horizon_hours": 4}},
    }


def _goto(page, view: str) -> None:
    page.locator(f".sidenav button[data-view='{view}']").click()
    page.locator(f"#h-{view}").wait_for()


def _wait_run(page, text: str, *, timeout: int = RUN_TIMEOUT_MS):
    row = page.locator("#runs-table tbody tr", has_text=text).first
    row.wait_for(timeout=timeout)
    return row


def test_full_browser_journey(tmp_path: Path) -> None:
    from playwright.sync_api import expect, sync_playwright

    port = _free_port()
    workspace = tmp_path / "workspace"
    log = (tmp_path / "serve.log").open("w")
    process = subprocess.Popen(
        [sys.executable, "-m", "powersim", "serve", "--workspace", str(workspace),
         "--host", "127.0.0.1", "--port", str(port)],
        stdout=log, stderr=subprocess.STDOUT, text=True,
    )
    url = f"http://127.0.0.1:{port}/"
    try:
        _wait_for_server(url, process)
        with sync_playwright() as playwright:
            executable = os.environ.get("POWERSIM_CHROMIUM_EXECUTABLE")
            browser = playwright.chromium.launch(executable_path=executable) if executable else playwright.chromium.launch()
            context = browser.new_context(accept_downloads=True, viewport={"width": 1400, "height": 900})
            page = context.new_page()
            page.set_default_timeout(30_000)
            expect.set_options(timeout=30_000)
            console_errors: list[str] = []
            failed_requests: list[str] = []
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
            page.on("pageerror", lambda error: console_errors.append(str(error)))
            page.on("requestfailed", lambda request: failed_requests.append(request.url))
            rejected: list[str] = []
            page.on("response", lambda response: rejected.append(response.url) if response.status >= 400 else None)
            page.on("dialog", lambda dialog: dialog.accept())

            # 1. the root URL is the PowerSim 1.0 application
            page.goto(url)
            page.locator("body[data-ready='true']").wait_for()
            assert page.title() == "PowerSim 1.0"
            expect(page.locator("#app-title")).to_have_text("PowerSim 1.0")
            assert "v4" not in page.content()
            expect(page.locator("#backend-status")).to_contain_text("Backend ready")
            expect(page.locator("#project-status")).to_have_text("No project open")

            # 2. workflows and boundaries
            overview = page.locator("#view-overview")
            for label in ("Deterministic UC/ED", "Stochastic UC", "N-1 security UC", "Chronological adequacy",
                          "Capacity-expansion screening"):
                expect(overview).to_contain_text(label)
            expect(overview).to_contain_text("AC power flow")
            # core navigation is keyboard operable
            page.locator(".sidenav button[data-view='about']").focus()
            page.keyboard.press("Enter")
            expect(page.locator("#h-about")).to_be_visible()
            expect(page.locator(".sidenav button[data-view='about']")).to_have_attribute("aria-current", "page")
            page.locator(".sidenav button[data-view='overview']").focus()
            page.keyboard.press("Space")
            page.locator("#h-overview").wait_for()

            # 3. structured diagnostics for an invalid typed document
            page.locator("#new-project-json").fill(json.dumps(_invalid_template()))
            page.locator("#validate-new-project").click()
            diagnostics = page.locator("#new-project-diagnostics")
            expect(diagnostics).to_contain_text("Validation failed")
            expect(diagnostics).to_contain_text("capacity_min_mw cannot exceed capacity_max_mw")
            expect(diagnostics).to_contain_text("assets.0")
            expect(diagnostics).to_contain_text("Correct the value at this path")

            # 4. creating it is refused
            page.locator("#create-project").click()
            expect(diagnostics).to_contain_text("The project was not created")

            # 5. corrected document is created and run-ready
            fixed = _invalid_template()
            fixed["assets"][0]["capacity_min_mw"] = 0
            page.locator("#new-project-json").fill(json.dumps(fixed))
            page.locator("#create-project").click()
            page.locator("#h-project").wait_for()
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready")

            # 6. Georgia demonstration
            _goto(page, "overview")
            page.locator("#load-sample-georgia_2026_baseline").click()
            page.locator("#h-project").wait_for()
            expect(page.locator("#h-project")).to_contain_text("Georgia 2026 Baseline")
            expect(page.locator("#view-project")).to_contain_text("8,760")
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready", timeout=60_000)

            # 7. editing an asset makes a draft and blocks run creation
            _goto(page, "assets")
            expect(page.locator("#assets-table")).to_contain_text("georgia_thermal_aggregate")
            page.locator("#edit-asset-0").click()
            page.locator("#asset-max").fill("1850")
            page.locator("#apply-asset").click()
            expect(page.locator("#project-status")).to_contain_text("Draft")
            expect(page.locator("#assets-table")).to_contain_text("1,850")
            _goto(page, "runs")
            expect(page.locator("#create-run")).to_be_disabled()
            expect(page.locator("#create-run-reason")).to_contain_text("Save your changes first")

            # 8. an invalid document edit is rejected on save with diagnostics
            _goto(page, "project")
            page.locator("#view-project details summary").click()
            document = page.locator("#project-document")
            expect(document).to_have_value(re.compile("georgia-2026-baseline"))
            project = json.loads(document.input_value())
            project["assets"][0]["profile_references"] = ["missing-profile"]
            document.fill(json.dumps(project))
            page.locator("#apply-document").click()
            page.locator("#save-project").click()
            page.locator("#h-validation").wait_for()
            issues = page.locator("#validation-issues")
            expect(issues).to_contain_text("dangling_profile_reference")
            expect(issues).to_contain_text("assets[0].profile_references")
            expect(issues).to_contain_text("Add the referenced profile or remove the reference.")
            expect(page.locator("#project-status")).to_have_text("Validation failed")

            # 9. corrected project saves
            project["assets"][0]["profile_references"] = []
            _goto(page, "project")
            page.locator("#project-document").fill(json.dumps(project))
            page.locator("#apply-document").click()
            page.locator("#save-project").click()
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready", timeout=60_000)

            # 10. scenarios
            _goto(page, "scenarios")
            expect(page.locator("#scenarios-table tbody tr")).to_have_count(4)
            page.locator("#scenario-id").fill("gap-check")
            page.locator("#scenario-name").fill("MIP gap check")
            page.locator("#scenario-overlay").fill('{"solver_settings": {"mip_gap": 0.01}}')
            page.locator("#add-scenario").click()
            expect(page.locator("#scenarios-table tbody tr")).to_have_count(5)
            page.locator("#save-project").click()
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready", timeout=60_000)

            # 11. base-case run lifecycle
            _goto(page, "runs")
            expect(page.locator("#create-run")).to_be_enabled()
            page.locator("#create-run").click()
            _wait_run(page, "completed")
            page.locator("#runs-table tbody tr", has_text="completed").first.locator(".view-run").click()
            timeline = page.locator("#run-detail .timeline")
            for status in ("queued", "solving", "validating_results", "completed"):
                expect(timeline).to_contain_text(status)

            # 12. QA-gated results and run export
            _goto(page, "results")
            expect(page.locator("#result-status")).to_contain_text("completed", timeout=60_000)
            expect(page.locator("#result-qa")).to_contain_text("QA pass")
            expect(page.locator("#result-publication")).to_contain_text("Publishable")
            expect(page.locator("#result-kpis")).to_contain_text("Energy served")
            expect(page.locator("#by-unit-table")).to_contain_text("Georgia thermal aggregate")
            if os.environ.get("POWERSIM_E2E_SCREENSHOTS"):
                page.screenshot(path=str(Path(os.environ["POWERSIM_E2E_SCREENSHOTS"]) / "georgia_results.png"), full_page=True)
            with page.expect_download() as download_info:
                page.locator("#export-run").click()
            bundle = tmp_path / "run.zip"
            download_info.value.save_as(bundle)
            with zipfile.ZipFile(bundle) as archive:
                names = set(archive.namelist())
                assert {"manifest.json", "run.json", "resolved_input.json", "result.json", "README.txt"} <= names
                envelope = json.loads(archive.read("result.json"))
                assert envelope["qa"]["status"] == "pass" and envelope["validity"] == "valid"
            with page.expect_download() as project_download:
                page.locator("#export-project").click()
            exported = json.loads(Path(project_download.value.path()).read_text(encoding="utf-8"))
            assert exported["id"] == "georgia-2026-baseline" and len(exported["scenarios"]) == 5

            # 13. second run (scenario) and comparison
            _goto(page, "runs")
            page.locator("#run-scenario").select_option("georgia-2027")
            page.locator("#create-run").click()
            row = page.locator("#runs-table tbody tr", has_text="georgia-2027").first
            expect(row).to_contain_text("completed", timeout=RUN_TIMEOUT_MS)
            _goto(page, "compare")
            page.locator("#compare-runs").click()
            result = page.locator("#compare-result")
            expect(result).to_contain_text("Compatible runs")
            expect(page.locator("#compare-table")).to_contain_text("georgia-2027")
            page.locator("#compare-right").select_option(page.locator("#compare-left").input_value())
            expect(page.locator("#compare-runs")).to_be_disabled()
            expect(page.locator("#compare-reason")).to_contain_text("Select two different runs")

            # 14. reference data staging, safeguards, overrides and import
            _goto(page, "reference")
            expect(page.locator("#reference-banner")).to_contain_text("requires user review")
            page.locator("#reference-search").fill("Gardabani TPP")
            expect(page.locator("#reference-count")).to_have_text("2 record(s)")
            page.locator("#reference-table input.stage-checkbox[value='stations:r140']").check()
            expect(page.locator("#staged-count")).to_contain_text("Gardabani TPP 1")
            page.locator("#review-import").click()
            card = page.locator(".review-card[data-record-id='stations:r140']")
            expect(card).to_contain_text("available_exceeds_installed")
            expect(card).to_contain_text("missing_fuel")
            expect(card).to_contain_text("blocking")
            expect(page.locator("#apply-import")).to_be_disabled()
            page.locator("#ov-stations_r140-capacity_max_mw").fill("231.2")
            page.locator("#ov-stations_r140-capacity_max_mw-reason").fill("Nameplate equals unit sum 2x73.6+84 MW")
            page.locator("#ov-stations_r140-fuel_type").fill("natural gas")
            page.locator("#ov-stations_r140-fuel_type-reason").fill("CCGT plant, operator public data")
            evidence = os.environ.get("POWERSIM_E2E_SCREENSHOTS")
            if evidence:
                page.screenshot(path=str(Path(evidence) / "reference_review_blocked.png"), full_page=True)
            page.locator("#rerun-review").click()
            card = page.locator(".review-card[data-record-id='stations:r140']")
            expect(card).to_contain_text("importable")
            expect(card).to_contain_text("resolved")
            expect(page.locator("#apply-import")).to_be_disabled()
            expect(page.locator("#apply-import-reason")).to_contain_text("Acknowledge the warnings")
            page.locator("#ack-warnings").check()
            page.locator("#import-reviewer").fill("E2E reviewer")
            page.locator("#apply-import").click()
            expect(page.locator("#import-result")).to_contain_text("Imported 1 asset(s)")
            _goto(page, "assets")
            imported = page.locator("#assets-table tbody tr", has_text="ref-gardabani-tpp-1-ccpp-1")
            expect(imported).to_contain_text("disabled")
            expect(imported).to_contain_text("requires review")

            # 15a. enabling an unreviewed reference asset blocks runs with a reason
            imported.locator("button").click()
            expect(page.locator("#asset-editor")).to_contain_text("Reference values require user review")
            expect(page.locator("#asset-editor")).to_contain_text("Stations by Location / row 140")
            page.locator("#asset-enabled").check()
            page.locator("#apply-asset").click()
            page.locator("#save-project").click()
            expect(page.locator("#project-status")).to_have_text("Saved - not run-ready", timeout=60_000)
            _goto(page, "runs")
            expect(page.locator("#create-run")).to_be_disabled()
            expect(page.locator("#create-run-reason")).to_contain_text("blocking validation errors")
            _goto(page, "validation")
            expect(page.locator("#validation-issues")).to_contain_text("reference_asset_unreviewed")
            expect(page.locator("#validation-issues")).to_contain_text("missing_cost_basis")
            if os.environ.get("POWERSIM_E2E_SCREENSHOTS"):
                page.screenshot(path=str(Path(os.environ["POWERSIM_E2E_SCREENSHOTS"]) / "unreviewed_asset_blocks_run.png"), full_page=True)

            # 15a'. complete and review it in bulk through the Excel parameter workbook
            import openpyxl
            _goto(page, "assets")
            with page.expect_download() as template_download:
                page.locator("#download-workbook").click()
            assert template_download.value.suggested_filename == "georgia-2026-baseline-parameters.xlsx"
            template = tmp_path / "template.xlsx"
            template_download.value.save_as(template)
            workbook = openpyxl.load_workbook(template)
            sheet = workbook["Assets"]
            header = [cell.value for cell in sheet[1]]
            for row in sheet.iter_rows(min_row=3):
                if row[0].value == "ref-gardabani-tpp-1-ccpp-1":
                    for column, value in {"heat_rate_gj_per_mwh": 7.2, "fuel_price_usd_per_mmbtu": 6, "vom_per_mwh": 3,
                                          "reviewed": "yes", "review_note": "E2E: checked against approved register"}.items():
                        row[header.index(column)].value = value
            filled = tmp_path / "filled-parameters.xlsx"
            workbook.save(filled)
            page.locator("#workbook-file").set_input_files(str(filled))
            page.locator("#check-workbook").click()
            expect(page.locator("#workbook-result")).to_contain_text("ready to apply")
            expect(page.locator("#workbook-changes")).to_contain_text("heat_rate_gj_per_mwh")
            page.locator("#apply-workbook").click()
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready", timeout=60_000)
            imported = page.locator("#assets-table tbody tr", has_text="ref-gardabani-tpp-1-ccpp-1")
            expect(imported).to_contain_text("reviewed")
            expect(imported).to_contain_text("enabled")

            # 15b. a run the backend rejects is visibly failed with its cause
            _goto(page, "overview")
            page.locator("#project-select").select_option("e2e-compact")
            page.locator("#open-project").click()
            page.locator("#h-project").wait_for()
            page.locator("#project-workflow").select_option("stochastic_uc")
            page.locator("#save-project").click()
            expect(page.locator("#project-status")).to_have_text("Saved - valid, run-ready")
            _goto(page, "runs")
            page.locator("#create-run").click()
            failed = _wait_run(page, "failed", timeout=120_000)
            failed.get_by_role("button", name="Results").click()
            expect(page.locator("#result-status")).to_contain_text("failed")
            expect(page.locator("#results-panel")).to_contain_text("stochastic scenarios must be a non-empty list")
            expect(page.locator("#result-publication")).to_contain_text("No result")

            # 15c. persistence across reload, and a clean console
            page.reload()
            page.locator("body[data-ready='true']").wait_for()
            _goto(page, "runs")
            expect(page.locator("#runs-table tbody tr", has_text="completed")).to_have_count(2)
            expect(page.locator("#runs-table tbody tr", has_text="failed")).to_have_count(1)
            # Exactly the two deliberate rejections (steps 4 and 8) may log a
            # browser resource error; anything else is a defect.
            assert sorted(u.split("/api/")[-1] for u in rejected) == ["projects/save", "projects/save"], rejected
            unexpected = [m for m in console_errors if "status of 422" not in m]
            assert not unexpected, unexpected
            assert len(console_errors) == len(rejected), console_errors
            assert not failed_requests, failed_requests
            browser.close()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        log.close()
