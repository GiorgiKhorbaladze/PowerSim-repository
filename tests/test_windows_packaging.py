"""Regression checks for the reproducible Windows distribution definition."""
from pathlib import Path

from powersim.desktop import _available_port


ROOT = Path(__file__).resolve().parents[1]


def test_windows_distribution_defines_installer_and_local_launcher():
    script = (ROOT / "packaging" / "windows" / "build.ps1").read_text(encoding="utf-8")
    installer = (ROOT / "packaging" / "windows" / "PowerSim.iss").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "windows-package.yml").read_text(encoding="utf-8")
    assert "PyInstaller" in script and "--collect-all solver" in script
    assert "PowerSim-" in installer and "Windows-x64-Setup" in installer
    assert "windows-latest" in workflow and "PowerSim.exe" in workflow
    assert _available_port("127.0.0.1") > 0
