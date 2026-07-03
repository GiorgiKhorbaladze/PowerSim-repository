import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.powersim_ai_safety import is_safe_path, redact_secrets, reject_unsafe_request, requires_confirmation


def test_parent_path_rejected():
    assert not is_safe_path("../secret.txt", "/workspace/PowerSim-repository")


def test_absolute_unsafe_path_rejected():
    assert not is_safe_path("/etc/passwd", "/workspace/PowerSim-repository")


def test_api_key_pattern_redacted():
    assert "[REDACTED]" in redact_secrets("api_key=abc123SECRET")


def test_git_push_rejected():
    assert reject_unsafe_request("git push origin main")


def test_delete_repo_rejected():
    assert reject_unsafe_request("delete repository")


def test_run_solver_requires_confirmation():
    assert requires_confirmation("run_solver")
