"""Configuration and safety helpers for the PowerSim AI backend."""
from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = REPO_ROOT / ".runtime"
SESSIONS_ROOT = RUNTIME_ROOT / "sessions"
ALLOWED_INPUT_ROOTS = (RUNTIME_ROOT, REPO_ROOT / "project_data", REPO_ROOT / "out", REPO_ROOT / "samples", REPO_ROOT / "tests")
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("POWERSIM_AI_TOOL_TIMEOUT", "120"))


def openai_configured() -> bool:
    return bool(os.getenv(OPENAI_API_KEY_ENV))


def ensure_runtime_dirs() -> None:
    SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)


def safe_session_id(session_id: str) -> str:
    if not session_id or any(c in session_id for c in ("/", "\\", "..")):
        raise ValueError("invalid session_id")
    return session_id


def session_dir(session_id: str) -> Path:
    ensure_runtime_dirs()
    path = (SESSIONS_ROOT / safe_session_id(session_id)).resolve()
    if not str(path).startswith(str(SESSIONS_ROOT.resolve())):
        raise ValueError("session path escapes runtime root")
    path.mkdir(parents=True, exist_ok=True)
    (path / "logs").mkdir(exist_ok=True)
    return path


def resolve_allowed_path(path: str | Path, *, must_be_under: tuple[Path, ...] = ALLOWED_INPUT_ROOTS) -> Path:
    raw = Path(path)
    if ".." in raw.parts:
        raise ValueError("paths containing '..' are not allowed")
    candidate = raw if raw.is_absolute() else REPO_ROOT / raw
    resolved = candidate.resolve()
    roots = tuple(root.resolve() for root in must_be_under)
    if not any(resolved == root or str(resolved).startswith(str(root) + os.sep) for root in roots):
        raise ValueError("path is outside allowed PowerSim workspace")
    return resolved
