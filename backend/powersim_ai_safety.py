"""Safety helpers for the embedded PowerSim AI assistant."""
from __future__ import annotations

import os
import re
from pathlib import Path

from backend.powersim_ai_tool_schemas import TOOL_SCHEMA_BY_NAME, REQUIRES_CONFIRMATION

UNSAFE_PATTERNS = [
    r"\b(delete|remove|rm\s+-rf|wipe)\b.*\b(repo|repository|all files|everything|ყველა ფაილი|რეპოზიტორ)",
    r"\b(api[_ -]?key|secret|token|credential|password|env|environment variables?)\b.*\b(print|show|expose|display|გამოაჩინე|მაჩვენე)",
    r"\b(print|show|expose|display)\b.*\b(api[_ -]?key|secret|token|credential|password|environment variables?)\b",
    r"\b(run|execute)\b.*\b(shell|bash|terminal|command)\b",
    r"\b(git\s+(push|merge|branch\s+-d|branch\s+-D|checkout|commit)|delete branch|merge pr)\b",
    r"\b(upload|send)\b.*\b(private gse|gse data|confidential)\b",
    r"\boverwrite\b.*\b(source files?|code)\b",
    r"\b(modify|edit|close|create)\b.*\b(github issues?|github prs?|pull requests?)\b",
]

SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_-]{12,}"),
    re.compile(r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,;]+"),
]


def is_safe_path(path: str, workspace_root: str) -> bool:
    """Return True only when path resolves inside workspace_root and has no traversal."""
    if not path or ".." in Path(path).parts:
        return False
    root = Path(workspace_root).resolve()
    candidate = Path(path)
    if candidate.is_absolute() and not str(candidate.resolve()).startswith(str(root) + os.sep):
        return False
    resolved = (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        resolved.relative_to(root)
        return True
    except ValueError:
        return False


def classify_action(tool_name: str) -> str:
    """Return safety classification for a registered tool name."""
    return TOOL_SCHEMA_BY_NAME.get(tool_name, {}).get("safety_classification", REQUIRES_CONFIRMATION)


def requires_confirmation(tool_name: str) -> bool:
    """Return whether a tool must be explicitly confirmed before execution."""
    return classify_action(tool_name) == REQUIRES_CONFIRMATION or tool_name.startswith("run_")


def reject_unsafe_request(message: str) -> bool:
    """Return True when message asks for prohibited operations."""
    text = (message or "").lower()
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in UNSAFE_PATTERNS)


def redact_secrets(text: str) -> str:
    """Redact likely secrets from free text."""
    redacted = text or ""
    for pattern in SECRET_PATTERNS:
        redacted = pattern.sub(lambda m: f"{m.group(1)}=[REDACTED]" if m.lastindex else "[REDACTED]", redacted)
    return redacted
