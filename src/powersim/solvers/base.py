"""Protocol and common implementation utilities for solver adapters."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from powersim.contracts import QAStatus, ResultValidity, SolverDiagnostics, SolverStatus


class SolverConfigurationError(ValueError):
    pass


class SolverUnavailableError(RuntimeError):
    pass


@dataclass(frozen=True)
class SolveOutcome:
    diagnostics: SolverDiagnostics
    raw_result: Any = None


class SolverBackend(Protocol):
    name: str
    def available(self) -> bool: ...
    def version(self) -> str | None: ...
    def normalize_options(self, options: dict[str, Any], *, compatibility: bool = False) -> dict[str, Any]: ...
    def solve(self, model: Any, options: dict[str, Any] | None = None, *, requested_backend: str | None = None) -> SolveOutcome: ...
    def cancel(self) -> bool: ...


def validate_common_options(options: dict[str, Any], supported: set[str], *, compatibility: bool = False) -> dict[str, Any]:
    aliases = {"mip_gap": "mip_gap", "requested_mip_gap": "mip_gap", "time_limit_s": "time_limit_s", "threads": "threads"}
    unknown = set(options) - supported - set(aliases)
    if unknown and not compatibility:
        raise SolverConfigurationError(f"unsupported solver option(s): {', '.join(sorted(unknown))}")
    normalized = {aliases.get(k, k): v for k, v in options.items() if compatibility or k in supported or k in aliases}
    if normalized.get("mip_gap") is not None and float(normalized["mip_gap"]) < 0:
        raise SolverConfigurationError("mip_gap must be >= 0")
    if normalized.get("time_limit_s") is not None and float(normalized["time_limit_s"]) <= 0:
        raise SolverConfigurationError("time_limit_s must be > 0")
    if normalized.get("threads") is not None and int(normalized["threads"]) < 0:
        raise SolverConfigurationError("threads must be >= 0")
    return normalized


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initial_validity(status: SolverStatus, has_incumbent: bool) -> ResultValidity:
    # Publication requires extraction and QA, so adapter output is never itself publishable.
    return ResultValidity.INVALID
