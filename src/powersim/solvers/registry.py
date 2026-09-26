"""Solver registry and deterministic backend selection."""

from .base import SolverUnavailableError
from .gurobi import GurobiBackend
from .highs import HighsBackend

AUTO_PRIORITY = ("gurobi", "highs")
_BACKENDS = {"highs": HighsBackend, "gurobi": GurobiBackend}


def get_backend(requested: str):
    name = requested.strip().lower()
    if name == "auto":
        for candidate in AUTO_PRIORITY:
            backend = _BACKENDS[candidate]()
            if backend.available():
                return backend
        raise SolverUnavailableError("no supported solver is available (tried gurobi, highs)")
    if name not in _BACKENDS:
        raise SolverUnavailableError(f"unsupported solver backend {requested!r}; supported: highs, gurobi")
    backend = _BACKENDS[name]()
    if not backend.available():
        raise SolverUnavailableError(f"requested solver backend {name!r} is unavailable")
    return backend


def solve_model(model, requested: str = "auto", options=None):
    return get_backend(requested).solve(model, options or {}, requested_backend=requested)
