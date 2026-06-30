"""Curated PyPSA-Eur technology cost / efficiency database.

A snapshot of PowerSim-relevant rows from
https://github.com/PyPSA/technology-data is committed at
``data/cost_database_pypsa_eur.json``. This module provides a
lightweight loader, lookup helper, and a small unit-conversion utility
so callers can pull validated CAPEX / OPEX / efficiency / lifetime
numbers when configuring assets, instead of guessing.

The snapshot is intentionally small (~120 KiB) — refresh it by
re-running ``scripts/refresh_cost_database.py`` (online; pulls from the
PyPSA technology-data master). Original data license: CC-BY 4.0
(PyPSA technology-data) — see ``data/cost_database_pypsa_eur.json.meta``.

Public API
----------
    load() -> dict                                  full snapshot
    available_years() -> list[int]
    available_technologies(year) -> list[str]
    get(tech, year, param=None) -> dict | float
    annuity(investment, lifetime, discount_rate=0.07) -> float
    annualized_capex_usd_per_mw_yr(
        tech, year, *, eur_to_usd=1.08, discount_rate=0.07
    ) -> float                                       handy for BESS sizing etc.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional


_SNAPSHOT_PATH = Path(__file__).resolve().parent.parent / "data" / "cost_database_pypsa_eur.json"


class CostDatabaseError(KeyError):
    """Raised when a (technology, year, parameter) lookup fails."""


@lru_cache(maxsize=1)
def load() -> dict:
    """Return the full snapshot dict. Cached after first read."""
    if not _SNAPSHOT_PATH.exists():
        raise FileNotFoundError(
            f"cost database snapshot missing at {_SNAPSHOT_PATH}; "
            f"run scripts/refresh_cost_database.py to rebuild")
    return json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))


def available_years() -> list[int]:
    return sorted(int(y) for y in load()["costs_by_year"].keys())


def available_technologies(year: int) -> list[str]:
    yd = load()["costs_by_year"].get(str(year), {})
    return sorted(yd.keys())


def get(tech: str, year: int, param: Optional[str] = None):
    """Look up a single technology / year (and optionally a single
    parameter). Returns the full params dict by default, or the numeric
    value (units stripped) when ``param`` is given.

    Raises ``CostDatabaseError`` on missing keys with a helpful message
    listing the closest matches.
    """
    db = load()["costs_by_year"]
    if str(year) not in db:
        raise CostDatabaseError(
            f"year {year} not in snapshot; available: {available_years()}")
    yd = db[str(year)]
    if tech not in yd:
        # Suggest fuzzy matches
        suggestions = [t for t in yd.keys()
                       if tech.lower() in t.lower() or t.lower() in tech.lower()]
        msg = f"technology {tech!r} not in snapshot for year {year}"
        if suggestions:
            msg += f"; did you mean one of: {suggestions[:6]}"
        raise CostDatabaseError(msg)
    params = yd[tech]
    if param is None:
        return params
    if param not in params:
        raise CostDatabaseError(
            f"parameter {param!r} not set for {tech!r} year {year}; "
            f"available: {sorted(params.keys())}")
    return float(params[param]["value"])


def annuity(investment: float, lifetime: float, discount_rate: float = 0.07) -> float:
    """Capital recovery factor × investment.

    ``annuity = inv × r / (1 - (1+r)^-n)`` — annualized capital cost
    over ``lifetime`` years at ``discount_rate``. Returns 0 if lifetime
    or investment is non-positive.
    """
    if investment <= 0 or lifetime <= 0:
        return 0.0
    r = float(discount_rate)
    n = float(lifetime)
    if r <= 0:
        return float(investment) / n
    factor = r / (1.0 - (1.0 + r) ** -n)
    return float(investment) * factor


def annualized_capex_usd_per_mw_yr(
    tech: str, year: int,
    *, eur_to_usd: float = 1.08, discount_rate: float = 0.07,
) -> float:
    """Return annualized CAPEX in USD per MW per year for a technology.

    The snapshot stores investment in EUR/kW (Danish Energy Agency
    convention used by PyPSA-Eur). Converts to USD/MW (× 1000 × FX) and
    applies a CRF over the technology's lifetime. ``eur_to_usd`` is the
    nominal FX rate (default 1.08 ≈ 2025 average).
    """
    inv_eur_per_kw = float(get(tech, year, "investment"))
    life_yrs      = float(get(tech, year, "lifetime"))
    inv_usd_per_mw = inv_eur_per_kw * 1000.0 * float(eur_to_usd)
    return annuity(inv_usd_per_mw, life_yrs, discount_rate)
