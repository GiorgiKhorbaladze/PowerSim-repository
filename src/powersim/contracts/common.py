"""Shared units, time, provenance, and deterministic serialization contracts."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator


class FrozenDict(dict):
    """A JSON-compatible dictionary whose complete value graph is immutable."""

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: Any):
        from pydantic_core import core_schema
        return core_schema.no_info_after_validator_function(cls.from_mapping, handler(dict[str, Any]))

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "FrozenDict":
        frozen = dict.__new__(cls)
        dict.update(frozen, {key: freeze_value(item) for key, item in value.items()})
        return frozen

    def _immutable(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("published contract data is immutable")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _immutable

    def __copy__(self) -> "FrozenDict":
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> "FrozenDict":
        return self


def freeze_value(value: Any) -> Any:
    if isinstance(value, FrozenDict):
        return value
    if isinstance(value, dict):
        frozen = dict.__new__(FrozenDict)
        dict.update(frozen, {key: freeze_value(item) for key, item in value.items()})
        return frozen
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(freeze_value(item) for item in value)
    return value


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False, frozen=True)

    def canonical_json(self) -> str:
        import json
        return json.dumps(self.model_dump(mode="json", exclude_none=False, by_alias=True),
                          sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def fingerprint(self) -> str:
        import json
        payload = json.dumps(self.model_dump(mode="json", exclude_none=False), sort_keys=True,
                             separators=(",", ":"), ensure_ascii=False)
        return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


class Unit(str, Enum):
    MW = "MW"
    MWH = "MWh"
    HOURS = "hours"
    MINUTES = "minutes"
    CURRENCY = "currency"


class FlowUnitMetadata(ContractModel):
    volume_unit: str = Field(min_length=1)
    rate_unit: str = Field(min_length=1)


class UnitSystem(ContractModel):
    power: Literal["MW"] = "MW"
    energy: Literal["MWh"] = "MWh"
    duration: Literal["hours"] = "hours"
    resolution: Literal["minutes"] = "minutes"
    currency: str = Field(min_length=1)
    water: FlowUnitMetadata | None = None
    gas: FlowUnitMetadata | None = None


class CalendarPolicy(str, Enum):
    NON_LEAP = "non_leap"
    EXPLICIT_PERIODS = "explicit_periods"


class TimeContract(ContractModel):
    timezone: str
    study_year: int = Field(ge=1900, le=9999)
    resolution_minutes: int = Field(gt=0, le=1440)
    interval_duration_hours: float = Field(gt=0)
    start: datetime
    periods: int = Field(gt=0)
    calendar_policy: CalendarPolicy

    @model_validator(mode="after")
    def validate_time(self) -> "TimeContract":
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone: {self.timezone}") from exc
        if abs(self.interval_duration_hours - self.resolution_minutes / 60) > 1e-12:
            raise ValueError("interval_duration_hours must equal resolution_minutes / 60")
        leap = self.study_year % 4 == 0 and (self.study_year % 100 != 0 or self.study_year % 400 == 0)
        if self.calendar_policy == CalendarPolicy.NON_LEAP:
            if leap:
                raise ValueError("non_leap calendar_policy does not support leap study years")
            year_minutes = 365 * 24 * 60
            if year_minutes % self.resolution_minutes:
                raise ValueError("resolution_minutes must divide a non-leap year exactly")
            expected = year_minutes // self.resolution_minutes
            if self.periods != expected:
                raise ValueError(f"non_leap calendar requires {expected} periods")
        return self


class Provenance(ContractModel):
    source: str
    source_version: str | None = None
    transformation: str | None = None
    details: FrozenDict = Field(default_factory=FrozenDict)
