"""Shared units, time, provenance, and deterministic serialization contracts."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)

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
            expected = 365 * 24 * 60 // self.resolution_minutes
            if self.periods != expected:
                raise ValueError(f"non_leap calendar requires {expected} periods")
        return self


class Provenance(ContractModel):
    source: str
    source_version: str | None = None
    transformation: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)
