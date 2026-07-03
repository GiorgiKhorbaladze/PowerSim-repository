"""Pydantic models for the PowerSim AI backend."""
from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field

Language = Literal["ka", "en", "auto"]
RiskLevel = Literal["low", "medium", "high"]

class ProposedAction(BaseModel):
    action_id: str
    title: str
    description: str
    changes: list[dict[str, Any]] = Field(default_factory=list)
    risk_level: RiskLevel = "low"
    requires_confirmation: bool = True
    tool_name: str | None = None
    tool_args: dict[str, Any] = Field(default_factory=dict)

class ToolResult(BaseModel):
    tool_name: str
    ok: bool
    result: Any = None
    error: str | None = None

class ChatRequest(BaseModel):
    session_id: str
    message: str
    current_input: dict[str, Any] | None = None
    current_results: dict[str, Any] | None = None
    language: Language = "auto"

class ChatResponse(BaseModel):
    reply: str
    proposed_actions: list[ProposedAction] = Field(default_factory=list)
    requires_confirmation: bool = False
    tool_results: list[ToolResult] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)

class ConfirmRequest(BaseModel):
    session_id: str
    action_id: str

class SummarizeResultsRequest(BaseModel):
    results_json: dict[str, Any] | None = None
    results_path: str | None = None

class CompareResultsRequest(BaseModel):
    base_results: dict[str, Any] | None = None
    candidate_results: dict[str, Any] | None = None
    base_path: str | None = None
    candidate_path: str | None = None
