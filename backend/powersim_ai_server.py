"""FastAPI app for the Stage 1 PowerSim AI backend orchestrator."""
from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI, HTTPException

from backend.powersim_ai_config import openai_configured, resolve_allowed_path, session_dir
from backend.powersim_ai_models import ChatRequest, ChatResponse, CompareResultsRequest, ConfirmRequest, ProposedAction, SummarizeResultsRequest, ToolResult
from backend.powersim_ai_tools import TOOL_REGISTRY, compare_results, load_input_summary, make_action, run_adequacy, run_solver, summarize_results

app = FastAPI(title="PowerSim AI Backend", version="0.1.0")
_PENDING: dict[str, dict[str, ProposedAction]] = {}


def _load_json_arg(data: dict[str, Any] | None, path: str | None) -> dict[str, Any]:
    if data is not None:
        return data
    if not path:
        raise HTTPException(status_code=400, detail="JSON object or safe path is required")
    safe = resolve_allowed_path(path)
    return json.loads(safe.read_text(encoding="utf-8"))


@app.get("/api/ai/health")
def health() -> dict[str, Any]:
    return {"ok": True, "service": "powersim-ai-backend", "openai_configured": openai_configured()}


@app.post("/api/ai/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    sd = session_dir(req.session_id)
    if req.current_input is not None:
        (sd / "input.json").write_text(json.dumps(req.current_input, indent=2), encoding="utf-8")
    if req.current_results is not None:
        (sd / "results.json").write_text(json.dumps(req.current_results, indent=2), encoding="utf-8")

    tool_results: list[ToolResult] = []
    actions: list[ProposedAction] = []
    msg = req.message.lower()
    if req.current_input is not None and any(word in msg for word in ("input", "asset", "fleet", "summary", "შეჯამ")):
        tool_results.append(ToolResult(tool_name="load_input_summary", ok=True, result=load_input_summary(req.current_input)))
    if req.current_results is not None and any(word in msg for word in ("result", "cost", "summary", "შეჯამ")):
        tool_results.append(ToolResult(tool_name="summarize_results", ok=True, result=summarize_results(req.current_results)))
    if any(word in msg for word in ("run solver", "solve", "solver")):
        action = ProposedAction(**make_action("run_solver", "Run PowerSim solver", "Execute the configured PowerSim horizon solver in a safe subprocess.", {"input_path": str(sd / "input.json"), "out_dir": str(sd / "solver_out")}, "high"))
        actions.append(action)
    _PENDING.setdefault(req.session_id, {}).update({a.action_id: a for a in actions})

    if not openai_configured():
        reply = "AI model is not configured. Set OPENAI_API_KEY on the backend. Stage 1 local tools can still summarize inputs/results and prepare confirmation actions."
    elif tool_results:
        reply = "I reviewed the PowerSim context and returned structured tool results."
    else:
        reply = "AI backend is configured. Stage 1 orchestrator is ready to propose safe PowerSim actions."
    return ChatResponse(reply=reply, proposed_actions=actions, requires_confirmation=bool(actions), tool_results=tool_results, diagnostics={"session_dir": str(sd), "tool_registry": list(TOOL_REGISTRY)})


@app.post("/api/ai/confirm")
def confirm(req: ConfirmRequest) -> dict[str, Any]:
    action = _PENDING.get(req.session_id, {}).get(req.action_id)
    if action is None:
        raise HTTPException(status_code=404, detail="unknown action_id for session")
    try:
        if action.tool_name == "run_solver":
            result = run_solver(**action.tool_args, confirmed=True)
        elif action.tool_name == "run_adequacy":
            result = run_adequacy(**action.tool_args, confirmed=True)
        else:
            raise HTTPException(status_code=400, detail="action tool is not executable")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"action_id": req.action_id, "tool_name": action.tool_name, "result": result}


@app.post("/api/ai/summarize-results")
def api_summarize_results(req: SummarizeResultsRequest) -> dict[str, Any]:
    return summarize_results(_load_json_arg(req.results_json, req.results_path))


@app.post("/api/ai/compare-results")
def api_compare_results(req: CompareResultsRequest) -> dict[str, Any]:
    base = _load_json_arg(req.base_results, req.base_path)
    cand = _load_json_arg(req.candidate_results, req.candidate_path)
    return compare_results(base, cand)
