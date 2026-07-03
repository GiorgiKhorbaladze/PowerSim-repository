# PowerSim AI Assistant Architecture

The AI assistant is optional and is not required for the base PowerSim CLI, solver, data I/O, or reporting workflows.

## Components

- **Frontend chat panel (Agent B):** sends user messages to `/api/ai/chat`, renders assistant answers, and displays action cards that must be confirmed before execution.
- **Backend orchestrator (Agent A):** owns API endpoints, session state, safe tool execution, confirmation flow, and result handoff.
- **AI skills/safety contract (Agent C):** defines the system prompt, deterministic intent routing, tool schemas, safety helpers, examples, and QA tests.

## Tool registry

Tool schemas are importable from `backend/powersim_ai_tool_schemas.py`. Tools are classified as:

- `read_only`: may summarize or compare existing inputs/results.
- `propose_change`: prepares a change list but does not write files.
- `requires_confirmation`: runs solvers, writes files, changes inputs, or exports reports only after explicit user confirmation.

## Confirmation flow

1. User asks for an action.
2. Backend classifies intent and builds an action card.
3. Frontend shows title, description, changes, risk level, and confirmation requirement.
4. Backend executes only after `/api/ai/confirm` receives explicit confirmation.

## Session workspace

All file access must stay inside the allowed workspace. Private GSE data, generated outputs, Excel workbooks, and project-specific artifacts must not be committed.

## Security boundary

API keys must remain server-side. No API key or secret belongs in browser code, action cards, logs, committed files, or chat output.

## Limitations

Assistant responses are planning support only. PowerSim outputs are not operationally certified and require validation against authorized data and planning standards before use.
