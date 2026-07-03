# PowerSim AI Chat Frontend

PowerSim v4 includes a static, embedded AI Assistant panel in `html/PowerSim_v4.html`. The browser never calls OpenAI directly and does not store an API key. It only talks to the local/backend service implemented separately by Agent A.

## Backend URL

Default URL: `http://localhost:8000`

Users can change it in the AI panel. The value is stored in `localStorage` under:

```text
powersim_ai_backend_url
```

## Backend endpoints used

- `GET /api/ai/health`
- `POST /api/ai/chat`
- `POST /api/ai/confirm`
- Stage 1 integration reserves hooks for `POST /api/ai/summarize-results` and `POST /api/ai/compare-results`.

## Context sent with chat

Each chat request includes:

```json
{
  "session_id": "...",
  "message": "...",
  "language": "auto|ka|en",
  "current_input": {},
  "current_results": {},
  "ui_state": {}
}
```

Large payloads are compacted before sending. Result summaries include metadata, system summary, diagnostics, and a small hourly sample instead of blindly sending huge JSON.

## Confirmation model

Backend-proposed actions are rendered as cards with title, description, changes, risk, Confirm, and Cancel controls. Solver runs and scenario-changing actions must be confirmed by the user before the frontend calls `POST /api/ai/confirm`.

## Offline/local mode

If the backend is offline, the panel remains available and shows setup instructions:

1. Install backend requirements.
2. Set `OPENAI_API_KEY` in the backend environment.
3. Run `uvicorn backend.powersim_ai_server:app --reload --port 8000`.

## Manual test checklist

- Open `html/PowerSim_v4.html` directly in a browser.
- Backend offline message appears and the rest of the app still works.
- Health indicator changes to online when Agent A backend is running.
- Georgian text sends correctly from the chat input.
- Mock or real backend action renders an action card.
- Confirm calls `/api/ai/confirm` and shows progress.
- Solver result response shows an **Import results** button.
- Existing **Export Input JSON** still downloads input JSON.
- Existing **Import Results JSON** still renders results.
- Compare tab still works with imported comparison scenarios.
- No API keys or secrets are present in frontend files.
