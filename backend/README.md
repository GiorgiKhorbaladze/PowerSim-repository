# PowerSim AI Backend (Stage 1)

This folder contains a local/backend prototype that places a secure FastAPI layer between the PowerSim HTML UI and any AI model. It keeps API keys out of the browser and exposes only allowlisted PowerSim tools.

## Start the backend

```bash
uvicorn backend.powersim_ai_server:app --reload --port 8000
```

## Configure OpenAI

Set the key only in the backend environment:

```bash
export OPENAI_API_KEY="..."
```

If `OPENAI_API_KEY` is not set, `/api/ai/chat` returns a clear configuration message while local deterministic tools remain available.

## Endpoints

- `GET /api/ai/health`
- `POST /api/ai/chat`
- `POST /api/ai/confirm`
- `POST /api/ai/summarize-results`
- `POST /api/ai/compare-results`

## Security model

- No API keys or secrets are placed in HTML.
- Tools are allowlisted in `powersim_ai_tools.py`.
- Paths are constrained to approved PowerSim runtime/project/sample folders and reject `..` traversal.
- Solver and adequacy execution uses `subprocess.run(..., shell=False)` with fixed Python script entrypoints.
- Chat tools do not delete files, run git commands, expose environment variables, or write into tracked source files.
- Runtime data belongs under ignored `.runtime/sessions/<session_id>/`.

## Confirmation model

Potentially expensive or file-writing tools, including solver and adequacy runs, return proposed actions and require explicit `/api/ai/confirm` before execution.

## Available tools

- `load_input_summary(input_json)`
- `summarize_results(results_json)`
- `compare_results(base_results, candidate_results)`
- `propose_add_bess(input_json, id, power_mw, energy_mwh, bus=None)`
- `propose_edit_asset(input_json, asset_id, field_path, value)`
- `propose_set_horizon(input_json, hours, start_hour)`
- `run_solver(input_path, out_dir, hours=None, scenario=None)`
- `run_adequacy(input_path, out_dir, mode="deterministic_derated")`
- `export_report_summary(results_json)`

## Current limitations

This is a Stage 1 local/backend prototype. It does not claim production cybersecurity certification, persistent database storage, multi-user access control, or fully automated model tool calling.
