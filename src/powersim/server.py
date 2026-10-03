"""Same-origin local PowerSim application server."""
from __future__ import annotations

from pathlib import Path
import sys
import sysconfig

from powersim.application import ApplicationService, build_fastapi_app
from powersim.execution import RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager


def _default_static_dir() -> Path:
    """Locate the UI both from a source checkout and an installed wheel."""
    frozen_assets = Path(getattr(sys, "_MEIPASS", "")) / "html"
    if frozen_assets.is_dir():
        return frozen_assets
    source_assets = Path(__file__).resolve().parents[2] / "html"
    if source_assets.is_dir():
        return source_assets
    installed_assets = Path(sysconfig.get_path("data")) / "powersim" / "ui"
    return installed_assets


def create_application(workspace: str | Path, static_dir: str | Path | None = None):
    """Serve the UI and API together with the validated local executor."""
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse
    from fastapi.staticfiles import StaticFiles

    workspace = Path(workspace).resolve()
    if static_dir is None:
        static_dir = _default_static_dir()
    static_dir = Path(static_dir).resolve()
    entrypoint = static_dir / "PowerSim_v4.html"
    if not entrypoint.is_file():
        raise RuntimeError(f"PowerSim UI asset is unavailable: {entrypoint}")
    app = FastAPI(title="PowerSim", version="1.0")
    service = ApplicationService(RunManager(workspace), RegisteredLocalWorkflowExecutor())

    @app.get("/api/ai/health", include_in_schema=False)
    def optional_ai_health():
        """Report the bundled UI's optional AI service as unavailable."""
        return {"available": False, "reason": "optional AI backend is not configured"}

    app.mount("/api", build_fastapi_app(service))

    @app.get("/", include_in_schema=False)
    def ui():
        # The historical static UI predates same-origin serving and defaults
        # its optional AI widget to localhost:8000.  Make its default current
        # origin at serve time, then let the explicit unavailable health route
        # keep the widget honest without producing a failed browser request.
        html = entrypoint.read_text(encoding="utf-8")
        html = html.replace("const DEFAULT_URL = 'http://localhost:8000';",
                            "const DEFAULT_URL = window.location.origin;")
        html = html.replace("try{ const r = await fetch(state.backendUrl + '/api/ai/health', {cache:'no-store'}); setStatus(r.ok, r.ok?'online':'offline'); }",
                            "try{ const r = await fetch(state.backendUrl + '/api/ai/health', {cache:'no-store'}); const body = r.ok ? await r.json() : {}; const available = r.ok && body.available === true; setStatus(available, available?'online':'offline'); }")
        return HTMLResponse(html)

    app.mount("/", StaticFiles(directory=static_dir, html=False), name="ui-assets")
    return app
