"""Same-origin local PowerSim application server.

The browser application in ``powersim/ui/static`` is the only UI served.  The
historical v4 static prototype under the repository's ``html/`` directory is
an archived asset and is deliberately not mounted.
"""
from __future__ import annotations

from pathlib import Path

from powersim.application import ApplicationService, build_fastapi_app
from powersim.execution import RegisteredLocalWorkflowExecutor
from powersim.platform import RunManager

UI_ENTRYPOINT = "index.html"
_SECURITY_HEADERS = {
    "Content-Security-Policy": ("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
                                "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"),
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Cache-Control": "no-cache",
}


def _default_static_dir() -> Path:
    """The packaged PowerSim 1.0 browser application."""
    return Path(__file__).resolve().parent / "ui" / "static"


def create_application(workspace: str | Path, static_dir: str | Path | None = None, *,
                       reference_directory: str | Path | None = None):
    """Serve the UI and API together with the validated local executor."""
    from fastapi import FastAPI
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    workspace = Path(workspace).resolve()
    static_dir = Path(static_dir or _default_static_dir()).resolve()
    entrypoint = static_dir / UI_ENTRYPOINT
    if not entrypoint.is_file():
        raise RuntimeError(f"PowerSim UI asset is unavailable: {entrypoint}")
    app = FastAPI(title="PowerSim", version="1.0", docs_url=None, redoc_url=None, openapi_url=None)
    service = ApplicationService(RunManager(workspace), RegisteredLocalWorkflowExecutor(),
                                 reference_directory=reference_directory)
    app.state.service = service

    @app.middleware("http")
    async def security_headers(request, call_next):
        response = await call_next(request)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

    app.mount("/api", build_fastapi_app(service))

    @app.get("/", include_in_schema=False)
    def ui():
        return FileResponse(entrypoint, media_type="text/html")

    app.mount("/static", StaticFiles(directory=static_dir), name="ui-assets")
    return app
