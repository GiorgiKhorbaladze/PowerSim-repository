"""Same-origin local PowerSim application server."""
from __future__ import annotations

from pathlib import Path
import sysconfig

from powersim.application import ApplicationService, build_fastapi_app
from powersim.execution import DeterministicLocalSolverExecutor
from powersim.platform import RunManager


def _default_static_dir() -> Path:
    """Locate the UI both from a source checkout and an installed wheel."""
    source_assets = Path(__file__).resolve().parents[2] / "html"
    if source_assets.is_dir():
        return source_assets
    installed_assets = Path(sysconfig.get_path("data")) / "powersim" / "ui"
    return installed_assets


def create_application(workspace: str | Path, static_dir: str | Path | None = None):
    """Serve the UI and API together with the validated local executor."""
    from fastapi import FastAPI
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    workspace = Path(workspace).resolve()
    if static_dir is None:
        static_dir = _default_static_dir()
    static_dir = Path(static_dir).resolve()
    entrypoint = static_dir / "PowerSim_v4.html"
    if not entrypoint.is_file():
        raise RuntimeError(f"PowerSim UI asset is unavailable: {entrypoint}")
    app = FastAPI(title="PowerSim", version="1.0")
    service = ApplicationService(RunManager(workspace), DeterministicLocalSolverExecutor())
    app.mount("/api", build_fastapi_app(service))

    @app.get("/", include_in_schema=False)
    def ui():
        return FileResponse(entrypoint)

    app.mount("/", StaticFiles(directory=static_dir, html=False), name="ui-assets")
    return app
