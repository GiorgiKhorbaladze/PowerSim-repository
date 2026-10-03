"""Windows-friendly local PowerSim launcher.

The frozen executable starts the same server and executor as ``powersim
serve``.  It deliberately contains no alternative solver implementation.
"""
from __future__ import annotations

import argparse
import socket
import webbrowser
from pathlib import Path
from threading import Timer


def _available_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _workspace() -> Path:
    base = Path.home() / "PowerSimWorkspace"
    base.mkdir(parents=True, exist_ok=True)
    return base


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="PowerSim")
    parser.add_argument("--workspace", type=Path, default=_workspace())
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)
    port = args.port or _available_port(args.host)
    if not args.no_browser:
        Timer(0.8, lambda: webbrowser.open(f"http://{args.host}:{port}/", new=1)).start()
    import uvicorn
    from powersim.server import create_application
    uvicorn.run(create_application(args.workspace), host=args.host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
