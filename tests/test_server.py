"""Same-origin application-server assembly regression coverage."""
from powersim.server import _default_static_dir, create_application


def test_same_origin_server_assembles_ui_and_api(tmp_path):
    app = create_application(tmp_path)
    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/api/ai/health" in paths
    assert any(path == "/api" for path in paths)
    assert (_default_static_dir() / "PowerSim_v4.html").is_file()
