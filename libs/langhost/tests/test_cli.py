from functools import wraps
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from langgraph_api import cli as upstream_cli

from langhost import cli as cli_module


def test_banner_uses_resolved_port() -> None:
    rendered = cli_module._langhost_welcome(
        host="127.0.0.1",
        port=51234,
        ssl=False,
        studio_origin=None,
        mount_prefix=None,
    )

    assert "http://127.0.0.1:51234" in rendered
    assert "31296" not in rendered


def test_serve_passes_resolved_port_to_banner_and_server(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    calls: dict[str, Any] = {}

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli_module, "load_dotenv", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_module, "validate_config_file", lambda _config: {})

    def resolve_port(host: str, port: int) -> int:
        calls["resolved"] = (host, port)
        return 51234

    def welcome(**kwargs: Any) -> str:
        calls["welcome"] = kwargs
        return "welcome"

    def run_server(host: str, port: int, *_args: Any, **_kwargs: Any) -> None:
        calls["server"] = (host, port)

    monkeypatch.setattr(cli_module, "_resolve_port", resolve_port)
    monkeypatch.setattr(cli_module, "_langhost_welcome", welcome)
    monkeypatch.setattr(cli_module, "run_server", run_server)

    cli_module.serve.callback(
        host="127.0.0.1",
        port=31296,
        config=tmp_path / "langgraph.json",
        env_file=None,
        database_uri="postgresql://example",
        redis_uri="redis://example",
        reload=False,
        reload_includes=(),
        reload_excludes=(),
        workers=1,
        n_jobs_per_worker=None,
        browser=False,
        studio_url=None,
        tunnel=False,
        debug_port=None,
        wait_for_client=False,
        allow_blocking=False,
        server_log_level="INFO",
        ssl_certfile=None,
        ssl_keyfile=None,
    )

    assert calls["resolved"] == ("127.0.0.1", 31296)
    assert calls["welcome"]["port"] == 51234
    assert calls["server"] == ("127.0.0.1", 51234)


@pytest.mark.parametrize(("workers", "reload"), [(1, False), (4, False), (1, True)])
def test_runtime_compat_preserves_upstream_uvicorn_options(
    monkeypatch: pytest.MonkeyPatch, workers: int, reload: bool
) -> None:
    calls: dict[str, Any] = {}

    # Keep the real signature: upstream filters kwargs using inspect.signature.
    @wraps(uvicorn.run)
    def uvicorn_run(app: str, *args: Any, **kwargs: Any) -> None:
        calls.update(app=app, args=args, kwargs=kwargs)

    monkeypatch.setattr(uvicorn, "run", uvicorn_run)
    monkeypatch.setattr(upstream_cli, "_resolve_port", lambda _host, port: port)
    monkeypatch.setenv("LANGGRAPH_NO_VERSION_CHECK", "true")

    # Exercise the real upstream run_server, including its kwarg filtering.
    cli_module._run_server_with_lifespan_compat(
        "127.0.0.1",
        51234,
        reload,
        {},
        workers=workers,
        lifespan="on",
        timeout_graceful_shutdown=5,
        reload_includes=["*.py"],
        studio_url="https://smith.langchain.com",
        runtime_edition="pg",
        __database_uri__="postgresql://example",
        __redis_uri__="redis://example",
    )

    assert calls["app"] == cli_module._LANGHOST_APP_TARGET
    assert calls["args"] == ()
    assert calls["kwargs"]["workers"] == workers
    assert calls["kwargs"]["lifespan"] == "on"
    assert calls["kwargs"]["timeout_graceful_shutdown"] == 5
    assert calls["kwargs"]["reload"] is reload
    assert calls["kwargs"]["reload_includes"] == ["*.py"]
    assert calls["kwargs"]["port"] == 51234
    assert uvicorn.run is uvicorn_run


def test_runtime_compat_restores_uvicorn_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    original_run = uvicorn.run

    def fail() -> None:
        assert uvicorn.run is not original_run
        raise RuntimeError("server startup failed")

    monkeypatch.setattr(cli_module, "run_server", fail)

    with pytest.raises(RuntimeError, match="server startup failed"):
        cli_module._run_server_with_lifespan_compat()

    assert uvicorn.run is original_run
