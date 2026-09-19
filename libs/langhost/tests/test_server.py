"""Custom-app startup through the real CLI, backed by the CI Postgres/Redis services."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

pytestmark = [
    pytest.mark.skipif(
        os.getenv("LG_RUNTIME_PG_TEST") != "1",
        reason="requires test Postgres/Redis services (LG_RUNTIME_PG_TEST=1)",
    ),
    pytest.mark.timeout(60),
]

CUSTOM_APP = """\
import os
from contextlib import asynccontextmanager
from pathlib import Path

from langgraph.config import get_store
from langgraph.graph import END, START, StateGraph
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

builder = StateGraph(dict)
builder.add_node("echo", lambda state: state)
builder.add_edge(START, "echo")
builder.add_edge("echo", END)
graph = builder.compile()

@asynccontextmanager
async def lifespan(app):
    # Runtime startup must finish before the custom lifespan can use the store.
    await get_store().aget(("langhost-lifespan-test",), "probe")
    app.state.started = True
    Path(f"started-{os.getpid()}").touch()
    try:
        yield
    finally:
        Path(f"stopped-{os.getpid()}").touch()

async def ready(request):
    # The original middleware must still install the store on HTTP requests.
    await get_store().aget(("langhost-lifespan-test",), "probe")
    return JSONResponse({"started": request.app.state.started, "pid": os.getpid()})

app = Starlette(routes=[Route("/custom-ready", ready)], lifespan=lifespan)
"""


def _wait_for_startup(
    process: subprocess.Popen, work: Path, url: str, expected_starts: int, log: Path
) -> set[int]:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"CLI exited with {process.returncode}:\n{log.read_text()}")
        started = {int(path.name.removeprefix("started-")) for path in work.glob("started-*")}
        if len(started) >= expected_starts:
            try:
                response = httpx.get(f"{url}/custom-ready", timeout=1)
                if response.status_code == 200:
                    assert response.json()["started"] is True
                    assert response.json()["pid"] in started
                    assert httpx.get(f"{url}/info", timeout=1).status_code == 200
                    return started
            except httpx.HTTPError:
                pass
        time.sleep(0.1)
    pytest.fail(f"Custom app did not start {expected_starts} worker(s):\n{log.read_text()}")


@pytest.mark.parametrize("mode", ["single", "workers", "reload"])
def test_custom_app_pg_lifespan_in_server_processes(tmp_path: Path, mode: str) -> None:
    (tmp_path / "custom_app.py").write_text(CUSTOM_APP)
    (tmp_path / "langgraph.json").write_text(
        json.dumps(
            {
                "dependencies": ["."],
                "graphs": {"probe": "./custom_app.py:graph"},
                "http": {"app": "./custom_app.py:app"},
            }
        )
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    command = [
        sys.executable,
        "-m",
        "langhost.cli",
        "serve",
        "--port",
        str(port),
        "--n-jobs-per-worker",
        "0",
        "--allow-blocking",
    ]
    if mode == "workers":
        command += ["--workers", "2"]
    elif mode == "reload":
        command += ["--reload"]
    env = {
        **os.environ,
        "LANGGRAPH_NO_VERSION_CHECK": "true",
        "PYTHONUNBUFFERED": "1",
    }
    log = tmp_path / "server.log"
    started: set[int] = set()
    with log.open("w") as output:
        process = subprocess.Popen(
            command,
            cwd=tmp_path,
            env=env,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            started = _wait_for_startup(process, tmp_path, url, 2 if mode == "workers" else 1, log)
            if mode == "reload":
                # Give WatchFiles time to initialize after the first worker starts.
                time.sleep(1)
                (tmp_path / "custom_app.py").write_text(CUSTOM_APP + "\n# Reload the custom app.\n")
                started = _wait_for_startup(process, tmp_path, url, 2, log)
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=5)
    stopped = {int(path.name.removeprefix("stopped-")) for path in tmp_path.glob("stopped-*")}
    assert started <= stopped, f"Custom app shutdown was skipped:\n{log.read_text()}"
    assert "ASGI 'lifespan' protocol appears unsupported" not in log.read_text()
