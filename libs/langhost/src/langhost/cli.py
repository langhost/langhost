"""langhost CLI — thin wrappers around langgraph-cli + langgraph_api.cli.run_server."""

from __future__ import annotations

import logging
import os
import pathlib
import sys
from collections.abc import Sequence
from typing import Any, cast

import click
from dotenv import load_dotenv
from langgraph_api.cli import _resolve_port, run_server
from langgraph_cli.config import validate_config_file
from langgraph_cli.constants import DEFAULT_CONFIG
from pyfiglet import figlet_format

from langhost import __version__

# Always the open Postgres+Redis runtime.
RUNTIME_EDITION = "pg"
DEFAULT_PORT = 31296
LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
DEFAULT_STUDIO_ORIGIN = "https://smith.langchain.com"

# Upstream run_server always logs an inmem-oriented welcome; replace it for langhost.
_UPSTREAM_WELCOME_MARKER = "This in-memory server is designed for development and testing"


def _display_host(host: str) -> str:
    if host in {"0.0.0.0", "::"}:
        return "127.0.0.1"
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _server_base_url(
    host: str,
    port: int,
    *,
    ssl: bool,
    mount_prefix: str | None,
) -> str:
    scheme = "https" if ssl else "http"
    url = f"{scheme}://{_display_host(host)}:{port}"
    if mount_prefix:
        url += mount_prefix
    return url


def _langhost_welcome(
    *,
    host: str,
    port: int,
    ssl: bool,
    studio_origin: str | None,
    mount_prefix: str | None,
) -> str:
    api_url = _server_base_url(host, port, ssl=ssl, mount_prefix=mount_prefix)
    origin = (studio_origin or DEFAULT_STUDIO_ORIGIN).rstrip("/")
    studio_url = f"{origin}/studio/?baseUrl={api_url}"
    agent_chat_url = f"https://agentchat.vercel.app/?apiUrl={api_url}&assistantId=agent"
    title = figlet_format("langhost", font="standard").rstrip()
    repo_url = "https://github.com/langhost/langhost"
    return f"""

{title}

- 🚀 API: \033[36m{api_url}\033[0m
- 🎨 Studio UI: \033[36m{studio_url}\033[0m
- 📚 API Docs: \033[36m{api_url}/docs\033[0m
- 💬 Agent Chat UI: \033[36m{agent_chat_url}\033[0m

\033[1;33m★\033[0m  If langhost helps you, a \033[1;33mGitHub star\033[0m keeps the project alive:
   \033[36m{repo_url}\033[0m

Self-hosted, production-grade LangGraph Agent Server (Postgres + Redis).
langhost {__version__}

"""


class _ReplaceWelcomeBanner(logging.Filter):
    """Swap the upstream inmem welcome for a prebuilt langhost banner."""

    def __init__(self, welcome: str) -> None:
        super().__init__()
        self._welcome = welcome

    def filter(self, record: logging.LogRecord) -> bool:
        if _UPSTREAM_WELCOME_MARKER in record.getMessage():
            record.msg = self._welcome
            record.args = ()
        return (
            True  # NOSONAR — logging.Filter keeps all records; we only rewrite the welcome banner
        )


def _validate_serve_options(
    reload: bool,
    workers: int,
    ssl_certfile: pathlib.Path | None,
    ssl_keyfile: pathlib.Path | None,
    tunnel: bool,
    wait_for_client: bool,
    debug_port: int | None,
) -> None:
    """Validate mutually exclusive CLI options, raising UsageError on conflicts."""
    if reload and workers > 1:
        raise click.UsageError("Cannot combine --reload with --workers > 1.")
    if (ssl_certfile is None) != (ssl_keyfile is None):
        raise click.UsageError("Both --ssl-certfile and --ssl-keyfile are required for HTTPS.")
    if ssl_certfile and ssl_keyfile and tunnel:
        raise click.UsageError("Cannot combine --tunnel with SSL options.")
    if wait_for_client and debug_port is None:
        raise click.UsageError("--wait-for-client requires --debug-port.")


def _prepare_serve_env(
    env_file: pathlib.Path | None,
    database_uri: str | None,
    redis_uri: str | None,
    n_jobs_per_worker: int | None,
) -> tuple[str, str, int | None]:
    """Load dotenv and resolve database/redis URIs. Returns (db_uri, redis_uri, n_jobs)."""
    load_dotenv(env_file or ".env", override=False)
    os.environ["LANGGRAPH_RUNTIME_EDITION"] = RUNTIME_EDITION

    database_uri = database_uri or os.environ.get("DATABASE_URI")
    redis_uri = redis_uri or os.environ.get("REDIS_URI")
    if not database_uri:
        raise click.UsageError(
            "DATABASE_URI is required. Please set it in the environment or pass it to the command via --database-uri."
        )
    if not redis_uri:
        raise click.UsageError(
            "REDIS_URI is required. Please set it in the environment or pass it to the command via --redis-uri."
        )
    if n_jobs_per_worker is None:
        raw = os.environ.get("N_JOBS_PER_WORKER")
        if raw:
            n_jobs_per_worker = int(raw)
    return database_uri, redis_uri, n_jobs_per_worker


def _build_uvicorn_kwargs(workers: int) -> dict[str, Any]:
    """Build extra kwargs passed to uvicorn."""
    uvicorn_kwargs: dict[str, Any] = {}
    if workers > 1:
        uvicorn_kwargs["workers"] = workers
    return uvicorn_kwargs


def _resolve_mount_prefix(config_json: dict[str, Any]) -> tuple[dict | None, str | None]:
    """Extract http config and resolve mount prefix from config/env."""
    http_cfg = config_json.get("http")
    mount_prefix = None
    if isinstance(http_cfg, dict):
        mount_prefix = http_cfg.get("mount_prefix")
    mount_prefix = (
        os.environ.get("MOUNT_PREFIX") or os.environ.get("LANGGRAPH_MOUNT_PREFIX") or mount_prefix
    )
    return http_cfg, mount_prefix


@click.group()
@click.version_option(version=__version__, prog_name="langhost")
def cli() -> None:
    """Self-hosted LangGraph Agent Server on Postgres + Redis (edition=pg)."""


@cli.command(
    "serve",
    help=(
        "Run the Agent Server (LANGGRAPH_RUNTIME_EDITION=pg). "
        "Use --reload for local development; --workers for production."
    ),
)
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help=(
        "Bind address. Prefer 127.0.0.1 for local work; use 0.0.0.0 only on "
        "trusted networks / behind a reverse proxy."
    ),
)
@click.option(
    "--port",
    "-p",
    default=DEFAULT_PORT,
    show_default=True,
    type=int,
    help="Port to bind the Agent Server to.",
)
@click.option(
    "--config",
    "-c",
    default=DEFAULT_CONFIG,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    help="Path to langgraph.json (graphs, deps, env, auth, http, …).",
)
@click.option(
    "--env-file",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help="Optional dotenv file to load before starting (default: .env if present).",
)
@click.option(
    "--database-uri",
    default=None,
    help="Postgres URI (overrides DATABASE_URI). Required unless set in the environment.",
)
@click.option(
    "--redis-uri",
    default=None,
    help="Redis URI (overrides REDIS_URI). Required unless set in the environment.",
)
@click.option(
    "--reload",
    is_flag=True,
    help="Auto-reload on code changes (dev). Incompatible with --workers > 1.",
)
@click.option(
    "--reload-include",
    "reload_includes",
    multiple=True,
    help="Glob(s) to watch when --reload is set (repeatable).",
)
@click.option(
    "--reload-exclude",
    "reload_excludes",
    multiple=True,
    help="Glob(s) to ignore when --reload is set (repeatable).",
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    type=click.IntRange(min=1),
    help="Uvicorn worker processes (prod). Incompatible with --reload.",
)
@click.option(
    "--n-jobs-per-worker",
    default=None,
    type=int,
    help=(
        "Max concurrent background jobs per server process "
        "(sets N_JOBS_PER_WORKER; default inside run_server is 1)."
    ),
)
@click.option(
    "--browser/--no-browser",
    default=False,
    help="Open LangSmith Studio in the browser when the server is ready.",
)
@click.option(
    "--studio-url",
    default=None,
    help="LangSmith Studio base URL (default: https://smith.langchain.com).",
)
@click.option(
    "--tunnel",
    is_flag=True,
    help="Expose the server via a Cloudflare tunnel (remote Studio access).",
)
@click.option(
    "--debug-port",
    default=None,
    type=int,
    help="Listen for a remote debugger (debugpy) on this port.",
)
@click.option(
    "--wait-for-client",
    is_flag=True,
    help="With --debug-port, block until a debugger attaches.",
)
@click.option(
    "--allow-blocking",
    is_flag=True,
    help="Do not raise on synchronous/blocking I/O in graph code.",
)
@click.option(
    "--server-log-level",
    default="INFO",
    show_default=True,
    type=click.Choice(LOG_LEVELS, case_sensitive=False),
    help="Log level for uvicorn / langgraph_api.server.",
)
@click.option(
    "--ssl-certfile",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help="TLS certificate file (serve over HTTPS).",
)
@click.option(
    "--ssl-keyfile",
    type=click.Path(exists=True, dir_okay=False, path_type=pathlib.Path),
    default=None,
    help="TLS private key file (serve over HTTPS).",
)
def serve(
    host: str,  # NOSONAR - Click command signature intentionally wide
    port: int,  # NOSONAR - Click command signature intentionally wide
    config: pathlib.Path,
    env_file: pathlib.Path | None,
    database_uri: str | None,
    redis_uri: str | None,
    reload: bool,
    reload_includes: tuple[str, ...],
    reload_excludes: tuple[str, ...],
    workers: int,
    n_jobs_per_worker: int | None,
    browser: bool,
    studio_url: str | None,
    tunnel: bool,
    debug_port: int | None,
    wait_for_client: bool,
    allow_blocking: bool,
    server_log_level: str,
    ssl_certfile: pathlib.Path | None,
    ssl_keyfile: pathlib.Path | None,
) -> None:
    _validate_serve_options(
        reload, workers, ssl_certfile, ssl_keyfile, tunnel, wait_for_client, debug_port
    )

    database_uri, redis_uri, n_jobs_per_worker = _prepare_serve_env(
        env_file, database_uri, redis_uri, n_jobs_per_worker
    )

    config_json = validate_config_file(config)
    if config_json.get("node_version"):
        raise click.UsageError(
            "JS graphs are not supported by langhost. Remove the 'node_version' field from the config."
        )

    cwd = pathlib.Path.cwd()
    sys.path.append(str(cwd))
    for dep in config_json.get("dependencies", []):
        dep_path = cwd / dep
        if dep_path.is_dir() and dep_path.exists():
            sys.path.append(str(dep_path))

    includes: Sequence[str] | None = list(reload_includes) or None
    excludes: Sequence[str] | None = list(reload_excludes) or None
    uvicorn_kwargs = _build_uvicorn_kwargs(workers)
    http_cfg, mount_prefix = _resolve_mount_prefix(cast(dict[str, Any], config_json))

    port = _resolve_port(host, port)
    welcome = _langhost_welcome(
        host=host,
        port=port,
        ssl=ssl_certfile is not None and ssl_keyfile is not None,
        studio_origin=studio_url,
        mount_prefix=mount_prefix,
    )
    api_cli_logger = logging.getLogger("langgraph_api.cli")
    banner_filter = _ReplaceWelcomeBanner(welcome)
    api_cli_logger.addFilter(banner_filter)
    try:
        run_server(
            host,
            port,
            reload,
            config_json.get("graphs", {}),
            n_jobs_per_worker=n_jobs_per_worker,
            open_browser=browser,
            tunnel=tunnel,
            debug_port=debug_port,
            wait_for_client=wait_for_client,
            env=config_json.get("env"),
            reload_includes=includes,
            reload_excludes=excludes,
            store=config_json.get("store"),
            auth=config_json.get("auth"),
            http=http_cfg,
            ui=config_json.get("ui"),
            ui_config=config_json.get("ui_config"),
            webhooks=config_json.get("webhooks"),
            checkpointer=config_json.get("checkpointer"),
            studio_url=studio_url,
            disable_persistence=config_json.get("disable_persistence", False),
            allow_blocking=allow_blocking,
            server_level=server_log_level,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            runtime_edition=RUNTIME_EDITION,
            __database_uri__=database_uri,
            __redis_uri__=redis_uri,
            __migrations_path__=None,
            **uvicorn_kwargs,
        )
    finally:
        api_cli_logger.removeFilter(banner_filter)


if __name__ == "__main__":
    cli()
