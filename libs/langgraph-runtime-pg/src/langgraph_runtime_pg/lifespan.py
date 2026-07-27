"""Starlette lifespan: PG pool, Redis, graphs, and queue."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import structlog
from langchain_core.runnables.config import RunnableConfig, var_child_runnable_config
from langgraph.constants import CONF
from starlette.applications import Starlette

from langgraph_runtime_pg import queue
from langgraph_runtime_pg.database import start_pool, stop_pool

logger = structlog.stdlib.get_logger(__name__)

_LAST_LIFESPAN_ERROR: BaseException | None = None


def get_last_error() -> BaseException | None:
    return _LAST_LIFESPAN_ERROR


class _StartedServices:
    """Tracks which services have been started for cleanup."""

    __slots__ = ("checkpointer", "http", "pool", "ui")

    def __init__(self) -> None:
        self.http = False
        self.pool = False
        self.checkpointer = False
        self.ui = False


async def _startup_runtime(started: _StartedServices) -> None:
    """Start HTTP client, PG pool, checkpointer, and UI bundler."""
    from langgraph_api import _checkpointer as api_checkpointer
    from langgraph_api.http import start_http_client
    from langgraph_api.js.ui import start_ui_bundler

    await start_http_client()
    started.http = True
    await start_pool()
    started.pool = True
    await api_checkpointer.start_checkpointer()
    started.checkpointer = True
    await start_ui_bundler()
    started.ui = True


async def _safe_shutdown(label: str, coro) -> None:
    try:
        await coro
    except Exception:
        logger.debug("%s failed during lifespan cleanup", label, exc_info=True)


async def _shutdown_runtime(started: _StartedServices) -> None:
    """Tear down services in reverse order, tolerating individual failures."""
    from langgraph_api import _checkpointer as api_checkpointer, graph, store as api_store
    from langgraph_api.http import stop_http_client, stop_webhook_http_client
    from langgraph_api.js.ui import stop_ui_bundler

    await _safe_shutdown("exit_store", api_store.exit_store())
    if started.checkpointer:
        await _safe_shutdown("exit_checkpointer", api_checkpointer.exit_checkpointer())
    if started.ui:
        await _safe_shutdown("stop_ui_bundler", stop_ui_bundler())
    await _safe_shutdown("stop_remote_graphs", graph.stop_remote_graphs())
    if started.http:
        await _safe_shutdown("stop_http_client", stop_http_client())
        await _safe_shutdown("stop_webhook_http_client", stop_webhook_http_client())
    if started.pool:
        await _safe_shutdown("stop_pool", stop_pool())


@asynccontextmanager
async def lifespan(
    app: Starlette | None = None,
    cancel_event: asyncio.Event | None = None,
    taskset: set[asyncio.Task] | None = None,
    **kwargs: Any,
) -> AsyncIterator[None]:
    import langgraph_api.config as config
    from langgraph_api import (
        __version__,
        feature_flags,
        graph,
        store as api_store,
    )
    from langgraph_api.asyncio import SimpleTaskGroup, set_event_loop
    from langgraph_api.metadata import metadata_loop

    from langgraph_runtime_pg import __version__ as runtime_version

    await logger.ainfo(
        f"Starting PG runtime with langgraph-api={__version__} "
        f"and langgraph-runtime-pg={runtime_version}",
        version=__version__,
        runtime_version=runtime_version,
    )
    try:
        current_loop = asyncio.get_running_loop()
        set_event_loop(current_loop)
    except RuntimeError:
        await logger.aerror("Failed to set loop")

    global _LAST_LIFESPAN_ERROR
    _LAST_LIFESPAN_ERROR = None

    async def _log_graph_load_failure(err: graph.GraphLoadError) -> None:
        cause = err.__cause__ or err.cause
        log_fields = err.log_fields()
        log_fields["action"] = "fix_user_graph"
        await logger.aerror(
            f"Graph '{err.spec.id}' failed to load: {err.cause_message}",
            **log_fields,
        )
        await logger.adebug(
            "Full graph load failure traceback (internal)",
            **{k: v for k, v in log_fields.items() if k != "user_traceback"},
            exc_info=cause,
        )

    started = _StartedServices()
    try:
        await _startup_runtime(started)

        async with SimpleTaskGroup(
            cancel=True,
            cancel_event=cancel_event,
            taskgroup_name="Lifespan",
        ) as tg:
            tg.create_task(metadata_loop())
            await api_store.collect_store_from_env()
            store_instance = await api_store.get_store()
            if not api_store.CUSTOM_STORE:
                tg.create_task(store_instance.start_ttl_sweeper())
            else:
                await logger.ainfo("Using custom store. Skipping store TTL sweeper.")

            if feature_flags.USE_RUNTIME_CONTEXT_API:
                from langgraph._internal._constants import (
                    CONFIG_KEY_RUNTIME,
                )
                from langgraph.runtime import Runtime

                langgraph_config = cast(
                    RunnableConfig,
                    {CONF: {CONFIG_KEY_RUNTIME: Runtime(store=store_instance)}},
                )
            else:
                from langgraph.constants import CONFIG_KEY_STORE

                langgraph_config = cast(
                    RunnableConfig,
                    {CONF: {CONFIG_KEY_STORE: store_instance}},
                )

            var_child_runnable_config.set(langgraph_config)

            graph.patch_packages_distributions()
            try:
                await graph.collect_graphs_from_env(True)
            except graph.GraphLoadError as exc:
                _LAST_LIFESPAN_ERROR = exc
                await _log_graph_load_failure(exc)
                raise
            if config.N_JOBS_PER_WORKER > 0:
                tg.create_task(queue_with_signal())

            from langgraph_api import cron_scheduler

            tg.create_task(cron_scheduler.cron_scheduler())

            yield
    except graph.GraphLoadError as exc:
        _LAST_LIFESPAN_ERROR = exc
        raise
    except asyncio.CancelledError:  # NOSONAR - preserve tested shutdown semantics
        pass
    finally:
        await _shutdown_runtime(started)


async def queue_with_signal() -> None:
    try:
        await queue.queue()
    except asyncio.CancelledError:  # NOSONAR - preserve tested shutdown semantics
        pass
    except Exception as exc:
        logger.exception("Queue failed. Signaling shutdown", exc_info=exc)
        signal.raise_signal(signal.SIGINT)


lifespan.get_last_error = get_last_error  # type: ignore[attr-defined]
