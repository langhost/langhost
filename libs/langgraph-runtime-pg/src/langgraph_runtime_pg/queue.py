"""Background run queue (Redis wake, SKIP LOCKED claim, worker dispatch)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import threading
from collections.abc import Callable, Coroutine
from contextlib import ExitStack
from typing import cast

import structlog

from langgraph_runtime_pg import database, ops
from langgraph_runtime_pg.redis_stream import (
    bg_job_heartbeat_secs,
    wait_for_queue_wake,
)

logger = structlog.stdlib.get_logger(__name__)

_WORKERS_LOCK = threading.Lock()
WORKERS: set = set()

SHUTDOWN_GRACE_PERIOD_SECS = 5


def _workers_add(item: object) -> None:
    with _WORKERS_LOCK:
        WORKERS.add(item)


def _workers_discard(item: object) -> None:
    with _WORKERS_LOCK:
        WORKERS.discard(item)


def _workers_snapshot() -> list:
    with _WORKERS_LOCK:
        return list(WORKERS)


class BgLoopRunner(asyncio.Runner):  # type: ignore[misc]
    """asyncio.Runner that owns a loop in a dedicated thread."""

    executor: concurrent.futures.ThreadPoolExecutor

    def __init__(self, idx: int):
        super().__init__()
        self.idx = idx

    def __enter__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(
            1, thread_name_prefix=f"bg-loop-{self.idx}"
        )
        self.executor.submit(self.get_loop).result()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            loop = self.get_loop()
            for task in asyncio.all_tasks(loop):
                task.cancel("Stopping background loop")
        except Exception:
            pass
        try:
            self.executor.shutdown(wait=False)
        except Exception:
            pass
        return super().__exit__(exc_type, exc_val, exc_tb)

    def submit(
        self,
        coro: Coroutine,
        *,
        name: str | None = None,
        callback: Callable | None = None,
    ):
        fut = self.executor.submit(self.run, coro, name=name)
        _workers_add(fut)
        if callback:
            fut.add_done_callback(callback)
        return fut

    def run(self, coro: Coroutine, *, name: str | None = None):  # type: ignore[override]
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError("Runner.run() cannot be called from a running event loop")
        self._lazy_init()  # type: ignore[attr-defined]
        task = self._loop.create_task(coro, name=name)  # type: ignore[attr-defined]
        try:
            return self._loop.run_until_complete(task)  # type: ignore[attr-defined]
        except asyncio.exceptions.CancelledError:  # NOSONAR - explicit re-raise kept for parity
            raise  # NOSONAR


def get_num_workers() -> int:
    with _WORKERS_LOCK:
        return len(WORKERS)


def _setup_runners(
    stack: ExitStack,
    concurrency: int,
) -> tuple:
    """Initialize runner pool and executor. Returns (runners, executor_or_None)."""
    from langgraph_api import config
    from langgraph_api.asyncio import AsyncQueue

    runners: AsyncQueue[BgLoopRunner] = AsyncQueue(concurrency)
    if config.BG_JOB_ISOLATED_LOOPS:
        executor = stack.enter_context(concurrent.futures.ThreadPoolExecutor())
        bg_runners = {stack.enter_context(BgLoopRunner(idx)) for idx in range(concurrency)}
        for r in bg_runners:
            runners.put_nowait(r)
            r.get_loop().set_default_executor(executor)
    else:
        for _ in range(concurrency):
            runners.put_nowait(cast(BgLoopRunner, object()))
    return runners, None


def _make_cleanup_callback(
    loop: asyncio.AbstractEventLoop,
    runners,
    expired_runners: list[BgLoopRunner],
    webhooks: set,
) -> Callable:
    """Build the done-callback for dispatched worker tasks."""
    from langgraph_api import config

    def cleanup(task, runner: BgLoopRunner):
        _workers_discard(task)
        try:
            if config.BG_JOB_ISOLATED_LOOPS:
                loop.call_soon_threadsafe(runners.put_nowait, runner)
            else:
                runners.put_nowait(runner)
        except Exception as exc:
            expired_runners.append(runner)
            logger.exception("Background worker cleanup failed", exc_info=exc)

        _handle_task_result(task, loop, webhooks)

    return cleanup


def _handle_task_result(
    task,
    loop: asyncio.AbstractEventLoop,
    webhooks: set,
) -> None:
    """Process completed task result — fire webhooks if needed."""

    try:
        if task.cancelled():
            return
        task_exc = task.exception()
        if task_exc:
            if not isinstance(task_exc, asyncio.CancelledError):
                logger.exception(
                    f"Background worker failed for task {task}",
                    exc_info=task_exc,
                )
            return
        result = task.result()
        if result and result.get("webhook"):
            _dispatch_webhook(result, loop, webhooks)
    except asyncio.CancelledError:  # NOSONAR - keep historical cleanup behavior
        pass
    except Exception as exc:
        logger.exception("Background worker cleanup failed", exc_info=exc)


def _dispatch_webhook(
    result: dict,
    loop: asyncio.AbstractEventLoop,
    webhooks: set,
) -> None:
    """Schedule webhook delivery for a completed run."""
    from langgraph_api import config, webhook

    if config.BG_JOB_ISOLATED_LOOPS:
        hook_fut = asyncio.run_coroutine_threadsafe(webhook.call_webhook(result), loop)
        webhooks.add(hook_fut)
        hook_fut.add_done_callback(webhooks.remove)
    else:
        hook_task = loop.create_task(
            webhook.call_webhook(result),
            name=f"webhook-{result['run']['run_id']}",
        )
        webhooks.add(hook_task)
        hook_task.add_done_callback(webhooks.remove)


async def _maybe_log_stats(
    concurrency: int,
    last_stats_secs: float | None,
    loop: asyncio.AbstractEventLoop,
    stats_interval: float,
) -> tuple[bool, float | None]:
    """Log worker/queue stats if interval elapsed. Returns (did_log, updated_ts)."""
    calc_stats = last_stats_secs is None or loop.time() - last_stats_secs > stats_interval
    if calc_stats:
        last_stats_secs = loop.time()
        active = get_num_workers()
        await logger.ainfo(
            "Worker stats",
            max=concurrency,
            available=concurrency - active,
            active=active,
        )
    return calc_stats, last_stats_secs


async def _maybe_sweep(
    last_sweep_secs: float | None,
    loop: asyncio.AbstractEventLoop,
    sweep_every: float,
    calc_stats: bool,
) -> float | None:
    """Sweep stale runs and log queue stats if intervals elapsed."""
    do_sweep = last_sweep_secs is None or loop.time() - last_sweep_secs > sweep_every
    if calc_stats or do_sweep:
        async with database.connect() as conn:
            if calc_stats:
                stats = await ops.Runs.stats(conn)
                await logger.ainfo("Queue stats", **stats)
            if do_sweep:
                last_sweep_secs = loop.time()
                swept = await ops.Runs.sweep()
                if swept:
                    await logger.awarning("Swept stale runs", count=len(swept))
    return last_sweep_secs


async def _claim_and_dispatch(
    runners,
    loop: asyncio.AbstractEventLoop,
    cleanup: Callable,
    last_run,
) -> object:
    """Claim runs from queue and dispatch to workers. Returns last run seen."""
    from langgraph_api import config, graph, worker

    claimed = last_run
    async for run, attempt in ops.Runs.next(wait=False, limit=runners.qsize()):
        claimed = run
        runner = runners.get_nowait()
        graph_id = run["kwargs"].get("config", {}).get("configurable", {}).get("graph_id")
        task_name = f"run-{run['run_id']}-attempt-{attempt}"
        if not config.BG_JOB_ISOLATED_LOOPS or (graph_id and graph.is_js_graph(graph_id)):
            task = asyncio.create_task(
                worker.worker(run, attempt, loop),
                name=task_name,
            )
            task.add_done_callback(functools.partial(cleanup, runner=runner))
            _workers_add(task)
        else:
            runner.submit(
                worker.worker(run, attempt, loop),
                name=task_name,
                callback=functools.partial(cleanup, runner=runner),
            )
    return claimed


async def _shutdown_workers(webhooks: set) -> None:
    """Cancel all workers/webhooks and wait for graceful shutdown."""
    from langgraph_api import config
    from langgraph_api.utils.future import chain_future

    logger.info("Shutting down background workers")
    workers = _workers_snapshot()
    webhook_list = list(webhooks)
    loop = asyncio.get_running_loop()
    for task in workers:
        task.cancel()
    for task in webhook_list:
        task.cancel()

    futs: list[asyncio.Future] = []
    if config.BG_JOB_ISOLATED_LOOPS:
        futs.extend(cast(asyncio.Future, chain_future(f, loop.create_future())) for f in workers)
        futs.extend(
            cast(asyncio.Future, chain_future(f, loop.create_future())) for f in webhook_list
        )
    else:
        futs.extend(cast(asyncio.Future, f) for f in workers)
        futs.extend(cast(asyncio.Future, f) for f in webhook_list)
    if futs:
        try:
            await asyncio.wait_for(
                asyncio.gather(*futs, return_exceptions=True),
                SHUTDOWN_GRACE_PERIOD_SECS,
            )
        except TimeoutError:
            logger.warning(
                "Background workers did not finish within grace period",
                timeout=SHUTDOWN_GRACE_PERIOD_SECS,
            )


async def queue() -> None:
    from langgraph_api import config
    from langgraph_api.asyncio import AsyncQueue

    concurrency = config.N_JOBS_PER_WORKER
    loop = asyncio.get_running_loop()
    last_stats_secs: float | None = None
    last_sweep_secs: float | None = None
    webhooks: set = set()

    with ExitStack() as stack:
        runners: AsyncQueue[BgLoopRunner] = AsyncQueue(concurrency)
        await _init_runners(stack, runners, concurrency, config)
        expired_runners: list[BgLoopRunner] = []

        cleanup = _make_cleanup_callback(loop, runners, expired_runners, webhooks)

        await logger.ainfo(f"Starting {concurrency} background workers")
        try:
            run = None
            while True:
                last_stats_secs, last_sweep_secs, run = await _queue_tick(
                    runners,
                    expired_runners,
                    run,
                    last_stats_secs,
                    last_sweep_secs,
                    concurrency,
                    loop,
                    cleanup,
                    config,
                )
        finally:
            await _shutdown_workers(webhooks)


async def _init_runners(stack, runners, concurrency, config) -> None:
    if config.BG_JOB_ISOLATED_LOOPS:
        await logger.ainfo("Starting queue with isolated loops")
        executor = stack.enter_context(concurrent.futures.ThreadPoolExecutor())
        bg_runners = {stack.enter_context(BgLoopRunner(idx)) for idx in range(concurrency)}
        for r in bg_runners:
            runners.put_nowait(r)
            r.get_loop().set_default_executor(executor)
    else:
        await logger.ainfo("Starting queue with shared loop")
        for _ in range(concurrency):
            runners.put_nowait(cast(BgLoopRunner, object()))


async def _queue_tick(
    runners,
    expired_runners,
    run,
    last_stats_secs,
    last_sweep_secs,
    concurrency,
    loop,
    cleanup,
    config,
):
    """Single iteration of the queue loop; returns updated state."""
    if expired_runners:
        for runner in expired_runners:
            await runners.put(runner)
        expired_runners.clear()
    await runners.wait()
    try:
        sweep_every = bg_job_heartbeat_secs() * 2
        calc_stats, last_stats_secs = await _maybe_log_stats(
            concurrency, last_stats_secs, loop, config.STATS_INTERVAL_SECS
        )
        if run is None and last_stats_secs is not None:
            await wait_for_queue_wake(timeout=0.5)
        run = await _claim_and_dispatch(runners, loop, cleanup, None)
        last_sweep_secs = await _maybe_sweep(last_sweep_secs, loop, sweep_every, calc_stats)
    except Exception as exc:
        logger.exception("Background worker scheduler failed", exc_info=exc)
        await asyncio.sleep(1)
    return last_stats_secs, last_sweep_secs, run
