"""Checkpointer factory wrapping langgraph-checkpoint-postgres."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from langgraph_runtime_pg.database import to_psycopg_uri

_POOL: AsyncConnectionPool[Any] | None = None
_CHECKPOINTER: AsyncPostgresSaver | None = None
_SETUP_LOCK = asyncio.Lock()


async def setup_checkpointer() -> AsyncPostgresSaver:
    """Open a psycopg pool and initialize AsyncPostgresSaver tables."""
    global _POOL, _CHECKPOINTER
    async with _SETUP_LOCK:
        if _CHECKPOINTER is not None:
            return _CHECKPOINTER
        if _POOL is not None:
            try:
                await _POOL.close()
            except Exception:
                pass
            _POOL = None

        uri = to_psycopg_uri()
        # autocommit required: setup() runs CREATE INDEX CONCURRENTLY
        pool = AsyncConnectionPool(
            conninfo=uri,
            min_size=1,
            max_size=10,
            open=False,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
        )
        try:
            await pool.open()
            saver = AsyncPostgresSaver(cast(Any, pool))
            await saver.setup()
        except Exception:
            try:
                await pool.close()
            except Exception:
                pass
            raise
        _POOL = pool
        _CHECKPOINTER = saver
        return _CHECKPOINTER


async def teardown_checkpointer() -> None:
    """Close the checkpointer pool."""
    global _POOL, _CHECKPOINTER
    _CHECKPOINTER = None
    pool = _POOL
    _POOL = None
    if pool is not None:
        try:
            await pool.close()
        except Exception:
            pass


def Checkpointer(  # NOSONAR - upstream factory name is PascalCase
    *_args: Any, unpack_hook: Any = None, **_kwargs: Any
) -> AsyncPostgresSaver:
    """Return the process-wide AsyncPostgresSaver (requires setup_checkpointer).

    Name matches the upstream ``langgraph_runtime`` factory API (PascalCase).
    """
    del unpack_hook  # accepted for API parity with upstream; unused here
    if _CHECKPOINTER is None:
        raise RuntimeError(
            "Checkpointer not initialized; call start_pool()/setup_checkpointer() first "
            "(DATABASE_URI required)"
        )
    return _CHECKPOINTER


__all__ = ["Checkpointer", "setup_checkpointer", "teardown_checkpointer"]
