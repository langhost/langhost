"""Store factory: AsyncPostgresStore for the API, PgStore for tests."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import delete, select as sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from langgraph_runtime_pg.database import to_psycopg_uri
from langgraph_runtime_pg.models import StoreItemRow

_STORE_CONFIG: dict | None = None
_STORE: AsyncPostgresStore | None = None
_STORE_POOL: AsyncConnectionPool[Any] | None = None
_STORE_INSTANCE: PgStore | None = None
_SETUP_LOCK = asyncio.Lock()


def set_store_config(config: dict) -> None:
    global _STORE_CONFIG
    _STORE_CONFIG = config


async def setup_store() -> AsyncPostgresStore:
    """Open AsyncPostgresStore and initialize its tables."""
    global _STORE, _STORE_POOL
    async with _SETUP_LOCK:
        if _STORE is not None:
            return _STORE
        if _STORE_POOL is not None:
            try:
                await _STORE_POOL.close()
            except Exception:
                pass
            _STORE_POOL = None

        uri = to_psycopg_uri()
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
            store = AsyncPostgresStore(cast(Any, pool))
            await store.setup()
        except Exception:
            try:
                await pool.close()
            except Exception:
                pass
            raise
        _STORE_POOL = pool
        _STORE = store
        return _STORE


async def teardown_store() -> None:
    global _STORE, _STORE_POOL
    store, pool = _STORE, _STORE_POOL
    _STORE = None
    _STORE_POOL = None
    if store is not None:
        try:
            stop = getattr(store, "stop_ttl_sweeper", None)
            if stop is not None:
                await asyncio.wait_for(stop(), timeout=2.0)
        except Exception:
            pass
    if pool is not None:
        try:
            await asyncio.wait_for(pool.close(), timeout=5.0)
        except Exception:
            pass


class PgStore:
    """Minimal prefix/key store over the store_items table (tests)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory

    async def start_ttl_sweeper(self) -> asyncio.Task:  # NOSONAR
        return asyncio.create_task(asyncio.sleep(0))

    async def aput(self, prefix: str, key: str, value: dict) -> None:
        now = datetime.now(UTC)
        async with self._sf() as session:
            await session.execute(
                pg_insert(StoreItemRow)
                .values(
                    prefix=prefix,
                    key=key,
                    value=value,
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=["prefix", "key"],
                    set_={"value": value, "updated_at": now},
                )
            )
            await session.commit()

    async def aget(self, prefix: str, key: str) -> dict | None:
        async with self._sf() as session:
            row = (
                await session.execute(
                    sa_select(StoreItemRow).where(
                        StoreItemRow.prefix == prefix,
                        StoreItemRow.key == key,
                    )
                )
            ).scalar_one_or_none()
            return row.value if row else None

    async def adelete(self, prefix: str, key: str) -> None:
        async with self._sf() as session:
            await session.execute(
                delete(StoreItemRow).where(
                    StoreItemRow.prefix == prefix,
                    StoreItemRow.key == key,
                )
            )
            await session.commit()

    async def alist(self, prefix: str) -> list[dict]:
        async with self._sf() as session:
            rows = (
                (
                    await session.execute(
                        sa_select(StoreItemRow).where(StoreItemRow.prefix == prefix)
                    )
                )
                .scalars()
                .all()
            )
            return [{"key": r.key, "value": r.value} for r in rows]

    def close(self) -> None:
        # No-op: sessions are opened per-call, so there is nothing to tear down.
        pass


def Store(*_args: Any, **_kwargs: Any) -> AsyncPostgresStore:  # NOSONAR
    """Return the process-wide AsyncPostgresStore (requires setup_store).

    Name matches the upstream ``langgraph_runtime`` factory API (PascalCase).
    """
    if _STORE is None:
        raise RuntimeError(
            "Store requires setup_store()/start_pool() first (DATABASE_URI required)"
        )
    return _STORE


def pg_store_for_tests() -> PgStore:
    """Return the test PgStore singleton over store_items."""
    global _STORE_INSTANCE
    if _STORE_INSTANCE is None:
        from langgraph_runtime_pg.database import _SESSION_FACTORY

        if _SESSION_FACTORY is None:
            raise RuntimeError("Call start_pool() first")
        _STORE_INSTANCE = PgStore(_SESSION_FACTORY)
    return _STORE_INSTANCE


def reset_store() -> None:
    """Reset the test PgStore singleton."""
    global _STORE_INSTANCE
    if _STORE_INSTANCE is not None:
        _STORE_INSTANCE.close()
    _STORE_INSTANCE = None
