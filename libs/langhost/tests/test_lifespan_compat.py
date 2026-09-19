from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock

import pytest
from asgi_lifespan import LifespanManager
from langchain_core.runnables.config import var_child_runnable_config
from starlette.applications import Starlette

from langhost.lifespan_compat import patch_ensure_store_lifespan


@pytest.fixture
def ensure_store(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("REDIS_URI", "redis://localhost:6379/0")
    from langgraph_api.middleware import ensure_store

    cls = ensure_store.EnsureStoreAccessible
    # Register both mutations so the production patch cannot leak into other tests.
    monkeypatch.setattr(cls, "__call__", cls.__call__)
    monkeypatch.setattr(
        cls,
        "_langhost_lifespan_patch",
        getattr(cls, "_langhost_lifespan_patch", False),
        raising=False,
    )
    monkeypatch.setattr(ensure_store, "_CONFIG", None)
    return ensure_store


@pytest.mark.asyncio
async def test_store_middleware_bypasses_lifespan_scope(
    monkeypatch: pytest.MonkeyPatch, ensure_store: ModuleType
) -> None:
    events = []

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        events.append("startup")
        yield
        events.append("shutdown")

    get_conf = AsyncMock(side_effect=AssertionError("store resolved before startup"))
    monkeypatch.setattr(ensure_store, "_get_partial_conf", get_conf)
    patch_ensure_store_lifespan()

    middleware = ensure_store.EnsureStoreAccessible(Starlette(lifespan=lifespan))
    async with LifespanManager(middleware):
        assert events == ["startup"]
    assert events == ["startup", "shutdown"]
    get_conf.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_type", ["http", "websocket"])
@pytest.mark.parametrize("raises", [False, True])
async def test_request_store_context_is_preserved_and_reset(
    monkeypatch: pytest.MonkeyPatch, ensure_store: ModuleType, scope_type: str, raises: bool
) -> None:
    request_config = {"configurable": {"store": object()}}
    outer_config = {"configurable": {"outer": True}}
    get_conf = AsyncMock(return_value=request_config)
    monkeypatch.setattr(ensure_store, "_get_partial_conf", get_conf)
    patch_ensure_store_lifespan()

    async def app(scope: dict[str, Any], _receive: Any, _send: Any) -> None:
        assert scope["type"] == scope_type
        assert var_child_runnable_config.get() is request_config
        if raises:
            raise RuntimeError("request failed")

    middleware = ensure_store.EnsureStoreAccessible(app)
    token = var_child_runnable_config.set(outer_config)
    try:
        if raises:
            with pytest.raises(RuntimeError, match="request failed"):
                await middleware({"type": scope_type}, None, None)
        else:
            await middleware({"type": scope_type}, None, None)
        assert var_child_runnable_config.get() is outer_config
        get_conf.assert_awaited_once_with()
    finally:
        var_child_runnable_config.reset(token)


def test_lifespan_patch_is_idempotent(ensure_store: ModuleType) -> None:
    patch_ensure_store_lifespan()
    patched_call = ensure_store.EnsureStoreAccessible.__call__
    patch_ensure_store_lifespan()
    assert ensure_store.EnsureStoreAccessible.__call__ is patched_call


@pytest.mark.asyncio
async def test_lifespan_startup_error_is_not_swallowed(ensure_store: ModuleType) -> None:
    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        raise RuntimeError("runtime startup failed")
        yield  # pragma: no cover

    patch_ensure_store_lifespan()
    middleware = ensure_store.EnsureStoreAccessible(Starlette(lifespan=lifespan))
    with pytest.raises(RuntimeError, match="runtime startup failed"):
        async with LifespanManager(middleware):
            pytest.fail("startup unexpectedly succeeded")
