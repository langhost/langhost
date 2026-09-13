from __future__ import annotations

from typing import Any

import pytest


@pytest.mark.asyncio
async def test_store_middleware_bypasses_lifespan_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_URI", "redis://localhost:6379/0")

    from langgraph_api.middleware import ensure_store

    from langhost.lifespan_compat import patch_ensure_store_lifespan

    async def app(scope: dict[str, Any], _receive: Any, _send: Any) -> None:
        assert scope["type"] == "lifespan"

    async def fail_if_called() -> None:
        raise AssertionError("lifespan must not resolve the store before startup")

    monkeypatch.setattr(ensure_store, "_get_partial_conf", fail_if_called)
    patch_ensure_store_lifespan()

    middleware = ensure_store.EnsureStoreAccessible(app)
    await middleware({"type": "lifespan"}, None, None)
