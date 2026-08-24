"""Compatibility for the stock Agent Server custom-app lifespan ordering."""

from __future__ import annotations

from typing import Any

from starlette.types import Receive, Scope, Send


def patch_ensure_store_lifespan() -> None:
    """Keep EnsureStoreAccessible out of the ASGI lifespan handshake."""
    from langgraph_api.middleware.ensure_store import EnsureStoreAccessible

    if getattr(EnsureStoreAccessible, "_langhost_lifespan_patch", False):
        return

    original_call = EnsureStoreAccessible.__call__

    async def call(
        self: Any,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] == "lifespan":
            await self.app(scope, receive, send)
            return
        await original_call(self, scope, receive, send)

    EnsureStoreAccessible.__call__ = call
    EnsureStoreAccessible._langhost_lifespan_patch = True
