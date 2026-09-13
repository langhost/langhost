"""LangHost ASGI entrypoint with custom-app lifespan compatibility."""

from importlib import import_module

from langhost.lifespan_compat import patch_ensure_store_lifespan

patch_ensure_store_lifespan()

app = import_module("langgraph_api.server").app

__all__ = ["app"]
