"""Presence checks: pg must expose surfaces used by langgraph_api / inmem."""

from __future__ import annotations

import ast
import importlib
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest


def _unwrap(obj: Any) -> Callable[..., Any] | None:
    if isinstance(obj, (staticmethod, classmethod)):
        obj = obj.__func__
    return obj if inspect.isroutine(obj) else None


def _own_callables(cls: type) -> dict[str, Callable[..., Any]]:
    out: dict[str, Callable[..., Any]] = {}
    for name, member in cls.__dict__.items():
        if name.startswith("_"):
            continue
        fn = _unwrap(member)
        if fn is not None:
            out[name] = fn
    return out


def _own_classes(cls: type) -> dict[str, type]:
    return {
        name: member
        for name, member in cls.__dict__.items()
        if not name.startswith("_") and isinstance(member, type)
    }


def _ops_service_classes(ops_mod: Any) -> dict[str, type]:
    """Top-level ops service classes (Assistants, Threads, …)."""
    return {
        name: cls
        for name, cls in ops_mod.__dict__.items()
        if not name.startswith("_")
        and isinstance(cls, type)
        and cls.__module__ == ops_mod.__name__
        and _own_callables(cls)
    }


def _resolve(root: Any, dotted: str) -> Any | None:
    cur = root
    for part in dotted.split("."):
        cur = getattr(cur, part, None)
        if cur is None:
            return None
    return cur


def _api_runtime_imports(api_root: Path) -> list[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for path in api_root.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            if not node.module.startswith("langgraph_runtime"):
                continue
            if node.module.startswith("langgraph_runtime_inmem"):
                continue
            for alias in node.names:
                if alias.name != "*":
                    found.add((node.module, alias.name))
    return sorted(found)


def _api_ops_import_names(api_root: Path) -> set[str]:
    """Local names bound from ``from langgraph_runtime.ops import …``."""
    names: set[str] = set()
    for path in api_root.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module != "langgraph_runtime.ops":
                continue
            for alias in node.names:
                if alias.name != "*":
                    names.add(alias.asname or alias.name)
    return names


def _api_ops_paths(api_root: Path) -> list[str]:
    """Dotted attribute paths like ``Assistants.search`` used by langgraph_api."""
    ops_roots = _api_ops_import_names(api_root)
    found: set[str] = set()

    class Visitor(ast.NodeVisitor):
        def visit_Attribute(self, node: ast.Attribute) -> None:
            parts: list[str] = []
            cur: ast.AST = node
            while isinstance(cur, ast.Attribute):
                parts.append(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name) and cur.id in ops_roots:
                parts.append(cur.id)
                parts.reverse()
                if len(parts) >= 2:
                    found.add(".".join(parts))
            self.generic_visit(node)

    for path in api_root.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        Visitor().visit(tree)
    return sorted(found)


@pytest.fixture(scope="module")
def api_root() -> Path:
    import langgraph_api

    return Path(langgraph_api.__file__).resolve().parent


@pytest.fixture(scope="module")
def pg():
    return importlib.import_module("langgraph_runtime_pg")


@pytest.fixture(scope="module")
def inmem():
    return importlib.import_module("langgraph_runtime_inmem")


def test_to_async_sqlalchemy_uri_normalizes_common_forms() -> None:
    from langgraph_runtime_pg.database import to_async_sqlalchemy_uri, to_psycopg_uri

    cases = (
        "postgres://u:p@host:5432/db",
        "postgresql://u:p@host:5432/db",
        "postgresql+asyncpg://u:p@host:5432/db",
        "postgresql+psycopg://u:p@host:5432/db",
        "postgres+psycopg2://u:p@host:5432/db",
    )
    for uri in cases:
        assert to_async_sqlalchemy_uri(uri) == "postgresql+asyncpg://u:p@host:5432/db"
        assert to_psycopg_uri(uri) == "postgresql://u:p@host:5432/db"

    with pytest.raises(ValueError, match="Unsupported DATABASE_URI scheme"):
        to_psycopg_uri("mysql://u:p@host:3306/db")


def test_asyncpg_engine_args_translate_libpq_sslmode() -> None:
    """Strip libpq sslmode from the URL and set asyncpg connect_args['ssl']."""
    from langgraph_runtime_pg.database import asyncpg_engine_args

    uri, connect_args = asyncpg_engine_args(
        "postgresql://u:p@host:5432/db?sslmode=require&application_name=lg"
    )
    assert uri.startswith("postgresql+asyncpg://")
    assert "sslmode=" not in uri
    assert "application_name=lg" in uri
    assert connect_args.get("ssl") is True

    uri_off, args_off = asyncpg_engine_args("postgresql+asyncpg://u:p@host:5432/db?sslmode=disable")
    assert "sslmode=" not in uri_off
    assert args_off.get("ssl") is False


def test_redis_pool_uses_driver_info_without_deprecation() -> None:
    """Pool kwargs must use driver_info — lib_name/lib_version warn on every connect."""
    import warnings

    import redis.asyncio as redis_async
    from redis.driver_info import DriverInfo

    from langgraph_runtime_pg.redis_stream import (
        _redis_pool_kwargs,
        _sanitize_redis_uri,
    )

    assert "lib_name=redis-py" not in _sanitize_redis_uri(
        "redis://localhost:6379/0?lib_name=redis-py&lib_version=8.0.1&db=0"
    )
    assert "lib_version=" not in _sanitize_redis_uri(
        "redis://localhost:6379/0?lib_name=redis-py&lib_version=8.0.1"
    )
    assert _sanitize_redis_uri("redis://localhost:6379/0") == "redis://localhost:6379/0"

    kwargs = _redis_pool_kwargs()
    assert "lib_name" not in kwargs
    assert "lib_version" not in kwargs
    assert isinstance(kwargs.get("driver_info"), DriverInfo)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pool = redis_async.ConnectionPool.from_url(
            "redis://localhost:6379/0",
            decode_responses=False,
            **kwargs,
        )
        pool.make_connection()
    leak = [
        str(w.message)
        for w in caught
        if "lib_name" in str(w.message) or "lib_version" in str(w.message)
    ]
    assert leak == [], f"redis pool still triggers deprecation warnings: {leak}"


@pytest.mark.asyncio
async def test_sslmode_require_no_longer_typeerrors_on_engine_connect() -> None:
    """``?sslmode=require`` must not TypeError after asyncpg_engine_args."""
    import os

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from langgraph_runtime_pg.database import asyncpg_engine_args, to_async_sqlalchemy_uri

    base = os.environ.get(
        "DATABASE_URI",
        "postgresql+asyncpg://postgres:postgres@localhost:5432/langgraph",
    )
    raw_async = to_async_sqlalchemy_uri(base)
    sep = "&" if "?" in raw_async else "?"
    broken = f"{raw_async}{sep}sslmode=require"

    broken_engine = create_async_engine(broken, pool_size=1, max_overflow=0)
    try:

        async def _probe_broken():
            async with broken_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

        with pytest.raises(TypeError, match="sslmode"):
            await _probe_broken()
    finally:
        await broken_engine.dispose()

    uri, connect_args = asyncpg_engine_args(broken)
    fixed_engine = create_async_engine(uri, pool_size=1, max_overflow=0, connect_args=connect_args)
    try:
        try:
            async with fixed_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except TypeError:
            pytest.fail("asyncpg_engine_args still produced an sslmode TypeError")
        except Exception:
            pass  # local PG may reject SSL upgrade; TypeError is the bug
    finally:
        await fixed_engine.dispose()


def test_api_imports_exist_on_pg(api_root: Path, pg) -> None:
    imports = _api_runtime_imports(api_root)
    assert imports, "expected langgraph_api to import langgraph_runtime.*"

    missing: list[str] = []
    for module_path, name in imports:
        if module_path == "langgraph_runtime":
            target, label = pg, f"langgraph_runtime_pg.{name}"
        else:
            suffix = module_path.removeprefix("langgraph_runtime.")
            if not hasattr(pg, suffix):
                missing.append(f"module {suffix} (for {module_path}.{name})")
                continue
            target = getattr(pg, suffix)
            label = f"langgraph_runtime_pg.{suffix}.{name}"
        if not hasattr(target, name):
            missing.append(label)
    assert not missing, "API imports missing on pg:\n" + "\n".join(f"  - {m}" for m in missing)


def test_api_ops_call_sites_exist_on_pg(api_root: Path, pg, inmem) -> None:
    roots = set(_ops_service_classes(inmem.ops))
    paths = [p for p in _api_ops_paths(api_root) if p.split(".", 1)[0] in roots]
    assert paths, "expected langgraph_api to call langgraph_runtime.ops.*"

    # Only require call sites that inmem actually implements.
    missing = [
        f"ops.{path}"
        for path in paths
        if _resolve(inmem.ops, path) is not None and _resolve(pg.ops, path) is None
    ]
    assert not missing, "API ops call sites missing:\n" + "\n".join(f"  - {m}" for m in missing)


def test_inmem_ops_surface_on_pg(inmem, pg) -> None:
    service_classes = _ops_service_classes(inmem.ops)
    assert service_classes, "expected inmem.ops to expose service classes"

    missing: list[str] = []
    for cls_name, icls in sorted(service_classes.items()):
        pcls = getattr(pg.ops, cls_name, None)
        if pcls is None:
            missing.append(f"ops.{cls_name}")
            continue
        for name in _own_callables(icls):
            if getattr(pcls, name, None) is None:
                missing.append(f"ops.{cls_name}.{name}")
        for nested_name, inested in _own_classes(icls).items():
            pnested = getattr(pcls, nested_name, None)
            if pnested is None:
                missing.append(f"ops.{cls_name}.{nested_name}")
                continue
            for name in _own_callables(inested):
                if getattr(pnested, name, None) is None:
                    missing.append(f"ops.{cls_name}.{nested_name}.{name}")
    assert not missing, "inmem ops surface missing on pg:\n" + "\n".join(
        f"  - {m}" for m in missing
    )
