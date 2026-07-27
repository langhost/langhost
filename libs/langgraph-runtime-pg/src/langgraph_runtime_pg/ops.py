"""SQL-native Postgres ops for assistants, threads, runs, and crons."""

from __future__ import annotations

import asyncio
import copy
import os
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import Any, Literal, cast
from uuid import UUID, uuid4

import orjson
import structlog
from langgraph.types import StateSnapshot
from sqlalchemy import delete, func, select as sa_select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from starlette.exceptions import HTTPException

from langgraph_runtime_pg.checkpoint import Checkpointer
from langgraph_runtime_pg.database import (
    PgConnectionProto,
    assistant_to_dict,
    assistant_version_to_dict,
    connect,
    cron_to_dict,
    get_session_factory,
    run_to_dict,
    thread_to_dict,
)
from langgraph_runtime_pg.models import (
    AssistantRow,
    AssistantVersionRow,
    CronRow,
    RetryCounterRow,
    RunRow,
    ThreadRow,
)
from langgraph_runtime_pg.redis_stream import (
    ContextQueue,
    Message,
    clear_run_heartbeat,
    get_stream_manager,
    has_run_heartbeat,
    heartbeat_refresh_interval_secs,
    ms_seq_id_gt,
    ms_seq_id_sort_key,
    set_run_heartbeat,
    wake_run_queue,
)

logger = structlog.stdlib.get_logger(__name__)

StreamHandler = ContextQueue

_RUN_NOT_FOUND = "Run not found"


async def _empty_aiter():
    for _ in ():
        yield


def _escape_like(value: str) -> str:
    """Escape ``%``, ``_``, and ``\\`` for SQL LIKE / ILIKE with escape='\\\\'."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _name_ilike(name: str):
    return AssistantRow.name.ilike(f"%{_escape_like(name)}%", escape="\\")


def _patch_interrupt(interrupt: Any) -> dict:
    """JSONB-safe interrupt dict."""
    from langgraph_api.state import patch_interrupt

    return cast(dict, patch_interrupt(interrupt))


def _thread_status_from_checkpoint(
    checkpoint: dict | None,
    exception: BaseException | None,
    *,
    ignore_user_control: bool = False,
) -> tuple[str, dict]:
    """Return ``(base_thread_status, interrupts)`` from a checkpoint snapshot."""
    has_next = bool(checkpoint and checkpoint.get("next"))
    base = _compute_base_status(exception, has_next, ignore_user_control)
    interrupts = _extract_interrupts(checkpoint)
    return base, interrupts


def _compute_base_status(
    exception: BaseException | None, has_next: bool, ignore_user_control: bool
) -> str:
    if not exception:
        return "interrupted" if has_next else "idle"
    if not ignore_user_control:
        return "error"
    from langgraph_api.errors import UserInterrupt, UserRollback

    if not isinstance(exception, (UserInterrupt, UserRollback)):
        return "error"
    return "interrupted" if has_next else "idle"


def _extract_interrupts(checkpoint: dict | None) -> dict:
    if checkpoint is None:
        return {}
    return {
        t["id"]: [_patch_interrupt(i) for i in (t.get("interrupts") or [])]
        for t in (checkpoint.get("tasks") or [])
        if t.get("interrupts")
    }


def _test_mode() -> bool:
    return os.environ.get("LG_RUNTIME_PG_TEST", "") == "1"


def _snapshot_defaults() -> dict:
    """Kwargs for StateSnapshot on langgraph versions that require ``interrupts``."""
    if not hasattr(StateSnapshot, "interrupts"):
        return {}
    return {"interrupts": ()}


def _empty_state_snapshot() -> StateSnapshot:
    return StateSnapshot(
        values={},
        next=(),
        config=cast(Any, None),
        metadata=None,
        created_at=None,
        parent_config=None,
        tasks=(),
        **_snapshot_defaults(),
    )


async def _get_checkpointer(*, unpack_hook=None):
    from langgraph_api import config as api_config

    if getattr(api_config, "USE_CUSTOM_CHECKPOINTER", False):
        from langgraph_api import _checkpointer as api_checkpointer

        return await api_checkpointer.get_checkpointer()
    return Checkpointer(unpack_hook=unpack_hook)


async def _adelete_thread_checkpoints(thread_id: UUID | str) -> None:
    checkpointer = await _get_checkpointer()
    await checkpointer.adelete_thread(str(thread_id))


async def _acopy_thread_checkpoints(source_thread_id: str, target_thread_id: str) -> None:
    """Copy checkpoints; fall back to alist/aput when acopy_thread is unimplemented."""
    checkpointer = await _get_checkpointer()
    try:
        await checkpointer.acopy_thread(source_thread_id, target_thread_id)
        return
    except NotImplementedError:
        pass

    cfg: dict[str, Any] = {"configurable": {"thread_id": source_thread_id}}
    checkpoints = [cp async for cp in checkpointer.alist(cfg)]
    checkpoints.sort(key=lambda x: x.config["configurable"]["checkpoint_id"])
    for cp in checkpoints:
        stored_config = await _copy_single_checkpoint(checkpointer, cp, target_thread_id)
        if cp.pending_writes:
            await _copy_pending_writes(checkpointer, stored_config, cp.pending_writes)


async def _copy_single_checkpoint(checkpointer, cp, target_thread_id: str) -> dict:
    ns = cp.config["configurable"].get("checkpoint_ns", "")
    new_config: dict[str, Any] = {
        "configurable": {
            "thread_id": target_thread_id,
            "checkpoint_ns": ns,
        }
    }
    parent_config = cp.parent_config
    if parent_config and parent_config.get("configurable"):
        parent_id = parent_config["configurable"].get("checkpoint_id")
        if parent_id is not None:
            new_config["configurable"]["checkpoint_id"] = parent_id
    new_metadata = dict(cp.metadata or {})
    if "thread_id" in new_metadata:
        new_metadata["thread_id"] = target_thread_id
    return await checkpointer.aput(
        new_config,
        cp.checkpoint,
        new_metadata,
        cp.checkpoint.get("channel_versions", {}),
    )


async def _copy_pending_writes(checkpointer, stored_config: dict, pending_writes) -> None:
    writes_by_task: dict[str, list[tuple[str, Any]]] = {}
    for task_id, channel, value in pending_writes:
        writes_by_task.setdefault(task_id, []).append((channel, value))
    for task_id, writes in writes_by_task.items():
        await checkpointer.aput_writes(stored_config, writes, task_id)


class Authenticated:
    resource: Literal["threads", "crons", "assistants"] = "threads"

    @classmethod
    def _context(
        cls,
        ctx: Any,
        action: str,
    ) -> Any:
        if not ctx:
            return None
        from langgraph_sdk import Auth

        return Auth.types.AuthContext(
            user=ctx.user,
            permissions=ctx.permissions,
            resource=cls.resource,
            action=cast(Any, action),
        )

    @classmethod
    async def handle_event(
        cls,
        ctx: Any,
        action: str,
        value: Any,
    ) -> Any:
        from langgraph_api.auth.custom import handle_event
        from langgraph_api.utils import get_auth_ctx

        ctx = ctx or get_auth_ctx()
        if not ctx:
            return None
        return await handle_event(cls._context(ctx, action), value)


def _run_stream_mode_matches(event_mode: str, stream_mode: list | str | None) -> bool:
    if not stream_mode:
        return True
    modes = [stream_mode] if isinstance(stream_mode, str) else list(stream_mode)
    if event_mode in modes:
        return True
    if ("messages" in modes or "messages-tuple" in modes) and event_mode.startswith("messages"):
        return True
    if "|" in event_mode:
        base_mode, _, _ = event_mode.partition("|")
        if base_mode in modes:
            return True
    return False


class WrappedHTTPException(Exception):
    def __init__(self, http_exception: HTTPException):
        self.http_exception = http_exception


def _ensure_uuid(id_: str | UUID | None) -> UUID:
    if isinstance(id_, str):
        return UUID(id_)
    if id_ is None:
        return uuid4()
    return id_


# Whitelist sort fields: getattr(Model, "metadata") is SQLAlchemy MetaData, not metadata_.
_ASSISTANT_SORT_FIELDS = frozenset({"assistant_id", "graph_id", "name", "created_at", "updated_at"})
_THREAD_SORT_FIELDS = frozenset(
    {"thread_id", "created_at", "updated_at", "state_updated_at", "status"}
)
_CRON_SORT_FIELDS = frozenset(
    {
        "cron_id",
        "assistant_id",
        "thread_id",
        "next_run_date",
        "end_time",
        "created_at",
        "updated_at",
    }
)


def _resolve_sort_field(
    sort_by: str | None,
    allowed: frozenset[str],
    default: str,
    *,
    raise_invalid: bool = False,
) -> str:
    sb = sort_by.lower() if sort_by else ""
    if sb in allowed:
        return sb
    if sort_by and raise_invalid:
        opts = ", ".join(sorted(allowed))
        raise HTTPException(
            status_code=422,
            detail=f"Invalid sort_by field: '{sort_by}'. Valid options are: {opts}",
        )
    return default


def _row_sort_key(row: Any, attr: str) -> tuple:
    """Nullable-safe sort key — avoids TypeError mixing datetime with None/str."""
    val = getattr(row, attr, None)
    return (val is None, val)


def _thread_search_item(
    row: Any,
    *,
    select: list | None = None,
    extract: dict | None = None,
) -> dict[str, Any]:
    """Build a Threads.search result dict."""
    d = thread_to_dict(row)
    d.setdefault("state_updated_at", d.get("updated_at"))
    if select:
        out = {k: v for k, v in d.items() if k in select}
    else:
        out = d
    if extract:
        from langgraph_api.utils.extract import extract_path_value

        out["extracted"] = {alias: extract_path_value(d, path) for alias, path in extract.items()}
    return out


def _merge_jsonb(*objects: dict | None) -> dict:
    """Shallow JSONB-style merge (later keys win)."""
    result: dict = {}
    for obj in objects:
        if obj:
            result.update(copy.deepcopy(obj))
    return result


def _plain_metadata_filter(filters: Any | None) -> dict | None:
    """Return filters pushable as JSONB ``@>``, or None if operators need Python."""
    if not filters:
        return {}
    if not isinstance(filters, dict):
        return None
    if any(str(k).startswith("$") for k in filters):
        return None
    for value in filters.values():
        if isinstance(value, dict) and any(str(k).startswith("$") for k in value):
            return None
    return filters


def _auth_denies(metadata: dict | None, filters: Any | None) -> bool:
    return bool(filters) and not _check_filter_match(metadata or {}, filters)


def _check_filter_match(
    metadata: dict,
    filters: Any | None,
    nesting_level: int = 0,
) -> bool:
    if not filters:
        return True
    if nesting_level > 2:
        raise HTTPException(status_code=500, detail="Too many nested filter operators")

    if "$or" in filters:
        return _check_logical_group(metadata, filters, "$or", nesting_level)
    if "$and" in filters:
        return _check_logical_group(metadata, filters, "$and", nesting_level)
    return _check_kv_filters(metadata, filters)


def _check_logical_group(metadata: dict, filters: dict, op: str, nesting_level: int) -> bool:
    groups = filters[op]
    if op == "$or":
        matched = any(_check_filter_match(metadata, g, nesting_level + 1) for g in groups)
    else:
        matched = all(_check_filter_match(metadata, g, nesting_level + 1) for g in groups)
    if not matched:
        return False
    remaining = {k: v for k, v in filters.items() if k != op}
    if remaining:
        return _check_filter_match(metadata, remaining, nesting_level + 1)
    return True


def _check_kv_filters(metadata: dict, filters: dict) -> bool:
    for key, value in filters.items():
        if isinstance(value, dict):
            if not _check_operator_filter(metadata, key, value):
                return False
        elif key not in metadata or metadata[key] != value:
            return False
    return True


def _check_operator_filter(metadata: dict, key: str, value: dict) -> bool:
    op = next(iter(value))
    filter_value = value[op]
    if op == "$eq":
        return key in metadata and metadata[key] == filter_value
    if op == "$contains":
        if key not in metadata or not isinstance(metadata[key], list):
            return False
        if isinstance(filter_value, list):
            return all(el in metadata[key] for el in filter_value)
        return filter_value in metadata[key]
    return True


def _assert_graph_exists(graph_id: str) -> None:
    if _test_mode():
        return
    try:
        from langgraph_api.graph import assert_graph_exists

        assert_graph_exists(graph_id)
    except ImportError:
        pass


async def _aget_assistant(session, assistant_id: UUID) -> AssistantRow | None:
    return await session.get(AssistantRow, assistant_id)


async def _aget_thread(session, thread_id: UUID) -> ThreadRow | None:
    return await session.get(ThreadRow, thread_id)


async def _aget_run(session, run_id: UUID) -> RunRow | None:
    return await session.get(RunRow, run_id)


async def _aget_cron(session, cron_id: UUID) -> CronRow | None:
    return await session.get(CronRow, cron_id)


async def _adelete_retry_counters(session, run_ids: Sequence[UUID]) -> None:
    """Drop retry_counters for deleted runs (no FK cascade on that table)."""
    if not run_ids:
        return
    await session.execute(delete(RetryCounterRow).where(RetryCounterRow.run_id.in_(run_ids)))


async def _thread_has_live_worker(session, thread_id: UUID) -> bool:
    """True if any in-flight/cancelled run still heartbeats."""
    run_ids = list(
        (
            await session.execute(
                sa_select(RunRow.run_id).where(
                    RunRow.thread_id == thread_id,
                    RunRow.status.in_(("pending", "running", "interrupted")),
                )
            )
        )
        .scalars()
        .all()
    )
    for rid in run_ids:
        alive = await has_run_heartbeat(rid)
        if alive is not False:
            return True
    return False


async def _thread_has_inflight_work(session, thread_id: UUID) -> bool:
    """Pending/running rows, or a cancelled worker that is still heartbeating."""
    pending = int(
        await session.scalar(
            sa_select(func.count())
            .select_from(RunRow)
            .where(
                RunRow.thread_id == thread_id,
                RunRow.status.in_(("pending", "running")),
            )
        )
        or 0
    )
    if pending:
        return True
    return await _thread_has_live_worker(session, thread_id)


async def _search_with_python_filter(
    conn: PgConnectionProto,
    q,
    filters,
    sort_by: str | None,
    sort_order: str | None,
    offset: int,
    limit: int,
    allowed_fields: frozenset[str],
    default_sort: str,
    to_dict_fn,
    *,
    select: list | None = None,
    extract: dict | None = None,
    item_fn=None,
) -> tuple[AsyncIterator, int | None]:
    """Filter rows in Python, sort, paginate, materialize."""
    rows = list((await conn.session.execute(q)).scalars())
    rows = [r for r in rows if _check_filter_match(r.metadata_ or {}, filters)]
    sb = _resolve_sort_field(sort_by, allowed_fields, default_sort)
    reverse = not (sort_order and sort_order.upper() == "ASC")
    rows.sort(key=lambda r: _row_sort_key(r, sb), reverse=reverse)
    page = rows[offset : offset + limit]
    cursor = offset + limit if len(rows) > offset + limit else None
    items = _materialize_page(page, to_dict_fn, select=select, extract=extract, item_fn=item_fn)

    async def _iter():
        for d in items:
            yield d

    return _iter(), cursor


async def _search_with_db_sort(
    conn: PgConnectionProto,
    q,
    sort_by: str | None,
    sort_order: str | None,
    offset: int,
    limit: int,
    model_cls,
    allowed_fields: frozenset[str],
    default_sort: str,
    to_dict_fn,
    *,
    select: list | None = None,
    extract: dict | None = None,
    item_fn=None,
) -> tuple[AsyncIterator, int | None]:
    """Sort/paginate in DB, materialize dicts while session is open."""
    sb = _resolve_sort_field(sort_by, allowed_fields, default_sort)
    col = getattr(model_cls, sb)
    reverse = not (sort_order and sort_order.upper() == "ASC")
    q = q.order_by(col.desc() if reverse else col.asc())
    q = q.offset(offset).limit(limit + 1)
    rows = list((await conn.session.execute(q)).scalars())
    cursor = offset + limit if len(rows) > limit else None
    page = rows[:limit]
    items = _materialize_page(page, to_dict_fn, select=select, extract=extract, item_fn=item_fn)

    async def _iter():
        for d in items:
            yield d

    return _iter(), cursor


def _materialize_page(
    page,
    to_dict_fn,
    *,
    select: list | None = None,
    extract: dict | None = None,
    item_fn=None,
) -> list[dict]:
    """Convert rows to dicts, optionally projecting fields or using a custom item builder."""
    if item_fn is not None:
        return [item_fn(r, select=select, extract=extract) for r in page]
    items: list[dict] = []
    for r in page:
        d = to_dict_fn(r)
        items.append({k: v for k, v in d.items() if k in select} if select else d)
    return items


def _normalize_config_context(config: dict, context: dict) -> tuple[dict, dict]:
    """Reconcile config.configurable and context; raises if both provided."""
    if config.get("configurable") and context:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both configurable and context.",
        )
    if config.get("configurable"):
        context = config["configurable"]
    elif context:
        config["configurable"] = context
    return config, context


def _handle_existing_assistant(
    existing: AssistantRow | None,
    assistant_id: UUID,
    if_exists: str,
    filters: Any,
) -> AsyncIterator:
    """Return an iterator or raise when an assistant already exists / race-lost."""
    if existing is None or (filters and not _check_filter_match(existing.metadata_ or {}, filters)):
        raise HTTPException(status_code=409, detail=f"Assistant {assistant_id} already exists")
    if if_exists == "raise":
        raise HTTPException(status_code=409, detail=f"Assistant {assistant_id} already exists")
    data = assistant_to_dict(existing)

    async def _yield():
        yield data

    return _yield()


def _handle_existing_thread(
    existing: ThreadRow | None,
    thread_id: UUID,
    if_exists: str,
    filters: Any,
) -> AsyncIterator:
    """Return an iterator or raise when a thread already exists / race-lost."""
    if existing is None or (filters and not _check_filter_match(existing.metadata_ or {}, filters)):
        raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")
    if if_exists == "raise":
        raise HTTPException(status_code=409, detail=f"Thread {thread_id} already exists")
    data = thread_to_dict(existing)

    async def _yield():
        yield data

    return _yield()


def _assistant_where_clauses(graph_id, name, metadata) -> list:
    clauses = []
    if graph_id:
        clauses.append(AssistantRow.graph_id == graph_id)
    if name:
        clauses.append(_name_ilike(name))
    if metadata:
        clauses.append(AssistantRow.metadata_.contains(metadata))
    return clauses


def _thread_where_clauses(metadata, values, status) -> list:
    clauses = []
    if metadata:
        clauses.append(ThreadRow.metadata_.contains(metadata))
    if values:
        clauses.append(ThreadRow.values_.contains(values))
    if status:
        clauses.append(ThreadRow.status == status)
    return clauses


def _cron_where_clauses(assistant_id, thread_id, metadata) -> list:
    clauses = []
    if assistant_id is not None:
        clauses.append(CronRow.assistant_id == _ensure_uuid(assistant_id))
    if thread_id is not None:
        clauses.append(CronRow.thread_id == _ensure_uuid(thread_id))
    if metadata:
        clauses.append(CronRow.metadata_.contains(metadata))
    return clauses


async def _count_with_auth(conn, model_cls, where_clauses, filters) -> int:
    """Count rows with optional Python-side auth filter fallback."""
    plain = _plain_metadata_filter(filters)
    if filters and plain is None:
        q = sa_select(model_cls)
        for clause in where_clauses:
            q = q.where(clause)
        rows = list((await conn.session.execute(q)).scalars())
        return sum(1 for r in rows if _check_filter_match(r.metadata_ or {}, filters))
    count_q = sa_select(func.count()).select_from(model_cls)
    for clause in where_clauses:
        count_q = count_q.where(clause)
    if plain:
        count_q = count_q.where(model_cls.metadata_.contains(plain))
    return int(await conn.session.scalar(count_q) or 0)


async def _apply_assistant_patch_fields(
    session,
    assistant,
    assistant_id,
    new_version_num,
    now,
    *,
    graph_id=None,
    config=None,
    context=None,
    metadata=None,
    name=None,
    description=None,
):
    """Compute merged fields, insert a version row, and update the assistant."""
    new_graph = graph_id if graph_id is not None else assistant.graph_id
    new_config = config if config else assistant.config
    new_context = context if context is not None else (assistant.context or {})
    new_meta = (
        {**(assistant.metadata_ or {}), **metadata} if metadata else (assistant.metadata_ or {})
    )
    new_name = name if name is not None else assistant.name
    new_desc = description if description is not None else assistant.description

    session.add(
        AssistantVersionRow(
            assistant_id=assistant_id,
            version=new_version_num,
            graph_id=new_graph,
            config=new_config,
            context=new_context,
            metadata_=new_meta,
            name=new_name,
            description=new_desc,
            created_at=now,
        )
    )
    assistant.graph_id = new_graph
    assistant.config = new_config
    assistant.context = new_context
    assistant.metadata_ = new_meta
    assistant.name = new_name
    assistant.description = new_desc
    assistant.updated_at = now
    assistant.version = new_version_num
    await session.flush()
    return assistant_to_dict(assistant)


class Assistants(Authenticated):
    resource = "assistants"

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        graph_id: str | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str | None = None,
        sort_order: str | None = None,
        select: list | None = None,
        ctx: Any = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "search",
            Auth.types.AssistantsSearch(
                graph_id=graph_id, metadata=metadata, limit=limit, offset=offset
            ),
        )
        if graph_id is not None:
            _assert_graph_exists(graph_id)

        q = sa_select(AssistantRow)
        if graph_id:
            q = q.where(AssistantRow.graph_id == graph_id)
        if name:
            q = q.where(_name_ilike(name))
        if metadata:
            q = q.where(AssistantRow.metadata_.contains(metadata))
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(AssistantRow.metadata_.contains(plain))
        elif filters:
            return await _search_with_python_filter(
                conn,
                q,
                filters,
                sort_by,
                sort_order,
                offset,
                limit,
                _ASSISTANT_SORT_FIELDS,
                "created_at",
                assistant_to_dict,
                select=select,
            )

        return await _search_with_db_sort(
            conn,
            q,
            sort_by,
            sort_order,
            offset,
            limit,
            AssistantRow,
            _ASSISTANT_SORT_FIELDS,
            "created_at",
            assistant_to_dict,
            select=select,
        )

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        filters = await Assistants.handle_event(
            ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
        )
        # Eagerly load before returning the iterator. langgraph_api often does
        # ``async with connect(): it = await Assistants.get(...)`` then
        # ``await fetchone(it)`` *outside* the connect block. A lazy DB await
        # inside ``_yield`` would re-checkout a pool connection after the
        # session closed and never check it in (GC non-checked-in warning).
        row = await _aget_assistant(conn.session, assistant_id)
        data = None
        if row is not None and (not filters or _check_filter_match(row.metadata_ or {}, filters)):
            data = copy.deepcopy(assistant_to_dict(row))

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        *,
        graph_id: str,
        config: dict | None = None,
        context: dict | None = None,
        metadata: dict | None = None,
        if_exists: str = "raise",
        name: str = "",
        description: str | None = None,
        ctx: Any = None,
        system: bool = False,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        config = config or {}
        context = context or {}
        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "create",
            Auth.types.AssistantsCreate(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata=metadata,
                name=name,
            ),
        )
        _assert_graph_exists(graph_id)
        config, context = _normalize_config_context(config, context)

        existing = await _aget_assistant(conn.session, assistant_id)
        if existing:
            return _handle_existing_assistant(existing, assistant_id, if_exists, filters)

        now = datetime.now(UTC)
        ins = (
            pg_insert(AssistantRow)
            .values(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata_=metadata,
                name=name,
                description=description,
                version=1,
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=["assistant_id"])
        )
        result = await conn.session.execute(ins)
        if result.rowcount == 0:
            existing = await _aget_assistant(conn.session, assistant_id)
            return _handle_existing_assistant(existing, assistant_id, if_exists, filters)

        await conn.session.execute(
            pg_insert(AssistantVersionRow)
            .values(
                assistant_id=assistant_id,
                version=1,
                graph_id=graph_id,
                config=config,
                context=context,
                metadata_=metadata,
                name=name,
                description=description,
                created_at=now,
            )
            .on_conflict_do_nothing(index_elements=["assistant_id", "version"])
        )
        await conn.session.flush()
        row = await _aget_assistant(conn.session, assistant_id)
        data = assistant_to_dict(row)

        async def _yield_new():
            yield data

        return _yield_new()

    @staticmethod
    async def patch(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        *,
        config: dict | None = None,
        context: dict | None = None,
        graph_id: str | None = None,
        metadata: dict | None = None,
        name: str | None = None,
        description: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        config = config if config is not None else {}
        metadata = metadata if metadata is not None else {}
        filters = await Assistants.handle_event(
            ctx,
            "update",
            Auth.types.AssistantsUpdate(
                assistant_id=assistant_id,
                graph_id=graph_id,
                config=config,
                metadata=metadata,
            ),
        )

        if graph_id is not None:
            _assert_graph_exists(graph_id)
        config, context = _normalize_config_context(config, context or {})

        # Lock so concurrent patches cannot allocate the same version (PK on assistant_versions).
        assistant = (
            await conn.session.execute(
                sa_select(AssistantRow)
                .where(AssistantRow.assistant_id == assistant_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
        if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        now = datetime.now(UTC)
        max_ver = await conn.session.scalar(
            sa_select(func.coalesce(func.max(AssistantVersionRow.version), 0)).where(
                AssistantVersionRow.assistant_id == assistant_id
            )
        )
        new_version_num = int(max_ver or 0) + 1

        data = await _apply_assistant_patch_fields(
            conn.session,
            assistant,
            assistant_id,
            new_version_num,
            now,
            graph_id=graph_id,
            config=config,
            context=context,
            metadata=metadata,
            name=name,
            description=description,
        )

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto | None,
        assistant_id: UUID | str,
        ctx: Any = None,
        *,
        delete_threads: bool = False,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        async with AsyncExitStack() as stack:
            if conn is None:
                conn = await stack.enter_async_context(connect())

            assistant_id = _ensure_uuid(assistant_id)
            filters = await Assistants.handle_event(
                ctx, "delete", Auth.types.AssistantsDelete(assistant_id=assistant_id)
            )
            assistant = await _aget_assistant(conn.session, assistant_id)
            if not assistant:
                raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
            if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
                raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

            if delete_threads:
                result = await conn.session.execute(
                    sa_select(ThreadRow.thread_id).where(
                        ThreadRow.metadata_.contains({"assistant_id": str(assistant_id)})
                    )
                )
                for (thread_id,) in result.all():
                    try:
                        async for _deleted in await Threads.delete(conn, thread_id, ctx=ctx):
                            await logger.adebug("cascade-deleted thread", thread_id=str(thread_id))
                    except HTTPException:
                        await logger.awarning(
                            "Skipping thread deletion during cascade delete",
                            thread_id=str(thread_id),
                            assistant_id=str(assistant_id),
                        )

            await Runs.cancel(conn, assistant_id=assistant_id, action="interrupt", ctx=ctx)

            await conn.session.execute(
                delete(AssistantVersionRow).where(AssistantVersionRow.assistant_id == assistant_id)
            )
            await conn.session.execute(delete(CronRow).where(CronRow.assistant_id == assistant_id))
            await conn.session.delete(assistant)
            await conn.session.flush()

            async def _yield():
                yield assistant_id

            return _yield()

    @staticmethod
    async def set_latest(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        version: int,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        filters = await Assistants.handle_event(
            ctx,
            "update",
            Auth.types.AssistantsUpdate(assistant_id=assistant_id, version=version),
        )
        assistant = await _aget_assistant(conn.session, assistant_id)
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")
        if filters and not _check_filter_match(assistant.metadata_ or {}, filters):
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        version_data = await conn.session.get(AssistantVersionRow, (assistant_id, version))
        if not version_data:
            raise HTTPException(status_code=404, detail=f"Version {version} not found")

        assistant.graph_id = version_data.graph_id
        assistant.config = version_data.config
        assistant.context = version_data.context
        assistant.metadata_ = version_data.metadata_
        assistant.version = version_data.version
        assistant.updated_at = datetime.now(UTC)
        assistant.name = version_data.name
        assistant.description = version_data.description
        await conn.session.flush()
        data = assistant_to_dict(assistant)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def get_versions(
        conn: PgConnectionProto,
        assistant_id: UUID | str,
        metadata: dict | None = None,
        limit: int = 10,
        offset: int = 0,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
        )
        assistant = await _aget_assistant(conn.session, assistant_id)
        if not assistant:
            raise HTTPException(status_code=404, detail=f"Assistant {assistant_id} not found")

        q = sa_select(AssistantVersionRow).where(AssistantVersionRow.assistant_id == assistant_id)
        if metadata:
            q = q.where(AssistantVersionRow.metadata_.contains(metadata))
        q = q.order_by(AssistantVersionRow.version.desc()).offset(offset).limit(limit)
        rows = list((await conn.session.execute(q)).scalars())
        # Materialize while session is open — API drains versions outside connect().
        default_name = assistant.name
        default_description = assistant.description
        items: list[dict] = []
        for r in rows:
            d = assistant_version_to_dict(r)
            if filters and not _check_filter_match(d.get("metadata") or {}, filters):
                continue
            d.setdefault("name", default_name)
            d.setdefault("description", default_description)
            items.append(d)

        async def _yield():
            for d in items:
                yield d

        return _yield()

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        graph_id: str | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        ctx: Any = None,
    ) -> int:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        filters = await Assistants.handle_event(
            ctx,
            "search",
            Auth.types.AssistantsSearch(graph_id=graph_id, metadata=metadata, limit=0, offset=0),
        )
        if graph_id is not None:
            _assert_graph_exists(graph_id)

        clauses = _assistant_where_clauses(graph_id, name, metadata)
        return await _count_with_auth(conn, AssistantRow, clauses, filters)


def _accept_message(message: Message, emitted_ids: set[str], resume_cursor: str | None) -> bool:
    """True if this frame should be emitted (cursor + dedup)."""
    if not message.id:
        return True
    mid = message.id.decode() if isinstance(message.id, bytes) else str(message.id)
    if mid in emitted_ids:
        return False
    if resume_cursor is not None and not ms_seq_id_gt(mid, resume_cursor):
        return False
    emitted_ids.add(mid)
    return True


def _rewrite_control_done(event_bytes: bytes, message_bytes: bytes, run_id) -> tuple[bytes, bytes]:
    """Convert control/done into metadata run_done event."""
    if event_bytes == b"control" and message_bytes == b"done":
        return b"metadata", orjson.dumps({"status": "run_done", "run_id": str(run_id)})
    return event_bytes, message_bytes


async def _stream_thread_events_loop(
    created_queues, thread_id, seen_runs, resume_cursor, accept_fn, filter_fn
) -> AsyncIterator[tuple[bytes, bytes, bytes | None, str | None]]:
    """Subscribe-drain loop for live thread events."""
    last_subscribe_mono = 0.0
    subscribe_interval = 0.15
    while True:
        now_mono = time.monotonic()
        last_subscribe_mono, new_qs = await _maybe_resubscribe(
            now_mono,
            last_subscribe_mono,
            subscribe_interval,
            created_queues,
            thread_id,
            seen_runs,
            resume_cursor,
        )
        created_queues.extend(new_qs)

        drained_any = False
        for run_id, queue in created_queues:
            async for frame in _drain_single_queue(run_id, queue, accept_fn, filter_fn):
                yield frame
                drained_any = True

        await asyncio.sleep(0 if drained_any else 0.02)


async def _restore_thread_events(
    stream_manager, thread_id: UUID, restore_cursor: str, accept_fn, should_filter_fn
) -> AsyncIterator[tuple[bytes, bytes, bytes | None, str | None]]:
    """Restore historical events from message stores and DB."""
    from langgraph_api.utils.stream_codec import decode_stream_message

    all_events: list[tuple[Message, UUID]] = []
    run_ids: set[UUID] = set(stream_manager.message_stores.get(thread_id, {}).keys())
    async with connect() as conn:
        result = await conn.session.execute(
            sa_select(RunRow.run_id).where(RunRow.thread_id == thread_id)
        )
        run_ids.update(row[0] for row in result.all())
    for run_id in run_ids:
        for message in await stream_manager.restore_messages_async(
            run_id, thread_id, restore_cursor
        ):
            all_events.append((message, run_id))
    all_events.sort(key=lambda x: ms_seq_id_sort_key((x[0].id or b"").decode()))
    for message, run_id in all_events:
        if not accept_fn(message):
            continue
        try:
            decoded = decode_stream_message(message.data, channel=message.topic)
        except (ValueError, KeyError):
            continue
        event_bytes, message_bytes = _rewrite_control_done(
            decoded.event_bytes, decoded.message_bytes, run_id
        )
        if not should_filter_fn(event_bytes.decode("utf-8"), message_bytes):
            yield (event_bytes, message_bytes, message.id, str(run_id))


def _make_event_filter(stream_modes: list):
    """Return a filter function that returns True when an event should be dropped."""

    def should_filter_event(event_name: str, message_bytes: bytes) -> bool:
        if "run_modes" in stream_modes and event_name != "state_update":
            return False
        if "state_update" in stream_modes and event_name == "state_update":
            return False
        if "lifecycle" in stream_modes and event_name == "metadata":
            return _lifecycle_passes(message_bytes)
        return True

    return should_filter_event


def _lifecycle_passes(message_bytes: bytes) -> bool:
    """Return False (don't filter) if lifecycle metadata matches."""
    try:
        message_data = orjson.loads(message_bytes)
    except (orjson.JSONDecodeError, TypeError):
        return True
    if message_data.get("status") == "run_done":
        return False
    return not ("attempt" in message_data and "run_id" in message_data)


async def _maybe_resubscribe(
    now_mono,
    last_subscribe_mono,
    subscribe_interval,
    created_queues,
    thread_id,
    seen_runs,
    resume_cursor,
) -> tuple[float, list]:
    """Re-subscribe if enough time has elapsed; returns (new_last_subscribe_mono, new_queues)."""
    if now_mono - last_subscribe_mono < subscribe_interval and created_queues:
        return last_subscribe_mono, []
    async with connect() as conn:
        new_queue_tuples = await Threads.Stream.subscribe(
            conn,
            thread_id,
            seen_runs,
            after_id=resume_cursor,
        )
    return now_mono, new_queue_tuples


async def _cleanup_stream_queues(created_queues, thread_id, stream_manager):
    """Remove all queues from the stream manager on exit."""
    for key, queue in created_queues:
        try:
            if key == thread_id:
                await stream_manager.remove_thread_stream(thread_id, queue)
            else:
                await stream_manager.remove_queue(key, thread_id, queue)
        except Exception:
            pass


async def _drain_single_queue(
    run_id: UUID, queue: asyncio.Queue, accept_fn, should_filter_fn
) -> AsyncIterator[tuple[bytes, bytes, bytes | None, str | None]]:
    """Drain messages from a single queue, yielding filtered frames."""
    from langgraph_api.utils.stream_codec import decode_stream_message

    while True:
        try:
            message = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        if not accept_fn(message):
            continue
        try:
            decoded = decode_stream_message(message.data, channel=message.topic)
        except (ValueError, KeyError):
            continue
        event = decoded.event_bytes
        payload = decoded.message_bytes

        if event == b"control" and payload == b"done":
            frame = _make_done_frame(message, run_id, should_filter_fn)
            if frame is not None:
                yield frame
        elif not should_filter_fn(event.decode("utf-8"), payload):
            yield (event, payload, message.id, str(run_id))
            await asyncio.sleep(0)


def _make_done_frame(
    message: Message, run_id: UUID, should_filter_fn
) -> tuple[bytes, bytes, bytes | None, str] | None:
    """Build a run_done metadata frame from a control/done message."""
    topic = message.topic.decode() if isinstance(message.topic, bytes) else message.topic
    done_run_id = topic.split("run:")[1].split(":")[0]
    meta_payload = orjson.dumps({"status": "run_done", "run_id": done_run_id})
    if not should_filter_fn("metadata", meta_payload):
        return (b"metadata", meta_payload, message.id, done_run_id)
    return None


class Threads(Authenticated):
    resource = "threads"

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        ids: list | None = None,
        metadata: dict | None = None,
        values: dict | None = None,
        status: str | None = None,
        limit: int = 10,
        offset: int = 0,
        sort_by: str | None = None,
        sort_order: str | None = None,
        select: list | None = None,
        extract: dict | None = None,
        ctx: Any = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        values = values or {}
        filters = await Threads.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(
                metadata=metadata,
                values=values,
                status=cast(Any, status),
                limit=limit,
                offset=offset,
            ),
        )
        q = sa_select(ThreadRow)
        if ids:
            id_set = [_ensure_uuid(i) for i in ids]
            q = q.where(ThreadRow.thread_id.in_(id_set))
        if metadata:
            q = q.where(ThreadRow.metadata_.contains(metadata))
        if values:
            q = q.where(ThreadRow.values_.contains(values))
        if status:
            q = q.where(ThreadRow.status == status)
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(ThreadRow.metadata_.contains(plain))
        elif filters:
            return await _search_with_python_filter(
                conn,
                q,
                filters,
                sort_by,
                sort_order,
                offset,
                limit,
                _THREAD_SORT_FIELDS,
                "updated_at",
                thread_to_dict,
                select=select,
                extract=extract,
                item_fn=_thread_search_item,
            )

        return await _search_with_db_sort(
            conn,
            q,
            sort_by,
            sort_order,
            offset,
            limit,
            ThreadRow,
            _THREAD_SORT_FIELDS,
            "updated_at",
            thread_to_dict,
            select=select,
            extract=extract,
            item_fn=_thread_search_item,
        )

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
        include_ttl: bool = False,
        read_mask_paths: list | None = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id)
        )
        row = await _aget_thread(conn.session, thread_id)
        if not row or (filters and not _check_filter_match(row.metadata_ or {}, filters)):
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
        d = thread_to_dict(row)
        d.setdefault("state_updated_at", d.get("updated_at"))

        async def _yield():
            yield d

        return _yield()

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        metadata: dict | None = None,
        if_exists: str = "raise",
        ttl: Any = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        metadata = metadata or {}
        filters = await Threads.handle_event(
            ctx,
            "create",
            Auth.types.ThreadsCreate(
                thread_id=thread_id,
                metadata=metadata,
                if_exists=cast(Any, if_exists),
            ),
        )

        existing = await _aget_thread(conn.session, thread_id)
        if existing:
            return _handle_existing_thread(existing, thread_id, if_exists, filters)

        now = datetime.now(UTC)
        ins = (
            pg_insert(ThreadRow)
            .values(
                thread_id=thread_id,
                created_at=now,
                updated_at=now,
                state_updated_at=now,
                metadata_=copy.deepcopy(metadata),
                status="idle",
                config={},
                values_=None,
                interrupts={},
            )
            .on_conflict_do_nothing(index_elements=["thread_id"])
        )
        result = await conn.session.execute(ins)
        if result.rowcount == 0:
            existing = await _aget_thread(conn.session, thread_id)
            return _handle_existing_thread(existing, thread_id, if_exists, filters)

        await conn.session.flush()
        row = await _aget_thread(conn.session, thread_id)
        data = thread_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def patch(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        metadata: dict | None = None,
        ttl: Any = None,
        ctx: Any = None,
        read_mask_paths: list | None = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx,
            "update",
            Auth.types.ThreadsUpdate(thread_id=thread_id, metadata=cast(Any, metadata or {})),
        )
        row = await _aget_thread(conn.session, thread_id)
        if row is not None and (not filters or _check_filter_match(row.metadata_ or {}, filters)):
            if metadata:
                row.metadata_ = {**(row.metadata_ or {}), **metadata}
                row.updated_at = datetime.now(UTC)
                await conn.session.flush()
            d = thread_to_dict(row)
            d.setdefault("state_updated_at", d.get("updated_at"))

            async def _yield():
                yield d

            return _yield()

        return _empty_aiter()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Threads.handle_event(
            ctx, "delete", Auth.types.ThreadsDelete(thread_id=thread_id)
        )
        row = await _aget_thread(conn.session, thread_id)
        if row is None or (filters and not _check_filter_match(row.metadata_ or {}, filters)):
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")

        run_ids = list(
            (
                await conn.session.execute(
                    sa_select(RunRow.run_id).where(RunRow.thread_id == thread_id)
                )
            )
            .scalars()
            .all()
        )
        await _adelete_retry_counters(conn.session, run_ids)
        await conn.session.execute(delete(RunRow).where(RunRow.thread_id == thread_id))
        await conn.session.execute(delete(CronRow).where(CronRow.thread_id == thread_id))
        await conn.session.delete(row)
        await conn.session.flush()
        # Checkpointer is a separate autocommit pool — delete after ops commit so we never roll back the thread after checkpoints are gone.
        deleted_id = thread_id

        async def _cleanup_checkpoints() -> None:
            try:
                await _adelete_thread_checkpoints(deleted_id)
            except Exception:
                logger.exception(
                    "Failed to delete checkpoints for thread %s",
                    deleted_id,
                )

        conn.schedule_after_commit(_cleanup_checkpoints)

        async def _yield():
            yield deleted_id

        return _yield()

    @staticmethod
    async def set_joint_status(
        conn: PgConnectionProto,
        thread_id: UUID,
        run_id: UUID,
        run_status: str,
        graph_id: str,
        checkpoint: dict | None = None,
        exception: BaseException | None = None,
    ) -> None:
        """Atomically update run + thread status; uses a fresh session (worker may close outer conn)."""
        from langgraph_api.serde import json_dumpb, json_loads

        thread_id = _ensure_uuid(thread_id)
        run_id = _ensure_uuid(run_id)

        base_thread_status, interrupts = _thread_status_from_checkpoint(
            checkpoint, exception, ignore_user_control=True
        )
        now = datetime.now(UTC)
        values = (
            json_loads(json_dumpb(checkpoint.get("values"))) if checkpoint is not None else None
        )
        error = json_loads(json_dumpb(exception)) if exception else None

        sf = get_session_factory()
        async with sf() as session:
            thread = await _aget_thread(session, thread_id)
            if thread is None:
                raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")
            run = await _aget_run(session, run_id)
            if run is None or run.thread_id != thread_id:
                raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

            if run_status == "rollback":
                await _adelete_retry_counters(session, [run_id])
                await session.delete(run)
                await session.flush()
            else:
                run.status = run_status
                run.updated_at = now
                await session.flush()

            pending = int(
                await session.scalar(
                    sa_select(func.count())
                    .select_from(RunRow)
                    .where(
                        RunRow.thread_id == thread_id,
                        RunRow.status.in_(("pending", "running")),
                    )
                )
                or 0
            )

            meta = dict(thread.metadata_ or {})
            meta["graph_id"] = graph_id
            thread.metadata_ = meta
            thread.status = "busy" if pending else base_thread_status
            thread.updated_at = now
            thread.state_updated_at = now
            thread.interrupts = interrupts
            thread.error = error
            # Always write values (including None) when a checkpoint is present so stale values cannot linger.
            if checkpoint is not None:
                thread.values_ = values
            await session.commit()

    @staticmethod
    async def set_status(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        checkpoint: dict | None,
        exception: BaseException | None,
    ) -> None:
        from langgraph_api.serde import json_dumpb, json_loads

        thread_id = _ensure_uuid(thread_id)
        thread = await _aget_thread(conn.session, thread_id)
        if not thread:
            raise HTTPException(status_code=404, detail=f"Thread {thread_id} not found")

        status, interrupts = _thread_status_from_checkpoint(checkpoint, exception)
        pending = await conn.session.scalar(
            sa_select(func.count())
            .select_from(RunRow)
            .where(
                RunRow.thread_id == thread_id,
                RunRow.status.in_(("pending", "running")),
            )
        )
        if pending:
            status = "busy"

        now = datetime.now(UTC)
        thread.updated_at = now
        thread.state_updated_at = now
        thread.status = status
        if checkpoint is not None:
            thread.values_ = json_loads(json_dumpb(checkpoint.get("values")))
        thread.interrupts = interrupts
        thread.error = json_loads(json_dumpb(exception)) if exception else None
        await conn.session.flush()

    @staticmethod
    async def copy(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        read_filters = await Threads.handle_event(
            ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id)
        )
        original = await _aget_thread(conn.session, thread_id)
        if not original or (
            read_filters and not _check_filter_match(original.metadata_ or {}, read_filters)
        ):
            return _empty_aiter()

        new_id = uuid4()
        meta = copy.deepcopy(original.metadata_ or {})
        await Threads.handle_event(
            ctx,
            "create",
            Auth.types.ThreadsCreate(thread_id=new_id, metadata=meta, if_exists="raise"),
        )
        now = datetime.now(UTC)
        row = ThreadRow(
            thread_id=new_id,
            created_at=now,
            updated_at=now,
            state_updated_at=now,
            metadata_=meta,
            status="idle",
            config=copy.deepcopy(original.config or {}),
            values_=copy.deepcopy(original.values_),
            interrupts=copy.deepcopy(original.interrupts or {}),
        )
        conn.session.add(row)
        await conn.session.flush()
        try:
            await _acopy_thread_checkpoints(str(thread_id), str(new_id))
        except Exception:
            # Separate autocommit pool — scrub target checkpoints if copy fails before this conn commits.
            try:
                await _adelete_thread_checkpoints(new_id)
            except Exception:
                logger.exception(
                    "Failed to scrub checkpoints after copy failure",
                    source=str(thread_id),
                    target=str(new_id),
                )
            raise

        # Checkpoints already durable; scrub target on ops txn rollback to avoid orphans.
        copied_target = new_id

        async def _scrub_orphaned_checkpoints() -> None:
            try:
                await _adelete_thread_checkpoints(copied_target)
            except Exception:
                logger.exception(
                    "Failed to scrub checkpoints after thread copy rollback",
                    source=str(thread_id),
                    target=str(copied_target),
                )

        conn.schedule_after_rollback(_scrub_orphaned_checkpoints)
        data = thread_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        metadata: dict | None = None,
        values: dict | None = None,
        status: str | None = None,
        ctx: Any = None,
    ) -> int:
        from langgraph_sdk import Auth

        metadata = metadata or {}
        values = values or {}
        filters = await Threads.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(
                metadata=metadata,
                values=values,
                status=cast(Any, status),
                limit=0,
                offset=0,
            ),
        )
        clauses = _thread_where_clauses(metadata, values, status)
        return await _count_with_auth(conn, ThreadRow, clauses, filters)

    @staticmethod
    async def prune(
        thread_ids: Sequence[str] | Sequence[UUID],
        strategy: Literal["delete", "keep_latest"] = "delete",
        batch_size: int = 100,
        ctx: Any = None,
    ) -> int:
        """Prune threads by ID. ``langgraph_api`` calls this without a conn."""
        del batch_size  # reserved for batched deletes; unused for now
        if not thread_ids:
            return 0
        if strategy == "keep_latest":
            # AsyncPostgresSaver inherits a stub aprune that raises NotImplementedError.
            checkpointer = await _get_checkpointer()
            try:
                await checkpointer.aprune(list(thread_ids), strategy=strategy)
            except (NotImplementedError, RuntimeError) as exc:  # NOSONAR - explicit 422 mapping for both
                raise HTTPException(
                    status_code=422,
                    detail="keep_latest strategy is not supported by this checkpointer",
                ) from exc
            return len(thread_ids)

        pruned = 0
        async with connect() as conn:
            for tid in thread_ids:
                try:
                    result = await Threads.delete(conn, tid, ctx=ctx)
                    async for _ in result:
                        pruned += 1
                except HTTPException:
                    pass
        return pruned

    @staticmethod
    async def sweep_ttl(
        conn: PgConnectionProto,
        *,
        limit: int | None = None,
        batch_size: int = 100,
    ) -> tuple[int, int]:
        """TTL sweep — no thread TTL column yet; no-op."""
        del conn, limit, batch_size
        return (0, 0)

    class State(Authenticated):
        resource = "threads"

        @staticmethod
        async def get(
            conn: PgConnectionProto,
            config: dict,
            subgraphs: bool = False,
            ctx: Any = None,
        ):
            from langgraph_api.graph import get_graph
            from langgraph_api.store import get_store

            checkpointer = await _get_checkpointer()
            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            try:
                thread = await anext(await Threads.get(conn, thread_id, ctx=ctx))
            except (HTTPException, StopAsyncIteration):
                return _empty_state_snapshot()

            metadata = thread.get("metadata", {}) or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }

            graph_id = metadata.get("graph_id")
            if not graph_id:
                result = await conn.session.execute(
                    sa_select(RunRow)
                    .where(RunRow.thread_id == thread_id)
                    .order_by(RunRow.created_at.desc())
                    .limit(1)
                )
                run = result.scalar_one_or_none()
                if run is not None:
                    try:
                        graph_id = (
                            (run.kwargs or {})
                            .get("config", {})
                            .get("configurable", {})
                            .get("graph_id")
                        )
                    except Exception:
                        graph_id = None

            if not graph_id:
                return _empty_state_snapshot()

            if hasattr(checkpointer, "latest_iter"):
                checkpointer.latest_iter = await checkpointer.aget(config)
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.read",
            ) as graph:
                result = await graph.aget_state(config, subgraphs=subgraphs)
                if (
                    result.metadata is not None
                    and "checkpoint_ns" in result.metadata
                    and result.metadata["checkpoint_ns"] == ""
                ):
                    result.metadata.pop("checkpoint_ns")
                return result

        @staticmethod
        async def post(
            conn: PgConnectionProto,
            config: dict,
            values: Any = None,
            as_node: str | None = None,
            ctx: Any = None,
        ):
            from langgraph_api.graph import get_graph
            from langgraph_api.schema import ThreadUpdateResponse
            from langgraph_api.serde import json_dumpb
            from langgraph_api.state import (
                state_snapshot_to_thread_state,
            )
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone
            from langgraph_sdk import Auth

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            filters = await Threads.handle_event(
                ctx,
                "update",
                Auth.types.ThreadsUpdate(thread_id=thread_id),
            )
            checkpointer = await _get_checkpointer()
            thread = await fetchone(
                await Threads.get(conn, thread_id, ctx=ctx),
                not_found_detail=f"Thread {thread_id} not found.",
            )
            if filters and not _check_filter_match(thread.get("metadata") or {}, filters):
                raise HTTPException(status_code=403, detail="Forbidden")

            if await _thread_has_inflight_work(conn.session, thread_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"Thread {thread_id} has in-flight runs",
                )

            metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = metadata.get("graph_id")
            if not graph_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Thread '{thread_id}' has no assigned graph ID. "
                        "Make a run first or set metadata.graph_id."
                    ),
                )

            config["configurable"].setdefault("graph_id", graph_id)
            if hasattr(checkpointer, "latest_iter"):
                checkpointer.latest_iter = await checkpointer.aget(config)
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.update",
            ) as graph:
                update_config = config.copy()
                update_config["configurable"] = {
                    **config["configurable"],
                    "checkpoint_ns": config["configurable"].get("checkpoint_ns", ""),
                }
                next_config = await graph.aupdate_state(update_config, values, as_node=as_node)
                state = await Threads.State.get(conn, config, subgraphs=False, ctx=ctx)
                await Threads.set_status(
                    conn,
                    thread_id,
                    {
                        "next": list(state.next),
                        "values": state.values,
                        "tasks": [
                            {"id": t.id, "interrupts": list(t.interrupts)} for t in state.tasks
                        ],
                    },
                    None,
                )
                await Threads.Stream.publish(
                    thread_id,
                    "state_update",
                    json_dumpb(
                        {
                            "state": state_snapshot_to_thread_state(state),
                            "thread_id": str(thread_id),
                        }
                    ),
                )
                return ThreadUpdateResponse(
                    checkpoint=next_config["configurable"],
                    configurable=next_config["configurable"],
                    checkpoint_id=next_config["configurable"]["checkpoint_id"],
                )

        @staticmethod
        async def bulk(
            conn: PgConnectionProto,
            *,
            config: dict,
            supersteps: Sequence[dict],
            ctx: Any = None,
        ):
            """Apply a batch of state updates."""
            from langgraph.types import StateUpdate
            from langgraph_api.command import map_cmd
            from langgraph_api.graph import get_graph
            from langgraph_api.schema import ThreadUpdateResponse
            from langgraph_api.serde import json_dumpb
            from langgraph_api.state import (
                state_snapshot_to_thread_state,
            )
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone
            from langgraph_sdk import Auth

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            filters = await Threads.handle_event(
                ctx,
                "update",
                Auth.types.ThreadsUpdate(thread_id=thread_id),
            )
            thread = await fetchone(
                await Threads.get(conn, thread_id, ctx=ctx),
                not_found_detail=f"Thread {thread_id} not found.",
            )
            if filters and not _check_filter_match(thread.get("metadata") or {}, filters):
                raise HTTPException(status_code=403, detail="Forbidden")

            if await _thread_has_inflight_work(conn.session, thread_id):
                raise HTTPException(
                    status_code=409,
                    detail=f"Thread {thread_id} has in-flight runs",
                )

            metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = metadata.get("graph_id")
            if not graph_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Thread '{thread_id}' has no assigned graph ID. "
                        "Make a run first or set metadata.graph_id."
                    ),
                )

            config["configurable"].setdefault("graph_id", graph_id)
            config["configurable"].setdefault("checkpoint_ns", "")
            checkpointer = await _get_checkpointer()
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.update",
            ) as graph:
                next_config = await graph.abulk_update_state(
                    config,
                    [
                        [
                            StateUpdate(
                                (
                                    map_cmd(update.get("command"))
                                    if update.get("command")
                                    else update.get("values")
                                ),
                                update.get("as_node"),
                            )
                            for update in superstep.get("updates", [])
                        ]
                        for superstep in supersteps
                    ],
                )
                state = await Threads.State.get(conn, config, subgraphs=False, ctx=ctx)
                await Threads.set_status(
                    conn,
                    thread_id,
                    {
                        "next": list(state.next),
                        "values": state.values,
                        "tasks": [
                            {"id": t.id, "interrupts": list(t.interrupts)} for t in state.tasks
                        ],
                    },
                    None,
                )
                await Threads.Stream.publish(
                    thread_id,
                    "state_update",
                    json_dumpb(
                        {
                            "state": state_snapshot_to_thread_state(state),
                            "thread_id": str(thread_id),
                        }
                    ),
                )
                return ThreadUpdateResponse(checkpoint=next_config["configurable"])

        @staticmethod
        async def list(
            conn: PgConnectionProto,
            *,
            config: dict,
            limit: int = 1,
            before: Any = None,
            metadata: dict | None = None,
            ctx: Any = None,
        ) -> list:
            from langgraph_api.graph import get_graph
            from langgraph_api.store import get_store
            from langgraph_api.utils import fetchone

            thread_id = _ensure_uuid(config["configurable"]["thread_id"])
            thread = await fetchone(await Threads.get(conn, thread_id, ctx=ctx))
            thread_metadata = thread.get("metadata") or {}
            thread_config = cast(dict[str, Any], thread.get("config") or {})
            thread_config = {
                **thread_config,
                "configurable": {
                    **thread_config.get("configurable", {}),
                    **config.get("configurable", {}),
                },
            }
            graph_id = thread_metadata.get("graph_id")
            if not graph_id:
                return []

            checkpointer = await _get_checkpointer()
            async with get_graph(
                graph_id,
                thread_config,
                checkpointer=checkpointer,
                store=(await get_store()),
                access_context="threads.read",
            ) as graph:
                before_param = (
                    {"configurable": {"checkpoint_id": before}}
                    if isinstance(before, str)
                    else before
                )
                return [
                    state
                    async for state in graph.aget_state_history(
                        config, limit=limit, filter=metadata, before=before_param
                    )
                ]

    class Stream(Authenticated):
        resource = "threads"

        @staticmethod
        async def subscribe(
            conn: PgConnectionProto,
            thread_id: UUID,
            seen_runs: set[UUID],
            *,
            after_id: str | None = None,
            replay: bool = True,
        ):
            sm = get_stream_manager()
            queues = []
            thread_id = _ensure_uuid(thread_id)
            if thread_id not in seen_runs:
                q = await sm.add_thread_stream(thread_id)
                queues.append((thread_id, q))
                seen_runs.add(thread_id)
            result = await conn.session.execute(
                sa_select(RunRow).where(RunRow.thread_id == thread_id)
            )
            for run in result.scalars():
                rid = run.run_id
                if rid not in seen_runs:
                    q = await sm.add_queue(rid, thread_id, after_id=after_id, replay=replay)
                    queues.append((rid, q))
                    seen_runs.add(rid)
            return queues

        @staticmethod
        async def join(
            thread_id: UUID,
            *,
            last_event_id: str | None = None,
            stream_modes: list | None = None,
            ctx: Any = None,
        ) -> AsyncIterator[tuple[bytes, bytes, bytes | None]]:
            async for event, payload, stream_id, _run_id in Threads.Stream.join_event_streaming(
                thread_id,
                last_event_id=last_event_id,
                stream_modes=stream_modes or [],
                ctx=ctx,
            ):
                yield event, payload, stream_id

        @staticmethod
        async def join_event_streaming(
            thread_id: UUID,
            *,
            last_event_id: str | None = None,
            stream_modes: list | None = None,
            ctx: Any = None,
        ) -> AsyncIterator[tuple[bytes, bytes, bytes | None, str | None]]:
            """Stream thread output (Protocol v3)."""
            await Threads.Stream.check_thread_stream_auth(thread_id, ctx)

            stream_modes = list(stream_modes or [])
            stream_manager = get_stream_manager()
            seen_runs: set[UUID] = set()
            created_queues: list[tuple[UUID, asyncio.Queue]] = []
            thread_id = _ensure_uuid(thread_id)
            from_beginning = last_event_id in ("-", "")
            resume_cursor = None if last_event_id is None or from_beginning else last_event_id
            restore_cursor = "-" if from_beginning else resume_cursor
            emitted_ids: set[str] = set()

            def _accept(message: Message) -> bool:
                return _accept_message(message, emitted_ids, resume_cursor)

            filter_fn = _make_event_filter(stream_modes)

            try:
                await logger.ainfo("Joined thread stream", thread_id=str(thread_id))

                if restore_cursor is not None:
                    async for frame in _restore_thread_events(
                        stream_manager, thread_id, restore_cursor, _accept, filter_fn
                    ):
                        yield frame

                async for frame in _stream_thread_events_loop(
                    created_queues, thread_id, seen_runs, resume_cursor, _accept, filter_fn
                ):
                    yield frame
            except WrappedHTTPException as e:
                raise e.http_exception from None
            except asyncio.CancelledError:
                await logger.awarning("Thread stream client disconnected", thread_id=str(thread_id))
                raise
            finally:
                await _cleanup_stream_queues(created_queues, thread_id, stream_manager)

        @staticmethod
        async def publish(thread_id: UUID | str, event: str, message: bytes) -> None:
            from langgraph_api.utils.stream_codec import STREAM_CODEC

            topic = f"thread:{thread_id}:stream".encode()
            sm = get_stream_manager()
            payload = STREAM_CODEC.encode(event, message)
            await sm.put_thread(_ensure_uuid(thread_id), Message(topic=topic, data=payload))

        @staticmethod
        async def check_thread_stream_auth(
            thread_id: UUID,
            ctx: Any = None,
        ) -> None:
            from langgraph_sdk import Auth

            async with connect() as conn:
                filters = await Threads.Stream.handle_event(
                    ctx,
                    "read",
                    Auth.types.ThreadsRead(thread_id=thread_id),
                )
                if filters:
                    try:
                        thread = await anext(await Threads.get(conn, thread_id, ctx=ctx))
                    except (HTTPException, StopAsyncIteration):
                        thread = None
                    if not thread or not _check_filter_match(thread.get("metadata") or {}, filters):
                        raise HTTPException(status_code=404, detail="Thread not found")


async def _sweep_single_run(sf, run_id: UUID, retry_counter, max_retries: int) -> bool:
    """Reclaim one stale run. Returns True if swept."""
    attempts = await retry_counter.get(run_id)
    new_status = "error" if attempts >= max_retries else "pending"
    async with sf() as session:
        result = await session.execute(
            text(
                """
                UPDATE runs
                SET status = :status, updated_at = now()
                WHERE run_id = :run_id AND status = 'running'
                RETURNING run_id, thread_id
                """
            ),
            {"status": new_status, "run_id": run_id},
        )
        row = result.first()
        if row is not None and new_status == "error" and row[1] is not None:
            await _mark_thread_error_if_idle(session, row[1])
        await session.commit()
    if row is None:
        return False
    await clear_run_heartbeat(run_id)
    await logger.awarning(
        "Swept stale run", run_id=str(run_id), new_status=new_status, attempts=attempts
    )
    return True


async def _has_heartbeating_sibling(session, thread_id: UUID, run_id: UUID) -> bool:
    """True if any sibling run on the same thread still has a heartbeat."""
    sibling_ids = list(
        (
            await session.execute(
                sa_select(RunRow.run_id).where(
                    RunRow.thread_id == thread_id,
                    RunRow.run_id != run_id,
                    RunRow.status.in_(("running", "interrupted")),
                )
            )
        )
        .scalars()
        .all()
    )
    for sid in sibling_ids:
        if await has_run_heartbeat(sid) is not False:
            return True
    return False


async def _claim_next_run(sf, blocked_threads, retry_counter_cls):
    """Try to claim the next pending run.

    Returns a 5-tuple ``(kind, run_id, thread_id, run_dict, attempt)`` where
    *kind* is one of ``"empty"``, ``"blocked"``, or ``"claimed"``.
    """
    async with sf() as session:
        exclude_sql, params = _build_exclude_sql(blocked_threads)
        result = await session.execute(
            text(
                f"""
                UPDATE runs SET status = 'running', updated_at = now()
                WHERE run_id = (
                    SELECT r.run_id FROM runs r
                    WHERE r.status = 'pending'
                      AND r.created_at <= now()
                      AND NOT EXISTS (
                          SELECT 1 FROM runs r2
                          WHERE r2.thread_id IS NOT NULL
                            AND r2.thread_id = r.thread_id
                            AND r2.status = 'running'
                      )
                      {exclude_sql}
                    ORDER BY r.created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING run_id
                """
            ),
            params,
        )
        row = result.first()
        if row is None:
            return ("empty", None, None, None, None)
        run_id = row[0]
        run_row = await session.get(RunRow, run_id)
        if run_row is None:
            await session.commit()
            return ("empty", None, None, None, None)
        if run_row.thread_id is not None:
            blocked_by_hb = await _has_heartbeating_sibling(session, run_row.thread_id, run_id)
            if blocked_by_hb:
                run_row.status = "pending"
                await session.commit()
                return ("blocked", run_id, run_row.thread_id, None, None)
        attempt = await retry_counter_cls(sf).increment(run_id, session=session)
        run_dict = run_to_dict(run_row)
        await set_run_heartbeat(run_id)
        try:
            await session.commit()
        except Exception:
            await clear_run_heartbeat(run_id)
            raise
        return ("claimed", run_id, None, run_dict, attempt)


def _build_exclude_sql(blocked_threads: list) -> tuple[str, dict]:
    if not blocked_threads:
        return "", {}
    placeholders = ", ".join(f":bt_{i}" for i in range(len(blocked_threads)))
    exclude_sql = f"AND (r.thread_id IS NULL OR r.thread_id NOT IN ({placeholders}))"
    params = {f"bt_{i}": t for i, t in enumerate(blocked_threads)}
    return exclude_sql, params


async def _ensure_run_thread(
    conn: PgConnectionProto,
    thread_id: UUID | None,
    assistant,
    config: dict,
    metadata: dict,
    assistant_id: UUID,
) -> tuple[UUID, ThreadRow | None, bool]:
    """Create a thread for a run if needed. Returns (thread_id, row, was_created)."""
    if thread_id is None:
        thread_id = uuid4()
    now = datetime.now(UTC)
    thread_meta = {
        "graph_id": assistant.graph_id,
        "assistant_id": str(assistant_id),
        **(config.get("metadata") or {}),
        **metadata,
    }
    thread_config = _merge_jsonb(
        assistant.config or {},
        config,
        {"configurable": _merge_jsonb((assistant.config or {}).get("configurable") or {})},
    )
    ins = (
        pg_insert(ThreadRow)
        .values(
            thread_id=thread_id,
            status="busy",
            metadata_=thread_meta,
            config=thread_config,
            created_at=now,
            updated_at=now,
            state_updated_at=now,
            values_=None,
            interrupts={},
        )
        .on_conflict_do_nothing(index_elements=["thread_id"])
    )
    result = await conn.session.execute(ins)
    if result.rowcount == 0:
        row = await _aget_thread(conn.session, thread_id)
        return thread_id, row, False
    await conn.session.flush()
    row = await _aget_thread(conn.session, thread_id)
    return thread_id, row, True


async def _mark_thread_error_if_idle(session, tid: UUID) -> None:
    """Mark a thread as error if no pending/running runs remain."""
    pending = int(
        await session.scalar(
            sa_select(func.count())
            .select_from(RunRow)
            .where(RunRow.thread_id == tid, RunRow.status.in_(("pending", "running")))
        )
        or 0
    )
    if pending != 0:
        return
    thread = await _aget_thread(session, tid)
    if thread is None:
        return
    thread.status = "error"
    thread.updated_at = datetime.now(UTC)
    if thread.error is None:
        thread.error = {
            "error": "RuntimeError",
            "message": "Run swept after exceeding max retries",
        }


def _run_join_accept(message: Message, emitted_ids: set, resume_cursor) -> bool:
    """Accept filter for run join streaming — dedup + cursor check."""
    if not message.id:
        return True
    mid = message.id.decode() if isinstance(message.id, bytes) else str(message.id)
    if mid in emitted_ids:
        return False
    if resume_cursor is not None and not ms_seq_id_gt(mid, resume_cursor):
        return False
    emitted_ids.add(mid)
    return True


async def _restore_run_messages(
    run_id,
    thread_id,
    last_event_id,
    accept_fn,
    stream_mode,
    decode_fn,
) -> tuple[list, bool]:
    """Replay stored messages; return (frames_list, is_done)."""
    frames: list = []
    messages = await get_stream_manager().restore_messages_async(run_id, thread_id, last_event_id)
    for message in messages:
        if not accept_fn(message):
            continue
        decoded = decode_fn(message.data, channel=message.topic)
        mode = decoded.event_bytes.decode("utf-8")
        payload = decoded.message_bytes
        if mode == "control" and payload == b"done":
            return frames, True
        if _run_stream_mode_matches(mode, stream_mode):
            frames.append((mode.encode(), payload, message.id))
    return frames, False


async def _stream_run_queue(
    queue,
    run,
    accept_fn,
    stream_mode,
    run_id,
    thread_id,
    ctx,
    ignore_404,
    json_dumpb,
    decode_fn,
):
    """Consume the live queue and yield frames until done or error."""
    while True:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=0.5)
        except TimeoutError:
            run = await _poll_run_status(run_id, thread_id, ctx)
            action = _run_poll_action(run, ignore_404, json_dumpb)
            if action == "break":
                return
            if action is not None:
                yield action
                return
            continue
        frame = _decode_run_message(message, accept_fn, decode_fn, stream_mode, run)
        if frame is None:
            continue
        if frame == "done":
            return
        yield frame


def _decode_run_message(message, accept_fn, decode_fn, stream_mode, run):
    """Decode a queue message; return frame tuple, 'done', or None to skip."""
    if not accept_fn(message):
        return None
    decoded = decode_fn(message.data, channel=message.topic)
    mode = decoded.event_bytes.decode("utf-8")
    payload = decoded.message_bytes
    if mode == "control" and payload == b"done":
        return "done"
    if not _run_stream_mode_matches(mode, stream_mode):
        return None
    stream_id = message.id if (run or {}).get("kwargs", {}).get("resumable") else None
    return mode.encode(), payload, stream_id


async def _poll_run_status(run_id: UUID, thread_id: UUID, ctx: Any) -> dict | None:
    """Re-fetch run status during stream join timeout."""
    async with connect() as conn:
        run_iter = await Runs.get(conn, run_id, thread_id=thread_id, ctx=ctx)
        return await anext(run_iter, None)


def _run_poll_action(
    run: dict | None, ignore_404: bool, json_dumpb
) -> tuple[bytes, bytes, None] | str | None:
    """Determine action after a poll timeout. Returns a yield frame, 'break', or None to continue."""
    if run is None:
        if ignore_404:
            return "break"
        return (
            b"error",
            json_dumpb(HTTPException(status_code=404, detail=_RUN_NOT_FOUND)),
            None,
        )
    if run["status"] not in ("pending", "running"):
        return "break"
    return None


async def _idle_orphaned_busy_threads(sf) -> bool:
    """Idle busy threads left behind when a cancelled worker dies without set_joint_status."""
    idled = False
    async with sf() as session:
        busy_threads = list(
            (await session.execute(sa_select(ThreadRow).where(ThreadRow.status == "busy")))
            .scalars()
            .all()
        )
        now = datetime.now(UTC)
        for thread in busy_threads:
            if await _thread_has_inflight_work(session, thread.thread_id):
                continue
            thread.status = "idle"
            thread.updated_at = now
            idled = True
        if idled:
            await session.commit()
    return idled


def _validate_cancel_args(assistant_id, thread_id, run_ids, status):
    """Validate mutually exclusive cancel arguments."""
    if assistant_id is not None:
        if thread_id is not None or run_ids is not None or status is not None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "Cannot specify 'thread_id', 'run_ids', or 'status' when using 'assistant_id'"
                ),
            )
    elif status is not None:
        if thread_id is not None or run_ids is not None:
            raise HTTPException(
                status_code=422,
                detail="Cannot specify 'thread_id' or 'run_ids' when using 'status'",
            )
    elif thread_id is None or run_ids is None:
        raise HTTPException(
            status_code=422,
            detail=(
                "Must provide either a status, an assistant_id, or both 'thread_id' and 'run_ids'"
            ),
        )


def _cancel_query(assistant_id, status, run_ids, thread_id):
    """Build the SELECT query for cancel candidates."""
    q = sa_select(RunRow)
    if assistant_id is not None:
        return q.where(
            RunRow.assistant_id == assistant_id, RunRow.status.in_(("pending", "running"))
        )
    if status is not None:
        statuses = _resolve_cancel_statuses(status)
        return q.where(RunRow.status.in_(statuses))
    return q.where(RunRow.run_id.in_(cast(Sequence[UUID], run_ids)), RunRow.thread_id == thread_id)


def _resolve_cancel_statuses(status: str) -> list[str]:
    if status == "all":
        return ["pending", "running"]
    if status in ("pending", "running"):
        return [status]
    raise HTTPException(
        status_code=422, detail="Invalid status: must be 'pending', 'running', or 'all'"
    )


async def _cancel_candidates_loop(session, candidates, action, sm, now) -> tuple[set, bool]:
    """Process each candidate run; return (affected thread IDs, cancelled_any)."""
    affected_threads: set = set()
    cancelled_any = False
    for r in candidates:
        result = await _cancel_single_run(session, r, action, sm, now)
        if result is None:
            continue
        cancelled_any = True
        run_thread_id, was_pending = result
        if run_thread_id is None:
            continue
        if was_pending or await has_run_heartbeat(r.run_id) is False:
            affected_threads.add(run_thread_id)
    return affected_threads, cancelled_any


async def _filter_cancel_candidates(session, candidates: list, filters) -> list:
    """Filter cancel candidates by thread auth."""
    allowed = []
    for r in candidates:
        if r.thread_id is None:
            continue
        thread = await _aget_thread(session, r.thread_id)
        if thread is not None and not _auth_denies(thread.metadata_, filters):
            allowed.append(r)
    return allowed


async def _cancel_single_run(
    session, run_row, action: str, sm, now: datetime
) -> tuple[UUID | None, bool] | None:
    """Lock and cancel a single run. Returns (thread_id, was_pending) or None if skipped."""
    locked = (
        await session.execute(
            sa_select(RunRow)
            .where(
                RunRow.run_id == run_row.run_id,
                RunRow.status.in_(("pending", "running")),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if locked is None:
        return None
    was_pending = locked.status == "pending"
    run_thread_id = locked.thread_id
    control = Message(
        topic=f"run:{locked.run_id}:control".encode(),
        data=action.encode(),
    )
    await sm.put(locked.run_id, run_thread_id, control)
    queues = sm.get_queues(locked.run_id, run_thread_id)
    delete_pending = action == "rollback" and was_pending and not queues
    if delete_pending:
        await _adelete_retry_counters(session, [locked.run_id])
        await session.delete(locked)
    else:
        locked.status = "interrupted"
        locked.updated_at = now
    if was_pending and not queues:
        try:
            await sm.clear_run_buffers(locked.run_id, run_thread_id, local_grace_secs=0.0)
        except Exception:
            logger.debug("clear_run_buffers after pending cancel failed", exc_info=True)
    return (run_thread_id, was_pending)


async def _idle_affected_threads(session, affected_threads: set[UUID], now: datetime) -> None:
    """Clear busy status on threads with no remaining in-flight runs."""
    for tid in affected_threads:
        pending = int(
            await session.scalar(
                sa_select(func.count())
                .select_from(RunRow)
                .where(
                    RunRow.thread_id == tid,
                    RunRow.status.in_(("pending", "running")),
                )
            )
            or 0
        )
        if pending:
            continue
        thread = await _aget_thread(session, tid)
        if thread is not None and thread.status == "busy":
            thread.status = "idle"
            thread.updated_at = now


async def _run_put_check_assistant(conn, ctx, assistant_id):
    """Return assistant row or None if not found / auth denied."""
    from langgraph_sdk import Auth

    assistant = await _aget_assistant(conn.session, assistant_id)
    if not assistant:
        return None
    if (assistant.metadata_ or {}).get("created_by") == "system":
        return assistant
    assistant_filters = await Assistants.handle_event(
        ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
    )
    if _auth_denies(assistant.metadata_, assistant_filters):
        return None
    return assistant


async def _run_put_lock_thread(conn, thread_id):
    """FOR UPDATE lock and return the thread row, or None."""
    locked_thread = (
        await conn.session.execute(
            sa_select(ThreadRow).where(ThreadRow.thread_id == thread_id).with_for_update()
        )
    ).scalar_one_or_none()
    return locked_thread


def _promote_thread_busy(existing_thread, assistant, assistant_id, config):
    """Promote thread to busy and update metadata/config."""
    if existing_thread.status == "busy":
        return
    existing_thread.status = "busy"
    existing_thread.metadata_ = _merge_jsonb(
        existing_thread.metadata_ or {},
        {"graph_id": assistant.graph_id, "assistant_id": str(assistant_id)},
    )
    existing_thread.config = _merge_jsonb(
        assistant.config or {},
        existing_thread.config or {},
        config,
        {
            "configurable": _merge_jsonb(
                (assistant.config or {}).get("configurable") or {},
                (existing_thread.config or {}).get("configurable") or {},
            )
        },
    )
    existing_thread.updated_at = datetime.now(UTC)


async def _get_inflight_runs(conn, thread_id) -> list[dict]:
    inflight_rows = list(
        (
            await conn.session.execute(
                sa_select(RunRow).where(
                    RunRow.thread_id == thread_id,
                    RunRow.status.in_(("pending", "running")),
                )
            )
        ).scalars()
    )
    return [run_to_dict(r) for r in inflight_rows]


def _yield_list(items: list):
    async def _iter():
        for r in items:
            yield r

    return _iter()


async def _insert_run_row(
    conn,
    run_id,
    thread_id,
    assistant_id,
    assistant,
    existing_thread,
    config,
    metadata,
    kwargs,
    status,
    multitask_strategy,
    after_seconds,
    user_id,
) -> dict:
    """Build merged configurable, create RunRow, flush, and return dict."""
    configurable = _build_run_configurable(
        config,
        existing_thread,
        assistant,
        run_id,
        thread_id,
        assistant_id,
        user_id,
    )
    merged_metadata = _build_run_metadata(
        assistant, existing_thread, config, metadata, assistant_id
    )

    effective_status = status or "pending"
    row = RunRow(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id=assistant_id,
        metadata_=merged_metadata,
        status=effective_status,
        kwargs={
            **kwargs,
            "config": {
                **(assistant.config or {}),
                **config,
                "configurable": configurable,
                "metadata": merged_metadata,
            },
            "context": {
                **(assistant.context or {}),
                **(kwargs.get("context") or {}),
            },
        },
        multitask_strategy=multitask_strategy,
        updated_at=datetime.now(UTC),
    )
    conn.session.add(row)
    await conn.session.flush()
    await conn.session.execute(
        text(
            "UPDATE runs SET created_at = now() + make_interval(secs => :secs) "
            "WHERE run_id = :run_id"
        ),
        {"secs": int(after_seconds or 0), "run_id": run_id},
    )
    await conn.session.refresh(row)
    return run_to_dict(row)


def _build_run_configurable(
    config, existing_thread, assistant, run_id, thread_id, assistant_id, user_id
):
    incoming_configurable = dict(config.get("configurable") or {})
    thread_configurable = dict(
        (existing_thread.config or {}).get("configurable") or {} if existing_thread else {}
    )
    assistant_configurable = dict((assistant.config or {}).get("configurable") or {})
    resolved_user_id = (
        incoming_configurable.get("user_id")
        or thread_configurable.get("user_id")
        or assistant_configurable.get("user_id")
        or user_id
    )
    return {
        **assistant_configurable,
        **thread_configurable,
        **incoming_configurable,
        "run_id": str(run_id),
        "thread_id": str(thread_id),
        "graph_id": assistant.graph_id,
        "assistant_id": str(assistant_id),
        "user_id": resolved_user_id,
    }


def _build_run_metadata(assistant, existing_thread, config, metadata, assistant_id):
    return {
        **(assistant.metadata_ or {}),
        **((existing_thread.metadata_ or {}) if existing_thread else {}),
        **(config.get("metadata") or {}),
        **metadata,
        "assistant_id": str(assistant_id),
    }


class Runs(Authenticated):
    resource = "threads"

    @staticmethod
    async def stats(conn: PgConnectionProto) -> dict:
        pending = int(
            await conn.session.scalar(
                sa_select(func.count()).select_from(RunRow).where(RunRow.status == "pending")
            )
            or 0
        )
        running = int(
            await conn.session.scalar(
                sa_select(func.count()).select_from(RunRow).where(RunRow.status == "running")
            )
            or 0
        )
        return {
            "n_pending": pending,
            "n_running": running,
            "pending_runs_wait_time_max_secs": None,
            "pending_runs_wait_time_med_secs": None,
            "pending_unblocked_runs_wait_time_max_secs": None,
        }

    @staticmethod
    async def pool_stats() -> dict:
        """Connection-pool stats for /info and self-hosted metrics."""
        return {}

    @staticmethod
    async def put(  # NOSONAR - complexity retained to preserve tested behavior
        conn: PgConnectionProto,  # NOSONAR - public API signature intentionally wide
        assistant_id: UUID | str,
        kwargs: dict,
        *,
        thread_id: UUID | None = None,
        user_id: str | None = None,
        run_id: UUID | None = None,
        status: str | None = "pending",
        metadata: dict | None = None,
        prevent_insert_if_inflight: bool = False,
        multitask_strategy: str = "reject",
        if_not_exists: str = "reject",
        after_seconds: int = 0,
        ctx: Any = None,
        langsmith_session_name: str | None = None,
        **_: Any,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        assistant_id = _ensure_uuid(assistant_id)
        thread_id = _ensure_uuid(thread_id) if thread_id else None
        run_id = _ensure_uuid(run_id) if run_id else uuid4()
        metadata = metadata or {}
        config = kwargs.get("config", {})
        temporary = bool(kwargs.get("temporary", False))

        filters = await Runs.handle_event(
            ctx,
            "create_run",
            Auth.types.RunsCreate(
                thread_id=None if temporary else thread_id,
                assistant_id=assistant_id,
                run_id=run_id,
                status=cast(Any, status),
                metadata=metadata,
                prevent_insert_if_inflight=prevent_insert_if_inflight,
                multitask_strategy=cast(Any, multitask_strategy),
                if_not_exists=cast(Any, if_not_exists),
                after_seconds=after_seconds,
                kwargs=kwargs,
            ),
        )

        assistant = await _run_put_check_assistant(conn, ctx, assistant_id)
        if assistant is None:
            return _empty_aiter()

        existing_thread = await _aget_thread(conn.session, thread_id) if thread_id else None
        if existing_thread and _auth_denies(existing_thread.metadata_, filters):
            return _empty_aiter()

        created_thread = False
        if not existing_thread and (thread_id is None or if_not_exists == "create"):
            thread_id, existing_thread, created_thread = await _ensure_run_thread(
                conn, thread_id, assistant, config, metadata, assistant_id
            )
            if existing_thread is None:
                return _empty_aiter()
        elif not existing_thread:
            return _empty_aiter()

        existing_thread = await _run_put_lock_thread(conn, thread_id)
        if existing_thread is None:
            return _empty_aiter()
        if not created_thread and _auth_denies(existing_thread.metadata_, filters):
            return _empty_aiter()

        if not created_thread:
            _promote_thread_busy(existing_thread, assistant, assistant_id, config)

        inflight = await _get_inflight_runs(conn, thread_id)
        if prevent_insert_if_inflight and inflight:
            return _yield_list(inflight)

        new_run = await _insert_run_row(
            conn,
            run_id,
            thread_id,
            assistant_id,
            assistant,
            existing_thread,
            config,
            metadata,
            kwargs,
            status,
            multitask_strategy,
            after_seconds,
            user_id,
        )

        if (status or "pending") == "pending":
            conn.schedule_after_commit(wake_run_queue)

        async def _yield():
            yield new_run
            for r in inflight:
                yield r

        return _yield()

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        run_id: UUID | str,
        *,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        run_id = _ensure_uuid(run_id)
        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(ctx, "read", Auth.types.ThreadsRead(thread_id=thread_id))
        # Eager load — API may drain the iterator after connect() closes.
        row = await _aget_run(conn.session, run_id)
        data = None
        if row is not None and row.thread_id == thread_id:
            if filters:
                thread = await _aget_thread(conn.session, thread_id)
                if thread is not None and not _auth_denies(thread.metadata_, filters):
                    data = run_to_dict(row)
            else:
                data = run_to_dict(row)

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        run_id: UUID | str,
        *,
        thread_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        run_id = _ensure_uuid(run_id)
        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(
            ctx,
            "delete",
            Auth.types.ThreadsDelete(run_id=run_id, thread_id=thread_id),
        )
        row = await _aget_run(conn.session, run_id)
        if row is None or row.thread_id != thread_id:
            raise HTTPException(status_code=404, detail=_RUN_NOT_FOUND)
        if filters:
            thread = await _aget_thread(conn.session, thread_id)
            if thread is None or _auth_denies(thread.metadata_, filters):
                raise HTTPException(status_code=404, detail=_RUN_NOT_FOUND)
        await _adelete_retry_counters(conn.session, [run_id])
        await conn.session.delete(row)
        await conn.session.flush()

        async def _yield():
            yield run_id

        return _yield()

    @staticmethod
    async def cancel(
        conn: PgConnectionProto,
        run_ids: Sequence[UUID | str] | None = None,
        *,
        action: str = "interrupt",
        thread_id: UUID | None = None,
        status: str | None = None,
        assistant_id: UUID | None = None,
        ctx: Any = None,
    ) -> None:
        from langgraph_sdk import Auth

        _validate_cancel_args(assistant_id, thread_id, run_ids, status)
        if assistant_id is not None:
            assistant_id = _ensure_uuid(assistant_id)
        if run_ids is not None:
            run_ids = [_ensure_uuid(rid) for rid in run_ids]
        if thread_id is not None:
            thread_id = _ensure_uuid(thread_id)

        filters = await Runs.handle_event(
            ctx,
            "update",
            Auth.types.ThreadsUpdate(
                thread_id=thread_id,  # type: ignore[typeddict-item]
                action=cast(Any, action),
                metadata={"run_ids": run_ids, "status": status},
            ),
        )
        q = _cancel_query(assistant_id, status, run_ids, thread_id)
        candidates = list((await conn.session.execute(q)).scalars())
        if filters:
            candidates = await _filter_cancel_candidates(conn.session, candidates, filters)
        if not candidates and assistant_id is None:
            raise HTTPException(status_code=404, detail="No runs found to cancel")

        sm = get_stream_manager()
        now = datetime.now(UTC)
        affected_threads, cancelled_any = await _cancel_candidates_loop(
            conn.session, candidates, action, sm, now
        )
        await conn.session.flush()

        await _idle_affected_threads(conn.session, affected_threads, now)
        await conn.session.flush()

        # Wake even if thread stays busy so a pending multitask-interrupt run becomes claimable when heartbeat drops.
        if cancelled_any:
            try:
                await wake_run_queue()
            except Exception:
                logger.debug("wake_run_queue after cancel failed", exc_info=True)

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        thread_id: UUID | str,
        *,
        limit: int = 10,
        offset: int = 0,
        status: str | None = None,
        select: list | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        thread_id = _ensure_uuid(thread_id)
        filters = await Runs.handle_event(
            ctx,
            "search",
            Auth.types.ThreadsSearch(thread_id=thread_id, limit=limit, offset=offset),
        )
        if filters:
            thread = await _aget_thread(conn.session, thread_id)
            if thread is None or _auth_denies(thread.metadata_, filters):
                return _empty_aiter()

        q = sa_select(RunRow).where(RunRow.thread_id == thread_id)
        if status is not None:
            q = q.where(RunRow.status == status)
        q = q.order_by(RunRow.created_at.desc()).offset(offset).limit(limit)
        rows = list((await conn.session.execute(q)).scalars())
        # Materialize while session is open — API drains runs outside connect().
        items: list[dict] = []
        for r in rows:
            d = run_to_dict(r)
            items.append({k: v for k, v in d.items() if k in select} if select else d)

        async def _yield():
            for d in items:
                yield d

        return _yield()

    @staticmethod
    async def set_status(
        conn: PgConnectionProto,
        run_id: UUID | str,
        status: str,
    ) -> dict | None:
        run_id = _ensure_uuid(run_id)
        run = await _aget_run(conn.session, run_id)
        if run:
            run.status = status
            run.updated_at = datetime.now(UTC)
            await conn.session.flush()
            return run_to_dict(run)
        return None

    @staticmethod
    async def next(wait: bool = False, limit: int = 1) -> AsyncIterator:
        """Claim pending runs via FOR UPDATE SKIP LOCKED (multi-replica safe)."""
        from sqlalchemy.exc import IntegrityError

        from langgraph_runtime_pg.database import PgRetryCounter, get_session_factory

        if wait:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0)

        sf = get_session_factory()
        claimed = 0
        conflict_budget = max(limit * 8, 8)
        blocked_threads: list[UUID] = []
        while claimed < limit and conflict_budget > 0:
            run_id: UUID | None = None
            try:
                kind, rid, tid, run_dict, attempt = await _claim_next_run(
                    sf, blocked_threads, PgRetryCounter
                )
                if kind == "empty":
                    break
                run_id = rid
                if kind == "blocked":
                    blocked_threads.append(tid)
                    conflict_budget -= 1
                    continue
                claimed += 1
                yield (run_dict, attempt)
            except IntegrityError:
                if run_id is not None:
                    await clear_run_heartbeat(run_id)
                conflict_budget -= 1
                continue

    @staticmethod
    @asynccontextmanager
    async def enter(
        run_id: UUID,
        thread_id: UUID | None,
        loop: asyncio.AbstractEventLoop,
        resumable: bool,
    ) -> AsyncIterator[Any]:
        """Enter a run, heartbeat + cancellation listen, signal done."""
        from langgraph_api.asyncio import SimpleTaskGroup, ValueEvent
        from langgraph_api.utils.stream_codec import STREAM_CODEC

        stream_manager = get_stream_manager()
        control_queue = await stream_manager.add_control_queue(run_id, thread_id)

        async def _heartbeat_loop() -> None:
            interval = heartbeat_refresh_interval_secs()
            while True:
                await set_run_heartbeat(run_id)
                await asyncio.sleep(interval)

        try:
            await set_run_heartbeat(run_id)
            async with SimpleTaskGroup(cancel=True, taskgroup_name="Runs.enter") as tg:
                done = ValueEvent()
                tg.create_task(_heartbeat_loop())
                tg.create_task(listen_for_cancellation(control_queue, run_id, thread_id, done))
                yield done
                control_message = Message(topic=f"run:{run_id}:control".encode(), data=b"done")
                await stream_manager.put(run_id, thread_id, control_message)
                stream_message = Message(
                    topic=f"run:{run_id}:stream".encode(),
                    data=STREAM_CODEC.encode("control", b"done"),
                )
                await stream_manager.put(run_id, thread_id, stream_message, resumable=resumable)
                await stream_manager.remove_control_queue(run_id, thread_id, control_queue)
        finally:
            await clear_run_heartbeat(run_id)
            try:
                await stream_manager.clear_run_buffers(run_id, thread_id)
            except Exception:
                logger.debug("clear_run_buffers failed", exc_info=True)

    @staticmethod
    async def sweep() -> list[UUID]:
        """Reclaim running runs whose Redis heartbeat has expired."""
        from langgraph_api import config

        from langgraph_runtime_pg.database import PgRetryCounter, get_session_factory

        sf = get_session_factory()
        reclaimed: list[UUID] = []
        max_retries = int(getattr(config, "BG_JOB_MAX_RETRIES", 3))
        retry_counter = PgRetryCounter(sf)

        async with sf() as session:
            result = await session.execute(
                sa_select(RunRow.run_id).where(RunRow.status == "running")
            )
            running_ids = [row[0] for row in result.all()]

        for run_id in running_ids:
            alive = await has_run_heartbeat(run_id)
            if alive is None or alive:
                continue
            swept = await _sweep_single_run(sf, run_id, retry_counter, max_retries)
            if swept:
                reclaimed.append(run_id)

        idled = await _idle_orphaned_busy_threads(sf)

        if reclaimed or idled:
            try:
                await wake_run_queue()
            except Exception:
                logger.debug("wake_run_queue after sweep failed", exc_info=True)
        return reclaimed

    class Stream:
        @staticmethod
        async def subscribe(run_id: UUID, thread_id: UUID | None = None) -> ContextQueue:
            sm = get_stream_manager()
            return cast(ContextQueue, await sm.add_queue(_ensure_uuid(run_id), thread_id))

        @staticmethod
        async def join(
            run_id: UUID,
            *,
            stream_channel: asyncio.Queue,
            thread_id: UUID,
            ignore_404: bool = False,
            cancel_on_disconnect: bool = False,
            stream_mode: list | str | None = None,
            last_event_id: str | None = None,
            ctx: Any = None,
        ) -> AsyncIterator:
            """Stream run output using short-lived DB sessions (avoids pool exhaustion)."""
            from langgraph_api.asyncio import create_task
            from langgraph_api.serde import json_dumpb
            from langgraph_api.utils.stream_codec import (
                decode_stream_message,
            )

            queue = stream_channel
            resume_cursor = (
                last_event_id
                if last_event_id is not None and last_event_id not in ("-", "")
                else None
            )
            emitted_ids: set[str] = set()

            def _accept(message: Message) -> bool:
                return _run_join_accept(message, emitted_ids, resume_cursor)

            try:
                try:
                    await Runs.Stream.check_run_stream_auth(run_id, thread_id, ctx)
                except HTTPException as e:
                    raise WrappedHTTPException(e) from None

                async with connect() as conn:
                    run_iter = await Runs.get(conn, run_id, thread_id=thread_id, ctx=ctx)
                    run = await anext(run_iter, None)

                frames, done = await _restore_run_messages(
                    run_id,
                    thread_id,
                    last_event_id,
                    _accept,
                    stream_mode,
                    decode_stream_message,
                )
                for f in frames:
                    yield f
                if not done:
                    async for frame in _stream_run_queue(
                        queue,
                        run,
                        _accept,
                        stream_mode,
                        run_id,
                        thread_id,
                        ctx,
                        ignore_404,
                        json_dumpb,
                        decode_stream_message,
                    ):
                        yield frame
            except WrappedHTTPException as e:
                raise e.http_exception from None
            except Exception:
                if cancel_on_disconnect:
                    create_task(cancel_run(thread_id, run_id))
                raise
            finally:
                await get_stream_manager().remove_queue(run_id, thread_id, queue)

        @staticmethod
        async def check_run_stream_auth(
            run_id: UUID,
            thread_id: UUID,
            ctx: Any = None,
        ) -> None:
            from langgraph_sdk import Auth

            del run_id  # auth is thread-scoped
            async with connect() as conn:
                filters = await Runs.handle_event(
                    ctx,
                    "read",
                    Auth.types.ThreadsRead(thread_id=thread_id),
                )
                if filters:
                    thread = await _aget_thread(conn.session, thread_id)
                    if thread is None or _auth_denies(thread.metadata_, filters):
                        raise HTTPException(status_code=404, detail="Thread not found")

        # Monotonic time of last custom publish; gives dual SSE consumers a beat before interrupt updates.
        _last_custom_publish_mono: float = 0.0

        @staticmethod
        async def publish(
            run_id: UUID | str,
            event: str,
            message: bytes,
            *,
            thread_id: UUID | str | None = None,
            resumable: bool = False,
        ) -> None:
            from langgraph_api.utils.stream_codec import STREAM_CODEC

            topic = f"run:{run_id}:stream".encode()
            sm = get_stream_manager()
            payload = STREAM_CODEC.encode(event, message)
            mode = event.split("|", 1)[0]

            # Brief yield after custom before interrupt-bearing values/updates so dual SSE does not close extension iterators early.
            if mode in ("updates", "values"):
                elapsed = time.monotonic() - Runs.Stream._last_custom_publish_mono
                if elapsed < 0.008:
                    await asyncio.sleep(0.008 - elapsed)

            await sm.put(run_id, thread_id, Message(topic=topic, data=payload), resumable)
            if mode == "custom" or mode.startswith("custom:"):
                Runs.Stream._last_custom_publish_mono = time.monotonic()


async def _paginate_crons_query(conn, q, filters, plain, offset: int, limit: int):
    """Paginate a cron query, filtering by auth when needed."""
    if filters and plain is None:
        rows = [
            r
            for r in (await conn.session.execute(q)).scalars()
            if not _auth_denies(r.metadata_, filters)
        ]
        page = rows[offset : offset + limit]
        cursor = offset + limit if len(rows) > offset + limit else None
    else:
        q = q.offset(offset).limit(limit + 1)
        rows = list((await conn.session.execute(q)).scalars())
        cursor = offset + limit if len(rows) > limit else None
        page = rows[:limit]
    return page, cursor


async def _cron_put_check_refs(conn, ctx, assistant_id, thread_uuid, system_ids):
    """Validate assistant and optional thread exist and caller has access."""
    from langgraph_sdk import Auth

    assistant = await _aget_assistant(conn.session, assistant_id)
    if assistant is None:
        raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found")
    if str(assistant_id) not in system_ids:
        assistant_filters = await Assistants.handle_event(
            ctx, "read", Auth.types.AssistantsRead(assistant_id=assistant_id)
        )
        if _auth_denies(assistant.metadata_, assistant_filters):
            raise HTTPException(status_code=404, detail=f"Assistant '{assistant_id}' not found")
    if thread_uuid is None:
        return
    thread = await _aget_thread(conn.session, thread_uuid)
    thread_filters = await Threads.handle_event(
        ctx, "read", Auth.types.ThreadsRead(thread_id=thread_uuid)
    )
    if thread is None or _auth_denies(thread.metadata_ if thread else None, thread_filters):
        raise HTTPException(status_code=404, detail=f"Thread '{thread_uuid}' not found")


def _cron_put_return_existing(existing, filters, cron_id):
    """Return existing cron or raise 404 if auth denied."""
    if _auth_denies(existing.metadata_, filters):
        raise HTTPException(status_code=404, detail=f"Cron '{cron_id}' not found")
    data = cron_to_dict(existing)

    async def _yield_existing():
        yield data

    return _yield_existing()


def _apply_cron_updates(
    row,
    schedule,
    end_time,
    enabled,
    on_run_completed,
    payload,
    metadata,
    timezone,
    cron_id,
    croniter_mod,
    next_cron_date,
):
    """Mutate cron row fields in place."""
    if timezone is not None:
        row.timezone = timezone
    if schedule is not None:
        if not croniter_mod.croniter.is_valid(schedule):
            raise HTTPException(status_code=422, detail=f"Invalid cron schedule: '{schedule}'")
        row.schedule = schedule
        row.next_run_date = next_cron_date(schedule, datetime.now(UTC), timezone=row.timezone)
    elif timezone is not None:
        row.next_run_date = next_cron_date(row.schedule, datetime.now(UTC), timezone=timezone)
    if end_time is not None:
        row.end_time = end_time
    if enabled is not None:
        row.enabled = enabled
    if on_run_completed is not None:
        row.on_run_completed = on_run_completed
    if metadata is not None:
        row.metadata_ = {**(row.metadata_ or {}), **metadata}
    if payload is not None:
        _merge_cron_payload(row, payload, cron_id)


def _merge_cron_payload(row, payload, cron_id):
    """Merge incoming payload into existing cron payload."""
    existing_payload = dict(row.payload or {})
    merged = {**existing_payload, **payload}
    merged["assistant_id"] = existing_payload.get("assistant_id", merged.get("assistant_id"))
    merged_config = dict(merged.get("config") or {})
    merged_configurable = dict(merged_config.get("configurable") or {})
    merged_configurable["cron_id"] = str(cron_id)
    merged_config["configurable"] = merged_configurable
    merged["config"] = merged_config
    row.payload = merged


class Crons(Authenticated):
    resource = "crons"

    @staticmethod
    async def put(
        conn: PgConnectionProto,
        *,
        payload: dict,
        schedule: str,
        cron_id: UUID | None = None,
        thread_id: UUID | None = None,
        end_time: datetime | None = None,
        metadata: dict | None = None,
        enabled: bool = True,
        timezone: str | None = None,
        on_run_completed: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        import croniter as croniter_mod
        from langgraph_api.graph import SYSTEM_ASSISTANT_IDS, get_assistant_id
        from langgraph_api.utils import get_auth_ctx, next_cron_date, uuid7
        from langgraph_sdk import Auth

        if not croniter_mod.croniter.is_valid(schedule):
            raise HTTPException(status_code=422, detail=f"Invalid cron schedule: '{schedule}'")

        ctx = ctx or get_auth_ctx()
        user_id = ctx.user.identity if ctx is not None else None
        cron_id = cron_id or uuid7()
        metadata = metadata or {}
        payload = dict(payload or {})
        config = dict(payload.get("config") or {})
        configurable = dict(config.get("configurable") or {})
        configurable["cron_id"] = str(cron_id)
        config["configurable"] = configurable
        payload["config"] = config
        assistant_id = _ensure_uuid(get_assistant_id(str(payload.get("assistant_id"))))
        payload["assistant_id"] = str(assistant_id)
        thread_uuid = _ensure_uuid(thread_id) if thread_id else None

        filters = await Crons.handle_event(
            ctx,
            "create",
            Auth.types.CronsCreate(
                payload=payload,
                schedule=schedule,
                cron_id=cron_id,
                thread_id=thread_uuid,
                end_time=end_time,
            ),
        )

        await _cron_put_check_refs(conn, ctx, assistant_id, thread_uuid, SYSTEM_ASSISTANT_IDS)

        existing = await _aget_cron(conn.session, cron_id)
        if existing:
            return _cron_put_return_existing(existing, filters, cron_id)

        now = datetime.now(UTC)
        next_run = next_cron_date(schedule, now, timezone=timezone)
        row = CronRow(
            cron_id=cron_id,
            assistant_id=assistant_id,
            thread_id=thread_uuid,
            schedule=schedule,
            payload=payload,
            metadata_=metadata,
            next_run_date=next_run,
            end_time=end_time,
            user_id=user_id,
            timezone=timezone,
            on_run_completed=on_run_completed,
            enabled=enabled,
            created_at=now,
            updated_at=now,
        )
        conn.session.add(row)
        await conn.session.flush()
        data = cron_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def update(
        conn: PgConnectionProto,
        *,
        cron_id: UUID,
        schedule: str | None = None,
        end_time: datetime | None = None,
        enabled: bool | None = None,
        on_run_completed: str | None = None,
        payload: dict | None = None,
        metadata: dict | None = None,
        timezone: str | None = None,
        ctx: Any = None,
    ) -> AsyncIterator:
        import croniter as croniter_mod
        from langgraph_api.utils import next_cron_date
        from langgraph_sdk import Auth

        has_updates = any(
            v is not None
            for v in [schedule, end_time, enabled, on_run_completed, payload, metadata, timezone]
        )
        if not has_updates:
            raise HTTPException(status_code=400, detail="No fields to update")

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(
            ctx,
            "update",
            Auth.types.CronsUpdate(cron_id=cron_id, payload=payload, schedule=schedule),
        )
        row = await _aget_cron(conn.session, cron_id)
        if row is None or _auth_denies(row.metadata_, filters):
            raise HTTPException(status_code=404, detail=f"Cron '{cron_id}' not found")

        _apply_cron_updates(
            row,
            schedule,
            end_time,
            enabled,
            on_run_completed,
            payload,
            metadata,
            timezone,
            cron_id,
            croniter_mod,
            next_cron_date,
        )

        row.updated_at = datetime.now(UTC)
        await conn.session.flush()
        data = cron_to_dict(row)

        async def _yield():
            yield data

        return _yield()

    @staticmethod
    async def get(
        conn: PgConnectionProto,
        cron_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(ctx, "read", Auth.types.CronsRead(cron_id=cron_id))
        # Eager load — API may drain the iterator after connect() closes.
        row = await _aget_cron(conn.session, cron_id)
        data = None
        if row is not None and not _auth_denies(row.metadata_, filters):
            data = copy.deepcopy(cron_to_dict(row))

        async def _yield():
            if data is not None:
                yield data

        return _yield()

    @staticmethod
    async def delete(
        conn: PgConnectionProto,
        cron_id: UUID | str,
        ctx: Any = None,
    ) -> AsyncIterator:
        from langgraph_sdk import Auth

        cron_id = _ensure_uuid(cron_id)
        filters = await Crons.handle_event(ctx, "delete", Auth.types.CronsDelete(cron_id=cron_id))
        row = await _aget_cron(conn.session, cron_id)
        deleted = row is not None and not _auth_denies(row.metadata_, filters)
        if deleted:
            await conn.session.delete(row)
            await conn.session.flush()

        async def _yield():
            if deleted:
                yield cron_id

        return _yield()

    @staticmethod
    async def search(
        conn: PgConnectionProto,
        *,
        assistant_id: UUID | None = None,
        thread_id: UUID | None = None,
        enabled: bool | None = None,
        limit: int = 10,
        offset: int = 0,
        select: list | None = None,
        ctx: Any = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        metadata: dict | None = None,
    ) -> tuple[AsyncIterator, int | None]:
        from langgraph_sdk import Auth

        filters = await Crons.handle_event(
            ctx,
            "search",
            Auth.types.CronsSearch(
                assistant_id=assistant_id,
                thread_id=thread_id,
                limit=limit,
                offset=offset,
            ),
        )
        q = sa_select(CronRow)
        if assistant_id is not None:
            q = q.where(CronRow.assistant_id == _ensure_uuid(assistant_id))
        if thread_id is not None:
            q = q.where(CronRow.thread_id == _ensure_uuid(thread_id))
        if enabled is not None:
            q = q.where(CronRow.enabled == enabled)
        if metadata:
            q = q.where(CronRow.metadata_.contains(metadata))
        plain = _plain_metadata_filter(filters)
        if plain:
            q = q.where(CronRow.metadata_.contains(plain))

        sb = _resolve_sort_field(sort_by, _CRON_SORT_FIELDS, "created_at", raise_invalid=True)
        col = getattr(CronRow, sb)
        reverse = not (sort_order and sort_order.upper() == "ASC")
        q = q.order_by(col.desc() if reverse else col.asc())

        page, cursor = await _paginate_crons_query(conn, q, filters, plain, offset, limit)

        items: list[dict] = []
        for r in page:
            d = cron_to_dict(r)
            items.append({k: v for k, v in d.items() if k in select} if select else d)

        async def _iter():
            for d in items:
                yield d

        return _iter(), cursor

    @staticmethod
    async def count(
        conn: PgConnectionProto,
        *,
        assistant_id: UUID | None = None,
        thread_id: UUID | None = None,
        ctx: Any = None,
        metadata: dict | None = None,
    ) -> int:
        from langgraph_sdk import Auth

        filters = await Crons.handle_event(
            ctx,
            "search",
            Auth.types.CronsSearch(
                assistant_id=assistant_id, thread_id=thread_id, limit=0, offset=0
            ),
        )
        clauses = _cron_where_clauses(assistant_id, thread_id, metadata)
        return await _count_with_auth(conn, CronRow, clauses, filters)

    @staticmethod
    async def next(conn: PgConnectionProto, ctx: Any = None) -> AsyncIterator:
        """Yield due crons, advancing next_run_date under SKIP LOCKED."""
        from langgraph_api.utils import next_cron_date

        del ctx
        now = datetime.now(UTC)
        # Claim due rows; advance next_run_date immediately so concurrent replicas skip them.
        result = await conn.session.execute(
            text(
                """
                SELECT cron_id FROM crons
                WHERE enabled = true
                  AND (end_time IS NULL OR end_time >= :now)
                  AND (next_run_date IS NULL OR next_run_date <= :now)
                ORDER BY next_run_date ASC NULLS FIRST
                FOR UPDATE SKIP LOCKED
                """
            ),
            {"now": now},
        )
        cron_ids = [row[0] for row in result.all()]
        for cron_id in cron_ids:
            row = await _aget_cron(conn.session, cron_id)
            if row is None:
                continue
            payload = cron_to_dict(row)
            # Advance before yield (commit happens when connect() exits).
            row.next_run_date = next_cron_date(row.schedule, now, timezone=row.timezone)
            row.updated_at = datetime.now(UTC)
            await conn.session.flush()
            yield {**payload, "now": now}

    @staticmethod
    async def set_next_run_date(
        conn: PgConnectionProto,
        cron_id: UUID,
        next_run_date: datetime,
        ctx: Any = None,
    ) -> None:
        cron_id = _ensure_uuid(cron_id)
        row = await _aget_cron(conn.session, cron_id)
        if row is None:
            return
        row.next_run_date = next_run_date
        row.updated_at = datetime.now(UTC)
        await conn.session.flush()


async def listen_for_cancellation(
    queue: asyncio.Queue, run_id: UUID, thread_id: UUID | None, done: Any
) -> None:
    """Listen for cancellation messages and set the done event accordingly."""
    from langgraph_api.errors import UserInterrupt, UserRollback

    stream_manager = get_stream_manager()

    # Hydrate from Redis so a cancel published before this worker subscribed is still observed.
    if control_key := await stream_manager.aget_control_key(run_id, thread_id):
        payload = control_key.data
        if payload == b"rollback":
            done.set(UserRollback())
        elif payload == b"interrupt":
            done.set(UserInterrupt())

    while not done.is_set():
        try:
            # Do not break on timeout — long runs stay quiet until interrupt/rollback/done.
            message = await asyncio.wait_for(queue.get(), timeout=240)
            payload = message.data
            if payload == b"rollback":
                done.set(UserRollback())
            elif payload == b"interrupt":
                done.set(UserInterrupt())
            elif payload == b"done":
                done.set()
                break
        except TimeoutError:
            continue


async def cancel_run(thread_id: UUID, run_id: UUID, ctx: Any = None) -> None:
    async with connect() as conn:
        await Runs.cancel(conn, [run_id], thread_id=thread_id, ctx=ctx)


__all__ = [
    "Assistants",
    "Crons",
    "Runs",
    "StreamHandler",
    "Threads",
]
