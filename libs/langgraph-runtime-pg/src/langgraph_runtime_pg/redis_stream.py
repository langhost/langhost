"""Redis Streams + Pub/Sub fanout into local asyncio queues (requires ``REDIS_URI``)."""

from __future__ import annotations

import asyncio
import base64
import importlib
import logging
import os
import threading
import time
from collections import defaultdict, deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import orjson
import redis.asyncio as redis
from redis.driver_info import DriverInfo

logger = logging.getLogger(__name__)

_REDIS: redis.Redis | None = None
_REDIS_POOL: redis.ConnectionPool | None = None
_WAKE_EVENT: asyncio.Event | None = None
_WAKE_TASK: asyncio.Task | None = None
# Monotonic wake generation — avoids asyncio.Event wait/clear lost-wakeup races.
_WAKE_GEN = 0

QUEUE_WAKE_CHANNEL = "langgraph:run_queue"
RUN_FANOUT_PATTERN = "lg:run-fanout:*"
THREAD_FANOUT_PATTERN = "lg:thread-fanout:*"

_REDIS_NOT_CONNECTED = "Redis not connected; call start_stream() first"


def _redis_max_connections() -> int:
    raw = os.environ.get("REDIS_MAX_CONNECTIONS", "64")
    try:
        return max(int(raw), 8)
    except ValueError:
        return 64


def _require_redis_uri() -> str:
    uri = os.environ.get("REDIS_URI")
    if not uri:
        raise RuntimeError("REDIS_URI is required for langgraph_runtime_pg redis_stream")
    return uri


def _sanitize_redis_uri(uri: str) -> str:
    """Drop deprecated ``lib_name`` / ``lib_version`` query params from ``REDIS_URI``.

    redis-py still accepts those query keys but warns on every connection; pass
    metadata via ``driver_info`` instead (see ``_redis_pool_kwargs``).
    """
    from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

    parsed = urlparse(uri)
    if not parsed.query:
        return uri
    kept = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k not in ("lib_name", "lib_version")
    ]
    return urlunparse(parsed._replace(query=urlencode(kept)))


def _redis_pool_kwargs() -> dict[str, Any]:
    """Explicit CLIENT SETINFO metadata via ``driver_info`` (not deprecated kwargs).

    Passing ``lib_name`` / ``lib_version`` triggers redis-py DeprecationWarning on
    every connection. Setting ``lib_version`` on ``DriverInfo`` also avoids
    ``importlib.metadata.version("redis")`` filesystem probes during connect.
    """
    name = os.environ.get("REDIS_LIB_NAME", "redis-py")
    version = os.environ.get("REDIS_LIB_VERSION")
    if not version:
        try:
            redis_mod = importlib.import_module("redis")
            version = str(getattr(redis_mod, "__version__", "") or "").strip()
        except Exception:
            version = ""
    if not version:
        version = "unknown"
    return {"driver_info": DriverInfo(name=name, lib_version=version)}


def _ensure_uuid(id_: str | UUID) -> UUID:
    return UUID(str(id_)) if isinstance(id_, str) else id_


def _heartbeat_key(run_id: UUID | str) -> str:
    return f"lg:run-heartbeat:{_ensure_uuid(run_id)}"


def bg_job_heartbeat_secs() -> float:
    """Heartbeat interval (seconds); ``LG_BG_JOB_HEARTBEAT`` overrides config default."""
    raw = os.environ.get("LG_BG_JOB_HEARTBEAT")
    if raw:
        return max(float(raw), 1.0)
    try:
        from langgraph_api import config

        return max(float(config.BG_JOB_HEARTBEAT), 1.0)
    except Exception:
        return 120.0


def _heartbeat_ttl_secs() -> int:
    """Redis heartbeat key TTL (~2x interval)."""
    return max(int(bg_job_heartbeat_secs() * 2), 4)


def heartbeat_refresh_interval_secs() -> float:
    return max(bg_job_heartbeat_secs() / 2.0, 1.0)


async def set_run_heartbeat(run_id: UUID | str, *, ttl: int | None = None) -> None:
    """Refresh worker heartbeat for a running run."""
    if _REDIS is None:
        return
    await _REDIS.set(_heartbeat_key(run_id), b"1", ex=ttl or _heartbeat_ttl_secs())


async def clear_run_heartbeat(run_id: UUID | str) -> None:
    """Clear heartbeat when a worker finishes or releases a run."""
    if _REDIS is None:
        return
    await _REDIS.delete(_heartbeat_key(run_id))


async def has_run_heartbeat(run_id: UUID | str) -> bool | None:
    """True/False if Redis is up; None if heartbeat cannot be checked."""
    if _REDIS is None:
        return None
    try:
        return bool(await _REDIS.exists(_heartbeat_key(run_id)))
    except Exception:
        logger.exception("Failed to check run heartbeat for %s", run_id)
        return None


# Same-ms stream ID collisions: bump seq so `_seen_ids` does not drop frames.
_id_lock = threading.Lock()
_id_last_ms = 0
_id_seq = 0


def _generate_ms_seq_id() -> str:
    """Monotonic Redis-style ``ms-seq`` id (safe under same-ms bursts + clock skew)."""
    global _id_last_ms, _id_seq
    ms = int(time.time() * 1000)
    with _id_lock:
        if ms > _id_last_ms:
            _id_last_ms = ms
            _id_seq = 0
        else:
            # Same ms or clock skew — keep ids non-decreasing for resume cursors.
            _id_seq += 1
        return f"{_id_last_ms}-{_id_seq}"


def _parse_ms_seq_id(mid: str) -> tuple[int, int] | None:
    """Parse Redis-style ``ms-seq`` IDs; None if not in that form."""
    try:
        ms_s, seq_s = mid.split("-", 1)
        return int(ms_s), int(seq_s)
    except (TypeError, ValueError):
        return None


def ms_seq_id_gt(left: str, right: str) -> bool:
    """True if ``left`` is strictly after ``right`` (Redis stream ID order)."""
    pl, pr = _parse_ms_seq_id(left), _parse_ms_seq_id(right)
    if pl is not None and pr is not None:
        return pl > pr
    return left > right


def ms_seq_id_sort_key(mid: str) -> tuple[int, int, str]:
    parsed = _parse_ms_seq_id(mid)
    if parsed is not None:
        return (*parsed, "")
    return (0, 0, mid)


def _stream_key(thread_id: Any, run_id: UUID) -> str:
    return f"lg:run-stream:{thread_id}:{run_id}"


def _control_key(thread_id: Any, run_id: UUID) -> str:
    return f"lg:run-control:{thread_id}:{run_id}"


def _fanout_channel(thread_id: Any, run_id: UUID) -> str:
    return f"lg:run-fanout:{thread_id}:{run_id}"


def _thread_fanout_channel(thread_id: UUID) -> str:
    return f"lg:thread-fanout:{thread_id}"


THREADLESS_KEY = "no-thread"


def _channel_str(channel: Any) -> str:
    if isinstance(channel, bytes):
        return channel.decode()
    return str(channel)


def _parse_run_fanout_channel(channel: str) -> tuple[Any, UUID] | None:
    prefix = "lg:run-fanout:"
    if not channel.startswith(prefix):
        return None
    rest = channel[len(prefix) :]
    if ":" not in rest:
        return None
    thread_part, run_part = rest.rsplit(":", 1)
    try:
        run_id = UUID(run_part)
    except ValueError:
        return None
    if thread_part == THREADLESS_KEY:
        return THREADLESS_KEY, run_id
    try:
        return UUID(thread_part), run_id
    except ValueError:
        return None


def _parse_thread_fanout_channel(channel: str) -> UUID | None:
    prefix = "lg:thread-fanout:"
    if not channel.startswith(prefix):
        return None
    try:
        return UUID(channel[len(prefix) :])
    except ValueError:
        return None


@dataclass
class Message:
    topic: bytes
    data: bytes
    id: bytes | None = None


class ContextQueue(asyncio.Queue):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args: object) -> None:
        while not self.empty():
            try:
                self.get_nowait()
            except asyncio.QueueEmpty:
                break


def _encode_envelope(message: Message, *, control: bool = False) -> bytes:
    topic = message.topic.decode() if isinstance(message.topic, bytes) else message.topic
    mid = message.id.decode() if isinstance(message.id, bytes) else (message.id or "")
    return orjson.dumps(
        {
            "topic": topic,
            "data": base64.b64encode(message.data).decode("ascii"),
            "id": mid,
            "control": control,
        }
    )


def _decode_envelope(payload: bytes | str) -> Message | None:
    try:
        raw = payload if isinstance(payload, (bytes, bytearray)) else payload.encode()
        obj = orjson.loads(raw)
        topic = obj["topic"].encode() if isinstance(obj["topic"], str) else obj["topic"]
        data = base64.b64decode(obj["data"])
        mid = obj.get("id") or ""
        mid_b = mid.encode() if isinstance(mid, str) else mid
        return Message(topic=topic, data=data, id=mid_b or None)
    except Exception:
        logger.exception("Failed to decode fanout envelope")
        return None


async def _reconnect_backoff() -> None:
    """Reconnect delay — same 1s sleep as the original mux loops."""
    await asyncio.sleep(1)  # NOSONAR — reconnect backoff, not a poll loop


class _SeenIds:
    """Bounded FIFO dedup set — prune oldest half when full (never wipe all)."""

    def __init__(self, maxlen: int = 10_000):
        self._maxlen = maxlen
        self._order: deque[str] = deque()
        self._ids: set[str] = set()

    def __contains__(self, mid: str) -> bool:
        return mid in self._ids

    def add(self, mid: str) -> bool:
        """Record ``mid``. Return False if already seen."""
        if mid in self._ids:
            return False
        self._ids.add(mid)
        self._order.append(mid)
        if len(self._order) > self._maxlen:
            for _ in range(len(self._order) // 2):
                self._ids.discard(self._order.popleft())
        return True


async def _fanout_put(queues: list, message: Message) -> None:
    if not queues:
        return
    results = await asyncio.gather(*[q.put(message) for q in queues], return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            logger.exception("Failed to put message in queue: %s", r)


class StreamManager:
    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client
        self.queues: dict = defaultdict(lambda: defaultdict(list))
        self.control_keys: dict = defaultdict(lambda: defaultdict())
        self.control_queues: dict = defaultdict(lambda: defaultdict(list))
        self.thread_streams: dict = defaultdict(list)
        self.message_stores: dict = defaultdict(lambda: defaultdict(list))
        self._seen_ids = _SeenIds()
        # Buffer+subscriber lock — avoids miss/double-deliver races with concurrent put().
        self._buf_lock = asyncio.Lock()
        self._mux_tasks: list[asyncio.Task] = []
        self._cleanup_tasks: set[asyncio.Task] = set()

    def start_mux(self) -> None:
        if self._mux_tasks:
            return
        self._mux_tasks = [
            asyncio.create_task(self._run_fanout_mux(), name="redis-run-fanout-mux"),
            asyncio.create_task(self._thread_fanout_mux(), name="redis-thread-fanout-mux"),
        ]

    def get_queues(self, run_id: UUID | str, thread_id: UUID | str | None) -> list[asyncio.Queue]:
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        return self.queues[thread_id][run_id]

    def get_control_queues(
        self, run_id: UUID | str, thread_id: UUID | str | None
    ) -> list[asyncio.Queue]:
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        return self.control_queues[thread_id][run_id]

    def get_control_key(self, run_id: UUID | str, thread_id: UUID | str | None) -> Message | None:
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        return self.control_keys.get(thread_id, {}).get(run_id)

    async def aget_control_key(
        self, run_id: UUID | str, thread_id: UUID | str | None
    ) -> Message | None:
        """Local control key, else hydrate from Redis (cross-replica cancel-before-listen)."""
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        local = self.control_keys.get(thread_id, {}).get(run_id)
        if local is not None:
            return local
        if self._redis is None:
            return None
        try:
            data = await self._redis.get(_control_key(thread_id, run_id))
        except Exception:
            logger.exception("Failed to read control key for run %s", run_id)
            return None
        if data is None:
            return None
        payload = data if isinstance(data, (bytes, bytearray)) else str(data).encode()
        message = Message(topic=f"run:{run_id}:control".encode(), data=bytes(payload))
        self.control_keys[thread_id][run_id] = message
        return message

    def _mark_seen(self, scope: str, message: Message) -> bool:
        """False if already delivered in ``scope`` (ids are not globally unique across runs)."""
        mid = message.id.decode() if message.id else None
        if mid is None:
            return True
        return self._seen_ids.add(f"{scope}:{mid}")

    async def _deliver_local(
        self,
        thread_id: Any,
        run_id: UUID,
        message: Message,
        *,
        control: bool,
        already_locked: bool = False,
    ) -> None:
        async def _do() -> None:
            if not self._mark_seen(f"run:{thread_id}:{run_id}", message):
                return
            if control:
                self.control_keys[thread_id][run_id] = message
                queues = self.control_queues[thread_id][run_id]
            else:
                queues = self.queues[thread_id][run_id]
            await _fanout_put(queues, message)

        if already_locked:
            await _do()
        else:
            async with self._buf_lock:
                await _do()

    async def _deliver_thread_local(self, thread_id: UUID, message: Message) -> None:
        async with self._buf_lock:
            if not self._mark_seen(f"thread:{thread_id}", message):
                return
            await _fanout_put(self.thread_streams[thread_id], message)

    # -- Pub/Sub mux infrastructure ------------------------------------------

    async def _pubsub_mux_loop(
        self,
        pattern: str,
        handler: Callable[[dict], Any],
    ) -> None:
        """Shared pattern-subscribe loop: dispatch each pmessage to *handler*."""
        while True:
            pubsub = self._redis.pubsub()
            try:
                await pubsub.psubscribe(pattern)
                async for msg in pubsub.listen():
                    if msg is None or msg.get("type") != "pmessage":
                        continue
                    await handler(msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s mux failed; reconnecting", pattern)
                await _reconnect_backoff()
            finally:
                try:
                    await pubsub.punsubscribe(pattern)
                    await pubsub.aclose()
                except Exception:
                    pass

    async def _handle_run_fanout_message(self, msg: dict) -> None:
        parsed = _parse_run_fanout_channel(_channel_str(msg.get("channel")))
        if parsed is None:
            return
        thread_id, run_id = parsed
        decoded = _decode_envelope(msg["data"])
        if decoded is None:
            return
        topic = decoded.topic.decode() if isinstance(decoded.topic, bytes) else decoded.topic
        await self._deliver_local(thread_id, run_id, decoded, control="control" in topic)

    async def _handle_thread_fanout_message(self, msg: dict) -> None:
        thread_id = _parse_thread_fanout_channel(_channel_str(msg.get("channel")))
        if thread_id is None:
            return
        decoded = _decode_envelope(msg["data"])
        if decoded is None:
            return
        await self._deliver_thread_local(thread_id, decoded)

    async def _run_fanout_mux(self) -> None:
        """Shared pattern-subscribe mux for all run fanout channels."""
        await self._pubsub_mux_loop(RUN_FANOUT_PATTERN, self._handle_run_fanout_message)

    async def _thread_fanout_mux(self) -> None:
        """Shared pattern-subscribe mux for all thread fanout channels."""
        await self._pubsub_mux_loop(THREAD_FANOUT_PATTERN, self._handle_thread_fanout_message)

    # -- Put / publish -------------------------------------------------------

    async def put(
        self,
        run_id: UUID | str | None,
        thread_id: UUID | str | None,
        message: Message,
        resumable: bool = False,
    ) -> None:
        if self._redis is None:
            raise RuntimeError(_REDIS_NOT_CONNECTED)
        if run_id is None:
            raise ValueError("run_id is required")

        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)

        topic = message.topic.decode() if isinstance(message.topic, bytes) else message.topic
        control = "control" in topic

        if resumable:
            entry_id = await self._redis.xadd(
                _stream_key(thread_id, run_id),
                {b"topic": message.topic, b"data": message.data},
            )
            message.id = entry_id if isinstance(entry_id, bytes) else str(entry_id).encode()
        else:
            message.id = _generate_ms_seq_id().encode()

        # Lock buffer+deliver (add_queue race); skip buffering raw run:*:control (not STREAM_CODEC).
        async with self._buf_lock:
            if not control:
                buf = self.message_stores[thread_id][run_id]
                buf.append(message)
                if len(buf) > 2_000:
                    del buf[: len(buf) - 2_000]
            if control:
                self.control_keys[thread_id][run_id] = message
            await self._deliver_local(
                thread_id, run_id, message, control=control, already_locked=True
            )

        if control:
            # TTL: pending cancels that never enter Runs.enter must not leak keys.
            await self._redis.set(
                _control_key(thread_id, run_id),
                message.data,
                ex=max(_heartbeat_ttl_secs() * 4, 3600),
            )

        envelope = _encode_envelope(message, control=control)
        await self._redis.publish(_fanout_channel(thread_id, run_id), envelope)

    async def put_thread(
        self,
        thread_id: UUID | str,
        message: Message,
    ) -> None:
        if self._redis is None:
            raise RuntimeError(_REDIS_NOT_CONNECTED)

        thread_id = _ensure_uuid(thread_id)
        message.id = _generate_ms_seq_id().encode()
        await self._deliver_thread_local(thread_id, message)

        envelope = _encode_envelope(message, control=False)
        await self._redis.publish(_thread_fanout_channel(thread_id), envelope)

    # -- Queue / subscriber management ---------------------------------------

    @staticmethod
    def _replay_into_queue(q: asyncio.Queue, messages: list | deque, after_id: str | None) -> None:
        """Replay buffered messages into *q*, skipping up to *after_id*."""
        for message in messages:
            if after_id is not None and message.id is not None:
                try:
                    if not ms_seq_id_gt(message.id.decode(), after_id):
                        continue
                except Exception:
                    pass
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                break

    async def add_queue(
        self,
        run_id: UUID | str,
        thread_id: UUID | str | None,
        *,
        after_id: str | None = None,
        replay: bool = True,
    ) -> asyncio.Queue:
        run_id = _ensure_uuid(run_id)
        q = ContextQueue()
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        # Replay then register under lock; after_id/replay=False avoid double-delivery after restore.
        async with self._buf_lock:
            if replay:
                # Snapshot under lock (same as original list(...) copy).
                messages = list(self.message_stores.get(thread_id, {}).get(run_id, []))
                self._replay_into_queue(q, messages, after_id)
            self.queues[thread_id][run_id].append(q)
        return q

    async def add_control_queue(  # NOSONAR - async API parity with await call sites
        self, run_id: UUID | str, thread_id: UUID | str | None
    ) -> asyncio.Queue:
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        q = ContextQueue()
        self.control_queues[thread_id][run_id].append(q)
        return q

    async def add_thread_stream(  # NOSONAR - async API parity with await call sites
        self, thread_id: UUID | str
    ) -> asyncio.Queue:
        thread_id = _ensure_uuid(thread_id)
        q = ContextQueue()
        self.thread_streams[thread_id].append(q)
        return q

    async def remove_thread_stream(  # NOSONAR - async API parity with await call sites
        self, thread_id: UUID | str, queue: asyncio.Queue
    ) -> None:
        thread_id = _ensure_uuid(thread_id)
        if thread_id not in self.thread_streams:
            return
        try:
            self.thread_streams[thread_id].remove(queue)
        except ValueError:
            pass
        if not self.thread_streams[thread_id]:
            del self.thread_streams[thread_id]

    async def remove_queue(  # NOSONAR - async API parity with await call sites
        self, run_id: UUID | str, thread_id: UUID | str | None, queue: asyncio.Queue
    ):
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        if thread_id in self.queues and run_id in self.queues[thread_id]:
            try:
                self.queues[thread_id][run_id].remove(queue)
            except ValueError:
                pass
            if not self.queues[thread_id][run_id]:
                del self.queues[thread_id][run_id]

    async def remove_control_queue(  # NOSONAR - async API parity with await call sites
        self, run_id: UUID | str, thread_id: UUID | str | None, queue: asyncio.Queue
    ):
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        if thread_id in self.control_queues and run_id in self.control_queues[thread_id]:
            try:
                self.control_queues[thread_id][run_id].remove(queue)
            except ValueError:
                pass
            if not self.control_queues[thread_id][run_id]:
                del self.control_queues[thread_id][run_id]

    # -- Restore / replay from Redis Streams ---------------------------------

    def restore_messages(
        self, run_id: UUID | str, thread_id: UUID | str | None, message_id: str | None
    ) -> Iterator[Message]:
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        if message_id is None:
            return
        store = self.message_stores.get(thread_id, {}).get(run_id)
        if not store:
            return
        try:
            for message in store:
                if message.id and ms_seq_id_gt(message.id.decode(), message_id):
                    yield message
        except TypeError:
            # Rare numeric cursor — treat as list index.
            message_idx = int(message_id) + 1
            yield from store[message_idx:]

    async def arestore_messages(
        self,
        run_id: UUID | str,
        thread_id: UUID | str | None,
        message_id: str | None,
    ) -> list[Message]:
        if self._redis is None:
            raise RuntimeError(_REDIS_NOT_CONNECTED)
        if message_id is None:
            return []
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        rows = await self._redis.xrange(
            _stream_key(thread_id, run_id),
            min=message_id,
            max="+",
        )
        out: list[Message] = []
        for entry_id, fields in rows or []:
            if fields is None:
                continue
            eid = entry_id if isinstance(entry_id, bytes) else str(entry_id).encode()
            # XRANGE min is inclusive — skip the cursor id itself.
            if not ms_seq_id_gt(eid.decode(), message_id):
                continue
            topic = fields.get(b"topic", fields.get("topic", b""))
            data = fields.get(b"data", fields.get("data", b""))
            if isinstance(topic, str):
                topic = topic.encode()
            if isinstance(data, str):
                data = data.encode()
            out.append(Message(topic=topic, data=data, id=eid))
        return out

    async def restore_messages_async(
        self,
        run_id: UUID | str,
        thread_id: UUID | str | None,
        message_id: str | None,
    ) -> list[Message]:
        """Resume from Redis Streams when present; fall back to local buffer."""
        if message_id is None:
            return []
        try:
            from_redis = await self.arestore_messages(run_id, thread_id, message_id)
        except Exception:
            logger.exception("Redis stream restore failed for run %s", run_id)
            from_redis = []
        if from_redis:
            return from_redis
        return list(self.restore_messages(run_id, thread_id, message_id))

    # -- Buffer cleanup ------------------------------------------------------

    async def _expire_redis_run_buffers(
        self, thread_id: Any, run_id: UUID, stream_ttl_secs: int
    ) -> None:
        if self._redis is None:
            return
        try:
            pipe = self._redis.pipeline()
            pipe.delete(_control_key(thread_id, run_id))
            pipe.expire(_stream_key(thread_id, run_id), max(int(stream_ttl_secs), 60))
            await pipe.execute()
        except Exception:
            logger.exception("Failed to clear Redis buffers for run %s", run_id)

    def _drop_local_store_sync(self, thread_id: Any, run_id: UUID) -> None:
        if thread_id in self.message_stores:
            self.message_stores[thread_id].pop(run_id, None)
            if not self.message_stores[thread_id]:
                del self.message_stores[thread_id]

    def _schedule_local_store_drop(
        self, thread_id: Any, run_id: UUID, local_grace_secs: float
    ) -> None:
        async def _drop_local_store() -> None:
            try:
                await asyncio.sleep(max(float(local_grace_secs), 0.0))
            except asyncio.CancelledError:  # NOSONAR - keep graceful cancelled cleanup behavior
                return
            self._drop_local_store_sync(thread_id, run_id)

        try:
            task = asyncio.create_task(
                _drop_local_store(),
                name=f"clear-run-buffers-{run_id}",
            )
            self._cleanup_tasks.add(task)
            task.add_done_callback(self._cleanup_tasks.discard)
        except RuntimeError:
            self._drop_local_store_sync(thread_id, run_id)

    async def clear_run_buffers(
        self,
        run_id: UUID | str,
        thread_id: UUID | str | None,
        *,
        stream_ttl_secs: int = 3600,
        local_grace_secs: float = 120.0,
    ) -> None:
        """Drop control state now; expire Redis stream; free local frames after grace."""
        run_id = _ensure_uuid(run_id)
        thread_id = THREADLESS_KEY if thread_id is None else _ensure_uuid(thread_id)
        if thread_id in self.control_keys:
            self.control_keys[thread_id].pop(run_id, None)
            if not self.control_keys[thread_id]:
                del self.control_keys[thread_id]
        await self._expire_redis_run_buffers(thread_id, run_id, stream_ttl_secs)
        self._schedule_local_store_drop(thread_id, run_id, local_grace_secs)

    # -- Misc ----------------------------------------------------------------

    def get_queues_by_thread_id(self, thread_id: UUID | str) -> list[asyncio.Queue]:
        all_queues: list[asyncio.Queue] = []
        thread_id = _ensure_uuid(thread_id)
        if thread_id in self.queues:
            for run_id in self.queues[thread_id]:
                all_queues.extend(self.queues[thread_id][run_id])
        return all_queues

    async def aclose_fanout(self) -> None:
        tasks = list(self._mux_tasks) + list(self._cleanup_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._mux_tasks.clear()
        self._cleanup_tasks.clear()


stream_manager: StreamManager | None = None


async def _wake_listener_loop() -> None:
    """Pubsub loop: set ``_WAKE_EVENT`` on each queue-wake publish."""
    global _WAKE_EVENT
    while _REDIS is not None and _WAKE_EVENT is not None:
        pubsub = _REDIS.pubsub()
        try:
            await pubsub.subscribe(QUEUE_WAKE_CHANNEL)
            async for msg in pubsub.listen():
                if msg is None or msg.get("type") != "message":
                    continue
                _signal_local_wake()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Queue wake listener failed; reconnecting")
            await _reconnect_backoff()
        finally:
            try:
                await pubsub.unsubscribe(QUEUE_WAKE_CHANNEL)
                await pubsub.aclose()
            except Exception:
                pass


async def start_stream() -> None:
    """Connect to Redis and install a fresh StreamManager."""
    global stream_manager, _REDIS, _REDIS_POOL, _WAKE_EVENT, _WAKE_TASK

    if stream_manager is not None and _REDIS is not None and _WAKE_TASK is not None:
        return
    # Partial leftover from a failed prior start — tear down before reconnect.
    if stream_manager is not None or _REDIS is not None or _WAKE_TASK is not None:
        await stop_stream()

    uri = _sanitize_redis_uri(_require_redis_uri())
    # Explicit pool size — default redis-py max_connections is too low under load.
    _REDIS_POOL = redis.ConnectionPool.from_url(
        uri,
        decode_responses=False,
        max_connections=_redis_max_connections(),
        **_redis_pool_kwargs(),
    )
    client = redis.Redis(connection_pool=_REDIS_POOL)
    await client.ping()
    _REDIS = client
    stream_manager = StreamManager(_REDIS)
    stream_manager.start_mux()

    _WAKE_EVENT = asyncio.Event()
    _WAKE_TASK = asyncio.create_task(_wake_listener_loop(), name="redis-queue-wake")


async def stop_stream() -> None:
    """Disconnect Redis and clear local fanout state."""
    global stream_manager, _REDIS, _REDIS_POOL, _WAKE_EVENT, _WAKE_TASK

    if _WAKE_TASK is not None:
        _WAKE_TASK.cancel()
        await asyncio.gather(_WAKE_TASK, return_exceptions=True)
        _WAKE_TASK = None
    _WAKE_EVENT = None

    if stream_manager is not None:
        await stream_manager.aclose_fanout()
        stream_manager.queues.clear()
        stream_manager.control_queues.clear()
        stream_manager.message_stores.clear()
        stream_manager.thread_streams.clear()
        stream_manager = None

    if _REDIS is not None:
        await _REDIS.aclose()
        _REDIS = None
    if _REDIS_POOL is not None:
        await _REDIS_POOL.aclose()
        _REDIS_POOL = None


def get_stream_manager() -> StreamManager:
    if stream_manager is None:
        raise RuntimeError(
            "Stream manager not started; call start_stream() first (REDIS_URI required)"
        )
    return stream_manager


def _signal_local_wake() -> None:
    """Bump wake generation and set the shared Event."""
    global _WAKE_GEN
    _WAKE_GEN += 1
    if _WAKE_EVENT is not None:
        _WAKE_EVENT.set()


async def wake_run_queue() -> None:
    """Notify all replicas that a pending run is available."""
    _signal_local_wake()
    if _REDIS is None:
        return
    await _REDIS.publish(QUEUE_WAKE_CHANNEL, b"wake")


async def wait_for_queue_wake(timeout: float = 0.5) -> bool:  # NOSONAR
    """Wait for wake or timeout; generation counter avoids Event wait/clear lost-wakeup."""
    if _WAKE_EVENT is None:
        await asyncio.sleep(timeout)
        return False
    start = _WAKE_GEN
    if _WAKE_EVENT.is_set():
        _WAKE_EVENT.clear()
        if start != _WAKE_GEN:
            _WAKE_EVENT.set()
        return True
    try:
        await asyncio.wait_for(_WAKE_EVENT.wait(), timeout=timeout)
    except TimeoutError:
        return start != _WAKE_GEN
    except asyncio.CancelledError:  # NOSONAR - explicit propagation for baseline parity
        raise  # NOSONAR
    end = _WAKE_GEN
    _WAKE_EVENT.clear()
    if end != _WAKE_GEN:
        _WAKE_EVENT.set()
    return True
