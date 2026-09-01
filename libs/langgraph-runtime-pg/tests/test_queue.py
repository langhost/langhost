"""Queue / HA tests: claim exclusivity, heartbeat reclaim, fanout."""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import suppress

import pytest
import redis.asyncio as redis
from httpx import ASGITransport, AsyncClient
from starlette.exceptions import HTTPException


async def _seed_pending_run():
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        return await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )


async def test_exactly_one_claim(pg_runtime):
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import has_run_heartbeat

    created = await _seed_pending_run()
    rid = created["run_id"]

    async def claim_one():
        return [
            (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
        ]

    winners = [
        item
        for batch in await asyncio.gather(claim_one(), claim_one(), claim_one())
        for item in batch
    ]
    assert len(winners) == 1
    assert winners[0][0] == rid
    assert await has_run_heartbeat(rid) is True

    async with connect() as conn:
        run = await anext(await ops.Runs.get(conn, rid, thread_id=created["thread_id"]))
        assert run["status"] == "running"


async def test_one_running_per_thread_under_concurrent_claim(pg_runtime):
    """Two pending runs on one thread must not both become running."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        first = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )
        tid = first["thread_id"]
        second = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                thread_id=tid,
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )

    rids = {first["run_id"], second["run_id"]}

    async def claim_one():
        return [
            (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
        ]

    winners = [
        item
        for batch in await asyncio.gather(claim_one(), claim_one(), claim_one(), claim_one())
        for item in batch
    ]
    assert len(winners) == 1, f"expected one winner, got {winners}"
    assert winners[0][0] in rids

    async with connect() as conn:
        statuses = []
        for rid in rids:
            run = await anext(await ops.Runs.get(conn, rid, thread_id=tid))
            statuses.append(run["status"])
        assert statuses.count("running") == 1
        assert statuses.count("pending") == 1


async def test_stale_heartbeat_reclaim(pg_runtime):
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import clear_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass

    await clear_run_heartbeat(rid)
    assert rid in await ops.Runs.sweep()

    async with connect() as conn:
        run = await anext(await ops.Runs.get(conn, rid, thread_id=thread_id))
        assert run["status"] == "pending"

    reclaimed = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert len(reclaimed) == 1
    assert reclaimed[0] == (rid, 2)


async def test_max_retries_marks_error(pg_runtime, monkeypatch):
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import PgRetryCounter, connect, get_session_factory
    from langgraph_runtime_pg.redis_stream import clear_run_heartbeat

    monkeypatch.setattr("langgraph_api.config.BG_JOB_MAX_RETRIES", 1)

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    assert await PgRetryCounter(get_session_factory()).get(rid) >= 1

    await clear_run_heartbeat(rid)
    assert rid in await ops.Runs.sweep()

    async with connect() as conn:
        run = await anext(await ops.Runs.get(conn, rid, thread_id=thread_id))
        assert run["status"] == "error"


async def test_sweep_skips_live_heartbeat(pg_runtime):
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(rid, ttl=60)

    assert rid not in await ops.Runs.sweep()

    async with connect() as conn:
        run = await anext(await ops.Runs.get(conn, rid, thread_id=thread_id))
        assert run["status"] == "running"


async def test_cancel_running_clears_thread_busy_when_worker_gone(pg_runtime):
    """Cancel of a running run must not leave the thread stuck busy forever."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import clear_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await clear_run_heartbeat(rid)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [rid], thread_id=thread_id, action="interrupt")
        run = await anext(await ops.Runs.get(conn, rid, thread_id=thread_id))
        thread = await anext(await ops.Threads.get(conn, thread_id))

    assert run["status"] == "interrupted"
    assert await ops.Runs.sweep() == []
    assert thread["status"] != "busy", (
        "thread left busy after cancelling a dead running run; sweep cannot "
        f"repair interrupted runs (status={thread['status']!r})"
    )


async def test_cancel_running_blocks_next_claim_while_heartbeat_alive(pg_runtime):
    """Do not claim another run while a cancelled worker still heartbeats."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import (
        clear_run_heartbeat,
        has_run_heartbeat,
        set_run_heartbeat,
    )

    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        first = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )
        tid = first["thread_id"]
        second = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                thread_id=tid,
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(first["run_id"], ttl=60)
    assert await has_run_heartbeat(first["run_id"]) is True

    async with connect() as conn:
        await ops.Runs.cancel(conn, [first["run_id"]], thread_id=tid, action="interrupt")

    claimed = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert claimed == [], (
        f"second run claimed while cancelled worker heartbeat still live: {claimed}; "
        f"second={second['run_id']}"
    )

    async with connect() as conn:
        statuses = {
            rid: (await anext(await ops.Runs.get(conn, rid, thread_id=tid)))["status"]
            for rid in (first["run_id"], second["run_id"])
        }
    assert statuses[first["run_id"]] == "interrupted"
    assert statuses[second["run_id"]] == "pending"

    await clear_run_heartbeat(first["run_id"])
    claimed_after = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert claimed_after == [(second["run_id"], 1)]


async def test_cancel_running_keeps_thread_busy_while_heartbeat_alive(pg_runtime):
    """Cancel must not idle the thread while the cancelled worker still heartbeats."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(rid, ttl=60)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [rid], thread_id=thread_id, action="interrupt")
        run = await anext(await ops.Runs.get(conn, rid, thread_id=thread_id))
        thread = await anext(await ops.Threads.get(conn, thread_id))

    assert run["status"] == "interrupted"
    assert thread["status"] == "busy", (
        "thread idled while cancelled worker heartbeat still live "
        f"(status={thread['status']!r}); State.post / clients would race the worker"
    )


async def test_state_update_blocked_while_cancelled_worker_heartbeat_alive(pg_runtime):
    """Threads.State.post must 409 while a cancelled worker heartbeat is live."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(rid, ttl=60)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [rid], thread_id=thread_id, action="interrupt")
        post = ops.Threads.State.post
        payload = {"configurable": {"thread_id": str(thread_id)}}
        with pytest.raises(HTTPException) as ei:
            await post(conn, payload, values={"x": 1})
    assert ei.value.status_code == 409, (
        f"expected 409 while cancelled worker still heartbeats, got {ei.value}"
    )


async def test_sweep_idles_busy_thread_after_cancelled_worker_heartbeat_expires(pg_runtime):
    """After cancel+live-hb, sweep must idle if the worker dies without set_joint_status."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import clear_run_heartbeat, set_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(rid, ttl=60)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [rid], thread_id=thread_id, action="interrupt")
        thread = await anext(await ops.Threads.get(conn, thread_id))
    assert thread["status"] == "busy"

    await clear_run_heartbeat(rid)
    await ops.Runs.sweep()

    async with connect() as conn:
        thread = await anext(await ops.Threads.get(conn, thread_id))
    assert thread["status"] != "busy", (
        f"thread stuck busy after cancelled worker heartbeat expired (status={thread['status']!r})"
    )


async def test_next_ignores_heartbeat_on_terminal_sibling(pg_runtime):
    """A leftover Redis heartbeat on a finished sibling must not block claim."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        first = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )
        tid = first["thread_id"]
        second = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                thread_id=tid,
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass

    async with connect() as conn:
        await ops.Runs.set_status(conn, first["run_id"], "success")
        from langgraph_runtime_pg.models import ThreadRow

        row = await conn.session.get(ThreadRow, tid)
        assert row is not None
        row.status = "busy"
        await conn.session.flush()
    await set_run_heartbeat(first["run_id"], ttl=60)

    claimed = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert claimed == [(second["run_id"], 1)], (
        f"pending run blocked by stale heartbeat on terminal sibling "
        f"first={first['run_id']} status=success; claimed={claimed}"
    )


async def test_next_treats_unknown_heartbeat_as_blocking(pg_runtime, monkeypatch):
    """When Redis cannot answer EXISTS, do not claim another run on that thread."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        first = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )
        tid = first["thread_id"]
        second = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                thread_id=tid,
                metadata={},
                prevent_insert_if_inflight=False,
            )
        )

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(first["run_id"], ttl=60)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [first["run_id"]], thread_id=tid, action="interrupt")

    async def _unknown(_run_id):
        return None

    monkeypatch.setattr(ops, "has_run_heartbeat", _unknown)

    claimed = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert claimed == [], (
        f"second run claimed while heartbeat check returned None (unknown): {claimed}; "
        f"second={second['run_id']}"
    )


async def test_state_update_blocked_when_heartbeat_check_unknown(pg_runtime, monkeypatch):
    """State.post must 409 when heartbeat existence cannot be determined."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import set_run_heartbeat

    created = await _seed_pending_run()
    rid, thread_id = created["run_id"], created["thread_id"]

    async for _ in ops.Runs.next(wait=False, limit=1):
        pass
    await set_run_heartbeat(rid, ttl=60)

    async with connect() as conn:
        await ops.Runs.cancel(conn, [rid], thread_id=thread_id, action="interrupt")

    async def _unknown(_run_id):
        return None

    monkeypatch.setattr(ops, "has_run_heartbeat", _unknown)

    async with connect() as conn:
        post = ops.Threads.State.post
        payload = {"configurable": {"thread_id": str(thread_id)}}
        with pytest.raises(HTTPException) as ei:
            await post(conn, payload, values={"x": 1})
    assert ei.value.status_code == 409, (
        f"expected 409 when heartbeat check is unknown, got {ei.value}"
    )


async def test_pubsub_fanout_across_managers(pg_runtime):
    from langgraph_runtime_pg.redis_stream import Message, StreamManager, get_stream_manager

    sm_a = get_stream_manager()
    run_id, thread_id = uuid.uuid4(), uuid.uuid4()

    client_b = redis.from_url(os.environ["REDIS_URI"], decode_responses=False)
    sm_b = StreamManager(client_b)
    sm_b.start_mux()
    q_b = await sm_b.add_queue(run_id, thread_id)
    await asyncio.sleep(0.15)

    await sm_a.put(
        run_id,
        thread_id,
        Message(topic=f"run:{run_id}:stream".encode(), data=b"from-a"),
        resumable=False,
    )
    assert (await asyncio.wait_for(q_b.get(), timeout=3.0)).data == b"from-a"

    await sm_b.aclose_fanout()
    await client_b.aclose()


async def test_thread_stream_dash_replays_redis_when_local_buffer_empty(pg_runtime):
    """Join with last_event_id='-' must replay from Redis when local buffer is empty."""
    from langgraph_api.utils.stream_codec import STREAM_CODEC

    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.redis_stream import Message, get_stream_manager

    created = await _seed_pending_run()
    rid, tid = created["run_id"], created["thread_id"]
    sm = get_stream_manager()

    marker = b'{"v":"cold-replica-history"}'
    payload = STREAM_CODEC.encode("values", marker)
    await sm.put(
        rid,
        tid,
        Message(topic=f"run:{rid}:stream".encode(), data=payload),
        resumable=True,
    )
    sm.message_stores.pop(tid, None)

    events: list[tuple[bytes, bytes]] = []

    async def _drain() -> None:
        async for event, message, _sid, _run in ops.Threads.Stream.join_event_streaming(
            tid,
            last_event_id="-",
            stream_modes=["run_modes"],
        ):
            events.append((event, message))
            if len(events) >= 1:
                return

    task = asyncio.create_task(_drain())
    try:
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            raise AssertionError(
                "expected Redis history replay for last_event_id='-' after "
                "local message_stores was cleared (cold replica / cleared buffer)"
            ) from None
    finally:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    assert events[0][0] == b"values"
    assert events[0][1] == marker


async def test_thread_stream_replays_redis_for_run_discovered_after_join(pg_runtime, monkeypatch):
    """A cross-worker run may finish before the thread stream discovers it."""
    from langgraph_api.utils.stream_codec import STREAM_CODEC

    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import Message, get_stream_manager

    aid, tid = uuid.uuid4(), uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g1", name="test", config={}, metadata={})
        )
        await anext(await ops.Threads.put(conn, tid))

    first_subscription_done = asyncio.Event()
    allow_next_scan = asyncio.Event()
    original_resubscribe = ops._maybe_resubscribe
    calls = 0

    async def _controlled_resubscribe(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            await allow_next_scan.wait()
        result = await original_resubscribe(*args, **kwargs)
        if calls == 1:
            first_subscription_done.set()
        return result

    monkeypatch.setattr(ops, "_maybe_resubscribe", _controlled_resubscribe)

    marker = b'{"v":"late-run-redis-history"}'
    events: list[tuple[bytes, bytes]] = []

    async def _drain() -> None:
        async for event, message, _sid, _run in ops.Threads.Stream.join_event_streaming(
            tid,
            last_event_id="-",
            stream_modes=["run_modes"],
        ):
            events.append((event, message))
            return

    task = asyncio.create_task(_drain())
    try:
        await asyncio.wait_for(first_subscription_done.wait(), timeout=2.0)
        async with connect() as conn:
            created = await anext(
                await ops.Runs.put(
                    conn,
                    aid,
                    {"config": {}},
                    thread_id=tid,
                    metadata={},
                    prevent_insert_if_inflight=False,
                )
            )
        rid = created["run_id"]
        sm = get_stream_manager()
        await sm.put(
            rid,
            tid,
            Message(
                topic=f"run:{rid}:stream".encode(),
                data=STREAM_CODEC.encode("values", marker),
            ),
            resumable=True,
        )
        # Simulate another worker: Redis has the frame, this process does not.
        sm.message_stores.pop(tid, None)
        allow_next_scan.set()
        await asyncio.wait_for(task, timeout=5.0)
    finally:
        allow_next_scan.set()
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    assert events == [(b"values", marker)]


async def test_threads_search_honors_extract(pg_runtime):
    """Threads.search must populate ``extracted`` when ``extract`` is passed."""
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.models import ThreadRow

    tid = uuid.uuid4()
    async with connect() as conn:
        conn.session.add(
            ThreadRow(
                thread_id=tid,
                status="idle",
                metadata_={"owner": "ops", "env": "test"},
                config={"configurable": {"model": "fast"}},
                values_={"messages": [{"role": "user", "content": "hi"}], "n": 7},
                interrupts={},
            )
        )
        await conn.session.flush()

        it, _ = await ops.Threads.search(
            conn,
            ids=[tid],
            extract={
                "last_content": "values.messages[-1].content",
                "owner": "metadata.owner",
                "model": "config.configurable.model",
            },
        )
        rows = [row async for row in it]

    assert len(rows) == 1
    assert "extracted" in rows[0], (
        "Threads.search ignored extract=; expected an 'extracted' projection "
        f"on the thread dict, got keys={sorted(rows[0])}"
    )
    assert rows[0]["extracted"] == {
        "last_content": "hi",
        "owner": "ops",
        "model": "fast",
    }

    async with connect() as conn:
        it, _ = await ops.Threads.search(
            conn,
            ids=[tid],
            select=["thread_id", "status"],
            extract={"n": "values.n"},
        )
        projected = [row async for row in it]
    assert projected == [
        {
            "thread_id": tid,
            "status": "idle",
            "extracted": {"n": 7},
        }
    ]


async def test_fake_death_queue_reclaims_and_finishes(api_lifespan_no_queue, monkeypatch):
    """Orphan a claimed run (dead worker), start queue → sweep → success."""
    monkeypatch.setattr("langgraph_api.config.N_JOBS_PER_WORKER", 1)

    from langgraph_api.server import app

    from langgraph_runtime_pg import ops, queue
    from langgraph_runtime_pg.database import connect
    from langgraph_runtime_pg.redis_stream import clear_run_heartbeat, wake_run_queue

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        aid = str(uuid.uuid4())
        ares = await client.post(
            "/assistants",
            json={
                "assistant_id": aid,
                "graph_id": "tools_agent",  # finishes to success (unlike streaming "agent")
                "name": "fake-death",
                "config": {},
                "metadata": {},
            },
        )
        assert ares.status_code in (200, 201), ares.text

        tres = await client.post("/threads", json={"metadata": {}})
        assert tres.status_code in (200, 201), tres.text
        tid = tres.json()["thread_id"]

        run_res = await client.post(
            f"/threads/{tid}/runs",
            json={
                "assistant_id": aid,
                "input": {"messages": [{"role": "user", "content": "reclaim-me"}]},
            },
        )
        assert run_res.status_code in (200, 201), run_res.text
        rid = uuid.UUID(run_res.json()["run_id"])
        tid_uuid = uuid.UUID(tid)

    claimed = [
        (run["run_id"], attempt) async for run, attempt in ops.Runs.next(wait=False, limit=1)
    ]
    assert len(claimed) == 1 and claimed[0] == (rid, 1)

    await clear_run_heartbeat(rid)
    await wake_run_queue()

    qtask = asyncio.create_task(queue.queue(), name="fake-death-queue")
    try:

        async def _wait_terminal() -> str:
            while True:
                async with connect() as conn:
                    run = await anext(await ops.Runs.get(conn, rid, thread_id=tid_uuid))
                    status = run["status"]
                if status in ("success", "error", "interrupted", "timeout"):
                    return status
                await asyncio.sleep(0.5)

        status = await asyncio.wait_for(_wait_terminal(), timeout=30.0)
        assert status == "success", f"expected success after fake death, got {status}"
    finally:
        qtask.cancel()
        with suppress(asyncio.CancelledError):
            await qtask
