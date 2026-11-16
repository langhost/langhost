"""Behavior added for langgraph-api 0.18."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest


async def test_encryption_fields_round_trip_without_cipher() -> None:
    from langgraph_runtime_pg.encryption import decrypt_fields, encrypt_fields, encryption_fields

    assert encryption_fields("run") == ["metadata", "kwargs"]
    payload = {"metadata": {"owner": "a"}, "kwargs": {"input": {"n": 1}}}
    encrypted = await encrypt_fields(payload, "run", encryption_fields("run"), None)
    assert encrypted["metadata"] == {"owner": "a"}
    await decrypt_fields(encrypted, "run", encryption_fields("run"), None)
    assert encrypted["kwargs"]["input"] == {"n": 1}


async def test_thread_exclude_metadata_search_and_count(pg_runtime) -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    del pg_runtime
    kept = uuid.uuid4()
    hidden = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Threads.put(conn, kept, metadata={"kind": "visible"}, if_exists="raise")
        )
        await anext(
            await ops.Threads.put(
                conn, hidden, metadata={"kind": "visible", "internal": True}, if_exists="raise"
            )
        )
        found, _cursor = await ops.Threads.search(
            conn,
            metadata={"kind": "visible"},
            values=None,
            status=None,
            limit=10,
            offset=0,
            exclude_metadata=[{"internal": True}],
        )
        rows = [row async for row in found]
        count = await ops.Threads.count(
            conn,
            metadata={"kind": "visible"},
            exclude_metadata=[{"internal": True}],
        )
    assert {row["thread_id"] for row in rows} == {kept}
    assert count == 1


async def test_cron_clear_end_time(pg_runtime) -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    del pg_runtime
    aid = uuid.uuid4()
    end = datetime.now(UTC) + timedelta(days=1)
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g", name="a", config={}, metadata={})
        )
        created = await anext(
            await ops.Crons.put(
                conn,
                payload={"assistant_id": str(aid)},
                schedule="0 * * * *",
                end_time=end,
            )
        )
        # Absent end_time must not clear; only clear_end_time does.
        unchanged = await anext(
            await ops.Crons.update(
                conn,
                cron_id=created["cron_id"],
                enabled=True,
                end_time=None,
                clear_end_time=False,
            )
        )
        assert unchanged["end_time"] is not None
        cleared = await anext(
            await ops.Crons.update(
                conn,
                cron_id=created["cron_id"],
                end_time=None,
                clear_end_time=True,
            )
        )
    assert cleared["end_time"] is None


async def test_cancel_records_reason_and_kill_switch_progress(pg_runtime) -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    del pg_runtime
    aid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g", name="a", config={}, metadata={})
        )
        created = await anext(
            await ops.Runs.put(
                conn,
                aid,
                {"config": {}},
                metadata={},
                prevent_insert_if_inflight=False,
                langsmith_session_name="trace-me",
            )
        )
        progress = await ops.Runs.cancel_accepted_before(
            conn,
            datetime.now(UTC) + timedelta(seconds=5),
            reason="kill switch",
        )
        run = await anext(
            await ops.Runs.get(conn, created["run_id"], thread_id=created["thread_id"])
        )
    assert progress == {"not_yet_reached": 0, "finishing": 0}
    assert run["status"] == "interrupted"
    assert run["cancel_reason"] == "kill switch"
    assert run["langsmith_session_name"] == "trace-me"
    assert run["priority"] == 0


async def test_thread_storage_shape_and_requeue(pg_runtime) -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    del pg_runtime
    aid = uuid.uuid4()
    tid = uuid.uuid4()
    async with connect() as conn:
        await anext(
            await ops.Assistants.put(conn, aid, graph_id="g", name="a", config={}, metadata={})
        )
        await anext(await ops.Threads.put(conn, tid, metadata={}, if_exists="raise"))
        report = await ops.Threads.storage(conn, tid)
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
    assert report["total_bytes"] == 0
    assert set(report) == {"total_bytes", "checkpoints", "channel_values", "pending_writes"}

    claimed = [item async for item in ops.Runs.next(wait=False, limit=1)]
    assert len(claimed) == 1
    run, attempt, encryption_context = claimed[0]
    assert run["run_id"] == created["run_id"]
    assert attempt == 1
    assert encryption_context is None

    async with connect() as conn:
        await ops.Runs.requeue(conn, created["run_id"], "drained")
    again = [item async for item in ops.Runs.next(wait=False, limit=1)]
    assert again[0][1] == 1
    assert again[0][0]["status"] == "running"


@pytest.mark.asyncio
async def test_dependency_checks_ping(pg_runtime) -> None:
    from langgraph_runtime_pg.database import dependency_checks

    del pg_runtime
    checks = dependency_checks()
    assert set(checks) == {"postgres", "redis"}
    await checks["postgres"]()
    await checks["redis"]()
