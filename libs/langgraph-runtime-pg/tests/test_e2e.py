"""Live-server E2E against LANGGRAPH_INTEGRATION_URL (sdk-py graphs)."""

from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.e2e

ASSISTANT_ID = "agent"
TOOLS_ASSISTANT_ID = "tools_agent"
DEEP_AGENT_ASSISTANT_ID = "deep_agent"
EXPECTED_TERMINAL_ITEMS = ["streamed", "tool", "asked", "sub"]

AGENT_INPUT = {"messages": [], "value": "init", "items": []}
TOOLS_INPUT = {"messages": [{"role": "user", "content": "search for v3"}]}
DEEP_INPUT = {"messages": [{"role": "user", "content": "research the v3 spec"}]}


async def test_interrupt_respond_reaches_terminal(async_threads) -> None:
    threads, _ = async_threads
    async with threads.stream(assistant_id=ASSISTANT_ID) as thread:
        await thread.run.start(input=AGENT_INPUT)
        async for _ in thread.values:
            if thread.interrupted:
                break
        assert thread.interrupted, "expected ask_human interrupt"
        assert thread.interrupts
        await thread.run.respond("yes")
        final = await thread.output
        assert final.get("items") == EXPECTED_TERMINAL_ITEMS


def test_interrupt_respond_sync(sync_threads) -> None:
    threads, _ = sync_threads
    with threads.stream(assistant_id=ASSISTANT_ID) as thread:
        thread.run.start(input=AGENT_INPUT)
        for _ in thread.values:
            if thread.interrupted:
                break
        assert thread.interrupted
        thread.run.respond("yes")
        assert thread.output.get("items") == EXPECTED_TERMINAL_ITEMS


async def test_update_state_while_interrupted(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        await async_sdk.runs.wait(
            tid,
            ASSISTANT_ID,
            input=AGENT_INPUT,
            interrupt_before=["ask_human"],
        )
        await async_sdk.threads.update_state(tid, values={"value": "patched"})
        state = await async_sdk.threads.get_state(tid)
        values = state.get("values") or {}
        assert values.get("value") == "patched"
    finally:
        await async_sdk.threads.delete(tid)


async def test_tools_agent_wait_success(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        result = await async_sdk.runs.wait(tid, TOOLS_ASSISTANT_ID, input=TOOLS_INPUT)
        assert isinstance(result, dict)
        runs = await async_sdk.runs.list(tid, limit=1)
        assert runs[0]["status"] == "success"
    finally:
        await async_sdk.threads.delete(tid)


async def test_deep_agent_wait_completes(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        result = await async_sdk.runs.wait(tid, DEEP_AGENT_ASSISTANT_ID, input=DEEP_INPUT)
        assert result is not None
        runs = await async_sdk.runs.list(tid, limit=1)
        assert runs[0]["status"] in {"success", "interrupted"}
    finally:
        await async_sdk.threads.delete(tid)


async def test_deep_agent_subgraphs_stream_reaches_terminal(async_threads) -> None:
    """A live projection must receive run completion, not only replay observers."""
    threads, _ = async_threads
    async with threads.stream(assistant_id=DEEP_AGENT_ASSISTANT_ID) as thread:
        await thread.run.start(input=DEEP_INPUT)

        async def collect_subgraphs():
            return [handle async for handle in thread.subgraphs]

        handles = await asyncio.wait_for(collect_subgraphs(), timeout=15)
        assert handles, "deep_agent should produce at least one direct-child handle"


async def test_tools_agent_tool_calls_channel(async_threads) -> None:
    threads, _ = async_threads
    async with threads.stream(assistant_id=TOOLS_ASSISTANT_ID) as thread:
        await thread.run.start(input=TOOLS_INPUT)
        # Drain fully — early break leaves ToolCallHandle.output futures unretrieved.
        handles = [h async for h in thread.tool_calls]
        assert handles, "tools_agent should emit tool_calls"
        for handle in handles:
            await handle.output
        final = await thread.output
        assert final is not None


async def test_cancel_prevents_success(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        run = await async_sdk.runs.create(tid, ASSISTANT_ID, input=AGENT_INPUT)
        rid = run["run_id"]
        await async_sdk.runs.cancel(tid, rid, wait=True, action="interrupt")
        fetched = await async_sdk.runs.get(tid, rid)
        assert fetched["status"] != "success"
        assert fetched["status"] in {"interrupted", "error", "pending", "running"}
    finally:
        await async_sdk.threads.delete(tid)


async def test_second_run_rejected_while_busy(async_sdk) -> None:
    """Second run with multitask_strategy=reject must 409 while thread is busy."""
    from langgraph_sdk.errors import ConflictError

    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        first = await async_sdk.runs.create(tid, ASSISTANT_ID, input=AGENT_INPUT)
        with pytest.raises(ConflictError):
            await async_sdk.runs.create(
                tid,
                ASSISTANT_ID,
                input=AGENT_INPUT,
                multitask_strategy="reject",
            )
        try:
            await async_sdk.runs.cancel(tid, first["run_id"], wait=False)
        except Exception:
            pass
    finally:
        await async_sdk.threads.delete(tid)


async def test_parallel_threads_both_complete(async_sdk) -> None:
    async def one() -> str:
        thread = await async_sdk.threads.create()
        tid = thread["thread_id"]
        try:
            await async_sdk.runs.wait(tid, TOOLS_ASSISTANT_ID, input=TOOLS_INPUT)
            runs = await async_sdk.runs.list(tid, limit=1)
            assert runs[0]["status"] == "success"
            return tid
        finally:
            await async_sdk.threads.delete(tid)

    a, b = await asyncio.gather(one(), one())
    assert a != b


async def test_stream_values_then_interrupt(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        events = []
        async for part in async_sdk.runs.stream(
            tid,
            ASSISTANT_ID,
            input=AGENT_INPUT,
            stream_mode="values",
            interrupt_before=["ask_human"],
        ):
            events.append(part)
            if len(events) >= 40:
                break
        assert events, "expected at least one values stream event"
        runs = await async_sdk.runs.list(tid, limit=1)
        assert runs[0]["status"] in {"interrupted", "success", "pending", "running"}
    finally:
        await async_sdk.threads.delete(tid)


async def test_thread_copy_preserves_history(async_sdk) -> None:
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    copy_id = None
    try:
        await async_sdk.runs.wait(
            tid,
            ASSISTANT_ID,
            input=AGENT_INPUT,
            interrupt_before=["ask_human"],
        )
        history = await async_sdk.threads.get_history(tid, limit=20)
        assert len(history) >= 1
        copied = await async_sdk.threads.copy(tid)
        copy_id = copied["thread_id"]
        assert copy_id != tid
        copy_history = await async_sdk.threads.get_history(copy_id, limit=20)
        assert len(copy_history) >= 1
    finally:
        await async_sdk.threads.delete(tid)
        if copy_id:
            await async_sdk.threads.delete(copy_id)


async def test_websocket_interrupt_respond(async_threads) -> None:
    threads, _ = async_threads
    async with threads.stream(assistant_id=ASSISTANT_ID, transport="websocket") as thread:
        await thread.run.start(input=AGENT_INPUT)
        async for _ in thread.values:
            if thread.interrupted:
                break
        assert thread.interrupted
        await thread.run.respond("yes")
        final = await thread.output
        assert final.get("items") == EXPECTED_TERMINAL_ITEMS


async def test_protocol_v3_late_observer_replays_resumable_history(
    async_sdk, async_threads
) -> None:
    """Late observer joining with last_event_id='-' should see resumable history."""
    thread = await async_sdk.threads.create()
    tid = thread["thread_id"]
    try:
        produced = 0
        async for _ in async_sdk.runs.stream(
            tid,
            TOOLS_ASSISTANT_ID,
            input=TOOLS_INPUT,
            stream_mode="values",
            stream_resumable=True,
        ):
            produced += 1
        assert produced >= 1, "resumable producer should emit values"

        threads, _ = async_threads
        async with threads.stream(thread_id=tid, assistant_id=TOOLS_ASSISTANT_ID) as obs:
            replayed: list[object] = []

            async def _collect() -> None:
                async for value in obs.values:
                    replayed.append(value)
                    return

            try:
                await asyncio.wait_for(_collect(), timeout=15.0)
            except TimeoutError:
                pass
            assert replayed, (
                "Protocol v3 late observer should replay resumable values "
                "history when joining with last_event_id='-'"
            )
    finally:
        await async_sdk.threads.delete(tid)
