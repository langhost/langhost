from __future__ import annotations

import uuid

import pytest


@pytest.mark.usefixtures("pg_runtime")
async def test_patch_preserves_context_when_omitted() -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    assistant_id = uuid.uuid4()
    original_context = {"tenant_id": "tenant-1", "role": "support"}

    async with connect() as conn:
        await anext(
            await ops.Assistants.put(
                conn,
                assistant_id,
                graph_id="g1",
                context=original_context,
                metadata={"revision": 1},
            )
        )

    async with connect() as conn:
        patched = await anext(
            await ops.Assistants.patch(
                conn,
                assistant_id,
                name="renamed",
                metadata={"revision": 2},
            )
        )
        versions = [
            version async for version in await ops.Assistants.get_versions(conn, assistant_id)
        ]

    assert patched["context"] == original_context
    assert versions[0]["context"] == original_context


@pytest.mark.usefixtures("pg_runtime")
async def test_patch_can_explicitly_clear_context() -> None:
    from langgraph_runtime_pg import ops
    from langgraph_runtime_pg.database import connect

    assistant_id = uuid.uuid4()

    async with connect() as conn:
        await anext(
            await ops.Assistants.put(
                conn,
                assistant_id,
                graph_id="g1",
                context={"tenant_id": "tenant-1"},
            )
        )

    async with connect() as conn:
        patched = await anext(await ops.Assistants.patch(conn, assistant_id, context={}))

    assert patched["context"] == {}
