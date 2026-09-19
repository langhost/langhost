# Agent Server compatibility

This matrix describes the source revision containing it. For a released build, use
the copy in that release's notes or its `COMPATIBILITY.md` asset. Changes on `main`
do not retroactively change an older release's coverage.

## Tested dependency set

These versions come from the frozen `uv.lock`. CI checks this table with
`scripts/check_versions.py` and runs the Postgres + Redis integration suite before
release. The current CI environment is Python 3.11, Postgres 16, and Redis 7;
package metadata allows Python 3.11 and later, but CI does not test every Python
version or dependency combination.

| Package | Tested version | Role |
| --- | --- | --- |
| `langhost` | `0.11.1.post1` | CLI; exact runtime dependency |
| `langgraph-runtime-pg` | `0.11.1.post1` | Postgres + Redis runtime |
| `langgraph-api` | `0.11.1` | Agent Server; exact runtime dependency |
| `langgraph-sdk` | `0.4.4` | Python client and upstream integration suite |
| `langgraph` | `1.2.11` | Graph execution |
| `langgraph-checkpoint` | `4.1.1` | Checkpoint interfaces |
| `langgraph-checkpoint-postgres` | `3.1.0` | Postgres checkpointer; exact runtime dependency |
| `langchain-core` | `1.6.1` | Message and model interfaces |
| `langchain` | `1.3.18` | Upstream integration graph dependency |
| `deepagents` | `0.7.11` | Upstream nested-agent fixture dependency |

The table is a tested combination, not a promise that all client dependencies are
exactly pinned in a downstream application. Workspace constraints and `uv.lock`
do not propagate to consumers of the published wheels. Lock your application's
dependencies and validate its graphs when changing this combination.

## Streaming interfaces

The version labels on these interfaces refer to different things:

- **SDK `runs.stream(version="v2")`** calls `/runs/stream` (or its thread-scoped
  equivalent). The SDK converts SSE frames into dictionaries containing `type`,
  `ns`, `data`, and `interrupts`. The `version` argument selects this client output
  shape; `stream_subgraphs=True` requests child graph events from the server.
- **Thread event protocol** uses `POST /threads/{thread_id}/commands` and
  `POST /threads/{thread_id}/stream/events`. Both exist in API 0.11.1. Subscriptions
  select channels and namespaces; commands include `run.start` and `input.respond`.
  This is the remote workflow called Protocol v2 in issue #19.
- **Local graph `astream_events(version="v3")`** is a graph execution interface.
  Its availability alone does not establish remote endpoint compatibility.

## Protocol feature matrix

“Tested” below means exercised against the Postgres runtime and Redis in CI. It
does not imply parity with every upstream release or every possible graph. Test
names refer to `libs/langgraph-runtime-pg/tests/test_e2e.py`, unless stated otherwise.

| Surface | Coverage | Evidence / scope |
| --- | --- | --- |
| SDK `runs.stream(version="v2")` | Tested | `test_runs_stream_v2_interrupt_resume_and_subgraphs`: dictionary shape, values/updates, interrupts moved out of values data, resume, terminal state |
| SDK v2 `stream_subgraphs` | Tested with both `False` and `True` | Same test: child namespaces and child values/updates are exposed only when requested |
| Thread `run.start` command | Tested over HTTP | `test_protocol_events_since_reconnect_preserves_nested_history`: command response and successful run |
| Thread `input.respond` command | Tested through SDK | `test_interrupt_respond_reaches_terminal` and `test_interrupt_respond_sync`: interrupt, response, final state |
| Thread SSE `values`, `messages`, `tools`, `lifecycle` | Tested over HTTP | Cursor test requires all four channels, checks SSE IDs/methods and root/child completion |
| Thread SSE `updates`, `input`, `tasks`, `checkpoints`, `custom` | Accepted by the pinned API; not individually asserted here | Event production depends on the graph and upstream projection; SDK run-stream `updates` coverage is a separate interface |
| Thread namespace/channel filters | Tested over HTTP | Cursor test selects one child namespace with `depth: 0`, restricts channels, and compares the result with unfiltered history |
| Thread reconnect with `since` | Tested over HTTP | Disconnect after a nested lifecycle event; finish the run; reconnect from its sequence; compare with complete replay for gaps, duplicates, ordering, and stable event identities |
| Remote tools and subgraphs | Tested through SDK | `test_tools_agent_tool_calls_channel`, `test_deep_agent_subgraphs_stream_reaches_terminal`, and the matching upstream SDK integration tests |
| WebSocket interrupt/resume | Tested through SDK | `test_websocket_interrupt_respond` |
| Late observer joining resumable history | Tested | `test_protocol_v3_late_observer_replays_resumable_history`; separate from the explicit `since` cursor test |
| MCP, A2A, Studio, third-party UIs | Inherited upstream surfaces | No dedicated end-to-end compatibility checks in this matrix |

### Reconnect scope

For the thread event endpoint, send the last received integer `seq` as `since` in
the POST body. The SSE `id` is that sequence encoded as text. Resume should emit
events with a strictly greater sequence. The `event_id` identifies an event; it
is not the `since` cursor. Run-stream `last_event_id` is a separate mechanism and
should not be substituted for a thread event sequence.

The regression test uses the same thread and subscription channels, then checks
a narrower namespace/channel subscription. It covers one nested agent run and a
short disconnect while replay history remains available. It does not establish
cursor recovery after history expiry, server/Redis restart, failover, or a switch
between server workers. It also does not establish cursor stability across
multiple runs in one thread. Persisted graph checkpoints alone do not guarantee
indefinite event replay.

## Upstream upgrade policy

Langhost targets an explicit, tested Agent Server release. It does not resolve
automatically to the newest API. The runtime's `langgraph-api==X.Y.Z` pin stays
exact because internal runtime interfaces can change independently of HTTP
endpoints. Both langhost packages use that base version; `.postN` releases carry
langhost fixes for the same API.

Evaluate API upgrades in a dedicated PR branch. Update the exact API pin, both
package versions, runtime interface adaptations, and frozen SDK/graph dependencies
together. Run the interface, queue, CLI/lifespan, first-party live E2E, and matching
upstream SDK suites against Postgres + Redis. Include the explicit v2 and cursor
tests above. Update this matrix and describe any API, schema, or runtime migration
requirements before merging or releasing. Do not widen the API constraint to make
an incompatible upgrade resolve.

SDK and graph dependency updates may ship on the existing API line when the same
checks pass; they do not imply an API upgrade. If an API upgrade cannot pass these
gates, retain the last tested pin and document the blocking incompatibilities in
the upgrade PR. A separate legacy maintenance branch needs an explicit maintainer
decision; this policy makes no ongoing support commitment for untested older
combinations. Release notes include the matrix from the released tag alongside
the generated change list.
