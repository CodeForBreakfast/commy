"""A minimal real MCP server over stdio, for the per-topic transport tests.

Stands in for the commy server so the lifecycle tests exercise a REAL
subprocess + REAL MCP handshake + REAL ``notifications/message`` emission with no
Zulip realm. It is faithful to the one behaviour under test: after the client
completes the MCP initialize handshake, it emits the inbound carrier the
substrate dual-emits — ``notifications/message`` with the ``{content, meta}``
frame nested under ``params.data`` (the bb7.1 envelope).

To prove the manager's env wiring end-to-end, the emitted frame echoes the
``COMMY_BOT_NAME`` and ``COMMY_SUBSCRIBE`` the parent process set —
so the test can assert the deterministic identity + subscriptions actually
reached a real subprocess. It also writes its own PID to ``STUB_PIDFILE`` so the
test can confirm the subprocess is really gone after an idle reap.
"""

import os

import anyio
from mcp.server.models import InitializationOptions
from mcp.server.session import InitializationState, ServerSession
from mcp.server.stdio import stdio_server
from mcp.types import ServerCapabilities

FRAME_CONTENT = "stub inbound frame"


def _frame() -> dict:
    return {
        "content": FRAME_CONTENT,
        "meta": {
            "channel_name": os.environ.get("STUB_CHANNEL", ""),
            "thread": os.environ.get("STUB_TOPIC", ""),
            "message_id": "stub-1",
            # Echo the env the parent computed, so the test can assert the
            # deterministic identity + subscriptions reached a real subprocess.
            "echo_bot_name": os.environ.get("COMMY_BOT_NAME", ""),
            "echo_subscribe": os.environ.get("COMMY_SUBSCRIBE", ""),
        },
    }


async def _serve() -> None:
    pidfile = os.environ.get("STUB_PIDFILE")
    if pidfile:
        with open(pidfile, "w") as handle:
            handle.write(str(os.getpid()))

    init_options = InitializationOptions(
        server_name="stub-commy",
        server_version="0.0.0",
        capabilities=ServerCapabilities(logging={}),
    )
    frame = _frame()

    async with stdio_server() as (read_stream, write_stream):
        async with ServerSession(read_stream, write_stream, init_options) as session:
            async with anyio.create_task_group() as task_group:

                async def emit() -> None:
                    # The substrate emits the frame server-initiated (catch-up /
                    # live). Re-emit on a short interval once initialized; the
                    # client dedups, so repeats are harmless and the test just
                    # waits for the first to land.
                    while True:
                        if session._initialization_state == InitializationState.Initialized:
                            try:
                                await session.send_log_message(
                                    level="info", data=frame, logger="commy"
                                )
                            except Exception:
                                pass
                        await anyio.sleep(0.05)

                task_group.start_soon(emit)
                async for _ in session.incoming_messages:
                    pass
                # Client disconnected (stdin closed) -> stop emitting and exit.
                task_group.cancel_scope.cancel()


if __name__ == "__main__":
    anyio.run(_serve)
