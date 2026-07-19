"""Real-subprocess + real-SDK tests for the per-topic transport.

No mocks and no Zulip: the manager drives the real ``McpTopicTransport`` against
a real stub MCP server subprocess (``_stub_mcp_server.py``). This proves the
whole I/O path the unit tests stub out — subprocess spawn, MCP initialize, env
wiring, the ``notifications/message`` -> ``params.data`` -> sink routing, idle
reap tearing the subprocess down, and respawn — end to end.

``_on_log`` is additionally tested against the genuine SDK
``LoggingMessageNotificationParams`` type so the carrier-binding logic is pinned
even without spinning a subprocess.
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

from commy.connection import ConnectionSpec, SpawnConfig, TopicConnectionManager
from commy.naming import deterministic_bot_name
from commy.transport import McpTopicTransport

_STUB = str(Path(__file__).parent / "_stub_mcp_server.py")
_POST_STUB = str(Path(__file__).parent / "_stub_post_server.py")


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _stub_config(clock_idle: float, pidfile: str, errlog_path: str) -> SpawnConfig:
    # Point command/args at the stub server instead of `bun packages/mcp/server.ts`.
    return SpawnConfig(
        repo_dir=str(Path(__file__).parent),
        zulip_site="https://zulip.example",
        minter_email="minter@example.com",
        minter_api_key="key",
        command=sys.executable,
        args=(_STUB,),
        idle_timeout_seconds=clock_idle,
        extra_env={"STUB_PIDFILE": pidfile, "STUB_ERRLOG": errlog_path},
    )


def _transport_factory(errlog_path: str):
    def factory(spec, sink):
        handle = open(errlog_path, "a")  # noqa: SIM115 — closed by GC at process end
        return McpTopicTransport(spec, sink, errlog=handle, stop_grace_seconds=5.0)

    return factory


async def _wait_for(predicate, timeout: float = 8.0, interval: float = 0.05) -> bool:
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        await asyncio.sleep(interval)
        waited += interval
    return predicate()


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_manager_spawns_real_subprocess_routes_frame_and_reaps():
    clock = _FakeClock()
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = os.path.join(tmp, "stub.pid")
        errlog = os.path.join(tmp, "stub.stderr.log")
        received: list = []

        async def sink(frame) -> None:
            received.append(frame)

        manager = TopicConnectionManager(
            _stub_config(300.0, pidfile, errlog),
            _transport_factory(errlog),
            sink,
            clock=clock,
        )

        async def scenario():
            await manager.ensure("myproject", "standup")
            got = await _wait_for(lambda: len(received) >= 1)
            assert got, "no inbound frame routed from the real subprocess"

            # The frame came through the real notifications/message carrier...
            frame = received[0]
            assert frame["content"] == "stub inbound frame"
            # ...carrying the env the manager computed -> proves real env wiring.
            assert frame["meta"]["echo_bot_name"] == deterministic_bot_name(
                "myproject", "standup"
            )
            assert frame["meta"]["echo_subscribe"] == "myproject/standup"

            pid = int(Path(pidfile).read_text())
            assert _process_alive(pid)

            # Idle reap tears the real subprocess down.
            clock.advance(301)
            reaped = await manager.reap_idle()
            assert reaped == [("myproject", "standup")]
            gone = await _wait_for(lambda: not _process_alive(pid))
            assert gone, "subprocess survived idle reap"
            assert manager.active_keys() == set()

        asyncio.run(scenario())


def test_respawn_after_reap_brings_up_a_fresh_subprocess():
    clock = _FakeClock()
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = os.path.join(tmp, "stub.pid")
        errlog = os.path.join(tmp, "stub.stderr.log")
        received: list = []

        async def sink(frame) -> None:
            received.append(frame)

        manager = TopicConnectionManager(
            _stub_config(300.0, pidfile, errlog),
            _transport_factory(errlog),
            sink,
            clock=clock,
        )

        async def scenario():
            spec1 = await manager.ensure("c", "t")
            assert await _wait_for(lambda: len(received) >= 1)
            pid1 = int(Path(pidfile).read_text())

            clock.advance(301)
            await manager.reap_idle()
            assert await _wait_for(lambda: not _process_alive(pid1))

            received.clear()
            spec2 = await manager.ensure("c", "t")
            assert await _wait_for(lambda: len(received) >= 1)
            pid2 = int(Path(pidfile).read_text())

            # Same deterministic identity name (substrate reuses the user_id),
            # but a genuinely new OS process.
            assert spec1.bot_name == spec2.bot_name
            assert pid2 != pid1
            await manager.shutdown()
            assert await _wait_for(lambda: not _process_alive(pid2))

        asyncio.run(scenario())


def test_shutdown_stops_real_subprocess():
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = os.path.join(tmp, "stub.pid")
        errlog = os.path.join(tmp, "stub.stderr.log")

        async def sink(frame) -> None:
            pass

        manager = TopicConnectionManager(
            _stub_config(300.0, pidfile, errlog),
            _transport_factory(errlog),
            sink,
        )

        async def scenario():
            await manager.ensure("c", "t")
            assert await _wait_for(lambda: Path(pidfile).exists())
            pid = int(Path(pidfile).read_text())
            assert _process_alive(pid)
            await manager.shutdown()
            assert await _wait_for(lambda: not _process_alive(pid))
            assert manager.active_keys() == set()

        asyncio.run(scenario())


# --- outbound post over a real MCP tools/call round-trip ---------------------


def _post_stub_spec(record_path: str) -> ConnectionSpec:
    return ConnectionSpec(
        channel="myproject",
        topic="standup",
        bot_name="stub-bot",
        command=sys.executable,
        args=(_POST_STUB,),
        cwd=str(Path(__file__).parent),
        env={"STUB_POST_RECORD": record_path},
    )


def test_transport_post_calls_the_commy_post_tool_over_real_mcp():
    with tempfile.TemporaryDirectory() as tmp:
        record = os.path.join(tmp, "post.json")
        errlog = os.path.join(tmp, "stub.stderr.log")

        async def sink(frame) -> None:
            pass

        async def scenario():
            handle = open(errlog, "a")  # noqa: SIM115
            transport = McpTopicTransport(_post_stub_spec(record), sink, errlog=handle)
            await transport.start()
            try:
                message_id = await transport.post(
                    "here is my reply", "myproject", "standup"
                )
            finally:
                await transport.stop()
            return message_id

        message_id = asyncio.run(scenario())

        assert message_id == "stub-msg-1"
        recorded = json.loads(Path(record).read_text())
        assert recorded == {
            "channel_name": "myproject",
            "thread": "standup",
            "body": "here is my reply",
        }


def test_transport_post_without_a_live_session_returns_none():
    # A post issued before start (or after stop) has no session to ride; it must
    # degrade to None rather than raise into the turn.
    transport = McpTopicTransport.__new__(McpTopicTransport)
    transport._session = None  # type: ignore[attr-defined]

    result = asyncio.run(transport.post("body", "c", "t"))
    assert result is None


# --- carrier binding against the genuine SDK type (no subprocess) ------------


def test_on_log_forwards_params_data_to_sink():
    from mcp.types import LoggingMessageNotificationParams

    received: list = []

    async def sink(frame) -> None:
        received.append(frame)

    transport = McpTopicTransport.__new__(McpTopicTransport)
    transport._sink = sink  # type: ignore[attr-defined]

    frame = {"content": "hi", "meta": {"channel_name": "c", "thread": "t"}}
    params = LoggingMessageNotificationParams(level="info", logger="commy", data=frame)

    asyncio.run(transport._on_log(params))
    assert received == [frame]


def test_on_log_ignores_non_mapping_data():
    from mcp.types import LoggingMessageNotificationParams

    received: list = []

    async def sink(frame) -> None:
        received.append(frame)

    transport = McpTopicTransport.__new__(McpTopicTransport)
    transport._sink = sink  # type: ignore[attr-defined]

    params = LoggingMessageNotificationParams(level="info", logger="commy", data="not-a-frame")
    asyncio.run(transport._on_log(params))
    assert received == []
