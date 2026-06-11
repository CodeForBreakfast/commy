"""Boot-time channel listener tests (comms-a7j.4).

The listener is the ONE persistent-identity connection created at startup,
subscribed ``channel:<name>`` + ``mentions``. Its sole job: notice a
``(channel, topic)`` that no per-topic identity owns yet and trigger a spawn
(``ensure_topic_connection``); an already-owned topic is a no-op (dedup by
ownership). These tests drive the REAL listener logic — spec construction, the
unowned→trigger / owned→no-trigger decision, ownership read live off the
manager — with a recording fake transport + recording trigger, and prove the
whole I/O path end-to-end against a REAL stub MCP server subprocess (no Zulip).
Nothing asserts a double's canned value; every assertion is on the listener's
own decisions and the env that reached a real subprocess.
"""

import asyncio
import os
import re
import sys
import tempfile
from pathlib import Path

import pytest
from gateway.config import PlatformConfig
from gateway.platforms.base import MessageEvent
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

from commy import register
from commy.adapter import CommyAdapter
from commy.connection import SpawnConfig, TopicConnectionManager
from commy.listener import (
    ChannelListener,
    build_listener_spec,
    channel_subscribe_tokens,
)
from commy.naming import deterministic_bot_name, deterministic_listener_name
from commy.transport import McpTopicTransport, make_listener

_STUB = str(Path(__file__).parent / "_stub_mcp_server.py")


@pytest.fixture(autouse=True)
def _platform_registered():
    manager = PluginManager()
    manifest = PluginManifest(name="commy-platform", kind="platform")
    register(PluginContext(manifest, manager))


def _config(**overrides) -> SpawnConfig:
    base = dict(
        repo_dir="/opt/commy",
        zulip_site="https://zulip.example",
        minter_email="minter@example.com",
        minter_api_key="key",
        channel="epr-backend",
    )
    base.update(overrides)
    return SpawnConfig(**base)


class _FakeTransport:
    def __init__(self, spec, sink) -> None:
        self.spec = spec
        self.sink = sink
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def emit(self, frame) -> None:
        await self.sink(frame)


class _RecordingFactory:
    def __init__(self) -> None:
        self.created: list[_FakeTransport] = []

    def __call__(self, spec, sink) -> _FakeTransport:
        transport = _FakeTransport(spec, sink)
        self.created.append(transport)
        return transport


def _make_listener(*, owned=None, channel="epr-backend"):
    owned_keys = set() if owned is None else owned
    triggered: list[tuple[str, str]] = []

    async def trigger(ch, tp):
        triggered.append((ch, tp))

    factory = _RecordingFactory()
    listener = ChannelListener(
        build_listener_spec(_config(channel=channel), channel),
        factory,
        trigger,
        lambda: owned_keys,
    )
    listener.triggered = triggered  # type: ignore[attr-defined]
    listener.factory = factory  # type: ignore[attr-defined]
    return listener


def _frame(channel, thread, **meta):
    m: dict[str, str] = {}
    if channel is not None:
        m["channel_name"] = channel
    if thread is not None:
        m["thread"] = thread
    m.update(meta)
    return {"content": "hi", "meta": m}


# --- subscription + identity (AC1) -------------------------------------------


def test_channel_subscribe_tokens_are_channel_then_mentions():
    assert channel_subscribe_tokens("epr-backend") == "channel:epr-backend,mentions"


def test_build_listener_spec_carries_persistent_identity_and_channel_subscription():
    spec = build_listener_spec(_config(), "epr-backend")
    assert spec.bot_name == deterministic_listener_name("epr-backend")
    assert spec.env["COMMY_BOT_NAME"] == spec.bot_name
    assert spec.env["COMMY_SUBSCRIBE"] == "channel:epr-backend,mentions"
    assert spec.env["ZULIP_SITE"] == "https://zulip.example"
    assert spec.env["ZULIP_MINTER_EMAIL"] == "minter@example.com"
    assert spec.env["ZULIP_MINTER_API_KEY"] == "key"
    assert spec.command == "bun"
    assert spec.args == ("packages/mcp/server.ts",)
    assert spec.cwd == "/opt/commy"


def test_listener_identity_is_stable_pure_and_brand_safe():
    name = deterministic_listener_name("epr-backend")
    assert name == deterministic_listener_name("epr-backend")  # pure across calls
    assert re.fullmatch(r"[a-z][a-z0-9_-]*", name)
    assert len(name) <= 40


def test_listener_identity_varies_by_channel():
    assert deterministic_listener_name("epr-backend") != deterministic_listener_name("epr-frontend")


def test_listener_identity_never_collides_with_a_per_topic_identity():
    # The boot listener and per-topic connections coexist; a name collision
    # would make the substrate minter hand them the SAME Zulip user_id.
    listener = deterministic_listener_name("epr-backend")
    assert listener != deterministic_bot_name("epr-backend", "standup")
    assert listener != deterministic_bot_name("epr-backend", "")


def test_listener_spec_passes_catchup_window_when_set():
    spec = build_listener_spec(_config(catchup_window_seconds=0), "epr-backend")
    assert spec.env["COMMY_CATCHUP_WINDOW_SECONDS"] == "0"


def test_listener_spec_omits_catchup_window_when_unset():
    spec = build_listener_spec(_config(), "epr-backend")
    assert "COMMY_CATCHUP_WINDOW_SECONDS" not in spec.env


# --- unowned -> trigger, owned -> no-trigger (AC2) ---------------------------


def test_unowned_topic_triggers_spawn():
    listener = _make_listener(owned=set())
    asyncio.run(listener.on_frame(_frame("epr-backend", "standup")))
    assert listener.triggered == [("epr-backend", "standup")]


def test_owned_topic_does_not_trigger():
    listener = _make_listener(owned={("epr-backend", "standup")})
    asyncio.run(listener.on_frame(_frame("epr-backend", "standup")))
    assert listener.triggered == []


def test_thread_less_frame_is_ignored():
    # Top-level channel posts carry no topic and key no per-topic session.
    listener = _make_listener()
    asyncio.run(listener.on_frame(_frame("epr-backend", None)))
    assert listener.triggered == []


def test_channel_less_frame_is_ignored():
    listener = _make_listener()
    asyncio.run(listener.on_frame(_frame(None, "standup")))
    assert listener.triggered == []


def test_cross_channel_mention_triggers_for_the_frames_own_topic():
    # `mentions` lets the boot listener hear @-mentions beyond its own channel;
    # it must spawn for the frame's (channel, topic), not its boot channel.
    listener = _make_listener(channel="epr-backend", owned=set())
    asyncio.run(listener.on_frame(_frame("epr-frontend", "incident")))
    assert listener.triggered == [("epr-frontend", "incident")]


def test_ownership_is_read_live_per_frame():
    # The spawn the first frame triggers makes the topic owned; the next frame
    # for that topic must see the updated ownership and not re-trigger.
    owned: set[tuple[str, str]] = set()
    listener = _make_listener(owned=owned)
    asyncio.run(listener.on_frame(_frame("c", "t")))
    owned.add(("c", "t"))
    asyncio.run(listener.on_frame(_frame("c", "t")))
    assert listener.triggered == [("c", "t")]


def test_start_and_stop_delegate_to_transport():
    listener = _make_listener()
    asyncio.run(listener.start())
    asyncio.run(listener.stop())
    assert listener.factory.created[0].started == 1
    assert listener.factory.created[0].stopped == 1


# --- adapter wiring: connect starts the listener, frames spawn per-topic -----


class _RecordingAdapter(CommyAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handled: list[MessageEvent] = []

    async def handle_message(self, event: MessageEvent) -> None:
        self.handled.append(event)


def _wired_adapter():
    adapter = _RecordingAdapter(PlatformConfig())
    manager_factory = _RecordingFactory()
    manager = TopicConnectionManager(_config(), manager_factory, adapter.receive_channel_notification)
    listener_factory = _RecordingFactory()
    listener = ChannelListener(
        build_listener_spec(_config(), "epr-backend"),
        listener_factory,
        adapter.ensure_topic_connection,
        manager.active_keys,
    )
    adapter._connection_manager = manager
    adapter._listener = listener
    adapter._reap_interval_seconds = 0.01
    return adapter, manager, manager_factory, listener, listener_factory


def test_connect_starts_the_listener_and_disconnect_stops_it():
    adapter, _, _, _, listener_factory = _wired_adapter()

    async def scenario():
        await adapter.connect()
        started = listener_factory.created[0].started
        await adapter.disconnect()
        return started, listener_factory.created[0].stopped

    started, stopped = asyncio.run(scenario())
    assert started == 1
    assert stopped == 1


def test_listener_frame_spawns_per_topic_connection_via_adapter():
    # Full integration with REAL adapter + REAL manager + REAL ownership: only
    # the leaf subprocess transport is faked. An unowned topic spawns once; a
    # repeat for the now-owned topic does not spawn again.
    adapter, manager, manager_factory, listener, _ = _wired_adapter()

    async def scenario():
        await adapter.connect()
        await listener.on_frame(_frame("epr-backend", "standup"))
        first = manager.active_keys()
        await listener.on_frame(_frame("epr-backend", "standup"))
        await adapter.disconnect()
        return first

    first = asyncio.run(scenario())
    assert first == {("epr-backend", "standup")}
    assert len(manager_factory.created) == 1


# --- real stub subprocess (no Zulip): boot -> frame -> trigger + dedup -------


async def _wait_for(predicate, timeout: float = 8.0, interval: float = 0.05) -> bool:
    waited = 0.0
    while waited < timeout:
        if predicate():
            return True
        await asyncio.sleep(interval)
        waited += interval
    return predicate()


def _stub_config(channel, topic, pidfile, *, idle=300.0, **extra) -> SpawnConfig:
    env = {"STUB_PIDFILE": pidfile, "STUB_CHANNEL": channel, "STUB_TOPIC": topic}
    env.update(extra)
    return SpawnConfig(
        repo_dir=str(Path(__file__).parent),
        zulip_site="https://zulip.example",
        minter_email="minter@example.com",
        minter_api_key="key",
        command=sys.executable,
        args=(_STUB,),
        channel=channel,
        idle_timeout_seconds=idle,
        extra_env=env,
    )


def test_listener_spec_drives_a_real_subprocess_with_the_channel_subscription():
    # Proves the listener SPEC's identity + channel subscription actually reach
    # a real commy server subprocess (the stub echoes the env it booted
    # with back into the frame), over the genuine notifications/message carrier.
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = os.path.join(tmp, "stub.pid")
        errlog_path = os.path.join(tmp, "stub.stderr.log")
        spec = build_listener_spec(_stub_config("epr-backend", "standup", pidfile), "epr-backend")
        received: list = []

        async def sink(frame) -> None:
            received.append(frame)

        async def scenario():
            handle = open(errlog_path, "a")  # noqa: SIM115 — closed at process end
            transport = McpTopicTransport(spec, sink, errlog=handle)
            await transport.start()
            assert await _wait_for(lambda: len(received) >= 1), "no frame from real subprocess"
            await transport.stop()

        asyncio.run(scenario())
        frame = received[0]
        assert frame["meta"]["echo_bot_name"] == deterministic_listener_name("epr-backend")
        assert frame["meta"]["echo_subscribe"] == "channel:epr-backend,mentions"


def test_listener_boots_real_subprocess_triggers_once_and_dedups_on_ownership():
    # The boot listener over a REAL subprocess: the stub re-emits the frame on a
    # short interval; ownership dedup must turn that into exactly one trigger.
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = os.path.join(tmp, "stub.pid")
        errlog_path = os.path.join(tmp, "stub.stderr.log")
        config = _stub_config("epr-backend", "standup", pidfile)
        owned: set[tuple[str, str]] = set()
        triggered: list[tuple[str, str]] = []

        async def trigger(channel, topic):
            triggered.append((channel, topic))
            owned.add((channel, topic))  # models ensure_topic_connection taking ownership

        async def scenario():
            handle = open(errlog_path, "a")  # noqa: SIM115 — closed at process end
            listener = make_listener(
                config, trigger=trigger, owned=lambda: owned, errlog=handle
            )
            await listener.start()
            assert await _wait_for(lambda: triggered), "boot listener never triggered a spawn"
            await asyncio.sleep(0.3)  # stub keeps re-emitting; dedup must hold
            await listener.stop()

        asyncio.run(scenario())
        assert triggered == [("epr-backend", "standup")]
