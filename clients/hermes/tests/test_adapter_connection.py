"""Adapter ↔ connection-lifecycle wiring tests.

Drives the real ``CommyAdapter`` + real ``TopicConnectionManager`` with a
recording fake transport at the subprocess/MCP seam, and asserts the adapter's
own behaviour: ``connect`` starts the reaper, ``ensure_topic_connection`` spawns,
an inbound frame routes through the full receive path into ``handle_message``,
and ``disconnect`` tears everything down.

``SpawnConfig.from_env`` is tested directly so the env-built production path is
covered without spinning a subprocess.
"""

import asyncio

import pytest
from gateway.config import PlatformConfig
from gateway.platforms.base import MessageEvent
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

from commy import register
from commy.adapter import CommyAdapter
from commy.connection import SpawnConfig, TopicConnectionManager
from commy.naming import deterministic_bot_name


@pytest.fixture(autouse=True)
def _platform_registered():
    manager = PluginManager()
    manifest = PluginManifest(name="commy-platform", kind="platform")
    register(PluginContext(manifest, manager))


class _FakeTransport:
    def __init__(self, spec, sink) -> None:
        self.spec = spec
        self.sink = sink
        self.started = 0
        self.stopped = 0
        self.posts: list[tuple[str, str, str]] = []

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def emit(self, frame) -> None:
        await self.sink(frame)

    async def post(self, body: str, channel: str, topic: str) -> str:
        self.posts.append((body, channel, topic))
        return f"posted-{len(self.posts)}"


class _RecordingFactory:
    def __init__(self) -> None:
        self.created: list[_FakeTransport] = []

    def __call__(self, spec, sink) -> _FakeTransport:
        transport = _FakeTransport(spec, sink)
        self.created.append(transport)
        return transport


class _RecordingAdapter(CommyAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handled: list[MessageEvent] = []

    async def handle_message(self, event: MessageEvent) -> None:
        self.handled.append(event)


def _config() -> SpawnConfig:
    return SpawnConfig(
        repo_dir="/opt/commy",
        zulip_site="https://zulip.example",
        minter_email="minter@example.com",
        minter_api_key="key",
        idle_timeout_seconds=300.0,
    )


def _adapter_with_fake_manager():
    factory = _RecordingFactory()
    adapter = _RecordingAdapter(PlatformConfig())
    manager = TopicConnectionManager(_config(), factory, adapter.receive_channel_notification)
    adapter._connection_manager = manager
    adapter._reap_interval_seconds = 0.01
    return adapter, factory


def test_connect_starts_reaper_and_returns_true():
    adapter, _ = _adapter_with_fake_manager()

    async def scenario():
        ok = await adapter.connect()
        running = adapter._reaper_task is not None and not adapter._reaper_task.done()
        await adapter.disconnect()
        return ok, running

    ok, running = asyncio.run(scenario())
    assert ok is True
    assert running


def test_ensure_topic_connection_spawns_via_manager():
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        spec = await adapter.ensure_topic_connection("myproject", "standup")
        await adapter.disconnect()
        return spec

    spec = asyncio.run(scenario())
    assert len(factory.created) == 1
    assert factory.created[0].started == 1
    assert spec.bot_name == deterministic_bot_name("myproject", "standup")
    assert spec.env["COMMY_SUBSCRIBE"] == "myproject/standup"


def test_inbound_frame_routes_through_receive_path_to_handle_message():
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        await adapter.ensure_topic_connection("myproject", "standup")
        frame = {
            "content": "ship it",
            "meta": {
                "channel_name": "myproject",
                "thread": "standup",
                "message_id": "m1",
                "sender_id": "u-alice",
                "sender_name": "Alice",
            },
        }
        await factory.created[0].emit(frame)
        await adapter.disconnect()

    asyncio.run(scenario())
    assert len(adapter.handled) == 1
    event = adapter.handled[0]
    assert event.text == "ship it"
    assert event.source.chat_id == "myproject"
    assert event.source.thread_id == "standup"


def test_ensure_before_connect_raises():
    adapter = _RecordingAdapter(PlatformConfig())

    async def scenario():
        await adapter.ensure_topic_connection("c", "t")

    with pytest.raises(RuntimeError):
        asyncio.run(scenario())


def test_disconnect_shuts_down_connections():
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        await adapter.ensure_topic_connection("c", "t")
        await adapter.disconnect()

    asyncio.run(scenario())
    assert factory.created[0].stopped == 1
    assert adapter._connection_manager.active_keys() == set()
    assert adapter._reaper_task is None


# --- Framework send delivers the prose reply ---------------------------------
#
# The Hermes gateway funnels each turn's composed prose through ``adapter.send``
# (stream_consumer per-turn delivery), guarded by ``if not text.strip()`` so it
# only fires when the model actually wrote prose. ``send`` delivers that prose
# into the inbound frame's channel + topic via the live per-topic connection's
# ``post`` tool.
#
# The explicit ``post`` MCP tool stays the agent's path for cross-topic replies;
# the two are complementary. A pure ``post``-tool
# turn emits no prose, so ``send`` is never called for it — no double-reply.


def test_send_delivers_prose_reply_to_the_inbound_channel_and_topic():
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        await adapter.ensure_topic_connection("myproject", "standup")
        result = await adapter.send(
            "myproject",
            "here is my reply",
            metadata={"thread_id": "standup"},
        )
        await adapter.disconnect()
        return result

    result = asyncio.run(scenario())
    assert factory.created[0].posts == [("here is my reply", "myproject", "standup")]
    assert result.success is True
    # No message_id even though delivery happened: a returned id would make the
    # stream consumer treat commy as editable and re-send per tool boundary
    # (the "155 comments under one PR" trap). Withholding it keeps the reply on
    # the consumer's single-delivery, no-editable-id path.
    assert result.message_id is None


def test_send_without_a_topic_is_a_noop_so_the_onboarding_notice_does_not_post():
    # The gateway's "no home channel" onboarding notice (run.py) and any other
    # topic-less send carry no ``thread_id``; with no topic there is no inbound
    # frame to reply into, so ``send`` stays the deliberate no-op it was.
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        await adapter.ensure_topic_connection("myproject", "standup")
        result = await adapter.send("myproject", "📬 onboarding notice")
        await adapter.disconnect()
        return result

    result = asyncio.run(scenario())
    assert factory.created[0].posts == []
    assert result.success is True
    assert result.message_id is None


def test_send_with_blank_content_does_not_post():
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        await adapter.ensure_topic_connection("myproject", "standup")
        result = await adapter.send(
            "myproject", "   ", metadata={"thread_id": "standup"}
        )
        await adapter.disconnect()
        return result

    result = asyncio.run(scenario())
    assert factory.created[0].posts == []
    assert result.success is True


def test_send_to_a_topic_without_a_live_connection_is_a_graceful_noop():
    # A reap between the inbound turn and delivery (or any unknown topic) must
    # not crash the turn — there is simply no live connection to ride.
    adapter, factory = _adapter_with_fake_manager()

    async def scenario():
        await adapter.connect()
        result = await adapter.send(
            "myproject", "orphan reply", metadata={"thread_id": "no-such-topic"}
        )
        await adapter.disconnect()
        return result

    result = asyncio.run(scenario())
    assert result.success is True
    assert result.message_id is None


def test_get_chat_info_returns_a_benign_payload_without_raising():
    adapter = _RecordingAdapter(PlatformConfig())

    async def scenario():
        return await adapter.get_chat_info("myproject")

    info = asyncio.run(scenario())
    assert isinstance(info, dict)


# --- SpawnConfig.from_env ----------------------------------------------------


def test_spawn_config_from_env_reads_required_and_optional():
    env = {
        "COMMY_SERVER_DIR": "/opt/commy",
        "COMMY_PROJECT": "myproject",
        "ZULIP_SITE": "https://zulip.example",
        "ZULIP_MINTER_EMAIL": "minter@example.com",
        "ZULIP_MINTER_API_KEY": "secret",
        "COMMY_IDLE_TIMEOUT_SECONDS": "120",
        "COMMY_REAP_INTERVAL_SECONDS": "30",
        "COMMY_CATCHUP_WINDOW_SECONDS": "0",
    }
    config = SpawnConfig.from_env(env)
    assert config.repo_dir == "/opt/commy"
    assert config.channel == "myproject"
    assert config.zulip_site == "https://zulip.example"
    assert config.minter_email == "minter@example.com"
    assert config.minter_api_key == "secret"
    assert config.idle_timeout_seconds == 120.0
    assert config.reap_interval_seconds == 30.0
    assert config.catchup_window_seconds == 0


def test_spawn_config_from_env_defaults_optional():
    env = {
        "COMMY_SERVER_DIR": "/opt/commy",
        "COMMY_PROJECT": "myproject",
        "ZULIP_SITE": "https://zulip.example",
        "ZULIP_MINTER_EMAIL": "minter@example.com",
        "ZULIP_MINTER_API_KEY": "secret",
    }
    config = SpawnConfig.from_env(env)
    assert config.idle_timeout_seconds == 300.0
    assert config.reap_interval_seconds == 60.0
    assert config.catchup_window_seconds is None


def test_spawn_config_from_env_reads_attach_identity():
    # COMMY_BOT_NAME (persona) + COMMY_BOT_API_KEY (stable key) are the attach
    # inputs the boot listener uses to bind a provisioned persona.
    env = {
        "COMMY_SERVER_DIR": "/opt/commy",
        "COMMY_PROJECT": "myproject",
        "ZULIP_SITE": "https://zulip.example",
        "ZULIP_MINTER_EMAIL": "minter@example.com",
        "ZULIP_MINTER_API_KEY": "secret",
        "COMMY_BOT_NAME": "hermes",
        "COMMY_BOT_API_KEY": "persona-key",
    }
    config = SpawnConfig.from_env(env)
    assert config.bot_name == "hermes"
    assert config.bot_api_key == "persona-key"


def test_spawn_config_from_env_attach_identity_absent_by_default():
    env = {
        "COMMY_SERVER_DIR": "/opt/commy",
        "COMMY_PROJECT": "myproject",
        "ZULIP_SITE": "https://zulip.example",
        "ZULIP_MINTER_EMAIL": "minter@example.com",
        "ZULIP_MINTER_API_KEY": "secret",
    }
    config = SpawnConfig.from_env(env)
    assert config.bot_name is None
    assert config.bot_api_key is None


def test_spawn_config_from_env_missing_required_raises():
    with pytest.raises(ValueError):
        SpawnConfig.from_env({"ZULIP_SITE": "https://zulip.example"})


def test_spawn_config_from_env_reads_canonical_commy():
    env = {
        "COMMY_SERVER_DIR": "/opt/commy",
        "COMMY_PROJECT": "myproject",
        "ZULIP_SITE": "https://zulip.example",
        "ZULIP_MINTER_EMAIL": "minter@example.com",
        "ZULIP_MINTER_API_KEY": "secret",
        "COMMY_IDLE_TIMEOUT_SECONDS": "120",
        "COMMY_REAP_INTERVAL_SECONDS": "30",
        "COMMY_CATCHUP_WINDOW_SECONDS": "0",
    }
    config = SpawnConfig.from_env(env)
    assert config.repo_dir == "/opt/commy"
    assert config.channel == "myproject"
    assert config.idle_timeout_seconds == 120.0
    assert config.reap_interval_seconds == 30.0
    assert config.catchup_window_seconds == 0
