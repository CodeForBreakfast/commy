"""Lifecycle tests for the per-topic connection manager.

These drive the REAL manager logic — spawn-on-demand, idle reap, respawn — with
a recording fake transport and a fake clock standing in for the subprocess/MCP
I/O seam. Nothing here asserts a double's canned return value: every assertion is
on the manager's own decisions (did it spawn with the right spec? did it tear
down only the idle connection? did a respawn reuse the same identity name?).

The real subprocess + MCP wiring is exercised separately in
``test_transport.py`` against a real stub MCP server (no Zulip needed).
"""

import asyncio

from commy.connection import (
    SpawnConfig,
    TopicConnectionManager,
    build_spec,
    subscribe_tokens,
)
from commy.naming import deterministic_bot_name


class _FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


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


def _config(**overrides) -> SpawnConfig:
    base = dict(
        repo_dir="/opt/commy",
        zulip_site="https://zulip.example",
        minter_email="minter-bot@example.com",
        minter_api_key="secret-key",
        idle_timeout_seconds=300.0,
    )
    base.update(overrides)
    return SpawnConfig(**base)


def _make_manager(factory, *, clock, sink=None, config=None):
    received: list = []

    async def default_sink(frame) -> None:
        received.append(frame)

    manager = TopicConnectionManager(
        config or _config(),
        factory,
        sink or default_sink,
        clock=clock,
    )
    manager.received = received  # type: ignore[attr-defined]
    return manager


# --- spec construction -------------------------------------------------------


def test_build_spec_carries_persistent_mode_identity_and_subscriptions():
    spec = build_spec(_config(), "epr-backend", "standup")
    assert spec.bot_name == deterministic_bot_name("epr-backend", "standup")
    assert spec.env["COMMY_BOT_NAME"] == spec.bot_name
    assert spec.env["COMMY_SUBSCRIBE"] == "thread:epr-backend/standup,mentions"
    assert spec.env["ZULIP_SITE"] == "https://zulip.example"
    assert spec.env["ZULIP_MINTER_EMAIL"] == "minter-bot@example.com"
    assert spec.env["ZULIP_MINTER_API_KEY"] == "secret-key"
    assert spec.command == "bun"
    assert spec.args == ("packages/mcp/server.ts",)
    assert spec.cwd == "/opt/commy"


def test_build_spec_ignores_attach_identity_keeping_per_topic_minted():
    # Hybrid scope: only the boot listener attaches the persona.
    # Per-topic connections keep their own deterministic `t-*` identity even
    # when an attach key is configured, preserving per-thread recent-window
    # catch-up. The attach key must never leak into a per-topic spec's env.
    spec = build_spec(
        _config(bot_name="hermes", bot_api_key="persona-key"),
        "epr-backend",
        "standup",
    )
    assert spec.bot_name == deterministic_bot_name("epr-backend", "standup")
    assert spec.env["COMMY_BOT_NAME"] == deterministic_bot_name("epr-backend", "standup")
    assert "COMMY_BOT_API_KEY" not in spec.env


def test_subscribe_tokens_are_thread_then_mentions():
    assert subscribe_tokens("c", "t") == "thread:c/t,mentions"


def test_catchup_window_is_passed_through_when_set():
    spec = build_spec(_config(catchup_window_seconds=0), "c", "t")
    assert spec.env["COMMY_CATCHUP_WINDOW_SECONDS"] == "0"


def test_catchup_window_absent_when_unset():
    spec = build_spec(_config(), "c", "t")
    assert "COMMY_CATCHUP_WINDOW_SECONDS" not in spec.env


# --- spawn-on-demand ---------------------------------------------------------


def test_ensure_spawns_one_connection_with_built_spec():
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock)

    spec = asyncio.run(manager.ensure("epr-backend", "standup"))

    assert len(factory.created) == 1
    assert factory.created[0].started == 1
    assert factory.created[0].spec is spec
    assert spec.env["COMMY_SUBSCRIBE"] == "thread:epr-backend/standup,mentions"
    assert manager.active_keys() == {("epr-backend", "standup")}


def test_ensure_is_idempotent_for_a_live_connection():
    factory = _RecordingFactory()
    manager = _make_manager(factory, clock=_FakeClock())

    asyncio.run(manager.ensure("c", "t"))
    asyncio.run(manager.ensure("c", "t"))

    assert len(factory.created) == 1
    assert factory.created[0].started == 1


def test_distinct_topics_get_distinct_connections():
    factory = _RecordingFactory()
    manager = _make_manager(factory, clock=_FakeClock())

    asyncio.run(manager.ensure("c", "one"))
    asyncio.run(manager.ensure("c", "two"))

    assert len(factory.created) == 2
    assert manager.active_keys() == {("c", "one"), ("c", "two")}


# --- inbound frames ----------------------------------------------------------


def test_inbound_frame_reaches_the_sink():
    factory = _RecordingFactory()
    manager = _make_manager(factory, clock=_FakeClock())

    async def scenario():
        await manager.ensure("c", "t")
        frame = {"content": "hi", "meta": {"channel_name": "c", "thread": "t"}}
        await factory.created[0].emit(frame)
        return frame

    frame = asyncio.run(scenario())
    assert manager.received == [frame]


# --- idle reap ---------------------------------------------------------------


def test_idle_connection_is_reaped():
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock, config=_config(idle_timeout_seconds=300))

    async def scenario():
        await manager.ensure("c", "t")
        clock.advance(301)
        return await manager.reap_idle()

    reaped = asyncio.run(scenario())
    assert reaped == [("c", "t")]
    assert factory.created[0].stopped == 1
    assert manager.active_keys() == set()


def test_active_connection_is_not_reaped():
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock, config=_config(idle_timeout_seconds=300))

    async def scenario():
        await manager.ensure("c", "t")
        clock.advance(120)
        return await manager.reap_idle()

    reaped = asyncio.run(scenario())
    assert reaped == []
    assert factory.created[0].stopped == 0
    assert manager.active_keys() == {("c", "t")}


def test_inbound_frame_resets_the_idle_timer():
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock, config=_config(idle_timeout_seconds=300))

    async def scenario():
        await manager.ensure("c", "t")
        clock.advance(250)
        # A live frame just under the timeout refreshes activity.
        await factory.created[0].emit({"content": "ping", "meta": {}})
        clock.advance(250)  # 500 since spawn, but only 250 since the frame
        return await manager.reap_idle()

    reaped = asyncio.run(scenario())
    assert reaped == []
    assert manager.active_keys() == {("c", "t")}


def test_reap_leaves_fresh_connections_alone():
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock, config=_config(idle_timeout_seconds=300))

    async def scenario():
        await manager.ensure("c", "stale")
        clock.advance(299)
        await manager.ensure("c", "fresh")  # fresh at clock=1299
        clock.advance(2)  # stale aged 301, fresh aged 2
        return await manager.reap_idle()

    reaped = asyncio.run(scenario())
    assert reaped == [("c", "stale")]
    assert manager.active_keys() == {("c", "fresh")}


# --- respawn reuses identity (AC2 at the layer we own) -----------------------


def test_respawn_after_reap_reuses_the_same_bot_name():
    # The substrate minter is idempotent by name -> a respawn under the SAME
    # COMMY_BOT_NAME re-acquires the SAME Zulip user_id and replays the
    # thread's recent window (persistent-mode catch-up). What this layer owns
    # and asserts is that a respawn computes the identical name; the user_id
    # reuse + window replay are the substrate's guarantee, not exercised here.
    factory = _RecordingFactory()
    clock = _FakeClock()
    manager = _make_manager(factory, clock=clock, config=_config(idle_timeout_seconds=300))

    async def scenario():
        spec1 = await manager.ensure("epr-backend", "standup")
        clock.advance(301)
        await manager.reap_idle()
        spec2 = await manager.ensure("epr-backend", "standup")
        return spec1, spec2

    spec1, spec2 = asyncio.run(scenario())
    assert spec1.bot_name == spec2.bot_name
    assert len(factory.created) == 2
    assert factory.created[0].started == 1
    assert factory.created[0].stopped == 1
    assert factory.created[1].started == 1


# --- shutdown ----------------------------------------------------------------


def test_shutdown_stops_every_connection():
    factory = _RecordingFactory()
    manager = _make_manager(factory, clock=_FakeClock())

    async def scenario():
        await manager.ensure("c", "one")
        await manager.ensure("c", "two")
        await manager.shutdown()

    asyncio.run(scenario())
    assert all(t.stopped == 1 for t in factory.created)
    assert manager.active_keys() == set()


# --- background reaper loop (real asyncio, no mocks) -------------------------


def test_run_reaper_loop_reaps_idle_connections():
    factory = _RecordingFactory()
    # idle_timeout 0 -> any elapsed monotonic time makes a connection eligible,
    # so the loop reaps on its first tick without timing flakiness.
    manager = _make_manager(
        factory, clock=__import__("time").monotonic, config=_config(idle_timeout_seconds=0)
    )

    async def scenario():
        await manager.ensure("c", "t")
        task = asyncio.create_task(manager.run_reaper(0.01))
        for _ in range(50):
            await asyncio.sleep(0.01)
            if not manager.active_keys():
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert factory.created[0].stopped == 1
    assert manager.active_keys() == set()
