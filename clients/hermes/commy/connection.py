"""Per-topic connection lifecycle for the commy Hermes adapter.

On-demand connections decoupled from process lifecycle. Given a
``(channel, topic)``, the manager brings up a fresh commy server
subprocess in **persistent mode** — ``COMMY_BOT_NAME`` set to a
deterministic per-topic name, subscribed to the bare ``<channel>/<topic>``
path (mentions need no token) — holds the MCP connection that streams inbound frames into the
adapter, reaps the subprocess after an idle window, and respawns on the next
frame reusing the **same identity name** (so the substrate minter, idempotent by
name, returns the same Zulip user_id and the persistent-mode catch-up replays
the thread's recent window — the connection self-catches-up its trigger).

Persistent mode (not ephemeral) is required: it gives eager bind, stable
identity across teardown/respawn, and channel/thread catch-up on (re)acquire.

This module is the lifecycle (pure of any MCP dependency, so it imports without
``mcp`` present). The subprocess + MCP transport that implements
``TopicTransport`` lives in ``transport.py`` and is injected as a factory.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Mapping, Optional, Protocol

from .naming import deterministic_bot_name

# A `{content, meta}` inbound frame — the same shape the receive path
# (`CommyAdapter.receive_channel_notification`) consumes. It rides under
# `params.data` of a `notifications/message` carrier on the wire.
Frame = Mapping[str, object]
FrameSink = Callable[[Frame], Awaitable[None]]
Clock = Callable[[], float]


@dataclass(frozen=True)
class SpawnConfig:
    """Static inputs shared by every per-topic spawn, plus the pod's channel.

    The minter creds + realm are the same the post-only pod already uses;
    ``command``/``args``/``repo_dir`` locate the commy
    server entrypoint (``bun packages/mcp/server.ts`` from the checkout root).
    ``channel`` is the project channel the boot listener subscribes
    to; per-topic spawns receive their channel per-call, so it carries an empty
    default for the connection-lifecycle paths that don't use it.
    """

    repo_dir: str
    zulip_site: str
    minter_email: str
    minter_api_key: str
    channel: str = ""
    command: str = "bun"
    args: tuple[str, ...] = ("packages/mcp/server.ts",)
    idle_timeout_seconds: float = 300.0
    reap_interval_seconds: float = 60.0
    catchup_window_seconds: Optional[int] = None
    bot_name: Optional[str] = None
    bot_api_key: Optional[str] = None
    extra_env: Mapping[str, str] = field(default_factory=dict)

    @staticmethod
    def from_env(env: Optional[Mapping[str, str]] = None) -> "SpawnConfig":
        """Build from the pod's environment.

        ``COMMY_SERVER_DIR`` is the commy checkout root the server runs from
        (``bun packages/mcp/server.ts``); ``COMMY_PROJECT`` is the channel the
        boot listener subscribes to; the minter creds + realm are the same the
        post-only pod already provisions. ``COMMY_BOT_NAME`` +
        ``COMMY_BOT_API_KEY`` are the optional attach inputs the boot listener
        uses to bind a provisioned persona (``build_listener_spec``); when unset
        the boot listener mints its own deterministic identity instead.
        """
        source = os.environ if env is None else env

        def require(key: str) -> str:
            value = source.get(key)
            if not value:
                raise ValueError(f"{key} is required to spawn per-topic connections")
            return value

        catchup = source.get("COMMY_CATCHUP_WINDOW_SECONDS")
        idle = source.get("COMMY_IDLE_TIMEOUT_SECONDS")
        reap = source.get("COMMY_REAP_INTERVAL_SECONDS")
        return SpawnConfig(
            repo_dir=require("COMMY_SERVER_DIR"),
            zulip_site=require("ZULIP_SITE"),
            minter_email=require("ZULIP_MINTER_EMAIL"),
            minter_api_key=require("ZULIP_MINTER_API_KEY"),
            channel=require("COMMY_PROJECT"),
            idle_timeout_seconds=float(idle) if idle is not None else 300.0,
            reap_interval_seconds=float(reap) if reap is not None else 60.0,
            catchup_window_seconds=int(catchup) if catchup is not None else None,
            bot_name=source.get("COMMY_BOT_NAME") or None,
            bot_api_key=source.get("COMMY_BOT_API_KEY") or None,
        )


@dataclass(frozen=True)
class ConnectionSpec:
    """The fully-resolved recipe to spawn one per-topic connection."""

    channel: str
    topic: str
    bot_name: str
    command: str
    args: tuple[str, ...]
    cwd: str
    env: Mapping[str, str]


def subscribe_tokens(channel: str, topic: str) -> str:
    """The ``COMMY_SUBSCRIBE`` value for a per-topic identity.

    Comma-separated tokens (the substrate splits on ``,`` and trims each;
    ``bootstrap.ts`` ``subscribeFromEnv``). A bare ``<channel>/<topic>`` path
    makes the bound identity receive the topic's frames (and triggers
    persistent-mode channels catch-up for that thread). @-mentions need no
    token — they reach the identity unconditionally, so cross-thread
    coordination arrives without one. The substrate splits the path on the
    first ``/`` only, so a topic containing ``/`` survives; a topic containing
    a literal ``,`` is not expressible through this env var (a substrate-side
    limitation, not a concern for slug-shaped topic names).
    """
    return f"{channel}/{topic}"


def build_spec(config: SpawnConfig, channel: str, topic: str) -> ConnectionSpec:
    bot_name = deterministic_bot_name(channel, topic)
    env: dict[str, str] = {
        "ZULIP_SITE": config.zulip_site,
        "ZULIP_MINTER_EMAIL": config.minter_email,
        "ZULIP_MINTER_API_KEY": config.minter_api_key,
        "COMMY_BOT_NAME": bot_name,
        "COMMY_SUBSCRIBE": subscribe_tokens(channel, topic),
    }
    if config.catchup_window_seconds is not None:
        env["COMMY_CATCHUP_WINDOW_SECONDS"] = str(config.catchup_window_seconds)
    env.update(config.extra_env)
    return ConnectionSpec(
        channel=channel,
        topic=topic,
        bot_name=bot_name,
        command=config.command,
        args=config.args,
        cwd=config.repo_dir,
        env=env,
    )


class TopicTransport(Protocol):
    """One per-topic connection's I/O: spawn+connect on ``start``, tear down on
    ``stop``, deliver an outbound reply on ``post``."""

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def post(self, body: str, channel: str, topic: str) -> Optional[str]: ...


# Builds a transport for a spec, wiring its inbound frames to the given sink.
TransportFactory = Callable[[ConnectionSpec, FrameSink], TopicTransport]


@dataclass
class _Connection:
    spec: ConnectionSpec
    transport: TopicTransport
    last_activity: float


class TopicConnectionManager:
    """Owns the set of live per-topic connections.

    ``ensure`` is the spawn entry point the boot-time listener
    calls when it sees an unowned ``(channel, topic)``; it is idempotent so a
    redundant trigger on an already-owned topic is a no-op (and refreshes the
    idle timer). ``reap_idle`` tears down connections silent past the idle
    window; ``run_reaper`` drives it on an interval.
    """

    def __init__(
        self,
        config: SpawnConfig,
        transport_factory: TransportFactory,
        sink: FrameSink,
        *,
        clock: Clock = time.monotonic,
    ) -> None:
        self._config = config
        self._factory = transport_factory
        self._sink = sink
        self._clock = clock
        self._connections: dict[tuple[str, str], _Connection] = {}
        self._lock = asyncio.Lock()

    async def ensure(self, channel: str, topic: str) -> ConnectionSpec:
        """Bring up the connection for ``(channel, topic)`` if it isn't already live."""
        key = (channel, topic)
        async with self._lock:
            existing = self._connections.get(key)
            if existing is not None:
                existing.last_activity = self._clock()
                return existing.spec
            spec = build_spec(self._config, channel, topic)
            transport = self._factory(spec, self._activity_sink(key))
            await transport.start()
            self._connections[key] = _Connection(spec, transport, self._clock())
            return spec

    async def deliver(self, channel: str, topic: str, body: str) -> Optional[str]:
        """Deliver an outbound reply into ``(channel, topic)`` via its live connection.

        Rides the per-topic connection the inbound turn already brought up, so the
        reply posts under the same per-topic identity and lands in the originating
        topic. Returns the posted message id, or ``None`` when no live connection
        owns the pair (e.g. it was idle-reaped between turn and delivery) — there
        is nothing to ride, so the caller treats it as a no-op rather than a crash.
        """
        async with self._lock:
            connection = self._connections.get((channel, topic))
            if connection is None:
                return None
            connection.last_activity = self._clock()
            transport = connection.transport
        return await transport.post(body, channel, topic)

    def _activity_sink(self, key: tuple[str, str]) -> FrameSink:
        async def sink(frame: Frame) -> None:
            connection = self._connections.get(key)
            if connection is not None:
                connection.last_activity = self._clock()
            await self._sink(frame)

        return sink

    async def reap_idle(self) -> list[tuple[str, str]]:
        """Tear down every connection idle for at least ``idle_timeout_seconds``."""
        now = self._clock()
        timeout = self._config.idle_timeout_seconds
        async with self._lock:
            stale = [
                key
                for key, conn in self._connections.items()
                if now - conn.last_activity >= timeout
            ]
            for key in stale:
                connection = self._connections.pop(key)
                await connection.transport.stop()
        return stale

    async def run_reaper(self, interval_seconds: float) -> None:
        """Reap idle connections every ``interval_seconds`` until cancelled."""
        while True:
            await asyncio.sleep(interval_seconds)
            await self.reap_idle()

    async def shutdown(self) -> None:
        """Tear down every connection (gateway stop)."""
        async with self._lock:
            connections = list(self._connections.values())
            self._connections.clear()
        for connection in connections:
            await connection.transport.stop()

    def active_keys(self) -> set[tuple[str, str]]:
        return set(self._connections)
