"""Boot-time channel listener for the commy Hermes adapter.

One persistent-identity connection, created at startup, subscribed
``channel:<name>`` + ``mentions``. Its sole job is the cold-start path: notice a
``(channel, topic)`` that no per-topic identity owns yet and trigger a spawn
(``CommyAdapter.ensure_topic_connection`` → ``TopicConnectionManager.ensure``).
Ongoing topics are owned by their per-topic
identity, which is subscribed to its own thread and refreshes its own idle
timer; the listener reads that ownership live off the manager and ignores an
already-owned topic, so it never re-triggers (dedup by ownership).

The listener's connection lifecycle is the process's — it is the only connection
created at startup and is never reaped, decoupled from the per-topic set the
manager spawns and reaps on demand.

This module is mcp-free (the transport is injected as a factory, exactly like
``connection.py``), so it imports without the ``mcp`` SDK present. The real MCP
transport wiring (``make_listener``) lives in ``transport.py``.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Set, Tuple

from .connection import (
    ConnectionSpec,
    Frame,
    SpawnConfig,
    TransportFactory,
)
from .naming import deterministic_listener_name
from .receive import frame_from_params

# The boot listener owns no single topic, so its ConnectionSpec.topic is empty.
_LISTENER_TOPIC = ""

TopicKey = Tuple[str, str]
# Triggers a per-topic spawn for (channel, topic); its return value is unused.
SpawnTrigger = Callable[[str, str], Awaitable[object]]
# Reads the manager's live ownership set — the (channel, topic) pairs already
# owned by a per-topic connection.
OwnedKeys = Callable[[], Set[TopicKey]]


def channel_subscribe_tokens(channel: str) -> str:
    """The ``COMMY_SUBSCRIBE`` value for the boot listener.

    ``channel:<name>`` delivers every topic's frames in the channel (so the
    listener sees the first message of any brand-new topic), and ``mentions``
    makes it hear @-mentions beyond its own channel so a mention into an unowned
    topic also cold-starts it. The substrate splits on ``,`` and trims each
    token (``bootstrap.ts`` ``subscribeFromEnv``).
    """
    return f"channel:{channel},mentions"


def build_listener_spec(config: SpawnConfig, channel: str) -> ConnectionSpec:
    """The fully-resolved recipe to spawn the boot listener for ``channel``.

    Two identity modes:

    * **Attach** (``config.bot_api_key`` set) — the boot listener binds the
      configured persona (``config.bot_name``) using the supplied stable key, no
      regenerate (the substrate attach path, ``bootstrap.ts`` ``attachIdentity``,
      fires on ``COMMY_BOT_NAME`` + ``COMMY_BOT_API_KEY``). Its realm-wide
      ``mentions`` then catches ``@<persona>`` so a mention in any channel wakes
      it. A key with no persona name is a misconfig and raises.
    * **Mint** (no key) — persistent mode driven by ``COMMY_BOT_NAME`` set to the
      stable ``deterministic_listener_name``, the same mechanism per-topic
      connections use, so a reboot re-acquires the same identity and replays the
      channel's recent window.

    Per-topic connections (``connection.build_spec``) always mint their own
    ``t-*`` identity regardless of the attach key — only the boot listener
    attaches the persona (the hybrid scope). Mirrors
    ``connection.build_spec`` but with a channel-level subscription.
    """
    env: dict[str, str] = {
        "ZULIP_SITE": config.zulip_site,
        "ZULIP_MINTER_EMAIL": config.minter_email,
        "ZULIP_MINTER_API_KEY": config.minter_api_key,
        "COMMY_SUBSCRIBE": channel_subscribe_tokens(channel),
    }
    if config.bot_api_key is not None:
        if config.bot_name is None:
            raise ValueError(
                "COMMY_BOT_API_KEY requires COMMY_BOT_NAME — the key identifies "
                "which provisioned persona the boot listener attaches to"
            )
        bot_name = config.bot_name
        env["COMMY_BOT_API_KEY"] = config.bot_api_key
    else:
        bot_name = deterministic_listener_name(channel)
    env["COMMY_BOT_NAME"] = bot_name
    if config.catchup_window_seconds is not None:
        env["COMMY_CATCHUP_WINDOW_SECONDS"] = str(config.catchup_window_seconds)
    env.update(config.extra_env)
    return ConnectionSpec(
        channel=channel,
        topic=_LISTENER_TOPIC,
        bot_name=bot_name,
        command=config.command,
        args=config.args,
        cwd=config.repo_dir,
        env=env,
    )


class ChannelListener:
    """The boot listener: routes channel frames to a cold-start spawn trigger.

    Owns its single channel-level transport (built from the injected factory,
    wired so every inbound frame reaches ``on_frame``). ``start``/``stop`` bring
    that connection up and tear it down; ``on_frame`` makes the per-frame
    decision. The trigger + ownership view are injected so this stays pure of any
    knowledge of the manager or the adapter.
    """

    def __init__(
        self,
        spec: ConnectionSpec,
        transport_factory: TransportFactory,
        trigger: SpawnTrigger,
        owned: OwnedKeys,
    ) -> None:
        self._spec = spec
        self._trigger = trigger
        self._owned = owned
        self._transport = transport_factory(spec, self.on_frame)

    @property
    def spec(self) -> ConnectionSpec:
        return self._spec

    async def start(self) -> None:
        await self._transport.start()

    async def stop(self) -> None:
        await self._transport.stop()

    async def on_frame(self, frame: Frame) -> None:
        """Trigger a spawn for an unowned ``(channel, topic)``; ignore the rest.

        A thread-less or channel-less frame keys no per-topic session, so it is
        ignored (matching the receive path's thread-less drop). An already-owned
        topic is the per-topic connection's business, never re-triggered.
        """
        parsed = frame_from_params(frame)
        channel = parsed.channel_name
        topic = parsed.thread
        if channel is None or topic is None:
            return
        if (channel, topic) in self._owned():
            return
        await self._trigger(channel, topic)
