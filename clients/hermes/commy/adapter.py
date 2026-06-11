"""commy platform adapter for Hermes Agent.

Pattern B inbound-axis consumer: presents commy as a Hermes gateway
platform so non-Claude-Code hosts can receive commy traffic.

This plugin is **inbound only, by design** — delivering incoming messages into
the agent's turn is the one axis MCP cannot push, so the host owns it; posting
and every other outbound action are commy MCP tools, never this adapter's job
(``send`` is a deliberate no-op, see below). A reply-capable Hermes bot wires
*both* this inbound plugin *and* a commy ``post`` MCP server in its
``mcp_servers`` config — see the "Reply path (outbound)" section of this
package's ``README.md``.

The receive path (inbound frame -> routing/dedup -> Hermes session) landed in
comms-a7j.2 via ``receive_channel_notification`` (self-echo is dropped at the
substrate emitter, comms-dtcm, so it is not a consumer concern). The connection
lifecycle that *delivers* a frame to that handler — per-topic commy server
subprocesses in persistent mode, holding the MCP connection, idle-reaped and
respawned with stable identity — landed in comms-a7j.5 and lives in
``connection.py`` (lifecycle) + ``transport.py`` (the real MCP transport). This
adapter owns a ``TopicConnectionManager`` and exposes ``ensure_topic_connection``
as the spawn entry point; the boot-time listener that *detects* an unowned
``(channel, topic)`` and calls it is comms-a7j.4 (not built here).

``BasePlatformAdapter`` and ``Platform`` come from the host Hermes runtime,
which is present in the pod (``~/.hermes/plugins/`` install) and provided to
the test environment via a ``--no-deps`` install of ``hermes-agent``.
"""

import asyncio
import contextlib
from typing import Any, Dict, Mapping, Optional, Set

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.session import SessionSource

from .connection import ConnectionSpec, SpawnConfig, TopicConnectionManager
from .listener import ChannelListener
from .receive import ChannelFrame, frame_from_params

PLATFORM_NAME = "commy"
PLATFORM_LABEL = "Commy"


class CommyAdapter(BasePlatformAdapter):
    """commy platform adapter.

    Subclasses ``BasePlatformAdapter`` so the platform registers and the
    ``Platform`` enum self-extends. The receive path is live
    (``receive_channel_notification``) and the connection lifecycle is wired:
    ``connect`` starts the boot listener + idle reaper, ``ensure_topic_connection``
    is the spawn entry point, and ``disconnect`` tears every connection down. The
    gateway's framework-send (``send`` / ``get_chat_info``) is deliberately
    suppressed — commy is inbound-only and the reply is the agent's own MCP
    ``post`` tool inside its turn (comms-uuy, comms-dgy8). ``check_requirements``
    activates the platform for live use once the pod's commy config is
    present (the environment ``SpawnConfig.from_env`` requires).

    A ``connection_manager`` and/or ``listener`` may be injected (tests);
    otherwise ``connect`` builds the real ones from the environment via
    ``SpawnConfig.from_env``.
    """

    def __init__(
        self,
        config,
        *,
        connection_manager: Optional[TopicConnectionManager] = None,
        listener: Optional[ChannelListener] = None,
        reap_interval_seconds: Optional[float] = None,
        **kwargs,
    ):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        self._seen_message_ids: Set[str] = set()
        self._connection_manager = connection_manager
        self._listener = listener
        self._reap_interval_seconds = reap_interval_seconds
        self._reaper_task: Optional[asyncio.Task[None]] = None

    @property
    def name(self) -> str:
        return PLATFORM_LABEL

    async def receive_channel_notification(self, params: Mapping[str, Any]) -> None:
        """Route one ``claude/channel`` inbound frame into a Hermes session.

        The contract's consumer receive-path checklist, in order: parse the
        ``{content, meta}`` frame, drop thread-less top-level posts, dedup by
        ``message_id``, then hand the routed ``MessageEvent`` to Hermes's
        existing session/turn pipeline via ``handle_message``. A thread-less or
        duplicate frame returns without reaching the pipeline.

        Self-echo needs no filter here: the substrate emitter drops the bot's
        own posts before they reach this carrier (comms-dtcm), so every frame
        that arrives was authored by someone else.

        Thread-less frames are dropped (comms-a7j.3): a top-level post carries no
        topic, but Hermes keys sessions on ``(channel, topic)``. By the substrate
        convention "top-level = terse pings only; substantive work goes in a
        topic", such a frame is not agent-actionable, so it is ignored rather
        than routed into a ``(channel, None)`` session that would collapse every
        unrelated top-level ping in the channel into one incoherent session.
        """
        frame = frame_from_params(params)
        if frame.thread is None:
            return
        message_id = frame.message_id
        if message_id is not None:
            if message_id in self._seen_message_ids:
                return
            self._seen_message_ids.add(message_id)
        await self.handle_message(self._to_message_event(frame))

    def _to_message_event(self, frame: ChannelFrame) -> MessageEvent:
        """Build the Hermes ``MessageEvent`` a frame routes to.

        ``(channel_name, thread)`` becomes the session-routing pair Hermes keys
        on: the channel is the parent ``chat_id`` and the topic is the
        ``thread_id`` (so all participants in a topic share one session, as for
        a Discord/Slack thread). The full ``meta`` map rides along as
        ``raw_message`` so downstream provenance is preserved. The caller's
        thread-less drop (``receive_channel_notification``) guarantees ``thread``
        is present here, so every routed frame keys a real topic session.
        """
        thread = frame.thread
        source = SessionSource(
            platform=Platform(PLATFORM_NAME),
            chat_id=frame.channel_name or "",
            chat_name=frame.channel_name,
            chat_type="thread",
            thread_id=thread,
            user_id=frame.sender_id,
            user_name=frame.sender_name,
            message_id=frame.message_id,
        )
        return MessageEvent(
            text=frame.content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=frame.message_id,
            raw_message=dict(frame.meta),
        )

    async def connect(self) -> bool:
        """Bring the connection lifecycle up: boot listener, manager, reaper.

        With nothing injected, the manager and the boot listener are built from
        the environment (``SpawnConfig.from_env``) wired to the real MCP
        transport, sinking inbound frames into ``receive_channel_notification``.
        The boot listener is the ONE connection started here — subscribed
        ``channel:<name>`` + ``mentions`` under a persistent identity — and it
        cold-starts per-topic connections by calling ``ensure_topic_connection``
        for any ``(channel, topic)`` the manager doesn't already own. No per-topic
        connection exists until then; connect only readies manager + reaper and
        brings the listener up.
        """
        if self._connection_manager is None:
            from .transport import make_listener, make_manager

            config = SpawnConfig.from_env()
            self._connection_manager = make_manager(config, self.receive_channel_notification)
            self._listener = make_listener(
                config,
                trigger=self.ensure_topic_connection,
                owned=self._connection_manager.active_keys,
            )
            interval = config.reap_interval_seconds
        else:
            interval = self._reap_interval_seconds or 60.0
        if self._listener is not None:
            await self._listener.start()
        self._reaper_task = asyncio.create_task(self._connection_manager.run_reaper(interval))
        return True

    async def ensure_topic_connection(self, channel: str, topic: str) -> ConnectionSpec:
        """Spawn (or no-op if already live) the per-topic connection for ``(channel, topic)``.

        The clean spawn entry point the listener (comms-a7j.4) drives. Brings up
        a persistent-mode commy server subprocess under a deterministic
        per-topic identity, subscribed ``thread:<channel>/<topic>`` + ``mentions``.
        """
        if self._connection_manager is None:
            raise RuntimeError("connect() must run before spawning per-topic connections")
        return await self._connection_manager.ensure(channel, topic)

    async def disconnect(self) -> None:
        if self._listener is not None:
            await self._listener.stop()
            self._listener = None
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reaper_task
            self._reaper_task = None
        if self._connection_manager is not None:
            await self._connection_manager.shutdown()

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Suppress gateway framework-send: outbound rides the agent's MCP tool.

        The Hermes gateway funnels every outbound through ``adapter.send`` — the
        "no home channel" onboarding notice (``run.py``), the per-turn streaming
        delivery (``stream_consumer.py``), and cron delivery (``delivery.py``) —
        and there is no native inbound-only capability flag to opt out of it. For
        commy the reply is the agent's *own* ``post`` MCP tool call inside
        its turn (comms-uuy, LIVE); a framework-driven post here would either
        double-reply (if implemented) or, as the prior ``NotImplementedError``
        did, abort the message handler before the agent ever ran. So this is a
        deliberate no-op that reports success — like the webhook adapter's
        ``log`` deliver path — leaving the agent's MCP post as the sole reply
        path.
        """
        return SendResult(success=True)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Benign default: the gateway never calls this for commy.

        ``get_chat_info`` is an abstractmethod (so the override must exist for
        the class to instantiate) but the only callers are Feishu-internal
        (``feishu.py`` self-calls); no generic gateway path invokes it for this
        platform. Returns minimal identity rather than raising, so an unexpected
        call degrades gracefully instead of crashing a turn.
        """
        return {"chat_id": chat_id, "platform": PLATFORM_NAME}


def check_requirements() -> bool:
    """Activate the platform when the pod's commy config is present.

    A Hermes plugin platform is selectable for live use only when its ``check_fn``
    is truthy (``gateway.config``). The commy inbound path needs the boot
    listener + per-topic servers to spawn, which require the same environment
    ``SpawnConfig.from_env`` consumes (server dir, realm, minter creds, project
    channel). Reusing that single source of truth — rather than re-listing the
    vars — keeps the gate honest: present → activate; absent → stay unselectable
    rather than failing at ``connect()``.
    """
    try:
        SpawnConfig.from_env()
    except ValueError:
        return False
    return True


def register(ctx) -> None:
    """Hermes plugin entry point — registers the commy platform."""
    ctx.register_platform(
        name=PLATFORM_NAME,
        label=PLATFORM_LABEL,
        adapter_factory=lambda cfg: CommyAdapter(cfg),
        check_fn=check_requirements,
        install_hint=(
            "Set COMMY_SERVER_DIR, ZULIP_SITE, ZULIP_MINTER_EMAIL, "
            "ZULIP_MINTER_API_KEY, COMMY_PROJECT to activate inbound."
        ),
        emoji="🛰️",
    )
