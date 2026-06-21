"""Real per-topic MCP transport for the commy Hermes adapter (comms-a7j.5).

Implements ``TopicTransport`` by spawning the commy server subprocess over
stdio and holding an MCP ``ClientSession``, routing each inbound frame into the
sink. The ``mcp`` SDK is imported lazily inside ``_run`` so the lifecycle module
(``connection.py``) stays importable in environments without ``mcp`` — the SDK
is provided by the host Hermes at pod runtime (and by the dev group in tests).

**Carrier: ``notifications/message``, frame at ``params.data``.** The substrate
dual-emits each inbound event as both ``notifications/claude/channel`` and the
MCP-standard ``notifications/message`` (``channelNotifier``,
``packages/mcp/event-pump.ts``). We bind the latter via the SDK's
``logging_callback``: ``notifications/message`` is ``LoggingMessageNotification``
and is delivered to ``logging_callback``, carrying the full machine frame (incl.
``sender_id``). Per the bb7.1 contract the ``{content, meta}`` frame is nested
under ``params.data`` (the MCP logging envelope requires ``level`` at the params
root), so we forward ``params.data`` — the same shape the receive path consumes.

The Python MCP SDK validates EVERY incoming notification against its typed
``ServerNotification`` union *before* any handler runs, and
``notifications/claude/channel`` has no slot in that union — so the SDK does NOT
silently drop it, it logs a ``"Failed to validate notification"`` warning on
every inbound frame (comms-b7it). ``ChannelAwareClientSession`` extends the union
to recognise the ungated display carrier, so the SDK validates and discards it
cleanly while the ``notifications/message`` binding above is untouched. See
``commy/session.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
from collections.abc import Mapping
from typing import IO, Any, Optional

from .connection import (
    Clock,
    ConnectionSpec,
    Frame,
    FrameSink,
    SpawnConfig,
    TopicConnectionManager,
    TransportFactory,
)
from .listener import ChannelListener, OwnedKeys, SpawnTrigger, build_listener_spec

_STOP_GRACE_SECONDS = 5.0


def _mcp_transport_factory(errlog: Optional[IO[str]]) -> TransportFactory:
    """A factory that builds real ``McpTopicTransport`` subprocess connections."""

    def factory(spec: ConnectionSpec, frame_sink: FrameSink) -> McpTopicTransport:
        return McpTopicTransport(spec, frame_sink, errlog=errlog)

    return factory


def make_manager(
    config: SpawnConfig,
    sink: FrameSink,
    *,
    clock: Clock = time.monotonic,
    errlog: Optional[IO[str]] = None,
) -> TopicConnectionManager:
    """A ``TopicConnectionManager`` whose transports are real MCP subprocesses."""
    return TopicConnectionManager(config, _mcp_transport_factory(errlog), sink, clock=clock)


def make_listener(
    config: SpawnConfig,
    *,
    trigger: SpawnTrigger,
    owned: OwnedKeys,
    errlog: Optional[IO[str]] = None,
) -> ChannelListener:
    """The boot listener over a real MCP subprocess, subscribed ``config.channel``.

    Wires the same real ``McpTopicTransport`` the per-topic manager uses, so the
    channel-level boot connection rides the identical subprocess + MCP path.
    """
    spec = build_listener_spec(config, config.channel)
    return ChannelListener(spec, _mcp_transport_factory(errlog), trigger, owned)


class McpTopicTransport:
    """One per-topic connection: commy server subprocess + MCP client session."""

    def __init__(
        self,
        spec: ConnectionSpec,
        sink: FrameSink,
        *,
        errlog: Optional[IO[str]] = None,
        stop_grace_seconds: float = _STOP_GRACE_SECONDS,
    ) -> None:
        self._spec = spec
        self._sink = sink
        self._errlog = errlog if errlog is not None else sys.stderr
        self._stop_grace_seconds = stop_grace_seconds
        self._task: Optional[asyncio.Task[None]] = None
        self._ready = asyncio.Event()
        self._closing = asyncio.Event()
        self._session: Optional[Any] = None

    async def _on_log(self, params: object) -> None:
        """Forward a ``notifications/message`` frame (``params.data``) to the sink."""
        data = getattr(params, "data", None)
        if isinstance(data, Mapping):
            frame: Frame = data
            await self._sink(frame)

    async def _run(self) -> None:
        from mcp import StdioServerParameters
        from mcp.client.stdio import stdio_client

        from .session import ChannelAwareClientSession

        server = StdioServerParameters(
            command=self._spec.command,
            args=list(self._spec.args),
            env=dict(self._spec.env),
            cwd=self._spec.cwd,
        )
        try:
            async with stdio_client(server, errlog=self._errlog) as (read, write):
                async with ChannelAwareClientSession(read, write, logging_callback=self._on_log) as session:
                    await session.initialize()
                    # Hold the live session so `post` can deliver outbound replies
                    # over the same connection (comms-a9q4). Concurrent requests
                    # from another task are safe — the SDK routes responses by id.
                    self._session = session
                    self._ready.set()
                    await self._closing.wait()
        finally:
            # Unblock a `start()` that is still waiting even if connect failed,
            # so the caller observes the failure rather than hanging.
            self._session = None
            self._ready.set()

    async def start(self) -> None:
        """Spawn the subprocess and connect; raise if the connection fails to come up."""
        self._task = asyncio.create_task(self._run())
        waiter = asyncio.ensure_future(self._ready.wait())
        try:
            await asyncio.wait({self._task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await waiter
        if self._task.done():
            # Completed before/at readiness — surface any connect failure.
            exc = self._task.exception()
            if exc is not None:
                raise exc

    async def stop(self) -> None:
        """Tear down: signal a graceful close, falling back to cancellation."""
        if self._task is None:
            return
        self._closing.set()
        try:
            await asyncio.wait_for(asyncio.shield(self._task), self._stop_grace_seconds)
        except asyncio.TimeoutError:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        except asyncio.CancelledError:
            pass
        except Exception:
            # A commanded teardown can race the subprocess/pipe close: an
            # in-flight server notification hitting a stream the SDK is already
            # tearing down surfaces as a BrokenResourceError out of stdio_client.
            # By here `_run` has completed and the connection is gone — which is
            # stop()'s whole job — so a teardown-time error is not a stop failure.
            pass
        finally:
            self._task = None

    async def post(self, body: str, channel: str, topic: str) -> Optional[str]:
        """Deliver an outbound reply by calling the commy ``post`` MCP tool.

        Rides the live per-topic session this connection already holds, so the
        reply posts as the per-topic identity into ``(channel, topic)``. Returns
        the posted message id when the server reports one. A connection that is
        not currently live (pre-start / post-stop) has no session to ride and
        returns ``None`` rather than raising into the agent's turn.
        """
        session = self._session
        if session is None:
            return None
        result = await session.call_tool(
            "post", {"channel_name": channel, "thread": topic, "body": body}
        )
        return _message_id_from_result(result)


def _message_id_from_result(result: object) -> Optional[str]:
    """Best-effort extraction of ``message_id`` from a ``post`` tool result.

    The commy ``post`` tool returns ``{message_id, channel_id, channel_name,
    thread}``; the SDK surfaces it as ``structuredContent`` and/or JSON text in
    ``content``. Read whichever is present, tolerating either shape, and never
    raise — a missing id just means the caller surfaces ``None`` (delivery still
    happened; only the id is unknown)."""
    for candidate in _result_payloads(result):
        message_id = candidate.get("message_id")
        if message_id is not None:
            return str(message_id)
    return None


def _result_payloads(result: object) -> list[Mapping[str, Any]]:
    payloads: list[Mapping[str, Any]] = []
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, Mapping):
        payloads.append(structured)
    for item in getattr(result, "content", None) or []:
        text = getattr(item, "text", None)
        if not text:
            continue
        try:
            parsed = json.loads(text)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, Mapping):
            payloads.append(parsed)
    return payloads
