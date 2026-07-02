"""Receive-notification contract tests for the channel-aware client session.

The substrate dual-emits each inbound event on ``notifications/message`` (the
machine carrier the adapter consumes) and ``notifications/claude/channel`` (the
ungated display carrier). The stock MCP SDK union has no slot for the display
carrier, so the receive loop fails validation and logs a warning on every frame;
``ChannelAwareClientSession`` extends the union so the frame validates and is
discarded, leaving the consumed carrier untouched.
"""

import logging

import anyio
import pydantic
import pytest
from mcp.shared.message import SessionMessage
from mcp.types import (
    JSONRPCMessage,
    LoggingMessageNotification,
    ServerNotification,
)

from commy.receive import NOTIFICATION_METHOD
from commy.session import (
    ChannelAwareClientSession,
    ChannelAwareServerNotification,
    ClaudeChannelNotification,
)

# The display-carrier params the substrate's `channelNotifier` constructs for
# `notifications/claude/channel` (packages/mcp/event-pump.ts): `{content, meta}`,
# meta minus the numeric identity ids.
_CLAUDE_CHANNEL_FRAME = {
    "content": "hello",
    "meta": {
        "channel_name": "commy",
        "thread": "standup",
        "message_id": "1",
        "sender_name": "peer",
    },
}


def _notification_dump(method, params):
    """The exact dict the SDK receive loop validates (``mcp/shared/session.py``)."""
    message = JSONRPCMessage.model_validate({"jsonrpc": "2.0", "method": method, "params": params})
    return message.root.model_dump(by_alias=True, mode="json", exclude_none=True)


def test_stock_sdk_union_rejects_the_claude_channel_frame():
    """Characterise the upstream gap the fix exists for: the SDK's own union has
    no slot for the ungated display carrier, so validation fails — this is what
    makes the receive loop log a warning on every inbound frame."""
    dump = _notification_dump(NOTIFICATION_METHOD, _CLAUDE_CHANNEL_FRAME)
    with pytest.raises(pydantic.ValidationError) as exc_info:
        ServerNotification.model_validate(dump)
    # The "v/missing" seen in the homelab log is pydantic's error-doc URL path
    # (errors.pydantic.dev/2.13/v/missing), NOT a field named "v": no union
    # member matched, surfaced as missing required fields on the closest members.
    assert "missing" in str(exc_info.value)


def test_channel_aware_union_accepts_the_claude_channel_frame():
    dump = _notification_dump(NOTIFICATION_METHOD, _CLAUDE_CHANNEL_FRAME)
    notification = ChannelAwareServerNotification.model_validate(dump)
    assert isinstance(notification.root, ClaudeChannelNotification)
    assert notification.root.params.content == "hello"
    assert notification.root.params.meta["message_id"] == "1"


def test_channel_aware_union_still_resolves_the_consumed_message_carrier():
    """The ``notifications/message`` carrier the adapter actually consumes must
    still resolve to ``LoggingMessageNotification`` under the extended union, with
    the full machine frame (incl. ``sender_id``) intact under ``params.data``."""
    machine_frame = {"content": "hello", "meta": {"message_id": "1", "sender_id": "42"}}
    dump = _notification_dump(
        "notifications/message",
        {"level": "info", "logger": "commy", "data": machine_frame},
    )
    notification = ChannelAwareServerNotification.model_validate(dump)
    assert isinstance(notification.root, LoggingMessageNotification)
    assert notification.root.params.data == machine_frame


def test_receive_loop_handles_both_carriers_without_a_validation_warning(caplog):
    """End-to-end against the REAL SDK receive loop: feed both dual-emitted
    carriers through a ``ChannelAwareClientSession`` and assert the consumed
    ``notifications/message`` carrier reaches ``logging_callback`` while the
    ungated ``notifications/claude/channel`` carrier produces NO validation
    warning (the defect was one warning per inbound frame)."""
    logged_frames: list = []

    async def logging_callback(params) -> None:
        logged_frames.append(params.data)

    async def scenario() -> None:
        to_client_send, to_client_recv = anyio.create_memory_object_stream(10)
        from_client_send, from_client_recv = anyio.create_memory_object_stream(10)
        async with ChannelAwareClientSession(
            to_client_recv, from_client_send, logging_callback=logging_callback
        ):
            display = JSONRPCMessage.model_validate(
                {"jsonrpc": "2.0", "method": NOTIFICATION_METHOD, "params": _CLAUDE_CHANNEL_FRAME}
            )
            machine_frame = {"content": "hello", "meta": {"message_id": "1", "sender_id": "42"}}
            machine = JSONRPCMessage.model_validate(
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/message",
                    "params": {"level": "info", "logger": "commy", "data": machine_frame},
                }
            )
            await to_client_send.send(SessionMessage(display))
            await to_client_send.send(SessionMessage(machine))

            with anyio.move_on_after(2.0):
                while not logged_frames:
                    await anyio.sleep(0.01)

        assert logged_frames == [machine_frame]

    with caplog.at_level(logging.WARNING):
        anyio.run(scenario)

    validation_warnings = [r for r in caplog.records if "Failed to validate notification" in r.getMessage()]
    assert validation_warnings == []
