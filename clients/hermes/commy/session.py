"""Channel-aware MCP client session for the commy Hermes adapter.

The substrate dual-emits each inbound event on two carriers (see
``docs/claude-channel-inbound-contract.md``): ``notifications/message`` — the
MCP-standard ``LoggingMessageNotification`` this adapter consumes for the full
machine frame (it alone carries ``sender_id``) — and
``notifications/claude/channel``, the Claude-Code display carrier. Delivery of
the display carrier is **ungated**: every connected client receives it, this
adapter included, even though the adapter binds the other carrier.

The MCP Python SDK validates every incoming notification against its typed
``ServerNotification`` union in the receive loop (``mcp/shared/session.py``)
*before* any handler or callback runs. ``notifications/claude/channel`` has no
slot in that union, so the SDK raises a ``ValidationError``, catches it, and logs
``"Failed to validate notification"`` on every inbound frame. (The ``v/missing``
seen in logs is pydantic's error-doc URL path, not a missing field named ``v``.)

Recognising the experimental method here — extending the session's
receive-notification union with a typed model for it — lets the SDK validate the
frame and route it to the no-op default handler, so the warning stops while the
``notifications/message`` carrier the adapter consumes is left untouched.
"""

from __future__ import annotations

from typing import Any, Literal, Union

import pydantic
from mcp import ClientSession
from mcp.types import Notification, ServerNotificationType

from .receive import NOTIFICATION_METHOD


class ChannelFrameParams(pydantic.BaseModel):
    """The ``{content, meta}`` params of a ``notifications/claude/channel`` frame.

    Permissive by design — the adapter does not consume this carrier, it only
    needs the frame to *validate* so the SDK stops warning. ``extra="allow"``
    keeps validation green if the display carrier ever grows a field.
    """

    content: str = ""
    meta: dict[str, str] = pydantic.Field(default_factory=dict)
    model_config = pydantic.ConfigDict(extra="allow")


class ClaudeChannelNotification(Notification[ChannelFrameParams, Literal["notifications/claude/channel"]]):
    """The ungated display-carrier notification the adapter recognises but ignores."""

    method: Literal["notifications/claude/channel"] = NOTIFICATION_METHOD
    params: ChannelFrameParams


ChannelAwareServerNotification = pydantic.RootModel[Union[ServerNotificationType, ClaudeChannelNotification]]


class ChannelAwareClientSession(ClientSession):
    """A ``ClientSession`` whose receive-notification union also accepts the
    ungated ``notifications/claude/channel`` display carrier.

    With the carrier in the union the SDK receive loop validates it and routes it
    to the default no-op message handler (it falls through
    ``_received_notification``'s catch-all), instead of failing validation and
    logging a warning on every inbound frame. The ``notifications/message``
    carrier the adapter binds via ``logging_callback`` resolves to
    ``LoggingMessageNotification`` exactly as before — the two methods are
    disambiguated by their distinct ``method`` literals.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._receive_notification_type = ChannelAwareServerNotification
