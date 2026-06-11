"""Receive-path frame model for the commy Hermes adapter (comms-a7j.2).

Models the host-neutral ``{content, meta}`` frame a ``claude/channel`` inbound
notification carries (the contract in ``docs/claude-channel-inbound-contract.md``)
and the routing facts the adapter reads off it: the dedup key and the
``(channel, thread)`` pair that keys a Hermes session. Self-echo is dropped at
the substrate emitter (the bot's own posts never reach this carrier), so no
self-echo verdict is computed here. Pure and dependency-free — the bind to the
MCP notification method, and the ``MessageEvent`` construction, live in the
adapter.
"""

from dataclasses import dataclass
from typing import Any, Mapping, Optional

NOTIFICATION_METHOD = "notifications/claude/channel"


@dataclass(frozen=True)
class ChannelFrame:
    """A parsed ``claude/channel`` notification frame.

    ``meta`` keys are all optional — the emitter omits a key when its source
    value is absent — so every accessor returns ``Optional[str]`` and callers
    key off presence.
    """

    content: str
    meta: Mapping[str, str]

    @property
    def message_id(self) -> Optional[str]:
        return self.meta.get("message_id")

    @property
    def channel_name(self) -> Optional[str]:
        return self.meta.get("channel_name")

    @property
    def thread(self) -> Optional[str]:
        return self.meta.get("thread")

    @property
    def sender_id(self) -> Optional[str]:
        return self.meta.get("sender_id")

    @property
    def sender_name(self) -> Optional[str]:
        return self.meta.get("sender_name")


def frame_from_params(params: Mapping[str, Any]) -> ChannelFrame:
    """Build a ``ChannelFrame`` from a ``claude/channel`` notification's ``params``.

    ``params`` is the host-neutral payload ``{content, meta}``. ``meta`` values
    are already strings on the wire; absent ``content``/``meta`` degrade to
    empty rather than raising, since the frame's own accessors gate on presence.
    """
    content = params.get("content") or ""
    meta = params.get("meta") or {}
    return ChannelFrame(content=str(content), meta=dict(meta))
