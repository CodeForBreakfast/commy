"""Receive-path tests for the commy Hermes adapter (comms-a7j.2).

Drives the real routing/dedup path with no mocks of that logic: a
``claude/channel`` notification frame goes through
``CommyAdapter.receive_channel_notification`` and we capture what reaches
the Hermes pipeline at the ``handle_message`` seam (the boundary the contract
hands off to Hermes — ``docs/claude-channel-inbound-contract.md`` render step).
The session key is asserted via the real ``gateway.session.build_session_key``.
"""

import asyncio

import pytest
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import build_session_key
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest

from commy import register
from commy.adapter import PLATFORM_NAME, CommyAdapter


@pytest.fixture(autouse=True)
def _platform_registered():
    """Register the platform so ``Platform('commy')`` self-extends.

    The adapter's ``__init__`` constructs ``Platform(PLATFORM_NAME)``, which
    only resolves once ``register`` has populated the live ``platform_registry``
    (the enum extends via ``_missing_``) — the same precondition the pod meets
    by loading the plugin before instantiating adapters.
    """
    manager = PluginManager()
    manifest = PluginManifest(name="commy-platform", kind="platform")
    register(PluginContext(manifest, manager))


class _RecordingAdapter(CommyAdapter):
    """Captures events at the ``handle_message`` pipeline seam.

    Overriding ``handle_message`` records the routed ``MessageEvent`` without
    spinning the gateway runner. Everything under test — frame parse, dedup,
    ``SessionSource`` construction — runs in ``receive_channel_notification``
    *before* this seam, so nothing in the routing path is mocked.
    """

    def __init__(self, config: PlatformConfig):
        super().__init__(config)
        self.handled: list[MessageEvent] = []

    async def handle_message(self, event: MessageEvent) -> None:
        self.handled.append(event)


def _make_adapter() -> _RecordingAdapter:
    return _RecordingAdapter(PlatformConfig())


def _frame(content: str = "hello from the topic", **overrides) -> dict:
    """A `claude/channel` notification `params` (`{content, meta}`).

    Pass a meta key as ``None`` to omit it (the contract omits absent keys).
    """
    meta = {
        "channel_id": "7",
        "channel_name": "epr-backend",
        "thread": "standup",
        "message_id": "100",
        "sender_id": "u-alice",
        "sender_name": "Alice",
        "sender_kind": "agent",
        "ts": "1780000000",
    }
    meta.update(overrides)
    meta = {k: v for k, v in meta.items() if v is not None}
    return {"content": content, "meta": meta}


def _deliver(adapter: _RecordingAdapter, params: dict) -> None:
    asyncio.run(adapter.receive_channel_notification(params))


def test_notification_yields_single_handle_message_with_content_and_meta():
    adapter = _make_adapter()
    _deliver(adapter, _frame(content="ship it"))

    assert len(adapter.handled) == 1
    event = adapter.handled[0]
    assert event.text == "ship it"
    assert event.message_type == MessageType.TEXT
    assert event.message_id == "100"
    # meta survives as accessible provenance
    assert event.raw_message["channel_name"] == "epr-backend"
    assert event.raw_message["sender_id"] == "u-alice"
    # source is routed from meta
    assert event.source.platform == Platform(PLATFORM_NAME)
    assert event.source.chat_id == "epr-backend"
    assert event.source.thread_id == "standup"
    assert event.source.user_id == "u-alice"


def test_duplicate_message_id_delivered_once():
    adapter = _make_adapter()
    _deliver(adapter, _frame(message_id="dup"))
    _deliver(adapter, _frame(message_id="dup", content="second copy"))

    assert len(adapter.handled) == 1
    assert adapter.handled[0].text == "hello from the topic"


def test_distinct_message_ids_both_delivered():
    adapter = _make_adapter()
    _deliver(adapter, _frame(message_id="a"))
    _deliver(adapter, _frame(message_id="b"))

    assert [e.message_id for e in adapter.handled] == ["a", "b"]


def test_thread_less_frame_is_ignored():
    # Policy (comms-a7j.3): a top-level post (no `thread`) carries no natural
    # session key — Hermes keys on (channel, topic). By the substrate convention
    # "top-level = terse pings only; substantive work goes in a topic", such a
    # frame is not agent-actionable, so it is dropped before handle_message
    # rather than routed into a (channel, None) junk-drawer session.
    adapter = _make_adapter()
    _deliver(adapter, _frame(thread=None))

    assert adapter.handled == []


def test_threaded_frame_still_delivered():
    # The drop is specific to absent `thread`: a frame with a topic still routes.
    adapter = _make_adapter()
    _deliver(adapter, _frame(thread="standup"))

    assert len(adapter.handled) == 1


def _key(event: MessageEvent) -> str:
    return build_session_key(event.source)


def test_channel_thread_maps_to_expected_session_key():
    adapter = _make_adapter()
    _deliver(adapter, _frame(channel_name="epr-backend", thread="standup"))

    assert _key(adapter.handled[0]) == "agent:main:commy:thread:epr-backend:standup"


def test_same_channel_thread_different_senders_share_session_key():
    # The routing key is (channel, thread), NOT the sender: two agents posting
    # in one topic share one Hermes session.
    adapter = _make_adapter()
    _deliver(adapter, _frame(message_id="m1", sender_id="u-alice"))
    _deliver(adapter, _frame(message_id="m2", sender_id="u-bob"))

    keys = {_key(e) for e in adapter.handled}
    assert len(adapter.handled) == 2
    assert keys == {"agent:main:commy:thread:epr-backend:standup"}


def test_catchup_live_window_overlap_deduped():
    # Substrate flags the catch-up/live-window overlap (channels-catch-up.ts:51-54):
    # the same message can arrive once as a replayed backfill and once live.
    adapter = _make_adapter()
    _deliver(adapter, _frame(message_id="overlap", replayed="true"))
    _deliver(adapter, _frame(message_id="overlap"))  # live re-delivery

    assert len(adapter.handled) == 1


def test_listener_vs_owner_double_delivery_deduped():
    # Two independent deliveries of the same substrate message (listener seat and
    # owner seat both receiving it) collapse to one handle_message.
    adapter = _make_adapter()
    _deliver(adapter, _frame(message_id="same"))
    _deliver(adapter, _frame(message_id="same"))

    assert len(adapter.handled) == 1
