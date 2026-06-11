"""Tests for the deterministic per-topic bot name (comms-a7j.5).

The name is the ``COMMY_BOT_NAME`` for a per-topic connection. It MUST
satisfy the substrate's ``BotName`` brand invariant — lowercase ASCII, digits,
dashes, underscores; starts with a letter; max 40 chars (the regex
``/^[a-z][a-z0-9_-]*$/`` at ``packages/mcp/bootstrap.ts:85``/``:96``) — because
the substrate rejects a non-conforming name at boot. It MUST also be a pure,
stable function of ``(channel, topic)`` so a respawn re-acquires the SAME Zulip
user_id (the minter is idempotent by name).
"""

import re

from commy.naming import deterministic_bot_name

# Mirror of the substrate brand invariant (packages/mcp/bootstrap.ts:85).
_BOT_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


def _assert_brand_safe(name: str) -> None:
    assert 1 <= len(name) <= 40, f"length {len(name)} out of [1,40]: {name!r}"
    assert _BOT_NAME_RE.match(name), f"not brand-safe: {name!r}"


def test_name_is_brand_safe():
    _assert_brand_safe(deterministic_bot_name("epr-backend", "standup"))


def test_name_is_stable_across_calls():
    a = deterministic_bot_name("epr-backend", "p2ac-a7j.5")
    b = deterministic_bot_name("epr-backend", "p2ac-a7j.5")
    assert a == b


def test_distinct_topics_yield_distinct_names():
    a = deterministic_bot_name("epr-backend", "topic-one")
    b = deterministic_bot_name("epr-backend", "topic-two")
    assert a != b


def test_distinct_channels_yield_distinct_names():
    a = deterministic_bot_name("alpha", "shared-topic")
    b = deterministic_bot_name("beta", "shared-topic")
    assert a != b


def test_channel_topic_order_is_not_symmetric():
    # (channel, topic) and (topic, channel) must not collide.
    assert deterministic_bot_name("a", "b") != deterministic_bot_name("b", "a")


def test_readable_slug_is_embedded_for_simple_inputs():
    name = deterministic_bot_name("epr-backend", "standup")
    assert "epr-backend" in name
    assert "standup" in name


def test_long_inputs_stay_within_brand_length():
    name = deterministic_bot_name("x" * 200, "y" * 200)
    _assert_brand_safe(name)


def test_long_inputs_remain_distinct():
    a = deterministic_bot_name("channel-" + "x" * 200, "topic-" + "a" * 200)
    b = deterministic_bot_name("channel-" + "x" * 200, "topic-" + "b" * 200)
    assert a != b
    _assert_brand_safe(a)
    _assert_brand_safe(b)


def test_non_ascii_and_punctuation_are_sanitised():
    name = deterministic_bot_name("café/#weird", "tøpic with spaces!")
    _assert_brand_safe(name)


def test_empty_slug_inputs_still_brand_safe():
    # Inputs that slug to nothing must still produce a valid (hash-only) name.
    name = deterministic_bot_name("!!!", "@@@")
    _assert_brand_safe(name)


def test_empty_slug_inputs_remain_distinct():
    assert deterministic_bot_name("!!!", "@@@") != deterministic_bot_name("###", "$$$")
