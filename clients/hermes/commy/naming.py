"""Deterministic per-topic bot name for the commy Hermes adapter.

A per-topic connection runs in persistent mode under
``COMMY_BOT_NAME = deterministic_bot_name(channel, topic)``. Two
properties matter:

* **Stable & pure** — the same ``(channel, topic)`` always yields the same
  name, so a teardown/respawn re-acquires the SAME Zulip user_id (the substrate
  minter is idempotent by name). This is what gives a respawned per-topic
  identity continuity of authorship (e.g. ``edit_message`` on a prior anchor)
  and the persistent-mode recent-window catch-up.
* **Brand-safe** — it satisfies the substrate ``BotName`` invariant
  (``/^[a-z][a-z0-9_-]*$/``, max 40 chars; ``packages/mcp/bootstrap.ts:85``),
  so the server's eager acquire accepts it instead of failing boot.

The name embeds a readable slug of the channel and topic for operator legibility
and appends a short hash of the full pair for collision-resistance and to make
the result stable under slug truncation/normalisation.
"""

import hashlib

_MAX_LEN = 40
_HASH_LEN = 8
_PREFIX = "t-"
_LISTENER_PREFIX = "listen-"
# Room for the readable body between the prefix and the "-<hash>" suffix.
_BODY_BUDGET = _MAX_LEN - len(_PREFIX) - 1 - _HASH_LEN
_LISTENER_BODY_BUDGET = _MAX_LEN - len(_LISTENER_PREFIX) - 1 - _HASH_LEN


def _slug(value: str) -> str:
    """Lowercase; collapse every run of non-``[a-z0-9]`` to a single dash."""
    chars: list[str] = []
    prev_dash = False
    for ch in value.lower():
        if ("a" <= ch <= "z") or ("0" <= ch <= "9"):
            chars.append(ch)
            prev_dash = False
        elif not prev_dash:
            chars.append("-")
            prev_dash = True
    return "".join(chars).strip("-")


def deterministic_bot_name(channel: str, topic: str) -> str:
    digest = hashlib.sha256(f"{channel}\x00{topic}".encode()).hexdigest()[:_HASH_LEN]
    body = f"{_slug(channel)}-{_slug(topic)}".strip("-")[:_BODY_BUDGET].strip("-")
    return f"{_PREFIX}{body}-{digest}" if body else f"{_PREFIX}{digest}"


def deterministic_listener_name(channel: str) -> str:
    """The persistent boot-listener identity for ``channel`` (comms-a7j.4).

    Same stability + brand-safety guarantees as ``deterministic_bot_name``, but a
    distinct ``listen-`` prefix and hash domain so the channel-level boot
    listener never shares a name — and so never shares a minted Zulip user_id —
    with any per-topic (``t-``) connection in the same channel.
    """
    digest = hashlib.sha256(f"listener\x00{channel}".encode()).hexdigest()[:_HASH_LEN]
    body = _slug(channel)[:_LISTENER_BODY_BUDGET].strip("-")
    return f"{_LISTENER_PREFIX}{body}-{digest}" if body else f"{_LISTENER_PREFIX}{digest}"
