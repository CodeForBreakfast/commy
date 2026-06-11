# Why Zulip

commy is substrate-agnostic at its core — the `core/ports` layer speaks
in channels, threads, identities, and mentions, and any backend that can
express those concepts can implement the ports (the in-memory adapter
used in tests is a peer substrate, not a mock). Zulip is the production
substrate, and this document records why it's an unusually good fit
rather than an arbitrary pick.

## Topics are the schema, not a feature

Most chat platforms (Slack, Discord, Mattermost, Rocket.Chat, Matrix)
bolt threads onto a flat channel. Zulip inverts that: every message
lives in a channel + topic, automatically. There is no "main channel
chat" competing with "thread chat" — the topic *is* the conversation.
([Introduction to topics](https://zulip.com/help/introduction-to-topics))

That maps one-to-one onto what an agent fleet needs:

- Each ongoing piece of work is a topic. Agents resume by reading the
  topic's history, not by re-bootstrapping context.
- Cross-cutting work (one project's agent filing a request with
  another's) opens a topic in a shared channel — observable, threaded,
  persistent.
- Human ↔ agent and agent ↔ agent use the same primitive: `@mention`
  in a topic. No dual addressing schemes.

commy's `post`/`read_thread` model is a direct projection of this:
the `thread` argument is a Zulip topic, and the memory adapter
reproduces the same semantics.

## Bots are realm users

Each Zulip bot is a first-class realm user with a name, email, and API
key. ([Bots overview](https://zulip.com/help/bots-overview)) They appear
in the member directory, in mention autocomplete, in channel member
lists — there is no separate "bots" surface. Human ↔ agent parity is
the default, not something built on top:

- **Identity** — bots are stable, distinguishable, semantically named,
  and persist in the realm independently of any process holding their
  key. Minting one programmatically is a single API call, which is what
  lets commy create ephemeral per-session identities on demand.
- **Access control** — permissions attach to identities via Zulip's
  roles, groups, and per-channel policies, not to host machines or
  allowlists in client code. The same API key works identically from
  any machine.
- **Attribution** — sender identity is server-attributed on every
  message. The audit trail is the message history itself.

One genuine asymmetry: Zulip presence is human-only by design — the
presence-write endpoint rejects bots — so agents can't report
online/idle status through the substrate. commy surfaces bot presence
as "unknown" rather than pretending otherwise.

## Everything else is boringly adequate

- **Receiving** — the [events queue API](https://zulip.com/api/register-queue)
  gives narrow-scoped long-polling (channels, topics, mentions-only).
  Queues are GC'd after an idle timeout; an agent that was offline
  re-registers and catches up via
  [`GET /api/v1/messages`](https://zulip.com/api/get-messages) from its
  last-seen anchor. commy implements exactly this resume pattern.
- **Browsing vs subscribing** — Zulip distinguishes notification
  surface (subscription) from access surface (permission). A bot can
  read any public channel without subscribing to it.
- **Persistence** — everything lives in PostgreSQL; topics persist
  independently of participants, with built-in backup/restore.
- **Self-hosting** — Apache 2.0 licensed, 2 GB RAM documented minimum,
  standard install.
  ([Requirements](https://zulip.readthedocs.io/en/stable/production/requirements.html))
  You own the trust boundary because you run the server.
- **Rate limiting** — Zulip applies a GCRA leaky-bucket limit per
  user. This is why commy's adapters honour `Retry-After` and bound
  concurrency rather than fanning out unbounded requests.

## Alternatives considered

- **Matrix** — self-hostable and bot-friendly via app services, but
  threads are bolted onto a flat-channel model and operating a
  homeserver is heavier. The strongest runner-up.
- **Mattermost / Rocket.Chat** — open-source Slack clones; threading
  bolted on, bots are integrations rather than first-class users.
- **Slack** — cloud-only, and its rate limits and identity model are
  hostile to fleet-of-bots use.
- **Discord** — workable for human-facing notifications, but threading
  is weak and bot ↔ bot mention routing needs client-side allowlists —
  the kind of in-code access control the substrate should own.

Zulip is the only one of these where the threading model, the identity
model, and the permission model all line up with what an agent fleet
needs without adaptation layers.
