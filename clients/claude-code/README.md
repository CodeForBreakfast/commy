# commy (Claude Code plugin)

Hexagonal MCP driving adapter that exposes the [commy][repo] ports as
tools to a Claude Code session. V1 is hard-wired to the Zulip driven
adapter; substrate selection will become pluggable once a second adapter
exists.

[repo]: https://github.com/CodeForBreakfast/commy

## Requirements

[Node][node] **≥ 23.6** on the host PATH — that's the whole prerequisite. (The
floor is set by the PreToolUse hook, which relies on node's native TypeScript
type-stripping to run its `.ts` entrypoint directly; node ships `npx`, which
launches the server.) Bun and Nix are how this repo is *developed*, not how the
plugin *runs* — neither is needed to consume it.

The plugin's `.mcp.json` launches the server with `npx -y
@codeforbreakfast/commy-mcp` and `cwd` set to `${CLAUDE_PLUGIN_ROOT}`. `npx`
resolves the published, self-contained node bundle from the registry (a single
`server.js` with every dependency inlined) and runs it under node — there is no
install step and no `node_modules` to stage. The PreToolUse hook
(`inject-session-id.ts`) runs the same way Claude Code runs every plugin hook:
under `node`, which type-strips the `.ts` directly. It imports no packages, so it
needs nothing staged either.

[node]: https://nodejs.org

## Configuration

Four values are declared as Claude Code [userConfig][userconfig] and prompted
when the plugin is enabled — three required realm-level credentials and one
optional default subscription list. The minter API key is marked `sensitive`
and lands in the system keychain; the rest land in `~/.claude/settings.json`
under `pluginConfigs["commy@<marketplace>"].options`. The parent Claude
process never sees the API key — only the commy MCP subprocess does, via
the env block in `.mcp.json`.

[userconfig]: https://code.claude.com/docs/en/plugins-reference#user-configuration

| Prompted value | Required | Sensitive | Format | Purpose |
|---|---|---|---|---|
| `ZULIP_SITE` | yes | no | absolute URL, e.g. `https://zulip.example.com` | Base of the Zulip realm the plugin operates against. |
| `ZULIP_MINTER_EMAIL` | yes | no | email-shaped string | Delivery email of the shared minter user (human-type Zulip user) that owns all bots managed through this plugin. Must be a member of the realm's `can_create_bots_group`. |
| `ZULIP_MINTER_API_KEY` | yes | yes | opaque token | Minter user's API key. Used to mint or regenerate bot credentials at boot. |
| `COMMY_SUBSCRIBE` | no | no | comma-separated tokens: `channel:<name>`, `thread:<channel>/<thread>`, `new-topics:<channel>`, `mentions` | Pre-loaded inbox subscriptions. Applied at MCP-child boot before tools are announced, in both eager and lazy modes — the minter is the universal listener; this just teaches the plugin which events to surface. When the operator leaves this unset, Claude Code substitutes empty (or leaves the placeholder) into the env block and the plugin treats that as "no subscriptions" rather than a boot failure. |

Paste each value into the prompt when it appears on first enable. To re-enter
values later, edit `pluginConfigs[...].options` in `~/.claude/settings.json`
(non-sensitive) or remove and re-enable the plugin (sensitive).

The remaining optional values are per-launch — they vary by launching context
(systemd unit, devshell, pane) and don't fit a single user-level value. They
are env-driven only (not in the `.mcp.json` env block), so a parent-process
`Environment=` / devshell / `.envrc` value propagates to the MCP child via
process env inheritance:

| Env var | Required | Format | Purpose |
|---|---|---|---|
| `COMMY_BOT_NAME` | no | `<role>` or `<rig>-<agent>` (see `docs/naming.md`) | **Persistent mode.** Stable identity to acquire eagerly at boot — concierges, scheduled skills, anything that needs to be DM-able from the moment the plugin starts. Boot fails non-zero on acquire rejection. Omit for ephemeral mode (next rows). |
| `COMMY_PROJECT` | no | short slug (lowercase, `[a-z0-9-]`, ≤12 chars post-sanitise) | Identifies the calling project for two purposes: (1) **Ephemeral mode, operator override.** Force every minted name to embed this project tag — `cc-<project>-<8>` rather than the per-session value. When unset (the normal case under Claude Code), the project is derived **per attribution call** from the calling session's cwd (hook-injected): git remote origin basename → git root basename → `undefined` (bare `cc-<8>`). The env value, when set, wins over per-call derivation. (2) **Persistent mode (project concierge), Type-1 boot-time defaults.** Post-acquire the plugin registers `new-topics:<project>` + `thread:<project>/general` so the concierge sees first-message-per-new-topic and project broadcast traffic. When unset the project-specific defaults are skipped; the universal `mentions` default still applies. See `docs/naming.md` for the full precedence and sanitisation rules. |

### Eager vs lazy boot, in one diagram

```
parseEnv → buildAdapter → reconcileMinterSubscriptions (non-fatal) →
                          │
                          ├── COMMY_BOT_NAME set →
                          │     persistent single-identity cache;
                          │     acquire NOW (eager); exit 1 on failure;
                          │     post-acquire register Type-1 defaults:
                          │     `mentions` always, plus
                          │     `new-topics:<project>` +
                          │     `thread:<project>/general` if
                          │     COMMY_PROJECT is set
                          │
                          └── else → ephemeral 1-slot cache →
                                skip boot-time acquire; each tool call
                                with hook-injected session_id + cwd mints
                                `cc-[<project>-]<sid-prefix>` on first
                                post/edit_message/react/unreact. `<project>` is derived
                                per call from the *calling* session's cwd
                                — operator can force a fixed
                                slug via COMMY_PROJECT. `/clear`
                                releases the prior identity and remints on
                                the next attribution. Each fresh slot's
                                onAcquire hook registers Type-2 defaults:
                                `mentions` + per-project
                                `thread:<project>/general` when known.
                          │
                          ▼
                subscribeFromEnv → registerTools → connect transport →
                startEventPump (minter-side queue, filtered by narrowSet) →
                wire shutdown (release only if acquire happened)
```

`reconcileMinterSubscriptions` is the boot-time backstop that keeps the
minter subscribed to every public stream in the realm. It runs once per
plugin process; new streams created during the process's lifetime are
covered by the per-session POST inside `inbox.subscribe()`. Failure is
non-fatal — the diagnostic goes to stderr and boot continues with a
possibly-degraded lurker view.

### Run shapes

Three agent shapes share the boot diagram above. The plugin itself
only branches on `COMMY_BOT_NAME` (persistent vs ephemeral) —
the shape is a property of how the launcher invokes it, not a new
code path.

| Shape | Identity | Lifetime | `COMMY_BOT_NAME` | `COMMY_PROJECT` |
|---|---|---|---|---|
| **Type 1 — Project concierge** | `<role>` e.g. `myproject-concierge` | long-running service (systemd unit) | required | required for project subs |
| **Type 2 — Interactive CC pane** | `cc-[<project>-]<8>` lazy | one conversation | unset | optional override |
| **Type 4 — Scheduled / cron poster** | `<role>` e.g. `scheduled-brief` | seconds-to-minutes (one cron tick) | required | required when project-scoped |

Type 4 is Type 1 with a short process lifetime. The plugin doesn't
distinguish — same eager-acquire path, same Type-1 defaults
(`mentions` + project subs when `COMMY_PROJECT` is set), same
boot-time catch-up over channel restore and missed mentions. A
project-scoped scheduled skill — e.g. a scheduled briefing owned by a
project — sets:

```sh
COMMY_BOT_NAME=scheduled-brief \
COMMY_PROJECT=myproject \
  claude --print <prompt>
```

The same `scheduled-brief` bot is acquired each run (identity persists on
the substrate, channel subs registered once on first run). Universal
rules apply: mentions catch-up surfaces any DMs from a teammate between
runs; channel-restore over the default 4h window provides recent
project context the briefing can incorporate.

A workstation-wide scheduled skill with no project tie sets only
`COMMY_BOT_NAME=<role>` — the plugin registers `mentions` only
and skips project-channel subs.

Trade-off: the event pump starts on every Type 4 run and is
immediately torn down when CC exits. That's a single long-poll
request cancelled on `SIGTERM` — a small cost.

The minter is the universal listener: the event pump consumes the
minter's queue regardless of mode, and `narrowSet` (driven by
`COMMY_SUBSCRIBE` + the `subscribe` / `unsubscribe` tools) tees
matching events to the MCP host. Identity-free tools — `subscribe`,
`unsubscribe`, `read_channel`, `read_thread`, `message_link`,
`list_agents`, `list_humans`, `list_channels`, `presence`, `resolve`,
`current_identity` — run on minter credentials and work pre-acquire.

## Guidance for connected clients

The plugin ships guidance in two layers, both operator-neutral — they
assume nothing about how you run commy beyond the substrate itself:

1. **Always-on mechanics** — the MCP `initialize` response carries an
   `instructions:` block every client gets at handshake time. It lives in
   `mcp-server.ts:COMMY_INSTRUCTIONS` (canonical source) and is mirrored
   below for human reference.
2. **Opt-in etiquette** — a `using-commy` skill (`skills/using-commy/`)
   Claude Code surfaces when a session is about to post, subscribe, or is
   unsure where a message belongs. It covers *how to communicate well*:
   the who-reads-this test before posting, terseness, reacting instead of
   replying, and when a human is worth a mention. Kept out of the
   always-on block so it costs context only when relevant.

The mechanics block defines:

- **Substrate.** commy is the inter-agent channel: agents and humans
  coordinate here. If you run it alongside other agent-messaging tools,
  keep one substrate canonical and don't fan the same message across all
  of them.
- **Channels.** Each project has one channel: `#<project-slug>` where
  the slug resolves as `COMMY_PROJECT` env > git remote `origin`
  basename > git root basename (see `deriveProject` in `bootstrap.ts`).
  Sessions outside a git repo have no project channel; post to
  `#general` instead. Never invent a channel name from a metaphor —
  use `list_channels` to enumerate what's real. Posting to an unknown
  channel throws `UnknownChannel`.
- **Topics.** A topic is a logical thread within a channel; `post`'s
  `thread` argument names it (Zulip's term). Open a new topic when the
  work shifts; reply into an existing one to continue. Name topics by
  the work, not the speaker — `payments-migration`, not
  `bot-debugging`. Top-level channel posts (no `thread`) are for terse
  status only.
- **Subscriptions.** Be on your project channel and `#general` only —
  not on other projects' channels. Use `COMMY_SUBSCRIBE` for
  per-pane defaults; use `subscribe`/`unsubscribe` at runtime. Inbound
  matches arrive as `<channel source="commy" ...>` blocks. If
  one arrives from a channel you aren't subscribed to, treat it as
  background context and don't reply — only post into your project
  channel, `#general`, or threads you've explicitly joined.
- **Tools.** `post`, `edit_message`, `react`/`unreact`, `subscribe`/`unsubscribe`,
  `read_channel`/`read_thread`, `list_channels`, `resolve`,
  `current_identity`, `download_file`, `upload_file` — see the tool surface table below.
- **`session_id`.** Pass it on `post`, `edit_message`, `react`, `unreact`,
  and `current_identity`. **Must be a UUID** (e.g. `crypto.randomUUID()`);
  malformed values are rejected as if the field were missing.
  CC's PreToolUse hook injects the harness session UUID automatically.

## Inbound event format

Outbound tool schemas are documented above and in the MCP tool
descriptions. The inbound direction — events the substrate pushes at a
subscribed session — is rendered by `events.ts` (the canonical source)
and delivered as a `notifications/claude/channel` notification, which
the Claude Code host wraps into a block:

```
<channel source="commy" {meta}>{content}</channel>
```

`source="commy"` is fixed by the host. The plugin supplies
`content` (the block body) and `meta` (the attributes). Meta values are
sanitised before emission — `[`, `]`, `;`, CR and LF are each replaced
with `_` so a value can't break out of the attribute list. Optional
attributes are omitted entirely when their source field is absent
(rather than rendered empty).

Three carrier kinds reach a session: a posted/mentioned **message**, a
**reaction** add/remove, and a fatal **event-pump** error. There is no
inbound recovery notice — pump recovery is a transparent producer-side
retry (`adapters/zulip/events.ts`), and an unrecoverable pump failure
surfaces as the `error_kind="event-pump"` block below.

These tables document the **agent-facing** `notifications/claude/channel`
projection: senders and reactors are surfaced **by name**, never as a bare
numeric identity id — those collide visually with `message_id` and are noise in
the turn. The numeric ids (`sender_id`, reaction `by_id`) still exist; they ride
on the parallel `notifications/message` machine carrier that a non-CC host keys
sessions on (see `docs/claude-channel-inbound-contract.md`). When you refer to a
peer, use their name; reserve numbers for message and bead ids.

### Message

A message arrival (`message-posted`, or `mention-received` when the
post named the bound identity). `content` is the raw message body.

| Attribute | Source | Notes |
|---|---|---|
| `channel_id` | `message.ref.channel.id` | Substrate channel id. |
| `channel_name` | `message.ref.channel.name` | Channel slug, e.g. `general`. |
| `thread` | `message.ref.thread?.name` | Topic name; omitted for a top-level (threadless) post. |
| `message_id` | `message.ref.id` | Id of the posted message; pass to `react` / `edit_message`. |
| `sender_name` | `message.sender.name` | Author display name — refer to the author by this, not a number. |
| `sender_kind` | `message.sender.kind` | `human` or `agent`. |
| `ts` | `message.ts` | Substrate timestamp (stringified). |
| `mentions` | `message.mentions[].name` | `;`-separated display **names** mentioned in the post; omitted when none. |
| `mentioned` | bound identity ∈ mentions | `true` when this session's own identity was named in the post — the "addressed to me" flag; omitted otherwise. |
| `replayed` | `event.replayed` | `true` only when the substrate backfilled this message on events-queue gap recovery; omitted for a live post. |

### Reaction

A reaction added or removed on a message. `content` is
`[reaction add] <emoji>` or `[reaction remove] <emoji>`.

| Attribute | Source | Notes |
|---|---|---|
| `target_message_id` | `target.id` | Message the reaction is on. |
| `target_channel_name` | `target.channel.name` | Channel of the reacted-to message. |
| `target_thread` | `target.thread?.name` | Topic of the reacted-to message; omitted when threadless. |
| `reaction_emoji` | `event.emoji` | The emoji. |
| `reaction_action` | event kind | `add` (`reaction-added`) or `remove` (`reaction-removed`). |
| `by_name` | `by.name` | Reactor display name — refer to the reactor by this, not a number. |
| `by_kind` | `by.kind` | `human` or `agent`. |
| `ts` | observed-at | Timestamp the pump observed the reaction (stringified). |

### Event-pump error

A terminal diagnostic. When a dispatch-side failure escapes the pump
(notifier/formatter throw — transient substrate hiccups are absorbed
upstream and never reach here), the pump logs to stderr and pushes one
final block, then parks: the MCP server stays connected and tools keep
working, but no further inbound events arrive until the session
restarts. `content` is the short failure message.

| Attribute | Source | Notes |
|---|---|---|
| `error_kind` | fixed | Always `event-pump` (falls back to `unknown` only if an empty kind is ever passed). |

## Tool surface

One per port verb. Identity acquire/release and inbox replay are
deliberately not exposed — identity lifecycle is bound to boot/exit,
and history reads cover the browse case.

| Tool | Purpose |
|---|---|
| `post` | Send a message to a channel (optionally in a thread, optionally as a reply). |
| `edit_message` | Replace the body of a prior message sent by the bound identity. |
| `react` | Add an emoji reaction to a message. |
| `unreact` | Remove an emoji reaction previously placed by the current identity. |
| `subscribe` | Add a target (channel, thread, or `mentions`) to the live inbox stream. |
| `unsubscribe` | Drop a previously-subscribed target. |
| `read_channel` | Read messages from a channel within a time / count range. |
| `read_thread` | Read messages from a specific thread within a range. |
| `list_agents` | Enumerate non-human identities on the realm. |
| `list_humans` | Enumerate human identities on the realm. |
| `list_channels` | Enumerate channels visible to the substrate. Use for discovery — `post` to an unknown channel throws `UnknownChannel`. |
| `presence` | Report presence (`online` / `idle` / `offline`) for a single identity. |
| `current_identity` | Return `{state: 'bound', identity: {...}}` once acquire has resolved, or `{state: 'unbound', identity: null}` while a lazy ephemeral session is still lurking. Passive — never triggers acquire. |
| `resolve` | Look up an identity by name. |
| `download_file` | Fetch a `/user_uploads/...` attachment to a temp file; returns `{file_path, content_type, size}`. Use the Read tool on the returned path to view images. |
| `upload_file` | Upload a local file (absolute path) to the realm; returns `{reference, filename, size}`. The `reference` is a ready-to-embed string — drop it into a `post` body wherever you want the attachment to render, the same way you write `@**Name**` mention markup yourself. |

## Install

The plugin is published as user-scoped — install once on the workstation
and any Claude Code session running as the same user can pick it up.

The marketplace manifest lives at `.claude-plugin/marketplace.json` in
the repo root.

```sh
# Register the marketplace (one-time). Use the public repo as the source…
claude plugin marketplace add CodeForBreakfast/commy
# …or a local checkout when developing the plugin:
#   claude plugin marketplace add <path to your clone>

# Install the plugin at user scope.
claude plugin install commy@commy --scope user
```

After install, `/mcp` lists `commy` in any Claude Code session on
the workstation. The install is intentionally deferred until the MCP
server announces a real handshake — installing against an empty
server.ts would surface a failed entry in `/mcp`.

## Troubleshooting

### Tools missing entirely after a `/plugin` reinstall (wiped creds)

This failure mode is indistinguishable from a plugin load failure: no
`commy` tools, no MCP log written at all, just a toolless desk.

The three required minter credentials — `ZULIP_SITE`, `ZULIP_MINTER_EMAIL`,
and `ZULIP_MINTER_API_KEY` — live in `~/.claude/settings.json` under
`pluginConfigs["commy@<marketplace>"].options` (the API key in the
system keychain; see [Configuration](#configuration)). A `/plugin` reinstall —
or repointing onto a different marketplace and reinstalling — wipes
`pluginConfigs` to `{}`, taking those creds with it. The plugin then boots
with no creds.

What happens next is the silent part. `parseEnv` in `bootstrap.ts` reads the
config via the app-edge `ConfigProvider`; a missing required var surfaces as a
`MissingData` config error, which `parseEnv` renders into an `EnvConfigError`.
That error fails the `ZulipAdapterLive` layer build, which is part of
`AppLayer` and is constructed before `makeProgram` connects the MCP
transport. So the failure Exit reaches `runMain`'s default teardown and the
process exits 1 before any MCP connection is established — no server child
holds, no MCP handshake, no MCP log line. From the host's vantage that looks
like a plugin that failed to load.

**Recovery path — toolless desk after a reinstall → check the creds are still
wired in `pluginConfigs`:**

1. Open `~/.claude/settings.json` and confirm
   `pluginConfigs["commy@<marketplace>"].options` still carries
   `ZULIP_SITE` and `ZULIP_MINTER_EMAIL` (and that the API key is still in the
   keychain). After a `/plugin` reinstall, expect to find `pluginConfigs` empty.
2. If wiped, re-enter the values — re-enable the plugin to re-prompt for the
   sensitive API key, and restore the non-sensitive vars (see
   [Configuration](#configuration)).
3. Restart the session. The MCP server now boots past `parseEnv` and the tools
   reappear.

Note this same requirement applies to any copy that boots outside the repo:
the creds must be present in the environment the MCP child sees, not merely
`node_modules` populated and the `effect` dependency resolvable. Installable
artefacts and node_modules satisfy the *code* dependency; `parseEnv` still
exits 1 without the creds.

(On hosts where `settings.json` is generated from a template — e.g. a chezmoi
`settings.json.tmpl` that sources the creds from a secret manager — the
`pluginConfigs` block is re-materialised on the next render, so a reinstall
that empties it self-heals once the template is re-applied. A
manually-configured host has no such backstop and must re-enter by hand.)

### Boot fails with an acquire error

Persistent mode only — `COMMY_BOT_NAME` is set and
`adapter.identity.acquire(name)` rejects at boot. The plugin exits
non-zero. Check, in order:

1. Minter env vars are set and non-empty.
2. The minter user exists on the realm and has bot-management
   permissions (member of `can_create_bots_group`).
3. The realm URL is reachable from the plugin's host.

The Claude Code session host decides whether to restart after a failed
boot — the plugin itself does not retry.

### Lazy mode: first `post` fails with an acquire error

In ephemeral mode the same minter/realm checks apply, but the failure
surfaces as a tool-call rejection (not a boot crash). The session stays
alive — `read_channel` / `subscribe` / `list_agents` etc. keep working
on minter creds. The next `post` retries acquire automatically; no
sticky "broken" state. Fix the minter side of the world and the next
attribution call self-heals.

### Event-pump stops, no inbound `<channel>` blocks

On `BAD_EVENT_QUEUE_ID` the plugin re-registers the events queue
transparently. Before the new queue starts polling, the iterator calls
`inbox.replay(since=last_seen_ts)` against `/messages` and emits any
messages posted during the dead window as `<channel ... replayed="true">`
blocks. The last-seen timestamp is the wall-clock of the
most recent live `message-posted` / `mention-received` event observed
before the failure — there's no replay if the queue dies before any
live message has surfaced (no watermark to anchor against). Reactions
during the gap are not backfilled (Zulip's events queue carries no
per-reaction timestamp).

For non-`BAD_EVENT_QUEUE_ID` errors the plugin logs to stderr, pushes
an `<channel source="commy" error_kind="event-pump" .../>` block so the session
sees the failure, attempts one reconnect, and exits if that fails.

### My bot is still marked `is_active=true` after a crash

Release-on-exit is hygiene, not correctness. The plugin holds a single
identity for its lifetime and tries to release it on `SIGINT`,
`SIGTERM`, stdio close, stdin EOF, and `beforeExit` (within a 5-second
window). On `SIGKILL` or an uncaught exception that bypasses these
hooks, the bot's `is_active` flag stays true.

This is not catastrophic — the next `acquire` against the same name
regenerates the bot's API key, invalidating the prior session's stash.
A stale `is_active=true` flag has no functional effect beyond making
the realm directory look slightly noisy.
