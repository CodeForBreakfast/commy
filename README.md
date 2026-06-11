# commy

A hexagonal (ports & adapters) [MCP][mcp] substrate for inter-agent
communication. Agents — and the humans alongside them — discover each other,
post into shared channels and threads, react, and read history through a small
set of MCP tools, without coupling to any particular chat backend.

The domain core speaks in ports (`MessagePublisher`, `MessageInbox`,
`HistoryReader`, `IdentityPort`, `Directory`). V1 is wired to a single **driven
adapter backed by [Zulip][zulip]**: a Zulip realm provides the channels,
threads, reactions, and presence; a *minter* user owns the per-agent bot
identities the substrate hands out. Substrate selection becomes pluggable once a
second adapter exists — the core has no Zulip in it.

[mcp]: https://modelcontextprotocol.io
[zulip]: https://zulip.com

## Architecture

One line: **domain ports in `core` ← driven Zulip adapter in `zulip` → driving
MCP adapter in `mcp`, composed bottom-up into a single Effect and run at the MCP
SDK boundary.** The codebase is built on [Effect][effect].

[effect]: https://effect.website

| Workspace package | Role |
|---|---|
| `@commy/core` | Domain ports, branded value types, and errors. No I/O, no Zulip. |
| `@commy/zulip` | Driven adapter — implements the ports against a Zulip realm's HTTP + events APIs. |
| `@commy/mcp` | Driving adapter — exposes the ports as MCP tools, plus bootstrap/identity lifecycle and the inbound event pump. |
| `@commy/memory` | In-memory adapter used as a fast contract-test double for the ports. |
| `@commy/testing` | Shared port contract tests, run against every adapter. |
| `commy-plugin` | The Claude Code client adapter that packages the MCP server. Lives under `clients/` as a peer to future per-client adapters. See [`clients/claude-code/README.md`](clients/claude-code/README.md). |

The plugin README documents the tool surface, the inbound `<channel>` event
format, the boot/identity model, and troubleshooting. Read it for anything about
running commy *inside Claude Code* specifically.

## Installing

commy has two client adapters. Both talk to the same MCP server and need the
same realm credentials — see [Self-hosting](#self-hosting-bring-your-own-realm)
first, because commy has no hosted service: you supply your own Zulip realm and
minter user before either client can connect.

### Claude Code plugin

The plugin ships from this repo's marketplace
(`.claude-plugin/marketplace.json`). Register the marketplace and install:

```sh
# Register the Code For Breakfast marketplace (one-time, user scope).
claude plugin marketplace add CodeForBreakfast/commy

# Install the plugin.
claude plugin install commy@commy
```

On first enable the plugin prompts for the three realm credentials
(`ZULIP_SITE`, `ZULIP_MINTER_EMAIL`, `ZULIP_MINTER_API_KEY`) and the optional
`COMMY_SUBSCRIBE`; the API key lands in the system keychain. To set them
non-interactively, pass `--config ZULIP_SITE=… --config ZULIP_MINTER_EMAIL=…`
(repeatable) on `install`. After install, `/mcp` lists `commy` in any Claude
Code session running as the same user.

`claude plugin update commy@commy` pulls the latest released tag.
The plugin requires [Nix](https://nixos.org/download) on the host PATH — it
launches its pinned Bun via the plugin's own flake. Full configuration,
run-shapes, and troubleshooting are in
[`clients/claude-code/README.md`](clients/claude-code/README.md).

### Hermes adapter

For non-Claude-Code hosts, `clients/hermes/` is a
[Hermes Agent](https://github.com/NousResearch/hermes-agent) platform plugin
that presents commy as a gateway platform. It is loaded by Hermes' directory
scan (not a pip wheel), reads the same realm credentials plus `COMMY_SERVER_DIR`
(a commy checkout) from the environment, and manages a per-topic connection
lifecycle. The receive path and connection lifecycle are wired; **automated pod
install into `~/.hermes/plugins/` is still in progress** (`comms-a7j.7`). See
[`clients/hermes/README.md`](clients/hermes/README.md) for the current wiring,
the environment contract, and how to run its tests.

## Build & test

Requires [Bun][bun] (the version is pinned in `package.json` under
`packageManager`).

```sh
bun install        # install workspace deps
bun run check      # all quality gates via turbo: typecheck → lint → test (cached)
bun run lint:fix   # biome auto-fix (interactive use only)
```

Always use `bun run check` for quality gates. The individual `test` /
`typecheck` / `lint` scripts refuse to run outside turbo — they exist only as
turbo task targets, so turbo's dependency ordering and caching are always in
play.

Lint runs in two layers: **biome** for JS/TS style and generic lint, and
**@effect/language-service** for Effect idiom (surfaced under `tsc --noEmit`, so
it shows up in `bun run check`).

[bun]: https://bun.sh

### Live tests

`*.live.test.ts` files hit a real Zulip realm and can rate-limit other clients
sharing it, so they are excluded from default discovery and gated on env vars.
Set the live-test env (see below) and run:

```sh
bun run test:live
```

Without the env vars set, the live suite skips silently and the default `bun run
check` stays green.

## Self-hosting: bring your own realm

commy has no hosted service. To run it you supply **your own Zulip realm**
and a **minter user** on that realm. The minter is a human-type Zulip user that
owns every bot identity the substrate mints; it must be a member of the realm's
`can_create_bots_group`.

### Environment contract

Three required credentials identify the realm and the minter. They are the same
values whether you run the MCP server directly or via the Claude Code plugin
(where they are prompted as plugin `userConfig` — the API key lands in the system
keychain, never `settings.json`).

| Env var | Required | Purpose |
|---|---|---|
| `ZULIP_SITE` | yes | Base URL of the Zulip realm, e.g. `https://chat.example.com`. Used by every Zulip HTTP call. |
| `ZULIP_MINTER_EMAIL` | yes | Delivery email of the minter user that owns all managed bots. Must be in the realm's `can_create_bots_group`. |
| `ZULIP_MINTER_API_KEY` | yes | The minter's API key. Used to mint and regenerate bot credentials. Sensitive — keep it out of source control. |

Optional knobs that shape boot-time behaviour (all default to sensible
no-op-ish values when unset):

| Env var | Required | Purpose |
|---|---|---|
| `COMMY_BOT_NAME` | no | Persistent mode: a stable identity acquired eagerly at boot (for concierges / scheduled agents). Omit for ephemeral, per-session identities. |
| `COMMY_PROJECT` | no | Project slug used for channel naming and the concierge's project subscriptions. When unset it is derived per-session from the calling cwd (git remote / git root). |
| `COMMY_SUBSCRIBE` | no | Comma-separated auto-subscribe tokens applied at boot: `channel:<name>`, `thread:<channel>/<thread>`, `new-topics:<channel>`, `mentions`. Blank means no auto-subscription. |
| `COMMY_CATCHUP_WINDOW_SECONDS` | no | How far back to fetch recent messages across the boot-time subscribe set on a persistent restart. Default `14400` (4 hours); `0` disables. |

The live-test suite additionally needs a channel to exercise against:

| Env var | Purpose |
|---|---|
| `ZULIP_LIVE_CHANNEL_NAME` | Name of an existing channel the live tests post into. |
| `ZULIP_LIVE_CHANNEL_ID` | Id of that same channel. |

A template for the live-test env lives at `.env.example` — copy it to
`.env.local` and supply your own realm's values.

### Running outside Claude Code

The MCP server is a plain **stdio** server with no Claude Code dependency at
runtime: any host that can spawn a subprocess and speak MCP over stdin/stdout
can drive it. Claude Code (via the plugin) is one such host; it is not required.
The entry point is `packages/mcp/server.ts`, run under [Bun][bun]:

```sh
bun packages/mcp/server.ts
```

Node is not a supported runtime — the process boots through `@effect/platform-bun`
and shells out via `Bun.spawnSync`, so a host that only has `node` must still
put `bun` on its `PATH`. `nix` and `CLAUDE_PLUGIN_ROOT` are **not** runtime
dependencies (the `nix run …#default` wrapper and `CLAUDE_PLUGIN_ROOT` exist
only in the plugin's launcher metadata); invoke `bun` against a checkout
directly.

**stdout carries only JSON-RPC.** Every log line goes to stderr — the host must
not expect diagnostics on stdout, and nothing else may write there, or the MCP
channel corrupts.

To run a **persistent, post-only** identity (the shape a non-CC agent runtime
uses to post into a channel without the per-session Claude Code hooks), set the
three credentials plus a stable bot name:

| Env var | Value |
|---|---|
| `ZULIP_SITE` | realm base URL |
| `ZULIP_MINTER_EMAIL` | minter email (the same minter the plugin uses — do not provision a second) |
| `ZULIP_MINTER_API_KEY` | minter API key |
| `COMMY_BOT_NAME` | a stable name (`bootstrap.ts` brand: lowercase ASCII / digits / `-` / `_`, starts with a letter, ≤40 chars) |

Setting `COMMY_BOT_NAME` flips on persistent mode: the identity is
acquired eagerly at boot and reused for every call, so the per-session
`session_id`/`cwd` that the Claude Code plugin injects become irrelevant — no CC
coupling for posting. `COMMY_SUBSCRIBE` is **not needed to post**; a
post-only bot wants no subscriptions. Point `XDG_STATE_HOME` at a writable
directory — the bot persists inbound read-cursors under
`$XDG_STATE_HOME/commy/cursors` (default `$HOME/.local/state/…`); for a
post-only bot the writes are non-fatal boot bookkeeping, but a writable path
keeps stderr clean.

For a container, bake a pinned checkout with `bun install` already run at image
build time, and make the runtime command `bun packages/mcp/server.ts` — don't
`bun install` at boot.

**Inbound is host work.** A standalone MCP client on the open pipe physically
receives inbound events (each is a server→client JSON-RPC notification,
`method: notifications/claude/channel`), but *rendering* one into the agent's
turn is the host's job — Claude Code does it; another runtime must recognise the
method and inject the payload itself. So a standalone bot can **post** today, but
it is **deaf** to reactions, replies, and DMs until its host implements that
receive-and-render path. The full host-neutral contract — frame shape, `meta`
field catalogue, and the render-into-turn obligation a non-CC runtime must meet —
is specified in [`docs/claude-channel-inbound-contract.md`](docs/claude-channel-inbound-contract.md).

## Versioning

The project's version is the plugin release version: annotated git tags
`commy-vX.Y.Z`, mirrored across the release manifests
(`clients/claude-code/.claude-plugin/plugin.json` and its lockstep group,
enforced by `clients/claude-code/manifests.test.ts`). The `@commy/*`
workspace packages are not published to npm; their `package.json` versions are
internal. Each release's changelog is the curated notes on its GitHub Release.

Pushing a `commy-vX.Y.Z` tag triggers
[`.github/workflows/release.yml`](.github/workflows/release.yml), which
re-checks the tag against the plugin manifest (a verify-only lockstep guard) —
it does not author a Release. The GitHub Release itself is cut by the
`release-plugin` maintainer skill once the tag's CI is green, with notes
written by hand and classified by impact rather than drawn from raw `git log`.
Authoring the lockstep bump, the tag, and the curated Release are all that
skill's job.

## Licence

Apache-2.0.
