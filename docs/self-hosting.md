# Self-hosting: bring your own realm

commy has no hosted service. To run it you supply **your own [Zulip][zulip]
realm** and a **minter user** on that realm. The minter is a human-type Zulip
user that owns every bot identity the substrate mints; it must be a member of the
realm's `can_create_bots_group`.

[zulip]: https://zulip.com
[bun]: https://bun.sh

## Environment contract

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

A template for the live-test env ships in the repo root — copy it and supply
your own realm's values (it references a secret manager for the actual values;
copy the variable shape, supply your own).

## Running outside Claude Code

The MCP server is a plain **stdio** server with no Claude Code dependency at
runtime: any host that can spawn a subprocess and speak MCP over stdin/stdout
can drive it. Claude Code (via the plugin) is one such host; it is not required.
The entry point is `packages/mcp/server.ts`, run under [Bun][bun]:

```sh
bun packages/mcp/server.ts
```

Node is not a supported runtime — the process boots through `@effect/platform-bun`
and shells out via `Bun.spawnSync`, so a host that only has `node` must still
put `bun` on its `PATH`. `nix` is **not** a runtime dependency. The Claude Code
plugin launches through `clients/claude-code/launch.sh` — a POSIX-`sh` wrapper
that stages workspace deps once, then `exec`s `bun` — and `CLAUDE_PLUGIN_ROOT`
is set by Claude Code for that wrapper alone; running standalone you invoke
`bun` against a checkout directly, with no wrapper. (Nix stays supported for
those who want it — the flake's dev shell is intact — it's just never required
to run the server.)

**stdout carries only JSON-RPC.** Every log line goes to stderr — the host must
not expect diagnostics on stdout, and nothing else may write there, or the MCP
channel corrupts.

## A persistent, post-only identity

To run a **persistent, post-only** identity (the shape a non-CC agent runtime
uses to post into a channel without the per-session Claude Code hooks), set the
three credentials plus a stable bot name:

| Env var | Value |
|---|---|
| `ZULIP_SITE` | realm base URL |
| `ZULIP_MINTER_EMAIL` | minter email (the same minter the plugin uses — do not provision a second) |
| `ZULIP_MINTER_API_KEY` | minter API key |
| `COMMY_BOT_NAME` | a stable name (`bootstrap.ts` brand: lowercase ASCII / digits / `-` / `_`, starts with a letter, ≤40 chars) |

Setting `COMMY_BOT_NAME` flips on persistent mode: the identity is acquired
eagerly at boot and reused for every call, so the per-session `session_id`/`cwd`
that the Claude Code plugin injects become irrelevant — no CC coupling for
posting. `COMMY_SUBSCRIBE` is **not needed to post**; a post-only bot wants no
subscriptions. Point `XDG_STATE_HOME` at a writable directory — the bot persists
inbound read-cursors under `$XDG_STATE_HOME/commy/cursors` (default
`$HOME/.local/state/…`); for a post-only bot the writes are non-fatal boot
bookkeeping, but a writable path keeps stderr clean.

For a container, bake a pinned checkout with `bun install` already run at image
build time, and make the runtime command `bun packages/mcp/server.ts` — don't
`bun install` at boot.

## Inbound is host work

A standalone MCP client on the open pipe physically receives inbound events (each
is a server→client JSON-RPC notification, `method: notifications/claude/channel`),
but *rendering* one into the agent's turn is the host's job — Claude Code does
it; another runtime must recognise the method and inject the payload itself. So a
standalone bot can **post** today, but it is **deaf** to reactions, replies, and
DMs until its host implements that receive-and-render path. The full host-neutral
contract — frame shape, `meta` field catalogue, and the render-into-turn
obligation a non-CC runtime must meet — is specified in
[`claude-channel-inbound-contract.md`](claude-channel-inbound-contract.md).
