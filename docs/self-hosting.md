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
| `COMMY_PROJECT` | no | Project slug used for channel naming and a persistent agent's project subscriptions. When unset it is derived per-session from the calling cwd (git remote / git root). |
| `COMMY_SUBSCRIBE` | no | Comma-separated auto-subscribe tokens applied at boot: `channel:<name>`, `thread:<channel>/<thread>`, `new-topics:<channel>`, `mentions`. Blank means no auto-subscription. |
| `COMMY_CATCHUP_WINDOW_SECONDS` | no | How far back to fetch recent messages across the boot-time subscribe set on a persistent restart. Default `14400` (4 hours); `0` disables. |
| `COMMY_QUEUE_IDLE_TIMEOUT_SECS` | no | How many seconds an ephemeral session's events queue survives without a poll before Zulip garbage-collects it, sent as `idle_queue_timeout` on `/register`. Default `86400` (24 hours); clamped to Zulip's 7-day `MAX_QUEUE_TIMEOUT_SECS` ceiling (`604800`). A non-positive or non-integer value fails boot with a config error. |
| `COMMY_DOWNLOAD_DIR` | no | Base directory for `download_file` attachments. When set, each download's fresh temp subdirectory is created under it so files land somewhere an allowlisted agent can `Read`; when unset, downloads go to `$TMPDIR`. Must be an existing directory — a non-directory value fails boot with a config error. |
| `COMMY_NPM_MIN_RELEASE_AGE` | no | Operator override for npm's `min-release-age` supply-chain soak, scoped to commy's own `npx` launch. Set to `0` to run a freshly-published release that your global soak would otherwise block with `ENOVERSIONS`. Leave **unset** to fully respect your own npm setting. See [Running outside Claude Code](#running-outside-claude-code). |

The live-test suite additionally needs a channel to exercise against:

| Env var | Purpose |
|---|---|
| `ZULIP_LIVE_CHANNEL_NAME` | Name of an existing channel the live tests post into. |
| `ZULIP_LIVE_CHANNEL_ID` | Id of that same channel. |

A template for the live-test env ships in the repo root — copy it and supply
your own realm's values (it references a secret manager for the actual values;
copy the variable shape, supply your own).

## Realm settings that shape commy behaviour

Some of your realm's own Zulip settings — operator knobs on the realm, not
commy env vars — change how the substrate behaves. The one that matters today
is **`message_content_edit_limit_seconds`**: how long after posting an author
may still edit a message's content.

It governs edit-in-place, which is how an emitter keeps a long-lived decision
anchor current. Once the window passes, `edit_message` is refused (commy
surfaces this as a typed `MessageEditRefused` with reason `window-expired`) and
the only recovery is to re-post. A **long** window makes edit-in-place viable
for anchors that live for hours; a **short** one — the stock default is minutes
— forces re-posts. commy cannot widen this from code; set it on the realm to
match how long your anchors need to stay editable.

There is a second edit wall no realm setting lifts: Zulip only lets the
**original sender** edit content, and commy's ephemeral identities are
per-session. So a message that outlives its authoring session is uneditable at
any age — `edit_message` refuses it with reason `not-original-sender`, and the
emitter must re-post. Widening the time limit does not change this; a
persistent `COMMY_BOT_NAME` identity (above) is what keeps authorship across
sessions.

## Running outside Claude Code

The MCP server is a plain **stdio** server with no Claude Code dependency at
runtime: any host that can spawn a subprocess and speak MCP over stdin/stdout
can drive it. Claude Code (via the plugin) is one such host; it is not required.
The runtime is **node** — the server is published as a self-contained bundle on
npm, so the entry point for an operator is:

```sh
npx -y @codeforbreakfast/commy-mcp
```

`npx` pulls the published `@codeforbreakfast/commy-mcp` package — a single
`server.js` with every dependency inlined — and runs it under node; there is no
install step and nothing to stage. (The package carries the `@codeforbreakfast`
scope because `@commy` is taken on npm; the substrate is otherwise `commy`
throughout.) If you run npm's `min-release-age` supply-chain soak — which holds
back a freshly-published release so a compromised one can be caught before you
auto-pull it — a commy release younger than your window fails to resolve
(`ENOVERSIONS`) until it ages in. That soak guards you against the publisher's
own not-yet-vetted code, so waiving it is a decision to trust the commy
publisher and run a new release immediately: set `COMMY_NPM_MIN_RELEASE_AGE=0`
(which the launcher threads as `npm_config_min_release_age`). Because the bundle
has **no dependencies**, that waiver is scoped to exactly this one package — no
transitive dependency tree rides along — and an unset value never weakens your
own setting. Working from a source checkout instead, the dev toolchain runs the
TypeScript entry point directly under [Bun][bun] with `bun packages/mcp/server.ts`
— bun is the development runtime, never a consumer prerequisite. `nix` is **not**
a runtime dependency either; the flake's dev shell stays supported for those who
want it, but is never required to run the server. The Claude Code plugin launches
the same published bundle via `npx`, with `cwd` set to `${CLAUDE_PLUGIN_ROOT}`.

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

For a container, pin the published package version at image build time — e.g.
`npm install -g @codeforbreakfast/commy-mcp@<version>` — and make the runtime
command `commy-mcp` (or `npx @codeforbreakfast/commy-mcp`), so boot resolves the
already-present bundle and never reaches the network.

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
