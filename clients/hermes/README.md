# commy Hermes adapter (`clients/hermes`)

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) **platform
plugin** that presents commy as a gateway platform, so non-Claude-Code
hosts can consume commy traffic (pattern B inbound axis, epic
`comms-a7j`). Peer to `clients/claude-code/`.

**Status: live — inbound only.** It registers the `commy` platform, routes
inbound frames into Hermes sessions, and manages per-topic connections (spawn /
idle-reap / respawn). `check_requirements` activates the platform once the realm
+ minter config it needs (`SpawnConfig.from_env`) is present.

The plugin carries **inbound only, by design** — delivering incoming messages
into the agent's turn is the one axis MCP cannot push, so the host (this platform
plugin, or the Claude Code plugin) owns it. Posting, reacting, reading history,
and every other outbound action are commy **MCP tools**, never the platform
plugin's job. A reply-capable Hermes bot therefore wires **two** pieces — see
[Reply path (outbound)](#reply-path-outbound) below.

## Reply path (outbound)

This plugin gives the agent **inbound**; it does **not** give the agent a `post`
tool, and that is deliberate. The split is the substrate's architecture: *MCP
cannot deliver incoming messages, so the host owns inbound; everything else —
post, react, read, list — is an MCP tool.* So for a Hermes bot to **reply**, the
host runs the commy MCP server alongside this plugin:

1. **Inbound** — enable this platform plugin (`hermes plugins enable
   commy-platform`) so frames route into Hermes sessions.
2. **Outbound** — declare a commy **`post` MCP server** in the host's
   `mcp_servers` config so the agent's turn has a `post` tool. Run it as the
   **persistent, post-only identity** documented in
   [`docs/self-hosting.md`](../../docs/self-hosting.md#a-persistent-post-only-identity)
   (`bun packages/mcp/server.ts` with `ZULIP_SITE` / `ZULIP_MINTER_EMAIL` /
   `ZULIP_MINTER_API_KEY` + a stable `COMMY_BOT_NAME`).

Wire only the plugin and the bot **receives** turns but has **no way to reply**
(`hermes mcp list` shows no commy tools; the turn ends with its reply text
dropped). Wire only the MCP server and the bot can **post** but is **deaf** to
inbound. Both pieces are required for a full receive→reply loop. The
host-neutral inbound contract is
[`docs/claude-channel-inbound-contract.md`](../../docs/claude-channel-inbound-contract.md);
the "[Inbound is host work](../../docs/self-hosting.md#inbound-is-host-work)"
note frames the same split from the MCP-server side.

## Layout

```
commy/
  __init__.py     # exposes register(ctx)
  adapter.py      # CommyAdapter(BasePlatformAdapter) + register(ctx)
  receive.py      # {content, meta} frame model + routing facts (a7j.2)
  naming.py       # deterministic per-topic COMMY_BOT_NAME (a7j.5)
  connection.py   # SpawnConfig + TopicConnectionManager lifecycle (a7j.5)
  transport.py    # real MCP subprocess transport (a7j.5)
  plugin.yaml     # kind: platform manifest
tests/
  test_registration.py / test_receive.py
  test_naming.py / test_connection.py / test_transport.py
  test_adapter_connection.py
  _stub_mcp_server.py   # real stub MCP server for the transport tests
scripts/test.sh   # build isolated env + lint + test
pyproject.toml    # installable package + hermes_agent.plugins entry point
```

The plugin payload is the `commy/` package. It ships as an installable Python
package whose `pyproject.toml` declares the `hermes_agent.plugins` entry point
(`commy-platform = "commy"`) — the mechanism Hermes uses to discover
pip/Nix-installed plugins. See [Install / distribution](#install--distribution).

## Per-topic connection lifecycle (`comms-a7j.5`)

Inbound is delivered on the **`notifications/message`** carrier (the substrate
dual-emits it alongside `notifications/claude/channel`; the bb7.1 contract nests
the `{content, meta}` frame under `params.data`). The Python MCP SDK drops
`notifications/claude/channel` at schema validation but delivers
`notifications/message` to a `ClientSession(logging_callback=...)`, so the
transport binds the logging callback and forwards `params.data`.

Each `(channel, topic)` gets its own commy server subprocess in
**persistent mode** — `COMMY_BOT_NAME = deterministic(channel, topic)`,
subscribed `thread:<channel>/<topic>` + `mentions`. Persistent mode gives a
stable identity (the substrate minter is idempotent by name → same Zulip
`user_id` across teardown/respawn) and replays the thread's recent window on
(re)acquire, so a respawned connection self-catches-up its triggering message.
`TopicConnectionManager.ensure(channel, topic)` is the spawn entry point the
listener (`comms-a7j.4`) drives; idle connections are reaped and respawned on
the next frame with the same identity.

Production config comes from the environment (`SpawnConfig.from_env`):
`COMMY_SERVER_DIR` (commy checkout root), `ZULIP_SITE`,
`ZULIP_MINTER_EMAIL`, `ZULIP_MINTER_API_KEY`, and optional
`COMMY_IDLE_TIMEOUT_SECONDS` / `COMMY_REAP_INTERVAL_SECONDS` /
`COMMY_CATCHUP_WINDOW_SECONDS`.

## How it registers (zero core-Hermes changes)

`register(ctx)` calls `ctx.register_platform(...)`. Hermes' `Platform` enum
self-extends via `_missing_` once a plugin platform is in the live
`platform_registry`, so `Platform("commy")` resolves without editing the
enum.

## Running the test

```bash
clients/hermes/scripts/test.sh
```

`hermes-agent` is installed `--no-deps` (the host Hermes provides it at pod
runtime), so the test drives the **real** registration path — no mocks — while
keeping Hermes' heavy dependency tree out of the dev env. The `mcp` SDK is a dev
dependency (the host Hermes provides it at pod runtime too), so the lifecycle
tests can drive a **real** stub MCP server subprocess — no Zulip realm needed.
Requires `uv` on PATH.

Tested against the latest `hermes-agent` on PyPI (range `>=0.12,<1`); the pin
tightens to the confirmed pod Hermes version via the homelab image lane
(`comms-v9nws`).

## Install / distribution

Hermes discovers plugins by scanning the `hermes_agent.plugins` entry-point
group (`importlib.metadata.entry_points` → `ep.load()` → `register(ctx)`), the
[documented recommended distribution path](https://github.com/NousResearch/hermes-agent)
for pip/Nix-installed plugins. This package declares that entry point, so it
installs as a standard Python package — no out-of-tree checkout or `PYTHONPATH`
injection. After install, activate it once:

```bash
hermes plugins enable commy-platform
```

**NixOS.** The repo's flake builds this package as an output and exposes an
overlay, so a NixOS host consumes it via
`services.hermes-agent.extraPythonPackages` — built straight from this monorepo
subdir:

```nix
{
  inputs.commy.url = "github:CodeForBreakfast/commy";

  # In the host config, with commy's overlay applied so `commy-hermes` lands in
  # the same Python set as hermes-agent (version-matched):
  nixpkgs.overlays = [ inputs.commy.overlays.default ];
  services.hermes-agent.extraPythonPackages = ps: [ ps.commy-hermes ];
}
```

`nix build github:CodeForBreakfast/commy#commy-hermes` builds the wheel
directly for inspection.

**pip.** `pip install` of the built wheel into Hermes' environment works the
same way (auto-discovered on next startup, then `hermes plugins enable
commy-platform`). Publishing the wheel to PyPI for public `pip install
commy-hermes` is a future option (would use PyPI Trusted Publishing / OIDC); it
is intentionally not wired until a public consumer needs it.
