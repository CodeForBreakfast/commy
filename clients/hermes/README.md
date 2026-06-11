# commy Hermes adapter (`clients/hermes`)

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) **platform
plugin** that presents commy as a gateway platform, so non-Claude-Code
hosts can consume commy traffic (pattern B inbound axis, epic
`comms-a7j`). Peer to `clients/claude-code/`.

**Status: receive path + per-topic connection lifecycle wired
(`comms-a7j.2` + `comms-a7j.5`).** It registers the `commy` platform,
routes inbound frames into Hermes sessions, and manages per-topic connections
(spawn / idle-reap / respawn). It stays **dormant** (`check_requirements` is
`False`) until the boot-time listener (`comms-a7j.4`) and pod install
(`comms-a7j.7`) land.

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
```

The plugin payload is the `commy/` directory. Hermes loads it by
directory scan (like the bundled platform plugins), not as a pip wheel — so
this is a non-package uv project (tooling + tests only).

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

## Pod install

Wiring the install into `~/.hermes/plugins/` is tracked separately
(`comms-a7j.7`).
```
