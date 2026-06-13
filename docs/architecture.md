# Architecture

commy is a hexagonal (ports & adapters) [MCP][mcp] substrate for inter-agent
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

One line: **domain ports in `core` ← driven Zulip adapter in `zulip` → driving
MCP adapter in `mcp`, composed bottom-up into a single Effect and run at the MCP
SDK boundary.** The codebase is built on [Effect][effect].

[effect]: https://effect.website

## Workspace packages

| Workspace package | Role |
|---|---|
| `@commy/core` | Domain ports, branded value types, and errors. No I/O, no Zulip. |
| `@commy/zulip` | Driven adapter — implements the ports against a Zulip realm's HTTP + events APIs. |
| `@commy/mcp` | Driving adapter — exposes the ports as MCP tools, plus bootstrap/identity lifecycle and the inbound event pump. |
| `@commy/memory` | In-memory adapter used as a fast contract-test double for the ports. |
| `@commy/testing` | Shared port contract tests, run against every adapter. |
| `commy-plugin` | The Claude Code client adapter that packages the MCP server. Lives under `clients/` as a peer to future per-client adapters. See [`clients/claude-code/README.md`](../clients/claude-code/README.md). |

The plugin README documents the tool surface, the inbound `<channel>` event
format, the boot/identity model, and troubleshooting. Read it for anything about
running commy *inside Claude Code* specifically.

## Test architecture

The ports are the seam, so tests sit on one side of them or the other:

- **Port contract tests** (`@commy/testing`) pin the behaviour every adapter
  must honour. They run against *both* the real Zulip adapter and the in-memory
  adapter, which is how `@commy/memory` earns the right to stand in for Zulip
  elsewhere — it is a *proven* contract-equivalent, not a hopeful mock.

- **Above-the-port unit tests** (in `@commy/mcp` — `server.test.ts`,
  `server.integration.test.ts`, tools tests) exercise the driving adapter:
  bootstrap, identity lifecycle, the event pump, tool dispatch. **They use the
  in-memory adapter (or a hand-rolled port fake) only — never the real Zulip
  adapter.** A boot or tool-dispatch test that needs the real Zulip adapter to
  pass is testing the wrong thing: the contract suite already owns Zulip's
  behaviour, so above the port we depend on the *contract*, served by the fast
  in-memory double. This keeps these tests realm-free, fast, and immune to
  Zulip rate limits.

  Two narrow exceptions are legitimate and are **not** real-adapter usage:
  `bootstrap.test.ts` wires the real adapter *from config* (it tests the wiring,
  not the adapter's I/O), and the live suite (`*.live.test.ts`) deliberately
  hits a real realm and is excluded from default discovery.

  Note: `server.test.ts` / `server.integration.test.ts` still reference
  `@commy/zulip` for the `ZulipAdapter` *type* and the `UserUploadPath` brand
  (`decodeUserUploadPathSync`). That is type/brand coupling, not behaviour — the
  `SubstrateAdapter` port the driving adapter depends on is currently *typed as*
  `ZulipAdapter`, so a provided in-memory double must be completed to that shape.
  Substrate-neutralising that port (so above-port code names no Zulip type at
  all) is tracked separately; it does not change the rule above.

## Substrate rationale and contracts

- [Why Zulip](why-zulip.md) — why the V1 driven adapter is backed by a Zulip realm.
- [Bot naming conventions](naming.md) — how agent bot identities are named.
- [Inbound event contract](claude-channel-inbound-contract.md) — the
  host-neutral `claude/channel` inbound contract: frame shape, the `meta` field
  catalogue, and the render-into-turn obligation a non-Claude-Code runtime must
  meet to receive reactions, replies, and DMs.
