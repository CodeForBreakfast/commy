# The `claude/channel` inbound contract

This is the host-neutral contract by which commy (and other emitters that
share the same host capability) deliver an inbound event to an agent runtime,
and the obligation that runtime takes on to surface it. It exists so a non-Claude-Code
runtime — e.g. Hermes — can implement the receive path against a stable
specification rather than reverse-engineering Claude Code's behaviour.

## Scope

This contract governs the **inbound** axis only: an emitter pushing an event
*to* a connected agent. The **outbound** axis (an agent calling the `post` /
`edit_message` / `react` tools to put a message *onto* the substrate) is ordinary
MCP tool invocation and needs no contract beyond each tool's own schema — a
tool-call request self-addresses via its JSON-RPC request id and its response is
auto-correlated by the SDK. Inbound is the asymmetric half: a notification has no
correlation id, so a runtime that does not explicitly handle it silently drops
the frame. That asymmetry is the entire reason this document exists.

## Transport: a JSON-RPC notification

An inbound event is delivered as a single server→client JSON-RPC **notification**
over the open MCP pipe:

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": { "content": "<string>", "meta": { "<key>": "<string>", ... } }
}
```

Properties of the transport that a consumer must rely on, and must not rely on:

- **Delivery is ungated.** The emitter sends this notification unconditionally —
  there is no capability negotiation or handshake that turns it on. A connected
  client receives the frame the moment it is sent. (In the MCP SDK,
  `assertNotificationCapability` has no case for this method, so it falls through
  and sends; commy emits it at `packages/mcp/event-pump.ts` `channelNotifier`.)
- **The client fires IFF it registered a handler.** The notification arrives at
  the client regardless, but the runtime only *acts* on it if it registered a
  handler for the method string (or a fallback notification handler). With no
  handler the frame is received and discarded. Wiring that handler is the whole
  of the consumer's receive-path obligation — see below.
- **The method string `notifications/claude/channel` is the stable contract
  identifier.** It is shared across emitters (commy and the Discord plugin
  both emit it; any future emitter against the same host capability does too). A
  consumer binds to this exact string. The string is Claude-Code-*named* for
  historical reasons but is **not** Claude-Code-*coupled*: binding a handler to a
  constant string is not a dependency on Claude Code. Whether to *additionally*
  emit a neutral method alias is a recorded decision — see "The method name" below.

## Frame shape: `{ content, meta }`

The notification `params` is the host-neutral payload. Its TypeScript source of
truth is `ChannelEventPayload` in `packages/mcp/events.ts`:

```ts
interface ChannelEventPayload {
  readonly content: string
  readonly meta: Record<string, string>
}
```

- **`content`** — the human-readable body of the event. For a posted message it
  is the message body verbatim; for a reaction it is a synthesised line
  (`[reaction add] :tada:`); for an error it is the short error message.
- **`meta`** — a flat string→string map of routing and provenance attributes. All
  values are strings (numbers and enums are stringified at the emit boundary).
  Keys present depend on the event kind (catalogue below). `meta` values are
  sanitised by the emitter: the characters `[`, `]`, `\r`, `\n`, and `;` are
  replaced with `_` so the host can render them as flat attributes without
  injection or delimiter collision. A consumer should treat `meta` values as
  already-safe-for-attribute but otherwise opaque.

Both `content` and `meta` are fully host-neutral on the wire — there is nothing
Claude-Code-specific in the payload itself.

### `meta` field catalogue

The emitter builds `meta` per event kind. A key is **omitted entirely** when its
source value is absent (e.g. `thread` for a top-level post, `replayed` for a live
message) — consumers must treat every field as optional and key off presence.

The catalogue below describes the **full machine frame** carried on
`notifications/message`. The `notifications/claude/channel` render projection
carries the **same frame minus the numeric identity-id keys** — `sender_id` and
reaction `by_id`, marked _machine-carrier-only_ below. That divergence is
deliberate (display-vs-machine), documented under "The method name" → "carrier
divergence" further down; the short version is that the CC host renders its
entire `meta` into the agent's turn, where a bare numeric identity id is noise
and collides visually with the equally-numeric `message_id`, so the display
carrier omits it while the machine carrier keeps it for session keying.

**Message events** (`message-posted`, `mention-received`) — `packages/mcp/events.ts` `formatMessage`:

| key | meaning |
|---|---|
| `channel_id` | substrate channel id |
| `channel_name` | channel name (the project slug, or `general`) |
| `thread` | topic name within the channel; **omitted** for a top-level post |
| `message_id` | substrate message id — the dedup key |
| `sender_id` | identity id of the sender — _machine-carrier-only_ (omitted from the claude/channel render projection) |
| `sender_name` | display name of the sender |
| `sender_kind` | `agent` or `human` |
| `ts` | message timestamp (stringified epoch) |
| `mentions` | `;`-separated display **names** mentioned (each name is sanitised, so `;` is an unambiguous delimiter); **omitted** if none |
| `mentioned` | `"true"` when the receiving identity is itself in the mention list — the self-address flag; **omitted** otherwise |
| `replayed` | `"true"` when this frame is a gap-replay backfill (substrate queue-expiry recovery), not a live post; **omitted** for live events |

**Reaction events** (`reaction-added`, `reaction-removed`) — `formatReaction`:

| key | meaning |
|---|---|
| `target_message_id` | id of the message reacted to |
| `target_channel_name` | channel of the target message |
| `target_thread` | topic of the target message; **omitted** if top-level |
| `reaction_emoji` | the emoji |
| `reaction_action` | `add` or `remove` |
| `by_id` | identity id of the reactor — _machine-carrier-only_ (omitted from the claude/channel render projection) |
| `by_name` / `by_kind` | display name and `agent`/`human` of the reactor |
| `ts` | observation timestamp (stringified epoch) |

**Error events** — `formatError`:

| key | meaning |
|---|---|
| `error_kind` | short error category (e.g. `event-pump`); `unknown` if unset |

`content` carries the error message.

The `(channel_name, thread)` pair is the routing key: it is exactly what a
multi-context consumer keys a session on. `message_id` is the dedup key (it covers
both the catch-up/live-window overlap the substrate flags, and any
listener-vs-owner double delivery). Both routing and dedup keys are present on
**both** carriers — the divergence is confined to the numeric identity ids.
Self-echo is **not** a consumer concern: the substrate emitter drops the bot's
own events before emit (see checklist item 4), so no self-identity key rides on
the frame.

## The host's obligation: render into the agent's turn

Receiving the frame is necessary but not sufficient. The contract's second half is
that **the host must render the payload into something the agent perceives as part
of its turn.** Claude Code does this by wrapping the frame as a single block:

```
<channel source="commy" channel_name="..." thread="..." message_id="..." sender_name="..." ...>
the content string
</channel>
```

- The wrapping tag (`<channel …>…</channel>`) and the `source` attribution label
  are the **host's** responsibility, not the emitter's. The emitter owns
  `content` and `meta`; the host owns the presentation. `source` is the host's
  own per-emitter label (`commy` here, `discord` for the Discord plugin) —
  it is not carried in the frame.
- A non-CC runtime is **not** required to reproduce this exact XML. It is required
  to surface the event to its agent equivalently: the `content` as the body and
  the `meta` attributes as accessible provenance, injected into the agent's
  context/turn so the agent can read and act on it. How (XML block, structured
  message event, platform-native message object) is the runtime's choice. Hermes,
  for instance, turns each frame into a `MessageEvent` and feeds it through its
  existing session/turn pipeline.

This is why a standalone MCP client that merely holds the pipe open is **deaf**:
it physically receives the notification but, lacking both the handler and the
render step, never turns it into agent-visible input.

## A consumer's receive-path checklist

To implement the receive path against this contract a runtime must:

1. **Register a notification handler** for `notifications/claude/channel` (do not
   leave it as a `case _: pass` / unhandled-method drop).
2. **Route** by `meta.channel_name` + `meta.thread` to the right agent context /
   session.
3. **Dedup** by `meta.message_id`.
4. **Self-echo needs no consumer filter.** The substrate emitter drops the bot's
   own posts and reactions before they are emitted (the `isSelfEvent` guard in
   `packages/mcp/event-pump.ts`), so every frame a consumer receives was authored
   by someone else. No self-identity key rides on the frame for the consumer to
   compare against; a consumer must **not** assume one exists.
5. **Render** `content` + `meta` into the agent's turn (the host obligation above).

Items 2, 3 and 5 are consumer policy; item 1 plus the frame/transport shape are this
contract. Subscription mechanics (which frames an identity receives at all) are a
substrate concern documented separately — an identity receives only frames
matching its own subscriptions and mentions of itself.

## The method name — decision: dual-emit `notifications/message` alongside `notifications/claude/channel`

The contract emits on **two** methods, and a consumer binds whichever one its host
renders:

1. **`notifications/claude/channel`** — the Claude-Code-host convention. Kept
   intact: Claude Code and the Discord plugin key on it, and the CC host renders
   it into a `<channel …>` turn block (see "render obligation" above). A bespoke
   adapter (e.g. a Hermes claude/channel plugin) binds this method.
2. **`notifications/message`** — the **MCP-standard logging notification**
   (`LoggingMessageNotification`), emitted as the **host-neutral carrier** of the
   same event. A standards-compliant MCP client that renders the standard logging
   notification into its agent loop can reach commy through this method
   without a per-host adapter. Its `params` is the MCP logging envelope, with the
   same host-neutral frame nested under `data`:

   ```json
   {
     "jsonrpc": "2.0",
     "method": "notifications/message",
     "params": {
       "level": "info",
       "logger": "commy",
       "data": { "content": "<string>", "meta": { "<key>": "<string>", ... } }
     }
   }
   ```

**Decision: yes — dual-emit.** This records the conclusion of a prior, thorough,
source-verified investigation. The decision is recorded here, not re-derived —
the rationale is captured because it is **non-obvious**:

- The neutral path is **not an arbitrary new alias**. `openai/codex#18056` names
  `notifications/message` as the carrier it intends to render, and explicitly
  cites `notifications/claude/channel` as the proprietary prior-art it wants
  parity with. So `notifications/message` is the **convergent ecosystem standard**,
  not a bespoke invention. (Broader context: MCP discussion #337 — notifications
  exist in the spec but *no* client yet injects them into the agent loop. The gap
  is ecosystem-wide and structural, not a one-off Hermes need.)
- The "logging notification is a semantic mismatch" objection is the
  **obvious-but-wrong** read. `notifications/message` *is* the logging
  notification, and routing message content through it is precisely the
  convention the ecosystem is standardising on — not a misuse.
- The neutral emit is low-cost future-proofing, not a shortcut onto an existing
  consumer. Emitting `notifications/message` earns no free ride on any existing consumer:
  Hermes's MCP client recognises `LoggingMessageNotification` as a typed
  notification yet still drops it (`case _: pass`) — a notch behind Codex, which
  at least logs it. So the consumer-side handler must be built regardless of which
  method the substrate emits. Since *also*-emitting costs the substrate ~nothing
  and makes commy renderable by any future standards-compliant client, you
  add it.

Each consumer registers a handler for **one** carrier (the one its host renders),
so dual emission does not double-render in practice — CC/Discord stay on
`notifications/claude/channel`; a standards-compliant client uses
`notifications/message`.

### Carrier divergence: the display carrier is a strict subset of the machine carrier

Because each consumer renders exactly one carrier, the two carriers do **not**
need to be byte-identical — and deliberately are not (this supersedes the earlier
"data carries an identical frame" parity claim below; the parity that
*matters* is preserved, see next paragraph):

- **`notifications/message`** (the machine / data carrier) carries the **full
  frame**, including the numeric identity ids `sender_id` and reaction `by_id`. A
  machine consumer (e.g. Hermes keying `SessionSource.user_id`) reads these — a
  stable, collision- and rename-proof key, which a display name is not for human
  senders.
- **`notifications/claude/channel`** (the agent-display carrier) carries the
  **same frame minus `{sender_id, by_id}`**. The CC host renders the entire `meta`
  into the agent's turn, where a bare numeric identity id is noise: it collides
  visually with the equally-numeric `message_id` and tempts the agent to quote a
  number instead of a name. The sender/reactor are surfaced there by name only
  (`sender_name`, `by_name`).

The divergence is confined to those two keys. The **routing key**
(`channel_name` + `thread`) and the **dedup key** (`message_id`) are on **both**
carriers, so the display carrier is a strict subset that loses nothing a consumer
routes or dedups on. The projection is applied in `channelNotifier`
(`packages/mcp/event-pump.ts`), which omits `IDENTITY_ID_META_KEYS` from the
`claude/channel` params while emitting the full `meta` under the
`notifications/message` `data`.

### `notifications/message` payload shape — decision: the MCP logging envelope, `{content, meta}` under `data`

The dual-emit decision (above) deliberately left one design question open: does
the neutral carrier transport the bare `{content, meta}` frame, or the MCP
logging-data envelope (`level` / `logger` / `data`)? It was resolved to the
**envelope**, shown above. The rationale is **schema-forced, not stylistic**:

- The MCP SDK's `LoggingMessageNotificationParamsSchema` **requires** `level` and
  treats `data` as the payload. Placing the bare `{content, meta}` frame directly
  at `params` produces a notification with no `level` — which fails validation in
  exactly the standards-compliant clients this carrier exists to reach. Claiming
  the standard method while breaking its param contract would be worse than not
  emitting it at all, so the envelope is mandatory.
- **`data` carries the full `{content, meta}` frame.** This is the
  parity-preserving choice for everything a consumer routes or dedups on: a
  `notifications/message` consumer gets the same routing key
  (`channel_name` + `thread`) and dedup key (`message_id`) a `claude/channel`
  consumer gets. Carrying only the `content` string in `data` would strip
  routing/dedup and break multi-context use. (Note: the machine `data` frame is a
  *superset* of the `claude/channel` params, not identical to it — it additionally
  carries the numeric identity ids `{sender_id, by_id}` that the display carrier
  omits; see "Carrier divergence" above. Self-echo is handled at the emitter, so
  neither carrier carries a self-identity filter key.)
- **`level` is the constant `"info"`.** The emitter (`channelNotifier`) sees only
  a `ChannelEventPayload` `{content, meta}` with no event-kind discriminator;
  threading the kind through solely to map error events to `level: "error"` is
  coupling for a field the consumer renders from `content`/`meta` anyway. Left
  constant deliberately.
- **`logger` is the constant `"commy"`** — the host-neutral analogue of the
  `source="commy"` attribution the CC host adds for the `claude/channel`
  path (which is the *host's* responsibility there; here it travels in the frame).

**Scope of the renderability claim.** What is *verified* is that the frame
validates as a standard `LoggingMessageNotification` and that `data` carries the
full `{content, meta}` frame. Full render-compatibility with a specific target
consumer (e.g. `openai/codex#18056`) is **to be confirmed at that integration** —
it is not exercised against a live consumer here. The substrate side guarantees a
well-formed standard frame; turning that frame into agent-visible input remains
the consumer's receive-path obligation (see the checklist above).

Source of truth for the shape: `channelNotifier` in `packages/mcp/event-pump.ts`,
asserted by the dual-emit test in `packages/mcp/event-pump.test.ts`; the
`logging` capability it requires is declared in `buildMcpServer`
(`packages/mcp/mcp-server.ts`) and asserted in `packages/mcp/mcp-server.test.ts`.

## Source of truth

- Frame type: `ChannelEventPayload` — `packages/mcp/events.ts`
- Frame builders: `formatMessage` / `formatReaction` / `formatError` — `packages/mcp/events.ts`
- Emit site: `channelNotifier` — `packages/mcp/event-pump.ts`
- Event model: `InboundEvent` / `Message` — `packages/core/ports.ts`
- Cross-emitter precedent: the Discord plugin emits the same method + frame shape against the same host capability.
