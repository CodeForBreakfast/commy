/**
 * Hexagonal port definitions for agent communications.
 *
 * The core holds these interfaces; substrate-specific work
 * lives in driven adapters that implement them, and Claude-Code-specific
 * work lives in driving adapters that expose them as MCP tools.
 *
 * Cross-cutting properties (trust model, persistence, cross-machine reach,
 * human↔agent parity) are constraints on adapter implementations, not
 * ports.
 */

import { Data, type Duration, type Effect, type Option, Schema, type Stream } from 'effect'

import { messageOf } from './messageOf.ts'

/** Opaque stable handle for a realm user (bot or human). */
export const IdentityIdSchema = Schema.NonEmptyString.pipe(Schema.brand('IdentityId'))
export type IdentityId = typeof IdentityIdSchema.Type
export const decodeIdentityId = Schema.decodeUnknown(IdentityIdSchema)

/** Opaque stable handle for a channel-shaped substrate object. */
export const ChannelIdSchema = Schema.NonEmptyString.pipe(Schema.brand('ChannelId'))
export type ChannelId = typeof ChannelIdSchema.Type
export const decodeChannelId = Schema.decodeUnknown(ChannelIdSchema)

/** Substrate-facing channel label. Used in narrows / send-to addressing. */
export const ChannelNameSchema = Schema.NonEmptyString.pipe(Schema.brand('ChannelName'))
export type ChannelName = typeof ChannelNameSchema.Type
export const decodeChannelName = Schema.decodeUnknown(ChannelNameSchema)

/** Opaque stable handle for a single posted message. */
export const MessageIdSchema = Schema.NonEmptyString.pipe(Schema.brand('MessageId'))
export type MessageId = typeof MessageIdSchema.Type
export const decodeMessageId = Schema.decodeUnknown(MessageIdSchema)

/** Topic / thread label. Threads have no separate id on substrates like Zulip. */
export const ThreadNameSchema = Schema.NonEmptyString.pipe(Schema.brand('ThreadName'))
export type ThreadName = typeof ThreadNameSchema.Type
export const decodeThreadName = Schema.decodeUnknown(ThreadNameSchema)

/**
 * Ready-to-click substrate permalinks, one brand per ref granularity so a
 * builder can't hand a message URL where a thread URL belongs. Branded at the
 * single construction site — the adapter's permalink builders — from strings
 * whose exact format is substrate-private.
 */
export const ChannelPermalinkSchema = Schema.NonEmptyString.pipe(Schema.brand('ChannelPermalink'))
export type ChannelPermalink = typeof ChannelPermalinkSchema.Type

export const ThreadPermalinkSchema = Schema.NonEmptyString.pipe(Schema.brand('ThreadPermalink'))
export type ThreadPermalink = typeof ThreadPermalinkSchema.Type

export const MessagePermalinkSchema = Schema.NonEmptyString.pipe(Schema.brand('MessagePermalink'))
export type MessagePermalink = typeof MessagePermalinkSchema.Type

/** Wall-clock timestamp in epoch seconds. Branded to keep counts/ids out. */
export const TimestampSchema = Schema.NonNegative.pipe(Schema.brand('Timestamp'))
export type Timestamp = typeof TimestampSchema.Type
export const decodeTimestamp = Schema.decodeUnknown(TimestampSchema)

/**
 * Message body text. Non-empty. Adapters mint via `decodeMessageBody` at the
 * parse boundary; driving adapters mint at the MCP args boundary.
 */
export const MessageBodySchema = Schema.NonEmptyString.pipe(Schema.brand('MessageBody'))
export type MessageBody = typeof MessageBodySchema.Type
export const decodeMessageBody = Schema.decodeUnknown(MessageBodySchema)

/**
 * User-surface display name for an identity (bot or human). Non-empty.
 * Adapters mint at the parse boundary (e.g. from Zulip's `full_name`).
 */
export const DisplayNameSchema = Schema.NonEmptyString.pipe(Schema.brand('DisplayName'))
export type DisplayName = typeof DisplayNameSchema.Type
export const decodeDisplayName = Schema.decodeUnknown(DisplayNameSchema)

/**
 * Identity name to acquire on the substrate. Brand carries the invariant
 * "this value came through a known mint point" — either `composeBotName`
 * (ephemeral path) or `parseBotName` (env-var path). Without the brand,
 * a bare string from any source could flow into `IdentityPort.acquire`
 * unchecked.
 *
 * The type lives in core (not bootstrap) because `IdentityPort.acquire`
 * needs it in its signature and core cannot import from the plugin layer.
 * The mint points stay in bootstrap.ts.
 */
export const BotNameSchema = Schema.NonEmptyString.pipe(Schema.brand('BotName'))
export type BotName = typeof BotNameSchema.Type
export const decodeBotName = Schema.decodeUnknown(BotNameSchema)

/**
 * Emoji shortcode as the substrate accepts it (no surrounding colons —
 * `smile`, not `:smile:`). The brand carries the local invariant "we ran
 * our validator"; the substrate may still reject downstream.
 *
 * Bare shortcode invariant: a non-empty string with neither a leading nor a
 * trailing colon (`smile`, not `:smile:`). Single-character shortcodes and
 * embedded whitespace/newlines are accepted — the substrate may still reject
 * the shortcode downstream. `[\s\S]` (not `.`) so the "no trailing colon"
 * tail anchors past embedded newlines.
 */
export const EmojiSchema = Schema.NonEmptyString.pipe(
  Schema.pattern(/^[^:](?:[\s\S]*[^:])?$/),
  Schema.brand('Emoji'),
)
export type Emoji = typeof EmojiSchema.Type
export const decodeEmoji = Schema.decodeUnknown(EmojiSchema)

/**
 * Synchronous brand decoders for test fixtures.
 *
 * In test setup a fixed literal that fails to decode is a programmer
 * error, so `Schema.decodeSync`'s throw is the legitimate fatal case —
 * fixtures need not thread a ParseError through Effect. Production code
 * uses the Effect-returning `decode*` decoders above, not these.
 */
export const decodeIdentityIdSync = Schema.decodeSync(IdentityIdSchema)
export const decodeChannelIdSync = Schema.decodeSync(ChannelIdSchema)
export const decodeChannelNameSync = Schema.decodeSync(ChannelNameSchema)
export const decodeMessageIdSync = Schema.decodeSync(MessageIdSchema)
export const decodeThreadNameSync = Schema.decodeSync(ThreadNameSchema)
export const decodeTimestampSync = Schema.decodeSync(TimestampSchema)
export const decodeMessageBodySync = Schema.decodeSync(MessageBodySchema)
export const decodeDisplayNameSync = Schema.decodeSync(DisplayNameSchema)
export const decodeBotNameSync = Schema.decodeSync(BotNameSchema)
export const decodeEmojiSync = Schema.decodeSync(EmojiSchema)

export type IdentityKind = 'human' | 'agent'

export interface Identity {
  /** Stable, branded handle. Adapters mint via `decodeIdentityId` at the parse boundary. */
  readonly id: IdentityId
  /** Display name. User-surface (mention autocomplete, message authorship). */
  readonly name: DisplayName
  readonly kind: IdentityKind
}

/**
 * The channel facet of an observation — a channel the substrate handed back,
 * carrying its stable id and a ready-to-click permalink so a caller never
 * hand-assembles one. Only ever produced as an observation (a message's
 * channel, or a `Directory.listChannels` entry), so its permalink is always
 * available — hence required, not optional. Addressing a channel (post,
 * read, subscribe) is by `ChannelName` alone; a `ChannelRef` is what the
 * substrate returns, never what a caller supplies.
 */
export const ChannelRefSchema = Schema.Struct({
  id: ChannelIdSchema,
  name: ChannelNameSchema,
  permalink: ChannelPermalinkSchema,
})
export type ChannelRef = typeof ChannelRefSchema.Type

/**
 * The thread facet of an observed message: a topic name paired with a
 * ready-to-click permalink and its resolution status. Only ever produced as
 * `MessageRef.thread` for a message the substrate handed back, so its permalink
 * is always available — hence required, not optional. An address target (a
 * message reconstructed from an id with no observation) carries
 * `MessageRef.thread: Option.none()`.
 *
 * `resolved` is observable state, not an address input: whether the substrate
 * reports this thread resolved (mark it via `MessagePublisher.resolveThread`,
 * clear it via `unresolveThread`). The `name` never encodes resolution — any
 * substrate-side marker (Zulip's ✔-prefixed topic) is stripped behind the
 * adapter seam so the port sees a clean name plus this flag. Required so every
 * producer states it explicitly rather than defaulting an absent value.
 */
export const ObservedThreadSchema = Schema.Struct({
  name: ThreadNameSchema,
  resolved: Schema.Boolean,
  permalink: ThreadPermalinkSchema,
})
export type ObservedThread = typeof ObservedThreadSchema.Type

/**
 * An observed message the substrate handed back: its stable id, its
 * observation `ChannelRef`, the `ObservedThread` it was seen in (`none` for
 * top-level messages), and a ready-to-click `MessagePermalink`. Only ever
 * produced as an observation, so the permalink is always available — hence
 * required, not optional. Addressing a message (react, edit, reply) is by a
 * bare id; the address-reconstruction path fills the observation facets with
 * transparent placeholders until the message-address split (comms-e6yi)
 * drops them from an address entirely.
 */
export const MessageRefSchema = Schema.Struct({
  id: MessageIdSchema,
  channel: ChannelRefSchema,
  thread: Schema.OptionFromSelf(ObservedThreadSchema),
  permalink: MessagePermalinkSchema,
})
export type MessageRef = typeof MessageRefSchema.Type

/**
 * Aggregated reaction state on a message, grouped by emoji. `by` lists
 * the identities who have reacted with that emoji. Adapters resolve
 * identities against the same directory used for `Message.sender` and
 * `Message.mentions`; unresolved reactors are surfaced with a synthetic
 * Identity so the count stays accurate.
 */
export interface Reaction {
  readonly emoji: Emoji
  readonly by: ReadonlyArray<Identity>
}

export interface Message {
  readonly ref: MessageRef
  readonly sender: Identity
  readonly body: MessageBody
  readonly ts: Timestamp
  readonly mentions: ReadonlyArray<Identity>
  readonly reactions: ReadonlyArray<Reaction>
}

export interface Range {
  /** Inclusive lower bound. Omit for "start of history". */
  readonly since?: Timestamp
  /** Inclusive upper bound. Omit for "up to now". */
  readonly until?: Timestamp
  /** Hard cap on returned messages. Adapters may return fewer. */
  readonly limit?: number
}

export interface PostOpts {
  /**
   * Identities to notify. Metadata-only: adapters do not mutate
   * `body` based on this list. Where a substrate needs literal mention
   * markup inside the message text to trigger a notification (Zulip's
   * `@**name**`, Discord's `<@id>`), the caller writes that markup
   * directly into `body` at the position they want it rendered. The
   * `mentions[]` array is a side-channel for adapters with a separate
   * notification primitive — substrate-derived render-time mentions
   * still surface via `Message.mentions` on the inbound side regardless
   * of whether this field was set on the post.
   */
  readonly mentions?: ReadonlyArray<Identity>
  /** Named conversation slice; adapter does find-or-create. */
  readonly thread?: ThreadName
  /** Best-effort in-thread reply. Adapters without an in-thread reply primitive may quote-block or drop. */
  readonly replyTo?: MessageRef
}

/**
 * `'unknown'` is distinct from `'offline'`: offline means the substrate
 * reports the identity as not present, whereas unknown means presence is
 * unknowable for that identity. Bots fall here — Zulip presence is human-only
 * by design (see the Zulip adapter), so an agent has no presence to read.
 */
export type Presence = 'online' | 'idle' | 'offline' | 'unknown'

export type SubscriptionTarget =
  | ChannelName
  | ThreadSubscription
  | NewTopicsInChannelSubscription
  | 'mentions'

export interface ThreadSubscription {
  readonly channel: ChannelName
  readonly thread: ThreadName
}

/**
 * "Deliver the first message of every new topic in this channel, exactly
 * once per topic." Used by project-concierge agents: fresh enquiries land
 * as new topics in `#<project>` and surface once; subsequent replies in
 * the same topic do not reach this narrow (the universal sticky-engagement
 * rule auto-subscribes the participant to the thread on first post/react,
 * so follow-on conversation flows via the thread subscription instead).
 *
 * "First message" is adapter-private bookkeeping — there is no port-level
 * seen-topics state. Adapters surface a topic exactly once per adapter
 * instance lifetime; persistence across restarts is not guaranteed, the
 * first message after restart may re-fire.
 *
 * Messages without a topic (Memory adapter posts with `thread: undefined`)
 * do not surface via this narrow — "new topic" presupposes a topic name.
 */
export interface NewTopicsInChannelSubscription {
  readonly kind: 'new-topics-in-channel'
  readonly channel: ChannelName
}

export type InboundEvent =
  | {
      readonly kind: 'message-posted'
      readonly message: Message
      /**
       * Set when the event was synthesised by the substrate's gap-replay path
       * (e.g. Zulip events-queue expiry recovery) rather than
       * observed live. Downstream renderers surface this as a `replayed`
       * attribute on the channel block so consumers can tell a backfilled
       * message from a fresh one. Absent means live.
       */
      readonly replayed?: boolean
    }
  | {
      readonly kind: 'mention-received'
      readonly message: Message
      readonly mentions: ReadonlyArray<Identity>
      readonly replayed?: boolean
    }
  | {
      readonly kind: 'reaction-added'
      readonly target: MessageRef
      readonly emoji: Emoji
      readonly by: Identity
    }
  | {
      readonly kind: 'reaction-removed'
      readonly target: MessageRef
      readonly emoji: Emoji
      readonly by: Identity
    }

/**
 * Opaque substrate-specific credential blob. Each adapter publishes the
 * keys it populates (Zulip: `email`+`apiKey`; Memory: synthesised
 * marker fields). Callers persist the blob if they need to reconstruct
 * an adapter later — they never read the keys themselves.
 */
export type Credentials = Readonly<Record<string, string>>

export interface AcquiredIdentity {
  readonly credentials: Credentials
  readonly identity: Identity
}

/**
 * Caller-supplied intent for `IdentityPort.release`. `persistent` flags an
 * identity meant to outlive the session — pinned via `COMMY_BOT_NAME`
 * rather than minted as an ephemeral `cc-*` seat. Substrates that deactivate
 * on release (Zulip) skip deactivation for a persistent identity, so a
 * later re-acquire is an owner-permitted regenerate rather than an admin-only
 * reactivate of a bot the minter deactivated itself. Omitted /
 * `false` is the ephemeral default: deactivate as usual.
 */
export interface ReleaseOpts {
  readonly persistent?: boolean
}

/**
 * Stateful per-instance lifecycle:
 *
 *     new ZulipAdapter(config)   → unauthenticated
 *     adapter.acquire(name)      → adapter holds creds for that bot
 *     adapter.publisher.post(…)  → operates as that bot
 *     adapter.release()          → drops creds, substrate-appropriate
 *                                  cleanup; back to unauthenticated.
 *
 * `acquire` is idempotent for the same name within an instance — repeat
 * calls return the existing binding. Calling `acquire(other-name)` on
 * an already-bound adapter throws; the caller must `release` first or
 * use a fresh adapter instance.
 *
 * `release` is a no-op on an already-unauthenticated adapter.
 */
export interface IdentityPort {
  /**
   * The identity this adapter is currently bound to. The Effect dies
   * with a defect when the adapter has not acquired an identity — that's
   * a programmer error, not a typed failure.
   */
  currentIdentity(): Effect.Effect<Identity, never>
  /**
   * Bind the adapter to an identity with the given display name. Per
   * substrate:
   *   - Memory: registers the identity in-process (always succeeds,
   *     unless configured with a strict allowlist).
   *   - Zulip: looks up an existing bot by full_name authenticated as
   *     the configured minter user; on hit regenerates its API key,
   *     on miss mints a new bot. Returned bot is owned by the minter.
   *   - Discord (forward-compat): looks up `name` in a pre-provisioned
   *     credentials cache and fails with `UnknownIdentity` on miss —
   *     the Discord substrate does not permit runtime account creation.
   */
  acquire(name: BotName): Effect.Effect<AcquiredIdentity, UnknownIdentity | IdentityError>
  /**
   * Release the bound identity. Substrate-side cleanup is best-effort
   * (Zulip deactivates the bot, Discord no-ops, Memory clears the
   * binding). Idempotent — calling on an unauthenticated adapter is
   * a no-op, never fails.
   *
   * Pass `{ persistent: true }` for an `COMMY_BOT_NAME`-pinned
   * identity so the substrate skips deactivation: a
   * deactivated bot's only path back is an admin-only reactivate, which
   * wedges a Member-rights minter. Leaving a persistent bot active keeps
   * re-acquire on the owner-permitted regenerate path. Omitted / `false`
   * is the ephemeral default and deactivates as usual.
   */
  release(opts?: ReleaseOpts): Effect.Effect<void, never>
  resolve(name: string): Effect.Effect<Option.Option<Identity>, IdentityError>
}

export interface MessagePublisher {
  /**
   * Publish a message to a channel as the bound identity. Fails with a typed
   * `UnknownChannel` when the substrate has no such channel (pre-flighted so
   * Zulip cannot silently swallow it — see the class doc) and with a
   * `PublisherError` wrapping any other substrate failure. Calling before
   * `identity.acquire` is a defect, not a typed failure.
   */
  post(
    channel: ChannelName,
    body: MessageBody,
    opts?: PostOpts,
  ): Effect.Effect<MessageRef, UnknownChannel | PublisherError>
  /**
   * Replace the body of an existing message attributed to the bound
   * identity. Adapters target the substrate's in-place edit primitive
   * (Zulip's PATCH /messages/{id}); substrates without one MAY surface
   * a substrate-shaped error. Edits do not emit InboundEvents — the
   * port surfaces the new body via history.readChannel only.
   */
  edit(message: MessageRef, body: MessageBody): Effect.Effect<void, PublisherError>
  react(message: MessageRef, emoji: Emoji): Effect.Effect<void, PublisherError>
  unreact(message: MessageRef, emoji: Emoji): Effect.Effect<void, PublisherError>
  /**
   * Mark a thread resolved / clear its resolved status across the whole thread,
   * addressed by `channel` + plain thread `name` (never a resolution-encoded
   * form — the paired verbs, mirroring `react`/`unreact`, carry the direction).
   * Idempotent: a thread already in the requested state is a no-op with no
   * substrate write. Like `edit`, this mutates substrate state and emits no
   * InboundEvent — a consumer observes the new status via `ObservedThread.resolved`
   * when it next reads the thread. A substrate failure (including a thread the
   * substrate has no messages for) surfaces as a typed `PublisherError`;
   * calling before `identity.acquire` is a defect, not a typed failure.
   */
  resolveThread(channel: ChannelName, thread: ThreadName): Effect.Effect<void, PublisherError>
  unresolveThread(channel: ChannelName, thread: ThreadName): Effect.Effect<void, PublisherError>
}

export interface MessageInbox {
  /**
   * Declare interest in a subscription target. When the returned
   * Effect resolves, a subsequent `events()` subscription observes
   * matching events posted from that moment onward — adapters are
   * responsible for completing whatever substrate-level priming is
   * needed before resolving (e.g. Zulip's `POST /register` to open
   * the events queue). Consumers can therefore write
   *
   *     yield* inbox.subscribe(channel)
   *     yield* publisher.post(channel, "hello")
   *     yield* Stream.runForEach(inbox.events(), handle)
   *
   * and trust that the post will be observable on the stream.
   */
  subscribe(target: SubscriptionTarget): Effect.Effect<void, InboxError>
  unsubscribe(target: SubscriptionTarget): Effect.Effect<void, InboxError>
  /**
   * Effect-native Stream of inbound events. Adapters drive this from
   * their substrate's event mechanism (Zulip's events queue, Discord
   * gateway, etc.). The Stream is infinite — consumers cancel by
   * interrupting the surrounding fiber. The error channel is `never`:
   * recoverable substrate hiccups (rate limits, queue expiry, network
   * blips) are absorbed inside the producer via internal `Schedule`
   * retry; consumers see only successfully-decoded events.
   */
  events(): Stream.Stream<InboundEvent>
  /**
   * Backfill messages newer than the given timestamp. Used at session
   * start to recover messages missed while offline.
   */
  replay(since: Timestamp): Effect.Effect<ReadonlyArray<InboundEvent>, InboxError>
}

export interface RecentThread {
  readonly channel: ChannelName
  readonly thread: ThreadName
  readonly lastPostTs: Timestamp
  readonly lastPostBody: MessageBody
}

export interface HistoryReader {
  readChannel(
    channel: ChannelName,
    range: Range,
  ): Effect.Effect<ReadonlyArray<Message>, HistoryError>
  readThread(
    channel: ChannelName,
    threadName: ThreadName,
    range?: Range,
  ): Effect.Effect<ReadonlyArray<Message>, HistoryError>
  recentThreads(
    sender: IdentityId,
    opts?: { limit?: number },
  ): Effect.Effect<ReadonlyArray<RecentThread>, HistoryError>
  /**
   * Resolve a clickable substrate permalink for a message id —
   * the id-only case where the caller doesn't hold a ref. URL construction
   * stays in the adapter. With a `hint` the adapter resolves the channel and
   * builds the link from channel + topic + id without locating the message;
   * without one it locates the message by id. `None` when the message cannot
   * be resolved (no hint and no such message).
   */
  messagePermalink(
    id: MessageId,
    hint?: { readonly channel: ChannelName; readonly thread?: ThreadName },
  ): Effect.Effect<Option.Option<MessagePermalink>, HistoryError>
}

/**
 * Failure surface for the Effect-returning `Directory` reads. The Zulip
 * adapter mints this when the backing substrate call fails, carrying the
 * underlying error as `cause`; the in-memory adapter never produces it.
 * `operation` names the port method that failed.
 */
export class DirectoryError extends Data.TaggedError('DirectoryError')<{
  readonly operation: 'presence' | 'listAgents' | 'listHumans' | 'listChannels'
  readonly cause: unknown
}> {
  override get message(): string {
    return messageOf(this.cause)
  }
}

export interface Directory {
  listAgents(): Effect.Effect<ReadonlyArray<Identity>, DirectoryError>
  listHumans(): Effect.Effect<ReadonlyArray<Identity>, DirectoryError>
  listChannels(): Effect.Effect<ReadonlyArray<ChannelRef>, DirectoryError>
  /**
   * The presence read recovers the "user has no presence record → offline"
   * case declaratively at the adapter and surfaces any other substrate
   * failure as a typed `DirectoryError`.
   */
  presence(identity: Identity): Effect.Effect<Presence, DirectoryError>
}

/**
 * Static, substrate-derived properties of an adapter's message-ordering
 * model that a consumer must adapt to. Not behaviour — these are facts about
 * the substrate the ports can't make uniform, surfaced so the same code
 * (tests and production) reads them rather than branching on a substrate name.
 * Deliberately minimal: one field per real consumer, never a junk drawer of
 * substrate flags.
 */
export interface Capabilities {
  /**
   * The smallest real-time gap between two `post`s that the substrate will
   * stamp with distinct `Timestamp`s — the resolution of the ordering model.
   * Memory keys `ts` off a monotonic counter so any two posts differ
   * (`Duration.zero`); Zulip stamps integer epoch seconds, so posts inside the
   * same second collide (`Duration.seconds(1)`). The gap-replay watermark
   * dedups on `ts`, so "`ts` is not a unique key below this resolution" is
   * knowledge production consults, not only a test concern — a caller that
   * needs two posts distinguishable by `ts` must space them by at least this.
   */
  readonly timestampGranularity: Duration.Duration
}

/**
 * Aggregate exposed by a driven adapter. Driving adapters depend on
 * this shape, never on substrate-specific extensions.
 */
export interface AgentComms {
  readonly capabilities: Capabilities
  readonly identity: IdentityPort
  readonly publisher: MessagePublisher
  readonly inbox: MessageInbox
  readonly history: HistoryReader
  readonly directory: Directory
}

/**
 * Failure of `IdentityPort.acquire` when the substrate cannot bind the
 * given name — Discord with no pre-provisioned entry, or a Memory
 * adapter configured with a strict allowlist that excludes it. Tagged
 * so callers can `Effect.catchTag('UnknownIdentity', ...)` it apart
 * from a generic `IdentityError`.
 */
export class UnknownIdentity extends Data.TaggedError('UnknownIdentity')<{
  readonly name: string
  readonly substrate: string
}> {
  override get message(): string {
    return `acquire(${this.name}) failed — ${this.substrate} substrate has no entry for that name and cannot mint one`
  }
}

/**
 * Failure of `MessagePublisher.post` when the substrate has no record of the
 * target channel. Adapters surface this instead of letting the substrate
 * handle the failure silently — Zulip in particular routes "channel doesn't
 * exist" to Notification Bot DMs while returning a success-shaped response to
 * the caller, so the caller would otherwise see no error at all. Tagged so the
 * MCP edge and callers can discriminate it from a generic `PublisherError`.
 */
export class UnknownChannel extends Data.TaggedError('UnknownChannel')<{
  readonly channel: ChannelName
  readonly substrate: string
}> {
  override get message(): string {
    return `post(${this.channel}) failed — ${this.substrate} substrate has no channel by that name`
  }
}

/**
 * Failure surface for the Effect-returning `MessagePublisher` writes. The
 * adapter mints this when the backing substrate call fails, carrying the
 * underlying error as `cause`; the in-memory adapter never produces it (its
 * only typed failure is `UnknownChannel`). `operation` names the port method
 * that failed. Mirrors `DirectoryError` so core stays substrate-agnostic.
 */
export class PublisherError extends Data.TaggedError('PublisherError')<{
  readonly operation: 'post' | 'edit' | 'react' | 'unreact' | 'resolveThread' | 'unresolveThread'
  readonly cause: unknown
}> {
  override get message(): string {
    return messageOf(this.cause)
  }
}

/**
 * Failure surface for the Effect-returning `IdentityPort` methods. `acquire`
 * surfaces it alongside `UnknownIdentity`; `release` and `resolve` surface it
 * alone. The Zulip adapter mints this when the backing substrate call fails;
 * the in-memory adapter never produces it. Mirrors `DirectoryError`.
 */
export class IdentityError extends Data.TaggedError('IdentityError')<{
  readonly operation: 'acquire' | 'release' | 'resolve'
  readonly cause: unknown
}> {
  override get message(): string {
    return messageOf(this.cause)
  }
}

/**
 * Failure surface for the Effect-returning `MessageInbox` methods other
 * than `events()`, whose error channel is `never`. The Zulip adapter mints
 * this when the backing substrate call fails; the in-memory adapter never
 * produces it.
 */
export class InboxError extends Data.TaggedError('InboxError')<{
  readonly operation: 'subscribe' | 'unsubscribe' | 'replay'
  readonly cause: unknown
}> {
  override get message(): string {
    return messageOf(this.cause)
  }
}

/**
 * Failure surface for the Effect-returning `HistoryReader` reads. The
 * Zulip adapter mints this when the backing substrate call fails; the
 * in-memory adapter never produces it.
 */
export class HistoryError extends Data.TaggedError('HistoryError')<{
  readonly operation: 'readChannel' | 'readThread' | 'recentThreads' | 'messagePermalink'
  readonly cause: unknown
}> {
  override get message(): string {
    return messageOf(this.cause)
  }
}
