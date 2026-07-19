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

import { Data, type Duration, type Effect, Match, type Option, Schema, type Stream } from 'effect'

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
 * A channel's standing self-description — the text a stranger reads to learn
 * what the channel is for, distinct from any message posted in it.
 *
 * Non-empty, because "has no description" is modelled as an absent `Option`
 * and needs no second spelling. A channel nobody has described and a channel
 * described as `""` are not two states a substrate can tell apart (Zulip
 * stores an undescribed stream as `description = ""`), so admitting
 * `Some("")` alongside `None` would be a distinction the port could express
 * but never read back. Clearing a description is `None`; the empty string is
 * simply not a description.
 *
 * Deliberately carries no length or format constraint. Substrates cap and
 * shape descriptions differently; a substrate that rejects a value it cannot
 * store says so through `ChannelDescriptionRejected`, so the limit stays the
 * adapter's fact rather than a constant baked into core.
 */
export const ChannelDescriptionSchema = Schema.NonEmptyString.pipe(
  Schema.brand('ChannelDescription'),
)
export type ChannelDescription = typeof ChannelDescriptionSchema.Type
export const decodeChannelDescription = Schema.decodeUnknown(ChannelDescriptionSchema)

/**
 * User-surface display name for an identity (bot or human). Non-empty.
 * Adapters mint at the parse boundary (e.g. from Zulip's `full_name`).
 */
export const DisplayNameSchema = Schema.NonEmptyString.pipe(Schema.brand('DisplayName'))
export type DisplayName = typeof DisplayNameSchema.Type
export const decodeDisplayName = Schema.decodeUnknown(DisplayNameSchema)

/** Substrate-facing name of a named group of identities. */
export const GroupNameSchema = Schema.NonEmptyString.pipe(Schema.brand('GroupName'))
export type GroupName = typeof GroupNameSchema.Type
export const decodeGroupName = Schema.decodeUnknown(GroupNameSchema)

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
export const decodeChannelDescriptionSync = Schema.decodeSync(ChannelDescriptionSchema)
export const decodeDisplayNameSync = Schema.decodeSync(DisplayNameSchema)
export const decodeBotNameSync = Schema.decodeSync(BotNameSchema)
export const decodeEmojiSync = Schema.decodeSync(EmojiSchema)
export const decodeGroupNameSync = Schema.decodeSync(GroupNameSchema)

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

/**
 * Who a message notified. Not every mention names a user: substrates also let
 * a message address a whole audience at once (Zulip's `@**all**` /
 * `@**channel**` / `@**topic**`, Discord's `@everyone` / `@here`) or a named
 * group. Those are the highest-consequence mentions on a substrate — they wake
 * everyone — and an `Identity` cannot represent one, so this is a union rather
 * than an identity list.
 *
 * The audience variants carry no payload because the audience *is* the
 * information: the four Zulip channel wildcards all address the same set, so
 * preserving which token was typed would be substrate trivia the port has no
 * use for. Adapters map their own spellings onto these.
 */
export type Mention = Data.TaggedEnum<{
  UserMention: { readonly identity: Identity }
  /** Everyone subscribed to the channel the message was posted in. */
  ChannelWildcardMention: Record<never, never>
  /** Everyone participating in the thread the message was posted in. */
  TopicWildcardMention: Record<never, never>
  GroupMention: { readonly name: GroupName }
}>
export const Mention = Data.taggedEnum<Mention>()

/** The mentions of a known set of identities, in order. */
export const userMentions = (identities: ReadonlyArray<Identity>): ReadonlyArray<Mention> =>
  identities.map((identity) => Mention.UserMention({ identity }))

/**
 * Whether `identity` was notified by these mentions.
 *
 * A channel or topic wildcard notifies everyone in the audience, so any
 * recipient observing the message was mentioned by it. A group mention is
 * deliberately *not* a match: membership is not on the message and commy does
 * not resolve it, so claiming a match would be a guess — and inventing a
 * mention nobody made is the failure mode the code-span carve-out exists to
 * prevent. Reading the substrate's own delivery signal (comms-l1i8) removes
 * the guess entirely.
 */
export const mentionsIdentity = (mentions: ReadonlyArray<Mention>, identity: IdentityId): boolean =>
  mentions.some(
    Mention.$match({
      UserMention: (m) => m.identity.id === identity,
      ChannelWildcardMention: () => true,
      TopicWildcardMention: () => true,
      GroupMention: () => false,
    }),
  )

/** The individually-named identities among these mentions, in order. */
export const mentionedIdentities = (mentions: ReadonlyArray<Mention>): ReadonlyArray<Identity> =>
  mentions.filter(Mention.$is('UserMention')).map((m) => m.identity)

export interface Message {
  readonly ref: MessageRef
  readonly sender: Identity
  readonly body: MessageBody
  readonly ts: Timestamp
  readonly mentions: ReadonlyArray<Mention>
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
      readonly mentions: ReadonlyArray<Mention>
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
   * Zulip cannot silently swallow it — see the class doc), a typed
   * `UnresolvedMention` when the body carries a mention token no identity
   * resolves (pre-flighted for the same reason — the substrate would post it
   * and notify nobody), and a `PublisherError` wrapping any other substrate
   * failure. Calling before `identity.acquire` is a defect, not a typed
   * failure.
   *
   * `opts.thread` names a thread by its clean name — resolution is never
   * encoded in the name (see `ObservedThreadSchema`). A post into a **resolved**
   * thread **appends to it and leaves it resolved**, and the returned
   * `MessageRef` reports `thread.resolved: true`. `post` never mutates thread
   * state: a caller who wants a reply to re-open the thread calls
   * `unresolveThread` itself. The alternative — auto-unresolving on post —
   * would bury a state mutation in the substrate's hottest write path and make
   * `post` partially failable (message lands, unresolve doesn't).
   *
   * Where a substrate expresses resolution by *renaming* the thread (Zulip's
   * ✔-prefixed topic) and creates threads implicitly on write, an adapter MUST
   * address the thread's current substrate form rather than the clean name, or
   * it will mint a sibling thread and split the conversation at the resolve.
   * When both forms exist, the adapter resolves the ambiguity the same way
   * `HistoryReader.readThread` does, so post and read can never disagree about
   * which thread a name means.
   */
  post(
    channel: ChannelName,
    body: MessageBody,
    opts?: PostOpts,
  ): Effect.Effect<MessageRef, UnknownChannel | UnresolvedMention | PublisherError>
  /**
   * Replace the body of an existing message attributed to the bound
   * identity. Adapters target the substrate's in-place edit primitive
   * (Zulip's PATCH /messages/{id}); substrates without one MAY surface
   * a substrate-shaped error. Edits do not emit InboundEvents — the
   * port surfaces the new body via history.readChannel only.
   *
   * Fails with a typed `MessageEditRefused` when the substrate refuses the
   * edit for a structural reason the caller can act on (edit-window expired,
   * or the bound identity is not the original sender — see the class doc),
   * so a caller distinguishes "re-post, never editable from here" from a
   * transient `PublisherError` it should retry.
   */
  edit(
    message: MessageRef,
    body: MessageBody,
  ): Effect.Effect<void, UnresolvedMention | MessageEditRefused | PublisherError>
  /**
   * Whether the substrate permits `edit` at all *right now*, for anyone —
   * the realm-wide switch behind `MessageEditRefused`'s `editing-disabled`
   * reason, not the per-message walls. A substrate with no such switch
   * answers `true`.
   *
   * Deliberately a verb rather than a field on `Capabilities`: this is a
   * setting an administrator flips at runtime, so any value a caller holds
   * is a sample with an age, not a static fact about the substrate. Callers
   * that need a durable answer must decide how long a sample stays good —
   * a driving adapter gating its tool surface samples once at connect and
   * accepts a stale answer until it reconnects, which is why the
   * `editing-disabled` arm of {@link MessageEditRefused} remains the
   * backstop and is not made redundant by this verb.
   *
   * Answering may cost a substrate round-trip, so callers sample it
   * deliberately rather than per-operation.
   */
  editingAvailable(): Effect.Effect<boolean, PublisherError>
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
   *
   * **Occupied-name semantics — these verbs MERGE.** Where a substrate expresses
   * resolution by renaming (see `post` above), a thread can exist in *both*
   * forms at once: a realm forked by a write that addressed the clean name of a
   * resolved thread holds a resolved half and a bare-name sibling. Flipping such
   * a thread renames one form onto a name the other already occupies. That is
   * defined, not a collision: the two halves **merge into a single thread under
   * the target name**, ordered by the substrate's message order — i.e. send
   * order, a chronological interleave of the halves rather than one block
   * appended after the other. No message is dropped, duplicated or reordered,
   * and the source form ceases to exist.
   *
   * The consequence worth naming: `unresolveThread` **is the repair** for a
   * thread already forked this way — one call rejoins it with its chronology
   * intact. Adapters MUST NOT guard against the occupied name, and callers MAY
   * rely on the merge. (Verified against a real Zulip realm in
   * `realm.live.test.ts` — `propagate_mode: change_all` onto an existing topic.
   * A substrate that instead errored, refused, or clobbered on an occupied name
   * would fail that assertion, which is the point of pinning it.)
   */
  resolveThread(channel: ChannelName, thread: ThreadName): Effect.Effect<void, PublisherError>
  unresolveThread(channel: ChannelName, thread: ThreadName): Effect.Effect<void, PublisherError>
  /**
   * Set a channel's standing description, or clear it with `Option.none()`.
   * Addressed by `ChannelName` like every other write. Idempotent: writing the
   * description a channel already carries is a no-op with no substrate write,
   * and clearing an already-absent description is likewise.
   *
   * Like `edit`, this mutates substrate state and emits no InboundEvent — a
   * consumer observes the new text via `Directory.channelDescription`. Reading
   * back what was written is the contract; a substrate that cannot store the
   * value verbatim must reject it rather than silently store a mangled form.
   *
   * `ChannelDescriptionRejected` is that refusal — the substrate's own limits
   * (a length cap, a format restriction) applied at the adapter, carrying
   * enough detail to tell a caller what to change. A caller whose identity
   * lacks permission to edit the channel gets a `PublisherError`, not a
   * defect, so an under-privileged session degrades rather than crashes.
   */
  setChannelDescription(
    channel: ChannelName,
    description: Option.Option<ChannelDescription>,
  ): Effect.Effect<void, PublisherError | UnknownChannel | ChannelDescriptionRejected>
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
  readonly operation:
    | 'presence'
    | 'listAgents'
    | 'listHumans'
    | 'listChannels'
    | 'channelDescription'
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
   * A channel's standing description, or `None` when nobody has set one (and
   * on a substrate with no notion of a channel description at all). Read
   * fresh, addressed by name, and deliberately kept off `ChannelRef`: a
   * `ChannelRef` is an observation facet carried by every `MessageRef`, so
   * folding mutable channel metadata into it would stamp each observed message
   * with a point-in-time copy of state that has nothing to do with that
   * message. A channel the substrate has no record of fails with
   * `UnknownChannel` rather than collapsing into `None`, keeping "no such
   * channel" distinct from "channel with nothing said about it".
   */
  channelDescription(
    channel: ChannelName,
  ): Effect.Effect<Option.Option<ChannelDescription>, DirectoryError | UnknownChannel>
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
 * Refusal of `MessagePublisher.setChannelDescription` when the substrate
 * cannot store the given text as written. Adapters surface this instead of
 * storing a mangled form, so a caller never has to read back what it wrote to
 * discover the substrate quietly changed it.
 *
 * `constraint` classifies the refusal without core learning any substrate's
 * numbers or syntax: `'length'` for a text longer than the substrate stores,
 * `'format'` for characters it cannot represent (Zulip rewrites newlines in a
 * stream description, so the adapter refuses them rather than let the write
 * round-trip differently than it went in). `detail` carries the adapter's own
 * explanation, including the actual limit, for a caller to show a human.
 * Tagged so the MCP edge can report a fixable input apart from a substrate
 * outage or a permission failure, both of which stay `PublisherError`.
 */
export class ChannelDescriptionRejected extends Data.TaggedError('ChannelDescriptionRejected')<{
  readonly channel: ChannelName
  readonly substrate: string
  readonly constraint: 'length' | 'format'
  readonly detail: string
}> {
  override get message(): string {
    return `setChannelDescription(${this.channel}) failed — ${this.substrate} rejected the description: ${this.detail}`
  }
}

/**
 * Failure of a `MessagePublisher` write whose body carries a mention token that
 * resolves to no known identity — a stale `@**Full Name**` the substrate would
 * post verbatim and notify nobody for. Surfaced instead of delivering silence,
 * for the same reason as `UnknownChannel`: the substrate accepts the write and
 * the sender sees success, so the dead mention is invisible without this. Only
 * tokens the substrate would actually render as mentions count — a dead form
 * inside a code span is literal text, not a failed delivery, and does not
 * trigger it. `tokens` lists the unresolved forms so the caller can fix the
 * name. Adapters with a directory (Zulip) produce it; the in-memory adapter
 * never does. Tagged so the MCP edge and callers discriminate it from a
 * generic `PublisherError`.
 */
export class UnresolvedMention extends Data.TaggedError('UnresolvedMention')<{
  readonly operation: 'post' | 'edit'
  readonly tokens: ReadonlyArray<string>
  readonly substrate: string
}> {
  override get message(): string {
    return `${this.operation} failed — ${this.substrate} substrate has no identity for mention ${this.tokens
      .map((t) => `@**${t}**`)
      .join(
        ', ',
      )}; the message would notify nobody. Fix the name or list_agents/list_humans to find the live form.`
  }
}

/**
 * A content edit the substrate refused for a structural reason the caller can
 * act on, surfaced by `edit` distinct from a transient `PublisherError` so a
 * caller can tell "this message can never be edited from here, re-post" apart
 * from "the substrate hiccuped, retry".
 *
 * Zulip walls a content edit three ways, and commy can fix none from code:
 *  - `editing-disabled`: the realm has `allow_message_editing` off, so no
 *    message on it is editable by anyone. Zulip checks this wall first, so on
 *    such a realm it is the only wall any caller ever meets.
 *  - `window-expired`: the realm's `message_content_edit_limit_seconds` has
 *    elapsed since the message was sent. An operator knob on the realm — see
 *    docs/self-hosting.md — not something the substrate widens from code.
 *  - `not-original-sender`: Zulip only lets the original sender edit content.
 *    commy edits as the bound identity, and ephemeral `cc-<8>` identities are
 *    per-session — so a message posted by a previous session's seat can never
 *    be edited by today's seat, at any age. No realm setting changes that.
 *
 * All three walls force the same recovery, which retrying cannot reach: re-post
 * rather than edit. Tagged (like `UnknownChannel`) so the MCP edge and callers
 * discriminate it from a generic `PublisherError`. Zulip returns no
 * distinguishing error code for any of them — only the human message string
 * differs — so the adapter classifies on that string, and any failure it does
 * not recognise stays a `PublisherError` (transient, retryable).
 */
export class MessageEditRefused extends Data.TaggedError('MessageEditRefused')<{
  readonly reason: 'editing-disabled' | 'window-expired' | 'not-original-sender'
  readonly cause: unknown
}> {
  override get message(): string {
    return Match.value(this.reason).pipe(
      Match.when(
        'editing-disabled',
        () =>
          'edit refused: this realm has message editing turned off (allow_message_editing), so no message on it is editable by anyone — re-post instead of editing; neither re-authoring nor waiting helps',
      ),
      Match.when(
        'window-expired',
        () =>
          "edit refused: the realm's message edit-window (message_content_edit_limit_seconds) has passed for this message — re-post instead of editing",
      ),
      Match.when(
        'not-original-sender',
        () =>
          "edit refused: only the original sender may edit this message, and a cross-session ephemeral seat can never edit a prior seat's message at any age — re-post, or ask the original author",
      ),
      Match.exhaustive,
    )
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
  readonly operation:
    | 'post'
    | 'edit'
    | 'react'
    | 'unreact'
    | 'resolveThread'
    | 'unresolveThread'
    | 'setChannelDescription'
    | 'editingAvailable'
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
