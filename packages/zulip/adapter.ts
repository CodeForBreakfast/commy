import type {
  AcquiredIdentity,
  AgentComms,
  BotName,
  ChannelDescription,
  ChannelId,
  ChannelName,
  ChannelRef,
  Credentials,
  Directory,
  HistoryReader,
  Identity,
  IdentityId,
  IdentityPort,
  InboundEvent,
  Message,
  MessageId,
  MessageInbox,
  MessagePublisher,
  MessageRef,
  PostOpts,
  Presence,
  Range,
  Reaction,
  RealmSettings,
  RecentThread,
  SubscriptionTarget,
  ThreadName,
  Timestamp as TimestampType,
} from '@commy/core/ports'
import {
  ChannelDescriptionRejected,
  DirectoryError,
  decodeChannelDescription,
  decodeChannelId,
  decodeChannelName,
  decodeDisplayName,
  decodeEmoji,
  decodeIdentityId,
  decodeMessageBody,
  decodeMessageId,
  decodeThreadName,
  decodeTimestamp,
  HistoryError,
  IdentityError,
  InboxError,
  MessageEditRefused,
  mentionsIdentity,
  PublisherError,
  UnknownChannel,
  UnresolvedMention,
} from '@commy/core/ports'
import { HttpClient } from '@effect/platform'
import {
  Array as Arr,
  Data,
  Duration,
  Effect,
  Equivalence,
  HashMap,
  HashSet,
  Option,
  Order,
  type ParseResult,
  Predicate,
  PubSub,
  Redacted,
  Ref,
  Schema,
  Stream,
  SynchronizedRef,
} from 'effect'
import type { BotHttp, RecipientDirectory } from './bot-dm-guard.ts'
import { wrapBotHttp } from './bot-dm-guard.ts'
import { fromWireDescription, rejectionFor, toWireDescription } from './channel-description.ts'
import type { QueueState } from './events.ts'
import {
  createMessageRefCache,
  createWatermarkStore,
  fetchMessageRef,
  inboxEvents,
  messageToInboundEvents,
  registerQueue,
  zulipMessageContentSchema,
} from './events.ts'
import type {
  ApiKey as ApiKeyType,
  BotEmail as BotEmailType,
  RawDownload,
  RealmUrl,
  UploadResult,
  UserUploadPath,
  ZulipHttpConfig,
} from './http.ts'
import { ApiKey, BotEmail, makeZulipHttp, ZulipApiError } from './http.ts'
import {
  extractMentions,
  type MentionDirectory,
  MentionToken,
  mentionTokens,
  unresolvedMentions,
} from './mentions.ts'
import type { ReconcileReport } from './minter-reconciler.ts'
import { reconcileMinterSubscriptions } from './minter-reconciler.ts'
import { buildMessageRef, permalinkBase, withChannelPermalink } from './permalink.ts'
import { applyResolvedPrefix, splitTopic } from './resolved-topic.ts'
import { senderNarrow, userPresencePath, ZulipUserRef } from './user-ref.ts'

export interface ZulipAdapterConfig {
  readonly realmUrl: RealmUrl
  /** Minter credentials. Owns POST /bots, regenerate, and DELETE /bots/{id}. */
  readonly minterEmail: BotEmailType
  /**
   * Minter API key wrapped in `Redacted` so the privileged secret masks to
   * `<redacted>` on any log / stringify / error rendering. It
   * is unwrapped via `Redacted.value` only at the single point the minter HTTP
   * client is constructed — the auth-header boundary.
   */
  readonly minterApiKey: Redacted.Redacted<ApiKeyType>
  /** Override outgoing Host header — required for cluster-internal callers. */
  readonly hostHeader?: string
  /**
   * Attach mode. When set, acquiring this exact bot name binds the
   * pre-provisioned persona using the supplied stable api key without
   * regenerating it — so many sessions/processes can share one identity (the
   * Discord-style single-identity model) with no acquire-time key rotation and
   * therefore no one-holder-per-name collision. Every other name keeps the
   * self-service mint/regenerate path. The key is `Redacted` for the same
   * secret-masking reason as `minterApiKey`.
   */
  readonly attachIdentity?: {
    readonly name: BotName
    readonly apiKey: Redacted.Redacted<ApiKeyType>
  }
  /**
   * Idle timeout (seconds) sent as `idle_queue_timeout` on every events-queue
   * register — both the eager subscribe-time register here and the producer's
   * own lazy / re-register path (threaded through {@link EventsConfig}). Set
   * for ephemeral seats so Zulip keeps the queue alive across a long idle;
   * omitted for persistent bots, which keep the server's default window.
   */
  readonly queueIdleTimeoutSecs?: number
  /**
   * Best-effort hook fired with the freshly registered queue at every register
   * site (eager subscribe-time + producer lazy/re-register). The wiring layer
   * persists `{queueId, lastEventId}` to the per-session queue-state store.
   * Total by contract (`Effect<void>`); the session gate / swallow / poll
   * discipline lives in the closure.
   */
  readonly onQueueRegister?: (queue: QueueState) => Effect.Effect<void>
  /**
   * Best-effort hook fired with the per-poll maximum event id whenever a poll
   * moves the cursor. The wiring layer advances the persisted `lastEventId`
   * (monotonic). Total by contract (`Effect<void>`).
   */
  readonly onQueueAdvance?: (lastEventId: number) => Effect.Effect<void>
  /**
   * Read half of long-idle queue resume: resolves the persisted queue-state a
   * resuming ephemeral seat should reuse, consulted once when the producer
   * materialises. A `Some` is handed to the producer as its `initialQueue`, so
   * it skips its own `POST /register` and resume-polls the surviving
   * server-side queue from the stored `lastEventId` — replaying the entire
   * missed backlog (messages AND reactions) through the normal producer path.
   * A `Some` wins over any eager subscribe-time registration: a boot-time
   * `COMMY_SUBSCRIBE` register must not shadow a live persisted queue. `None`
   * (fresh session, or the session id not yet known) leaves the register-time
   * queue / lazy-register path untouched. The wiring layer's closure owns the
   * session gate and the store read; total by contract (`Effect<Option>`).
   * Omitted for persistent bots — they never resume-poll.
   */
  readonly resumeQueue?: () => Effect.Effect<Option.Option<QueueState>>
  /**
   * Resume-verdict sink for the ephemeral history-catch-up fallback. Fired once
   * per pump lifetime: `false` the instant `resumeQueue` yields `None` (fresh
   * session / nothing persisted — the seat must run its normal boot catch-up);
   * otherwise wired through to the producer, which reports `true` when the
   * surviving queue's resume-poll succeeds (backlog replays natively — catch-up
   * must stand down) or `false` when it is dead (`BAD_EVENT_QUEUE_ID` — history
   * catch-up backfills the gap). The wiring layer resolves a shared `Deferred`
   * the seat's `onAcquire` awaits. Total by contract (`Effect<void>`). Omitted
   * for persistent bots — their boot catch-up is unconditional.
   */
  readonly onResumeOutcome?: (queueReplayed: boolean) => Effect.Effect<void>
}

export type ZulipAdapter = AgentComms & {
  /**
   * Subscribe the minter to every public stream it isn't yet on.
   * Boot-time backstop so the plugin's event pump observes
   * events on streams created after the minter's initial subscription
   * set. Non-throwing: failure is captured in the returned report and
   * the caller decides whether to log + continue or abort.
   */
  reconcileMinterSubscriptions(): Effect.Effect<ReconcileReport, never>
  downloadFile(urlPath: UserUploadPath): Effect.Effect<RawDownload, ZulipApiError>
  uploadFile(
    filename: string,
    data: Uint8Array,
  ): Effect.Effect<UploadResult, ZulipApiError | ParseResult.ParseError>
  close(): Promise<void>
}

/**
 * Raised when re-acquiring a deactivated bot needs the admin-only
 * `POST /users/{id}/reactivate` and the minter lacks the rights (a plain
 * Member, `is_admin` false). The underlying realm response is often the
 * opaque "Invalid API key" / "Must be an organization administrator", which
 * reads as a credential bug; this error states the real cause so the next
 * debugger is not sent chasing a phantom. The fix that removes
 * the dependency for pinned identities is `release({ persistent: true })` —
 * a persistent bot is never deactivated, so re-acquire stays on the
 * owner-permitted regenerate path and never reaches reactivate.
 */
class ReactivateForbidden extends Data.TaggedError('ReactivateForbidden')<{
  readonly userId: number
  readonly cause: ZulipApiError
}> {
  override get message(): string {
    return (
      `cannot reactivate deactivated bot ${this.userId}: the minter is not an ` +
      `organization administrator, and POST /users/{id}/reactivate is admin-only. ` +
      `Pin this identity with COMMY_BOT_NAME so it is never deactivated on ` +
      `release, or grant the minter admin rights. Underlying realm ` +
      `error: ${this.cause.message}`
    )
  }
}

/**
 * Raised when attach mode is configured for a persona that the
 * realm has no provisioned bot for. Attach binds an existing identity by its
 * supplied stable key — it never mints — so a missing persona is
 * an operator provisioning gap, not something to paper over by self-minting.
 */
class AttachIdentityNotFound extends Data.TaggedError('AttachIdentityNotFound')<{
  readonly name: BotName
}> {
  override get message(): string {
    return (
      `attach mode: no provisioned bot named '${this.name}' on the realm. ` +
      `Provision the persona once and supply its stable api key before attaching.`
    )
  }
}

const SHORT_NAME_PATTERN = /[^a-z0-9._-]/g

const sanitiseShortName = (name: string): string => {
  const collapsed = name.toLowerCase().replace(SHORT_NAME_PATTERN, '-')
  return collapsed.replace(/^-+|-+$/g, '') || 'bot'
}

/**
 * Wall-clock ceiling on the acquire mint round-trip. None of the minter HTTP
 * calls (`GET /users`, `POST /bots`, reactivate, regenerate) is otherwise
 * bounded, so a stalled socket parks `acquire` on `Effect.never` forever —
 * and because the round-trip runs while `boundRef` is write-locked, that one
 * hang wedges every later acquire, hence every `post`/`react`. Bounding it
 * turns a silent indefinite hang into a prompt `IdentityError` the plugin
 * layer can escalate.
 *
 * 30s clears the healthy p99 (a few sub-second sequential calls) with room to
 * spare, and tolerates a call riding most of its 429 retry budget
 * (`RATE_LIMIT_RETRY_BUDGET_MS` = 15s per call in http.ts) without a false
 * timeout — while staying firmly in "seconds, not minutes" so a human is never
 * left cancelling a wedged tool call by hand.
 */
const ACQUIRE_MINT_TIMEOUT = Duration.seconds(30)

const userSchema = Schema.Struct({
  user_id: Schema.Int,
  email: Schema.String,
  full_name: Schema.String,
  is_bot: Schema.Boolean,
  is_active: Schema.Boolean,
})

type ZulipUser = Schema.Schema.Type<typeof userSchema>

const usersSchema = Schema.Struct({
  result: Schema.Literal('success'),
  members: Schema.Array(userSchema),
})

const newBotSchema = Schema.Struct({
  result: Schema.Literal('success'),
  api_key: Schema.NonEmptyString,
  user_id: Schema.Int,
})

const regenerateKeySchema = Schema.Struct({
  result: Schema.Literal('success'),
  api_key: Schema.NonEmptyString,
})

const sentMessageSchema = Schema.Struct({
  result: Schema.Literal('success'),
  id: Schema.Int,
})

const successSchema = Schema.Struct({ result: Schema.Literal('success') })

/**
 * Classify a failed content edit. Zulip walls a content edit three ways —
 * realm editing disabled, edit-window expired, and not-the-original-sender —
 * but raises all three as a bare 400 with the same generic code; only the
 * human message string distinguishes them (verified against Zulip's
 * `validate_user_can_edit_message`). Match on stable substrings of those three
 * messages so a caller gets a typed `MessageEditRefused` reason; every other
 * failure (network, 5xx, an unrecognised 400) stays a generic, retryable
 * `PublisherError`.
 *
 * These strings are i18n'd by Zulip, so matching them only works because every
 * request pins `Accept-Language: en` (see `makeZulipHttp`) — that is what keeps
 * the responses English regardless of the realm's or the bot's own language.
 */
const classifyEditFailure = (cause: unknown): MessageEditRefused | PublisherError => {
  if (cause instanceof ZulipApiError) {
    const text = cause.message.toLowerCase()
    if (text.includes('turned off message editing')) {
      return new MessageEditRefused({ reason: 'editing-disabled', cause })
    }
    if (text.includes('time limit for editing this message')) {
      return new MessageEditRefused({ reason: 'window-expired', cause })
    }
    if (text.includes('permission to edit this message')) {
      return new MessageEditRefused({ reason: 'not-original-sender', cause })
    }
  }
  return new PublisherError({ operation: 'edit', cause })
}

interface HistoricalReaction {
  readonly userId: ZulipUserRef
  readonly emojiName: string
}

interface HistoricalMessage {
  readonly id: MessageId
  readonly channelId: ChannelId
  readonly channelName: ChannelName
  readonly senderId: ZulipUserRef
  readonly senderFullName: string
  readonly subject: ThreadName
  readonly resolved: boolean
  readonly content: string
  readonly ts: TimestampType
  readonly reactions: ReadonlyArray<HistoricalReaction>
}

const historicalReactionSchema = Schema.Struct({
  user_id: Schema.Int,
  emoji_name: Schema.String,
})

const historicalMessageRawSchema = Schema.Struct({
  id: Schema.Int,
  stream_id: Schema.Int,
  display_recipient: Schema.String,
  sender_id: Schema.Int,
  sender_full_name: Schema.String,
  subject: Schema.NonEmptyString,
  content: Schema.String,
  timestamp: Schema.NonNegative,
  reactions: Schema.optional(Schema.Array(historicalReactionSchema)),
})

const toHistoricalReaction = (
  r: Schema.Schema.Type<typeof historicalReactionSchema>,
): HistoricalReaction => ({ userId: ZulipUserRef(r.user_id), emojiName: r.emoji_name })

const toHistoricalMessage = (
  m: Schema.Schema.Type<typeof historicalMessageRawSchema>,
): Effect.Effect<HistoricalMessage, ParseResult.ParseError> => {
  // A resolved topic reaches us as a ✔-prefixed subject; split the marker off
  // here so the port only ever sees the clean name plus the resolved flag.
  const { name, resolved } = splitTopic(m.subject)
  return Effect.all({
    id: decodeMessageId(String(m.id)),
    channelId: decodeChannelId(String(m.stream_id)),
    channelName: decodeChannelName(m.display_recipient),
    subject: decodeThreadName(name),
    ts: decodeTimestamp(m.timestamp),
  }).pipe(
    Effect.map(
      ({ id, channelId, channelName, subject, ts }): HistoricalMessage => ({
        id,
        channelId,
        channelName,
        senderId: ZulipUserRef(m.sender_id),
        senderFullName: m.sender_full_name,
        subject,
        resolved,
        content: m.content,
        ts,
        reactions: (m.reactions ?? []).map(toHistoricalReaction),
      }),
    ),
  )
}

const messagesResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  messages: Schema.Array(historicalMessageRawSchema),
})

const senderMessageSchema = Schema.Struct({
  subject: Schema.NonEmptyString,
  display_recipient: Schema.String,
  content: Schema.String,
  timestamp: Schema.NonNegative,
})

const senderMessagesResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  messages: Schema.Array(senderMessageSchema),
})

const RECENT_THREADS_DEFAULT_LIMIT = 10
const RECENT_THREADS_FETCH_LIMIT = 50
const HISTORY_DEFAULT_LIMIT = 100

/**
 * The slice of Zulip's `/register` initial state that carries the realm-wide
 * editing switch. Zulip exposes realm settings on no GET endpoint at all
 * (`rest_path("realm", PATCH=update_realm)` is write-only), so `/register`
 * with `fetch_event_types` is the only read path. `fetch_event_types` selects
 * the returned state independently of `event_types`, which selects what the
 * queue receives — so asking for `[]` events and `['realm']` state fetches the
 * setting without subscribing to anything.
 */
const realmEditingStateSchema = Schema.Struct({
  queue_id: Schema.String,
  realm_allow_message_editing: Schema.Boolean,
})

const presenceStatusSchema = Schema.Literal('active', 'idle', 'offline')
type ZulipPresenceStatus = Schema.Schema.Type<typeof presenceStatusSchema>

const presenceResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  presence: Schema.Struct({
    aggregated: Schema.optional(Schema.Struct({ status: presenceStatusSchema })),
  }),
})

const toPresence = (status: ZulipPresenceStatus): Presence => {
  if (status === 'active') return 'online'
  if (status === 'idle') return 'idle'
  return 'offline'
}

type NarrowFilter =
  | { readonly operator: 'channel'; readonly operand: ChannelName }
  // A topic narrow matches the raw substrate topic, which resolution decorates
  // with the ✔ prefix — so the operand is a raw topic string, not a clean
  // port-facing ThreadName.
  | { readonly operator: 'topic'; readonly operand: string }

const inRange =
  (range: Range) =>
  (m: HistoricalMessage): boolean => {
    if (range.since !== undefined && m.ts < range.since) return false
    if (range.until !== undefined && m.ts > range.until) return false
    return true
  }

const toIdentity = (u: ZulipUser): Effect.Effect<Identity, ParseResult.ParseError> =>
  Effect.all({
    id: decodeIdentityId(String(u.user_id)),
    name: decodeDisplayName(u.full_name),
  }).pipe(
    Effect.map(
      ({ id, name }): Identity => ({
        id,
        name,
        kind: u.is_bot ? 'agent' : 'human',
      }),
    ),
  )

const isActiveOfKind =
  (kind: 'agent' | 'human') =>
  (u: ZulipUser): boolean =>
    u.is_active && (kind === 'agent' ? u.is_bot : !u.is_bot)

interface BoundState {
  readonly acquiredName: BotName
  readonly identity: Identity
  readonly credentials: Credentials
  readonly userId: ZulipUserRef
  readonly http: BotHttp
}

// Channels are addressed by name, so the narrow sets and the per-channel
// seen-topics ledger key on `ChannelName` — the same address the subscription
// target carries and that inbound messages expose as `ref.channel.name`.
interface InboxState {
  readonly subscribedChannels: HashSet.HashSet<ChannelName>
  readonly newTopicsChannels: HashSet.HashSet<ChannelName>
  readonly seenTopicsByChannel: HashMap.HashMap<ChannelName, HashSet.HashSet<ThreadName>>
  readonly registration: Option.Option<QueueState>
}

const streamIsListening = (state: InboxState, channelName: ChannelName): boolean =>
  HashSet.has(state.subscribedChannels, channelName) ||
  HashSet.has(state.newTopicsChannels, channelName)

// Record (channelName, threadName) in the per-channel seen set, returning
// whether this is the first observation of the topic and the next state.
const observeNewTopic = (
  state: InboxState,
  channelName: ChannelName,
  threadName: ThreadName,
): readonly [boolean, InboxState] => {
  const seen = HashMap.get(state.seenTopicsByChannel, channelName).pipe(
    Option.getOrElse(() => HashSet.empty<ThreadName>()),
  )
  if (HashSet.has(seen, threadName)) return [false, state]
  return [
    true,
    {
      ...state,
      seenTopicsByChannel: HashMap.set(
        state.seenTopicsByChannel,
        channelName,
        HashSet.add(seen, threadName),
      ),
    },
  ]
}

const buildHttpConfig = (
  realmUrl: RealmUrl,
  email: BotEmailType,
  apiKey: ApiKeyType,
  hostHeader: string | undefined,
): ZulipHttpConfig =>
  hostHeader === undefined ? { realmUrl, email, apiKey } : { realmUrl, email, apiKey, hostHeader }

export const zulipAdapter = (
  config: ZulipAdapterConfig,
): Effect.Effect<ZulipAdapter, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    // Read HttpClient from context once, at construction. The minter client
    // gets it via this gen's requirements; per-bot clients minted later in
    // `acquire` are built against this captured value so their construction
    // doesn't re-enter the requirements channel (request-time DI is a
    // separate concern — out of scope here).
    const httpClient = yield* HttpClient.HttpClient
    const minterHttp = yield* makeZulipHttp(
      buildHttpConfig(
        config.realmUrl,
        config.minterEmail,
        Redacted.value(config.minterApiKey),
        config.hostHeader,
      ),
    )
    const boundRef = yield* SynchronizedRef.make<Option.Option<BoundState>>(Option.none())

    const requireBound = (): Effect.Effect<BoundState> =>
      SynchronizedRef.get(boundRef).pipe(
        Effect.map(
          Option.getOrThrowWith(
            () => new Error('zulipAdapter: not acquired — call identity.acquire(name) first'),
          ),
        ),
      )

    // boundHttp is reserved for publisher operations — the three
    // attribution-producing verbs (post / react / unreact) that leave a
    // persistent signal on the substrate attributed to this session's
    // bot. Everything else (inbox / history / directory) flows through
    // `minterHttp` so the plugin can run pre-acquire.
    // The bound HTTP is wrapped by bot-dm-guard so any future code that
    // constructs a `POST /messages` with `type=private` and an all-bot
    // recipient list is rejected before reaching the wire.
    const boundHttp = (): Effect.Effect<BotHttp> => requireBound().pipe(Effect.map((b) => b.http))

    const fetchMembers = (): Effect.Effect<
      ReadonlyArray<ZulipUser>,
      ZulipApiError | ParseResult.ParseError
    > => minterHttp.get('/users', usersSchema).pipe(Effect.map((res) => res.members))

    interface DirectoryLookup {
      readonly byId: ReadonlyMap<ZulipUserRef, Identity>
      readonly byName: ReadonlyMap<string, Identity>
      readonly byIdentityId: ReadonlyMap<IdentityId, ZulipUserRef>
    }

    const buildDirectoryLookup = (): Effect.Effect<
      DirectoryLookup,
      ZulipApiError | ParseResult.ParseError
    > =>
      fetchMembers().pipe(
        Effect.flatMap((members) =>
          Effect.gen(function* () {
            const byId = new Map<ZulipUserRef, Identity>()
            const byName = new Map<string, Identity>()
            const byIdentityId = new Map<IdentityId, ZulipUserRef>()
            for (const u of members) {
              const ident = yield* toIdentity(u)
              byId.set(ZulipUserRef(u.user_id), ident)
              byName.set(u.full_name, ident)
              byIdentityId.set(ident.id, ZulipUserRef(u.user_id))
            }
            return { byId, byName, byIdentityId }
          }),
        ),
      )

    // Recipient directory shape consumed by bot-dm-guard — narrower than
    // the full DirectoryLookup so the wrapping helper stays decoupled.
    const recipientDirectory = (): Effect.Effect<
      RecipientDirectory,
      ZulipApiError | ParseResult.ParseError
    > =>
      buildDirectoryLookup().pipe(
        Effect.map((lookup) => ({
          byId: new Map(
            [...lookup.byId.entries()].map(([id, ident]) => [id, { kind: ident.kind } as const]),
          ),
        })),
      )

    // A directory the shared mention helpers can resolve against — the
    // name-keyed map plus an id resolver for the disambiguated `@**Name|id**`
    // form, adapting this path's `ZulipUserRef`-keyed `byId` to a raw id.
    const mentionDirectory = (directory: DirectoryLookup): MentionDirectory => ({
      byName: directory.byName,
      byUserId: (userId) => directory.byId.get(ZulipUserRef(userId)),
    })

    // Write-path mention pre-flight: a `@**Name**` token in an outbound body
    // that resolves to no known identity would be posted verbatim and notify
    // nobody — Zulip accepts it silently. Reject the write instead. The token
    // scan is markdown-aware (a dead form quoted inside a code span is literal
    // text Zulip never delivers, not a failed mention) and runs first so a
    // mention-free body — the common case — never pays for a directory fetch
    // onto the rate-limited realm. Only user tokens can be dead, so a body
    // whose only mentions are wildcards or groups skips the fetch too.
    const validateOutboundMentions = (
      operation: 'post' | 'edit',
      body: string,
    ): Effect.Effect<void, UnresolvedMention | ZulipApiError | ParseResult.ParseError> =>
      !mentionTokens(body).some(MentionToken.$is('UserToken'))
        ? Effect.void
        : buildDirectoryLookup().pipe(
            Effect.flatMap((directory) => {
              const dead = unresolvedMentions(body, mentionDirectory(directory))
              return dead.length === 0
                ? Effect.void
                : Effect.fail(
                    new UnresolvedMention({ operation, tokens: dead, substrate: 'zulip' }),
                  )
            }),
          )

    const mapHistoricalReactions = (
      raw: ReadonlyArray<HistoricalReaction>,
      directory: DirectoryLookup,
    ): Effect.Effect<ReadonlyArray<Reaction>, ParseResult.ParseError> =>
      Effect.gen(function* () {
        const byEmoji = new Map<string, Identity[]>()
        for (const r of raw) {
          const cached = directory.byId.get(r.userId)
          const ident: Identity =
            cached ??
            (yield* Effect.all({
              id: decodeIdentityId(String(r.userId)),
              name: decodeDisplayName(`user-${r.userId}`),
            }).pipe(Effect.map(({ id, name }): Identity => ({ id, name, kind: 'human' }))))
          const list = byEmoji.get(r.emojiName) ?? []
          if (!list.some((i) => i.id === ident.id)) list.push(ident)
          byEmoji.set(r.emojiName, list)
        }
        const out: Reaction[] = []
        for (const [emoji, by] of byEmoji) {
          out.push({ emoji: yield* decodeEmoji(emoji), by })
        }
        return out
      })

    const mapHistoricalMessage = (
      m: HistoricalMessage,
      directory: DirectoryLookup,
    ): Effect.Effect<Message, ParseResult.ParseError> =>
      Effect.gen(function* () {
        const channel = decorateChannel({ id: m.channelId, name: m.channelName })
        const cached = directory.byId.get(m.senderId)
        const sender: Identity =
          cached ??
          (yield* Effect.all({
            id: decodeIdentityId(String(m.senderId)),
            name: decodeDisplayName(m.senderFullName),
          }).pipe(Effect.map(({ id, name }): Identity => ({ id, name, kind: 'human' }))))
        const body = yield* decodeMessageBody(m.content)
        const reactions = yield* mapHistoricalReactions(m.reactions, directory)
        return {
          ref: decorateMessageRef(m.id, channel, { name: m.subject, resolved: m.resolved }),
          sender,
          body,
          ts: m.ts,
          mentions: yield* extractMentions(m.content, mentionDirectory(directory)),
          reactions,
        }
      })

    const readMessages = (
      range: Range,
      narrow: ReadonlyArray<NarrowFilter>,
    ): Effect.Effect<ReadonlyArray<Message>, ZulipApiError | ParseResult.ParseError> =>
      Effect.all(
        [
          buildDirectoryLookup(),
          minterHttp.get('/messages', messagesResponseSchema, {
            anchor: 'newest',
            num_before: range.limit ?? HISTORY_DEFAULT_LIMIT,
            num_after: 0,
            narrow: JSON.stringify(narrow),
            apply_markdown: false,
          }),
        ],
        { concurrency: 2 },
      ).pipe(
        Effect.flatMap(([directory, res]) =>
          Effect.forEach(res.messages, toHistoricalMessage).pipe(
            Effect.map((historical) => historical.filter(inRange(range))),
            Effect.flatMap((inRangeMessages) =>
              Effect.forEach(inRangeMessages, (m) => mapHistoricalMessage(m, directory)),
            ),
          ),
        ),
      )

    // Zulip constructs bot delivery emails as `<short_name>-bot@<bot_domain>`
    // (see zerver/lib/users.py:validate_short_name_and_construct_bot_email),
    // and on standard installs `bot_domain == realm_host`. POST /bots returns
    // user_id + api_key but not the email — we mirror Zulip's algorithm
    // client-side rather than spend an extra round-trip on /users/{id} (whose
    // email field is subject to email_address_visibility settings and may
    // return the privacy alias `user{id}@…`, which doesn't authenticate).
    const realmHost = new URL(config.realmUrl).hostname

    // Human-facing realm origin for permalinks (public host when a Host-header
    // override is in play). Every ref this adapter hands back is decorated with
    // its narrow URL so callers can quote a clickable link.
    const base = permalinkBase(config)
    const decorateChannel = (channel: {
      readonly id: ChannelId
      readonly name: ChannelName
    }): ChannelRef => withChannelPermalink(base, channel)
    const decorateMessageRef = (
      id: MessageId,
      channel: ChannelRef,
      thread: { readonly name: ThreadName; readonly resolved: boolean } | undefined,
    ): MessageRef => buildMessageRef(base, id, channel, thread)

    const buildBotEmail = (
      shortName: string,
    ): Effect.Effect<BotEmailType, ParseResult.ParseError> =>
      BotEmail(`${shortName}-bot@${realmHost}`)

    const findAnyBotByName = (
      name: BotName,
    ): Effect.Effect<Option.Option<ZulipUser>, ZulipApiError | ParseResult.ParseError> =>
      // GET /users returns active AND inactive users. We need both to
      // implement reactivation: a deactivated bot's email is still
      // reserved (Zulip blocks mint with EmailAlreadyInUseError), so the
      // only path back is reactivate-then-regenerate, never re-mint.
      minterHttp
        .get('/users', usersSchema)
        .pipe(
          Effect.map((res) =>
            Option.fromNullable(res.members.find((u) => u.full_name === name && u.is_bot)),
          ),
        )

    interface MintedBot {
      readonly userId: ZulipUserRef
      readonly apiKey: ApiKeyType
      readonly email: BotEmailType
    }

    const mintBot = (
      name: BotName,
    ): Effect.Effect<MintedBot, ZulipApiError | ParseResult.ParseError> => {
      const shortName = sanitiseShortName(name)
      return minterHttp
        .post('/bots', newBotSchema, {
          full_name: name,
          short_name: shortName,
          bot_type: 1,
        })
        .pipe(
          Effect.flatMap((res) =>
            Effect.all({
              email: buildBotEmail(shortName),
              apiKey: ApiKey(res.api_key),
            }).pipe(
              Effect.map(
                ({ email, apiKey }): MintedBot => ({
                  userId: ZulipUserRef(res.user_id),
                  apiKey,
                  email,
                }),
              ),
            ),
          ),
        )
    }

    const reactivateBot = (
      existing: ZulipUser,
    ): Effect.Effect<void, ReactivateForbidden | ParseResult.ParseError> =>
      minterHttp.post(`/users/${existing.user_id}/reactivate`, successSchema, {}).pipe(
        Effect.asVoid,
        // POST /users/{id}/reactivate is admin-only; a Member-rights minter is
        // refused. Re-flag the failure as ReactivateForbidden so the surfaced
        // IdentityError names the real cause instead of the opaque realm error.
        Effect.catchTag('ZulipApiError', (cause) =>
          Effect.fail(new ReactivateForbidden({ userId: existing.user_id, cause })),
        ),
      )

    const regenerateBotKey = (
      existing: ZulipUser,
    ): Effect.Effect<MintedBot, ZulipApiError | ParseResult.ParseError> =>
      minterHttp.post(`/bots/${existing.user_id}/api_key/regenerate`, regenerateKeySchema, {}).pipe(
        Effect.flatMap((res) =>
          Effect.all({
            email: BotEmail(existing.email),
            apiKey: ApiKey(res.api_key),
          }).pipe(
            Effect.map(
              ({ email, apiKey }): MintedBot => ({
                userId: ZulipUserRef(existing.user_id),
                apiKey,
                email,
              }),
            ),
          ),
        ),
      )

    const acquireBot = (
      name: BotName,
    ): Effect.Effect<MintedBot, ZulipApiError | ReactivateForbidden | ParseResult.ParseError> =>
      findAnyBotByName(name).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => mintBot(name),
            onSome: (existing) => {
              const reactivate = existing.is_active ? Effect.void : reactivateBot(existing)
              return reactivate.pipe(Effect.andThen(regenerateBotKey(existing)))
            },
          }),
        ),
      )

    // Attach mode: bind the pre-provisioned persona with the
    // supplied stable key, never regenerating. No key rotation means no holder
    // of this identity is invalidated, so the listener and poster (and every
    // per-topic session) can share one identity without the one-holder-per-name collision.
    const attachBot = (
      name: BotName,
      apiKey: ApiKeyType,
    ): Effect.Effect<MintedBot, ZulipApiError | AttachIdentityNotFound | ParseResult.ParseError> =>
      findAnyBotByName(name).pipe(
        Effect.flatMap(
          Option.match({
            onNone: (): Effect.Effect<MintedBot, AttachIdentityNotFound | ParseResult.ParseError> =>
              Effect.fail(new AttachIdentityNotFound({ name })),
            onSome: (
              existing,
            ): Effect.Effect<MintedBot, AttachIdentityNotFound | ParseResult.ParseError> =>
              BotEmail(existing.email).pipe(
                Effect.map(
                  (email): MintedBot => ({
                    userId: ZulipUserRef(existing.user_id),
                    apiKey,
                    email,
                  }),
                ),
              ),
          }),
        ),
      )

    // Pick the provider for a fresh bind: attach when this exact name is the
    // configured attach persona, else the self-service mint/regenerate.
    const provideMintedFor = (
      name: BotName,
    ): Effect.Effect<
      MintedBot,
      ZulipApiError | ReactivateForbidden | AttachIdentityNotFound | ParseResult.ParseError
    > => {
      const attach = config.attachIdentity
      return attach !== undefined && attach.name === name
        ? attachBot(name, Redacted.value(attach.apiKey))
        : acquireBot(name)
    }

    const identity: IdentityPort = {
      // Pre-acquire callers (the plugin-layer current_identity tool
      // reads through ensureBound.current() and never invokes this) still
      // need a coherent error — dying matches the port
      // contract of "throws when unbound".
      currentIdentity: () => requireBound().pipe(Effect.map((b) => b.identity)),
      // check-bound -> mint -> set is one atomic SynchronizedRef transition:
      // the effectful mint round-trip runs while the ref is locked, so two
      // concurrent acquires can never both observe `none` and double-mint.
      acquire: (name) =>
        SynchronizedRef.modifyEffect(boundRef, (current) =>
          Option.match(current, {
            onSome: (existing) => {
              if (existing.acquiredName === name) {
                return Effect.succeed<readonly [AcquiredIdentity, Option.Option<BoundState>]>([
                  { identity: existing.identity, credentials: existing.credentials },
                  current,
                ])
              }
              return Effect.die(
                new Error(
                  `zulipAdapter: already bound to ${existing.acquiredName} — release() before acquiring ${name}`,
                ),
              )
            },
            onNone: () =>
              provideMintedFor(name).pipe(
                // A minter call that never returns must not park acquire (and
                // the held boundRef lock) indefinitely — bound the round-trip
                // so the timeout surfaces as the IdentityError mapped below.
                Effect.timeout(ACQUIRE_MINT_TIMEOUT),
                Effect.flatMap((minted) =>
                  Effect.gen(function* () {
                    const innerHttp = yield* makeZulipHttp(
                      buildHttpConfig(
                        config.realmUrl,
                        minted.email,
                        minted.apiKey,
                        config.hostHeader,
                      ),
                    ).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))
                    const http = wrapBotHttp(innerHttp, recipientDirectory, minted.userId)
                    const ident: Identity = {
                      id: yield* decodeIdentityId(String(minted.userId)),
                      name: yield* decodeDisplayName(name),
                      kind: 'agent',
                    }
                    const credentials: Credentials = {
                      substrate: 'zulip',
                      realmUrl: config.realmUrl,
                      email: minted.email,
                      apiKey: minted.apiKey,
                    }
                    const next: BoundState = {
                      acquiredName: name,
                      identity: ident,
                      credentials,
                      userId: minted.userId,
                      http,
                    }
                    return [{ identity: ident, credentials }, Option.some(next)] as const
                  }),
                ),
                Effect.mapError((cause) => new IdentityError({ operation: 'acquire', cause })),
              ),
          }),
        ),
      release: (opts) =>
        SynchronizedRef.modifyEffect(boundRef, (current) =>
          Option.match(current, {
            onNone: () => Effect.succeed([undefined, current] as const),
            // A persistent identity (COMMY_BOT_NAME-pinned) must stay
            // active: deactivating it forces the next acquire onto the
            // admin-only reactivate path, which wedges a Member-rights minter.
            // Clear the binding either way; only skip the
            // substrate-side deactivate. Deactivation failure is non-fatal —
            // the bot expires via the realm's idle GC, so we swallow it and
            // release always resolves cleanly.
            onSome: (existing) =>
              (opts?.persistent === true
                ? Effect.void
                : minterHttp.delete(`/bots/${existing.userId}`, successSchema).pipe(
                    Effect.asVoid,
                    Effect.catchAll(() => Effect.void),
                  )
              ).pipe(Effect.as([undefined, Option.none<BoundState>()] as const)),
          }),
        ).pipe(Effect.asVoid),
      resolve: (name) =>
        minterHttp.get('/users', usersSchema).pipe(
          Effect.flatMap((res) =>
            Arr.findFirst(res.members, (u) => u.is_active && u.full_name === name).pipe(
              Option.match({
                onNone: () => Effect.succeed(Option.none<Identity>()),
                onSome: (match) => toIdentity(match).pipe(Effect.map(Option.some)),
              }),
            ),
          ),
          Effect.mapError((cause) => new IdentityError({ operation: 'resolve', cause })),
        ),
    }

    const directory: Directory = {
      listAgents: () =>
        fetchMembers().pipe(
          Effect.flatMap((members) =>
            Effect.forEach(members.filter(isActiveOfKind('agent')), toIdentity),
          ),
          Effect.mapError((cause) => new DirectoryError({ operation: 'listAgents', cause })),
        ),
      listHumans: () =>
        fetchMembers().pipe(
          Effect.flatMap((members) =>
            Effect.forEach(members.filter(isActiveOfKind('human')), toIdentity),
          ),
          Effect.mapError((cause) => new DirectoryError({ operation: 'listHumans', cause })),
        ),
      listChannels: () =>
        listKnownChannels().pipe(
          Effect.mapError((cause) => new DirectoryError({ operation: 'listChannels', cause })),
        ),
      channelDescription: (channel) =>
        resolvePublishChannel(channel).pipe(
          Effect.flatMap(fetchChannelDescription),
          Effect.mapError((cause) =>
            cause instanceof UnknownChannel
              ? cause
              : new DirectoryError({ operation: 'channelDescription', cause }),
          ),
        ),
      // Zulip presence is human-only by design — POST /users/me/presence is
      // @human_users_only, so a bot has no presence record and a read would
      // 400 into a misleading 'offline'. An agent's presence is genuinely
      // unknowable here, so short-circuit to 'unknown' before any lookup or
      // GET. Only humans run the read path below.
      //
      // The human path needs a ZulipUserRef (integer user id), so it resolves
      // the IdentityId through the directory first; an unresolvable identity
      // has no Zulip user and is functionally offline. Both fetches
      // are lifted into the Effect error channel so the 400 recovery below is a
      // declarative `catchIf` rather than nested try/catch, and anything left
      // over surfaces as a typed DirectoryError.
      presence: (identity) =>
        identity.kind === 'agent'
          ? Effect.succeed<Presence>('unknown')
          : buildDirectoryLookup().pipe(
              Effect.flatMap((lookup) => {
                const ref = lookup.byIdentityId.get(identity.id)
                if (ref === undefined) return Effect.succeed<Presence>('offline')
                return minterHttp.get(userPresencePath(ref), presenceResponseSchema).pipe(
                  Effect.map((res): Presence => {
                    const status = res.presence.aggregated?.status
                    return status === undefined ? 'offline' : toPresence(status)
                  }),
                  // Zulip raises 400 BAD_REQUEST for both "No presence data for
                  // {user}" and "No such user", distinguishable only by free-form
                  // `msg`. A user we cannot read presence for is functionally
                  // offline, so recover both rather than parse the text.
                  Effect.catchIf(
                    (cause): cause is ZulipApiError =>
                      cause instanceof ZulipApiError &&
                      cause.status === 400 &&
                      cause.code === 'BAD_REQUEST',
                    () => Effect.succeed<Presence>('offline'),
                  ),
                )
              }),
              Effect.mapError((cause) => new DirectoryError({ operation: 'presence', cause })),
            ),
    }

    const channelOf = (target: SubscriptionTarget): ChannelName => {
      if (Predicate.hasProperty(target, 'kind')) return target.channel
      if (Predicate.hasProperty(target, 'thread')) return target.channel
      return target
    }

    const subscriptionsResponseSchema = Schema.Struct({ result: Schema.Literal('success') })

    // POST /users/me/subscriptions response carries a per-user map of
    // names actually subscribed vs already subscribed. For minter-routed
    // calls we only care about the minter's row; defaults to empty so
    // the schema parses cleanly when the realm omits the minter (a race
    // where every requested stream was already subscribed by someone
    // else in the interim).
    const reconcileSubscriptionsResponseSchema = Schema.Struct({
      result: Schema.Literal('success'),
      subscribed: Schema.optional(
        Schema.Record({ key: Schema.String, value: Schema.Array(Schema.String) }),
      ),
      already_subscribed: Schema.optional(
        Schema.Record({ key: Schema.String, value: Schema.Array(Schema.String) }),
      ),
    })

    const streamsListResponseSchema = Schema.Struct({
      result: Schema.Literal('success'),
      streams: Schema.Array(Schema.Struct({ name: Schema.NonEmptyString, stream_id: Schema.Int })),
    })

    // GET /streams/{id}. Read per-channel rather than off the streams list so a
    // description reflects the realm now, not whenever the name→id cache was
    // last filled.
    const streamResponseSchema = Schema.Struct({
      result: Schema.Literal('success'),
      stream: Schema.Struct({ description: Schema.String }),
    })

    const REPLAY_NUM_BEFORE = 1000

    // The replay-fetch schema decorates the message-content shape with the
    // `flags` array; the iterator strips `flags` before mapping. Defined
    // inline (not via .extend on the imported struct) so the resulting
    // Schema is a plain Struct that decodeUnknown accepts directly.
    const replayMessageSchema = Schema.Struct({
      ...zulipMessageContentSchema.fields,
      flags: Schema.optional(Schema.Array(Schema.String)),
    })

    const replayResponseSchema = Schema.Struct({
      result: Schema.Literal('success'),
      messages: Schema.Array(replayMessageSchema),
    })

    // Adapter-scoped cache of streams the realm knows about, keyed by name.
    // Loaded lazily on first miss and refreshed once per miss so newly-created
    // streams become visible without restarting the adapter. Used both to
    // pre-flight `publisher.post` (so unknown channels surface as
    // UnknownChannel instead of being silently routed by Zulip to Notification
    // Bot DMs) and to back `directory.listChannels`.
    const knownStreamsRef = yield* Ref.make<Option.Option<Map<string, ChannelRef>>>(Option.none())
    const fetchKnownStreams = (): Effect.Effect<
      Map<string, ChannelRef>,
      ZulipApiError | ParseResult.ParseError
    > =>
      minterHttp
        .get('/streams', streamsListResponseSchema, {
          include_public: true,
          include_subscribed: true,
        })
        .pipe(
          Effect.flatMap((res) =>
            Effect.gen(function* () {
              const map = new Map<string, ChannelRef>()
              for (const s of res.streams) {
                map.set(
                  s.name,
                  decorateChannel({
                    id: yield* decodeChannelId(String(s.stream_id)),
                    name: yield* decodeChannelName(s.name),
                  }),
                )
              }
              return map
            }),
          ),
        )

    // Refetch from the realm and overwrite the cache; returns the fresh map.
    const refreshKnownStreams = (): Effect.Effect<
      Map<string, ChannelRef>,
      ZulipApiError | ParseResult.ParseError
    > => fetchKnownStreams().pipe(Effect.tap((map) => Ref.set(knownStreamsRef, Option.some(map))))

    // Resolve to the cache that should answer the lookup: a cold cache is
    // filled first, then a miss (cold-fill that still lacks the name, or a
    // warm cache that lacks it) triggers exactly one refresh so streams
    // created since the last fetch become visible. Returns the realm's ref for
    // the name (carrying its real stream id) or None when no such stream
    // exists.
    const lookupChannel = (
      name: string,
    ): Effect.Effect<Option.Option<ChannelRef>, ZulipApiError | ParseResult.ParseError> =>
      Ref.get(knownStreamsRef).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => refreshKnownStreams(),
            onSome: Effect.succeed,
          }),
        ),
        Effect.flatMap((map) => (map.has(name) ? Effect.succeed(map) : refreshKnownStreams())),
        Effect.map((map) => Option.fromNullable(map.get(name))),
      )

    // Confirm the requested channel exists so an unknown channel surfaces as
    // UnknownChannel instead of being silently routed by Zulip to Notification
    // Bot DMs.
    const resolvePublishChannel = (
      requested: ChannelName,
    ): Effect.Effect<ChannelRef, UnknownChannel | ZulipApiError | ParseResult.ParseError> =>
      lookupChannel(requested).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(new UnknownChannel({ channel: requested, substrate: 'zulip' })),
            onSome: Effect.succeed,
          }),
        ),
      )

    // A channel's stored description, read fresh from the realm. The name→id
    // cache only supplies the id; the description itself is never cached,
    // because a caller reading one wants the realm's current answer.
    const fetchChannelDescription = (
      channel: ChannelRef,
    ): Effect.Effect<Option.Option<ChannelDescription>, ZulipApiError | ParseResult.ParseError> =>
      minterHttp.get(`/streams/${encodeURIComponent(channel.id)}`, streamResponseSchema).pipe(
        Effect.flatMap((res) =>
          Option.match(fromWireDescription(res.stream.description), {
            onNone: () => Effect.succeed(Option.none<ChannelDescription>()),
            onSome: (raw) => decodeChannelDescription(raw).pipe(Effect.map(Option.some)),
          }),
        ),
      )

    // Zulip expresses "set the description" as a stream update, and has no
    // separate clear — an empty `description` is how a stream becomes
    // undescribed, which `toWireDescription` maps `Option.none()` onto.
    //
    // Runs as the bound identity, not the minter: editing a channel is a
    // permission the acting bot either has or lacks, and a realm that refuses
    // it must surface that refusal to the caller rather than be worked around
    // with a more privileged credential. Zulip answers a non-administrator
    // with a 400, which becomes a PublisherError carrying its message.
    //
    // Idempotent by pre-read: a description already equal to the requested one
    // short-circuits before any write, so re-running a charter update is free
    // and leaves no stream-update event behind.
    const setChannelDescription = (
      channel: ChannelName,
      description: Option.Option<ChannelDescription>,
    ): Effect.Effect<void, PublisherError | UnknownChannel | ChannelDescriptionRejected> =>
      resolvePublishChannel(channel).pipe(
        Effect.flatMap(
          (
            channelRef,
          ): Effect.Effect<
            void,
            ChannelDescriptionRejected | ZulipApiError | ParseResult.ParseError
          > =>
            Option.match(
              Option.flatMap(description, (text) => rejectionFor(channelRef.name, text)),
              {
                onSome: (rejection) => Effect.fail(rejection),
                onNone: () =>
                  fetchChannelDescription(channelRef).pipe(
                    Effect.flatMap((current) =>
                      Option.getEquivalence(Equivalence.string)(current, description)
                        ? Effect.void
                        : boundHttp().pipe(
                            Effect.flatMap((http) =>
                              http.patch(
                                `/streams/${encodeURIComponent(channelRef.id)}`,
                                successSchema,
                                { description: toWireDescription(description) },
                              ),
                            ),
                            Effect.asVoid,
                          ),
                    ),
                  ),
              },
            ),
        ),
        Effect.mapError((cause) =>
          cause instanceof UnknownChannel || cause instanceof ChannelDescriptionRejected
            ? cause
            : new PublisherError({ operation: 'setChannelDescription', cause }),
        ),
      )

    // listChannels is a discovery surface — callers want a fresh snapshot,
    // not whatever the pre-flight cache happens to hold. Always re-fetches
    // and updates the cache as a side effect so subsequent pre-flights are
    // also working from the latest data.
    const listKnownChannels = (): Effect.Effect<
      ReadonlyArray<ChannelRef>,
      ZulipApiError | ParseResult.ParseError
    > => refreshKnownStreams().pipe(Effect.map((map) => [...map.values()]))

    // The newest message id in a raw topic, or None when the topic holds no
    // messages. Resolution edits target any message in the topic (change_all
    // fans out to the rest), so the setter uses this to find one to PATCH.
    const findNewestInTopic = (
      channel: ChannelName,
      topic: string,
    ): Effect.Effect<Option.Option<MessageId>, ZulipApiError | ParseResult.ParseError> =>
      minterHttp
        .get('/messages', messagesResponseSchema, {
          anchor: 'newest',
          num_before: 1,
          num_after: 0,
          narrow: JSON.stringify([
            { operator: 'channel', operand: channel },
            { operator: 'topic', operand: topic },
          ]),
          apply_markdown: false,
        })
        .pipe(
          Effect.flatMap((res) =>
            Arr.last(res.messages).pipe(
              Option.match({
                onNone: () => Effect.succeed(Option.none<MessageId>()),
                onSome: (m) => decodeMessageId(String(m.id)).pipe(Effect.map(Option.some)),
              }),
            ),
          ),
        )

    // Resolution renames the topic (✔ prefix) via change_all, so we PATCH any
    // message in it. `thread` is the plain name; find the source-state topic
    // and flip it to the target state, treating an absent source whose target
    // already exists as an idempotent no-op, and both absent as a thread that
    // isn't there to (un)resolve. Reads route through the minter; the
    // attributed edit goes through the bound identity.
    //
    // Substrate fact worth knowing before you touch any resolution-aware path:
    // the realm posts its own "<user> has marked this topic as resolved"
    // notification *into the topic it just renamed*. So a resolved topic always
    // holds at least one message nobody in commy sent, and reading one back
    // yields a message with no counterpart on our side. It lands in the ✔ form
    // (the rename runs first), which is why an emptiness probe for the bare name
    // stays a sound test for "this thread is not here under its plain name".
    // `realm.live.test.ts` asserts that notice rather than filtering it out —
    // its presence between a pre-resolve and a post-resolve message is
    // substrate-side proof the two landed in one conversation, which is the
    // whole claim `addressThread` below exists to make good on.
    //
    // The absent occupied-name guard is deliberate. In a realm forked by the
    // resolve-then-post bug both topic forms exist, so this PATCH renames one
    // onto a name the other already holds — and Zulip merges them, folding the
    // source topic's messages into the destination in message-id order (a
    // chronological interleave; nothing dropped or reordered) and leaving no
    // source topic behind. Measured on the realm, pinned in `realm.live.test.ts`,
    // and now part of the port's contract. So `unresolveThread` is the *repair*
    // for a forked thread: guarding the occupied name would break it.
    const setThreadResolved = (
      channel: ChannelName,
      thread: ThreadName,
      resolved: boolean,
    ): Effect.Effect<void, PublisherError> => {
      const operation = resolved ? 'resolveThread' : 'unresolveThread'
      const sourceTopic = applyResolvedPrefix(thread, !resolved)
      const targetTopic = applyResolvedPrefix(thread, resolved)
      return boundHttp().pipe(
        Effect.flatMap((http) =>
          findNewestInTopic(channel, sourceTopic).pipe(
            Effect.flatMap(
              Option.match({
                onSome: (id) =>
                  http
                    .patch(`/messages/${encodeURIComponent(id)}`, successSchema, {
                      topic: targetTopic,
                      propagate_mode: 'change_all',
                    })
                    .pipe(Effect.asVoid),
                onNone: () =>
                  findNewestInTopic(channel, targetTopic).pipe(
                    Effect.flatMap(
                      Option.match({
                        onSome: () => Effect.void,
                        onNone: () =>
                          Effect.fail(
                            new PublisherError({
                              operation,
                              cause: new Error(
                                `no thread '${thread}' in ${channel} to ${
                                  resolved ? 'resolve' : 'unresolve'
                                }`,
                              ),
                            }),
                          ),
                      }),
                    ),
                  ),
              }),
            ),
          ),
        ),
        Effect.mapError((cause) =>
          cause instanceof PublisherError ? cause : new PublisherError({ operation, cause }),
        ),
      )
    }

    // Zulip creates topics implicitly on post, and resolution *renames* the
    // topic (✔ prefix) rather than flagging it — so a write addressed to the
    // plain name of a resolved thread mints a bare-name sibling and splits the
    // conversation at the resolve. Probe for the thread's current substrate
    // form and address that. Precedence matches `readThread`: plain first,
    // then the resolved form, so post and read always mean the same topic —
    // including in a realm already forked by this bug. A thread that exists in
    // neither form is genuinely new, so it takes the plain name and Zulip
    // creates it. The common (unresolved, non-empty) case costs one probe; a
    // resolved-or-new thread pays for the second.
    const addressThread = (
      channel: ChannelName,
      thread: ThreadName,
    ): Effect.Effect<
      { readonly topic: string; readonly resolved: boolean },
      ZulipApiError | ParseResult.ParseError
    > => {
      const resolvedTopic = applyResolvedPrefix(thread, true)
      return findNewestInTopic(channel, thread).pipe(
        Effect.flatMap(
          Option.match({
            onSome: () => Effect.succeed({ topic: thread as string, resolved: false }),
            onNone: () =>
              findNewestInTopic(channel, resolvedTopic).pipe(
                Effect.map(
                  Option.match({
                    onSome: () => ({ topic: resolvedTopic, resolved: true }),
                    onNone: () => ({ topic: thread as string, resolved: false }),
                  }),
                ),
              ),
          }),
        ),
      )
    }

    const publisher: MessagePublisher = {
      post: (channel, body, opts?: PostOpts) => {
        const thread = opts?.thread
        // Touch boundHttp() first so unacquired callers die on the
        // "not acquired" invariant before the pre-flight fires — that
        // misuse is a defect, not a typed failure. The pre-flight resolves the
        // compat alias and checks substrate-correctness, not authentication; an
        // unknown channel surfaces as UnknownChannel, every other substrate
        // failure as a PublisherError carrying the cause.
        return boundHttp().pipe(
          Effect.flatMap((http) =>
            validateOutboundMentions('post', body).pipe(
              Effect.zipRight(resolvePublishChannel(channel)),
              Effect.flatMap((effective) =>
                // Zulip rejects stream messages without a topic on realms with
                // `mandatory_topics: true` (a common realm default). Default to
                // the server-canonical "(no topic)" placeholder when the caller
                // omits a thread — same string Zulip's UI uses for empty
                // topics; an unnamed thread has no resolution state to probe
                // for. `opts.mentions` is metadata-only per PostOpts: we
                // don't fold it into `content`. Zulip pings are driven by
                // `@**Name**` markup inside the body, so callers that want a
                // ping write the markup themselves where they want it rendered.
                (thread === undefined
                  ? Effect.succeed({ topic: '(no topic)', resolved: false })
                  : addressThread(effective.name, thread)
                ).pipe(
                  Effect.flatMap((addressed) =>
                    http
                      .post('/messages', sentMessageSchema, {
                        type: 'channel',
                        to: effective.name,
                        content: body,
                        topic: addressed.topic,
                      })
                      .pipe(
                        Effect.flatMap((sent) =>
                          decodeMessageId(String(sent.id)).pipe(
                            Effect.map(
                              (id): MessageRef =>
                                // The probe tells us which substrate form the
                                // message landed in, so the ref reports the
                                // thread's real resolution state rather than
                                // assuming an unresolved one.
                                decorateMessageRef(
                                  id,
                                  effective,
                                  thread === undefined
                                    ? undefined
                                    : { name: thread, resolved: addressed.resolved },
                                ),
                            ),
                          ),
                        ),
                      ),
                  ),
                ),
              ),
            ),
          ),
          Effect.mapError((cause) =>
            cause instanceof UnknownChannel || cause instanceof UnresolvedMention
              ? cause
              : new PublisherError({ operation: 'post', cause }),
          ),
        )
      },
      edit: (message, body) =>
        boundHttp().pipe(
          Effect.flatMap((http) =>
            validateOutboundMentions('edit', body).pipe(
              Effect.zipRight(
                http.patch(`/messages/${encodeURIComponent(message.id)}`, successSchema, {
                  content: body,
                }),
              ),
            ),
          ),
          Effect.asVoid,
          Effect.mapError((cause) =>
            cause instanceof UnresolvedMention ? cause : classifyEditFailure(cause),
          ),
        ),
      // Read through the MINTER, never `boundHttp()`: this is sampled at
      // connect, when a listen-only seat has acquired no identity and
      // `boundHttp()` would fail. The setting is realm-scoped rather than
      // viewer-scoped, so the minter's answer is the same answer any seat
      // would get.
      //
      // Reading costs a queue: `/register` always allocates one, even asked
      // for no event types. We hand it straight back. If that DELETE fails we
      // ignore it — the queue is then orphaned until Zulip reaps it at its
      // DEFAULT idle timeout (600s; note this register does not ask for the
      // 86400 the event pump uses), which is bounded and self-healing, and
      // not worth failing a capability probe over.
      editingAvailable: () =>
        minterHttp
          .post('/register', realmEditingStateSchema, {
            event_types: JSON.stringify([]),
            fetch_event_types: JSON.stringify(['realm']),
          })
          .pipe(
            Effect.tap((res) =>
              minterHttp
                .delete('/events', successSchema, { queue_id: res.queue_id })
                .pipe(Effect.ignore),
            ),
            Effect.map((res) => res.realm_allow_message_editing),
            Effect.mapError(
              (cause) => new PublisherError({ operation: 'editingAvailable', cause }),
            ),
          ),
      react: (message, emoji) =>
        boundHttp().pipe(
          Effect.flatMap((http) =>
            http.post(`/messages/${encodeURIComponent(message.id)}/reactions`, successSchema, {
              emoji_name: emoji,
            }),
          ),
          Effect.asVoid,
          Effect.mapError((cause) => new PublisherError({ operation: 'react', cause })),
        ),
      unreact: (message, emoji) =>
        boundHttp().pipe(
          Effect.flatMap((http) =>
            http.delete(`/messages/${encodeURIComponent(message.id)}/reactions`, successSchema, {
              emoji_name: emoji,
            }),
          ),
          Effect.asVoid,
          Effect.mapError((cause) => new PublisherError({ operation: 'unreact', cause })),
        ),
      resolveThread: (channel, thread) => setThreadResolved(channel, thread, true),
      unresolveThread: (channel, thread) => setThreadResolved(channel, thread, false),
      setChannelDescription,
    }

    // The inbox subscription spine — the channel/new-topic narrow sets, the
    // per-channel seen-topics dedupe, and the currently registered
    // events-queue identity — as one immutable record behind a single Ref.
    // subscribe/unsubscribe are Ref.update transitions; reads
    // (streamIsListening, shouldDeliver) snapshot the record.
    const inboxRef = yield* SynchronizedRef.make<InboxState>({
      subscribedChannels: HashSet.empty<ChannelName>(),
      newTopicsChannels: HashSet.empty<ChannelName>(),
      seenTopicsByChannel: HashMap.empty<ChannelName, HashSet.HashSet<ThreadName>>(),
      registration: Option.none(),
    })
    // Adapter-scoped so the cache outlives any single `events()`
    // iterator — consumers that re-call `events()` retain the
    // MessageRef context built up from earlier message-posted events.
    const messageRefCache = createMessageRefCache()
    // Adapter-scoped watermark for the gap-replay anchor. Lifted out of
    // the iterator's closure so the event-pump's auto-reconnect
    // creates a fresh iterator that still knows where the
    // previous session left off — the BAD_EVENT_QUEUE_ID replay path
    // then backfills the gap on the new iterator's first
    // poll instead of skipping it.
    const watermarkStore = yield* createWatermarkStore()
    // Realm-settings fan-out. Adapter-scoped and unbounded-dropping: the
    // producer's hook must never block the event loop waiting on a
    // subscriber, and a settings signal is a latest-wins fact rather than a
    // log to preserve. `settingsChanges()` republishes it as a Stream so the
    // signal reaches consumers without joining `InboundEvent`.
    const realmSettingsHub = yield* PubSub.dropping<RealmSettings>(16)

    // Per-event filter. The new-topics-in-channel narrow is the only
    // narrow that requires adapter-side state (seen topics); channel:X is
    // enforced server-side via the minter's /users/me/subscriptions list.
    // We therefore only intercept messages belonging to a channel that has
    // the new-topics narrow active, and pass everything else through
    // unchanged (preserves the plumbing contract: events queue →
    // InboundEvent).
    //
    // Mentions are implicit and unconditional — a bot always receives its
    // own, and cannot subscribe to or unsubscribe from them. So a message
    // mentioning the bound identity passes the new-topics narrow on that
    // ground alone, with nothing to opt into.
    //
    // The seen-topics tick mutates inbox state, so the decision is one
    // atomic Ref.modify: it records the observed topic and computes the
    // delivery verdict from the same snapshot. The bound identity is read
    // first because it lives in a separate ref.
    const shouldDeliver = (event: InboundEvent): Effect.Effect<boolean> => {
      if (event.kind !== 'message-posted') return Effect.succeed(true)
      const message = event.message
      const cname = message.ref.channel.name
      const thread = message.ref.thread
      return SynchronizedRef.get(boundRef).pipe(
        Effect.flatMap((current) => {
          const me = Option.getOrUndefined(current)?.identity
          return SynchronizedRef.modify(inboxRef, (state) => {
            if (!HashSet.has(state.newTopicsChannels, cname)) return [true, state]
            // Tick the seen state regardless of whether another narrow also
            // covers this channel — so unsubscribing the broader narrow later
            // does not re-fire topics already observed.
            const [isFirstOfTopic, ticked] = Option.match(thread, {
              onNone: () => [false, state] as const,
              onSome: (t) => observeNewTopic(state, cname, t.name),
            })
            if (HashSet.has(state.subscribedChannels, cname)) return [true, ticked]
            if (me !== undefined && mentionsIdentity(message.mentions, me.id)) {
              return [true, ticked]
            }
            return [isFirstOfTopic, ticked]
          })
        }),
      )
    }

    // Eager /register at subscribe-time. The port contract is "when
    // subscribe() resolves, events() observes matching posts from this
    // moment onward" — for Zulip that means the events queue must exist
    // before subscribe() returns, otherwise posts race ahead of the
    // queue registration and are lost. The queue carries no narrow, so one
    // registration serves every subscription state and later subscribes
    // reuse it.
    //
    // The queue is registered against the minter, not the per-session
    // bot — the inbox is a minter-side surface so lurking
    // sessions can receive events before any acquire happens.
    const ensureQueueRegistered = (): Effect.Effect<void, ZulipApiError | ParseResult.ParseError> =>
      // Atomic read-decide-register-write: the lock is held across registerQueue
      // so two concurrent subscribe() calls can't both read registration=None
      // and double-register the events queue. The snapshot is
      // current-under-lock, so the `{ ...state }` write-back cannot clobber a
      // concurrent inboxRef mutation — they block on the same lock.
      SynchronizedRef.modifyEffect(inboxRef, (state) => {
        if (Option.isSome(state.registration)) {
          return Effect.succeed([undefined, state] as const)
        }
        return registerQueue(minterHttp, config.queueIdleTimeoutSecs).pipe(
          Effect.tap((q) => config.onQueueRegister?.(q) ?? Effect.void),
          Effect.map((q) => [undefined, { ...state, registration: Option.some(q) }] as const),
        )
      })

    const inbox: MessageInbox = {
      subscribe: (target) =>
        Effect.suspend(() => {
          const channel = channelOf(target)
          // Record the narrow first, snapshotting whether the channel was
          // already listened to under any narrow — that decides whether the
          // remote /users/me/subscriptions call is needed.
          return SynchronizedRef.modify(inboxRef, (state) => {
            const wasListening = streamIsListening(state, channel)
            const next: InboxState = Predicate.hasProperty(target, 'kind')
              ? {
                  ...state,
                  newTopicsChannels: HashSet.add(state.newTopicsChannels, channel),
                }
              : {
                  ...state,
                  subscribedChannels: HashSet.add(state.subscribedChannels, channel),
                }
            return [wasListening, next]
          }).pipe(
            Effect.flatMap((wasListening) => {
              const subscribeRemote = wasListening
                ? Effect.void
                : minterHttp
                    .post('/users/me/subscriptions', subscriptionsResponseSchema, {
                      subscriptions: JSON.stringify([{ name: channel }]),
                    })
                    .pipe(Effect.asVoid)
              // /users/me/subscriptions is "me" = minter. The
              // boot-time reconciler covers the universal-listener backstop;
              // this per-session call still matters for streams created
              // *after* the plugin booted.
              return subscribeRemote.pipe(Effect.andThen(ensureQueueRegistered()))
            }),
          )
        }).pipe(Effect.mapError((cause) => new InboxError({ operation: 'subscribe', cause }))),
      unsubscribe: (target) =>
        Effect.suspend(() => {
          const channel = channelOf(target)
          // Drop the narrow, snapshotting whether the channel is still
          // listened to afterward — if so, the minter stays subscribed.
          return SynchronizedRef.modify(inboxRef, (state) => {
            const next: InboxState = Predicate.hasProperty(target, 'kind')
              ? {
                  ...state,
                  newTopicsChannels: HashSet.remove(state.newTopicsChannels, channel),
                  seenTopicsByChannel: HashMap.remove(state.seenTopicsByChannel, channel),
                }
              : {
                  ...state,
                  subscribedChannels: HashSet.remove(state.subscribedChannels, channel),
                }
            return [streamIsListening(next, channel), next]
          }).pipe(
            Effect.flatMap((stillListening) =>
              stillListening
                ? Effect.void
                : minterHttp
                    .delete('/users/me/subscriptions', subscriptionsResponseSchema, {
                      subscriptions: JSON.stringify([channel]),
                    })
                    .pipe(Effect.asVoid),
            ),
          )
        }).pipe(Effect.mapError((cause) => new InboxError({ operation: 'unsubscribe', cause }))),
      events: () =>
        Stream.unwrap(
          Effect.all([
            SynchronizedRef.get(boundRef),
            SynchronizedRef.get(inboxRef),
            // Read half of long-idle queue resume: consulted once as the
            // producer materialises (the pump calls events() exactly once, so
            // this fires per pump lifetime). A persisted queue is preferred
            // over the eager register-time queue below — a boot COMMY_SUBSCRIBE
            // register must not shadow a live persisted queue. No resolver
            // (persistent seat) or None (fresh session / session id not yet
            // known) leaves the register-time / lazy-register path standing.
            config.resumeQueue?.() ?? Effect.succeedNone,
          ]).pipe(
            Effect.flatMap(([current, state, resumed]) => {
              const initialQueue = Option.orElse(resumed, () => state.registration)
              // Resume-verdict wiring. No persisted resume-state → report
              // 'missed' now so the seat runs its normal boot catch-up; the
              // eager register-time queue below is a fresh register, not a
              // resume. A persisted `Some` defers the verdict to the producer's
              // first resume-poll (alive → true, dead → false).
              const reportAbsentResume = Option.isNone(resumed)
                ? (config.onResumeOutcome?.(false) ?? Effect.void)
                : Effect.void
              return reportAbsentResume.pipe(
                Effect.as(
                  inboxEvents({
                    http: minterHttp,
                    permalinkBase: base,
                    resolveDirectory: buildDirectoryLookup,
                    // Live registration read. A seat that had no queue when
                    // the producer started registers one on its first
                    // `subscribe()` — the lazy-acquire path, where the
                    // acquire-time default narrows land mid-stream. Without
                    // this the already-running producer would never learn of
                    // that queue and would poll its own for the rest of the
                    // process.
                    currentRegistration: SynchronizedRef.get(inboxRef).pipe(
                      Effect.map((s) => s.registration),
                    ),
                    messageRefCache,
                    watermarkStore,
                    // Queue-state write half: the timeout on the producer's own
                    // register + the persistence hooks so a long-idle resume can
                    // recover the queue. Undefined for persistent seats.
                    ...(config.queueIdleTimeoutSecs === undefined
                      ? {}
                      : { queueIdleTimeoutSecs: config.queueIdleTimeoutSecs }),
                    ...(config.onQueueRegister === undefined
                      ? {}
                      : { onQueueRegister: config.onQueueRegister }),
                    ...(config.onQueueAdvance === undefined
                      ? {}
                      : { onQueueAdvance: config.onQueueAdvance }),
                    // Wire the port's own replay() into the producer so BAD_EVENT_QUEUE_ID
                    // recovery can transparently backfill the gap window with replayed=true
                    // events. Late-bound to inbox.replay so the closure picks
                    // up the function defined below in the same object literal.
                    replay: (since) => inbox.replay(since),
                    // Republish onto the adapter-scoped hub. `PubSub.publish`
                    // returns whether the value was taken; a dropping hub with
                    // no subscriber says `false`, which is the designed
                    // outcome, not a failure to report.
                    onRealmSettings: (settings) =>
                      PubSub.publish(realmSettingsHub, settings).pipe(Effect.asVoid),
                    ...Option.match(current, {
                      onNone: () => ({}),
                      onSome: (b) => ({ boundIdentity: b.identity }),
                    }),
                    ...Option.match(initialQueue, {
                      onNone: () => ({}),
                      onSome: (q) => ({ initialQueue: q }),
                    }),
                    // Resume-verdict: only a genuine resume (persisted `Some`) lets
                    // the producer report the verdict — a fresh register-time queue
                    // must never report 'replayed'. The 'missed' for a None resume
                    // was already reported above.
                    ...(Option.isSome(resumed) && config.onResumeOutcome !== undefined
                      ? { onResumeOutcome: config.onResumeOutcome }
                      : {}),
                  }),
                ),
              )
            }),
          ),
        ).pipe(Stream.filterEffect(shouldDeliver)),
      settingsChanges: () => Stream.fromPubSub(realmSettingsHub),
      replay: (since) => {
        // The port's surface is channel-rooted: PMs are out of scope.
        // Ask Zulip to exclude DMs at source so the replay schema (which
        // requires stream_id / string display_recipient / non-empty
        // subject) never sees PM-shaped rows — any DM in the minter's
        // recent history would otherwise crash the schema decode.
        const replayNarrow = JSON.stringify([{ negated: true, operator: 'is', operand: 'dm' }])
        return Effect.all(
          [
            buildDirectoryLookup(),
            minterHttp.get('/messages', replayResponseSchema, {
              anchor: 'newest',
              num_before: REPLAY_NUM_BEFORE,
              num_after: 0,
              narrow: replayNarrow,
              apply_markdown: false,
            }),
            SynchronizedRef.get(boundRef),
          ],
          { concurrency: 2 },
        ).pipe(
          Effect.flatMap(([directory, res, current]) =>
            Effect.forEach(
              res.messages.filter((raw) => raw.timestamp >= since),
              (raw) => {
                const { flags: _flags, ...message } = raw
                return messageToInboundEvents(
                  message,
                  directory,
                  Option.getOrUndefined(current)?.identity,
                  base,
                )
              },
            ).pipe(
              Effect.map((perMessage) => {
                const out: InboundEvent[] = []
                for (const mapped of perMessage) {
                  for (const ev of mapped) {
                    if (ev.kind === 'message-posted') {
                      messageRefCache.set(ev.message.ref.id, ev.message.ref)
                    }
                  }
                  out.push(...mapped)
                }
                return out
              }),
            ),
          ),
          Effect.mapError((cause) => new InboxError({ operation: 'replay', cause })),
        )
      },
    }

    const history: HistoryReader = {
      readChannel: (channel, range) =>
        readMessages(range, [{ operator: 'channel', operand: channel }]).pipe(
          Effect.mapError((cause) => new HistoryError({ operation: 'readChannel', cause })),
        ),
      // A thread is addressed by its plain name, but resolution renames the
      // underlying topic (✔ prefix). Read the plain topic first; when it holds
      // nothing, fall back to the resolved form so read_thread keeps working
      // after a thread is resolved. The common (unresolved) case stays one
      // fetch; only a resolved-or-absent thread pays for the second.
      readThread: (channel, threadName, range) => {
        const readTopic = (topic: string) =>
          readMessages(range ?? {}, [
            { operator: 'channel', operand: channel },
            { operator: 'topic', operand: topic },
          ])
        return readTopic(threadName).pipe(
          Effect.flatMap((messages) =>
            messages.length > 0
              ? Effect.succeed(messages)
              : readTopic(applyResolvedPrefix(threadName, true)),
          ),
          Effect.mapError((cause) => new HistoryError({ operation: 'readThread', cause })),
        )
      },
      recentThreads: (sender, opts) => {
        const limit = opts?.limit ?? RECENT_THREADS_DEFAULT_LIMIT
        // The `sender` narrow operand must be a ZulipUserRef (integer user id) —
        // a numeric-string id is rejected as BAD_NARROW.
        // Resolve the cross-substrate IdentityId to a ref via the directory; an
        // unresolvable sender has no Zulip user to query, so return [] rather
        // than issue a doomed request.
        return buildDirectoryLookup().pipe(
          Effect.flatMap((lookup) => {
            const ref = lookup.byIdentityId.get(sender)
            if (ref === undefined) return Effect.succeed<ReadonlyArray<RecentThread>>([])
            const narrow = JSON.stringify([senderNarrow(ref)])
            return minterHttp
              .get('/messages', senderMessagesResponseSchema, {
                anchor: 'newest',
                num_before: RECENT_THREADS_FETCH_LIMIT,
                num_after: 0,
                narrow,
                apply_markdown: false,
              })
              .pipe(
                Effect.flatMap((res) =>
                  Effect.gen(function* () {
                    const threadKey = (channel: ChannelName, thread: ThreadName) =>
                      Data.struct({ channel, thread })
                    let seen = HashMap.empty<ReturnType<typeof threadKey>, RecentThread>()
                    for (const m of res.messages) {
                      const channel = yield* decodeChannelName(m.display_recipient)
                      // Surface the clean thread name — a resolved topic's ✔
                      // prefix is a substrate detail, never crosses the port.
                      const thread = yield* decodeThreadName(splitTopic(m.subject).name)
                      const key = threadKey(channel, thread)
                      if (!HashMap.has(seen, key)) {
                        seen = HashMap.set(seen, key, {
                          channel,
                          thread,
                          lastPostTs: yield* decodeTimestamp(m.timestamp),
                          lastPostBody: yield* decodeMessageBody(m.content),
                        })
                      }
                      if (HashMap.size(seen) >= limit) break
                    }
                    const threads = [...HashMap.values(seen)]
                    return Arr.sort(
                      threads,
                      Order.reverse(
                        Order.mapInput(Order.number, (t: RecentThread) => t.lastPostTs),
                      ),
                    )
                  }),
                ),
              )
          }),
          Effect.mapError((cause) => new HistoryError({ operation: 'recentThreads', cause })),
        )
      },
      messagePermalink: (id, hint) =>
        (hint === undefined
          ? // No coordinates supplied: locate the message by id (the minter can
            // see any channel message) and hand back the permalink the decode
            // already built.
            fetchMessageRef(minterHttp, Number(id), base).pipe(
              Effect.map(Option.map((ref) => ref.permalink)),
            )
          : // Channel hint supplied: resolve the name to its numeric stream and
            // build the link directly — no need to locate the message itself.
            lookupChannel(hint.channel).pipe(
              Effect.map(
                Option.map(
                  (channel) =>
                    buildMessageRef(
                      base,
                      id,
                      channel,
                      // A hint addresses a thread by its plain name; resolution
                      // is unknown here and irrelevant to the link, so unset.
                      hint.thread === undefined
                        ? undefined
                        : { name: hint.thread, resolved: false },
                    ).permalink,
                ),
              ),
            )
        ).pipe(
          Effect.mapError((cause) => new HistoryError({ operation: 'messagePermalink', cause })),
        ),
    }

    const reconcileMinter = (): Effect.Effect<ReconcileReport, never> =>
      reconcileMinterSubscriptions({
        listUnsubscribedPublicStreams: () =>
          minterHttp
            .get('/streams', streamsListResponseSchema, {
              include_public: true,
              include_subscribed: false,
            })
            .pipe(
              Effect.flatMap((res) =>
                Effect.forEach(res.streams, (s) =>
                  decodeChannelName(s.name).pipe(Effect.map((name) => ({ name }))),
                ),
              ),
            ),
        subscribeToStreams: (names) =>
          minterHttp
            .post('/users/me/subscriptions', reconcileSubscriptionsResponseSchema, {
              subscriptions: JSON.stringify(names.map((name) => ({ name }))),
            })
            .pipe(
              Effect.flatMap((res) => {
                const mintedFor = res.subscribed?.[config.minterEmail] ?? []
                return Effect.forEach(mintedFor, (name) => decodeChannelName(name))
              }),
            ),
      })

    return {
      // Zulip stamps integer epoch seconds, so two posts inside the same
      // second collide on `ts`; a caller needing distinct timestamps must
      // space posts by ≥1s.
      capabilities: { timestampGranularity: Duration.seconds(1) },
      identity,
      publisher,
      inbox,
      history,
      directory,
      reconcileMinterSubscriptions: reconcileMinter,
      downloadFile: (urlPath: UserUploadPath) => minterHttp.downloadRaw(urlPath),
      uploadFile: (filename: string, data: Uint8Array) => minterHttp.uploadRaw(filename, data),
      close: async () => {},
    }
  })

/**
 * Render an uploaded file as the Markdown link Zulip embeds in a message
 * body to surface the attachment inline. Callers place the returned string
 * in their `post` body wherever they want it rendered — the same caller-owns-
 * body division of labour as `@**name**` mention markup.
 */
export const attachmentReference = (upload: UploadResult): string =>
  `[${upload.filename}](${upload.url})`
