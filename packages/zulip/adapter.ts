import type {
  AcquiredIdentity,
  AgentComms,
  BotName,
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
  RecentThread,
  SubscriptionTarget,
  ThreadName,
  Timestamp as TimestampType,
} from '@codeforbreakfast/core/ports'
import {
  DirectoryError,
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
  PublisherError,
  UnknownChannel,
} from '@codeforbreakfast/core/ports'
import { HttpClient } from '@effect/platform'
import {
  Array as Arr,
  Data,
  Effect,
  HashMap,
  HashSet,
  Option,
  Order,
  type ParseResult,
  Predicate,
  Redacted,
  Ref,
  Schema,
  Stream,
  SynchronizedRef,
} from 'effect'
import type { BotHttp, RecipientDirectory } from './bot-dm-guard.ts'
import { wrapBotHttp } from './bot-dm-guard.ts'
import type { QueueState } from './events.ts'
import {
  createMessageRefCache,
  createWatermarkStore,
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
import type { ReconcileReport } from './minter-reconciler.ts'
import { reconcileMinterSubscriptions } from './minter-reconciler.ts'
import { senderNarrow, userPresencePath, ZulipUserRef } from './user-ref.ts'

export interface ZulipAdapterConfig {
  readonly realmUrl: RealmUrl
  /** Minter credentials. Owns POST /bots, regenerate, and DELETE /bots/{id}. */
  readonly minterEmail: BotEmailType
  /**
   * Minter API key wrapped in `Redacted` so the privileged secret masks to
   * `<redacted>` on any log / stringify / error rendering (comms-spj3.38). It
   * is unwrapped via `Redacted.value` only at the single point the minter HTTP
   * client is constructed — the auth-header boundary.
   */
  readonly minterApiKey: Redacted.Redacted<ApiKeyType>
  /** Override outgoing Host header — required for cluster-internal callers. */
  readonly hostHeader?: string
}

export type ZulipAdapter = AgentComms & {
  /**
   * Subscribe the minter to every public stream it isn't yet on
   * (ass-6a77). Boot-time backstop so the plugin's event pump observes
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
 * debugger is not sent chasing a phantom (comms-ch7). The fix that removes
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

const SHORT_NAME_PATTERN = /[^a-z0-9._-]/g

const sanitiseShortName = (name: string): string => {
  const collapsed = name.toLowerCase().replace(SHORT_NAME_PATTERN, '-')
  return collapsed.replace(/^-+|-+$/g, '') || 'bot'
}

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

interface HistoricalReaction {
  readonly userId: number
  readonly emojiName: string
}

interface HistoricalMessage {
  readonly id: MessageId
  readonly senderId: number
  readonly senderFullName: string
  readonly subject: ThreadName
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
  sender_id: Schema.Int,
  sender_full_name: Schema.String,
  subject: Schema.NonEmptyString,
  content: Schema.String,
  timestamp: Schema.NonNegative,
  reactions: Schema.optional(Schema.Array(historicalReactionSchema)),
})

const toHistoricalReaction = (
  r: Schema.Schema.Type<typeof historicalReactionSchema>,
): HistoricalReaction => ({ userId: r.user_id, emojiName: r.emoji_name })

const toHistoricalMessage = (
  m: Schema.Schema.Type<typeof historicalMessageRawSchema>,
): Effect.Effect<HistoricalMessage, ParseResult.ParseError> =>
  Effect.all({
    id: decodeMessageId(String(m.id)),
    subject: decodeThreadName(m.subject),
    ts: decodeTimestamp(m.timestamp),
  }).pipe(
    Effect.map(
      ({ id, subject, ts }): HistoricalMessage => ({
        id,
        senderId: m.sender_id,
        senderFullName: m.sender_full_name,
        subject,
        content: m.content,
        ts,
        reactions: (m.reactions ?? []).map(toHistoricalReaction),
      }),
    ),
  )

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
  | { readonly operator: 'topic'; readonly operand: ThreadName }

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

interface InboxState {
  readonly mentionsSubscribed: boolean
  readonly subscribedChannels: HashSet.HashSet<string>
  readonly newTopicsChannels: HashSet.HashSet<string>
  readonly seenTopicsByChannel: HashMap.HashMap<string, HashSet.HashSet<string>>
  readonly registeredQueue: Option.Option<QueueState>
  readonly registeredMode: Option.Option<'all' | 'mentions'>
}

const streamIsListening = (state: InboxState, channelId: string): boolean =>
  HashSet.has(state.subscribedChannels, channelId) ||
  HashSet.has(state.newTopicsChannels, channelId)

const currentMode = (state: InboxState): 'all' | 'mentions' =>
  state.mentionsSubscribed &&
  HashSet.size(state.subscribedChannels) === 0 &&
  HashSet.size(state.newTopicsChannels) === 0
    ? 'mentions'
    : 'all'

// Record (channelId, threadName) in the per-channel seen set, returning
// whether this is the first observation of the topic and the next state.
const observeNewTopic = (
  state: InboxState,
  channelId: string,
  threadName: string,
): readonly [boolean, InboxState] => {
  const seen = HashMap.get(state.seenTopicsByChannel, channelId).pipe(
    Option.getOrElse(() => HashSet.empty<string>()),
  )
  if (HashSet.has(seen, threadName)) return [false, state]
  return [
    true,
    {
      ...state,
      seenTopicsByChannel: HashMap.set(
        state.seenTopicsByChannel,
        channelId,
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
    // doesn't re-enter the requirements channel (request-time DI is the
    // separate comms-7v3 concern — out of scope here).
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
    // `minterHttp` so the plugin can run pre-acquire (ass-220u).
    // The bound HTTP is wrapped by bot-dm-guard so any future code that
    // constructs a `POST /messages` with `type=private` and an all-bot
    // recipient list is rejected before reaching the wire (comms-7yk).
    const boundHttp = (): Effect.Effect<BotHttp> => requireBound().pipe(Effect.map((b) => b.http))

    const fetchMembers = (): Effect.Effect<
      ReadonlyArray<ZulipUser>,
      ZulipApiError | ParseResult.ParseError
    > => minterHttp.get('/users', usersSchema).pipe(Effect.map((res) => res.members))

    interface DirectoryLookup {
      readonly byId: ReadonlyMap<number, Identity>
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
            const byId = new Map<number, Identity>()
            const byName = new Map<string, Identity>()
            const byIdentityId = new Map<IdentityId, ZulipUserRef>()
            for (const u of members) {
              const ident = yield* toIdentity(u)
              byId.set(u.user_id, ident)
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

    const MENTION_PATTERN = /@\*\*([^*]+)\*\*/g

    const extractMentions = (
      content: string,
      byName: ReadonlyMap<string, Identity>,
    ): ReadonlyArray<Identity> => {
      const results: Identity[] = []
      const seen = new Set<string>()
      for (const match of content.matchAll(MENTION_PATTERN)) {
        const name = match[1]
        if (name === undefined) continue
        const ident = byName.get(name)
        if (ident === undefined) continue
        if (seen.has(ident.id)) continue
        seen.add(ident.id)
        results.push(ident)
      }
      return results
    }

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
      channel: ChannelRef,
      directory: DirectoryLookup,
    ): Effect.Effect<Message, ParseResult.ParseError> =>
      Effect.gen(function* () {
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
          ref: { id: m.id, channel, thread: { name: m.subject } },
          sender,
          body,
          ts: m.ts,
          mentions: extractMentions(m.content, directory.byName),
          reactions,
        }
      })

    const readMessages = (
      channel: ChannelRef,
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
              Effect.forEach(inRangeMessages, (m) => mapHistoricalMessage(m, channel, directory)),
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
              return reactivate.pipe(Effect.flatMap(() => regenerateBotKey(existing)))
            },
          }),
        ),
      )

    const identity: IdentityPort = {
      // Pre-acquire callers (the plugin-layer current_identity tool now
      // reads through ensureBound.current() and never invokes this) still
      // need a coherent error — dying matches the long-standing port
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
              acquireBot(name).pipe(
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
                    yield* Ref.update(inboxRef, (state) => ({
                      ...state,
                      mentionsSubscribed: true,
                    }))
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
            // admin-only reactivate path, which wedges a Member-rights minter
            // (comms-ch7). Clear the binding either way; only skip the
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
      // Zulip presence is human-only by design — POST /users/me/presence is
      // @human_users_only, so a bot has no presence record and a read would
      // 400 into a misleading 'offline'. An agent's presence is genuinely
      // unknowable here, so short-circuit to 'unknown' before any lookup or
      // GET (comms-1mnb). Only humans run the read path below.
      //
      // The human path needs a ZulipUserRef (integer user id), so it resolves
      // the IdentityId through the directory first; an unresolvable identity
      // has no Zulip user and is functionally offline (comms-7ee). Both fetches
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

    const channelOf = (target: SubscriptionTarget): ChannelRef | undefined => {
      if (target === 'mentions') return undefined
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
                map.set(s.name, {
                  id: yield* decodeChannelId(String(s.stream_id)),
                  name: yield* decodeChannelName(s.name),
                })
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
      requested: ChannelRef,
    ): Effect.Effect<ChannelRef, UnknownChannel | ZulipApiError | ParseResult.ParseError> =>
      lookupChannel(requested.name).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(new UnknownChannel({ channel: requested.name, substrate: 'zulip' })),
            onSome: Effect.succeed,
          }),
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
            resolvePublishChannel(channel).pipe(
              Effect.flatMap((effective) =>
                // Zulip rejects stream messages without a topic on realms with
                // `mandatory_topics: true` (the homelab default). Default to
                // the server-canonical "(no topic)" placeholder when the caller
                // omits a thread — same string Zulip's UI uses for empty
                // topics. `opts.mentions` is metadata-only per PostOpts: we
                // don't fold it into `content`. Zulip pings are driven by
                // `@**Name**` markup inside the body, so callers that want a
                // ping write the markup themselves where they want it rendered.
                http
                  .post('/messages', sentMessageSchema, {
                    type: 'channel',
                    to: effective.name,
                    content: body,
                    topic: thread === undefined ? '(no topic)' : thread.name,
                  })
                  .pipe(
                    Effect.flatMap((sent) =>
                      decodeMessageId(String(sent.id)).pipe(
                        Effect.map(
                          (id): MessageRef =>
                            thread === undefined
                              ? { id, channel: effective }
                              : { id, channel: effective, thread },
                        ),
                      ),
                    ),
                  ),
              ),
            ),
          ),
          Effect.mapError((cause) =>
            cause instanceof UnknownChannel
              ? cause
              : new PublisherError({ operation: 'post', cause }),
          ),
        )
      },
      edit: (message, body) =>
        boundHttp().pipe(
          Effect.flatMap((http) =>
            http.patch(`/messages/${encodeURIComponent(message.id)}`, successSchema, {
              content: body,
            }),
          ),
          Effect.asVoid,
          Effect.mapError((cause) => new PublisherError({ operation: 'edit', cause })),
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
    }

    // The inbox subscription spine — mentions flag, the channel/new-topic
    // narrow sets, the per-channel seen-topics dedupe, and the currently
    // registered events-queue identity — as one immutable record behind a
    // single Ref. subscribe/unsubscribe are Ref.update transitions; reads
    // (currentMode, streamIsListening, shouldDeliver) snapshot the record.
    const inboxRef = yield* Ref.make<InboxState>({
      mentionsSubscribed: false,
      subscribedChannels: HashSet.empty<string>(),
      newTopicsChannels: HashSet.empty<string>(),
      seenTopicsByChannel: HashMap.empty<string, HashSet.HashSet<string>>(),
      registeredQueue: Option.none<QueueState>(),
      registeredMode: Option.none<'all' | 'mentions'>(),
    })
    // Adapter-scoped so the cache outlives any single `events()`
    // iterator — consumers that re-call `events()` retain the
    // MessageRef context built up from earlier message-posted events.
    const messageRefCache = createMessageRefCache()
    // Adapter-scoped watermark for the gap-replay anchor. Lifted out of
    // the iterator's closure so the event-pump's auto-reconnect
    // (comms-ynb) creates a fresh iterator that still knows where the
    // previous session left off — the BAD_EVENT_QUEUE_ID replay path
    // (comms-jnn) then backfills the gap on the new iterator's first
    // poll instead of skipping it (comms-4au).
    const watermarkStore = yield* createWatermarkStore()

    // Per-event filter. The new-topics-in-channel narrow is the only
    // narrow that requires adapter-side state (seen topics); every other
    // narrow is enforced server-side — channel:X via the minter's
    // /users/me/subscriptions list, `mentions` via the events queue's
    // `is:mentioned` narrow. We therefore only intercept messages
    // belonging to a channel that has the new-topics narrow active, and
    // pass everything else through unchanged (preserves the legacy
    // plumbing contract: events queue → InboundEvent, no second-guessing).
    //
    // The seen-topics tick mutates inbox state, so the decision is one
    // atomic Ref.modify: it records the observed topic and computes the
    // delivery verdict from the same snapshot. The bound identity is read
    // first because it lives in a separate ref.
    const shouldDeliver = (event: InboundEvent): Effect.Effect<boolean> => {
      if (event.kind !== 'message-posted') return Effect.succeed(true)
      const message = event.message
      const cid = message.ref.channel.id
      const thread = message.ref.thread
      return SynchronizedRef.get(boundRef).pipe(
        Effect.flatMap((current) => {
          const me = Option.getOrUndefined(current)?.identity
          return Ref.modify(inboxRef, (state) => {
            if (!HashSet.has(state.newTopicsChannels, cid)) return [true, state]
            // Tick the seen state regardless of whether another narrow also
            // covers this channel — so unsubscribing the broader narrow later
            // does not re-fire topics already observed.
            const [isFirstOfTopic, ticked] =
              thread === undefined ? [false, state] : observeNewTopic(state, cid, thread.name)
            if (HashSet.has(state.subscribedChannels, cid)) return [true, ticked]
            if (
              state.mentionsSubscribed &&
              me !== undefined &&
              message.mentions.some((m) => m.id === me.id)
            ) {
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
    // queue registration and are lost. A mode flip (mentions ↔ all
    // narrow) abandons the old queue (Zulip GCs by TTL) and registers
    // afresh so the narrow matches the current subscription state.
    //
    // The queue is registered against the minter, not the per-session
    // bot — ass-220u makes the inbox a minter-side surface so lurking
    // sessions can receive events before any acquire happens.
    const ensureQueueRegistered = (): Effect.Effect<void, ZulipApiError | ParseResult.ParseError> =>
      Ref.get(inboxRef).pipe(
        Effect.flatMap((state) => {
          const mode = currentMode(state)
          if (Option.isSome(state.registeredQueue) && Option.contains(state.registeredMode, mode)) {
            return Effect.void
          }
          return registerQueue(minterHttp, mode).pipe(
            Effect.flatMap((q) =>
              Ref.update(inboxRef, (current) => ({
                ...current,
                registeredQueue: Option.some(q),
                registeredMode: Option.some(mode),
              })),
            ),
          )
        }),
      )

    const inbox: MessageInbox = {
      subscribe: (target) =>
        Effect.suspend(() => {
          if (target === 'mentions') {
            return Ref.update(inboxRef, (state) => ({ ...state, mentionsSubscribed: true })).pipe(
              Effect.flatMap(() => ensureQueueRegistered()),
            )
          }
          const channel = channelOf(target)
          if (channel === undefined) return ensureQueueRegistered()
          // Record the narrow first, snapshotting whether the channel was
          // already listened to under any narrow — that decides whether the
          // remote /users/me/subscriptions call is needed.
          return Ref.modify(inboxRef, (state) => {
            const wasListening = streamIsListening(state, channel.id)
            const next: InboxState = Predicate.hasProperty(target, 'kind')
              ? {
                  ...state,
                  newTopicsChannels: HashSet.add(state.newTopicsChannels, channel.id),
                }
              : {
                  ...state,
                  subscribedChannels: HashSet.add(state.subscribedChannels, channel.id),
                }
            return [wasListening, next]
          }).pipe(
            Effect.flatMap((wasListening) => {
              const subscribeRemote = wasListening
                ? Effect.void
                : minterHttp
                    .post('/users/me/subscriptions', subscriptionsResponseSchema, {
                      subscriptions: JSON.stringify([{ name: channel.name }]),
                    })
                    .pipe(Effect.asVoid)
              // /users/me/subscriptions is "me" = minter. ass-6a77's
              // boot-time reconciler covers the universal-listener backstop;
              // this per-session call still matters for streams created
              // *after* the plugin booted.
              return subscribeRemote.pipe(Effect.flatMap(() => ensureQueueRegistered()))
            }),
          )
        }).pipe(Effect.mapError((cause) => new InboxError({ operation: 'subscribe', cause }))),
      unsubscribe: (target) =>
        Effect.suspend(() => {
          if (target === 'mentions') {
            return Ref.update(inboxRef, (state) => ({ ...state, mentionsSubscribed: false }))
          }
          const channel = channelOf(target)
          if (channel === undefined) return Effect.void
          // Drop the narrow, snapshotting whether the channel is still
          // listened to afterward — if so, the minter stays subscribed.
          return Ref.modify(inboxRef, (state) => {
            const next: InboxState = Predicate.hasProperty(target, 'kind')
              ? {
                  ...state,
                  newTopicsChannels: HashSet.remove(state.newTopicsChannels, channel.id),
                  seenTopicsByChannel: HashMap.remove(state.seenTopicsByChannel, channel.id),
                }
              : {
                  ...state,
                  subscribedChannels: HashSet.remove(state.subscribedChannels, channel.id),
                }
            return [streamIsListening(next, channel.id), next]
          }).pipe(
            Effect.flatMap((stillListening) =>
              stillListening
                ? Effect.void
                : minterHttp
                    .delete('/users/me/subscriptions', subscriptionsResponseSchema, {
                      subscriptions: JSON.stringify([channel.name]),
                    })
                    .pipe(Effect.asVoid),
            ),
          )
        }).pipe(Effect.mapError((cause) => new InboxError({ operation: 'unsubscribe', cause }))),
      events: () =>
        Stream.unwrap(
          Effect.all([SynchronizedRef.get(boundRef), Ref.get(inboxRef)]).pipe(
            Effect.map(([current, state]) =>
              inboxEvents({
                http: minterHttp,
                resolveDirectory: buildDirectoryLookup,
                mode: currentMode(state),
                messageRefCache,
                watermarkStore,
                // Wire the port's own replay() into the producer so BAD_EVENT_QUEUE_ID
                // recovery can transparently backfill the gap window with replayed=true
                // events (comms-jnn). Late-bound to inbox.replay so the closure picks
                // up the function defined below in the same object literal.
                replay: (since) => inbox.replay(since),
                ...Option.match(current, {
                  onNone: () => ({}),
                  onSome: (b) => ({ boundIdentity: b.identity }),
                }),
                ...Option.match(state.registeredQueue, {
                  onNone: () => ({}),
                  onSome: (q) => ({ initialQueue: q }),
                }),
              }),
            ),
          ),
        ).pipe(Stream.filterEffect(shouldDeliver)),
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
        readMessages(channel, range, [{ operator: 'channel', operand: channel.name }]).pipe(
          Effect.mapError((cause) => new HistoryError({ operation: 'readChannel', cause })),
        ),
      readThread: (channel, threadName, range) =>
        readMessages(channel, range ?? {}, [
          { operator: 'channel', operand: channel.name },
          { operator: 'topic', operand: threadName },
        ]).pipe(Effect.mapError((cause) => new HistoryError({ operation: 'readThread', cause }))),
      recentThreads: (sender, opts) => {
        const limit = opts?.limit ?? RECENT_THREADS_DEFAULT_LIMIT
        // The `sender` narrow operand must be a ZulipUserRef (integer user id) —
        // a numeric-string id is rejected as BAD_NARROW (comms-wpp/comms-7ee).
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
                      const thread = yield* decodeThreadName(m.subject)
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
