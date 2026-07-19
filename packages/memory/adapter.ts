/**
 * In-memory reference adapter for AgentComms.
 *
 * Substrate-free implementation of every port. Passing the contract suite
 * (testing/contract.ts) is the spec. Drives two consumers: the suite itself
 * (kept honest by running it against an in-memory backend) and any driving
 * adapter that wants a working AgentComms without standing up a real
 * substrate.
 *
 * Lifecycle mirrors the port: the adapter is unauthenticated at
 * construction, `acquire(name)` binds it to an identity, ports operate
 * as that identity, `release()` clears the binding. See `IdentityPort`
 * in core/ports.ts for the full contract.
 *
 * Mutable substrate state lives in fiber-safe references created by the
 * construction Effect — the monotonic id counters and the acquired-identity
 * binding in `Ref`s, the pending-event buffer in an unbounded `Queue`.
 * Counter allocation is an atomic `Ref.getAndUpdate`; the acquire
 * check-then-set is a single `Ref.modify`, so concurrent posters/acquirers
 * under `Effect.all` allocate distinct ids and bind exactly once instead of
 * racing between read and write.
 */

import type {
  AcquiredIdentity,
  AgentComms,
  ChannelDescription,
  ChannelId,
  ChannelName,
  ChannelPermalink,
  ChannelRef,
  Credentials,
  Directory,
  HistoryReader,
  Identity,
  IdentityKind,
  IdentityPort,
  InboundEvent,
  Message,
  MessageBody as MessageBodyType,
  MessageId,
  MessageInbox,
  MessagePermalink,
  MessagePublisher,
  MessageRef,
  PostOpts,
  Presence,
  Range,
  Reaction,
  RecentThread,
  SubscriptionTarget,
  ThreadName,
  ThreadPermalink,
  Timestamp as TimestampType,
} from '@commy/core/ports'
import {
  type BotName,
  ChannelDescriptionRejected,
  ChannelPermalinkSchema,
  decodeChannelId,
  decodeChannelName,
  decodeDisplayName,
  decodeIdentityId,
  decodeMessageId,
  decodeTimestamp,
  type Emoji,
  HistoryError,
  MessagePermalinkSchema,
  PublisherError,
  ThreadPermalinkSchema,
  UnknownChannel,
  UnknownIdentity,
} from '@commy/core/ports'
import {
  Array as Arr,
  Clock,
  Data,
  Duration,
  Effect,
  HashMap,
  HashSet,
  Option,
  Order,
  type ParseResult,
  Predicate,
  Queue,
  Ref,
  Stream,
} from 'effect'

// Synthesised, stable permalinks. The memory substrate has no web
// client, so these are deliberately fake `memory://` URIs — their only job is to
// let the MCP tools rig assert that the permalink field is plumbed on every
// surface without a live Zulip realm. They mirror the message/channel/topic
// shape the real Zulip narrow builder produces.
const MEMORY_REALM = 'memory://commy'
const synthChannelPermalink = (id: ChannelId): ChannelPermalink =>
  ChannelPermalinkSchema.make(`${MEMORY_REALM}/channel/${id}`)
const synthTopicPermalink = (id: ChannelId, topic: ThreadName): ThreadPermalink =>
  ThreadPermalinkSchema.make(`${synthChannelPermalink(id)}/topic/${topic}`)
const synthMessagePermalink = (
  id: ChannelId,
  messageId: MessageId,
  topic?: ThreadName,
): MessagePermalink =>
  MessagePermalinkSchema.make(
    topic === undefined
      ? `${synthChannelPermalink(id)}/near/${messageId}`
      : `${synthTopicPermalink(id, topic)}/near/${messageId}`,
  )

interface StoredMessage {
  readonly ref: MessageRef
  readonly sender: Identity
  readonly body: MessageBodyType
  readonly ts: TimestampType
  readonly mentions: ReadonlyArray<Identity>
}

interface Binding {
  readonly acquiredName: BotName
  readonly identity: Identity
  readonly credentials: Credentials
}

type Emit = (event: InboundEvent) => void

export interface MemoryAdapterConfig {
  /**
   * When set, `acquire(name)` succeeds only for names in this list and
   * throws `UnknownIdentity` otherwise — used by the contract suite to
   * exercise Discord-shaped pre-minted-pool semantics. Omit for the
   * default Zulip-shaped "always succeeds" behaviour.
   */
  readonly acquirableNames?: ReadonlyArray<string>
  /**
   * Identity kind bound on acquire. Defaults to `agent` — pass `human`
   * to model a human-facing client.
   */
  readonly selfKind?: IdentityKind
  /**
   * Longest channel description this substrate will store, in characters.
   * Anything longer is refused with `ChannelDescriptionRejected` rather than
   * stored truncated — the same refusal a real substrate makes, so the
   * contract suite exercises that path here too.
   *
   * The default is deliberately *not* Zulip's 1024. Memory is a peer
   * substrate with its own limit, not a Zulip emulator, and a contract suite
   * that reads the limit from the environment it is given rather than
   * assuming a number is the thing worth proving.
   */
  readonly channelDescriptionLimit?: number
}

const DEFAULT_CHANNEL_DESCRIPTION_LIMIT = 512

export type MemoryAdapter = AgentComms & {
  /**
   * Make a channel addressable. Returns the same ref for repeated
   * names so callers can stash it once and re-seed cheaply.
   */
  readonly seedChannel: (name: string) => Effect.Effect<ChannelRef, ParseResult.ParseError>
  /**
   * Provision a peer identity (kind=agent by default). Returns the
   * existing identity for repeat names so tests can re-resolve safely.
   */
  readonly seedAgent: (name: string) => Effect.Effect<Identity, ParseResult.ParseError>
  /**
   * Same as `seedAgent` but yields kind=human — used by directory
   * tests and for modelling humans in MCP plugin tests.
   */
  readonly seedHuman: (name: string) => Effect.Effect<Identity, ParseResult.ParseError>
  /**
   * Inject a message authored by `peer` (sender ≠ the bound self) into the
   * substrate, running the same fan-out as a real post. The contract's
   * mention-floor tests use this to prove a peer's @-mention of self surfaces
   * on self's `events()` — a shape the single-identity `publisher.post`
   * (always authored as self) cannot express. Unlike `post`, requires no
   * bound identity: the peer is the author, not the adapter's own binding.
   */
  readonly peerPost: (
    peer: Identity,
    channel: ChannelName,
    body: MessageBodyType,
    opts?: PostOpts,
  ) => Effect.Effect<MessageRef, UnknownChannel>
}

const inRange =
  (range: Range) =>
  (m: StoredMessage): boolean => {
    if (range.since !== undefined && m.ts < range.since) return false
    if (range.until !== undefined && m.ts > range.until) return false
    return true
  }

const toMessage = (stored: StoredMessage, reactions: ReadonlyArray<Reaction>): Message => ({
  ref: stored.ref,
  sender: stored.sender,
  body: stored.body,
  ts: stored.ts,
  mentions: stored.mentions,
  reactions,
})

export const memoryAdapter = (config: MemoryAdapterConfig = {}): Effect.Effect<MemoryAdapter> =>
  Effect.gen(function* () {
    const presenceByIdentity = new Map<string, Presence>()
    const channelsById = new Map<string, ChannelRef>()
    const channelsByName = new Map<string, ChannelRef>()
    const identitiesById = new Map<string, Identity>()
    const identitiesByName = new Map<string, Identity>()
    const messagesByChannel = new Map<string, StoredMessage[]>()
    const messagesById = new Map<string, StoredMessage>()
    const reactionsByMessage = new Map<string, Map<Emoji, Set<string>>>()
    // Resolution is a status kept separate from the (always-clean) thread name,
    // per channel id → set of resolved thread names. Overlaid onto a ref's
    // thread at read time so a later resolve/unresolve is reflected without
    // rewriting stored messages.
    const resolvedThreadsByChannel = new Map<string, Set<string>>()
    // A channel's standing description, per channel id. Absent from the map is
    // the undescribed state — the port models that as Option.none, so there is
    // no blank-string entry to tell apart from a missing one.
    const descriptionsByChannel = new Map<string, ChannelDescription>()
    const descriptionLimit = config.channelDescriptionLimit ?? DEFAULT_CHANNEL_DESCRIPTION_LIMIT
    const isThreadResolved = (channelId: string, threadName: string): boolean =>
      resolvedThreadsByChannel.get(channelId)?.has(threadName) ?? false
    const withThreadResolution = (stored: StoredMessage): StoredMessage =>
      Option.match(stored.ref.thread, {
        onNone: () => stored,
        onSome: (thread) => ({
          ...stored,
          ref: {
            ...stored.ref,
            thread: Option.some({
              ...thread,
              resolved: isThreadResolved(stored.ref.channel.id, thread.name),
            }),
          },
        }),
      })

    const nextChannelId = yield* Ref.make(1)
    const nextIdentityId = yield* Ref.make(1)
    const nextMessageId = yield* Ref.make(1)
    const nextCredentialsId = yield* Ref.make(1)
    const nextTs = yield* Ref.make(Math.floor((yield* Clock.currentTimeMillis) / 1000))
    const bound = yield* Ref.make(Option.none<Binding>())

    const allowlist =
      config.acquirableNames === undefined ? undefined : new Set(config.acquirableNames)
    const selfKind: IdentityKind = config.selfKind ?? 'agent'

    const allocId = (counter: Ref.Ref<number>): Effect.Effect<number> =>
      Ref.getAndUpdate(counter, (n) => n + 1)

    const registerIdentity = (
      name: string,
      kind: IdentityKind,
    ): Effect.Effect<Identity, ParseResult.ParseError> =>
      Effect.gen(function* () {
        const existing = identitiesByName.get(name)
        if (existing !== undefined) return existing
        const id = yield* decodeIdentityId(String(yield* allocId(nextIdentityId))).pipe(
          Effect.orDie,
        )
        const displayName = yield* decodeDisplayName(name)
        const identity: Identity = { id, name: displayName, kind }
        identitiesById.set(id, identity)
        identitiesByName.set(name, identity)
        return identity
      })

    const requireBound = (): Effect.Effect<Identity> =>
      Ref.get(bound).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.die(
                new Error('memoryAdapter: not acquired — call identity.acquire(name) first'),
              ),
            onSome: (b) => Effect.succeed(b.identity),
          }),
        ),
      )

    const registerChannel = (name: string): Effect.Effect<ChannelRef, ParseResult.ParseError> =>
      Effect.gen(function* () {
        const existing = channelsByName.get(name)
        if (existing !== undefined) return existing
        const id = yield* decodeChannelId(String(yield* allocId(nextChannelId))).pipe(Effect.orDie)
        const channelName = yield* decodeChannelName(name)
        const ref: ChannelRef = { id, name: channelName, permalink: synthChannelPermalink(id) }
        channelsById.set(id, ref)
        channelsByName.set(name, ref)
        messagesByChannel.set(id, [])
        return ref
      })

    // Effect-returning channel resolution shared by publisher.post and the
    // history readers: channels are addressed by name, so an unregistered name
    // is a typed UnknownChannel. post surfaces it directly; the history readers
    // map it into a HistoryError to match their port signature (and the Zulip
    // adapter).
    const resolveChannel = (name: ChannelName): Effect.Effect<ChannelRef, UnknownChannel> => {
      const existing = channelsByName.get(name)
      return existing === undefined
        ? Effect.fail(new UnknownChannel({ channel: name, substrate: 'memory' }))
        : Effect.succeed(existing)
    }

    // The bucket is created alongside the channel in registerChannel, so a
    // resolved ref whose bucket is missing is an invariant violation (a defect).
    const bucketOf = (channel: ChannelRef): Effect.Effect<StoredMessage[]> => {
      const bucket = messagesByChannel.get(channel.id)
      return bucket === undefined
        ? Effect.die(new Error(`memoryAdapter: missing message bucket for channel ${channel.name}`))
        : Effect.succeed(bucket)
    }

    const resolveBucket = (name: ChannelName): Effect.Effect<StoredMessage[], UnknownChannel> =>
      resolveChannel(name).pipe(Effect.flatMap(bucketOf))

    const identity: IdentityPort = {
      currentIdentity: () => requireBound(),
      acquire: (name) =>
        Ref.get(bound).pipe(
          Effect.flatMap(
            Option.match({
              onSome: (b) =>
                b.acquiredName === name
                  ? Effect.succeed<AcquiredIdentity>(b)
                  : Effect.die(
                      new Error(
                        `memoryAdapter: already bound to ${b.acquiredName} — release() before acquiring ${name}`,
                      ),
                    ),
              onNone: () =>
                allowlist !== undefined && !allowlist.has(name)
                  ? Effect.fail(new UnknownIdentity({ name, substrate: 'memory' }))
                  : // `name` is a validated non-empty BotName, so DisplayName
                    // decode inside registerIdentity cannot fail — orDie the
                    // (impossible) ParseError to keep acquire's typed channel
                    // UnknownIdentity-only.
                    registerIdentity(name, selfKind).pipe(
                      Effect.orDie,
                      Effect.flatMap((ident) =>
                        allocId(nextCredentialsId).pipe(
                          Effect.flatMap((credId) => {
                            const credentials: Credentials = {
                              substrate: 'memory',
                              identityId: ident.id,
                              token: `memory-credential-${credId}`,
                            }
                            const acquired: Binding = {
                              acquiredName: name,
                              identity: ident,
                              credentials,
                            }
                            // Atomic check-then-set: claim the slot only if
                            // still free, otherwise honour the winner. Loses
                            // a counter id under contention, never the bind.
                            return Ref.modify(
                              bound,
                              (current): [AcquiredIdentity, Option.Option<Binding>] =>
                                Option.match(current, {
                                  onNone: () => [acquired, Option.some(acquired)],
                                  onSome: (winner) => [winner, current],
                                }),
                            ).pipe(
                              Effect.tap((winner) =>
                                winner === acquired
                                  ? Ref.update(subscriptions, HashSet.add<SubKey>(MENTIONS_KEY))
                                  : Effect.void,
                              ),
                            )
                          }),
                        ),
                      ),
                    ),
            }),
          ),
        ),
      release: () => Ref.set(bound, Option.none()),
      resolve: (name) => Effect.sync(() => Option.fromNullable(identitiesByName.get(name))),
    }

    const buildRef = (
      id: string,
      channel: ChannelRef,
      thread: PostOpts['thread'],
    ): Effect.Effect<MessageRef> =>
      // `id` is a monotonic counter stringified, never empty — orDie the
      // (impossible) ParseError to keep post's typed channel intact.
      decodeMessageId(id).pipe(
        Effect.orDie,
        Effect.map((messageId) =>
          thread === undefined
            ? {
                id: messageId,
                channel,
                thread: Option.none(),
                permalink: synthMessagePermalink(channel.id, messageId),
              }
            : {
                id: messageId,
                channel,
                thread: Option.some({
                  name: thread,
                  resolved: false,
                  permalink: synthTopicPermalink(channel.id, thread),
                }),
                permalink: synthMessagePermalink(channel.id, messageId, thread),
              },
        ),
      )

    const collectReactions = (messageId: string): ReadonlyArray<Reaction> => {
      const byEmoji = reactionsByMessage.get(messageId)
      if (byEmoji === undefined) return []
      const out: Reaction[] = []
      for (const [emoji, reactors] of byEmoji) {
        const by: Identity[] = []
        for (const reactorId of reactors) {
          const ident = identitiesById.get(reactorId)
          if (ident !== undefined) by.push(ident)
        }
        if (by.length > 0) out.push({ emoji, by })
      }
      return out
    }

    const reactionsForMessage = (messageId: string): Map<Emoji, Set<string>> => {
      const existing = reactionsByMessage.get(messageId)
      if (existing !== undefined) return existing
      const created = new Map<Emoji, Set<string>>()
      reactionsByMessage.set(messageId, created)
      return created
    }

    // Resolution is tracked as a status separate from the thread name (the name
    // never encodes it), surfaced on reads via ObservedThread.resolved.
    // Idempotent — re-setting the same state is a no-op. Like edit, emits no
    // InboundEvent. Mirrors the Zulip adapter's "no messages in the thread →
    // PublisherError" so the contract holds across both substrates; an unknown
    // channel likewise has no such thread and surfaces the same failure.
    const setThreadResolved = (
      channel: ChannelName,
      thread: ThreadName,
      resolved: boolean,
    ): Effect.Effect<void, PublisherError> => {
      const operation = resolved ? 'resolveThread' : 'unresolveThread'
      return requireBound().pipe(
        Effect.flatMap(() =>
          resolveChannel(channel).pipe(
            Effect.mapError((cause): PublisherError => new PublisherError({ operation, cause })),
            Effect.flatMap((channelRef) => {
              const bucket = messagesByChannel.get(channelRef.id) ?? []
              const hasThread = bucket.some((m) =>
                Option.exists(m.ref.thread, (t) => t.name === thread),
              )
              if (!hasThread) {
                return Effect.fail(
                  new PublisherError({
                    operation,
                    cause: new Error(
                      `memoryAdapter: no thread '${thread}' in ${channelRef.name} to ${
                        resolved ? 'resolve' : 'unresolve'
                      }`,
                    ),
                  }),
                )
              }
              const resolvedNames = resolvedThreadsByChannel.get(channelRef.id) ?? new Set<string>()
              if (resolved) resolvedNames.add(thread)
              else resolvedNames.delete(thread)
              if (resolvedNames.size > 0) resolvedThreadsByChannel.set(channelRef.id, resolvedNames)
              else resolvedThreadsByChannel.delete(channelRef.id)
              return Effect.void
            }),
          ),
        ),
      )
    }

    // A real peer implementation, not a stub: the description round-trips
    // verbatim through `directory.channelDescription`, and the two ways a
    // substrate can fail to store one are both reproduced so the contract has
    // something to assert on either side. `'length'` is this substrate's own
    // cap (see MemoryAdapterConfig.channelDescriptionLimit); `'format'` mirrors
    // the fact that a substrate may be unable to hold a multi-line description
    // — Zulip rewrites newlines to spaces, so refusing them keeps "what you
    // read back is what you wrote" true on both substrates instead of only
    // this one. Idempotent: writing the text already stored changes nothing.
    const setChannelDescription = (
      channel: ChannelName,
      description: Option.Option<ChannelDescription>,
    ): Effect.Effect<void, UnknownChannel | ChannelDescriptionRejected> =>
      requireBound().pipe(
        Effect.flatMap(() => resolveChannel(channel)),
        Effect.flatMap((channelRef) =>
          Option.match(description, {
            onNone: () =>
              Effect.sync(() => {
                descriptionsByChannel.delete(channelRef.id)
              }),
            onSome: (text) =>
              text.length > descriptionLimit
                ? Effect.fail(
                    new ChannelDescriptionRejected({
                      channel: channelRef.name,
                      substrate: 'memory',
                      constraint: 'length',
                      detail: `description is too long (limit: ${descriptionLimit} characters, got ${text.length})`,
                    }),
                  )
                : text.includes('\n')
                  ? Effect.fail(
                      new ChannelDescriptionRejected({
                        channel: channelRef.name,
                        substrate: 'memory',
                        constraint: 'format',
                        detail: 'description must be a single line — newlines are not stored',
                      }),
                    )
                  : Effect.sync(() => {
                      descriptionsByChannel.set(channelRef.id, text)
                    }),
          }),
        ),
      )

    const publisher: MessagePublisher = {
      // Posting before acquire is an invariant violation (a defect via
      // requireBound's die); an unknown channel is the one typed failure.
      post: (channel, body, opts) =>
        Effect.gen(function* () {
          const self = yield* requireBound()
          const channelRef = yield* resolveChannel(channel)
          const bucket = yield* bucketOf(channelRef)
          const id = String(yield* allocId(nextMessageId))
          const ref = yield* buildRef(id, channelRef, opts?.thread)
          // The timestamp counter is a monotonic clock-derived value, always
          // non-negative — orDie the (impossible) ParseError.
          const ts = yield* decodeTimestamp(yield* allocId(nextTs)).pipe(Effect.orDie)
          const stored: StoredMessage = {
            ref,
            sender: self,
            body,
            ts,
            mentions: opts?.mentions === undefined ? [] : [...opts.mentions],
          }
          bucket.push(stored)
          messagesById.set(id, stored)
          yield* fanOutOnPost(stored)
          return ref
        }),
      // Editing before acquire is an invariant violation (a defect via
      // requireBound's die); editing a message the store has never seen
      // is a non-fatal domain failure surfaced as a typed PublisherError,
      // mirroring the Zulip adapter's PATCH-on-unknown-id → PublisherError.
      edit: (message, body) =>
        requireBound().pipe(
          Effect.flatMap(() => {
            const stored = messagesById.get(message.id)
            if (stored === undefined) {
              return Effect.fail(
                new PublisherError({
                  operation: 'edit',
                  cause: new Error(`memoryAdapter: no message ${message.id} to edit`),
                }),
              )
            }
            const updated: StoredMessage = { ...stored, body }
            messagesById.set(message.id, updated)
            const bucket = messagesByChannel.get(stored.ref.channel.id)
            if (bucket !== undefined) {
              const idx = bucket.indexOf(stored)
              if (idx !== -1) bucket[idx] = updated
            }
            return Effect.void
          }),
        ),
      // The in-memory store has no realm-wide editing switch to turn off, so
      // editing is always available — the honest answer for a substrate whose
      // only edit refusals are per-message.
      editingAvailable: () => Effect.succeed(true),
      react: (message, emoji) =>
        requireBound().pipe(
          Effect.flatMap((self) => {
            const byEmoji = reactionsForMessage(message.id)
            const reactors = byEmoji.get(emoji) ?? new Set<string>()
            reactors.add(self.id)
            byEmoji.set(emoji, reactors)
            return fanOutOnReaction(message.id, 'reaction-added', emoji, self)
          }),
        ),
      unreact: (message, emoji) =>
        requireBound().pipe(
          Effect.flatMap((self) => {
            const byEmoji = reactionsByMessage.get(message.id)
            const reactors = byEmoji?.get(emoji)
            if (reactors !== undefined) {
              reactors.delete(self.id)
              if (reactors.size === 0) byEmoji?.delete(emoji)
            }
            return fanOutOnReaction(message.id, 'reaction-removed', emoji, self)
          }),
        ),
      resolveThread: (channel, thread) => setThreadResolved(channel, thread, true),
      unresolveThread: (channel, thread) => setThreadResolved(channel, thread, false),
      setChannelDescription,
    }

    // Channels are addressed by name, so subscription keys use `ChannelName` —
    // the same value the subscription target carries and that a stored
    // message's observation exposes as `ref.channel.name`.
    const MENTIONS_KEY = Data.struct({ kind: 'mentions' as const })
    const channelSubKey = (channelName: ChannelName) =>
      Data.struct({ kind: 'channel' as const, channelName })
    const threadSubKey = (channelName: ChannelName, threadName: string) =>
      Data.struct({ kind: 'thread' as const, channelName, threadName })
    const newTopicsSubKey = (channelName: ChannelName) =>
      Data.struct({ kind: 'newTopics' as const, channelName })
    type SubKey =
      | typeof MENTIONS_KEY
      | ReturnType<typeof channelSubKey>
      | ReturnType<typeof threadSubKey>
      | ReturnType<typeof newTopicsSubKey>
    const subscriptionKey = (target: SubscriptionTarget): SubKey => {
      if (target === 'mentions') return MENTIONS_KEY
      if (Predicate.hasProperty(target, 'kind')) return newTopicsSubKey(target.channel)
      if (Predicate.hasProperty(target, 'thread'))
        return threadSubKey(target.channel, target.thread)
      return channelSubKey(target)
    }

    const subscriptions = yield* Ref.make(HashSet.empty<SubKey>())
    const seenTopicsByChannel = new Map<string, Set<string>>()

    // Events posted while no Stream subscription is active accumulate in
    // `eventQueue`; the next `events()` subscription drains it (FIFO) before
    // installing its own emit hook. This preserves the "subscribe → post →
    // events() observes the post" contract for callers that subscribe first
    // and consume later. `activeEmit` holds the live emit hook (or none);
    // dispatch reads it then routes. Both are construction-time state — the
    // pending events an unbounded `Queue`, the live hook a `Ref`.
    const eventQueue = yield* Queue.unbounded<InboundEvent>()
    const activeEmit = yield* Ref.make(Option.none<Emit>())

    const dispatchEvent = (event: InboundEvent): Effect.Effect<void> =>
      Ref.get(activeEmit).pipe(
        Effect.flatMap(
          Option.match({
            onSome: (emit) => Effect.sync(() => emit(event)),
            onNone: () => Queue.offer(eventQueue, event).pipe(Effect.asVoid),
          }),
        ),
      )

    const matchesStaticSub = (stored: StoredMessage): Effect.Effect<boolean> =>
      Effect.all([Ref.get(bound), Ref.get(subscriptions)]).pipe(
        Effect.map(([current, subs]) => {
          if (HashSet.has(subs, channelSubKey(stored.ref.channel.name))) return true
          const thread = stored.ref.thread
          if (
            Option.isSome(thread) &&
            HashSet.has(subs, threadSubKey(stored.ref.channel.name, thread.value.name))
          ) {
            return true
          }
          if (HashSet.has(subs, MENTIONS_KEY) && Option.isSome(current)) {
            const me = current.value.identity
            if (stored.mentions.some((m) => m.id === me.id)) return true
          }
          return false
        }),
      )

    const deliverableForPost = (stored: StoredMessage): Effect.Effect<boolean> =>
      Effect.all([matchesStaticSub(stored), Ref.get(subscriptions)]).pipe(
        Effect.map(([matches, subs]) => {
          const thread = stored.ref.thread
          if (
            Option.isSome(thread) &&
            HashSet.has(subs, newTopicsSubKey(stored.ref.channel.name))
          ) {
            const seen = seenTopicsByChannel.get(stored.ref.channel.id) ?? new Set<string>()
            if (!seen.has(thread.value.name)) {
              seen.add(thread.value.name)
              seenTopicsByChannel.set(stored.ref.channel.id, seen)
              return true
            }
          }
          return matches
        }),
      )

    const fanOutOnPost = (stored: StoredMessage): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!(yield* deliverableForPost(stored))) return
        const portMessage = toMessage(withThreadResolution(stored), collectReactions(stored.ref.id))
        yield* dispatchEvent({ kind: 'message-posted', message: portMessage })
        const current = yield* Ref.get(bound)
        if (Option.isSome(current)) {
          const me = current.value.identity
          if (stored.mentions.some((m) => m.id === me.id)) {
            yield* dispatchEvent({
              kind: 'mention-received',
              message: portMessage,
              mentions: portMessage.mentions,
            })
          }
        }
      })

    const fanOutOnReaction = (
      messageId: string,
      kind: 'reaction-added' | 'reaction-removed',
      emoji: Emoji,
      by: Identity,
    ): Effect.Effect<void> => {
      const stored = messagesById.get(messageId)
      if (stored === undefined) return Effect.void
      return matchesStaticSub(stored).pipe(
        Effect.flatMap((matches) =>
          matches ? dispatchEvent({ kind, target: stored.ref, emoji, by }) : Effect.void,
        ),
      )
    }

    const inbox: MessageInbox = {
      subscribe: (target) => Ref.update(subscriptions, HashSet.add(subscriptionKey(target))),
      unsubscribe: (target) => Ref.update(subscriptions, HashSet.remove(subscriptionKey(target))),
      events: () =>
        Stream.asyncPush<InboundEvent>((emit) =>
          Effect.acquireRelease(
            Effect.gen(function* () {
              // Drain anything dispatched while no subscription was active.
              const queued = yield* Queue.takeAll(eventQueue)
              for (const ev of queued) void emit.single(ev)
              const cb: Emit = (event) => {
                void emit.single(event)
              }
              yield* Ref.set(activeEmit, Option.some(cb))
              return cb
            }),
            // Relinquish the slot on scope close, only if we still own it.
            (cb) =>
              Ref.update(activeEmit, (current) =>
                Option.exists(current, (live) => live === cb) ? Option.none() : current,
              ),
          ),
        ),
      replay: (since: TimestampType) =>
        requireBound().pipe(
          Effect.map((me) => {
            const ordered: StoredMessage[] = []
            for (const bucket of messagesByChannel.values()) {
              for (const stored of bucket) {
                if (stored.ts < since) continue
                ordered.push(stored)
              }
            }
            const sorted = Arr.sort(
              ordered,
              Order.mapInput(Order.number, (m: StoredMessage) => m.ts),
            )
            const out: InboundEvent[] = []
            for (const stored of sorted) {
              const portMessage = toMessage(
                withThreadResolution(stored),
                collectReactions(stored.ref.id),
              )
              out.push({ kind: 'message-posted', message: portMessage })
              if (stored.mentions.some((m) => m.id === me.id)) {
                out.push({
                  kind: 'mention-received',
                  message: portMessage,
                  mentions: portMessage.mentions,
                })
              }
            }
            return out
          }),
        ),
    }

    const applyLimit = (
      messages: ReadonlyArray<StoredMessage>,
      limit: number | undefined,
    ): ReadonlyArray<StoredMessage> => (limit === undefined ? messages : messages.slice(0, limit))

    const projectStored = (stored: StoredMessage): Message =>
      toMessage(withThreadResolution(stored), collectReactions(stored.ref.id))

    const history: HistoryReader = {
      // An unknown channel is a non-fatal domain failure on the typed E
      // channel (HistoryError wrapping UnknownChannel), not a defect —
      // resolveBucket fails with UnknownChannel and mapError tags it,
      // mirroring the Zulip adapter's readChannel/readThread.
      readChannel: (channel, range) =>
        resolveBucket(channel).pipe(
          Effect.map((bucket) => {
            const filtered = bucket.filter(inRange(range))
            return applyLimit(filtered, range.limit).map(projectStored)
          }),
          Effect.mapError((cause) => new HistoryError({ operation: 'readChannel', cause })),
        ),
      readThread: (channel, threadName, range) =>
        resolveBucket(channel).pipe(
          Effect.map((bucket) => {
            const filtered = bucket
              .filter((m) => Option.exists(m.ref.thread, (t) => t.name === threadName))
              .filter(inRange(range ?? {}))
            return applyLimit(filtered, range?.limit).map(projectStored)
          }),
          Effect.mapError((cause) => new HistoryError({ operation: 'readThread', cause })),
        ),
      recentThreads: (sender, opts) =>
        Effect.sync(() => {
          const limit = opts?.limit ?? 10
          const collected: StoredMessage[] = []
          for (const bucket of messagesByChannel.values()) {
            for (const stored of bucket) {
              if (stored.sender.id === sender && Option.isSome(stored.ref.thread)) {
                collected.push(stored)
              }
            }
          }
          const bySender = Arr.sort(
            collected,
            Order.reverse(Order.mapInput(Order.number, (m: StoredMessage) => m.ts)),
          )
          const threadKey = (channel: string, thread: string) => Data.struct({ channel, thread })
          let seen = HashMap.empty<ReturnType<typeof threadKey>, StoredMessage>()
          for (const msg of bySender) {
            const thread = msg.ref.thread
            if (Option.isNone(thread)) continue
            const key = threadKey(msg.ref.channel.name, thread.value.name)
            if (!HashMap.has(seen, key)) seen = HashMap.set(seen, key, msg)
            if (HashMap.size(seen) >= limit) break
          }
          const threads: RecentThread[] = []
          for (const msg of HashMap.values(seen)) {
            const thread = msg.ref.thread
            if (Option.isNone(thread)) continue
            threads.push({
              channel: msg.ref.channel.name,
              thread: thread.value.name,
              lastPostTs: msg.ts,
              lastPostBody: msg.body,
            })
          }
          return Arr.sort(
            threads,
            Order.reverse(Order.mapInput(Order.number, (t: RecentThread) => t.lastPostTs)),
          )
        }),
      messagePermalink: (id, hint) =>
        Effect.sync(() => {
          if (hint !== undefined) {
            return Option.fromNullable(channelsByName.get(hint.channel)).pipe(
              Option.map((channel) => synthMessagePermalink(channel.id, id, hint.thread)),
            )
          }
          const stored = messagesById.get(id)
          return stored === undefined
            ? Option.none<MessagePermalink>()
            : Option.some(stored.ref.permalink)
        }),
    }

    const directory: Directory = {
      listAgents: () =>
        Effect.succeed([...identitiesById.values()].filter((i) => i.kind === 'agent')),
      listHumans: () =>
        Effect.succeed([...identitiesById.values()].filter((i) => i.kind === 'human')),
      listChannels: () => Effect.succeed([...channelsById.values()]),
      channelDescription: (channel) =>
        resolveChannel(channel).pipe(
          Effect.map((channelRef) => Option.fromNullable(descriptionsByChannel.get(channelRef.id))),
        ),
      presence: (target) => Effect.succeed(presenceByIdentity.get(target.id) ?? 'offline'),
    }

    // seed* expose the registration Effects directly: a malformed name
    // surfaces as a typed ParseError in the E channel, run by the caller.
    const seedChannel = (name: string): Effect.Effect<ChannelRef, ParseResult.ParseError> =>
      registerChannel(name)
    const seedAgent = (name: string): Effect.Effect<Identity, ParseResult.ParseError> =>
      registerIdentity(name, 'agent')
    const seedHuman = (name: string): Effect.Effect<Identity, ParseResult.ParseError> =>
      registerIdentity(name, 'human')

    // Authored by `peer`, not the bound self: mirrors publisher.post's store +
    // fan-out path but stamps `sender: peer` and skips requireBound. The
    // monotonic `nextTs` counter already yields a distinct ts per message, so
    // memory reports `Duration.zero` granularity below — no spacing needed.
    const peerPost = (
      peer: Identity,
      channel: ChannelName,
      body: MessageBodyType,
      opts?: PostOpts,
    ): Effect.Effect<MessageRef, UnknownChannel> =>
      Effect.gen(function* () {
        const channelRef = yield* resolveChannel(channel)
        const bucket = yield* bucketOf(channelRef)
        const id = String(yield* allocId(nextMessageId))
        const ref = yield* buildRef(id, channelRef, opts?.thread)
        const ts = yield* decodeTimestamp(yield* allocId(nextTs)).pipe(Effect.orDie)
        const stored: StoredMessage = {
          ref,
          sender: peer,
          body,
          ts,
          mentions: opts?.mentions === undefined ? [] : [...opts.mentions],
        }
        bucket.push(stored)
        messagesById.set(id, stored)
        yield* fanOutOnPost(stored)
        return ref
      })

    return {
      // Memory's `ts` is a monotonic counter, so any two posts already differ —
      // no real-time spacing is needed for distinct timestamps.
      capabilities: { timestampGranularity: Duration.zero },
      identity,
      publisher,
      inbox,
      history,
      directory,
      seedChannel,
      seedAgent,
      seedHuman,
      peerPost,
    }
  })
