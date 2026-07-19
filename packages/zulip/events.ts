/**
 * Zulip events queue ↔ AgentComms `MessageInbox.events()` mapping.
 *
 * Zulip exposes a long-polling events queue: register once via
 * `POST /register`, then chain `GET /events?queue_id=…&last_event_id=…`
 * round-trips, advancing `last_event_id` from each response. Every poll
 * holds for ~50s before Zulip heartbeats, and the next poll must re-issue
 * immediately.
 *
 * The returned Stream owns the long-poll loop as a Schedule-driven
 * unfold. When the consumer interrupts the fiber, the in-flight HTTP
 * request unwinds via @effect/platform's HttpClient — no manual abort
 * plumbing is needed because every layer is Effect-native.
 */

import { messageOf } from '@commy/core/messageOf'
import type { Identity, InboundEvent, InboxError, MessageRef, Timestamp } from '@commy/core/ports'
import {
  decodeChannelId,
  decodeChannelName,
  decodeDisplayName,
  decodeEmoji,
  decodeIdentityId,
  decodeMessageBody,
  decodeMessageId,
  decodeThreadName,
  decodeTimestamp,
} from '@commy/core/ports'
import {
  Chunk,
  Duration,
  Effect,
  Either,
  Option,
  type ParseResult,
  Ref,
  Schedule,
  Schema,
  Stream,
} from 'effect'
import type { ZulipApiError, ZulipHttp } from './http.ts'
import { extractMentions } from './mentions.ts'
import { buildMessageRef } from './permalink.ts'
import { splitTopic } from './resolved-topic.ts'

export interface DirectoryLookup {
  readonly byId: ReadonlyMap<number, Identity>
  readonly byName: ReadonlyMap<string, Identity>
}

/**
 * LRU cache of MessageRefs keyed by Zulip message id. Reaction events
 * on Zulip's queue carry `message_id` but not stream/topic context —
 * resolving an `InboundEvent.target: MessageRef` therefore needs a
 * lookup. The adapter populates this cache as `message-posted` events
 * flow through `events()`, so subsequent reaction events on those same
 * messages resolve in O(1) without an extra round-trip. On a miss the
 * iterator falls back to `GET /messages?anchor=<id>&num_before=0&num_after=0`
 * and write-throughs the result, so reactions on messages
 * the iterator never observed still resolve at the cost of one extra
 * GET per first-time miss.
 */
export interface MessageRefCache {
  set(id: string, ref: MessageRef): void
  get(id: string): Option.Option<MessageRef>
}

export const createMessageRefCache = (maxSize = 10_000): MessageRefCache => {
  const map = new Map<string, MessageRef>()
  return {
    set(id, ref) {
      if (map.has(id)) map.delete(id)
      map.set(id, ref)
      if (map.size > maxSize) {
        const oldest = map.keys().next().value
        if (oldest !== undefined) map.delete(oldest)
      }
    },
    get(id) {
      const ref = map.get(id)
      if (ref !== undefined) {
        map.delete(id)
        map.set(id, ref)
      }
      return Option.fromNullable(ref)
    },
  }
}

/**
 * Monotonic watermark of the latest live message timestamp the producer
 * has dispatched. Lifted out of the producer's closure so a fresh
 * Stream subscription created by an upstream retry inherits the
 * previous run's anchor — without it, the first
 * BAD_EVENT_QUEUE_ID after reconnect would skip the gap-replay,
 * which is exactly when the replay matters most. Owned by
 * the adapter alongside `MessageRefCache` so its state survives across
 * `events()` calls; `advance` is monotonic so out-of-order timestamps
 * don't roll the watermark backwards.
 */
export interface WatermarkStore {
  get(): Effect.Effect<Option.Option<Timestamp>>
  advance(ts: Timestamp): Effect.Effect<void>
}

export const createWatermarkStore = (): Effect.Effect<WatermarkStore> =>
  Ref.make(Option.none<Timestamp>()).pipe(
    Effect.map((ref) => ({
      get: () => Ref.get(ref),
      advance: (next: Timestamp) =>
        Ref.update(
          ref,
          Option.match({
            onNone: () => Option.some(next),
            onSome: (prev) => (next > prev ? Option.some(next) : Option.some(prev)),
          }),
        ),
    })),
  )

export interface EventsConfig {
  readonly http: ZulipHttp
  /**
   * Human-facing realm origin for narrow permalinks. The adapter
   * resolves it once from its config (public host when a Host-header override
   * is set) and hands it down so every inbound ref carries a clickable URL.
   */
  readonly permalinkBase: string
  /**
   * Resolve the realm's user directory. Called once per long-poll
   * batch so the mapping picks up users joining mid-session without
   * round-tripping per event.
   */
  readonly resolveDirectory: () => Effect.Effect<
    DirectoryLookup,
    ZulipApiError | ParseResult.ParseError
  >
  /**
   * When `'mentions'`, the events queue is registered with a
   * narrow that only matches messages mentioning the bot. Otherwise
   * all messages visible to the bot's subscriptions are surfaced.
   */
  readonly mode: 'all' | 'mentions'
  /**
   * Identity of the bot bound to this inbox. When provided,
   * `mention-received` is synthesised whenever the bound identity
   * appears in a message's extracted mentions — regardless of the
   * raw `flags.mentioned` (which Zulip keys to the events queue
   * owner, not the mentioned user). Omit pre-bind: no
   * `mention-received` events will be emitted until acquired.
   */
  readonly boundIdentity?: Identity
  /**
   * Pre-registered events queue. When supplied, the producer skips
   * its own `POST /register` round-trip and resumes from this state
   * — used by the adapter to satisfy `subscribe()`'s readiness
   * contract (register at subscribe-time, hand the resulting queue
   * to the first `events()` call). Omit to fall back to the lazy
   * register-on-first-poll path, which is fine for consumers
   * that call `events()` without a preceding `subscribe()`.
   */
  readonly initialQueue?: QueueState
  /**
   * Shared cache for resolving reaction events' target MessageRef.
   * Lives at the adapter so its state survives across producer
   * instances. Omit to disable reaction event emission — reactions
   * are dropped silently when the cache is absent.
   */
  readonly messageRefCache?: MessageRefCache
  /**
   * Shared monotonic watermark for the gap-replay anchor.
   * When supplied, the producer reads its initial value on the first
   * BAD_EVENT_QUEUE_ID and advances it as live messages dispatch.
   * Wiring this at the adapter lets a fresh Stream subscription
   * created by upstream auto-reconnect pick up the
   * previous run's watermark — the BAD_EVENT_QUEUE_ID gap-replay
   * then fires on the new subscription's first poll
   * instead of skipping it. Omit to fall back to per-run watermark
   * semantics: each Stream subscription builds its own anchor, and a
   * fresh subscription's first BAD_EVENT_QUEUE_ID can't replay
   * because it has no live message yet.
   */
  readonly watermarkStore?: WatermarkStore
  /**
   * Gap-replay callback invoked on BAD_EVENT_QUEUE_ID recovery
   * When set, the producer calls `replay(lastSeenTs)`
   * before re-registering the events queue, flags each returned
   * event with `replayed: true`, and emits them into the live stream
   * so the consumer sees messages posted during the dead window.
   * Omit to fall back to silent-reconnect behaviour (messages
   * during the gap are lost). The callback is also skipped on the
   * first BAD_EVENT_QUEUE_ID before any live message has surfaced —
   * there is no watermark to anchor the replay window in that case.
   */
  readonly replay?: (since: Timestamp) => Effect.Effect<ReadonlyArray<InboundEvent>, InboxError>
  /**
   * Idle timeout (seconds) sent as `idle_queue_timeout` on the producer's
   * own `POST /register` (the lazy / re-register-after-BAD_EVENT_QUEUE_ID
   * path). Omit to register with no explicit timeout — the eager
   * adapter-side register carries its own copy of this for the
   * subscribe-time queue.
   */
  readonly queueIdleTimeoutSecs?: number
  /**
   * Best-effort hook fired with the freshly registered queue whenever the
   * producer registers one itself (lazy first poll or re-register after a
   * dead queue). The adapter persists the `{queueId, lastEventId}` to the
   * per-session queue-state store so a long-idle resume can recover it.
   * Total by contract (`Effect<void>` — never fails); the persistence
   * discipline (session gate, swallow, non-blocking) lives in the closure.
   */
  readonly onQueueRegister?: (queue: QueueState) => Effect.Effect<void>
  /**
   * Best-effort hook fired with the per-poll maximum event id whenever a
   * poll pulls the cursor forward. The adapter advances the persisted
   * `lastEventId` (monotonic) so a resume replays only what was missed.
   * Fired from the producer because only the `step` sees the per-poll max —
   * the pump never does. Total by contract (`Effect<void>`); monotonicity
   * and swallowing live in the closure.
   */
  readonly onQueueAdvance?: (lastEventId: number) => Effect.Effect<void>
  /**
   * One-shot hook reporting the outcome of the FIRST poll: `true` when it
   * succeeds, `false` when it hits `BAD_EVENT_QUEUE_ID`. The adapter wires this
   * only on a genuine resume (an `initialQueue` sourced from persisted
   * queue-state), so `true` means "the surviving queue replayed the downtime
   * backlog natively" and `false` means "the persisted queue was dead — the
   * seat must fall back to history catch-up". Fired at most once; a later
   * mid-life `BAD_EVENT_QUEUE_ID` never re-reports. Total by contract
   * (`Effect<void>`); the rendezvous (a shared `Deferred`, first write wins)
   * lives in the closure.
   */
  readonly onResumeOutcome?: (queueReplayed: boolean) => Effect.Effect<void>
}

const registerResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  queue_id: Schema.String,
  last_event_id: Schema.Int,
})

// The envelope validates the events array loosely so a
// single shape-violating event cannot fail the whole-batch parse and
// crash the pump. Per-event envelope validation
// (`eventEnvelopeSchema` below) runs inside the for-loop where a bad
// event can be logged and skipped individually.
const eventsResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  events: Schema.Array(Schema.Unknown),
})

const eventEnvelopeSchema = Schema.Struct({
  id: Schema.Int,
  type: Schema.String,
})
const decodeEventEnvelope = Schema.decodeUnknownEither(eventEnvelopeSchema, {
  onExcessProperty: 'preserve',
})

export const zulipMessageContentSchema = Schema.Struct({
  id: Schema.Int,
  sender_id: Schema.Int,
  sender_full_name: Schema.String,
  stream_id: Schema.Int,
  display_recipient: Schema.String,
  subject: Schema.NonEmptyString,
  content: Schema.String,
  timestamp: Schema.NonNegative,
})
const decodeZulipMessageContent = Schema.decodeUnknown(zulipMessageContentSchema, {
  onExcessProperty: 'preserve',
})

export type ParsedZulipMessage = Schema.Schema.Type<typeof zulipMessageContentSchema>

const zulipReactionEventSchema = Schema.Struct({
  id: Schema.Int,
  type: Schema.Literal('reaction'),
  op: Schema.Literal('add', 'remove'),
  user_id: Schema.Int,
  message_id: Schema.Int,
  emoji_name: Schema.String,
})
const decodeZulipReactionEvent = Schema.decodeUnknownEither(zulipReactionEventSchema, {
  onExcessProperty: 'preserve',
})

export type ParsedZulipReactionEvent = Schema.Schema.Type<typeof zulipReactionEventSchema>

const decodeMessageRef = (
  message: ParsedZulipMessage,
  base: string,
): Effect.Effect<MessageRef, ParseResult.ParseError> =>
  Effect.gen(function* () {
    const id = yield* decodeMessageId(String(message.id))
    const channelId = yield* decodeChannelId(String(message.stream_id))
    const channelName = yield* decodeChannelName(message.display_recipient)
    // A resolved topic arrives ✔-prefixed; split the marker off so the port
    // sees a clean name and the resolution surfaces as ObservedThread.resolved.
    const { name, resolved } = splitTopic(message.subject)
    const threadName = yield* decodeThreadName(name)
    return buildMessageRef(
      base,
      id,
      { id: channelId, name: channelName },
      { name: threadName, resolved },
    )
  })

const decodeSenderIdentity = (
  message: ParsedZulipMessage,
  directory: DirectoryLookup,
): Effect.Effect<Identity, ParseResult.ParseError> => {
  const cached = directory.byId.get(message.sender_id)
  if (cached !== undefined) return Effect.succeed(cached)
  return Effect.gen(function* () {
    const id = yield* decodeIdentityId(String(message.sender_id))
    const name = yield* decodeDisplayName(message.sender_full_name)
    return { id, name, kind: 'human' } satisfies Identity
  })
}

export const messageToInboundEvents = (
  message: ParsedZulipMessage,
  directory: DirectoryLookup,
  boundIdentity: Identity | undefined,
  base: string,
): Effect.Effect<ReadonlyArray<InboundEvent>, ParseResult.ParseError> =>
  Effect.gen(function* () {
    const sender = yield* decodeSenderIdentity(message, directory)
    const ref = yield* decodeMessageRef(message, base)
    const body = yield* decodeMessageBody(message.content)
    const ts = yield* decodeTimestamp(message.timestamp)
    const portMessage = {
      ref,
      sender,
      body,
      ts,
      mentions: extractMentions(message.content, {
        byName: directory.byName,
        byUserId: (userId) => directory.byId.get(userId),
      }),
      // Reactions arrive as separate `reaction` events. A freshly-posted
      // message carries no reaction state — anything that exists already
      // (via history reads) surfaces through the HistoryReader path.
      reactions: [],
    }
    const out: InboundEvent[] = [{ kind: 'message-posted', message: portMessage }]
    // Synthesise from content, not from Zulip's `flags.mentioned` — on the
    // real realm the events queue is registered against the minter, so the
    // flag is keyed to the queue owner rather than the bound bot. The
    // extracted mentions list is the authoritative answer to "was the bound
    // bot mentioned in this message?".
    if (
      boundIdentity !== undefined &&
      portMessage.mentions.some((m) => m.id === boundIdentity.id)
    ) {
      out.push({
        kind: 'mention-received',
        message: portMessage,
        mentions: portMessage.mentions,
      })
    }
    return out
  })

/**
 * Positive channel-shape gate: a message event is in scope only when it
 * carries a numeric `stream_id`. DM-shaped events lack it and are skipped
 * before the strict body schema runs (a `Number`, not `Int`, gate so a
 * float `stream_id` still reaches the strict decode and surfaces there).
 */
const channelShapeSchema = Schema.Struct({ stream_id: Schema.Number })
const decodeChannelShape = Schema.decodeUnknownEither(channelShapeSchema)

/**
 * Parse a `message`-typed event envelope and produce zero or more
 * port-level `InboundEvent`s. Fails with `ParseError` for malformed
 * channel messages — including brand-decode failures on the sender,
 * ref, body, or timestamp — so the producer can log+skip; succeeds with
 * `[]` for DM-shaped events that are out of scope for a
 * channel-rooted port. DM gating happens before the strict schema
 * because the events queue narrow on `/register` cannot exclude DMs
 * at source — Zulip's queue narrow path coerces input through
 * `NeverNegatedNarrowTerm` and explicitly drops `negated:true`
 * (zerver/lib/narrow_helpers.py; zerver/lib/narrow_predicate.py has the
 * outstanding TODO). The replay path uses `/messages`, which is on a
 * different narrow surface that does support negation, so it can filter
 * DMs at source. Here we gate on positive channel shape — anything
 * missing a numeric `stream_id` (DMs, plus any future non-channel event
 * variant) is silently dropped.
 */
export const mapMessageEvent = (
  raw: { readonly [key: string]: unknown },
  directory: DirectoryLookup,
  boundIdentity: Identity | undefined,
  base: string,
): Effect.Effect<ReadonlyArray<InboundEvent>, ParseResult.ParseError> => {
  const message = raw['message']
  if (Either.isLeft(decodeChannelShape(message))) {
    return Effect.succeed([])
  }
  return decodeZulipMessageContent(message).pipe(
    Effect.flatMap((parsed) => messageToInboundEvents(parsed, directory, boundIdentity, base)),
  )
}

const reactionKind = (op: 'add' | 'remove'): 'reaction-added' | 'reaction-removed' =>
  op === 'add' ? 'reaction-added' : 'reaction-removed'

const decodeReactionIdentity = (
  reaction: ParsedZulipReactionEvent,
  directory: DirectoryLookup,
): Effect.Effect<Identity, ParseResult.ParseError> => {
  const cached = directory.byId.get(reaction.user_id)
  if (cached !== undefined) return Effect.succeed(cached)
  return Effect.gen(function* () {
    const id = yield* decodeIdentityId(String(reaction.user_id))
    const name = yield* decodeDisplayName(`user-${reaction.user_id}`)
    return { id, name, kind: 'human' } satisfies Identity
  })
}

export const reactionToInboundEvent = (
  reaction: ParsedZulipReactionEvent,
  directory: DirectoryLookup,
  target: MessageRef,
): Effect.Effect<InboundEvent, ParseResult.ParseError> =>
  Effect.gen(function* () {
    const by = yield* decodeReactionIdentity(reaction, directory)
    const emoji = yield* decodeEmoji(reaction.emoji_name)
    return {
      kind: reactionKind(reaction.op),
      target,
      emoji,
      by,
    }
  })

export interface QueueState {
  readonly queueId: string
  readonly lastEventId: number
}

/**
 * Zulip's server-side ceiling on `idle_queue_timeout` (`MAX_QUEUE_TIMEOUT_SECS`
 * — 7 days). A requested timeout is clamped to this at the config edge, so the
 * value {@link registerQueue} sends is already in range.
 */
export const MAX_QUEUE_TIMEOUT_SECS = 604800

export const registerQueue = (
  http: ZulipHttp,
  mode: 'all' | 'mentions',
  idleTimeoutSecs?: number,
): Effect.Effect<QueueState, ZulipApiError | ParseResult.ParseError> => {
  const baseParams = { event_types: JSON.stringify(['message', 'reaction']) }
  const modeParams =
    mode === 'mentions'
      ? { ...baseParams, narrow: JSON.stringify([['is', 'mentioned']]) }
      : baseParams
  const params =
    idleTimeoutSecs === undefined
      ? modeParams
      : { ...modeParams, idle_queue_timeout: idleTimeoutSecs }
  return http
    .post('/register', registerResponseSchema, params)
    .pipe(Effect.map((res) => ({ queueId: res.queue_id, lastEventId: res.last_event_id })))
}

const populateCacheFromInboundEvents = (
  cache: MessageRefCache,
  events: ReadonlyArray<InboundEvent>,
): void => {
  for (const ev of events) {
    if (ev.kind === 'message-posted') {
      cache.set(ev.message.ref.id, ev.message.ref)
    }
  }
}

const singleMessageResponseSchema = Schema.Struct({
  result: Schema.Literal('success'),
  messages: Schema.Array(zulipMessageContentSchema),
})

export const fetchMessageRef = (
  http: ZulipHttp,
  messageId: number,
  base: string,
): Effect.Effect<Option.Option<MessageRef>, ZulipApiError | ParseResult.ParseError> =>
  http
    .get('/messages', singleMessageResponseSchema, {
      anchor: messageId,
      num_before: 0,
      num_after: 0,
      narrow: '[]',
      apply_markdown: false,
    })
    .pipe(
      Effect.flatMap((res) => {
        const message = res.messages[0]
        if (message === undefined || message.id !== messageId) return Effect.succeed(Option.none())
        return decodeMessageRef(message, base).pipe(Effect.map(Option.some))
      }),
    )

const describeError = (err: unknown): string => messageOf(err)

const markReplayed = (events: ReadonlyArray<InboundEvent>): ReadonlyArray<InboundEvent> =>
  events.map((ev) => {
    if (ev.kind === 'message-posted' || ev.kind === 'mention-received') {
      return { ...ev, replayed: true }
    }
    return ev
  })

/**
 * Substrate-reconnect backoff: exponential from 1s, capped at 30s — the
 * delay sequence is 1s, 2s, 4s, 8s, 16s, 30s, 30s, ... `Schedule.either`
 * (a.k.a. `union`) merges the two schedules' intervals by selecting the
 * shorter delay (min), so `exponential(1s) ∪ spaced(30s)` yields the cap.
 * `intersect` would select the longer delay (max) — a 30s floor — which is
 * the wrong policy for reconnect: it would stall every early retry behind a
 * full 30s wait. The cap keeps the bot responsive on transient blips while
 * bounding the poll rate during a prolonged outage.
 */
export const defaultRetrySchedule: Schedule.Schedule<[Duration.Duration, number]> =
  Schedule.exponential(Duration.seconds(1)).pipe(
    Schedule.either(Schedule.spaced(Duration.seconds(30))),
  )

/**
 * Bounded fallback for the resume verdict. On a resuming seat (`onResumeOutcome`
 * wired) the verdict normally rides the first poll — `true` on a live queue,
 * `false` on `BAD_EVENT_QUEUE_ID`. A first poll that never reaches either
 * terminal (a deterministic `ParseError`, a non-`BAD_EVENT_QUEUE_ID` error, or a
 * wedged `429`) retries forever under {@link defaultRetrySchedule}, which would
 * leave the verdict latch pending and hang the seat's first `post`/`react`. This
 * timer guarantees the latch completes: after the bound, report `false` (no
 * resume → history catch-up). It is deliberately long enough that a healthy
 * resume-poll — which returns in well under a second — always wins the race and
 * reports the honest verdict first; the fallback only fires when the poll is
 * genuinely starved. The pump's own never-give-up retry is untouched.
 */
export const RESUME_VERDICT_FALLBACK: Duration.Duration = Duration.seconds(60)

type EventEnvelope = {
  readonly id: number
  readonly type: string
  readonly [key: string]: unknown
}

const skipWithLog = (line: string): Effect.Effect<ReadonlyArray<InboundEvent>> =>
  Effect.logInfo(line).pipe(Effect.as([] as ReadonlyArray<InboundEvent>))

const processSingleEvent = (
  evt: EventEnvelope,
  directory: DirectoryLookup,
  config: EventsConfig,
): Effect.Effect<ReadonlyArray<InboundEvent>, ZulipApiError | ParseResult.ParseError> => {
  if (evt.type === 'message') {
    // The per-event brand decode (sender / ref / body / ts) now flows
    // through mapMessageEvent's ParseError channel. A malformed channel
    // message is logged+skipped here so a single shape-violating event
    // can't crash the pump.
    return mapMessageEvent(evt, directory, config.boundIdentity, config.permalinkBase).pipe(
      Effect.tap((events) =>
        Effect.sync(() => {
          if (config.messageRefCache !== undefined) {
            populateCacheFromInboundEvents(config.messageRefCache, events)
          }
        }),
      ),
      Effect.catchTag('ParseError', (err) =>
        skipWithLog(
          `commy zulip events: skipping event id=${evt.id} type=${evt.type}: ${err.message}`,
        ),
      ),
    )
  }
  if (evt.type === 'reaction' && config.messageRefCache !== undefined) {
    const cache = config.messageRefCache
    const parsedReaction = decodeZulipReactionEvent(evt)
    if (Either.isLeft(parsedReaction)) {
      return skipWithLog(
        `commy zulip events: skipping malformed reaction event id=${evt.id}: ${parsedReaction.left.message}`,
      )
    }
    const reaction = parsedReaction.right
    const eventsForTarget = (
      target: MessageRef,
    ): Effect.Effect<ReadonlyArray<InboundEvent>, ParseResult.ParseError> =>
      reactionToInboundEvent(reaction, directory, target).pipe(Effect.map((ev) => [ev]))
    const resolveTarget: Effect.Effect<
      ReadonlyArray<InboundEvent>,
      ParseResult.ParseError | ZulipApiError
    > = cache.get(String(reaction.message_id)).pipe(
      Option.match({
        onSome: eventsForTarget,
        onNone: () =>
          fetchMessageRef(config.http, reaction.message_id, config.permalinkBase).pipe(
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.succeed([] as ReadonlyArray<InboundEvent>),
                onSome: (target) => {
                  cache.set(target.id, target)
                  return eventsForTarget(target)
                },
              }),
            ),
          ),
      }),
    )
    return resolveTarget.pipe(
      // Only Schema parse errors on the cache-miss /messages response and
      // the reaction's own brand decode get logged+skipped. A ZulipApiError
      // on /messages bubbles to the outer retry so the substrate's 429/BAD_QUEUE
      // recovery still drives.
      Effect.catchTag('ParseError', (err) =>
        skipWithLog(
          `commy zulip events: skipping reaction event id=${evt.id}: ${describeError(err)}`,
        ),
      ),
    )
  }
  return Effect.succeed([])
}

type StepResult = readonly [ReadonlyArray<InboundEvent>, QueueState | undefined]

/**
 * Stream-shaped Zulip events producer. Each step runs the
 * register→poll→advance loop once: lazily registers the events queue,
 * issues one long-poll GET, decodes per-event, advances the cursor
 * past any skipped events, and returns the new chunk + next queue
 * state. Recovery for the two adapter-handled errors is inline:
 *
 *   - `BAD_EVENT_QUEUE_ID` resets the queue to `undefined` so the next
 *     step re-registers. If a watermark and a `replay` callback are
 *     wired, the gap window (since the last live message) is
 *     backfilled with `replayed: true` events before the next register.
 *   - HTTP 429 sleeps for `retry_after` seconds via `Effect.sleep`
 *     (interruptible by fiber cancellation) and returns an empty chunk
 *     with the queue state unchanged. The next step retries the
 *     long-poll.
 *
 * Any other failure — non-recoverable `ZulipApiError`, response-shape
 * `ParseError`, network error from `@effect/platform`'s HttpClient —
 * escapes the inner Effect and is caught by the surrounding
 * `Schedule`-based retry. Each retry attempt logs a
 * `transient error (attempt N)` breadcrumb; on the first chunk of
 * events after recovery, a `reconnected after N transient error(s)`
 * breadcrumb lands. Retry is unbounded by design — the realm can be
 * down for an arbitrarily long window and the bot must recover when
 * it comes back, matching the Discord substrate's gateway reconnect
 * shape. Backoff caps via `defaultRetrySchedule` at 30 s so the bot is
 * responsive once the realm heals.
 *
 * The resulting Stream's error channel is `never`: every recoverable
 * substrate hiccup is absorbed before reaching the consumer. The
 * pump's only remaining concern at the consumer end is dispatch
 * (notifier-side) errors, which it catches separately.
 */
interface BreadcrumbState {
  readonly consecutiveFailures: number
  readonly needsBreadcrumb: boolean
}

export const inboxEvents = (config: EventsConfig): Stream.Stream<InboundEvent> =>
  Stream.unwrapScoped(
    Effect.gen(function* () {
      const watermark = config.watermarkStore ?? (yield* createWatermarkStore())

      const breadcrumb = yield* Ref.make<BreadcrumbState>({
        consecutiveFailures: 0,
        needsBreadcrumb: false,
      })

      // Resume-outcome one-shot. The FIRST poll's terminal outcome (success vs
      // BAD_EVENT_QUEUE_ID) is the seat's resume verdict; transient errors
      // retry underneath so a network blip never reports a false 'missed'. A
      // later mid-life BAD_EVENT_QUEUE_ID must not re-report, so the report is
      // latched. No-op when the caller wired no hook (the non-resume path).
      const resumeReported = yield* Ref.make(false)
      const reportResume = (queueReplayed: boolean): Effect.Effect<void> => {
        const onResumeOutcome = config.onResumeOutcome
        if (onResumeOutcome === undefined) return Effect.void
        return Ref.getAndSet(resumeReported, true).pipe(
          Effect.flatMap((already) => (already ? Effect.void : onResumeOutcome(queueReplayed))),
        )
      }

      // Bounded fallback so the resume-verdict latch always completes. Arms B
      // (first poll succeeds → true) and C (first poll BAD_EVENT_QUEUE_ID →
      // false) both need the first poll to reach a terminal outcome; a poll that
      // never does — a deterministic decode ParseError, a non-BAD_EVENT_QUEUE_ID
      // error, or a wedged 429 — retries forever under the never-give-up schedule
      // and the verdict is starved, hanging the seat's inline post/react await.
      // A scoped timer reports `false` (no resume → history catch-up) after the
      // bound, losing the race to any healthy poll (reportResume is one-shot, so
      // this no-ops once B or C fired) and winning only when the poll is starved.
      // Gated to a resuming seat — onResumeOutcome is wired solely for a genuine
      // resume — and scoped to the stream so it is interrupted when the pump ends.
      if (config.onResumeOutcome !== undefined) {
        yield* Effect.forkScoped(
          Effect.sleep(RESUME_VERDICT_FALLBACK).pipe(Effect.zipRight(reportResume(false))),
        )
      }

      const recordTransientFailure = (cause: unknown): Effect.Effect<void> =>
        Ref.modify(breadcrumb, (s) => {
          const consecutiveFailures = s.consecutiveFailures + 1
          return [consecutiveFailures, { consecutiveFailures, needsBreadcrumb: true }] as const
        }).pipe(
          Effect.flatMap((consecutiveFailures) =>
            Effect.logInfo(
              `commy zulip events: transient error (attempt ${consecutiveFailures}): ${describeError(cause)}`,
            ),
          ),
        )

      const recordRecovery: Effect.Effect<void> = Ref.modify(breadcrumb, (s) =>
        s.needsBreadcrumb
          ? [Option.some(s.consecutiveFailures), { consecutiveFailures: 0, needsBreadcrumb: false }]
          : [Option.none<number>(), s],
      ).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.void,
            onSome: (consecutiveFailures) => {
              const noun = consecutiveFailures === 1 ? 'error' : 'errors'
              return Effect.logInfo(
                `commy zulip events: reconnected after ${consecutiveFailures} transient ${noun}`,
              )
            },
          }),
        ),
      )

      const registerFreshQueue: Effect.Effect<QueueState, ZulipApiError | ParseResult.ParseError> =
        registerQueue(config.http, config.mode, config.queueIdleTimeoutSecs).pipe(
          Effect.tap((q) => config.onQueueRegister?.(q) ?? Effect.void),
        )

      /**
       * Recovery from an expired queue spans two collections: the replay
       * snapshot covers everything up to the instant its HTTP call was served,
       * and the fresh queue covers everything after `POST /register` fixed its
       * baseline. Register FIRST so those two overlap. Replaying first leaves a
       * window between them — the snapshot's upper bound is open and nothing
       * carries the boundary into the register — and a message posted in that
       * window is in neither set, with no error and no watermark trace.
       *
       * Overlapping instead can deliver a message both replayed and live. That
       * is the trade the snapshot's lower bound already makes (`timestamp >=
       * since`, inclusive), and the pump's dedup absorbs the duplicate.
       */
      const handleBadQueue: Effect.Effect<StepResult, ZulipApiError | ParseResult.ParseError> =
        Effect.gen(function* () {
          yield* reportResume(false)
          const since = yield* watermark.get()
          const freshQueue = yield* registerFreshQueue
          if (config.replay === undefined || Option.isNone(since)) {
            return [[], freshQueue] as const
          }
          const gapEither = yield* Effect.either(config.replay(since.value))
          if (Either.isLeft(gapEither)) {
            const err = gapEither.left
            yield* Effect.logInfo(
              `commy zulip events: gap-replay failed after BAD_EVENT_QUEUE_ID (${err._tag}): ${err.message}`,
            )
            return [[], freshQueue] as const
          }
          return [markReplayed(gapEither.right), freshQueue] as const
        })

      const step = (
        queue: QueueState | undefined,
      ): Effect.Effect<StepResult, ZulipApiError | ParseResult.ParseError> =>
        Effect.gen(function* () {
          const currentQueue: QueueState = queue ?? (yield* registerFreshQueue)
          const res = yield* config.http.get('/events', eventsResponseSchema, {
            queue_id: currentQueue.queueId,
            last_event_id: currentQueue.lastEventId,
          })
          // The poll returned, so the queue this step polled is live. On the
          // first poll that is the resume verdict: the surviving queue is
          // replaying the backlog — report it so history catch-up stands down.
          yield* reportResume(true)
          const directory = res.events.length > 0 ? yield* config.resolveDirectory() : undefined
          let maxId = currentQueue.lastEventId
          const mapped: InboundEvent[] = []
          for (const rawEvt of res.events) {
            const envelope = decodeEventEnvelope(rawEvt)
            if (Either.isLeft(envelope)) {
              // Per-event envelope decode. The events array
              // is unwrapped loosely above so a single bad item can't fail
              // the batch — but we still need {id, type} to log+skip and
              // to advance the queue cursor. Without id we can't advance,
              // so a missing-id envelope is logged and dropped without
              // contributing to maxId; subsequent valid events will still
              // pull the cursor forward.
              yield* Effect.logInfo(
                `commy zulip events: skipping event with malformed envelope: ${envelope.left.message}`,
              )
              continue
            }
            const evt = envelope.right as EventEnvelope
            if (evt.id > maxId) maxId = evt.id
            if (directory === undefined) continue
            const events = yield* processSingleEvent(evt, directory, config)
            mapped.push(...events)
          }
          for (const ev of mapped) {
            if (ev.kind === 'message-posted' || ev.kind === 'mention-received') {
              yield* watermark.advance(ev.message.ts)
            }
          }
          if (config.onQueueAdvance !== undefined && maxId > currentQueue.lastEventId) {
            yield* config.onQueueAdvance(maxId)
          }
          return [mapped, { queueId: currentQueue.queueId, lastEventId: maxId }] as const
        }).pipe(
          // catchTag covers ALL ZulipApiError; the branches conditionally
          // re-fail with `Effect.fail(e)` so the surviving non-recoverable
          // errors stay in the E channel for the surrounding retry. Using
          // `catchIf` with a refined type guard would type-erase ZulipApiError
          // entirely after the first call — the predicate's `e is ZulipApiError`
          // shape removes the whole class regardless of the runtime filter,
          // letting unhandled errors slip past the type system.
          Effect.catchTag('ZulipApiError', (e) => {
            if (e.code === 'BAD_EVENT_QUEUE_ID') return handleBadQueue
            if (e.status === 429) {
              const retryAfter = e.retryAfter ?? 1
              return Effect.logWarning(
                `commy zulip events: rate-limited on /events (429), backing off ${retryAfter}s`,
              ).pipe(
                Effect.zipRight(Effect.sleep(Duration.seconds(retryAfter))),
                Effect.as([[], queue] as StepResult),
              )
            }
            return Effect.fail(e)
          }),
        )

      const stepWithRetry = (
        queue: QueueState | undefined,
      ): Effect.Effect<
        Option.Option<readonly [Chunk.Chunk<InboundEvent>, QueueState | undefined]>
      > =>
        step(queue).pipe(
          Effect.tapError(recordTransientFailure),
          Effect.retry(defaultRetrySchedule),
          Effect.tap(([events]) => (events.length > 0 ? recordRecovery : Effect.void)),
          Effect.map(([events, nextQueue]) =>
            Option.some([Chunk.fromIterable(events), nextQueue] as const),
          ),
          // defaultRetrySchedule never gives up — orDie narrows the type-level
          // E from "ZulipApiError | ParseError" to `never` so the Stream
          // can be `Stream<InboundEvent, never>`. The defect path is
          // unreachable at runtime.
          Effect.orDie,
        )

      return Stream.unfoldChunkEffect(config.initialQueue, stepWithRetry)
    }),
  )
