import { expect, test } from 'bun:test'
import { captureLogger } from '@commy/core/logging'
import type { Identity, InboundEvent } from '@commy/core/ports'
import {
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  InboxError,
} from '@commy/core/ports'
import {
  Array as Arr,
  Chunk,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  type ParseResult,
  Schedule,
  Schema,
  Stream,
  TestClock,
  TestContext,
} from 'effect'
import type { DirectoryLookup, EventsConfig, ParsedZulipMessage } from './events.ts'
import {
  createMessageRefCache,
  defaultRetrySchedule,
  inboxEvents,
  mapMessageEvent,
  messageToInboundEvents,
} from './events.ts'
import type { ZulipHttp } from './http.ts'
import { ZulipApiError } from './http.ts'

const PERMALINK_BASE = 'https://zulip.example.com'

const HERMES: Identity = {
  id: decodeIdentityIdSync('9'),
  name: decodeDisplayNameSync('hermes-agent'),
  kind: 'agent',
}

const GRAEME: Identity = {
  id: decodeIdentityIdSync('5'),
  name: decodeDisplayNameSync('Graeme Foster'),
  kind: 'human',
}

const directoryFor = (...identities: ReadonlyArray<Identity>): DirectoryLookup => ({
  byId: new Map(identities.map((i) => [Number(i.id), i])),
  byName: new Map(identities.map((i) => [i.name, i])),
})

const messageMentioning = (target: Identity, sender: Identity): ParsedZulipMessage => ({
  id: 100,
  sender_id: Number(sender.id),
  sender_full_name: sender.name,
  stream_id: 1,
  display_recipient: 'general',
  subject: 'topic',
  content: `oi @**${target.name}** wake up`,
  timestamp: 1_700_000_000,
})

test('fires mention-received when bound identity is in content even if flags lack "mentioned"', () => {
  // The minter-routing gap (comms-wy0): on the real realm, the events
  // queue is registered against the minter, so `flags.mentioned` is keyed
  // to the queue owner — false for cross-bot mentions. Synthesis must gate
  // on the bound identity appearing in `portMessage.mentions`, not on the
  // queue-owner's flag.
  const directory = directoryFor(HERMES, GRAEME)
  const message = messageMentioning(HERMES, GRAEME)

  const events = Effect.runSync(messageToInboundEvents(message, directory, HERMES, PERMALINK_BASE))
  const mention = events.find((e) => e.kind === 'mention-received')

  expect(mention).toBeDefined()
  if (mention !== undefined && mention.kind === 'mention-received') {
    expect(mention.mentions.map((m) => m.id)).toContain(HERMES.id)
  }
})

test('decorates the inbound message ref with message, channel and topic permalinks', () => {
  const directory = directoryFor(HERMES, GRAEME)
  const message = messageMentioning(HERMES, GRAEME)

  const events = Effect.runSync(
    messageToInboundEvents(message, directory, HERMES, 'https://zulip.example.com'),
  )
  const posted = events.find((e) => e.kind === 'message-posted')

  expect(posted).toBeDefined()
  if (posted !== undefined && posted.kind === 'message-posted') {
    const ref = posted.message.ref
    expect(ref.permalink).toBe(
      'https://zulip.example.com/#narrow/channel/1-general/topic/topic/near/100',
    )
    expect(ref.channel.permalink).toBe('https://zulip.example.com/#narrow/channel/1-general')
    expect(ref.thread?.permalink).toBe(
      'https://zulip.example.com/#narrow/channel/1-general/topic/topic',
    )
  }
})

test('does not fire mention-received when bound identity is not in content', () => {
  // Negative case: a message that mentions someone else (not the bound
  // bot) must not synthesise a mention-received for the bound bot, even
  // when the queue happens to have flagged it (e.g. minter was mentioned).
  const RIQ: Identity = {
    id: decodeIdentityIdSync('11'),
    name: decodeDisplayNameSync('riq6r230'),
    kind: 'agent',
  }
  const directory = directoryFor(HERMES, GRAEME, RIQ)
  const message = messageMentioning(RIQ, GRAEME)

  const events = Effect.runSync(messageToInboundEvents(message, directory, HERMES, PERMALINK_BASE))
  const mention = events.find((e) => e.kind === 'mention-received')

  expect(mention).toBeUndefined()
})

const dmRawEvent = (
  sender: Identity,
  recipients: ReadonlyArray<Identity>,
): {
  readonly [key: string]: unknown
} => ({
  id: 99,
  type: 'message',
  message: {
    id: 100,
    sender_id: Number(sender.id),
    sender_full_name: sender.name,
    type: 'private',
    display_recipient: recipients.map((r) => ({
      id: Number(r.id),
      email: `${r.name}@example.com`,
      full_name: r.name,
    })),
    subject: '',
    content: 'hi there',
    timestamp: 1_700_000_000,
  },
})

const channelRawEvent = (message: ParsedZulipMessage): { readonly [key: string]: unknown } => ({
  id: 99,
  type: 'message',
  message,
})

test('mapMessageEvent skips DM-shaped events instead of failing the parser (comms-ov3)', () => {
  // DMs arrive on the live events queue with `display_recipient` as the
  // recipient array, no `stream_id`, and an empty `subject` — the strict
  // schema would fail and the unhandled error would surface to the
  // producer's outer retry, churning the pump on every DM. The
  // events-queue narrow path cannot exclude DMs at source because
  // Zulip's /register drops negation (zerver/lib/narrow_helpers.py:
  // NeverNegatedNarrowTerm), so the gate has to live here.
  const directory = directoryFor(HERMES, GRAEME)
  const dm = dmRawEvent(GRAEME, [HERMES, GRAEME])

  const result = Effect.runSync(mapMessageEvent(dm, directory, HERMES, PERMALINK_BASE))
  expect(result).toEqual([])
})

test('mapMessageEvent skips events whose message field is not a channel-shaped object', () => {
  const directory = directoryFor(HERMES, GRAEME)

  expect(
    Effect.runSync(
      mapMessageEvent({ id: 1, type: 'message', message: null }, directory, HERMES, PERMALINK_BASE),
    ),
  ).toEqual([])
  expect(
    Effect.runSync(
      mapMessageEvent(
        { id: 2, type: 'message', message: 'nope' },
        directory,
        HERMES,
        PERMALINK_BASE,
      ),
    ),
  ).toEqual([])
  expect(
    Effect.runSync(mapMessageEvent({ id: 3, type: 'message' }, directory, HERMES, PERMALINK_BASE)),
  ).toEqual([])
})

test('mapMessageEvent parses channel-shaped events through messageToInboundEvents (comms-ov3 regression)', () => {
  // Positive regression: a normal channel message must still flow through
  // the strict parser and emit a message-posted event. Guards the gate from
  // over-filtering.
  const directory = directoryFor(HERMES, GRAEME)
  const raw = channelRawEvent(messageMentioning(HERMES, GRAEME))

  const result = Effect.runSync(mapMessageEvent(raw, directory, HERMES, PERMALINK_BASE))
  expect(result.some((e) => e.kind === 'message-posted')).toBe(true)
  expect(result.some((e) => e.kind === 'mention-received')).toBe(true)
})

// ---------------------------------------------------------------------------
// Iterator-level defence (comms-aod): shape-violating events must be logged
// and skipped, never escape the iterator. A ZodError escaping fetchBatch
// reaches startEventPump.reportFatal → process.exit(1), killing MCP for
// every bot in the realm. The patches below generalise the comms-ov3 DM gate
// to any unforeseen schema drift.
// ---------------------------------------------------------------------------

type HttpHandlers = {
  readonly onPost?: (path: string, body?: Record<string, unknown>) => unknown
  readonly onGet?: (path: string, params?: Record<string, unknown>) => unknown
}

const invokeHandler = <A, I>(
  schema: Schema.Schema<A, I>,
  invoke: () => unknown,
): Effect.Effect<A, ZulipApiError | ParseResult.ParseError> =>
  Effect.suspend((): Effect.Effect<A, ZulipApiError | ParseResult.ParseError> => {
    try {
      return Schema.decodeUnknown(schema)(invoke())
    } catch (e) {
      if (e instanceof ZulipApiError) return Effect.fail(e)
      throw e
    }
  })

const fakeHttp = (handlers: HttpHandlers): ZulipHttp =>
  ({
    post: <A, I>(
      path: string,
      schema: Schema.Schema<A, I>,
      body?: Record<string, unknown>,
    ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError> => {
      if (handlers.onPost === undefined) {
        return Effect.die(new Error(`unexpected POST ${path}`))
      }
      const onPost = handlers.onPost
      return invokeHandler(schema, () => onPost(path, body))
    },
    get: <A, I>(
      path: string,
      schema: Schema.Schema<A, I>,
      params?: Record<string, unknown>,
    ): Effect.Effect<A, ZulipApiError | ParseResult.ParseError> => {
      if (handlers.onGet === undefined) {
        return Effect.die(new Error(`unexpected GET ${path}`))
      }
      const onGet = handlers.onGet
      return invokeHandler(schema, () => onGet(path, params))
    },
  }) as unknown as ZulipHttp

const drainOne = (
  config: EventsConfig,
): Effect.Effect<{ readonly events: ReadonlyArray<InboundEvent> }> =>
  Effect.gen(function* () {
    const head = yield* Stream.runHead(inboxEvents(config))
    return { events: Option.isSome(head) ? [head.value] : [] }
  })

const drainN = (config: EventsConfig, n: number): Effect.Effect<ReadonlyArray<InboundEvent>> =>
  Stream.runCollect(Stream.take(inboxEvents(config), n)).pipe(
    Effect.map((chunk) => Chunk.toReadonlyArray(chunk)),
  )

/**
 * Drain the producer's first event under Effect's TestClock. The retry
 * backoff is now a real stock `Schedule` (capped exponential), so a
 * transient-failure test can't drain synchronously — the retry sleeps on
 * the virtual clock. Fork the drain, then advance the clock past the cap
 * enough times to clear every scheduled retry, then join. No real waits.
 */
const drainOneUnderTestClock = (
  config: EventsConfig,
  retries: number,
): Effect.Effect<{ readonly events: ReadonlyArray<InboundEvent> }> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(drainOne(config))
    // Each advance releases at most one scheduled retry sleep; advancing
    // by the 30s cap clears any single backoff regardless of which rung of
    // the exponential ramp it sits on.
    for (let i = 0; i <= retries; i += 1) {
      yield* TestClock.adjust(Duration.seconds(30))
    }
    return yield* Fiber.join(fiber)
  }).pipe(Effect.provide(TestContext.TestContext))

const aChannelMessage = (overrides: Partial<ParsedZulipMessage> = {}): Record<string, unknown> => ({
  id: 100,
  sender_id: Number(GRAEME.id),
  sender_full_name: GRAEME.name,
  stream_id: 1,
  display_recipient: 'general',
  subject: 'topic',
  content: 'hi',
  timestamp: 1_700_000_000,
  ...overrides,
})

// 2-second per-test timeout: the fake HTTP resolves promptly, so an
// iterator-level test that lasts >2s has lost its way (typically the
// pump looping forever against an empty events stub) — fail fast
// instead of hanging the suite.
const ITERATOR_TEST_TIMEOUT_MS = 2_000

test(
  'iterator skips malformed reaction event and yields subsequent valid events (comms-aod)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // Reaction missing required `user_id` field. The strict schema would
        // throw a ZodError that escapes fetchBatch — instead, safeParse + log +
        // skip keeps the iterator alive so the message event behind it still
        // surfaces to the pump.
        const lines: string[] = []
        let getCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                return {
                  result: 'success',
                  events: [
                    {
                      id: 11,
                      type: 'reaction',
                      op: 'add',
                      message_id: 100,
                      emoji_name: 'thumbs_up',
                    },
                    { id: 12, type: 'message', message: aChannelMessage() },
                  ],
                }
              }
              return { result: 'success', events: [] }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
        }
        const { events } = yield* drainOne(config).pipe(Effect.provide(captureLogger(lines)))
        expect(events).toHaveLength(1)
        expect(events[0]?.kind).toBe('message-posted')
        expect(lines.some((l) => l.includes('id=11') && l.includes('reaction'))).toBe(true)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator skips event whose message body fails strict schema (comms-aod)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // Channel-shaped event (stream_id present, passes the comms-ov3 gate)
        // but with an empty subject — the strict zulipMessageContentSchema
        // requires subject.min(1). Before comms-aod the ZodError escaped
        // fetchBatch and crashed the pump.
        const lines: string[] = []
        let getCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                return {
                  result: 'success',
                  events: [
                    { id: 21, type: 'message', message: aChannelMessage({ subject: '' }) },
                    { id: 22, type: 'message', message: aChannelMessage({ id: 101 }) },
                  ],
                }
              }
              return { result: 'success', events: [] }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
        }
        const { events } = yield* drainOne(config).pipe(Effect.provide(captureLogger(lines)))
        expect(events).toHaveLength(1)
        const first = events[0]
        expect(first?.kind).toBe('message-posted')
        if (first?.kind === 'message-posted') {
          expect(String(first.message.ref.id)).toBe('101')
        }
        expect(lines.some((l) => l.includes('id=21'))).toBe(true)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator skips reaction whose cache-miss lookup returns a malformed message (comms-aod)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // The reaction is well-formed; the cache misses; fetchMessageRef hits
        // /messages and the returned message fails zulipMessageContentSchema
        // (subject=''). Before comms-aod the ZodError escaped fetchBatch and
        // killed the pump for every bot in the realm. Now the reaction is
        // logged + skipped and the iterator survives.
        const lines: string[] = []
        let getCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
            onGet: (path) => {
              if (path === '/messages') {
                return {
                  result: 'success',
                  messages: [aChannelMessage({ id: 100, subject: '' })],
                }
              }
              getCalls += 1
              if (getCalls === 1) {
                return {
                  result: 'success',
                  events: [
                    {
                      id: 31,
                      type: 'reaction',
                      op: 'add',
                      user_id: 5,
                      message_id: 100,
                      emoji_name: 'tada',
                    },
                    { id: 32, type: 'message', message: aChannelMessage({ id: 200 }) },
                  ],
                }
              }
              return { result: 'success', events: [] }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
        }
        const { events } = yield* drainOne(config).pipe(Effect.provide(captureLogger(lines)))
        expect(events).toHaveLength(1)
        expect(events[0]?.kind).toBe('message-posted')
        expect(lines.some((l) => l.includes('id=31') && l.includes('reaction'))).toBe(true)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator advances queue past a malformed event so it is not re-delivered (comms-aod)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // Queue-advance is the difference between "skip-once" and "stuck loop".
        // After a malformed event id=41 is skipped, the next /events poll must
        // send last_event_id >= 41 — otherwise Zulip re-delivers the same bad
        // event forever and the iterator never makes progress.
        const polls: number[] = []
        let getCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
            onGet: (_path, params) => {
              if (params !== undefined && 'last_event_id' in params) {
                polls.push(Number(params['last_event_id']))
              }
              getCalls += 1
              if (getCalls === 1) {
                return {
                  result: 'success',
                  events: [
                    { id: 41, type: 'reaction', op: 'add', message_id: 100 }, // malformed
                    { id: 42, type: 'message', message: aChannelMessage() },
                  ],
                }
              }
              return {
                result: 'success',
                events: [{ id: 43, type: 'message', message: aChannelMessage({ id: 200 }) }],
              }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
        }
        // Take 2 events: first drains batch 1 (malformed skipped, msg id=42
        // yielded); second triggers batch 2 poll — must carry last_event_id>=42.
        // Capture (and discard) the skip diagnostic so it doesn't reach STDERR.
        yield* drainN(config, 2).pipe(Effect.provide(captureLogger([])))
        // First poll on a freshly-registered queue is last_event_id=0; the
        // SECOND poll proves queue advanced past the malformed event.
        expect(polls.length).toBeGreaterThanOrEqual(2)
        expect(polls[1]).toBeGreaterThanOrEqual(42)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

// ---------------------------------------------------------------------------
// Transparent gap-replay on BAD_EVENT_QUEUE_ID (comms-jnn): when Zulip
// invalidates the events queue (TTL expiry, manual invalidation, etc.) the
// iterator re-registers — but anything posted during the dead window is lost
// from the live stream. Wire a `replay(since)` callback into EventsConfig that
// the iterator invokes on BAD_EVENT_QUEUE_ID; the returned events are emitted
// with `replayed: true` so the event-pump can surface them as
// `<channel ... replayed="true">` blocks. Without this the consumer sees an
// invisible message gap whenever a queue dies.
// ---------------------------------------------------------------------------

test(
  'iterator calls replay() on BAD_EVENT_QUEUE_ID and emits gap events flagged replayed=true (comms-jnn)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        let registerCalls = 0
        let getCalls = 0
        let replayCalledWith: number | undefined
        const gapEvent: InboundEvent = {
          kind: 'message-posted',
          message: {
            ref: {
              id: decodeMessageIdSync('150'),
              channel: { id: decodeChannelIdSync('1'), name: decodeChannelNameSync('general') },
              thread: { name: decodeThreadNameSync('topic') },
            },
            sender: GRAEME,
            body: decodeMessageBodySync('posted while the queue was dead'),
            ts: decodeTimestampSync(1500),
            mentions: [],
            reactions: [],
          },
        }
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => {
              registerCalls += 1
              return { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 }
            },
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                // Live message at ts=1000 establishes the last-seen watermark.
                return {
                  result: 'success',
                  events: [
                    {
                      id: 1,
                      type: 'message',
                      message: aChannelMessage({ id: 100, timestamp: 1000 }),
                    },
                  ],
                }
              }
              if (getCalls === 2) {
                throw new ZulipApiError({
                  message: 'queue expired',
                  status: 400,
                  code: 'BAD_EVENT_QUEUE_ID',
                  retryAfter: undefined,
                })
              }
              // Third poll: the new queue carries a live message at ts=2000.
              return {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aChannelMessage({ id: 200, timestamp: 2000 }),
                  },
                ],
              }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
          replay: (since) =>
            Effect.sync(() => {
              replayCalledWith = since
              return [gapEvent]
            }),
        }
        const collected = yield* drainN(config, 3)
        expect(collected).toHaveLength(3)
        // First is live.
        expect(collected[0]?.kind).toBe('message-posted')
        if (collected[0]?.kind === 'message-posted') {
          expect(collected[0].replayed).toBeUndefined()
          expect(String(collected[0].message.ref.id)).toBe('100')
        }
        // Second came from the replay path — must carry replayed=true.
        expect(collected[1]?.kind).toBe('message-posted')
        if (collected[1]?.kind === 'message-posted') {
          expect(collected[1].replayed).toBe(true)
          expect(String(collected[1].message.ref.id)).toBe('150')
        }
        // Third is live again on the freshly registered queue.
        expect(collected[2]?.kind).toBe('message-posted')
        if (collected[2]?.kind === 'message-posted') {
          expect(collected[2].replayed).toBeUndefined()
          expect(String(collected[2].message.ref.id)).toBe('200')
        }
        // Replay called with the last-seen timestamp from the live stream.
        expect(replayCalledWith).toBe(1000)
        // Two registers: initial + post-BAD_EVENT_QUEUE_ID.
        expect(registerCalls).toBe(2)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator skips replay() when no replay callback configured (comms-jnn back-compat)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // Existing pre-comms-jnn behaviour must remain: BAD_EVENT_QUEUE_ID is
        // handled by silent re-register, no replay call attempted.
        let registerCalls = 0
        let getCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => {
              registerCalls += 1
              return { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 }
            },
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                throw new ZulipApiError({
                  message: 'queue expired',
                  status: 400,
                  code: 'BAD_EVENT_QUEUE_ID',
                  retryAfter: undefined,
                })
              }
              return {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aChannelMessage({ id: 100, timestamp: 1000 }),
                  },
                ],
              }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
          // No `replay` field — iterator should skip gap-fill entirely.
        }
        const { events } = yield* drainOne(config)
        expect(events).toHaveLength(1)
        expect(events[0]?.kind).toBe('message-posted')
        if (events[0]?.kind === 'message-posted') {
          expect(events[0].replayed).toBeUndefined()
        }
        expect(registerCalls).toBe(2)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator skips replay() when no live message has been seen yet (comms-jnn)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // If the queue dies before any live message lands, there is no
        // last-seen timestamp to anchor the replay window — skip the call
        // entirely rather than backfill arbitrarily far. The Zulip /messages
        // endpoint caps `num_before` and an unbounded replay would drown the
        // session in old history that was never meant to be live.
        let registerCalls = 0
        let getCalls = 0
        let replayCalls = 0
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => {
              registerCalls += 1
              return { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 }
            },
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                throw new ZulipApiError({
                  message: 'queue expired',
                  status: 400,
                  code: 'BAD_EVENT_QUEUE_ID',
                  retryAfter: undefined,
                })
              }
              return {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aChannelMessage({ id: 100, timestamp: 2000 }),
                  },
                ],
              }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
          replay: () =>
            Effect.sync(() => {
              replayCalls += 1
              return []
            }),
        }
        const { events } = yield* drainOne(config)
        expect(events).toHaveLength(1)
        expect(replayCalls).toBe(0)
        expect(registerCalls).toBe(2)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

test(
  'iterator logs gap-replay failure via the InboxError tag and recovers on the next poll (comms-spj3.28)',
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        // The replay callback is typed Effect<…, InboxError>: when it fails,
        // the gap-replay log path renders the error through its `_tag`, not a
        // string-coerced `unknown`. A replay failure is non-fatal — the
        // producer re-registers and the next live message still surfaces.
        let registerCalls = 0
        let getCalls = 0
        const logLines: string[] = []
        const config: EventsConfig = {
          permalinkBase: PERMALINK_BASE,
          http: fakeHttp({
            onPost: () => {
              registerCalls += 1
              return { result: 'success', queue_id: `q${registerCalls}`, last_event_id: 0 }
            },
            onGet: () => {
              getCalls += 1
              if (getCalls === 1) {
                return {
                  result: 'success',
                  events: [
                    {
                      id: 1,
                      type: 'message',
                      message: aChannelMessage({ id: 100, timestamp: 1000 }),
                    },
                  ],
                }
              }
              if (getCalls === 2) {
                throw new ZulipApiError({
                  message: 'queue expired',
                  status: 400,
                  code: 'BAD_EVENT_QUEUE_ID',
                  retryAfter: undefined,
                })
              }
              return {
                result: 'success',
                events: [
                  {
                    id: 1,
                    type: 'message',
                    message: aChannelMessage({ id: 200, timestamp: 2000 }),
                  },
                ],
              }
            },
          }),
          resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
          mode: 'all',
          boundIdentity: HERMES,
          messageRefCache: createMessageRefCache(),
          replay: () =>
            Effect.fail(new InboxError({ operation: 'replay', cause: 'realm offline' })),
        }
        const collected = yield* drainN(config, 2).pipe(Effect.provide(captureLogger(logLines)))
        // Live message before the gap, then the live message after re-register.
        // The replay produced nothing (it failed), so no replayed events land.
        expect(collected).toHaveLength(2)
        expect(String((collected[0] as { message: { ref: { id: unknown } } }).message.ref.id)).toBe(
          '100',
        )
        expect(String((collected[1] as { message: { ref: { id: unknown } } }).message.ref.id)).toBe(
          '200',
        )
        // The failure is rendered via the InboxError tag + its message.
        expect(logLines).toContain(
          'commy zulip events: gap-replay failed after BAD_EVENT_QUEUE_ID (InboxError): realm offline',
        )
        expect(registerCalls).toBe(2)
      }),
    ),
  ITERATOR_TEST_TIMEOUT_MS,
)

// ---------------------------------------------------------------------------
// Producer-level retry + breadcrumb (comms-ynb): non-recoverable substrate
// hiccups (network failure, 500-class ZulipApiError, response-shape parse
// drift) drive an internal capped-exponential `Schedule` retry inside the
// producer. The Stream's E channel stays `never`; the breadcrumb format is
// `transient error (attempt N): <message>` and (on first event after
// recovery) `reconnected after N transient error(s)`. These tests pin the
// invariants the pump's old `Stream.tapError + Stream.retry` wiring used
// to enforce externally. Since comms-spj3.27 the backoff is a real
// `Schedule` on the virtual clock, so the drain runs under TestClock.
// ---------------------------------------------------------------------------

test('producer retries on transient ZulipApiError and emits transient/reconnect breadcrumbs (comms-ynb)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let getCalls = 0
      const logLines: string[] = []
      const config: EventsConfig = {
        permalinkBase: PERMALINK_BASE,
        http: fakeHttp({
          onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
          onGet: () => {
            getCalls += 1
            if (getCalls === 1) {
              throw new ZulipApiError({
                message: 'The operation timed out.',
                status: 500,
                code: undefined,
                retryAfter: undefined,
              })
            }
            return {
              result: 'success',
              events: [
                {
                  id: 1,
                  type: 'message',
                  message: aChannelMessage({ id: 100, timestamp: 1000 }),
                },
              ],
            }
          },
        }),
        resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
        mode: 'all',
        boundIdentity: HERMES,
        messageRefCache: createMessageRefCache(),
      }
      const { events } = yield* drainOneUnderTestClock(config, 1).pipe(
        Effect.provide(captureLogger(logLines)),
      )
      expect(events).toHaveLength(1)
      expect(getCalls).toBeGreaterThanOrEqual(2)
      expect(logLines).toEqual([
        'commy zulip events: transient error (attempt 1): The operation timed out.',
        'commy zulip events: reconnected after 1 transient error',
      ])
    }),
  ))

test('producer survives multiple consecutive transient failures before recovery (comms-ynb)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let getCalls = 0
      const logLines: string[] = []
      const config: EventsConfig = {
        permalinkBase: PERMALINK_BASE,
        http: fakeHttp({
          onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
          onGet: () => {
            getCalls += 1
            if (getCalls <= 5) {
              throw new ZulipApiError({
                message: `fail-${getCalls}`,
                status: 500,
                code: undefined,
                retryAfter: undefined,
              })
            }
            return {
              result: 'success',
              events: [
                {
                  id: 1,
                  type: 'message',
                  message: aChannelMessage({ id: 100, timestamp: 1000 }),
                },
              ],
            }
          },
        }),
        resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
        mode: 'all',
        boundIdentity: HERMES,
        messageRefCache: createMessageRefCache(),
      }
      const { events } = yield* drainOneUnderTestClock(config, 5).pipe(
        Effect.provide(captureLogger(logLines)),
      )
      expect(events).toHaveLength(1)
      expect(getCalls).toBe(6)
      expect(logLines.filter((l) => l.startsWith('commy zulip events: transient error'))).toEqual([
        'commy zulip events: transient error (attempt 1): fail-1',
        'commy zulip events: transient error (attempt 2): fail-2',
        'commy zulip events: transient error (attempt 3): fail-3',
        'commy zulip events: transient error (attempt 4): fail-4',
        'commy zulip events: transient error (attempt 5): fail-5',
      ])
      expect(logLines).toContain('commy zulip events: reconnected after 5 transient errors')
    }),
  ))

test('default retry schedule is exponential and caps at 30s (comms-spj3.27)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // The reconnect backoff must be a CAP, not a floor: delays grow
      // 1s, 2s, 4s, 8s, 16s then hold at 30s — early transient blips
      // recover fast and a long outage settles at a 30s ceiling. This is
      // exponential ∪ spaced(30s) where `union` selects the SHORTER delay
      // (min), giving the cap. (`intersect` would give max — a 30s floor.)
      const delays = yield* Schedule.run(
        Schedule.delays(defaultRetrySchedule),
        0,
        Arr.makeBy(8, () => undefined),
      )
      expect(Chunk.toReadonlyArray(delays).map(Duration.toMillis)).toEqual([
        1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000,
      ])
    }).pipe(Effect.provide(TestContext.TestContext)),
  ))

test('producer fires each retry only after its capped-exponential backoff elapses (comms-spj3.27)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // Behavioural proof of the CAP through the live producer: after each
      // transient failure the next poll fires only once the virtual clock
      // advances by that rung's backoff. Six failures exercise the full
      // ramp — 1s, 2s, 4s, 8s, 16s — and then the 6th retry, which an
      // uncapped exponential would delay 32s, fires at the 30s ceiling.
      // Advancing by JUST UNDER each delay must not release the poll;
      // reaching it must. A floor (intersect → max) would stall every
      // early retry behind a full 30s and fail at the first rung.
      const perRungDelaysMs = [1000, 2000, 4000, 8000, 16000, 30000]
      let getCalls = 0
      const config: EventsConfig = {
        permalinkBase: PERMALINK_BASE,
        http: fakeHttp({
          onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
          onGet: () => {
            getCalls += 1
            if (getCalls <= perRungDelaysMs.length) {
              throw new ZulipApiError({
                message: `fail-${getCalls}`,
                status: 500,
                code: undefined,
                retryAfter: undefined,
              })
            }
            return {
              result: 'success',
              events: [
                { id: 1, type: 'message', message: aChannelMessage({ id: 100, timestamp: 1000 }) },
              ],
            }
          },
        }),
        resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
        mode: 'all',
        boundIdentity: HERMES,
        messageRefCache: createMessageRefCache(),
      }
      yield* Effect.gen(function* () {
        const fiber = yield* Effect.fork(drainOne(config))
        // First poll fires immediately (no delay before attempt 1).
        yield* TestClock.adjust(Duration.zero)
        expect(getCalls).toBe(1)
        for (const [rung, delayMs] of perRungDelaysMs.entries()) {
          // Just under the rung's backoff: the retry must stay parked.
          yield* TestClock.adjust(Duration.millis(delayMs - 1))
          expect(getCalls).toBe(rung + 1)
          // Crossing the threshold releases exactly one more poll.
          yield* TestClock.adjust(Duration.millis(1))
          expect(getCalls).toBe(rung + 2)
        }
        const { events } = yield* Fiber.join(fiber)
        expect(events).toHaveLength(1)
        expect(getCalls).toBe(perRungDelaysMs.length + 1)
      }).pipe(Effect.provide(Layer.merge(TestContext.TestContext, captureLogger([]))))
    }),
  ))

test('producer surfaces non-Error rejections via the ZulipApiError message in the transient-error log (comms-ynb)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      let getCalls = 0
      const logLines: string[] = []
      const config: EventsConfig = {
        permalinkBase: PERMALINK_BASE,
        http: fakeHttp({
          onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
          onGet: () => {
            getCalls += 1
            if (getCalls === 1) {
              throw new ZulipApiError({
                message: 'plain string rejection',
                status: 500,
                code: undefined,
                retryAfter: undefined,
              })
            }
            return {
              result: 'success',
              events: [
                {
                  id: 1,
                  type: 'message',
                  message: aChannelMessage({ id: 100, timestamp: 1000 }),
                },
              ],
            }
          },
        }),
        resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
        mode: 'all',
        boundIdentity: HERMES,
        messageRefCache: createMessageRefCache(),
      }
      yield* drainOneUnderTestClock(config, 1).pipe(Effect.provide(captureLogger(logLines)))
      expect(logLines[0]).toBe(
        'commy zulip events: transient error (attempt 1): plain string rejection',
      )
    }),
  ))

test('producer logs a rate-limit backoff breadcrumb when /events returns HTTP 429 (comms-l8v)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // The 429 branch sleeps for retry_after and returns an empty chunk —
      // it bypasses the transient-error breadcrumb path, so without an
      // explicit log a rate-limit stall is indistinguishable from a hang.
      // Pin the warning breadcrumb so the backoff stays observable.
      let getCalls = 0
      const logLines: string[] = []
      const config: EventsConfig = {
        permalinkBase: PERMALINK_BASE,
        http: fakeHttp({
          onPost: () => ({ result: 'success', queue_id: 'q1', last_event_id: 0 }),
          onGet: () => {
            getCalls += 1
            if (getCalls === 1) {
              throw new ZulipApiError({
                message: 'rate limited',
                status: 429,
                code: undefined,
                retryAfter: 7,
              })
            }
            return {
              result: 'success',
              events: [
                { id: 1, type: 'message', message: aChannelMessage({ id: 100, timestamp: 1000 }) },
              ],
            }
          },
        }),
        resolveDirectory: () => Effect.succeed(directoryFor(HERMES, GRAEME)),
        mode: 'all',
        boundIdentity: HERMES,
        messageRefCache: createMessageRefCache(),
      }
      const { events } = yield* drainOneUnderTestClock(config, 1).pipe(
        Effect.provide(captureLogger(logLines)),
      )
      expect(events).toHaveLength(1)
      expect(logLines).toContain(
        'commy zulip events: rate-limited on /events (429), backing off 7s',
      )
    }),
  ))
