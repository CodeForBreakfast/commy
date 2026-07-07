/**
 * Event-pump LOGIC, exercised through the full adapter stack
 * (adapter → ZulipHttp → HttpClient) on the **owned-fake stub HttpClient +
 * TestClock** — no `Bun.serve`, no real socket.
 *
 * These long-poll / reconnect tests run the LOGIC on the stub rather than a
 * real socket, which makes them deterministic and avoids real-socket
 * contention flakes: the infinite long-poll hold becomes a stub
 * `{ hang: true }` response that parks on `Effect.never`, and scope-close
 * interruption of `Effect.never` is deterministic. The retry/backoff sleeps
 * run on the virtual clock.
 *
 * SCOPE EDGE: the stub proves the *Effect fiber-interrupt* logic. The genuine
 * `AbortSignal → fetch → TCP teardown` of an in-flight `FetchHttpClient`
 * long-poll on scope close stays a real-socket Tier-3 test — that
 * integration cannot move off the socket.
 *
 * The producer-level pump logic (transient-retry breadcrumbs, the exponential
 * backoff cap, per-event parsing) is covered separately at the `inboxEvents`
 * unit level in `events.test.ts` with a `ZulipHttp`-level fake. These tests add
 * the *adapter wiring*: the adapter-scoped gap-replay watermark surviving across
 * `events()` iterator instances, `inbox.replay()` wired into the producer's
 * BAD_EVENT_QUEUE_ID recovery, and the forked-drain scope-close path.
 */

import { expect } from 'bun:test'
import type { ChannelRef, InboundEvent } from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeMessageBodySync,
  decodeTimestampSync,
  type IdentityError,
  type UnknownIdentity,
} from '@commy/core/ports'
import { effectTest } from '@commy/testing/effect-test'
import {
  type CapturedHttpRequest,
  makeStubHttpClient,
  type StubHttpClient,
} from '@commy/testing/stub-http-client'
import { HttpClient } from '@effect/platform'
import {
  Duration,
  Effect,
  Queue,
  Redacted,
  type Scope,
  Stream,
  TestClock,
  TestContext,
} from 'effect'
import type { ZulipAdapter } from './adapter.ts'
import { zulipAdapter } from './adapter.ts'
import type { QueueState } from './events.ts'
import { ApiKey, BotEmail, RealmUrl } from './http.ts'

const HERMES = {
  user_id: 9,
  email: 'hermes-agent-bot@example.com',
  full_name: 'hermes-agent',
  is_bot: true,
  is_active: true,
  role: 400,
} as const

const MAINTAINER = {
  user_id: 5,
  email: 'user@example.com',
  full_name: 'Robin Reyes',
  is_bot: false,
  is_active: true,
  role: 100,
} as const

const REALM_URL = 'https://zulip.example.com'

const seedUsers = (stub: StubHttpClient, members: ReadonlyArray<unknown>): Effect.Effect<void> =>
  stub.respond('GET', '/api/v1/users', { body: { result: 'success', members } })

const seedRegenerate = (stub: StubHttpClient, userId: number): Effect.Effect<void> =>
  stub.respond('POST', `/api/v1/bots/${userId}/api_key/regenerate`, {
    body: { result: 'success', api_key: 'fresh-key' },
  })

const seedRegister = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/register', {
    body: { result: 'success', queue_id: 'queue-1', last_event_id: 0 },
  })

const buildAdapter = (
  stub: StubHttpClient,
): Effect.Effect<ZulipAdapter, IdentityError | UnknownIdentity> =>
  Effect.gen(function* () {
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedRegenerate(stub, HERMES.user_id)
    const config = {
      realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
      minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
      minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
    }
    const adapter = yield* zulipAdapter(config).pipe(
      Effect.provideService(HttpClient.HttpClient, stub.client),
    )
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    return adapter
  })

// Attach-mode adapter: bind HERMES via a supplied stable key —
// seed NEITHER regenerate NOR mint, so the bind proves it took the attach path.
const buildAttachAdapter = (
  stub: StubHttpClient,
): Effect.Effect<ZulipAdapter, IdentityError | UnknownIdentity> =>
  Effect.gen(function* () {
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    const config = {
      realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
      minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
      minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
      attachIdentity: {
        name: decodeBotNameSync('hermes-agent'),
        apiKey: Redacted.make(yield* ApiKey('stable-provided-key').pipe(Effect.orDie)),
      },
    }
    const adapter = yield* zulipAdapter(config).pipe(
      Effect.provideService(HttpClient.HttpClient, stub.client),
    )
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    return adapter
  })

// Build an adapter carrying the queue-state write-half config (idle timeout +
// persistence hooks) so the eager subscribe-time register can be inspected.
const buildAdapterWithQueueConfig = (
  stub: StubHttpClient,
  queueConfig: {
    readonly queueIdleTimeoutSecs?: number
    readonly onQueueRegister?: (queue: QueueState) => Effect.Effect<void>
    readonly onQueueAdvance?: (lastEventId: number) => Effect.Effect<void>
  },
): Effect.Effect<ZulipAdapter, IdentityError | UnknownIdentity> =>
  Effect.gen(function* () {
    yield* seedUsers(stub, [HERMES, MAINTAINER])
    yield* seedRegenerate(stub, HERMES.user_id)
    const config = {
      realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
      minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
      minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
      ...queueConfig,
    }
    const adapter = yield* zulipAdapter(config).pipe(
      Effect.provideService(HttpClient.HttpClient, stub.client),
    )
    yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
    return adapter
  })

const homeChannel: ChannelRef = {
  id: decodeChannelIdSync('1234'),
  name: decodeChannelNameSync('general'),
  permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/1234-general'),
}

const seedSubscribeOk = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/users/me/subscriptions', {
    body: {
      result: 'success',
      subscribed: { 'hermes-agent-bot@example.com': ['general'] },
      already_subscribed: {},
      unauthorized: [],
    },
  })

// Mirror inbox.events() into an unbounded Queue under the caller's Scope. The
// forked Stream.runDrain fiber lives for the scope's lifetime, so scope close
// interrupts it (and any in-flight long-poll). The stub answers instantly, so
// every event sequence ends with a `{ hang: true }` entry — the long-poll hold
// that keeps the eager drain from spinning past the canned responses.
const eventQueue = (
  adapter: ZulipAdapter,
): Effect.Effect<Queue.Queue<InboundEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<InboundEvent>()
    yield* Effect.forkScoped(
      adapter.inbox.events().pipe(
        Stream.tap((event) => Queue.offer(queue, event)),
        Stream.runDrain,
      ),
    )
    return queue
  })

const isEventsPoll = (r: CapturedHttpRequest): boolean =>
  r.method === 'GET' && r.url.pathname === '/api/v1/events'

const isRegisterPost = (r: CapturedHttpRequest): boolean =>
  r.method === 'POST' && r.url.pathname === '/api/v1/register'

const eventPolls = (stub: StubHttpClient): Effect.Effect<ReadonlyArray<CapturedHttpRequest>> =>
  stub.captured.pipe(Effect.map((reqs) => reqs.filter(isEventsPoll)))

const registerPosts = (stub: StubHttpClient): Effect.Effect<ReadonlyArray<CapturedHttpRequest>> =>
  stub.captured.pipe(Effect.map((reqs) => reqs.filter(isRegisterPost)))

// Condition-gate (not a sleep): yield to the forked drain until it has issued
// `n` GET /events polls. Replaces the original real `Effect.sleep` that waited
// for a long-poll "to get into flight" — here the drain parks on a stub hang,
// and we wait for that poll to have been captured before unwinding the scope.
const awaitEventPolls = (stub: StubHttpClient, n: number): Effect.Effect<void> =>
  Effect.yieldNow().pipe(
    Effect.zipRight(eventPolls(stub)),
    Effect.map((polls) => polls.length),
    Effect.repeat({ until: (count) => count >= n }),
    Effect.asVoid,
  )

const aZulipMessage = (
  overrides: Partial<{
    id: number
    sender_id: number
    sender_full_name: string
    stream_id: number
    display_recipient: string
    subject: string
    content: string
    timestamp: number
  }> = {},
): Record<string, unknown> => ({
  id: 100,
  sender_id: MAINTAINER.user_id,
  sender_full_name: MAINTAINER.full_name,
  stream_id: 1234,
  display_recipient: 'general',
  subject: 'lobby',
  content: 'hello',
  timestamp: 1715000000,
  ...overrides,
})

const messageEvent = (id: number, message: Record<string, unknown>): Record<string, unknown> => ({
  id,
  type: 'message',
  message,
  flags: [],
})

const gapMessagesBody = (content: string): Record<string, unknown> => ({
  result: 'success',
  messages: [
    {
      id: 150,
      sender_id: MAINTAINER.user_id,
      sender_full_name: MAINTAINER.full_name,
      stream_id: 1234,
      display_recipient: 'general',
      subject: 'lobby',
      content,
      timestamp: 1500,
      flags: [],
    },
  ],
  anchor: 0,
  found_anchor: false,
  found_newest: true,
  found_oldest: false,
  history_limited: false,
})

effectTest(
  'inbox.events advances last_event_id between long-poll calls',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [messageEvent(5, aZulipMessage({ content: 'first' }))],
          },
        },
        {
          body: {
            result: 'success',
            events: [messageEvent(11, aZulipMessage({ id: 200, content: 'second' }))],
          },
        },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      yield* Queue.take(queue)
      yield* Queue.take(queue)
      const polls = yield* eventPolls(stub)
      expect(polls.length).toBeGreaterThanOrEqual(2)
      expect(polls[0]?.url.searchParams.get('last_event_id')).toBe('0')
      expect(polls[1]?.url.searchParams.get('last_event_id')).toBe('5')
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'eager subscribe-time register carries idle_queue_timeout and fires onQueueRegister',
  () =>
    Effect.gen(function* () {
      const registered: QueueState[] = []
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapterWithQueueConfig(stub, {
        queueIdleTimeoutSecs: 3600,
        onQueueRegister: (q) => Effect.sync(() => void registered.push(q)),
      })
      yield* seedRegister(stub)
      yield* seedSubscribeOk(stub)
      yield* adapter.inbox.subscribe(homeChannel.name)
      const registers = yield* registerPosts(stub)
      expect(registers).toHaveLength(1)
      expect(new URLSearchParams(registers[0]?.body).get('idle_queue_timeout')).toBe('3600')
      // The eager register persisted the queue — the queueId a resume recovers.
      expect(registered).toEqual([{ queueId: 'queue-1', lastEventId: 0 }])
    }),
  { layer: TestContext.TestContext },
)

// Attached-persona capability: a session attached to a persona via a
// supplied stable key wakes on `@persona` mentioned in a different channel than
// the one it subscribed — because attach binds the persona as the session's own
// identity, so the existing content-synthesis mention path fires for `@persona`
// realm-wide. The home channel subscribe puts the queue in mode-'all'; the
// mention lands in a channel the session never subscribed to.
effectTest(
  'inbox.events: attached persona wakes on @persona mentioned in another channel (no key regenerate)',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAttachAdapter(stub)
      yield* seedSubscribeOk(stub)
      yield* seedRegister(stub)
      yield* adapter.inbox.subscribe(homeChannel.name)
      yield* adapter.inbox.subscribe('mentions')
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [
              messageEvent(
                7,
                aZulipMessage({
                  id: 300,
                  stream_id: 5678,
                  display_recipient: 'brewlife',
                  subject: 'tasting',
                  content: 'hey @**hermes-agent** thoughts?',
                }),
              ),
            ],
          },
        },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      // Drain until the mention-received surfaces (a message-posted for the same
      // frame may arrive first).
      const mention = yield* Queue.take(queue).pipe(
        Effect.repeat({ until: (e: InboundEvent) => e.kind === 'mention-received' }),
      )
      expect(mention.kind).toBe('mention-received')
      if (mention.kind === 'mention-received') {
        expect(mention.mentions.map((m) => m.name)).toContain(decodeDisplayNameSync('hermes-agent'))
        expect(mention.message.ref.channel.name).toEqual(decodeChannelNameSync('brewlife'))
      }
      // The wake came via attach, not a rotated key.
      const reqs = yield* stub.captured
      expect(reqs.find((r) => r.url.pathname.endsWith('/api_key/regenerate'))).toBeUndefined()
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events ignores Zulip heartbeat events but advances last_event_id',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'success', events: [{ id: 17, type: 'heartbeat' }] } },
        { body: { result: 'success', events: [messageEvent(18, aZulipMessage())] } },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      const event = yield* Queue.take(queue)
      expect(event.kind).toBe('message-posted')
      const polls = yield* eventPolls(stub)
      expect(polls.length).toBeGreaterThanOrEqual(2)
      expect(polls[1]?.url.searchParams.get('last_event_id')).toBe('17')
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events re-registers and resumes when /events returns BAD_EVENT_QUEUE_ID',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
          status: 400,
        },
        { body: { result: 'success', events: [messageEvent(1, aZulipMessage())] } },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      const event = yield* Queue.take(queue)
      expect(event.kind).toBe('message-posted')
      // First poll's dead queue forces a fresh /register before the retry.
      expect(yield* registerPosts(stub)).toHaveLength(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events backfills the gap via inbox.replay() on BAD_EVENT_QUEUE_ID and marks events replayed=true',
  () =>
    Effect.gen(function* () {
      // Live message at ts=1000 sets the watermark; BAD_EVENT_QUEUE_ID dies the
      // queue; the iterator calls replay(since=1000) which returns a message
      // posted at ts=1500 during the dead window; a fresh live message at
      // ts=2000 follows on the re-registered queue. The middle event must
      // surface with replayed=true; the live ones must not.
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respond('GET', '/api/v1/messages', {
        body: gapMessagesBody('posted during the gap'),
      })
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [messageEvent(1, aZulipMessage({ id: 100, timestamp: 1000 }))],
          },
        },
        {
          body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
          status: 400,
        },
        {
          body: {
            result: 'success',
            events: [messageEvent(1, aZulipMessage({ id: 200, timestamp: 2000 }))],
          },
        },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      const collected: InboundEvent[] = []
      for (let i = 0; i < 3; i += 1) {
        collected.push(yield* Queue.take(queue))
      }
      expect(collected).toHaveLength(3)
      expect(collected[0]?.kind).toBe('message-posted')
      if (collected[0]?.kind === 'message-posted') {
        expect(collected[0].replayed).toBeUndefined()
        expect(String(collected[0].message.ref.id)).toBe('100')
      }
      expect(collected[1]?.kind).toBe('message-posted')
      if (collected[1]?.kind === 'message-posted') {
        expect(collected[1].replayed).toBe(true)
        expect(String(collected[1].message.ref.id)).toBe('150')
        expect(collected[1].message.body).toBe(decodeMessageBodySync('posted during the gap'))
      }
      expect(collected[2]?.kind).toBe('message-posted')
      if (collected[2]?.kind === 'message-posted') {
        expect(collected[2].replayed).toBeUndefined()
        expect(String(collected[2].message.ref.id)).toBe('200')
      }
      expect(yield* registerPosts(stub)).toHaveLength(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events persists the gap-replay watermark across iterator instances so reconnect-then-BAD_EVENT_QUEUE_ID still backfills',
  () =>
    Effect.gen(function* () {
      // The gap-replay watermark lives at the adapter, not in the iterator's
      // closure, so a second events() iterator inherits the first's last-seen
      // ts and a BAD_EVENT_QUEUE_ID on the new iterator fires the replay.
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respond('GET', '/api/v1/messages', {
        body: gapMessagesBody('posted during the reconnect gap'),
      })
      // Sequence across both iterators:
      //   poll1 (iter1) — live ts=1000, advances the watermark
      //   poll2 (iter1) — HANG: the long-poll holds, so iter1's eager drain
      //                   does not race ahead and consume the BAD_QUEUE meant
      //                   for iter2; iter1's scope close interrupts it.
      //   poll3 (iter2) — BAD_EVENT_QUEUE_ID; iter2's first poll hits replay.
      //   poll4 (iter2) — live ts=2000 on the re-registered queue.
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [messageEvent(1, aZulipMessage({ id: 100, timestamp: 1000 }))],
          },
        },
        { hang: true },
        {
          body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' },
          status: 400,
        },
        {
          body: {
            result: 'success',
            events: [messageEvent(1, aZulipMessage({ id: 200, timestamp: 2000 }))],
          },
        },
        { hang: true },
      ])

      yield* Effect.scoped(
        Effect.gen(function* () {
          const queue1 = yield* eventQueue(adapter)
          const first = yield* Queue.take(queue1)
          expect(first.kind).toBe('message-posted')
          if (first.kind === 'message-posted') {
            expect(first.message.ts).toBe(decodeTimestampSync(1000))
            expect(first.replayed).toBeUndefined()
          }
          // Hold the scope open until poll2 (the hang) is in flight, so iter1
          // owns exactly poll1+poll2 and iter2 starts at the BAD_QUEUE poll.
          yield* awaitEventPolls(stub, 2)
        }),
      )

      yield* Effect.scoped(
        Effect.gen(function* () {
          const queue2 = yield* eventQueue(adapter)
          const collected: InboundEvent[] = []
          for (let i = 0; i < 2; i += 1) {
            collected.push(yield* Queue.take(queue2))
          }
          expect(collected[0]?.kind).toBe('message-posted')
          if (collected[0]?.kind === 'message-posted') {
            expect(collected[0].replayed).toBe(true)
            expect(collected[0].message.ts).toBe(decodeTimestampSync(1500))
            expect(collected[0].message.body).toBe(
              decodeMessageBodySync('posted during the reconnect gap'),
            )
          }
          expect(collected[1]?.kind).toBe('message-posted')
          if (collected[1]?.kind === 'message-posted') {
            expect(collected[1].replayed).toBeUndefined()
            expect(collected[1].message.ts).toBe(decodeTimestampSync(2000))
          }
        }),
      )
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events recovers when /events returns 429 RATE_LIMIT_HIT',
  () =>
    Effect.gen(function* () {
      // A 429 from /events is backpressure: the send path waits out the
      // retry-after and retries. The wait runs on the virtual clock.
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'error',
            code: 'RATE_LIMIT_HIT',
            msg: 'API usage exceeded rate limit',
            'retry-after': 0,
          },
          status: 429,
        },
        { body: { result: 'success', events: [messageEvent(1, aZulipMessage())] } },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      // The rate-limit backoff (min 100ms) sleeps on the virtual clock.
      yield* TestClock.adjust(Duration.millis(100))
      const event = yield* Queue.take(queue)
      expect(event.kind).toBe('message-posted')
      expect((yield* eventPolls(stub)).length).toBeGreaterThanOrEqual(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events recovers when /register returns 429 RATE_LIMIT_HIT',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* stub.respondSequence('POST', '/api/v1/register', [
        {
          body: {
            result: 'error',
            code: 'RATE_LIMIT_HIT',
            msg: 'API usage exceeded rate limit',
            'retry-after': 0,
          },
          status: 429,
        },
        { body: { result: 'success', queue_id: 'queue-1', last_event_id: 0 } },
      ])
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'success', events: [messageEvent(1, aZulipMessage())] } },
        { hang: true },
      ])
      const queue = yield* eventQueue(adapter)
      yield* TestClock.adjust(Duration.millis(100))
      const event = yield* Queue.take(queue)
      expect(event.kind).toBe('message-posted')
      expect(yield* registerPosts(stub)).toHaveLength(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'inbox.events long-poll fiber interrupts cleanly when the consumer scope closes',
  () =>
    Effect.gen(function* () {
      // A hung long-poll must not pin the pump on shutdown. The stub holds the
      // /events poll open (Effect.never); scope close interrupts the forked
      // Stream fiber. The test passes iff the outer scope close completes — the
      // genuine AbortSignal→fetch→TCP teardown is the Tier-3 residue.
      const stub = yield* makeStubHttpClient
      const adapter = yield* buildAdapter(stub)
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [{ hang: true }])
      yield* Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* eventQueue(adapter)
          // Let the long-poll get into flight before the scope unwinds.
          yield* awaitEventPolls(stub, 1)
          void queue
        }),
      )
      expect((yield* eventPolls(stub)).length).toBeGreaterThanOrEqual(1)
    }),
  { layer: TestContext.TestContext },
)
