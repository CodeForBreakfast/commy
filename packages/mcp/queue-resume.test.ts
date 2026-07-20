/**
 * Queue-resume WRITE half, integration level: a running ephemeral seat
 * persists its events-queue state as it registers and polls, so a later
 * (out-of-scope, read-half) resume can recover it. Drives the REAL Zulip
 * adapter over an owned-fake stub HttpClient, wired with the REAL
 * `buildQueueStateHooks` closure over the REAL file-backed queue-state store
 * and the shared session-id deferred — no mocked persistence. Asserts the three
 * write-half guarantees the DoD names:
 *
 *   1. the eager subscribe-time register carries `idle_queue_timeout`,
 *   2. the register writes `{queueId, lastEventId}` to the per-session store,
 *   3. a poll advances the persisted `lastEventId` to the batch's max event id.
 */
import { expect } from 'bun:test'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { decodeBotNameSync, decodeChannelNameSync, type InboundEvent } from '@commy/core/ports'
import { effectTest } from '@commy/testing/effect-test'
import { makeStubHttpClient, type StubHttpClient } from '@commy/testing/stub-http-client'
import { zulipAdapter } from '@commy/zulip/adapter'
import { ApiKey, BotEmail, RealmUrl } from '@commy/zulip/http'
import { FileSystem, HttpClient } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import {
  Deferred,
  Effect,
  Fiber,
  Option,
  Queue,
  Redacted,
  type Scope,
  Stream,
  TestClock,
  TestContext,
} from 'effect'
import { parseSessionId, type SessionId } from './bootstrap.ts'
import { buildQueueStateHooks } from './queue-state-hooks.ts'
import { createFileQueueStateStore, type QueueStateStore } from './queue-state-store.ts'

const REALM_URL = 'https://zulip.example.com'
const SID = '61b08d76-0000-4000-8000-000000000001'
const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

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

const nodeFs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const seedUsers = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('GET', '/api/v1/users', {
    body: { result: 'success', members: [HERMES, MAINTAINER] },
  })
const seedRegenerate = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('POST', `/api/v1/bots/${HERMES.user_id}/api_key/regenerate`, {
    body: { result: 'success', api_key: 'fresh-key' },
  })
const seedRegister = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/register', {
    body: { result: 'success', queue_id: 'queue-1', last_event_id: 0 },
  })
const seedSubscribeOk = (stub: StubHttpClient): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/users/me/subscriptions', {
    body: {
      result: 'success',
      subscribed: { 'hermes-agent-bot@example.com': ['general'] },
      already_subscribed: {},
      unauthorized: [],
    },
  })

const aZulipMessage = (id: number): Record<string, unknown> => ({
  id,
  sender_id: MAINTAINER.user_id,
  sender_full_name: MAINTAINER.full_name,
  stream_id: 1234,
  display_recipient: 'general',
  subject: 'lobby',
  content: 'hello',
  timestamp: 1715000000,
})
const messageEvent = (id: number): Record<string, unknown> => ({
  id,
  type: 'message',
  message: aZulipMessage(100 + id),
  flags: [],
})
const reactionEvent = (
  id: number,
  op: 'add' | 'remove',
  messageId: number,
  emojiName: string,
  userId: number,
): Record<string, unknown> => ({
  id,
  type: 'reaction',
  op,
  user_id: userId,
  message_id: messageId,
  emoji_name: emojiName,
})

const tmpQueueStore = (): Effect.Effect<QueueStateStore, never, Scope.Scope> =>
  Effect.gen(function* () {
    const dir = mkdtempSync(join(tmpdir(), 'queue-resume-'))
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => rmSync(dir, { recursive: true, force: true })),
    )
    return createFileQueueStateStore({ dir, fs: nodeFs })
  })

effectTest(
  'ephemeral seat persists queueId on register and advances lastEventId on poll — register carries idle_queue_timeout',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      // The boot-env feeder has already filled the shared id (the fleet's real
      // state — CC injects the session id at boot), so the write-half hooks can
      // key against it from the very first register.
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub)
      yield* seedRegenerate(stub)
      const adapter = yield* zulipAdapter({
        realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
        minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
        minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
        queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
        onQueueRegister: hooks.onQueueRegister,
        onQueueAdvance: hooks.onQueueAdvance,
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))
      yield* seedRegister(stub)
      yield* seedSubscribeOk(stub)

      // Subscribe → eager register → onQueueRegister persists the queue.
      yield* adapter.inbox.subscribe(decodeChannelNameSync('general'))

      const registers = (yield* stub.captured).filter(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
      )
      expect(registers).toHaveLength(1)
      expect(new URLSearchParams(registers[0]?.body).get('idle_queue_timeout')).toBe('3600')
      // Written on register: the queueId a resume recovers, at the queue's start.
      expect(yield* store.read(sid(SID))).toEqual(
        Option.some({ queueId: 'queue-1', lastEventId: 0 }),
      )

      // Poll returns events up to id 7; the drain runs the producer step, which
      // fires onQueueAdvance(7) → store.advance before the chunk is emitted.
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'success', events: [messageEvent(5), messageEvent(7)] } },
        { hang: true },
      ])
      const queue = yield* Queue.unbounded<unknown>()
      yield* Effect.forkScoped(
        adapter.inbox.events().pipe(
          Stream.tap((event) => Queue.offer(queue, event)),
          Stream.runDrain,
        ),
      )
      yield* Queue.take(queue)

      // Advanced on poll: lastEventId walked forward to the batch max, queueId kept.
      expect(yield* store.read(sid(SID))).toEqual(
        Option.some({ queueId: 'queue-1', lastEventId: 7 }),
      )
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'onQueueRegister with an unfed session deferred no-ops without parking or writing',
  () =>
    Effect.gen(function* () {
      // A seat whose id no source has delivered yet: the hot-path hooks must
      // poll the deferred and return promptly on None — never park on await —
      // and leave the store untouched (nothing to key against).
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const outcome = yield* hooks
        .onQueueRegister({ queueId: 'queue-1', lastEventId: 0 })
        .pipe(Effect.timeoutOption('2 seconds'))

      expect(Option.isSome(outcome)).toBe(true)
      expect(Option.isNone(yield* store.read(sid(SID)))).toBe(true)
    }),
  { layer: TestContext.TestContext },
)

// The advance-hook half of the same hot-path guarantee. Added with comms-9iro:
// `onQueueRegister` was covered above and `onQueueAdvance` was not, so when
// `resumeQueue` stopped being non-blocking the only remaining assertion holding
// the hot path would have been a single hook's. `onQueueAdvance` fires on EVERY
// cursor-moving poll — the hottest of the three — so it is the one that least
// tolerates parking.
effectTest(
  'onQueueAdvance with an unfed session deferred no-ops without parking or writing',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const outcome = yield* hooks.onQueueAdvance(7).pipe(Effect.timeoutOption('2 seconds'))

      expect(Option.isSome(outcome)).toBe(true)
      expect(Option.isNone(yield* store.read(sid(SID)))).toBe(true)
    }),
  { layer: TestContext.TestContext },
)

// ─── Queue-resume READ half ──────────────────────────────────────
// The payoff: a resumed ephemeral seat whose surviving queue-state is on disk
// resume-polls that queue from the stored cursor — skipping its own register —
// and the backlog buffered during downtime, REACTIONS INCLUDED, replays through
// the normal producer path with zero tool calls. Reactions are the reason this
// path exists: no history-read catch-up can reconstruct them.
effectTest(
  'resumed seat resume-polls its on-disk queue and replays a buffered reaction with no fresh register',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      // A prior life left the queue-state on disk; the boot-env feeder has
      // filled the shared id, so the read-half resolver can key against it.
      yield* store.write(sid(SID), { queueId: 'resumed-q', lastEventId: 41 })
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub)
      yield* seedRegenerate(stub)
      const adapter = yield* zulipAdapter({
        realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
        minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
        minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
        queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
        onQueueRegister: hooks.onQueueRegister,
        onQueueAdvance: hooks.onQueueAdvance,
        resumeQueue: hooks.resumeQueue,
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))

      // The backlog the surviving server-side queue buffered while dead: the
      // reacted-to message (seeds the ref cache in-batch) then the reaction.
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [
              messageEvent(42),
              reactionEvent(43, 'add', 142, 'thumbs_up', MAINTAINER.user_id),
            ],
          },
        },
        { hang: true },
      ])
      const queue = yield* Queue.unbounded<InboundEvent>()
      yield* Effect.forkScoped(
        adapter.inbox.events().pipe(
          Stream.tap((event) => Queue.offer(queue, event)),
          Stream.runDrain,
        ),
      )
      const first = yield* Queue.take(queue)
      const second = yield* Queue.take(queue)

      // Resume-poll: the first /events poll targets the persisted queue at the
      // stored cursor.
      const polls = (yield* stub.captured).filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/events',
      )
      expect(polls[0]?.url.searchParams.get('queue_id')).toBe('resumed-q')
      expect(polls[0]?.url.searchParams.get('last_event_id')).toBe('41')
      // No fresh register — the surviving queue is reused wholesale.
      const registers = (yield* stub.captured).filter(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
      )
      expect(registers).toHaveLength(0)
      // The backlog replays: message first, then the reaction that no history
      // catch-up could have recovered — all with zero tool calls.
      expect(first.kind).toBe('message-posted')
      expect(second.kind).toBe('reaction-added')
    }),
  { layer: TestContext.TestContext },
)

// ─── The LATE session id: a listen-only seat's backlog (comms-9iro) ──────────
// The population defect (1) actually reaches: a client with NO boot session id,
// i.e. a non-CC MCP client supplying its own UUID through a tool call. (Fleet CC
// seats carry CLAUDE_CODE_SESSION_ID, measured present 6/6, so the resume poll
// wins for them and this never fires — the blast radius is narrower than the
// bead's description reads.)
//
// The shape: the pump materialises BEFORE any source has delivered the id, so
// `resumeQueue` resolves against an unfed deferred. The id then arrives — but a
// one-shot poll has already reported 'no candidate', latched the verdict, and
// registered a fresh empty queue. The surviving queue-state sits on disk, valid
// and never read again for the pump's lifetime.
//
// Why this is the reaction-losing path specifically, and why REST catch-up is
// not a substitute: only NATIVE QUEUE REPLAY reconstructs reactions
// (server.ts:585-586 — reactions are the accepted >24h-downtime limit of the
// history fallback). A seat that loses its queue loses every reaction sent
// during downtime with no second route to them, which is exactly the payload
// class that motivated this bead.
effectTest(
  'a session id arriving after pump materialisation still resumes the surviving queue instead of abandoning it for a fresh register',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      // A prior life left the queue-state on disk — but NOTHING has fed the id:
      // no boot env var, and a listen-only seat fires no hook-matched tool call.
      yield* store.write(sid(SID), { queueId: 'resumed-q', lastEventId: 41 })
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub)
      yield* seedRegenerate(stub)
      const adapter = yield* zulipAdapter({
        realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
        minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
        minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
        queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
        onQueueRegister: hooks.onQueueRegister,
        onQueueAdvance: hooks.onQueueAdvance,
        resumeQueue: hooks.resumeQueue,
        onResumeOutcome: hooks.onResumeOutcome,
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))

      // Seeded so the BROKEN path fails on its assertions rather than erroring:
      // a fresh register is what today's code does, and it must be answerable.
      yield* seedRegister(stub)
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [
              messageEvent(42),
              reactionEvent(43, 'add', 142, 'thumbs_up', MAINTAINER.user_id),
            ],
          },
        },
        { hang: true },
      ])

      const queue = yield* Queue.unbounded<InboundEvent>()
      yield* Effect.forkScoped(
        adapter.inbox.events().pipe(
          Stream.tap((event) => Queue.offer(queue, event)),
          Stream.runDrain,
        ),
      )
      // Let the forked pump reach its resume decision on an unfed deferred
      // before the id lands — this ordering IS the defect under test.
      yield* Effect.yieldNow()
      yield* Effect.yieldNow()
      yield* Deferred.succeed(session, sid(SID))

      const first = yield* Queue.take(queue)
      const second = yield* Queue.take(queue)

      // WHICH ASSERTIONS DISCRIMINATE, stated because it is not obvious and the
      // difference is the whole point of the test. The stub keys responses on
      // METHOD + PATH ONLY — not on the queue_id query param — so it serves the
      // same canned backlog to a fresh queue as to the resumed one. The event
      // assertions below therefore pass in BOTH the fixed and the broken world:
      // they document intent, they do not detect the defect. Against a real
      // realm a fresh queue starts empty and those events are simply gone.
      //
      // The load-bearing assertions are these three, which fail today: the seat
      // must poll the SURVIVING queue at its STORED cursor and must NOT register
      // a fresh one. Abandoning the queue is the mechanism by which reactions
      // become unrecoverable, since no history catch-up can reconstruct them.
      const polls = (yield* stub.captured).filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/events',
      )
      expect(polls[0]?.url.searchParams.get('queue_id')).toBe('resumed-q')
      expect(polls[0]?.url.searchParams.get('last_event_id')).toBe('41')
      const registers = (yield* stub.captured).filter(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/register',
      )
      expect(registers).toHaveLength(0)
      // Corroborative only, per the note above — the backlog shape replays.
      expect(first.kind).toBe('message-posted')
      expect(second.kind).toBe('reaction-added')
    }),
  { layer: TestContext.TestContext },
)

// ─── The COMMY_SUBSCRIBE clobber regression (comms-us7t) ──────────
// The fleet's real resume shape: CC re-passes COMMY_SUBSCRIBE on every boot, so
// server.ts runs an eager subscribe-time register BEFORE the producer resumes
// (subscribeFromEnv at boot, pump forked after). That eager register must not
// let its onQueueRegister store-write clobber the surviving queue-state the
// producer is about to resume from — otherwise the seat silently registers a
// fresh empty queue and loses its entire downtime backlog.
effectTest(
  'an eager subscribe-time register on resume boot does not clobber the surviving queue — the backlog replays off it',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const resumeOutcome = yield* Deferred.make<boolean>()
      // A prior life's surviving queue-state on disk; the boot-env feeder has
      // filled the shared id (the fleet's real state).
      yield* store.write(sid(SID), { queueId: 'resumed-q', lastEventId: 41 })
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({ store, session, idleTimeoutSecs: 3600, resumeOutcome })

      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub)
      yield* seedRegenerate(stub)
      const adapter = yield* zulipAdapter({
        realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
        minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
        minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
        queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
        onQueueRegister: hooks.onQueueRegister,
        onQueueAdvance: hooks.onQueueAdvance,
        resumeQueue: hooks.resumeQueue,
        onResumeOutcome: hooks.onResumeOutcome,
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))

      // The eager COMMY_SUBSCRIBE register: a subscribe BEFORE the producer
      // resumes, doing its own POST /register → queue-1. Its onQueueRegister
      // write must be suppressed so it cannot overwrite the surviving state.
      yield* seedRegister(stub)
      yield* seedSubscribeOk(stub)
      yield* adapter.inbox.subscribe(decodeChannelNameSync('general'))

      // Surviving state intact — the eager register's queue-1 did not clobber it.
      expect(yield* store.read(sid(SID))).toEqual(
        Option.some({ queueId: 'resumed-q', lastEventId: 41 }),
      )

      // The downtime backlog the surviving server-side queue buffered.
      yield* stub.respondSequence('GET', '/api/v1/events', [
        {
          body: {
            result: 'success',
            events: [
              messageEvent(42),
              reactionEvent(43, 'add', 142, 'thumbs_up', MAINTAINER.user_id),
            ],
          },
        },
        { hang: true },
      ])
      const queue = yield* Queue.unbounded<InboundEvent>()
      yield* Effect.forkScoped(
        adapter.inbox.events().pipe(
          Stream.tap((event) => Queue.offer(queue, event)),
          Stream.runDrain,
        ),
      )
      const first = yield* Queue.take(queue)
      const second = yield* Queue.take(queue)

      // The resume-poll targets the SURVIVING queue at its stored cursor — proof
      // the eager register's queue-1 did not shadow the resume candidate.
      const polls = (yield* stub.captured).filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/events',
      )
      expect(polls[0]?.url.searchParams.get('queue_id')).toBe('resumed-q')
      expect(polls[0]?.url.searchParams.get('last_event_id')).toBe('41')
      // The backlog replays — message then the reaction only native replay recovers.
      expect(first.kind).toBe('message-posted')
      expect(second.kind).toBe('reaction-added')
      // Resume verdict TRUE → the seat's REST catch-up stands down (no double-delivery).
      expect(yield* Deferred.await(resumeOutcome)).toBe(true)
    }),
  { layer: TestContext.TestContext },
)

// The other side of the guard: preserving the surviving candidate must NOT
// suppress the .3 fallback. When that candidate is dead server-side, the
// producer's resume-poll still hits BAD_EVENT_QUEUE_ID and reports 'missed', so
// the seat runs its REST catch-up. The eager register must not have masked the
// dead queue behind a fresh-but-alive one (which would report 'replayed' and
// wrongly skip the fallback).
effectTest(
  'an eager subscribe-time register on a queue-dead resume still reports missed so REST catch-up engages',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const resumeOutcome = yield* Deferred.make<boolean>()
      // A surviving queue on disk that is dead server-side (expired past TTL).
      yield* store.write(sid(SID), { queueId: 'dead-q', lastEventId: 41 })
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({ store, session, idleTimeoutSecs: 3600, resumeOutcome })

      const stub = yield* makeStubHttpClient
      yield* seedUsers(stub)
      yield* seedRegenerate(stub)
      const adapter = yield* zulipAdapter({
        realmUrl: yield* RealmUrl(REALM_URL).pipe(Effect.orDie),
        minterEmail: yield* BotEmail('minter@example.com').pipe(Effect.orDie),
        minterApiKey: Redacted.make(yield* ApiKey('minter-key').pipe(Effect.orDie)),
        queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
        onQueueRegister: hooks.onQueueRegister,
        onQueueAdvance: hooks.onQueueAdvance,
        resumeQueue: hooks.resumeQueue,
        onResumeOutcome: hooks.onResumeOutcome,
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      yield* adapter.identity.acquire(decodeBotNameSync('hermes-agent'))

      yield* seedRegister(stub)
      yield* seedSubscribeOk(stub)
      yield* adapter.inbox.subscribe(decodeChannelNameSync('general'))

      // The eager register preserved the (dead) surviving candidate for the
      // producer to try — it did not overwrite it with a fresh live queue.
      expect(yield* store.read(sid(SID))).toEqual(
        Option.some({ queueId: 'dead-q', lastEventId: 41 }),
      )

      // The surviving queue is gone server-side: the resume-poll hits BAD_EVENT_QUEUE_ID.
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'queue expired' } },
        { hang: true },
      ])
      yield* Effect.forkScoped(adapter.inbox.events().pipe(Stream.runDrain))

      // Resume verdict FALSE → the seat's REST catch-up fallback engages (no .3 regression).
      expect(yield* Deferred.await(resumeOutcome)).toBe(false)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'resumeQueue resolves the on-disk queue-state once the session id is known',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      yield* store.write(sid(SID), { queueId: 'resumed-q', lastEventId: 41 })
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      expect(yield* hooks.resumeQueue()).toEqual(
        Option.some({ queueId: 'resumed-q', lastEventId: 41 }),
      )
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'resumeQueue yields None when the session id is known but nothing is persisted',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      yield* Deferred.succeed(session, sid(SID))
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      expect(Option.isNone(yield* hooks.resumeQueue())).toBe(true)
    }),
  { layer: TestContext.TestContext },
)

// This pair replaces an earlier test that asserted `resumeQueue` returns
// PROMPTLY on an unfed deferred and never parks, justified as "mirrors the
// write-half hooks". That justification was the comms-9iro defect written into
// a test: `resumeQueue` does NOT mirror the write-half hooks — they fire on the
// producer's hot poll path (where parking would stall live delivery), it fires
// once at pump materialisation. Returning promptly there is what abandoned the
// surviving queue when an id was merely late rather than absent.
//
// The constraint that was REALLY being protected is that the pump must never
// park forever on an id that never comes. That is still true, and these two
// tests pin it precisely: waits for a late id, gives up bounded.
effectTest(
  'resumeQueue waits past pump materialisation for a late session id and resumes off it',
  () =>
    Effect.gen(function* () {
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      yield* store.write(sid(SID), { queueId: 'resumed-q', lastEventId: 41 })
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      // Nothing has fed the id when the resume is asked for.
      const fiber = yield* Effect.fork(hooks.resumeQueue())
      yield* TestClock.adjust('1 second')
      // The id lands late — but inside the bound, so it is still honoured.
      yield* Deferred.succeed(session, sid(SID))

      expect(yield* Fiber.join(fiber)).toEqual(
        Option.some({ queueId: 'resumed-q', lastEventId: 41 }),
      )
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'resumeQueue gives up BOUNDED on a session id that never arrives — it must not park the pump forever',
  () =>
    Effect.gen(function* () {
      // The listen-only client with no boot session id and no tool calls: the
      // deferred is never fed. Waiting outright would trade a lost backlog for
      // total deafness, so the wait has a ceiling.
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const fiber = yield* Effect.fork(hooks.resumeQueue())

      // Inside the bound it is still waiting — this is the half the old test got
      // wrong, and returning here is what cost the backlog.
      yield* TestClock.adjust('4 seconds')
      expect(Option.isNone(yield* Fiber.poll(fiber))).toBe(true)

      // Past the bound it gives up and the pump proceeds with a fresh register.
      yield* TestClock.adjust('2 seconds')
      expect(Option.isNone(yield* Fiber.join(fiber))).toBe(true)
    }),
  { layer: TestContext.TestContext },
)
