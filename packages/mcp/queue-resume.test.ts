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
import { Deferred, Effect, Option, Queue, Redacted, type Scope, Stream, TestContext } from 'effect'
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

effectTest(
  'resumeQueue yields None promptly on an unfed session deferred without parking',
  () =>
    Effect.gen(function* () {
      // Mirrors the write-half hooks: the read-half resolver must poll the
      // deferred and return promptly on None — never park on await — when no
      // source has delivered the id yet.
      const store = yield* tmpQueueStore()
      const session = yield* Deferred.make<SessionId>()
      const hooks = buildQueueStateHooks({
        store,
        session,
        idleTimeoutSecs: 3600,
        resumeOutcome: yield* Deferred.make<boolean>(),
      })

      const outcome = yield* hooks.resumeQueue().pipe(Effect.timeoutOption('2 seconds'))

      // Completed within the timeout (Some) and resolved to no-resume (inner None).
      expect(Option.isSome(outcome)).toBe(true)
      expect(Option.isNone(Option.getOrThrow(outcome))).toBe(true)
    }),
  { layer: TestContext.TestContext },
)
