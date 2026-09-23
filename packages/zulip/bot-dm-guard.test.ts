/**
 * bot-dm-guard wrapper logic, exercised through a real `ZulipHttp` whose
 * underlying `HttpClient` is the **owned-fake stub** — no `Bun.serve`, no
 * real socket. The wrapper sits on every bot-authenticated `ZulipHttp` and
 * its job is a decision: intercept `POST /messages` with `type=private` and
 * reject when every non-self recipient is a bot, otherwise forward. That
 * decision is ours to test against the seam we own; the stub gives the
 * forwarded request something to land on so we can assert it was (or was not)
 * sent. None of these are live-contract duplicates — real Zulip cannot be
 * driven to bot↔bot rejection, and the allow-cases assert the guard's *allow
 * decision*, not that `POST /messages` works.
 */

import { expect } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import { makeStubHttpClient, type StubHttpClient } from '@commy/testing/stub-http-client'
import { HttpClient } from '@effect/platform'
import { Cause, Effect, Exit, Option, Schema } from 'effect'
import { BotToBotDirectMessageError, type RecipientDirectory, wrapBotHttp } from './bot-dm-guard.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'
import { ZulipUserRef } from './user-ref.ts'

const REALM_URL = 'https://zulip.example.com'

const SELF_ID = ZulipUserRef(100)
const OTHER_BOT_ID = 200
const SECOND_BOT_ID = 201
const HUMAN_ID = 300

const directoryStub = (): Effect.Effect<RecipientDirectory> =>
  Effect.succeed({
    byId: new Map<number, { kind: 'agent' | 'human' }>([
      [SELF_ID, { kind: 'agent' }],
      [OTHER_BOT_ID, { kind: 'agent' }],
      [SECOND_BOT_ID, { kind: 'agent' }],
      [HUMAN_ID, { kind: 'human' }],
    ]),
  })

const successSchema = Schema.Struct({ result: Schema.Literal('success') })

const sentMessageSchema = Schema.Struct({
  result: Schema.Literal('success'),
  id: Schema.Int,
})

const buildHttp = (stub: StubHttpClient): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    const realmUrl = yield* RealmUrl(REALM_URL)
    const email = yield* BotEmail('self-bot@example.com')
    const apiKey = yield* ApiKey('self-key')
    return yield* makeZulipHttp({ realmUrl, email, apiKey })
  }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client), Effect.orDie)

const seedMessageOk = (stub: StubHttpClient, id: number): Effect.Effect<void> =>
  stub.respond('POST', '/api/v1/messages', { body: { result: 'success', id } })

const requestsTo = (
  stub: StubHttpClient,
  method: string,
  pathname: string,
): Effect.Effect<number> =>
  stub.captured.pipe(
    Effect.map(
      (reqs) => reqs.filter((r) => r.method === method && r.url.pathname === pathname).length,
    ),
  )

const messagesPosted = (stub: StubHttpClient): Effect.Effect<number> =>
  requestsTo(stub, 'POST', '/api/v1/messages')

const expectBotDirectMessageDefect = <A>(eff: Effect.Effect<A, unknown>): Effect.Effect<void> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(eff)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const defect = Cause.dieOption(exit.cause)
      expect(Option.isSome(defect)).toBe(true)
      expect(Option.getOrThrow(defect)).toBeInstanceOf(BotToBotDirectMessageError)
    }
  })

effectTest('rejects /messages POST with type=private and a single bot recipient', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* expectBotDirectMessageDefect(
      http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([OTHER_BOT_ID]),
        content: 'hi',
      }),
    )
    expect(yield* messagesPosted(stub)).toBe(0)
  }),
)

effectTest('rejects /messages POST with type=private and multiple bot recipients', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* expectBotDirectMessageDefect(
      http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([OTHER_BOT_ID, SECOND_BOT_ID]),
        content: 'hi',
      }),
    )
    expect(yield* messagesPosted(stub)).toBe(0)
  }),
)

effectTest(
  'rejects /messages POST with type=private when self is included alongside bots only',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([SELF_ID, OTHER_BOT_ID]),
          content: 'hi',
        }),
      )
      expect(yield* messagesPosted(stub)).toBe(0)
    }),
)

effectTest('allows /messages POST with type=private to a single human recipient', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedMessageOk(stub, 1)
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.post('/messages', sentMessageSchema, {
      type: 'private',
      to: JSON.stringify([HUMAN_ID]),
      content: 'hi',
    })
    expect(yield* messagesPosted(stub)).toBe(1)
  }),
)

effectTest('allows /messages POST with type=private to a mixed bot+human group', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedMessageOk(stub, 2)
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.post('/messages', sentMessageSchema, {
      type: 'private',
      to: JSON.stringify([OTHER_BOT_ID, HUMAN_ID]),
      content: 'hi',
    })
    expect(yield* messagesPosted(stub)).toBe(1)
  }),
)

effectTest('allows /messages POST with type=private to self only (no non-self recipients)', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedMessageOk(stub, 3)
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.post('/messages', sentMessageSchema, {
      type: 'private',
      to: JSON.stringify([SELF_ID]),
      content: 'note to self',
    })
    expect(yield* messagesPosted(stub)).toBe(1)
  }),
)

effectTest('allows /messages POST with type=channel regardless of recipient lookup', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* seedMessageOk(stub, 4)
    let directoryCalls = 0
    const trackedDirectory = (): Effect.Effect<RecipientDirectory> => {
      directoryCalls += 1
      return directoryStub()
    }
    const http = wrapBotHttp(yield* buildHttp(stub), trackedDirectory, SELF_ID)
    yield* http.post('/messages', sentMessageSchema, {
      type: 'channel',
      to: 'general',
      topic: 'x',
      content: 'hi',
    })
    expect(yield* messagesPosted(stub)).toBe(1)
    expect(directoryCalls).toBe(0)
  }),
)

effectTest('non-/messages POSTs pass through unchanged', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/messages/123/reactions', { body: { result: 'success' } })
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.post('/messages/123/reactions', successSchema, { emoji_name: 'tada' })
    expect(yield* requestsTo(stub, 'POST', '/api/v1/messages/123/reactions')).toBe(1)
  }),
)

effectTest('GET requests pass through unchanged', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users', { body: { result: 'success', members: [] } })
    const usersResponseSchema = Schema.Struct({
      result: Schema.Literal('success'),
      members: Schema.Array(Schema.Unknown),
    })
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.get('/users', usersResponseSchema)
    expect(yield* requestsTo(stub, 'GET', '/api/v1/users')).toBe(1)
  }),
)

effectTest('DELETE requests pass through unchanged', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('DELETE', '/api/v1/messages/123/reactions', { body: { result: 'success' } })
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
    yield* http.delete('/messages/123/reactions', successSchema, { emoji_name: 'tada' })
    expect(yield* requestsTo(stub, 'DELETE', '/api/v1/messages/123/reactions')).toBe(1)
  }),
)

effectTest(
  'rejects /messages POST with type=private when `to` cannot be parsed as a user-id list',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: 'someone@example.com',
          content: 'hi',
        }),
      )
      expect(yield* messagesPosted(stub)).toBe(0)
    }),
)

effectTest(
  'rejects /messages POST with type=private when `to` is a JSON array of non-integers',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([1.5]),
          content: 'hi',
        }),
      )
      expect(yield* messagesPosted(stub)).toBe(0)
    }),
)

/**
 * `BotHttp` carries `uploadRaw` because an upload has to go out under the same
 * account that sends the referencing message — Zulip validates a message's
 * attachments against the SENDER (comms-qpup). Widening a security-adjacent
 * type is the move that gets accepted once and then cited for the next member,
 * so the reasoning is pinned here rather than left in a PR body: THE WALL THIS
 * WRAPPER ENFORCES IS A RECIPIENT RULE ON `POST /messages`, and an upload
 * addresses no recipient. A member that can address one does not belong here on
 * this precedent.
 *
 * One test, two assertions, and the second is the one that matters: the same
 * wrapped client that just uploaded still refuses a bot-to-bot direct message.
 */
effectTest('uploadRaw passes through, and the DM wall still stands on the same client', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: { url: '/user_uploads/2/ab/token/chart.svg', filename: 'chart.svg' },
    })
    const http = wrapBotHttp(yield* buildHttp(stub), directoryStub, SELF_ID)

    const uploaded = yield* http
      .uploadRaw('chart.svg', new Uint8Array([1, 2, 3]))
      .pipe(Effect.orDie)
    expect(uploaded.filename).toBe('chart.svg')
    expect(yield* requestsTo(stub, 'POST', '/api/v1/user_uploads')).toBe(1)

    yield* expectBotDirectMessageDefect(
      http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([OTHER_BOT_ID]),
        content: 'hi',
      }),
    )
    expect(yield* messagesPosted(stub)).toBe(0)
  }),
)
