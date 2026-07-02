/**
 * `ZulipHttp` request-shape and response-handling, exercised on the **owned-fake
 * stub HttpClient** — no `Bun.serve`, no real socket.
 *
 * The stub answers each request from a canned `(method, path)` registry and
 * captures the outgoing `HttpClientRequest` (serialized exactly as the wire
 * would see it), so request-shape assertions read the captured request and
 * response-handling assertions drive the canned response. Status codes, error
 * envelopes, non-JSON bodies, schema mismatches, the 429 retry policy and the
 * download/upload paths all run deterministically, off any socket.
 *
 * Two kinds of test deliberately stay off the stub:
 *
 *  - **Pure-unit tests** (the `rateLimitSchedule` replay, the `RealmUrl` /
 *    `BotEmail` / `ApiKey` brand validators, `decodeUserUploadPath`, the
 *    `ZulipApiError` tag) never touch HTTP at all — they keep their plain
 *    `Effect.runPromise` shape.
 *  - **One irreducible real-socket case** — `a transport failure surfaces as a
 *    ZulipApiError` — needs a genuine platform `RequestError` from a refused
 *    connection, which the in-memory stub cannot fabricate without lowering
 *    fidelity. It keeps a real `FetchHttpClient` against a claimed-then-released
 *    port.
 *
 * Success-body round-trips (GET success envelope, POST success body) are
 * exercised end-to-end against a live realm by the contract-against-real run
 * (`contract.live.test.ts`), so they are not re-asserted here.
 */

import { expect, test } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import {
  type CapturedHttpRequest,
  makeStubHttpClient,
  type StubHttpClient,
  type StubResponse,
} from '@commy/testing/stub-http-client'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import {
  Cause,
  Duration,
  Effect,
  Encoding,
  Exit,
  Fiber,
  Option,
  ParseResult,
  Schedule,
  Schema,
  TestClock,
  TestContext,
} from 'effect'
import type { ZulipHttp, ZulipHttpConfig } from './http.ts'
import {
  ApiKey,
  BotEmail,
  decodeUserUploadPath,
  decodeUserUploadPathSync,
  makeZulipHttp,
  RealmUrl,
  rateLimitSchedule,
  ZulipApiError,
} from './http.ts'

const REALM_URL = 'https://zulip.example.com'

const successSchema = Schema.Struct({ result: Schema.Literal('success') })

const userMeSchema = Schema.Struct({
  result: Schema.Literal('success'),
  user_id: Schema.Int,
  full_name: Schema.String,
})

const sentMessageSchema = Schema.Struct({
  result: Schema.Literal('success'),
  id: Schema.Int,
})

// Build a ZulipHttp wired to the stub: the port reads `HttpClient` from
// context, so we provide `stub.client` exactly where the application edge would
// provide the real `FetchHttpClient`.
const makeHttp = (
  stub: StubHttpClient,
  overrides: Partial<ZulipHttpConfig> = {},
): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    const base: ZulipHttpConfig = {
      realmUrl: yield* RealmUrl(REALM_URL),
      email: yield* BotEmail('bot@example.com'),
      apiKey: yield* ApiKey('sekret'),
    }
    return yield* makeZulipHttp({ ...base, ...overrides })
  }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client), Effect.orDie)

const firstRequest = (stub: StubHttpClient): Effect.Effect<CapturedHttpRequest> =>
  stub.captured.pipe(
    Effect.flatMap((reqs) => {
      const head = reqs[0]
      return head === undefined
        ? Effect.die(new Error('expected a captured request'))
        : Effect.succeed(head)
    }),
  )

// --- request shape: URL, auth, query, body encoding, host ---

effectTest('GET prepends /api/v1 to the supplied resource path', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: { result: 'success', user_id: 1, full_name: 'b' },
    })
    const http = yield* makeHttp(stub)
    yield* http.get('/users/me', userMeSchema)
    const req = yield* firstRequest(stub)
    expect(req.url.pathname).toBe('/api/v1/users/me')
  }),
)

effectTest('GET sends HTTP Basic auth with email:apiKey base64-encoded', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: { result: 'success', user_id: 1, full_name: 'b' },
    })
    const http = yield* makeHttp(stub)
    yield* http.get('/users/me', userMeSchema)
    const req = yield* firstRequest(stub)
    expect(req.headers.get('authorization')).toBe(
      `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
    )
  }),
)

effectTest('the Basic auth header base64-decodes back to email:apiKey (Encoding round-trip)', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: { result: 'success', user_id: 1, full_name: 'b' },
    })
    const http = yield* makeHttp(stub)
    yield* http.get('/users/me', userMeSchema)
    const req = yield* firstRequest(stub)
    const auth = req.headers.get('authorization')
    if (auth === null) throw new Error('expected an authorization header')
    const decoded = yield* Encoding.decodeBase64String(auth.slice('Basic '.length))
    expect(decoded).toBe('bot@example.com:sekret')
  }),
)

effectTest('GET appends params as URL-encoded query string', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/messages', { body: { result: 'success' } })
    const http = yield* makeHttp(stub)
    yield* http.get('/messages', successSchema, {
      anchor: 'newest',
      num_before: 50,
      apply_markdown: true,
      narrow: '[["stream","x"]]',
    })
    const req = yield* firstRequest(stub)
    expect(req.url.searchParams.get('anchor')).toBe('newest')
    expect(req.url.searchParams.get('num_before')).toBe('50')
    expect(req.url.searchParams.get('apply_markdown')).toBe('true')
    expect(req.url.searchParams.get('narrow')).toBe('[["stream","x"]]')
  }),
)

effectTest('POST form-encodes the body and sets the content-type', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/messages', { body: { result: 'success', id: 42 } })
    const http = yield* makeHttp(stub)
    yield* http.post('/messages', sentMessageSchema, {
      type: 'stream',
      to: 'general',
      topic: 'hello',
      content: 'hey there & friends',
    })
    const req = yield* firstRequest(stub)
    expect(req.method).toBe('POST')
    expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
    const params = new URLSearchParams(req.body)
    expect(params.get('type')).toBe('stream')
    expect(params.get('to')).toBe('general')
    expect(params.get('topic')).toBe('hello')
    expect(params.get('content')).toBe('hey there & friends')
  }),
)

effectTest('PATCH form-encodes the body and sets the content-type', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('PATCH', '/api/v1/messages/42', { body: { result: 'success' } })
    const http = yield* makeHttp(stub)
    yield* http.patch('/messages/42', successSchema, { content: 'edited body & more' })
    const req = yield* firstRequest(stub)
    expect(req.method).toBe('PATCH')
    expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
    const params = new URLSearchParams(req.body)
    expect(params.get('content')).toBe('edited body & more')
  }),
)

effectTest('DELETE with a body form-encodes it and sets content-type', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('DELETE', '/api/v1/messages/42/reactions', { body: { result: 'success' } })
    const http = yield* makeHttp(stub)
    yield* http.delete('/messages/42/reactions', successSchema, { emoji_name: 'thumbs_up' })
    const req = yield* firstRequest(stub)
    expect(req.method).toBe('DELETE')
    expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
    const params = new URLSearchParams(req.body)
    expect(params.get('emoji_name')).toBe('thumbs_up')
  }),
)

effectTest('DELETE without a body issues a bare DELETE with no content-type or body', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('DELETE', '/api/v1/users/me/queues/q-1', { body: { result: 'success' } })
    const http = yield* makeHttp(stub)
    yield* http.delete('/users/me/queues/q-1', successSchema)
    const req = yield* firstRequest(stub)
    expect(req.method).toBe('DELETE')
    expect(req.headers.get('content-type')).toBeNull()
    expect(req.body).toBe('')
  }),
)

effectTest('hostHeader overrides the Host header on outgoing requests', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
    const http = yield* makeHttp(stub, { hostHeader: 'zulip.example.com' })
    yield* http.get('/users/me', successSchema)
    const req = yield* firstRequest(stub)
    expect(req.headers.get('host')).toBe('zulip.example.com')
  }),
)

effectTest(
  'without hostHeader, no Host override is sent (the transport fills it at the wire)',
  () =>
    Effect.gen(function* () {
      // Our seam only injects a Host header when `hostHeader` is configured;
      // deriving Host from the URL is the transport's job at the socket, which
      // the contract-against-real run covers. The stub sends no socket, so the
      // observable contract here is the negative: we add no override.
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
      const http = yield* makeHttp(stub)
      yield* http.get('/users/me', successSchema)
      const req = yield* firstRequest(stub)
      expect(req.headers.get('host')).toBeNull()
    }),
)

effectTest('trailing slashes on the realm URL are normalised before /api/v1 is appended', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
    const realmUrl = yield* RealmUrl(`${REALM_URL}///`)
    const http = yield* makeHttp(stub, { realmUrl })
    yield* http.get('/users/me', successSchema)
    const req = yield* firstRequest(stub)
    expect(req.url.pathname).toBe('/api/v1/users/me')
    expect(req.url.href).not.toContain('//api')
  }),
)

// --- response handling: error envelopes, non-JSON, schema mismatch ---

effectTest('GET throws ZulipApiError with msg + code + status when result=error', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: { result: 'error', msg: 'Invalid API key', code: 'BAD_API_KEY' },
      status: 401,
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
    expect(err).toBeInstanceOf(ZulipApiError)
    const apiErr = err as ZulipApiError
    expect(apiErr.message).toContain('Invalid API key')
    expect(apiErr.code).toBe('BAD_API_KEY')
    expect(apiErr.status).toBe(401)
  }),
)

test('ZulipApiError carries Data.TaggedError discriminator for Effect.catchTag', () => {
  const err = new ZulipApiError({
    message: 'boom',
    status: 500,
    code: undefined,
    retryAfter: undefined,
  })
  expect(err._tag).toBe('ZulipApiError')
})

effectTest('GET throws ZulipApiError on non-JSON, non-2xx upstream', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: 'upstream blew up',
      status: 502,
      headers: { 'content-type': 'text/plain' },
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
    expect(err).toBeInstanceOf(ZulipApiError)
    expect((err as ZulipApiError).status).toBe(502)
  }),
)

effectTest(
  'GET throws ZulipApiError "non-JSON response" on a 2xx body that is not a JSON envelope',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', {
        body: 'totally not json',
        status: 200,
        headers: { 'content-type': 'text/plain' },
      })
      const http = yield* makeHttp(stub)
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      const apiErr = err as ZulipApiError
      expect(apiErr.message).toContain('non-JSON response')
      expect(apiErr.status).toBe(200)
      expect(apiErr.code).toBeUndefined()
    }),
)

effectTest(
  'GET throws ZulipApiError carrying the envelope msg/code when a non-error envelope arrives with a non-2xx status',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', {
        body: { result: 'success', msg: 'odd state', code: 'WAT' },
        status: 418,
      })
      const http = yield* makeHttp(stub)
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      const apiErr = err as ZulipApiError
      expect(apiErr.message).toBe('odd state')
      expect(apiErr.code).toBe('WAT')
      expect(apiErr.status).toBe(418)
    }),
)

effectTest('POST surfaces Zulip API errors the same way GET does', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/messages', {
      body: { result: 'error', msg: 'Topic too long', code: 'BAD_REQUEST' },
      status: 400,
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.post('/messages', sentMessageSchema, { content: 'x' }))
    expect(err).toBeInstanceOf(ZulipApiError)
    expect((err as ZulipApiError).code).toBe('BAD_REQUEST')
    expect((err as ZulipApiError).status).toBe(400)
  }),
)

effectTest('PATCH surfaces Zulip API errors the same way POST does', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('PATCH', '/api/v1/messages/42', {
      body: {
        result: 'error',
        msg: "You don't have permission to edit this message",
        code: 'BAD_REQUEST',
      },
      status: 400,
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.patch('/messages/42', successSchema, { content: 'x' }))
    expect(err).toBeInstanceOf(ZulipApiError)
    expect((err as ZulipApiError).status).toBe(400)
  }),
)

effectTest(
  'schema mismatch on a 200/result=success response surfaces a ParseError in the E channel',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', {
        body: { result: 'success', user_id: 'not a number', full_name: 'b' },
      })
      const http = yield* makeHttp(stub)
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(ParseResult.isParseError(err)).toBe(true)
    }),
)

test('a transport failure surfaces as a ZulipApiError that preserves the underlying cause', () =>
  // Irreducible real socket: a genuine platform
  // `RequestError` only arises from a real refused connection. Claim a port
  // then release it so the connection is refused — no mock, no in-memory stub
  // (which cannot fabricate a real RequestError without dropping fidelity).
  // This is the one case in this file that keeps `FetchHttpClient`.
  Effect.runPromise(
    Effect.gen(function* () {
      const deadServer = Bun.serve({ port: 0, fetch: () => new Response('') })
      const deadPort = deadServer.port
      yield* Effect.promise(() => deadServer.stop(true))
      const http = yield* makeZulipHttp({
        realmUrl: yield* RealmUrl(`http://localhost:${deadPort}`),
        email: yield* BotEmail('bot@example.com'),
        apiKey: yield* ApiKey('sekret'),
      })
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      const apiErr = err as ZulipApiError
      expect(apiErr.status).toBe(0)
      expect(apiErr.cause).toBeDefined()
      expect((apiErr.cause as { _tag?: unknown })._tag).toBe('RequestError')
    }).pipe(Effect.provide(FetchHttpClient.layer)),
  ))

// --- 429 rate-limit retry ---
//
// A 429 carries `retry-after` — backpressure with instructions, not a fatal
// error. The send path absorbs it: wait the realm's retry-after and retry
// within a bounded total wait budget, so callers never see a transient rate
// limit. retry-after stays adapter-internal; it never reaches the port.
//
// Two seams cover this without sleeping real time:
//   - policy: `Schedule.delays(rateLimitSchedule())` replayed over a list of
//     ZulipApiErrors observes the exact wait sequence, purely, no timers.
//   - end-to-end: drive the stub round-trip under `TestClock`. On the stub the
//     response is in-memory (no real socket off the test clock, unlike the old
//     Bun.serve fixture), so the retry sleeps run entirely on the virtual clock.

const rateLimited = (retryAfter?: number): StubResponse => ({
  body:
    retryAfter === undefined
      ? { result: 'error', code: 'RATE_LIMIT_HIT', msg: 'API rate limit exceeded' }
      : {
          result: 'error',
          code: 'RATE_LIMIT_HIT',
          msg: 'API rate limit exceeded',
          'retry-after': retryAfter,
        },
  status: 429,
})

const apiError = (status: number, retryAfter: number | undefined): ZulipApiError =>
  new ZulipApiError({ message: 'rate limited', status, code: 'RATE_LIMIT_HIT', retryAfter })

// Replay the retry policy over a list of errors and collect the wait it would
// pick before each retry. `Schedule.delays` reads the delay off each decision's
// interval, so no real (or virtual) time passes. A terminal (done) decision
// carries a zero-length interval, so the sequence ends in a trailing 0 once the
// policy stops retrying — dropped here so the result is just the non-zero waits
// the policy actually schedules.
const waitsFor = (errors: ReadonlyArray<ZulipApiError>): Effect.Effect<ReadonlyArray<number>> =>
  Schedule.run(Schedule.delays(rateLimitSchedule()), 0, errors).pipe(
    Effect.map((chunk) =>
      Array.from(chunk)
        .map(Duration.toMillis)
        .filter((ms) => ms > 0),
    ),
  )

test('the retry policy honours the realm retry-after as its wait', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const waits = yield* waitsFor([apiError(429, 0.25)])
      expect(waits).toEqual([250])
    }),
  ))

test('the retry policy falls back to a default wait when retry-after is absent', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const waits = yield* waitsFor([apiError(429, undefined)])
      expect(waits).toEqual([1000])
    }),
  ))

test('the retry policy clamps a single oversized retry-after to the remaining budget', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const waits = yield* waitsFor([apiError(429, 600)])
      expect(waits).toEqual([15_000])
    }),
  ))

test('the retry policy spends the budget in equal waits then stops', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // Each 5s 429 is honoured until the 15s budget is exhausted: three 5s
      // waits, then the schedule is done (no fourth wait).
      const waits = yield* waitsFor([
        apiError(429, 5),
        apiError(429, 5),
        apiError(429, 5),
        apiError(429, 5),
      ])
      expect(waits).toEqual([5000, 5000, 5000])
    }),
  ))

test('the retry policy emits no wait for a non-429 error', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const waits = yield* waitsFor([apiError(400, undefined)])
      expect(waits).toEqual([])
    }),
  ))

// Advance the virtual clock until the forked request settles. Each retry sleep
// is only registered once its 429 response lands, so a single adjust can race
// the not-yet-scheduled next sleep; advance-poll-yield in a loop releases each
// retry sleep as it appears. Unlike the old `runUnderTestClock`, the stub
// round-trip is fully in-memory — there is no real socket I/O off the test
// clock, so this is a deterministic settle loop, not a race against the wire.
const settleUnderTestClock = <A, E>(
  build: Effect.Effect<A, E>,
  step: Duration.DurationInput,
  maxAdvances: number,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(build)
    for (let i = 0; i < maxAdvances; i++) {
      const settled = yield* Fiber.poll(fiber)
      if (Option.isSome(settled)) break
      yield* TestClock.adjust(step)
      yield* Effect.yieldNow()
    }
    return yield* Fiber.join(fiber)
  })

effectTest(
  'GET retries after a 429 (under TestClock) and returns the success body',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respondSequence('GET', '/api/v1/users/me', [
        rateLimited(0.25),
        { body: { result: 'success', user_id: 5, full_name: 'bot' } },
      ])
      const http = yield* makeHttp(stub)
      const body = yield* settleUnderTestClock(
        http.get('/users/me', userMeSchema),
        Duration.millis(250),
        4,
      )
      expect(body.user_id).toBe(5)
      expect((yield* stub.captured).length).toBe(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'GET gives up and throws the 429 once the retry budget is spent',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', rateLimited(5))
      const http = yield* makeHttp(stub)
      const err = yield* settleUnderTestClock(
        Effect.flip(http.get('/users/me', userMeSchema)),
        Duration.seconds(5),
        6,
      )
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).status).toBe(429)
      // Initial attempt + three retries (3 × 5s = 15s budget).
      expect((yield* stub.captured).length).toBe(4)
    }),
  { layer: TestContext.TestContext },
)

effectTest(
  'POST retries on 429 too — the retry lives in the shared send path',
  () =>
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respondSequence('POST', '/api/v1/messages', [
        rateLimited(0.01),
        { body: { result: 'success', id: 7 } },
      ])
      const http = yield* makeHttp(stub)
      const body = yield* settleUnderTestClock(
        http.post('/messages', sentMessageSchema, { content: 'x' }),
        Duration.millis(100),
        4,
      )
      expect(body.id).toBe(7)
      expect((yield* stub.captured).length).toBe(2)
    }),
  { layer: TestContext.TestContext },
)

effectTest('non-429 errors are surfaced immediately without retry', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/api/v1/users/me', {
      body: { result: 'error', code: 'BAD_REQUEST', msg: 'nope' },
      status: 400,
    })
    const http = yield* makeHttp(stub)
    yield* Effect.ignore(http.get('/users/me', userMeSchema))
    expect((yield* stub.captured).length).toBe(1)
  }),
)

// --- brand validation (no HTTP) ---

test('RealmUrl rejects non-URL strings', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(RealmUrl('not a url'))
      expect(err).toBeInstanceOf(ParseResult.ParseError)
    }),
  ))

test('RealmUrl rejects non-http(s) schemes', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(RealmUrl('ftp://zulip.example.com'))
      expect(err).toBeInstanceOf(ParseResult.ParseError)
    }),
  ))

test('RealmUrl preserves the original string verbatim (no URL normalisation)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const raw = 'https://zulip.example.com'
      const realmUrl = yield* RealmUrl(raw)
      expect(realmUrl as string).toBe(raw)
    }),
  ))

test('BotEmail rejects strings without an @', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(BotEmail('not-an-email'))
      expect(err).toBeInstanceOf(ParseResult.ParseError)
    }),
  ))

test('ApiKey rejects empty strings', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(ApiKey(''))
      expect(err).toBeInstanceOf(ParseResult.ParseError)
    }),
  ))

// A path not starting with '/' is a programmer error, so it surfaces as a
// TypeError defect in the Effect channel. No request is sent, so these run on the
// stub with no canned response registered.
const expectPathDefect = <A, E>(eff: Effect.Effect<A, E>): Effect.Effect<void> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(eff)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const defect = Cause.dieOption(exit.cause)
      expect(Option.isSome(defect)).toBe(true)
      expect(Option.getOrThrow(defect)).toBeInstanceOf(TypeError)
    }
  })

effectTest('GET fails with a TypeError defect when path does not start with /', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = yield* makeHttp(stub)
    yield* expectPathDefect(http.get('users/me', successSchema))
  }),
)

effectTest('POST fails with a TypeError defect when path does not start with /', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = yield* makeHttp(stub)
    yield* expectPathDefect(http.post('messages', successSchema, { content: 'x' }))
  }),
)

effectTest('DELETE fails with a TypeError defect when path does not start with /', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = yield* makeHttp(stub)
    yield* expectPathDefect(http.delete('messages/42/reactions', successSchema))
  }),
)

// --- downloadRaw ---

effectTest('downloadRaw resolves path against realm root, not /api/v1', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/2/56/image.jpeg', {
      body: new Uint8Array([0xff, 0xd8, 0xff, 0xe0]),
      headers: { 'content-type': 'image/jpeg' },
    })
    const http = yield* makeHttp(stub)
    yield* http.downloadRaw('/user_uploads/2/56/image.jpeg')
    const req = yield* firstRequest(stub)
    expect(req.url.pathname).toBe('/user_uploads/2/56/image.jpeg')
    expect(req.url.href).not.toContain('/api/v1')
  }),
)

effectTest('downloadRaw sends HTTP Basic auth header', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/1/abc/photo.png', {
      body: new Uint8Array([0x89, 0x50, 0x4e, 0x47]),
      headers: { 'content-type': 'image/png' },
    })
    const http = yield* makeHttp(stub)
    yield* http.downloadRaw('/user_uploads/1/abc/photo.png')
    const req = yield* firstRequest(stub)
    expect(req.headers.get('authorization')).toBe(
      `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
    )
  }),
)

effectTest('downloadRaw returns raw bytes and content-type from the response', () =>
  Effect.gen(function* () {
    const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a])
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/1/abc/photo.png', {
      body: bytes,
      headers: { 'content-type': 'image/png' },
    })
    const http = yield* makeHttp(stub)
    const result = yield* http.downloadRaw('/user_uploads/1/abc/photo.png')
    expect(new Uint8Array(result.data)).toEqual(bytes)
    expect(result.contentType).toBe('image/png')
  }),
)

effectTest('downloadRaw throws ZulipApiError on non-2xx response', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/1/abc/missing.png', {
      body: 'Not found',
      status: 404,
      headers: { 'content-type': 'text/plain' },
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.downloadRaw('/user_uploads/1/abc/missing.png'))
    expect(err).toBeInstanceOf(ZulipApiError)
    expect((err as ZulipApiError).status).toBe(404)
  }),
)

effectTest('downloadRaw fails with a TypeError defect when path does not start with /', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    const http = yield* makeHttp(stub)
    yield* expectPathDefect(http.downloadRaw('user_uploads/1/abc/photo.png'))
  }),
)

effectTest('downloadRaw sends host header when configured', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/1/a/f.bin', {
      body: new Uint8Array([0x00]),
      headers: { 'content-type': 'application/octet-stream' },
    })
    const http = yield* makeHttp(stub, { hostHeader: 'zulip.example.com' })
    yield* http.downloadRaw('/user_uploads/1/a/f.bin')
    const req = yield* firstRequest(stub)
    expect(req.headers.get('host')).toBe('zulip.example.com')
  }),
)

effectTest('downloadRaw uses GET method', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('GET', '/user_uploads/1/a/f.bin', {
      body: new Uint8Array([0x00]),
      headers: { 'content-type': 'application/octet-stream' },
    })
    const http = yield* makeHttp(stub)
    yield* http.downloadRaw('/user_uploads/1/a/f.bin')
    const req = yield* firstRequest(stub)
    expect(req.method).toBe('GET')
  }),
)

// --- uploadRaw ---

const uploadSuccess = (urlPath: string, filename: string) => ({
  result: 'success',
  msg: '',
  uri: urlPath,
  url: urlPath,
  filename,
})

effectTest('uploadRaw POSTs multipart form-data to /api/v1/user_uploads', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: uploadSuccess('/user_uploads/1/ab/chart.png', 'chart.png'),
    })
    const http = yield* makeHttp(stub)
    yield* http.uploadRaw('chart.png', new Uint8Array([1, 2, 3]))
    const req = yield* firstRequest(stub)
    expect(req.url.pathname).toBe('/api/v1/user_uploads')
    expect(req.method).toBe('POST')
    expect(req.headers.get('content-type')).toMatch(/^multipart\/form-data/)
  }),
)

effectTest('uploadRaw includes the file bytes and filename in the multipart body', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: uploadSuccess('/user_uploads/1/ab/notes.txt', 'notes.txt'),
    })
    const http = yield* makeHttp(stub)
    yield* http.uploadRaw('notes.txt', new TextEncoder().encode('hello upload'))
    const req = yield* firstRequest(stub)
    expect(req.body).toContain('filename="notes.txt"')
    expect(req.body).toContain('hello upload')
  }),
)

effectTest('uploadRaw returns the canonical url and filename from the response', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: uploadSuccess('/user_uploads/1/ab/chart.png', 'chart.png'),
    })
    const http = yield* makeHttp(stub)
    const result = yield* http.uploadRaw('chart.png', new Uint8Array([0x89, 0x50]))
    expect(result.url).toBe(decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'))
    expect(result.filename).toBe('chart.png')
  }),
)

effectTest('uploadRaw sends HTTP Basic auth header', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: uploadSuccess('/user_uploads/1/a/f.bin', 'f.bin'),
    })
    const http = yield* makeHttp(stub)
    yield* http.uploadRaw('f.bin', new Uint8Array([0]))
    const req = yield* firstRequest(stub)
    expect(req.headers.get('authorization')).toBe(
      `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
    )
  }),
)

effectTest('uploadRaw surfaces Zulip API errors the same way other verbs do', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: {
        result: 'error',
        msg: 'File is larger than the maximum upload size',
        code: 'BAD_REQUEST',
      },
      status: 400,
    })
    const http = yield* makeHttp(stub)
    const err = yield* Effect.flip(http.uploadRaw('big.bin', new Uint8Array([0])))
    expect(err).toBeInstanceOf(ZulipApiError)
    expect((err as ZulipApiError).status).toBe(400)
  }),
)

effectTest('uploadRaw sends host header when configured', () =>
  Effect.gen(function* () {
    const stub = yield* makeStubHttpClient
    yield* stub.respond('POST', '/api/v1/user_uploads', {
      body: uploadSuccess('/user_uploads/1/a/f.bin', 'f.bin'),
    })
    const http = yield* makeHttp(stub, { hostHeader: 'zulip.example.com' })
    yield* http.uploadRaw('f.bin', new Uint8Array([0]))
    const req = yield* firstRequest(stub)
    expect(req.headers.get('host')).toBe('zulip.example.com')
  }),
)

// --- decodeUserUploadPath ---

test('decodeUserUploadPath succeeds on a /user_uploads/ path', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const path = yield* decodeUserUploadPath('/user_uploads/abc/def.png')
      expect(path).toBe(decodeUserUploadPathSync('/user_uploads/abc/def.png'))
    }),
  ))

test('decodeUserUploadPath fails with a ParseError on a path not starting with /user_uploads/', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(decodeUserUploadPath('/api/v1/messages'))
      expect(ParseResult.isParseError(err)).toBe(true)
    }),
  ))

test('decodeUserUploadPath fails with a ParseError on the empty string', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const err = yield* Effect.flip(decodeUserUploadPath(''))
      expect(ParseResult.isParseError(err)).toBe(true)
    }),
  ))
