import { afterEach, beforeEach, expect, test } from 'bun:test'
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

type Captured = {
  readonly url: string
  readonly method: string
  readonly headers: Headers
  readonly body: string
}

type FixtureResponseInit = {
  readonly status?: number
  readonly statusText?: string
  readonly headers?: Readonly<Record<string, string>>
}

type FixtureResponse = {
  readonly body: unknown
  readonly init?: FixtureResponseInit
}

type Fixture = {
  readonly port: number
  readonly captured: ReadonlyArray<Captured>
  readonly respond: (body: unknown, init?: FixtureResponseInit) => void
  /**
   * Queue responses consumed one-per-request before falling back to the
   * `respond` default. Lets a test drive a 429-then-success sequence to
   * exercise the send layer's rate-limit retry.
   */
  readonly respondSequence: (responses: ReadonlyArray<FixtureResponse>) => void
  readonly stop: () => Promise<void>
}

const startFixture = (): Fixture => {
  const captured: Captured[] = []
  let respondWith: FixtureResponse = { body: { result: 'success', msg: '' } }
  const queue: FixtureResponse[] = []
  const server = Bun.serve({
    port: 0,
    fetch: async (req) => {
      captured.push({
        url: req.url,
        method: req.method,
        headers: new Headers(req.headers),
        body: await req.text(),
      })
      const responder = queue.length > 0 ? (queue.shift() as FixtureResponse) : respondWith
      const init = responder.init
      const headers: Record<string, string> = {}
      if (!(responder.body instanceof Uint8Array)) {
        headers['content-type'] = 'application/json'
      }
      if (init?.headers !== undefined) {
        for (const [k, v] of Object.entries(init.headers)) {
          headers[k] = v
        }
      }
      const responseBody =
        responder.body instanceof Uint8Array
          ? responder.body
          : typeof responder.body === 'string'
            ? responder.body
            : JSON.stringify(responder.body)
      const responseInit: ResponseInit = { status: init?.status ?? 200, headers }
      if (init?.statusText !== undefined) {
        Object.assign(responseInit, { statusText: init.statusText })
      }
      return new Response(responseBody, responseInit)
    },
  })
  if (typeof server.port !== 'number') {
    throw new Error('fixture server failed to bind a TCP port')
  }
  return {
    port: server.port,
    captured,
    respond: (body, init) => {
      respondWith = init === undefined ? { body } : { body, init }
    },
    respondSequence: (responses) => {
      queue.length = 0
      queue.push(...responses)
    },
    stop: async () => {
      await server.stop(true)
    },
  }
}

let fixture: Fixture

beforeEach(() => {
  fixture = startFixture()
})

afterEach(async () => {
  await fixture.stop()
})

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

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

const makeHttp = (overrides: Partial<ZulipHttpConfig> = {}): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    const base: ZulipHttpConfig = {
      realmUrl: yield* RealmUrl(`http://localhost:${fixture.port}`),
      email: yield* BotEmail('bot@example.com'),
      apiKey: yield* ApiKey('sekret'),
    }
    return yield* makeZulipHttp({ ...base, ...overrides })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

test('GET prepends /api/v1 to the supplied resource path', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', user_id: 1, full_name: 'b' })
      const http = yield* makeHttp()
      yield* http.get('/users/me', userMeSchema)
      expect(fixture.captured[0]?.url).toMatch(/\/api\/v1\/users\/me$/)
    }),
  ))

test('GET sends HTTP Basic auth with email:apiKey base64-encoded', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', user_id: 1, full_name: 'b' })
      const http = yield* makeHttp()
      yield* http.get('/users/me', userMeSchema)
      expect(fixture.captured[0]?.headers.get('authorization')).toBe(
        `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
      )
    }),
  ))

test('the Basic auth header base64-decodes back to email:apiKey (Encoding round-trip)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', user_id: 1, full_name: 'b' })
      const http = yield* makeHttp()
      yield* http.get('/users/me', userMeSchema)
      const auth = fixture.captured[0]?.headers.get('authorization')
      if (auth === undefined || auth === null) throw new Error('expected an authorization header')
      const decoded = yield* Encoding.decodeBase64String(auth.slice('Basic '.length))
      expect(decoded).toBe('bot@example.com:sekret')
    }),
  ))

test('GET parses the JSON envelope through the supplied schema', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', user_id: 7, full_name: 'bot' })
      const http = yield* makeHttp()
      const body = yield* http.get('/users/me', userMeSchema)
      expect(body.user_id).toBe(7)
      expect(body.full_name).toBe('bot')
    }),
  ))

test('GET throws ZulipApiError with msg + code + status when result=error', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(
        { result: 'error', msg: 'Invalid API key', code: 'BAD_API_KEY' },
        { status: 401 },
      )
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      const apiErr = err as ZulipApiError
      expect(apiErr.message).toContain('Invalid API key')
      expect(apiErr.code).toBe('BAD_API_KEY')
      expect(apiErr.status).toBe(401)
    }),
  ))

test('ZulipApiError carries Data.TaggedError discriminator for Effect.catchTag', () => {
  const err = new ZulipApiError({
    message: 'boom',
    status: 500,
    code: undefined,
    retryAfter: undefined,
  })
  expect(err._tag).toBe('ZulipApiError')
})

test('GET throws ZulipApiError on non-JSON, non-2xx upstream', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond('upstream blew up', {
        status: 502,
        headers: { 'content-type': 'text/plain' },
      })
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).status).toBe(502)
    }),
  ))

test('GET appends params as URL-encoded query string', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp()
      yield* http.get('/messages', successSchema, {
        anchor: 'newest',
        num_before: 50,
        apply_markdown: true,
        narrow: '[["stream","x"]]',
      })
      const req = fixture.captured[0]
      if (req === undefined) throw new Error('expected captured request')
      const url = new URL(req.url)
      expect(url.searchParams.get('anchor')).toBe('newest')
      expect(url.searchParams.get('num_before')).toBe('50')
      expect(url.searchParams.get('apply_markdown')).toBe('true')
      expect(url.searchParams.get('narrow')).toBe('[["stream","x"]]')
    }),
  ))

test('POST form-encodes the body and sets the content-type', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', id: 42 })
      const http = yield* makeHttp()
      yield* http.post('/messages', sentMessageSchema, {
        type: 'stream',
        to: 'general',
        topic: 'hello',
        content: 'hey there & friends',
      })
      const req = fixture.captured[0]
      if (req === undefined) throw new Error('expected captured request')
      expect(req.method).toBe('POST')
      expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
      const params = new URLSearchParams(req.body)
      expect(params.get('type')).toBe('stream')
      expect(params.get('to')).toBe('general')
      expect(params.get('topic')).toBe('hello')
      expect(params.get('content')).toBe('hey there & friends')
    }),
  ))

test('POST returns the parsed JSON body when result=success', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', id: 99 })
      const http = yield* makeHttp()
      const body = yield* http.post('/messages', sentMessageSchema, { content: 'x' })
      expect(body.id).toBe(99)
    }),
  ))

test('POST surfaces Zulip API errors the same way GET does', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(
        { result: 'error', msg: 'Topic too long', code: 'BAD_REQUEST' },
        { status: 400 },
      )
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.post('/messages', sentMessageSchema, { content: 'x' }))
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).code).toBe('BAD_REQUEST')
      expect((err as ZulipApiError).status).toBe(400)
    }),
  ))

test('PATCH form-encodes the body and sets the content-type', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp()
      yield* http.patch('/messages/42', successSchema, {
        content: 'edited body & more',
      })
      const req = fixture.captured[0]
      if (req === undefined) throw new Error('expected captured request')
      expect(req.method).toBe('PATCH')
      expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
      const params = new URLSearchParams(req.body)
      expect(params.get('content')).toBe('edited body & more')
    }),
  ))

test('PATCH surfaces Zulip API errors the same way POST does', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(
        {
          result: 'error',
          msg: "You don't have permission to edit this message",
          code: 'BAD_REQUEST',
        },
        { status: 400 },
      )
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.patch('/messages/42', successSchema, { content: 'x' }))
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).status).toBe(400)
    }),
  ))

test('DELETE with a body form-encodes it and sets content-type', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp()
      yield* http.delete('/messages/42/reactions', successSchema, {
        emoji_name: 'thumbs_up',
      })
      const req = fixture.captured[0]
      if (req === undefined) throw new Error('expected captured request')
      expect(req.method).toBe('DELETE')
      expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
      const params = new URLSearchParams(req.body)
      expect(params.get('emoji_name')).toBe('thumbs_up')
    }),
  ))

test('DELETE without a body issues a bare DELETE with no content-type or body', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp()
      yield* http.delete('/users/me/queues/q-1', successSchema)
      const req = fixture.captured[0]
      if (req === undefined) throw new Error('expected captured request')
      expect(req.method).toBe('DELETE')
      expect(req.headers.get('content-type')).toBeNull()
      expect(req.body).toBe('')
    }),
  ))

test('hostHeader overrides the Host header on outgoing requests', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp({ hostHeader: 'zulip.example.com' })
      yield* http.get('/users/me', successSchema)
      expect(fixture.captured[0]?.headers.get('host')).toBe('zulip.example.com')
    }),
  ))

test('without hostHeader, Host header matches the URL the wrapper was built from', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const http = yield* makeHttp()
      yield* http.get('/users/me', successSchema)
      expect(fixture.captured[0]?.headers.get('host')).toBe(`localhost:${fixture.port}`)
    }),
  ))

test('schema mismatch on a 200/result=success response surfaces a ParseError in the E channel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success', user_id: 'not a number', full_name: 'b' })
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(ParseResult.isParseError(err)).toBe(true)
    }),
  ))

test('a transport failure surfaces as a ZulipApiError that preserves the underlying cause', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // Claim a port then immediately release it so the connection is
      // refused — a real RequestError from the platform HttpClient, no mock.
      const deadServer = Bun.serve({ port: 0, fetch: () => new Response('') })
      const deadPort = deadServer.port
      yield* Effect.promise(() => deadServer.stop(true))
      const realmUrl = yield* RealmUrl(`http://localhost:${deadPort}`)
      const email = yield* BotEmail('bot@example.com')
      const apiKey = yield* ApiKey('sekret')
      const http = yield* makeZulipHttp({ realmUrl, email, apiKey }).pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      )

      const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
      expect(err).toBeInstanceOf(ZulipApiError)
      const apiErr = err as ZulipApiError
      expect(apiErr.status).toBe(0)
      expect(apiErr.cause).toBeDefined()
      expect((apiErr.cause as { _tag?: unknown })._tag).toBe('RequestError')
    }),
  ))

// --- 429 rate-limit retry (comms-nbz) ---
//
// A 429 carries `retry-after` — backpressure with instructions, not a
// fatal error. The send path absorbs it: wait the realm's retry-after and
// retry within a bounded total wait budget, so callers never see a
// transient rate limit. retry-after stays adapter-internal; it never
// reaches the port.
//
// The wait now rides on `Effect.sleep` (Clock default service) inside the
// retry Schedule, so two test seams replace the old injected `sleep` thunk:
//   - policy: `Schedule.delays(rateLimitSchedule())` replayed over a list
//     of ZulipApiErrors observes the exact wait sequence, purely, no timers
//     (same approach as the event-pump's `defaultRetrySchedule` test).
//   - end-to-end: fork the request and drive `TestClock` so the real
//     fixture round-trip retries without sleeping real time.

const rateLimited = (retryAfter?: number): FixtureResponse => ({
  body:
    retryAfter === undefined
      ? { result: 'error', code: 'RATE_LIMIT_HIT', msg: 'API rate limit exceeded' }
      : {
          result: 'error',
          code: 'RATE_LIMIT_HIT',
          msg: 'API rate limit exceeded',
          'retry-after': retryAfter,
        },
  init: { status: 429 },
})

const apiError = (status: number, retryAfter: number | undefined): ZulipApiError =>
  new ZulipApiError({ message: 'rate limited', status, code: 'RATE_LIMIT_HIT', retryAfter })

// Replay the retry policy over a list of errors and collect the wait it
// would pick before each retry. `Schedule.delays` reads the delay off each
// decision's interval, so no real (or virtual) time passes. A terminal
// (done) decision carries a zero-length interval, so the sequence ends in a
// trailing 0 once the policy stops retrying — dropped here so the result is
// just the non-zero waits the policy actually schedules.
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
      // Each 5s 429 is honoured until the 15s budget is exhausted: three
      // 5s waits, then the schedule is done (no fourth wait).
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

// Fork the request, then repeatedly advance the virtual clock until the
// fiber settles. The fixture round-trip is real socket I/O (off the test
// clock), so a single adjust can race the scheduled retry sleep: the sleep
// is only registered once the real 429 response lands. Looping — adjust,
// yield, poll — releases each retry sleep as it appears without sleeping
// real time, the same shape as the event-pump's drainOneUnderTestClock.
const runUnderTestClock = <A, E>(
  build: Effect.Effect<A, E, HttpClient.HttpClient>,
  step: Duration.DurationInput,
  maxAdvances: number,
): Promise<A> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(build)
    for (let i = 0; i < maxAdvances; i++) {
      const settled = yield* Fiber.poll(fiber)
      if (settled._tag === 'Some') break
      yield* TestClock.adjust(step)
      yield* Effect.yieldNow()
    }
    return yield* Fiber.join(fiber)
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, httpClient),
    Effect.provide(TestContext.TestContext),
    Effect.runPromise,
  )

const withCreds = <A, E, R>(
  use: (http: ZulipHttp) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | ParseResult.ParseError, R | HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const http = yield* makeZulipHttp({
      realmUrl: yield* RealmUrl(`http://localhost:${fixture.port}`),
      email: yield* BotEmail('bot@example.com'),
      apiKey: yield* ApiKey('sekret'),
    })
    return yield* use(http)
  })

test('GET retries after a 429 (under TestClock) and returns the success body', () => {
  fixture.respond({ result: 'success', user_id: 5, full_name: 'bot' })
  fixture.respondSequence([rateLimited(0.25)])
  return runUnderTestClock(
    withCreds((http) =>
      Effect.gen(function* () {
        const body = yield* http.get('/users/me', userMeSchema)
        expect(body.user_id).toBe(5)
        expect(fixture.captured.length).toBe(2)
        return body
      }),
    ),
    Duration.millis(250),
    4,
  )
})

test('GET gives up and throws the 429 once the retry budget is spent', () => {
  fixture.respond(rateLimited(5).body, { status: 429 })
  return runUnderTestClock(
    withCreds((http) =>
      Effect.gen(function* () {
        const err = yield* Effect.flip(http.get('/users/me', userMeSchema))
        expect(err).toBeInstanceOf(ZulipApiError)
        expect((err as ZulipApiError).status).toBe(429)
        // Initial attempt + three retries (3 × 5s = 15s budget).
        expect(fixture.captured.length).toBe(4)
        return err
      }),
    ),
    Duration.seconds(5),
    6,
  )
})

test('POST retries on 429 too — the retry lives in the shared send path', () => {
  fixture.respond({ result: 'success', id: 7 })
  fixture.respondSequence([rateLimited(0.01)])
  return runUnderTestClock(
    withCreds((http) =>
      Effect.gen(function* () {
        const body = yield* http.post('/messages', sentMessageSchema, { content: 'x' })
        expect(body.id).toBe(7)
        expect(fixture.captured.length).toBe(2)
        return body
      }),
    ),
    Duration.millis(100),
    4,
  )
})

test('non-429 errors are surfaced immediately without retry', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'error', code: 'BAD_REQUEST', msg: 'nope' }, { status: 400 })
      const http = yield* makeHttp()
      yield* Effect.ignore(http.get('/users/me', userMeSchema))
      expect(fixture.captured.length).toBe(1)
    }),
  ))

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
// TypeError defect in the Effect channel (comms-0m8) — no longer a synchronous
// throw from an Effect-returning verb.
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

test('GET fails with a TypeError defect when path does not start with /', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = yield* makeHttp()
      yield* expectPathDefect(http.get('users/me', successSchema))
    }),
  ))

test('POST fails with a TypeError defect when path does not start with /', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = yield* makeHttp()
      yield* expectPathDefect(http.post('messages', successSchema, { content: 'x' }))
    }),
  ))

test('DELETE fails with a TypeError defect when path does not start with /', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = yield* makeHttp()
      yield* expectPathDefect(http.delete('messages/42/reactions', successSchema))
    }),
  ))

test('trailing slashes on the realm URL are normalised before /api/v1 is appended', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond({ result: 'success' })
      const realmUrl = yield* RealmUrl(`http://localhost:${fixture.port}///`)
      const email = yield* BotEmail('bot@example.com')
      const apiKey = yield* ApiKey('sekret')
      const httpWithTrailing = yield* makeZulipHttp({ realmUrl, email, apiKey }).pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      )
      yield* httpWithTrailing.get('/users/me', successSchema)
      expect(fixture.captured[0]?.url).toMatch(/\/api\/v1\/users\/me$/)
      expect(fixture.captured[0]?.url).not.toContain('//api')
    }).pipe(Effect.orDie),
  ))

// --- downloadRaw (comms-xos) ---

test('downloadRaw resolves path against realm root, not /api/v1', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bytes = new Uint8Array([0xff, 0xd8, 0xff, 0xe0])
      fixture.respond(bytes, { headers: { 'content-type': 'image/jpeg' } })
      const http = yield* makeHttp()
      yield* http.downloadRaw('/user_uploads/2/56/image.jpeg')
      expect(fixture.captured[0]?.url).toMatch(/\/user_uploads\/2\/56\/image\.jpeg$/)
      expect(fixture.captured[0]?.url).not.toContain('/api/v1')
    }),
  ))

test('downloadRaw sends HTTP Basic auth header', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47])
      fixture.respond(bytes, { headers: { 'content-type': 'image/png' } })
      const http = yield* makeHttp()
      yield* http.downloadRaw('/user_uploads/1/abc/photo.png')
      expect(fixture.captured[0]?.headers.get('authorization')).toBe(
        `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
      )
    }),
  ))

test('downloadRaw returns raw bytes and content-type from the response', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a])
      fixture.respond(bytes, { headers: { 'content-type': 'image/png' } })
      const http = yield* makeHttp()
      const result = yield* http.downloadRaw('/user_uploads/1/abc/photo.png')
      expect(new Uint8Array(result.data)).toEqual(bytes)
      expect(result.contentType).toBe('image/png')
    }),
  ))

test('downloadRaw throws ZulipApiError on non-2xx response', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond('Not found', { status: 404, headers: { 'content-type': 'text/plain' } })
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.downloadRaw('/user_uploads/1/abc/missing.png'))
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).status).toBe(404)
    }),
  ))

test('downloadRaw fails with a TypeError defect when path does not start with /', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = yield* makeHttp()
      yield* expectPathDefect(http.downloadRaw('user_uploads/1/abc/photo.png'))
    }),
  ))

test('downloadRaw sends host header when configured', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bytes = new Uint8Array([0x00])
      fixture.respond(bytes, { headers: { 'content-type': 'application/octet-stream' } })
      const http = yield* makeHttp({ hostHeader: 'zulip.example.com' })
      yield* http.downloadRaw('/user_uploads/1/a/f.bin')
      expect(fixture.captured[0]?.headers.get('host')).toBe('zulip.example.com')
    }),
  ))

test('downloadRaw uses GET method', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bytes = new Uint8Array([0x00])
      fixture.respond(bytes, { headers: { 'content-type': 'application/octet-stream' } })
      const http = yield* makeHttp()
      yield* http.downloadRaw('/user_uploads/1/a/f.bin')
      expect(fixture.captured[0]?.method).toBe('GET')
    }),
  ))

// --- uploadRaw (comms-nsa) ---

const uploadSuccess = (urlPath: string, filename: string) => ({
  result: 'success',
  msg: '',
  uri: urlPath,
  url: urlPath,
  filename,
})

test('uploadRaw POSTs multipart form-data to /api/v1/user_uploads', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(uploadSuccess('/user_uploads/1/ab/chart.png', 'chart.png'))
      const http = yield* makeHttp()
      yield* http.uploadRaw('chart.png', new Uint8Array([1, 2, 3]))
      expect(fixture.captured[0]?.url).toMatch(/\/api\/v1\/user_uploads$/)
      expect(fixture.captured[0]?.method).toBe('POST')
      expect(fixture.captured[0]?.headers.get('content-type')).toMatch(/^multipart\/form-data/)
    }),
  ))

test('uploadRaw includes the file bytes and filename in the multipart body', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(uploadSuccess('/user_uploads/1/ab/notes.txt', 'notes.txt'))
      const http = yield* makeHttp()
      yield* http.uploadRaw('notes.txt', new TextEncoder().encode('hello upload'))
      expect(fixture.captured[0]?.body).toContain('filename="notes.txt"')
      expect(fixture.captured[0]?.body).toContain('hello upload')
    }),
  ))

test('uploadRaw returns the canonical url and filename from the response', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(uploadSuccess('/user_uploads/1/ab/chart.png', 'chart.png'))
      const http = yield* makeHttp()
      const result = yield* http.uploadRaw('chart.png', new Uint8Array([0x89, 0x50]))
      expect(result.url).toBe(decodeUserUploadPathSync('/user_uploads/1/ab/chart.png'))
      expect(result.filename).toBe('chart.png')
    }),
  ))

test('uploadRaw sends HTTP Basic auth header', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(uploadSuccess('/user_uploads/1/a/f.bin', 'f.bin'))
      const http = yield* makeHttp()
      yield* http.uploadRaw('f.bin', new Uint8Array([0]))
      expect(fixture.captured[0]?.headers.get('authorization')).toBe(
        `Basic ${Encoding.encodeBase64('bot@example.com:sekret')}`,
      )
    }),
  ))

test('uploadRaw surfaces Zulip API errors the same way other verbs do', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(
        {
          result: 'error',
          msg: 'File is larger than the maximum upload size',
          code: 'BAD_REQUEST',
        },
        { status: 400 },
      )
      const http = yield* makeHttp()
      const err = yield* Effect.flip(http.uploadRaw('big.bin', new Uint8Array([0])))
      expect(err).toBeInstanceOf(ZulipApiError)
      expect((err as ZulipApiError).status).toBe(400)
    }),
  ))

test('uploadRaw sends host header when configured', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      fixture.respond(uploadSuccess('/user_uploads/1/a/f.bin', 'f.bin'))
      const http = yield* makeHttp({ hostHeader: 'zulip.example.com' })
      yield* http.uploadRaw('f.bin', new Uint8Array([0]))
      expect(fixture.captured[0]?.headers.get('host')).toBe('zulip.example.com')
    }),
  ))

// --- decodeUserUploadPath (comms-spj3.13) ---

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
