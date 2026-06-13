import { expect, test } from 'bun:test'
import { HttpClient, HttpClientRequest } from '@effect/platform'
import { Duration, Effect, Fiber, Option } from 'effect'
import { makeStubHttpClient } from './stub-http-client.ts'

const REALM = 'https://zulip.example.com/api/v1'

const textOf = (response: { readonly text: Effect.Effect<string, unknown> }) => response.text

test('respond maps a GET by method+path to a canned JSON body', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', {
        body: { result: 'success', user_id: 7, full_name: 'bot' },
      })
      const response = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/users/me`))
      expect(response.status).toBe(200)
      const body = yield* textOf(response)
      expect(JSON.parse(body)).toEqual({ result: 'success', user_id: 7, full_name: 'bot' })
    }),
  ))

test('a canned response defaults to content-type application/json', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
      const response = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/users/me`))
      expect(response.headers['content-type']).toMatch(/application\/json/)
    }),
  ))

test('keys responses by method AND path — GET and POST to one path differ', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/messages', { body: { result: 'success', messages: [] } })
      yield* stub.respond('POST', '/api/v1/messages', { body: { result: 'success', id: 99 } })
      const got = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/messages`))
      const posted = yield* stub.client.execute(
        HttpClientRequest.post(`${REALM}/messages`).pipe(
          HttpClientRequest.bodyUrlParams({ content: 'hi' }),
        ),
      )
      expect(JSON.parse(yield* textOf(got))).toEqual({ result: 'success', messages: [] })
      expect(JSON.parse(yield* textOf(posted))).toEqual({ result: 'success', id: 99 })
    }),
  ))

test('respondSequence returns queued responses one-per-request in order', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      // Event-pump shape: register, then chained /events polls that advance
      // last_event_id, each poll consuming the next queued batch.
      yield* stub.respond('POST', '/api/v1/register', {
        body: { result: 'success', queue_id: 'q-1', last_event_id: 0 },
      })
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'success', events: [{ id: 1, type: 'message' }] } },
        { body: { result: 'success', events: [{ id: 2, type: 'message' }] } },
      ])
      const register = yield* stub.client.execute(HttpClientRequest.post(`${REALM}/register`))
      expect(JSON.parse(yield* textOf(register)).queue_id).toBe('q-1')
      const poll1 = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/events`))
      const poll2 = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/events`))
      expect(JSON.parse(yield* textOf(poll1)).events[0].id).toBe(1)
      expect(JSON.parse(yield* textOf(poll2)).events[0].id).toBe(2)
    }),
  ))

test('a drained sequence falls back to the registered default for that key', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/events', {
        body: { result: 'success', events: [] },
      })
      yield* stub.respondSequence('GET', '/api/v1/events', [
        { body: { result: 'success', events: [{ id: 1, type: 'message' }] } },
      ])
      const first = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/events`))
      const second = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/events`))
      expect(JSON.parse(yield* textOf(first)).events).toHaveLength(1)
      expect(JSON.parse(yield* textOf(second)).events).toHaveLength(0)
    }),
  ))

test('captures request method, url, and auth headers', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
      yield* stub.client.execute(
        HttpClientRequest.get(`${REALM}/users/me`).pipe(
          HttpClientRequest.setHeader('authorization', 'Basic Ym90OnNla3JldA=='),
        ),
      )
      const captured = yield* stub.captured
      expect(captured).toHaveLength(1)
      const req = captured[0]
      if (req === undefined) throw new Error('expected a captured request')
      expect(req.method).toBe('GET')
      expect(req.url.pathname).toBe('/api/v1/users/me')
      expect(req.headers.get('authorization')).toBe('Basic Ym90OnNla3JldA==')
    }),
  ))

test('captures the form-encoded body and content-type of a POST', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('POST', '/api/v1/messages', { body: { result: 'success', id: 1 } })
      yield* stub.client.execute(
        HttpClientRequest.post(`${REALM}/messages`).pipe(
          HttpClientRequest.bodyUrlParams({ type: 'stream', content: 'hey there & friends' }),
        ),
      )
      const req = (yield* stub.captured)[0]
      if (req === undefined) throw new Error('expected a captured request')
      expect(req.method).toBe('POST')
      expect(req.headers.get('content-type')).toBe('application/x-www-form-urlencoded')
      const params = new URLSearchParams(req.body)
      expect(params.get('type')).toBe('stream')
      expect(params.get('content')).toBe('hey there & friends')
    }),
  ))

test('captures query-string params on the request url', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/messages', { body: { result: 'success' } })
      yield* stub.client.execute(
        HttpClientRequest.get(`${REALM}/messages`).pipe(
          HttpClientRequest.setUrlParams({ anchor: 'newest', num_before: 50 }),
        ),
      )
      const req = (yield* stub.captured)[0]
      if (req === undefined) throw new Error('expected a captured request')
      expect(req.url.searchParams.get('anchor')).toBe('newest')
      expect(req.url.searchParams.get('num_before')).toBe('50')
    }),
  ))

test('passes the response status through verbatim (e.g. a 429 error envelope)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', {
        body: { result: 'error', code: 'RATE_LIMIT_HIT', msg: 'API rate limit exceeded' },
        status: 429,
      })
      const response = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/users/me`))
      expect(response.status).toBe(429)
      expect(JSON.parse(yield* textOf(response)).code).toBe('RATE_LIMIT_HIT')
    }),
  ))

test('an unregistered route answers 404 with an error envelope, not a crash', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const response = yield* stub.client.execute(HttpClientRequest.get(`${REALM}/nope`))
      expect(response.status).toBe(404)
      const body = JSON.parse(yield* textOf(response))
      expect(body.result).toBe('error')
      expect(body.code).toBe('NO_STUB_HANDLER')
    }),
  ))

test('serves a Uint8Array body verbatim (download path)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47])
      yield* stub.respond('GET', '/user_uploads/1/a/p.png', {
        body: bytes,
        headers: { 'content-type': 'image/png' },
      })
      const response = yield* stub.client.execute(
        HttpClientRequest.get('https://zulip.example.com/user_uploads/1/a/p.png'),
      )
      expect(response.headers['content-type']).toBe('image/png')
      const buf = yield* response.arrayBuffer
      expect(new Uint8Array(buf)).toEqual(bytes)
    }),
  ))

test('opens no socket — a request to an unroutable host still resolves', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success' } })
      // Were the stub dialling a socket, this host would fail DNS. It resolves
      // because the stub answers from its registry without touching the wire.
      const response = yield* stub.client.execute(
        HttpClientRequest.get('https://this-host-does-not-resolve.invalid/api/v1/users/me'),
      )
      expect(response.status).toBe(200)
    }),
  ))

test('a hang response captures the request, then never resolves and stays interruptible', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      // The long-poll hold: the stub answers instantly for everything else, so
      // a terminal hang is what stops an eager consumer draining the sequence.
      yield* stub.respondSequence('GET', '/api/v1/events', [{ hang: true }])
      const fiber = yield* Effect.fork(
        stub.client.execute(HttpClientRequest.get(`${REALM}/events`)),
      )
      // Let the forked request issue and park on the hang.
      yield* Effect.sleep(Duration.millis(10))
      const captured = yield* stub.captured
      expect(captured).toHaveLength(1)
      expect(captured[0]?.url.pathname).toBe('/api/v1/events')
      // Still parked — a hang resolves to no Exit.
      expect(Option.isNone(yield* fiber.poll)).toBe(true)
      // Interrupting unwinds it cleanly (the scope-close path).
      const exit = yield* Fiber.interrupt(fiber)
      expect(exit._tag).toBe('Failure')
    }),
  ))

test('is a drop-in for the HttpClient.HttpClient service', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stub = yield* makeStubHttpClient
      yield* stub.respond('GET', '/api/v1/users/me', { body: { result: 'success', user_id: 1 } })
      // Exactly how the zulip adapter reaches the port: read HttpClient from
      // context and execute. Provided as the service, no FetchHttpClient.layer.
      const body = yield* Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient
        const response = yield* client.execute(HttpClientRequest.get(`${REALM}/users/me`))
        return yield* response.text
      }).pipe(Effect.provideService(HttpClient.HttpClient, stub.client))
      expect(JSON.parse(body).user_id).toBe(1)
    }),
  ))
