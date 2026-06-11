import { afterEach, beforeEach, expect, test } from 'bun:test'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Cause, Effect, Exit, Option, Schema } from 'effect'
import { BotToBotDirectMessageError, type RecipientDirectory, wrapBotHttp } from './bot-dm-guard.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'
import type { TestRealm } from './test-server.ts'
import { startTestRealm } from './test-server.ts'
import { ZulipUserRef } from './user-ref.ts'

let realm: TestRealm

beforeEach(() => {
  realm = startTestRealm()
})

afterEach(async () => {
  await realm.stop()
})

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

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

const buildHttp = (): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    const realmUrl = yield* RealmUrl(realm.url)
    const email = yield* BotEmail('self-bot@example.com')
    const apiKey = yield* ApiKey('self-key')
    return yield* makeZulipHttp({ realmUrl, email, apiKey })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

const messagesCaptured = (): number =>
  realm.captured.filter((r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages').length

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

test('rejects /messages POST with type=private and a single bot recipient', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([OTHER_BOT_ID]),
          content: 'hi',
        }),
      )
      expect(messagesCaptured()).toBe(0)
    }),
  ))

test('rejects /messages POST with type=private and multiple bot recipients', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([OTHER_BOT_ID, SECOND_BOT_ID]),
          content: 'hi',
        }),
      )
      expect(messagesCaptured()).toBe(0)
    }),
  ))

test('rejects /messages POST with type=private when self is included alongside bots only', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([SELF_ID, OTHER_BOT_ID]),
          content: 'hi',
        }),
      )
      expect(messagesCaptured()).toBe(0)
    }),
  ))

test('allows /messages POST with type=private to a single human recipient', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 1 },
      }))
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([HUMAN_ID]),
        content: 'hi',
      })
      expect(messagesCaptured()).toBe(1)
    }),
  ))

test('allows /messages POST with type=private to a mixed bot+human group', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 2 },
      }))
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([OTHER_BOT_ID, HUMAN_ID]),
        content: 'hi',
      })
      expect(messagesCaptured()).toBe(1)
    }),
  ))

test('allows /messages POST with type=private to self only (no non-self recipients)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 3 },
      }))
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.post('/messages', sentMessageSchema, {
        type: 'private',
        to: JSON.stringify([SELF_ID]),
        content: 'note to self',
      })
      expect(messagesCaptured()).toBe(1)
    }),
  ))

test('allows /messages POST with type=channel regardless of recipient lookup', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages', () => ({
        body: { result: 'success', id: 4 },
      }))
      let directoryCalls = 0
      const trackedDirectory = (): Effect.Effect<RecipientDirectory> => {
        directoryCalls += 1
        return directoryStub()
      }
      const http = wrapBotHttp(yield* buildHttp(), trackedDirectory, SELF_ID)
      yield* http.post('/messages', sentMessageSchema, {
        type: 'channel',
        to: 'general',
        topic: 'x',
        content: 'hi',
      })
      expect(messagesCaptured()).toBe(1)
      expect(directoryCalls).toBe(0)
    }),
  ))

test('non-/messages POSTs pass through unchanged', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('POST', '/api/v1/messages/123/reactions', () => ({
        body: { result: 'success' },
      }))
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.post('/messages/123/reactions', successSchema, { emoji_name: 'tada' })
      const reactionPosts = realm.captured.filter(
        (r) => r.method === 'POST' && r.url.pathname === '/api/v1/messages/123/reactions',
      )
      expect(reactionPosts.length).toBe(1)
    }),
  ))

test('GET requests pass through unchanged', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('GET', '/api/v1/users', () => ({
        body: { result: 'success', members: [] },
      }))
      const usersResponseSchema = Schema.Struct({
        result: Schema.Literal('success'),
        members: Schema.Array(Schema.Unknown),
      })
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.get('/users', usersResponseSchema)
      const userGets = realm.captured.filter(
        (r) => r.method === 'GET' && r.url.pathname === '/api/v1/users',
      )
      expect(userGets.length).toBe(1)
    }),
  ))

test('DELETE requests pass through unchanged', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      realm.handle('DELETE', '/api/v1/messages/123/reactions', () => ({
        body: { result: 'success' },
      }))
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* http.delete('/messages/123/reactions', successSchema, { emoji_name: 'tada' })
      const reactionDeletes = realm.captured.filter(
        (r) => r.method === 'DELETE' && r.url.pathname === '/api/v1/messages/123/reactions',
      )
      expect(reactionDeletes.length).toBe(1)
    }),
  ))

test('rejects /messages POST with type=private when `to` cannot be parsed as a user-id list', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: 'someone@example.com',
          content: 'hi',
        }),
      )
      expect(messagesCaptured()).toBe(0)
    }),
  ))

test('rejects /messages POST with type=private when `to` is a JSON array of non-integers', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = wrapBotHttp(yield* buildHttp(), directoryStub, SELF_ID)
      yield* expectBotDirectMessageDefect(
        http.post('/messages', sentMessageSchema, {
          type: 'private',
          to: JSON.stringify([1.5]),
          content: 'hi',
        }),
      )
      expect(messagesCaptured()).toBe(0)
    }),
  ))
