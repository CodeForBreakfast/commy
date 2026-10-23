import { expect } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import { HttpClient } from '@effect/platform'
import { Chunk, Duration, Effect, Fiber, Schema, TestClock, TestContext } from 'effect'
import { ApiKey, makeZulipHttp, ZulipApiError } from './http.ts'
import { makeInProcessRealm } from './in-process-realm.ts'

const success = Schema.Struct({ result: Schema.Literal('success') })
const sent = Schema.Struct({ id: Schema.Int })
const registered = Schema.Struct({ queue_id: Schema.String, last_event_id: Schema.Int })
const polled = Schema.Struct({
  events: Schema.Array(Schema.Struct({ id: Schema.Int, type: Schema.String })),
})
const page = Schema.Struct({ messages: Schema.Array(Schema.Struct({ id: Schema.Int })) })

const minterHttp = Effect.gen(function* () {
  const realm = yield* makeInProcessRealm
  const http = yield* makeZulipHttp({
    realmUrl: realm.url,
    email: realm.minter.email,
    apiKey: realm.minter.apiKey,
  }).pipe(Effect.provideService(HttpClient.HttpClient, realm.client))
  return { realm, http }
})

effectTest('the messages endpoint returns the newest page oldest-first', () =>
  Effect.gen(function* () {
    const { http } = yield* minterHttp
    yield* http.post('/users/me/subscriptions', success, {
      subscriptions: JSON.stringify([{ name: 'lobby' }]),
    })
    const ids = yield* Effect.forEach(['first', 'second', 'third'], (content) =>
      http
        .post('/messages', sent, { type: 'channel', to: 'lobby', topic: 'design', content })
        .pipe(Effect.map((res) => res.id)),
    )
    const res = yield* http.get('/messages', page, {
      anchor: 'newest',
      num_before: 2,
      num_after: 0,
      narrow: JSON.stringify([{ operator: 'channel', operand: 'lobby' }]),
    })
    expect(res.messages.map((m) => m.id)).toEqual(ids.slice(1))
  }),
)

effectTest('a request under a wrong key is refused as Zulip refuses it', () =>
  Effect.gen(function* () {
    const { realm } = yield* minterHttp
    const impostor = yield* makeZulipHttp({
      realmUrl: realm.url,
      email: realm.minter.email,
      apiKey: yield* ApiKey('not-the-key'),
    }).pipe(Effect.provideService(HttpClient.HttpClient, realm.client))
    const error = yield* Effect.flip(impostor.get('/users', success))
    expect(error).toBeInstanceOf(ZulipApiError)
    expect(error).toMatchObject({ status: 401, code: 'INVALID_API_KEY' })
  }),
)

effectTest(
  'an idle long-poll answers with a heartbeat',
  () =>
    Effect.gen(function* () {
      const { http } = yield* minterHttp
      const queue = yield* http.post('/register', registered, {
        event_types: JSON.stringify(['message']),
      })
      const poll = yield* Effect.fork(
        http.get('/events', polled, {
          queue_id: queue.queue_id,
          last_event_id: queue.last_event_id,
        }),
      )
      // Advance the clock only once the poll is parked on its hold.
      yield* TestClock.sleeps().pipe(
        Effect.zipLeft(Effect.yieldNow()),
        Effect.repeat({ until: Chunk.isNonEmpty }),
      )
      yield* TestClock.adjust(Duration.minutes(1))
      const res = yield* Fiber.join(poll)
      expect(res.events).toEqual([{ id: queue.last_event_id + 1, type: 'heartbeat' }])
    }),
  { layer: TestContext.TestContext },
)

const rendered = Schema.Struct({ rendered: Schema.String })
const members = Schema.Struct({
  members: Schema.Array(Schema.Struct({ user_id: Schema.Int, full_name: Schema.String })),
})

effectTest('a name|id mention renders only when the name matches the member exactly', () =>
  Effect.gen(function* () {
    const { http } = yield* minterHttp
    const self = (yield* http.get('/users', members)).members.find((m) => m.full_name === 'Minter')
    if (self === undefined) return yield* Effect.die('the minter is not in the member list')
    const render = (name: string) =>
      http
        .post('/messages/render', rendered, { content: `@**${name}|${self.user_id}** hi` })
        .pipe(Effect.map((res) => res.rendered))
    expect(yield* render(self.full_name)).toContain(`data-user-id="${self.user_id}"`)
    expect(yield* render(self.full_name.toUpperCase())).not.toContain('data-user-id')
  }),
)

effectTest("a bot's owner may reactivate it while no active user holds its name", () =>
  Effect.gen(function* () {
    const { http } = yield* minterHttp
    const minted = Schema.Struct({ user_id: Schema.Int })
    const mint = (shortName: string) =>
      http
        .post('/bots', minted, { full_name: 'returning-bot', short_name: shortName, bot_type: 1 })
        .pipe(Effect.map((res) => res.user_id))
    const first = yield* mint('returning-a')
    yield* http.delete(`/bots/${first}`, success)
    yield* http.post(`/users/${first}/reactivate`, success, {})
    yield* http.delete(`/bots/${first}`, success)
    yield* mint('returning-b')
    const error = yield* Effect.flip(http.post(`/users/${first}/reactivate`, success, {}))
    expect(error.message).toContain('already an active bot named')
  }),
)
