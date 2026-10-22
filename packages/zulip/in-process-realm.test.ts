import { expect } from 'bun:test'
import { effectTest } from '@commy/testing/effect-test'
import { HttpClient } from '@effect/platform'
import { Effect, Schema } from 'effect'
import { ApiKey, makeZulipHttp, ZulipApiError } from './http.ts'
import { makeInProcessRealm } from './in-process-realm.ts'

const success = Schema.Struct({ result: Schema.Literal('success') })
const sent = Schema.Struct({ id: Schema.Int })
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
