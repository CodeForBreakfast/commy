import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Fiber, Option } from 'effect'
import { parseSessionId } from './bootstrap.ts'
import { SessionId, SessionIdLive, type SessionIdValue } from './session-id.ts'

const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'

const sid = (raw: string): SessionIdValue => Option.getOrThrow(parseSessionId(raw))

describe('SessionId service', () => {
  test('first succeed wins and await unblocks on it; a second succeed is a no-op', async () => {
    const result = await Effect.gen(function* () {
      const deferred = yield* SessionId
      const awaiter = yield* Effect.fork(Deferred.await(deferred))
      const first = yield* Deferred.succeed(deferred, sid(SID_A))
      const second = yield* Deferred.succeed(deferred, sid(SID_B))
      const awaited = yield* Fiber.join(awaiter)
      return { first, second, awaited }
    }).pipe(Effect.provide(SessionIdLive), Effect.runPromise)

    expect(result.first).toBe(true)
    expect(result.second).toBe(false)
    expect(result.awaited).toBe(sid(SID_A))
  })

  test('the layer provides one shared instance: an independent await read unblocks on a set through a separate read', async () => {
    const awaited = await Effect.gen(function* () {
      const awaiter = yield* Effect.fork(Effect.flatMap(SessionId, Deferred.await))
      const setter = yield* SessionId
      yield* Deferred.succeed(setter, sid(SID_A))
      return yield* Fiber.join(awaiter)
    }).pipe(Effect.provide(SessionIdLive), Effect.runPromise)

    expect(awaited).toBe(sid(SID_A))
  })
})
