import { expect, test } from 'bun:test'
import type { AcquiredIdentity, Identity } from '@commy/core/ports'
import {
  type BotName,
  decodeBotNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
} from '@commy/core/ports'
import { Data, Deferred, Effect, Fiber } from 'effect'
import { createEnsureBound } from './ensure-bound.ts'

// Production deps fail with tagged port errors (UnknownIdentity / IdentityError);
// this stand-in mirrors that shape so the failure-path tests assert the message
// that survives on the typed E channel without a bare global Error.
class AcquireFailure extends Data.TaggedError('AcquireFailure')<{ readonly message: string }> {}

const buildIdentity = (name: string): Identity => ({
  id: decodeIdentityIdSync(`bot:${name}`),
  name: decodeDisplayNameSync(name),
  kind: 'agent',
})

const buildAcquired = (name: string): AcquiredIdentity => ({
  identity: buildIdentity(name),
  credentials: { apiKey: `key-${name}` },
})

const run = <A, E>(self: Effect.Effect<A, E>): Promise<A> => Effect.runPromise(self)

test('first call invokes acquire(name) once and returns the AcquiredIdentity', () =>
  run(
    Effect.gen(function* () {
      const calls: string[] = []
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
        Effect.sync(() => {
          calls.push(name)
          return buildAcquired(name)
        })
      const ensureBound = yield* createEnsureBound({
        acquire,
        name: decodeBotNameSync('cc-abcdef12'),
      })
      const result = yield* ensureBound()
      expect(calls).toEqual(['cc-abcdef12'])
      expect(result.identity.name).toBe(decodeDisplayNameSync('cc-abcdef12'))
    }),
  ))

test('second call after success returns cached AcquiredIdentity without re-invoking acquire', () =>
  run(
    Effect.gen(function* () {
      const calls: string[] = []
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
        Effect.sync(() => {
          calls.push(name)
          return buildAcquired(name)
        })
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('foo') })
      const first = yield* ensureBound()
      const second = yield* ensureBound()
      expect(calls).toEqual(['foo'])
      expect(second).toBe(first)
    }),
  ))

test('concurrent first-call race resolves to a single acquire invocation', () =>
  run(
    Effect.gen(function* () {
      let calls = 0
      const started = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<AcquiredIdentity>()
      const acquire = (_name: BotName): Effect.Effect<AcquiredIdentity> =>
        Effect.sync(() => {
          calls += 1
        }).pipe(
          Effect.zipRight(Deferred.succeed(started, undefined)),
          Effect.zipRight(Deferred.await(gate)),
        )
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('racer') })
      const a = yield* Effect.fork(ensureBound())
      const b = yield* Effect.fork(ensureBound())
      const c = yield* Effect.fork(ensureBound())
      // The single winner enters acquire exactly once; the others await its Deferred.
      yield* Deferred.await(started)
      expect(calls).toBe(1)
      yield* Deferred.succeed(gate, buildAcquired('racer'))
      const ra = yield* Fiber.join(a)
      const rb = yield* Fiber.join(b)
      const rc = yield* Fiber.join(c)
      expect(ra).toBe(rb)
      expect(rb).toBe(rc)
    }),
  ))

test('acquire failure clears the cached deferred so the next call retries', () =>
  run(
    Effect.gen(function* () {
      let calls = 0
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
        calls += 1
        if (calls === 1) {
          return Effect.fail(new AcquireFailure({ message: 'boom' }))
        }
        return Effect.succeed(buildAcquired(name))
      }
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('retry') })
      const err = yield* Effect.flip(ensureBound())
      expect(err.message).toBe('boom')
      // Next call retries; succeeds.
      const result = yield* ensureBound()
      expect(calls).toBe(2)
      expect(result.identity.name).toBe(decodeDisplayNameSync('retry'))
    }),
  ))

test('concurrent callers all fail when acquire fails, then a fresh call retries', () =>
  run(
    Effect.gen(function* () {
      let calls = 0
      const started = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<AcquiredIdentity, AcquireFailure>()
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
        calls += 1
        if (calls === 1) {
          return Deferred.succeed(started, undefined).pipe(Effect.zipRight(Deferred.await(gate)))
        }
        return Effect.succeed(buildAcquired(name))
      }
      const ensureBound = yield* createEnsureBound({
        acquire,
        name: decodeBotNameSync('concurrent-fail'),
      })
      const a = yield* Effect.fork(ensureBound())
      const b = yield* Effect.fork(ensureBound())
      yield* Deferred.await(started)
      yield* Deferred.fail(gate, new AcquireFailure({ message: 'substrate rejected' }))
      const aErr = yield* Effect.flip(Fiber.join(a))
      const bErr = yield* Effect.flip(Fiber.join(b))
      expect(aErr.message).toBe('substrate rejected')
      expect(bErr.message).toBe('substrate rejected')
      // After failure, a fresh call rebuilds the deferred.
      const c = yield* ensureBound()
      expect(calls).toBe(2)
      expect(c.identity.name).toBe(decodeDisplayNameSync('concurrent-fail'))
    }),
  ))

test('current() returns undefined before first acquire and the AcquiredIdentity after', () =>
  run(
    Effect.gen(function* () {
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
        Effect.sync(() => buildAcquired(name))
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('inspect') })
      expect(ensureBound.current()).toBeUndefined()
      const acquired = yield* ensureBound()
      expect(ensureBound.current()).toBe(acquired)
    }),
  ))

test('current() stays undefined while acquire is in flight', () =>
  run(
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const gate = yield* Deferred.make<AcquiredIdentity>()
      const acquire = (_name: BotName): Effect.Effect<AcquiredIdentity> =>
        Deferred.succeed(started, undefined).pipe(Effect.zipRight(Deferred.await(gate)))
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('pending') })
      const fiber = yield* Effect.fork(ensureBound())
      yield* Deferred.await(started)
      expect(ensureBound.current()).toBeUndefined()
      yield* Deferred.succeed(gate, buildAcquired('pending'))
      yield* Fiber.join(fiber)
      expect(ensureBound.current()).toBeDefined()
    }),
  ))

test('current() resets to undefined after a failure so release-shutdown can skip', () =>
  run(
    Effect.gen(function* () {
      let calls = 0
      const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
        calls += 1
        if (calls === 1) return Effect.fail(new AcquireFailure({ message: 'nope' }))
        return Effect.succeed(buildAcquired(name))
      }
      const ensureBound = yield* createEnsureBound({ acquire, name: decodeBotNameSync('rejected') })
      const err = yield* Effect.flip(ensureBound())
      expect(err.message).toBe('nope')
      expect(ensureBound.current()).toBeUndefined()
    }),
  ))
