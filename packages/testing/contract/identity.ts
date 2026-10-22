/** The shared contract's tests for IdentityPort. */

import { expect, test } from 'bun:test'
import { decodeBotNameSync, decodeDisplayNameSync, UnknownIdentity } from '@commy/core/ports'
import { Cause, Effect, Exit, Option } from 'effect'
import type { ContractEnv } from '../contract.ts'

export const identityContract = (env: () => ContractEnv): void => {
  test('identity.resolve returns the identity for a known seeded peer', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const alice = yield* env().seedAgent('alice')
        const found = yield* env().comms.identity.resolve(alice.name)
        expect(Option.getOrUndefined(found)?.id).toEqual(alice.id)
      }),
    ))
  test('identity.resolve returns undefined for an unknown name', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const found = yield* env().comms.identity.resolve(decodeDisplayNameSync('does-not-exist'))
        expect(Option.isNone(found)).toBe(true)
      }),
    ))
  test('identity.resolve returns undefined for a deactivated agent', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const deactivateAgent = env().deactivateAgent
        if (deactivateAgent === undefined) return
        const gone = yield* env().seedAgent('alice')
        yield* deactivateAgent(gone)
        const found = yield* env().comms.identity.resolve(gone.name)
        expect(Option.isNone(found)).toBe(true)
      }),
    ))
  test('identity.acquire on the bound name is idempotent — returns the same identity', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const me = yield* env().comms.identity.currentIdentity()
        const result = yield* env().comms.identity.acquire(decodeBotNameSync(me.name))
        expect(result.identity.id).toEqual(me.id)
        expect(result.identity.name).toEqual(me.name)
      }),
    ))
  test('identity.acquire on a different name rejects when already bound', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const me = yield* env().comms.identity.currentIdentity()
        const otherName = decodeBotNameSync(`${me.name}-conflict`)
        const exit = yield* Effect.exit(env().comms.identity.acquire(otherName))
        expect(Exit.isFailure(exit)).toBe(true)
      }),
    ))
  test('identity.release on an unauthenticated adapter is a no-op (does not throw)', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        yield* fresh.identity.release()
      }),
    ))
  test('identity.release then identity.acquire(name) rebinds the adapter to that name', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        const current = yield* env().comms.identity.currentIdentity()
        const rawName = `${current.name}-cycle`
        const acquireName = decodeBotNameSync(rawName)
        yield* fresh.identity.acquire(acquireName)
        const before = yield* fresh.identity.currentIdentity()
        expect(before.name).toEqual(decodeDisplayNameSync(rawName))
        yield* fresh.identity.release()
        yield* fresh.identity.acquire(acquireName)
        const after = yield* fresh.identity.currentIdentity()
        expect(after.name).toEqual(decodeDisplayNameSync(rawName))
      }),
    ))
  test('identity.acquire on a deactivated agent brings it back under its old id', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        const deactivateAgent = env().deactivateAgent
        if (build === undefined || deactivateAgent === undefined) return
        const current = yield* env().comms.identity.currentIdentity()
        const name = decodeBotNameSync(`${current.name}-returning`)
        const first = yield* (yield* build()).identity.acquire(name)
        yield* deactivateAgent(first.identity)
        const again = yield* (yield* build()).identity.acquire(name)
        expect(again.identity.id).toEqual(first.identity.id)
        const found = yield* env().comms.identity.resolve(first.identity.name)
        expect(Option.getOrUndefined(found)?.id).toEqual(first.identity.id)
        const agents = yield* env().comms.directory.listAgents()
        expect(agents.map((a) => a.id)).toContain(first.identity.id)
      }),
    ))
  for (const { release, expected, label } of [
    { release: undefined, expected: false, label: 'an ephemeral release takes' },
    { release: { persistent: true }, expected: true, label: 'a persistent release leaves' },
  ]) {
    test(`identity.release: ${label} the agent in the directory`, () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const build = env().newUnacquiredAdapter
          if (build === undefined) return
          const current = yield* env().comms.identity.currentIdentity()
          const fresh = yield* build()
          const { identity } = yield* fresh.identity.acquire(
            decodeBotNameSync(`${current.name}-released`),
          )
          yield* fresh.identity.release(release)
          const found = yield* env().comms.identity.resolve(identity.name)
          expect(Option.isSome(found)).toBe(expected)
          const agents = yield* env().comms.directory.listAgents()
          expect(agents.map((a) => a.id).includes(identity.id)).toBe(expected)
        }),
      ))
  }
  test('identity.acquire binds the adapter to the identity it returns', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        const current = yield* env().comms.identity.currentIdentity()
        const acquired = yield* fresh.identity.acquire(decodeBotNameSync(`${current.name}-cycle`))
        expect(yield* fresh.identity.currentIdentity()).toEqual(acquired.identity)
      }),
    ))
  test('identity.currentIdentity dies before the adapter has acquired', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        const exit = yield* Effect.exit(fresh.identity.currentIdentity())
        expect(Exit.isFailure(exit) && Cause.isDie(exit.cause)).toBe(true)
      }),
    ))
  test('identity.release clears the binding — currentIdentity then dies', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* env().comms.identity.release()
        const exit = yield* Effect.exit(env().comms.identity.currentIdentity())
        expect(Exit.isFailure(exit) && Cause.isDie(exit.cause)).toBe(true)
      }),
    ))
  test('identity.release({ persistent: true }) still clears the binding', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* env().comms.identity.release({ persistent: true })
        const exit = yield* Effect.exit(env().comms.identity.currentIdentity())
        expect(Exit.isFailure(exit) && Cause.isDie(exit.cause)).toBe(true)
      }),
    ))
  test('concurrent identity.acquire of different names on a fresh adapter binds exactly one', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const build = env().newUnacquiredAdapter
        if (build === undefined) return
        const fresh = yield* build()
        const current = yield* env().comms.identity.currentIdentity()
        const names = ['a', 'b', 'c', 'd'].map((suffix) =>
          decodeBotNameSync(`${current.name}-${suffix}`),
        )
        // Unbounded is safe here: an adapter binds at most once, so at most one
        // acquire reaches the substrate's mint path.
        const exits = yield* Effect.forEach(
          names,
          (name) => Effect.exit(fresh.identity.acquire(name)),
          {
            concurrency: 'unbounded',
          },
        )
        const bound = exits.filter(Exit.isSuccess).map((exit) => exit.value.identity)
        expect(bound).toEqual([yield* fresh.identity.currentIdentity()])
      }),
    ))
  test('identity.acquire fails with UnknownIdentity for a name the substrate cannot bind', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const target = env().unacquirableName
        const build = env().newUnacquiredAdapter
        if (target === undefined || build === undefined) return
        const fresh = yield* build()
        const error = yield* Effect.flip(fresh.identity.acquire(decodeBotNameSync(target)))
        expect(error).toBeInstanceOf(UnknownIdentity)
      }),
    ))
}
