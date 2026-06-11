import { expect, test } from 'bun:test'
import type { AcquiredIdentity, Identity } from '@commy/core/ports'
import {
  type BotName,
  decodeBotNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
} from '@commy/core/ports'
import { Data, Effect } from 'effect'
import { createEnsureBound } from './ensure-bound.ts'

// Production deps fail with tagged port errors (UnknownIdentity / IdentityError);
// this stand-in mirrors that shape so the failure-path tests assert the message
// that survives Cause.squash without failing with a bare global Error.
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

test('first call invokes acquire(name) once and returns the AcquiredIdentity', async () => {
  const calls: string[] = []
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
    Effect.sync(() => {
      calls.push(name)
      return buildAcquired(name)
    })
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('cc-abcdef12') })
  const result = await ensureBound()
  expect(calls).toEqual(['cc-abcdef12'])
  expect(result.identity.name).toBe(decodeDisplayNameSync('cc-abcdef12'))
})

test('second call after success returns cached AcquiredIdentity without re-invoking acquire', async () => {
  const calls: string[] = []
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
    Effect.sync(() => {
      calls.push(name)
      return buildAcquired(name)
    })
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('foo') })
  const first = await ensureBound()
  const second = await ensureBound()
  expect(calls).toEqual(['foo'])
  expect(second).toBe(first)
})

test('concurrent first-call race resolves to a single acquire invocation', async () => {
  let calls = 0
  let resolveAcquire: ((value: AcquiredIdentity) => void) | undefined
  const acquire = (_name: BotName): Effect.Effect<AcquiredIdentity> =>
    Effect.async<AcquiredIdentity>((resume) => {
      calls += 1
      resolveAcquire = (value) => resume(Effect.succeed(value))
    })
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('racer') })
  const a = ensureBound()
  const b = ensureBound()
  const c = ensureBound()
  expect(calls).toBe(1)
  if (resolveAcquire === undefined) throw new Error('expected resolveAcquire')
  resolveAcquire(buildAcquired('racer'))
  const [ra, rb, rc] = await Promise.all([a, b, c])
  expect(ra).toBe(rb)
  expect(rb).toBe(rc)
})

test('acquire failure clears the cached deferred so the next call retries', async () => {
  let calls = 0
  const errors: Error[] = []
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
    calls += 1
    if (calls === 1) {
      return Effect.fail(new AcquireFailure({ message: 'boom' }))
    }
    return Effect.succeed(buildAcquired(name))
  }
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('retry') })
  try {
    await ensureBound()
  } catch (err) {
    errors.push(err as Error)
  }
  expect(errors).toHaveLength(1)
  expect(errors[0]?.message).toBe('boom')
  // Next call retries; succeeds.
  const result = await ensureBound()
  expect(calls).toBe(2)
  expect(result.identity.name).toBe(decodeDisplayNameSync('retry'))
})

test('concurrent callers all reject when acquire fails, then a fresh call retries', async () => {
  let calls = 0
  let rejectAcquire: ((reason: AcquireFailure) => void) | undefined
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
    calls += 1
    if (calls === 1) {
      return Effect.async<AcquiredIdentity, AcquireFailure>((resume) => {
        rejectAcquire = (reason) => resume(Effect.fail(reason))
      })
    }
    return Effect.succeed(buildAcquired(name))
  }
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('concurrent-fail') })
  const a = ensureBound()
  const b = ensureBound()
  if (rejectAcquire === undefined) throw new Error('expected rejectAcquire')
  // Capture both outcomes with handlers attached immediately — each caller gets
  // its own promise around the shared single-flight failure, so attaching late
  // would leave one momentarily unhandled.
  const aOutcome = a.then(
    (): unknown => 'resolved',
    (err: unknown) => err,
  )
  const bOutcome = b.then(
    (): unknown => 'resolved',
    (err: unknown) => err,
  )
  rejectAcquire(new AcquireFailure({ message: 'substrate rejected' }))
  expect(((await aOutcome) as Error).message).toBe('substrate rejected')
  expect(((await bOutcome) as Error).message).toBe('substrate rejected')
  // After failure, a fresh call rebuilds the deferred.
  const c = await ensureBound()
  expect(calls).toBe(2)
  expect(c.identity.name).toBe(decodeDisplayNameSync('concurrent-fail'))
})

test('current() returns undefined before first acquire and the AcquiredIdentity after', async () => {
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity> =>
    Effect.sync(() => buildAcquired(name))
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('inspect') })
  expect(ensureBound.current()).toBeUndefined()
  const acquired = await ensureBound()
  expect(ensureBound.current()).toBe(acquired)
})

test('current() stays undefined while acquire is in flight', async () => {
  let resolveAcquire: ((value: AcquiredIdentity) => void) | undefined
  const acquire = (_name: BotName): Effect.Effect<AcquiredIdentity> =>
    Effect.async<AcquiredIdentity>((resume) => {
      resolveAcquire = (value) => resume(Effect.succeed(value))
    })
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('pending') })
  const pending = ensureBound()
  expect(ensureBound.current()).toBeUndefined()
  if (resolveAcquire === undefined) throw new Error('expected resolveAcquire')
  resolveAcquire(buildAcquired('pending'))
  await pending
  expect(ensureBound.current()).toBeDefined()
})

test('current() resets to undefined after a failure so release-shutdown can skip', async () => {
  let calls = 0
  const acquire = (name: BotName): Effect.Effect<AcquiredIdentity, AcquireFailure> => {
    calls += 1
    if (calls === 1) return Effect.fail(new AcquireFailure({ message: 'nope' }))
    return Effect.succeed(buildAcquired(name))
  }
  const ensureBound = createEnsureBound({ acquire, name: decodeBotNameSync('rejected') })
  await expect(ensureBound()).rejects.toThrow('nope')
  expect(ensureBound.current()).toBeUndefined()
})
