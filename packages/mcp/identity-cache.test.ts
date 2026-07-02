import { describe, expect, test } from 'bun:test'
import type { AcquiredIdentity, Identity } from '@commy/core/ports'
import {
  type BotName,
  decodeBotNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  IdentityError,
} from '@commy/core/ports'
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  TestClock,
  TestContext,
} from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { parseSessionId, sanitiseProjectSlug } from './bootstrap.ts'
import { createEnsureBound } from './ensure-bound.ts'
import {
  createEphemeralIdentityCache,
  createSingleIdentityCache,
  UnboundEphemeralSession,
} from './identity-cache.ts'

/**
 * Test helper. Constructs a `SessionId` from an 8-char hex prefix so the
 * minted bot name is `cc-<prefix>` (the leading 8 of a UUID are what
 * `composeBotName` slices). All session ids in this suite are UUIDs since
 * the brand demands UUID shape.
 */
const sid = (hex8: string): SessionId => {
  const parsed = parseSessionId(`${hex8}-0000-0000-0000-000000000000`)
  if (Option.isNone(parsed)) {
    throw new Error(`identity-cache.test sid helper: not a hex8 prefix: ${hex8}`)
  }
  return parsed.value
}

const slug = (raw: string): ProjectSlug => {
  const result = sanitiseProjectSlug(raw)
  if (Option.isNone(result)) throw new Error(`slug helper: sanitises to nothing: ${raw}`)
  return result.value
}

const buildIdentity = (name: string): Identity => ({
  id: decodeIdentityIdSync(`bot:${name}`),
  name: decodeDisplayNameSync(name),
  kind: 'agent',
})

const buildAcquired = (name: string): AcquiredIdentity => ({
  identity: buildIdentity(name),
  credentials: { apiKey: `key-${name}` },
})

/**
 * Run a bind Effect that is expected to fail and surface the squashed
 * error — typed failure OR defect, mirroring the raw error a caller sees
 * when the bind runs at the MCP edge (`runEdge`'s `Cause.squash`).
 */
const captureError = <A, E>(self: Effect.Effect<A, E>): Effect.Effect<unknown> =>
  Effect.exit(self).pipe(
    Effect.flatMap((exit) =>
      Exit.isFailure(exit)
        ? Effect.succeed(Cause.squash(exit.cause))
        : Effect.die(new Error('expected the bind effect to fail')),
    ),
  )

interface AdapterSpy {
  readonly acquire: (name: BotName) => Effect.Effect<AcquiredIdentity>
  readonly release: () => Effect.Effect<void>
  readonly acquireCalls: string[]
  readonly releaseCalls: number[]
  readonly bound: () => string | undefined
}

const buildAdapterSpy = (): AdapterSpy => {
  const acquireCalls: string[] = []
  const releaseCalls: number[] = []
  let boundName: string | undefined
  let nextReleaseIndex = 0
  return {
    acquireCalls,
    releaseCalls,
    bound: () => boundName,
    acquire: (name): Effect.Effect<AcquiredIdentity> =>
      Effect.sync(() => {
        acquireCalls.push(name)
        if (boundName !== undefined && boundName !== name) {
          throw new Error(
            `adapter already bound to ${boundName} — release before acquiring ${name}`,
          )
        }
        boundName = name
        return buildAcquired(name)
      }),
    release: (): Effect.Effect<void> =>
      Effect.sync(() => {
        releaseCalls.push(nextReleaseIndex++)
        boundName = undefined
      }),
  }
}

/**
 * Run an Effect with the deterministic `TestClock` provided. `ensureBoundFor`
 * stamps `lastUsedMs` from `Clock.currentTimeMillis`; under `TestClock` that
 * starts at 0 and only advances on `TestClock.adjust`, so the idle-sweep
 * assertions are reproducible without an injected `now` lambda.
 */
const runTest = <A, E>(self: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(self.pipe(Effect.provide(TestContext.TestContext)))

describe('createSingleIdentityCache (persistent mode)', () => {
  test('ensureBoundFor returns the singleton regardless of session_id', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const ensureBound = yield* createEnsureBound({
          acquire: spy.acquire,
          name: decodeBotNameSync('myproject-concierge'),
        })
        const cache = createSingleIdentityCache({ ensureBound })
        const a = yield* cache.ensureBoundFor(sid('11111111'))
        const b = yield* cache.ensureBoundFor(sid('22222222'))
        expect(a).toBe(ensureBound)
        expect(b).toBe(ensureBound)
        const ra = yield* a()
        const rb = yield* b()
        expect(spy.acquireCalls).toEqual(['myproject-concierge'])
        expect(rb).toBe(ra)
      }),
    ))

  test('ensureBoundFor(undefined) returns the singleton', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const ensureBound = yield* createEnsureBound({
          acquire: spy.acquire,
          name: decodeBotNameSync('myproject-concierge'),
        })
        const cache = createSingleIdentityCache({ ensureBound })
        expect(yield* cache.ensureBoundFor(undefined)).toBe(ensureBound)
      }),
    ))

  test('boundIdentityIds is empty before acquire and contains the identity after', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const ensureBound = yield* createEnsureBound({
          acquire: spy.acquire,
          name: decodeBotNameSync('persistent-bot'),
        })
        const cache = createSingleIdentityCache({ ensureBound })
        expect([...cache.boundIdentityIds()]).toEqual([])
        const eb = yield* cache.ensureBoundFor(sid('33333333'))
        yield* eb()
        expect([...cache.boundIdentityIds()]).toEqual([buildIdentity('persistent-bot').id])
      }),
    ))

  test('sweepIdle is a no-op for the persistent singleton', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const ensureBound = yield* createEnsureBound({
          acquire: spy.acquire,
          name: decodeBotNameSync('persistent-bot'),
        })
        const cache = createSingleIdentityCache({ ensureBound })
        const eb = yield* cache.ensureBoundFor(sid('33333333'))
        yield* eb()
        yield* cache.sweepIdle(Number.POSITIVE_INFINITY)
        expect(spy.releaseCalls).toEqual([])
        expect([...cache.boundIdentityIds()]).toEqual([buildIdentity('persistent-bot').id])
      }),
    ))
})

describe('createEphemeralIdentityCache (ephemeral mode)', () => {
  test('first ensureBoundFor mints cc-<first-8> from session_id', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('abcdef12'))
        const acquired = yield* eb()
        expect(spy.acquireCalls).toEqual(['cc-abcdef12'])
        expect(acquired.identity.name).toBe(decodeDisplayNameSync('cc-abcdef12'))
      }),
    ))

  test('second call with same session_id returns the same EnsureBound (no re-acquire)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const first = yield* cache.ensureBoundFor(sid('aaaaaaaa'))
        const second = yield* cache.ensureBoundFor(sid('aaaaaaaa'))
        expect(second).toBe(first)
        yield* second()
        expect(spy.acquireCalls).toEqual(['cc-aaaaaaaa'])
      }),
    ))

  test('new session_id releases prior identity and mints a new one', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const ebA = yield* cache.ensureBoundFor(sid('aaaaaaaa'))
        yield* ebA()
        expect(spy.acquireCalls).toEqual(['cc-aaaaaaaa'])
        expect(spy.releaseCalls).toEqual([])
        const newEB = yield* cache.ensureBoundFor(sid('bbbbbbbb'))
        yield* newEB()
        expect(spy.releaseCalls).toHaveLength(1)
        expect(spy.acquireCalls).toEqual(['cc-aaaaaaaa', 'cc-bbbbbbbb'])
      }),
    ))

  test('transition before prior is acquired skips release (nothing bound yet)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        yield* cache.ensureBoundFor(sid('aaaaaaaa')) // no acquire yet
        const eb = yield* cache.ensureBoundFor(sid('bbbbbbbb'))
        yield* eb()
        expect(spy.releaseCalls).toEqual([])
        expect(spy.acquireCalls).toEqual(['cc-bbbbbbbb'])
      }),
    ))

  test('boundIdentityIds contains current identity only', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        expect([...cache.boundIdentityIds()]).toEqual([])
        const eb1 = yield* cache.ensureBoundFor(sid('a1a1a1a1'))
        yield* eb1()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-a1a1a1a1')])
        const eb2 = yield* cache.ensureBoundFor(sid('b2b2b2b2'))
        yield* eb2()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-b2b2b2b2')])
      }),
    ))

  test('two distinct sessions yield two distinct bound identities over time', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb1 = yield* cache.ensureBoundFor(sid('a1f7a1f7'))
        yield* eb1()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-a1f7a1f7')])
        const eb2 = yield* cache.ensureBoundFor(sid('d0d0d0d0'))
        yield* eb2()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-d0d0d0d0')])
      }),
    ))

  test('sweepIdle releases the entry when idle > idleReleaseMs', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        const eb = yield* cache.ensureBoundFor(sid('1d1ee575'))
        yield* eb()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-1d1ee575')])
        yield* TestClock.adjust(Duration.millis(5001))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(spy.releaseCalls).toHaveLength(1)
        expect([...cache.boundIdentityIds()]).toEqual([])
      }),
    ))

  test('sweepIdle is a no-op when entry is fresh', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        const eb = yield* cache.ensureBoundFor(sid('f7e57e57'))
        yield* eb()
        yield* TestClock.adjust(Duration.millis(4999))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(spy.releaseCalls).toEqual([])
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-f7e57e57')])
      }),
    ))

  test('sweepIdle is a no-op when nothing is bound', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        yield* cache.sweepIdle(Number.POSITIVE_INFINITY)
        expect(spy.releaseCalls).toEqual([])
      }),
    ))

  test('sweepIdle skips an entry that has not yet acquired', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        yield* cache.ensureBoundFor(sid('aaaaaaaa')) // create entry but never call
        yield* TestClock.adjust(Duration.millis(10_000))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(spy.releaseCalls).toEqual([])
      }),
    ))

  test('after sweepIdle releases, next ensureBoundFor for same session_id remints', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        const eb = yield* cache.ensureBoundFor(sid('7e7e7e7e'))
        yield* eb()
        yield* TestClock.adjust(Duration.millis(6000))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(spy.releaseCalls).toHaveLength(1)
        const eb2 = yield* cache.ensureBoundFor(sid('7e7e7e7e'))
        yield* eb2()
        expect(spy.acquireCalls).toEqual(['cc-7e7e7e7e', 'cc-7e7e7e7e'])
      }),
    ))

  test('ensureBoundFor(undefined) returns the unbound stub even when a slot is active (no leak across /clear)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('5005ed01'))
        yield* eb()
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-5005ed01')])
        const stub = yield* cache.ensureBoundFor(undefined)
        expect(stub).not.toBe(eb)
        expect(stub.current()).toBeUndefined()
        const err = yield* captureError(stub())
        expect(err).toBeInstanceOf(UnboundEphemeralSession)
      }),
    ))

  test('ensureBoundFor(undefined) is passive: does not release or replace the active slot', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('5005ed01'))
        yield* eb()
        yield* cache.ensureBoundFor(undefined)
        expect(spy.releaseCalls).toEqual([])
        const sameSid = yield* cache.ensureBoundFor(sid('5005ed01'))
        expect(sameSid).toBe(eb)
        expect(sameSid.current()?.identity.name).toBe(decodeDisplayNameSync('cc-5005ed01'))
      }),
    ))

  test('ensureBoundFor(undefined) with no current entry yields an EnsureBound that errors when called', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(undefined)
        const err = yield* captureError(eb())
        expect(err).toBeInstanceOf(UnboundEphemeralSession)
      }),
    ))

  test('unbound stub rejects with an UnboundEphemeralSession tagged error', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(undefined)
        const err = yield* captureError(eb())
        expect(err).toBeInstanceOf(UnboundEphemeralSession)
        expect((err as UnboundEphemeralSession)._tag).toBe('UnboundEphemeralSession')
        expect((err as UnboundEphemeralSession).message).toMatch(/session_id|ephemeral/i)
      }),
    ))

  test('releaseAllBound releases the active entry and clears state', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('e177e177'))
        yield* eb()
        yield* cache.releaseAllBound()
        expect(spy.releaseCalls).toHaveLength(1)
        expect([...cache.boundIdentityIds()]).toEqual([])
      }),
    ))

  test('releaseAllBound is a no-op when nothing is bound', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        yield* cache.releaseAllBound()
        expect(spy.releaseCalls).toEqual([])
      }),
    ))

  test('mints cc-<project>-<8> when project is supplied per call', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('abcdef12'), slug('myproject'))
        yield* eb()
        expect(spy.acquireCalls).toEqual(['cc-myproject-abcdef12'])
      }),
    ))

  test('two sessions in different projects mint two project-prefixed names', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb1 = yield* cache.ensureBoundFor(sid('a3a3a3a3'), slug('myproject-a'))
        yield* eb1()
        const eb2 = yield* cache.ensureBoundFor(sid('b3b3b3b3'), slug('myproject-b'))
        yield* eb2()
        expect(spy.acquireCalls).toEqual(['cc-myproject-a-a3a3a3a3', 'cc-myproject-b-b3b3b3b3'])
      }),
    ))

  test('project on a returning sid is ignored — the slot is named once', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb1 = yield* cache.ensureBoundFor(sid('aa110110'), slug('first-proj'))
        const eb2 = yield* cache.ensureBoundFor(sid('aa110110'), slug('second-proj'))
        expect(eb2).toBe(eb1)
        yield* eb2()
        expect(spy.acquireCalls).toEqual(['cc-first-proj-aa110110'])
      }),
    ))

  test('boundIdentityIds reflects project-prefixed name', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('aabbccdd'), slug('commy'))
        yield* eb()
        expect([...cache.boundIdentityIds()]).toEqual([
          decodeIdentityIdSync('bot:cc-commy-aabbccdd'),
        ])
      }),
    ))

  test('omitted project arg falls back to bare cc-<8> (no leak)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
        })
        const eb = yield* cache.ensureBoundFor(sid('ba4e1f33'))
        yield* eb()
        expect(spy.acquireCalls).toEqual(['cc-ba4e1f33'])
      }),
    ))

  test('onAcquire fires after the first successful acquire with the acquired identity', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const acquired: AcquiredIdentity[] = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: (id) =>
            Effect.sync(() => {
              acquired.push(id)
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('abcdef12'))
        const result = yield* eb()
        expect(acquired).toHaveLength(1)
        expect(acquired[0]?.identity.id).toBe(result.identity.id)
        expect(acquired[0]?.identity.name).toBe(decodeDisplayNameSync('cc-abcdef12'))
      }),
    ))

  test('onAcquire does not fire when ensureBoundFor returns a cached entry', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        let fired = 0
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Effect.sync(() => {
              fired += 1
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('aa110110'))
        yield* eb()
        yield* eb()
        const eb2 = yield* cache.ensureBoundFor(sid('aa110110'))
        yield* eb2()
        expect(fired).toBe(1)
      }),
    ))

  test('onAcquire fires again on session_id transition after release-then-acquire', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const acquired: string[] = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: (id) =>
            Effect.sync(() => {
              acquired.push(id.identity.name)
            }),
        })
        const eb1 = yield* cache.ensureBoundFor(sid('a1f7a1f7'))
        yield* eb1()
        const eb2 = yield* cache.ensureBoundFor(sid('d0d0d0d0'))
        yield* eb2()
        expect(acquired).toEqual(['cc-a1f7a1f7', 'cc-d0d0d0d0'])
      }),
    ))

  test('onAcquire fires after sweepIdle release on re-acquire', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const acquired: string[] = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
          onAcquire: (id) =>
            Effect.sync(() => {
              acquired.push(id.identity.name)
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('7e7e7e7e'))
        yield* eb()
        yield* TestClock.adjust(Duration.millis(6000))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(acquired).toEqual(['cc-7e7e7e7e'])
        const eb2 = yield* cache.ensureBoundFor(sid('7e7e7e7e'))
        yield* eb2()
        expect(acquired).toEqual(['cc-7e7e7e7e', 'cc-7e7e7e7e'])
      }),
    ))

  test('onAcquire does not fire when acquire rejects', () =>
    runTest(
      Effect.gen(function* () {
        let fired = 0
        const cache = yield* createEphemeralIdentityCache({
          acquire: () =>
            Effect.fail(
              new IdentityError({ operation: 'acquire', cause: new Error('acquire boom') }),
            ),
          release: () => Effect.void,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Effect.sync(() => {
              fired += 1
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('fa11fa11'))
        const err = yield* captureError(eb())
        expect((err as Error).message).toMatch(/acquire boom/)
        expect(fired).toBe(0)
      }),
    ))

  test('onAcquire is awaited inline — the acquire result does not return until onAcquire resolves', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const reached = yield* Deferred.make<void>()
        const gate = yield* Deferred.make<void>()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Deferred.succeed(reached, undefined).pipe(Effect.zipRight(Deferred.await(gate))),
        })
        const eb = yield* cache.ensureBoundFor(sid('a7a17a17'))
        const fiber = yield* Effect.fork(eb())
        // onAcquire has begun but is parked on the gate — acquire must not have
        // returned yet.
        yield* Deferred.await(reached)
        expect(Option.isNone(yield* fiber.poll)).toBe(true)
        yield* Deferred.succeed(gate, undefined)
        const acquired = yield* Fiber.join(fiber)
        expect(acquired.identity.name).toBe(decodeDisplayNameSync('cc-a7a17a17'))
      }),
    ))

  test('onAcquire rejection surfaces to the caller and reverts the ensureBound to idle', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        let shouldThrow = true
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Effect.sync(() => {
              if (shouldThrow) throw new Error('catch-up boom')
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('e44ba4ce'))
        const err = yield* captureError(eb())
        expect((err as Error).message).toMatch(/catch-up boom/)
        expect([...cache.boundIdentityIds()]).toEqual([])
        shouldThrow = false
        yield* eb()
        expect(spy.acquireCalls).toEqual(['cc-e44ba4ce', 'cc-e44ba4ce'])
        expect([...cache.boundIdentityIds()]).toEqual([decodeIdentityIdSync('bot:cc-e44ba4ce')])
      }),
    ))

  test('lastUsedMs bumps on each ensureBoundFor for the same sid', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 5000,
        })
        const eb = yield* cache.ensureBoundFor(sid('b00b00b0'))
        yield* eb()
        // Bump activity at +4000 — within the idle window, so the stamp moves.
        yield* TestClock.adjust(Duration.millis(4000))
        yield* cache.ensureBoundFor(sid('b00b00b0'))
        // After bump, even at +6000 total the entry is only 2000ms idle.
        yield* TestClock.adjust(Duration.millis(2000))
        const nowMs = yield* Clock.currentTimeMillis
        yield* cache.sweepIdle(nowMs)
        expect(spy.releaseCalls).toEqual([])
      }),
    ))

  // ─── onAcquire hook ────────────────────────────────────────────────────

  test('onAcquire fires once per fresh slot, receives identity + project', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const calls: Array<{ name: string; project: string | undefined }> = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: (id, project) =>
            Effect.sync(() => {
              calls.push({ name: id.identity.name, project })
            }),
        })
        const eb1 = yield* cache.ensureBoundFor(sid('a5a5a5a5'), slug('commy'))
        yield* eb1()
        const eb2 = yield* cache.ensureBoundFor(sid('a5a5a5a5'), slug('commy'))
        yield* eb2()
        expect(calls).toEqual([{ name: 'cc-commy-a5a5a5a5', project: 'commy' }])
      }),
    ))

  test('onAcquire fires again for a new session_id (different slot)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const calls: Array<{ name: string; project: string | undefined }> = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: (id, project) =>
            Effect.sync(() => {
              calls.push({ name: id.identity.name, project })
            }),
        })
        const eb1 = yield* cache.ensureBoundFor(sid('a1f1a1f1'), slug('myproject-a'))
        yield* eb1()
        const eb2 = yield* cache.ensureBoundFor(sid('21121121'), slug('myproject-b'))
        yield* eb2()
        expect(calls).toEqual([
          { name: 'cc-myproject-a-a1f1a1f1', project: 'myproject-a' },
          { name: 'cc-myproject-b-21121121', project: 'myproject-b' },
        ])
      }),
    ))

  test('onAcquire receives undefined project when none was supplied', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        const calls: Array<{ project: string | undefined }> = []
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: (_id, project) =>
            Effect.sync(() => {
              calls.push({ project })
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('ba4ebbbb'))
        yield* eb()
        expect(calls).toEqual([{ project: undefined }])
      }),
    ))

  test('onAcquire rejection propagates through ensureBound and clears the slot for retry', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        let fires = 0
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Effect.sync(() => {
              fires += 1
              if (fires === 1) throw new Error('boom')
            }),
        })
        const eb = yield* cache.ensureBoundFor(sid('7e747ed1'), slug('commy'))
        const err = yield* captureError(eb())
        expect((err as Error).message).toMatch(/boom/)
        expect(eb.current()).toBeUndefined()
        // Second attempt — same EnsureBound (sid hasn't changed). The state
        // machine reverted to idle, so a new acquire round-trip runs; the
        // hook is the same wrapped acquire so onAcquire fires again, this
        // time without throwing.
        yield* eb()
        expect(fires).toBe(2)
        expect(eb.current()?.identity.name).toBe(decodeDisplayNameSync('cc-commy-7e747ed1'))
      }),
    ))

  test('onAcquire is NOT invoked by ensureBoundFor(undefined) (no slot, no acquire)', () =>
    runTest(
      Effect.gen(function* () {
        const spy = buildAdapterSpy()
        let fires = 0
        const cache = yield* createEphemeralIdentityCache({
          acquire: spy.acquire,
          release: spy.release,
          idleReleaseMs: 60_000,
          onAcquire: () =>
            Effect.sync(() => {
              fires += 1
            }),
        })
        const eb = yield* cache.ensureBoundFor(undefined)
        const err = yield* captureError(eb())
        expect(err).toBeInstanceOf(UnboundEphemeralSession)
        expect(fires).toBe(0)
      }),
    ))
})
