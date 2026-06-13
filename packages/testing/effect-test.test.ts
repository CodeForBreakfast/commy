import { describe, expect, test } from 'bun:test'
import { captureLogger } from '@commy/core/logging'
import { Context, Data, Effect, Layer, Ref, TestClock, TestContext } from 'effect'
import { effectTest, runTestEffect } from './effect-test.ts'

class Boom extends Data.TaggedError('Boom')<{ readonly detail: string }> {}

describe('runTestEffect', () => {
  test('a succeeding Effect resolves with its value', async () => {
    expect(await runTestEffect(() => Effect.succeed(42))).toBe(42)
  })

  test('a typed failure rejects with the raw error, not a FiberFailure', async () => {
    const error = await runTestEffect(() => Effect.fail(new Boom({ detail: 'boom' }))).then(
      () => undefined,
      (caught: unknown) => caught,
    )
    expect(error).toBeInstanceOf(Boom)
    expect((error as Boom).detail).toBe('boom')
    expect((error as object).constructor.name).not.toBe('FiberFailure')
  })

  test('a defect thrown in the body surfaces the original object, not a FiberFailure', async () => {
    const thrown = new Error('expected 1 to be 2')
    const error = await runTestEffect(() =>
      Effect.gen(function* () {
        yield* Effect.void
        throw thrown
      }),
    ).then(
      () => undefined,
      (caught: unknown) => caught,
    )
    expect(error).toBe(thrown)
  })

  test('per-test Scope: finalizers run after the body completes', async () => {
    const events: Array<string> = []
    await runTestEffect(() =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Effect.sync(() => events.push('finalized')))
        events.push('body')
      }),
    )
    expect(events).toEqual(['body', 'finalized'])
  })

  test('per-test Scope: finalizers run even when the body fails', async () => {
    const events: Array<string> = []
    await runTestEffect(() =>
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => Effect.sync(() => events.push('finalized')))
        return yield* new Boom({ detail: 'kaboom' })
      }),
    ).catch(() => undefined)
    expect(events).toEqual(['finalized'])
  })

  test('layer provision: a service from options.layer satisfies the body R', async () => {
    class Greeting extends Context.Tag('test/Greeting')<Greeting, string>() {}
    const captured: Array<string> = []
    await runTestEffect(
      () =>
        Effect.gen(function* () {
          captured.push(yield* Greeting)
        }),
      { layer: Layer.succeed(Greeting, 'hello') },
    )
    expect(captured).toEqual(['hello'])
  })

  test('layer provision: captureLogger captures the body diagnostics', async () => {
    const lines: Array<string> = []
    await runTestEffect(() => Effect.logInfo('diagnostic'), { layer: captureLogger(lines) })
    expect(lines).toContain('diagnostic')
  })

  test('layer provision: TestContext makes TestClock available to the body', async () => {
    const observed = await runTestEffect(
      () =>
        Effect.gen(function* () {
          yield* TestClock.setTime(5_000)
          const ref = yield* Ref.make(0)
          yield* Effect.sleep('5 seconds').pipe(Effect.zipRight(Ref.set(ref, 1)), Effect.fork)
          yield* TestClock.adjust('5 seconds')
          return yield* Ref.get(ref)
        }),
      { layer: TestContext.TestContext },
    )
    expect(observed).toBe(1)
  })

  test('layer build failure surfaces the raw layer error', async () => {
    class Resource extends Context.Tag('test/Resource')<Resource, string>() {}
    const failingLayer = Layer.effect(
      Resource,
      Effect.fail(new Boom({ detail: 'layer build failed' })),
    )
    const error = await runTestEffect(() => Effect.succeed(0), { layer: failingLayer }).then(
      () => undefined,
      (caught: unknown) => caught,
    )
    expect(error).toBeInstanceOf(Boom)
    expect((error as Boom).detail).toBe('layer build failed')
  })
})

describe('effectTest', () => {
  effectTest('registers a passing test from a returning Effect', () =>
    Effect.gen(function* () {
      expect(yield* Effect.succeed('ok')).toBe('ok')
    }),
  )

  class Greeting extends Context.Tag('test/EffectTestGreeting')<Greeting, string>() {}

  effectTest(
    'provides options.layer to the body',
    () =>
      Effect.gen(function* () {
        expect(yield* Greeting).toBe('hi')
      }),
    { layer: Layer.succeed(Greeting, 'hi') },
  )
})
