/**
 * Effect-native bun:test harness.
 *
 * Lets a test body **return** an Effect instead of hand-wrapping
 * `Effect.runPromise(Effect.scoped(Effect.gen(...)))` at every call site.
 * The harness owns three concerns the wrapper used to repeat by hand:
 *
 * 1. **Per-test Scope** — the body runs inside `Effect.scoped`, so a test can
 *    `Effect.addFinalizer` / acquire a scoped resource and its finalizers run
 *    when the test ends, on both the success and failure paths.
 * 2. **Per-test layer provision** — `options.layer` is how a test threads its
 *    requirements (`R`) without a global: pass `captureLogger(lines)`,
 *    `TestContext.TestContext` (for `TestClock`), or the stub HttpClient layer,
 *    merged into one layer. The base harness stays on the live clock; TestClock
 *    is opt-in via that layer, so live-clock tests are unaffected.
 * 3. **Real error surfacing** — `Effect.runPromise` rejects with a
 *    `FiberFailure` that buries the underlying error (the promise-boundary
 *    gotcha). The harness runs `Effect.runPromiseExit` and, on failure, throws
 *    `Cause.squash(cause)` — the raw failure value or defect. `squash` (rather
 *    than `Cause.prettyErrors`) deliberately returns the original thrown object
 *    untouched, so a failed `expect(...)` reaches bun's reporter with its
 *    matcher diff intact instead of a re-wrapped error.
 */

import { test } from 'bun:test'
import { Cause, Effect, Exit, type Layer, type Scope } from 'effect'

interface LayerOption<RIn, RErr> {
  readonly layer: Layer.Layer<RIn, RErr>
}

interface EffectTestOptions<RIn, RErr> extends Partial<LayerOption<RIn, RErr>> {
  readonly timeout?: number
}

/**
 * Run a test-body Effect to a resolved/rejected Promise: a successful Effect
 * resolves with its value, a failed one (typed error or defect) rejects with
 * the squashed cause. The engine behind {@link effectTest}, exported so the
 * pass/fail mapping is itself testable and so a caller can drive a returned
 * Effect inside a custom `test.each` / table.
 */
export function runTestEffect<A, E>(body: () => Effect.Effect<A, E, Scope.Scope>): Promise<A>
export function runTestEffect<A, E, RIn, RErr>(
  body: () => Effect.Effect<A, E, RIn | Scope.Scope>,
  options: LayerOption<RIn, RErr>,
): Promise<A>
export function runTestEffect<A, E, RIn, RErr>(
  body: () => Effect.Effect<A, E, RIn | Scope.Scope>,
  options?: LayerOption<RIn, RErr>,
): Promise<A> {
  const scoped = Effect.scoped(Effect.suspend(body))
  const provided =
    options === undefined
      ? (scoped as Effect.Effect<A, E | RErr>)
      : Effect.provide(scoped, options.layer)
  return Effect.runPromiseExit(provided).then((exit) =>
    Exit.isSuccess(exit) ? exit.value : Promise.reject(Cause.squash(exit.cause)),
  )
}

/**
 * Register a bun:test test whose body returns an Effect. The Effect runs inside
 * a per-test Scope with `options.layer` provided; success passes, failure fails
 * with the real error surfaced (see {@link runTestEffect}).
 */
export function effectTest<A, E>(
  name: string,
  body: () => Effect.Effect<A, E, Scope.Scope>,
  options?: { readonly timeout?: number },
): void
export function effectTest<A, E, RIn, RErr>(
  name: string,
  body: () => Effect.Effect<A, E, RIn | Scope.Scope>,
  options: EffectTestOptions<RIn, RErr> & LayerOption<RIn, RErr>,
): void
export function effectTest<A, E, RIn, RErr>(
  name: string,
  body: () => Effect.Effect<A, E, RIn | Scope.Scope>,
  options?: EffectTestOptions<RIn, RErr>,
): void {
  test(
    name,
    () =>
      options?.layer === undefined
        ? runTestEffect(body as () => Effect.Effect<A, E, Scope.Scope>)
        : runTestEffect(body, { layer: options.layer }),
    options?.timeout,
  )
}
