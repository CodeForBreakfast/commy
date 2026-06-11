import type { AcquiredIdentity, BotName } from '@codeforbreakfast/core/ports'
import { Cause, Deferred, Effect, Exit, Ref } from 'effect'

export interface EnsureBoundDeps<E> {
  /**
   * Bind the plugin to an identity by name. Production passes
   * `adapter.identity.acquire`; tests pass a spy. Idempotent semantics
   * are NOT assumed here — this helper is the source of de-duplication
   * via a single-flight Deferred.
   */
  readonly acquire: (name: BotName) => Effect.Effect<AcquiredIdentity, E>
  /**
   * Identity name to bind to. Resolved upstream — `server.ts` uses
   * `parsed.botName` verbatim for persistent bots, while the identity
   * cache mints the ephemeral `cc-<suffix>` name per call via
   * `composeBotName`.
   */
  readonly name: BotName
}

/**
 * Lazy-acquire wrapper for ephemeral Claude Code sessions
 * (ass-220u). Production callers wrap `post`/`react`/`unreact` with
 * this so the first attribution-producing tool call triggers
 * `adapter.identity.acquire(name)` once; subsequent calls await the
 * cached result.
 *
 * On failure the state resets to idle so the next caller rebuilds it —
 * no sticky "acquire-broken" state. Concurrent first callers see the
 * same outcome (success or failure) via a shared Deferred; a *fresh*
 * call after a failure issues a brand-new acquire round-trip.
 *
 * `current()` is a passive accessor — it never triggers acquire. The
 * release-shutdown body reads it to decide whether to call
 * `identity.release()` on exit; pre-acquire ephemeral sessions skip
 * release entirely.
 *
 * The acquire chain is Effect-native; the call site stays Promise-shaped
 * because the `IdentityCache` interface its consumers depend on is still
 * Promise/sync. `ensureBound()` is the single runtime call for this
 * island — it runs the acquire Effect and squashes the failure cause so
 * the caller sees the raw port error, not a FiberFailure wrapper.
 */
export interface EnsureBound {
  (): Promise<AcquiredIdentity>
  /** AcquiredIdentity if acquire has resolved, undefined otherwise. */
  current(): AcquiredIdentity | undefined
}

/**
 * Internal state machine. The three variants are mutually exclusive
 * — at any instant the helper is in exactly one. Encoding as a tagged
 * union (instead of two independent slots) makes the invariant explicit
 * to the compiler and rules out "pending AND acquired both set" as an
 * unrepresentable state. Held in a plain Ref: the idle→pending swap is a
 * single synchronous `Ref.modify`, so the first caller wins the race and
 * runs the acquire inline while concurrent callers await its Deferred —
 * no extra fiber, no semaphore.
 */
type AcquireState<E> =
  | { readonly kind: 'idle' }
  | { readonly kind: 'pending'; readonly deferred: Deferred.Deferred<AcquiredIdentity, E> }
  | { readonly kind: 'acquired'; readonly identity: AcquiredIdentity }

type AcquireDecision<E> =
  | { readonly kind: 'have'; readonly identity: AcquiredIdentity }
  | { readonly kind: 'await'; readonly deferred: Deferred.Deferred<AcquiredIdentity, E> }
  | { readonly kind: 'run'; readonly deferred: Deferred.Deferred<AcquiredIdentity, E> }

export const createEnsureBound = <E>(deps: EnsureBoundDeps<E>): EnsureBound => {
  const stateRef = Effect.runSync(Ref.make<AcquireState<E>>({ kind: 'idle' }))

  const acquire: Effect.Effect<AcquiredIdentity, E> = Effect.gen(function* () {
    // A fresh Deferred per call; only the idle winner installs and uses
    // its own, the rest discard theirs and await the pending one.
    const fresh = yield* Deferred.make<AcquiredIdentity, E>()
    const decision = yield* Ref.modify(
      stateRef,
      (state): readonly [AcquireDecision<E>, AcquireState<E>] => {
        if (state.kind === 'acquired') {
          return [{ kind: 'have', identity: state.identity }, state]
        }
        if (state.kind === 'pending') {
          return [{ kind: 'await', deferred: state.deferred }, state]
        }
        return [
          { kind: 'run', deferred: fresh },
          { kind: 'pending', deferred: fresh },
        ]
      },
    )
    if (decision.kind === 'have') return decision.identity
    if (decision.kind === 'await') return yield* Deferred.await(decision.deferred)
    return yield* deps.acquire(deps.name).pipe(
      Effect.onExit((exit) =>
        Ref.set(
          stateRef,
          Exit.match(exit, {
            onFailure: () => ({ kind: 'idle' as const }),
            onSuccess: (identity) => ({ kind: 'acquired' as const, identity }),
          }),
        ).pipe(Effect.zipRight(Deferred.done(decision.deferred, exit))),
      ),
    )
  })

  const ensureBound = (): Promise<AcquiredIdentity> =>
    Effect.runPromiseExit(acquire).then((exit) =>
      Exit.isSuccess(exit) ? exit.value : Promise.reject(Cause.squash(exit.cause)),
    )

  ensureBound.current = (): AcquiredIdentity | undefined => {
    const state = Effect.runSync(Ref.get(stateRef))
    return state.kind === 'acquired' ? state.identity : undefined
  }

  return ensureBound
}
