import type { AcquiredIdentity, BindError } from '@commy/core/ports'
import { UnboundEphemeralSession } from '@commy/core/ports'
import { Context, Effect, Layer, Option, Ref } from 'effect'
import type { IdentityCache } from './identity-cache.ts'
import { currentSessionContext } from './session-context.ts'

/**
 * How this seat obtains an identity when a write reaches for a bound
 * credential. Resolving it is the whole of "bind on demand": the adapter asks,
 * this answers or refuses.
 */
export type BindOnDemand = Effect.Effect<AcquiredIdentity, BindError>

/**
 * The bind strategy as a late-installed requirement.
 *
 * The adapter is constructed before the identity cache exists — the cache is
 * built FROM `adapter.identity.acquire` — so the binder cannot be handed over
 * at adapter construction without a cycle. This holder breaks it: the adapter
 * reads through it, the wiring layer installs into it once the cache is up.
 *
 * A `Ref<Option<…>>` rather than a `Deferred`, deliberately, and the
 * difference is the whole point. `Deferred.await` would PARK a caller that
 * arrives before installation, converting a misconfiguration into a hang. An
 * unfilled `Ref` reads `None` immediately and the seam refuses with
 * {@link UnboundEphemeralSession} — the same fail-fast contract the no-session
 * path already has. Nothing on this path may block waiting for an identity.
 */
export class SessionBinder extends Context.Tag('commy/mcp/SessionBinder')<
  SessionBinder,
  Ref.Ref<Option.Option<BindOnDemand>>
>() {}

export const SessionBinderLive: Layer.Layer<SessionBinder> = Layer.effect(
  SessionBinder,
  Ref.make<Option.Option<BindOnDemand>>(Option.none()),
)

/**
 * The `bindOnDemand` an adapter is built with: read the installed binder and
 * run it, refusing immediately when none is installed.
 */
export const bindThrough = (ref: Ref.Ref<Option.Option<BindOnDemand>>): BindOnDemand =>
  Ref.get(ref).pipe(
    Effect.flatMap(
      Option.match({
        onNone: (): BindOnDemand =>
          Effect.fail(
            new UnboundEphemeralSession({
              message:
                'commy: this seat cannot obtain an identity — no bind strategy is ' +
                'installed. This is a wiring defect, not a caller error.',
            }),
          ),
        onSome: (binder: BindOnDemand) => binder,
      }),
    ),
  )

/** Install the binder the wiring layer built once the identity cache exists. */
export const installBinder = (
  ref: Ref.Ref<Option.Option<BindOnDemand>>,
  binder: BindOnDemand,
): Effect.Effect<void> => Ref.set(ref, Option.some(binder))

/**
 * The bind strategy itself: resolve the calling session's `EnsureBound` from
 * the cache, then run it. THE definition — production and every test rig
 * install this same one, so a rig cannot accidentally exercise a seam that
 * differs from the one that ships.
 *
 * `currentSessionContext` is read, never awaited. A call carrying no session
 * id resolves to the cache's unbound stub and fails immediately; nothing here
 * parks a caller waiting for an identity to arrive.
 */
export const binderFor = (identityCache: IdentityCache): BindOnDemand =>
  currentSessionContext.pipe(
    Effect.flatMap((ctx) => identityCache.ensureBoundFor(ctx.sessionId, ctx.project)),
    Effect.flatMap((ensureBound) => ensureBound()),
  )
