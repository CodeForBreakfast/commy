import type {
  AcquiredIdentity,
  BotName,
  IdentityError,
  IdentityId,
  UnknownIdentity,
} from '@commy/core/ports'
import { Clock, Data, Effect, SynchronizedRef } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { composeBotName } from './bootstrap.ts'
import type { EnsureBound } from './ensure-bound.ts'
import { createEnsureBound } from './ensure-bound.ts'

/**
 * Error channel of the `EnsureBound` an `IdentityCache` hands out. A real
 * slot acquires through the adapter (`UnknownIdentity | IdentityError`); the
 * ephemeral unbound stub fails with `UnboundEphemeralSession`. The interface
 * surfaces the union so consumers can run the bind Effect and `Effect.catchTag`
 * whichever case applies.
 */
export type EnsureBoundError = UnknownIdentity | IdentityError | UnboundEphemeralSession

/**
 * Identity binding strategy for a single MCP child (ass-2dhb).
 *
 * Two implementations:
 *   - **Single (persistent mode)**: wraps one pre-built `ensureBound` and
 *     returns it for every `ensureBoundFor` call. `COMMY_BOT_NAME`
 *     is set; the bot lives for the child's lifetime.
 *   - **Ephemeral**: at-most-one slot keyed by `session_id`. A new sid
 *     releases the prior identity (Zulip deactivates the bot) before
 *     acquiring the new one. Idle sweep releases the slot after N
 *     minutes of inactivity. Slot collapses Path 1 of ass-2dhb's bead
 *     body — see the 1-slot simplification note for why this isn't a
 *     `Map<sid, AcquiredIdentity>`.
 *
 * Consumers (`tools.ts`, `event-pump.ts`, `release-shutdown.ts`) depend
 * only on the `IdentityCache` interface.
 */
export interface IdentityCache {
  /**
   * Return an `EnsureBound` appropriate for the given session_id.
   *
   * Persistent: ignores sid and project, always returns the singleton.
   *
   * Ephemeral:
   *   - `state === undefined` → mint a fresh `EnsureBound` for
   *     `cc-[<project>-]<sid.slice(0,8)>` and store it. `project` is
   *     a per-call arg (ass-v7b4): the calling session's project,
   *     derived from the hook-injected cwd at the boundary in
   *     `tools.ts`. Process-level project state is gone — that was
   *     the leak source.
   *   - `state.sessionId === sid` → bump `lastUsedMs`, return existing
   *     (project arg is ignored; the slot is named once).
   *   - `state.sessionId !== sid` → release the prior identity (if
   *     acquired), then mint a fresh entry for the new sid using
   *     the new call's `project`.
   *   - `sid === undefined` → return the unbound stub regardless of
   *     slot state (comms-67j). We can't tell "same conversation, hook
   *     didn't fire" apart from "different conversation, hook didn't
   *     fire" — and the latter leaked the prior conversation's seat
   *     into the new one across `/clear`. Stub's `current()` reads
   *     unbound; running its bind Effect fails with the "missing
   *     session_id" error. The existing slot is left intact: a follow-up
   *     call with the original sid still reaches the same binding.
   */
  ensureBoundFor(
    sessionId: SessionId | undefined,
    project?: ProjectSlug,
  ): Effect.Effect<EnsureBound<EnsureBoundError>>
  /**
   * IDs of currently-bound identities. Size is 0 or 1 in V1 — the set
   * shape leaves room for future N-identity adapters (see Path 2 note
   * on ass-2dhb) without churning consumers.
   *
   * Consumers:
   *   - `event-pump`/`narrow-set` use this for the mentions narrow
   *     and the rendered `mentioned` meta flag.
   */
  boundIdentityIds(): ReadonlySet<IdentityId>
  /**
   * Release the currently-bound identity (if any). Invoked from the
   * shutdown body when the MCP child is exiting; idempotent.
   */
  releaseAllBound(): Effect.Effect<void>
  /**
   * If the slot's last activity is older than `idleReleaseMs`, release
   * the bound identity and clear the slot. Called by a periodic timer
   * the server boot wires up in ephemeral mode. Persistent mode is a
   * no-op.
   */
  sweepIdle(nowMs: number): Effect.Effect<void>
}

export interface SingleIdentityCacheDeps {
  readonly ensureBound: EnsureBound<UnknownIdentity | IdentityError>
}

export const createSingleIdentityCache = (deps: SingleIdentityCacheDeps): IdentityCache => {
  const ensureBound = deps.ensureBound
  return {
    ensureBoundFor: () => Effect.succeed(ensureBound),
    boundIdentityIds: () => {
      const current = ensureBound.current()
      return current === undefined ? new Set() : new Set([current.identity.id])
    },
    // The persistent-mode release path runs through `release-shutdown.ts`
    // (which calls `adapter.identity.release()` directly). The cache has no
    // extra teardown to do; keeping these present means server-boot wiring
    // stays symmetric across the two factories.
    releaseAllBound: () => Effect.void,
    // Persistent identities live for the child's lifetime.
    sweepIdle: () => Effect.void,
  }
}

export interface EphemeralIdentityCacheDeps {
  /**
   * Adapter-side acquire (`adapter.identity.acquire`). The cache calls
   * this through each entry's `ensureBound`; failure plumbs back to
   * the caller and clears the entry per `createEnsureBound`'s semantics.
   */
  readonly acquire: (
    name: BotName,
  ) => Effect.Effect<AcquiredIdentity, UnknownIdentity | IdentityError>
  /**
   * Adapter-side release (`adapter.identity.release`). Called on
   * session transitions (release-then-acquire) and idle sweeps. The
   * adapter is single-bound, so there's no per-name release argument
   * — release affects whichever identity is currently bound.
   */
  readonly release: () => Effect.Effect<void>
  /** Idle threshold for `sweepIdle`. */
  readonly idleReleaseMs: number
  /**
   * Post-acquire hook (comms-ae4, comms-iyf). Fires after every
   * successful adapter-side acquire — both the fresh-slot path (first
   * call for a sid) and the different-sid transition path
   * (release-then-acquire). Receives the freshly-acquired identity
   * plus the per-call `project` slug the slot was minted under (same
   * value passed to `ensureBoundFor(sid, project)`), so the callback
   * can key per-identity state (cursor lookups for catch-up) and
   * per-project state (project broadcast topic for Type-2 defaults)
   * without going back through `boundIdentityIds()` or `projectForCwd`.
   *
   * Sequenced inline as part of the acquire Effect — the caller's
   * first attribution-producing tool call does not return until
   * `onAcquire` has completed. The plugin's catch-up wiring
   * (comms-ae4) relies on this so a replayed mention surfaces
   * alongside the tool result that triggered acquire.
   *
   * If the callback fails, the failure is threaded back to the
   * caller of `ensureBound()` and the wrapped acquire state reverts
   * to idle — the next `ensureBound()` for the same sid will issue
   * a brand-new adapter-side acquire (and another `onAcquire` call).
   * Catch the failure inside the callback if it should not refuse the
   * user's tool call; server.ts's catch-up and Type-2-defaults
   * wirings both do exactly that.
   */
  readonly onAcquire?: (
    acquired: AcquiredIdentity,
    project: ProjectSlug | undefined,
  ) => Effect.Effect<void>
}

interface Slot {
  readonly sessionId: SessionId
  readonly ensureBound: EnsureBound<UnknownIdentity | IdentityError>
  readonly lastUsedMs: number
}

/**
 * Refusal raised when an ephemeral tool call arrives without a usable
 * `session_id`. A tagged error (not a bare `Error`) so the MCP edge
 * surfaces the `UnboundEphemeralSession:` discriminator instead of the
 * naked message — `Data.TaggedError` sets `name === _tag`, which the
 * edge reshape (tools.ts) keys on — and so callers can `Effect.catchTag`
 * it on the bind Effect's typed E channel.
 */
export class UnboundEphemeralSession extends Data.TaggedError('UnboundEphemeralSession')<{
  readonly message: string
}> {}

const unboundStub: EnsureBound<UnboundEphemeralSession> = Object.assign(
  (): Effect.Effect<AcquiredIdentity, UnboundEphemeralSession> =>
    Effect.fail(
      new UnboundEphemeralSession({
        message:
          'commy: ephemeral mode requires a session_id; the plugin hook ' +
          'should inject it from PreToolUse stdin. For non-CC clients, pass ' +
          'session_id explicitly in the tool call arguments.',
      }),
    ),
  { current: (): AcquiredIdentity | undefined => undefined },
)

export const createEphemeralIdentityCache = (
  deps: EphemeralIdentityCacheDeps,
): Effect.Effect<IdentityCache> =>
  Effect.map(SynchronizedRef.make<Slot | undefined>(undefined), (slotRef): IdentityCache => {
    const deriveBotName = (sessionId: SessionId, project: ProjectSlug | undefined): BotName =>
      composeBotName({
        sessionId,
        ...(project !== undefined ? { project } : {}),
      })

    const wrapWithOnAcquire =
      (project: ProjectSlug | undefined) =>
      (n: BotName): Effect.Effect<AcquiredIdentity, UnknownIdentity | IdentityError> =>
        deps
          .acquire(n)
          .pipe(
            Effect.tap((acquired) =>
              deps.onAcquire !== undefined ? deps.onAcquire(acquired, project) : Effect.void,
            ),
          )

    // Capture the prior slot's release into the new slot's first acquire:
    // the release-then-acquire only fires if the prior identity actually
    // bound. `priorEnsure.current()` is read when the new acquire runs, so a
    // prior still mid-acquire (current() === undefined) skips release.
    const acquireForTransition =
      (
        priorEnsure: EnsureBound<UnknownIdentity | IdentityError>,
        project: ProjectSlug | undefined,
      ) =>
      (n: BotName): Effect.Effect<AcquiredIdentity, UnknownIdentity | IdentityError> =>
        priorEnsure.current() !== undefined
          ? deps.release().pipe(Effect.zipRight(wrapWithOnAcquire(project)(n)))
          : wrapWithOnAcquire(project)(n)

    const mintSlot = (
      sessionId: SessionId,
      project: ProjectSlug | undefined,
      nowMs: number,
      prior: Slot | undefined,
    ): Effect.Effect<readonly [EnsureBound<UnknownIdentity | IdentityError>, Slot]> => {
      const name = deriveBotName(sessionId, project)
      const acquire =
        prior !== undefined
          ? acquireForTransition(prior.ensureBound, project)
          : wrapWithOnAcquire(project)
      return createEnsureBound({ acquire, name }).pipe(
        Effect.map(
          (ensureBound) => [ensureBound, { sessionId, ensureBound, lastUsedMs: nowMs }] as const,
        ),
      )
    }

    // Atomic read-release-clear: the effectful release runs inside the
    // SynchronizedRef's serialised region, so it never clobbers a slot a
    // concurrent `ensureBoundFor` acquired between the read and the release
    // (the lost-update window the plain-`let` version raced through).
    const releaseBoundSlot: Effect.Effect<void> = SynchronizedRef.updateEffect(slotRef, (slot) =>
      Effect.as(
        slot !== undefined && slot.ensureBound.current() !== undefined
          ? deps.release()
          : Effect.void,
        undefined,
      ),
    )

    return {
      // Reads the activity stamp from Effect's `Clock` (the same source
      // `forkIdleSweep` feeds `sweepIdle`), so the idle comparison is
      // consistent and `TestClock`-drivable. The slot swap is serialised
      // through `SynchronizedRef.modifyEffect`, which also allocates the new
      // entry's `EnsureBound` inside the atomic region.
      ensureBoundFor: (sessionId, project): Effect.Effect<EnsureBound<EnsureBoundError>> =>
        sessionId === undefined
          ? // Refuse to surface the slot when no sid was supplied (comms-67j).
            // A missing sid means the PreToolUse hook either didn't fire or
            // didn't see one in CC's event — and post-`/clear` that meant the
            // prior conversation's seat leaked into the new one via
            // `current_identity`/`post`/`react`/`unreact`. The slot itself is
            // preserved: a follow-up call with the original sid still reaches
            // the same binding.
            Effect.succeed(unboundStub)
          : Clock.currentTimeMillis.pipe(
              Effect.flatMap((nowMs) =>
                SynchronizedRef.modifyEffect(slotRef, (slot) => {
                  if (slot !== undefined && slot.sessionId === sessionId) {
                    const bumped: Slot = { ...slot, lastUsedMs: nowMs }
                    return Effect.succeed([slot.ensureBound, bumped] as const)
                  }
                  // Fresh sid or a transition off the prior slot. The prior
                  // slot's release is captured into the new entry's first
                  // acquire (see `acquireForTransition`).
                  return mintSlot(sessionId, project, nowMs, slot)
                }),
              ),
            ),
      boundIdentityIds: () => {
        const slot = Effect.runSync(SynchronizedRef.get(slotRef))
        if (slot === undefined) return new Set()
        const current = slot.ensureBound.current()
        return current === undefined ? new Set() : new Set([current.identity.id])
      },
      releaseAllBound: () => releaseBoundSlot,
      sweepIdle: (nowMs) =>
        // Idle check and release are one atomic region — a slot a request
        // fiber bumped (or replaced) between fork and sweep is re-read here,
        // so the sweep only releases a slot still genuinely idle past the
        // threshold; otherwise it leaves the slot untouched.
        SynchronizedRef.updateEffect(slotRef, (slot) =>
          slot !== undefined &&
          slot.ensureBound.current() !== undefined &&
          nowMs - slot.lastUsedMs > deps.idleReleaseMs
            ? Effect.as(deps.release(), undefined)
            : Effect.succeed(slot),
        ),
    }
  })
