import type { InboxError, MessageInbox } from '@commy/core/ports'
import type { PlatformError } from '@effect/platform/Error'
import { Effect, Option, type ParseResult } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import type { NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'

/**
 * Restore (or seed) a session's narrow set on its first action.
 *
 * Resume vs fresh is decided by the per-session_id subscription store, exactly
 * as the mentions cursor decides resume vs fresh for catch-up:
 *   - store absent → fresh: register the Type-2 defaults, leaving whatever
 *     `COMMY_SUBSCRIBE` seeded at boot in place.
 *   - store present → resume: replace the narrow set with the persisted intents
 *     (including prior unsubscribes — a dropped default stays dropped) and wire
 *     each on the substrate. The defaults are NOT re-applied. An empty persisted
 *     set is honoured verbatim: the session resumes hearing nothing.
 *
 * This runs once per session_id, before any persistence write, so the store's
 * presence is a true resume signal — never a file this same process just wrote.
 */
export interface SubscriptionRestoreDeps {
  readonly subscriptionStore: Pick<SubscriptionStore, 'read'>
  readonly narrowSet: NarrowSet
  readonly inbox: Pick<MessageInbox, 'subscribe'>
  readonly registerDefaults: (project: ProjectSlug | undefined) => Effect.Effect<void>
}

const applyRestored = (
  deps: Pick<SubscriptionRestoreDeps, 'narrowSet' | 'inbox'>,
  intents: ReadonlyArray<SubscribeIntent>,
): Effect.Effect<void, InboxError> =>
  Effect.sync(() => deps.narrowSet.replace(intents)).pipe(
    Effect.zipRight(
      Effect.forEach(intents, (intent) => deps.inbox.subscribe(intentToTarget(intent)), {
        discard: true,
      }),
    ),
  )

export const restoreOrSeedSubscriptions = (
  deps: SubscriptionRestoreDeps,
  sessionId: SessionId,
  project: ProjectSlug | undefined,
): Effect.Effect<void, PlatformError | ParseResult.ParseError | InboxError> =>
  deps.subscriptionStore.read(sessionId).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => deps.registerDefaults(project),
        onSome: (intents) => applyRestored(deps, intents),
      }),
    ),
  )

/**
 * Restore a resuming session's narrow set — the resume half of
 * {@link restoreOrSeedSubscriptions} with the fresh-session seed deliberately
 * omitted. A passive `current_identity` reachability check (the natural, and
 * often only, first move of a resumed block-and-sleep seat) routes through
 * this so the narrow set is live before the seat parks — without registering
 * the acquire-gated Type-2 defaults a fresh ephemeral session must not get
 * until it actually attributes something.
 *
 * Returns whether a restore happened: `true` when the store was present (a
 * true resume, now rehydrated), `false` when absent (a fresh session — left
 * untouched for the post-acquire seed to handle). The caller keys its
 * once-per-session memo on this, so a fresh session's later acquire still
 * seeds rather than being pre-empted by this no-op read.
 */
export const restoreSubscriptions = (
  deps: Pick<SubscriptionRestoreDeps, 'subscriptionStore' | 'narrowSet' | 'inbox'>,
  sessionId: SessionId,
): Effect.Effect<boolean, PlatformError | ParseResult.ParseError | InboxError> =>
  deps.subscriptionStore.read(sessionId).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.succeed(false),
        onSome: (intents) => applyRestored(deps, intents).pipe(Effect.as(true)),
      }),
    ),
  )

/**
 * Persist the current narrow-set snapshot under the session_id. Called after
 * every runtime `subscribe`/`unsubscribe` mutation so a later resume restores
 * the exact set — defaults included, unsubscribes included.
 */
export const persistSubscriptions = (
  store: Pick<SubscriptionStore, 'write'>,
  narrowSet: NarrowSet,
  sessionId: SessionId,
): Effect.Effect<void, PlatformError> => store.write(sessionId, narrowSet.intents())
