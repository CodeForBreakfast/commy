import type { InboxError, MessageInbox } from '@commy/core/ports'
import type { PlatformError } from '@effect/platform/Error'
import { Deferred, Effect, Option, type ParseResult } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import type { NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'

/**
 * Restore and seed are two independent reactions to a session's narrow set,
 * split apart for the reactive core (comms-k7cv). Resume vs fresh is decided by
 * the per-session_id subscription store, exactly as the mentions cursor decides
 * resume vs fresh for catch-up:
 *   - store present → resume: {@link restoreSubscriptions} replaces the narrow
 *     set with the persisted intents (including prior unsubscribes — a dropped
 *     default stays dropped) and wires each on the substrate. An empty persisted
 *     set is honoured verbatim: the session resumes hearing nothing. Fired the
 *     moment a feeder delivers the session_id, via {@link makeSessionRestore}'s
 *     `Deferred` latch — once per session_id, before any persistence write, so
 *     the store's presence is a true resume signal, never a file this same
 *     process just wrote.
 *   - store absent → fresh: {@link seedDefaultsIfFresh} registers the
 *     acquire-gated Type-2 defaults, leaving whatever `COMMY_SUBSCRIBE` seeded at
 *     boot in place. Store-gated and restore-free, so it never rehydrates a
 *     resumed set and needs no once-per-session memo.
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

/**
 * Seed the acquire-gated Type-2 defaults, but only for a fresh session (store
 * absent). The seed half of the old restore-or-seed, split out for the reactive
 * core (comms-k7cv): restore is now a reaction to the session_id via
 * {@link makeSessionRestore}, leaving seeding as its own store-gated step. The
 * deps carry no `narrowSet`/`inbox`, so this structurally CANNOT restore — a
 * resumed session (store present, even an empty persisted set) gets nothing from
 * here; its set is rehydrated by the restore reaction instead.
 *
 * Idempotent by construction: the store's presence is the guard, and once the
 * session subscribes anything the persist write makes the store present, so a
 * later call is a no-op. This is why the reactive core needs no `restoredSessions`
 * memo for the seed — the store gates it.
 */
export const seedDefaultsIfFresh = (
  deps: Pick<SubscriptionRestoreDeps, 'subscriptionStore' | 'registerDefaults'>,
  sessionId: SessionId,
  project: ProjectSlug | undefined,
): Effect.Effect<void, PlatformError | ParseResult.ParseError> =>
  deps.subscriptionStore.read(sessionId).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => deps.registerDefaults(project),
        onSome: () => Effect.void,
      }),
    ),
  )

/**
 * Restore a resuming session's narrow set — the resume half of the old
 * restore-or-seed, with the fresh-session seed deliberately omitted (that is
 * {@link seedDefaultsIfFresh}'s job). Replaces the narrow set with the persisted
 * intents and wires each on the substrate; an empty persisted set is honoured
 * verbatim (the session resumes hearing nothing). This is what
 * {@link makeSessionRestore} runs when a feeder first delivers the session_id.
 * A fresh session (store absent) is a no-op here — left untouched for
 * {@link seedDefaultsIfFresh} to handle on acquire.
 */
const restoreSubscriptions = (
  deps: Pick<SubscriptionRestoreDeps, 'subscriptionStore' | 'narrowSet' | 'inbox'>,
  sessionId: SessionId,
): Effect.Effect<void, PlatformError | ParseResult.ParseError | InboxError> =>
  deps.subscriptionStore.read(sessionId).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (intents) => applyRestored(deps, intents),
      }),
    ),
  )

/**
 * A feeder: hand the reactive restore a session_id the moment it becomes known.
 * Ignorant of restore — it just publishes the id. The first call wins; the rest
 * are no-ops.
 */
export type SessionRestoreFeed = (
  sessionId: SessionId,
) => Effect.Effect<void, PlatformError | ParseResult.ParseError | InboxError>

/**
 * The reactive restore core (comms-k7cv). Restore is a reaction to the session_id
 * becoming known, not a thing a specific action triggers: a resumed MCP child
 * boots session-blind (the id is not in its env), so restore must fire the moment
 * some feeder — any tool call carrying the id, or acquire — hands it over.
 *
 * A `Deferred<SessionId>` is the first-wins latch. The first feeder to complete it
 * runs {@link restoreSubscriptions} inline (restore-only — a fresh session's
 * acquire-gated Type-2 defaults are never seeded from here); every later feeder
 * sees the latch already closed and does nothing. This retires the per-session_id
 * `restoredSessions` memo the old acquire/`current_identity` triggers needed: the
 * Deferred's completion IS the once-guard, and the store's presence stays a true
 * resume signal rather than a file this same process just wrote.
 */
export const makeSessionRestore = (
  deps: Pick<SubscriptionRestoreDeps, 'subscriptionStore' | 'narrowSet' | 'inbox'>,
): Effect.Effect<SessionRestoreFeed> =>
  Effect.map(
    Deferred.make<SessionId>(),
    (known) =>
      (
        sessionId: SessionId,
      ): Effect.Effect<void, PlatformError | ParseResult.ParseError | InboxError> =>
        Deferred.succeed(known, sessionId).pipe(
          Effect.flatMap((won) => (won ? restoreSubscriptions(deps, sessionId) : Effect.void)),
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
