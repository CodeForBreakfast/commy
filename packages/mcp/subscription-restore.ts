import type { InboxError, MessageInbox } from '@commy/core/ports'
import type { PlatformError } from '@effect/platform/Error'
import { Effect, Option, type ParseResult } from 'effect'
import type { ProjectSlug } from './bootstrap.ts'
import type { NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'

/**
 * Restore and seed are two independent reactions to a session's narrow set,
 * split apart for the reactive core. Resume vs fresh is decided by
 * the per-session_id subscription store, exactly as the mentions cursor decides
 * resume vs fresh for catch-up:
 *   - store present → resume: {@link restoreSubscriptions} loads the persisted
 *     intents as the narrow set's base (including prior unsubscribes — a dropped
 *     default stays dropped) and wires each on the substrate. Runtime
 *     subscribe/unsubscribe deltas that raced the still-loading restore were
 *     journaled by the narrow set and replay onto this base in arrival order, so
 *     none is lost. An empty persisted set is honoured verbatim: the session
 *     resumes hearing only what it re-subscribed since boot. The store's `read`
 *     awaits the shared session-id `Deferred` internally, so restore is forked
 *     once at boot and blocks there until any source fills the id — no
 *     session_id threaded through its signature, no per-session memo, no gate on
 *     the mutation path.
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
  Effect.sync(() => deps.narrowSet.load(Option.some(intents))).pipe(
    Effect.zipRight(
      Effect.forEach(intents, (intent) => deps.inbox.subscribe(intentToTarget(intent)), {
        discard: true,
      }),
    ),
  )

/**
 * Seed the acquire-gated Type-2 defaults, but only for a fresh session (store
 * absent). The seed half of the old restore-or-seed, split out for the reactive
 * core: restore is now a boot-forked reaction to the session_id via
 * {@link restoreSubscriptions}, leaving seeding as its own store-gated step. The
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
  project: ProjectSlug | undefined,
): Effect.Effect<void, PlatformError | ParseResult.ParseError> =>
  deps.subscriptionStore.read().pipe(
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
 * {@link seedDefaultsIfFresh}'s job). On a store hit it loads the persisted
 * intents as the narrow set's base and wires each on the substrate; an empty
 * persisted set is honoured verbatim. On a store miss it still loads — with no
 * base — to end the narrow set's buffering window, leaving the boot-time
 * `COMMY_SUBSCRIBE` seed standing as the fresh-session set. Either way the
 * narrow set replays whatever runtime deltas it journaled while restore was
 * loading, so a subscribe/unsubscribe that raced the load is never lost.
 *
 * Restore is a reaction to the session_id becoming known, not a thing a specific
 * action triggers: a host that does not inject the session id into the MCP
 * child's env boots session-blind, and there the id cannot arrive until the seat
 * itself acts. The store's `read` awaits the shared session-id `Deferred`
 * internally, so this is forked once at boot (server.ts) and parks on that read
 * until any source — the boot-env feeder, which covers a Claude Code seat whether
 * fresh or resumed, or the first tool call of an acting seat — fills the id, then
 * rehydrates. The `Deferred`'s single completion is the once-guard; no session_id
 * threads through this signature and no per-session memo is needed.
 */
export const restoreSubscriptions = (
  deps: Pick<SubscriptionRestoreDeps, 'subscriptionStore' | 'narrowSet' | 'inbox'>,
): Effect.Effect<void, PlatformError | ParseResult.ParseError | InboxError> =>
  deps.subscriptionStore.read().pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.sync(() => deps.narrowSet.load(Option.none())),
        onSome: (intents) => applyRestored(deps, intents),
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
): Effect.Effect<void, PlatformError> => store.write(narrowSet.intents())
