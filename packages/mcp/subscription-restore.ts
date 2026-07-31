import type { BindError, ChannelName, InboxError, MessageInbox } from '@commy/core/ports'
import type { PlatformError } from '@effect/platform/Error'
import { Array as Arr, Effect, HashSet, Option, type ParseResult } from 'effect'
import type { ProjectSlug } from './bootstrap.ts'
import type { NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'

/**
 * Rebuilding a seat's narrow set on the way up, from the realm plus the one
 * thing the realm cannot hold.
 *
 * The realm is the authority on what a seat is subscribed to: those rows are
 * written under the seat's own principal and they are what actually governs
 * delivery. So on the way up a seat ASKS — `inbox.subscriptions()` — rather than
 * replaying a local copy of what it once asked for. A local copy can disagree
 * with delivery; the realm's answer cannot.
 *
 * What a subscription row cannot express is narrowing BELOW a channel, so
 * `#chan/topic` and `new-topics:#chan` both read back as plain `#chan`.
 * Reconstructing from the realm alone would silently widen every topic
 * subscription into its whole channel. That is why a small local record of
 * TOPIC-LEVEL intents survives — not as authority over what the seat receives,
 * but as the record of how narrowly it wanted to listen.
 *
 * That record is INTERIM, and `docs/agent-experience.md` is careful about why:
 * Zulip does have somewhere to put topic intent (`POST /user_topics` with
 * `visibility_policy: FOLLOWED` persists a per-`(user, stream, topic)` row), it
 * just does not DELIVER against it, so a client still filters its own queue.
 * Recording intent there would make this record a cache of realm truth instead
 * of the last piece of client-side authority. Deliberately not done here: this
 * change removes state, and adding a new realm write is a different kind of
 * change with its own risks.
 *
 * The two compose without overlapping: every channel the realm reports becomes
 * a channel-wide narrow UNLESS a persisted topic intent names it, in which case
 * the topic intents stand in its place.
 */
export interface SubscriptionRestoreDeps {
  readonly subscriptionStore: Pick<SubscriptionStore, 'read'>
  readonly narrowSet: NarrowSet
  readonly inbox: Pick<MessageInbox, 'subscribe' | 'subscriptions'>
  readonly registerDefaults: (project: ProjectSlug | undefined) => Effect.Effect<void>
}

/**
 * The topic-level record a rebuild narrows the realm's answer with. Supplied by
 * the caller rather than read here, because where it comes from differs by
 * seat: an ephemeral seat has a per-session record on disk, while a pinned bot
 * has none at all — its session id changes every launch, so a session-keyed
 * record could never be its own. Passing it in keeps the pinned seat off a read
 * that would park forever waiting for an id no host supplies.
 */
export type PersistedTopicIntents = Effect.Effect<
  Option.Option<ReadonlyArray<SubscribeIntent>>,
  PlatformError | ParseResult.ParseError
>

/** The intents a topic-level record holds: everything narrower than a channel. */
export const isTopicLevel = (intent: SubscribeIntent): boolean => intent.kind !== 'channel'

/**
 * The subset of a narrow set worth persisting. Channel-wide intents are dropped
 * because the realm already holds them — persisting them would re-create the
 * divergence this design removes, where a local copy and the delivery rules
 * disagree and the local copy wins.
 */
export const topicLevelIntents = (
  intents: ReadonlyArray<SubscribeIntent>,
): ReadonlyArray<SubscribeIntent> => Arr.filter(intents, isTopicLevel)

const sameIntent = (a: SubscribeIntent, b: SubscribeIntent): boolean =>
  a.kind === b.kind &&
  a.channelName === b.channelName &&
  (a.kind !== 'thread' || b.kind !== 'thread' || a.threadName === b.threadName)

const combineWithRealm = (
  realmChannels: ReadonlyArray<ChannelName>,
  topicIntents: ReadonlyArray<SubscribeIntent>,
): ReadonlyArray<SubscribeIntent> => {
  const deduped = Arr.dedupeWith(topicIntents, sameIntent)
  const narrowed = HashSet.fromIterable(deduped.map((intent) => intent.channelName))
  const channelIntents = Arr.filterMap(Arr.dedupe(realmChannels), (channelName) =>
    HashSet.has(narrowed, channelName)
      ? Option.none()
      : Option.some<SubscribeIntent>({ kind: 'channel', channelName }),
  )
  return [...channelIntents, ...deduped]
}

const applyRestored = (
  deps: Pick<SubscriptionRestoreDeps, 'narrowSet' | 'inbox'>,
  intents: ReadonlyArray<SubscribeIntent>,
): Effect.Effect<void, BindError | InboxError> =>
  Effect.suspend(() => {
    // Declared on the substrate as well as loaded locally, because the adapter
    // holds per-process state of its own — which channels it is listening to,
    // which ones carry a new-topics narrow — and a fresh process starts with
    // none. Only what this process has NOT already declared: the boot-time
    // defaults and any bootstrap went through `subscribe` on the way in, and
    // re-declaring them would be a second round-trip for an answer the adapter
    // already has.
    const alreadyDeclared = deps.narrowSet.intents()
    const undeclared = Arr.filter(
      intents,
      (intent) => !alreadyDeclared.some((declared) => sameIntent(declared, intent)),
    )
    return Effect.sync(() => deps.narrowSet.load(Option.some(intents))).pipe(
      Effect.zipRight(
        Effect.forEach(undeclared, (intent) => deps.inbox.subscribe(intentToTarget(intent)), {
          discard: true,
        }),
      ),
    )
  })

/**
 * Seed the acquire-gated Type-2 defaults, but only for a session this
 * installation has not run before (store absent). The store's presence is the
 * guard, and once the session subscribes anything the persist write makes it
 * present — so a later call is a no-op and no once-per-session memo is needed.
 *
 * Store PRESENCE is the only thing read here; its contents are not consulted.
 * That distinction is what lets it stay a true "has this session run before?"
 * signal even though the record itself shrank to topic-level intents.
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
 * Rebuild this seat's narrow set: ask the realm what it is subscribed to, and
 * narrow the answer with whatever topic-level intents were recorded for this
 * session.
 *
 * Runs for every seat, not just a resuming one. A pinned pane has no session
 * record at all — its session id changes every launch — so before this it came
 * up with nothing but its boot-time defaults, and any channel it had joined at
 * runtime was silently dropped even though the realm was still delivering it.
 * Asking the realm covers that seat for the first time.
 *
 * Forked at boot rather than awaited: the topic record is session-keyed and its
 * read parks until the session id arrives, which on a seat whose host injects
 * no id means until the seat itself acts. Whatever the outcome, the narrow set
 * is loaded exactly once so the buffering window closes and the deltas
 * journaled since boot replay onto the result — a subscribe that raced the
 * rebuild is never lost.
 */
export const restoreSubscriptions = (
  deps: Pick<SubscriptionRestoreDeps, 'narrowSet' | 'inbox'> & {
    readonly persisted: PersistedTopicIntents
    /**
     * Whether this seat already holds an identity. Read passively — asking must
     * never be the thing that mints one.
     */
    readonly isBound: () => boolean
  },
): Effect.Effect<void, PlatformError | ParseResult.ParseError | BindError | InboxError> =>
  deps.persisted.pipe(
    Effect.flatMap((persisted) =>
      // Reading the realm needs this seat's credential, so it can only be asked
      // by a seat that has one. A seat with no identity has no subscription
      // rows either — the honest answer is "nothing", not "mint a bot and go
      // and look". A topic record IS grounds to bind: it can only exist because
      // this session ran before, so the bot exists and binding re-acquires it
      // rather than bringing a new one into being.
      Option.isNone(persisted) && !deps.isBound()
        ? Effect.sync(() => deps.narrowSet.load(Option.none()))
        : deps.inbox.subscriptions().pipe(
            Effect.flatMap((realmChannels) =>
              applyRestored(
                deps,
                combineWithRealm(realmChannels, [
                  ...topicLevelIntents(Option.getOrElse(persisted, () => [])),
                  // Topic intents this boot has already registered — the
                  // boot-time defaults, and a fresh bot's COMMY_SUBSCRIBE
                  // bootstrap. Without these the realm's channel-wide answer
                  // would REPLACE them and quietly widen a `new-topics:` or
                  // single-topic narrow into its whole channel.
                  ...topicLevelIntents(deps.narrowSet.intents()),
                ]),
              ),
            ),
          ),
    ),
  )

/**
 * Persist the topic-level slice of the current narrow set under the session id,
 * after every runtime `subscribe`/`unsubscribe`. Channel-wide intents are
 * deliberately absent: the realm holds those, and a second copy could only
 * disagree with it.
 */
export const persistSubscriptions = (
  store: Pick<SubscriptionStore, 'write'>,
  narrowSet: NarrowSet,
): Effect.Effect<void, PlatformError> => store.write(topicLevelIntents(narrowSet.intents()))
