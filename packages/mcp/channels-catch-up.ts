import type { HistoryError, HistoryReader, IdentityId, Message, Timestamp } from '@commy/core/ports'
import { decodeTimestamp } from '@commy/core/ports'
import { Array as Arr, Clock, Effect, Match, Option, Order } from 'effect'
import type { Notifier } from './event-pump.ts'
import { formatMessage } from './events.ts'
import { CatchUpError, catchUpAt } from './mentions-catch-up.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'

/**
 * Boot-time window-bounded catch-up of channel + thread narrows.
 * Companion to `catchUpMentions`, which catches up missed mentions
 * with cursor-bounded semantics.
 *
 * Mental model: what would a human do when they returned to the app?
 * Check mentions first, then skim the channels they care
 * about. This catch-up reads `windowSeconds` of recent
 * traffic from each subscribed channel / thread and surfaces it as
 * channel-source events ahead of the live pump, so a persistent bot
 * resuming after downtime sees recent context in its first turn.
 *
 * Implementation choice: in-process plugin
 * boot helper, NOT a SessionStart hook and NOT agent-driven prompt
 * instructions. The mentions catch-up chose the same shape
 * — keeping both halves of the boot-time catch-up in one layer means
 * the dispatch path is uniform (`formatMessage` → `notifier`), the
 * substrate's history API is the only thing each helper needs, and
 * per-bot prompt engineering stays out of it. The hook variant was
 * rejected because injecting messages as a system reminder would
 * diverge from the live pump's wire shape; the agent-driven variant
 * was rejected because it would push policy ("which channels to skim,
 * how far back, in what order") into every bot's prompt.
 *
 * Mentions are not a subscribe intent — they arrive unconditionally,
 * so there is no mentions arm for `fetchForIntent` to skip. They are
 * caught up solely by the mentions helper, whose cursor-bounded fetch
 * has the semantics they need (since-last-seen, not last-N-hours).
 * When both catch-ups run on the same boot that split is what dedupes
 * them by route: this helper doesn't fetch mentions, the mentions
 * helper does.
 *
 * Messages from every requested narrow are merged and sorted by ts
 * before dispatch so the bot reads them in wall-clock order even
 * across channel/thread boundaries — closer to the human-skim model.
 *
 * Duplicates with the live pump are possible in the boot window
 * (catch-up covers (now - window, now]; pump covers
 * (queue-register-ts, ∞]; those overlap if subscribe registered
 * before catch-up ran). V1 accepts this — narrowing is a follow-up
 * if it shows up as a problem in practice. Same boundary trade-off
 * as the mentions catch-up.
 */
export interface ChannelsCatchUpDeps {
  readonly intents: ReadonlyArray<SubscribeIntent>
  readonly history: HistoryReader
  readonly notifier: Notifier
  readonly botIdentityId: IdentityId | undefined
  /** Hours of recent traffic to surface, in seconds. */
  readonly windowSeconds: number
  /**
   * Optional tighter window for `new-topics-in-channel` intents — these
   * narrows can flood on busy channels (first-message-per-topic still
   * runs the channel's full topic-creation rate). Defaults to
   * `windowSeconds` when omitted.
   */
  readonly newTopicsWindowSeconds?: number
}

/**
 * Clamp `now - windowSeconds` to a valid non-negative `Timestamp`. The
 * decode is dieable: `Math.max(0, …)` guarantees the constraint, so it
 * cannot fail.
 */
const clampedSince = (now: Timestamp, windowSeconds: number): Effect.Effect<Timestamp> =>
  decodeTimestamp(Math.max(0, now - windowSeconds)).pipe(Effect.orDie)

const firstMessagePerTopic = (messages: ReadonlyArray<Message>): ReadonlyArray<Message> => {
  const seen = new Set<string>()
  const out: Message[] = []
  const ordered = Arr.sort(
    messages,
    Order.mapInput(Order.number, (m: Message) => m.ts),
  )
  for (const m of ordered) {
    const thread = m.ref.thread
    if (Option.isNone(thread)) continue
    if (seen.has(thread.value.name)) continue
    seen.add(thread.value.name)
    out.push(m)
  }
  return out
}

const fetchForIntent = (
  intent: SubscribeIntent,
  history: HistoryReader,
  defaultSince: Timestamp,
  newTopicsSince: Timestamp,
): Effect.Effect<ReadonlyArray<Message>, HistoryError> =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      channel: (i) => history.readChannel(i.channelName, { since: defaultSince }),
      'new-topics-in-channel': (i) =>
        history
          .readChannel(i.channelName, { since: newTopicsSince })
          .pipe(Effect.map(firstMessagePerTopic)),
      thread: (i) => history.readThread(i.channelName, i.threadName, { since: defaultSince }),
    }),
  )

export const catchUpChannels = (deps: ChannelsCatchUpDeps): Effect.Effect<void, CatchUpError> =>
  Effect.gen(function* () {
    if (deps.intents.length === 0) return
    const ms = yield* Clock.currentTimeMillis
    const now = yield* decodeTimestamp(Math.floor(ms / 1000)).pipe(Effect.orDie)
    const since = yield* clampedSince(now, deps.windowSeconds)
    const newTopicsSince = yield* clampedSince(
      now,
      deps.newTopicsWindowSeconds ?? deps.windowSeconds,
    )
    const batches = yield* Effect.forEach(
      deps.intents,
      (intent) =>
        fetchForIntent(intent, deps.history, since, newTopicsSince).pipe(catchUpAt('history')),
      { concurrency: 2 },
    )
    const messages = Arr.sort(
      Arr.flatten(batches),
      Order.mapInput(Order.number, (m: Message) => m.ts),
    )
    yield* Effect.forEach(
      messages,
      (message) =>
        Effect.tryPromise({
          try: () =>
            deps.notifier(formatMessage({ kind: 'message-posted', message }, deps.botIdentityId)),
          catch: (cause) => new CatchUpError({ stage: 'dispatch', cause }),
        }),
      { discard: true },
    )
  })
