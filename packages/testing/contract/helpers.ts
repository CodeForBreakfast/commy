import type {
  AgentComms,
  BindError,
  ChannelName,
  InboundEvent,
  MessageBody,
  PublisherError,
  UnknownChannel,
  UnresolvedMention,
} from '@commy/core/ports'
import { Duration, Effect, Option, Queue, type Scope, Stream } from 'effect'

/**
 * Post `bodies` in order, spacing consecutive posts by the substrate's
 * `timestampGranularity` so each lands on a distinct `Timestamp`. Memory
 * reports `Duration.zero` (its `ts` counter already separates posts) so the
 * sleep is a no-op there and the fixture stays fast; live Zulip reports 1s so
 * the three posts straddle distinct integer seconds and the range/replay
 * filters assert at full strength on the real adapter. The first post is never
 * delayed.
 */
export const postSpaced = (
  comms: AgentComms,
  channel: ChannelName,
  bodies: ReadonlyArray<MessageBody>,
): Effect.Effect<void, BindError | UnknownChannel | UnresolvedMention | PublisherError> =>
  Effect.forEach(
    bodies,
    (body, index) =>
      (index === 0 ? Effect.void : Effect.sleep(comms.capabilities.timestampGranularity)).pipe(
        Effect.zipRight(comms.publisher.post(channel, body)),
      ),
    { discard: true },
  )

/**
 * Generous deadline for "this event must eventually arrive" assertions. On an
 * in-process substrate the event is already enqueued so the wait resolves
 * immediately; on the live Zulip realm the events queue can take several
 * seconds to surface a post/mention. Sits well under the live suite's 45s
 * per-test budget so a genuine no-show still fails with the test's own clear
 * message rather than a bun:test timeout.
 */
const EVENT_DELIVERY_DEADLINE = Duration.seconds(30)

/**
 * Pull events off the queue in order until one satisfies `predicate`,
 * returning it. Recurses on each non-match — the caller wraps this in
 * `Effect.timeoutOption` so an event that never arrives bounds the wait
 * instead of blocking forever. Lets the mention-floor tests assert on the
 * `mention-received` event without coupling to whether a `message-posted`
 * precedes it (it does on Memory, may not live when the only thing
 * matching is the unconditional mention rule).
 */
const takeUntil = (
  queue: Queue.Queue<InboundEvent>,
  predicate: (event: InboundEvent) => boolean,
): Effect.Effect<InboundEvent> =>
  Queue.take(queue).pipe(Effect.filterOrElse(predicate, () => takeUntil(queue, predicate)))

/**
 * Await the first event matching `predicate`, failing the test (with
 * `description`) if none arrives within `EVENT_DELIVERY_DEADLINE`. Intervening
 * events are drained and discarded — so a positive event-delivery assertion
 * does not depend on the *position* of its event in the stream. On the live
 * realm an events queue under 429 backoff can redeliver or gap-replay a
 * `message-posted`, which a positional `Queue.take` + `.kind`
 * assertion would mistake for the wrong event; draining-until-match is immune.
 * Memory delivers exactly once so the match is immediate there.
 */
export const awaitEvent = (
  queue: Queue.Queue<InboundEvent>,
  description: string,
  predicate: (event: InboundEvent) => boolean,
): Effect.Effect<InboundEvent> =>
  takeUntil(queue, predicate).pipe(
    Effect.timeoutOption(EVENT_DELIVERY_DEADLINE),
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.die(new Error(`events() did not observe ${description} within the deadline`)),
        onSome: Effect.succeed,
      }),
    ),
  )

/**
 * Start a subscriber on `events()` that mirrors every inbound event into an
 * unbounded queue. Tests pull from the queue in order, mimicking
 * AsyncIterator.next() semantics, and use `Effect.timeoutOption` on
 * `Queue.take` for "no event within Nms" assertions. Requires `Scope` — the
 * caller's `Effect.scoped` block owns the consumer fiber's lifetime, so the
 * fork is interrupted when the test body exits.
 */
export const eventQueue = (
  comms: AgentComms,
): Effect.Effect<Queue.Queue<InboundEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<InboundEvent>()
    yield* Effect.forkScoped(
      comms.inbox.events().pipe(Stream.runForEach((event) => Queue.offer(queue, event))),
    )
    return queue
  })
