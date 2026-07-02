import type { IdentityId, InboundEvent, MessageInbox } from '@commy/core/ports'
import { decodeTimestamp } from '@commy/core/ports'
import { Array as Arr, Clock, Data, Effect, Option, Order, Predicate } from 'effect'
import type { CursorStore } from './cursor-store.ts'
import type { Notifier } from './event-pump.ts'
import { formatMessage } from './events.ts'

/**
 * Replay missed @-mentions on session resume.
 *
 * Mental model: "what would a human do when they returned to the app?"
 * Check mentions first. The plugin equivalent is — on resume, fetch the
 * mentions of `identityId` that landed since the persistent cursor and
 * surface them as channel-source events ahead of the live pump.
 *
 * Resume vs fresh acquire is decided by cursor presence:
 *   - cursor present → resume: fetch since cursor, dispatch, advance cursor.
 *   - cursor absent → fresh: no missed mentions to replay; initialise the
 *     cursor to `now()` so the next resume has a starting point.
 *
 * The dispatch path mirrors the event-pump (`formatMessage` →
 * `ChannelEventPayload` → `notifier`) so replayed mentions land in the
 * same wire shape as live ones — the model can't tell the difference.
 *
 * Duplicates with the live queue are possible in the boot window
 * (replay covers (cursor, now]; queue covers (queue-register-ts, ∞];
 * those overlap if subscribe happened before catch-up). V1 accepts a
 * single boot-window's worth of dupes — narrowing the window is a
 * follow-up if it becomes a problem in practice.
 */
export interface MentionsCatchUpDeps {
  readonly cursorStore: CursorStore
  readonly inbox: Pick<MessageInbox, 'replay'>
  readonly identityId: IdentityId
  readonly notifier: Notifier
}

/**
 * Where a catch-up run failed. Both `catchUpMentions` and `catchUpChannels`
 * fold every sub-failure (cursor i/o, history/replay reads, notifier
 * dispatch) into one `CatchUpError` so the boot edge handles a single
 * typed channel.
 */
export type CatchUpStage = 'cursor-read' | 'cursor-write' | 'replay' | 'history' | 'dispatch'

export class CatchUpError extends Data.TaggedError('CatchUpError')<{
  readonly stage: CatchUpStage
  readonly cause: unknown
}> {
  override get message(): string {
    const detail = Predicate.isError(this.cause) ? this.cause.message : String(this.cause)
    return `catch-up failed at ${this.stage}: ${detail}`
  }
}

export const catchUpAt =
  (stage: CatchUpStage) =>
  <A, E, R>(self: Effect.Effect<A, E, R>): Effect.Effect<A, CatchUpError, R> =>
    Effect.mapError(self, (cause) => new CatchUpError({ stage, cause }))

const isMention = (
  event: InboundEvent,
): event is Extract<InboundEvent, { readonly kind: 'mention-received' }> =>
  event.kind === 'mention-received'

export const catchUpMentions = (deps: MentionsCatchUpDeps): Effect.Effect<void, CatchUpError> =>
  Effect.gen(function* () {
    const cursor = yield* deps.cursorStore.read(deps.identityId).pipe(catchUpAt('cursor-read'))
    const ms = yield* Clock.currentTimeMillis
    const ts = yield* decodeTimestamp(Math.floor(ms / 1000)).pipe(Effect.orDie)
    if (Option.isNone(cursor)) {
      yield* deps.cursorStore.write(deps.identityId, ts).pipe(catchUpAt('cursor-write'))
      return
    }
    const events = yield* deps.inbox.replay(cursor.value).pipe(catchUpAt('replay'))
    const mentions = Arr.sort(
      events.filter(isMention),
      Order.mapInput(
        Order.number,
        (e: Extract<InboundEvent, { readonly kind: 'mention-received' }>) => e.message.ts,
      ),
    )
    yield* Effect.forEach(
      mentions,
      (event) =>
        Effect.tryPromise({
          try: () => deps.notifier(formatMessage(event, deps.identityId)),
          catch: (cause) => new CatchUpError({ stage: 'dispatch', cause }),
        }),
      { discard: true },
    )
    yield* deps.cursorStore.write(deps.identityId, ts).pipe(catchUpAt('cursor-write'))
  })
