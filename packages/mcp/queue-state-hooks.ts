import type { QueueState } from '@commy/zulip/events'
import { Deferred, Effect, Option } from 'effect'
import type { QueueStateStore } from './queue-state-store.ts'
import type { SessionIdValue } from './session-id.ts'

/**
 * The queue-state write-half hooks handed to the Zulip adapter: the idle
 * timeout its registers carry, plus the two best-effort persistence callbacks
 * the producer fires. `onQueueRegister` writes the fresh `{queueId,
 * lastEventId}` at every register site; `onQueueAdvance` walks the persisted
 * `lastEventId` forward (monotonic, guarded by the store) on every poll that
 * moves the cursor. Both are total (`Effect<void>`) — the adapter/producer
 * simply call them.
 */
export interface QueueStateHooks {
  readonly queueIdleTimeoutSecs: number
  readonly onQueueRegister: (queue: QueueState) => Effect.Effect<void>
  readonly onQueueAdvance: (lastEventId: number) => Effect.Effect<void>
}

/**
 * Bind the per-session queue-state store to the producer's register/poll hooks
 * for an ephemeral seat. The ephemeral gate lives at the call site (the wiring
 * layer builds these only when `botName` is unset) — a persistent bot passes no
 * hooks and keeps the server's default queue window.
 *
 * The session id is resolved NON-BLOCKING, mirroring the subscription store's
 * `advanceCursor`: register and advance fire on the producer's hot poll path,
 * so a blocking `Deferred.await` would park inbound delivery on an unfed id.
 * Poll instead — persist on `Some`, no-op on `None` (id not known yet, so
 * there is nothing to key against and the resume this feeds has not happened).
 * Every write swallows its own failures: persistence is best-effort and must
 * never surface into the live event stream.
 */
export const buildQueueStateHooks = (deps: {
  readonly store: QueueStateStore
  readonly session: Deferred.Deferred<SessionIdValue>
  readonly idleTimeoutSecs: number
}): QueueStateHooks => {
  const { store, session, idleTimeoutSecs } = deps
  const withSession = (use: (id: SessionIdValue) => Effect.Effect<void>): Effect.Effect<void> =>
    Deferred.poll(session).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (awaitId) => Effect.flatMap(awaitId, use),
        }),
      ),
    )
  return {
    queueIdleTimeoutSecs: idleTimeoutSecs,
    onQueueRegister: (queue) =>
      withSession((id) => store.write(id, queue).pipe(Effect.catchAllCause(() => Effect.void))),
    onQueueAdvance: (lastEventId) =>
      withSession((id) =>
        store.advance(id, lastEventId).pipe(Effect.catchAllCause(() => Effect.void)),
      ),
  }
}
