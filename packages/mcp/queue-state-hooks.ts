import type { QueueState } from '@commy/zulip/events'
import { Deferred, Effect, Option } from 'effect'
import type { QueueStateStore } from './queue-state-store.ts'
import type { SessionIdValue } from './session-id.ts'

/**
 * The queue-state write-half hooks handed to the Zulip adapter: the idle
 * timeout its registers carry, plus the two best-effort persistence callbacks
 * the producer fires. `onQueueRegister` persists the fresh `{queueId,
 * lastEventId}` at a register site — but only when it will not clobber an
 * unconsumed resume candidate (see the guard below); `onQueueAdvance` walks the
 * persisted `lastEventId` forward (monotonic, guarded by the store) on every
 * poll that moves the cursor. Both are total (`Effect<void>`) — the
 * adapter/producer simply call them.
 */
/**
 * How long `resumeQueue` waits at pump materialisation for a session id that
 * has not landed yet, before giving up and registering fresh.
 *
 * WHY BOUNDED AT ALL, rather than awaiting outright: a listen-only client with
 * no boot session id may never produce one — nothing feeds the deferred until a
 * hook-matched tool call arrives, and a seat that only listens makes none. An
 * unbounded await would park pump start forever and trade a lost backlog for
 * total deafness, which is strictly worse than the defect being fixed.
 *
 * WHY FIVE SECONDS. The wait only has to cover the gap between pump
 * materialisation and a session id that is already on its way — the boot-env
 * feeder fills the deferred before the pump starts, so the only racing source
 * is a non-CC client's first `session_id`-bearing tool call. That is one local
 * round trip: milliseconds, and tens of milliseconds on a loaded box. Five
 * seconds sits two orders of magnitude above that, and two orders BELOW the
 * Zulip queue TTL the resume is racing — so widening it buys no additional
 * recoveries while lengthening the one-time stall for clients that never send
 * an id. It is a ceiling on a fast path, not a tuned latency.
 *
 * COST WHEN IT EXPIRES: a single one-time delay before inbound begins flowing,
 * once per pump lifetime, for clients that would have recovered nothing anyway.
 */
const RESUME_SESSION_ID_WAIT = '5 seconds'

export interface QueueStateHooks {
  readonly queueIdleTimeoutSecs: number
  readonly onQueueRegister: (queue: QueueState) => Effect.Effect<void>
  readonly onQueueAdvance: (lastEventId: number) => Effect.Effect<void>
  /**
   * Read half: resolves the persisted queue-state a resuming seat should reuse,
   * for the adapter to hand the producer as its `initialQueue`. Waits for the
   * shared session id, BOUNDED by {@link RESUME_SESSION_ID_WAIT} — `Some(queueState)`
   * when the id arrives within the bound and a queue-state is on disk, `None`
   * for a fresh session, an id that never arrives, or an unreadable/corrupt
   * store (best-effort: a resume that can't be recovered degrades to a fresh
   * register rather than stranding the seat).
   *
   * Called ONCE per pump lifetime, at materialisation — unlike the two hot-path
   * hooks above, which must never park. Do not "restore symmetry" by polling
   * here: that is the comms-9iro defect.
   */
  readonly resumeQueue: () => Effect.Effect<Option.Option<QueueState>>
  /**
   * Resume-verdict sink handed to the adapter: completes the shared
   * {@link ResumeOutcome} deferred the seat's `onAcquire` awaits. `true` when
   * the surviving queue's resume-poll replayed the backlog (skip catch-up),
   * `false` on a dead/absent queue (run catch-up). First write wins — the
   * deferred's own idempotency; not session-keyed (one verdict per boot).
   */
  readonly onResumeOutcome: (queueReplayed: boolean) => Effect.Effect<void>
}

/**
 * Bind the per-session queue-state store to the producer's register/poll hooks
 * for an ephemeral seat. The ephemeral gate lives at the call site (the wiring
 * layer builds these only when `botName` is unset) — a persistent bot passes no
 * hooks and keeps the server's default queue window.
 *
 * `onQueueRegister` and `onQueueAdvance` resolve the session id NON-BLOCKING:
 * these two fire on the producer's hot poll path, so a blocking `Deferred.await`
 * would park inbound delivery on an unfed id. Poll instead — persist on `Some`,
 * no-op on `None` (id not known yet, so there is nothing to key against).
 * Every write swallows its own failures: persistence is best-effort and must
 * never surface into the live event stream.
 *
 * `resumeQueue` is DELIBERATELY NOT one of them, and the distinction is
 * load-bearing (comms-9iro). It is called exactly ONCE per pump lifetime, at
 * pump materialisation, and is not on the hot path at all — so the argument
 * above does not reach it. Polling there cost real backlog: a seat whose id had
 * not landed at that instant reported "no resume candidate", latched the verdict
 * for the pump's lifetime, and registered a fresh empty queue while its
 * surviving queue-state sat valid on disk. It waits, bounded — see
 * {@link RESUME_SESSION_ID_WAIT}.
 */
export const buildQueueStateHooks = (deps: {
  readonly store: QueueStateStore
  readonly session: Deferred.Deferred<SessionIdValue>
  readonly idleTimeoutSecs: number
  readonly resumeOutcome: Deferred.Deferred<boolean>
}): QueueStateHooks => {
  const { store, session, idleTimeoutSecs, resumeOutcome } = deps
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
    // Register-time persistence, guarded against clobbering an unconsumed resume
    // candidate. `resumeOutcome` still pending means the producer has not yet
    // reported its resume verdict, so this register is a PRE-producer eager one
    // — the `subscribeFromEnv` register that fires at boot, before the pump
    // resume-polls. Such a register must never overwrite the surviving
    // queue-state a resume is about to recover: write only when nothing is
    // persisted (a genuine fresh session, or an unreadable store treated as
    // fresh), and otherwise leave the candidate untouched. Once the verdict is
    // reported the producer owns the queue lifecycle (e.g. a fresh register
    // after BAD_EVENT_QUEUE_ID), so a wholesale replace is correct.
    onQueueRegister: (queue) =>
      withSession((id) =>
        Deferred.poll(resumeOutcome).pipe(
          Effect.flatMap(
            Option.match({
              onSome: () => store.write(id, queue),
              onNone: () =>
                store.read(id).pipe(
                  Effect.map(Option.isNone),
                  Effect.catchAll(() => Effect.succeed(true)),
                  Effect.flatMap((isFresh) => (isFresh ? store.write(id, queue) : Effect.void)),
                ),
            }),
          ),
          Effect.catchAllCause(() => Effect.void),
        ),
      ),
    onQueueAdvance: (lastEventId) =>
      withSession((id) =>
        store.advance(id, lastEventId).pipe(Effect.catchAllCause(() => Effect.void)),
      ),
    resumeQueue: () =>
      Deferred.await(session).pipe(
        Effect.timeoutOption(RESUME_SESSION_ID_WAIT),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.succeedNone,
            onSome: (id) => store.read(id).pipe(Effect.catchAllCause(() => Effect.succeedNone)),
          }),
        ),
        Effect.catchAllCause(() => Effect.succeedNone),
      ),
    onResumeOutcome: (queueReplayed) =>
      Deferred.succeed(resumeOutcome, queueReplayed).pipe(Effect.asVoid),
  }
}
