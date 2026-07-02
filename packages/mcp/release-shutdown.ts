import { Cause, Duration, Effect, Option } from 'effect'

const DEFAULT_TIMEOUT_MS = 5000

/**
 * Release the substrate identity within a fixed budget — the body of
 * the program's release-on-shutdown finalizer. Three
 * outcomes are interesting and each gets its own stderr line:
 *
 *   - resolved within budget → no log
 *   - exceeded budget        → "release timed out after <N>ms"
 *   - threw / defected       → "release failed: <Cause.pretty>"
 *
 * `Effect.tapDefect` renders a thrown release via `Cause.pretty`, then
 * `Effect.catchAllDefect` recovers it to void so a failed release never
 * aborts the surrounding shutdown — the substrate layer's `close()`
 * finalizer must still run regardless. `Effect.timeoutOption`
 * discriminates the timeout (`None`) from success (`Some`).
 *
 * Negative requirements (must stay true as this code evolves):
 *   - Does NOT call inbox.unsubscribe. Zulip drops the events queue
 *     when the bot deactivates; the queue id we hold dies with the
 *     process either way.
 *   - Does NOT touch minter creds. Those belong to the process owner,
 *     not the per-session identity.
 *
 * Release-on-exit is hygiene, not correctness-critical. If we exit
 * without firing it (SIGKILL, uncaught, host death), the bot's
 * is_active flag stays true. The next session's acquire(same-name)
 * regenerates the api key, invalidating the prior session's stash
 * anyway — so a missed release is annoying, not catastrophic.
 *
 * Whether release fires at all (the acquisition gate) and the
 * pump-cancel-then-release-then-close ordering are owned by the program
 * scope's finalizer registration order (server.ts `makeProgram`), not
 * this helper.
 */
export const raceReleaseAgainstTimeout = (
  release: Effect.Effect<void, never>,
  timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Effect.Effect<void> =>
  release.pipe(
    Effect.tapDefect((cause) => Effect.logError(`release failed: ${Cause.pretty(cause)}`)),
    Effect.catchAllDefect(() => Effect.void),
    Effect.timeoutOption(Duration.millis(timeoutMs)),
    Effect.flatMap((result) =>
      Option.isNone(result)
        ? Effect.logError(`release timed out after ${timeoutMs}ms`)
        : Effect.void,
    ),
  )
