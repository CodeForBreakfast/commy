import { Context, Deferred, Layer } from 'effect'

/**
 * The queue-resume verdict, as a requirement rather than a value threaded
 * through call sites. Two-sided by nature (the {@link SessionId} pattern): the
 * Zulip adapter's resume wiring completes it, the ephemeral seat's `onAcquire`
 * awaits it. `true` means the surviving events-queue's resume-poll succeeded and
 * the pump is replaying the downtime backlog natively — so history catch-up must
 * NOT run, or every missed message double-delivers. `false` means there was no
 * queue resume (nothing persisted, or the persisted queue was dead on the
 * resume-poll) — so history catch-up is the best-effort backfill.
 *
 * The service type IS a `Deferred<boolean>` — no wrapper. First writer wins; a
 * later `succeed` is a no-op. That idempotency is what makes the adapter's two
 * feeders safe: the synchronous 'absent' report and the producer's first-poll
 * report can never both fire on the same boot (they are the None and Some arms
 * of the same match), but the latch is the honest primitive regardless.
 */
export class ResumeOutcome extends Context.Tag('commy/mcp/ResumeOutcome')<
  ResumeOutcome,
  Deferred.Deferred<boolean>
>() {}

/**
 * Provide the ONE shared `Deferred` for {@link ResumeOutcome}, built once at the
 * runtime root and memoized by the layer to a single instance for the whole MCP
 * child — exactly as {@link SessionIdLive} does. The adapter's resume wiring
 * completes it; the ephemeral `onAcquire` awaits it; a single shared instance is
 * what lets the writer unblock the awaiter.
 *
 * Load-bearing: bind at module top-level and provide once at the composition
 * root. A per-request build or `Layer.fresh` would mint a fresh deferred per
 * build — the writer would complete one while the awaiter blocks on another, a
 * silent deadlock.
 */
export const ResumeOutcomeLive: Layer.Layer<ResumeOutcome> = Layer.effect(
  ResumeOutcome,
  Deferred.make<boolean>(),
)
