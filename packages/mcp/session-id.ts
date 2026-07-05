import { Context, Deferred, Layer } from 'effect'
import type { SessionId as SessionIdValue } from './bootstrap.ts'

/**
 * The obtained-once session id, as its branded value type. Re-exported here
 * under the `SessionIdValue` name so consumers of the {@link SessionId} service
 * can name the value the service carries without colliding with the Tag itself.
 * `bootstrap.ts` remains the single mint point (`parseSessionId`).
 */
export type { SessionIdValue }

/**
 * The session id as a requirement, not a value threaded through call sites.
 * Obtaining the id and using it are separated (the resolved comms-k7cv design):
 * every action that needs the id declares `SessionId` in `R` and awaits it,
 * while obtaining it is a separate, idempotent concern set at every opportunity.
 *
 * The service type IS a `Deferred<SessionIdValue>` — no wrapper. A `Deferred` is
 * already two-sided: `Deferred.await` reads (blocks until set, instant once
 * set) and `Deferred.succeed` writes (first writer wins; a later `succeed` on an
 * already-completed deferred is a no-op returning `false`, never a throw or
 * overwrite). That idempotency is what makes "set the id at every opportunity"
 * safe: no setter is load-bearing alone, and the first source to win fills it.
 *
 * A `Deferred`, not a `Latch`: a Latch is a valueless gate, whereas the id must
 * be carried through.
 */
export class SessionId extends Context.Tag('commy/mcp/SessionId')<
  SessionId,
  Deferred.Deferred<SessionIdValue>
>() {}

/**
 * Provide the ONE shared `Deferred` for {@link SessionId}, built once at the
 * runtime root and memoized by the layer to a single instance for the whole MCP
 * child. Setters complete this deferred; awaiters read it; because it is a
 * single shared instance, a set by any feeder unblocks every awaiter.
 *
 * Load-bearing: this layer must be bound at module top-level and provided once
 * at the composition root. Building it per-request or via `Layer.fresh` would
 * mint a fresh deferred per build — setters would complete one while awaiters
 * block on another, a silent deadlock.
 */
export const SessionIdLive: Layer.Layer<SessionId> = Layer.effect(
  SessionId,
  Deferred.make<SessionIdValue>(),
)
