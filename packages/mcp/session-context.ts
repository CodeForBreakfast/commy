import { Effect, FiberRef } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'

/**
 * What the calling session told us about itself on this tool call — the
 * naming inputs an ephemeral identity is minted from.
 *
 * Both fields are optional because both sources are optional. A Claude Code
 * seat gets them from the PreToolUse hook; a host that injects nothing gets
 * them only if the caller passed them in the tool-call arguments, which for
 * that host class is the sole inbound binding it has. Absent, the seat cannot
 * mint, and the bind seam refuses rather than guessing a name.
 */
export interface SessionContext {
  readonly sessionId: SessionId | undefined
  readonly project: ProjectSlug | undefined
}

const empty: SessionContext = { sessionId: undefined, project: undefined }

/**
 * The calling session's context, for the duration of one tool call.
 *
 * A `FiberRef` rather than a `Context.Tag` because the reader is the adapter's
 * bind-on-demand closure, which is handed to the adapter as a plain
 * `Effect<AcquiredIdentity, BindError>` at construction. A tag would surface in
 * that Effect's `R`, and from there in the port signatures every write flows
 * through — pushing an MCP-layer concern onto the shared port and onto the
 * in-memory adapter, which has no session to speak of. A `FiberRef` reads with
 * `R = never`, so the requirement stays where the dependency actually is.
 *
 * Every tool call supplies this, uniformly. Which calls go on to need an
 * identity is not decided here and not decided by any list — a caller reaching
 * for a bound credential is the decision, and that happens at the port.
 */
export const CurrentSessionContext = FiberRef.unsafeMake<SessionContext>(empty)

export const currentSessionContext: Effect.Effect<SessionContext> =
  FiberRef.get(CurrentSessionContext)

/** Run `effect` with `context` as the calling session's context. */
export const withSessionContext = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  context: SessionContext,
): Effect.Effect<A, E, R> => Effect.locally(effect, CurrentSessionContext, context)
