import { messageOf } from '@commy/core/messageOf'
import type { ChannelName } from '@commy/core/ports'
import { Effect } from 'effect'

/**
 * Minter subscription reconciler (ass-6a77).
 *
 * Substrates with realm-level subscription (Zulip) require the
 * minter to be subscribed to every public stream so the plugin's
 * event pump observes events even from lurker (un-acquired) sessions
 * — see ass-220u. This module is the boot-time pass that closes any
 * gap between "every public stream" and the minter's current
 * subscription set. Per-session `inbox.subscribe()` continues to
 * register the minter for streams created after boot.
 *
 * Pure logic; substrate I/O is injected. The Zulip adapter wires
 * `minterHttp` into `listUnsubscribedPublicStreams` /
 * `subscribeToStreams` and exposes the composed call as a method
 * on its public shape.
 *
 * Failure is non-fatal: any failing dep is captured in `error` and
 * the boot path keeps running with a degraded lurker view (`bd show
 * ass-6a77` "log + continue" decision). The deps' typed error
 * channel is collapsed into the report, so the reconciler never
 * fails — its E channel is `never`.
 */
export interface ReconcilerDeps<E> {
  readonly listUnsubscribedPublicStreams: () => Effect.Effect<
    ReadonlyArray<{ readonly name: ChannelName }>,
    E
  >
  readonly subscribeToStreams: (
    names: ReadonlyArray<ChannelName>,
  ) => Effect.Effect<ReadonlyArray<ChannelName>, E>
}

export interface ReconcileReport {
  readonly added: ReadonlyArray<ChannelName>
  readonly error: string | undefined
}

export const reconcileMinterSubscriptions = <E>(
  deps: ReconcilerDeps<E>,
): Effect.Effect<ReconcileReport, never> =>
  deps.listUnsubscribedPublicStreams().pipe(
    Effect.flatMap((candidates) =>
      candidates.length === 0
        ? Effect.succeed<ReconcileReport>({ added: [], error: undefined })
        : deps
            .subscribeToStreams(candidates.map((c) => c.name))
            .pipe(Effect.map((added) => ({ added, error: undefined }))),
    ),
    Effect.catchAll((err) =>
      Effect.succeed<ReconcileReport>({
        added: [],
        error: messageOf(err),
      }),
    ),
  )
