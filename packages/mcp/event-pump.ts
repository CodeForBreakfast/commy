import type {
  Identity,
  IdentityId,
  InboundEvent,
  MessageId,
  MessageInbox,
  Timestamp,
} from '@codeforbreakfast/core/ports'
import { decodeTimestamp } from '@codeforbreakfast/core/ports'
import type { Server } from '@modelcontextprotocol/sdk/server/index.js'
import {
  Array as Arr,
  Clock,
  Data,
  Effect,
  Fiber,
  Match,
  Predicate,
  Record as Rec,
  Stream,
} from 'effect'
import type { ChannelEventPayload } from './events.ts'
import { formatError, formatMessage, formatReaction, IDENTITY_ID_META_KEYS } from './events.ts'

/**
 * Sink for one rendered channel-event payload. Production callers use
 * `channelNotifier(mcp)` to push as a `notifications/claude/channel`
 * MCP notification; tests pass a spy to assert against the payload
 * stream without touching transport.
 */
export type Notifier = (payload: ChannelEventPayload) => Promise<void>

export interface EventPumpDeps {
  /**
   * The pump only ever calls `inbox.events()`. Narrowing via `Pick`
   * documents that — tests can pass a hand-rolled iterator source
   * without satisfying the rest of the `MessageInbox` shape, and any
   * future addition to the port that the pump genuinely needs has to
   * be added here on purpose.
   */
  readonly inbox: Pick<MessageInbox, 'events'>
  readonly notifier: Notifier
  /**
   * Bot identity getter. Returns `undefined` pre-acquire (ephemeral
   * mode before first attribution call). The pump uses this for two
   * things: (1) computing the rendered `mentioned` meta flag (true when
   * the bound bot is in a message's mention list); (2) feeding `match`
   * so `mentions` narrows can compare against the bot's id once it
   * exists. Persistent mode supplies a getter that always returns the
   * acquired id.
   */
  readonly getBotIdentityId: () => IdentityId | undefined
  /**
   * Narrow-filter predicate. The pump tees an event to the notifier
   * only when this returns true. Production wires this to
   * `NarrowSet.matches(event, getBotIdentityId())`. Omit for
   * "deliver everything" — useful in tests that don't exercise the
   * narrow filter.
   */
  readonly match?: (event: InboundEvent) => boolean
  /**
   * Optional sink for identities observed on inbound events (sender,
   * mentions, reactor). Production wires this to the `ToolsCache`
   * returned by `registerTools` so `presence` / `post` / `react` can
   * resolve ids that were only ever seen via a notification —
   * without this, those tools throw `UnknownIdentity` for ids that
   * never came through `current_identity` / `resolve` / `list_*`
   * (comms-lox). Invoked after the narrow filter and self-echo
   * guard, so cached identities mirror what the consumer actually
   * sees in notifications.
   */
  readonly rememberIdentity?: (identity: Identity) => void
  /**
   * Optional sink invoked with the timestamp of each delivered
   * `mention-received` event (comms-rxo). Production wires this to
   * the cursor store so the per-identity "have-seen-up-to" mark
   * advances forward, letting the next resume narrow its replay to
   * only-actually-missed mentions. Invoked after the narrow filter
   * and self-echo guard — mirrors the events the consumer sees.
   *
   * Returns an Effect the pump sequences inside its dispatch loop, so the
   * cursor advance composes into the pump rather than being run at an
   * internal seam (comms-2y4.6). The error channel is `void`: a cursor
   * write is a best-effort monotonic advance, so the provider swallows its
   * own failures rather than failing the pump.
   */
  readonly onMention?: (ts: Timestamp) => Effect.Effect<void>
}

export interface EventPumpHandle {
  /**
   * Effect that completes when the underlying stream ends naturally or
   * the fiber is interrupted. Run via `Effect.runPromise` at the
   * application edge (production: server.ts main; tests: at the test
   * boundary until comms-30i lifts the test suite).
   */
  readonly done: Effect.Effect<void>
  /**
   * Effect that interrupts the pump fiber. Cancelling unwinds the
   * stream's acquireRelease (calling `iterator.return()`) and any
   * pending `Schedule` backoff wait. Running this Effect more than once
   * is a no-op once the fiber is interrupted.
   */
  readonly cancel: Effect.Effect<void>
}

const originatorId = (event: InboundEvent): IdentityId =>
  Match.value(event).pipe(
    Match.discriminatorsExhaustive('kind')({
      'message-posted': (message) => message.message.sender.id,
      'mention-received': (message) => message.message.sender.id,
      'reaction-added': (reaction) => reaction.by.id,
      'reaction-removed': (reaction) => reaction.by.id,
    }),
  )

const dedupeById = (identities: ReadonlyArray<Identity>): ReadonlyArray<Identity> =>
  Arr.dedupeWith(identities, (a, b) => a.id === b.id)

const identitiesIn = (event: InboundEvent): ReadonlyArray<Identity> =>
  Match.value(event).pipe(
    Match.discriminatorsExhaustive('kind')({
      'message-posted': (message) =>
        dedupeById([message.message.sender, ...message.message.mentions]),
      'mention-received': (message) =>
        dedupeById([message.message.sender, ...message.message.mentions, ...message.mentions]),
      'reaction-added': (reaction) => dedupeById([reaction.by]),
      'reaction-removed': (reaction) => dedupeById([reaction.by]),
    }),
  )

/**
 * Self-echo guard (comms-vkx). The substrate's events queue replays
 * the bot's own posts and reactions back to it — useful to no
 * subscriber and forces every consumer to reimplement the same
 * filter. Drop here so the inbound stream contains only events the
 * bot didn't originate. Pre-acquire (botIdentityId undefined) we
 * cannot compare, so deliver everything — the ephemeral cache mints
 * after first attribution call, before which there are no self-posts
 * to echo anyway.
 */
const isSelfEvent = (event: InboundEvent, botIdentityId: IdentityId | undefined): boolean =>
  botIdentityId !== undefined && originatorId(event) === botIdentityId

/**
 * A reaction event carries no substrate timestamp (the events queue
 * reports the reaction without one), so the pump stamps it with the
 * current time read from Effect's `Clock`. The seconds-resolution value
 * is non-negative by construction, so the `Timestamp` decode is
 * genuinely fatal-only — `Effect.orDie` is the sanctioned shape for a
 * clock-derived `Timestamp` (it cannot be negative, cannot fail
 * validation). Message-shaped events already carry the substrate's own
 * `ts`, so they render synchronously and never touch the clock.
 */
const renderEvent = (
  event: InboundEvent,
  botIdentityId: IdentityId | undefined,
): Effect.Effect<ChannelEventPayload> =>
  Match.value(event).pipe(
    Match.discriminatorsExhaustive('kind')({
      'reaction-added': (reaction) => renderReaction(reaction),
      'reaction-removed': (reaction) => renderReaction(reaction),
      'message-posted': (message) => Effect.succeed(formatMessage(message, botIdentityId)),
      'mention-received': (message) => Effect.succeed(formatMessage(message, botIdentityId)),
    }),
  )

const renderReaction = (
  event: Extract<InboundEvent, { readonly kind: 'reaction-added' | 'reaction-removed' }>,
): Effect.Effect<ChannelEventPayload> =>
  Clock.currentTimeMillis.pipe(
    Effect.flatMap((ms) => decodeTimestamp(Math.floor(ms / 1000))),
    Effect.orDie,
    Effect.map((ts) => formatReaction(event, ts)),
  )

/**
 * Bounded retention for the message-delivery dedup log (comms-oyy). A
 * single Zulip mention surfaces as both `message-posted` and
 * `mention-received` from the adapter (`messageToInboundEvents` in
 * `adapters/zulip/events.ts`) — the consumer should still see one
 * `<channel>` block, not two. Cap at a few hours of message volume so
 * the log can't grow without bound on a long-lived pump.
 */
const SEEN_DELIVERED_CAPACITY = 1024

/**
 * Has this message's `<channel>` block already been delivered to the
 * notifier? Only the two message-shaped event kinds use the dedup log;
 * reactions are a distinct delivery and share no key with messages.
 */
const recordMessageDelivery = (
  event: InboundEvent,
  seen: Map<MessageId, true>,
): 'duplicate' | 'fresh' | 'not-message' => {
  if (event.kind !== 'message-posted' && event.kind !== 'mention-received') {
    return 'not-message'
  }
  const id = event.message.ref.id
  if (seen.has(id)) return 'duplicate'
  seen.set(id, true)
  while (seen.size > SEEN_DELIVERED_CAPACITY) {
    const oldest = seen.keys().next().value
    if (oldest === undefined) break
    seen.delete(oldest)
  }
  return 'fresh'
}

/**
 * Dispatch-side failure (notifier throw, formatter throw,
 * `rememberIdentity` throw). Escapes `Stream.runForEach`
 * and is caught at the outer `Effect.catchTag` to drive the
 * fatal-park flow (comms-ian) — log + sticky error block + wait for
 * cancel before resolving `done`.
 */
class DispatchFailure extends Data.TaggedError('DispatchFailure')<{
  readonly cause: unknown
}> {}

/**
 * Start consuming `inbox.events()` and dispatch each event as a
 * formatted channel-event payload via `notifier`. The pump returns
 * a handle whose `done` and `cancel` are themselves Effects (compose
 * via `Fiber.await` and `Fiber.interrupt`). On cancel: subsequent
 * events are not delivered, the underlying Stream scope is closed,
 * and the pump fiber unwinds cleanly.
 *
 * `cancel` is registered as a scope finalizer by `server.ts`
 * `makeProgram` (the pump is a daemon, so scope-close alone won't stop
 * it), running before the release finalizer so SIGINT/SIGTERM cleanly
 * stop event delivery before `identity.release()` fires.
 *
 * Adapter-handled errors (rate-limit 429, BAD_EVENT_QUEUE_ID,
 * transient network/parse failures) never reach this layer — they're
 * absorbed inside the producer via `Schedule`-based retry
 * (`adapters/zulip/events.ts`). The Stream's error channel is `never`
 * by construction, so the pump only has to handle dispatch-side
 * failures: notifier throw, formatter throw, `rememberIdentity`
 * throw. Those escape `Stream.runForEach` as a
 * `DispatchFailure` and drive the fatal-park flow (comms-ian).
 * `onMention` returns an `Effect<void>` the pump sequences — its own
 * error channel is `void`, so a cursor-write failure the provider
 * already swallowed cannot fail the pump:
 *   1. Logs a stderr diagnostic.
 *   2. Pushes a `formatError('event-pump', …)` payload so every
 *      connected session sees a final visible
 *      `<channel source="commy" error_kind="event-pump">…</channel>`
 *      block describing the failure.
 *   3. Parks via `Effect.never` so `done` doesn't resolve. The MCP
 *      server stays connected and tool calls (`post`, `subscribe`,
 *      `react`, `list_*`, etc.) continue working — these
 *      dispatch-side errors represent either a programming bug we
 *      cannot auto-heal or a broken MCP transport that a fresh
 *      iterator will not fix.
 *   4. The park unwinds (and `done` resolves) only when `cancel()`
 *      fires (the pump-cancel scope finalizer on SIGINT/SIGTERM).
 */
export const startEventPump = (deps: EventPumpDeps): Effect.Effect<EventPumpHandle> =>
  Effect.gen(function* () {
    const seenDelivered = new Map<MessageId, true>()

    const dispatchEvent = (event: InboundEvent): Effect.Effect<void, DispatchFailure> =>
      Effect.suspend(() => {
        if (deps.match !== undefined && !deps.match(event)) return Effect.void
        const botIdentityId = deps.getBotIdentityId()
        if (isSelfEvent(event, botIdentityId)) return Effect.void

        if (deps.rememberIdentity !== undefined) {
          for (const identity of identitiesIn(event)) deps.rememberIdentity(identity)
        }

        const mentionAdvance =
          deps.onMention !== undefined && event.kind === 'mention-received'
            ? deps.onMention(event.message.ts)
            : Effect.void

        if (recordMessageDelivery(event, seenDelivered) === 'duplicate') return mentionAdvance

        return mentionAdvance.pipe(
          Effect.zipRight(renderEvent(event, botIdentityId)),
          Effect.flatMap((payload) =>
            Effect.tryPromise({
              try: () => deps.notifier(payload),
              catch: (cause) => new DispatchFailure({ cause }),
            }),
          ),
        )
      })

    const fatalPark = (cause: unknown): Effect.Effect<never> =>
      Effect.suspend(() => {
        // `message` is reused for the user-facing `<channel>` error block
        // via `formatError`, so it stays the short rendering rather than a
        // full `Cause.pretty` dump.
        const message = Predicate.isError(cause) ? cause.message : String(cause)
        return Effect.logError(`commy plugin: event-pump error: ${message}`).pipe(
          Effect.zipRight(
            Effect.tryPromise({
              try: () => deps.notifier(formatError('event-pump', message)),
              catch: () => undefined,
            }),
          ),
          Effect.ignore,
          Effect.zipRight(Effect.never),
        )
      })

    const program = Stream.runForEach(deps.inbox.events(), dispatchEvent).pipe(
      Effect.catchTag('DispatchFailure', (failure) => fatalPark(failure.cause)),
    )

    const fiber = yield* Effect.forkDaemon(program)

    return {
      done: Effect.asVoid(Fiber.await(fiber)),
      cancel: Effect.asVoid(Fiber.interrupt(fiber)),
    }
  })

/**
 * Adapt an MCP `Server` instance to the `Notifier` contract by
 * dual-emitting each payload as two notifications carrying the same
 * `{ content, meta }` frame:
 *
 * The two carriers DIVERGE by design (comms-dtcm): each host renders exactly
 * one of them, so the agent-display carrier can drop fields the machine carrier
 * keeps without starving any consumer.
 *
 * - `notifications/claude/channel` — the Claude-Code-host convention. The CC
 *   host (and the Discord plugin, against the same capability) wraps the params
 *   into a `<channel source="commy" {meta}>{content}</channel>` block, so
 *   its ENTIRE meta lands in the agent's turn. The bare numeric identity ids
 *   (`sender_id`, reaction `by_id`) are noise there — they collide with the
 *   equally-numeric `message_id` and tempt agents to quote a number instead of
 *   a name — so this carrier omits `IDENTITY_ID_META_KEYS`. Sender/reactor are
 *   surfaced by name only.
 * - `notifications/message` — the MCP-standard `LoggingMessageNotification`,
 *   the host-neutral carrier any standards-compliant MCP client can render
 *   (comms-bb7.1, decision recorded in comms-di3). Its params MUST satisfy the
 *   SDK's `LoggingMessageNotificationParamsSchema` (`level` required, `data`
 *   carries the payload), so the `{ content, meta }` frame is nested under
 *   `data` rather than placed at the params root — a raw frame would fail
 *   validation in the very clients this carrier targets. `logger` names the
 *   emitter, the neutral analogue of the CC host's `source="commy"`.
 *   This carrier keeps the FULL meta, including the identity ids machine
 *   consumers key on (Hermes `SessionSource.user_id`).
 */
export const channelNotifier =
  (mcp: Server): Notifier =>
  (payload: ChannelEventPayload): Promise<void> => {
    const machineFrame = { content: payload.content, meta: payload.meta }
    const displayMeta = Arr.reduce(IDENTITY_ID_META_KEYS, payload.meta, (meta, key) =>
      Rec.remove(meta, key),
    )
    const displayFrame = { content: payload.content, meta: displayMeta }
    return Promise.all([
      mcp.notification({ method: 'notifications/claude/channel', params: displayFrame }),
      mcp.notification({
        method: 'notifications/message',
        params: { level: 'info', logger: 'commy', data: machineFrame },
      }),
    ]).then(() => undefined)
  }
