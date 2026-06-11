import type {
  ChannelName as ChannelNameType,
  SubscriptionTarget,
  ThreadName as ThreadNameType,
} from '@codeforbreakfast/core/ports'
import { decodeChannelId, decodeChannelName, decodeThreadName } from '@codeforbreakfast/core/ports'
import { Data, Effect, Match } from 'effect'

/**
 * Parser-stage representation of `COMMY_SUBSCRIBE` tokens and
 * `subscribe`/`unsubscribe` MCP tool arguments. Distinct from
 * `SubscriptionTarget` (in `core/ports.ts`) which requires a resolved
 * `ChannelId` — substrate resolution happens downstream once an
 * adapter is available.
 */
export type SubscribeIntent =
  | { readonly kind: 'channel'; readonly channelName: ChannelNameType }
  | {
      readonly kind: 'thread'
      readonly channelName: ChannelNameType
      readonly threadName: ThreadNameType
    }
  | { readonly kind: 'new-topics-in-channel'; readonly channelName: ChannelNameType }
  | { readonly kind: 'mentions' }

export class SubscribeTokenError extends Data.TaggedError('SubscribeTokenError')<{
  readonly token: string
  readonly reason: string
}> {
  override get message(): string {
    return `invalid subscribe token "${this.token}" — ${this.reason}`
  }
}

const CHANNEL_PREFIX = 'channel:'
const THREAD_PREFIX = 'thread:'
const NEW_TOPICS_PREFIX = 'new-topics:'
const MENTIONS_LITERAL = 'mentions'

/**
 * Non-emptiness is the only thing the name brands validate, so a decode
 * failure here means the slice was empty — re-surfaced as the grammar-level
 * `SubscribeTokenError` the caller already handles, never as a raw
 * `ParseError`.
 */
const asTokenError = (token: string, reason: string) =>
  Effect.mapError(() => new SubscribeTokenError({ token, reason }))

export const parseSubscribeTarget = (
  token: string,
): Effect.Effect<SubscribeIntent, SubscribeTokenError> =>
  Effect.gen(function* () {
    if (token.length === 0) {
      return yield* new SubscribeTokenError({ token, reason: 'token is empty' })
    }
    if (token !== token.trim()) {
      return yield* new SubscribeTokenError({
        token,
        reason: 'leading/trailing whitespace is not allowed',
      })
    }

    if (token === MENTIONS_LITERAL) {
      return { kind: 'mentions' } as const
    }

    if (token.startsWith(CHANNEL_PREFIX)) {
      const channelName = yield* decodeChannelName(token.slice(CHANNEL_PREFIX.length)).pipe(
        asTokenError(token, 'channel name after "channel:" must not be empty'),
      )
      return { kind: 'channel', channelName } as const
    }

    if (token.startsWith(NEW_TOPICS_PREFIX)) {
      const channelName = yield* decodeChannelName(token.slice(NEW_TOPICS_PREFIX.length)).pipe(
        asTokenError(token, 'channel name after "new-topics:" must not be empty'),
      )
      return { kind: 'new-topics-in-channel', channelName } as const
    }

    if (token.startsWith(THREAD_PREFIX)) {
      const rest = token.slice(THREAD_PREFIX.length)
      const slashAt = rest.indexOf('/')
      if (slashAt < 0) {
        return yield* new SubscribeTokenError({
          token,
          reason: 'thread: requires "<channel>/<thread>" — missing "/"',
        })
      }
      const channelName = yield* decodeChannelName(rest.slice(0, slashAt)).pipe(
        asTokenError(token, 'channel name in thread:<channel>/<thread> must not be empty'),
      )
      const threadName = yield* decodeThreadName(rest.slice(slashAt + 1)).pipe(
        asTokenError(token, 'thread name in thread:<channel>/<thread> must not be empty'),
      )
      return { kind: 'thread', channelName, threadName } as const
    }

    return yield* new SubscribeTokenError({
      token,
      reason: `unknown prefix — expected "channel:<name>", "thread:<channel>/<thread>", "new-topics:<channel>", or "mentions"`,
    })
  })

/**
 * Map a parsed SubscribeIntent to the port-shaped SubscriptionTarget the
 * adapter expects. The ChannelId is synthesised from the name — Zulip
 * addresses channels by name on the wire, and memory uses the id only
 * as a cache key, so a name-shaped placeholder is faithful for both V1
 * substrates. A future substrate that demands a real id at subscribe
 * time should introduce a port-level resolver and replace this mapping.
 *
 * The id decode is dieable, not a real failure mode: `intent.channelName`
 * is already a validated non-empty `ChannelName`, and `ChannelId` carries
 * the same non-empty constraint, so the decode provably cannot fail.
 */
export const intentToTarget = (intent: SubscribeIntent): Effect.Effect<SubscriptionTarget> =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      mentions: () => Effect.succeed('mentions' as const),
      channel: ({ channelName }) =>
        decodeChannelId(channelName).pipe(
          Effect.map((id) => ({ id, name: channelName })),
          Effect.orDie,
        ),
      'new-topics-in-channel': ({ channelName }) =>
        decodeChannelId(channelName).pipe(
          Effect.map((id) => ({
            kind: 'new-topics-in-channel' as const,
            channel: { id, name: channelName },
          })),
          Effect.orDie,
        ),
      thread: ({ channelName, threadName }) =>
        decodeChannelId(channelName).pipe(
          Effect.map((id) => ({
            channel: { id, name: channelName },
            thread: { name: threadName },
          })),
          Effect.orDie,
        ),
    }),
  )
