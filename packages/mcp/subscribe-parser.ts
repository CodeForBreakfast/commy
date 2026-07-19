import type {
  ChannelName as ChannelNameType,
  SubscriptionTarget,
  ThreadName as ThreadNameType,
} from '@commy/core/ports'
import { decodeChannelName, decodeThreadName } from '@commy/core/ports'
import { Data, Effect, Match, Option, String as Str } from 'effect'

/**
 * Parser-stage representation of `COMMY_SUBSCRIBE` tokens and
 * `subscribe`/`unsubscribe` MCP tool arguments. Carries the same channel
 * *address* (a `ChannelName`) as the port's `SubscriptionTarget`; the two
 * differ only in the thread/new-topics wrapper shapes, so `intentToTarget`
 * is a pure re-shaping with nothing to resolve.
 */
export type SubscribeIntent =
  | { readonly kind: 'channel'; readonly channelName: ChannelNameType }
  | {
      readonly kind: 'thread'
      readonly channelName: ChannelNameType
      readonly threadName: ThreadNameType
    }
  | { readonly kind: 'new-topics-in-channel'; readonly channelName: ChannelNameType }

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

/**
 * `None` means the token is valid but names no narrow — the caller registers
 * nothing and reports success. `mentions` is the only such token: mentions are
 * implicit and unconditional, so there is nothing to subscribe to. It parses
 * rather than failing so a config written before the keyword was retired still
 * boots; a later grammar change retires the dead form for good.
 */
export const parseSubscribeTarget = (
  token: string,
): Effect.Effect<Option.Option<SubscribeIntent>, SubscribeTokenError> =>
  Effect.gen(function* () {
    if (Str.isEmpty(token)) {
      return yield* new SubscribeTokenError({ token, reason: 'token is empty' })
    }
    if (token !== Str.trim(token)) {
      return yield* new SubscribeTokenError({
        token,
        reason: 'leading/trailing whitespace is not allowed',
      })
    }

    if (token === MENTIONS_LITERAL) {
      return Option.none()
    }

    if (token.startsWith(CHANNEL_PREFIX)) {
      const channelName = yield* decodeChannelName(token.slice(CHANNEL_PREFIX.length)).pipe(
        asTokenError(token, 'channel name after "channel:" must not be empty'),
      )
      return Option.some({ kind: 'channel', channelName } as const)
    }

    if (token.startsWith(NEW_TOPICS_PREFIX)) {
      const channelName = yield* decodeChannelName(token.slice(NEW_TOPICS_PREFIX.length)).pipe(
        asTokenError(token, 'channel name after "new-topics:" must not be empty'),
      )
      return Option.some({ kind: 'new-topics-in-channel', channelName } as const)
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
      return Option.some({ kind: 'thread', channelName, threadName } as const)
    }

    return yield* new SubscribeTokenError({
      token,
      reason: `unknown prefix — expected "channel:<name>", "thread:<channel>/<thread>", or "new-topics:<channel>"`,
    })
  })

/**
 * Map a parsed SubscribeIntent to the port-shaped SubscriptionTarget the
 * adapter expects. Channels are addressed by name, so this is a pure
 * re-shaping — the `channel:` arm collapses to the bare `ChannelName`, and
 * the thread / new-topics arms wrap it in their port record.
 */
export const intentToTarget = (intent: SubscribeIntent): SubscriptionTarget =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      channel: ({ channelName }): SubscriptionTarget => channelName,
      'new-topics-in-channel': ({ channelName }): SubscriptionTarget => ({
        kind: 'new-topics-in-channel',
        channel: channelName,
      }),
      thread: ({ channelName, threadName }): SubscriptionTarget => ({
        channel: channelName,
        thread: threadName,
      }),
    }),
  )
