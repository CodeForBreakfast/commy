import type {
  ChannelName as ChannelNameType,
  SubscriptionTarget,
  ThreadName as ThreadNameType,
} from '@commy/core/ports'
import { decodeChannelName, decodeThreadName } from '@commy/core/ports'
import { Data, Effect, Match, String as Str } from 'effect'

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

const NEW_TOPICS_PREFIX = 'new-topics:'

/**
 * Token forms the grammar used to accept, each mapped to the bare-path form
 * that replaced it. They are rejected rather than translated, and rejection is
 * the load-bearing part: channel names are validated for non-emptiness only, so
 * a surviving `channel:foo` would otherwise parse *successfully* as a channel
 * literally named `channel:foo`. The seat would boot, register a narrow against
 * a channel nobody posts to, and hear nothing — a silent misparse where the
 * operator sees no error at all.
 */
const RETIRED_FORMS = [
  { matches: (token: string) => token.startsWith('channel:'), replacement: '<channel>' },
  {
    matches: (token: string) => token.startsWith('thread:'),
    replacement: '<channel>/<thread>',
  },
  {
    matches: (token: string) => token === 'mentions',
    replacement: 'nothing — mentions of the bot arrive unconditionally and need no token',
  },
] as const

/**
 * Non-emptiness is the only thing the name brands validate, so a decode
 * failure here means the slice was empty — re-surfaced as the grammar-level
 * `SubscribeTokenError` the caller already handles, never as a raw
 * `ParseError`.
 */
const asTokenError = (token: string, reason: string) =>
  Effect.mapError(() => new SubscribeTokenError({ token, reason }))

/**
 * The grammar says exactly one thing per position: a bare path is *scope*, a
 * prefix is a *rule*.
 *
 *     foo               the channel
 *     foo/bar           one topic in it
 *     new-topics:foo    a rule over that channel — first-of-topic only
 *
 * `new-topics:` keeps its prefix because it is not a different place, it is a
 * different rule about which messages in a place reach you. Every other former
 * prefix was pure scope and carried no information once mentions left the
 * grammar, taking the last bare keyword with it and making a bare word
 * unambiguously a channel.
 */
export const parseSubscribeTarget = (
  token: string,
): Effect.Effect<SubscribeIntent, SubscribeTokenError> =>
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

    const retired = RETIRED_FORMS.find((form) => form.matches(token))
    if (retired !== undefined) {
      return yield* new SubscribeTokenError({
        token,
        reason: `retired token form — write ${retired.replacement}`,
      })
    }

    if (token.startsWith(NEW_TOPICS_PREFIX)) {
      const channelName = yield* decodeChannelName(token.slice(NEW_TOPICS_PREFIX.length)).pipe(
        asTokenError(token, 'channel name after "new-topics:" must not be empty'),
      )
      return { kind: 'new-topics-in-channel', channelName } as const
    }

    // Only the first `/` divides scope: channel names cannot contain one, but
    // Zulip topics can, so `foo/a/b` is topic `a/b` in channel `foo`.
    const slashAt = token.indexOf('/')
    if (slashAt < 0) {
      const channelName = yield* decodeChannelName(token).pipe(
        asTokenError(token, 'channel name must not be empty'),
      )
      return { kind: 'channel', channelName } as const
    }

    const channelName = yield* decodeChannelName(token.slice(0, slashAt)).pipe(
      asTokenError(token, 'channel name in <channel>/<thread> must not be empty'),
    )
    const threadName = yield* decodeThreadName(token.slice(slashAt + 1)).pipe(
      asTokenError(token, 'thread name in <channel>/<thread> must not be empty'),
    )
    return { kind: 'thread', channelName, threadName } as const
  })

/**
 * Map a parsed SubscribeIntent to the port-shaped SubscriptionTarget the
 * adapter expects. Channels are addressed by name, so this is a pure
 * re-shaping — the channel arm collapses to the bare `ChannelName`, and the
 * thread / new-topics arms wrap it in their port record.
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

/**
 * Render an intent back into the `COMMY_SUBSCRIBE` token that produces it —
 * the exact inverse of {@link parseSubscribeTarget}. Boot logs the applied set
 * in this vocabulary so an operator can compare the line against what they
 * configured, character for character, rather than against an internal shape.
 */
export const intentToToken = (intent: SubscribeIntent): string =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      channel: ({ channelName }) => channelName as string,
      'new-topics-in-channel': ({ channelName }) => `${NEW_TOPICS_PREFIX}${channelName}`,
      thread: ({ channelName, threadName }) => `${channelName}/${threadName}`,
    }),
  )
