import type { IdentityId, InboundEvent, MessageRef } from '@codeforbreakfast/core/ports'
import { Match } from 'effect'
import type { SubscribeIntent } from './subscribe-parser.ts'

/**
 * Plugin-layer narrow filter for inbound events (ass-220u).
 *
 * The commy Zulip adapter ships the minter's full event stream
 * — every public stream the minter is subscribed to. The pump tees
 * an event to the MCP host only if at least one narrow intent in this
 * set matches. Empty narrow → no events delivered, matching today's
 * "you only see what you subscribed to" semantics.
 *
 * Adding / removing intents is local-only; nothing here touches the
 * realm. Substrate-side minter-to-stream subscription is owned by the
 * boot-time reconciler (ass-6a77) plus the per-session substrate POST
 * inside `inbox.subscribe()` for streams created after the plugin
 * booted.
 *
 * `mentions` matches only when the bot identity is known (i.e.
 * post-acquire). Pre-acquire it never matches a `message-posted`
 * event — there is nothing to compare the message's mentions
 * against.
 */
export interface NarrowSet {
  add(intent: SubscribeIntent): void
  remove(intent: SubscribeIntent): void
  matches(event: InboundEvent, botIdentityId: IdentityId | undefined): boolean
  size(): number
}

const MENTIONS_KEY = 'mentions' as const

/**
 * Internal key representation. A template-literal union (instead of
 * plain `string`) lets the compiler verify that every key constructed
 * inside this module is one of the three legal shapes. The Set is
 * typed as `Set<IntentKey>`, so `has()` rejects free-form strings —
 * the only entry points are `intentKey`, `channelKey`, and
 * `threadKey` below.
 */
type IntentKey =
  | typeof MENTIONS_KEY
  | `channel:${string}`
  | `thread:${string}/${string}`
  | `new-topics:${string}`

const intentKey = (intent: SubscribeIntent): IntentKey =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      mentions: (): IntentKey => MENTIONS_KEY,
      channel: (i): IntentKey => `channel:${i.channelName}`,
      thread: (i): IntentKey => `thread:${i.channelName}/${i.threadName}`,
      'new-topics-in-channel': (i): IntentKey => `new-topics:${i.channelName}`,
    }),
  )

const channelKey = (ref: MessageRef): IntentKey => `channel:${ref.channel.name}`

const newTopicsKey = (ref: MessageRef): IntentKey => `new-topics:${ref.channel.name}`

const threadKey = (ref: MessageRef): IntentKey | undefined =>
  ref.thread === undefined ? undefined : `thread:${ref.channel.name}/${ref.thread.name}`

const refMatches = (ref: MessageRef, intents: ReadonlySet<IntentKey>): boolean => {
  if (intents.has(channelKey(ref))) return true
  const tk = threadKey(ref)
  return tk !== undefined && intents.has(tk)
}

/**
 * First-message-per-topic match for a `new-topics:<ch>` intent
 * (comms-bb7.2). A `new-topics` narrow delivers the first message of each
 * topic in a channel exactly once, consistent with the catch-up layer's
 * `firstMessagePerTopic` (channels-catch-up.ts) and the port contract
 * (ports.ts `NewTopicsInChannelSubscription`) — not channel-wide like a
 * `channel:<ch>` narrow.
 *
 * This is the one stateful match in the set: the decision *is* the record,
 * so it mutates `seenTopics`. A topic is identified by the same key shape a
 * `thread:` intent uses (`thread:<ch>/<topic>`), kept in a set separate from
 * `intents`. Topic-less messages never surface — "new topic" presupposes a
 * topic. `seenTopics` persists for the set's lifetime (an unsubscribe /
 * resubscribe does not re-surface an already-seen topic), matching the
 * port's "exactly once per adapter-instance lifetime" semantics.
 *
 * The pump calls `matches` once per event before its self-echo and dedup
 * guards (event-pump.ts), which is intentional: the bot's own first post in
 * a topic marks it seen (the bot created the topic; replies aren't "new"),
 * and new-topics only matches `message-posted`, so a given message id reaches
 * this path once.
 */
const newTopicsMatches = (
  ref: MessageRef,
  intents: ReadonlySet<IntentKey>,
  seenTopics: Set<IntentKey>,
): boolean => {
  const tk = threadKey(ref)
  if (tk === undefined) return false
  if (!intents.has(newTopicsKey(ref))) return false
  if (seenTopics.has(tk)) return false
  seenTopics.add(tk)
  return true
}

const messagePostedMatches = (
  ref: MessageRef,
  intents: ReadonlySet<IntentKey>,
  seenTopics: Set<IntentKey>,
): boolean => refMatches(ref, intents) || newTopicsMatches(ref, intents, seenTopics)

const mentionsMatches = (
  intents: ReadonlySet<IntentKey>,
  mentions: ReadonlyArray<{ readonly id: IdentityId }>,
  botIdentityId: IdentityId | undefined,
): boolean => {
  if (botIdentityId === undefined) return false
  if (!intents.has(MENTIONS_KEY)) return false
  return mentions.some((m) => m.id === botIdentityId)
}

export const createNarrowSet = (): NarrowSet => {
  const intents = new Set<IntentKey>()
  const seenTopics = new Set<IntentKey>()
  return {
    add: (intent) => {
      intents.add(intentKey(intent))
    },
    remove: (intent) => {
      intents.delete(intentKey(intent))
    },
    size: () => intents.size,
    matches: (event, botIdentityId): boolean =>
      Match.value(event).pipe(
        Match.discriminatorsExhaustive('kind')({
          'message-posted': (e) =>
            messagePostedMatches(e.message.ref, intents, seenTopics) ||
            mentionsMatches(intents, e.message.mentions, botIdentityId),
          'mention-received': (e) =>
            refMatches(e.message.ref, intents) ||
            mentionsMatches(intents, e.mentions, botIdentityId),
          'reaction-added': (e) => refMatches(e.target, intents),
          'reaction-removed': (e) => refMatches(e.target, intents),
        }),
      ),
  }
}
