import type {
  ChannelName,
  IdentityId,
  InboundEvent,
  MessageRef,
  ThreadName,
} from '@commy/core/ports'
import { Array as Arr, Data, HashSet, Match, Option } from 'effect'
import type { SubscribeIntent } from './subscribe-parser.ts'

/**
 * Plugin-layer narrow filter for inbound events.
 *
 * The commy Zulip adapter ships the minter's full event stream
 * — every public stream the minter is subscribed to. The pump tees
 * an event to the MCP host only if at least one narrow intent in this
 * set matches. Empty narrow → no events delivered, matching today's
 * "you only see what you subscribed to" semantics.
 *
 * Adding / removing intents is local-only; nothing here touches the
 * realm. Substrate-side minter-to-stream subscription is owned by the
 * boot-time reconciler plus the per-session substrate POST
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
  /**
   * Snapshot the current subscription intents (the membership set, not the
   * `seenTopics` first-message ledger). The persistence layer
   * captures this on every mutation so a resumed session restores the exact
   * set it had.
   */
  intents(): ReadonlyArray<SubscribeIntent>
  /**
   * Replace the entire membership set with `intents`, dropping whatever was
   * there before. Used once on resume to restore the persisted
   * set — including prior unsubscribes — over whatever boot-time seeding ran.
   */
  replace(intents: ReadonlyArray<SubscribeIntent>): void
}

/**
 * Internal key representation. Each key is a `Data.struct`, so membership
 * is by structural value equality rather than the JS `===` of a
 * delimiter-joined string. Two distinct `(channel, thread)` pairs whose
 * names happen to contain `:` or `/` can no longer alias the same key —
 * the channel and thread names live in separate, individually-compared
 * fields instead of a single `thread:<ch>/<thread>` literal. Both the
 * `intents` membership set and the `seenTopics` first-message ledger key
 * on this type.
 */
const MENTIONS_KEY = Data.struct({ kind: 'mentions' as const })
const channelKeyOf = (channelName: ChannelName) =>
  Data.struct({ kind: 'channel' as const, channelName })
const threadKeyOf = (channelName: ChannelName, threadName: ThreadName) =>
  Data.struct({ kind: 'thread' as const, channelName, threadName })
const newTopicsKeyOf = (channelName: ChannelName) =>
  Data.struct({ kind: 'new-topics' as const, channelName })

type IntentKey =
  | typeof MENTIONS_KEY
  | ReturnType<typeof channelKeyOf>
  | ReturnType<typeof threadKeyOf>
  | ReturnType<typeof newTopicsKeyOf>

const intentKey = (intent: SubscribeIntent): IntentKey =>
  Match.value(intent).pipe(
    Match.discriminatorsExhaustive('kind')({
      mentions: (): IntentKey => MENTIONS_KEY,
      channel: (i): IntentKey => channelKeyOf(i.channelName),
      thread: (i): IntentKey => threadKeyOf(i.channelName, i.threadName),
      'new-topics-in-channel': (i): IntentKey => newTopicsKeyOf(i.channelName),
    }),
  )

/**
 * Reverse of {@link intentKey}: reconstruct the `SubscribeIntent` a stored key
 * came from. The only non-identity mapping is `new-topics` → the intent's
 * `new-topics-in-channel` discriminator (the key normalises the longer name).
 */
const keyToIntent = (key: IntentKey): SubscribeIntent =>
  Match.value(key).pipe(
    Match.discriminatorsExhaustive('kind')({
      mentions: (): SubscribeIntent => ({ kind: 'mentions' }),
      channel: (k): SubscribeIntent => ({ kind: 'channel', channelName: k.channelName }),
      thread: (k): SubscribeIntent => ({
        kind: 'thread',
        channelName: k.channelName,
        threadName: k.threadName,
      }),
      'new-topics': (k): SubscribeIntent => ({
        kind: 'new-topics-in-channel',
        channelName: k.channelName,
      }),
    }),
  )

const channelKey = (ref: MessageRef): IntentKey => channelKeyOf(ref.channel.name)

const newTopicsKey = (ref: MessageRef): IntentKey => newTopicsKeyOf(ref.channel.name)

const threadKey = (ref: MessageRef): Option.Option<IntentKey> =>
  Option.map(ref.thread, (thread) => threadKeyOf(ref.channel.name, thread.name))

const refMatches = (ref: MessageRef, intents: HashSet.HashSet<IntentKey>): boolean =>
  HashSet.has(intents, channelKey(ref)) ||
  Option.exists(threadKey(ref), (tk) => HashSet.has(intents, tk))

const mentionsMatches = (
  intents: HashSet.HashSet<IntentKey>,
  mentions: ReadonlyArray<{ readonly id: IdentityId }>,
  botIdentityId: IdentityId | undefined,
): boolean => {
  if (botIdentityId === undefined) return false
  if (!HashSet.has(intents, MENTIONS_KEY)) return false
  return mentions.some((m) => m.id === botIdentityId)
}

export const createNarrowSet = (): NarrowSet => {
  let intents = HashSet.empty<IntentKey>()
  let seenTopics = HashSet.empty<IntentKey>()

  /**
   * First-message-per-topic match for a `new-topics:<ch>` intent. A
   * `new-topics` narrow delivers the first message of each
   * topic in a channel exactly once, consistent with the catch-up layer's
   * `firstMessagePerTopic` (channels-catch-up.ts) and the port contract
   * (ports.ts `NewTopicsInChannelSubscription`) — not channel-wide like a
   * `channel:<ch>` narrow.
   *
   * This is the one stateful match in the set: the decision *is* the record,
   * so it appends to `seenTopics`. A topic is identified by the same key a
   * `thread:` intent uses, kept in a set separate from `intents`. Topic-less
   * messages never surface — "new topic" presupposes a topic. `seenTopics`
   * persists for the set's lifetime (an unsubscribe / resubscribe does not
   * re-surface an already-seen topic), matching the port's "exactly once per
   * adapter-instance lifetime" semantics.
   *
   * The pump calls `matches` once per event before its self-echo and dedup
   * guards (event-pump.ts), which is intentional: the bot's own first post in
   * a topic marks it seen (the bot created the topic; replies aren't "new"),
   * and new-topics only matches `message-posted`, so a given message id reaches
   * this path once.
   */
  const newTopicsMatches = (ref: MessageRef): boolean =>
    Option.match(threadKey(ref), {
      onNone: () => false,
      onSome: (tk) => {
        if (!HashSet.has(intents, newTopicsKey(ref))) return false
        if (HashSet.has(seenTopics, tk)) return false
        seenTopics = HashSet.add(seenTopics, tk)
        return true
      },
    })

  const messagePostedMatches = (ref: MessageRef): boolean =>
    refMatches(ref, intents) || newTopicsMatches(ref)

  return {
    add: (intent) => {
      intents = HashSet.add(intents, intentKey(intent))
    },
    remove: (intent) => {
      intents = HashSet.remove(intents, intentKey(intent))
    },
    size: () => HashSet.size(intents),
    intents: () => Arr.fromIterable(intents).map(keyToIntent),
    replace: (next) => {
      intents = HashSet.fromIterable(next.map(intentKey))
    },
    matches: (event, botIdentityId): boolean =>
      Match.value(event).pipe(
        Match.discriminatorsExhaustive('kind')({
          'message-posted': (e) =>
            messagePostedMatches(e.message.ref) ||
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
