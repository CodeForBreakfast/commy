import { expect, test } from 'bun:test'
import type {
  Identity,
  IdentityId as IdentityIdType,
  InboundEvent,
  MessageRef,
} from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
  userMentions,
} from '@commy/core/ports'
import { Option } from 'effect'
import { createNarrowSet } from './narrow-set.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'

const buildIdentity = (id: string, name: string, kind: 'agent' | 'human' = 'agent'): Identity => ({
  id: decodeIdentityIdSync(id),
  name: decodeDisplayNameSync(name),
  kind,
})

const buildMessageRef = (channelName: string, threadName?: string): MessageRef => {
  const channel = {
    id: decodeChannelIdSync(channelName),
    name: decodeChannelNameSync(channelName),
    permalink: ChannelPermalinkSchema.make(
      `https://zulip.example.com/#narrow/channel/${channelName}`,
    ),
  }
  return threadName === undefined
    ? {
        id: decodeMessageIdSync(`msg-${channelName}`),
        channel,
        thread: Option.none(),
        permalink: MessagePermalinkSchema.make(
          `https://zulip.example.com/#narrow/channel/${channelName}/near/1`,
        ),
      }
    : {
        id: decodeMessageIdSync(`msg-${channelName}-${threadName}`),
        channel,
        thread: Option.some({
          name: decodeThreadNameSync(threadName),
          resolved: false,
          permalink: ThreadPermalinkSchema.make(
            `https://zulip.example.com/#narrow/channel/${channelName}/topic/${threadName}`,
          ),
        }),
        permalink: MessagePermalinkSchema.make(
          `https://zulip.example.com/#narrow/channel/${channelName}/topic/${threadName}/near/1`,
        ),
      }
}

const buildMessagePosted = (
  channelName: string,
  threadName: string | undefined,
  mentions: ReadonlyArray<Identity> = [],
): InboundEvent => ({
  kind: 'message-posted',
  message: {
    ref: buildMessageRef(channelName, threadName),
    sender: buildIdentity('user:alice', 'alice'),
    body: decodeMessageBodySync('hi'),
    ts: decodeTimestampSync(1_700_000_000),
    mentions: userMentions(mentions),
    reactions: [],
  },
})

const buildMentionReceived = (
  channelName: string,
  mentions: ReadonlyArray<Identity>,
): InboundEvent => ({
  kind: 'mention-received',
  message: {
    ref: buildMessageRef(channelName),
    sender: buildIdentity('user:alice', 'alice'),
    body: decodeMessageBodySync('hi'),
    ts: decodeTimestampSync(1_700_000_000),
    mentions: userMentions(mentions),
    reactions: [],
  },
  mentions: userMentions(mentions),
})

const buildReactionAdded = (channelName: string, threadName?: string): InboundEvent => ({
  kind: 'reaction-added',
  target: buildMessageRef(channelName, threadName),
  emoji: decodeEmojiSync('thumbs_up'),
  by: buildIdentity('user:bob', 'bob'),
})

const buildReactionRemoved = (channelName: string, threadName?: string): InboundEvent => ({
  kind: 'reaction-removed',
  target: buildMessageRef(channelName, threadName),
  emoji: decodeEmojiSync('thumbs_up'),
  by: buildIdentity('user:bob', 'bob'),
})

const channelIntent = (channel: string): SubscribeIntent => ({
  kind: 'channel',
  channelName: decodeChannelNameSync(channel),
})

const threadIntent = (channel: string, thread: string): SubscribeIntent => ({
  kind: 'thread',
  channelName: decodeChannelNameSync(channel),
  threadName: decodeThreadNameSync(thread),
})

const mentionsIntent = (): SubscribeIntent => ({ kind: 'mentions' })

const newTopicsIntent = (channel: string): SubscribeIntent => ({
  kind: 'new-topics-in-channel',
  channelName: decodeChannelNameSync(channel),
})

const noBot: IdentityIdType | undefined = undefined

test('empty narrow set matches nothing', () => {
  const set = createNarrowSet()
  expect(set.size()).toBe(0)
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
  expect(set.matches(buildReactionAdded('home'), noBot)).toBe(false)
})

test('channel narrow matches message-posted on that channel', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('other', undefined), noBot)).toBe(false)
})

test('channel narrow matches message-posted in any thread of that channel', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'payments'), noBot)).toBe(true)
})

test('thread narrow matches only the named thread within the channel', () => {
  const set = createNarrowSet()
  set.add(threadIntent('home', 'payments'))
  expect(set.matches(buildMessagePosted('home', 'payments'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('home', 'breakfast'), noBot)).toBe(false)
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
  expect(set.matches(buildMessagePosted('other', 'payments'), noBot)).toBe(false)
})

test('thread narrow does not alias a distinct channel/thread pair sharing a slash boundary', () => {
  const set = createNarrowSet()
  set.add(threadIntent('a', 'b/c'))
  expect(set.matches(buildMessagePosted('a', 'b/c'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('a/b', 'c'), noBot)).toBe(false)
})

test('channel narrow matches reaction-added/removed on that channel', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  expect(set.matches(buildReactionAdded('home'), noBot)).toBe(true)
  expect(set.matches(buildReactionRemoved('home'), noBot)).toBe(true)
  expect(set.matches(buildReactionAdded('other'), noBot)).toBe(false)
})

test('thread narrow matches reactions on a message in that thread', () => {
  const set = createNarrowSet()
  set.add(threadIntent('home', 'payments'))
  expect(set.matches(buildReactionAdded('home', 'payments'), noBot)).toBe(true)
  expect(set.matches(buildReactionAdded('home', 'other'), noBot)).toBe(false)
})

test('mentions narrow matches message-posted only when bot is among mentions', () => {
  const set = createNarrowSet()
  set.add(mentionsIntent())
  const bot = buildIdentity('bot:me', 'me')
  const other = buildIdentity('user:other', 'other')
  expect(set.matches(buildMessagePosted('home', undefined, [bot]), bot.id)).toBe(true)
  expect(set.matches(buildMessagePosted('home', undefined, [other]), bot.id)).toBe(false)
})

test('mentions narrow does not match pre-acquire (no bot identity)', () => {
  const set = createNarrowSet()
  set.add(mentionsIntent())
  const bot = buildIdentity('bot:me', 'me')
  expect(set.matches(buildMessagePosted('home', undefined, [bot]), noBot)).toBe(false)
})

test('mentions narrow matches mention-received events whenever bot is in mentions', () => {
  const set = createNarrowSet()
  set.add(mentionsIntent())
  const bot = buildIdentity('bot:me', 'me')
  expect(set.matches(buildMentionReceived('home', [bot]), bot.id)).toBe(true)
})

test('combined channel + thread + mentions narrows widen the match window', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(threadIntent('llm-feed', 'paper-2026'))
  set.add(mentionsIntent())
  const bot = buildIdentity('bot:me', 'me')
  expect(set.matches(buildMessagePosted('home', undefined), bot.id)).toBe(true)
  expect(set.matches(buildMessagePosted('llm-feed', 'paper-2026'), bot.id)).toBe(true)
  expect(set.matches(buildMessagePosted('llm-feed', 'other'), bot.id)).toBe(false)
  expect(set.matches(buildMessagePosted('elsewhere', undefined, [bot]), bot.id)).toBe(true)
})

test('remove drops an intent so subsequent events no longer match', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(true)
  set.remove(channelIntent('home'))
  expect(set.size()).toBe(0)
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
})

test('remove on an unknown intent is a no-op', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.remove(channelIntent('elsewhere'))
  expect(set.size()).toBe(1)
})

test('adding the same channel intent twice is idempotent', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(channelIntent('home'))
  expect(set.size()).toBe(1)
  set.remove(channelIntent('home'))
  expect(set.size()).toBe(0)
})

test('thread and channel narrows for the same channel coexist as distinct entries', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(threadIntent('home', 'payments'))
  expect(set.size()).toBe(2)
})

test('new-topics narrow matches the first message of a topic on that channel', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('elsewhere', 'fresh-topic'), noBot)).toBe(false)
})

test('new-topics narrow does not match a topic-less message', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
})

test('new-topics narrow does not match a second message in an already-seen topic', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(false)
})

test('new-topics narrow tracks each topic independently', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'topic-a'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('home', 'topic-b'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('home', 'topic-a'), noBot)).toBe(false)
  expect(set.matches(buildMessagePosted('home', 'topic-b'), noBot)).toBe(false)
})

test('new-topics first-per-topic is scoped per channel, not channel-wide', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  set.add(newTopicsIntent('elsewhere'))
  expect(set.matches(buildMessagePosted('home', 'shared-name'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('elsewhere', 'shared-name'), noBot)).toBe(true)
})

test('channel narrow still matches every message in a topic, even after new-topics has seen it', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(true)
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(true)
})

test('new-topics narrow does not match reactions on the channel', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildReactionAdded('home', 'fresh-topic'), noBot)).toBe(false)
  expect(set.matches(buildReactionRemoved('home', 'fresh-topic'), noBot)).toBe(false)
})

test('new-topics narrow does not match mention-received on the channel', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  const bot = buildIdentity('bot:me', 'me')
  expect(set.matches(buildMentionReceived('home', [bot]), bot.id)).toBe(false)
})

test('new-topics narrow coexists with channel narrow as a distinct entry', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(newTopicsIntent('home'))
  expect(set.size()).toBe(2)
})

test('new-topics narrow remove drops only the new-topics entry', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(newTopicsIntent('home'))
  set.remove(newTopicsIntent('home'))
  expect(set.size()).toBe(1)
  expect(set.matches(buildReactionAdded('home'), noBot)).toBe(true)
})

const sortIntents = (intents: ReadonlyArray<SubscribeIntent>): ReadonlyArray<SubscribeIntent> =>
  [...intents].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))

test('intents() on an empty set returns no intents', () => {
  const set = createNarrowSet()
  expect(set.intents()).toEqual([])
})

test('intents() returns every added intent, reconstructing the new-topics-in-channel kind', () => {
  const set = createNarrowSet()
  const added = [
    mentionsIntent(),
    channelIntent('home'),
    threadIntent('home', 'payments'),
    newTopicsIntent('general'),
  ]
  for (const intent of added) set.add(intent)
  expect(sortIntents(set.intents())).toEqual(sortIntents(added))
})

test('intents() excludes a removed intent', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(mentionsIntent())
  set.remove(channelIntent('home'))
  expect(set.intents()).toEqual([mentionsIntent()])
})

test('intents() does not leak the seen-topics ledger after a new-topics match', () => {
  const set = createNarrowSet()
  set.add(newTopicsIntent('home'))
  expect(set.matches(buildMessagePosted('home', 'fresh-topic'), noBot)).toBe(true)
  expect(set.intents()).toEqual([newTopicsIntent('home')])
})

test('load(Some) sets the base on an empty set', () => {
  const set = createNarrowSet()
  set.load(Option.some([channelIntent('home'), mentionsIntent()]))
  expect(sortIntents(set.intents())).toEqual(sortIntents([channelIntent('home'), mentionsIntent()]))
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(true)
})

test('load(Some) replaces the prior base — the old set no longer matches', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.load(Option.some([channelIntent('work')]))
  expect(set.intents()).toEqual([channelIntent('work')])
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
  expect(set.matches(buildMessagePosted('work', undefined), noBot)).toBe(true)
})

test('load(Some([])) drops every narrow — matches nothing', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.add(mentionsIntent())
  set.load(Option.some([]))
  expect(set.intents()).toEqual([])
  expect(set.size()).toBe(0)
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
})

test('load(None) keeps the current base — the fresh-session fallback', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.load(Option.none())
  expect(set.intents()).toEqual([channelIntent('home')])
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(true)
})

test('a buffered subscribe replays onto the loaded base', () => {
  const set = createNarrowSet()
  set.beginBuffering()
  set.add(channelIntent('work'))
  // Restore installs the persisted base after the subscribe already happened.
  set.load(Option.some([channelIntent('home')]))
  expect(sortIntents(set.intents())).toEqual(
    sortIntents([channelIntent('home'), channelIntent('work')]),
  )
})

test('a buffered unsubscribe of a base member is applied after the base loads', () => {
  const set = createNarrowSet()
  set.beginBuffering()
  // The seat unsubscribes a sub the persisted set still holds, before restore ran.
  set.remove(channelIntent('home'))
  set.load(Option.some([channelIntent('home'), mentionsIntent()]))
  expect(set.intents()).toEqual([mentionsIntent()])
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
})

test('an env seed applied before buffering is not resurrected by load(Some) — dropped stays dropped', () => {
  const set = createNarrowSet()
  // COMMY_SUBSCRIBE base seeded at boot, before the buffering window opens.
  set.add(channelIntent('home'))
  set.beginBuffering()
  // Persisted set unsubscribed the env default; nothing re-adds it.
  set.load(Option.some([]))
  expect(set.intents()).toEqual([])
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(false)
})

test('load(None) keeps the env seed and applies buffered deltas', () => {
  const set = createNarrowSet()
  set.add(channelIntent('home'))
  set.beginBuffering()
  set.add(channelIntent('work'))
  set.load(Option.none())
  expect(sortIntents(set.intents())).toEqual(
    sortIntents([channelIntent('home'), channelIntent('work')]),
  )
})

test('a delta applies live during buffering — matches is not blacked out before load', () => {
  const set = createNarrowSet()
  set.beginBuffering()
  set.add(channelIntent('home'))
  expect(set.matches(buildMessagePosted('home', undefined), noBot)).toBe(true)
})
