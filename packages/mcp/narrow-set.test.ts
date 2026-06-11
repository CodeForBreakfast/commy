import { expect, test } from 'bun:test'
import type {
  Identity,
  IdentityId as IdentityIdType,
  InboundEvent,
  MessageRef,
} from '@codeforbreakfast/core/ports'
import {
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
} from '@codeforbreakfast/core/ports'
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
  }
  return threadName === undefined
    ? { id: decodeMessageIdSync(`msg-${channelName}`), channel }
    : {
        id: decodeMessageIdSync(`msg-${channelName}-${threadName}`),
        channel,
        thread: { name: decodeThreadNameSync(threadName) },
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
    mentions,
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
    mentions,
    reactions: [],
  },
  mentions,
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
