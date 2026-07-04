import { expect, test } from 'bun:test'
import type {
  Identity,
  IdentityId as IdentityIdType,
  InboundEvent,
  Message,
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
} from '@commy/core/ports'
import { Option } from 'effect'
import { formatError, formatMessage, formatReaction } from './events.ts'

const BOT_ID: IdentityIdType = decodeIdentityIdSync('bot-42')

const paymentsThread = Option.some({
  name: decodeThreadNameSync('payments'),
  resolved: false,
  permalink: ThreadPermalinkSchema.make(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments',
  ),
})

const sender: Identity = {
  id: decodeIdentityIdSync('user-7'),
  name: decodeDisplayNameSync('Carol'),
  kind: 'human',
}

const alice: Identity = {
  id: decodeIdentityIdSync('user-11'),
  name: decodeDisplayNameSync('Alice'),
  kind: 'agent',
}
const bob: Identity = {
  id: decodeIdentityIdSync('user-12'),
  name: decodeDisplayNameSync('Bob'),
  kind: 'human',
}

const baseMessage = (overrides: Partial<Message> = {}): Message => ({
  ref: {
    id: decodeMessageIdSync('msg-1'),
    channel: {
      id: decodeChannelIdSync('chan-9'),
      name: decodeChannelNameSync('home'),
      permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-home'),
    },
    thread: paymentsThread,
    permalink: MessagePermalinkSchema.make(
      'https://zulip.example.com/#narrow/channel/9-home/topic/payments/near/1',
    ),
  },
  sender,
  body: decodeMessageBodySync('hello world'),
  ts: decodeTimestampSync(1715450000),
  mentions: [],
  reactions: [],
  ...overrides,
})

const messagePosted = (msg: Message): InboundEvent & { kind: 'message-posted' } => ({
  kind: 'message-posted',
  message: msg,
})

const mentionReceived = (
  msg: Message,
  mentions: ReadonlyArray<Identity>,
): InboundEvent & { kind: 'mention-received' } => ({
  kind: 'mention-received',
  message: msg,
  mentions,
})

test('formatMessage — content is the message body verbatim', () => {
  const out = formatMessage(
    messagePosted(baseMessage({ body: decodeMessageBodySync('hello world') })),
    BOT_ID,
  )
  expect(out.content).toBe('hello world')
})

test('formatMessage — body passes @**name** mention markers through verbatim', () => {
  // The body is the agent's authoritative "where was I mentioned" anchor: the
  // mention marker sits inline at the mention site. A future body-sanitiser
  // that stripped or transformed @**name** would blind the agent to where it
  // was addressed. This pins the passthrough.
  const body = 'hey @**cc-myproject-352c0d96** and @**Carol**, can you review?'
  const out = formatMessage(
    messagePosted(baseMessage({ body: decodeMessageBodySync(body) })),
    BOT_ID,
  )
  expect(out.content).toBe(body)
})

test('formatMessage — meta carries identifying attributes', () => {
  const out = formatMessage(messagePosted(baseMessage()), BOT_ID)
  expect(out.meta['channel_id']).toBe('chan-9')
  expect(out.meta['channel_name']).toBe('home')
  expect(out.meta['thread']).toBe('payments')
  expect(out.meta['message_id']).toBe('msg-1')
  expect(out.meta['sender_id']).toBe('user-7')
  expect(out.meta['sender_name']).toBe('Carol')
  expect(out.meta['sender_kind']).toBe('human')
  expect(out.meta['ts']).toBe('1715450000')
})

const messageWithPermalinks = (): Message =>
  baseMessage({
    ref: {
      id: decodeMessageIdSync('msg-1'),
      channel: {
        id: decodeChannelIdSync('chan-9'),
        name: decodeChannelNameSync('home'),
        permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-home'),
      },
      thread: paymentsThread,
      permalink: MessagePermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/9-home/topic/payments/near/1',
      ),
    },
  })

test('formatMessage — meta carries message, channel and topic permalinks', () => {
  const out = formatMessage(messagePosted(messageWithPermalinks()), BOT_ID)
  expect(out.meta['permalink']).toBe(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments/near/1',
  )
  expect(out.meta['channel_permalink']).toBe('https://zulip.example.com/#narrow/channel/9-home')
  expect(out.meta['thread_permalink']).toBe(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments',
  )
})

test('formatMessage — a top-level message still surfaces its message and channel permalinks', () => {
  // An observed message always carries a message permalink (and its channel a
  // channel permalink), so both are surfaced even for a threadless message —
  // only thread_permalink is conditional on the message having a thread.
  const noThread = baseMessage()
  const msg: Message = {
    ref: {
      id: noThread.ref.id,
      channel: noThread.ref.channel,
      thread: Option.none(),
      permalink: MessagePermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/9-home/near/1',
      ),
    },
    sender: noThread.sender,
    body: noThread.body,
    ts: noThread.ts,
    mentions: noThread.mentions,
    reactions: noThread.reactions,
  }
  const out = formatMessage(messagePosted(msg), BOT_ID)
  expect(out.meta['permalink']).toBe('https://zulip.example.com/#narrow/channel/9-home/near/1')
  expect(out.meta['channel_permalink']).toBe('https://zulip.example.com/#narrow/channel/9-home')
  expect(out.meta).not.toHaveProperty('thread_permalink')
})

test('formatMessage — bot_identity_id is never surfaced (self-echo is emitter-guaranteed)', () => {
  const out = formatMessage(messagePosted(baseMessage()), BOT_ID)
  expect(out.meta).not.toHaveProperty('bot_identity_id')
})

test('formatMessage — mentions meta lists display names ;-separated when non-empty', () => {
  const out = formatMessage(messagePosted(baseMessage({ mentions: [alice, bob] })), BOT_ID)
  expect(out.meta['mentions']).toBe('Alice;Bob')
})

test('formatMessage — mentions meta absent when list is empty', () => {
  const out = formatMessage(messagePosted(baseMessage()), BOT_ID)
  expect(out.meta).not.toHaveProperty('mentions')
})

test('formatMessage — a mention name containing the ; delimiter is sanitised, not a second field', () => {
  const evil: Identity = {
    id: decodeIdentityIdSync('user-13'),
    name: decodeDisplayNameSync('Ev;il'),
    kind: 'agent',
  }
  const out = formatMessage(messagePosted(baseMessage({ mentions: [evil, bob] })), BOT_ID)
  expect(out.meta['mentions']).toBe('Ev_il;Bob')
})

test('formatMessage — mentioned="true" when the bound bot is in the mention list', () => {
  const self: Identity = {
    id: BOT_ID,
    name: decodeDisplayNameSync('Me'),
    kind: 'agent',
  }
  const out = formatMessage(messagePosted(baseMessage({ mentions: [alice, self] })), BOT_ID)
  expect(out.meta['mentioned']).toBe('true')
})

test('formatMessage — mentioned absent when the bound bot is not in the mention list', () => {
  const out = formatMessage(messagePosted(baseMessage({ mentions: [alice, bob] })), BOT_ID)
  expect(out.meta).not.toHaveProperty('mentioned')
})

test('formatMessage — mentioned absent when there is no bound bot', () => {
  const self: Identity = {
    id: BOT_ID,
    name: decodeDisplayNameSync('Me'),
    kind: 'agent',
  }
  const out = formatMessage(messagePosted(baseMessage({ mentions: [self] })), undefined)
  expect(out.meta).not.toHaveProperty('mentioned')
})

test('formatMessage — thread meta present when message lives in a thread', () => {
  const out = formatMessage(messagePosted(baseMessage()), BOT_ID)
  expect(out.meta['thread']).toBe('payments')
})

test('formatMessage — thread meta absent when message has no thread', () => {
  const noThread = baseMessage()
  const msg: Message = {
    ref: {
      id: noThread.ref.id,
      channel: noThread.ref.channel,
      thread: Option.none(),
      permalink: MessagePermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/9-home/near/1',
      ),
    },
    sender: noThread.sender,
    body: noThread.body,
    ts: noThread.ts,
    mentions: noThread.mentions,
    reactions: noThread.reactions,
  }
  const out = formatMessage(messagePosted(msg), BOT_ID)
  expect(out.meta).not.toHaveProperty('thread')
})

test('formatMessage — sender name with control chars is sanitised in meta', () => {
  const evil: Identity = {
    id: decodeIdentityIdSync('user-99'),
    name: decodeDisplayNameSync('Evil[];\r\nName'),
    kind: 'human',
  }
  const out = formatMessage(messagePosted(baseMessage({ sender: evil })), BOT_ID)
  expect(out.meta['sender_name']).toBe('Evil_____Name')
})

test('formatMessage — body is NOT mutated by the formatter (host owns content escaping)', () => {
  const body = '<script>alert("xss")&go</script>'
  const out = formatMessage(
    messagePosted(baseMessage({ body: decodeMessageBodySync(body) })),
    BOT_ID,
  )
  expect(out.content).toBe(body)
})

test('formatMessage — mention-received produces the same payload as message-posted for matching input', () => {
  const msg = baseMessage({ mentions: [alice] })
  const posted = formatMessage(messagePosted(msg), BOT_ID)
  const mentioned = formatMessage(mentionReceived(msg, [alice]), BOT_ID)
  expect(mentioned).toEqual(posted)
})

test('formatMessage — replayed=true on the event surfaces as meta.replayed="true"', () => {
  const replayed: InboundEvent & { kind: 'message-posted' } = {
    kind: 'message-posted',
    message: baseMessage(),
    replayed: true,
  }
  const out = formatMessage(replayed, BOT_ID)
  expect(out.meta['replayed']).toBe('true')
})

test('formatMessage — replayed=true also flagged on mention-received', () => {
  const replayed: InboundEvent & { kind: 'mention-received' } = {
    kind: 'mention-received',
    message: baseMessage({ mentions: [alice] }),
    mentions: [alice],
    replayed: true,
  }
  const out = formatMessage(replayed, BOT_ID)
  expect(out.meta['replayed']).toBe('true')
})

test('formatMessage — live (no replayed flag) omits the replayed meta attribute', () => {
  const out = formatMessage(messagePosted(baseMessage()), BOT_ID)
  expect(out.meta).not.toHaveProperty('replayed')
})

const REACTION_TS = decodeTimestampSync(1715450010)

const threadedRef: MessageRef = {
  id: decodeMessageIdSync('msg-1'),
  channel: {
    id: decodeChannelIdSync('chan-9'),
    name: decodeChannelNameSync('home'),
    permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-home'),
  },
  thread: paymentsThread,
  permalink: MessagePermalinkSchema.make(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments/near/1',
  ),
}

const rootRef: MessageRef = {
  id: decodeMessageIdSync('msg-2'),
  channel: {
    id: decodeChannelIdSync('chan-9'),
    name: decodeChannelNameSync('home'),
    permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-home'),
  },
  thread: Option.none(),
  permalink: MessagePermalinkSchema.make('https://zulip.example.com/#narrow/channel/9-home/near/2'),
}

const reactionAdded = (
  target: MessageRef,
  emoji: string,
  by: Identity,
): InboundEvent & { kind: 'reaction-added' } => ({
  kind: 'reaction-added',
  target,
  emoji: decodeEmojiSync(emoji),
  by,
})

const reactionRemoved = (
  target: MessageRef,
  emoji: string,
  by: Identity,
): InboundEvent & { kind: 'reaction-removed' } => ({
  kind: 'reaction-removed',
  target,
  emoji: decodeEmojiSync(emoji),
  by,
})

test('formatReaction — reaction-added on a channel-root message omits target_thread', () => {
  const out = formatReaction(reactionAdded(rootRef, 'tada', sender), REACTION_TS)
  expect(out.content).toBe('[reaction add] tada')
  expect(out.meta['reaction_action']).toBe('add')
  expect(out.meta['reaction_emoji']).toBe('tada')
  expect(out.meta['target_message_id']).toBe('msg-2')
  expect(out.meta['target_channel_name']).toBe('home')
  expect(out.meta).not.toHaveProperty('target_thread')
  expect(out.meta['by_id']).toBe('user-7')
  expect(out.meta['by_name']).toBe('Carol')
  expect(out.meta['by_kind']).toBe('human')
  expect(out.meta['ts']).toBe('1715450010')
})

test('formatReaction — reaction-added on a thread message includes target_thread', () => {
  const out = formatReaction(reactionAdded(threadedRef, 'check', sender), REACTION_TS)
  expect(out.meta['target_thread']).toBe('payments')
})

test('formatReaction — reaction-removed renders reaction_action="remove"', () => {
  const out = formatReaction(reactionRemoved(threadedRef, 'check', sender), REACTION_TS)
  expect(out.content).toBe('[reaction remove] check')
  expect(out.meta['reaction_action']).toBe('remove')
})

test('formatReaction — meta carries the target message permalink', () => {
  const out = formatReaction(reactionAdded(threadedRef, 'check', sender), REACTION_TS)
  expect(out.meta['target_permalink']).toBe(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments/near/1',
  )
})

test('formatReaction — a channel-root reaction target still surfaces its target_permalink', () => {
  // An observed reaction target always carries a message permalink, so it is
  // surfaced even for a threadless (channel-root) target — only target_thread
  // is conditional on the target having a thread.
  const out = formatReaction(reactionAdded(rootRef, 'tada', sender), REACTION_TS)
  expect(out.meta['target_permalink']).toBe(
    'https://zulip.example.com/#narrow/channel/9-home/near/2',
  )
})

test('formatReaction — emoji name with control chars is sanitised in meta but raw in content', () => {
  const out = formatReaction(reactionAdded(rootRef, 'evil[];\r\nemoji', sender), REACTION_TS)
  expect(out.meta['reaction_emoji']).toBe('evil_____emoji')
  expect(out.content).toBe('[reaction add] evil[];\r\nemoji')
})

test('formatReaction — by.kind=agent renders by_kind="agent" in meta', () => {
  const bot: Identity = {
    id: decodeIdentityIdSync('bot-1'),
    name: decodeDisplayNameSync('OtherBot'),
    kind: 'agent',
  }
  const out = formatReaction(reactionAdded(rootRef, 'tada', bot), REACTION_TS)
  expect(out.meta['by_kind']).toBe('agent')
})

test('formatError — content is the error message, meta carries error_kind', () => {
  const out = formatError('event-pump', 'queue expired')
  expect(out.content).toBe('queue expired')
  expect(out.meta['error_kind']).toBe('event-pump')
})

test('formatError — control chars in error_kind are sanitised in meta', () => {
  const out = formatError('bad[kind]', 'oops')
  expect(out.meta['error_kind']).toBe('bad_kind_')
})

test('formatError — empty kind is treated as "unknown"', () => {
  const out = formatError('', 'something happened')
  expect(out.meta['error_kind']).toBe('unknown')
})

test('formatError — content is not sanitised (host owns content escaping)', () => {
  const out = formatError('event-pump', 'queue expired with </channel> in it')
  expect(out.content).toBe('queue expired with </channel> in it')
})
