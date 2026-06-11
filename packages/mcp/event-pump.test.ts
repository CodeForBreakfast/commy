import { expect, test } from 'bun:test'
import { captureLogger } from '@codeforbreakfast/core/logging'
import type {
  Identity,
  IdentityId as IdentityIdType,
  InboundEvent,
  Message,
  MessageInbox,
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
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Effect, Ref, Schema, Stream, TestClock, TestContext } from 'effect'
import { channelNotifier, startEventPump } from './event-pump.ts'
import type { ChannelEventPayload } from './events.ts'
import { buildMcpServer } from './mcp-server.ts'

const BOT_ID: IdentityIdType = decodeIdentityIdSync('bot-42')

const sender: Identity = {
  id: decodeIdentityIdSync('user-7'),
  name: decodeDisplayNameSync('Graeme'),
  kind: 'human',
}

const msg = (overrides: Partial<Message> = {}): Message => ({
  ref: {
    id: decodeMessageIdSync('msg-1'),
    channel: { id: decodeChannelIdSync('chan-9'), name: decodeChannelNameSync('home') },
    thread: { name: decodeThreadNameSync('payments') },
  },
  sender,
  body: decodeMessageBodySync('hello'),
  ts: decodeTimestampSync(1715450000),
  mentions: [],
  reactions: [],
  ...overrides,
})

const ref: MessageRef = {
  id: decodeMessageIdSync('msg-1'),
  channel: { id: decodeChannelIdSync('chan-9'), name: decodeChannelNameSync('home') },
}

interface QueueInboxOptions {
  readonly events: ReadonlyArray<InboundEvent>
  readonly closeAfterDrain?: boolean
}

interface QueueInboxHandle {
  readonly inbox: MessageInbox
  push(event: InboundEvent): void
  end(): void
}

const queueInbox = (options: QueueInboxOptions = { events: [] }): QueueInboxHandle => {
  const queue: InboundEvent[] = [...options.events]
  let activeEmit: ((event: InboundEvent) => void) | undefined
  let activeEnd: (() => void) | undefined
  let ended = options.closeAfterDrain === true

  const inbox: MessageInbox = {
    subscribe: () => Effect.void,
    unsubscribe: () => Effect.void,
    events: () =>
      Stream.async<InboundEvent>((emit) => {
        for (const ev of queue) void emit.single(ev)
        queue.length = 0
        if (ended) {
          void emit.end()
          return Effect.void
        }
        const onEvent = (event: InboundEvent): void => {
          void emit.single(event)
        }
        const onEnd = (): void => {
          void emit.end()
        }
        activeEmit = onEvent
        activeEnd = onEnd
        return Effect.sync(() => {
          if (activeEmit === onEvent) activeEmit = undefined
          if (activeEnd === onEnd) activeEnd = undefined
        })
      }),
    replay: () => Effect.succeed([]),
  }

  return {
    inbox,
    push: (event) => {
      if (activeEmit !== undefined) activeEmit(event)
      else queue.push(event)
    },
    end: () => {
      ended = true
      if (activeEnd !== undefined) activeEnd()
    },
  }
}

const collectingNotifier = (): {
  readonly notifier: (payload: ChannelEventPayload) => Promise<void>
  readonly calls: ChannelEventPayload[]
} => {
  const calls: ChannelEventPayload[] = []
  return {
    calls,
    notifier: async (payload) => {
      calls.push(payload)
    },
  }
}

/**
 * Run a pump test against a deterministic clock. `TestContext` installs
 * Effect's `TestClock`; `TestClock.setTime` pins `Clock.currentTimeMillis`
 * to `seconds * 1000` so the reaction timestamps the pump stamps from the
 * clock land on a fixed value — replacing the old `now: () => seconds`
 * injection seam. Used only by tests that render a reaction (the sole
 * clock-reading path); message/cancel tests stay on the live clock since
 * they assert no timestamp or rely on real `Effect.sleep`.
 */
const runWithClockSeconds = <A, E>(seconds: number, effect: Effect.Effect<A, E>): Promise<A> =>
  Effect.runPromise(
    TestClock.setTime(seconds * 1000).pipe(
      Effect.zipRight(effect),
      Effect.provide(TestContext.TestContext),
    ),
  )

test('pump filters out events that fail the narrow predicate', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const events: InboundEvent[] = [
        { kind: 'message-posted', message: msg({ body: decodeMessageBodySync('kept') }) },
        {
          kind: 'message-posted',
          message: msg({
            body: decodeMessageBodySync('dropped'),
            ref: {
              id: decodeMessageIdSync('msg-2'),
              channel: {
                id: decodeChannelIdSync('chan-other'),
                name: decodeChannelNameSync('other'),
              },
            },
          }),
        },
      ]
      const inbox = queueInbox({ events, closeAfterDrain: true })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        match: (event) =>
          event.kind === 'message-posted' && event.message.ref.channel.name === 'home',
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('kept')
    }),
  ))

test('pump drops message-posted whose sender is the bound bot identity (self-echo)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const selfSender: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const events: InboundEvent[] = [
        {
          kind: 'message-posted',
          message: msg({ body: decodeMessageBodySync('self'), sender: selfSender }),
        },
        { kind: 'message-posted', message: msg({ body: decodeMessageBodySync('other') }) },
      ]
      const inbox = queueInbox({ events, closeAfterDrain: true })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('other')
    }),
  ))

test('pump drops mention-received whose message sender is the bound bot identity', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const selfSender: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const alice: Identity = {
        id: decodeIdentityIdSync('user-11'),
        name: decodeDisplayNameSync('Alice'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ sender: selfSender, mentions: [alice] }),
            mentions: [alice],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(0)
    }),
  ))

test('pump drops reaction-added whose actor is the bound bot identity', () =>
  runWithClockSeconds(
    1715450010,
    Effect.gen(function* () {
      const selfActor: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('tada'), by: selfActor },
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('wave'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('[reaction add] wave')
    }),
  ))

test('pump drops reaction-removed whose actor is the bound bot identity', () =>
  runWithClockSeconds(
    1715450020,
    Effect.gen(function* () {
      const selfActor: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-removed', target: ref, emoji: decodeEmojiSync('tada'), by: selfActor },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(0)
    }),
  ))

test('pump delivers self-shaped events when bot identity is unknown (pre-acquire)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const selfSender: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          {
            kind: 'message-posted',
            message: msg({ body: decodeMessageBodySync('pre-acquire'), sender: selfSender }),
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => undefined,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('pre-acquire')
    }),
  ))

test('pump sets mentioned="true" when the bound bot is in the mention list', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg({ mentions: [bot] }) }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.meta['mentioned']).toBe('true')
    }),
  ))

test('pump omits mentioned (and never surfaces bot_identity_id) when getBotIdentityId returns undefined', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg({ mentions: [bot] }) }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => undefined,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.meta).not.toHaveProperty('mentioned')
      expect(collector.calls[0]?.meta).not.toHaveProperty('bot_identity_id')
    }),
  ))

test('pump dispatches message-posted as a formatMessage payload', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg() }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('hello')
      expect(collector.calls[0]?.meta['message_id']).toBe('msg-1')
      expect(collector.calls[0]?.meta['sender_name']).toBe('Graeme')
    }),
  ))

test('pump dispatches mention-received with mentions meta populated', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const alice: Identity = {
        id: decodeIdentityIdSync('user-11'),
        name: decodeDisplayNameSync('Alice'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ mentions: [alice] }),
            mentions: [alice],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.meta['mentions']).toBe('Alice')
    }),
  ))

test('pump dispatches reaction-added as a formatReaction payload with clock timestamp', () =>
  runWithClockSeconds(
    1715450010,
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('[reaction add] tada')
      expect(collector.calls[0]?.meta['ts']).toBe('1715450010')
      expect(collector.calls[0]?.meta['reaction_action']).toBe('add')
    }),
  ))

test('pump dispatches reaction-removed as a formatReaction payload', () =>
  runWithClockSeconds(
    1715450020,
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-removed', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('[reaction remove] tada')
      expect(collector.calls[0]?.meta['reaction_action']).toBe('remove')
    }),
  ))

test('pump calls rememberIdentity with the sender of a message-posted event', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg() }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
      })
      yield* handle.done
      expect(remembered).toEqual([sender])
    }),
  ))

test('pump calls rememberIdentity with sender + mentions of a mention-received event', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const alice: Identity = {
        id: decodeIdentityIdSync('user-11'),
        name: decodeDisplayNameSync('Alice'),
        kind: 'agent',
      }
      const bob: Identity = {
        id: decodeIdentityIdSync('user-12'),
        name: decodeDisplayNameSync('Bob'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ mentions: [alice, bob] }),
            mentions: [alice],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
      })
      yield* handle.done
      const rememberedIds = new Set(remembered.map((i) => i.id))
      expect(rememberedIds).toEqual(new Set([sender.id, alice.id, bob.id]))
    }),
  ))

test('pump calls rememberIdentity with the actor of a reaction-added event', () =>
  runWithClockSeconds(
    1715450010,
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
      })
      yield* handle.done
      expect(remembered).toEqual([sender])
    }),
  ))

test('pump calls rememberIdentity with the actor of a reaction-removed event', () =>
  runWithClockSeconds(
    1715450020,
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'reaction-removed', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
      })
      yield* handle.done
      expect(remembered).toEqual([sender])
    }),
  ))

test('pump does NOT call rememberIdentity for events filtered out by the narrow predicate', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          {
            kind: 'message-posted',
            message: msg({
              ref: {
                id: decodeMessageIdSync('msg-x'),
                channel: {
                  id: decodeChannelIdSync('chan-other'),
                  name: decodeChannelNameSync('other'),
                },
              },
            }),
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
        match: (event) =>
          event.kind === 'message-posted' && event.message.ref.channel.name === 'home',
      })
      yield* handle.done
      expect(remembered).toEqual([])
    }),
  ))

test('pump does NOT call rememberIdentity for self-echo events', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const selfSender: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg({ sender: selfSender }) }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const remembered: Identity[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        rememberIdentity: (identity) => {
          remembered.push(identity)
        },
      })
      yield* handle.done
      expect(remembered).toEqual([])
    }),
  ))

test('pump works without a rememberIdentity dep (optional)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [{ kind: 'message-posted', message: msg() }],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
    }),
  ))

test('pump calls onMention with the ts of each delivered mention-received event', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const alice: Identity = {
        id: decodeIdentityIdSync('user-11'),
        name: decodeDisplayNameSync('Alice'),
        kind: 'agent',
      }
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ ts: decodeTimestampSync(1715450100), sender: alice, mentions: [bot] }),
            mentions: [bot],
          },
          {
            kind: 'mention-received',
            message: msg({
              ts: decodeTimestampSync(1715450200),
              sender: alice,
              mentions: [bot],
              ref: {
                id: decodeMessageIdSync('msg-2'),
                channel: { id: decodeChannelIdSync('chan-9'), name: decodeChannelNameSync('home') },
              },
            }),
            mentions: [bot],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const mentionTs: number[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        onMention: (ts) =>
          Effect.sync(() => {
            mentionTs.push(ts)
          }),
      })
      yield* handle.done
      expect(mentionTs).toEqual([1715450100, 1715450200])
    }),
  ))

test('pump does NOT call onMention for non-mention events', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'message-posted', message: msg() },
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const mentionTs: number[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        onMention: (ts) =>
          Effect.sync(() => {
            mentionTs.push(ts)
          }),
      })
      yield* handle.done
      expect(mentionTs).toEqual([])
    }),
  ))

test('pump does NOT call onMention for events filtered out by the narrow predicate', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const alice: Identity = {
        id: decodeIdentityIdSync('user-11'),
        name: decodeDisplayNameSync('Alice'),
        kind: 'agent',
      }
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ sender: alice, mentions: [bot] }),
            mentions: [bot],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const mentionTs: number[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        match: () => false,
        onMention: (ts) =>
          Effect.sync(() => {
            mentionTs.push(ts)
          }),
      })
      yield* handle.done
      expect(mentionTs).toEqual([])
    }),
  ))

test('pump does NOT call onMention for self-echo mentions', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // Bot mentioning itself shouldn't advance the cursor — already filtered
      // by the self-echo guard, and onMention should respect that.
      const selfSender: Identity = {
        id: BOT_ID,
        name: decodeDisplayNameSync('cc-bot'),
        kind: 'agent',
      }
      const someone: Identity = {
        id: decodeIdentityIdSync('user-99'),
        name: decodeDisplayNameSync('someone'),
        kind: 'human',
      }
      const inbox = queueInbox({
        events: [
          {
            kind: 'mention-received',
            message: msg({ sender: selfSender, mentions: [someone] }),
            mentions: [someone],
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const mentionTs: number[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        onMention: (ts) =>
          Effect.sync(() => {
            mentionTs.push(ts)
          }),
      })
      yield* handle.done
      expect(mentionTs).toEqual([])
    }),
  ))

test('pump dispatches a single notifier call when message-posted and mention-received share the same message.ref.id (comms-oyy)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const portMessage = msg({ mentions: [bot] })
      const inbox = queueInbox({
        events: [
          { kind: 'message-posted', message: portMessage },
          { kind: 'mention-received', message: portMessage, mentions: [bot] },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.meta['message_id']).toBe('msg-1')
    }),
  ))

test('pump still calls onMention when mention-received is the deduped duplicate (comms-oyy)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      // The notifier-side dedup must not swallow the mention-cursor advance:
      // even though the consumer sees one block, the bot's seen-up-to mark
      // for mentions must still move forward off the mention-received event.
      const bot: Identity = { id: BOT_ID, name: decodeDisplayNameSync('cc-bot'), kind: 'agent' }
      const portMessage = msg({ ts: decodeTimestampSync(1715450500), mentions: [bot] })
      const inbox = queueInbox({
        events: [
          { kind: 'message-posted', message: portMessage },
          { kind: 'mention-received', message: portMessage, mentions: [bot] },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const mentionTs: number[] = []
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
        onMention: (ts) =>
          Effect.sync(() => {
            mentionTs.push(ts)
          }),
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(1)
      expect(mentionTs).toEqual([1715450500])
    }),
  ))

test('pump dedup is keyed per message id — distinct ids each fire (comms-oyy)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({
        events: [
          { kind: 'message-posted', message: msg({ body: decodeMessageBodySync('first') }) },
          {
            kind: 'message-posted',
            message: msg({
              body: decodeMessageBodySync('second'),
              ref: {
                id: decodeMessageIdSync('msg-2'),
                channel: { id: decodeChannelIdSync('chan-9'), name: decodeChannelNameSync('home') },
                thread: { name: decodeThreadNameSync('payments') },
              },
            }),
          },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(2)
      expect(collector.calls[0]?.content).toBe('first')
      expect(collector.calls[1]?.content).toBe('second')
    }),
  ))

test('pump does not dedup a reaction whose target.id matches a previously delivered message (comms-oyy)', () =>
  runWithClockSeconds(
    1715450010,
    Effect.gen(function* () {
      // Reactions are a distinct event kind — the consumer wants both the
      // message block and any subsequent reaction blocks, even when the
      // reaction targets a message id we already delivered.
      const inbox = queueInbox({
        events: [
          { kind: 'message-posted', message: msg() },
          { kind: 'reaction-added', target: ref, emoji: decodeEmojiSync('tada'), by: sender },
        ],
        closeAfterDrain: true,
      })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toHaveLength(2)
      expect(collector.calls[0]?.content).toBe('hello')
      expect(collector.calls[1]?.content).toBe('[reaction add] tada')
    }),
  ))

test('pump done resolves when iterator naturally ends', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox({ events: [], closeAfterDrain: true })
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })
      yield* handle.done
      expect(collector.calls).toEqual([])
    }),
  ))

test('pump exits and produces no further notifications after cancel', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const inbox = queueInbox()
      const collector = collectingNotifier()
      const handle = yield* startEventPump({
        inbox: inbox.inbox,
        notifier: collector.notifier,
        getBotIdentityId: () => BOT_ID,
      })

      inbox.push({ kind: 'message-posted', message: msg({ body: decodeMessageBodySync('first') }) })
      yield* Effect.sleep('5 millis')
      expect(collector.calls).toHaveLength(1)

      yield* handle.cancel
      yield* handle.done

      inbox.push({
        kind: 'message-posted',
        message: msg({ body: decodeMessageBodySync('after-cancel') }),
      })
      yield* Effect.sleep('5 millis')
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('first')
    }),
  ))

// The comms-ynb invariants (auto-reconnect on transient substrate
// failure, `transient error (attempt N)` + `reconnected after N
// transient error(s)` breadcrumbs, default backoff schedule, clean
// cancel-during-backoff unwind, non-Error rejection stringification)
// now live inside the producer (`adapters/zulip/events.ts`), so the
// matching tests run against the Zulip Stream-shaped producer in
// `adapters/zulip/events.test.ts` rather than via the pump's iterator
// shim. With the pump's `Stream<InboundEvent, never, never>` consumer
// shape, no error surfaces here to retry against — substrate hiccups
// are absorbed at the producer with `Schedule.forever.addDelay` and
// the consumer just runs the stream to completion.

test('pump still parks on dispatch failure to preserve comms-ian for non-iterator errors', () => {
  const logLines: string[] = []
  return Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // The fatal-park behaviour is the right answer when the failure is
        // downstream of iterator.next() — a notifier throw means MCP transport
        // is broken or the host rejected the params, neither of which a fresh
        // iterator will fix. Logging + a sticky error block + not resolving
        // done (until cancel) keeps the process alive so other MCP tools still
        // work, matching the original comms-ian rationale.
        const inbox = queueInbox({
          events: [
            {
              kind: 'message-posted',
              message: msg({ body: decodeMessageBodySync('triggers-notifier') }),
            },
          ],
        })
        const calls: ChannelEventPayload[] = []
        const failingNotifier = async (payload: ChannelEventPayload): Promise<void> => {
          calls.push(payload)
          if (
            payload.meta['error_kind'] !== 'event-pump' &&
            payload.meta['notice_kind'] === undefined
          ) {
            throw new Error('notifier dead')
          }
        }
        const handle = yield* startEventPump({
          inbox: inbox.inbox,
          notifier: failingNotifier,
          getBotIdentityId: () => BOT_ID,
        })

        yield* Effect.sleep('10 millis')

        // The pump fiber forked inside startEventPump inherits the capture
        // logger from the provided layer, so its fatal-park diagnostic lands
        // in logLines.
        expect(logLines).toEqual(['commy plugin: event-pump error: notifier dead'])
        expect(calls).toHaveLength(2)
        expect(calls[1]?.meta['error_kind']).toBe('event-pump')

        const resolvedEarly = yield* Ref.make(false)
        yield* handle.done.pipe(
          Effect.tap(() => Ref.set(resolvedEarly, true)),
          Effect.forkScoped,
        )
        yield* Effect.sleep('10 millis')
        expect(yield* Ref.get(resolvedEarly)).toBe(false)

        yield* handle.cancel
        yield* handle.done
      }),
    ).pipe(Effect.provide(captureLogger(logLines))),
  )
})

test('channelNotifier dual-emits the claude/channel frame and the MCP-standard notifications/message carrier', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const server = buildMcpServer()
        const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
        const client = new Client(
          { name: 'commy-test-client', version: '0.0.0' },
          { capabilities: {} },
        )

        const received: Array<{ method: string; params: unknown }> = []
        client.fallbackNotificationHandler = async (notification) => {
          received.push({ method: notification.method, params: notification.params ?? null })
        }

        yield* Effect.promise(() =>
          Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
        )
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            await client.close()
            await server.close()
          }),
        )

        const notifier = channelNotifier(server)
        yield* Effect.promise(() =>
          notifier({
            content: 'hello',
            meta: {
              channel_name: 'home',
              message_id: 'msg-1',
              sender_id: 'user-7',
              by_id: 'user-9',
            },
          }),
        )

        yield* Effect.sleep('5 millis')

        const frameSchema = Schema.Struct({
          content: Schema.String,
          meta: Schema.Record({ key: Schema.String, value: Schema.String }),
        })

        expect(received).toHaveLength(2)

        const claudeChannel = received.find((n) => n.method === 'notifications/claude/channel')
        expect(claudeChannel).toBeDefined()
        const claudeFrame = yield* Schema.decodeUnknown(frameSchema)(claudeChannel?.params)
        expect(claudeFrame.content).toBe('hello')
        expect(claudeFrame.meta['channel_name']).toBe('home')
        expect(claudeFrame.meta['message_id']).toBe('msg-1')
        // The agent-display carrier omits the numeric identity ids.
        expect(claudeFrame.meta).not.toHaveProperty('sender_id')
        expect(claudeFrame.meta).not.toHaveProperty('by_id')

        const standard = received.find((n) => n.method === 'notifications/message')
        expect(standard).toBeDefined()
        const standardParams = yield* Schema.decodeUnknown(
          Schema.Struct({
            level: Schema.Literal('info'),
            logger: Schema.Literal('commy'),
            data: frameSchema,
          }),
        )(standard?.params)
        expect(standardParams.data.content).toBe('hello')
        expect(standardParams.data.meta['channel_name']).toBe('home')
        expect(standardParams.data.meta['message_id']).toBe('msg-1')
        // The machine carrier retains the numeric identity ids consumers key on.
        expect(standardParams.data.meta['sender_id']).toBe('user-7')
        expect(standardParams.data.meta['by_id']).toBe('user-9')
      }),
    ),
  ))
