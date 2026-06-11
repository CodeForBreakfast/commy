import { describe, expect, test } from 'bun:test'
import type {
  ChannelRef,
  HistoryReader,
  Identity,
  Message,
  Range,
  ThreadName,
} from '@codeforbreakfast/core/ports'
import {
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeTimestampSync,
} from '@codeforbreakfast/core/ports'
import { Effect, TestClock, TestContext } from 'effect'
import { type ChannelsCatchUpDeps, catchUpChannels } from './channels-catch-up.ts'

/** Fixed boot "now" the catch-up reads from the TestClock: 5000 epoch seconds. */
const NOW_SECONDS = 5000

import type { ChannelEventPayload } from './events.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'

const botIdentity: Identity = {
  id: decodeIdentityIdSync('bot-1'),
  name: decodeDisplayNameSync('assistant-concierge'),
  kind: 'agent',
}

const human: Identity = {
  id: decodeIdentityIdSync('user-42'),
  name: decodeDisplayNameSync('mhairi'),
  kind: 'human',
}

const channelRef = (name: string): ChannelRef => ({
  id: decodeChannelIdSync(name),
  name: decodeChannelNameSync(name),
})

const buildMessage = (
  ts: number,
  body: string,
  opts: {
    readonly channel?: string
    readonly thread?: string
    readonly mentions?: ReadonlyArray<Identity>
  } = {},
): Message => ({
  ref: {
    id: decodeMessageIdSync(`msg-${body}-${ts}`),
    channel: channelRef(opts.channel ?? 'general'),
    ...(opts.thread !== undefined ? { thread: { name: opts.thread as ThreadName } } : {}),
  },
  sender: human,
  body: decodeMessageBodySync(body),
  ts: decodeTimestampSync(ts),
  mentions: opts.mentions ?? [],
  reactions: [],
})

const buildHistorySpy = (
  byChannel: Record<string, ReadonlyArray<Message>> = {},
  byThread: Record<string, ReadonlyArray<Message>> = {},
): {
  readonly history: HistoryReader
  readonly channelCalls: Array<{ channel: string; range: Range }>
  readonly threadCalls: Array<{ channel: string; thread: string; range: Range | undefined }>
} => {
  const channelCalls: Array<{ channel: string; range: Range }> = []
  const threadCalls: Array<{ channel: string; thread: string; range: Range | undefined }> = []
  return {
    channelCalls,
    threadCalls,
    history: {
      readChannel: (channel, range) =>
        Effect.sync(() => {
          channelCalls.push({ channel: channel.name, range })
          return byChannel[channel.name] ?? []
        }),
      readThread: (channel, threadName, range) =>
        Effect.sync(() => {
          threadCalls.push({ channel: channel.name, thread: threadName, range })
          return byThread[`${channel.name}/${threadName}`] ?? []
        }),
      recentThreads: () => Effect.succeed([]),
    },
  }
}

const buildNotifierSpy = (): {
  readonly notifier: (payload: ChannelEventPayload) => Promise<void>
  readonly payloads: ChannelEventPayload[]
} => {
  const payloads: ChannelEventPayload[] = []
  return {
    payloads,
    notifier: async (payload) => {
      payloads.push(payload)
    },
  }
}

const runCatchUp = (deps: ChannelsCatchUpDeps): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW_SECONDS * 1000)
      yield* catchUpChannels(deps)
    }).pipe(Effect.provide(TestContext.TestContext)),
  )

describe('catchUpChannels', () => {
  test('empty intents → no history calls, no notifications', async () => {
    const history = buildHistorySpy()
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 4 * 3600,
    })
    expect(history.channelCalls).toEqual([])
    expect(history.threadCalls).toEqual([])
    expect(notifier.payloads).toEqual([])
  })

  test('channel intent → readChannel with since=now-window, dispatches each message', async () => {
    const history = buildHistorySpy({
      general: [buildMessage(4800, 'first'), buildMessage(4900, 'second')],
    })
    const notifier = buildNotifierSpy()
    const intents: ReadonlyArray<SubscribeIntent> = [
      { kind: 'channel', channelName: decodeChannelNameSync('general') },
    ]
    await runCatchUp({
      intents,
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 200,
    })
    expect(history.channelCalls).toEqual([
      { channel: 'general', range: { since: decodeTimestampSync(4800) } },
    ])
    expect(notifier.payloads.map((p) => p.content)).toEqual(['first', 'second'])
  })

  test('thread intent → readThread with since=now-window', async () => {
    const history = buildHistorySpy(
      {},
      {
        'home/payments': [buildMessage(4900, 'in-thread', { channel: 'home', thread: 'payments' })],
      },
    )
    const notifier = buildNotifierSpy()
    const intents: ReadonlyArray<SubscribeIntent> = [
      {
        kind: 'thread',
        channelName: decodeChannelNameSync('home'),
        threadName: 'payments' as ThreadName,
      },
    ]
    await runCatchUp({
      intents,
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 200,
    })
    expect(history.threadCalls).toEqual([
      { channel: 'home', thread: 'payments', range: { since: decodeTimestampSync(4800) } },
    ])
    expect(notifier.payloads.length).toBe(1)
    expect(notifier.payloads[0]?.content).toBe('in-thread')
  })

  test('mentions intent → skipped (deferred to comms-rxo), no calls or dispatches', async () => {
    const history = buildHistorySpy()
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [{ kind: 'mentions' }],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 200,
    })
    expect(history.channelCalls).toEqual([])
    expect(history.threadCalls).toEqual([])
    expect(notifier.payloads).toEqual([])
  })

  test('mixed intents: dispatch in chronological order across narrows', async () => {
    const history = buildHistorySpy(
      {
        general: [
          buildMessage(4910, 'channel-late', { channel: 'general' }),
          buildMessage(4810, 'channel-early', { channel: 'general' }),
        ],
      },
      {
        'home/payments': [
          buildMessage(4870, 'thread-mid', { channel: 'home', thread: 'payments' }),
        ],
      },
    )
    const notifier = buildNotifierSpy()
    const intents: ReadonlyArray<SubscribeIntent> = [
      { kind: 'channel', channelName: decodeChannelNameSync('general') },
      {
        kind: 'thread',
        channelName: decodeChannelNameSync('home'),
        threadName: 'payments' as ThreadName,
      },
      { kind: 'mentions' },
    ]
    await runCatchUp({
      intents,
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 200,
    })
    expect(notifier.payloads.map((p) => p.content)).toEqual([
      'channel-early',
      'thread-mid',
      'channel-late',
    ])
  })

  test('dispatched payload meta carries mentioned="true" when the bound bot is mentioned', async () => {
    const history = buildHistorySpy({
      general: [buildMessage(4900, 'hi', { mentions: [botIdentity] })],
    })
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [{ kind: 'channel', channelName: decodeChannelNameSync('general') }],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 200,
    })
    expect(notifier.payloads[0]?.meta['mentioned']).toBe('true')
  })

  test('botIdentityId undefined → payload omits mentioned meta but still dispatches', async () => {
    const history = buildHistorySpy({
      general: [buildMessage(4900, 'hi', { mentions: [botIdentity] })],
    })
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [{ kind: 'channel', channelName: decodeChannelNameSync('general') }],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: undefined,
      windowSeconds: 200,
    })
    expect(notifier.payloads.length).toBe(1)
    expect(notifier.payloads[0]?.meta).not.toHaveProperty('mentioned')
  })

  test('new-topics intent → readChannel with default window when no override given', async () => {
    const history = buildHistorySpy({
      busy: [
        buildMessage(4820, 'topic-a-first', { channel: 'busy', thread: 'topic-a' }),
        buildMessage(4830, 'topic-b-first', { channel: 'busy', thread: 'topic-b' }),
      ],
    })
    const notifier = buildNotifierSpy()
    const intents: ReadonlyArray<SubscribeIntent> = [
      { kind: 'new-topics-in-channel', channelName: decodeChannelNameSync('busy') },
    ]
    await runCatchUp({
      intents,
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 400,
    })
    expect(history.channelCalls).toEqual([
      { channel: 'busy', range: { since: decodeTimestampSync(4600) } },
    ])
    expect(notifier.payloads.map((p) => p.content)).toEqual(['topic-a-first', 'topic-b-first'])
  })

  test('new-topics intent → uses newTopicsWindowSeconds override when provided', async () => {
    const history = buildHistorySpy({
      busy: [buildMessage(4960, 'recent-topic', { channel: 'busy', thread: 'fresh' })],
    })
    const notifier = buildNotifierSpy()
    const intents: ReadonlyArray<SubscribeIntent> = [
      { kind: 'new-topics-in-channel', channelName: decodeChannelNameSync('busy') },
    ]
    await runCatchUp({
      intents,
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 400,
      newTopicsWindowSeconds: 50,
    })
    expect(history.channelCalls).toEqual([
      { channel: 'busy', range: { since: decodeTimestampSync(4950) } },
    ])
    expect(notifier.payloads.map((p) => p.content)).toEqual(['recent-topic'])
  })

  test('new-topics intent → dedups to first message per topic within the window', async () => {
    const history = buildHistorySpy({
      busy: [
        buildMessage(4810, 'topic-a-first', { channel: 'busy', thread: 'topic-a' }),
        buildMessage(4820, 'topic-a-followup', { channel: 'busy', thread: 'topic-a' }),
        buildMessage(4830, 'topic-b-first', { channel: 'busy', thread: 'topic-b' }),
        buildMessage(4840, 'topic-b-followup', { channel: 'busy', thread: 'topic-b' }),
      ],
    })
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [{ kind: 'new-topics-in-channel', channelName: decodeChannelNameSync('busy') }],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 400,
    })
    expect(notifier.payloads.map((p) => p.content)).toEqual(['topic-a-first', 'topic-b-first'])
  })

  test('new-topics intent → messages without a thread are skipped', async () => {
    const history = buildHistorySpy({
      busy: [
        buildMessage(4810, 'no-topic', { channel: 'busy' }),
        buildMessage(4820, 'with-topic', { channel: 'busy', thread: 'fresh' }),
      ],
    })
    const notifier = buildNotifierSpy()
    await runCatchUp({
      intents: [{ kind: 'new-topics-in-channel', channelName: decodeChannelNameSync('busy') }],
      history: history.history,
      notifier: notifier.notifier,
      botIdentityId: botIdentity.id,
      windowSeconds: 400,
    })
    expect(notifier.payloads.map((p) => p.content)).toEqual(['with-topic'])
  })
})
