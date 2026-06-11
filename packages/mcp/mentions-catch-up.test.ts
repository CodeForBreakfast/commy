import { describe, expect, test } from 'bun:test'
import type { Identity, InboundEvent, MessageInbox } from '@commy/core/ports'
import {
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeTimestampSync,
  type Timestamp,
} from '@commy/core/ports'
import { Effect, Option, TestClock, TestContext } from 'effect'
import type { CursorStore } from './cursor-store.ts'
import type { ChannelEventPayload } from './events.ts'
import { catchUpMentions, type MentionsCatchUpDeps } from './mentions-catch-up.ts'

/** Fixed boot "now" the catch-up reads from the TestClock: 5000 epoch seconds. */
const NOW_SECONDS = 5000

const runCatchUp = (deps: MentionsCatchUpDeps): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* TestClock.setTime(NOW_SECONDS * 1000)
      yield* catchUpMentions(deps)
    }).pipe(Effect.provide(TestContext.TestContext)),
  )

const botIdentity: Identity = {
  id: decodeIdentityIdSync('bot-1'),
  name: decodeDisplayNameSync('assistant-concierge'),
  kind: 'agent',
}

const otherSender: Identity = {
  id: decodeIdentityIdSync('user-42'),
  name: decodeDisplayNameSync('mhairi'),
  kind: 'human',
}

const channelRef = {
  id: decodeChannelIdSync('chan-1'),
  name: decodeChannelNameSync('general'),
}

const buildMention = (ts: number, body: string): InboundEvent => ({
  kind: 'mention-received',
  message: {
    ref: {
      id: decodeMessageIdSync(`msg-${ts}`),
      channel: channelRef,
    },
    sender: otherSender,
    body: decodeMessageBodySync(body),
    ts: decodeTimestampSync(ts),
    mentions: [botIdentity],
    reactions: [],
  },
  mentions: [botIdentity],
})

const buildMessagePosted = (ts: number, body: string): InboundEvent => ({
  kind: 'message-posted',
  message: {
    ref: {
      id: decodeMessageIdSync(`msg-${ts}`),
      channel: channelRef,
    },
    sender: otherSender,
    body: decodeMessageBodySync(body),
    ts: decodeTimestampSync(ts),
    mentions: [],
    reactions: [],
  },
})

const buildInboxSpy = (
  events: ReadonlyArray<InboundEvent>,
): {
  readonly inbox: Pick<MessageInbox, 'replay'>
  readonly replayCalls: number[]
} => {
  const replayCalls: number[] = []
  return {
    replayCalls,
    inbox: {
      replay: (since) =>
        Effect.sync(() => {
          replayCalls.push(since)
          return events.filter((e) => {
            const ts = 'message' in e ? e.message.ts : decodeTimestampSync(0)
            return ts >= since
          })
        }),
    },
  }
}

const buildCursorSpy = (
  initial?: number,
): {
  readonly store: CursorStore
  readonly writes: number[]
  readonly get: () => number | undefined
} => {
  let value = initial
  const writes: number[] = []
  return {
    writes,
    get: () => value,
    store: {
      read: () =>
        Effect.sync(() =>
          value === undefined ? Option.none<Timestamp>() : Option.some(decodeTimestampSync(value)),
        ),
      write: (_id, ts) =>
        Effect.sync(() => {
          writes.push(ts)
          if (value === undefined || value < ts) value = ts
        }),
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

describe('catchUpMentions', () => {
  test('fresh acquire (no cursor): does not call replay, initialises cursor to now', async () => {
    const inbox = buildInboxSpy([buildMention(1000, 'hello')])
    const cursor = buildCursorSpy()
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(inbox.replayCalls).toEqual([])
    expect(notifier.payloads.length).toBe(0)
    expect(cursor.get()).toBe(5000)
  })

  test('resume (cursor present): replays from cursor, dispatches only mention-received', async () => {
    const inbox = buildInboxSpy([
      buildMention(1100, 'first'),
      buildMessagePosted(1200, 'noise'),
      buildMention(1300, 'second'),
    ])
    const cursor = buildCursorSpy(1000)
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(inbox.replayCalls).toEqual([1000])
    expect(notifier.payloads.length).toBe(2)
    expect(notifier.payloads[0]?.content).toBe('first')
    expect(notifier.payloads[1]?.content).toBe('second')
  })

  test('resume: cursor advances to now after replay', async () => {
    const inbox = buildInboxSpy([buildMention(1100, 'hello')])
    const cursor = buildCursorSpy(1000)
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(cursor.get()).toBe(5000)
  })

  test('resume with empty replay: cursor still advances to now', async () => {
    const inbox = buildInboxSpy([])
    const cursor = buildCursorSpy(1000)
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(inbox.replayCalls).toEqual([1000])
    expect(notifier.payloads.length).toBe(0)
    expect(cursor.get()).toBe(5000)
  })

  test('dispatched payload meta carries mentioned="true" (the replayed mention targets the bound bot)', async () => {
    const inbox = buildInboxSpy([buildMention(1100, 'hello')])
    const cursor = buildCursorSpy(1000)
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(notifier.payloads[0]?.meta['mentioned']).toBe('true')
  })

  test('dispatch order is chronological (oldest mention first)', async () => {
    // Inbox spy intentionally returns unsorted to prove the catch-up
    // sorts by ts before dispatch.
    const inbox = buildInboxSpy([
      buildMention(1300, 'newest'),
      buildMention(1100, 'oldest'),
      buildMention(1200, 'middle'),
    ])
    const cursor = buildCursorSpy(1000)
    const notifier = buildNotifierSpy()
    await runCatchUp({
      cursorStore: cursor.store,
      inbox: inbox.inbox,
      identityId: botIdentity.id,
      notifier: notifier.notifier,
    })
    expect(notifier.payloads.map((p) => p.content)).toEqual(['oldest', 'middle', 'newest'])
  })
})
