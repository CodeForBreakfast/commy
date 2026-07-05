import { describe, expect, test } from 'bun:test'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import type { InboundEvent, Message, MessageInbox } from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  decodeTimestampSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import { ConfigProvider, Data, Deferred, Effect, Option, Stream } from 'effect'
import { parseSessionId, type SessionId } from './bootstrap.ts'
import { startEventPump } from './event-pump.ts'
import type { ChannelEventPayload } from './events.ts'
import { createFileSubscriptionStore } from './subscription-store.ts'
import {
  advanceTargetCursor,
  type DeliveryTarget,
  deliveryTargetOf,
  targetCursorDirConfig,
} from './target-cursor.ts'

const fs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'
const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

const ts = (n: number) => decodeTimestampSync(n)
const target = (channel: string, thread?: string): DeliveryTarget =>
  Data.struct({
    channel: decodeChannelNameSync(channel),
    thread: thread === undefined ? Option.none() : Option.some(decodeThreadNameSync(thread)),
  })

const buildTmpDir = (): { readonly path: string; readonly cleanup: () => void } => {
  const path = mkdtempSync(join(tmpdir(), 'target-cursor-'))
  return { path, cleanup: () => rmSync(path, { recursive: true, force: true }) }
}

/** The persisted file: an array of `[encodedTarget, ts]` pairs (HashMap encoding). */
type PersistedEntry = readonly [{ channel: string; thread: string | null }, number]
const readEntries = (dir: string, id: string): ReadonlyArray<PersistedEntry> =>
  JSON.parse(readFileSync(join(dir, `${id}.json`), 'utf8'))

describe('advanceTargetCursor', () => {
  test('writes a per-target cursor that reads back as the target + ts', async () => {
    const tmp = buildTmpDir()
    try {
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home', 'payments'), ts(1715450000)),
      )
      expect(readEntries(tmp.path, SID_A)).toContainEqual([
        { channel: 'home', thread: 'payments' },
        1715450000,
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('a top-level (thread-less) target persists with a null thread', async () => {
    const tmp = buildTmpDir()
    try {
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home'), ts(1715450000)),
      )
      expect(readEntries(tmp.path, SID_A)).toContainEqual([
        { channel: 'home', thread: null },
        1715450000,
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('is monotonic — an earlier ts for the same target is a no-op', async () => {
    const tmp = buildTmpDir()
    try {
      const t = target('home', 'payments')
      await Effect.runPromise(advanceTargetCursor(fs, tmp.path, sid(SID_A), t, ts(1715450200)))
      await Effect.runPromise(advanceTargetCursor(fs, tmp.path, sid(SID_A), t, ts(1715450100)))
      expect(readEntries(tmp.path, SID_A)).toEqual([
        [{ channel: 'home', thread: 'payments' }, 1715450200],
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('advances forward on a newer ts for the same target', async () => {
    const tmp = buildTmpDir()
    try {
      const t = target('home', 'payments')
      await Effect.runPromise(advanceTargetCursor(fs, tmp.path, sid(SID_A), t, ts(1715450100)))
      await Effect.runPromise(advanceTargetCursor(fs, tmp.path, sid(SID_A), t, ts(1715450200)))
      expect(readEntries(tmp.path, SID_A)).toEqual([
        [{ channel: 'home', thread: 'payments' }, 1715450200],
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('distinct targets in one session coexist — a channel and a thread are separate keys', async () => {
    const tmp = buildTmpDir()
    try {
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home'), ts(1715450100)),
      )
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home', 'payments'), ts(1715450200)),
      )
      const entries = readEntries(tmp.path, SID_A)
      expect(entries).toContainEqual([{ channel: 'home', thread: null }, 1715450100])
      expect(entries).toContainEqual([{ channel: 'home', thread: 'payments' }, 1715450200])
      expect(entries).toHaveLength(2)
    } finally {
      tmp.cleanup()
    }
  })

  test('sessions are isolated — one session’s file does not carry another’s cursors', async () => {
    const tmp = buildTmpDir()
    try {
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home'), ts(1715450100)),
      )
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_B), target('general'), ts(1715450200)),
      )
      expect(readEntries(tmp.path, SID_A)).toEqual([
        [{ channel: 'home', thread: null }, 1715450100],
      ])
      expect(readEntries(tmp.path, SID_B)).toEqual([
        [{ channel: 'general', thread: null }, 1715450200],
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('recovers from a corrupt prior file by overwriting it', async () => {
    const tmp = buildTmpDir()
    try {
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      await Effect.runPromise(
        advanceTargetCursor(fs, tmp.path, sid(SID_A), target('home'), ts(1715450100)),
      )
      expect(readEntries(tmp.path, SID_A)).toEqual([
        [{ channel: 'home', thread: null }, 1715450100],
      ])
    } finally {
      tmp.cleanup()
    }
  })
})

describe('deliveryTargetOf', () => {
  test('derives channel + some(thread) from a message with a topic', () => {
    expect(deliveryTargetOf(msg({}))).toEqual(target('home', 'payments'))
  })

  test('derives channel + none from a top-level message', () => {
    expect(deliveryTargetOf(msg({ ref: refTopLevel }))).toEqual(target('home'))
  })
})

/**
 * `targetCursorDirConfig` reads `XDG_STATE_HOME` the same way its siblings do —
 * resolved against a fixture `ConfigProvider.fromMap` rather than mutating env.
 */
const resolveDir = (env: Record<string, string>): string =>
  Effect.runSync(
    targetCursorDirConfig.pipe(
      Effect.withConfigProvider(ConfigProvider.fromMap(new Map(Object.entries(env)))),
    ),
  )

describe('targetCursorDirConfig', () => {
  test('falls back to the XDG default under the home dir when unset', () => {
    expect(resolveDir({})).toBe(join(homedir(), '.local', 'state', 'commy', 'target-cursors'))
  })

  test('uses XDG_STATE_HOME when set', () => {
    expect(resolveDir({ XDG_STATE_HOME: '/var/lib/state' })).toBe(
      join('/var/lib/state', 'commy', 'target-cursors'),
    )
  })

  test('is distinct from the cursors and subscriptions dirs (no collision)', () => {
    expect(resolveDir({ XDG_STATE_HOME: '/s' })).not.toBe(join('/s', 'commy', 'cursors'))
    expect(resolveDir({ XDG_STATE_HOME: '/s' })).not.toBe(join('/s', 'commy', 'subscriptions'))
  })
})

// --- store-level advanceCursor: id resolved off the captured deferred ---

const boundStore = (dir: string, cursorDir: string, id: SessionId) =>
  Effect.runSync(
    Effect.gen(function* () {
      const session = yield* Deferred.make<SessionId>()
      yield* Deferred.succeed(session, id)
      return createFileSubscriptionStore({ dir, fs, cursorDir, session })
    }),
  )

describe('SubscriptionStore.advanceCursor', () => {
  test('writes the per-target cursor when the session deferred is already filled', async () => {
    const tmp = buildTmpDir()
    try {
      const cursorDir = join(tmp.path, 'target-cursors')
      const store = boundStore(join(tmp.path, 'subs'), cursorDir, sid(SID_A))
      await Effect.runPromise(store.advanceCursor(target('home', 'payments'), ts(1715450000)))
      expect(readEntries(cursorDir, SID_A)).toContainEqual([
        { channel: 'home', thread: 'payments' },
        1715450000,
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('is a NON-BLOCKING no-op when the session id is not yet known — returns promptly, writes nothing', async () => {
    const tmp = buildTmpDir()
    try {
      // Build against an UNfilled deferred. A blocking `Deferred.await` here
      // would park forever (the hot-path drop this effort kills); the poll path
      // must resolve to a no-op and complete. `runPromise` resolving at all IS
      // the non-blocking proof — a park would hang the test.
      const cursorDir = join(tmp.path, 'target-cursors')
      const session = Effect.runSync(Deferred.make<SessionId>())
      const store = createFileSubscriptionStore({
        dir: join(tmp.path, 'subs'),
        fs,
        cursorDir,
        session,
      })
      await Effect.runPromise(store.advanceCursor(target('home', 'payments'), ts(1715450000)))
      expect(existsSync(join(cursorDir, `${SID_A}.json`))).toBe(false)
    } finally {
      tmp.cleanup()
    }
  })
})

// --- pump → store integration: the write half fires on plain message delivery ---

const BOT_ID = decodeIdentityIdSync('bot-42')
const sender = {
  id: decodeIdentityIdSync('user-7'),
  name: decodeDisplayNameSync('Carol'),
  kind: 'human' as const,
}

const paymentsThread = Option.some({
  name: decodeThreadNameSync('payments'),
  resolved: false,
  permalink: ThreadPermalinkSchema.make(
    'https://zulip.example.com/#narrow/channel/9-home/topic/payments',
  ),
})

function msg(overrides: Partial<Message>): Message {
  return {
    ref: {
      id: decodeMessageIdSync('msg-1'),
      channel: {
        id: decodeChannelIdSync('chan-9'),
        name: decodeChannelNameSync('home'),
        permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/home'),
      },
      thread: paymentsThread,
      permalink: MessagePermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/home/topic/payments/near/1',
      ),
    },
    sender,
    body: decodeMessageBodySync('hello'),
    ts: decodeTimestampSync(1715450000),
    mentions: [],
    reactions: [],
    ...overrides,
  }
}

const refTopLevel = {
  id: decodeMessageIdSync('msg-1'),
  channel: {
    id: decodeChannelIdSync('chan-9'),
    name: decodeChannelNameSync('home'),
    permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/home'),
  },
  thread: Option.none(),
  permalink: MessagePermalinkSchema.make('https://zulip.example.com/#narrow/channel/home/near/1'),
}

const drainingInbox = (events: ReadonlyArray<InboundEvent>): Pick<MessageInbox, 'events'> => ({
  events: () =>
    Stream.async<InboundEvent>((emit) => {
      for (const ev of events) void emit.single(ev)
      void emit.end()
      return Effect.void
    }),
})

const collectingNotifier = () => {
  const calls: ChannelEventPayload[] = []
  return { calls, notifier: async (p: ChannelEventPayload) => void calls.push(p) }
}

describe('pump → store write half', () => {
  test('a delivered plain message-posted (NOT a mention) persists a per-target cursor', async () => {
    const tmp = buildTmpDir()
    try {
      const cursorDir = join(tmp.path, 'target-cursors')
      const store = boundStore(join(tmp.path, 'subs'), cursorDir, sid(SID_A))
      const collector = collectingNotifier()
      await Effect.runPromise(
        Effect.gen(function* () {
          const handle = yield* startEventPump({
            inbox: drainingInbox([
              { kind: 'message-posted', message: msg({ ts: decodeTimestampSync(1715450500) }) },
            ]),
            notifier: collector.notifier,
            getBotIdentityId: () => BOT_ID,
            onDelivery: (t, at) =>
              store.advanceCursor(t, at).pipe(Effect.catchAllCause(() => Effect.void)),
          })
          yield* handle.done
        }),
      )
      // The consumer saw the plain channel message...
      expect(collector.calls).toHaveLength(1)
      expect(collector.calls[0]?.content).toBe('hello')
      // ...and its subscription's cursor was laid down, message-posted included.
      expect(readEntries(cursorDir, SID_A)).toContainEqual([
        { channel: 'home', thread: 'payments' },
        1715450500,
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('delivery does not park when the session id is unknown — message still delivered, no cursor written', async () => {
    const tmp = buildTmpDir()
    try {
      // Real store on an UNfilled session deferred wired to the pump. If
      // advanceCursor blocked on the id, dispatch would park and the drain
      // would never end — `handle.done` resolving is the end-to-end no-park
      // proof for the hot delivery path.
      const cursorDir = join(tmp.path, 'target-cursors')
      const session = Effect.runSync(Deferred.make<SessionId>())
      const store = createFileSubscriptionStore({
        dir: join(tmp.path, 'subs'),
        fs,
        cursorDir,
        session,
      })
      const collector = collectingNotifier()
      await Effect.runPromise(
        Effect.gen(function* () {
          const handle = yield* startEventPump({
            inbox: drainingInbox([{ kind: 'message-posted', message: msg({}) }]),
            notifier: collector.notifier,
            getBotIdentityId: () => BOT_ID,
            onDelivery: (t, at) =>
              store.advanceCursor(t, at).pipe(Effect.catchAllCause(() => Effect.void)),
          })
          yield* handle.done
        }),
      )
      expect(collector.calls).toHaveLength(1)
      expect(existsSync(join(cursorDir, `${SID_A}.json`))).toBe(false)
    } finally {
      tmp.cleanup()
    }
  })
})
