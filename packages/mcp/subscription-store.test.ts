import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import { ConfigProvider, Deferred, Effect, Option } from 'effect'
import { parseSessionId, type SessionId } from './bootstrap.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import {
  createFileSubscriptionStore,
  type SubscriptionStore,
  subscriptionDirConfig,
} from './subscription-store.ts'

const fs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'

const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

/**
 * A store bound to `id` — the session-id deferred it captures is already
 * completed, so its methods resolve the id-keyed path from the deferred with no
 * per-call argument. Mirrors how the live layer captures the one shared
 * deferred at build.
 */
const boundStore = (dir: string, id: SessionId): SubscriptionStore =>
  Effect.runSync(
    Effect.gen(function* () {
      const session = yield* Deferred.make<SessionId>()
      yield* Deferred.succeed(session, id)
      return createFileSubscriptionStore({ dir, fs, session })
    }),
  )

const channel = (name: string): SubscribeIntent => ({
  kind: 'channel',
  channelName: decodeChannelNameSync(name),
})
const thread = (channelName: string, threadName: string): SubscribeIntent => ({
  kind: 'thread',
  channelName: decodeChannelNameSync(channelName),
  threadName: decodeThreadNameSync(threadName),
})
const newTopics = (name: string): SubscribeIntent => ({
  kind: 'new-topics-in-channel',
  channelName: decodeChannelNameSync(name),
})

const buildTmpDir = (): { readonly path: string; readonly cleanup: () => void } => {
  const path = mkdtempSync(join(tmpdir(), 'subscription-store-'))
  return {
    path,
    cleanup: () => rmSync(path, { recursive: true, force: true }),
  }
}

describe('createFileSubscriptionStore', () => {
  test('read yields none when no file exists for the session', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      const result = await Effect.runPromise(store.read())
      expect(Option.isNone(result)).toBe(true)
    } finally {
      tmp.cleanup()
    }
  })

  test('write then read round-trips every intent kind', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      const intents = [
        newTopics('general'),
        channel('commy'),
        thread('commy', 'sub-restore-topic'),
        newTopics('general'),
      ]
      await Effect.runPromise(store.write(intents))
      const result = await Effect.runPromise(store.read())
      expect(result).toEqual(Option.some(intents))
    } finally {
      tmp.cleanup()
    }
  })

  test('write creates the subscription directory if it does not exist yet', async () => {
    const tmp = buildTmpDir()
    try {
      const nested = join(tmp.path, 'nested', 'subscriptions')
      const store = boundStore(nested, sid(SID_A))
      await Effect.runPromise(store.write([newTopics('general'), channel('commy')]))
      const written = JSON.parse(readFileSync(join(nested, `${SID_A}.json`), 'utf8'))
      expect(written).toEqual([
        { kind: 'new-topics-in-channel', channelName: 'general' },
        { kind: 'channel', channelName: 'commy' },
      ])
    } finally {
      tmp.cleanup()
    }
  })

  test('write overwrites — a later write replaces the earlier set (a removal persists)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      await Effect.runPromise(store.write([newTopics('general'), channel('commy')]))
      await Effect.runPromise(store.write([newTopics('general')]))
      const result = await Effect.runPromise(store.read())
      expect(result).toEqual(Option.some([newTopics('general')]))
    } finally {
      tmp.cleanup()
    }
  })

  test('an empty set round-trips as Some([]) — distinct from None (resume-with-nothing, not fresh)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      await Effect.runPromise(store.write([]))
      const result = await Effect.runPromise(store.read())
      expect(result).toEqual(Option.some([]))
    } finally {
      tmp.cleanup()
    }
  })

  test('session ids are isolated — a store bound to one id does not read another id’s set', async () => {
    const tmp = buildTmpDir()
    try {
      const storeA = boundStore(tmp.path, sid(SID_A))
      const storeB = boundStore(tmp.path, sid(SID_B))
      await Effect.runPromise(storeA.write([channel('commy')]))
      await Effect.runPromise(storeB.write([channel('general')]))
      expect(await Effect.runPromise(storeA.read())).toEqual(Option.some([channel('commy')]))
      expect(await Effect.runPromise(storeB.read())).toEqual(Option.some([channel('general')]))
    } finally {
      tmp.cleanup()
    }
  })

  test('the session id maps onto a single flat file that is a direct child of dir', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      await Effect.runPromise(store.write([newTopics('general')]))
      const entries = readdirSync(tmp.path)
      expect(entries).toEqual([`${SID_A}.json`])
    } finally {
      tmp.cleanup()
    }
  })

  test('resolves the id from the bound deferred, not a call argument — write keys the file off the deferred', async () => {
    const tmp = buildTmpDir()
    try {
      // The method takes no id: the only source for the path key is the
      // captured deferred, so a write lands under exactly that id's file.
      const store = boundStore(tmp.path, sid(SID_A))
      await Effect.runPromise(store.write([newTopics('general')]))
      expect(readdirSync(tmp.path)).toEqual([`${SID_A}.json`])
    } finally {
      tmp.cleanup()
    }
  })

  test('a method awaits the deferred lazily — it parks until the id is delivered, then resolves keyed to it', async () => {
    const tmp = buildTmpDir()
    try {
      // Build the store against an UNfilled deferred, then start the read.
      // If the method resolved the id at build it would already have failed;
      // instead it parks on the await and completes only once the id arrives.
      const session = Effect.runSync(Deferred.make<SessionId>())
      const store = createFileSubscriptionStore({
        dir: tmp.path,
        fs,
        session,
      })
      writeFileSync(
        join(tmp.path, `${SID_A}.json`),
        JSON.stringify([{ kind: 'new-topics-in-channel', channelName: 'general' }]),
      )
      const pending = Effect.runPromise(store.read())
      Effect.runSync(Deferred.succeed(session, sid(SID_A)))
      expect(await pending).toEqual(Option.some([newTopics('general')]))
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a malformed subscription file (not silently absent)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      const err = await Effect.runPromise(Effect.flip(store.read()))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a file with the wrong shape', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      writeFileSync(join(tmp.path, `${SID_A}.json`), JSON.stringify([{ kind: 'bogus' }]))
      const err = await Effect.runPromise(Effect.flip(store.read()))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  // comms-n1my. A set persisted before mentions became implicit still holds a
  // `mentions` entry. Rejecting it would fail the whole restore over one dead
  // intent and leave a resuming seat with none of its real narrows — so it
  // decodes and is dropped, matching the retired token's treatment in
  // COMMY_SUBSCRIBE.
  test('read drops a persisted retired mentions intent and keeps the rest', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      writeFileSync(
        join(tmp.path, `${SID_A}.json`),
        JSON.stringify([{ kind: 'mentions' }, { kind: 'channel', channelName: 'commy' }]),
      )
      const result = await Effect.runPromise(store.read())
      expect(result).toEqual(Option.some([channel('commy')]))
    } finally {
      tmp.cleanup()
    }
  })

  test('write overwrites a corrupt prior file', async () => {
    const tmp = buildTmpDir()
    try {
      const store = boundStore(tmp.path, sid(SID_A))
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      await Effect.runPromise(store.write([newTopics('general')]))
      const result = await Effect.runPromise(store.read())
      expect(result).toEqual(Option.some([newTopics('general')]))
    } finally {
      tmp.cleanup()
    }
  })
})

/**
 * `subscriptionDirConfig` reads `XDG_STATE_HOME` via Effect `Config`, the same
 * way `cursorDirConfig` does — resolved here against a fixture
 * `ConfigProvider.fromMap` rather than mutating `process.env`.
 */
const resolveSubscriptionDir = (env: Record<string, string>): string =>
  Effect.runSync(
    subscriptionDirConfig.pipe(
      Effect.withConfigProvider(ConfigProvider.fromMap(new Map(Object.entries(env)))),
    ),
  )

describe('subscriptionDirConfig', () => {
  test('falls back to the XDG default under the home dir when XDG_STATE_HOME is unset', () => {
    expect(resolveSubscriptionDir({})).toBe(
      join(homedir(), '.local', 'state', 'commy', 'subscriptions'),
    )
  })

  test('uses XDG_STATE_HOME when set', () => {
    expect(resolveSubscriptionDir({ XDG_STATE_HOME: '/var/lib/state' })).toBe(
      join('/var/lib/state', 'commy', 'subscriptions'),
    )
  })

  test('treats an empty XDG_STATE_HOME as unset and falls back to the home dir', () => {
    expect(resolveSubscriptionDir({ XDG_STATE_HOME: '' })).toBe(
      join(homedir(), '.local', 'state', 'commy', 'subscriptions'),
    )
  })
})
