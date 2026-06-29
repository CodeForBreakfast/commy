import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { decodeChannelNameSync, decodeThreadNameSync } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import { ConfigProvider, Effect, Option } from 'effect'
import { parseSessionId, type SessionId } from './bootstrap.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import { createFileSubscriptionStore, subscriptionDirConfig } from './subscription-store.ts'

const fs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'

const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

const mentions: SubscribeIntent = { kind: 'mentions' }
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
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(Option.isNone(result)).toBe(true)
    } finally {
      tmp.cleanup()
    }
  })

  test('write then read round-trips every intent kind', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      const intents = [
        mentions,
        channel('commy'),
        thread('commy', 'sub-restore-comms-4pgy'),
        newTopics('general'),
      ]
      await Effect.runPromise(store.write(sid(SID_A), intents))
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(result).toEqual(Option.some(intents))
    } finally {
      tmp.cleanup()
    }
  })

  test('write creates the subscription directory if it does not exist yet', async () => {
    const tmp = buildTmpDir()
    try {
      const nested = join(tmp.path, 'nested', 'subscriptions')
      const store = createFileSubscriptionStore({ dir: nested, fs })
      await Effect.runPromise(store.write(sid(SID_A), [mentions, channel('commy')]))
      const written = JSON.parse(readFileSync(join(nested, `${SID_A}.json`), 'utf8'))
      expect(written).toEqual([{ kind: 'mentions' }, { kind: 'channel', channelName: 'commy' }])
    } finally {
      tmp.cleanup()
    }
  })

  test('write overwrites — a later write replaces the earlier set (a removal persists)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(sid(SID_A), [mentions, channel('commy')]))
      await Effect.runPromise(store.write(sid(SID_A), [mentions]))
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(result).toEqual(Option.some([mentions]))
    } finally {
      tmp.cleanup()
    }
  })

  test('an empty set round-trips as Some([]) — distinct from None (resume-with-nothing, not fresh)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(sid(SID_A), []))
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(result).toEqual(Option.some([]))
    } finally {
      tmp.cleanup()
    }
  })

  test('session ids are isolated — write under one id does not leak to another', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(sid(SID_A), [channel('commy')]))
      await Effect.runPromise(store.write(sid(SID_B), [channel('general')]))
      expect(await Effect.runPromise(store.read(sid(SID_A)))).toEqual(
        Option.some([channel('commy')]),
      )
      expect(await Effect.runPromise(store.read(sid(SID_B)))).toEqual(
        Option.some([channel('general')]),
      )
    } finally {
      tmp.cleanup()
    }
  })

  test('the session id maps onto a single flat file that is a direct child of dir', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(sid(SID_A), [mentions]))
      const entries = readdirSync(tmp.path)
      expect(entries).toEqual([`${SID_A}.json`])
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a malformed subscription file (not silently absent)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      const err = await Effect.runPromise(Effect.flip(store.read(sid(SID_A))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a file with the wrong shape', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), JSON.stringify([{ kind: 'bogus' }]))
      const err = await Effect.runPromise(Effect.flip(store.read(sid(SID_A))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('write overwrites a corrupt prior file', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileSubscriptionStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      await Effect.runPromise(store.write(sid(SID_A), [mentions]))
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(result).toEqual(Option.some([mentions]))
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
