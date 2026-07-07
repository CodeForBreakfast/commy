import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { FileSystem } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import { ConfigProvider, Effect, Option } from 'effect'
import { parseSessionId, type SessionId } from './bootstrap.ts'
import { createFileQueueStateStore, queueStateDirConfig } from './queue-state-store.ts'

const fs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const SID_A = '11111111-1111-4111-8111-111111111111'
const SID_B = '22222222-2222-4222-8222-222222222222'
const sid = (raw: string): SessionId => Option.getOrThrow(parseSessionId(raw))

const buildTmpDir = (): { readonly path: string; readonly cleanup: () => void } => {
  const path = mkdtempSync(join(tmpdir(), 'queue-state-store-'))
  return { path, cleanup: () => rmSync(path, { recursive: true, force: true }) }
}

describe('createFileQueueStateStore', () => {
  test('read yields none when no state file exists for the session', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(Option.isNone(result)).toBe(true)
    } finally {
      tmp.cleanup()
    }
  })

  test('write then read round-trips the persisted queue state', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.write(id, { queueId: 'q-1', lastEventId: 7 }))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some({ queueId: 'q-1', lastEventId: 7 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('write creates the state directory if it does not exist yet', async () => {
    const tmp = buildTmpDir()
    try {
      const nested = join(tmp.path, 'nested', 'queue-state')
      const store = createFileQueueStateStore({ dir: nested, fs })
      await Effect.runPromise(store.write(sid(SID_A), { queueId: 'q-9', lastEventId: 42 }))
      const written = JSON.parse(readFileSync(join(nested, `${SID_A}.json`), 'utf8'))
      expect(written).toEqual({ queueId: 'q-9', lastEventId: 42 })
    } finally {
      tmp.cleanup()
    }
  })

  test('write replaces the prior state wholesale — a new queue with a lower lastEventId lands', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.write(id, { queueId: 'q-old', lastEventId: 500 }))
      await Effect.runPromise(store.write(id, { queueId: 'q-new', lastEventId: 1 }))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some({ queueId: 'q-new', lastEventId: 1 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('advance moves lastEventId forward while preserving the queueId', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.write(id, { queueId: 'q-1', lastEventId: 7 }))
      await Effect.runPromise(store.advance(id, 12))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some({ queueId: 'q-1', lastEventId: 12 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('advance is monotonic — an earlier lastEventId does not regress the cursor', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.write(id, { queueId: 'q-1', lastEventId: 100 }))
      await Effect.runPromise(store.advance(id, 50))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some({ queueId: 'q-1', lastEventId: 100 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('advance on an equal lastEventId is a no-op', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.write(id, { queueId: 'q-1', lastEventId: 7 }))
      await Effect.runPromise(store.advance(id, 7))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some({ queueId: 'q-1', lastEventId: 7 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('advance with no prior state is a no-op (nothing to advance)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      const id = sid(SID_A)
      await Effect.runPromise(store.advance(id, 5))
      const result = await Effect.runPromise(store.read(id))
      expect(Option.isNone(result)).toBe(true)
    } finally {
      tmp.cleanup()
    }
  })

  test('session ids are isolated — write under one id does not leak to another', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(sid(SID_A), { queueId: 'q-a', lastEventId: 1 }))
      await Effect.runPromise(store.write(sid(SID_B), { queueId: 'q-b', lastEventId: 2 }))
      expect(await Effect.runPromise(store.read(sid(SID_A)))).toEqual(
        Option.some({ queueId: 'q-a', lastEventId: 1 }),
      )
      expect(await Effect.runPromise(store.read(sid(SID_B)))).toEqual(
        Option.some({ queueId: 'q-b', lastEventId: 2 }),
      )
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a malformed state file (not silently absent)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      const err = await Effect.runPromise(Effect.flip(store.read(sid(SID_A))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a state file with the wrong shape', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), JSON.stringify({ wrong: 'shape' }))
      const err = await Effect.runPromise(Effect.flip(store.read(sid(SID_A))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('write recovers from a corrupt prior state by overwriting it', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      await Effect.runPromise(store.write(sid(SID_A), { queueId: 'q-1', lastEventId: 3 }))
      const result = await Effect.runPromise(store.read(sid(SID_A)))
      expect(result).toEqual(Option.some({ queueId: 'q-1', lastEventId: 3 }))
    } finally {
      tmp.cleanup()
    }
  })

  test('advance treats a corrupt prior state as absent — a no-op', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileQueueStateStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, `${SID_A}.json`), 'not json at all')
      await Effect.runPromise(store.advance(sid(SID_A), 5))
      const entries = readdirSync(tmp.path)
      // The corrupt file is left untouched; advance had nothing valid to move.
      expect(readFileSync(join(tmp.path, entries[0] ?? ''), 'utf8')).toBe('not json at all')
    } finally {
      tmp.cleanup()
    }
  })
})

/**
 * `queueStateDirConfig` reads `XDG_STATE_HOME` via Effect `Config`; the tests
 * resolve it against a fixture `ConfigProvider.fromMap` rather than mutating
 * `process.env`, mirroring how the boot edge feeds the ambient provider.
 */
const resolveQueueStateDir = (env: Record<string, string>): string =>
  Effect.runSync(
    queueStateDirConfig.pipe(
      Effect.withConfigProvider(ConfigProvider.fromMap(new Map(Object.entries(env)))),
    ),
  )

describe('queueStateDirConfig', () => {
  test('falls back to the XDG default under the home dir when XDG_STATE_HOME is unset', () => {
    expect(resolveQueueStateDir({})).toBe(
      join(homedir(), '.local', 'state', 'commy', 'queue-state'),
    )
  })

  test('uses XDG_STATE_HOME when set', () => {
    expect(resolveQueueStateDir({ XDG_STATE_HOME: '/var/lib/state' })).toBe(
      join('/var/lib/state', 'commy', 'queue-state'),
    )
  })

  test('treats an empty XDG_STATE_HOME as unset and falls back to the home dir', () => {
    expect(resolveQueueStateDir({ XDG_STATE_HOME: '' })).toBe(
      join(homedir(), '.local', 'state', 'commy', 'queue-state'),
    )
  })
})
