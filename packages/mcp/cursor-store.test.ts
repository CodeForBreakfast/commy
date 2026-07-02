import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { decodeIdentityIdSync, decodeTimestampSync } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import { NodeFileSystem } from '@effect/platform-node'
import { ConfigProvider, Effect, Option } from 'effect'
import { createFileCursorStore, cursorDirConfig } from './cursor-store.ts'

const fs = Effect.runSync(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))

const buildTmpDir = (): { readonly path: string; readonly cleanup: () => void } => {
  const path = mkdtempSync(join(tmpdir(), 'cursor-store-'))
  return {
    path,
    cleanup: () => rmSync(path, { recursive: true, force: true }),
  }
}

describe('createFileCursorStore', () => {
  test('read yields none when no cursor file exists for the identity', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      const result = await Effect.runPromise(store.read(decodeIdentityIdSync('bot-1')))
      expect(Option.isNone(result)).toBe(true)
    } finally {
      tmp.cleanup()
    }
  })

  test('write then read returns the persisted timestamp', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      const id = decodeIdentityIdSync('bot-1')
      await Effect.runPromise(store.write(id, decodeTimestampSync(1715000000)))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some(decodeTimestampSync(1715000000)))
    } finally {
      tmp.cleanup()
    }
  })

  test('write creates the cursor directory if it does not exist yet', async () => {
    const tmp = buildTmpDir()
    try {
      const nested = join(tmp.path, 'nested', 'cursors')
      const store = createFileCursorStore({ dir: nested, fs })
      await Effect.runPromise(store.write(decodeIdentityIdSync('bot-1'), decodeTimestampSync(42)))
      const written = JSON.parse(readFileSync(join(nested, 'bot-1.json'), 'utf8'))
      expect(written).toEqual({ lastSeenTs: 42 })
    } finally {
      tmp.cleanup()
    }
  })

  test('write monotonically advances — earlier ts does not regress the cursor', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      const id = decodeIdentityIdSync('bot-1')
      await Effect.runPromise(store.write(id, decodeTimestampSync(1000)))
      await Effect.runPromise(store.write(id, decodeTimestampSync(500)))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some(decodeTimestampSync(1000)))
    } finally {
      tmp.cleanup()
    }
  })

  test('write advances forward when a later ts is supplied', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      const id = decodeIdentityIdSync('bot-1')
      await Effect.runPromise(store.write(id, decodeTimestampSync(1000)))
      await Effect.runPromise(store.write(id, decodeTimestampSync(2000)))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some(decodeTimestampSync(2000)))
    } finally {
      tmp.cleanup()
    }
  })

  test('identity ids are isolated — write under one id does not leak to another', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      await Effect.runPromise(store.write(decodeIdentityIdSync('bot-1'), decodeTimestampSync(1000)))
      await Effect.runPromise(store.write(decodeIdentityIdSync('bot-2'), decodeTimestampSync(2000)))
      expect(await Effect.runPromise(store.read(decodeIdentityIdSync('bot-1')))).toEqual(
        Option.some(decodeTimestampSync(1000)),
      )
      expect(await Effect.runPromise(store.read(decodeIdentityIdSync('bot-2')))).toEqual(
        Option.some(decodeTimestampSync(2000)),
      )
    } finally {
      tmp.cleanup()
    }
  })

  test('identity ids with path-traversal characters are sanitised', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      const id = decodeIdentityIdSync('../../etc/passwd')
      await Effect.runPromise(store.write(id, decodeTimestampSync(123)))
      const result = await Effect.runPromise(store.read(id))
      expect(result).toEqual(Option.some(decodeTimestampSync(123)))
      // Path separators in the id collapse to `_` so the cursor file
      // is always a direct child of `dir` — never escapes it.
      const entries = readdirSync(tmp.path)
      expect(entries.length).toBe(1)
      const only = entries[0] ?? ''
      expect(only).not.toContain('/')
      expect(only).toMatch(/\.json$/)
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a malformed cursor file (no longer silently absent)', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, 'bot-1.json'), 'not json at all')
      const err = await Effect.runPromise(Effect.flip(store.read(decodeIdentityIdSync('bot-1'))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('read surfaces a ParseError for a cursor file with the wrong shape', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, 'bot-1.json'), JSON.stringify({ wrong: 'shape' }))
      const err = await Effect.runPromise(Effect.flip(store.read(decodeIdentityIdSync('bot-1'))))
      expect(err._tag).toBe('ParseError')
    } finally {
      tmp.cleanup()
    }
  })

  test('write recovers from a corrupt prior cursor by overwriting it', async () => {
    const tmp = buildTmpDir()
    try {
      const store = createFileCursorStore({ dir: tmp.path, fs })
      writeFileSync(join(tmp.path, 'bot-1.json'), 'not json at all')
      await Effect.runPromise(store.write(decodeIdentityIdSync('bot-1'), decodeTimestampSync(777)))
      const result = await Effect.runPromise(store.read(decodeIdentityIdSync('bot-1')))
      expect(result).toEqual(Option.some(decodeTimestampSync(777)))
    } finally {
      tmp.cleanup()
    }
  })
})

/**
 * `cursorDirConfig` reads `XDG_STATE_HOME` via Effect `Config`;
 * the tests resolve it against a fixture `ConfigProvider.fromMap` rather
 * than mutating `process.env`, mirroring how the boot edge feeds the
 * ambient provider.
 */
const resolveCursorDir = (env: Record<string, string>): string =>
  Effect.runSync(
    cursorDirConfig.pipe(
      Effect.withConfigProvider(ConfigProvider.fromMap(new Map(Object.entries(env)))),
    ),
  )

describe('cursorDirConfig', () => {
  test('falls back to the XDG default under the home dir when XDG_STATE_HOME is unset', () => {
    expect(resolveCursorDir({})).toBe(join(homedir(), '.local', 'state', 'commy', 'cursors'))
  })

  test('uses XDG_STATE_HOME when set', () => {
    expect(resolveCursorDir({ XDG_STATE_HOME: '/var/lib/state' })).toBe(
      join('/var/lib/state', 'commy', 'cursors'),
    )
  })

  test('treats an empty XDG_STATE_HOME as unset and falls back to the home dir', () => {
    expect(resolveCursorDir({ XDG_STATE_HOME: '' })).toBe(
      join(homedir(), '.local', 'state', 'commy', 'cursors'),
    )
  })

  test('treats a whitespace-only XDG_STATE_HOME as a value, not as unset', () => {
    expect(resolveCursorDir({ XDG_STATE_HOME: '   ' })).toBe(join('   ', 'commy', 'cursors'))
  })
})
