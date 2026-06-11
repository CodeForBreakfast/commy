import { homedir } from 'node:os'
import { join } from 'node:path'
import type { IdentityId, Timestamp } from '@commy/core/ports'
import { TimestampSchema } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import {
  Config,
  ConfigError,
  Context,
  Effect,
  Either,
  Layer,
  Option,
  type ParseResult,
  Schema,
} from 'effect'

/**
 * Persistent per-identity "have-seen-up-to" cursor (comms-rxo).
 *
 * On resume the plugin reads the cursor for its identity to know how far
 * back to fetch missed @-mentions; on every observed mention the cursor
 * advances forward. The store is intentionally tiny: a single `lastSeenTs`
 * per identity, stored as JSON so a human can inspect or wipe it.
 *
 * `read` yields `Option.none` when no cursor file exists for the identity —
 * the "fresh acquire, nothing to replay" signal callers gate on. A file
 * that exists but cannot be parsed surfaces a `ParseError` rather than
 * masquerading as absent, so a corrupt cursor is logged instead of
 * silently re-replaying from the beginning.
 *
 * Writes are monotonic — supplying a ts earlier than the stored value
 * is a no-op so out-of-order delivery on a single substrate does not
 * walk the cursor backwards. A corrupt prior file is treated as no
 * prior for the monotonic guard, so `write` recovers by overwriting it.
 */
export interface CursorStore {
  read(
    identityId: IdentityId,
  ): Effect.Effect<Option.Option<Timestamp>, PlatformError | ParseResult.ParseError>
  write(identityId: IdentityId, ts: Timestamp): Effect.Effect<void, PlatformError>
}

/**
 * Context tag for the per-identity mentions cursor store (comms-spj3.39).
 * The boot program reads it from context; the app layer registers
 * {@link FileCursorStoreLive} in production, tests register an in-memory
 * layer.
 */
export class CursorStoreTag extends Context.Tag('CursorStore')<CursorStoreTag, CursorStore>() {}

export interface FileCursorStoreDeps {
  /** Directory the per-identity JSON files live under. Created lazily on first write. */
  readonly dir: string
  /**
   * The filesystem every read/write executes against, injected at
   * construction (comms-5db). {@link FileCursorStoreLive} reads it from
   * context (`BunFileSystem.layer`, provided once in the app layer).
   */
  readonly fs: FileSystem.FileSystem
}

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map an opaque identity id onto a single safe filename. The mapping
 * preserves alphanumerics / `-` / `_` / `.` verbatim and replaces
 * everything else with `_` — so path-traversal payloads (`../`, etc.)
 * collapse into a flat string with no separators, and the resulting
 * path is always a direct child of the configured directory.
 */
const cursorFilename = (id: IdentityId): string => {
  const safe = (id as string).replace(FILENAME_SAFE, '_')
  return `${safe}.json`
}

const CursorFileSchema = Schema.parseJson(Schema.Struct({ lastSeenTs: TimestampSchema }))
const decodeCursorFile = Schema.decodeUnknown(CursorFileSchema)

const isNotFound = (error: PlatformError | ParseResult.ParseError): boolean =>
  error._tag === 'SystemError' && error.reason === 'NotFound'

/**
 * Read and decode the cursor file at `path` against the supplied
 * `FileSystem`. A missing file becomes `Option.none`; a
 * present-but-unparseable file fails with the decode's `ParseError`.
 * Other filesystem failures (permissions, etc.) propagate as
 * `PlatformError`.
 */
const readCursor = (
  fs: FileSystem.FileSystem,
  path: string,
): Effect.Effect<Option.Option<Timestamp>, PlatformError | ParseResult.ParseError> =>
  fs.readFileString(path).pipe(
    Effect.flatMap(decodeCursorFile),
    Effect.map((file) => Option.some(file.lastSeenTs as Timestamp)),
    Effect.catchIf(isNotFound, () => Effect.succeed(Option.none<Timestamp>())),
  )

export const createFileCursorStore = (deps: FileCursorStoreDeps): CursorStore => {
  const { dir, fs } = deps
  const pathFor = (id: IdentityId): string => join(dir, cursorFilename(id))

  const read: CursorStore['read'] = (id) => readCursor(fs, pathFor(id))

  const write: CursorStore['write'] = (id, ts) =>
    readCursor(fs, pathFor(id)).pipe(
      Effect.catchTag('ParseError', () => Effect.succeed(Option.none<Timestamp>())),
      Effect.flatMap((prior) =>
        Option.isSome(prior) && prior.value >= ts
          ? Effect.void
          : fs
              .makeDirectory(dir, { recursive: true })
              .pipe(
                Effect.zipRight(
                  fs.writeFileString(pathFor(id), JSON.stringify({ lastSeenTs: ts })),
                ),
              ),
      ),
    )

  return { read, write }
}

const STATE_SEGMENT = 'commy'

/**
 * The XDG state-home base, read from the ambient ConfigProvider at the
 * boot edge (comms-nrv). `XDG_STATE_HOME` is the base; an unset or empty
 * value falls back to `<home>/.local/state`. Empty is folded into
 * `MissingData` so `Config.withDefault` supplies the home-dir fallback for
 * both the unset and blank cases.
 */
const stateBaseConfig: Config.Config<string> = Config.string('XDG_STATE_HOME').pipe(
  Config.mapOrFail((value) =>
    value.length === 0
      ? Either.left(ConfigError.MissingData(['XDG_STATE_HOME'], 'XDG_STATE_HOME is empty'))
      : Either.right(value),
  ),
  Config.withDefault(join(homedir(), '.local', 'state')),
)

/**
 * Cursor directory under the XDG state home. The plugin owns
 * `<state-home>/commy/cursors` and writes only there — the convention
 * asserted by `server.integration.test.ts`.
 */
export const cursorDirConfig: Config.Config<string> = stateBaseConfig.pipe(
  Config.map((base) => join(base, STATE_SEGMENT, 'cursors')),
)

/**
 * Production cursor-store layer (comms-spj3.39): a file-backed store
 * under the XDG state home. Reads `FileSystem` and the cursor directory
 * (via {@link cursorDirConfig} against the boot-edge ConfigProvider,
 * comms-nrv) from context — the app layer provides `BunFileSystem.layer`
 * and the ConfigProvider once (comms-5db) — and injects them into the
 * store at construction.
 *
 * `cursorDirConfig` always yields a value (`withDefault` covers the
 * unset/empty `XDG_STATE_HOME` case), so a residual `ConfigError` here
 * means the config source itself is unavailable — an unrecoverable boot
 * fault, defected with `orDie` to keep the layer's error channel `never`.
 */
export const FileCursorStoreLive: Layer.Layer<CursorStoreTag, never, FileSystem.FileSystem> =
  Layer.effect(
    CursorStoreTag,
    Effect.all([FileSystem.FileSystem, Effect.orDie(cursorDirConfig)]).pipe(
      Effect.map(([fs, dir]) => createFileCursorStore({ dir, fs })),
    ),
  )
