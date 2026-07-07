import { homedir } from 'node:os'
import { join } from 'node:path'
import type { QueueState } from '@commy/zulip/events'
import { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import { Config, Context, Effect, Layer, Option, type ParseResult, Schema } from 'effect'
import type { SessionIdValue } from './session-id.ts'

/**
 * Persistent per-session events-queue state — the write half of long-idle
 * queue resume for ephemeral seats.
 *
 * On resume the plugin reads the state for its session to recover the
 * `queue_id` it last held and how far it drained (`lastEventId`), so it can
 * `GET /events` from that cursor and replay everything missed rather than
 * re-registering blind. As batches drain, {@link QueueStateStore.advance}
 * walks `lastEventId` forward; registering a fresh queue replaces the whole
 * state via {@link QueueStateStore.write}.
 *
 * Distinct from `cursor-store.ts` (a per-`IdentityId` mentions watermark):
 * different key shape, different directory (`<state>/commy/queue-state`), no
 * collision. One file per session holds a single `QueueState`, stored as JSON
 * so a human can inspect or wipe it.
 *
 * `read` yields `Option.none` when no state file exists — the "fresh session,
 * nothing to resume" signal callers gate on. A present-but-unparseable file
 * surfaces a `ParseError` rather than masquerading as absent, so a corrupt
 * state is logged instead of silently re-registering.
 *
 * `write` replaces the state wholesale — a newly registered queue legitimately
 * carries a `lastEventId` below the prior one, so the monotonic guard lives on
 * `advance` (never walks `lastEventId` backwards within the current queue), not
 * on `write`. A corrupt prior file is treated as no prior for `advance`, so it
 * simply has nothing to move.
 */
export interface QueueStateStore {
  read(
    sessionId: SessionIdValue,
  ): Effect.Effect<Option.Option<QueueState>, PlatformError | ParseResult.ParseError>
  write(sessionId: SessionIdValue, state: QueueState): Effect.Effect<void, PlatformError>
  advance(sessionId: SessionIdValue, lastEventId: number): Effect.Effect<void, PlatformError>
}

/**
 * Context tag for the per-session queue-state store. The boot program reads it
 * from context; the app layer registers {@link FileQueueStateStoreLive} in
 * production, tests register an in-memory or file-backed layer.
 */
export class QueueStateStoreTag extends Context.Tag('QueueStateStore')<
  QueueStateStoreTag,
  QueueStateStore
>() {}

export interface FileQueueStateStoreDeps {
  /** Directory the per-session JSON files live under. Created lazily on first write. */
  readonly dir: string
  /**
   * The filesystem every read/write executes against, injected at
   * construction. {@link FileQueueStateStoreLive} reads it from context
   * (`NodeContext.layer`, provided once in the app layer).
   */
  readonly fs: FileSystem.FileSystem
}

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map a session id onto a single safe filename. The mapping preserves
 * alphanumerics / `-` / `_` / `.` verbatim and replaces everything else with
 * `_` — so any path-traversal payload collapses into a flat string with no
 * separators, and the resulting path is always a direct child of the
 * configured directory.
 */
const stateFilename = (id: SessionIdValue): string =>
  `${(id as string).replace(FILENAME_SAFE, '_')}.json`

const QueueStateFileSchema = Schema.parseJson(
  Schema.Struct({ queueId: Schema.String, lastEventId: Schema.Int }),
)
const decodeQueueStateFile = Schema.decodeUnknown(QueueStateFileSchema)
const encodeQueueStateFile = Schema.encode(QueueStateFileSchema)

const isNotFound = (error: PlatformError | ParseResult.ParseError): boolean =>
  error._tag === 'SystemError' && error.reason === 'NotFound'

/**
 * Read and decode the state file at `path`. A missing file becomes
 * `Option.none`; a present-but-unparseable file fails with the decode's
 * `ParseError`. Other filesystem failures propagate as `PlatformError`.
 */
const readState = (
  fs: FileSystem.FileSystem,
  path: string,
): Effect.Effect<Option.Option<QueueState>, PlatformError | ParseResult.ParseError> =>
  fs.readFileString(path).pipe(
    Effect.flatMap(decodeQueueStateFile),
    Effect.map(Option.some),
    Effect.catchIf(isNotFound, () => Effect.succeed(Option.none<QueueState>())),
  )

export const createFileQueueStateStore = (deps: FileQueueStateStoreDeps): QueueStateStore => {
  const { dir, fs } = deps
  const pathFor = (id: SessionIdValue): string => join(dir, stateFilename(id))

  const persist = (path: string, state: QueueState): Effect.Effect<void, PlatformError> =>
    encodeQueueStateFile(state).pipe(
      Effect.orDie,
      Effect.flatMap((json) =>
        fs
          .makeDirectory(dir, { recursive: true })
          .pipe(Effect.zipRight(fs.writeFileString(path, json))),
      ),
    )

  const read: QueueStateStore['read'] = (id) => readState(fs, pathFor(id))

  const write: QueueStateStore['write'] = (id, state) => persist(pathFor(id), state)

  const advance: QueueStateStore['advance'] = (id, lastEventId) => {
    const path = pathFor(id)
    return readState(fs, path).pipe(
      Effect.catchTag('ParseError', () => Effect.succeed(Option.none<QueueState>())),
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: (prior) =>
            lastEventId > prior.lastEventId
              ? persist(path, { queueId: prior.queueId, lastEventId })
              : Effect.void,
        }),
      ),
    )
  }

  return { read, write, advance }
}

const STATE_SEGMENT = 'commy'

/**
 * The XDG state-home base, read from the ambient ConfigProvider at the boot
 * edge — identical to cursor-store's. `XDG_STATE_HOME` is the base; an unset
 * or empty value falls back to `<home>/.local/state`.
 */
const stateBaseConfig: Config.Config<string> = Config.nonEmptyString('XDG_STATE_HOME').pipe(
  Config.withDefault(join(homedir(), '.local', 'state')),
)

/**
 * Queue-state directory under the XDG state home. The plugin owns
 * `<state-home>/commy/queue-state` and writes only there — separate from
 * `commy/cursors` and `commy/subscriptions`.
 */
export const queueStateDirConfig: Config.Config<string> = stateBaseConfig.pipe(
  Config.map((base) => join(base, STATE_SEGMENT, 'queue-state')),
)

/**
 * Production queue-state-store layer: a file-backed store under the XDG state
 * home. Reads `FileSystem` and the queue-state directory (via
 * {@link queueStateDirConfig} against the boot-edge ConfigProvider) from
 * context — the app layer provides `NodeContext.layer` and the ConfigProvider
 * once — and injects them into the store at construction.
 *
 * `queueStateDirConfig` always yields a value (`withDefault` covers the
 * unset/empty `XDG_STATE_HOME` case), so a residual `ConfigError` here means
 * the config source itself is unavailable — an unrecoverable boot fault,
 * defected with `orDie` to keep the layer's error channel `never`.
 */
export const FileQueueStateStoreLive: Layer.Layer<
  QueueStateStoreTag,
  never,
  FileSystem.FileSystem
> = Layer.effect(
  QueueStateStoreTag,
  Effect.all([FileSystem.FileSystem, Effect.orDie(queueStateDirConfig)]).pipe(
    Effect.map(([fs, dir]) => createFileQueueStateStore({ dir, fs })),
  ),
)
