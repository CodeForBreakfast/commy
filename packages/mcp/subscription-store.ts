import { homedir } from 'node:os'
import { join } from 'node:path'
import { ChannelNameSchema, ThreadNameSchema } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import { Config, Context, Effect, Layer, Option, type ParseResult, Schema } from 'effect'
import type { SessionId } from './bootstrap.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'

/**
 * Persistent per-session_id narrow-set snapshot.
 *
 * The plugin-layer `narrowSet` (narrow-set.ts) decides which inbound events
 * reach this agent; it is in-memory only and rebuilt from `COMMY_SUBSCRIBE`
 * on every boot, so a stopped-then-resumed Claude Code session silently
 * loses every runtime `subscribe`/`unsubscribe`. This store persists the
 * current set to disk on each mutation and restores it on resume, so the
 * session comes back with exactly the subscriptions it had — including
 * runtime unsubscribes (a dropped default channel stays dropped).
 *
 * The store is keyed on **session_id**, never identity id: the rule is
 * about the session, and Claude Code keeps the same session_id across a
 * resume. Keying on identity id would wrongly treat each relaunch of a
 * pinned `COMMY_BOT_NAME` pane as a resume; a new session_id every launch
 * keeps those panes on the fresh `COMMY_SUBSCRIBE`-only path for free.
 *
 * `read` yields `Option.none` only when no file exists for the session —
 * the "fresh session, seed from `COMMY_SUBSCRIBE`" signal. An empty
 * persisted set returns `Option.some([])`, NOT `none`: a session that
 * unsubscribed from everything must resume with nothing, not fall back to
 * the env defaults. A file that exists but cannot be parsed surfaces a
 * `ParseError` rather than masquerading as absent.
 *
 * Writes overwrite unconditionally — the latest snapshot is authoritative,
 * so a smaller set (after an unsubscribe) correctly replaces a larger one.
 */
export interface SubscriptionStore {
  read(
    sessionId: SessionId,
  ): Effect.Effect<
    Option.Option<ReadonlyArray<SubscribeIntent>>,
    PlatformError | ParseResult.ParseError
  >
  write(
    sessionId: SessionId,
    intents: ReadonlyArray<SubscribeIntent>,
  ): Effect.Effect<void, PlatformError>
}

/**
 * Context tag for the per-session subscription store. The boot program
 * reads it from context; the app layer registers {@link FileSubscriptionStoreLive}
 * in production, tests register a file-backed temp-dir store directly.
 */
export class SubscriptionStoreTag extends Context.Tag('SubscriptionStore')<
  SubscriptionStoreTag,
  SubscriptionStore
>() {}

export interface FileSubscriptionStoreDeps {
  /** Directory the per-session JSON files live under. Created lazily on first write. */
  readonly dir: string
  /**
   * The filesystem every read/write executes against, injected at
   * construction (mirroring cursor-store.ts). {@link FileSubscriptionStoreLive}
   * reads it from context (`NodeContext.layer`, provided once in the app layer).
   */
  readonly fs: FileSystem.FileSystem
}

/**
 * Persisted shape of a single `SubscribeIntent`. A discriminated union on
 * `kind` mirroring `subscribe-parser.ts`; the branded name schemas decode
 * the persisted strings straight back into the in-memory intent type.
 */
const SubscribeIntentSchema = Schema.Union(
  Schema.Struct({ kind: Schema.Literal('mentions') }),
  Schema.Struct({ kind: Schema.Literal('channel'), channelName: ChannelNameSchema }),
  Schema.Struct({
    kind: Schema.Literal('thread'),
    channelName: ChannelNameSchema,
    threadName: ThreadNameSchema,
  }),
  Schema.Struct({ kind: Schema.Literal('new-topics-in-channel'), channelName: ChannelNameSchema }),
)

const SubscriptionsFileSchema = Schema.parseJson(Schema.Array(SubscribeIntentSchema))
const decodeSubscriptionsFile = Schema.decodeUnknown(SubscriptionsFileSchema)

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map a session_id onto a single safe filename. A `SessionId` is already
 * UUID-shaped, but the same sanitisation cursor-store applies is kept as
 * defence-in-depth: non-`[a-zA-Z0-9._-]` characters collapse to `_`, so the
 * resulting path is always a direct child of the configured directory.
 */
const subscriptionFilename = (id: SessionId): string => {
  const safe = (id as string).replace(FILENAME_SAFE, '_')
  return `${safe}.json`
}

const isNotFound = (error: PlatformError | ParseResult.ParseError): boolean =>
  error._tag === 'SystemError' && error.reason === 'NotFound'

/**
 * Read and decode the subscription file at `path`. A missing file becomes
 * `Option.none`; a present-but-unparseable file fails with the decode's
 * `ParseError`. Other filesystem failures propagate as `PlatformError`.
 */
const readSubscriptions = (
  fs: FileSystem.FileSystem,
  path: string,
): Effect.Effect<
  Option.Option<ReadonlyArray<SubscribeIntent>>,
  PlatformError | ParseResult.ParseError
> =>
  fs.readFileString(path).pipe(
    Effect.flatMap(decodeSubscriptionsFile),
    Effect.map((intents) => Option.some(intents)),
    Effect.catchIf(isNotFound, () => Effect.succeed(Option.none<ReadonlyArray<SubscribeIntent>>())),
  )

export const createFileSubscriptionStore = (deps: FileSubscriptionStoreDeps): SubscriptionStore => {
  const { dir, fs } = deps
  const pathFor = (id: SessionId): string => join(dir, subscriptionFilename(id))

  const read: SubscriptionStore['read'] = (id) => readSubscriptions(fs, pathFor(id))

  const write: SubscriptionStore['write'] = (id, intents) =>
    fs
      .makeDirectory(dir, { recursive: true })
      .pipe(Effect.zipRight(fs.writeFileString(pathFor(id), JSON.stringify(intents))))

  return { read, write }
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
 * Subscription directory under the XDG state home. The plugin owns
 * `<state-home>/commy/subscriptions` and writes only there.
 */
export const subscriptionDirConfig: Config.Config<string> = stateBaseConfig.pipe(
  Config.map((base) => join(base, STATE_SEGMENT, 'subscriptions')),
)

/**
 * Production subscription-store layer: a file-backed store under the XDG
 * state home. Reads `FileSystem` and the subscription directory (via
 * {@link subscriptionDirConfig} against the boot-edge ConfigProvider) from
 * context and injects them into the store at construction.
 *
 * `subscriptionDirConfig` always yields a value (`withDefault` covers the
 * unset/empty case), so a residual `ConfigError` here means the config
 * source itself is unavailable — an unrecoverable boot fault, defected with
 * `orDie` to keep the layer's error channel `never`.
 */
export const FileSubscriptionStoreLive: Layer.Layer<
  SubscriptionStoreTag,
  never,
  FileSystem.FileSystem
> = Layer.effect(
  SubscriptionStoreTag,
  Effect.all([FileSystem.FileSystem, Effect.orDie(subscriptionDirConfig)]).pipe(
    Effect.map(([fs, dir]) => createFileSubscriptionStore({ dir, fs })),
  ),
)
