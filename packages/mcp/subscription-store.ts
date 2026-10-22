import { homedir } from 'node:os'
import { join } from 'node:path'
import { type BotName, ChannelNameSchema, ThreadNameSchema } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import {
  Array as Arr,
  Config,
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  type ParseResult,
  Schema,
} from 'effect'
import { botNameConfig } from './bootstrap.ts'
import { SessionId } from './session-id.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'

/**
 * Per-seat record of the narrows a subscription row cannot express.
 *
 * This file used to hold the whole narrow set, and its own reasoning said why:
 * the set was in-memory, rebuilt from `COMMY_SUBSCRIBE` every boot, so a
 * resumed session lost every runtime `subscribe`/`unsubscribe`. That premise is
 * gone. Subscriptions are the seat's own realm state now, and a seat rebuilds
 * by ASKING the realm what it is subscribed to (`subscription-restore.ts`).
 *
 * What the realm cannot answer is narrowing below a channel — a subscription
 * row names a channel, so `#chan/topic` and `new-topics:#chan` read back as
 * plain `#chan`. That is all this record still holds. Channel-wide intents are
 * deliberately absent: a second copy of something the realm already holds could
 * only ever disagree with the rows that actually govern delivery.
 *
 * An ephemeral seat's record is keyed on its **session_id**. A pinned
 * `COMMY_BOT_NAME` bot's is keyed on its bot name, because its session id is
 * new every launch while the realm rows the record narrows belong to the bot
 * and outlive the pane.
 *
 * `read` yields `Option.none` only when no file exists for the session, which
 * is what marks a first launch. An empty record returns `Option.some([])`, NOT
 * `none`: a session whose narrows are all channel-wide has still run before,
 * and `seedDefaultsIfFresh` reads exactly that distinction. A file that exists
 * but cannot be parsed surfaces a `ParseError` rather than masquerading as
 * absent.
 *
 * Writes overwrite unconditionally — the latest snapshot is authoritative, so a
 * smaller set (after an unsubscribe) correctly replaces a larger one.
 */
export interface SubscriptionStore {
  read(): Effect.Effect<
    Option.Option<ReadonlyArray<SubscribeIntent>>,
    PlatformError | ParseResult.ParseError
  >
  write(intents: ReadonlyArray<SubscribeIntent>): Effect.Effect<void, PlatformError>
}

/**
 * Context tag for the per-seat subscription store. The boot program
 * reads it from context; the app layer registers {@link FileSubscriptionStoreLive}
 * in production, tests register a file-backed temp-dir store directly.
 */
export class SubscriptionStoreTag extends Context.Tag('SubscriptionStore')<
  SubscriptionStoreTag,
  SubscriptionStore
>() {}

export interface FileSubscriptionStoreDeps {
  /** Directory the per-seat JSON files live under. Created lazily on first write. */
  readonly dir: string
  /**
   * The filesystem every read/write executes against, injected at
   * construction (mirroring cursor-store.ts). {@link FileSubscriptionStoreLive}
   * reads it from context (`NodeContext.layer`, provided once in the app layer).
   */
  readonly fs: FileSystem.FileSystem
  /**
   * Whose record this is: the file's key. Every read/write runs it lazily
   * inside the method, so the key is never threaded through a call site. For an
   * ephemeral seat it awaits the one shared session-id deferred, and awaiting
   * here rather than at build is load-bearing: a build-time await would block
   * the whole layer build until a feeder delivers the id.
   */
  readonly owner: Effect.Effect<string>
}

/**
 * Persisted shape of a single `SubscribeIntent`. A discriminated union on
 * `kind` mirroring `subscribe-parser.ts`; the branded name schemas decode
 * the persisted strings straight back into the in-memory intent type.
 */
const SubscribeIntentSchema = Schema.Union(
  Schema.Struct({ kind: Schema.Literal('channel'), channelName: ChannelNameSchema }),
  Schema.Struct({
    kind: Schema.Literal('thread'),
    channelName: ChannelNameSchema,
    threadName: ThreadNameSchema,
  }),
  Schema.Struct({ kind: Schema.Literal('new-topics-in-channel'), channelName: ChannelNameSchema }),
)

/**
 * The retired `mentions` intent, still decodable so a set persisted before
 * mentions became implicit does not fail its whole restore on one dead entry.
 * It carries no narrow, so {@link decodeSubscriptionsFile} drops it.
 */
const RetiredMentionsSchema = Schema.Struct({ kind: Schema.Literal('mentions') })

const PersistedIntentSchema = Schema.Union(SubscribeIntentSchema, RetiredMentionsSchema)

const isRetired = (
  intent: Schema.Schema.Type<typeof PersistedIntentSchema>,
): intent is Schema.Schema.Type<typeof RetiredMentionsSchema> => intent.kind === 'mentions'

const SubscriptionsFileSchema = Schema.parseJson(Schema.Array(PersistedIntentSchema))

/**
 * Shares the `commy plugin: <component> drop (<reason>) …` shape the event
 * pump's discards use (`event-pump.ts`), so one grep answers "what did this
 * seat silently lose?" across both layers. It names the record by its owner,
 * the session id or the pinned bot's key.
 *
 * Observability only. This is NOT an expiry instrument: the on-disk entry is
 * cleared solely by `persistSubscriptions`, which fires on a runtime
 * subscribe/unsubscribe, so a listen-only seat keeps it forever and
 * re-discards on every resume. Listen-only is precisely the population the
 * retired schema protects, so these lines will never go quiet for the seats
 * that matter — silence here is not permission to delete the shim.
 */
const traceRetiredDiscards = (
  owner: string,
  intents: ReadonlyArray<Schema.Schema.Type<typeof PersistedIntentSchema>>,
): Effect.Effect<void> =>
  Effect.forEach(Arr.filter(intents, isRetired), (retired) =>
    Effect.logInfo(
      `commy plugin: subscription-store drop (retired-intent) kind=${retired.kind} record=${owner}`,
    ),
  ).pipe(Effect.asVoid)

const decodeSubscriptionsFile = (
  owner: string,
  raw: string,
): Effect.Effect<ReadonlyArray<SubscribeIntent>, ParseResult.ParseError> =>
  Schema.decodeUnknown(SubscriptionsFileSchema)(raw).pipe(
    Effect.tap((intents) => traceRetiredDiscards(owner, intents)),
    Effect.map(Arr.filter((intent) => !isRetired(intent))),
  )

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map a record's owner onto a single safe filename. Non-`[a-zA-Z0-9._-]`
 * characters collapse to `_`, the same sanitisation cursor-store applies, so
 * the resulting path is always a direct child of the configured directory.
 */
const subscriptionFilename = (owner: string): string => `${owner.replace(FILENAME_SAFE, '_')}.json`

const isNotFound = (error: PlatformError | ParseResult.ParseError): boolean =>
  error._tag === 'SystemError' && error.reason === 'NotFound'

/**
 * Read and decode the subscription file at `path`. A missing file becomes
 * `Option.none`; a present-but-unparseable file fails with the decode's
 * `ParseError`. Other filesystem failures propagate as `PlatformError`.
 */
const readSubscriptions = (
  fs: FileSystem.FileSystem,
  owner: string,
  path: string,
): Effect.Effect<
  Option.Option<ReadonlyArray<SubscribeIntent>>,
  PlatformError | ParseResult.ParseError
> =>
  fs.readFileString(path).pipe(
    Effect.flatMap((raw) => decodeSubscriptionsFile(owner, raw)),
    Effect.asSome,
    Effect.catchIf(isNotFound, () => Effect.succeed(Option.none<ReadonlyArray<SubscribeIntent>>())),
  )

export const createFileSubscriptionStore = (deps: FileSubscriptionStoreDeps): SubscriptionStore => {
  const { dir, fs, owner } = deps
  const pathFor = (key: string): string => join(dir, subscriptionFilename(key))

  const read: SubscriptionStore['read'] = () =>
    Effect.flatMap(owner, (key) => readSubscriptions(fs, key, pathFor(key)))

  const write: SubscriptionStore['write'] = (intents) =>
    Effect.flatMap(owner, (key) =>
      Schema.encode(SubscriptionsFileSchema)(intents).pipe(
        Effect.orDie,
        Effect.tap(() => fs.makeDirectory(dir, { recursive: true })),
        Effect.flatMap((raw) => fs.writeFileString(pathFor(key), raw)),
      ),
    )

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
 * state home. Reads `FileSystem`, the subscription directory (via
 * {@link subscriptionDirConfig} against the boot-edge ConfigProvider) and the
 * seat's bot name from context and injects them into the store at
 * construction.
 *
 * Both dir configs always yield a value (`withDefault` covers the unset/empty
 * case), so a residual `ConfigError` here means the config source itself is
 * unavailable — an unrecoverable boot fault, defected with `orDie` to keep the
 * layer's error channel `never`. An invalid bot name is `parseEnv`'s to
 * report, and it refuses boot, so here it reads as no bot name.
 */
export const FileSubscriptionStoreLive: Layer.Layer<
  SubscriptionStoreTag,
  never,
  FileSystem.FileSystem | SessionId
> = Layer.effect(
  SubscriptionStoreTag,
  Effect.all([
    FileSystem.FileSystem,
    Effect.orDie(subscriptionDirConfig),
    SessionId,
    Effect.orElseSucceed(botNameConfig, () => Option.none<BotName>()),
  ]).pipe(
    Effect.map(([fs, dir, session, botName]) =>
      createFileSubscriptionStore({
        dir,
        fs,
        owner: Option.match(botName, {
          onNone: () => Deferred.await(session),
          onSome: (name) => Effect.succeed(`bot-${name}`),
        }),
      }),
    ),
  ),
)
