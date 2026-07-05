import { homedir } from 'node:os'
import { join } from 'node:path'
import type { ChannelName, Message, ThreadName, Timestamp } from '@commy/core/ports'
import { ChannelNameSchema, ThreadNameSchema, TimestampSchema } from '@commy/core/ports'
import type { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import { Config, Data, Effect, HashMap, Option, type ParseResult, Schema } from 'effect'
import type { SessionIdValue } from './session-id.ts'

/**
 * Persistent per-(session, target) "have-seen-up-to" cursor — the write half
 * of precise resume replay. A channel/thread subscription that delivers a
 * `message-posted` advances this cursor so a later resume can narrow its
 * catch-up to only-actually-missed messages, per target.
 *
 * Distinct from `cursor-store.ts` (a per-`IdentityId` single mentions
 * watermark) and from the subscription-intents file: different key, different
 * directory, no collision. One file per session holds a `target → ts` map, so
 * channel/thread names never reach a filename and there is no path-escaping to
 * defend against — only the session id maps onto a path.
 *
 * Writes are monotonic — a ts earlier than the stored value for that target is
 * a no-op, mirroring `cursor-store.ts`, so out-of-order delivery on a single
 * substrate does not walk a cursor backwards. A corrupt prior file is treated
 * as no prior, so a write recovers by overwriting it.
 */

/**
 * The channel-or-thread a delivered message landed in — the key the cursor map
 * advances against. A channel-level delivery carries `thread: none`; a
 * thread/topic delivery carries `some(name)`.
 *
 * A `Data.struct` (not a `` `${channel}|${thread}` `` string key) so equal
 * targets share one `HashMap` slot by value: `Data` supplies structural
 * `Equal`/`Hash` and the `Option` field composes into it, so there is no
 * delimiter that a channel/thread name containing one could collide on.
 */
export type DeliveryTarget = {
  readonly channel: ChannelName
  readonly thread: Option.Option<ThreadName>
}

/**
 * Derive the {@link DeliveryTarget} a message was delivered in from its
 * `MessageRef`: the channel name always, and the thread name when the message
 * carried a topic (`MessageRef.thread` is `none` for top-level messages).
 */
export const deliveryTargetOf = (message: Message): DeliveryTarget =>
  Data.struct({
    channel: message.ref.channel.name,
    thread: Option.map(message.ref.thread, (observed) => observed.name),
  })

/**
 * Persisted key shape. `Schema.Data` re-equips the decoded struct with the
 * same structural `Equal`/`Hash` {@link deliveryTargetOf} builds, so a key read
 * back from disk matches an in-memory target by value. `OptionFromNullOr`
 * renders the absent thread as JSON `null`.
 */
const DeliveryTargetSchema = Schema.Data(
  Schema.Struct({
    channel: ChannelNameSchema,
    thread: Schema.OptionFromNullOr(ThreadNameSchema),
  }),
)

/**
 * File shape: the `target → ts` map persisted as a JSON array of
 * `[target, ts]` pairs (`Schema.HashMap`'s encoding), wrapped by `parseJson`.
 */
const TargetCursorsFileSchema = Schema.parseJson(
  Schema.HashMap({ key: DeliveryTargetSchema, value: TimestampSchema }),
)
const decodeTargetCursorsFile = Schema.decodeUnknown(TargetCursorsFileSchema)
const encodeTargetCursorsFile = Schema.encode(TargetCursorsFileSchema)

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map a session id onto a single safe filename — the same sanitisation
 * cursor-store / subscription-store apply, kept as defence-in-depth so the
 * resulting path is always a direct child of the configured directory.
 */
const cursorsFilename = (id: SessionIdValue): string =>
  `${(id as string).replace(FILENAME_SAFE, '_')}.json`

const isNotFound = (error: PlatformError | ParseResult.ParseError): boolean =>
  error._tag === 'SystemError' && error.reason === 'NotFound'

/**
 * Read and decode the per-session cursor map at `path`. A missing file becomes
 * `Option.none`; a present-but-unparseable file fails with the decode's
 * `ParseError`. Other filesystem failures propagate as `PlatformError`.
 */
const readCursors = (
  fs: FileSystem.FileSystem,
  path: string,
): Effect.Effect<
  Option.Option<HashMap.HashMap<DeliveryTarget, Timestamp>>,
  PlatformError | ParseResult.ParseError
> =>
  fs.readFileString(path).pipe(
    Effect.flatMap(decodeTargetCursorsFile),
    Effect.map(Option.some),
    Effect.catchIf(isNotFound, () =>
      Effect.succeed(Option.none<HashMap.HashMap<DeliveryTarget, Timestamp>>()),
    ),
  )

/**
 * Advance the cursor for `target` to `ts` in the session's cursor file.
 * Monotonic: a ts not newer than the stored value for that target is a no-op.
 * A corrupt prior file is treated as no prior, so the write recovers by
 * overwriting it. Encoding a valid in-memory map cannot fail on already-branded
 * values, so a residual `ParseError` there is a defect (`orDie`).
 */
export const advanceTargetCursor = (
  fs: FileSystem.FileSystem,
  dir: string,
  id: SessionIdValue,
  target: DeliveryTarget,
  ts: Timestamp,
): Effect.Effect<void, PlatformError> => {
  const path = join(dir, cursorsFilename(id))
  return readCursors(fs, path).pipe(
    Effect.catchTag('ParseError', () =>
      Effect.succeed(Option.none<HashMap.HashMap<DeliveryTarget, Timestamp>>()),
    ),
    Effect.map(Option.getOrElse(() => HashMap.empty<DeliveryTarget, Timestamp>())),
    Effect.flatMap((cursors) => {
      const prior = HashMap.get(cursors, target)
      return Option.isSome(prior) && prior.value >= ts
        ? Effect.void
        : encodeTargetCursorsFile(HashMap.set(cursors, target, ts)).pipe(
            Effect.orDie,
            Effect.flatMap((json) =>
              fs
                .makeDirectory(dir, { recursive: true })
                .pipe(Effect.zipRight(fs.writeFileString(path, json))),
            ),
          )
    }),
  )
}

const STATE_SEGMENT = 'commy'

/**
 * The XDG state-home base, read from the ambient ConfigProvider at the boot
 * edge — identical to cursor-store's / subscription-store's. `XDG_STATE_HOME`
 * is the base; an unset or empty value falls back to `<home>/.local/state`.
 */
const stateBaseConfig: Config.Config<string> = Config.nonEmptyString('XDG_STATE_HOME').pipe(
  Config.withDefault(join(homedir(), '.local', 'state')),
)

/**
 * Target-cursor directory under the XDG state home. The plugin owns
 * `<state-home>/commy/target-cursors` and writes only there — separate from
 * `commy/cursors` (mentions watermark) and `commy/subscriptions` (intents).
 */
export const targetCursorDirConfig: Config.Config<string> = stateBaseConfig.pipe(
  Config.map((base) => join(base, STATE_SEGMENT, 'target-cursors')),
)
