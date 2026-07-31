import { homedir } from 'node:os'
import { join } from 'node:path'
import type { DisplayName } from '@commy/core/ports'
import { FileSystem } from '@effect/platform'
import type { PlatformError } from '@effect/platform/Error'
import { Config, Context, Effect, Layer } from 'effect'

/**
 * Record of which bots this installation has already applied
 * `COMMY_SUBSCRIBE` to.
 *
 * `COMMY_SUBSCRIBE` bootstraps a bot's subscriptions exactly once and is then
 * inert — the bot owns its subscriptions from that point, and they live in the
 * realm under its own principal. The obvious "once" signal is the mint, and for
 * a bot minted after this change that is the whole story.
 *
 * This ledger exists for the population that has no mint left to observe: every
 * pinned `COMMY_BOT_NAME` bot in an existing fleet is ALREADY minted when this
 * change lands. Seeding only at mint would never fire for any of them, and they
 * would come up with no subscriptions at all — a fleet-wide silent deafening
 * that passes every test, because tests mint fresh bots. The ledger gives that
 * population exactly one seeding and then gets out of the way.
 *
 * WHAT THIS IS NOT. It is not subscription state and it is not a cache of one.
 * It records what this installation has DONE, never what a seat is subscribed
 * to; nothing reads it to answer "what am I subscribed to?". That question has
 * one authority, the realm. Keeping the two apart is the difference between
 * migration bookkeeping and the client-side subscription authority this effort
 * deleted.
 *
 * A LIMITATION THAT CANNOT BE ENGINEERED AWAY. For a bot that goes into the
 * upgrade with no subscriptions, "never seeded" and "deliberately unsubscribed
 * from everything" have the same footprint — nothing recorded the difference at
 * the time, and it cannot be recovered afterwards. Such a bot is seeded once.
 * From then on the ledger records the difference and the ambiguity is gone for
 * good.
 */
export interface SeedLedger {
  /** Whether `COMMY_SUBSCRIBE` has already been applied to this bot. */
  hasSeeded(botName: DisplayName): Effect.Effect<boolean, PlatformError>
  /** Record that it has been. Idempotent. */
  recordSeeded(botName: DisplayName): Effect.Effect<void, PlatformError>
}

/**
 * Context tag for the seed ledger. The boot program reads it from context; the
 * app layer registers {@link FileSeedLedgerLive} in production, tests register
 * an in-memory layer.
 */
export class SeedLedgerTag extends Context.Tag('SeedLedger')<SeedLedgerTag, SeedLedger>() {}

export interface FileSeedLedgerDeps {
  /** Directory the per-bot marker files live under. Created lazily on first write. */
  readonly dir: string
  /**
   * The filesystem every read/write executes against, injected at construction.
   * {@link FileSeedLedgerLive} reads it from context (`NodeContext.layer`,
   * provided once in the app layer).
   */
  readonly fs: FileSystem.FileSystem
}

const FILENAME_SAFE = /[^a-zA-Z0-9._-]/g

/**
 * Map a bot name onto a single safe filename, the same way the cursor and
 * queue-state stores map their keys: alphanumerics / `-` / `_` / `.` survive
 * verbatim and everything else collapses to `_`, so a name carrying path
 * separators still resolves to a direct child of the configured directory.
 */
const markerFilename = (botName: DisplayName): string =>
  `${(botName as string).replace(FILENAME_SAFE, '_')}.json`

/**
 * In-memory seed ledger for tests and for any host with no writable state
 * directory. A process-lifetime ledger still makes the seeding idempotent
 * WITHIN a boot; it just cannot carry the answer across one.
 */
export const createInMemorySeedLedger = (): SeedLedger => {
  const seeded = new Set<string>()
  return {
    hasSeeded: (botName) => Effect.sync(() => seeded.has(botName as string)),
    recordSeeded: (botName) =>
      Effect.sync(() => {
        seeded.add(botName as string)
      }),
  }
}

export const createFileSeedLedger = (deps: FileSeedLedgerDeps): SeedLedger => {
  const { dir, fs } = deps
  const pathFor = (botName: DisplayName): string => join(dir, markerFilename(botName))
  return {
    // Presence IS the record, so there is nothing to decode and no corrupt-file
    // case to handle: a marker that exists but is unreadable still answers the
    // only question asked of it.
    hasSeeded: (botName) => fs.exists(pathFor(botName)),
    recordSeeded: (botName) =>
      fs
        .makeDirectory(dir, { recursive: true })
        .pipe(
          Effect.zipRight(
            fs.writeFileString(pathFor(botName), JSON.stringify({ seededFrom: 'COMMY_SUBSCRIBE' })),
          ),
        ),
  }
}

const STATE_SEGMENT = 'commy'

/**
 * The XDG state-home base, read from the ambient ConfigProvider at the boot
 * edge — same convention as the cursor and queue-state stores.
 */
const stateBaseConfig: Config.Config<string> = Config.nonEmptyString('XDG_STATE_HOME').pipe(
  Config.withDefault(join(homedir(), '.local', 'state')),
)

/** Marker directory under the XDG state home. */
export const seedLedgerDirConfig: Config.Config<string> = stateBaseConfig.pipe(
  Config.map((base) => join(base, STATE_SEGMENT, 'seeded')),
)

/**
 * Production seed-ledger layer: file-backed under the XDG state home.
 *
 * `seedLedgerDirConfig` always yields a value (`withDefault` covers the
 * unset/empty `XDG_STATE_HOME` case), so a residual `ConfigError` here means
 * the config source itself is unavailable — an unrecoverable boot fault,
 * defected with `orDie` to keep the layer's error channel `never`.
 */
export const FileSeedLedgerLive: Layer.Layer<SeedLedgerTag, never, FileSystem.FileSystem> =
  Layer.effect(
    SeedLedgerTag,
    Effect.all([FileSystem.FileSystem, Effect.orDie(seedLedgerDirConfig)]).pipe(
      Effect.map(([fs, dir]) => createFileSeedLedger({ dir, fs })),
    ),
  )
