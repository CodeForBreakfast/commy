import type { EventQueueCursor } from '@commy/core/ports'
import type { CommandExecutor, FileSystem } from '@effect/platform'
import { NodeContext } from '@effect/platform-node'
import { ConfigProvider, Effect, Layer, Option } from 'effect'
import { type QueueStateStore, QueueStateStoreTag } from './queue-state-store.ts'
import { createInMemorySeedLedger, SeedLedgerTag } from './seed-ledger.ts'
import type { SessionIdValue } from './session-id.ts'

/**
 * Fixture config source for the boot tests. `parseEnv` reads the ambient
 * ConfigProvider; production's `PlatformLive` sets it from
 * `ConfigProvider.fromEnv()`, the tests set it from a fixture env map —
 * overriding at the dependency boundary rather than mutating
 * `process.env`. `ConfigProvider.fromMap` wants a `Map<string, string>`,
 * so the `Record` fixture (which carries `undefined` slots) is narrowed
 * here.
 */
export const testConfigProviderLayer = (
  env: Record<string, string | undefined>,
): Layer.Layer<never> => {
  const map = new Map<string, string>()
  for (const [key, value] of Object.entries(env)) {
    if (value !== undefined) map.set(key, value)
  }
  return Layer.setConfigProvider(ConfigProvider.fromMap(map))
}

/**
 * Fixture platform bundle for the boot tests: the fixture config source
 * plus the node platform context. The substituted-adapter test programs read
 * `FileSystem` from context (the cursor store) and the command executor (the
 * per-call project probe), plus the ConfigProvider at build; they never reach
 * the network, so no `HttpClient` leaf is needed. This is the test-side mirror
 * of production's `PlatformLive` — provision at the dependency boundary over
 * the same app composition.
 */
export const testPlatformLayer = (
  env: Record<string, string | undefined>,
): Layer.Layer<FileSystem.FileSystem | CommandExecutor.CommandExecutor> =>
  Layer.merge(testConfigProviderLayer(env), NodeContext.layer)

/**
 * In-memory queue-state store for the boot tests — keeps the runner's homedir
 * untouched, the same reason the cursor store is faked there. Boot reads it to
 * answer "is there anything to resume?"; a harness that never persists a queue
 * always answers no, which is the fresh-session case.
 */
export const createInMemoryQueueStateStore = (): QueueStateStore => {
  const states = new Map<string, EventQueueCursor>()
  return {
    read: (sessionId: SessionIdValue) =>
      Effect.sync(() => Option.fromNullable(states.get(sessionId as string))),
    write: (sessionId: SessionIdValue, state: EventQueueCursor) =>
      Effect.sync(() => {
        states.set(sessionId as string, state)
      }),
    advance: (sessionId: SessionIdValue, lastEventId: number) =>
      Effect.sync(() => {
        const prior = states.get(sessionId as string)
        if (prior !== undefined) states.set(sessionId as string, { ...prior, lastEventId })
      }),
  }
}

/**
 * The two boot-time stores every substituted-adapter harness needs and none of
 * them cares about: the queue-state store boot reads for the resume verdict,
 * and the seed ledger that makes `COMMY_SUBSCRIBE` a once-per-bot bootstrap.
 * Both in-memory, so a test run leaves nothing behind. A test that cares about
 * either — the seeding-is-once tests do — provides its own instead.
 *
 * A FUNCTION, not a shared constant: the ledger is keyed by bot name and most
 * boot tests boot the same one, so a module-level instance would let the first
 * test's seeding suppress every later test's.
 */
export const testBootStoresLayer = (): Layer.Layer<QueueStateStoreTag | SeedLedgerTag> =>
  Layer.mergeAll(
    Layer.succeed(QueueStateStoreTag, createInMemoryQueueStateStore()),
    Layer.succeed(SeedLedgerTag, createInMemorySeedLedger()),
  )
