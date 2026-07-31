import type { EventQueueCursor } from '@commy/core/ports'
import type { CommandExecutor, FileSystem } from '@effect/platform'
import { NodeContext } from '@effect/platform-node'
import { ConfigProvider, Effect, Layer, Option } from 'effect'
import { type QueueStateStore, QueueStateStoreTag } from './queue-state-store.ts'
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
 * The boot-time store every substituted-adapter harness needs and none of them
 * cares about: the queue-state store boot reads to answer "is there anything to
 * resume?". A FUNCTION, not a shared constant, so one harness's writes cannot
 * reach another's boot.
 */
export const testBootStoresLayer = (): Layer.Layer<QueueStateStoreTag> =>
  Layer.succeed(QueueStateStoreTag, createInMemoryQueueStateStore())
