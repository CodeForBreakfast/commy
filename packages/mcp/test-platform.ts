import type { FileSystem } from '@effect/platform'
import { BunFileSystem } from '@effect/platform-bun'
import { ConfigProvider, Layer } from 'effect'

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
 * plus the real file system. The substituted-adapter test programs read
 * `FileSystem` from context (the cursor store) and the ConfigProvider at
 * build; they never reach the network, so no `HttpClient` leaf is needed.
 * This is the test-side mirror of production's `PlatformLive` — provision
 * at the dependency boundary over the same app composition.
 */
export const testPlatformLayer = (
  env: Record<string, string | undefined>,
): Layer.Layer<FileSystem.FileSystem> =>
  Layer.merge(testConfigProviderLayer(env), BunFileSystem.layer)
