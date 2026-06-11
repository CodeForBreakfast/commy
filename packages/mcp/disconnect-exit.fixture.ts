/**
 * Subprocess fixture for `disconnect-exit.test.ts` (comms-hfhm). A
 * realm-free mirror of {@link main}: it boots the real `makeProgram` with
 * the real {@link clientDisconnect} over the real `process.stdin`, but on a
 * network-free in-memory substrate so the test never touches the Zulip
 * realm. The memory adapter's `events()` parks (an `asyncPush` stream that
 * never ends), exactly like production's forever-long-polling Zulip pump —
 * so the program blocks on the `raceFirst(pump.done, shutdownSignal)` and
 * ONLY a stdin disconnect can unwind it. The test spawns this directly
 * (claude's-pipe topology), closes stdin, and asserts the process exits.
 *
 * `process.stdin` is wrapped so the fixture writes `FIXTURE_ARMED` to
 * stderr at the moment `clientDisconnect` has attached BOTH its listeners
 * — i.e. once the disconnect race is live. The test waits for that line
 * before closing stdin, so the close can't land in the boot window before
 * the one-shot `end`/`close` listeners exist (which would be a false
 * reproduction of the very race under test).
 */
import { stderrLoggerLayer } from '@codeforbreakfast/core/logging'
import { type MemoryAdapter, memoryAdapter } from '@codeforbreakfast/memory/adapter'
import type { ZulipAdapter } from '@codeforbreakfast/zulip/adapter'
import { FetchHttpClient } from '@effect/platform'
import { BunFileSystem, BunRuntime } from '@effect/platform-bun'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { ConfigProvider, Effect, Layer, Option } from 'effect'
import { substrateAdapterLayer } from './bootstrap.ts'
import { CursorStoreTag } from './cursor-store.ts'
import { clientDisconnect, makeProgram } from './server.ts'

const inMemoryCursorStore = {
  read: () => Effect.succeed(Option.none()),
  write: () => Effect.void,
}

/**
 * The in-memory substrate is a `MemoryAdapter`; complete it to the
 * `ZulipAdapter` shape the program expects. `reconcileMinterSubscriptions`
 * (boot) and `close` (shutdown finalizer) are exercised, so they are real
 * no-ops; `uploadFile`/`downloadFile` are never reached without an MCP
 * client driving tools.
 */
const asZulipAdapter = (adapter: Effect.Effect<MemoryAdapter>): Effect.Effect<ZulipAdapter> =>
  Effect.map(
    adapter,
    (base): ZulipAdapter => ({
      ...base,
      reconcileMinterSubscriptions: () => Effect.succeed({ added: [], error: undefined }),
      uploadFile: () => Effect.die(new Error('disconnect-exit fixture: uploadFile unused')),
      downloadFile: () => Effect.die(new Error('disconnect-exit fixture: downloadFile unused')),
      close: async () => {},
    }),
  )

let attached = 0
const armedStdin = {
  once: (event: string, listener: () => void): unknown => {
    const result = process.stdin.once(event, listener)
    attached += 1
    if (attached === 2) process.stderr.write('FIXTURE_ARMED\n')
    return result
  },
  removeListener: (event: string, listener: () => void): unknown =>
    process.stdin.removeListener(event, listener),
}

BunRuntime.runMain(
  makeProgram({
    transport: new StdioServerTransport(),
    shutdownSignal: clientDisconnect(armedStdin),
  }).pipe(
    Effect.provide(
      Layer.provideMerge(
        Layer.mergeAll(
          substrateAdapterLayer(asZulipAdapter(memoryAdapter())),
          Layer.succeed(CursorStoreTag, inMemoryCursorStore),
          stderrLoggerLayer,
        ),
        Layer.mergeAll(
          Layer.setConfigProvider(ConfigProvider.fromEnv()),
          FetchHttpClient.layer,
          BunFileSystem.layer,
        ),
      ),
    ),
  ),
  { disablePrettyLogger: true },
)
