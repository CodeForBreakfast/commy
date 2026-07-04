import { expect, test } from 'bun:test'
import { EventEmitter } from 'node:events'
import { captureLogger, stderrLoggerLayer } from '@commy/core/logging'
import type {
  AcquiredIdentity,
  Directory,
  HistoryReader,
  Identity,
  IdentityId as IdentityIdType,
  IdentityPort,
  MessageInbox,
  MessagePublisher,
  Presence,
  Range,
  SubscriptionTarget,
  Timestamp as TimestampType,
} from '@commy/core/ports'
import {
  type ChannelName,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeThreadNameSync,
  InboxError,
} from '@commy/core/ports'
import { memoryAdapter } from '@commy/memory/adapter'
import {
  Cause,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Scope,
  Stream,
  TestClock,
  TestContext,
} from 'effect'
import type { SessionId } from './bootstrap.ts'
import { EnvConfigError, NotInRepo, parseEnv, substrateAdapterLayer } from './bootstrap.ts'
import type { CursorStore } from './cursor-store.ts'
import { CursorStoreTag } from './cursor-store.ts'
import type { IdentityCache } from './identity-cache.ts'
// Above-port unit tests drive the substrate through hand-rolled port fakes and
// the in-memory adapter only — never the real Zulip adapter (see
// docs/architecture.md § Test architecture). `completeAsSubstrate` is the single
// seam that completes either to the Zulip-shaped `SubstrateAdapter` port, and it
// re-exports the `ZulipAdapter` type so this file names no `@commy/zulip` module
// directly.
import { completeAsSubstrate, type ZulipAdapter } from './memory-substrate.ts'
import { clientDisconnect, forkIdleSweep, makeProgram, type ProgramParams } from './server.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'
import { SubscriptionStoreTag } from './subscription-store.ts'
import { testPlatformLayer } from './test-platform.ts'

/**
 * In-memory cursor store for the boot tests — keeps the runner's homedir
 * untouched (the file-backed store writes to `<XDG_STATE_HOME>`).
 */
const inMemoryCursorStore = (): CursorStore => {
  const store = new Map<string, TimestampType>()
  return {
    read: (id: IdentityIdType) => Effect.sync(() => Option.fromNullable(store.get(id as string))),
    write: (id: IdentityIdType, ts: TimestampType) =>
      Effect.sync(() => {
        const prior = store.get(id as string)
        if (prior !== undefined && prior >= ts) return
        store.set(id as string, ts)
      }),
  }
}

/**
 * In-memory subscription store for the boot tests — keeps the runner's
 * homedir untouched, the same reason the cursor store is faked here.
 */
const inMemorySubscriptionStore = (): SubscriptionStore => {
  const store = new Map<string, ReadonlyArray<SubscribeIntent>>()
  return {
    read: (id: SessionId) => Effect.sync(() => Option.fromNullable(store.get(id as string))),
    write: (id: SessionId, intents: ReadonlyArray<SubscribeIntent>) =>
      Effect.sync(() => {
        store.set(id as string, intents)
      }),
  }
}

/**
 * Run the boot program with a substituted adapter. The
 * adapter layer is parse-gated like the production `ZulipAdapterLive`, so
 * an invalid env fails the layer build — the adapter is never acquired
 * and `close()` never fires — mirroring production exactly. The logger is
 * provided both at the program edge and to `onAcquire` (cache edge).
 */
const runProgram = (
  env: Record<string, string | undefined>,
  adapter: ZulipAdapter,
  params: ProgramParams = {},
) => {
  const loggerLayer = params.loggerLayer ?? stderrLoggerLayer
  return Effect.runPromiseExit(
    makeProgram({ ...params, loggerLayer }).pipe(
      Effect.provide(
        Layer.provideMerge(
          Layer.mergeAll(
            substrateAdapterLayer(parseEnv.pipe(Effect.as(adapter))),
            Layer.succeed(CursorStoreTag, inMemoryCursorStore()),
            Layer.succeed(SubscriptionStoreTag, inMemorySubscriptionStore()),
            loggerLayer,
          ),
          testPlatformLayer(env),
        ),
      ),
    ),
  )
}

/** Squash a failure Exit to its underlying error for instanceof / message assertions. */
const failureValue = (exit: Exit.Exit<void, unknown>): unknown =>
  Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined

const validEnv = {
  ZULIP_SITE: 'https://zulip.example.com',
  ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
  ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
  COMMY_BOT_NAME: 'myproject-concierge',
} as const

interface FakeAdapterCalls {
  readonly acquired: string[]
  readonly closes: { count: number }
  readonly subscribed: SubscriptionTarget[]
  readonly reconcileCalls: { count: number }
  readonly events: string[]
}

const buildFakeAdapter = (
  options: {
    readonly acquireError?: unknown
    readonly reconcileReport?: {
      readonly added: ReadonlyArray<ChannelName>
      readonly error: string | undefined
    }
  } = {},
): { readonly adapter: ZulipAdapter; readonly calls: FakeAdapterCalls } => {
  const acquired: string[] = []
  const closes = { count: 0 }
  const subscribed: SubscriptionTarget[] = []
  const reconcileCalls = { count: 0 }
  const events: string[] = []
  const identity: Identity = {
    id: decodeIdentityIdSync('bot:myproject-concierge'),
    name: decodeDisplayNameSync('myproject-concierge'),
    kind: 'agent',
  }
  const acquiredIdentity: AcquiredIdentity = {
    credentials: { apiKey: 'fresh-key' },
    identity,
  }
  const identityPort: IdentityPort = {
    currentIdentity: () => Effect.succeed(identity),
    acquire: (name) =>
      Effect.suspend(() => {
        events.push(`acquire(${name})`)
        acquired.push(name)
        if (options.acquireError !== undefined) return Effect.die(options.acquireError)
        return Effect.succeed(acquiredIdentity)
      }),
    release: () => Effect.void,
    resolve: () => Effect.succeed(Option.none()),
  }
  const publisher: MessagePublisher = {
    post: () => Effect.die(new Error('unused fake')),
    edit: () => Effect.void,
    react: () => Effect.void,
    unreact: () => Effect.void,
  }
  const inbox: MessageInbox = {
    subscribe: (target: SubscriptionTarget) =>
      Effect.sync(() => {
        events.push('subscribe')
        subscribed.push(target)
      }),
    unsubscribe: (_target: SubscriptionTarget) => Effect.void,
    events: () => Stream.empty,
    replay: (_since: TimestampType) => Effect.succeed([]),
  }
  const history: HistoryReader = {
    readChannel: (_channel: ChannelName, _range: Range) => Effect.succeed([]),
    readThread: (_channel: ChannelName, _threadName, _range?: Range) => Effect.succeed([]),
    recentThreads: () => Effect.succeed([]),
    messagePermalink: () => Effect.succeed(Option.none()),
  }
  const directory: Directory = {
    listAgents: () => Effect.succeed([]),
    listHumans: () => Effect.succeed([]),
    listChannels: () => Effect.succeed([]),
    presence: (_id: Identity): Effect.Effect<Presence> => Effect.succeed('offline'),
  }
  const defaultReconcileReport = {
    added: [] as ReadonlyArray<ChannelName>,
    error: undefined as string | undefined,
  }
  const adapter = completeAsSubstrate(
    { identity: identityPort, publisher, inbox, history, directory },
    {
      reconcileMinterSubscriptions: () =>
        Effect.sync(() => {
          events.push('reconcile')
          reconcileCalls.count += 1
          return options.reconcileReport ?? defaultReconcileReport
        }),
      close: async () => {
        closes.count += 1
      },
    },
  )
  return { adapter, calls: { acquired, closes, subscribed, reconcileCalls, events } }
}

test('main resolves cleanly when given a valid env', async () => {
  const fake = buildFakeAdapter()
  const exit = await runProgram(validEnv, fake.adapter)
  expect(Exit.isSuccess(exit)).toBe(true)
  expect(fake.calls.acquired).toEqual(['myproject-concierge'])
  expect(fake.calls.closes.count).toBe(1)
})

test('main propagates EnvConfigError when env is invalid', async () => {
  const fake = buildFakeAdapter()
  // Invalid env fails the parse-gated adapter layer build, so the adapter
  // is never acquired and `close()` never fires — as in production where
  // ZulipAdapterLive can't construct without valid config.
  const exit = await runProgram({}, fake.adapter)
  expect(failureValue(exit)).toBeInstanceOf(EnvConfigError)
  expect(fake.calls.acquired).toEqual([])
  expect(fake.calls.closes.count).toBe(0)
})

test('main writes acquire failure to stderr in the canonical format, fails boot, still closes adapter', async () => {
  const fake = buildFakeAdapter({ acquireError: new Error('substrate rejected acquire') })
  const stderr: string[] = []
  // Exit-code-1 is runMain's defaultTeardown (framework behaviour, not
  // re-tested); the boot Effect failing is what the test pins.
  const exit = await runProgram(validEnv, fake.adapter, { loggerLayer: captureLogger(stderr) })
  expect(stderr).toEqual([
    'commy plugin: acquire("myproject-concierge") failed: substrate rejected acquire',
  ])
  expect(Exit.isFailure(exit)).toBe(true)
  expect(fake.calls.acquired).toEqual(['myproject-concierge'])
  expect(fake.calls.closes.count).toBe(1)
})

test('lazy mode (cc-<8> from session id) does NOT acquire at boot', async () => {
  // when COMMY_BOT_NAME is unset and we fall back to
  // cc-<first-8-of-session-id>, the plugin boots without minting a
  // bot. Acquire is deferred to the first attribution-producing
  // tool call. Even an acquireError-rigged adapter must not see a
  // call during boot.
  const fake = buildFakeAdapter({ acquireError: new Error('would have rejected') })
  const stderr: string[] = []
  const env = {
    ZULIP_SITE: 'https://zulip.example.com',
    ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
    ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
    CLAUDE_CODE_SESSION_ID: 'abcdef12-3456-4789-89ab-cdef01234567',
  }
  const exit = await runProgram(env, fake.adapter, { loggerLayer: captureLogger(stderr) })
  // No acquire call, no acquire-failure stderr, clean boot, adapter still
  // closed. Reconcile is silent in the no-op case.
  expect(Exit.isSuccess(exit)).toBe(true)
  expect(fake.calls.acquired).toEqual([])
  expect(stderr).toEqual([])
  expect(fake.calls.closes.count).toBe(1)
})

test('lazy mode still applies COMMY_SUBSCRIBE at boot (pre-acquire subscriptions)', async () => {
  const fake = buildFakeAdapter()
  const env = {
    ZULIP_SITE: 'https://zulip.example.com',
    ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
    ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
    CLAUDE_CODE_SESSION_ID: 'abcdef12-3456-4789-89ab-cdef01234567',
    COMMY_SUBSCRIBE: 'channel:home,mentions',
  }
  await runProgram(env, fake.adapter)
  expect(fake.calls.acquired).toEqual([])
  expect(fake.calls.subscribed).toEqual([decodeChannelNameSync('home'), 'mentions'])
  expect(fake.calls.closes.count).toBe(1)
})

test('main acquire failure stringifies non-Error rejections', async () => {
  const fake = buildFakeAdapter({ acquireError: 'plain string rejection' })
  const stderr: string[] = []
  const exit = await runProgram(validEnv, fake.adapter, { loggerLayer: captureLogger(stderr) })
  expect(stderr).toEqual([
    'commy plugin: acquire("myproject-concierge") failed: plain string rejection',
  ])
  expect(Exit.isFailure(exit)).toBe(true)
})

test('main does not subscribe when acquire fails', async () => {
  const fake = buildFakeAdapter({ acquireError: new Error('boom') })
  const env = {
    ...validEnv,
    COMMY_SUBSCRIBE: 'channel:home',
  }
  await runProgram(env, fake.adapter, { loggerLayer: captureLogger([]) })
  expect(fake.calls.subscribed).toEqual([])
})

test('main subscribes only the Type-1 mentions default when COMMY_SUBSCRIBE is unset and no project is set', async () => {
  const fake = buildFakeAdapter()
  await runProgram(validEnv, fake.adapter, { readGitContext: () => Effect.succeed(NotInRepo()) })
  expect(fake.calls.subscribed).toEqual(['mentions'])
})

test('main drives a real memory adapter through acquire + env subscribe + close', async () => {
  const adapter = await Effect.runPromise(memoryAdapter())
  const subscribed: SubscriptionTarget[] = []
  const realSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
  const spy: typeof adapter.inbox.subscribe = (target) =>
    Effect.sync(() => {
      subscribed.push(target)
    }).pipe(Effect.flatMap(() => realSubscribe(target)))
  let closes = 0
  const oneShotEvents: MessageInbox['events'] = () => Stream.empty
  const memoryAdapterAsZulipShape = completeAsSubstrate(
    { ...adapter, inbox: { ...adapter.inbox, subscribe: spy, events: oneShotEvents } },
    {
      close: async () => {
        closes += 1
      },
    },
  )
  const env = {
    ...validEnv,
    COMMY_SUBSCRIBE: 'channel:home,thread:home/payments,mentions',
  }
  const logs: string[] = []
  // The memory adapter's history is id-strict; the synthetic refs the
  // boot-time channels catch-up builds from token names
  // don't resolve. Capture the log so the test's stderr stays pristine.
  // The catch-up failure is non-fatal, and this test asserts only the
  // subscribe wiring — catch-up integration is covered by the
  // integration harness via its name-lenient history wrapper.
  await runProgram(env, memoryAdapterAsZulipShape, {
    loggerLayer: captureLogger(logs),
    readGitContext: () => Effect.succeed(NotInRepo()),
  })
  // Type-1 default `mentions` lands first (post-acquire); the env tokens follow.
  expect(subscribed).toEqual([
    'mentions',
    decodeChannelNameSync('home'),
    {
      channel: decodeChannelNameSync('home'),
      thread: decodeThreadNameSync('payments'),
    },
    'mentions',
  ])
  expect(closes).toBe(1)
})

test('main aborts non-zero when COMMY_SUBSCRIBE contains a malformed token', async () => {
  const fake = buildFakeAdapter()
  const env = {
    ...validEnv,
    COMMY_SUBSCRIBE: 'channel:home,not-a-thing,channel:llm-feed',
  }
  const exit = await runProgram(env, fake.adapter, {
    readGitContext: () => Effect.succeed(NotInRepo()),
  })
  expect(Exit.isFailure(exit)).toBe(true)
  expect(String((failureValue(exit) as { message?: string }).message)).toMatch(
    /invalid subscribe token/,
  )
  // Type-1 default (`mentions`) lands first, then the env channel sub,
  // then the parser aborts on the malformed token before reaching `channel:llm-feed`.
  expect(fake.calls.subscribed).toEqual(['mentions', decodeChannelNameSync('home')])
  expect(fake.calls.closes.count).toBe(1)
})

test('main applies env-driven subscriptions in order after acquire and Type-1 defaults', async () => {
  const fake = buildFakeAdapter()
  const env = {
    ...validEnv,
    COMMY_SUBSCRIBE: 'channel:home,thread:home/payments,mentions',
  }
  await runProgram(env, fake.adapter, { readGitContext: () => Effect.succeed(NotInRepo()) })
  expect(fake.calls.acquired).toEqual(['myproject-concierge'])
  // Type-1 default `mentions` (no project) comes first; env tokens follow.
  expect(fake.calls.subscribed).toEqual([
    'mentions',
    decodeChannelNameSync('home'),
    {
      channel: decodeChannelNameSync('home'),
      thread: decodeThreadNameSync('payments'),
    },
    'mentions',
  ])
  expect(fake.calls.closes.count).toBe(1)
})

test('main reconciles minter subscriptions during boot before env subscribes', async () => {
  const fake = buildFakeAdapter({
    reconcileReport: {
      added: [decodeChannelNameSync('commy'), decodeChannelNameSync('general')],
      error: undefined,
    },
  })
  const log: string[] = []
  const env = { ...validEnv, COMMY_SUBSCRIBE: 'channel:home' }
  await runProgram(env, fake.adapter, { loggerLayer: captureLogger(log) })
  expect(fake.calls.reconcileCalls.count).toBe(1)
  expect(fake.calls.events.indexOf('reconcile')).toBeLessThan(
    fake.calls.events.indexOf('subscribe'),
  )
  expect(log.some((line) => line.includes('commy') && line.includes('general'))).toBe(true)
})

test('main calls reconcile but stays silent when there is nothing to add', async () => {
  const fake = buildFakeAdapter()
  const log: string[] = []
  await runProgram(validEnv, fake.adapter, { loggerLayer: captureLogger(log) })
  expect(fake.calls.reconcileCalls.count).toBe(1)
  expect(log).toEqual([])
})

test('main keeps booting when reconcile reports an error (log + continue)', async () => {
  const fake = buildFakeAdapter({
    reconcileReport: { added: [], error: 'realm unreachable' },
  })
  const log: string[] = []
  const exit = await runProgram(validEnv, fake.adapter, { loggerLayer: captureLogger(log) })
  expect(Exit.isSuccess(exit)).toBe(true)
  expect(fake.calls.acquired).toEqual(['myproject-concierge'])
  expect(log.some((line) => line.includes('realm unreachable'))).toBe(true)
})

// ─── Type-1 default sub set for project concierges ──────────────

test('persistent mode + project registers Type-1 defaults (mentions + new-topics + thread/general)', async () => {
  const fake = buildFakeAdapter()
  const env = { ...validEnv, COMMY_PROJECT: 'foo' }
  await runProgram(env, fake.adapter)
  expect(fake.calls.acquired).toEqual(['myproject-concierge'])
  expect(fake.calls.subscribed).toEqual([
    'mentions',
    {
      kind: 'new-topics-in-channel',
      channel: decodeChannelNameSync('foo'),
    },
    {
      channel: decodeChannelNameSync('foo'),
      thread: decodeThreadNameSync('general'),
    },
  ])
})

test('persistent mode without project registers only the universal mentions narrow', async () => {
  const fake = buildFakeAdapter()
  // Inject readGitContext so the worktree's own git context can't
  // accidentally resolve a project slug for the boot-time defaults.
  await runProgram(validEnv, fake.adapter, { readGitContext: () => Effect.succeed(NotInRepo()) })
  expect(fake.calls.subscribed).toEqual(['mentions'])
})

test('Type-1 defaults register after acquire and before COMMY_SUBSCRIBE entries', async () => {
  const fake = buildFakeAdapter()
  const env = {
    ...validEnv,
    COMMY_PROJECT: 'foo',
    COMMY_SUBSCRIBE: 'channel:home',
  }
  await runProgram(env, fake.adapter)
  expect(fake.calls.events.indexOf('acquire(myproject-concierge)')).toBeLessThan(
    fake.calls.events.indexOf('subscribe'),
  )
  expect(fake.calls.subscribed).toEqual([
    'mentions',
    {
      kind: 'new-topics-in-channel',
      channel: decodeChannelNameSync('foo'),
    },
    {
      channel: decodeChannelNameSync('foo'),
      thread: decodeThreadNameSync('general'),
    },
    decodeChannelNameSync('home'),
  ])
})

test('Type-1 defaults do not register when acquire fails', async () => {
  const fake = buildFakeAdapter({ acquireError: new Error('boom') })
  const env = { ...validEnv, COMMY_PROJECT: 'foo' }
  await runProgram(env, fake.adapter, { loggerLayer: captureLogger([]) })
  expect(fake.calls.subscribed).toEqual([])
})

test('Type-1 defaults are skipped in ephemeral mode (Type-2 onAcquire owns that path)', async () => {
  // Ephemeral mode: COMMY_BOT_NAME unset, no acquire at boot, so
  // no Type-1 defaults either. COMMY_PROJECT is irrelevant here —
  // even if set, the boot-time path skips Type-1 wholesale.
  const fake = buildFakeAdapter()
  const env = {
    ZULIP_SITE: 'https://zulip.example.com',
    ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
    ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
    COMMY_PROJECT: 'foo',
    CLAUDE_CODE_SESSION_ID: 'abcdef12-3456-4789-89ab-cdef01234567',
  }
  await runProgram(env, fake.adapter)
  expect(fake.calls.acquired).toEqual([])
  expect(fake.calls.subscribed).toEqual([])
})

test('Type-1 default failure is logged + continues — does not crash boot', async () => {
  // Substrate-side subscribe rejects; we should log and proceed so a
  // transient Zulip hiccup never refuses concierge boot. The bot is
  // already minted at this point — refusing service over a missing
  // default would be worse than the missing default.
  const fake = buildFakeAdapter()
  let calls = 0
  const failingInbox: MessageInbox = {
    ...fake.adapter.inbox,
    subscribe: (_target: SubscriptionTarget) =>
      Effect.suspend(() => {
        calls += 1
        if (calls === 1)
          return Effect.fail(
            new InboxError({
              operation: 'subscribe',
              cause: new Error('substrate transient'),
            }),
          )
        return Effect.void
      }),
  }
  const failingAdapter: ZulipAdapter = { ...fake.adapter, inbox: failingInbox }
  const env = { ...validEnv, COMMY_PROJECT: 'foo' }
  const log: string[] = []
  const exit = await runProgram(env, failingAdapter, { loggerLayer: captureLogger(log) })
  expect(Exit.isSuccess(exit)).toBe(true)
  expect(log.some((line) => line.includes('Type-1 default narrow registration failed'))).toBe(true)
})

// ─── exit on client disconnect ──────────────

test('a resolved shutdown signal unwinds a live event pump and closes the adapter', async () => {
  const fake = buildFakeAdapter()
  // Production's Zulip `events()` long-polls forever, so the pump parks on
  // `Effect.never` and only a signal unwinds it. Mirror that with a stream
  // that never ends: without a disconnect signal `makeProgram` would block
  // here forever — the orphaned-server leak.
  const liveAdapter: ZulipAdapter = {
    ...fake.adapter,
    inbox: { ...fake.adapter.inbox, events: () => Stream.never },
  }
  const exit = await runProgram(validEnv, liveAdapter, { shutdownSignal: Effect.void })
  expect(Exit.isSuccess(exit)).toBe(true)
  // The shutdown ran the scope finalizers: the adapter was closed exactly once.
  expect(fake.calls.closes.count).toBe(1)
}, 2000)

test('clientDisconnect resolves when stdin reaches EOF (end)', async () => {
  const stdin = new EventEmitter()
  const resolved = Effect.runPromise(clientDisconnect(stdin))
  stdin.emit('end')
  expect(await resolved).toBeUndefined()
})

test('clientDisconnect resolves when stdin closes', async () => {
  const stdin = new EventEmitter()
  const resolved = Effect.runPromise(clientDisconnect(stdin))
  stdin.emit('close')
  expect(await resolved).toBeUndefined()
})

test('clientDisconnect resolves once and detaches its listeners after EOF', async () => {
  const stdin = new EventEmitter()
  const resolved = Effect.runPromise(clientDisconnect(stdin))
  stdin.emit('end')
  // A later 'close' must not re-resume (no double-resume defect), and no
  // listener may linger on the shared stdin emitter.
  stdin.emit('close')
  expect(await resolved).toBeUndefined()
  expect(stdin.listenerCount('end')).toBe(0)
  expect(stdin.listenerCount('close')).toBe(0)
})

// ─── ephemeral idle sweep on Clock + Schedule ──────────────

interface SweepSpyCache {
  readonly cache: Pick<IdentityCache, 'sweepIdle'>
  readonly nowMsCalls: number[]
}

const buildSweepSpyCache = (): SweepSpyCache => {
  const nowMsCalls: number[] = []
  return {
    nowMsCalls,
    cache: {
      sweepIdle: (nowMs) =>
        Effect.sync(() => {
          nowMsCalls.push(nowMs)
        }),
    },
  }
}

test('forkIdleSweep runs sweepIdle on the spaced schedule, stamping the Clock time', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const spy = buildSweepSpyCache()
        const intervalMs = 5 * 60 * 1000
        // Forking happens inside the scope; Schedule.spaced runs the body
        // once immediately at fork, then once per interval thereafter.
        yield* forkIdleSweep(spy.cache, intervalMs)
        // Let the immediate first sweep settle.
        yield* TestClock.adjust(Duration.zero)
        expect(spy.nowMsCalls).toEqual([0])
        // Each interval advance fires exactly one more sweep, stamped with
        // the advancing Clock time (proves the body reads Clock, not Date.now).
        yield* TestClock.adjust(Duration.millis(intervalMs))
        expect(spy.nowMsCalls).toEqual([0, intervalMs])
        yield* TestClock.adjust(Duration.millis(intervalMs))
        expect(spy.nowMsCalls).toEqual([0, intervalMs, intervalMs * 2])
      }),
    ).pipe(Effect.provide(TestContext.TestContext)),
  ))

test('forkIdleSweep fiber is interrupted when its scope closes (does not block exit)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const spy = buildSweepSpyCache()
      const intervalMs = 5 * 60 * 1000
      const scope = yield* Scope.make()
      yield* forkIdleSweep(spy.cache, intervalMs).pipe(Scope.extend(scope))
      yield* TestClock.adjust(Duration.zero)
      expect(spy.nowMsCalls).toEqual([0])
      yield* TestClock.adjust(Duration.millis(intervalMs))
      expect(spy.nowMsCalls).toEqual([0, intervalMs])
      // Closing the scope interrupts the forked sweep fiber: no further
      // sweeps fire even as time advances past more intervals.
      yield* Scope.close(scope, Exit.void)
      yield* TestClock.adjust(Duration.millis(intervalMs * 3))
      expect(spy.nowMsCalls).toEqual([0, intervalMs])
    }).pipe(Effect.provide(TestContext.TestContext)),
  ))
