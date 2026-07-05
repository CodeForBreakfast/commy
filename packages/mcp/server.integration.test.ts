import { expect, test } from 'bun:test'
import { readdirSync, readFileSync, rmSync, type Stats, statSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'
import { captureLogger, stderrLoggerLayer } from '@commy/core/logging'
import type {
  Directory,
  HistoryReader,
  IdentityId as IdentityIdType,
  IdentityPort,
  InboundEvent,
  MessageInbox,
  MessagePublisher,
  SubscriptionTarget,
  Timestamp as TimestampType,
} from '@commy/core/ports'
import {
  type ChannelName,
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeIdentityIdSync,
  decodeMessageBodySync,
  decodeMessageIdSync,
  decodeTimestampSync,
  InboxError,
  MessagePermalinkSchema,
} from '@commy/core/ports'
import { memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Deferred, Effect, FiberId, Layer, Option, Stream } from 'effect'
import { parseEnv, parseSessionId, substrateAdapterLayer } from './bootstrap.ts'
import type { CursorStore } from './cursor-store.ts'
import { CursorStoreTag } from './cursor-store.ts'
// Above-port unit tests drive the substrate through the in-memory adapter only —
// never the real Zulip adapter (see docs/architecture.md § Test architecture).
// `completeAsSubstrate` is the single seam that completes it to the Zulip-shaped
// `SubstrateAdapter` port; no Zulip type or brand is named directly here.
import { completeAsSubstrate } from './memory-substrate.ts'
import { makeProgram } from './server.ts'
import { SessionId as SessionIdTag, type SessionIdValue } from './session-id.ts'
import type { SubscribeIntent } from './subscribe-parser.ts'
import type { SubscriptionStore } from './subscription-store.ts'
import { SubscriptionStoreTag } from './subscription-store.ts'
import { testPlatformLayer } from './test-platform.ts'

const createMemoryCursorStore = (): CursorStore => {
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

// The session-bound in-memory store: it resolves its id from the shared
// session-id deferred — never a per-call argument — exactly as the live file
// store does, so the harness exercises the real session-binding path.
const createMemorySubscriptionStore = (
  session: Deferred.Deferred<SessionIdValue>,
): SubscriptionStore => {
  const store = new Map<string, ReadonlyArray<SubscribeIntent>>()
  return {
    read: () =>
      Effect.map(Deferred.await(session), (id) => Option.fromNullable(store.get(id as string))),
    write: (intents: ReadonlyArray<SubscribeIntent>) =>
      Effect.flatMap(Deferred.await(session), (id) =>
        Effect.sync(() => {
          store.set(id as string, intents)
        }),
      ),
  }
}

const validEnv = {
  ZULIP_SITE: 'https://zulip.example.com',
  ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
  ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
  COMMY_BOT_NAME: 'myproject-concierge',
} as const

const EXPECTED_TOOL_NAMES = [
  'current_identity',
  'resolve',
  'list_agents',
  'list_humans',
  'list_channels',
  'presence',
  'read_channel',
  'read_thread',
  'resolve_thread',
  'unresolve_thread',
  'message_link',
  'subscribe',
  'unsubscribe',
  'post',
  'edit_message',
  'react',
  'unreact',
  'download_file',
  'upload_file',
] as const

const FORBIDDEN_TOOL_NAMES = ['mint', 'acquire', 'release', 'replay'] as const

interface ToolCallResult {
  readonly structuredContent?: unknown
  readonly content?: ReadonlyArray<{ readonly type: string; readonly text?: string }>
  readonly isError?: boolean
}

interface CapturedNotification {
  readonly method: string
  readonly params: unknown
}

interface AdapterOverrides {
  readonly identityOverrides?: Partial<IdentityPort>
  readonly publisherOverrides?: Partial<MessagePublisher>
  readonly inboxOverrides?: Partial<MessageInbox>
  readonly historyOverrides?: Partial<HistoryReader>
  readonly directoryOverrides?: Partial<Directory>
  /** Seed channels before main() boots. */
  readonly seedChannels?: ReadonlyArray<string>
  /** Seed peer agent identities before main() boots. */
  readonly seedAgents?: ReadonlyArray<string>
  /** Seed peer human identities before main() boots. */
  readonly seedHumans?: ReadonlyArray<string>
  /** Override the COMMY_SUBSCRIBE env var (default: unset). */
  readonly subscribe?: string
  /** Override the cursor store (default: in-memory). */
  readonly cursorStore?: CursorStore
  /** Override the subscription store (default: in-memory, bound to the shared deferred). */
  readonly subscriptionStore?: SubscriptionStore
  /**
   * Override the shared session-id deferred (default: a fresh, uncompleted
   * one). Pass a deferred a custom `subscriptionStore` also awaits, so the
   * store resolves the same id the tool-call feeders complete.
   */
  readonly sessionIdDeferred?: Deferred.Deferred<SessionIdValue>
  /**
   * Boot in ephemeral mode (omits COMMY_BOT_NAME). First
   * attribution-producing tool call must include a `session_id`
   * argument so the cache can mint a lazy identity.
   */
  readonly ephemeral?: boolean
  /**
   * Capture stderr-shaped log output (default: route to the runner's
   * STDERR via the production logger). Pass an array to collect the
   * diagnostics an Effect emitted — keeps the runner output clean for
   * tests that intentionally trigger plugin-internal failures and lets
   * the test assert on the captured lines.
   */
  readonly capturedLogs?: string[]
  /**
   * Arbitrary env overrides on top of `validEnv`. Pass `undefined` to
   * effectively drop a key (parseEnv treats missing and undefined
   * identically) — used by tests to set
   * COMMY_PROJECT for Type-2 default-narrow derivation.
   */
  readonly env?: Record<string, string | undefined>
}

interface Harness {
  readonly client: Client
  readonly notifications: ReadonlyArray<CapturedNotification>
  readonly closes: { readonly count: number }
  /**
   * Acquire/release counters drawn from the wrapped IdentityPort. Used
   * by the Type 4 cron-shape integration test to assert
   * the boot-acquire-then-shutdown-release sequence fires exactly once
   * each — the memory adapter's underlying state is silent on call
   * counts.
   */
  readonly identityCalls: {
    readonly acquires: ReadonlyArray<string>
    readonly releases: number
  }
  /**
   * Fire the program's shutdown: interrupt the pump's events stream so
   * the scope unwinds (pump-cancel → release → close), exactly as a
   * SIGTERM does under runMain. Awaits the run so callers can assert on
   * the release/close counters afterwards. Idempotent — `cleanup` calls
   * it too.
   */
  readonly shutdown: () => Promise<void>
  readonly cleanup: () => Promise<void>
  /**
   * The shared session-id deferred the program booted against. A test polls
   * it to assert the boot feeder (comms-k7cv) filled it from
   * CLAUDE_CODE_SESSION_ID in the MCP child env.
   */
  readonly sessionIdDeferred: Deferred.Deferred<SessionIdValue>
}

const buildHarness = async (overrides: AdapterOverrides = {}): Promise<Harness> => {
  const base = await Effect.runPromise(memoryAdapter())

  // Seed the channels/agents/humans the test asked for. Channels are addressed
  // by name end-to-end, so the memory adapter resolves a bare ChannelName to its
  // stored ref internally — no id-strict ref-substitution bridge is needed.
  for (const name of overrides.seedChannels ?? []) {
    await Effect.runPromise(base.seedChannel(name).pipe(Effect.orDie))
  }
  for (const name of overrides.seedAgents ?? []) {
    await Effect.runPromise(base.seedAgent(name).pipe(Effect.orDie))
  }
  for (const name of overrides.seedHumans ?? []) {
    await Effect.runPromise(base.seedHuman(name).pipe(Effect.orDie))
  }

  const killSwitch = Deferred.unsafeMake<void>(FiberId.none)
  let resolvePumpStarted: () => void = () => {}
  const pumpStarted = new Promise<void>((resolve) => {
    resolvePumpStarted = resolve
  })
  const wrappedEvents: MessageInbox['events'] = () => base.inbox.events()

  const closes = { count: 0 }

  const nameLenientPublisher: MessagePublisher = base.publisher

  const nameLenientInbox: MessageInbox = { ...base.inbox, events: wrappedEvents }

  const nameLenientHistory: HistoryReader = base.history

  // Always wrap identity to count acquires/releases — we assert
  // the cron-shape lifecycle (one acquire at boot, one release on
  // SIGTERM). Wrapping here (before composing overrides) means a test
  // that supplies its own `identityOverrides.acquire` is never observed,
  // but no current test does both at once. If that combination ever
  // arises, the counter wrapping should move ahead of the override
  // composition explicitly.
  const acquires: string[] = []
  let releases = 0
  const countingIdentity: IdentityPort = {
    ...base.identity,
    acquire: (name) =>
      Effect.sync(() => {
        acquires.push(name)
      }).pipe(Effect.flatMap(() => base.identity.acquire(name))),
    release: () =>
      Effect.sync(() => {
        releases += 1
      }).pipe(Effect.flatMap(() => base.identity.release())),
  }
  const composedIdentity: IdentityPort = { ...countingIdentity, ...overrides.identityOverrides }
  const composedPublisher: MessagePublisher = {
    ...nameLenientPublisher,
    ...overrides.publisherOverrides,
  }
  const composedInboxRaw: MessageInbox = {
    ...nameLenientInbox,
    ...overrides.inboxOverrides,
  }
  // Apply the killSwitch + pumpStarted notification to the FINAL composed
  // events() so an inboxOverrides.events fake still unwinds on cleanup.
  // Without this re-wrapping, a test fake replaces wrappedEvents wholesale
  // and main() can no longer be interrupted via Deferred.succeed(killSwitch).
  const composedInbox: MessageInbox = {
    ...composedInboxRaw,
    events: () => {
      resolvePumpStarted()
      return composedInboxRaw.events().pipe(Stream.interruptWhenDeferred(killSwitch))
    },
  }
  const composedHistory: HistoryReader = {
    ...nameLenientHistory,
    ...overrides.historyOverrides,
  }
  const composedDirectory: Directory = { ...base.directory, ...overrides.directoryOverrides }

  const adapter = completeAsSubstrate(
    {
      identity: composedIdentity,
      publisher: composedPublisher,
      inbox: composedInbox,
      history: composedHistory,
      directory: composedDirectory,
    },
    {
      close: async () => {
        closes.count += 1
      },
    },
  )

  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
  const client = new Client(
    { name: 'commy-integration-test', version: '0.0.0' },
    { capabilities: {} },
  )

  const notifications: CapturedNotification[] = []
  client.fallbackNotificationHandler = async (notification) => {
    notifications.push({
      method: notification.method,
      params: notification.params ?? null,
    })
  }

  const baseEnv: Record<string, string | undefined> = { ...validEnv }
  if (overrides.ephemeral === true) {
    delete baseEnv['COMMY_BOT_NAME']
  }
  const env: Record<string, string | undefined> = {
    ...baseEnv,
    ...(overrides.subscribe !== undefined ? { COMMY_SUBSCRIBE: overrides.subscribe } : {}),
    ...(overrides.env ?? {}),
  }

  // The boot program: the substrate adapter and cursor
  // store arrive through the app Layer (the adapter layer parse-gated like
  // production's `ZulipAdapterLive`, so `close()` is a layer-scope
  // finalizer), the env through an outermost ConfigProvider. A single
  // `runPromiseExit` drives it; `killSwitch` interrupts the pump's events
  // stream so the scope unwinds (pump-cancel → release → close) just as a
  // SIGTERM does under `runMain`.
  const loggerLayer =
    overrides.capturedLogs !== undefined ? captureLogger(overrides.capturedLogs) : stderrLoggerLayer
  const cursorStore = overrides.cursorStore ?? createMemoryCursorStore()
  // A known shared session-id deferred, so a test can assert the boot feeder
  // filled it from CLAUDE_CODE_SESSION_ID in the child env, and can bind a
  // custom store to the same deferred the tool-call feeders complete.
  const sessionIdDeferred =
    overrides.sessionIdDeferred ?? Deferred.unsafeMake<SessionIdValue>(FiberId.none)
  const subscriptionStore =
    overrides.subscriptionStore ?? createMemorySubscriptionStore(sessionIdDeferred)
  const runExit = Effect.runPromiseExit(
    makeProgram({
      transport: serverTransport,
      loggerLayer,
    }).pipe(
      Effect.provide(
        Layer.provideMerge(
          Layer.mergeAll(
            substrateAdapterLayer(parseEnv.pipe(Effect.as(adapter))),
            Layer.succeed(CursorStoreTag, cursorStore),
            Layer.succeed(SubscriptionStoreTag, subscriptionStore),
            Layer.succeed(SessionIdTag, sessionIdDeferred),
            loggerLayer,
          ),
          testPlatformLayer(env),
        ),
      ),
    ),
  )

  await client.connect(clientTransport)

  const fireShutdown = async (): Promise<void> => {
    // Wait briefly for the pump to start so there's a live Stream
    // subscription to interrupt. Without this, firing the killSwitch
    // before the program reaches `startEventPump` would await a run
    // that never completes (nobody cancels the pump that eventually
    // does start). `runPromiseExit` never rejects — a boot failure
    // resolves to a failure Exit, which the harness ignores.
    await Promise.race([pumpStarted, new Promise<void>((r) => setTimeout(r, 500))])
    Deferred.unsafeDone(killSwitch, Effect.void)
    await runExit
  }

  return {
    client,
    notifications,
    closes,
    identityCalls: {
      get acquires() {
        return acquires
      },
      get releases() {
        return releases
      },
    },
    shutdown: fireShutdown,
    cleanup: async () => {
      await fireShutdown()
      try {
        await client.close()
      } catch {
        // Client may already be closed (e.g. server hit a fatal error first).
      }
    },
    sessionIdDeferred,
  }
}

const callTool = async (
  client: Client,
  name: string,
  args: Readonly<Record<string, unknown>> = {},
): Promise<ToolCallResult> => (await client.callTool({ name, arguments: args })) as ToolCallResult

const expectStructured = (result: ToolCallResult): Record<string, unknown> => {
  if (
    typeof result.structuredContent !== 'object' ||
    result.structuredContent === null ||
    Array.isArray(result.structuredContent)
  ) {
    throw new Error(
      `tool returned non-object structuredContent: ${JSON.stringify(result.structuredContent)}`,
    )
  }
  return result.structuredContent as Record<string, unknown>
}

// ─── Negative-requirement assertions ────────────────────────────────────────

test('initialize response declares experimental claude/channel capability', async () => {
  const h = await buildHarness()
  try {
    const capabilities = h.client.getServerCapabilities()
    expect(capabilities?.experimental).toBeDefined()
    expect(capabilities?.experimental?.['claude/channel']).toEqual({})
  } finally {
    await h.cleanup()
  }
})

test('initialize response does NOT declare claude/channel/permission capability', async () => {
  const h = await buildHarness()
  try {
    const capabilities = h.client.getServerCapabilities()
    expect(capabilities?.experimental?.['claude/channel/permission']).toBeUndefined()
  } finally {
    await h.cleanup()
  }
})

test('tools/list returns exactly the expected tool names — no more, no fewer', async () => {
  const h = await buildHarness()
  try {
    const listed = await h.client.listTools()
    const names = listed.tools.map((t) => t.name).sort()
    const expected = [...EXPECTED_TOOL_NAMES].sort()
    expect(names).toEqual(expected)
  } finally {
    await h.cleanup()
  }
})

test('tools/list does NOT contain mint, acquire, release, replay', async () => {
  const h = await buildHarness()
  try {
    const listed = await h.client.listTools()
    const names = new Set(listed.tools.map((t) => t.name))
    for (const forbidden of FORBIDDEN_TOOL_NAMES) {
      expect(names.has(forbidden)).toBe(false)
    }
  } finally {
    await h.cleanup()
  }
})

test('all tool input schemas use channel_name (string), never channel_id', async () => {
  const h = await buildHarness()
  try {
    const listed = await h.client.listTools()
    for (const tool of listed.tools) {
      const schema = tool.inputSchema as { properties?: Record<string, { type?: string }> }
      const props = schema.properties ?? {}
      expect(props['channel_id']).toBeUndefined()
      if (props['channel_name'] !== undefined) {
        expect(props['channel_name'].type).toBe('string')
      }
    }
  } finally {
    await h.cleanup()
  }
})

test('no allowlist code path exists in server.ts source', () => {
  const source = readFileSync(join(import.meta.dir, 'server.ts'), 'utf8')
  expect(source).not.toMatch(/access\.json/)
  expect(source).not.toMatch(/allowFrom/)
})

test('static audit: no plugin source imports or calls fs-write APIs', () => {
  const pluginDir = import.meta.dir
  // Sanctioned writers:
  //   cursor-store.ts        — per-identity mentions cursor under <XDG_STATE_HOME>
  //   subscription-store.ts  — per-session_id narrow-set snapshot under <XDG_STATE_HOME>
  //   server.ts              — download_file temp files under os.tmpdir()
  const writeAllowlist = new Set(['cursor-store.ts', 'subscription-store.ts', 'server.ts'])
  const sources = readdirSync(pluginDir).filter(
    (n) =>
      n.endsWith('.ts') &&
      !n.endsWith('.test.ts') &&
      !n.endsWith('.integration.test.ts') &&
      !writeAllowlist.has(n),
  )
  // If ANY plugin source outside the allowlist contains an fs-write
  // symbol, runtime state writes become possible. This is
  // proof-by-source-audit; combined with the runtime checks below it
  // gives a complete picture.
  const forbiddenSymbols = [
    'writeFile',
    'writeFileSync',
    'appendFile',
    'appendFileSync',
    'mkdir',
    'mkdirSync',
    'createWriteStream',
    'unlinkSync',
    'rmdirSync',
    'rmSync',
    'rename',
    'renameSync',
    'symlinkSync',
    'truncateSync',
  ]
  expect(sources.length).toBeGreaterThan(0)
  for (const file of sources) {
    const src = readFileSync(join(pluginDir, file), 'utf8')
    for (const sym of forbiddenSymbols) {
      expect({ file, src_contains: sym, found: src.includes(sym) }).toEqual({
        file,
        src_contains: sym,
        found: false,
      })
    }
  }
})

test('runtime: ~/.local/state/commy is untouched during a full plugin exercise', async () => {
  // XDG-state convention puts plugin state at `~/.local/state/<plugin-name>/`.
  // The "commy" path is name-claimed by this plugin alone; other
  // Claude Code processes on the shared machine never create it. A precise
  // before/after comparison of this specific path proves the plugin wrote
  // nothing to its conventional state home — without scanning the wider
  // ~/.local/state tree (which is full of unrelated session activity).
  const xdgStateCommy = join(homedir(), '.local', 'state', 'commy')
  const before = describeFsEntry(xdgStateCommy)

  const h = await buildHarness({
    seedChannels: ['home'],
    seedAgents: ['alice'],
    seedHumans: ['carol'],
    subscribe: 'channel:home',
  })
  try {
    await callTool(h.client, 'current_identity')
    await callTool(h.client, 'post', { channel_name: 'home', body: 'hello' })
    await callTool(h.client, 'read_channel', { channel_name: 'home' })
    await callTool(h.client, 'list_agents')
    await callTool(h.client, 'list_humans')
  } finally {
    await h.cleanup()
  }

  expect(describeFsEntry(xdgStateCommy)).toEqual(before)
})

test('runtime: plugin directory tree is unchanged across a full plugin exercise', async () => {
  // The plugin directory is owned by us and small. Other Claude sessions
  // don't touch it, so a recursive before/after snapshot (excluding the
  // known non-state subtrees) is precise.
  const pluginDir = import.meta.dir
  const before = snapshotPluginDir(pluginDir)

  const h = await buildHarness({
    seedChannels: ['home'],
    seedAgents: ['alice'],
    seedHumans: ['carol'],
    subscribe: 'channel:home',
  })
  try {
    await callTool(h.client, 'current_identity')
    await callTool(h.client, 'post', { channel_name: 'home', body: 'hello' })
    await callTool(h.client, 'read_channel', { channel_name: 'home' })
    await callTool(h.client, 'list_agents')
    await callTool(h.client, 'list_humans')
  } finally {
    await h.cleanup()
  }

  expect(snapshotPluginDir(pluginDir)).toEqual(before)
})

// ─── Per-tool: download_file / upload_file — real FileSystem builder ───

test('download_file: server FileSystem builder writes the download to a real temp file and returns its path', async () => {
  const h = await buildHarness({
    seedChannels: ['home'],
    seedAgents: ['alice'],
    seedHumans: ['carol'],
  })
  try {
    const result = await callTool(h.client, 'download_file', {
      url_path: '/user_uploads/2/56/image.png',
    })
    const content = expectStructured(result)
    expect(content['content_type']).toBe('application/octet-stream')
    expect(content['size']).toBe(0)
    const filePath = content['file_path']
    expect(typeof filePath).toBe('string')
    // The builder created a real temp directory and wrote the (empty)
    // download there — prove the file exists on disk with the reported size.
    const written = readFileSync(filePath as string)
    expect(written.byteLength).toBe(0)
    rmSync(filePath as string, { force: true })
  } finally {
    await h.cleanup()
  }
})

test('upload_file: server FileSystem builder reads the real local file and reports its byte length', async () => {
  const localPath = join(tmpdir(), `upload-test-${process.pid}.bin`)
  const payload = new Uint8Array([1, 2, 3, 4, 5])
  writeFileSync(localPath, payload)
  const h = await buildHarness({
    seedChannels: ['home'],
    seedAgents: ['alice'],
    seedHumans: ['carol'],
  })
  try {
    const result = await callTool(h.client, 'upload_file', { path: localPath })
    const content = expectStructured(result)
    // `size` comes from the builder's own FileSystem.readFile, not the
    // substrate stub — proving the real local file was read (5 bytes).
    expect(content['size']).toBe(payload.byteLength)
    expect(content['filename']).toBe('stub')
    expect(content['reference']).toBe('[stub](/user_uploads/0/stub)')
  } finally {
    rmSync(localPath, { force: true })
    await h.cleanup()
  }
})

// ─── Per-tool: current_identity ─────────────────────────────────────────────

test('current_identity (happy): returns the bound bot identity envelope', async () => {
  const h = await buildHarness()
  try {
    const result = await callTool(h.client, 'current_identity')
    const body = expectStructured(result)
    expect(body['state']).toBe('bound')
    const identity = body['identity'] as Record<string, unknown>
    expect(identity['name']).toBe('myproject-concierge')
    expect(identity['kind']).toBe('agent')
    expect(typeof identity['id']).toBe('string')
  } finally {
    await h.cleanup()
  }
})

test('current_identity is passive: does NOT call IdentityPort.currentIdentity', async () => {
  // current_identity reads from the boot-orchestrator's
  // ensureBound state, never round-trips to the substrate. Wire a
  // throwing currentIdentity to prove the tool path doesn't touch it.
  class SubstrateOffline extends Error {
    constructor() {
      super('substrate temporarily offline')
      this.name = 'SubstrateOffline'
    }
  }
  const h = await buildHarness({
    identityOverrides: {
      currentIdentity: () => Effect.die(new SubstrateOffline()),
    },
  })
  try {
    const result = await callTool(h.client, 'current_identity')
    const body = expectStructured(result)
    expect(body['state']).toBe('bound')
    const identity = body['identity'] as Record<string, unknown>
    expect(identity['name']).toBe('myproject-concierge')
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: resolve ──────────────────────────────────────────────────────

test('resolve (happy): returns identity for known name; null for unknown', async () => {
  const h = await buildHarness({ seedAgents: ['alice'] })
  try {
    const hit = expectStructured(await callTool(h.client, 'resolve', { name: 'alice' }))
    expect((hit['identity'] as Record<string, unknown>)['name']).toBe('alice')
    const miss = expectStructured(await callTool(h.client, 'resolve', { name: 'nobody' }))
    expect(miss['identity']).toBeNull()
  } finally {
    await h.cleanup()
  }
})

test('resolve (error): rejects when name argument is missing', async () => {
  const h = await buildHarness()
  try {
    await expect(callTool(h.client, 'resolve', {})).rejects.toThrow(/ParseError[\s\S]*name/)
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: list_agents ──────────────────────────────────────────────────

test('list_agents (happy): returns all seeded agent identities', async () => {
  const h = await buildHarness({ seedAgents: ['alice', 'bob'] })
  try {
    const body = expectStructured(await callTool(h.client, 'list_agents'))
    const names = (body['identities'] as ReadonlyArray<{ name: string }>).map((i) => i.name)
    // Bot's own identity is bound at boot, so it's in the agent list too.
    expect(names).toContain('alice')
    expect(names).toContain('bob')
    expect(names).toContain('myproject-concierge')
  } finally {
    await h.cleanup()
  }
})

test('list_agents (error): surfaces port exception with class-name prefix', async () => {
  class DirectoryFault extends Error {
    constructor() {
      super('directory listing failed')
      this.name = 'DirectoryFault'
    }
  }
  const h = await buildHarness({
    directoryOverrides: {
      listAgents: () => Effect.die(new DirectoryFault()),
    },
  })
  try {
    const err = await callTool(h.client, 'list_agents').then(
      () => {
        throw new Error('expected list_agents to reject')
      },
      (e: unknown) => e as Error,
    )
    expect(err.message).toMatch(/DirectoryFault: directory listing failed/)
    expect(err.message).not.toContain('FiberFailure')
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: list_humans ──────────────────────────────────────────────────

test('list_humans (happy): returns all seeded human identities', async () => {
  const h = await buildHarness({ seedHumans: ['carol', 'dave'] })
  try {
    const body = expectStructured(await callTool(h.client, 'list_humans'))
    const names = (body['identities'] as ReadonlyArray<{ name: string }>).map((i) => i.name)
    expect(names.sort()).toEqual(['carol', 'dave'])
  } finally {
    await h.cleanup()
  }
})

test('list_humans (error): surfaces port exception with class-name prefix', async () => {
  class DirectoryFault extends Error {
    constructor() {
      super('directory listing failed')
      this.name = 'DirectoryFault'
    }
  }
  const h = await buildHarness({
    directoryOverrides: {
      listHumans: () => Effect.die(new DirectoryFault()),
    },
  })
  try {
    const err = await callTool(h.client, 'list_humans').then(
      () => {
        throw new Error('expected list_humans to reject')
      },
      (e: unknown) => e as Error,
    )
    expect(err.message).toMatch(/DirectoryFault: directory listing failed/)
    expect(err.message).not.toContain('FiberFailure')
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: presence ─────────────────────────────────────────────────────

test('presence (happy): returns presence for a cached identity', async () => {
  const h = await buildHarness({ seedAgents: ['alice'] })
  try {
    // Cache alice first.
    const alice = (
      expectStructured(await callTool(h.client, 'resolve', { name: 'alice' }))['identity'] as {
        id: string
      }
    ).id
    const body = expectStructured(await callTool(h.client, 'presence', { identity_id: alice }))
    expect(['online', 'idle', 'offline']).toContain(body['presence'] as string)
  } finally {
    await h.cleanup()
  }
})

test('presence (error): UnknownIdentity when identity_id is not cached', async () => {
  const h = await buildHarness()
  try {
    await expect(
      callTool(h.client, 'presence', { identity_id: 'definitely-not-cached' }),
    ).rejects.toThrow(/UnknownIdentity:/)
  } finally {
    await h.cleanup()
  }
})

test('presence (error): surfaces port exception with class-name prefix', async () => {
  class DirectoryFault extends Error {
    constructor() {
      super('presence read failed')
      this.name = 'DirectoryFault'
    }
  }
  const h = await buildHarness({
    seedAgents: ['alice'],
    directoryOverrides: {
      presence: () => Effect.die(new DirectoryFault()),
    },
  })
  try {
    const alice = (
      expectStructured(await callTool(h.client, 'resolve', { name: 'alice' }))['identity'] as {
        id: string
      }
    ).id
    const err = await callTool(h.client, 'presence', { identity_id: alice }).then(
      () => {
        throw new Error('expected presence to reject')
      },
      (e: unknown) => e as Error,
    )
    expect(err.message).toMatch(/DirectoryFault: presence read failed/)
    expect(err.message).not.toContain('FiberFailure')
  } finally {
    await h.cleanup()
  }
})

test('presence (happy): resolves an identity first seen only via an inbound notification', async () => {
  // Drive a single message-posted event from a peer the bot never
  // resolved/listed. The pump populates the tools-side cache from inbound
  // events, so presence(stranger.id) resolves instead of throwing
  // UnknownIdentity.
  const stranger = {
    id: decodeIdentityIdSync('stranger-99'),
    name: decodeDisplayNameSync('stranger'),
    kind: 'agent' as const,
  }
  const inboundMessage = {
    ref: {
      id: decodeMessageIdSync('inbound-msg-1'),
      channel: {
        id: decodeChannelIdSync('home'),
        name: decodeChannelNameSync('home'),
        permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/home'),
      },
      thread: Option.none(),
      permalink: MessagePermalinkSchema.make(
        'https://zulip.example.com/#narrow/channel/home/near/inbound-msg-1',
      ),
    },
    sender: stranger,
    body: decodeMessageBodySync('first contact'),
    ts: decodeTimestampSync(1715450000),
    mentions: [],
    reactions: [],
  }
  let yielded = false
  const firstEvent: InboundEvent = { kind: 'message-posted', message: inboundMessage }
  const events: MessageInbox['events'] = () =>
    Stream.succeed(firstEvent).pipe(
      Stream.tap(() =>
        Effect.sync(() => {
          yielded = true
        }),
      ),
      Stream.concat(Stream.never),
    )
  const h = await buildHarness({
    seedChannels: ['home'],
    subscribe: 'channel:home',
    inboxOverrides: { events },
  })
  try {
    await waitFor(() => yielded, 200)
    // Slack the pump + transport so rememberIdentity has time to run.
    await new Promise((r) => setTimeout(r, 50))

    const body = expectStructured(
      await callTool(h.client, 'presence', { identity_id: 'stranger-99' }),
    )
    expect(['online', 'idle', 'offline']).toContain(body['presence'] as string)
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: read_channel ─────────────────────────────────────────────────

test('read_channel (happy): returns messages posted to the channel', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await callTool(h.client, 'post', { channel_name: 'home', body: 'one' })
    await callTool(h.client, 'post', { channel_name: 'home', body: 'two' })
    const body = expectStructured(
      await callTool(h.client, 'read_channel', { channel_name: 'home' }),
    )
    const messages = body['messages'] as ReadonlyArray<{ body: string }>
    expect(messages.map((m) => m.body)).toEqual(['one', 'two'])
  } finally {
    await h.cleanup()
  }
})

test('read_channel (error): rejects when channel_name argument is missing', async () => {
  const h = await buildHarness()
  try {
    await expect(callTool(h.client, 'read_channel', {})).rejects.toThrow(
      /ParseError[\s\S]*channel_name/,
    )
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: read_thread ──────────────────────────────────────────────────

test('read_thread (happy): returns messages for the named thread only', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await callTool(h.client, 'post', { channel_name: 'home', body: 'general msg' })
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'tea time',
      thread: 'breakfast',
    })
    const body = expectStructured(
      await callTool(h.client, 'read_thread', {
        channel_name: 'home',
        thread: 'breakfast',
      }),
    )
    const messages = body['messages'] as ReadonlyArray<{ body: string }>
    expect(messages.map((m) => m.body)).toEqual(['tea time'])
  } finally {
    await h.cleanup()
  }
})

test('read_thread (error): rejects when thread argument is missing', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await expect(callTool(h.client, 'read_thread', { channel_name: 'home' })).rejects.toThrow(
      /ParseError[\s\S]*thread/,
    )
  } finally {
    await h.cleanup()
  }
})

test('unknown arguments are rejected with a clear error', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await expect(
      callTool(h.client, 'post', {
        channel_name: 'home',
        body: 'hi',
        thread_name: 'oops',
      }),
    ).rejects.toThrow(/unknown argument.*thread_name/)
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: subscribe ────────────────────────────────────────────────────

test('subscribe (happy): accepts channel/thread/mentions tokens', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    const a = expectStructured(await callTool(h.client, 'subscribe', { target: 'channel:home' }))
    const b = expectStructured(
      await callTool(h.client, 'subscribe', { target: 'thread:home/payments' }),
    )
    const c = expectStructured(await callTool(h.client, 'subscribe', { target: 'mentions' }))
    expect(a).toEqual({})
    expect(b).toEqual({})
    expect(c).toEqual({})
  } finally {
    await h.cleanup()
  }
})

test('subscribe (error): rejects malformed token', async () => {
  const h = await buildHarness()
  try {
    await expect(callTool(h.client, 'subscribe', { target: 'not-a-known-shape' })).rejects.toThrow(
      /invalid subscribe token/,
    )
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: unsubscribe ──────────────────────────────────────────────────

test('unsubscribe (happy): accepts the same token grammar as subscribe', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await callTool(h.client, 'subscribe', { target: 'channel:home' })
    const result = expectStructured(
      await callTool(h.client, 'unsubscribe', { target: 'channel:home' }),
    )
    expect(result).toEqual({})
  } finally {
    await h.cleanup()
  }
})

test('unsubscribe (error): rejects malformed token', async () => {
  const h = await buildHarness()
  try {
    await expect(
      callTool(h.client, 'unsubscribe', { target: 'not-a-known-shape' }),
    ).rejects.toThrow(/invalid subscribe token/)
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: post ─────────────────────────────────────────────────────────

test('post (happy): writes a message and returns its id and channel ref', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    const body = expectStructured(
      await callTool(h.client, 'post', { channel_name: 'home', body: 'hello' }),
    )
    expect(typeof body['message_id']).toBe('string')
    expect(body['channel_name']).toBe('home')
  } finally {
    await h.cleanup()
  }
})

test('post (error): rejects when body argument is missing', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    await expect(callTool(h.client, 'post', { channel_name: 'home' })).rejects.toThrow(
      /ParseError[\s\S]*body/,
    )
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: react ────────────────────────────────────────────────────────

test('react (happy): adds a reaction to a known message', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    const posted = expectStructured(
      await callTool(h.client, 'post', { channel_name: 'home', body: 'reactable' }),
    )
    const messageId = posted['message_id'] as string
    const result = expectStructured(
      await callTool(h.client, 'react', {
        message_id: messageId,
        emoji: 'thumbs_up',
      }),
    )
    expect(result).toEqual({})
  } finally {
    await h.cleanup()
  }
})

test('react (error): UnknownMessage for an uncached message_id with no channel_name', async () => {
  const h = await buildHarness()
  try {
    await expect(
      callTool(h.client, 'react', { message_id: 'ghost-id', emoji: 'thumbs_up' }),
    ).rejects.toThrow(/UnknownMessage:/)
  } finally {
    await h.cleanup()
  }
})

// ─── Per-tool: unreact ──────────────────────────────────────────────────────

test('unreact (happy): removes a reaction from a known message', async () => {
  const h = await buildHarness({ seedChannels: ['home'] })
  try {
    const posted = expectStructured(
      await callTool(h.client, 'post', { channel_name: 'home', body: 'reactable' }),
    )
    const messageId = posted['message_id'] as string
    await callTool(h.client, 'react', { message_id: messageId, emoji: 'thumbs_up' })
    const result = expectStructured(
      await callTool(h.client, 'unreact', {
        message_id: messageId,
        emoji: 'thumbs_up',
      }),
    )
    expect(result).toEqual({})
  } finally {
    await h.cleanup()
  }
})

test('unreact (error): UnknownMessage for an uncached message_id with no channel_name', async () => {
  const h = await buildHarness()
  try {
    await expect(
      callTool(h.client, 'unreact', { message_id: 'ghost-id', emoji: 'thumbs_up' }),
    ).rejects.toThrow(/UnknownMessage:/)
  } finally {
    await h.cleanup()
  }
})

// ─── Self-echo suppression ───────────────────────────────────────

test('post by self does NOT fire a claude/channel notification (self-echo suppressed)', async () => {
  // The substrate's events queue replays the
  // poster's own message back to it. Without the pump's self-echo guard
  // every poster receives an inbound copy of the message it just sent —
  // useful to no subscriber and forcing every consumer to filter out
  // events whose sender is its own bound identity. The pump drops those
  // events at the emitter (so no self-echo identity id need ride on the
  // frame at all); this asserts that wiring end-to-end.
  const h = await buildHarness({
    seedChannels: ['home'],
    subscribe: 'channel:home',
  })
  try {
    await callTool(h.client, 'post', { channel_name: 'home', body: 'hello channel' })

    // Give the pump + transport ample time to deliver a notification if
    // the self-echo guard were missing. 200ms is well above the in-memory
    // round-trip; we err on giving the buggy path enough rope to hang.
    await new Promise((r) => setTimeout(r, 200))

    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toEqual([])
  } finally {
    await h.cleanup()
  }
})

// ─── Pump narrow-set filter ─────────────────────────────────────────────────

test('pump filter: event for a never-subscribed channel does NOT fire claude/channel notification', async () => {
  // Production wiring assertion. The Zulip minter is subscribed to every
  // public stream (per `minter-reconciler.ts`), so the adapter
  // inbox yields events for streams the calling session never subscribed
  // to via the MCP `subscribe` tool or `COMMY_SUBSCRIBE` env. The
  // plugin-layer NarrowSet (`narrow-set.ts`) is the filter that decides
  // which of those events the MCP host actually sees. This test exercises
  // the pump's wiring to that NarrowSet via the production `main()`
  // boot path — hand-rolling the inbox's events() iterator so a single
  // un-subscribed-channel event is yielded, and asserting the client
  // receives no `notifications/claude/channel` for it.
  const noiseEvent: InboundEvent = {
    kind: 'message-posted',
    message: {
      ref: {
        id: decodeMessageIdSync('noise-msg-1'),
        channel: {
          id: decodeChannelIdSync('noise'),
          name: decodeChannelNameSync('noise'),
          permalink: ChannelPermalinkSchema.make('https://zulip.example.com/#narrow/channel/noise'),
        },
        thread: Option.none(),
        permalink: MessagePermalinkSchema.make(
          'https://zulip.example.com/#narrow/channel/noise/near/noise-msg-1',
        ),
      },
      sender: {
        id: decodeIdentityIdSync('outsider'),
        name: decodeDisplayNameSync('outsider'),
        kind: 'agent',
      },
      body: decodeMessageBodySync('this should be filtered'),
      ts: decodeTimestampSync(1),
      mentions: [],
      reactions: [],
    },
  }

  let yielded = false
  const events: MessageInbox['events'] = () =>
    Stream.succeed(noiseEvent).pipe(
      Stream.tap(() =>
        Effect.sync(() => {
          yielded = true
        }),
      ),
      Stream.concat(Stream.never),
    )

  // No `subscribe` env var → narrowSet boots empty. Without the pump
  // wired to the narrowSet, the noiseEvent flows through unfiltered and
  // a `claude/channel` notification fires. With the wiring in place, the
  // empty narrowSet rejects the event and no notification fires.
  const h = await buildHarness({ inboxOverrides: { events } })
  try {
    await waitFor(() => yielded, 200)
    // Slack the pump + transport so any (incorrect) notification has time
    // to reach the client's fallback handler. 50ms is comfortably above
    // the in-memory transport round-trip; we err on the side of giving
    // the buggy path enough rope to hang itself.
    await new Promise((r) => setTimeout(r, 50))

    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toEqual([])
  } finally {
    await h.cleanup()
  }
})

// ─── mentions catch-up on persistent-mode resume ────────────────

test('persistent boot with no prior cursor: no replay events fired, cursor initialised to now', async () => {
  const writes: { id: string; ts: number }[] = []
  let storedTs: number | undefined
  const cursorStore: CursorStore = {
    read: () =>
      Effect.sync(() =>
        storedTs === undefined
          ? Option.none<TimestampType>()
          : Option.some(decodeTimestampSync(storedTs)),
      ),
    write: (id, ts) =>
      Effect.sync(() => {
        writes.push({ id: id as string, ts })
        if (storedTs === undefined || storedTs < ts) storedTs = ts
      }),
  }

  const replayCalls: number[] = []
  const inboxOverrides: Partial<MessageInbox> = {
    replay: (since) =>
      Effect.sync(() => {
        replayCalls.push(since)
        return []
      }),
  }

  const before = Math.floor(Date.now() / 1000)
  const h = await buildHarness({
    cursorStore,
    inboxOverrides,
  })
  try {
    await waitFor(() => writes.length > 0, 200)
    const after = Math.floor(Date.now() / 1000)
    expect(replayCalls).toEqual([])
    expect(writes).toHaveLength(1)
    // The catch-up stamps the cursor with the current Clock time (seconds).
    expect(writes[0]?.ts).toBeGreaterThanOrEqual(before)
    expect(writes[0]?.ts).toBeLessThanOrEqual(after)
    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toEqual([])
  } finally {
    await h.cleanup()
  }
})

test('persistent boot with a prior cursor: replay fires, mention-received notifications dispatched ahead of pump', async () => {
  const PRIOR_CURSOR_TS = 1000
  const cursorStore: CursorStore = {
    read: () => Effect.succeed(Option.some(decodeTimestampSync(PRIOR_CURSOR_TS))),
    write: () => Effect.void,
  }

  const mentionedIdentity = {
    id: decodeIdentityIdSync('bot-placeholder'),
    name: decodeDisplayNameSync('myproject-concierge'),
    kind: 'agent' as const,
  }
  const senderIdentity = {
    id: decodeIdentityIdSync('user-99'),
    name: decodeDisplayNameSync('carol'),
    kind: 'human' as const,
  }
  const replayed: InboundEvent[] = [
    {
      kind: 'mention-received',
      message: {
        ref: {
          id: decodeMessageIdSync('msg-replay-1'),
          channel: {
            id: decodeChannelIdSync('chan-home'),
            name: decodeChannelNameSync('home'),
            permalink: ChannelPermalinkSchema.make(
              'https://zulip.example.com/#narrow/channel/home',
            ),
          },
          thread: Option.none(),
          permalink: MessagePermalinkSchema.make(
            'https://zulip.example.com/#narrow/channel/home/near/msg-replay-1',
          ),
        },
        sender: senderIdentity,
        body: decodeMessageBodySync('hey concierge, missed you'),
        ts: decodeTimestampSync(PRIOR_CURSOR_TS + 50),
        mentions: [mentionedIdentity],
        reactions: [],
      },
      mentions: [mentionedIdentity],
    },
    // A non-mention event in the same replay window — proves the catch-up
    // filters to mention-received only.
    {
      kind: 'message-posted',
      message: {
        ref: {
          id: decodeMessageIdSync('msg-replay-2'),
          channel: {
            id: decodeChannelIdSync('chan-home'),
            name: decodeChannelNameSync('home'),
            permalink: ChannelPermalinkSchema.make(
              'https://zulip.example.com/#narrow/channel/home',
            ),
          },
          thread: Option.none(),
          permalink: MessagePermalinkSchema.make(
            'https://zulip.example.com/#narrow/channel/home/near/msg-replay-2',
          ),
        },
        sender: senderIdentity,
        body: decodeMessageBodySync('unrelated chatter'),
        ts: decodeTimestampSync(PRIOR_CURSOR_TS + 60),
        mentions: [],
        reactions: [],
      },
    },
  ]

  const replayCalls: number[] = []
  const inboxOverrides: Partial<MessageInbox> = {
    replay: (since) =>
      Effect.sync(() => {
        replayCalls.push(since)
        return replayed
      }),
  }

  const h = await buildHarness({
    cursorStore,
    inboxOverrides,
  })
  try {
    await waitFor(
      () => h.notifications.some((n) => n.method === 'notifications/claude/channel'),
      200,
    )
    expect(replayCalls).toEqual([PRIOR_CURSOR_TS])
    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toHaveLength(1)
    const params = channelNotifications[0]?.params as {
      content: string
      meta: Record<string, string>
    }
    expect(params.content).toBe('hey concierge, missed you')
    expect(params.meta['message_id']).toBe('msg-replay-1')
  } finally {
    await h.cleanup()
  }
})

// ─── mentions catch-up on ephemeral-mode lazy acquire ────────────

test('ephemeral lazy acquire with no prior cursor: no replay, cursor initialised to now', async () => {
  const writes: { id: string; ts: number }[] = []
  let storedTs: number | undefined
  const cursorStore: CursorStore = {
    read: () =>
      Effect.sync(() =>
        storedTs === undefined
          ? Option.none<TimestampType>()
          : Option.some(decodeTimestampSync(storedTs)),
      ),
    write: (id, ts) =>
      Effect.sync(() => {
        writes.push({ id: id as string, ts })
        if (storedTs === undefined || storedTs < ts) storedTs = ts
      }),
  }

  const replayCalls: number[] = []
  const inboxOverrides: Partial<MessageInbox> = {
    replay: (since) =>
      Effect.sync(() => {
        replayCalls.push(since)
        return []
      }),
  }

  const h = await buildHarness({
    ephemeral: true,
    cursorStore,
    inboxOverrides,
    seedChannels: ['home'],
  })
  try {
    const before = Math.floor(Date.now() / 1000)
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'hello from ephemeral',
      session_id: 'f1e5f1e5-0000-4000-8000-000000000001',
    })
    await waitFor(() => writes.length > 0, 200)
    const after = Math.floor(Date.now() / 1000)
    expect(replayCalls).toEqual([])
    expect(writes).toHaveLength(1)
    // The catch-up stamps the cursor with the current Clock time (seconds).
    expect(writes[0]?.ts).toBeGreaterThanOrEqual(before)
    expect(writes[0]?.ts).toBeLessThanOrEqual(after)
    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toEqual([])
  } finally {
    await h.cleanup()
  }
})

test('ephemeral lazy acquire with a prior cursor: replay fires, mention dispatched ahead of tool result', async () => {
  const PRIOR_CURSOR_TS = 1000
  const cursorStore: CursorStore = {
    read: () => Effect.succeed(Option.some(decodeTimestampSync(PRIOR_CURSOR_TS))),
    write: () => Effect.void,
  }

  const mentionedIdentity = {
    id: decodeIdentityIdSync('bot-placeholder'),
    name: decodeDisplayNameSync('cc-resume-s'),
    kind: 'agent' as const,
  }
  const senderIdentity = {
    id: decodeIdentityIdSync('user-99'),
    name: decodeDisplayNameSync('carol'),
    kind: 'human' as const,
  }
  const replayed: InboundEvent[] = [
    {
      kind: 'mention-received',
      message: {
        ref: {
          id: decodeMessageIdSync('msg-replay-1'),
          channel: {
            id: decodeChannelIdSync('chan-home'),
            name: decodeChannelNameSync('home'),
            permalink: ChannelPermalinkSchema.make(
              'https://zulip.example.com/#narrow/channel/home',
            ),
          },
          thread: Option.none(),
          permalink: MessagePermalinkSchema.make(
            'https://zulip.example.com/#narrow/channel/home/near/msg-replay-1',
          ),
        },
        sender: senderIdentity,
        body: decodeMessageBodySync('hey ephemeral session, missed you'),
        ts: decodeTimestampSync(PRIOR_CURSOR_TS + 50),
        mentions: [mentionedIdentity],
        reactions: [],
      },
      mentions: [mentionedIdentity],
    },
  ]

  const replayCalls: number[] = []
  const inboxOverrides: Partial<MessageInbox> = {
    replay: (since) =>
      Effect.sync(() => {
        replayCalls.push(since)
        return replayed
      }),
  }

  const h = await buildHarness({
    ephemeral: true,
    cursorStore,
    inboxOverrides,
    seedChannels: ['home'],
  })
  try {
    const postPromise = callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'hello from resuming ephemeral',
      session_id: '7e5057e5-0000-4000-8000-000000000002',
    })
    await waitFor(
      () => h.notifications.some((n) => n.method === 'notifications/claude/channel'),
      200,
    )
    await postPromise
    expect(replayCalls).toEqual([PRIOR_CURSOR_TS])
    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toHaveLength(1)
    const params = channelNotifications[0]?.params as {
      content: string
      meta: Record<string, string>
    }
    expect(params.content).toBe('hey ephemeral session, missed you')
    expect(params.meta['message_id']).toBe('msg-replay-1')
  } finally {
    await h.cleanup()
  }
})

test('ephemeral catch-up failure is non-fatal: tool call succeeds, failure is logged', async () => {
  const cursorStore: CursorStore = {
    read: () => Effect.succeed(Option.some(decodeTimestampSync(1000))),
    write: () => Effect.void,
  }

  const logged: string[] = []
  const inboxOverrides: Partial<MessageInbox> = {
    replay: () =>
      Effect.fail(
        new InboxError({
          operation: 'replay',
          cause: new Error('replay boom'),
        }),
      ),
  }

  const h = await buildHarness({
    ephemeral: true,
    cursorStore,
    inboxOverrides,
    seedChannels: ['home'],
    capturedLogs: logged,
  })
  try {
    const result = await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'hello',
      session_id: 'fa110fed-0000-4000-8000-000000000003',
    })
    expect(result.isError).toBeFalsy()
  } finally {
    await h.cleanup()
  }
  expect(
    logged.some(
      (line) => line.includes('ephemeral mentions catch-up failed') && line.includes('replay boom'),
    ),
  ).toBe(true)
})

// ─── Type-2 default sub set for interactive CC sessions ─────────

const captureSubscribes = (): {
  readonly inboxOverrides: Partial<MessageInbox>
  readonly tokens: ReadonlyArray<string>
} => {
  const tokens: string[] = []
  const renderTarget = (target: SubscriptionTarget): string => {
    if (target === 'mentions') return 'mentions'
    if (typeof target === 'string') return `channel:${target}`
    if ('kind' in target) return `new-topics:${target.channel}`
    return `thread:${target.channel}/${target.thread}`
  }
  return {
    tokens,
    inboxOverrides: {
      subscribe: (target: SubscriptionTarget) =>
        Effect.sync(() => {
          tokens.push(renderTarget(target))
        }),
    },
  }
}

test('ephemeral mode + project: first post registers mentions and thread:#<project>/general', async () => {
  const cap = captureSubscribes()
  const h = await buildHarness({
    ephemeral: true,
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject', 'home'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    expect(cap.tokens).toEqual([])
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'first attribution',
      session_id: 'a1aaa1aa-0000-4000-8000-000000000004',
    })
    // Order isn't load-bearing — only membership. The captured subscribes
    // here are exactly the onAcquire-time defaults; sticky-engagement
    // doesn't fire because the post has no thread.
    expect(new Set(cap.tokens)).toEqual(new Set(['mentions', 'thread:myproject/general']))
  } finally {
    await h.cleanup()
  }
})

test('ephemeral mode without project: first post registers only the universal mentions narrow', async () => {
  const cap = captureSubscribes()
  // No COMMY_PROJECT, no BOT_NAME. The post handler passes
  // session_id but no cwd, so the project resolver returns undefined
  // and the project broadcast topic is skipped.
  const h = await buildHarness({
    ephemeral: true,
    seedChannels: ['home'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'project-less attribution',
      session_id: 'b7a4ab07-0000-4000-8000-000000000005',
    })
    expect(cap.tokens).toEqual(['mentions'])
  } finally {
    await h.cleanup()
  }
})

test('ephemeral mode + project: current_identity (passive read) does NOT register defaults', async () => {
  const cap = captureSubscribes()
  const h = await buildHarness({
    ephemeral: true,
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    // current_identity is passive — never triggers acquire. Defaults
    // are wired to the post-acquire hook, so they should not fire here.
    const result = expectStructured(
      await callTool(h.client, 'current_identity', {
        session_id: 'c4a311ec-0000-4000-8000-000000000006',
      }),
    )
    expect(result['state']).toBe('unbound')
    expect(cap.tokens).toEqual([])
  } finally {
    await h.cleanup()
  }
})

test('ephemeral mode + project: repeat posts on same session_id register defaults exactly once', async () => {
  const cap = captureSubscribes()
  const h = await buildHarness({
    ephemeral: true,
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject', 'home'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    const sid = 'de17ade1-0000-4000-8000-000000000007'
    await callTool(h.client, 'post', { channel_name: 'home', body: 'one', session_id: sid })
    await callTool(h.client, 'post', { channel_name: 'home', body: 'two', session_id: sid })
    const counts = cap.tokens.reduce<Record<string, number>>((acc, t) => {
      acc[t] = (acc[t] ?? 0) + 1
      return acc
    }, {})
    expect(counts).toEqual({ mentions: 1, 'thread:myproject/general': 1 })
  } finally {
    await h.cleanup()
  }
})

test('ephemeral mode + project: distinct session_ids each register their own defaults', async () => {
  const cap = captureSubscribes()
  const h = await buildHarness({
    ephemeral: true,
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject', 'home'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'session A',
      session_id: 'ec40ec40-0000-4000-8000-000000000008',
    })
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'session B',
      session_id: 'f077e077-0000-4000-8000-000000000009',
    })
    // Two slots → onAcquire fires twice → universal mentions + project
    // broadcast topic added twice each (idempotent at the narrowSet
    // level, but the substrate-side call is repeated).
    const counts = cap.tokens.reduce<Record<string, number>>((acc, t) => {
      acc[t] = (acc[t] ?? 0) + 1
      return acc
    }, {})
    expect(counts).toEqual({ mentions: 2, 'thread:myproject/general': 2 })
  } finally {
    await h.cleanup()
  }
})

// ─── subscription persist + restore across ephemeral resume ─────

test('ephemeral subscribe persists the live narrow set (defaults + new sub) under the session_id', async () => {
  const writes: { sid: string; intents: ReadonlyArray<SubscribeIntent> }[] = []
  // The store resolves the id from the shared deferred the harness completes
  // when the subscribe call feeds its session_id — proving the snapshot is
  // keyed under that id with no id ever passed to write().
  const sessionIdDeferred = Deferred.unsafeMake<SessionIdValue>(FiberId.none)
  const subscriptionStore: SubscriptionStore = {
    read: () => Effect.succeed(Option.none()),
    write: (intents) =>
      Effect.flatMap(Deferred.await(sessionIdDeferred), (id) =>
        Effect.sync(() => {
          writes.push({ sid: id as string, intents })
        }),
      ),
  }
  const sid = '5b5c81b5-0000-4000-8000-0000000000a1'
  const h = await buildHarness({
    ephemeral: true,
    seedChannels: ['home'],
    subscriptionStore,
    sessionIdDeferred,
  })
  try {
    await callTool(h.client, 'subscribe', { target: 'channel:home', session_id: sid })
    await waitFor(() => writes.length > 0, 200)
    const last = writes.at(-1)
    expect(last?.sid).toBe(sid)
    // The snapshot is the full live set: the fresh-path Type-2 default
    // (mentions, no project) seeded before the write, plus the channel just
    // subscribed. So a later resume restores both.
    expect(new Set((last?.intents ?? []).map((i) => JSON.stringify(i)))).toEqual(
      new Set([
        JSON.stringify({ kind: 'mentions' }),
        JSON.stringify({ kind: 'channel', channelName: decodeChannelNameSync('home') }),
      ]),
    )
  } finally {
    await h.cleanup()
  }
})

test('ephemeral resume restores the persisted narrow set and does NOT re-apply Type-2 defaults', async () => {
  // A prior session persisted a single channel and had dropped the mentions
  // default. Resume must honour that exactly — restore channel:home, and never
  // re-add mentions (a dropped default stays dropped).
  const persisted: ReadonlyArray<SubscribeIntent> = [
    { kind: 'channel', channelName: decodeChannelNameSync('home') },
  ]
  const subscriptionStore: SubscriptionStore = {
    read: () => Effect.succeed(Option.some(persisted)),
    write: () => Effect.void,
  }
  const cap = captureSubscribes()
  const h = await buildHarness({
    ephemeral: true,
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject', 'home'],
    subscriptionStore,
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    // First acquiring call → onAcquire → restore. Even with a project set
    // (whose fresh path would seed mentions + thread:myproject/general), the
    // resume path restores the persisted set verbatim and skips the defaults.
    await callTool(h.client, 'post', {
      channel_name: 'home',
      body: 'resumed',
      session_id: '5e54e5e5-0000-4000-8000-0000000000a2',
    })
    await waitFor(() => cap.tokens.length > 0, 200)
    expect(new Set(cap.tokens)).toEqual(new Set(['channel:home']))
  } finally {
    await h.cleanup()
  }
})

// ─── channel/thread catch-up on persistent-mode boot ─────────────

test('persistent boot surfaces recent channel messages within the catch-up window', async () => {
  // Stub history.readChannel so we can control what the catch-up sees
  // without first having to seed messages through the publisher (which
  // mints fresh wall-clock timestamps that drift past any test window).
  const readChannelCalls: Array<{ channel: string; since: number | undefined }> = []
  const historyOverrides: Partial<HistoryReader> = {
    readChannel: (channel, range) =>
      Effect.sync(() => {
        readChannelCalls.push({ channel: channel as string, since: range.since })
        if (channel !== ('home' as unknown as ChannelName)) return []
        return [
          {
            ref: {
              id: decodeMessageIdSync('catchup-1'),
              channel: {
                id: decodeChannelIdSync('chan-home'),
                name: decodeChannelNameSync('home'),
                permalink: ChannelPermalinkSchema.make(
                  'https://zulip.example.com/#narrow/channel/chan-home-home',
                ),
              },
              thread: Option.none(),
              permalink: MessagePermalinkSchema.make(
                'https://zulip.example.com/#narrow/channel/chan-home-home/near/catchup-1',
              ),
            },
            sender: {
              id: decodeIdentityIdSync('user-carol'),
              name: decodeDisplayNameSync('carol'),
              kind: 'human' as const,
            },
            body: decodeMessageBodySync('morning concierge'),
            ts: decodeTimestampSync(4900),
            mentions: [],
            reactions: [],
          },
        ]
      }),
  }

  const before = Math.floor(Date.now() / 1000)
  const h = await buildHarness({
    seedChannels: ['home'],
    subscribe: 'channel:home',
    historyOverrides,
  })
  try {
    await waitFor(
      () => h.notifications.some((n) => n.method === 'notifications/claude/channel'),
      200,
    )
    const after = Math.floor(Date.now() / 1000)
    const channelNotifications = h.notifications.filter(
      (n) => n.method === 'notifications/claude/channel',
    )
    expect(channelNotifications).toHaveLength(1)
    const params = channelNotifications[0]?.params as {
      content: string
      meta: Record<string, string>
    }
    expect(params.content).toBe('morning concierge')
    expect(params.meta['message_id']).toBe('catchup-1')
    // The default window is 14400s (4h): the helper reads from
    // `now - 14400`, where `now` is the current Clock time in seconds.
    const DEFAULT_WINDOW_SECONDS = 14400
    expect(readChannelCalls).toHaveLength(1)
    expect(readChannelCalls[0]?.channel).toBe('home')
    expect(readChannelCalls[0]?.since).toBeGreaterThanOrEqual(before - DEFAULT_WINDOW_SECONDS)
    expect(readChannelCalls[0]?.since).toBeLessThanOrEqual(after - DEFAULT_WINDOW_SECONDS)
  } finally {
    await h.cleanup()
  }
})

// ─── Type-1 default sub set for project concierges ──────────────

test('persistent boot with COMMY_PROJECT registers Type-1 defaults at the substrate', async () => {
  const cap = captureSubscribes()
  const h = await buildHarness({
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject'],
    inboxOverrides: cap.inboxOverrides,
  })
  try {
    // Give the boot sequence time to settle — Type-1 fires post-acquire.
    await new Promise((r) => setTimeout(r, 50))
    // Order isn't load-bearing — only membership. Persistent mode has no
    // additional onAcquire defaults beyond Type-1 itself.
    expect(new Set(cap.tokens)).toEqual(
      new Set(['mentions', 'new-topics:myproject', 'thread:myproject/general']),
    )
  } finally {
    await h.cleanup()
  }
})

test('persistent boot Type-1 intents feed the channels catch-up (new-topics + thread/general history reads)', async () => {
  const readChannelCalls: Array<{ channel: string }> = []
  const readThreadCalls: Array<{ channel: string; thread: string }> = []
  const historyOverrides: Partial<HistoryReader> = {
    readChannel: (channel, _range) =>
      Effect.sync(() => {
        readChannelCalls.push({ channel: channel as string })
        return []
      }),
    readThread: (channel, thread, _range) =>
      Effect.sync(() => {
        readThreadCalls.push({ channel: channel as string, thread: thread as string })
        return []
      }),
  }
  const h = await buildHarness({
    env: { COMMY_PROJECT: 'myproject' },
    seedChannels: ['myproject'],
    historyOverrides,
  })
  try {
    await new Promise((r) => setTimeout(r, 50))
    // new-topics:myproject → readChannel('myproject'); thread:myproject/general → readThread.
    // The `mentions` Type-1 default is intentionally skipped by the channels
    // catch-up (the cursor-bounded mentions catch-up owns that path).
    expect(readChannelCalls).toEqual([{ channel: 'myproject' }])
    expect(readThreadCalls).toEqual([{ channel: 'myproject', thread: 'general' }])
  } finally {
    await h.cleanup()
  }
})

// ─── channel/thread catch-up on persistent-mode boot (continued) ─

test('persistent boot with no env subscriptions: no readChannel/readThread calls', async () => {
  const readChannelCalls: number[] = []
  const readThreadCalls: number[] = []
  const historyOverrides: Partial<HistoryReader> = {
    readChannel: (_channel, _range) =>
      Effect.sync(() => {
        readChannelCalls.push(1)
        return []
      }),
    readThread: (_channel, _thread, _range) =>
      Effect.sync(() => {
        readThreadCalls.push(1)
        return []
      }),
  }
  const h = await buildHarness({
    historyOverrides,
  })
  try {
    // Give the boot sequence time to settle — catch-up runs eagerly.
    await new Promise((r) => setTimeout(r, 50))
    expect(readChannelCalls).toEqual([])
    expect(readThreadCalls).toEqual([])
  } finally {
    await h.cleanup()
  }
})

// ─── Type-4 cron-shape boot (acquire → post → SIGTERM → release) ─

test('Type-4 cron-shape (no project): acquire-post-shutdown-release fires once with no leaks', async () => {
  // The cron / scheduled poster shape: persistent boot
  // path (`COMMY_BOT_NAME` set), one tool call, then shutdown.
  // The Type-4 design's no-new-code argument is "Type 1 covers it" —
  // this test verifies that end-to-end against the real boot program,
  // driving shutdown via the scope-unwinding `h.shutdown()` (the
  // in-test equivalent of the SIGTERM interrupt runMain applies).
  const logs: string[] = []
  const cap = captureSubscribes()

  const h = await buildHarness({
    seedChannels: ['home'],
    inboxOverrides: cap.inboxOverrides,
    capturedLogs: logs,
  })
  try {
    // Boot's eager acquire (persistent mode) ran before client.connect
    // resolved. Type-1 defaults registered post-acquire; without a
    // project slug, only the universal `mentions` narrow lands.
    expect(h.identityCalls.acquires).toEqual(['myproject-concierge'])
    expect(cap.tokens).toEqual(['mentions'])

    const posted = expectStructured(
      await callTool(h.client, 'post', { channel_name: 'home', body: 'cron tick' }),
    )
    expect(posted['channel_name']).toBe('home')

    // Verify the post landed in the channel — read_channel surfaces
    // the in-memory adapter's stored bucket.
    const reread = expectStructured(
      await callTool(h.client, 'read_channel', { channel_name: 'home' }),
    )
    expect((reread['messages'] as ReadonlyArray<{ body: string }>).map((m) => m.body)).toEqual([
      'cron tick',
    ])

    // Quick exit: host tears the program down after the print run
    // finishes. `shutdown()` awaits the scope unwind so the assertions
    // below are serialised behind release + close.
    await h.shutdown()

    expect(h.identityCalls.acquires).toEqual(['myproject-concierge'])
    expect(h.identityCalls.releases).toBe(1)
    expect(h.closes.count).toBe(1)
    // No leaked subscriptions: only the Type-1 universal mentions
    // narrow registered, and shutdown deliberately doesn't call
    // inbox.unsubscribe (release-shutdown.ts negative requirement).
    expect(cap.tokens).toEqual(['mentions'])
    // No error log lines from boot, post, or shutdown.
    expect(logs).toEqual([])
  } finally {
    await h.cleanup()
  }
})

test('Type-4 cron-shape (project-scoped): Type-1 default subs registered post-acquire, clean shutdown', async () => {
  // Project-scoped variant of the cron-shape boot. Also asserts the
  // Type-1 default sub set
  // (mentions + new-topics:<project> + thread:<project>/general) is
  // registered after the eager acquire. Catch-up window disabled so
  // the boot doesn't block on history reads — Type-4 runs are
  // short-lived and don't need the 4h skim.
  const logs: string[] = []
  const cap = captureSubscribes()

  const h = await buildHarness({
    env: {
      COMMY_PROJECT: 'myproject',
      COMMY_CATCHUP_WINDOW_SECONDS: '0',
    },
    seedChannels: ['myproject'],
    inboxOverrides: cap.inboxOverrides,
    capturedLogs: logs,
  })
  try {
    expect(h.identityCalls.acquires).toEqual(['myproject-concierge'])
    // Type-1 defaults landed at boot — order isn't load-bearing,
    // membership is.
    expect(new Set(cap.tokens)).toEqual(
      new Set(['mentions', 'new-topics:myproject', 'thread:myproject/general']),
    )

    const posted = expectStructured(
      await callTool(h.client, 'post', { channel_name: 'myproject', body: 'cron tick' }),
    )
    expect(posted['channel_name']).toBe('myproject')

    await h.shutdown()

    expect(h.identityCalls.acquires).toEqual(['myproject-concierge'])
    expect(h.identityCalls.releases).toBe(1)
    expect(h.closes.count).toBe(1)
    // Sticky-engagement only fires for thread posts; the
    // post above had no thread, so no extra subscribes beyond the
    // Type-1 default set.
    expect(new Set(cap.tokens)).toEqual(
      new Set(['mentions', 'new-topics:myproject', 'thread:myproject/general']),
    )
    expect(logs).toEqual([])
  } finally {
    await h.cleanup()
  }
})

// ─── Helpers ────────────────────────────────────────────────────────────────

const waitFor = async (predicate: () => boolean, timeoutMs: number): Promise<void> => {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return
    await new Promise((r) => setTimeout(r, 5))
  }
  if (!predicate()) {
    throw new Error(`waitFor: predicate did not become true within ${timeoutMs}ms`)
  }
}

// Describe a single filesystem entry by kind+size if it exists; null if not.
// Used to assert that a specific path is unchanged across the test window.
const describeFsEntry = (path: string): { kind: string; size: number } | null => {
  try {
    const s = statSync(path)
    if (s.isDirectory()) return { kind: 'dir', size: 0 }
    return { kind: 'file', size: s.size }
  } catch {
    return null
  }
}

// Recursive snapshot of the plugin directory, excluding noise subtrees that
// aren't plugin-managed state (node_modules churn, .git refs, lockfile
// touches). The tree is small and stable so this terminates quickly.
const PLUGIN_DIR_PRUNED = new Set(['node_modules', '.git', '.direnv'])
const PLUGIN_DIR_FILE_NOISE = new Set(['bun.lock', 'bun.lockb'])

const snapshotPluginDir = (
  root: string,
): ReadonlyArray<{ readonly path: string; readonly size: number }> => {
  const out: { path: string; size: number }[] = []
  const visit = (cur: string): void => {
    let names: ReadonlyArray<string>
    try {
      names = readdirSync(cur)
    } catch {
      return
    }
    for (const name of names) {
      const abs = join(cur, name)
      let s: Stats
      try {
        s = statSync(abs)
      } catch {
        continue
      }
      if (s.isDirectory()) {
        if (PLUGIN_DIR_PRUNED.has(name)) continue
        visit(abs)
      } else {
        if (PLUGIN_DIR_FILE_NOISE.has(name)) continue
        const rel = abs.substring(root.length + 1)
        out.push({ path: rel, size: s.size })
      }
    }
  }
  visit(root)
  return out.sort((a, b) => a.path.localeCompare(b.path))
}

// ─── Boot feeder (comms-k7cv.4): CLAUDE_CODE_SESSION_ID → session-id deferred ──
// Claude Code injects CLAUDE_CODE_SESSION_ID into the MCP child env at spawn
// (verified empirically on CC 2.1.201). The boot feeder mints a SessionId from
// it and fills the shared deferred at boot — with zero tool calls, the exact
// path a resumed listen-only seat needs. By the time `buildHarness` returns the
// server has connected (mcp.connect runs after the boot feeder), so the feed
// has already happened.
const BOOT_SID = 'f73f0ef0-1234-4abc-8def-000000000000'

test('boot feeder fills the session-id deferred from CLAUDE_CODE_SESSION_ID in the child env', async () => {
  const harness = await buildHarness({ env: { CLAUDE_CODE_SESSION_ID: BOOT_SID } })
  try {
    expect(await Effect.runPromise(Deferred.isDone(harness.sessionIdDeferred))).toBe(true)
    const value = await Effect.runPromise(Deferred.await(harness.sessionIdDeferred))
    expect(value).toBe(Option.getOrThrow(parseSessionId(BOOT_SID)))
  } finally {
    await harness.cleanup()
  }
})

test('boot feeder is a no-op when CLAUDE_CODE_SESSION_ID is absent — deferred stays unfilled', async () => {
  const harness = await buildHarness({ env: { CLAUDE_CODE_SESSION_ID: undefined } })
  try {
    expect(await Effect.runPromise(Deferred.isDone(harness.sessionIdDeferred))).toBe(false)
  } finally {
    await harness.cleanup()
  }
})
