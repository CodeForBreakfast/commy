/**
 * The Claude Code production configuration, exercised end to end (comms-hsym).
 *
 * WHY THIS FILE EXISTS. Every other identity test in this suite asserts one
 * half of the supply chain against a model of the other half:
 *
 *   - `tools-session.test.ts` runs the real ephemeral cache and the real
 *     adapter, but hand-writes `session_id` into `callTool` arguments. It
 *     asserts the seam works GIVEN the argument arrives.
 *   - `hooks-manifest.test.ts` regex-scans `hooks.json`, `tools.ts` and
 *     `adapter.ts` and asserts they agree on paper. It executes nothing.
 *
 * Nothing composes them, so a change can be exhaustively green while the
 * chain that actually delivers an identity to a fleet seat is broken. That is
 * how #197 went 1182-green while changing what three tools do in production.
 *
 * WHAT THIS FILE DOES DIFFERENTLY. It does not construct the mechanism it is
 * asking about. Every link is the shipped artefact:
 *
 *   - the matcher and the hook command come from the real
 *     `clients/claude-code/hooks/hooks.json`;
 *   - the injection is performed by SPAWNING the real
 *     `hooks/inject-session-id.ts` with the manifest's own interpreter, fed a
 *     genuine PreToolUse event on stdin;
 *   - the server is the real `makeProgram`, whose type signature forces this
 *     rig to supply exactly production's requirement set — there is no
 *     friendlier subset available to reach for;
 *   - ephemeral mode is expressed the way deployment expresses it: by the
 *     ABSENCE of `COMMY_BOT_NAME` from the child env. `server.ts` decides
 *     which cache to build; this file does not name a cache constructor.
 *
 * The only substitution is the substrate itself (memory for Zulip), and the
 * memory adapter reaches `requireBound()` on the same six verbs the Zulip
 * adapter reaches `boundHttp()` on — so the bind decision observed here is the
 * deployed one, not a softer one.
 *
 * THE NEGATIVE CONTROLS ARE THE POINT. A green suite that would stay green
 * against severed wiring is the exact failure this file exists to end, so the
 * supply channels are severed on purpose below and the calls are asserted to
 * REFUSE. If someone deletes the matcher entry, or the hook stops emitting
 * `updatedInput`, those tests are what notice.
 */
import { expect, test } from 'bun:test'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { captureLogger } from '@commy/core/logging'
import { memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Deferred, Effect, FiberId, Layer, Option, Predicate, Ref, Stream } from 'effect'
import { parseEnv, substrateAdapterLayer } from './bootstrap.ts'
import { CursorStoreTag } from './cursor-store.ts'
import { completeAsSubstrate } from './memory-substrate.ts'
import { ResumeOutcome as ResumeOutcomeTag } from './resume-outcome.ts'
import { makeProgram } from './server.ts'
import type { BindOnDemand } from './session-binder.ts'
import { bindThrough, SessionBinder as SessionBinderTag } from './session-binder.ts'
import { SessionId as SessionIdTag, type SessionIdValue } from './session-id.ts'
import { SubscriptionStoreTag } from './subscription-store.ts'
import { testBootStoresLayer, testPlatformLayer } from './test-platform.ts'

const REPO_ROOT = join(import.meta.dir, '..', '..')
const PLUGIN_ROOT = join(REPO_ROOT, 'clients', 'claude-code')
const HOOKS_MANIFEST_PATH = join(PLUGIN_ROOT, 'hooks', 'hooks.json')

/** Claude Code namespaces a plugin's MCP tools; the matchers are written against this shape. */
const toolNameAsClaudeCodeSeesIt = (tool: string): string => `mcp__plugin_commy_commy__${tool}`

// UUID-shaped session ids: the SessionId brand demands UUID format, and the
// leading eight hex characters become the bot-name suffix.
const SID_CONVERSATION = 'aaaaaaaa-0000-4000-8000-000000000001'
const SID_AFTER_CLEAR = 'bbbbbbbb-0000-4000-8000-000000000002'

interface HookCommand {
  readonly type: string
  readonly command: string
  readonly args?: ReadonlyArray<string>
}

interface PreToolUseEntry {
  readonly matcher: string
  readonly hooks: ReadonlyArray<HookCommand>
}

interface HooksManifest {
  readonly hooks: { readonly PreToolUse?: ReadonlyArray<PreToolUseEntry> }
}

const readShippedHooksManifest = async (): Promise<HooksManifest> =>
  JSON.parse(await Bun.file(HOOKS_MANIFEST_PATH).text()) as HooksManifest

/** The PreToolUse event Claude Code pipes to a hook's stdin. */
interface PreToolUseEvent {
  readonly session_id?: string
  readonly cwd?: string
  readonly tool_name: string
  readonly tool_input: Readonly<Record<string, unknown>>
}

/**
 * Reproduce Claude Code's PreToolUse stage: select the hooks whose matcher
 * matches the tool name, run each one, and let `updatedInput` replace the
 * arguments that go on the wire. What a tool receives is decided entirely by
 * the manifest and the hook executable — nothing here knows what
 * `session_id` is for.
 */
const argumentsAfterPreToolUse = async (
  manifest: HooksManifest,
  event: PreToolUseEvent,
): Promise<Record<string, unknown>> => {
  let toolInput: Record<string, unknown> = { ...event.tool_input }
  for (const entry of manifest.hooks.PreToolUse ?? []) {
    if (!new RegExp(entry.matcher).test(event.tool_name)) continue
    for (const hook of entry.hooks) {
      const argv = [
        hook.command,
        // biome-ignore lint/suspicious/noTemplateCurlyInString: the literal placeholder Claude Code substitutes in a hook manifest
        ...(hook.args ?? []).map((arg) => arg.replaceAll('${CLAUDE_PLUGIN_ROOT}', PLUGIN_ROOT)),
      ]
      const proc = Bun.spawn(argv, { stdin: 'pipe', stdout: 'pipe', stderr: 'pipe' })
      proc.stdin.write(JSON.stringify({ ...event, tool_input: toolInput }))
      await proc.stdin.end()
      const [stdout, stderr, exitCode] = await Promise.all([
        new Response(proc.stdout).text(),
        new Response(proc.stderr).text(),
        proc.exited,
      ])
      if (exitCode !== 0) {
        throw new Error(`PreToolUse hook \`${argv.join(' ')}\` exited ${exitCode}: ${stderr}`)
      }
      const parsed = JSON.parse(stdout) as {
        hookSpecificOutput?: { updatedInput?: Record<string, unknown> }
      }
      const updated = parsed.hookSpecificOutput?.updatedInput
      if (updated !== undefined) toolInput = updated
    }
  }
  return toolInput
}

/**
 * The MCP child's environment as a fleet seat launches it. `COMMY_BOT_NAME` is
 * absent, which is how an operator asks for ephemeral mode; and
 * `CLAUDE_CODE_SESSION_ID` is absent, which is how Claude Code launches the
 * child — the harness id reaches the server per call through the hook, not
 * once at boot. Between them these two absences ARE the configuration under
 * test.
 */
const CLAUDE_CODE_CHILD_ENV: Record<string, string | undefined> = {
  ZULIP_SITE: 'https://zulip.example.com',
  ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
  ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
}

interface ToolCallResult {
  readonly structuredContent?: unknown
  readonly isError?: boolean
}

interface DeployedSeat {
  /**
   * Call a tool the way a Claude Code seat calls it: the model supplies only
   * the arguments it can see, the PreToolUse stage adds whatever the shipped
   * manifest and hook decide to add, and the result goes to the server.
   */
  readonly callAsClaudeCode: (
    tool: string,
    modelArgs?: Record<string, unknown>,
    session?: { readonly sessionId?: string; readonly cwd?: string },
  ) => Promise<ToolCallResult>
  /** The tool names this child advertises — the surface a seat actually sees. */
  readonly listTools: () => Promise<ReadonlyArray<string>>
  /** The name the substrate currently holds a binding for, read below the tool layer. */
  readonly boundName: () => Promise<string | undefined>
  readonly shutdown: () => Promise<void>
}

/**
 * Boot a real MCP child in the deployed configuration and give it a client
 * that goes through the PreToolUse stage.
 *
 * `manifest` is a parameter so the negative controls can sever a supply
 * channel and re-run the same assertions; every positive test passes the
 * shipped one.
 */
const bootDeployedSeat = async (
  manifest: HooksManifest,
  seedChannels: ReadonlyArray<string> = ['home'],
): Promise<DeployedSeat> => {
  const binderRef = await Effect.runPromise(Ref.make<Option.Option<BindOnDemand>>(Option.none()))
  const base = await Effect.runPromise(memoryAdapter({ bindOnDemand: bindThrough(binderRef) }))
  for (const name of seedChannels) {
    await Effect.runPromise(base.seedChannel(name).pipe(Effect.orDie))
  }

  // Unwinding the program means interrupting the pump's stream, exactly as a
  // SIGTERM does under runMain; the memory adapter's `events()` otherwise
  // parks forever, like production's long poll.
  const killSwitch = Deferred.unsafeMake<void>(FiberId.none)
  const adapter = completeAsSubstrate({
    ...base,
    inbox: {
      ...base.inbox,
      events: () => base.inbox.events().pipe(Stream.interruptWhenDeferred(killSwitch)),
    },
  })

  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
  const client = new Client(
    { name: 'commy-deployed-wiring-test', version: '0.0.0' },
    { capabilities: {} },
  )

  // The memory adapter never fires the resume-outcome hook, so the deferred the
  // ephemeral `onAcquire` awaits is pre-resolved; leaving it open would park the
  // first acquire rather than exercise it.
  const resumeOutcome = Deferred.unsafeMake<boolean>(FiberId.none)
  Deferred.unsafeDone(resumeOutcome, Effect.succeed(false))
  const sessionIdDeferred = Deferred.unsafeMake<SessionIdValue>(FiberId.none)
  const inMemoryCursorStore = {
    read: () => Effect.succeed(Option.none()),
    write: () => Effect.void,
  }
  const inMemorySubscriptionStore = {
    read: () => Effect.succeed(Option.none()),
    write: () => Effect.void,
  }

  const runExit = Effect.runPromiseExit(
    makeProgram({ transport: serverTransport, loggerLayer: captureLogger([]) }).pipe(
      Effect.provide(
        Layer.provideMerge(
          Layer.mergeAll(
            substrateAdapterLayer(parseEnv.pipe(Effect.as(adapter))),
            Layer.succeed(CursorStoreTag, inMemoryCursorStore),
            Layer.succeed(SubscriptionStoreTag, inMemorySubscriptionStore),
            Layer.succeed(SessionIdTag, sessionIdDeferred),
            Layer.succeed(ResumeOutcomeTag, resumeOutcome),
            Layer.succeed(SessionBinderTag, binderRef),
            testBootStoresLayer(),
            captureLogger([]),
          ),
          testPlatformLayer(CLAUDE_CODE_CHILD_ENV),
        ),
      ),
    ),
  )

  await client.connect(clientTransport)

  return {
    callAsClaudeCode: async (tool, modelArgs = {}, session = {}) => {
      const toolName = toolNameAsClaudeCodeSeesIt(tool)
      const args = await argumentsAfterPreToolUse(manifest, {
        ...(session.sessionId !== undefined ? { session_id: session.sessionId } : {}),
        ...(session.cwd !== undefined ? { cwd: session.cwd } : {}),
        tool_name: toolName,
        tool_input: modelArgs,
      })
      return (await client.callTool({ name: tool, arguments: args })) as ToolCallResult
    },
    listTools: async () => (await client.listTools()).tools.map((tool) => tool.name),
    boundName: async () => {
      const exit = await Effect.runPromiseExit(base.identity.currentIdentity())
      return exit._tag === 'Success' ? (exit.value.name as string) : undefined
    },
    shutdown: async () => {
      Deferred.unsafeDone(killSwitch, Effect.void)
      await client.close()
      await runExit
    },
  }
}

/**
 * The message of a refused tool call. The MCP client surfaces a server-side
 * refusal by throwing, so a test that only checked `isError` would pass
 * whatever happened; this insists the call did not succeed AND hands back the
 * text, so each caller can pin WHICH refusal it got rather than merely that
 * something went wrong.
 */
const refusalMessage = async (call: Promise<ToolCallResult>): Promise<string> => {
  const outcome = await call.then(
    (result) => ({ refused: false as const, result }),
    (error: unknown) => ({
      refused: true as const,
      message: Predicate.isError(error) ? error.message : String(error),
    }),
  )
  if (!outcome.refused) {
    throw new Error(`expected a refusal, but the call succeeded: ${JSON.stringify(outcome.result)}`)
  }
  return outcome.message
}

/** A directory that is deliberately not a git checkout, so the project probe finds nothing. */
const nonRepoCwd = (): { readonly path: string; readonly remove: () => void } => {
  const path = mkdtempSync(join(tmpdir(), 'commy-deployed-'))
  return { path, remove: () => rmSync(path, { recursive: true, force: true }) }
}

/** The shipped manifest with `post` removed from the alternation — one channel severed. */
const manifestWithoutPostInMatcher = (manifest: HooksManifest): HooksManifest => ({
  hooks: {
    PreToolUse: (manifest.hooks.PreToolUse ?? []).map((entry) => ({
      ...entry,
      matcher: entry.matcher.replace('post|', ''),
    })),
  },
})

test('the shipped hook injects session_id and cwd into the arguments a matched tool receives', async () => {
  const manifest = await readShippedHooksManifest()
  const args = await argumentsAfterPreToolUse(manifest, {
    session_id: SID_CONVERSATION,
    cwd: '/home/x/myproject',
    tool_name: toolNameAsClaudeCodeSeesIt('post'),
    tool_input: { channel_name: 'home', body: 'hello' },
  })
  expect(args).toEqual({
    channel_name: 'home',
    body: 'hello',
    session_id: SID_CONVERSATION,
    cwd: '/home/x/myproject',
  })
})

test('post through the shipped hook chain mints this conversation its own seat', async () => {
  const cwd = nonRepoCwd()
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  try {
    const result = await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'first attribution' },
      { sessionId: SID_CONVERSATION, cwd: cwd.path },
    )
    expect(result.isError).toBeFalsy()
    expect(await seat.boundName()).toBe('cc-aaaaaaaa')
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

test('the injected cwd names the seat after the calling session project, not the plugin', async () => {
  const cwd = nonRepoCwd()
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  try {
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'from inside a checkout' },
      { sessionId: SID_CONVERSATION, cwd: REPO_ROOT },
    )
    // The project component is whatever this checkout's git context yields, so
    // it is not pinned by name; that it is PRESENT is the claim, and it is
    // absent for the non-repo cwd below.
    expect(await seat.boundName()).toMatch(/^cc-.+-aaaaaaaa$/)
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

test('current_identity through the shipped hook chain reads this seat back', async () => {
  const cwd = nonRepoCwd()
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  try {
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'mint trigger' },
      { sessionId: SID_CONVERSATION, cwd: cwd.path },
    )
    const read = await seat.callAsClaudeCode(
      'current_identity',
      {},
      {
        sessionId: SID_CONVERSATION,
        cwd: cwd.path,
      },
    )
    expect(read.structuredContent).toMatchObject({
      state: 'bound',
      identity: { name: 'cc-aaaaaaaa' },
    })
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

test('a fresh conversation gets a fresh seat rather than inheriting the last one', async () => {
  const cwd = nonRepoCwd()
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  try {
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'before /clear' },
      { sessionId: SID_CONVERSATION, cwd: cwd.path },
    )
    expect(await seat.boundName()).toBe('cc-aaaaaaaa')
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'after /clear' },
      { sessionId: SID_AFTER_CLEAR, cwd: cwd.path },
    )
    expect(await seat.boundName()).toBe('cc-bbbbbbbb')
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

/**
 * The three verbs `comms-tww6` is open about, observed rather than argued
 * about. They reach the substrate's bind seam but sit outside the matcher, so
 * the hook never runs for them and no `session_id` reaches the server. Under
 * the deployed configuration they refuse — a typed refusal, not silent
 * attribution to whatever seat an earlier call happened to bind.
 *
 * This is a characterisation of HEAD, not an endorsement: whether they should
 * instead be stamped is `comms-tww6`'s fork. When that lands, this test is
 * what has to be rewritten, deliberately.
 */
test('the tww6 verbs are outside the matcher, so the deployed configuration refuses them', async () => {
  const cwd = nonRepoCwd()
  const manifest = await readShippedHooksManifest()
  const seat = await bootDeployedSeat(manifest)
  try {
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'seed', thread: 'a-topic' },
      { sessionId: SID_CONVERSATION, cwd: cwd.path },
    )
    for (const tool of ['resolve_thread', 'unresolve_thread'] as const) {
      const args = await argumentsAfterPreToolUse(manifest, {
        session_id: SID_CONVERSATION,
        cwd: cwd.path,
        tool_name: toolNameAsClaudeCodeSeesIt(tool),
        tool_input: { channel_name: 'home', thread: 'a-topic' },
      })
      expect(args).not.toHaveProperty('session_id')
      const refusal = await refusalMessage(
        seat.callAsClaudeCode(tool, { channel_name: 'home', thread: 'a-topic' }),
      )
      expect(refusal).toContain('UnboundEphemeralSession')
    }
    const description = await refusalMessage(
      seat.callAsClaudeCode('set_channel_description', {
        channel_name: 'home',
        description: 'a new description',
      }),
    )
    expect(description).toContain('UnboundEphemeralSession')
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

/**
 * Every tool this plugin ships, driven through the deployed chain, classified
 * by the only question this file is about: did the call fail FOR WANT OF AN
 * IDENTITY?
 *
 * A tool is `usable` if it did not. It may still fail for its own reasons
 * (an unknown name, a missing file) — that is the tool's domain behaviour and
 * belongs to whichever test owns it. A tool is `refused` if it came back with
 * the typed unbound refusal, meaning the deployed configuration cannot get it
 * an identity at all.
 *
 * THE TABLE IS THE INVARIANT the bead asks for: a tool that works on this
 * fleet today must still work after the change. Flip any `usable` to `refused`
 * and this fails — which is precisely the class of surprise that reached
 * production once already while everything else stayed green. Add a tool and
 * this fails too, until someone states what the deployed configuration does
 * with it.
 */
type DeployedOutcome = 'usable' | 'refused-for-want-of-identity'

const DEPLOYED_OUTCOMES: Readonly<Record<string, DeployedOutcome>> = {
  current_identity: 'usable',
  resolve: 'usable',
  list_agents: 'usable',
  list_humans: 'usable',
  list_channels: 'usable',
  presence: 'usable',
  read_channel: 'usable',
  read_thread: 'usable',
  get_channel_description: 'usable',
  message_link: 'usable',
  subscribe: 'usable',
  unsubscribe: 'usable',
  post: 'usable',
  edit_message: 'usable',
  react: 'usable',
  unreact: 'usable',
  download_file: 'usable',
  upload_file: 'usable',
  // The three `comms-tww6` is open about: they reach the substrate's bind seam
  // but sit outside the PreToolUse matcher, so no session id ever reaches the
  // server on their behalf. Recorded as observed, not as endorsed — when
  // `comms-tww6` is decided, these three entries are what changes.
  resolve_thread: 'refused-for-want-of-identity',
  unresolve_thread: 'refused-for-want-of-identity',
  set_channel_description: 'refused-for-want-of-identity',
}

test('every shipped tool, driven through the deployed chain, lands where the table says', async () => {
  const cwd = nonRepoCwd()
  const uploadable = join(cwd.path, 'attachment.txt')
  await Bun.write(uploadable, 'contents')
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  const session = { sessionId: SID_CONVERSATION, cwd: cwd.path }
  try {
    // Seed the state the sweep addresses: a bound seat, a message, a thread.
    const seeded = (await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'seed', thread: 'a-topic' },
      session,
    )) as { structuredContent?: { message_id?: string } }
    const messageId = seeded.structuredContent?.message_id
    expect(messageId).toBeDefined()
    const self = (await seat.callAsClaudeCode('current_identity', {}, session)) as {
      structuredContent?: { identity?: { id?: string } }
    }
    const selfId = self.structuredContent?.identity?.id
    expect(selfId).toBeDefined()

    const argsFor: Readonly<Record<string, Record<string, unknown>>> = {
      current_identity: {},
      resolve: { name: 'nobody-in-particular' },
      list_agents: {},
      list_humans: {},
      list_channels: {},
      presence: { identity_id: selfId },
      read_channel: { channel_name: 'home' },
      read_thread: { channel_name: 'home', thread: 'a-topic' },
      get_channel_description: { channel_name: 'home' },
      message_link: { message_id: messageId },
      subscribe: { target: 'home' },
      unsubscribe: { target: 'home' },
      post: { channel_name: 'home', body: 'swept' },
      edit_message: { message_id: messageId, body: 'edited by the sweep' },
      react: { message_id: messageId, emoji: 'check' },
      unreact: { message_id: messageId, emoji: 'check' },
      download_file: { url_path: '/user_uploads/0/stub' },
      upload_file: { path: uploadable },
      resolve_thread: { channel_name: 'home', thread: 'a-topic' },
      unresolve_thread: { channel_name: 'home', thread: 'a-topic' },
      set_channel_description: { channel_name: 'home', description: 'swept' },
    }

    const listed = await seat.listTools()
    expect(listed.slice().sort()).toEqual(Object.keys(DEPLOYED_OUTCOMES).sort())

    const observed: Record<string, DeployedOutcome> = {}
    for (const tool of listed) {
      const outcome = await seat.callAsClaudeCode(tool, argsFor[tool], session).then(
        (): DeployedOutcome => 'usable',
        (error: unknown): DeployedOutcome =>
          (Predicate.isError(error) ? error.message : String(error)).includes(
            'UnboundEphemeralSession',
          )
            ? 'refused-for-want-of-identity'
            : 'usable',
      )
      observed[tool] = outcome
    }
    expect(observed).toEqual(DEPLOYED_OUTCOMES)
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

// ---------------------------------------------------------------------------
// Negative controls: sever one supply channel at a time and require a refusal.
// A green above that survives either of these is the assurance-that-asserts-
// nothing this file exists to prevent.
// ---------------------------------------------------------------------------

test('severing the matcher: post outside the alternation gets no identity and refuses', async () => {
  const cwd = nonRepoCwd()
  const severed = manifestWithoutPostInMatcher(await readShippedHooksManifest())
  const seat = await bootDeployedSeat(severed)
  try {
    const refusal = await refusalMessage(
      seat.callAsClaudeCode(
        'post',
        { channel_name: 'home', body: 'never sent' },
        { sessionId: SID_CONVERSATION, cwd: cwd.path },
      ),
    )
    expect(refusal).toContain('UnboundEphemeralSession')
    expect(await seat.boundName()).toBeUndefined()
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})

test('severing the harness feed: a host that supplies no session id gets a refusal, not a borrowed seat', async () => {
  const cwd = nonRepoCwd()
  const seat = await bootDeployedSeat(await readShippedHooksManifest())
  try {
    await seat.callAsClaudeCode(
      'post',
      { channel_name: 'home', body: 'this conversation bound a seat' },
      { sessionId: SID_CONVERSATION, cwd: cwd.path },
    )
    expect(await seat.boundName()).toBe('cc-aaaaaaaa')
    // Same child, same hook, but the harness event carries no session_id — the
    // hook passes through without `updatedInput`.
    const refusal = await refusalMessage(
      seat.callAsClaudeCode(
        'post',
        { channel_name: 'home', body: 'never sent' },
        { cwd: cwd.path },
      ),
    )
    expect(refusal).toContain('UnboundEphemeralSession')
    // And the refusal must not have disturbed the seat that legitimately bound.
    expect(await seat.boundName()).toBe('cc-aaaaaaaa')
  } finally {
    await seat.shutdown()
    cwd.remove()
  }
})
