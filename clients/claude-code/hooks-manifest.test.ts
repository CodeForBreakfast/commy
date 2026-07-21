import { expect, test } from 'bun:test'

import hooksManifest from './hooks/hooks.json'

/**
 * The PreToolUse hook in `hooks/hooks.json` injects `session_id` (and `cwd`)
 * into MCP tool args before the call reaches the server. Its matcher is a
 * hand-curated alternation over tool names.
 *
 * WHAT THIS ASSERTS, and why it changed (comms-g5zh.1). It used to derive
 * "tools that need an identity" by scanning `tools.ts` for `ensureBoundFor(`
 * call sites — a hand-maintained trigger table in the tool layer. That table is
 * gone: the mint decision now lives at the adapter port, where reaching for a
 * bound credential (`boundHttp`) IS the declaration that an identity is needed.
 * So the derivation is traced from there instead, which is also what
 * `comms-tww6` specifies:
 *
 *   A TOOL WHOSE ADAPTER PATH REACHES `boundHttp` MUST DECLARE `session_id`
 *   AND BE IN THE `hooks.json` MATCHER.
 *
 * That is a stronger rule than the old one. The old test could only catch a
 * tool that called the wrapper and was missing from the matcher; it could not
 * see a tool that reached `boundHttp` while appearing in neither set — which is
 * exactly the live P1 that `comms-tww6` is open about.
 *
 * KNOWN VIOLATIONS ARE NAMED, NOT PAPERED OVER. Three tools violate the rule at
 * HEAD (see `TWW6_EXCEPTIONS`). Fixing them means adding `session_id` to their
 * schemas, which is `comms-tg70`'s ground and out of scope here. Encoding the
 * real rule with a visible exception list beats asserting a weaker rule that
 * passes: the day `comms-tww6` lands, its author deletes entries from that list
 * and this test proves the fix.
 */

/**
 * Publisher verbs whose adapter implementation reaches `boundHttp`. Pinned
 * rather than parsed: two of them reach it through shared helpers
 * (`setThreadResolved`, `setChannelDescription`), which no line-wise scan
 * resolves honestly. `adapterVerbsReachingBoundHttp` below guards the pin, so a
 * verb that joins or leaves the seam fails this suite rather than silently
 * widening the set a tool has to be stamped for.
 */
const BOUND_VERBS = ['post', 'edit', 'react', 'unreact'] as const

/**
 * Declarations in `packages/zulip/adapter.ts` that call `boundHttp()`. The two
 * helpers here back four port verbs: `setThreadResolved` implements
 * `resolveThread` / `unresolveThread`, and `setChannelDescription` is its own
 * verb.
 */
const BOUND_HTTP_CALLERS = [
  'edit',
  'post',
  'react',
  'setChannelDescription',
  'setThreadResolved',
  'unreact',
] as const

/**
 * Tools that reach `boundHttp` while declaring no `session_id` and sitting
 * outside the matcher — the open P1 `comms-tww6`. They run under whatever seat
 * an EARLIER call happened to bind, so their attribution is inherited by
 * accident of ordering rather than established by the call itself.
 *
 * Delete an entry here when that tool gains `session_id`; the assertion below
 * then holds it to the rule.
 */
const TWW6_EXCEPTIONS = ['resolve_thread', 'set_channel_description', 'unresolve_thread'] as const

/** Enclosing declaration names in the adapter source that call `boundHttp()`. */
function adapterVerbsReachingBoundHttp(source: string): ReadonlySet<string> {
  const reaching = new Set<string>()
  let current: string | undefined
  for (const line of source.split('\n')) {
    // `      post: (channel, body, opts?) => {` or `    const setThreadResolved = (`
    const named =
      line.match(/^ {6}([a-zA-Z]+): \(/)?.[1] ?? line.match(/^ {4}const ([a-zA-Z]+) = \(/)?.[1]
    if (named !== undefined) current = named
    // Skip prose: several comments name `boundHttp()` to explain why a path
    // deliberately does NOT use it.
    if (
      current !== undefined &&
      line.includes('boundHttp()') &&
      !line.trimStart().startsWith('//')
    ) {
      reaching.add(current)
    }
  }
  return reaching
}

interface ToolFacts {
  readonly verbs: ReadonlySet<string>
  readonly declaresSessionId: boolean
}

/** Per-tool: which publisher verbs its handler calls, and whether it declares `session_id`. */
function toolFactsFromToolsSource(source: string): ReadonlyMap<string, ToolFacts> {
  const facts = new Map<string, { verbs: Set<string>; declaresSessionId: boolean }>()
  let current: string | undefined
  for (const line of source.split('\n')) {
    const named = line.match(/^ {6}name: '([a-z_]+)',$/)?.[1]
    if (named !== undefined) {
      current = named
      facts.set(named, { verbs: new Set(), declaresSessionId: false })
    }
    const entry = current === undefined ? undefined : facts.get(current)
    if (entry === undefined) continue
    for (const verb of line.matchAll(/adapter\.publisher\.(\w+)/g)) {
      const captured = verb[1]
      if (captured !== undefined) entry.verbs.add(captured)
    }
    if (line.includes('session_id: sessionIdField')) entry.declaresSessionId = true
  }
  return new Map([...facts].map(([name, e]) => [name, { ...e, verbs: e.verbs }]))
}

function alternationToolsFromMatcher(matcher: string): ReadonlySet<string> {
  const captured = matcher.match(/\(([\w|]+)\)$/)?.[1]
  if (captured === undefined) {
    throw new Error(`matcher missing trailing alternation group: ${matcher}`)
  }
  return new Set(captured.split('|'))
}

interface PreToolUseEntry {
  readonly matcher: string
  readonly hooks: ReadonlyArray<{ readonly args?: ReadonlyArray<string> }>
}

function injectSessionIdMatcher(manifest: typeof hooksManifest): string {
  const entries: ReadonlyArray<PreToolUseEntry> = manifest.hooks.PreToolUse ?? []
  const found = entries.find((entry) =>
    entry.hooks.some((h) => (h.args ?? []).some((a) => a.includes('inject-session-id.ts'))),
  )
  if (found === undefined) {
    throw new Error('no PreToolUse hook found that runs inject-session-id.ts')
  }
  return found.matcher
}

const adapterSource = (): Promise<string> =>
  Bun.file(Bun.resolveSync('@commy/zulip/adapter', import.meta.dir)).text()

const toolsSource = (): Promise<string> =>
  Bun.file(Bun.resolveSync('@commy/mcp/tools', import.meta.dir)).text()

test('the set of adapter declarations reaching boundHttp is the pinned one', async () => {
  expect([...adapterVerbsReachingBoundHttp(await adapterSource())].sort()).toEqual([
    ...BOUND_HTTP_CALLERS,
  ])
})

test('every tool whose adapter path reaches boundHttp declares session_id', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const offenders = [...facts]
    .filter(
      ([, f]) =>
        [...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v)) &&
        !f.declaresSessionId,
    )
    .map(([name]) => name)
    .sort()
  expect(offenders).toEqual([])
})

test('every tool whose adapter path reaches boundHttp is in the PreToolUse matcher', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  const missing = [...facts]
    .filter(([, f]) => [...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v)))
    .map(([name]) => name)
    .filter((name) => !matched.has(name))
    .sort()
  expect(missing).toEqual([])
})

// The rule stated over ALL bound verbs, including the two helper-backed ones
// the tool layer never stamps. This is the assertion `comms-tww6` closes.
test('comms-tww6: the known unstamped bound-path tools are exactly the recorded exceptions', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const boundHttpVerbs = new Set(['resolveThread', 'unresolveThread', 'setChannelDescription'])
  const unstamped = [...facts]
    .filter(([, f]) => [...f.verbs].some((v) => boundHttpVerbs.has(v)) && !f.declaresSessionId)
    .map(([name]) => name)
    .sort()
  expect(unstamped).toEqual([...TWW6_EXCEPTIONS])
})

test('the matcher carries no tool that never reaches boundHttp and never binds', async () => {
  const facts = toolFactsFromToolsSource(await toolsSource())
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))
  // `current_identity` is in the matcher without reaching boundHttp: it reads
  // the binding passively and feeds the session-id deferred by asking. That is
  // a legitimate reason to be stamped, so it is named rather than derived.
  const orphans = [...matched]
    .filter((name) => name !== 'current_identity')
    .filter((name) => {
      const f = facts.get(name)
      return (
        f === undefined ||
        ![...f.verbs].some((v) => (BOUND_VERBS as ReadonlyArray<string>).includes(v))
      )
    })
    .sort()
  expect(orphans).toEqual([])
})

test('adapterVerbsReachingBoundHttp ignores commented-out mentions of boundHttp', () => {
  const synthetic = `
      editingAvailable: () =>
        // Read through the MINTER, never \`boundHttp()\`: sampled pre-acquire.
        minterHttp.post('/register'),
      react: (message, emoji) =>
        boundHttp().pipe(Effect.flatMap((http) => http.post('/x'))),
  `
  expect(adapterVerbsReachingBoundHttp(synthetic)).toEqual(new Set(['react']))
})

test('toolFactsFromToolsSource attributes verbs and session_id to the enclosing tool', () => {
  const synthetic = `
      name: 'alpha',
      inputSchema: {
        properties: { session_id: sessionIdField },
      },
      handler: async (args) => {
        await run(adapter.publisher.post(channel, body))
      },
      name: 'beta',
      handler: async () => {
        await run(adapter.history.readChannel(channel))
      },
  `
  const facts = toolFactsFromToolsSource(synthetic)
  expect(facts.get('alpha')).toEqual({ verbs: new Set(['post']), declaresSessionId: true })
  expect(facts.get('beta')).toEqual({ verbs: new Set(), declaresSessionId: false })
})

test('alternationToolsFromMatcher splits the trailing parenthesised group', () => {
  expect(alternationToolsFromMatcher('mcp__plugin_commy_commy__(post|edit_message)')).toEqual(
    new Set(['post', 'edit_message']),
  )
})

test('alternationToolsFromMatcher throws when the matcher has no alternation group', () => {
  expect(() => alternationToolsFromMatcher('mcp__plugin_commy_commy__post')).toThrow(
    /missing trailing alternation group/,
  )
})
