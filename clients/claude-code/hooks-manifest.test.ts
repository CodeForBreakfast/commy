import { expect, test } from 'bun:test'

import hooksManifest from './hooks/hooks.json'

/**
 * The PreToolUse hook in `hooks/hooks.json` injects `session_id` (and
 * `cwd`) into MCP tool args before the call reaches the server. Its
 * matcher is a hand-curated alternation over tool names, and the invariant
 * is a declaration rather than an inference: **a tool that advertises
 * `session_id` in its input schema must be in the alternation.** A tool
 * outside it sees `session_id` undefined on every Claude Code call, so
 * `readSessionId` yields nothing and (comms-k7cv) the session-id `Deferred`
 * is never fed from that call — while the schema tells every agent the
 * argument is honoured.
 *
 * Declaring the field and consuming it are the same thing here: each such
 * tool either binds (`ensureBoundFor` / the `ensureBoundForArgs` wrapper)
 * or feeds (`readSessionId` + `feedSession`). Pinning on the schema
 * declaration covers both, which pinning on the binding call alone did not
 * — `subscribe` and `unsubscribe` feed without binding, and sat outside the
 * matcher unnoticed (comms-65nj).
 *
 * This is the only pin that can see the break. On our own fleet the boot-env
 * feeder fills the `Deferred` at boot, so a Claude Code seat mints correctly
 * whether or not its tool is matched; the failure is only visible to a host
 * that injects no zero-action id source (comms-4ji2). Nothing behavioural
 * can catch it here — the matcher set itself is the artefact under test.
 */

const SESSION_ID_FIELD_TOKEN = 'session_id: sessionIdField'

function sessionIdToolsFromToolsSource(source: string): ReadonlySet<string> {
  const declaring = new Set<string>()
  let currentName: string | undefined
  for (const line of source.split('\n')) {
    const named = line.match(/^\s*name:\s*['"]([a-z_]+)['"]/)
    const captured = named?.[1]
    if (captured !== undefined) currentName = captured
    if (currentName !== undefined && line.includes(SESSION_ID_FIELD_TOKEN)) {
      declaring.add(currentName)
    }
  }
  return declaring
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

test('PreToolUse matcher covers every tool declaring session_id in tools.ts', async () => {
  const toolsSource = await Bun.file(Bun.resolveSync('@commy/mcp/tools', import.meta.dir)).text()
  const declaring = sessionIdToolsFromToolsSource(toolsSource)
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))

  const missingFromMatcher = [...declaring].filter((n) => !matched.has(n)).sort()
  const matcherHasOrphans = [...matched].filter((n) => !declaring.has(n)).sort()

  expect({ missingFromMatcher, matcherHasOrphans }).toEqual({
    missingFromMatcher: [],
    matcherHasOrphans: [],
  })
})

test('sessionIdToolsFromToolsSource attributes each schema declaration to the enclosing tool', () => {
  const synthetic = `
    {
      name: 'alpha',
      inputSchema: {
        properties: { session_id: sessionIdField, cwd: cwdField },
      },
    },
    {
      name: 'beta',
      inputSchema: {
        properties: { target: { type: 'string' } },
      },
    },
    {
      name: 'gamma',
      inputSchema: {
        properties: {
          target: { type: 'string' },
          session_id: sessionIdField,
          cwd: cwdField,
        },
      },
    },
  `
  expect(sessionIdToolsFromToolsSource(synthetic)).toEqual(new Set(['alpha', 'gamma']))
})

test('sessionIdToolsFromToolsSource sees a tool that feeds the deferred without binding', () => {
  // The shape that escaped the old binding-call pin (comms-65nj): the handler
  // reads and feeds the session id but never calls ensureBoundFor.
  const synthetic = `
    {
      name: 'subscribe',
      inputSchema: {
        properties: { target: { type: 'string' }, session_id: sessionIdField },
      },
      handler: async (args) => {
        yield* feedSession(readSessionId(args))
      },
    },
  `
  expect(sessionIdToolsFromToolsSource(synthetic)).toEqual(new Set(['subscribe']))
})

test('sessionIdToolsFromToolsSource ignores name properties whose value is not a string literal', () => {
  const synthetic = `
    const shape = { name: identity.name }
    const schema = { name: { type: 'string' } }
    {
      name: 'delta',
      inputSchema: { properties: { session_id: sessionIdField } },
    },
  `
  expect(sessionIdToolsFromToolsSource(synthetic)).toEqual(new Set(['delta']))
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
