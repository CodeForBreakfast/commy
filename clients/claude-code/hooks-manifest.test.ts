import { expect, test } from 'bun:test'

import hooksManifest from './hooks/hooks.json'

/**
 * The PreToolUse hook in `hooks/hooks.json` injects `session_id` (and
 * `cwd`) into MCP tool args before the call reaches the server. Its
 * matcher is a hand-curated alternation over tool names. Every tool
 * whose handler binds an identity for `session_id` must be in the
 * alternation — otherwise the handler sees `session_id` undefined,
 * `readSessionId` falls back to ephemeral binding, and (comms-k7cv) the
 * session-id `Deferred` never gets fed from that call.
 *
 * Binding routes through one of two shapes: a direct
 * `identityCache.ensureBoundFor(...)` call, or the `ensureBoundForArgs(...)`
 * wrapper (comms-k7cv), which feeds the session-id deferred and then binds.
 * The parser attributes both to their enclosing tool.
 */

const BINDING_CALL_TOKENS = ['ensureBoundFor(', 'ensureBoundForArgs('] as const

function mintingToolsFromToolsSource(source: string): ReadonlySet<string> {
  const minting = new Set<string>()
  let currentName: string | undefined
  for (const line of source.split('\n')) {
    const named = line.match(/^\s*name:\s*['"]([a-z_]+)['"]/)
    const captured = named?.[1]
    if (captured !== undefined) currentName = captured
    if (currentName !== undefined && BINDING_CALL_TOKENS.some((token) => line.includes(token))) {
      minting.add(currentName)
    }
  }
  return minting
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

test('PreToolUse matcher covers every ensureBoundFor caller in tools.ts', async () => {
  const toolsSource = await Bun.file(Bun.resolveSync('@commy/mcp/tools', import.meta.dir)).text()
  const minting = mintingToolsFromToolsSource(toolsSource)
  const matched = alternationToolsFromMatcher(injectSessionIdMatcher(hooksManifest))

  const missingFromMatcher = [...minting].filter((n) => !matched.has(n)).sort()
  const matcherHasOrphans = [...matched].filter((n) => !minting.has(n)).sort()

  expect({ missingFromMatcher, matcherHasOrphans }).toEqual({
    missingFromMatcher: [],
    matcherHasOrphans: [],
  })
})

test('mintingToolsFromToolsSource attributes each ensureBoundFor call to the enclosing tool', () => {
  const synthetic = `
    {
      name: 'alpha',
      handler: async (args) => {
        await identityCache.ensureBoundFor(args)()
      },
    },
    {
      name: 'beta',
      handler: async () => {
        return {}
      },
    },
    {
      name: 'gamma',
      handler: async (args) => {
        const result = identityCache.ensureBoundFor(args).current()
        return result
      },
    },
  `
  expect(mintingToolsFromToolsSource(synthetic)).toEqual(new Set(['alpha', 'gamma']))
})

test('mintingToolsFromToolsSource attributes ensureBoundForArgs wrapper calls to the enclosing tool', () => {
  const synthetic = `
    const ensureBoundForArgs = (args) =>
      Effect.gen(function* () {
        yield* feedSession(readSessionId(args))
        return yield* identityCache.ensureBoundFor(readSessionId(args), yield* projectForArgs(args))
      })
    const coreTools = [
      {
        name: 'epsilon',
        handler: async (args) => {
          await runEdge(ensureBoundForArgs(args).pipe(Effect.flatMap((b) => b())))
        },
      },
      {
        name: 'zeta',
        handler: async (args) => {
          const b = await runEdge(ensureBoundForArgs(args))
          return b.current()
        },
      },
    ]
  `
  // The wrapper definition itself (before any name:) is not attributed; both
  // callers are — matching the real tools.ts shape.
  expect(mintingToolsFromToolsSource(synthetic)).toEqual(new Set(['epsilon', 'zeta']))
})

test('mintingToolsFromToolsSource ignores name properties whose value is not a string literal', () => {
  const synthetic = `
    const shape = { name: identity.name }
    const schema = { name: { type: 'string' } }
    {
      name: 'delta',
      handler: async () => { identityCache.ensureBoundFor() },
    },
  `
  expect(mintingToolsFromToolsSource(synthetic)).toEqual(new Set(['delta']))
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
