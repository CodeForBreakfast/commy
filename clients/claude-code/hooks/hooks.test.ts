import { expect, test } from 'bun:test'

import hooks from './hooks.json'

const PLUGIN_SLUG = 'commy'
const EXPECTED_PREFIX = `mcp__plugin_${PLUGIN_SLUG}_${PLUGIN_SLUG}__`
const ATTRIBUTION_TOOLS = [['post'], ['react'], ['unreact'], ['current_identity']] as const

interface HookEntry {
  readonly matcher: string
  readonly hooks: ReadonlyArray<{
    readonly type: string
    readonly command: string
    readonly args?: ReadonlyArray<string>
  }>
}

interface HooksConfig {
  readonly hooks: {
    readonly PreToolUse?: ReadonlyArray<HookEntry>
  }
}

const preToolUse = (hooks as HooksConfig).hooks.PreToolUse ?? []

test('PreToolUse hook is registered', () => {
  expect(preToolUse.length).toBe(1)
})

test('matcher uses the doubled-prefix shape Claude Code emits for plugin-shipped MCP tools', () => {
  // Plugin-shipped MCP tool names are mcp__plugin_<plugin>_<plugin>__<tool>
  // (the plugin slug appears twice; the marketplace name does not appear).
  // The un-prefixed mcp__<name>__<tool> shape only matches plain stdio MCP
  // servers registered via `claude mcp add` — it does NOT match plugin tools.
  // ass-4umr: shipping the un-prefixed shape meant the hook never fired and
  // every ephemeral CC session errored on its first attribution call.
  const matcher = preToolUse[0]?.matcher
  expect(matcher).toBeDefined()
  expect(matcher).toStartWith(EXPECTED_PREFIX)
})

test('matcher compiles as a regex', () => {
  const matcher = preToolUse[0]?.matcher ?? ''
  expect(() => new RegExp(`^${matcher}$`)).not.toThrow()
})

test.each(ATTRIBUTION_TOOLS)('matcher matches the actual CC tool name for %s', (tool) => {
  const matcher = preToolUse[0]?.matcher ?? ''
  const re = new RegExp(`^${matcher}$`)
  expect(re.test(`${EXPECTED_PREFIX}${tool}`)).toBe(true)
})

test('matcher does NOT match arbitrary other MCP tools (no over-broad capture)', () => {
  const matcher = preToolUse[0]?.matcher ?? ''
  const re = new RegExp(`^${matcher}$`)
  expect(re.test('mcp__plugin_discord_discord__reply')).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}subscribe`)).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}list_agents`)).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}list_channels`)).toBe(false)
})

test('command points at the plugin-local bun wrapper, not bare `bun` on host PATH (comms-f9n)', () => {
  // Bare `bun` would fail in CC sessions where bun is not on the host PATH —
  // CC reports the hook error as non-blocking, the tool call proceeds with
  // unmodified tool_input, and no session_id injection happens. The wrapper
  // resolves bun via the plugin's own Nix flake (same provenance as the MCP
  // server's `nix run path:.#default`), so the hook works regardless of host PATH.
  const hook = preToolUse[0]?.hooks[0]
  // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
  expect(hook?.command).toBe('${CLAUDE_PLUGIN_ROOT}/hooks/bun-wrap.sh')
  // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
  expect(hook?.args).toEqual(['${CLAUDE_PLUGIN_ROOT}/hooks/inject-session-id.ts'])
})
