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
  // servers registered via `claude mcp add` — it does not match plugin tools.
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

test('matcher does not match arbitrary other MCP tools (no over-broad capture)', () => {
  const matcher = preToolUse[0]?.matcher ?? ''
  const re = new RegExp(`^${matcher}$`)
  expect(re.test('mcp__plugin_discord_discord__reply')).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}subscribe`)).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}list_agents`)).toBe(false)
  expect(re.test(`${EXPECTED_PREFIX}list_channels`)).toBe(false)
})

test('command invokes node on PATH against the hook entrypoint — no bun, no Nix wrapper', () => {
  // The plugin's prereq is `node` on PATH. The hook entrypoint imports no workspace
  // packages and uses only erasable TS syntax, so node runs the .ts directly
  // via native type-stripping (node ≥23.6) — nothing staged, no build artifact.
  const hook = preToolUse[0]?.hooks[0]
  expect(hook?.command).toBe('node')
  // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — testing placeholder rejection
  expect(hook?.args).toEqual(['${CLAUDE_PLUGIN_ROOT}/hooks/inject-session-id.ts'])
})
