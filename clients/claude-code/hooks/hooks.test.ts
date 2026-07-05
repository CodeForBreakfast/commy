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

interface McpToolHook {
  readonly type: string
  readonly server?: string
  readonly tool?: string
  readonly input?: Record<string, unknown>
}

interface UserPromptSubmitEntry {
  readonly hooks: ReadonlyArray<McpToolHook>
}

interface HooksConfig {
  readonly hooks: {
    readonly PreToolUse?: ReadonlyArray<HookEntry>
    readonly UserPromptSubmit?: ReadonlyArray<UserPromptSubmitEntry>
  }
}

const preToolUse = (hooks as HooksConfig).hooks.PreToolUse ?? []
const userPromptSubmit = (hooks as HooksConfig).hooks.UserPromptSubmit ?? []

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

// UserPromptSubmit feeds the session_id to the reactive restore latch at wake
// time, with zero agent action — this is what restores a listen-only resumed
// seat that never posts/reacts/reads. It fires an mcp_tool hook calling
// current_identity (a passive read that doubles as the latch feeder), templating
// the id straight from the hook's stdin so nothing depends on the model.

test('UserPromptSubmit hook is registered', () => {
  expect(userPromptSubmit.length).toBe(1)
})

test('UserPromptSubmit fires an mcp_tool hook', () => {
  const hook = userPromptSubmit[0]?.hooks[0]
  expect(hook?.type).toBe('mcp_tool')
})

test('the mcp_tool hook targets the commy server by its configured name (not the tool-name namespace)', () => {
  // The mcp_tool `server` field is the raw server name from .mcp.json — `commy`.
  // The doubled `plugin_commy_commy` prefix is the tool-NAME namespace (used by
  // the PreToolUse matcher), not the server identifier for an mcp_tool hook.
  const hook = userPromptSubmit[0]?.hooks[0]
  expect(hook?.server).toBe('commy')
})

test('the mcp_tool hook calls current_identity — the passive feeder', () => {
  const hook = userPromptSubmit[0]?.hooks[0]
  expect(hook?.tool).toBe('current_identity')
})

test('the mcp_tool hook templates session_id and cwd from the hook stdin', () => {
  const hook = userPromptSubmit[0]?.hooks[0]
  expect(hook?.input).toEqual({
    // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — CC ${path} interpolation from hook stdin
    session_id: '${session_id}',
    // biome-ignore lint/suspicious/noTemplateCurlyInString: intentional — CC ${path} interpolation from hook stdin
    cwd: '${cwd}',
  })
})
