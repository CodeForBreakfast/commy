#!/usr/bin/env node
/**
 * PreToolUse hook for the commy plugin (ass-2dhb + ass-v7b4).
 *
 * Claude Code invokes this script before any matching tool call
 * (the plugin-namespaced `mcp__plugin_commy_commy__*`
 * shape — see ass-4umr) and pipes the event JSON to stdin. The
 * script:
 *
 *   1. Reads `session_id` and `cwd` from the event.
 *   2. Emits `hookSpecificOutput.updatedInput` with `session_id`
 *      AND `cwd` merged into the tool's `tool_input`. Claude Code
 *      propagates `updatedInput` to the MCP server's
 *      `params.arguments` over the stdio wire (confirmed empirically
 *      — see ass-2vg0).
 *
 * The server-side handler reads `params.arguments.session_id` and
 * routes through `identityCache.ensureBoundFor(session_id, project)`
 * where `project` is derived from `params.arguments.cwd` via the
 * boot-time `projectForCwd` resolver (operator-override > git probe;
 * see `server.ts`). This way the minted `cc-<project>-<8>` name
 * reflects the *calling* session's project rather than the plugin's
 * own pinned cwd (ass-v7b4).
 *
 * The model never sees either field; the hook is a deterministic
 * shell that stamps fields the model never touches.
 *
 * `permissionDecision` is intentionally omitted: the plugin's hook
 * MUST NOT auto-allow write operations on behalf of the user. The
 * standard CC permission flow proceeds untouched.
 *
 * Missing or malformed `session_id` → pass through with no
 * `updatedInput`. The server's handler will return the unbound
 * stub error, surfacing the misconfiguration to the model.
 * Missing or malformed `cwd` → include `session_id` but not `cwd`;
 * server falls back to bare `cc-<8>` (or the operator override).
 */

const readStdin = async (): Promise<string> => {
  const decoder = new TextDecoder()
  let buf = ''
  for await (const chunk of process.stdin) {
    buf += decoder.decode(chunk, { stream: true })
  }
  buf += decoder.decode()
  return buf
}

interface HookInput {
  readonly session_id?: unknown
  readonly cwd?: unknown
  readonly tool_input?: unknown
}

const main = async (): Promise<void> => {
  const raw = await readStdin()
  let parsed: HookInput = {}
  try {
    parsed = JSON.parse(raw) as HookInput
  } catch {
    // Malformed input. Pass through silently — better to let the tool
    // call proceed unmodified than to break the user's session.
    process.stdout.write(
      JSON.stringify({
        hookSpecificOutput: { hookEventName: 'PreToolUse' },
      }),
    )
    return
  }

  const sessionId = parsed.session_id
  if (typeof sessionId !== 'string' || sessionId.length === 0) {
    process.stdout.write(
      JSON.stringify({
        hookSpecificOutput: { hookEventName: 'PreToolUse' },
      }),
    )
    return
  }

  const toolInput =
    typeof parsed.tool_input === 'object' && parsed.tool_input !== null
      ? (parsed.tool_input as Record<string, unknown>)
      : {}

  const cwd = parsed.cwd
  const cwdField = typeof cwd === 'string' && cwd.length > 0 ? { cwd } : {}

  process.stdout.write(
    JSON.stringify({
      hookSpecificOutput: {
        hookEventName: 'PreToolUse',
        updatedInput: { ...toolInput, session_id: sessionId, ...cwdField },
      },
    }),
  )
}

await main()
