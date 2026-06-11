import { expect, test } from 'bun:test'

import mcpConfig from './.mcp.json'

/**
 * The plugin's stdio MCP server must be claude's DIRECT child so its
 * `process.stdin` is claude's pipe (comms-hfhm). The earlier launch went
 * `bun run --silent start` → the `start` script's `bun …/server.ts`, which
 * SPAWNS the server as a grandchild: the server's stdin became a socket to
 * the intermediate `bun run` process, not claude's pipe. When claude
 * disconnected, `clientDisconnect(process.stdin)` (comms-8nkv) never saw it
 * across that boundary, so the ~200MB server orphaned to `systemd --user`
 * and accumulated until memory pressure starved fresh seats' 30s connect
 * handshake — the dead-front-desk failure (comms-4c26 / comms-8nkv).
 *
 * Collapsing the launch — point `bun` straight at the entrypoint file, no
 * `run`/`start` script indirection — removes the grandchild so the
 * disconnect signal lands. This pins that contract so a future edit can't
 * silently re-introduce the spawning indirection. The runtime proof that
 * the collapsed launch actually exits on disconnect lives in
 * `packages/mcp/disconnect-exit.test.ts`.
 */

const commyServer = mcpConfig.mcpServers['commy']

const bunArgs = (() => {
  const sep = commyServer.args.indexOf('--')
  return sep === -1 ? [] : commyServer.args.slice(sep + 1)
})()

test('the launcher invokes nix run (bun via the plugin flake)', () => {
  expect(commyServer.command).toBe('nix')
  expect(commyServer.args[0]).toBe('run')
})

test('the launch runs the MCP server entrypoint file directly — no package-script indirection', () => {
  // `bun run start` is what spawned the grandchild; a direct `bun <file>`
  // is one process whose stdin is claude's pipe.
  expect(bunArgs).not.toContain('run')
  expect(bunArgs).not.toContain('start')
  expect(bunArgs.some((arg) => arg.endsWith('mcp/server.ts'))).toBe(true)
})
