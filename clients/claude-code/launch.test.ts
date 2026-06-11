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
 * The launcher is now `launch.sh` (comms-ip4q): a bun-on-PATH bootstrap, no
 * Nix, that stages the workspace deps once before `exec bun …/server.ts`.
 * The `exec` is what keeps the server claude's direct child — it replaces the
 * shell rather than spawning a grandchild. These tests pin both contracts so
 * a future edit can't silently re-introduce the spawning indirection or a Nix
 * requirement. The runtime proof that the launch exits on disconnect lives in
 * `packages/mcp/disconnect-exit.test.ts`.
 */

const commyServer = mcpConfig.mcpServers['commy']

const launcherScript = await Bun.file(new URL('./launch.sh', import.meta.url)).text()

test('the launcher is the bun bootstrap script — no Nix in the launch metadata', () => {
  expect(commyServer.command).toBe('${CLAUDE_PLUGIN_ROOT}/launch.sh')
  expect('args' in commyServer).toBe(false)
  expect(JSON.stringify(mcpConfig)).not.toContain('nix')
})

test('a host with only bun on PATH can launch — the bootstrap shells out to bun, never nix', () => {
  expect(launcherScript).not.toMatch(/\bnix\b/)
  expect(launcherScript).toMatch(/exec bun /)
})

test('the launcher imposes no shell of our choosing — POSIX sh, no bashisms', () => {
  // The standing principle (commy-no-environment-assumptions-on-consumers) is
  // that our preferences never become consumer prerequisites. A bash shebang
  // would foist bash; the script uses zero bashisms, so it runs under any
  // POSIX sh (dash/ash/busybox/bash). `set -o pipefail` and fractional `sleep`
  // are the easy bashism regressions — pin against them.
  expect(launcherScript.startsWith('#!/bin/sh\n')).toBe(true)
  expect(launcherScript).not.toContain('pipefail')
  expect(launcherScript).not.toMatch(/sleep 0\.\d/)
})

test('the launch execs the MCP server entrypoint directly — no package-script indirection', () => {
  // `bun run start` is what spawned the grandchild; a direct `exec bun <file>`
  // is one process whose stdin is claude's pipe.
  const execLine = launcherScript
    .split('\n')
    .find((line) => line.trimStart().startsWith('exec bun'))
  expect(execLine).toBeDefined()
  expect(execLine).toContain('mcp/server.ts')
  expect(execLine).not.toContain(' run ')
  expect(execLine).not.toContain(' start')
})

test('the one-time dep stage is guarded against the concurrent-cold-start race (comms-ae3)', () => {
  // The original sin was an unguarded connect-time `bun install` racing to
  // EEXIST across burst-booted seats. The stage must sit behind a mutex and
  // only run when the entrypoint is genuinely absent.
  expect(launcherScript).toContain('bun install --frozen-lockfile')
  expect(launcherScript).toMatch(/mkdir .*LOCK|mkdir "\$\{LOCK\}"/)
})
