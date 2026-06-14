import { cpSync, existsSync, mkdirSync, renameSync, rmSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, dirname, join } from 'node:path'

import { assembleNpmPackage, NPM_PACKAGE_NAME } from './assemble-npm-package.ts'

// Where released, self-contained plugin copies live. Sessions launch from here,
// never from the dev working tree — that decoupling is what comms-2mx delivers.
// Referenced by the release process and the one-time marketplace registration only.
const xdgDataHome = process.env['XDG_DATA_HOME'] ?? join(homedir(), '.local', 'share')
export const DEFAULT_FIXED_PATH = join(xdgDataHome, 'commy', 'marketplace')

// The plugin's own node_modules is regenerated as the staged bundle override
// below; .bun-result is a nix-store symlink that would otherwise drag the whole
// bun derivation into the copy; *.test.ts is dev-only and never runs in a
// release. Everything else under the plugin dir is shipped verbatim.
const EXCLUDED = new Set(['node_modules', '.bun-result'])
const stageFilter = (source: string): boolean => {
  const name = basename(source)
  return !EXCLUDED.has(name) && !name.endsWith('.test.ts')
}

// Stage a self-contained, node-runnable marketplace into stagingDir. The frozen
// copy carries the root marketplace manifest and the claude-code plugin only —
// no bun workspace, no source tree, no `bun install`. The plugin's `.mcp.json`
// launches `npx @codeforbreakfast/commy-mcp` with cwd = CLAUDE_PLUGIN_ROOT (the
// staged clients/claude-code dir); to make that resolve the LOCAL build with zero
// registry hits, we assemble the node bundle straight into the plugin's
// node_modules as @codeforbreakfast/commy-mcp. A consumer installing the plugin
// from the public marketplace has no such local install and so resolves the
// published build from npm. The marketplace identity stays commy
// (marketplace.json name + plugin name); only the npm package carries the
// @codeforbreakfast scope, because @commy is taken on npm.
export function stageMarketplace(
  repoRoot: string,
  stagingDir: string,
  assemble: (repoRoot: string, outDir: string) => unknown = assembleNpmPackage,
): void {
  mkdirSync(stagingDir, { recursive: true })
  cpSync(join(repoRoot, '.claude-plugin'), join(stagingDir, '.claude-plugin'), { recursive: true })
  cpSync(join(repoRoot, 'clients', 'claude-code'), join(stagingDir, 'clients', 'claude-code'), {
    recursive: true,
    filter: stageFilter,
  })
  const override = join(stagingDir, 'clients', 'claude-code', 'node_modules', NPM_PACKAGE_NAME)
  assemble(repoRoot, override)
}

// Replace targetPath with stagingDir atomically enough for a deliberate, single
// release: rename the live copy aside, rename the new one in, then drop the old.
// stagingDir must share a filesystem with targetPath. Servers running from the
// old copy keep its inode alive through their cwd until they restart; cold
// launches only ever see a complete tree. The target stays a real directory (not
// a symlink) so its path string is stable and CC cannot canonicalize it away.
export function atomicSwap(stagingDir: string, targetPath: string): void {
  if (!existsSync(targetPath)) {
    renameSync(stagingDir, targetPath)
    return
  }
  const trash = `${targetPath}.trash.${process.pid}.${Date.now()}`
  renameSync(targetPath, trash)
  renameSync(stagingDir, targetPath)
  rmSync(trash, { recursive: true, force: true })
}

async function readStreamUntil(
  stream: ReadableStream<Uint8Array>,
  needle: string,
): Promise<'ok' | 'closed'> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let text = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return 'closed'
      if (value !== undefined) {
        text += decoder.decode(value, { stream: true })
        if (text.includes(needle)) return 'ok'
      }
    }
  } catch {
    return 'closed'
  } finally {
    reader.releaseLock()
  }
}

async function drainStream(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let text = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) break
      if (value !== undefined) text += decoder.decode(value, { stream: true })
    }
  } catch {
    // Stream torn down by the kill below; whatever we captured is enough.
  } finally {
    reader.releaseLock()
  }
  return text
}

// Kill the child's entire process group. The server runs an event pump and never
// exits on stdin EOF, and it may itself fork; signalling only the direct child
// can orphan a forked grandchild that keeps the inherited stdout pipe open, so a
// bare `child.kill()` hangs. Spawning detached makes the child a session/group
// leader (setsid), and signalling the negative pid reaps the whole tree.
function killBootGroup(child: Bun.Subprocess): void {
  try {
    process.kill(-child.pid, 'SIGKILL')
  } catch {
    // Group already gone (clean exit, or never started); nothing to reap.
  }
}

// Boot smoke test: launch the freshly staged bundle under `node` from the plugin
// dir — the same node-runnable @codeforbreakfast/commy-mcp/server.js that the
// `.mcp.json` launcher resolves via npx — and require an MCP initialize response.
// A bundle that fails to load (syntax error, missing builtin) exits before
// answering, so the release aborts instead of shipping a server that fails to
// reconnect (-32000). The real server runs an event pump and won't exit on stdin
// EOF, so we stop at the first serverInfo and kill the child's process group
// rather than waiting for it to close.
export async function verifyBoots(pluginDir: string, timeoutMs = 30_000): Promise<void> {
  const child = Bun.spawn(['node', join('node_modules', NPM_PACKAGE_NAME, 'server.js')], {
    cwd: pluginDir,
    stdin: 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
    detached: true,
  })

  let capturedStderr = ''
  const stderrDrained = drainStream(child.stderr).then((text) => {
    capturedStderr = text
  })
  const handshake = readStreamUntil(child.stdout, '"serverInfo"')

  try {
    child.stdin.write(
      `${JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          clientInfo: { name: 'publish-smoke-test', version: '0' },
        },
      })}\n`,
    )
    await child.stdin.flush()
  } catch {
    // A server that crashed on launch rejects the write; the race below reports it.
  }

  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => resolve('timeout'), timeoutMs)
  })
  const exited = child.exited.then((code): { readonly code: number } => ({ code }))

  try {
    const winner = await Promise.race([handshake, exited, timeout])
    if (winner === 'ok') return
    killBootGroup(child)
    await stderrDrained
    const detail = capturedStderr.trim()
    const reason =
      winner === 'timeout'
        ? `did not answer the MCP initialize handshake within ${timeoutMs}ms`
        : winner === 'closed'
          ? 'closed stdout before answering the MCP initialize handshake'
          : `exited with code ${winner.code} before answering the MCP initialize handshake`
    throw new Error(
      `commy server failed its boot smoke test in ${pluginDir}: ${reason}${
        detail ? `\n--- server stderr ---\n${detail}` : ''
      }`,
    )
  } finally {
    if (timer !== undefined) clearTimeout(timer)
    killBootGroup(child)
    await child.exited.catch(() => {})
  }
}

export async function publishMarketplace(
  repoRoot: string,
  targetPath: string,
  verify: (pluginDir: string) => Promise<void> = verifyBoots,
  assemble: (repoRoot: string, outDir: string) => unknown = assembleNpmPackage,
): Promise<void> {
  const parent = dirname(targetPath)
  mkdirSync(parent, { recursive: true })
  const staging = join(parent, `.staging.${process.pid}.${Date.now()}`)
  try {
    stageMarketplace(repoRoot, staging, assemble)
    const pluginDir = join(staging, 'clients', 'claude-code')
    await verify(pluginDir)
    atomicSwap(staging, targetPath)
  } finally {
    rmSync(staging, { recursive: true, force: true })
  }
}

if (import.meta.main) {
  const repoRoot = process.argv[2] ?? join(import.meta.dir, '..')
  const targetPath = process.argv[3] ?? DEFAULT_FIXED_PATH
  console.error(`publish-marketplace: ${repoRoot} -> ${targetPath}`)
  await publishMarketplace(repoRoot, targetPath)
  console.error(`publish-marketplace: published frozen marketplace at ${targetPath}`)
}
