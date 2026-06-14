import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  symlinkSync,
} from 'node:fs'
import { homedir } from 'node:os'
import { basename, dirname, join, relative } from 'node:path'

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
  const nodeModules = join(stagingDir, 'clients', 'claude-code', 'node_modules')
  const override = join(nodeModules, NPM_PACKAGE_NAME)
  assemble(repoRoot, override)
  linkLocalBin(nodeModules, NPM_PACKAGE_NAME)
}

// npm normalises a package's `bin`: a string means a single bin named after the
// package's unscoped name; an object maps bin names to relative paths.
function normalizeBin(
  packageName: string,
  bin: string | Record<string, string> | undefined,
): Record<string, string> {
  if (typeof bin === 'string') {
    const unscoped = packageName.includes('/')
      ? (packageName.split('/').pop() ?? packageName)
      : packageName
    return { [unscoped]: bin }
  }
  return bin ?? {}
}

// Recreate what `npm install` does for the staged override: link each of its `bin`
// entries into node_modules/.bin and make the target executable. The launcher is a
// bare `npx @codeforbreakfast/commy-mcp` (no version pin, so a fleet seat resolves
// this LOCAL override with zero registry hops); npx resolves a package's bin, so
// without the .bin link it falls through to a PATH lookup and dies
// `commy-mcp: command not found`, booting every fleet seat toolless (comms-hl7y).
// The link is RELATIVE so it survives atomicSwap renaming the staging dir into the
// fixed path — an absolute link into the staging path would dangle.
function linkLocalBin(nodeModulesDir: string, packageName: string): void {
  const pkgDir = join(nodeModulesDir, packageName)
  const manifest = JSON.parse(readFileSync(join(pkgDir, 'package.json'), 'utf8')) as {
    readonly bin?: string | Record<string, string>
  }
  const binDir = join(nodeModulesDir, '.bin')
  mkdirSync(binDir, { recursive: true })
  for (const [name, rel] of Object.entries(normalizeBin(packageName, manifest.bin))) {
    const target = join(pkgDir, rel)
    chmodSync(target, 0o755)
    const link = join(binDir, name)
    rmSync(link, { force: true })
    symlinkSync(relative(binDir, target), link)
  }
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

// The unscoped package name is the bin name npm links into node_modules/.bin and
// the name npx resolves (`npx @codeforbreakfast/commy-mcp` execs `.bin/commy-mcp`).
const BIN_NAME = NPM_PACKAGE_NAME.includes('/')
  ? (NPM_PACKAGE_NAME.split('/').pop() ?? NPM_PACKAGE_NAME)
  : NPM_PACKAGE_NAME

// Kill the child's entire process group. The server may fork, and signalling only
// the direct child can orphan a forked grandchild that keeps the inherited stdout
// pipe open, so a bare `child.kill()` can hang. Spawning detached makes the child a
// session/group leader (setsid), and signalling the negative pid reaps the whole
// tree — we stop at the first serverInfo rather than driving a full disconnect.
function killBootGroup(child: Bun.Subprocess): void {
  try {
    process.kill(-child.pid, 'SIGKILL')
  } catch {
    // Group already gone (clean exit, or never started); nothing to reap.
  }
}

// Boot smoke test: launch the freshly staged bundle through its bin entry —
// node_modules/.bin/<BIN_NAME>, the exact file the `.mcp.json` launcher's
// `npx @codeforbreakfast/commy-mcp` resolves and execs (via its shebang) — and
// require an MCP initialize response. Going through the bin entry, not a bare
// `node server.js`, means the smoke catches a missing/broken bin link or a
// non-executable entry (comms-hl7y) as well as a bundle that fails to load (syntax
// error, missing builtin); either way the release aborts instead of shipping a
// server every seat fails to launch. We stop at the first serverInfo and kill the
// child's process group rather than driving a full client disconnect.
export async function verifyBoots(pluginDir: string, timeoutMs = 30_000): Promise<void> {
  const child = Bun.spawn([join(pluginDir, 'node_modules', '.bin', BIN_NAME)], {
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
