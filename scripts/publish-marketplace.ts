import { execFileSync } from 'node:child_process'
import { cpSync, existsSync, mkdirSync, renameSync, rmSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, dirname, join } from 'node:path'

// Where released, self-contained plugin copies live. Sessions launch from here,
// never from the dev working tree — that decoupling is what comms-2mx delivers.
// Referenced by the release process and the one-time marketplace registration only.
const xdgDataHome = process.env['XDG_DATA_HOME'] ?? join(homedir(), '.local', 'share')
export const DEFAULT_FIXED_PATH = join(xdgDataHome, 'commy', 'marketplace')

// node_modules is reinstalled fresh at the frozen root; .bun-result is a nix-store
// symlink that would otherwise drag the whole bun derivation into the copy. The
// rest are dev-only state — git history, the bead store, turbo cache, sibling
// worktrees — that have no business in a frozen release artifact.
const EXCLUDED = new Set(['node_modules', '.bun-result', '.worktrees', '.beads', '.turbo', '.git'])

const excludedFilter = (source: string): boolean => !EXCLUDED.has(basename(source))

// Freeze the whole Bun workspace into stagingDir so the frozen install can
// resolve the @commy/* workspace symlinks in-tree. We carry the repo-root
// package.json + bun.lock (the lockfile the frozen install pins against) and
// bunfig.toml (it governs install behaviour — exact pins, minimum release age),
// the entire packages/ tree (the universal substrate — core, testing, zulip,
// memory, mcp; including the test-only testing/memory — pruning is deferred,
// disk is cheap), the clients/ tree (the per-client adapters; clients/claude-code
// is the CC adapter the boot smoke test launches), and the root .claude-plugin/
// marketplace manifest. The manifest identity is commy and
// installers resolve the plugin as commy@commy (marketplace name +
// plugin name).
// tsconfig/biome are not copied: the boot runs the server at runtime, not under
// tsc, so it needs module resolution, not typecheck config. No dependencies
// installed yet, and no symlink dereferencing — the workspace symlinks are
// recreated by the frozen install at the staged root.
export function stageMarketplace(repoRoot: string, stagingDir: string): void {
  mkdirSync(stagingDir, { recursive: true })
  for (const file of ['package.json', 'bun.lock', 'bunfig.toml']) {
    cpSync(join(repoRoot, file), join(stagingDir, file))
  }
  cpSync(join(repoRoot, '.claude-plugin'), join(stagingDir, '.claude-plugin'), { recursive: true })
  for (const tree of ['packages', 'clients']) {
    cpSync(join(repoRoot, tree), join(stagingDir, tree), {
      recursive: true,
      filter: excludedFilter,
    })
  }
}

// Install the workspace's pinned dependencies at the frozen workspace root,
// network-free when bun's cache is warm. Running at the root (not a single
// package) is what recreates the @commy/* workspace symlinks in-tree.
// --frozen-lockfile fails loudly if the staged lockfile drifts. --ignore-scripts
// skips lifecycle scripts: the only one is the root `prepare` (dev-env git-hooks
// path + an editor-only effect-language-service patch), which is meaningless in a
// frozen artifact and would fail (no .git in the frozen tree) — and no runtime
// dependency carries a postinstall the server needs.
export function installDeps(workspaceRoot: string): void {
  execFileSync(process.execPath, ['install', '--frozen-lockfile', '--ignore-scripts'], {
    cwd: workspaceRoot,
    stdio: 'inherit',
  })
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

// Kill the child's entire process group. `bun run start` forks: the `run`
// wrapper is the direct child, but the actual server is a grandchild that
// inherits the group. Signalling only the direct child orphans that grandchild
// — it keeps the inherited stdout pipe open and the parent never sees EOF, so a
// bare `child.kill()` hangs. Spawning detached makes the child a session/group
// leader (setsid), and signalling the negative pid reaps the whole tree.
function killBootGroup(child: Bun.Subprocess): void {
  try {
    process.kill(-child.pid, 'SIGKILL')
  } catch {
    // Group already gone (clean exit, or never started); nothing to reap.
  }
}

// Boot smoke test: launch the freshly installed server exactly as a session would
// (the plugin package's `start` script, cwd = the frozen clients/claude-code dir,
// mirroring how .mcp.json invokes `bun ... run ... start`), speak the MCP
// initialize handshake, and require a serverInfo response. A tree whose
// dependencies didn't install crashes here — process exits before answering — so
// the release aborts instead of shipping a server that fails to reconnect
// (-32000). The real server runs an event pump and won't exit on stdin EOF, so we
// stop reading at the first serverInfo and kill the child's process group rather
// than waiting for it to close.
export async function verifyBoots(pluginDir: string, timeoutMs = 30_000): Promise<void> {
  const child = Bun.spawn([process.execPath, 'run', 'start'], {
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
  install: (workspaceRoot: string) => void = installDeps,
  verify: (pluginDir: string) => Promise<void> = verifyBoots,
): Promise<void> {
  const parent = dirname(targetPath)
  mkdirSync(parent, { recursive: true })
  const staging = join(parent, `.staging.${process.pid}.${Date.now()}`)
  try {
    stageMarketplace(repoRoot, staging)
    const pluginDir = join(staging, 'clients', 'claude-code')
    install(staging)
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
