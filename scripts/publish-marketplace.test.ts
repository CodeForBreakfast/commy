import { afterEach, expect, test } from 'bun:test'
import {
  closeSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  readSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  atomicSwap,
  DEFAULT_FIXED_PATH,
  publishMarketplace,
  stageMarketplace,
  verifyBoots,
} from './publish-marketplace.ts'

const tmpRoots: string[] = []

function tmp(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix))
  tmpRoots.push(dir)
  return dir
}

afterEach(() => {
  for (const dir of tmpRoots.splice(0)) {
    rmSync(dir, { recursive: true, force: true })
  }
})

function writeFile(path: string, content: string): void {
  mkdirSync(join(path, '..'), { recursive: true })
  writeFileSync(path, content)
}

// The default target dir's leaf path component (~/.local/share/<dir>/marketplace)
// is what the dev-channel load path turns into the live MCP tool-prefix first
// segment. It MUST be `commy` — the wrong leaf would flip the live prefix and
// mute inbound for every seat.
test('DEFAULT_FIXED_PATH targets the commy marketplace dir', () => {
  expect(DEFAULT_FIXED_PATH.endsWith(join('commy', 'marketplace'))).toBe(true)
})

// Build a minimal but faithful workspace-shaped source tree: the repo-root
// package.json + bun.lock + bunfig.toml the frozen install pins against, the
// root .claude-plugin/marketplace.json (its identity is commy — the
// public marketplace name),
// the packages/ tree with the universal mcp substrate package, the clients/ tree
// with the claude-code adapter (npm name commy-plugin, kept), plus
// heavy/dev-only dirs (node_modules, .git, .beads, .turbo, .worktrees, the
// .bun-result nix-store symlink) that must NOT be carried into the frozen copy.
function makeSource(): string {
  const src = tmp('pub-src-')
  writeFile(join(src, 'package.json'), JSON.stringify({ name: 'commy' }))
  writeFile(join(src, 'bun.lock'), '{}\n')
  writeFile(join(src, 'bunfig.toml'), '[install]\nexact = true\n')
  writeFile(
    join(src, '.claude-plugin', 'marketplace.json'),
    JSON.stringify({
      name: 'commy',
      plugins: [{ name: 'commy', source: './clients/claude-code' }],
    }),
  )
  const plugin = join(src, 'clients', 'claude-code')
  writeFile(join(plugin, 'package.json'), JSON.stringify({ name: 'commy-plugin' }))
  writeFile(join(plugin, 'node_modules', 'junk.ts'), 'junk\n')
  const mcp = join(src, 'packages', 'mcp')
  writeFile(join(mcp, 'server.ts'), "import './bootstrap.ts'\n")
  writeFile(join(mcp, 'package.json'), JSON.stringify({ name: '@commy/mcp' }))
  // Dev-only state that the frozen tree must never carry.
  writeFile(join(src, 'node_modules', 'junk.ts'), 'junk\n')
  writeFile(join(src, '.beads', 'issues.jsonl'), '{}\n')
  writeFile(join(src, '.turbo', 'cache.json'), '{}\n')
  writeFile(join(src, '.git', 'HEAD'), 'ref: refs/heads/main\n')
  writeFile(join(src, '.worktrees', 'comms-x', 'file.ts'), 'wt\n')
  const nixStore = tmp('pub-nix-')
  writeFile(join(nixStore, 'bun'), 'binary\n')
  symlinkSync(nixStore, join(plugin, '.bun-result'))
  return src
}

test('stageMarketplace freezes the whole workspace root', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging)

  expect(existsSync(join(staging, 'package.json'))).toBe(true)
  expect(existsSync(join(staging, 'bun.lock'))).toBe(true)
  expect(existsSync(join(staging, 'bunfig.toml'))).toBe(true)
  expect(existsSync(join(staging, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(existsSync(join(staging, 'packages', 'mcp', 'server.ts'))).toBe(true)
})

test('stageMarketplace does not carry a stray plugins/ path', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging)

  expect(existsSync(join(staging, 'plugins'))).toBe(false)
})

test('stageMarketplace excludes node_modules, the .bun-result nix symlink, and dev-only state', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging)

  expect(existsSync(join(staging, 'node_modules'))).toBe(false)
  expect(existsSync(join(staging, '.git'))).toBe(false)
  expect(existsSync(join(staging, '.beads'))).toBe(false)
  expect(existsSync(join(staging, '.turbo'))).toBe(false)
  expect(existsSync(join(staging, '.worktrees'))).toBe(false)
  expect(existsSync(join(staging, 'clients', 'claude-code', 'node_modules'))).toBe(false)
  expect(existsSync(join(staging, 'clients', 'claude-code', '.bun-result'))).toBe(false)
})

test('stageMarketplace copies marketplace.json verbatim, preserving its identity', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging)

  const manifest = JSON.parse(
    readFileSync(join(staging, '.claude-plugin', 'marketplace.json'), 'utf8'),
  )
  expect(manifest.name).toBe('commy')
  expect(manifest.plugins[0].name).toBe('commy')
  expect(manifest.plugins[0].source).toBe('./clients/claude-code')
})

test('atomicSwap replaces existing content and leaves no trash behind', () => {
  const parent = tmp('pub-swap-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'old.txt'), 'OLD')
  const staging = join(parent, '.staging')
  writeFile(join(staging, 'new.txt'), 'NEW')

  atomicSwap(staging, target)

  expect(readFileSync(join(target, 'new.txt'), 'utf8')).toBe('NEW')
  expect(existsSync(join(target, 'old.txt'))).toBe(false)
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

test('atomicSwap keeps the swapped-out inode alive for an open handle (running servers)', () => {
  const parent = tmp('pub-swap-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'server.ts'), 'OLD')
  const staging = join(parent, '.staging')
  writeFile(join(staging, 'server.ts'), 'NEW')

  const fd = openSync(join(target, 'server.ts'), 'r')
  try {
    atomicSwap(staging, target)
    const buf = Buffer.alloc(3)
    readSync(fd, buf, 0, 3, 0)
    expect(buf.toString('utf8')).toBe('OLD')
  } finally {
    closeSync(fd)
  }
})

const noopVerify = (): Promise<void> => Promise.resolve()

test('publishMarketplace stages, installs at the workspace root, and swaps into the fixed path', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')

  let installedAt = ''
  const install = (workspaceRoot: string): void => {
    installedAt = workspaceRoot
    writeFile(join(workspaceRoot, 'node_modules', '.installed'), 'yes')
  }

  await publishMarketplace(src, target, install, noopVerify)

  // install runs at the frozen workspace root — the staging tree that gets
  // atomically swapped into the target afterward, not the plugin dir and not the
  // final target (which doesn't exist yet at install time).
  expect(installedAt.startsWith(join(parent, '.staging.'))).toBe(true)
  expect(existsSync(join(target, '.claude-plugin', 'marketplace.json'))).toBe(true)
  expect(existsSync(join(target, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(existsSync(join(target, 'node_modules', '.installed'))).toBe(true)
  // No staging detritus left next to the published tree.
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

test('publishMarketplace is repeatable — a second publish replaces the first', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')
  const install = (workspaceRoot: string): void => {
    mkdirSync(join(workspaceRoot, 'node_modules'), { recursive: true })
  }

  await publishMarketplace(src, target, install, noopVerify)
  await publishMarketplace(src, target, install, noopVerify)

  expect(existsSync(join(target, 'packages', 'mcp', 'server.ts'))).toBe(true)
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

test('publishMarketplace verifies the plugin dir after install and before the swap', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'LIVE'), 'live')

  const install = (workspaceRoot: string): void => {
    writeFile(join(workspaceRoot, 'node_modules', '.installed'), 'yes')
  }

  let sawInstalledDeps = false
  let sawLiveTreeStillInPlace = false
  let verifiedDir = ''
  const verify = (pluginDir: string): Promise<void> => {
    verifiedDir = pluginDir
    // The plugin dir sits under the staged workspace root, where install ran.
    sawInstalledDeps = existsSync(join(pluginDir, '..', '..', 'node_modules', '.installed'))
    sawLiveTreeStillInPlace = existsSync(join(target, 'LIVE'))
    return Promise.resolve()
  }

  await publishMarketplace(src, target, install, verify)

  expect(verifiedDir.endsWith(join('clients', 'claude-code'))).toBe(true)
  expect(sawInstalledDeps).toBe(true)
  expect(sawLiveTreeStillInPlace).toBe(true)
  expect(existsSync(join(target, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(existsSync(join(target, 'LIVE'))).toBe(false)
})

test('publishMarketplace aborts the swap when verify fails, leaving the live tree untouched', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'server.ts'), 'LIVE')

  const install = (workspaceRoot: string): void => {
    mkdirSync(join(workspaceRoot, 'node_modules'), { recursive: true })
  }
  const verify = (): Promise<void> =>
    Promise.reject(new Error('server crashed on launch — missing dependency'))

  await expect(publishMarketplace(src, target, install, verify)).rejects.toThrow(
    'missing dependency',
  )

  // The broken tree never went live, and no staging detritus is left behind.
  expect(readFileSync(join(target, 'server.ts'), 'utf8')).toBe('LIVE')
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

// A faithful-enough stand-in for the real plugin package: a `start` script that
// launches a server which stays alive after the handshake (the real server runs
// an event pump and never exits on stdin EOF) and answers any request with a
// JSON-RPC result carrying serverInfo. verifyBoots launches it via `bun run start`
// with cwd = the plugin dir, exactly as a session does.
const respondingServer = `
process.stdin.on('data', () => {
  process.stdout.write('{"jsonrpc":"2.0","id":1,"result":{"serverInfo":{"name":"fake","version":"0"}}}\\n')
})
setInterval(() => {}, 1 << 30)
`

function pluginWithStart(serverBody: string): string {
  const dir = tmp('pub-boot-')
  writeFileSync(join(dir, 'server.ts'), serverBody)
  writeFileSync(
    join(dir, 'package.json'),
    JSON.stringify({ name: 'commy-plugin', scripts: { start: 'bun server.ts' } }),
  )
  return dir
}

test('verifyBoots resolves when the start script answers the MCP initialize handshake', async () => {
  const dir = pluginWithStart(respondingServer)
  await verifyBoots(dir, 15_000)
})

test('verifyBoots fails and surfaces stderr when the server crashes on launch', async () => {
  const dir = pluginWithStart(
    `process.stderr.write('Cannot find module "@effect/platform"\\n'); process.exit(1)`,
  )
  await expect(verifyBoots(dir, 15_000)).rejects.toThrow('Cannot find module "@effect/platform"')
})

test('verifyBoots fails when the server never answers within the timeout', async () => {
  const dir = pluginWithStart(`setInterval(() => {}, 1 << 30)`)
  await expect(verifyBoots(dir, 250)).rejects.toThrow('did not answer the MCP initialize handshake')
})
