import { afterEach, expect, test } from 'bun:test'
import {
  chmodSync,
  closeSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  readSync,
  realpathSync,
  rmSync,
  statSync,
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

// A stub for assembleNpmPackage: writes a node bundle (server.js + manifest) at
// outDir, standing in for the real `bun build` so stageMarketplace stays a unit
// test. The real builder is exercised by scripts/assemble-npm-package.test.ts.
const STAGED_PACKAGE_NAME = '@codeforbreakfast/commy-mcp'
function stubAssemble(_repoRoot: string, outDir: string): void {
  writeFile(join(outDir, 'server.js'), '#!/usr/bin/env node\nprocess.exit(0)\n')
  writeFile(
    join(outDir, 'package.json'),
    JSON.stringify({ name: STAGED_PACKAGE_NAME, bin: './server.js' }),
  )
}

// The default target dir's leaf path component (~/.local/share/<dir>/marketplace)
// is what the dev-channel load path turns into the live MCP tool-prefix first
// segment. It MUST be `commy` — the wrong leaf would flip the live prefix and
// mute inbound for every seat.
test('DEFAULT_FIXED_PATH targets the commy marketplace dir', () => {
  expect(DEFAULT_FIXED_PATH.endsWith(join('commy', 'marketplace'))).toBe(true)
})

// Build a minimal but faithful source tree: the root .claude-plugin/marketplace.json
// (its identity is commy — the public marketplace name) and the clients/claude-code
// adapter (npm name commy-plugin, kept). The plugin's pre-existing node_modules, the
// .bun-result nix-store symlink, and *.test.ts files must NOT be carried into the
// frozen copy. No bun workspace / packages tree is staged any more.
function makeSource(): string {
  const src = tmp('pub-src-')
  writeFile(
    join(src, '.claude-plugin', 'marketplace.json'),
    JSON.stringify({
      name: 'commy',
      plugins: [{ name: 'commy', source: './clients/claude-code' }],
    }),
  )
  const plugin = join(src, 'clients', 'claude-code')
  writeFile(join(plugin, 'package.json'), JSON.stringify({ name: 'commy-plugin' }))
  writeFile(join(plugin, '.mcp.json'), JSON.stringify({ mcpServers: {} }))
  writeFile(join(plugin, 'hooks', 'inject-session-id.ts'), 'export {}\n')
  // Dev-only artefacts the frozen plugin must never carry.
  writeFile(join(plugin, 'launcher.test.ts'), 'test\n')
  writeFile(join(plugin, 'node_modules', 'junk.ts'), 'junk\n')
  const nixStore = tmp('pub-nix-')
  writeFile(join(nixStore, 'bun'), 'binary\n')
  symlinkSync(nixStore, join(plugin, '.bun-result'))
  return src
}

test('stageMarketplace stages the marketplace manifest and the claude-code plugin', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

  expect(existsSync(join(staging, '.claude-plugin', 'marketplace.json'))).toBe(true)
  expect(existsSync(join(staging, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(existsSync(join(staging, 'clients', 'claude-code', '.mcp.json'))).toBe(true)
  expect(existsSync(join(staging, 'clients', 'claude-code', 'hooks', 'inject-session-id.ts'))).toBe(
    true,
  )
})

test('stageMarketplace stages the node bundle as the local @codeforbreakfast/commy-mcp override', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

  const override = join(
    staging,
    'clients',
    'claude-code',
    'node_modules',
    '@codeforbreakfast',
    'commy-mcp',
  )
  expect(existsSync(join(override, 'server.js'))).toBe(true)
  const manifest = JSON.parse(readFileSync(join(override, 'package.json'), 'utf8'))
  expect(manifest.name).toBe('@codeforbreakfast/commy-mcp')
})

// The launcher is a bare `npx @codeforbreakfast/commy-mcp` (no version pin) so a
// fleet seat resolves the staged LOCAL override with zero registry hops. But npx
// resolves a package's *bin*, and a hand-placed override carries no
// node_modules/.bin link — npm install would create it. Without the link npx
// falls through to a PATH lookup and dies `commy-mcp: command not found`, so every
// fleet seat boots toolless (comms-hl7y). stageMarketplace must recreate the link
// (relative, so it survives the atomic rename into the fixed path) and make the
// entry executable, mirroring an install.
test('stageMarketplace links the local bin so npx resolves the override with zero registry hops', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

  const nodeModules = join(staging, 'clients', 'claude-code', 'node_modules')
  const binLink = join(nodeModules, '.bin', 'commy-mcp')
  const serverJs = join(nodeModules, '@codeforbreakfast', 'commy-mcp', 'server.js')

  // The bin link exists, is a symlink, and resolves to the override's server.js.
  expect(lstatSync(binLink).isSymbolicLink()).toBe(true)
  expect(realpathSync(binLink)).toBe(realpathSync(serverJs))
  // The launched entry is executable (npx execs it via its shebang).
  expect(statSync(serverJs).mode & 0o111).not.toBe(0)
})

test('stageMarketplace does not carry a bun workspace or source tree', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

  // The node-only marketplace ships the bundle, not the workspace — there is no
  // root package.json / bun.lock and no packages/ source tree to install.
  expect(existsSync(join(staging, 'package.json'))).toBe(false)
  expect(existsSync(join(staging, 'bun.lock'))).toBe(false)
  expect(existsSync(join(staging, 'packages'))).toBe(false)
  expect(existsSync(join(staging, 'plugins'))).toBe(false)
})

test('stageMarketplace excludes the plugin node_modules, the .bun-result nix symlink, and test files', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

  const plugin = join(staging, 'clients', 'claude-code')
  // The pre-existing junk node_modules is excluded; the only node_modules in the
  // frozen tree is the bundle override the stub assemble writes.
  expect(existsSync(join(plugin, 'node_modules', 'junk.ts'))).toBe(false)
  expect(existsSync(join(plugin, '.bun-result'))).toBe(false)
  expect(existsSync(join(plugin, 'launcher.test.ts'))).toBe(false)
})

test('stageMarketplace copies marketplace.json verbatim, preserving its identity', () => {
  const src = makeSource()
  const staging = tmp('pub-stage-')

  stageMarketplace(src, staging, stubAssemble)

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
  writeFile(join(target, 'server.js'), 'OLD')
  const staging = join(parent, '.staging')
  writeFile(join(staging, 'server.js'), 'NEW')

  const fd = openSync(join(target, 'server.js'), 'r')
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

test('publishMarketplace stages the bundle and swaps it into the fixed path', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')

  await publishMarketplace(src, target, noopVerify, stubAssemble)

  expect(existsSync(join(target, '.claude-plugin', 'marketplace.json'))).toBe(true)
  expect(existsSync(join(target, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(
    existsSync(
      join(
        target,
        'clients',
        'claude-code',
        'node_modules',
        '@codeforbreakfast',
        'commy-mcp',
        'server.js',
      ),
    ),
  ).toBe(true)
  // No staging detritus left next to the published tree.
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

test('publishMarketplace is repeatable — a second publish replaces the first', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')

  await publishMarketplace(src, target, noopVerify, stubAssemble)
  await publishMarketplace(src, target, noopVerify, stubAssemble)

  expect(existsSync(join(target, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

test('publishMarketplace verifies the staged plugin dir before the swap, leaving the live tree in place', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'LIVE'), 'live')

  let sawStagedBundle = false
  let sawLiveTreeStillInPlace = false
  let verifiedDir = ''
  const verify = (pluginDir: string): Promise<void> => {
    verifiedDir = pluginDir
    sawStagedBundle = existsSync(
      join(pluginDir, 'node_modules', '@codeforbreakfast', 'commy-mcp', 'server.js'),
    )
    sawLiveTreeStillInPlace = existsSync(join(target, 'LIVE'))
    return Promise.resolve()
  }

  await publishMarketplace(src, target, verify, stubAssemble)

  expect(verifiedDir.endsWith(join('clients', 'claude-code'))).toBe(true)
  expect(sawStagedBundle).toBe(true)
  expect(sawLiveTreeStillInPlace).toBe(true)
  expect(existsSync(join(target, 'clients', 'claude-code', 'package.json'))).toBe(true)
  expect(existsSync(join(target, 'LIVE'))).toBe(false)
})

test('publishMarketplace aborts the swap when verify fails, leaving the live tree untouched', async () => {
  const src = makeSource()
  const parent = tmp('pub-target-')
  const target = join(parent, 'marketplace')
  writeFile(join(target, 'server.js'), 'LIVE')

  const verify = (): Promise<void> => Promise.reject(new Error('bundle failed to load on launch'))

  await expect(publishMarketplace(src, target, verify, stubAssemble)).rejects.toThrow(
    'bundle failed to load on launch',
  )

  // The broken tree never went live, and no staging detritus is left behind.
  expect(readFileSync(join(target, 'server.js'), 'utf8')).toBe('LIVE')
  expect(readdirSync(parent)).toEqual(['marketplace'])
})

// A faithful-enough stand-in for the real bundle: a server staged at
// node_modules/@codeforbreakfast/commy-mcp/server.js, linked into
// node_modules/.bin/commy-mcp exactly as a real install (and stageMarketplace)
// would. verifyBoots launches it through that bin entry — the same file npx
// resolves and execs — so the smoke test covers the bin-link + exec-bit + shebang
// surface that comms-hl7y broke, not just `node server.js`.
const respondingServer = `
process.stdin.on('data', () => {
  process.stdout.write('{"jsonrpc":"2.0","id":1,"result":{"serverInfo":{"name":"fake","version":"0"}}}\\n')
})
setInterval(() => {}, 1 << 30)
`

function pluginWithBundle(serverBody: string): string {
  const dir = tmp('pub-boot-')
  const nodeModules = join(dir, 'node_modules')
  const serverJs = join(nodeModules, '@codeforbreakfast', 'commy-mcp', 'server.js')
  writeFile(serverJs, `#!/usr/bin/env node\n${serverBody}`)
  chmodSync(serverJs, 0o755)
  const binDir = join(nodeModules, '.bin')
  mkdirSync(binDir, { recursive: true })
  symlinkSync(join('..', '@codeforbreakfast', 'commy-mcp', 'server.js'), join(binDir, 'commy-mcp'))
  return dir
}

test('verifyBoots resolves when the staged bundle answers the MCP initialize handshake', async () => {
  const dir = pluginWithBundle(respondingServer)
  await verifyBoots(dir, 15_000)
})

test('verifyBoots fails and surfaces stderr when the bundle crashes on launch', async () => {
  const dir = pluginWithBundle(
    `process.stderr.write('SyntaxError: Unexpected token\\n'); process.exit(1)`,
  )
  await expect(verifyBoots(dir, 15_000)).rejects.toThrow('SyntaxError: Unexpected token')
})

test('verifyBoots fails when the bundle never answers within the timeout', async () => {
  const dir = pluginWithBundle(`setInterval(() => {}, 1 << 30)`)
  await expect(verifyBoots(dir, 250)).rejects.toThrow('did not answer the MCP initialize handshake')
})
