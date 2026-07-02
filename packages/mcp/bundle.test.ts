import { expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// The npx-migration ships the commy MCP server as a
// self-contained node-runnable bundle so `npx -y @commy/mcp` works with
// only node on PATH — no bun runtime, no workspace:* resolution at consume
// time. These tests lock that contract: the build wiring exists, and the
// emitted artifact loads and executes under node with its workspace deps
// inlined and no bun-runtime surface.

const REPO_ROOT = join(import.meta.dir, '..', '..')
const SERVER_ENTRY = join('packages', 'mcp', 'server.ts')

interface BuildResult {
  readonly dir: string
  readonly outfile: string
  readonly exitCode: number
  readonly stderr: string
}

// Build an entrypoint to a node-target bundle in a throwaway dir. The dir
// carries a `type: module` package.json so node treats the `.js` as ESM
// without the MODULE_TYPELESS reparse warning — exactly as the published
// `type: module` package will. Mirrors the `bun build … --target=node`
// command wired into the root `build` script (asserted below).
const buildToTmp = (entry: string): BuildResult => {
  const dir = mkdtempSync(join(tmpdir(), 'commy-bundle-'))
  writeFileSync(join(dir, 'package.json'), JSON.stringify({ type: 'module' }))
  const outfile = join(dir, 'bundle.js')
  const proc = Bun.spawnSync(['bun', 'build', entry, '--target=node', '--outfile', outfile], {
    cwd: REPO_ROOT,
    stdout: 'pipe',
    stderr: 'pipe',
  })
  return { dir, outfile, exitCode: proc.exitCode, stderr: proc.stderr.toString() }
}

interface NodeRun {
  readonly stdout: string
  readonly stderr: string
  readonly exitCode: number
}

// Run a file under node with a deliberately minimal environment (PATH only,
// so no ZULIP_* leaks in from the test runner) and the given stdin.
const runUnderNode = (file: string, stdin: string): NodeRun => {
  const proc = Bun.spawnSync(['node', file], {
    env: { PATH: process.env['PATH'] ?? '' },
    stdin: Buffer.from(stdin),
    stdout: 'pipe',
    stderr: 'pipe',
  })
  return {
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
    exitCode: proc.exitCode,
  }
}

test('build script bundles the server for node via bun build --target=node', () => {
  const pkg = JSON.parse(readFileSync(join(REPO_ROOT, 'package.json'), 'utf8')) as {
    scripts?: Record<string, string>
  }
  const build = pkg.scripts?.['build']
  expect(typeof build).toBe('string')
  expect(build).toContain('bun build')
  expect(build).toContain('--target=node')
  expect(build).toContain('packages/mcp/server.ts')
})

test('turbo declares a //#build task that emits the dist bundles', () => {
  const turbo = JSON.parse(readFileSync(join(REPO_ROOT, 'turbo.json'), 'utf8')) as {
    tasks?: Record<string, { outputs?: ReadonlyArray<string> }>
  }
  const build = turbo.tasks?.['//#build']
  expect(build).toBeDefined()
  expect(build?.outputs).toContain('packages/mcp/dist/**')
})

test('a single bun build produces a node-runnable server bundle with no workspace:* or bun runtime', () => {
  const { dir, outfile, exitCode, stderr } = buildToTmp(SERVER_ENTRY)
  try {
    expect(stderr).not.toContain('error')
    expect(exitCode).toBe(0)
    // A real bundle inlines effect + the MCP SDK + the workspace packages.
    expect(statSync(outfile).size).toBeGreaterThan(500_000)

    const bundle = readFileSync(outfile, 'utf8')
    // Workspace deps are inlined, not left as resolvable specifiers.
    expect(bundle).not.toContain('workspace:')
    expect(bundle).not.toMatch(/from\s*["']@commy\//)
    expect(bundle).not.toMatch(/require\(["']@commy\//)
    // No bun runtime surface survives (the .1 port moved off platform-bun).
    expect(bundle).not.toMatch(/from\s*["']bun["']/)
    expect(bundle).not.toMatch(/require\(["']bun["']\)/)
    expect(bundle).not.toContain('BunFileSystem')
    expect(bundle).not.toContain('BunRuntime')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('the server bundle loads and executes under node, reaching env validation', () => {
  const { dir, outfile } = buildToTmp(SERVER_ENTRY)
  try {
    // Boot with no ZULIP_* env: the program loads its entire module graph
    // under node, then fails at env validation — proving node ran the
    // bundle through to domain logic, not a module/syntax/bun error. The
    // boot-time failure is reported by NodeRuntime's runMain on stdout
    // (the program never reaches the point of serving protocol traffic,
    // so this is not the live JSON-RPC channel).
    const run = runUnderNode(outfile, '')
    const output = run.stdout + run.stderr
    expect(run.exitCode).not.toBe(0)
    expect(output).toContain('EnvConfigError')
    expect(output).toContain('ZULIP_SITE')
    expect(output).not.toContain('Cannot find module')
    expect(output).not.toContain('SyntaxError')
    expect(output).not.toContain('is not defined')
    // node treats the bundle as ESM via the dist `type: module` — no reparse.
    expect(run.stderr).not.toContain('MODULE_TYPELESS_PACKAGE_JSON')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})
