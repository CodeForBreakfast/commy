import { expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// comms-iw8w.3: the PreToolUse hook ships as a node-runnable bundle so the
// plugin's only prereq is node on PATH — the bundled `.js` drops the
// reliance on node's native TS type-stripping (node ≥23.6) that running the
// raw `.ts` would require. These tests prove the bundle builds and behaves
// identically to the source under node, with no bun-runtime surface.

const REPO_ROOT = join(import.meta.dir, '..', '..', '..')
const HOOK_ENTRY = join('clients', 'claude-code', 'hooks', 'inject-session-id.ts')

interface BuildResult {
  readonly dir: string
  readonly outfile: string
  readonly exitCode: number
  readonly stderr: string
}

const buildToTmp = (entry: string): BuildResult => {
  const dir = mkdtempSync(join(tmpdir(), 'commy-hook-bundle-'))
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

test('build script bundles the hook for node', () => {
  const pkg = JSON.parse(readFileSync(join(REPO_ROOT, 'package.json'), 'utf8')) as {
    scripts?: Record<string, string>
  }
  expect(pkg.scripts?.['build']).toContain('clients/claude-code/hooks/inject-session-id.ts')
})

test('the hook bundle builds with no bun runtime surface', () => {
  const { dir, outfile, exitCode, stderr } = buildToTmp(HOOK_ENTRY)
  try {
    expect(stderr).not.toContain('error')
    expect(exitCode).toBe(0)
    const bundle = readFileSync(outfile, 'utf8')
    expect(bundle).not.toMatch(/from\s*["']bun["']/)
    expect(bundle).not.toContain('Bun.stdin')
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('the hook bundle forwards session_id and cwd into updatedInput under node', () => {
  const { dir, outfile } = buildToTmp(HOOK_ENTRY)
  try {
    const run = runUnderNode(
      outfile,
      JSON.stringify({
        session_id: 'sess-abcdef',
        cwd: '/home/x/proj',
        tool_input: { channel_name: 'home', body: 'hello' },
      }),
    )
    expect(run.exitCode).toBe(0)
    // Clean stderr — no MODULE_TYPELESS reparse warning, no diagnostics.
    expect(run.stderr).toBe('')
    const out = JSON.parse(run.stdout) as {
      hookSpecificOutput?: { updatedInput?: Record<string, unknown> }
    }
    expect(out.hookSpecificOutput?.updatedInput).toEqual({
      channel_name: 'home',
      body: 'hello',
      session_id: 'sess-abcdef',
      cwd: '/home/x/proj',
    })
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})
