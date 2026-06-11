import { afterEach, expect, test } from 'bun:test'
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'

// Tests for the bun-wrap.sh PreToolUse hook wrapper.
//
// The wrapper's job is to invoke bun without needing it on the host PATH:
//   - First call after install / Nix GC: lazy-build a GC-root symlink at
//     $CLAUDE_PLUGIN_ROOT/.bun-result -> /nix/store/...-bun via
//     `nix build --out-link`.
//   - Every call thereafter: `exec $CLAUDE_PLUGIN_ROOT/.bun-result/bin/bun "$@"`.
//
// Tests run the wrapper in a sandboxed CLAUDE_PLUGIN_ROOT with a stub `nix`
// on PATH so we don't actually hit the Nix evaluator.

const WRAPPER_SOURCE = new URL('./bun-wrap.sh', import.meta.url).pathname

type Sandbox = {
  readonly pluginRoot: string
  readonly stubBinDir: string
  readonly nixLog: string
  cleanup: () => void
}

const realBunPath = (): string => {
  // Bun.argv[0] is the bun executable that's currently running the tests,
  // resolved against $PATH or already absolute. Either way, exec'ing it works.
  const argv0 = Bun.argv[0]
  if (!argv0) throw new Error('Bun.argv[0] missing')
  return argv0
}

const writeStub = (path: string, body: string): void => {
  mkdirSync(dirname(path), { recursive: true })
  writeFileSync(path, body, { encoding: 'utf-8' })
  chmodSync(path, 0o755)
}

const makeSandbox = (opts: { readonly nixBehavior: 'success' | 'fail' }): Sandbox => {
  const root = mkdtempSync(join(tmpdir(), 'bun-wrap-'))
  const pluginRoot = join(root, 'plugin')
  const stubBinDir = join(root, 'bin')
  const nixLog = join(root, 'nix.log')

  // Copy the wrapper into the sandboxed plugin root so $CLAUDE_PLUGIN_ROOT
  // resolves there. The wrapper resolves sibling paths via its own location.
  mkdirSync(join(pluginRoot, 'hooks'), { recursive: true })
  const wrapperBody = readFileSync(WRAPPER_SOURCE, 'utf-8')
  const sandboxedWrapper = join(pluginRoot, 'hooks', 'bun-wrap.sh')
  writeFileSync(sandboxedWrapper, wrapperBody, { encoding: 'utf-8' })
  chmodSync(sandboxedWrapper, 0o755)

  // Stub `nix` that simulates `nix build --out-link PATH path:ROOT`:
  // it parses --out-link's argument, creates a fake .bun-result tree, and
  // appends its invocation to nix.log so tests can assert.
  const nixStub =
    opts.nixBehavior === 'success'
      ? `#!/usr/bin/env bash
set -euo pipefail
echo "$@" >> "${nixLog}"
out_link=""
while [ $# -gt 0 ]; do
  case "$1" in
    --out-link) out_link="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [ -n "$out_link" ]; then
  mkdir -p "$out_link/bin"
  ln -sf "${realBunPath()}" "$out_link/bin/bun"
fi
exit 0
`
      : `#!/usr/bin/env bash
echo "stub nix: build failed" >&2
echo "$@" >> "${nixLog}"
exit 1
`
  writeStub(join(stubBinDir, 'nix'), nixStub)

  return {
    pluginRoot,
    stubBinDir,
    nixLog,
    cleanup: () => rmSync(root, { recursive: true, force: true }),
  }
}

const runWrapper = async (
  sb: Sandbox,
  args: ReadonlyArray<string>,
  stdin?: string,
): Promise<{ readonly exitCode: number; readonly stdout: string; readonly stderr: string }> => {
  const wrapper = join(sb.pluginRoot, 'hooks', 'bun-wrap.sh')
  // Stub bin dir first so the stub `nix` shadows any real one. We keep the
  // rest of the host PATH so bash, the shebang's `env`, and stdlib helpers
  // resolve normally — the wrapper bypasses PATH for `bun` (it execs an
  // absolute path under .bun-result), so a host bun on PATH cannot affect the
  // result of these tests.
  const proc = Bun.spawn([wrapper, ...args], {
    env: {
      ...process.env,
      CLAUDE_PLUGIN_ROOT: sb.pluginRoot,
      PATH: `${sb.stubBinDir}:${process.env['PATH'] ?? ''}`,
    },
    stdin: stdin === undefined ? 'inherit' : 'pipe',
    stdout: 'pipe',
    stderr: 'pipe',
  })
  if (stdin !== undefined) {
    if (proc.stdin == null) throw new Error('expected proc.stdin to be writable')
    proc.stdin.write(stdin)
    await proc.stdin.end()
  }
  const exitCode = await proc.exited
  const stdout = await new Response(proc.stdout).text()
  const stderr = await new Response(proc.stderr).text()
  return { exitCode, stdout, stderr }
}

let sb: Sandbox | undefined

afterEach(() => {
  if (sb) {
    sb.cleanup()
    sb = undefined
  }
})

test('runs bun via the pre-existing .bun-result symlink without invoking nix', async () => {
  sb = makeSandbox({ nixBehavior: 'success' })
  // Pre-populate the symlink, as if a prior session had already built it.
  const resultDir = join(sb.pluginRoot, '.bun-result', 'bin')
  mkdirSync(resultDir, { recursive: true })
  symlinkSync(realBunPath(), join(resultDir, 'bun'))

  const r = await runWrapper(sb, ['--version'])
  expect(r.exitCode).toBe(0)
  // bun --version prints e.g. "1.3.13\n"
  expect(r.stdout).toMatch(/^\d+\.\d+\.\d+/)
  // nix must NOT have been called when the symlink already exists.
  expect(existsSync(sb.nixLog)).toBe(false)
})

test('lazy-builds .bun-result via nix when the symlink is missing', async () => {
  sb = makeSandbox({ nixBehavior: 'success' })
  expect(existsSync(join(sb.pluginRoot, '.bun-result'))).toBe(false)

  const r = await runWrapper(sb, ['--version'])
  expect(r.exitCode).toBe(0)
  expect(r.stdout).toMatch(/^\d+\.\d+\.\d+/)

  // nix was invoked, with --out-link pointing inside the plugin root and a
  // path: installable pointing at the plugin root.
  expect(existsSync(sb.nixLog)).toBe(true)
  const log = readFileSync(sb.nixLog, 'utf-8')
  expect(log).toContain('--out-link')
  expect(log).toContain(join(sb.pluginRoot, '.bun-result'))
  expect(log).toContain(`path:${sb.pluginRoot}`)

  // The symlink the stub created exists and is usable.
  expect(existsSync(join(sb.pluginRoot, '.bun-result', 'bin', 'bun'))).toBe(true)
})

test('forwards script args and stdin through to bun', async () => {
  sb = makeSandbox({ nixBehavior: 'success' })
  const resultDir = join(sb.pluginRoot, '.bun-result', 'bin')
  mkdirSync(resultDir, { recursive: true })
  symlinkSync(realBunPath(), join(resultDir, 'bun'))

  // A tiny bun script that echoes stdin to stdout — lets us verify the
  // wrapper forwards both argv AND stdin, not just argv.
  const script = join(sb.pluginRoot, 'hooks', 'echo-stdin.ts')
  writeFileSync(
    script,
    `const decoder = new TextDecoder()
let buf = ''
for await (const chunk of Bun.stdin.stream()) buf += decoder.decode(chunk, { stream: true })
buf += decoder.decode()
process.stdout.write(buf)
`,
  )

  const r = await runWrapper(sb, [script], 'hello from stdin')
  expect(r.exitCode).toBe(0)
  expect(r.stdout).toBe('hello from stdin')
})

test('exits non-zero and surfaces the error when nix build fails', async () => {
  sb = makeSandbox({ nixBehavior: 'fail' })
  const r = await runWrapper(sb, ['--version'])
  expect(r.exitCode).not.toBe(0)
  expect(r.stderr.length).toBeGreaterThan(0)
})
