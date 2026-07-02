import { expect, test } from 'bun:test'

/**
 * Runtime proof: the stdio MCP server exits when its client
 * closes stdin, over a real OS pipe and a real spawned process — the
 * coverage the earlier unit tests lacked. Those fed `clientDisconnect` a bare
 * `EventEmitter`; that proves the listener wiring but not that bun's
 * `process.stdin` actually emits `end`/`close` on a real pipe peer-close,
 * which is the property the production launch depends on once the server
 * is claude's direct child (no `bun run start` grandchild — see
 * `clients/claude-code/launch.test.ts`).
 *
 * The fixture boots the real program on a network-free memory substrate
 * whose pump parks forever, so only the disconnect can unwind it. It
 * announces `FIXTURE_ARMED` once `clientDisconnect` has attached its
 * listeners; we close stdin only after that, then require the process to
 * exit cleanly within a bounded window. If it hangs, the disconnect signal
 * never fired — the orphan-leak regression.
 */

const FIXTURE = `${import.meta.dir}/disconnect-exit.fixture.ts`
const REPO_ROOT = `${import.meta.dir}/../..`

const ARMED_TIMEOUT_MS = 15_000
const EXIT_TIMEOUT_MS = 10_000

const TIMED_OUT = Symbol('timed-out')
const withTimeout = <T>(p: Promise<T>, ms: number): Promise<T | typeof TIMED_OUT> =>
  Promise.race([p, Bun.sleep(ms).then((): typeof TIMED_OUT => TIMED_OUT)])

test('the server exits cleanly when its client closes stdin (real pipe, direct child)', async () => {
  const proc = Bun.spawn(['bun', FIXTURE], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      // Hermetic, realm-free: the memory substrate ignores these, but
      // `parseEnv` validates their shape during boot.
      ZULIP_SITE: 'https://zulip.example.com',
      ZULIP_MINTER_EMAIL: 'minter-bot@zulip.example.com',
      ZULIP_MINTER_API_KEY: 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk1',
      COMMY_BOT_NAME: 'disconnect-exit-fixture',
      COMMY_SUBSCRIBE: '',
    },
    stdin: 'pipe',
    stdout: 'ignore',
    stderr: 'pipe',
  })

  let stderr = ''
  let onArmed: () => void = () => {}
  const armed = new Promise<void>((resolve) => {
    onArmed = resolve
  })
  const draining = (async () => {
    const decoder = new TextDecoder()
    for await (const chunk of proc.stderr as ReadableStream<Uint8Array>) {
      stderr += decoder.decode(chunk, { stream: true })
      if (stderr.includes('FIXTURE_ARMED')) onArmed()
    }
  })()

  try {
    const reachedArmed = await withTimeout(armed, ARMED_TIMEOUT_MS)
    expect(reachedArmed, `fixture never armed the disconnect race; stderr:\n${stderr}`).not.toBe(
      TIMED_OUT,
    )

    // Negative control: while stdin stays open the parked pump must keep
    // the server alive. Without this, a server that self-exited for any
    // other reason would make the exit-after-close assertion meaningless.
    const aliveWhileConnected = await withTimeout(proc.exited, 1500)
    expect(
      aliveWhileConnected,
      `server exited before stdin close — pump did not park, so the test cannot attribute exit to disconnect; stderr:\n${stderr}`,
    ).toBe(TIMED_OUT)

    // Close the client's end of the pipe — the only disconnect signal a
    // plain parent exit delivers. A direct child sees this as stdin EOF.
    proc.stdin.end()

    const exitCode = await withTimeout(proc.exited, EXIT_TIMEOUT_MS)
    expect(
      exitCode,
      `server did not exit after stdin close — orphan leak; stderr:\n${stderr}`,
    ).not.toBe(TIMED_OUT)
    expect(exitCode).toBe(0)
  } finally {
    proc.kill()
    await draining.catch(() => {})
  }
})
