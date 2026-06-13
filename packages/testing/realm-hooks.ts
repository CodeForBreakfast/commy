import { afterEach, beforeEach } from 'bun:test'

/**
 * bun:test lifecycle hooks keep their own 5s timeout, independent of any
 * per-test timeout. In isolation a real in-process realm starts and stops in a
 * few ms, but under a full-parallel `bun run check` the starved event loop can
 * stretch that setup/teardown past 5s — and then the HOOK, not the test body,
 * times out, landing the failure on whichever test the hook happened to be
 * wrapping (comms-xwqm). 30s gives that contention wide headroom while still
 * failing if a hook genuinely wedges.
 *
 * The infinite-long-poll cases that used to strain even this headroom (the
 * gap-replay / scope-close-interrupt drains whose forked fiber's AbortSignal
 * unwinding could starve teardown under aggressive contention) no longer run
 * here: comms-e5vm.2 moved that LOGIC onto the stub HttpClient + TestClock
 * (deterministic, no socket, no forked drain), and the one genuinely-socket
 * teardown assertion that remains is self-contained in
 * `packages/zulip/scope-teardown.test.ts` with its own `Effect.acquireRelease`
 * (comms-4lz5). So every fixture still wired through these hooks is a plain
 * request/response realm that starts and stops in a few ms — the 30s is wide
 * headroom for contention, not a cure for a teardown-starvation mode that is
 * now structurally gone.
 */
export const REALM_HOOK_TIMEOUT_MS = 30_000

/**
 * Register per-test realm lifecycle on the bun:test hooks — a fresh instance
 * before each test, stopped after — with the contention-proof timeout above.
 * The caller keeps its own directly-referenced binding via `assign`, so
 * existing `realm.` / `fixture.` call sites stay untouched.
 */
export const registerRealmHooks = <T extends { readonly stop: () => Promise<void> }>(
  start: () => T,
  assign: (instance: T) => void,
): void => {
  let instance: T
  beforeEach(() => {
    instance = start()
    assign(instance)
  }, REALM_HOOK_TIMEOUT_MS)
  afterEach(() => instance.stop(), REALM_HOOK_TIMEOUT_MS)
}
