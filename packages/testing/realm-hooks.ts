import { afterEach, beforeEach } from 'bun:test'

/**
 * bun:test lifecycle hooks keep their own 5s timeout, independent of any
 * per-test timeout. In isolation a real in-process realm starts and stops in a
 * few ms, but under a full-parallel `bun run check` the starved event loop can
 * stretch that setup/teardown past 5s — and then the HOOK, not the test body,
 * times out, landing the failure on whichever test the hook happened to be
 * wrapping. 30s gives that contention wide headroom while still
 * failing if a hook genuinely wedges.
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
