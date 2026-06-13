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
 * This is headroom, not a cure-all: the long-poll teardown tests (the
 * gap-replay / scope-close-interrupt cases that drive an infinite server
 * handler and rely on AbortSignal unwinding a forked drain fiber) can still
 * exceed even this under aggressive contention. That residual is a fiber/scope
 * lifecycle problem, not a too-small-number problem, and is tracked for the
 * Effect-Scope-based realm rework (comms-4lz5 / comms-30hq) — running the whole
 * test, realm acquisition included, inside one scope with guaranteed finalizers
 * removes the leftover-fiber-starves-teardown failure mode structurally.
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
