import { basename } from 'node:path'
import type { AgentComms, Capabilities } from '@commy/core/ports'
import { decodeAttachmentRefSync } from '@commy/core/ports'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { Duration, Effect } from 'effect'

/**
 * The single seam where above-the-port tests name the Zulip adapter.
 *
 * Above-port unit tests (`server.test.ts`, `server.integration.test.ts`, the
 * `disconnect-exit` fixture) drive the substrate through the in-memory adapter
 * or a hand-rolled port fake — never the real Zulip adapter (see
 * docs/architecture.md § Test architecture). But the `SubstrateAdapter` port
 * those programs depend on is currently *typed as* {@link ZulipAdapter}, so any
 * provided double must be completed from the universal {@link AgentComms} core
 * to that Zulip-shaped aggregate: `reconcileMinterSubscriptions`,
 * `downloadFile`, `uploadFile`, `close`. Concentrating that completion here
 * keeps the rule self-enforcing: among the above-port tests, `@commy/zulip`
 * appears in exactly one module, this one. Tests that deliberately drive the
 * real adapter (`queue-resume`, `bootstrap`, the live suite) name it for the
 * same reason `bootstrap.ts` does — they are below the port or are the
 * composition root, and are not what this seam is about.
 *
 * The members themselves no longer speak Zulip. `downloadFile` / `uploadFile`
 * are the port's `AttachmentStore`, so their doubles are built from
 * `@commy/core/ports` types alone; only `reconcileMinterSubscriptions` and
 * `close` are still Zulip-shaped, and they are why the aggregate is still
 * named here at all.
 *
 * `ZulipAdapter` is re-exported so callers annotate their doubles without
 * naming `@commy/zulip` themselves.
 */
export type { ZulipAdapter } from '@commy/zulip/adapter'

/** The four members that complete `AgentComms` to a `ZulipAdapter`. */
type SubstrateExtras = Pick<
  ZulipAdapter,
  'reconcileMinterSubscriptions' | 'downloadFile' | 'uploadFile' | 'close'
>

/**
 * Per-member overrides. Anything omitted falls back to an inert default: a
 * no-op reconcile report, an empty download, a stub upload result, a no-op
 * close. Tests override only the member whose behaviour they actually assert.
 */
type SubstrateExtrasOverrides = Partial<SubstrateExtras>

const stubAttachmentRef = decodeAttachmentRefSync('/user_uploads/0/stub')

const inertExtras: SubstrateExtras = {
  reconcileMinterSubscriptions: () => Effect.succeed({ added: [], error: undefined }),
  // Empty bytes, but a real filename: the port's contract is that the adapter
  // names the file, so a double that answered a constant would let a caller
  // deriving its own name from the handle pass its tests.
  downloadFile: (ref) =>
    Effect.succeed({
      data: new Uint8Array([]),
      contentType: 'application/octet-stream',
      filename: basename(ref),
    }),
  uploadFile: () =>
    Effect.succeed({
      ref: stubAttachmentRef,
      filename: 'stub',
      reference: '[stub](/user_uploads/0/stub)',
    }),
  close: async () => {},
}

/**
 * Above-port tests don't exercise timestamp granularity, so a hand-rolled port
 * fake need not declare it; the inert default stands in (a real adapter passed
 * as `base` overrides it via the spread).
 */
const inertCapabilities: Capabilities = { timestampGranularity: Duration.zero }

/**
 * Complete an {@link AgentComms} core (the in-memory adapter, or a hand-rolled
 * port fake) to the `ZulipAdapter` shape the `SubstrateAdapter` port expects.
 * Pass overrides for the Zulip-shaped members a given test asserts on (e.g. a
 * counting `close`); the rest stay inert. `capabilities` may be omitted from a
 * hand-rolled `base` — the inert default fills it.
 */
export const completeAsSubstrate = (
  base: Omit<AgentComms, 'capabilities'> & Partial<Pick<AgentComms, 'capabilities'>>,
  overrides: SubstrateExtrasOverrides = {},
): ZulipAdapter => ({
  capabilities: inertCapabilities,
  ...base,
  ...inertExtras,
  ...overrides,
})
