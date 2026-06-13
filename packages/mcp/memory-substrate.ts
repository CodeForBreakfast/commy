import type { AgentComms } from '@commy/core/ports'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { decodeUserUploadPathSync } from '@commy/zulip/http'
import { Effect } from 'effect'

/**
 * The single seam where above-the-port tests touch a Zulip type or brand.
 *
 * Above-port unit tests (`server.test.ts`, `server.integration.test.ts`, the
 * `disconnect-exit` fixture) drive the substrate through the in-memory adapter
 * or a hand-rolled port fake — never the real Zulip adapter (see
 * docs/architecture.md § Test architecture). But the `SubstrateAdapter` port
 * those programs depend on is currently *typed as* {@link ZulipAdapter}, so any
 * provided double must be completed from the universal {@link AgentComms} core
 * to that Zulip-shaped aggregate: `reconcileMinterSubscriptions`,
 * `downloadFile`, `uploadFile`, `close`. Concentrating that completion — and the
 * lone `UserUploadPath` brand mint it needs — here keeps the rule self-enforcing:
 * `@commy/zulip` appears in exactly one test-side module, this one.
 *
 * `ZulipAdapter` is re-exported so callers annotate their doubles without
 * naming `@commy/zulip` themselves.
 */
export type { ZulipAdapter } from '@commy/zulip/adapter'

/** The four Zulip-shaped members that complete `AgentComms` to a `ZulipAdapter`. */
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

const inertExtras: SubstrateExtras = {
  reconcileMinterSubscriptions: () => Effect.succeed({ added: [], error: undefined }),
  downloadFile: () =>
    Effect.succeed({ data: new Uint8Array([]), contentType: 'application/octet-stream' }),
  uploadFile: () =>
    Effect.succeed({ url: decodeUserUploadPathSync('/user_uploads/0/stub'), filename: 'stub' }),
  close: async () => {},
}

/**
 * Complete an {@link AgentComms} core (the in-memory adapter, or a hand-rolled
 * port fake) to the `ZulipAdapter` shape the `SubstrateAdapter` port expects.
 * Pass overrides for the Zulip-shaped members a given test asserts on (e.g. a
 * counting `close`); the rest stay inert.
 */
export const completeAsSubstrate = (
  base: AgentComms,
  overrides: SubstrateExtrasOverrides = {},
): ZulipAdapter => ({
  ...base,
  ...inertExtras,
  ...overrides,
})
