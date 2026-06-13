/**
 * The AgentComms contract, run against a REAL Zulip realm.
 *
 * This is the Tier-0 "contract-against-real" run (comms-e5vm.5): the same
 * `runAgentCommsContract` suite that the stateful fake and the memory adapter
 * pass, pointed at a live realm via a ContractFactory built on the real
 * `makeZulipHttp` + minter-driven identity flow (the seam reused from
 * `realm.live.test.ts`).
 *
 * **Realm-friendliness strategy** (the shared realm is also serving live
 * concierges — saturating it knocks them off MCP):
 *
 * - **Per-suite identity, per-test adapter.** The `self` bot
 *   (`cc-contract-self`) is persistent: acquired via reactivate+regenerate,
 *   released `{ persistent: true }` so it is never deactivated and the next
 *   run re-acquires it without a mint. A FRESH adapter instance is built per
 *   test (per `factory()` call) so each test gets a fresh Zulip events queue
 *   (`/register`) — a reused adapter re-polls a stale queue that silently
 *   stops delivering (the realm.live.test.ts lesson, comms-hcw). The bot
 *   account is suite-scoped; only the adapter is per-test.
 * - **Per-test channel namespace.** `seedChannel(name)` creates a real stream
 *   named `cc-ct-<run>-<seq>-<name>` so each test reads/writes an isolated
 *   channel. The contract reuses fixed logical names (`lobby`, `alpha`) across
 *   ~50 tests; on a realm that persists channel state that is cross-test
 *   contamination WITHIN a run (the channel-wide `readChannel` range tests
 *   assert `not.toContain` / exact-equality). The fake sidesteps it by
 *   building a fresh realm per `beforeEach`; the live factory namespaces
 *   instead. Streams accumulate (the non-admin minter cannot delete them) —
 *   acceptable on a test realm (orchestrator ruling).
 * - **Shared peer agents.** `seedAgent(name)` find-or-creates a persistent
 *   peer bot (`cc-contract-peer-<name>`) once and reuses it — peers carry no
 *   per-test state, so suite-level reuse costs no per-test mint.
 * - **`newUnacquiredAdapter` omitted.** The contract sanctions this for
 *   substrates "whose acquire-acceptable names are pinned by external
 *   configuration (transitional Zulip)". It skips the rebind/no-op lifecycle
 *   tests, which on a non-admin minter would hit the admin-only
 *   reactivate path (comms-ch7) and fail.
 * - **Retry-After** is honoured automatically: the request layer's
 *   `rateLimitSchedule` absorbs 429s under a cumulative-wait budget.
 *
 * **Local-only** — env-gated like `realm.live.test.ts`, excluded from default
 * discovery via `bunfig.toml`, run via `bun run test:live`. With env vars
 * unset the suite is skipped.
 */

import { afterAll, describe, test } from 'bun:test'
import type { ChannelRef, Identity } from '@commy/core/ports'
import {
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
} from '@commy/core/ports'
import { runAgentCommsContract } from '@commy/testing/contract'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Duration, Effect, Option, Redacted, Schema } from 'effect'
import type { ZulipAdapter } from './adapter.ts'
import { zulipAdapter } from './adapter.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'

interface LiveEnv {
  readonly site: string
  readonly minterEmail: string
  readonly minterApiKey: string
}

const readEnv = (): LiveEnv | undefined => {
  const required = {
    site: process.env['ZULIP_SITE'],
    minterEmail: process.env['ZULIP_MINTER_EMAIL'],
    minterApiKey: process.env['ZULIP_MINTER_API_KEY'],
  }
  for (const value of Object.values(required)) {
    if (value === undefined || value.length === 0) return undefined
  }
  return required as Record<keyof typeof required, string>
}

const env = readEnv()

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

// Spacing between minter-authenticated calls (acquire/release/regenerate),
// mirroring realm.live.test.ts. The contract regenerates the self key once per
// test (~50×) through the shared minter; bursting trips Zulip's per-user rate
// limit and knocks live concierges off MCP for the cool-off (comms-jfd).
const MINTER_PACE = Duration.millis(900)

const SELF_NAME = decodeBotNameSync('cc-contract-self')
const NS_PREFIX = 'cc-ct'
const PEER_PREFIX = 'cc-contract-peer'

const successSchema = Schema.Struct({ result: Schema.Literal('success') })
const streamsListSchema = Schema.Struct({
  result: Schema.Literal('success'),
  streams: Schema.Array(Schema.Struct({ name: Schema.NonEmptyString, stream_id: Schema.Int })),
})
const usersListSchema = Schema.Struct({
  result: Schema.Literal('success'),
  members: Schema.Array(
    Schema.Struct({
      user_id: Schema.Int,
      full_name: Schema.String,
      is_bot: Schema.Boolean,
      is_active: Schema.Boolean,
    }),
  ),
})
const newBotSchema = Schema.Struct({
  result: Schema.Literal('success'),
  api_key: Schema.NonEmptyString,
  user_id: Schema.Int,
})

const SHORT_NAME_PATTERN = /[^a-z0-9._-]/g
const sanitiseShortName = (name: string): string =>
  name
    .toLowerCase()
    .replace(SHORT_NAME_PATTERN, '-')
    .replace(/^-+|-+$/g, '') || 'bot'

if (env === undefined) {
  describe.skip('AgentComms contract — zulip-live (env unset)', () => {
    test('skipped — set ZULIP_SITE / ZULIP_MINTER_EMAIL / ZULIP_MINTER_API_KEY', () => {})
  })
} else {
  const liveEnv = env

  const buildMinterHttp = (): Effect.Effect<ZulipHttp> =>
    makeZulipHttp({
      realmUrl: Effect.runSync(RealmUrl(liveEnv.site)),
      email: Effect.runSync(BotEmail(liveEnv.minterEmail)),
      apiKey: Effect.runSync(ApiKey(liveEnv.minterApiKey)),
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

  const buildAdapter = (): Effect.Effect<ZulipAdapter> =>
    zulipAdapter({
      realmUrl: Effect.runSync(RealmUrl(liveEnv.site)),
      minterEmail: Effect.runSync(BotEmail(liveEnv.minterEmail)),
      minterApiKey: Redacted.make(Effect.runSync(ApiKey(liveEnv.minterApiKey))),
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

  // One process-wide run id keeps each `bun run test:live` invocation's streams
  // distinct from prior runs; the per-call sequence keeps tests distinct within
  // a run. (Test files may use Date.now — only Workflow scripts may not.)
  const runId = Date.now().toString(36)
  let seq = 0

  // Persistent peer bots are find-or-created once and cached for the suite.
  const peerCache = new Map<string, Identity>()

  // Adapters built per test, torn down by the module afterAll — the contract's
  // per-test dispose() releases the binding (persistent) but the underlying
  // event pump is unwound here so handles don't leak across the run.
  const builtAdapters: ZulipAdapter[] = []

  const ensureStream = (minter: ZulipHttp, realName: string): Effect.Effect<ChannelRef> =>
    minter
      .post('/users/me/subscriptions', successSchema, {
        subscriptions: JSON.stringify([{ name: realName }]),
      })
      .pipe(
        Effect.zipRight(
          minter.get('/streams', streamsListSchema, {
            include_public: true,
            include_subscribed: true,
          }),
        ),
        Effect.flatMap((res) => {
          const found = res.streams.find((s) => s.name === realName)
          if (found === undefined)
            return Effect.die(new Error(`seedChannel: stream ${realName} not found after create`))
          return Effect.succeed<ChannelRef>({
            id: decodeChannelIdSync(String(found.stream_id)),
            name: decodeChannelNameSync(realName),
          })
        }),
        Effect.orDie,
      )

  const ensurePeer = (
    minter: ZulipHttp,
    adapter: ZulipAdapter,
    name: string,
  ): Effect.Effect<Identity> =>
    Effect.gen(function* () {
      const cached = peerCache.get(name)
      if (cached !== undefined) return cached
      const realName = `${PEER_PREFIX}-${name}`
      const members = yield* minter.get('/users', usersListSchema)
      const existing = members.members.find(
        (u) => u.full_name === realName && u.is_bot && u.is_active,
      )
      if (existing === undefined) {
        yield* minter.post('/bots', newBotSchema, {
          full_name: realName,
          short_name: sanitiseShortName(realName),
          bot_type: 1,
        })
      }
      const resolved = yield* adapter.identity.resolve(decodeDisplayNameSync(realName))
      if (Option.isNone(resolved))
        return yield* Effect.die(
          new Error(`seedAgent: peer ${realName} not resolvable after create`),
        )
      peerCache.set(name, resolved.value)
      return resolved.value
    }).pipe(Effect.orDie)

  afterAll(async () => {
    for (const a of builtAdapters) await a.close()
  })

  runAgentCommsContract('zulip-live', async () => {
    const ns = `${NS_PREFIX}-${runId}-${seq++}`
    const adapter = await Effect.runPromise(buildAdapter())
    builtAdapters.push(adapter)
    const minter = await Effect.runPromise(buildMinterHttp())
    await Effect.runPromise(
      Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.acquire(SELF_NAME))),
    )

    return {
      comms: adapter,
      seedChannel: (name) => ensureStream(minter, `${ns}-${name}`),
      seedAgent: (name) => ensurePeer(minter, adapter, name),
      // Zulip stamps integer-second timestamps, so the range/replay tests that
      // need distinct per-message ts can't hold here (covered by memory/fake).
      coarseTimestamps: true,
      // The shared realm doesn't surface a bot's own posts/mentions on its own
      // events() within the contract's window (inline readiness too tight;
      // minter-side `is:mentioned` narrow keyed to the queue owner).
      // realm.live.test.ts owns the live event-delivery coverage proper.
      noSelfEventDelivery: true,
      dispose: () =>
        Effect.sleep(MINTER_PACE).pipe(
          Effect.zipRight(adapter.identity.release({ persistent: true })),
        ),
    }
  })
}
