/**
 * The AgentComms contract, run against a real Zulip realm.
 *
 * This is the Tier-0 "contract-against-real" run: the same
 * `runAgentCommsContract` suite that the stateful fake and the memory adapter
 * pass, pointed at a live realm via a ContractFactory built on the real
 * `makeZulipHttp` + minter-driven identity flow (the seam reused from
 * `realm.live.test.ts`).
 *
 * **Realm-friendliness strategy** (the shared realm is also serving other live
 * clients — saturating it knocks them off MCP):
 *
 * - **Per-suite identity, per-test adapter.** The `self` bot
 *   (`cc-contract-self`) is persistent: acquired via reactivate+regenerate,
 *   released `{ persistent: true }` so it is never deactivated and the next
 *   run re-acquires it without a mint. A fresh adapter instance is built per
 *   test (per `factory()` call) so each test gets a fresh Zulip events queue
 *   (`/register`) — a reused adapter re-polls a stale queue that silently
 *   stops delivering. The bot account is suite-scoped; only the adapter is
 *   per-test.
 * - **A fixed pool of channels, emptied before each use.** Zulip's API cannot
 *   delete a channel, and an archived one still loads in every web client, so
 *   a run must create none. `seedChannel(name)` maps each logical name the
 *   contract uses (`lobby`, `alpha`, …) to one permanent stream,
 *   `cc-ct-<name>`, created by the first run ever and reused by every run
 *   after. The contract's channel-wide reads assert exact contents, so the
 *   first seed of a name in a test empties that stream: it clears the
 *   description and moves every topic out to `cc-ct-spent`. The minter created
 *   both streams, so the realm makes it their administrator, which lets it
 *   move topics out. Emptying before use rather than after covers a run that
 *   failed or was killed. The fake sidesteps all this by building a fresh
 *   realm per `beforeEach`.
 * - **Shared peer agents.** `seedAgent(name)` find-or-creates a persistent
 *   peer bot (`cc-contract-peer-<name>`) once and reuses it — peers carry no
 *   per-test state, so suite-level reuse costs no per-test mint.
 * - **`newUnacquiredAdapter` omitted.** The contract sanctions this for
 *   substrates "whose acquire-acceptable names are pinned by external
 *   configuration (transitional Zulip)". It skips the rebind/no-op lifecycle
 *   tests.
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
import {
  type AttachmentContractFactory,
  runAgentCommsContract,
  runAttachmentStoreContract,
} from '@commy/testing/contract'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Duration, Effect, Option, Redacted, Schema } from 'effect'
import type { ZulipAdapter } from './adapter.ts'
import { zulipAdapter } from './adapter.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'
import { permalinkBase, withChannelPermalink } from './permalink.ts'

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
// limit and knocks other live clients off MCP for the cool-off.
const MINTER_PACE = Duration.millis(900)

const SELF_NAME = decodeBotNameSync('cc-contract-self')
// ponytail: one pool per realm, so two runs at once on the same realm
// trample each other's channels; namespace the pool per machine if that happens
const POOL_PREFIX = 'cc-ct'
const SPENT_CHANNEL = `${POOL_PREFIX}-spent`
const PEER_PREFIX = 'cc-contract-peer'

const successSchema = Schema.Struct({ result: Schema.Literal('success') })
const streamIdSchema = Schema.Struct({ result: Schema.Literal('success'), stream_id: Schema.Int })
const streamSchema = Schema.Struct({
  result: Schema.Literal('success'),
  stream: Schema.Struct({ description: Schema.String }),
})
const topicsSchema = Schema.Struct({
  result: Schema.Literal('success'),
  topics: Schema.Array(Schema.Struct({ max_id: Schema.Int })),
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

  // Pool stream ids never change, so each is looked up once per run.
  const poolIds = new Map<string, number>()

  // Persistent peer bots are find-or-created once and cached for the suite.
  const peerCache = new Map<string, Identity>()

  // A peer bot bound to its own adapter so it can POST (not just resolve) —
  // drives the contract's mention-floor tests, where a peer @-mentions self.
  // Acquired once per peer name and reused for the suite (posting carries no
  // per-test queue state, unlike the per-test self adapter); the regenerate
  // costs one minter-paced acquire total.
  const peerAdapterCache = new Map<string, ZulipAdapter>()

  const postingPeerAdapter = (realName: string): Effect.Effect<ZulipAdapter> =>
    Effect.gen(function* () {
      const cached = peerAdapterCache.get(realName)
      if (cached !== undefined) return cached
      const peerAdapter = yield* buildAdapter()
      builtAdapters.push(peerAdapter)
      yield* Effect.sleep(MINTER_PACE).pipe(
        Effect.zipRight(peerAdapter.identity.acquire(decodeBotNameSync(realName))),
      )
      peerAdapterCache.set(realName, peerAdapter)
      return peerAdapter
    }).pipe(Effect.orDie)

  // Adapters built per test, torn down by the module afterAll — the contract's
  // per-test dispose() releases the binding (persistent) but the underlying
  // event pump is unwound here so handles don't leak across the run.
  const builtAdapters: ZulipAdapter[] = []

  // Subscribing creates the stream only the first time any run asks for it.
  const poolStream = (minter: ZulipHttp, realName: string) =>
    Effect.gen(function* () {
      const cached = poolIds.get(realName)
      if (cached !== undefined) return cached
      yield* minter.post('/users/me/subscriptions', successSchema, {
        subscriptions: JSON.stringify([{ name: realName }]),
      })
      const { stream_id } = yield* minter.get('/get_stream_id', streamIdSchema, {
        stream: realName,
      })
      poolIds.set(realName, stream_id)
      return stream_id
    })

  // A minter that is not a moderator may move a message only within the
  // realm's move time limit, so a stream left unemptied for longer than that
  // fails here until a realm administrator empties it.
  const emptyStream = (minter: ZulipHttp, streamId: number) =>
    Effect.gen(function* () {
      const spentId = yield* poolStream(minter, SPENT_CHANNEL)
      const { stream } = yield* minter.get(`/streams/${streamId}`, streamSchema)
      // Clearing a description posts a notice into the stream, so this goes
      // before the topics are moved out.
      if (stream.description !== '')
        yield* minter.patch(`/streams/${streamId}`, successSchema, { description: '' })
      const { topics } = yield* minter.get(`/users/me/${streamId}/topics`, topicsSchema)
      yield* Effect.forEach(
        topics,
        (topic) =>
          minter.patch(`/messages/${topic.max_id}`, successSchema, {
            stream_id: spentId,
            propagate_mode: 'change_all',
            send_notification_to_old_thread: false,
            send_notification_to_new_thread: false,
          }),
        { discard: true },
      )
    })

  const seedPoolChannel = (minter: ZulipHttp, name: string): Effect.Effect<ChannelRef> => {
    const realName = `${POOL_PREFIX}-${name}`
    return poolStream(minter, realName).pipe(
      Effect.tap((streamId) => emptyStream(minter, streamId)),
      Effect.map((streamId) =>
        withChannelPermalink(permalinkBase({ realmUrl: Effect.runSync(RealmUrl(liveEnv.site)) }), {
          id: decodeChannelIdSync(String(streamId)),
          name: decodeChannelNameSync(realName),
        }),
      ),
      Effect.orDie,
    )
  }

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

  const liveContractFactory: AttachmentContractFactory = async () => {
    const adapter = await Effect.runPromise(buildAdapter())
    builtAdapters.push(adapter)
    const minter = await Effect.runPromise(buildMinterHttp())
    await Effect.runPromise(
      Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.acquire(SELF_NAME))),
    )

    // The self bot is persistent, so subscriptions from earlier tests and runs
    // outlive them and would break the tests that assert the exact set.
    const held = await Effect.runPromise(adapter.inbox.subscriptions().pipe(Effect.orDie))
    for (const channel of held)
      await Effect.runPromise(adapter.inbox.unsubscribe(channel).pipe(Effect.orDie))

    // A name seeded twice in one test is the same channel, emptied only once.
    const seeded = new Map<string, ChannelRef>()

    return {
      comms: adapter,
      attachments: adapter,
      seedChannel: (name) => {
        const cached = seeded.get(name)
        if (cached !== undefined) return Effect.succeed(cached)
        return seedPoolChannel(minter, name).pipe(
          Effect.tap((channel) => Effect.sync(() => seeded.set(name, channel))),
        )
      },
      seedAgent: (name) => ensurePeer(minter, adapter, name),
      // A seeded peer posts (mentioning self) through its own bound adapter so
      // the mention-floor tests run cross-identity — the shape that surfaces on
      // the real realm where a single-identity self-mention does not.
      peerPost: (peer, channel, body, opts) =>
        postingPeerAdapter(String(peer.name)).pipe(
          Effect.flatMap((peerAdapter) => peerAdapter.publisher.post(channel, body, opts)),
          Effect.asVoid,
          Effect.orDie,
        ),
      // Stream.MAX_DESCRIPTION_LENGTH — the realm's own cap.
      channelDescriptionLimit: 1024,
      messageLengthLimit: 10_000,
      // MAX_TOPIC_NAME_LENGTH — the realm's own cap.
      threadNameLimit: 60,
      dispose: () =>
        Effect.sleep(MINTER_PACE).pipe(
          Effect.zipRight(adapter.identity.release({ persistent: true })),
        ),
    }
  }

  runAgentCommsContract('zulip-live', liveContractFactory)
  runAttachmentStoreContract('zulip-live', liveContractFactory)
}
