/**
 * Optional live test against `zulip.example.com`.
 *
 * Exercises the commy plugin end-to-end against a real Zulip
 * realm — booting the program (`makeProgram`) with the real
 * `zulipAdapter` provided through the app Layer, connecting an MCP
 * client through `InMemoryTransport`, and round-tripping the publisher /
 * history / identity tools.
 *
 * The substrate-level live coverage already lives in
 * `adapters/zulip/realm.live.test.ts`; this file's job is the plugin
 * layer that sits on top — program boot, MCP server wiring, identity
 * cache, narrow-set subscribe path.
 *
 * Skipped silently when the live env vars are unset; never runs in CI.
 *
 * Required env vars (all must be present, else the suite is skipped):
 * - `ZULIP_SITE`              e.g. `https://zulip.example.com`
 * - `ZULIP_MINTER_EMAIL`      minter user email
 * - `ZULIP_MINTER_API_KEY`    minter user API key
 * - `ZULIP_LIVE_CHANNEL_NAME` stream the plugin subscribes to
 *
 * Realm-side churn is kept low by reusing the same bot
 * names across runs — first run mints, subsequent runs reactivate then
 * deactivate the existing account.
 */

import { describe, expect, test } from 'bun:test'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { stderrLoggerLayer } from '@commy/core/logging'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { zulipAdapter } from '@commy/zulip/adapter'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl } from '@commy/zulip/http'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import {
  Array as Arr,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Redacted,
  Ref,
  Schedule,
  Schema,
  type Scope,
  Stream,
} from 'effect'
import { parseEnv, substrateAdapterLayer } from './bootstrap.ts'
import { FileCursorStoreLive } from './cursor-store.ts'
import { buildQueueStateHooks } from './queue-state-hooks.ts'
import { FileQueueStateStoreLive, QueueStateStoreTag } from './queue-state-store.ts'
import { ResumeOutcomeLive, ResumeOutcome as ResumeOutcomeService } from './resume-outcome.ts'
import { makeProgram } from './server.ts'
import { SessionIdLive, SessionId as SessionIdService } from './session-id.ts'
import { FileSubscriptionStoreLive } from './subscription-store.ts'
import { testPlatformLayer } from './test-platform.ts'

/**
 * Realm-call observatory, read by the delivery-fidelity soak.
 *
 * Every call this suite makes — the seat's boot and long-poll, and the minter's
 * own posts — flows through the one module-level client, so wrapping it once
 * accounts for a run's whole cost against Zulip's per-user budget (GCRA,
 * `api_by_user` at 60/200: ~3.3 req/s sustained on a burst of 200, draining
 * continuously rather than resetting).
 *
 * `rateLimited` is the soak's exclusion signal rather than a statistic. A 429
 * means the realm decided when an event arrived, so a trial spanning one is
 * VOID, not a miss — `http.ts` retries 429s on a wait budget, which turns
 * saturation into delay, and delay past the fixture's timeout is
 * indistinguishable from the silent drop being hunted. Counting those as hits
 * would contaminate the result in the direction that looks like a finding.
 */
const realmObservations = Effect.runSync(Ref.make({ requests: 0, rateLimited: 0 }))

const httpClient = Effect.runSync(
  HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)),
).pipe(
  HttpClient.tapRequest(() =>
    Ref.update(realmObservations, (o) => ({ ...o, requests: o.requests + 1 })),
  ),
  HttpClient.tap((response) =>
    response.status === 429
      ? Ref.update(realmObservations, (o) => ({ ...o, rateLimited: o.rateLimited + 1 }))
      : Effect.void,
  ),
)

interface LiveEnv {
  readonly site: string
  readonly minterEmail: string
  readonly minterApiKey: string
  readonly channelName: string
}

const readEnv = (): LiveEnv | undefined => {
  const required = {
    site: process.env['ZULIP_SITE'],
    minterEmail: process.env['ZULIP_MINTER_EMAIL'],
    minterApiKey: process.env['ZULIP_MINTER_API_KEY'],
    channelName: process.env['ZULIP_LIVE_CHANNEL_NAME'],
  }
  for (const value of Object.values(required)) {
    if (value === undefined || value.length === 0) return undefined
  }
  return required as LiveEnv
}

const env = readEnv()
const describeLive = env === undefined ? describe.skip : describe

/** Narrow `env` inside test callbacks where describe.skip has already guarded. */
const liveEnv = (): LiveEnv => {
  if (env === undefined) throw new Error('unreachable — describe.skip should have run')
  return env
}

// Same minter-call spacing as the substrate live suite —
// boot's reconcile + acquire + subscribe sequence plus per-test
// release all hit the shared minter, so the plugin live suite trips
// the same per-user limit if we don't pace.
const MINTER_PACE = Duration.millis(900)

// Pre-provisioned bot names (avoid per-run realm-side churn).
// First run mints; the persistent bot stays active across runs
// (COMMY_BOT_NAME releases skip deactivation), so
// subsequent runs re-acquire the still-active account.
const PERSISTENT_BOT_NAME = 'cc-live-pl-test'
// UUID-shaped session ids — the session_id parser rejects anything else.
// Deterministic prefixes give predictable bot
// names (`cc-<first-8-hex>`) so the teardown assertions can fetch by
// name; the trailing UUID body is arbitrary but must be UUID-shaped
// (the session-id validator in bootstrap.ts).
const EPHEMERAL_SESSION_A = 'aaaaaaaa-0000-0000-0000-000000000001'
const EPHEMERAL_SESSION_B = 'bbbbbbbb-0000-0000-0000-000000000001'
const EPHEMERAL_BOT_A = `cc-${EPHEMERAL_SESSION_A.slice(0, 8)}`
const EPHEMERAL_BOT_B = `cc-${EPHEMERAL_SESSION_B.slice(0, 8)}`

// Fixed boot session id shared across the resume test's two boots — the id the
// on-disk subscription store keys on, and the id CC re-injects into a resumed
// MCP child's env. Setting it ourselves is this test's one honest limit (it
// stubs the zero-action id SOURCE); CC's boot-env injection on resume was
// confirmed separately on CC 2.1.201 (comms-k7cv.4, and live via comms-k7cv.7).
const RESUME_SESSION = 'cccccccc-0000-0000-0000-000000000001'

// A second fixed boot session id, distinct from RESUME_SESSION so the two
// resume tests never share an on-disk store or a minted `cc-<8>` bot. This one
// keys the DOWNTIME queue-replay acceptance: the reaction lands while the seat
// is dead, so boot-2 must resume the surviving queue rather than catch a live
// post-resume event.
const DOWNTIME_RESUME_SESSION = 'dddddddd-0000-0000-0000-000000000001'

// Fixed boot session id for the delivery-fidelity soak, distinct from every
// other so its store and its minted `cc-<8>` bot are never shared.
const FIDELITY_SESSION = 'eeeeeeee-0000-0000-0000-000000000001'

// How many live events the soak observes. The default is deliberately small:
// this suite runs against the realm the fleet coordinates on, and the number
// that makes a null informative is a number worth choosing on purpose. See the
// soak's own comment for what each N buys.
const SOAK_EVENTS = Number(process.env['COMMY_FIDELITY_EVENTS'] ?? '5')

// Per-event patience. Generous relative to observed live delivery (sub-second)
// so a slow realm is not miscounted as a drop.
const SOAK_EVENT_TIMEOUT = Duration.seconds(30)

const postedMessageSchema = Schema.Struct({
  result: Schema.Literal('success'),
  id: Schema.Int,
})

const usersListSchema = Schema.Struct({
  result: Schema.Literal('success'),
  members: Schema.Array(
    Schema.Struct({
      user_id: Schema.Int,
      full_name: Schema.String,
      is_active: Schema.Boolean,
      is_bot: Schema.Boolean,
    }),
  ),
})

interface ToolCallResult {
  readonly structuredContent?: unknown
  readonly content?: ReadonlyArray<{ readonly type: string; readonly text?: string }>
  readonly isError?: boolean
}

const callTool = (
  client: Client,
  name: string,
  args: Readonly<Record<string, unknown>> = {},
): Effect.Effect<ToolCallResult> =>
  Effect.promise(() => client.callTool({ name, arguments: args }) as Promise<ToolCallResult>)

const expectStructured = (result: ToolCallResult): Record<string, unknown> => {
  if (
    typeof result.structuredContent !== 'object' ||
    result.structuredContent === null ||
    Array.isArray(result.structuredContent)
  ) {
    throw new Error(
      `tool returned non-object structuredContent: ${JSON.stringify(result.structuredContent)}`,
    )
  }
  return result.structuredContent as Record<string, unknown>
}

// Wrap `inbox.events()` so a harness can stop the event pump cleanly on scope
// close. The deferred fires from the finalizer to interrupt the Stream —
// without it, the program's `await pump.done` never resolves and the test
// leaks the long-poll.
const withKillSwitch = (
  adapter: ZulipAdapter,
  killSwitch: Deferred.Deferred<void>,
): ZulipAdapter => ({
  ...adapter,
  inbox: {
    ...adapter.inbox,
    events: () => adapter.inbox.events().pipe(Stream.interruptWhenDeferred(killSwitch)),
  },
})

// Fork the boot program as a scoped daemon, register the teardown finalizer,
// and connect the MCP client — the tail every harness variant shares. The
// `buildProgram` callback fully provides the program's layers (differing per
// variant); the finalizer paces (the release runs a minter-authenticated
// DELETE, so leave the substrate suite's inter-call gap), fires the killSwitch
// to interrupt the pump's events stream so the scope unwinds (pump-cancel →
// release → close), joins the fiber (boot/teardown failure surfaces there, but
// the test body's assertions remain the sole pass/fail signal, so ignore the
// Exit), and closes the client.
const forkAndConnect = (
  killSwitch: Deferred.Deferred<void>,
  buildProgram: (serverTransport: InMemoryTransport) => Effect.Effect<unknown, unknown, never>,
): Effect.Effect<Client, never, Scope.Scope> =>
  Effect.gen(function* () {
    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'commy-live-test', version: '0.0.0' }, { capabilities: {} })
    const fiber = yield* Effect.forkScoped(buildProgram(serverTransport))
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Effect.sleep(MINTER_PACE)
        yield* Deferred.succeed(killSwitch, undefined)
        yield* Fiber.join(fiber).pipe(Effect.ignore)
        yield* Effect.promise(() => client.close())
      }),
    )
    yield* Effect.promise(() => client.connect(clientTransport))
    return client
  })

const buildHarness = (
  envOverrides: Readonly<Record<string, string>> = {},
): Effect.Effect<Client, never, Scope.Scope> =>
  Effect.gen(function* () {
    const e = liveEnv()

    const realAdapter = yield* zulipAdapter({
      realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
      minterEmail: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
      minterApiKey: Redacted.make(yield* ApiKey(e.minterApiKey).pipe(Effect.orDie)),
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

    const killSwitch = yield* Deferred.make<void>()
    const wrappedAdapter = withKillSwitch(realAdapter, killSwitch)

    const mainEnv: Record<string, string | undefined> = {
      ZULIP_SITE: e.site,
      ZULIP_MINTER_EMAIL: e.minterEmail,
      ZULIP_MINTER_API_KEY: e.minterApiKey,
      ...envOverrides,
    }

    // The boot program: the real adapter (parse-gated like
    // production's `ZulipAdapterLive`, so `close()` is a layer-scope
    // finalizer) and the file-backed cursor store arrive through the app
    // Layer; the env through an outermost ConfigProvider. No queue-state hooks:
    // this harness's adapter always registers a fresh queue, so its resume
    // coverage is the LIVE pump delivering a post-resume event — the DOWNTIME
    // queue-replay case is `buildResumeHarness` below.
    return yield* forkAndConnect(killSwitch, (serverTransport) =>
      makeProgram({ transport: serverTransport }).pipe(
        Effect.provide(
          Layer.provideMerge(
            Layer.mergeAll(
              substrateAdapterLayer(parseEnv.pipe(Effect.as(wrappedAdapter))),
              FileCursorStoreLive,
              // Feed the one shared session-id deferred into the store, which now
              // awaits it — mergeAll won't wire a sibling's output to a sibling's
              // input, so a plain merge leaves the store's SessionId unsatisfied.
              Layer.provideMerge(FileSubscriptionStoreLive, SessionIdLive),
              ResumeOutcomeLive,
              stderrLoggerLayer,
            ),
            testPlatformLayer(mainEnv),
          ),
        ),
      ),
    )
  })

// Harness variant that wires the production long-idle queue-resume hooks — the
// write half (register the events queue with the idle timeout, persist
// `{queueId, lastEventId}`) and the read half (`resumeQueue`) — over the SAME
// shared `SessionId` / `ResumeOutcome` deferreds the boot program fills, plus a
// file-backed `QueueStateStore` under the state home. This is the difference
// from `buildHarness` (which wires NO queue hooks, so its adapter always
// registers a fresh queue): only a resumed queue survives a downtime window and
// replays the events that landed while the seat was dead. Topology mirrors
// `ZulipAdapterLive` — one memoized `SessionId` the boot-env feeder fills and
// the hooks poll; building the adapter OUTSIDE the layer would mint private
// deferreds the feeder never fills, so `resumeQueue` would never see the id.
const buildResumeHarness = (
  envOverrides: Readonly<Record<string, string>> = {},
): Effect.Effect<Client, never, Scope.Scope> =>
  Effect.gen(function* () {
    const e = liveEnv()
    const killSwitch = yield* Deferred.make<void>()
    const mainEnv: Record<string, string | undefined> = {
      ZULIP_SITE: e.site,
      ZULIP_MINTER_EMAIL: e.minterEmail,
      ZULIP_MINTER_API_KEY: e.minterApiKey,
      ...envOverrides,
    }

    const adapterLayer = substrateAdapterLayer(
      Effect.gen(function* () {
        const parsed = yield* parseEnv
        const hooks = buildQueueStateHooks({
          store: yield* QueueStateStoreTag,
          session: yield* SessionIdService,
          idleTimeoutSecs: parsed.queueIdleTimeoutSecs,
          resumeOutcome: yield* ResumeOutcomeService,
        })
        const realAdapter = yield* zulipAdapter({
          realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
          minterEmail: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
          minterApiKey: Redacted.make(yield* ApiKey(e.minterApiKey).pipe(Effect.orDie)),
          queueIdleTimeoutSecs: hooks.queueIdleTimeoutSecs,
          onQueueRegister: hooks.onQueueRegister,
          onQueueAdvance: hooks.onQueueAdvance,
          resumeQueue: hooks.resumeQueue,
          onResumeOutcome: hooks.onResumeOutcome,
        }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))
        return withKillSwitch(realAdapter, killSwitch)
      }),
    )

    // Shared singletons under the platform base: `SessionIdLive` /
    // `ResumeOutcomeLive` / `FileQueueStateStoreLive` are provided ONCE and fed
    // to the adapter hooks, the subscription store, and the program — a single
    // memoized `SessionId` deferred the boot-env feeder fills. The platform base
    // (`FileSystem` + `ConfigProvider`) is the outermost provide, exactly as
    // `buildHarness` layers it.
    return yield* forkAndConnect(killSwitch, (serverTransport) =>
      makeProgram({ transport: serverTransport }).pipe(
        Effect.provide(
          Layer.provideMerge(
            Layer.mergeAll(
              adapterLayer,
              FileCursorStoreLive,
              FileSubscriptionStoreLive,
              stderrLoggerLayer,
            ).pipe(
              Layer.provideMerge(
                Layer.mergeAll(SessionIdLive, ResumeOutcomeLive, FileQueueStateStoreLive),
              ),
            ),
            testPlatformLayer(mainEnv),
          ),
        ),
      ),
    )
  })

const fetchActiveStatus = (
  fullName: string,
): Effect.Effect<{ exists: boolean; isActive: boolean }> =>
  Effect.gen(function* () {
    const e = liveEnv()
    const minterHttp = yield* makeZulipHttp({
      realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
      email: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
      apiKey: yield* ApiKey(e.minterApiKey).pipe(Effect.orDie),
    }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))
    const users = yield* minterHttp.get('/users', usersListSchema).pipe(Effect.orDie)
    const found = users.members.find((u) => u.full_name === fullName)
    if (found === undefined) return { exists: false, isActive: false }
    return { exists: true, isActive: found.is_active }
  })

describeLive('commy plugin live integration — zulip.example.com', () => {
  const TEST_TIMEOUT_MS = 60_000

  test(
    'persistent-mode round-trip: current_identity → post → read_channel → react → unreact',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const e = liveEnv()
          const status = yield* Effect.scoped(
            Effect.gen(function* () {
              // Pace before main()'s boot-time acquire (minter API).
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildHarness({
                COMMY_BOT_NAME: PERSISTENT_BOT_NAME,
                COMMY_SUBSCRIBE: `${e.channelName}`,
              })

              const ident = expectStructured(yield* callTool(client, 'current_identity'))
              expect(ident['state']).toBe('bound')
              const identity = ident['identity'] as { id: string; name: string; kind: string }
              expect(identity.name).toBe(PERSISTENT_BOT_NAME)
              expect(identity.kind).toBe('agent')

              const marker = `live-plugin-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
              const posted = expectStructured(
                yield* callTool(client, 'post', { channel_name: e.channelName, body: marker }),
              )
              const messageId = posted['message_id'] as string
              expect(typeof messageId).toBe('string')
              expect(posted['channel_name']).toBe(e.channelName)

              const history = expectStructured(
                yield* callTool(client, 'read_channel', { channel_name: e.channelName }),
              )
              const messages = history['messages'] as ReadonlyArray<{ body: string }>
              expect(messages.some((m) => m.body.includes(marker))).toBe(true)

              const reacted = expectStructured(
                yield* callTool(client, 'react', { message_id: messageId, emoji: 'thumbs_up' }),
              )
              expect(reacted).toEqual({})

              const unreacted = expectStructured(
                yield* callTool(client, 'unreact', { message_id: messageId, emoji: 'thumbs_up' }),
              )
              expect(unreacted).toEqual({})
            }),
            // The release runs as a scope finalizer, so the bot's status is
            // only settled once the scope above has closed.
          ).pipe(Effect.zipRight(fetchActiveStatus(PERSISTENT_BOT_NAME)))

          // A persistent COMMY_BOT_NAME release skips
          // deactivation so the next acquire stays on the owner-permitted
          // regenerate path. The bot therefore survives teardown active.
          expect(status.exists).toBe(true)
          expect(status.isActive).toBe(true)
        }),
      ),
    TEST_TIMEOUT_MS,
  )

  test(
    'ephemeral-mode: two distinct session_ids mint two cc-<8> bots; both deactivated on teardown',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const e = liveEnv()
          yield* Effect.scoped(
            Effect.gen(function* () {
              // Pace before main()'s boot-time minter calls (reconcile + subscribe).
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildHarness({
                COMMY_SUBSCRIBE: `${e.channelName}`,
              })

              // Session A — mints bot A.
              yield* Effect.sleep(MINTER_PACE)
              const postedA = expectStructured(
                yield* callTool(client, 'post', {
                  channel_name: e.channelName,
                  body: `live-ephemeral-A-${Date.now()}`,
                  session_id: EPHEMERAL_SESSION_A,
                }),
              )
              expect(typeof postedA['message_id']).toBe('string')

              // Session B — release-then-acquire transition. Bot A deactivates;
              // bot B mints.
              yield* Effect.sleep(MINTER_PACE)
              const postedB = expectStructured(
                yield* callTool(client, 'post', {
                  channel_name: e.channelName,
                  body: `live-ephemeral-B-${Date.now()}`,
                  session_id: EPHEMERAL_SESSION_B,
                }),
              )
              expect(typeof postedB['message_id']).toBe('string')
            }),
          )

          // Both bots must appear on the realm in deactivated state.
          const statusA = yield* fetchActiveStatus(EPHEMERAL_BOT_A)
          const statusB = yield* fetchActiveStatus(EPHEMERAL_BOT_B)
          expect(statusA.exists).toBe(true)
          expect(statusB.exists).toBe(true)
          expect(statusA.isActive).toBe(false)
          expect(statusB.isActive).toBe(false)
        }),
      ),
    TEST_TIMEOUT_MS,
  )

  test(
    'resume restore: boot-2 restores boot-1 persisted narrow set and delivers an inbound reaction with zero tool calls',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const e = liveEnv()
          const minterHttp = yield* makeZulipHttp({
            realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
            email: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
            apiKey: yield* ApiKey(e.minterApiKey).pipe(Effect.orDie),
          }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

          // Isolated on-disk state home so the two boots share a private
          // subscription store keyed by the fixed boot session id.
          const stateHome = mkdtempSync(join(tmpdir(), 'commy-resume-'))
          const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
          const verifyTopic = `k7cv-resume-${suffix}`
          const marker = `resume-marker-${suffix}`
          const bootEnv = {
            CLAUDE_CODE_SESSION_ID: RESUME_SESSION,
            XDG_STATE_HOME: stateHome,
          }

          // BOOT 1 — post a marker into a NON-DEFAULT thread, then subscribe that
          // thread (persists the thread narrow under the session id), tear down.
          const messageId = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildHarness(bootEnv)
              yield* Effect.sleep(MINTER_PACE)
              const posted = expectStructured(
                yield* callTool(client, 'post', {
                  channel_name: e.channelName,
                  thread: verifyTopic,
                  body: marker,
                  session_id: RESUME_SESSION,
                }),
              )
              const id = posted['message_id'] as string
              expect(typeof id).toBe('string')
              yield* callTool(client, 'subscribe', {
                target: `${e.channelName}/${verifyTopic}`,
                session_id: RESUME_SESSION,
              })
              return id
            }),
          )

          // The persist half: boot-1's subscribe wrote the narrow set to disk
          // under the session id, including the non-default thread.
          const persistedPath = join(stateHome, 'commy', 'subscriptions', `${RESUME_SESSION}.json`)
          const persisted = JSON.parse(readFileSync(persistedPath, 'utf8')) as ReadonlyArray<{
            readonly kind: string
            readonly threadName?: string
          }>
          expect(persisted.some((i) => i.kind === 'thread' && i.threadName === verifyTopic)).toBe(
            true,
          )

          // BOOT 2 — same session id via boot env + same state home. A fresh MCP
          // child boots session-blind; the boot-env feeder fills the id and the
          // boot-forked restore rehydrates the narrow set with ZERO tool calls.
          // Capture host-facing `notifications/claude/channel` frames, then react
          // from the minter (a different identity than the resumed subject bot);
          // the reaction must be delivered off the restored subscription.
          const frames: Array<{ readonly meta: Record<string, string> }> = []
          const matchingFrame = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildHarness(bootEnv)
              client.fallbackNotificationHandler = (n: {
                readonly method: string
                readonly params?: unknown
              }): Promise<void> => {
                if (
                  n.method === 'notifications/claude/channel' &&
                  typeof n.params === 'object' &&
                  n.params !== null
                ) {
                  const p = n.params as { readonly meta?: Record<string, string> }
                  if (p.meta !== undefined) frames.push({ meta: p.meta })
                }
                return Promise.resolve()
              }

              // Let the boot-forked restore load the narrow set and wire the
              // thread on the substrate before the reaction is posted, so the
              // pump's narrow-set match is in place when the event flows.
              yield* Effect.sleep(Duration.seconds(5))

              yield* minterHttp
                .post(
                  `/messages/${messageId}/reactions`,
                  Schema.Struct({ result: Schema.Literal('success') }),
                  { emoji_name: 'thumbs_up' },
                )
                .pipe(Effect.orDie)

              // Poll the captured frames for the reaction — delivered with no
              // tool call on this boot. The outer timeout is the fail signal.
              return yield* Effect.suspend(() =>
                Option.match(
                  Option.fromNullable(
                    frames.find(
                      (f) =>
                        f.meta['reaction_action'] === 'add' &&
                        f.meta['reaction_emoji'] === 'thumbs_up' &&
                        f.meta['target_thread'] === verifyTopic,
                    ),
                  ),
                  {
                    onNone: () => Effect.fail('not-yet' as const),
                    onSome: (f) => Effect.succeed(f),
                  },
                ),
              ).pipe(
                // Retries the `not-yet` failure on a fixed cadence; the timeout
                // is the fail signal (a `TimeoutException`) if the reaction never
                // lands, meaning restore did not re-wire the subscription.
                Effect.retry(Schedule.spaced(Duration.millis(500))),
                Effect.timeout(Duration.seconds(30)),
              )
            }),
          )

          expect(matchingFrame.meta['reaction_action']).toBe('add')
          expect(matchingFrame.meta['reaction_emoji']).toBe('thumbs_up')
          expect(matchingFrame.meta['target_thread']).toBe(verifyTopic)
          expect(matchingFrame.meta['target_message_id']).toBe(messageId)

          rmSync(stateHome, { recursive: true, force: true })
        }),
      ),
    90_000,
  )

  test(
    'downtime queue-replay: a reaction posted while the seat is DEAD is replayed on resume with zero tool calls',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const e = liveEnv()
          const minterHttp = yield* makeZulipHttp({
            realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
            email: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
            apiKey: yield* ApiKey(e.minterApiKey).pipe(Effect.orDie),
          }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

          // Isolated on-disk state home shared by the two boots: the subscription
          // store AND the queue-state store key off the fixed boot session id.
          const stateHome = mkdtempSync(join(tmpdir(), 'commy-downtime-'))
          const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
          const verifyTopic = `downtime-resume-${suffix}`
          const marker = `downtime-marker-${suffix}`
          // BOOT 1 subscribes the thread via COMMY_SUBSCRIBE so the producer polls
          // an all-mode queue from its first poll (the pump captures the producer's
          // queue+mode at boot and only re-registers on BAD_EVENT_QUEUE_ID, never on
          // a live mode flip — so a post-boot subscribe would leave a second
          // all-mode queue persisted-but-unpolled). BOOT 2 carries NO COMMY_SUBSCRIBE:
          // a resumed ephemeral seat reboots with an empty narrow and rehydrates from
          // the subscription store (restore), NOT from the env seed. This ordering is
          // load-bearing — `subscribeFromEnv` runs before the pump, and its register
          // writes the queue-state store, so a boot-2 env seed would clobber the
          // persisted resume-state with a fresh empty queue before the producer ever
          // resume-polls the surviving one.
          const boot1Env = {
            CLAUDE_CODE_SESSION_ID: DOWNTIME_RESUME_SESSION,
            XDG_STATE_HOME: stateHome,
            COMMY_SUBSCRIBE: `${e.channelName}/${verifyTopic}`,
          }
          const boot2Env = {
            CLAUDE_CODE_SESSION_ID: DOWNTIME_RESUME_SESSION,
            XDG_STATE_HOME: stateHome,
          }

          // BOOT 1 — the all-mode events queue is registered at boot (idle timeout
          // long enough to survive downtime) and its state persisted under the
          // session id; the producer polls it, advancing `lastEventId`. Post a
          // marker into the subscribed thread, subscribe that thread as a tool call
          // (persists the narrow set to the store so boot-2's restore rehydrates it),
          // let the producer drain past the marker, then tear down. The adapter's
          // `close()` is a no-op and never DELETEs the queue, so the server keeps it
          // alive for the idle window with no seat polling it.
          const messageId = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildResumeHarness(boot1Env)
              yield* Effect.sleep(MINTER_PACE)
              const posted = expectStructured(
                yield* callTool(client, 'post', {
                  channel_name: e.channelName,
                  thread: verifyTopic,
                  body: marker,
                  session_id: DOWNTIME_RESUME_SESSION,
                }),
              )
              const id = posted['message_id'] as string
              expect(typeof id).toBe('string')
              // Persist the narrow set (the thread is already subscribed via
              // COMMY_SUBSCRIBE, so mode stays 'all' and no queue re-registers).
              yield* callTool(client, 'subscribe', {
                target: `${e.channelName}/${verifyTopic}`,
                session_id: DOWNTIME_RESUME_SESSION,
              })
              // Let the producer's long-poll return the marker and advance the
              // persisted `lastEventId` past it, so boot-2 resumes from a recent
              // cursor rather than replaying the whole queue.
              yield* Effect.sleep(Duration.seconds(3))
              return id
            }),
          )

          // The persist half: boot-1 wrote BOTH stores under the session id — the
          // narrow set (with the thread) for restore, and the queue-state
          // (`{queueId, lastEventId}`) so boot-2 resume-polls the surviving queue.
          const subsPath = join(
            stateHome,
            'commy',
            'subscriptions',
            `${DOWNTIME_RESUME_SESSION}.json`,
          )
          const subs = JSON.parse(readFileSync(subsPath, 'utf8')) as ReadonlyArray<{
            readonly kind: string
            readonly threadName?: string
          }>
          expect(subs.some((i) => i.kind === 'thread' && i.threadName === verifyTopic)).toBe(true)

          const queueStatePath = join(
            stateHome,
            'commy',
            'queue-state',
            `${DOWNTIME_RESUME_SESSION}.json`,
          )
          const queueState = JSON.parse(readFileSync(queueStatePath, 'utf8')) as {
            readonly queueId: string
            readonly lastEventId: number
          }
          expect(typeof queueState.queueId).toBe('string')
          expect(queueState.queueId.length).toBeGreaterThan(0)
          expect(Number.isInteger(queueState.lastEventId)).toBe(true)

          // DOWNTIME — no seat is running. React to the marker from the minter (a
          // different identity than the resumed subject bot). The reaction lands
          // in the surviving server-side queue while the child is dead; only a
          // resumed queue can carry it across the gap.
          yield* Effect.sleep(MINTER_PACE)
          yield* minterHttp
            .post(
              `/messages/${messageId}/reactions`,
              Schema.Struct({ result: Schema.Literal('success') }),
              { emoji_name: 'thumbs_up' },
            )
            .pipe(Effect.orDie)

          // BOOT 2 — same session id via boot env + same state home. A fresh MCP
          // child boots session-blind; the boot-env feeder fills the id, the
          // boot-forked restore rehydrates the narrow set, and the producer's
          // resume-poll recovers the persisted queue and replays the reaction that
          // landed during downtime — all with ZERO tool calls on this boot. The
          // reaction is ALREADY queued before boot, so (unlike the live-pump resume
          // test above) we do not post it here; we only capture the frame it
          // replays into.
          const frames: Array<{ readonly meta: Record<string, string> }> = []
          const matchingFrame = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.sleep(MINTER_PACE)
              const client = yield* buildResumeHarness(boot2Env)
              client.fallbackNotificationHandler = (n: {
                readonly method: string
                readonly params?: unknown
              }): Promise<void> => {
                if (
                  n.method === 'notifications/claude/channel' &&
                  typeof n.params === 'object' &&
                  n.params !== null
                ) {
                  const p = n.params as { readonly meta?: Record<string, string> }
                  if (p.meta !== undefined) frames.push({ meta: p.meta })
                }
                return Promise.resolve()
              }

              // Poll the captured frames for the downtime reaction, replayed off
              // the resumed queue with no tool call on this boot. The outer timeout
              // is the fail signal: if the reaction never lands, the queue did not
              // survive/resume, or restore did not re-wire the narrow before the
              // replay flowed.
              return yield* Effect.suspend(() =>
                Option.match(
                  Option.fromNullable(
                    frames.find(
                      (f) =>
                        f.meta['reaction_action'] === 'add' &&
                        f.meta['reaction_emoji'] === 'thumbs_up' &&
                        f.meta['target_thread'] === verifyTopic,
                    ),
                  ),
                  {
                    onNone: () => Effect.fail('not-yet' as const),
                    onSome: (f) => Effect.succeed(f),
                  },
                ),
              ).pipe(
                Effect.retry(Schedule.spaced(Duration.millis(500))),
                Effect.timeout(Duration.seconds(45)),
              )
            }),
          )

          expect(matchingFrame.meta['reaction_action']).toBe('add')
          expect(matchingFrame.meta['reaction_emoji']).toBe('thumbs_up')
          expect(matchingFrame.meta['target_thread']).toBe(verifyTopic)
          expect(matchingFrame.meta['target_message_id']).toBe(messageId)

          rmSync(stateHome, { recursive: true, force: true })
        }),
      ),
    120_000,
  )

  // Live delivery fidelity — the property message 19803 violated.
  //
  // 19803 was a LIVE steady-state event in a correctly-subscribed topic,
  // bracketed by a delivery before it and a delivery after it. Every hypothesis
  // that would have explained it is disproved (resume, phantom subscribe, wake,
  // ts-collision dedup, acquire handover), so nothing establishes what provokes
  // the drop — which means a single run seeing no drop measures nothing at all.
  //
  // Sized instead to make a NULL result informative: N events bound the
  // per-event drop rate under 3/N at 95% (rule of three). 19803 was one drop
  // among the couple of dozen events its seat handled — order 5% — so a null at
  // N=100 puts the rate under 3% and discriminates a random per-event race from
  // a state-dependent trigger. A null at N=1 discriminates nothing, which is
  // why the count is explicit rather than implied.
  //
  // EXERCISED CONDITIONS, written down so a null is read for what it covers and
  // no more: runtime subscribe (not a `COMMY_SUBSCRIBE` seed), identity acquire
  // via a first post, then live steady-state delivery — the incident's own
  // sequence. Resume, downtime replay, queue re-register and mode flip are
  // UNTESTED BY THIS RUN, not cleared by it.
  //
  // The count comes from the env because the realm is the substrate the fleet
  // coordinates on: the committed default is cheap, and the diagnostic run
  // raises it deliberately.
  test(
    'delivery fidelity: every live message posted into a subscribed thread reaches the seat',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const e = liveEnv()
          const minterHttp = yield* makeZulipHttp({
            realmUrl: yield* RealmUrl(e.site).pipe(Effect.orDie),
            email: yield* BotEmail(e.minterEmail).pipe(Effect.orDie),
            apiKey: yield* ApiKey(e.minterApiKey).pipe(Effect.orDie),
          }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient))

          const stateHome = mkdtempSync(join(tmpdir(), 'commy-fidelity-'))
          const suffix = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
          const verifyTopic = `fidelity-${suffix}`

          const frames: Array<{ readonly meta: Record<string, string> }> = []

          const report = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* Effect.sleep(MINTER_PACE)
              // No `COMMY_SUBSCRIBE`: the seat that dropped 19803 booted with an
              // empty seed (measured on its still-live child) and subscribed at
              // runtime, so the seed path would not be the same experiment.
              const client = yield* buildHarness({
                CLAUDE_CODE_SESSION_ID: FIDELITY_SESSION,
                XDG_STATE_HOME: stateHome,
                COMMY_SUBSCRIBE: '',
              })
              client.fallbackNotificationHandler = (n: {
                readonly method: string
                readonly params?: unknown
              }): Promise<void> => {
                if (
                  n.method === 'notifications/claude/channel' &&
                  typeof n.params === 'object' &&
                  n.params !== null
                ) {
                  const p = n.params as { readonly meta?: Record<string, string> }
                  if (p.meta !== undefined) frames.push({ meta: p.meta })
                }
                return Promise.resolve()
              }

              yield* Effect.sleep(MINTER_PACE)
              yield* callTool(client, 'subscribe', {
                target: `thread:${e.channelName}/${verifyTopic}`,
                session_id: FIDELITY_SESSION,
              })
              // The first post is what forces identity acquire and the
              // acquire-triggered catch-up — the state 19803 landed 62s after.
              yield* callTool(client, 'post', {
                channel_name: e.channelName,
                thread: verifyTopic,
                body: `fidelity-anchor-${suffix}`,
                session_id: FIDELITY_SESSION,
              })

              const costAfterBoot = yield* Ref.get(realmObservations)

              const trials = yield* Effect.forEach(Arr.range(1, SOAK_EVENTS), (n) =>
                Effect.gen(function* () {
                  const before = yield* Ref.get(realmObservations)
                  yield* Effect.sleep(MINTER_PACE)
                  const publish = minterHttp
                    .post('/messages', postedMessageSchema, {
                      type: 'stream',
                      to: e.channelName,
                      topic: verifyTopic,
                      content: `fidelity-${suffix}-${n}`,
                    })
                    .pipe(Effect.orDie)
                  // Alternating arms, interleaved in ONE boot rather than run as
                  // two soaks: same seat, same queue, same realm conditions, so
                  // a difference between them is the arm and not the run. Half
                  // the realm cost of two runs, and a paired comparison.
                  //
                  const posted = yield* publish
                  const id = String(posted.id)
                  const arrived = yield* Effect.suspend(() =>
                    frames.some((f) => f.meta['message_id'] === id)
                      ? Effect.void
                      : Effect.fail('not-yet' as const),
                  ).pipe(
                    Effect.retry(Schedule.spaced(Duration.millis(500))),
                    Effect.timeout(SOAK_EVENT_TIMEOUT),
                    Effect.match({ onFailure: () => false, onSuccess: () => true }),
                  )
                  const after = yield* Ref.get(realmObservations)
                  // A 429 anywhere in this trial's window means the realm, not
                  // the pump, decided when the event arrived. Void, not a miss.
                  const rateLimited = after.rateLimited > before.rateLimited
                  const outcome = rateLimited
                    ? ('void' as const)
                    : arrived
                      ? ('hit' as const)
                      : ('miss' as const)
                  // Logged per trial, not only in the final summary: a miss costs
                  // the full timeout, so a soak sized slightly too tight dies
                  // mid-loop and a summary-only fixture would report nothing at
                  // all about the trials it did complete. A run that can only
                  // speak when it finishes cannot report the runs that don't.
                  yield* Effect.logInfo(
                    `commy fidelity trial ${n}/${SOAK_EVENTS}: ${outcome} id=${id} calls=${after.requests - before.requests}`,
                  )
                  return { n, id, outcome, calls: after.requests - before.requests }
                }),
              )

              return { costAfterBoot, trials }
            }),
          )

          const misses = report.trials.filter((t) => t.outcome === 'miss')
          const voids = report.trials.filter((t) => t.outcome === 'void')
          const counted = report.trials.length - voids.length
          const callsPerTrial =
            report.trials.reduce((sum, t) => sum + t.calls, 0) / Math.max(report.trials.length, 1)

          // The realm-cost figures are the point of a pilot run, so they are
          // reported on success too — a soak that only speaks when it fails
          // cannot tell you what the next N would cost.
          yield* Effect.logInfo(
            `commy fidelity soak: ${counted} counted (${voids.length} void, ${misses.length} miss) — ` +
              `boot cost ${report.costAfterBoot.requests} calls, ~${callsPerTrial.toFixed(1)} calls/event, ` +
              `misses=[${misses.map((t) => t.id).join(',')}]`,
          )

          expect(misses.map((t) => t.id)).toEqual([])
          // A run drowned in 429s bounds nothing; fail rather than report a
          // vacuous null as a clean result.
          expect(counted).toBeGreaterThan(0)

          rmSync(stateHome, { recursive: true, force: true })
        }),
      ),
    // A miss costs the full per-event timeout, so the worst case is every trial
    // waiting it out — the budget has to cover that, plus a live boot and the
    // paced teardown. Sized from an observed run rather than estimated: the
    // first pilot died on its own deadline with the loop still going.
    120_000 + SOAK_EVENTS * 35_000,
  )
})
