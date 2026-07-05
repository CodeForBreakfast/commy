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
import { stderrLoggerLayer } from '@commy/core/logging'
import type { ZulipAdapter } from '@commy/zulip/adapter'
import { zulipAdapter } from '@commy/zulip/adapter'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl } from '@commy/zulip/http'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Redacted,
  Schema,
  type Scope,
  Stream,
} from 'effect'
import { parseEnv, substrateAdapterLayer } from './bootstrap.ts'
import { FileCursorStoreLive } from './cursor-store.ts'
import { makeProgram } from './server.ts'
import { SessionIdLive } from './session-id.ts'
import { FileSubscriptionStoreLive } from './subscription-store.ts'
import { testPlatformLayer } from './test-platform.ts'

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

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

// Boot the plugin program against the live realm and connect an MCP
// client, all under the caller's Scope. Mirrors tools.test.ts's
// `mountAndConnect`: the program is forked under the scope and the
// killSwitch + client.close are scope finalizers. Returns the connected
// client. The MCP SDK is Promise-shaped, so `Effect.promise` bridges its
// connect/close at the host boundary — the only place a Promise is owed.
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

    // Wrap `inbox.events()` so the harness can stop the event pump cleanly
    // on scope close. The deferred fires from the finalizer to interrupt
    // the Stream — without it, the program's `await pump.done` never
    // resolves and the test leaks the long-poll.
    const killSwitch = yield* Deferred.make<void>()
    const wrappedAdapter: ZulipAdapter = {
      ...realAdapter,
      inbox: {
        ...realAdapter.inbox,
        events: () => realAdapter.inbox.events().pipe(Stream.interruptWhenDeferred(killSwitch)),
      },
    }

    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'commy-live-test', version: '0.0.0' }, { capabilities: {} })

    const mainEnv: Record<string, string | undefined> = {
      ZULIP_SITE: e.site,
      ZULIP_MINTER_EMAIL: e.minterEmail,
      ZULIP_MINTER_API_KEY: e.minterApiKey,
      ...envOverrides,
    }

    // The boot program: the real adapter (parse-gated like
    // production's `ZulipAdapterLive`, so `close()` is a layer-scope
    // finalizer) and the file-backed cursor store arrive through the app
    // Layer; the env through an outermost ConfigProvider. Forked under the
    // scope; `killSwitch` interrupts the pump's events stream on cleanup so
    // the scope unwinds (pump-cancel → release → close).
    const program = makeProgram({ transport: serverTransport }).pipe(
      Effect.provide(
        Layer.provideMerge(
          Layer.mergeAll(
            substrateAdapterLayer(parseEnv.pipe(Effect.as(wrappedAdapter))),
            FileCursorStoreLive,
            // Feed the one shared session-id deferred into the store, which now
            // awaits it — mergeAll won't wire a sibling's output to a sibling's
            // input, so a plain merge leaves the store's SessionId unsatisfied.
            Layer.provideMerge(FileSubscriptionStoreLive, SessionIdLive),
            stderrLoggerLayer,
          ),
          testPlatformLayer(mainEnv),
        ),
      ),
    )
    const fiber = yield* Effect.forkScoped(program)

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        // Pace before the scope unwinds: the release finalizer issues a
        // minter-authenticated DELETE, so leave the same gap the substrate
        // live suite keeps between minter calls.
        yield* Effect.sleep(MINTER_PACE)
        yield* Deferred.succeed(killSwitch, undefined)
        // Await the program: the scope unwind runs pump-cancel → release →
        // close. A boot/teardown failure surfaces in the fiber's Exit but
        // the assertions in the test body remain the sole pass/fail signal,
        // so join here and ignore the outcome.
        yield* Fiber.join(fiber).pipe(Effect.ignore)
        yield* Effect.promise(() => client.close())
      }),
    )

    yield* Effect.promise(() => client.connect(clientTransport))
    return client
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
                COMMY_SUBSCRIBE: `channel:${e.channelName}`,
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
                COMMY_SUBSCRIBE: `channel:${e.channelName}`,
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
})
