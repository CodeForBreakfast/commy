/**
 * Live multi-adapter integration tests against a real Zulip realm.
 *
 * Two layers:
 *
 * 1. **Identity acquire/release** — mint, regenerate, deactivate via the
 *    real /api/v1/bots endpoints. Verifies the minter-driven flow
 *    against the live realm at zulip.example.com.
 *
 * 2. **Multi-adapter event delivery** — a shared persistent sender adapter
 *    plus a fresh observer adapter per test exercise post / subscribe /
 *    events / replay / react-unreact across them. Catches drift between the
 *    in-tree realm fixture (`test-realm.ts`) and actual Zulip semantics.
 *
 * **Local-only** — env-gated, never runs in CI. With env vars unset the
 * suite skips silently so default `bun test` stays green.
 *
 * Required env vars (all must be present, else the suite is skipped):
 * - `ZULIP_SITE`              e.g. `https://zulip.example.com`
 * - `ZULIP_MINTER_EMAIL`      minter user email
 * - `ZULIP_MINTER_API_KEY`    minter user API key
 * - `ZULIP_LIVE_CHANNEL_NAME` stream both bots subscribe to
 * - `ZULIP_LIVE_CHANNEL_ID`   stream id (numeric string)
 */

import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import type {
  ChannelRef,
  Credentials,
  Identity,
  InboundEvent,
  ReleaseOpts,
} from '@commy/core/ports'
import {
  type BotName,
  type DisplayName,
  decodeBotNameSync,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeMessageBodySync,
} from '@commy/core/ports'
import { FetchHttpClient, HttpClient } from '@effect/platform'
import {
  Context,
  Duration,
  Effect,
  Layer,
  ManagedRuntime,
  Option,
  Queue,
  Redacted,
  Schema,
  type Scope,
  Stream,
} from 'effect'
import type { ZulipAdapter } from './adapter.ts'
import { zulipAdapter } from './adapter.ts'
import { ApiKey, BotEmail, makeZulipHttp, RealmUrl, type ZulipHttp } from './http.ts'

interface LiveEnv {
  readonly site: string
  readonly minterEmail: string
  readonly minterApiKey: string
  readonly channelName: string
  readonly channelId: string
}

const readEnv = (): LiveEnv | undefined => {
  const required = {
    site: process.env['ZULIP_SITE'],
    minterEmail: process.env['ZULIP_MINTER_EMAIL'],
    minterApiKey: process.env['ZULIP_MINTER_API_KEY'],
    channelName: process.env['ZULIP_LIVE_CHANNEL_NAME'],
    channelId: process.env['ZULIP_LIVE_CHANNEL_ID'],
  }
  for (const value of Object.values(required)) {
    if (value === undefined || value.length === 0) return undefined
  }
  return required as Record<keyof typeof required, string>
}

const env = readEnv()
const describeLive = env === undefined ? describe.skip : describe

/** Narrow `env` inside test callbacks where describe.skip has already guarded. */
const liveEnv = (): LiveEnv => {
  if (env === undefined) throw new Error('unreachable — describe.skip should have run')
  return env
}

/** Extract email + apiKey from an acquired identity's credentials record. */
const credentialsOf = (creds: Credentials): { email: string; apiKey: string } => {
  const email = creds['email']
  const apiKey = creds['apiKey']
  if (email === undefined || apiKey === undefined) {
    throw new Error('acquired credentials missing email or apiKey')
  }
  return { email, apiKey }
}

const isMessagePosted = (e: InboundEvent): e is Extract<InboundEvent, { kind: 'message-posted' }> =>
  e.kind === 'message-posted'

const isMentionReceived = (
  e: InboundEvent,
): e is Extract<InboundEvent, { kind: 'mention-received' }> => e.kind === 'mention-received'

const isReactionAdded = (e: InboundEvent): e is Extract<InboundEvent, { kind: 'reaction-added' }> =>
  e.kind === 'reaction-added'

const isReactionRemoved = (
  e: InboundEvent,
): e is Extract<InboundEvent, { kind: 'reaction-removed' }> => e.kind === 'reaction-removed'

interface DrainResult {
  readonly events: ReadonlyArray<InboundEvent>
  readonly timedOut: boolean
}

// Mirror inbox.events() into an unbounded Queue under the caller's Scope.
// The forked Stream.runDrain fiber lives for the scope's lifetime, so
// scope close interrupts it — which aborts any in-flight long-poll via the
// HttpClient's AbortSignal. Tests pull events with `Queue.take`.
const eventQueue = (
  adapter: ZulipAdapter,
): Effect.Effect<Queue.Queue<InboundEvent>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<InboundEvent>()
    yield* Effect.forkScoped(
      adapter.inbox.events().pipe(
        Stream.tap((event) => Queue.offer(queue, event)),
        Stream.runDrain,
      ),
    )
    return queue
  })

// Take events from the queue, accumulating until `predicate` matches one or
// the overall `deadline` elapses. The Effect-native analogue of the old
// AsyncIterator drain loop: per-take `Effect.timeoutOption` bounds each pull,
// and the running total of remaining time bounds the whole drain.
const drainUntil = (
  queue: Queue.Queue<InboundEvent>,
  predicate: (e: InboundEvent) => boolean,
  deadline: Duration.Duration,
): Effect.Effect<DrainResult> =>
  Effect.gen(function* () {
    const deadlineMs = Duration.toMillis(deadline)
    const start = yield* Effect.sync(() => Date.now())
    const collected: InboundEvent[] = []
    while (Date.now() - start < deadlineMs) {
      const remaining = deadlineMs - (Date.now() - start)
      const taken = yield* Queue.take(queue).pipe(Effect.timeoutOption(Duration.millis(remaining)))
      if (Option.isNone(taken)) return { events: collected, timedOut: true }
      collected.push(taken.value)
      if (predicate(taken.value)) return { events: collected, timedOut: false }
    }
    return { events: collected, timedOut: true }
  })

const httpClient = Effect.runSync(HttpClient.HttpClient.pipe(Effect.provide(FetchHttpClient.layer)))

const NAME_PREFIX = 'cc-live-test'

const uniqueName = (role: string): DisplayName =>
  decodeDisplayNameSync(
    `${NAME_PREFIX}-${role}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  )

const buildAdapter = (): Effect.Effect<ZulipAdapter> =>
  Effect.gen(function* () {
    const e = liveEnv()
    return yield* zulipAdapter({
      realmUrl: yield* RealmUrl(e.site),
      minterEmail: yield* BotEmail(e.minterEmail),
      minterApiKey: Redacted.make(yield* ApiKey(e.minterApiKey)),
    })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

// Spacing between minter-authenticated calls. The live suite mints,
// regenerates and deactivates 30+ bots per run; bursting them through
// the shared minter trips Zulip's per-user rate limit on the homelab
// realm (zulip.example.com), which knocks live concierges off MCP
// for the duration of the cool-off. Sleep before every acquire/release
// keeps the suite under the limit and leaves headroom for interactive
// sessions sharing the realm. (comms-jfd)
const MINTER_PACE = Duration.millis(900)

const pacedAcquire = (
  adapter: ZulipAdapter,
  name: BotName,
): ReturnType<ZulipAdapter['identity']['acquire']> =>
  Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.acquire(name)))

const pacedRelease = (
  adapter: ZulipAdapter,
  opts?: ReleaseOpts,
): ReturnType<ZulipAdapter['identity']['release']> =>
  Effect.sleep(MINTER_PACE).pipe(Effect.zipRight(adapter.identity.release(opts)))

const botHttp = (e: LiveEnv, creds: { email: string; apiKey: string }): Effect.Effect<ZulipHttp> =>
  Effect.gen(function* () {
    return yield* makeZulipHttp({
      realmUrl: yield* RealmUrl(e.site),
      email: yield* BotEmail(creds.email),
      apiKey: yield* ApiKey(creds.apiKey),
    })
  }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.orDie)

const minterHttp = (e: LiveEnv): Effect.Effect<ZulipHttp> =>
  botHttp(e, { email: e.minterEmail, apiKey: e.minterApiKey })

const usersMeSchema = Schema.Struct({
  result: Schema.Literal('success'),
  user_id: Schema.Int,
  full_name: Schema.String,
  is_bot: Schema.Boolean,
  is_active: Schema.Boolean,
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

describeLive('zulip live identity acquire/release — zulip.example.com', () => {
  test(
    'first-time acquire mints a fresh bot reachable via /users/me with the new key',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const adapter = yield* buildAdapter()
          const name = uniqueName('mint')
          yield* Effect.acquireUseRelease(
            pacedAcquire(adapter, decodeBotNameSync(name)),
            (acquired) =>
              Effect.gen(function* () {
                expect(acquired.identity.name).toEqual(name)
                expect(acquired.identity.kind).toBe('agent')
                // The credentials are the minted bot's, not the minter's. Hit
                // /users/me with them — the response identifies the bot.
                const creds = credentialsOf(acquired.credentials)
                const http = yield* botHttp(liveEnv(), creds)
                const me = yield* http.get('/users/me', usersMeSchema)
                expect(me.full_name).toEqual(name)
                expect(me.is_bot).toBe(true)
                expect(me.is_active).toBe(true)
                expect(String(me.user_id)).toEqual(acquired.identity.id)
              }),
            () => pacedRelease(adapter),
          )
        }),
      ),
    30_000,
  )

  test(
    'repeat acquire on the same name regenerates the API key (new key authenticates, old key may also still work)',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const name = uniqueName('regen')
          const first = yield* buildAdapter()
          yield* Effect.acquireUseRelease(
            pacedAcquire(first, decodeBotNameSync(name)),
            (firstAcq) =>
              Effect.gen(function* () {
                const secondAdapter = yield* buildAdapter()
                yield* Effect.acquireUseRelease(
                  pacedAcquire(secondAdapter, decodeBotNameSync(name)),
                  (secondAcq) =>
                    Effect.gen(function* () {
                      expect(secondAcq.identity.id).toEqual(firstAcq.identity.id)
                      // The two acquires return distinct credentials objects — the
                      // second is the regenerated key. Verify the regenerated key
                      // authenticates as the same bot.
                      const secondCreds = credentialsOf(secondAcq.credentials)
                      const newHttp = yield* botHttp(liveEnv(), secondCreds)
                      const me = yield* newHttp.get('/users/me', usersMeSchema)
                      expect(String(me.user_id)).toEqual(firstAcq.identity.id)
                    }),
                  () => pacedRelease(secondAdapter),
                )
              }),
            // First adapter's credentials were superseded by the regenerate;
            // release attempts a deactivate via the minter http (which still
            // works) and clears the in-memory binding.
            () => pacedRelease(first),
          )
        }),
      ),
    30_000,
  )

  test(
    'release deactivates the bot — /users no longer lists it as active',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const adapter = yield* buildAdapter()
          const name = uniqueName('deact')
          const acquired = yield* pacedAcquire(adapter, decodeBotNameSync(name))
          yield* pacedRelease(adapter)

          // Re-list /users via a fresh minter http; the deactivated bot
          // either disappears from the active set or is flagged is_active=false.
          const http = yield* minterHttp(liveEnv())
          const users = yield* http.get('/users', usersListSchema)
          const found = users.members.find((u) => String(u.user_id) === acquired.identity.id)
          if (found !== undefined) {
            expect(found.is_active).toBe(false)
          }
        }),
      ),
    30_000,
  )

  test(
    'acquire → release → acquire same name reactivates the SAME bot (deactivated email stays reserved)',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const name = uniqueName('cycle')
          const first = yield* buildAdapter()
          const firstAcq = yield* pacedAcquire(first, decodeBotNameSync(name))
          const firstId = firstAcq.identity.id
          yield* pacedRelease(first)

          const second = yield* buildAdapter()
          yield* Effect.acquireUseRelease(
            pacedAcquire(second, decodeBotNameSync(name)),
            (acquired) =>
              Effect.gen(function* () {
                expect(acquired.identity.name).toEqual(name)
                // Reactivation preserves the user_id — we must not have minted
                // a fresh bot (Zulip would reject mint with the deactivated
                // bot's email still reserved).
                expect(acquired.identity.id).toEqual(firstId)
                const acquiredCreds = credentialsOf(acquired.credentials)
                const http = yield* botHttp(liveEnv(), acquiredCreds)
                const me = yield* http.get('/users/me', usersMeSchema)
                expect(me.full_name).toEqual(name)
                expect(me.is_active).toBe(true)
              }),
            () => pacedRelease(second),
          )
        }),
      ),
    30_000,
  )

  test(
    'acquire of an UnknownIdentity-like name in our substrate still succeeds — Zulip can always mint',
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          // Sanity: there's no `unacquirableName` for Zulip — the minter can
          // always mint. Captured to make the substrate's capability explicit
          // (Discord will not pass this).
          const adapter = yield* buildAdapter()
          const name = uniqueName('any')
          yield* Effect.acquireUseRelease(
            pacedAcquire(adapter, decodeBotNameSync(name)),
            (acquired) => Effect.sync(() => expect(acquired.identity.name).toEqual(name)),
            () => pacedRelease(adapter),
          )
        }),
      ),
    30_000,
  )
})

describeLive('zulip live multi-adapter integration — zulip.example.com', () => {
  // DRAIN_DEADLINE is how long a single drain waits for its event before
  // declaring it lost. It must exceed the homelab realm's worst event-delivery
  // latency under load — NOT request-layer rate limiting (the client absorbs
  // 429s and none were observed), but Zulip's long-poll/event-propagation
  // latency, which was measured spiking to ~20s when the live suite is run
  // back-to-back. 15s sat right on that edge and produced false timeouts; 30s
  // clears it with margin. The deadline still fails honestly if an event never
  // arrives — it just waits longer.
  const DRAIN_DEADLINE = Duration.seconds(30)
  // The reaction-added/removed test runs three SEQUENTIAL drains (message-posted,
  // reaction-added, reaction-removed). The per-test wall is DERIVED from the
  // drains rather than guessed — wall ≥ Σ(drain deadlines) + setup headroom — so
  // three correct-but-slow round-trips can never blow it. A flat 30s wall under
  // three 15s drains (45 > 30) was the RC1 flake (comms-ifo.1).
  const MAX_SEQUENTIAL_DRAINS = 3
  const SETUP_HEADROOM = Duration.seconds(15)
  const EVENTS_TEST_TIMEOUT_MS = Duration.toMillis(
    Duration.sum(Duration.times(DRAIN_DEADLINE, MAX_SEQUENTIAL_DRAINS), SETUP_HEADROOM),
  )

  // Bot strategy (comms-hcw). The old harness minted 2 fresh bots per test
  // (2 × 7 = 14 mint + 14 deactivate cycles through the single shared minter);
  // that rolling-window rate-limit pressure inflated event-delivery latency on
  // the long-poll and surfaced as the reaction-test flake (RC2). Two roles,
  // two lifetimes:
  //
  // - SENDER: one bound adapter built once and reused across the whole block.
  //   It only posts / reacts / edits / reads history — no event-queue state —
  //   so reuse is safe. Acquired once, released { persistent: true } at block
  //   teardown.
  // - OBSERVER: a freshly built adapter PER TEST. The Zulip event queue is
  //   adapter-scoped and cached in inboxRef (registered once, reused), and its
  //   lastEventId is never written back — so a reused observer re-polls a stale
  //   queue that nobody kept warm between tests and silently stops delivering.
  //   A new adapter instance per test gives a fresh /register aligned to that
  //   test's post. The underlying bot is persistent (reactivate+regenerate on
  //   acquire, never deactivated), so a fresh observer still costs no mint.
  //
  // Net: 14 mint + 14 deactivate → 1 sender acquire + one observer acquire per
  // test, zero deactivates.
  interface Bots {
    readonly sender: ZulipAdapter
    readonly observer: ZulipAdapter
    readonly channel: ChannelRef
  }

  const SENDER_NAME = decodeBotNameSync('cc-live-sender')
  const OBSERVER_NAME = decodeBotNameSync('cc-live-observer')

  const channelRef = (): ChannelRef => {
    const e = liveEnv()
    return {
      id: decodeChannelIdSync(e.channelId),
      name: decodeChannelNameSync(e.channelName),
    }
  }

  // Subscribe the bots to the channel up-front. The SENDER subscribe is what
  // subscribes the realm's minter to the channel (inbox.subscribe is minter-side)
  // and gives events()/replay their /messages visibility before any test posts.
  //
  // The OBSERVER subscribe matters for a subtler reason: a `mentions`-only
  // subscription registers the events queue with an `is:mentioned` narrow keyed
  // to the QUEUE OWNER (the minter), but mention-received is synthesised when the
  // BOUND observer appears in a message's mentions — so an observer-only mention
  // is excluded by that narrow. Only a channel subscription puts the queue in
  // 'all' mode (no narrow), where the observer's mention surfaces. So the mention
  // test needs the observer channel-subscribed; subscribing it here gives every
  // test mode 'all'.
  //
  // The cache-miss test is the exception (includeObserver=false): it must
  // subscribe its observer AFTER its post so message-posted never enters the
  // observer's queue and the MessageRef cache stays empty, genuinely exercising
  // the /messages?anchor= fallback. It subscribes the observer to the channel in
  // its own body (still mode 'all', just registered after the post).
  const subscribeUpFront = (
    sender: ZulipAdapter,
    observer: ZulipAdapter,
    channel: ChannelRef,
    includeObserver: boolean,
  ) =>
    sender.inbox
      .subscribe(channel)
      .pipe(Effect.zipRight(includeObserver ? observer.inbox.subscribe(channel) : Effect.void))

  // release({ persistent: true }) clears the binding without deactivating the
  // bot, so the account survives for the next run (the comms-ch7 precedent).
  // close() unwinds the adapter's event pump (Promise-shaped, host-bridged).
  const releasePersistent = (adapter: ZulipAdapter): Effect.Effect<void> =>
    pacedRelease(adapter, { persistent: true }).pipe(
      Effect.zipRight(Effect.promise(() => adapter.close())),
    )

  // The shared sender, provided as a scoped service: built+acquired once when
  // the layer is first built, released { persistent } when the runtime is
  // disposed (afterAll). ManagedRuntime memoises the layer across every test.
  const Sender = Context.GenericTag<ZulipAdapter>('live/Sender')

  const senderLayer = Layer.scoped(
    Sender,
    Effect.gen(function* () {
      const adapter = yield* buildAdapter()
      yield* Effect.acquireRelease(pacedAcquire(adapter, SENDER_NAME), () =>
        releasePersistent(adapter),
      )
      return adapter
    }).pipe(Effect.orDie),
  )

  const liveRuntime = ManagedRuntime.make(senderLayer)

  // Build+acquire the sender outside any test's wall so the first event test
  // doesn't pay it (and an acquire failure surfaces here, not mid-test).
  beforeAll(() => liveRuntime.runPromise(Sender.pipe(Effect.asVoid)), EVENTS_TEST_TIMEOUT_MS)

  afterAll(() => liveRuntime.dispose())

  // The shared sender plus a fresh observer adapter bound for the duration of
  // the test scope. The fresh observer is what guarantees reliable event
  // delivery (fresh /register) AND, for the cache-miss test, an empty
  // MessageRef cache / None gap-replay watermark — so /messages?anchor= is
  // genuinely exercised rather than silently satisfied from a reused cache.
  const withBots = <A, E, R>(
    use: (bots: Bots) => Effect.Effect<A, E, R>,
    opts?: { readonly subscribeObserverUpFront?: boolean },
  ): Effect.Effect<A, E, R | ZulipAdapter> =>
    Effect.scoped(
      Effect.gen(function* () {
        const sender = yield* Sender
        const observer = yield* buildAdapter()
        yield* Effect.acquireRelease(pacedAcquire(observer, OBSERVER_NAME), () =>
          releasePersistent(observer),
        )
        const channel = channelRef()
        yield* subscribeUpFront(sender, observer, channel, opts?.subscribeObserverUpFront ?? true)
        return { sender, observer, channel } satisfies Bots
      }).pipe(Effect.orDie, Effect.flatMap(use)),
    )

  test(
    'observer.inbox.events() yields message-posted for sender.publisher.post',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, observer, channel }) =>
          Effect.scoped(
            Effect.gen(function* () {
              const marker = `live-msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
              yield* observer.inbox.subscribe(channel)
              yield* sender.publisher.post(channel, decodeMessageBodySync(marker))
              const queue = yield* eventQueue(observer)
              const result = yield* drainUntil(
                queue,
                (e) => isMessagePosted(e) && e.message.body.includes(marker),
                DRAIN_DEADLINE,
              )
              expect(result.timedOut).toBe(false)
              const match = result.events.find(
                (e): e is Extract<InboundEvent, { kind: 'message-posted' }> =>
                  isMessagePosted(e) && e.message.body.includes(marker),
              )
              expect(match).toBeDefined()
              expect(match?.message.ref.channel.id).toEqual(channel.id)
              const senderId = (yield* sender.identity.currentIdentity()).id
              expect(match?.message.sender.id).toEqual(senderId)
            }),
          ),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'observer.inbox.events() yields mention-received when sender mentions observer',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, observer, channel }) =>
          Effect.scoped(
            Effect.gen(function* () {
              const marker = `live-mention-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
              const observerSelf: Identity = yield* observer.identity.currentIdentity()
              yield* observer.inbox.subscribe('mentions')
              yield* sender.publisher.post(
                channel,
                decodeMessageBodySync(`@**${observerSelf.name}** ${marker}`),
                { mentions: [observerSelf] },
              )
              const queue = yield* eventQueue(observer)
              const result = yield* drainUntil(
                queue,
                (e) => isMentionReceived(e) && e.message.body.includes(marker),
                DRAIN_DEADLINE,
              )
              expect(result.timedOut).toBe(false)
              const match = result.events.find(
                (e): e is Extract<InboundEvent, { kind: 'mention-received' }> =>
                  isMentionReceived(e) && e.message.body.includes(marker),
              )
              expect(match).toBeDefined()
              expect(match?.mentions.map((m) => m.id)).toContain(observerSelf.id)
            }),
          ),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'observer.inbox.replay(since) backfills sender.publisher.post',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, observer, channel }) =>
          Effect.gen(function* () {
            const marker = `live-replay-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
            yield* sender.publisher.post(channel, decodeMessageBodySync(marker))
            yield* Effect.sleep(Duration.millis(500))
            const history = yield* sender.history.readChannel(channel, {})
            const ours = history.find((m) => m.body.includes(marker))
            if (ours === undefined) throw new Error('marker message not found in channel history')
            const events = yield* observer.inbox.replay(ours.ts)
            const match = events.find(
              (e): e is Extract<InboundEvent, { kind: 'message-posted' }> =>
                isMessagePosted(e) && e.message.body.includes(marker),
            )
            expect(match).toBeDefined()
          }),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'sender.publisher.edit replaces the message body on the live realm',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, channel }) =>
          Effect.gen(function* () {
            const marker = `live-edit-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
            const ref = yield* sender.publisher.post(
              channel,
              decodeMessageBodySync(`${marker} original`),
            )
            yield* sender.publisher.edit(ref, decodeMessageBodySync(`${marker} replaced`))
            yield* Effect.sleep(Duration.millis(500))
            const history = yield* sender.history.readChannel(channel, {})
            const found = history.find((m) => String(m.ref.id) === String(ref.id))
            if (found === undefined) throw new Error('edited message not found in channel history')
            expect(found.body).toBe(decodeMessageBodySync(`${marker} replaced`))
          }),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'sender.publisher.react / unreact round-trip without throwing',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, channel }) =>
          Effect.gen(function* () {
            const marker = `live-react-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
            const ref = yield* sender.publisher.post(channel, decodeMessageBodySync(marker))
            yield* sender.publisher.react(ref, decodeEmojiSync('thumbs_up'))
            yield* sender.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
          }),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'observer.inbox.events() yields reaction-added / reaction-removed for sender.publisher.react / unreact',
    () =>
      liveRuntime.runPromise(
        withBots(({ sender, observer, channel }) =>
          Effect.scoped(
            Effect.gen(function* () {
              const marker = `live-rxn-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
              yield* observer.inbox.subscribe(channel)
              const senderId = (yield* sender.identity.currentIdentity()).id
              const ref = yield* sender.publisher.post(channel, decodeMessageBodySync(marker))
              const queue = yield* eventQueue(observer)
              // Drain message-posted so the observer's MessageRef cache picks
              // up the target — without it the reaction events that follow
              // have no resolvable target and get dropped.
              const posted = yield* drainUntil(
                queue,
                (e) => e.kind === 'message-posted' && e.message.body.includes(marker),
                DRAIN_DEADLINE,
              )
              expect(posted.timedOut).toBe(false)

              yield* sender.publisher.react(ref, decodeEmojiSync('thumbs_up'))
              const added = yield* drainUntil(
                queue,
                (e) => isReactionAdded(e) && e.target.id === ref.id && e.emoji === 'thumbs_up',
                DRAIN_DEADLINE,
              )
              expect(added.timedOut).toBe(false)
              const addedMatch = added.events.find(
                (e): e is Extract<InboundEvent, { kind: 'reaction-added' }> =>
                  isReactionAdded(e) && e.target.id === ref.id,
              )
              expect(addedMatch).toBeDefined()
              expect(addedMatch?.by.id).toEqual(senderId)
              expect(addedMatch?.target.channel.id).toEqual(channel.id)

              yield* sender.publisher.unreact(ref, decodeEmojiSync('thumbs_up'))
              const removed = yield* drainUntil(
                queue,
                (e) => isReactionRemoved(e) && e.target.id === ref.id && e.emoji === 'thumbs_up',
                DRAIN_DEADLINE,
              )
              expect(removed.timedOut).toBe(false)
              const removedMatch = removed.events.find(
                (e): e is Extract<InboundEvent, { kind: 'reaction-removed' }> =>
                  isReactionRemoved(e) && e.target.id === ref.id,
              )
              expect(removedMatch).toBeDefined()
              expect(removedMatch?.by.id).toEqual(senderId)
            }),
          ),
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )

  test(
    'observer.inbox.events() yields reaction-added when reaction targets a pre-subscribe message (cache-miss fallback)',
    () =>
      liveRuntime.runPromise(
        withBots(
          ({ sender, observer, channel }) =>
            Effect.scoped(
              Effect.gen(function* () {
                // ass-ps6h: post BEFORE subscribe so the observer's iterator never
                // sees the message-posted and its MessageRef cache is empty when the
                // reaction lands. Option (b) — per-event /messages?anchor=<id>
                // lookup — has to bridge the gap, otherwise the reaction drops.
                const marker = `live-rxn-miss-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
                const senderId = (yield* sender.identity.currentIdentity()).id
                const ref = yield* sender.publisher.post(channel, decodeMessageBodySync(marker))
                yield* observer.inbox.subscribe(channel)
                const queue = yield* eventQueue(observer)
                yield* sender.publisher.react(ref, decodeEmojiSync('eyes'))
                const added = yield* drainUntil(
                  queue,
                  (e) => isReactionAdded(e) && e.target.id === ref.id && e.emoji === 'eyes',
                  DRAIN_DEADLINE,
                )
                expect(added.timedOut).toBe(false)
                const match = added.events.find(
                  (e): e is Extract<InboundEvent, { kind: 'reaction-added' }> =>
                    isReactionAdded(e) && e.target.id === ref.id,
                )
                expect(match).toBeDefined()
                expect(match?.target.channel.id).toEqual(channel.id)
                expect(match?.by.id).toEqual(senderId)
                // Tidy up — leave no reaction on the test channel between runs.
                yield* sender.publisher.unreact(ref, decodeEmojiSync('eyes'))
              }),
            ),
          // Defer the observer's channel subscribe to the body (after the post),
          // so its queue never sees message-posted and the cache-miss fallback is
          // genuinely exercised rather than satisfied from a populated cache.
          { subscribeObserverUpFront: false },
        ),
      ),
    EVENTS_TEST_TIMEOUT_MS,
  )
})

describeLive('zulip live upload round-trip — zulip.example.com', () => {
  // The unit tests prove uploadRaw shapes a multipart request; only a real
  // round-trip proves Django actually accepts that body and the bytes survive.
  // Uses the minter directly — two HTTP calls, no bot minting, no rate-limit
  // pacing needed (comms-nsa).
  test('uploadRaw → downloadRaw round-trips the bytes through /user_uploads', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* minterHttp(liveEnv())
        const bytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02])
        const filename = `cc-live-upload-${Date.now()}.bin`

        const uploaded = yield* http.uploadRaw(filename, bytes)
        expect(uploaded.url).toMatch(/^\/user_uploads\//)
        expect(uploaded.filename.length).toBeGreaterThan(0)

        const back = yield* http.downloadRaw(uploaded.url)
        expect(new Uint8Array(back.data)).toEqual(bytes)
      }),
    ))
})
