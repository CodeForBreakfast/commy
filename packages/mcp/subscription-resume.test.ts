import { expect, test } from 'bun:test'
import type { InboundEvent, MessageRef, SubscriptionTarget } from '@commy/core/ports'
import {
  ChannelPermalinkSchema,
  decodeChannelIdSync,
  decodeChannelNameSync,
  decodeDisplayNameSync,
  decodeEmojiSync,
  decodeIdentityIdSync,
  decodeMessageIdSync,
  decodeThreadNameSync,
  MessagePermalinkSchema,
  ThreadPermalinkSchema,
} from '@commy/core/ports'
import { memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Deferred, Effect, Fiber, Option, Predicate, type Scope } from 'effect'
import type { SessionId } from './bootstrap.ts'
import { createEphemeralIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import { createNarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import { restoreSubscriptions } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'
import { registerTools } from './tools.ts'

// The reactive core: a resumed ephemeral seat reboots with an empty narrow set,
// so the event pump matches nothing and silently drops inbound — including a
// human's decision reaction on a subscribed thread. Restore is a reaction to the
// session_id becoming known: `restoreSubscriptions` is forked once at boot and
// parks on the session-bound store's internal `Deferred.await` until any source
// fills the shared session-id — the boot-env feeder for a listen-only seat, or a
// stamped tool call carrying the id. No session_id threads through its signature;
// filling the shared deferred is the whole trigger.
//
// Because restore loads asynchronously, subscribe/unsubscribe deltas can arrive
// before it completes. The narrow set journals those deltas while buffering and
// replays them onto the restored base, so none is lost and a dropped default is
// not resurrected. These tests drive that model deterministically: fork restore,
// fill the shared deferred with `Deferred.succeed` alone, `Fiber.join`, assert.

const DECISIONS_CHANNEL = 'commy'
const DECISIONS_THREAD = 'ref-types-split-decisions'
const SID_RESUME = '61b08d76-0000-4000-8000-000000000001'
const SID_FRESH = 'f9e5f9e5-0000-4000-8000-000000000002'

const generalTopicsIntent: SubscribeIntent = {
  kind: 'new-topics-in-channel',
  channelName: decodeChannelNameSync('general'),
}
const decisionsThreadIntent: SubscribeIntent = {
  kind: 'thread',
  channelName: decodeChannelNameSync(DECISIONS_CHANNEL),
  threadName: decodeThreadNameSync(DECISIONS_THREAD),
}
const channelIntent = (name: string): SubscribeIntent => ({
  kind: 'channel',
  channelName: decodeChannelNameSync(name),
})
const sortIntents = (intents: ReadonlyArray<SubscribeIntent>): ReadonlyArray<SubscribeIntent> =>
  [...intents].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)))

// A substrate-subscribe spy: restore wires each restored intent on the substrate
// via `inbox.subscribe`, mirroring the real adapter's side effect.
const spyInbox = (): {
  readonly inbox: { subscribe: (target: SubscriptionTarget) => Effect.Effect<void> }
  readonly subscribes: ReadonlyArray<SubscriptionTarget>
} => {
  const subscribes: SubscriptionTarget[] = []
  return {
    subscribes,
    inbox: { subscribe: (target) => Effect.sync(() => void subscribes.push(target)) },
  }
}

const asSessionId = (raw: string): SessionId => raw as unknown as SessionId

// The session-bound in-memory store: it resolves its id from the shared
// session-id deferred it captures — never a per-call argument — mirroring the
// live file store. The Map keying survives so multi-session seeds still work.
const inMemorySubscriptionStore = (
  session: Deferred.Deferred<SessionId>,
  seed: ReadonlyMap<string, ReadonlyArray<SubscribeIntent>> = new Map(),
): SubscriptionStore => {
  const store = new Map<string, ReadonlyArray<SubscribeIntent>>(seed)
  return {
    read: () =>
      Effect.map(Deferred.await(session), (id) => Option.fromNullable(store.get(id as string))),
    write: (intents: ReadonlyArray<SubscribeIntent>) =>
      Effect.flatMap(Deferred.await(session), (id) =>
        Effect.sync(() => void store.set(id as string, intents)),
      ),
  }
}

const reactionOnDecisionsThread = (): InboundEvent => {
  const channel = {
    id: decodeChannelIdSync(DECISIONS_CHANNEL),
    name: decodeChannelNameSync(DECISIONS_CHANNEL),
    permalink: ChannelPermalinkSchema.make(
      `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}`,
    ),
  }
  const target: MessageRef = {
    id: decodeMessageIdSync('msg-decisions-ask'),
    channel,
    thread: Option.some({
      name: decodeThreadNameSync(DECISIONS_THREAD),
      resolved: false,
      permalink: ThreadPermalinkSchema.make(
        `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}/topic/${DECISIONS_THREAD}`,
      ),
    }),
    permalink: MessagePermalinkSchema.make(
      `https://zulip.example.com/#narrow/channel/${DECISIONS_CHANNEL}/topic/${DECISIONS_THREAD}/near/1`,
    ),
  }
  return {
    kind: 'reaction-added',
    target,
    emoji: decodeEmojiSync('one'),
    by: {
      id: decodeIdentityIdSync('user:graeme'),
      name: decodeDisplayNameSync('graeme'),
      kind: 'human',
    },
  }
}

// The zero-action rehydrate: restore fires purely from the shared session-id
// deferred being filled — no tool call, no ensureSessionSubscriptions. This is
// the resumed listen-only seat the reactive core exists to close.
test('restore rehydrates the persisted narrow set when the shared deferred is filled — zero tool calls', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      narrowSet.beginBuffering()
      const { inbox, subscribes } = spyInbox()
      const session = yield* Deferred.make<SessionId>()
      const subscriptionStore = inMemorySubscriptionStore(
        session,
        new Map([[SID_RESUME, [decisionsThreadIntent, generalTopicsIntent]]]),
      )

      // Boot-fork restore: it parks on the store read, which awaits the deferred.
      const fiber = yield* Effect.fork(
        restoreSubscriptions({ subscriptionStore, narrowSet, inbox }),
      )

      // Rebooted, deaf: nothing restored while the id is unknown.
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)

      // Fill the shared deferred ALONE — no callTool.
      yield* Deferred.succeed(session, asSessionId(SID_RESUME))
      yield* Fiber.join(fiber)

      // Restored: the human's :one: reaction on the decisions thread now matches,
      // and the thread was re-wired on the substrate.
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(true)
      expect(subscribes).toContainEqual(intentToTarget(decisionsThreadIntent))
      expect(sortIntents(narrowSet.intents())).toEqual(
        sortIntents([decisionsThreadIntent, generalTopicsIntent]),
      )
    }),
  ))

// The buffer-and-replay proof: subscribe/unsubscribe deltas that arrive before
// restore loads are journaled and replayed onto the restored base in arrival
// order — the pre-restore subscribe survives the base swap, the pre-restore
// unsubscribe of a persisted sub still removes it, and a default the persisted
// set had dropped is not resurrected.
test('deltas racing the load are journaled and replayed onto the restored base', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      // A COMMY_SUBSCRIBE default the persisted set turns out to have dropped.
      narrowSet.add(channelIntent('env-default'))
      narrowSet.beginBuffering()
      const { inbox } = spyInbox()
      const session = yield* Deferred.make<SessionId>()
      const subscriptionStore = inMemorySubscriptionStore(
        session,
        new Map([[SID_RESUME, [decisionsThreadIntent, generalTopicsIntent]]]),
      )

      const fiber = yield* Effect.fork(
        restoreSubscriptions({ subscriptionStore, narrowSet, inbox }),
      )

      // Before the id lands: subscribe something new, unsubscribe a persisted sub.
      narrowSet.add(channelIntent('fresh-sub'))
      narrowSet.remove(generalTopicsIntent)

      yield* Deferred.succeed(session, asSessionId(SID_RESUME))
      yield* Fiber.join(fiber)

      // Restored base with deltas replayed: fresh-sub survived, new-topics removed,
      // env-default stayed dropped.
      expect(sortIntents(narrowSet.intents())).toEqual(
        sortIntents([decisionsThreadIntent, channelIntent('fresh-sub')]),
      )
    }),
  ))

// A fresh session (store miss) loads no base: the COMMY_SUBSCRIBE seed stands and
// buffered deltas replay onto it. Restoring nothing is the whole point — a
// never-seen session must not inherit another session's persisted set.
test('a fresh session (store miss) keeps the env seed and replays buffered deltas', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      narrowSet.add(channelIntent('env-default'))
      narrowSet.beginBuffering()
      const { inbox, subscribes } = spyInbox()
      const session = yield* Deferred.make<SessionId>()
      // The store holds a DIFFERENT session's data; SID_FRESH is a miss.
      const subscriptionStore = inMemorySubscriptionStore(
        session,
        new Map([[SID_RESUME, [decisionsThreadIntent]]]),
      )

      const fiber = yield* Effect.fork(
        restoreSubscriptions({ subscriptionStore, narrowSet, inbox }),
      )
      narrowSet.add(channelIntent('fresh-sub'))

      yield* Deferred.succeed(session, asSessionId(SID_FRESH))
      yield* Fiber.join(fiber)

      // Env seed kept, buffered delta replayed, nothing re-subscribed, and the
      // other session's decisions thread was NOT restored.
      expect(sortIntents(narrowSet.intents())).toEqual(
        sortIntents([channelIntent('env-default'), channelIntent('fresh-sub')]),
      )
      expect(subscribes).toEqual([])
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
    }),
  ))

// An empty persisted set is honoured verbatim: the resumed seat hears only what
// it re-subscribed since boot, and a dropped default stays dropped.
test('an empty persisted set is honoured — the env default is not resurrected', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      narrowSet.add(channelIntent('env-default'))
      narrowSet.beginBuffering()
      const { inbox } = spyInbox()
      const session = yield* Deferred.make<SessionId>()
      const subscriptionStore = inMemorySubscriptionStore(session, new Map([[SID_RESUME, []]]))

      const fiber = yield* Effect.fork(
        restoreSubscriptions({ subscriptionStore, narrowSet, inbox }),
      )
      yield* Deferred.succeed(session, asSessionId(SID_RESUME))
      yield* Fiber.join(fiber)

      expect(narrowSet.intents()).toEqual([])
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
    }),
  ))

// The persist path: subscribe/unsubscribe write the current narrow-set
// snapshot to the session-keyed store after each mutation. But the
// PreToolUse matcher never stamps `session_id` on subscribe/unsubscribe, so
// persist cannot read the id from its own args — it must be id-blind. It polls
// the shared session-id deferred and writes only when the id is already known
// (fed by the boot-env feeder or a prior hooked tool call), no-op otherwise.
// Writing unconditionally would park on the store's `Deferred.await` for a
// subscribe-first seat whose id no source has delivered.
const channelOtherIntent: SubscribeIntent = {
  kind: 'channel',
  channelName: decodeChannelNameSync('other'),
}

interface PersistRig {
  readonly client: Client
  readonly store: SubscriptionStore
  readonly session: Deferred.Deferred<SessionId>
}

const buildPersistRig = (): Effect.Effect<PersistRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    // The sticky-engagement tests below post into this channel; the subscribe
    // tests never touch it.
    yield* adapter.seedChannel(DECISIONS_CHANNEL).pipe(Effect.orDie)
    const identityCache = yield* createEphemeralIdentityCache({
      acquire: adapter.identity.acquire,
      release: adapter.identity.release,
      idleReleaseMs: 60 * 60 * 1000,
    })
    const narrowSet = createNarrowSet()
    const session = yield* Deferred.make<SessionId>()
    const store = inMemorySubscriptionStore(session)
    // Mirror server.ts `persistSessionSubscriptions`: a bare lazy Effect that
    // polls the shared deferred, writing the snapshot only when the id is known,
    // no-op (never park) when not.
    const persistSessionSubscriptions: Effect.Effect<void> = Deferred.poll(session).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.void,
          onSome: () => store.write(narrowSet.intents()).pipe(Effect.catchAll(() => Effect.void)),
        }),
      ),
    )

    const server = buildMcpServer()
    registerTools(server, {
      adapter,
      identityCache,
      narrowSet,
      persistSessionSubscriptions,
    })

    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client(
      { name: 'commy-persist-test', version: '0.0.0' },
      { capabilities: {} },
    )
    yield* Effect.promise(() =>
      Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await client.close()
        await server.close()
      }),
    )

    return { client, store, session }
  })

test('subscribe carrying no session_id persists the snapshot when the id is already known', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildPersistRig()
        // The boot-env feeder (or a prior hooked call) has already filled the
        // shared deferred — the fleet's real state (CC injects the env at boot).
        yield* Deferred.succeed(rig.session, asSessionId(SID_RESUME))

        // A subscribe that carries NO session_id in args: the matcher never
        // stamps it on subscribe, so this is the real live shape.
        yield* Effect.promise(() =>
          rig.client.callTool({ name: 'subscribe', arguments: { target: 'other' } }),
        )

        // Persist fired id-blind: the session-keyed snapshot now holds the new
        // intent, so a later resume restores it.
        const persisted = yield* rig.store.read()
        expect(persisted).toEqual(Option.some([channelOtherIntent]))
      }),
    ),
  ))

test('subscribe carrying no session_id with an unfed deferred returns promptly and does not park', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildPersistRig()
        // Deferred deliberately UNFED: a subscribe-first seat whose id no source
        // has delivered. An unconditional persist would park on the store's
        // `Deferred.await`; the poll-guard must no-op and let the call return.
        const outcome = yield* Effect.promise(() =>
          rig.client.callTool({ name: 'subscribe', arguments: { target: 'other' } }),
        ).pipe(Effect.timeoutOption('2 seconds'))

        // Handler returned rather than hanging on the unfed deferred.
        expect(Option.isSome(outcome)).toBe(true)
      }),
    ),
  ))

// Sticky engagement — participating in a thread by posting or reacting — adds a
// thread intent to the narrow set and wires it on the substrate. It used to stop
// there. Restore honours the persisted set verbatim, so an intent acquired by
// PARTICIPATING never reached disk and a resumed seat came back silently deaf to
// threads it believed it was in: correctly engaged, silently deaf, and the store
// reading clean to anyone who inspected it afterwards, because the store was
// never wrong about what it held — it simply never held them.

// The MCP client types `callTool`'s result as a union without a structured
// payload, so narrow rather than cast.
const postedMessageId = (result: unknown): string => {
  const structured = Predicate.hasProperty(result, 'structuredContent')
    ? result.structuredContent
    : undefined
  const id =
    Predicate.hasProperty(structured, 'message_id') && Predicate.isString(structured.message_id)
      ? structured.message_id
      : undefined
  expect(id).toBeString()
  return id ?? ''
}

const stickyThreadIntent = (channel: string, thread: string): SubscribeIntent => ({
  kind: 'thread',
  channelName: decodeChannelNameSync(channel),
  threadName: decodeThreadNameSync(thread),
})

test('posting into a thread persists the sticky intent so a resume restores it', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildPersistRig()
        yield* Deferred.succeed(rig.session, asSessionId(SID_RESUME))

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: DECISIONS_CHANNEL,
              thread: DECISIONS_THREAD,
              body: 'engagement acquired by speaking',
              session_id: SID_RESUME,
            },
          }),
        )

        const persisted = yield* rig.store.read()
        expect(persisted).toEqual(
          Option.some([stickyThreadIntent(DECISIONS_CHANNEL, DECISIONS_THREAD)]),
        )
      }),
    ),
  ))

test('reacting in a thread persists the sticky intent so a resume restores it', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildPersistRig()
        yield* Deferred.succeed(rig.session, asSessionId(SID_RESUME))

        const posted = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: DECISIONS_CHANNEL,
              thread: DECISIONS_THREAD,
              body: 'a message to react to',
              session_id: SID_RESUME,
            },
          }),
        )

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'react',
            arguments: {
              message_id: postedMessageId(posted),
              emoji: 'wave',
              session_id: SID_RESUME,
            },
          }),
        )

        // React reaches sticky engagement on its own path (a cache-hit ref
        // carries the observed thread), so the intent survives either way.
        const persisted = yield* rig.store.read()
        expect(persisted).toEqual(
          Option.some([stickyThreadIntent(DECISIONS_CHANNEL, DECISIONS_THREAD)]),
        )
      }),
    ),
  ))

// Top-level posts get no sticky thread behaviour, so there is nothing to
// persist — and persisting an empty snapshot would overwrite a restored set
// with nothing.
test('a top-level post acquires no sticky intent and writes no snapshot', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildPersistRig()
        yield* Deferred.succeed(rig.session, asSessionId(SID_RESUME))

        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'post',
            arguments: {
              channel_name: DECISIONS_CHANNEL,
              body: 'terse status ping',
              session_id: SID_RESUME,
            },
          }),
        )

        expect(yield* rig.store.read()).toEqual(Option.none())
      }),
    ),
  ))
