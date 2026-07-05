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
import { type MemoryAdapter, memoryAdapter } from '@commy/memory/adapter'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { Effect, Option, type Scope } from 'effect'
import type { ProjectSlug, SessionId } from './bootstrap.ts'
import { createEphemeralIdentityCache } from './identity-cache.ts'
import { buildMcpServer } from './mcp-server.ts'
import { createNarrowSet, type NarrowSet } from './narrow-set.ts'
import { intentToTarget, type SubscribeIntent } from './subscribe-parser.ts'
import { makeSessionRestore, seedDefaultsIfFresh } from './subscription-restore.ts'
import type { SubscriptionStore } from './subscription-store.ts'
import { registerTools } from './tools.ts'

// The reactive core (comms-k7cv): a resumed ephemeral seat reboots with an empty
// narrow set, so the event pump matches nothing and silently drops inbound —
// including a human's decision reaction on a subscribed thread. Restore is a
// reaction to the session_id becoming known: the moment a feeder (acquire /
// subscribe / unsubscribe) delivers the id, the reactive latch rehydrates the
// persisted set. The passive `current_identity` read is deliberately NOT a feeder
// here — it is a pure read; delivering the id from a zero-tool-call listen-only
// seat is its own follow-up (comms-3qi2.2).

const DECISIONS_CHANNEL = 'commy'
const DECISIONS_THREAD = 'ref-types-split-decisions'
const SID_RESUME = '61b08d76-0000-4000-8000-000000000001'

const decisionsThreadIntent: SubscribeIntent = {
  kind: 'thread',
  channelName: decodeChannelNameSync(DECISIONS_CHANNEL),
  threadName: decodeThreadNameSync(DECISIONS_THREAD),
}

const inMemorySubscriptionStore = (
  seed: ReadonlyMap<string, ReadonlyArray<SubscribeIntent>> = new Map(),
): SubscriptionStore => {
  const store = new Map<string, ReadonlyArray<SubscribeIntent>>(seed)
  return {
    read: (id: SessionId) => Effect.sync(() => Option.fromNullable(store.get(id as string))),
    write: (id: SessionId, intents: ReadonlyArray<SubscribeIntent>) =>
      Effect.sync(() => void store.set(id as string, intents)),
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

interface ResumeRig {
  readonly client: Client
  readonly adapter: MemoryAdapter
  readonly narrowSet: NarrowSet
  readonly substrateSubscribes: ReadonlyArray<SubscriptionTarget>
  readonly seedCalls: ReadonlyArray<ProjectSlug | undefined>
}

// Boot a fresh MCP server whose subscription store already holds the persisted
// narrow set of a prior session — the exact state after an MCP-child reboot on
// resume: in-memory narrow set empty, disk snapshot intact, same session_id.
// `ensureSessionSubscriptions` mirrors server.ts: the reactive restore latch
// (`makeSessionRestore`) plus the store-gated fresh-session seed. It is wired as
// the tools' feeder — subscribe/unsubscribe call it; current_identity does not.
const buildResumeRig = (
  persisted: ReadonlyArray<SubscribeIntent>,
): Effect.Effect<ResumeRig, never, Scope.Scope> =>
  Effect.gen(function* () {
    const adapter = yield* memoryAdapter()
    const substrateSubscribes: SubscriptionTarget[] = []
    const realSubscribe = adapter.inbox.subscribe.bind(adapter.inbox)
    const inbox = {
      ...adapter.inbox,
      subscribe: (target: SubscriptionTarget) =>
        Effect.sync(() => void substrateSubscribes.push(target)).pipe(
          Effect.zipRight(realSubscribe(target)),
        ),
    }
    const spiedAdapter = { ...adapter, inbox } as MemoryAdapter

    const identityCache = yield* createEphemeralIdentityCache({
      acquire: adapter.identity.acquire,
      release: adapter.identity.release,
      idleReleaseMs: 60 * 60 * 1000,
    })
    const narrowSet = createNarrowSet()
    const subscriptionStore = inMemorySubscriptionStore(new Map([[SID_RESUME, persisted]]))

    const seedCalls: (ProjectSlug | undefined)[] = []
    const sessionRestoreFeed = yield* makeSessionRestore({ subscriptionStore, narrowSet, inbox })
    const ensureSessionSubscriptions = (
      sessionId: SessionId,
      project: ProjectSlug | undefined,
    ): Effect.Effect<void> =>
      sessionRestoreFeed(sessionId).pipe(
        Effect.catchAll(() => Effect.void),
        Effect.zipRight(
          seedDefaultsIfFresh(
            {
              subscriptionStore,
              registerDefaults: (p) => Effect.sync(() => void seedCalls.push(p)),
            },
            sessionId,
            project,
          ).pipe(Effect.catchAll(() => Effect.void)),
        ),
      )

    const server = buildMcpServer()
    registerTools(server, {
      adapter: spiedAdapter,
      identityCache,
      narrowSet,
      ensureSessionSubscriptions,
    })

    const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'commy-resume-test', version: '0.0.0' }, { capabilities: {} })
    yield* Effect.promise(() =>
      Promise.all([server.connect(serverTransport), client.connect(clientTransport)]),
    )
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        await client.close()
        await server.close()
      }),
    )

    return { client, adapter: spiedAdapter, narrowSet, substrateSubscribes, seedCalls }
  })

test('a feeder (subscribe) on resume rehydrates the persisted narrow set so a decisions-topic reaction is deliverable', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildResumeRig([decisionsThreadIntent, { kind: 'mentions' }])

        // Rebooted: nothing restored yet, so the pump would drop the reaction.
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)

        // A feeder delivers the session_id — here the seat's own subscribe.
        yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'subscribe',
            arguments: { target: 'channel:other', session_id: SID_RESUME },
          }),
        )

        // The reactive latch rehydrated the persisted set: the human's :one:
        // reaction on the decisions thread now matches...
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(true)
        // ...the restored thread was re-subscribed on the substrate...
        expect(rig.substrateSubscribes).toContainEqual(intentToTarget(decisionsThreadIntent))
        // ...and the newly-subscribed intent was added on top of the restored set.
        expect(rig.narrowSet.intents()).toContainEqual({
          kind: 'channel',
          channelName: decodeChannelNameSync('other'),
        })
        // A resume is not a fresh session: no Type-2 defaults were seeded.
        expect(rig.seedCalls).toEqual([])
      }),
    ),
  ))

test('passive current_identity on resume is a pure read — it does NOT rehydrate the persisted set', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildResumeRig([decisionsThreadIntent, { kind: 'mentions' }])

        // Rebooted: deaf. The passive "am I still bound?" check must not change that.
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_RESUME },
          }),
        )

        // Passivity preserved: no identity acquired by the check.
        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })

        // Still deaf: current_identity is not a feeder, so it restored nothing.
        // Rehydrating a zero-tool-call listen-only seat is comms-3qi2.2's job.
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
        expect(rig.substrateSubscribes).toEqual([])
        expect(rig.seedCalls).toEqual([])
      }),
    ),
  ))

// The fresh-session contract: a passive current_identity for a session_id the
// store has never seen neither restores nor seeds — Type-2 defaults stay on the
// acquire hook (server.integration.test.ts).
const SID_FRESH = 'f9e5f9e5-0000-4000-8000-000000000002'

test('passive current_identity for a never-seen session restores and seeds nothing', () =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const rig = yield* buildResumeRig([decisionsThreadIntent])

        const result = yield* Effect.promise(() =>
          rig.client.callTool({
            name: 'current_identity',
            arguments: { session_id: SID_FRESH },
          }),
        )

        expect(result.structuredContent).toEqual({ state: 'unbound', identity: null })
        expect(rig.narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
        expect(rig.substrateSubscribes).toEqual([])
        expect(rig.seedCalls).toEqual([])
      }),
    ),
  ))

// The reactive core (comms-k7cv): restore is a reaction to the session_id
// becoming known, not a thing a specific action triggers. A `Deferred<SessionId>`
// is the first-wins latch — the first feeder to deliver the id runs restore-only
// (synchronously, so the narrow set is live before that call returns); every later
// delivery is a no-op. Feeders (any tool call carrying session_id, acquire) stay
// ignorant of restore; they just publish the id.
const asSessionId = (raw: string): SessionId => raw as unknown as SessionId

test('makeSessionRestore restores a resumed narrow set on the first session_id delivery and is a no-op thereafter', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      const substrateSubscribes: SubscriptionTarget[] = []
      const inbox = {
        subscribe: (target: SubscriptionTarget) =>
          Effect.sync(() => {
            substrateSubscribes.push(target)
          }),
      }
      const subscriptionStore = inMemorySubscriptionStore(
        new Map([[SID_RESUME, [decisionsThreadIntent, { kind: 'mentions' }]]]),
      )

      const feed = yield* makeSessionRestore({ subscriptionStore, narrowSet, inbox })

      // Rebooted: empty narrow set, deaf.
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)

      // The moment the id is known — via any feeder — restore reacts, restore-only.
      yield* feed(asSessionId(SID_RESUME))
      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(true)
      expect(substrateSubscribes).toContainEqual(intentToTarget(decisionsThreadIntent))

      // First-wins latch: a second delivery does not restore again.
      const subscribedCount = substrateSubscribes.length
      yield* feed(asSessionId(SID_RESUME))
      expect(substrateSubscribes.length).toBe(subscribedCount)
    }),
  ))

// The fresh-session contract holds at the core too: a delivery for a session the
// store has never seen restores nothing and — crucially — seeds nothing. Type-2
// defaults stay acquire-gated; the reactive core is restore-only.
test('makeSessionRestore restores nothing for a never-seen session (no fresh-session seed)', () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const narrowSet = createNarrowSet()
      const substrateSubscribes: SubscriptionTarget[] = []
      const inbox = {
        subscribe: (target: SubscriptionTarget) =>
          Effect.sync(() => {
            substrateSubscribes.push(target)
          }),
      }
      const subscriptionStore = inMemorySubscriptionStore(
        new Map([[SID_RESUME, [decisionsThreadIntent]]]),
      )

      const feed = yield* makeSessionRestore({ subscriptionStore, narrowSet, inbox })

      yield* feed(asSessionId(SID_FRESH))

      expect(narrowSet.matches(reactionOnDecisionsThread(), undefined)).toBe(false)
      expect(substrateSubscribes).toEqual([])
    }),
  ))
